"""FastAPI payout gateway for the Moodle plugin.

Contract:
  POST /pay                  → {tx_id, status, dest_type, error, retryable}
                               idempotent by payout_id: a payout_id pays at most once
  POST /deposit              → {payment_request, fee_sat} bolt11 to fund the wallet
  GET  /status/{tx_id}       → live SDK state (debug)
  GET  /balance              → wallet balance (debug)
  GET  /rate                 → BTC/USD rate in cents
  GET  /webhooks/undelivered → webhook deliveries still retrying or given up
  Webhook (outbound)         → Moodle receives terminal events, retried until 2xx
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import time
from contextlib import asynccontextmanager

import breez_sdk_spark as spark
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

import db
import receipts
import spark_client
import rate as rate_module
from config import settings
from webhook import retrier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    await spark_client.connect()
    retrier.start()
    receipts.checker.start()
    yield
    receipts.checker.stop()
    retrier.stop()
    await spark_client.disconnect()


app = FastAPI(title="BTC Payout Service", lifespan=lifespan)


# ── Dependencies ───────────────────────────────────────────────────────────

def require_token(x_internal_token: str | None = Header(default=None)) -> None:
    """Reject requests whose `X-Internal-Token` is missing or doesn't match.

    Accept the header as optional so we control the response (401) instead of
    letting FastAPI emit a 422 for the missing-header case.
    """
    if not x_internal_token or not hmac.compare_digest(
        x_internal_token.encode(), settings.internal_token.encode()
    ):
        raise HTTPException(status_code=401, detail="Invalid token")


# Convenience: route-level guard that doesn't pollute the handler signature.
auth_required = Depends(require_token)


# ── Models ─────────────────────────────────────────────────────────────────

class PayRequest(BaseModel):
    # The plugin's id for this payout, unique per Moodle site. A repeated
    # payout_id returns the first attempt's outcome instead of paying again.
    payout_id: str = Field(min_length=1, max_length=200)
    amount_sats: int
    destination: str


class PayResponse(BaseModel):
    tx_id: str
    status: str
    dest_type: str
    error: str = ""
    retryable: bool = True


class DepositRequest(BaseModel):
    amount_sats: int
    description: str = "BTC Rewards wallet funding"


class DepositResponse(BaseModel):
    payment_request: str
    fee_sat: int = 0


class StatusResponse(BaseModel):
    tx_id: str
    status: str
    amount_sats: int = 0


# ── Endpoints ──────────────────────────────────────────────────────────────

# One payout at a time, so the daily-cap check and the send can't interleave.
_pay_lock = asyncio.Lock()

# A request that waited longer than this for the lock is refused instead of
# sent: the plugin has likely timed out on it already and will retry the same
# payout_id later.
MAX_LOCK_WAIT_SECONDS = 30


@app.post("/pay", response_model=PayResponse, dependencies=[auth_required])
async def pay(body: PayRequest) -> PayResponse:
    """Send a payout, at most once per payout_id. Terminal state arrives later via webhook.

    The service gets the invoice from the Lightning address itself and keeps it
    with the payout, so every retry pays the same invoice with the same SDK
    idempotency key: a retry after a timeout, crash or SDK error returns the
    original payment if one was made and pays only if none was. A send that
    raised answers 503: the plugin keeps the payout pending and retries.

    When the SDK reports the payment failed and the invoice has a LUD-21 verify
    URL, the payout is 'verifying': answered "processing" until the receiver
    confirms it (webhook settled) or still says "not paid" after
    VERIFY_FAIL_AFTER_HOURS (webhook failed). See receipts.py.
    """
    cap = settings.daily_send_cap_sats
    if body.amount_sats <= 0:
        raise HTTPException(status_code=400, detail="amount_sats must be positive")
    if cap > 0 and body.amount_sats > cap:
        raise HTTPException(status_code=400, detail=f"amount_sats exceeds the daily cap of {cap} sats")

    waiting_since = time.monotonic()
    async with _pay_lock:
        if time.monotonic() - waiting_since > MAX_LOCK_WAIT_SECONDS:
            raise HTTPException(status_code=503, detail="service busy, retry later")

        key = spark_client.payment_key(body.payout_id)
        payout = db.get_payout(body.payout_id)
        if payout is not None:
            if payout["amount_sats"] != body.amount_sats or payout["destination"] != body.destination:
                raise HTTPException(status_code=409, detail="payout_id was already used for a different payment")
            if payout["state"] == "sent":
                return await _sent_response(payout)
            if payout["state"] == "verifying":
                return _verifying_response(payout)
            if payout["state"] == "failed":
                return _payment_response(payout["tx_id"] or "", "failed")
            # 'pending': an earlier attempt got no payment back. If it went out
            # anyway, answer with it; otherwise this is a fresh send.
            try:
                found = await spark_client.find_payment(key)
            except spark_client.PaymentLookupUnavailable as exc:
                raise HTTPException(status_code=503, detail=f"payment lookup unavailable: {exc}")
            if found is not None:
                return _record_payment(payout, found)

        await _check_cap(body.amount_sats)
        if payout is None:
            invoice = await _new_invoice(body.payout_id, body.destination, body.amount_sats)
            db.start_payout(body.payout_id, body.amount_sats, body.destination, invoice, tx_id=key)
        else:
            # Payouts recorded before tx_id was set at creation get it now, so
            # events for this send are matched to the payout.
            db.set_tx_id_if_missing(body.payout_id, key)
            if not payout["invoice"] or payout["expires_at"] <= time.time():
                # No invoice yet (payout from before invoices were stored), or it
                # expired: no payment exists under this key, so a new invoice is
                # safe. The old one stays in payout_invoices and is still verified.
                db.set_invoice(body.payout_id, await _new_invoice(body.payout_id, body.destination, body.amount_sats))
        payout = db.get_payout(body.payout_id)

        result = await spark_client.send_invoice(payout["invoice"], key)
        if not result["tx_id"]:
            db.finish_payout(body.payout_id, "pending", error=result["error"])
            raise HTTPException(status_code=503, detail=f"send failed, retry later: {result['error']}")
        return _record_payment(payout, result)


async def _new_invoice(payout_id: str, destination: str, amount_sats: int) -> dict:
    """Get and check an invoice for amount_sats from a Lightning address."""
    try:
        details = await spark_client.pay_request(destination)
    except spark_client.UnsupportedDestination as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not details.min_sendable <= amount_sats * 1000 <= details.max_sendable:
        raise HTTPException(status_code=400, detail="amount outside the Lightning address's limits")
    try:
        answer = await asyncio.to_thread(receipts.request_invoice, details.callback, amount_sats)
    except receipts.InvoiceUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    try:
        invoice = await spark_client.invoice_details(answer["invoice"])
        receipts.check_invoice(invoice, amount_sats, details.metadata_str)
    except receipts.InvalidInvoice as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    owner = db.invoice_owner(invoice.payment_hash)
    if owner is not None and owner != payout_id:
        # Paying it would fail, and its verify URL would vouch for the other
        # payout's payment: this payout would look paid without being paid.
        raise HTTPException(status_code=502, detail="the Lightning address returned an invoice already used")
    return {
        "invoice": answer["invoice"],
        "payment_hash": invoice.payment_hash,
        "verify_url": answer["verify_url"],
        "expires_at": invoice.timestamp + invoice.expiry,
    }


def _record_payment(payout: dict, payment: dict) -> PayResponse:
    """Record the SDK's payment for a payout and answer with the payout's state."""
    payout_id, tx_id = payout["payout_id"], payment["tx_id"]
    db.record_sent(payout_id, tx_id)
    # With idempotency keys the SDK may return an earlier payment whose webhook was given up.
    db.revive(tx_id)
    if payment["status"] == "failed":
        if payout["verify_url"]:
            db.mark_verifying(payout_id)
        else:
            db.finish_payout(payout_id, "failed", tx_id=tx_id)
    payout = db.get_payout(payout_id)
    if payout["state"] == "verifying":
        return _verifying_response(payout)
    if payout["preimage"]:
        return _payment_response(tx_id, "settled")  # The receiver confirmed it.
    return _payment_response(tx_id, payment["status"])


def _verifying_response(payout: dict) -> PayResponse:
    # The plugin keeps the payout ACCEPTED; the verify checker sends the outcome by webhook.
    return PayResponse(tx_id=payout["tx_id"] or "", status="processing", dest_type="ln_address")


async def _check_cap(amount_sats: int) -> None:
    """Refuse a send that would take the rolling 24h total over the cap."""
    cap = settings.daily_send_cap_sats
    if cap <= 0:
        return
    try:
        spent = await spark_client.daily_sent_sats()
    except spark_client.CapCheckUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"daily cap check unavailable: {exc}")
    if spent + amount_sats > cap:
        raise HTTPException(
            status_code=429,
            detail=f"daily send cap reached: {spent} of {cap} sats spent in the last 24h",
        )


def _payment_response(tx_id: str, status: str) -> PayResponse:
    # A payment the SDK reports failed is final: no money left, and its key
    # would only return the same failed payment.
    return PayResponse(tx_id=tx_id, status=status, dest_type="ln_address", retryable=status != "failed")


async def _sent_response(payout: dict) -> PayResponse:
    """Answer a payout that already has a payment with that payment's live state.

    Moodle is asking about this payout again, so it will now store the tx_id:
    retry its webhook from scratch in case the retrier had given up on it.
    """
    tx_id = payout["tx_id"]
    db.revive(tx_id)
    if payout["preimage"]:
        return _payment_response(tx_id, "settled")  # The receiver confirmed it.
    try:
        found = await spark_client.find_payment(tx_id)
    except spark_client.PaymentLookupUnavailable:
        found = None
    if found is not None and found["status"] == "failed" and payout["verify_url"]:
        db.mark_verifying(payout["payout_id"])
        return _verifying_response(payout)
    return _payment_response(tx_id, found["status"] if found else "processing")


@app.post("/deposit", response_model=DepositResponse, dependencies=[auth_required])
async def deposit(body: DepositRequest) -> DepositResponse:
    """Create a bolt11 invoice so an admin can fund the payout wallet from an
    external wallet. The invoice expires after 1 hour."""
    if body.amount_sats <= 0:
        raise HTTPException(status_code=400, detail="amount_sats must be positive")
    try:
        result = await spark_client.create_deposit_invoice(body.amount_sats, body.description)
    except Exception as exc:
        logger.exception("deposit invoice creation failed")
        raise HTTPException(status_code=502, detail=f"invoice creation failed: {exc}")
    return DepositResponse(**result)


@app.get("/status/{tx_id}", response_model=StatusResponse, dependencies=[auth_required])
async def get_status(tx_id: str) -> StatusResponse:
    """Debug — reads live state from the SDK, not the outbox."""
    info = await spark_client.lookup_status(tx_id)
    if info is None:
        raise HTTPException(status_code=404, detail="tx_id not found")
    return StatusResponse(**info)


@app.get("/webhooks/undelivered", dependencies=[auth_required])
def webhooks_undelivered():
    """Webhook deliveries Moodle hasn't accepted: still retrying, or given up
    after WEBHOOK_GIVE_UP_DAYS and waiting for manual review. Also the payouts
    the SDK reported failed that are still waiting for the receiver's word."""
    waiting = [
        {key: p[key] for key in ("payout_id", "tx_id", "state", "amount_sats", "unconfirmed_since", "hours_unconfirmed")}
        for p in db.verifying()
    ]
    return {**db.undelivered(), "verifying": waiting}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/balance", dependencies=[auth_required])
async def balance():
    """Debug — wallet balance in sats. pending_* fields are synthesized from
    list_payments since Spark's GetInfoResponse doesn't expose them."""
    sdk = await spark_client.connect()
    info = await sdk.get_info(request=spark.GetInfoRequest(ensure_synced=False))
    pending = await spark_client.pending_balances()
    return {
        "balance_sat": info.balance_sats,
        "pending_send_sat": pending["pending_send_sat"],
        "pending_receive_sat": pending["pending_receive_sat"],
    }


@app.get("/rate", dependencies=[auth_required])
async def rate():
    """Current BTC/USD rate in cents per BTC. Plugin calls this at claim time
    to lock the USD→sats conversion. Cached ~60s in the rate module."""
    try:
        cents = await rate_module.get_cents_per_btc()
    except rate_module.RateUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"cents_per_btc": cents}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=settings.host, port=settings.port, reload=True)
