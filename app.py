"""FastAPI payout gateway for the Moodle plugin.

Contract:
  POST /pay                  → {tx_id, status, dest_type, error, retryable}
                               idempotent by payout_id: a repeat never pays twice
  POST /deposit              → {payment_request, fee_sat} bolt11 to fund the wallet
  GET  /status/{tx_id}       → live SDK state (debug)
  GET  /balance              → wallet balance (debug)
  GET  /rate                 → BTC/USD rate in cents
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
    yield
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


@app.post("/pay", response_model=PayResponse, dependencies=[auth_required])
async def pay(body: PayRequest) -> PayResponse:
    """Accept a payout request, fire off the Lightning send, return immediately.
    Terminal state arrives later via webhook."""
    cap = settings.daily_send_cap_sats
    if body.amount_sats <= 0:
        raise HTTPException(status_code=400, detail="amount_sats must be positive")
    if cap > 0 and body.amount_sats > cap:
        raise HTTPException(status_code=400, detail=f"amount_sats exceeds the daily cap of {cap} sats")

    async with _pay_lock:
        existing = db.get_payout(body.payout_id)
        if existing is not None:
            return _replay(existing, body)

        if cap > 0:
            try:
                spent = await spark_client.daily_sent_sats()
            except spark_client.CapCheckUnavailable as exc:
                raise HTTPException(status_code=503, detail=f"daily cap check unavailable: {exc}")
            if spent + body.amount_sats > cap:
                raise HTTPException(
                    status_code=429,
                    detail=f"daily send cap reached: {spent} of {cap} sats spent in the last 24h",
                )

        try:
            await spark_client.ensure_ln_address(body.destination)
        except spark_client.UnsupportedDestination as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        db.start_payout(body.payout_id, body.amount_sats, body.destination)
        started = int(time.time()) - 5
        result = await spark_client.send_lightning(body.destination, body.amount_sats)

        if result["tx_id"]:
            db.finish_payout(body.payout_id, "sent", tx_id=result["tx_id"])
        elif not result["attempted"]:
            # Nothing left the wallet: let the plugin's retry send it.
            db.forget_payout(body.payout_id)
        else:
            found = await spark_client.find_send(body.amount_sats, started)
            if found is not None:
                logger.warning("send for payout %s raised but payment %s exists", body.payout_id, found["tx_id"])
                db.finish_payout(body.payout_id, "sent", tx_id=found["tx_id"])
                result = {**result, **found, "error": ""}
            else:
                db.finish_payout(body.payout_id, "unknown", error=result["error"])
                result = {**result, "retryable": False, "error": _unknown_error(result["error"])}

    return PayResponse(
        tx_id=result["tx_id"],
        status=result["status"],
        dest_type="ln_address",
        error=result["error"],
        retryable=bool(result.get("retryable", True)),
    )


def _unknown_error(cause: str) -> str:
    return f"send outcome unknown, check the wallet before paying this again: {cause}"


def _replay(existing: dict, body: PayRequest) -> PayResponse:
    """Answer a repeated payout_id from the ledger without sending anything."""
    if existing["amount_sats"] != body.amount_sats or existing["destination"] != body.destination:
        raise HTTPException(status_code=409, detail="payout_id was already used for a different payment")
    logger.info("payout %s already recorded (state=%s), not sending again", body.payout_id, existing["state"])
    if existing["state"] == "sent":
        return PayResponse(tx_id=existing["tx_id"], status="processing", dest_type="ln_address")
    if existing["state"] == "sending":
        # A send for this payout is in flight or the service died mid-send.
        return PayResponse(
            tx_id="", status="failed", dest_type="ln_address", retryable=False,
            error=_unknown_error("a previous attempt did not finish"),
        )
    return PayResponse(
        tx_id="", status="failed", dest_type="ln_address", retryable=False,
        error=_unknown_error(existing["error"] or ""),
    )


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
