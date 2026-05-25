"""FastAPI payout gateway for the Moodle plugin.

Contract:
  POST /pay                  → {tx_id, status, dest_type, error, retryable}
  POST /parse                → {dest_type, invoice_msat}
  GET  /status/{tx_id}       → live SDK state (debug)
  GET  /balance              → wallet balance (debug)
  GET  /rate                 → BTC/USD rate in cents
  GET  /limits               → live per-rail send limits
  Webhook (outbound)         → Moodle receives terminal events, retried until 2xx
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import breez_sdk_liquid as breez
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

import breez_client
import rate as rate_module
from config import settings
from webhook import retrier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    breez_client.connect()
    retrier.start()
    yield
    retrier.stop()
    breez_client.disconnect()


app = FastAPI(title="BTC Payout Service", lifespan=lifespan)


# ── Dependencies ───────────────────────────────────────────────────────────

def require_token(x_internal_token: str | None = Header(default=None)) -> None:
    """Reject requests whose `X-Internal-Token` is missing or doesn't match.

    Accept the header as optional so we control the response (401) instead of
    letting FastAPI emit a 422 for the missing-header case.
    """
    if not x_internal_token or x_internal_token != settings.internal_token:
        raise HTTPException(status_code=401, detail="Invalid token")


def get_sdk() -> breez.BindingLiquidSdk:
    """Return the singleton SDK handle."""
    return breez_client.connect()


# Convenience: route-level guard that doesn't pollute the handler signature.
auth_required = Depends(require_token)


# ── Models ─────────────────────────────────────────────────────────────────

class PayRequest(BaseModel):
    amount_sats: int
    destination: str


class PayResponse(BaseModel):
    tx_id: str
    status: str
    dest_type: str
    error: str = ""
    retryable: bool = True


class ParseRequest(BaseModel):
    destination: str


class ParseResponse(BaseModel):
    dest_type: str
    invoice_msat: int | None = None


class StatusResponse(BaseModel):
    tx_id: str
    status: str
    amount_sats: int = 0


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.post("/pay", response_model=PayResponse, dependencies=[auth_required])
def pay(body: PayRequest) -> PayResponse:
    """Accept a payout request. Classifies the destination, fires off the payment,
    and returns immediately. Terminal state arrives later via webhook."""
    cap = settings.daily_send_cap_sats
    if cap > 0:
        spent = breez_client.daily_sent_sats()
        if spent + body.amount_sats > cap:
            raise HTTPException(
                status_code=429,
                detail=f"daily send cap reached: {spent} of {cap} sats spent in the last 24h",
            )

    try:
        dest_type = breez_client.classify(body.destination)
    except breez_client.UnsupportedDestination as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if dest_type == "onchain":
        result = breez_client.send_onchain(body.destination, body.amount_sats)
    else:
        result = breez_client.send_lightning(body.destination, body.amount_sats, dest_type)

    return PayResponse(
        tx_id=result["tx_id"],
        status=result["status"],
        dest_type=dest_type,
        error=result["error"],
        retryable=bool(result.get("retryable", True)),
    )


@app.post("/parse", response_model=ParseResponse, dependencies=[auth_required])
def parse(body: ParseRequest) -> ParseResponse:
    """Decode a destination and return its type plus embedded amount (if any).
    Used by the plugin to validate bolt11 amounts at claim time before
    persisting a queue row."""
    try:
        result = breez_client.parse(body.destination)
    except breez_client.UnsupportedDestination as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return ParseResponse(**result)


@app.get("/status/{tx_id}", response_model=StatusResponse, dependencies=[auth_required])
def get_status(tx_id: str) -> StatusResponse:
    """Debug — reads live state from the SDK, not the outbox."""
    info = breez_client.lookup_status(tx_id)
    if info is None:
        raise HTTPException(status_code=404, detail="tx_id not found")
    return StatusResponse(**info)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/balance", dependencies=[auth_required])
def balance(sdk: breez.BindingLiquidSdk = Depends(get_sdk)):
    """Debug — wallet balance in sats."""
    w = sdk.get_info().wallet_info
    return {
        "balance_sat": w.balance_sat,
        "pending_send_sat": w.pending_send_sat,
        "pending_receive_sat": w.pending_receive_sat,
    }


@app.get("/limits", dependencies=[auth_required])
def limits():
    """Live per-rail send limits in sats. Onchain has a non-trivial floor
    (Boltz swap minimum); Lightning has a much lower one. The plugin uses
    this to warn the student before submitting an unpayable destination.
    Cached ~60s in the breez_client module."""
    return breez_client.get_send_limits()


@app.get("/rate", dependencies=[auth_required])
def rate():
    """Current BTC/USD rate in cents per BTC. Plugin calls this at claim time
    to lock the USD→sats conversion. Cached ~60s in the rate module."""
    try:
        cents = rate_module.get_cents_per_btc()
    except rate_module.RateUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"cents_per_btc": cents}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=settings.host, port=settings.port, reload=True)
