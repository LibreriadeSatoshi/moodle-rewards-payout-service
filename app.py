"""FastAPI payout gateway for the Moodle plugin.

Contract:
  POST /pay                  → {tx_id, status, dest_type, error, retryable}
  GET  /status/{tx_id}       → live SDK state (debug)
  GET  /balance              → wallet balance (debug)
  GET  /rate                 → BTC/USD rate in cents
  Webhook (outbound)         → Moodle receives terminal events, retried until 2xx
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import breez_sdk_spark as spark
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

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
    if not x_internal_token or x_internal_token != settings.internal_token:
        raise HTTPException(status_code=401, detail="Invalid token")


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


class StatusResponse(BaseModel):
    tx_id: str
    status: str
    amount_sats: int = 0


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.post("/pay", response_model=PayResponse, dependencies=[auth_required])
async def pay(body: PayRequest) -> PayResponse:
    """Accept a payout request, fire off the Lightning send, return immediately.
    Terminal state arrives later via webhook."""
    cap = settings.daily_send_cap_sats
    if cap > 0:
        spent = await spark_client.daily_sent_sats()
        if spent + body.amount_sats > cap:
            raise HTTPException(
                status_code=429,
                detail=f"daily send cap reached: {spent} of {cap} sats spent in the last 24h",
            )

    try:
        await spark_client.ensure_ln_address(body.destination)
    except spark_client.UnsupportedDestination as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    result = await spark_client.send_lightning(body.destination, body.amount_sats)

    return PayResponse(
        tx_id=result["tx_id"],
        status=result["status"],
        dest_type="ln_address",
        error=result["error"],
        retryable=bool(result.get("retryable", True)),
    )


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
