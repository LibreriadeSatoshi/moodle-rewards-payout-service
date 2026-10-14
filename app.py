"""FastAPI payout gateway for the Moodle plugin.

Contract:
  POST /pay                  → {tx_id, status, dest_type, error, retryable}
  POST /deposit              → {payment_request, fee_sat} bolt11 to fund the wallet
  GET  /status/{tx_id}       → live SDK state (debug)
  GET  /balance              → wallet balance (debug)
  GET  /rate                 → BTC/USD rate in cents
  Webhook (outbound)         → Moodle receives terminal events, retried until 2xx
"""

from __future__ import annotations

import logging
import json
from uuid import UUID
from typing import Any

import db
from contextlib import asynccontextmanager

import breez_sdk_spark as spark
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

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

class KeyedPayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    amount_sats: int = Field(gt=0)
    destination: str = Field(min_length=1)


class PayRequest(BaseModel):
    amount_sats: int
    destination: str
    request_id: UUID | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_keyed_request(cls, data: Any) -> Any:
        # Preserve legacy validation when no recovery identity is supplied.
        if isinstance(data, dict) and data.get("request_id") is not None:
            return KeyedPayRequest.model_validate(data).model_dump()
        return data


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

@app.post("/pay", response_model=PayResponse, dependencies=[auth_required])
async def pay(body: PayRequest) -> PayResponse:
    """Accept a payout request, fire off the Lightning send, return immediately.
    An optional request ID enables durable recovery; terminal state also arrives
    later via webhook."""
    if body.request_id is not None:
        return await _pay_with_request_id(body)

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


async def _pay_with_request_id(body: PayRequest) -> PayResponse:
    """Durable identity, single submission, and conservative recovery for /pay.

    An unknown result must be reconciled using this same key, never a new payout.
    """
    request_id = str(body.request_id)
    row, owner = db.reserve_payout(request_id, body.destination, body.amount_sats)
    if row["destination"] != body.destination or row["amount_sats"] != body.amount_sats:
        raise HTTPException(status_code=409, detail="request_id already belongs to a different payout")
    saved = json.loads(row["response_json"]) if row["response_json"] else None
    if saved and "rejected" in saved:
        raise HTTPException(**saved["rejected"])
    if saved and saved["status"] in {"settled", "failed"}:
        return PayResponse(**saved)
    result = None
    if not owner:
        result = await spark_client.lookup_request(request_id)
    else:
        rejection = None
        try:
            cap = settings.daily_send_cap_sats
            if cap > 0 and await spark_client.daily_sent_sats() + body.amount_sats > cap:
                rejection = {"status_code": 429, "detail": "daily send cap reached"}
            else:
                await spark_client.ensure_ln_address(body.destination)
                result = await spark_client.send_lightning(body.destination, body.amount_sats, request_id=request_id)
        except spark_client.DestinationResolutionFailed as exc:
            rejection = {"status_code": 502, "detail": f"destination resolution failed: {exc}"}
        except spark_client.UnsupportedDestination as exc:
            rejection = {"status_code": 400, "detail": str(exc)}
        except Exception:
            logger.exception("Payout submission did not return a definitive result")
        if rejection:
            # No send occurred; the identity stays reserved and every retry gets the same rejection.
            db.record_payout(request_id, {"rejected": rejection})
            raise HTTPException(**rejection)
    if result is None:
        result = {"tx_id": "", "status": "unknown", "error": "Payment requires reconciliation", "retryable": False}
    response = PayResponse(**result, dest_type="ln_address")
    if result["status"] in {"settled", "failed"}:
        db.record_payout(request_id, response.model_dump())
    return response


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
