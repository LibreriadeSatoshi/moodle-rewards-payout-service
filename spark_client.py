"""Wrapper around the Breez SDK Spark Python bindings.

Spark is Lightning-only on this service — onchain destinations are rejected
upstream at the FastAPI layer before reaching this module.
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
import breez_sdk_spark as spark

import db
from config import settings

logger = logging.getLogger(__name__)

_sdk: spark.BreezSdk | None = None

# Fixed namespace so payment_key() maps a payout_id to the same UUID forever.
_PAYOUT_NAMESPACE = uuid.UUID("6c1f0b9e-3a57-4c39-9d0e-5f2b8a1c7e44")


class UnsupportedDestination(Exception):
    """Raised when the SDK can't parse the destination or the type isn't payable."""


class _Listener(spark.EventListener):
    """On terminal outbound payment events, enqueues a webhook delivery.

    The translation logic — event variant → status, payment → body — lives in
    webhook.py next to the consumer. This class is here only because the
    listener registers with the SDK at connect() time.
    """

    async def on_event(self, event: spark.SdkEvent) -> None:
        from webhook import event_to_terminal_status, payload_for  # break the cycle

        logger.info("Spark event: %s", type(event).__name__)
        status = event_to_terminal_status(event)
        if status is None:
            return
        payment = getattr(event, "payment", None)
        if payment is None or payment.payment_type == spark.PaymentType.RECEIVE:
            return
        tx_id = payment.id
        if not tx_id:
            return
        payload = payload_for(payment, status)
        inserted = db.enqueue(tx_id, status, payload)
        if inserted:
            logger.info("Enqueued webhook for tx_id=%s status=%s", tx_id, status)


def _get_network() -> spark.Network:
    net = settings.breez_network.lower()
    if net == "regtest":
        return spark.Network.REGTEST
    return spark.Network.MAINNET


async def connect() -> spark.BreezSdk:
    """Initialise (or return cached) SDK connection."""
    global _sdk
    if _sdk is not None:
        return _sdk

    Path(settings.breez_working_dir).mkdir(parents=True, exist_ok=True)

    config = spark.default_config(network=_get_network())
    config.api_key = settings.breez_api_key

    req = spark.ConnectRequest(
        config=config,
        seed=spark.Seed.MNEMONIC(mnemonic=settings.breez_mnemonic, passphrase=None),
        storage_dir=settings.breez_working_dir,
    )
    sdk = await spark.connect(req)
    await sdk.add_event_listener(listener=_Listener())
    _sdk = sdk
    logger.info("Spark SDK connected (network=%s)", settings.breez_network)
    return _sdk


async def disconnect() -> None:
    global _sdk
    if _sdk is not None:
        await _sdk.disconnect()
        _sdk = None
        logger.info("Spark SDK disconnected")


async def ensure_ln_address(destination: str) -> None:
    """Verify the SDK parses the destination as a Lightning address.

    Raises UnsupportedDestination for anything else (onchain, bolt11/bolt12,
    raw spark addresses) and for garbage input. The plugin layer additionally
    enforces a domain allowlist; this is the rail-level check.
    """
    sdk = await connect()
    try:
        parsed = await sdk.parse(input=destination)
    except Exception as exc:
        raise UnsupportedDestination(f"Unparseable destination: {exc}") from exc
    if not parsed.is_lightning_address():
        raise UnsupportedDestination("Only Lightning address destinations are supported")


def payment_key(payout_id: str) -> str:
    """The SDK idempotency key (a UUID) for a payout_id. Stable across restarts."""
    return str(uuid.uuid5(_PAYOUT_NAMESPACE, payout_id))


async def send_lightning(destination: str, amount_sats: int, idempotency_key: str) -> dict:
    """Send a Lightning payment to a Lightning address via the LNURL-pay flow.

    The SDK makes at most one payment per idempotency_key: its storage is
    checked first and the key doubles as the Spark transfer id, so a repeat
    returns the original payment instead of paying again. That makes every
    failure here safe to retry with the same key.

    Returns: {tx_id, status, error, retryable}. For terminal outcomes a webhook
    will follow (from the listener); the caller gets only the *initial* state.
    """
    sdk = await connect()
    try:
        parsed = await sdk.parse(input=destination)
        if not parsed.is_lightning_address():
            raise UnsupportedDestination("Destination is not a Lightning address")
        prepare = await sdk.prepare_lnurl_pay(
            request=spark.PrepareLnurlPayRequest(amount=amount_sats, pay_request=parsed[0].pay_request)
        )
        response = await sdk.lnurl_pay(
            request=spark.LnurlPayRequest(prepare_response=prepare, idempotency_key=idempotency_key)
        )
    except Exception as exc:
        logger.exception("lightning payment failed")
        return {"tx_id": "", "status": "failed", "error": str(exc), "retryable": True}

    payment = response.payment
    status = _map_status(payment.status)
    return {
        "tx_id": payment.id or "",
        "status": status,
        "error": "",
        # A payment the SDK reports failed is final for this key: no money left,
        # and resending the key would only return the same failed payment.
        "retryable": status != "failed",
    }


async def payment_status(tx_id: str) -> str | None:
    """Live status of a payment, or None if the SDK can't say."""
    sdk = await connect()
    try:
        response = await sdk.get_payment(request=spark.GetPaymentRequest(payment_id=tx_id))
    except Exception:
        logger.exception("get_payment failed for %s", tx_id)
        return None
    return _map_status(response.payment.status)


async def create_deposit_invoice(amount_sats: int, description: str) -> dict:
    """Create a bolt11 invoice paying into this service's wallet.

    Used by the plugin's admin "fund wallet" flow. Settlement is visible via
    /balance; the webhook listener deliberately ignores RECEIVE events.
    """
    sdk = await connect()
    response = await sdk.receive_payment(
        request=spark.ReceivePaymentRequest(
            payment_method=spark.ReceivePaymentMethod.BOLT11_INVOICE(
                description=description,
                amount_sats=amount_sats,
                expiry_secs=3600,
                payment_hash=None,
                receiver_identity_public_key=None,
            )
        )
    )
    return {"payment_request": response.payment_request, "fee_sat": int(response.fee)}


class CapCheckUnavailable(Exception):
    """Raised when the wallet's payment history can't be read for the daily cap."""


async def _sends_since(from_timestamp: int) -> list:
    """All outbound payments since from_timestamp, following pagination."""
    sdk = await connect()
    payments: list = []
    page = 100
    while True:
        response = await sdk.list_payments(
            request=spark.ListPaymentsRequest(
                from_timestamp=from_timestamp,
                type_filter=[spark.PaymentType.SEND],
                offset=len(payments),
                limit=page,
            )
        )
        payments.extend(response.payments)
        if len(response.payments) < page:
            return payments


async def daily_sent_sats() -> int:
    """Sum of outbound sats over the last rolling 24 hours, failed sends excluded.

    Used by the daily-cap policy at /pay. Raises CapCheckUnavailable if the SDK
    can't list payments: the cap must fail closed, or an SDK error would turn it
    off exactly when something is going wrong.
    """
    try:
        payments = await _sends_since(int(time.time()) - 86400)
    except Exception as exc:
        logger.exception("list_payments failed during daily-cap check")
        raise CapCheckUnavailable(str(exc)) from exc
    return sum(int(p.amount or 0) for p in payments if p.status != spark.PaymentStatus.FAILED)


async def pending_balances() -> dict:
    """Sum pending send/receive sats from list_payments (Spark has no native field)."""
    sdk = await connect()
    try:
        response = await sdk.list_payments(
            request=spark.ListPaymentsRequest(status_filter=[spark.PaymentStatus.PENDING], limit=200)
        )
    except Exception:
        logger.exception("list_payments failed during pending-balance check")
        return {"pending_send_sat": 0, "pending_receive_sat": 0}
    send = sum(int(p.amount or 0) for p in response.payments if p.payment_type == spark.PaymentType.SEND)
    recv = sum(int(p.amount or 0) for p in response.payments if p.payment_type == spark.PaymentType.RECEIVE)
    return {"pending_send_sat": send, "pending_receive_sat": recv}


async def lookup_status(tx_id: str) -> dict | None:
    """Return the current SDK-reported status for a given tx_id, or None."""
    sdk = await connect()
    try:
        response = await sdk.list_payments(request=spark.ListPaymentsRequest(limit=200))
    except Exception:
        logger.exception("list_payments failed")
        return None
    for p in response.payments:
        if p.id == tx_id:
            return {"tx_id": tx_id, "status": _map_status(p.status), "amount_sats": int(p.amount or 0)}
    return None


def _map_status(sdk_status) -> str:
    if sdk_status == spark.PaymentStatus.COMPLETED:
        return "settled"
    if sdk_status == spark.PaymentStatus.FAILED:
        return "failed"
    if sdk_status == spark.PaymentStatus.PENDING:
        return "processing"
    return "accepted"
