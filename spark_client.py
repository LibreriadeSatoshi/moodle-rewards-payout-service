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
import receipts
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
        if status == "failed":
            payout = db.get_payout_by_tx(tx_id)
            if payout is not None and payout["verify_url"]:
                # The SDK can report failed although the receiver was paid: the
                # verify checker reports the outcome once the receiver confirms.
                if db.mark_verifying(payout["payout_id"]):
                    logger.warning("Payment %s reported failed: verifying with the receiver", tx_id)
                return
        else:
            payout = db.get_payout_by_tx(tx_id)
            if payout is not None and db.mark_paid(payout["payout_id"]):
                logger.warning("Payment %s reported failed earlier now succeeded", tx_id)
        payload = payload_for(payment, status)
        outcome = db.enqueue(tx_id, status, payload)
        if outcome == "inserted":
            logger.info("Enqueued webhook for tx_id=%s status=%s", tx_id, status)
        elif outcome == "upgraded":
            logger.warning("Payment %s reported failed earlier is now settled: delivering settled", tx_id)


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


async def pay_request(destination: str):
    """The LNURL-pay details of a Lightning address (callback, metadata, limits).

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
    return parsed[0].pay_request


async def ensure_ln_address(destination: str) -> None:
    """Raise UnsupportedDestination unless destination is a Lightning address."""
    await pay_request(destination)


async def invoice_details(invoice: str):
    """SDK-parsed details of a bolt11 invoice (amount, payment hash, expiry...)."""
    sdk = await connect()
    try:
        parsed = await sdk.parse(input=invoice)
    except Exception as exc:
        raise receipts.InvalidInvoice(f"unparseable invoice: {exc}") from exc
    if not parsed.is_bolt11_invoice():
        raise receipts.InvalidInvoice("the Lightning address did not answer with a bolt11 invoice")
    return parsed[0]


def payment_key(payout_id: str) -> str:
    """The SDK idempotency key (a UUID) for a payout_id. Stable across restarts."""
    return str(uuid.uuid5(_PAYOUT_NAMESPACE, payout_id))


async def send_invoice(invoice: str, idempotency_key: str) -> dict:
    """Pay a bolt11 invoice.

    The SDK makes at most one payment per idempotency_key: its storage is
    checked first and the key doubles as the Spark transfer id, so a repeat
    returns the original payment instead of paying again. That makes every
    failure here safe to retry with the same key.

    Returns: {tx_id, status, error}; tx_id is empty when the send raised. For
    terminal outcomes a webhook will follow (from the listener); the caller
    gets only the *initial* state.
    """
    sdk = await connect()
    try:
        prepare = await sdk.prepare_send_payment(
            request=spark.PrepareSendPaymentRequest(payment_request=spark.PaymentRequest.INPUT(input=invoice))
        )
        response = await sdk.send_payment(
            request=spark.SendPaymentRequest(prepare_response=prepare, idempotency_key=idempotency_key)
        )
    except Exception as exc:
        logger.exception("lightning payment failed")
        return {"tx_id": "", "status": "failed", "error": str(exc)}

    payment = response.payment
    return {"tx_id": payment.id or "", "status": _map_status(payment.status), "error": ""}


class PaymentLookupUnavailable(Exception):
    """Raised when the SDK can't say whether a payment exists."""


async def find_payment(payment_id: str) -> dict | None:
    """{tx_id, status} of a payment, or None if the wallet has no such payment.

    A payment sent with an idempotency key has that key as its id. Raises
    PaymentLookupUnavailable on any other SDK error, so callers never mistake
    an unreadable wallet for a missing payment.
    """
    sdk = await connect()
    try:
        response = await sdk.get_payment(request=spark.GetPaymentRequest(payment_id=payment_id))
    except Exception as exc:
        # SDK 0.25 storage reports a missing row as "Query returned no rows".
        if "no rows" in str(exc).lower():
            return None
        logger.exception("get_payment failed for %s", payment_id)
        raise PaymentLookupUnavailable(str(exc)) from exc
    payment = response.payment
    return {"tx_id": payment.id, "status": _map_status(payment.status)}


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
