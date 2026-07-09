"""Wrapper around the Breez SDK Spark Python bindings.

Spark is Lightning-only on this service — onchain destinations are rejected
upstream at the FastAPI layer before reaching this module.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Awaitable, Callable

import breez_sdk_spark as spark

import db
from config import settings

logger = logging.getLogger(__name__)

_sdk: spark.BreezSdk | None = None


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


async def _run_send(label: str, op: Callable[[], Awaitable["spark.Payment"]]) -> dict:
    """Run an SDK send operation and shape success/failure into our response dict."""
    try:
        payment = await op()
        return {
            "tx_id": payment.id or "",
            "status": _map_status(payment.status),
            "error": "",
            "retryable": True,
        }
    except Exception as exc:
        logger.exception("%s payment failed", label)
        return {"tx_id": "", "status": "failed", "error": str(exc), "retryable": True}


async def send_lightning(destination: str, amount_sats: int) -> dict:
    """Send a Lightning payment to a Lightning address via the LNURL-pay flow.

    Returns: {tx_id, status, error, retryable}. For terminal outcomes a webhook
    will follow (from the listener); the caller gets only the *initial* state.
    """
    sdk = await connect()

    async def _do() -> "spark.Payment":
        parsed = await sdk.parse(input=destination)
        if not parsed.is_lightning_address():
            raise UnsupportedDestination("Destination is not a Lightning address")
        pay_request = parsed[0].pay_request
        prepare = await sdk.prepare_lnurl_pay(
            request=spark.PrepareLnurlPayRequest(amount=amount_sats, pay_request=pay_request)
        )
        response = await sdk.lnurl_pay(
            request=spark.LnurlPayRequest(prepare_response=prepare)
        )
        return response.payment

    return await _run_send("lightning", _do)


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
            )
        )
    )
    return {"payment_request": response.payment_request, "fee_sat": int(response.fee)}


async def daily_sent_sats() -> int:
    """Sum of outbound sats over the last rolling 24 hours.

    Used by the daily-cap policy at /pay. Failures fall back to 0 so a transient
    SDK hiccup doesn't pause payouts; the cap is defense-in-depth, not the
    primary correctness guarantee.
    """
    sdk = await connect()
    one_day_ago = int(time.time()) - 86400
    try:
        response = await sdk.list_payments(
            request=spark.ListPaymentsRequest(
                from_timestamp=one_day_ago,
                type_filter=[spark.PaymentType.SEND],
            )
        )
    except Exception:
        logger.exception("list_payments failed during daily-cap check")
        return 0
    return sum(int(p.amount or 0) for p in response.payments)


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
