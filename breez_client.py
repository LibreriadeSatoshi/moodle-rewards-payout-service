"""Wrapper around the Breez SDK Liquid Python bindings."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Callable

import breez_sdk_liquid as breez

import db
from config import settings

logger = logging.getLogger(__name__)

_sdk: breez.BindingLiquidSdk | None = None
_lock = threading.Lock()


class UnsupportedDestination(Exception):
    """Raised when the SDK can't parse the destination or the type isn't payable."""


class _Listener(breez.EventListener):
    """On terminal payment events, enqueues a webhook delivery for the retrier.

    The translation logic — event variant → status, payment → body — lives in
    webhook.py next to the consumer. This class is here only because the
    listener registers with the SDK at connect() time.
    """

    def on_event(self, event: breez.SdkEvent) -> None:
        from webhook import event_to_terminal_status, payload_for  # local import to break the cycle

        logger.info("Breez event: %s", type(event).__name__)
        status = event_to_terminal_status(event)
        if status is None:
            return
        payment = getattr(event, "details", None)
        if payment is None or getattr(payment, "payment_type", None) == breez.PaymentType.RECEIVE:
            return
        tx_id = getattr(payment, "tx_id", None)
        if not tx_id:
            return
        payload = payload_for(payment, status)
        inserted = db.enqueue(tx_id, status, payload)
        if inserted:
            logger.info("Enqueued webhook for tx_id=%s status=%s", tx_id, status)


def _get_network() -> breez.LiquidNetwork:
    net = settings.breez_network.lower()
    if net == "regtest":
        return breez.LiquidNetwork.REGTEST
    return breez.LiquidNetwork.MAINNET


def connect() -> breez.BindingLiquidSdk:
    """Initialise (or return cached) SDK connection."""
    global _sdk
    if _sdk is not None:
        return _sdk
    with _lock:
        if _sdk is not None:
            return _sdk

        Path(settings.breez_working_dir).mkdir(parents=True, exist_ok=True)

        config = breez.default_config(
            network=_get_network(),
            breez_api_key=settings.breez_api_key,
        )
        config.working_dir = settings.breez_working_dir

        req = breez.ConnectRequest(
            mnemonic=settings.breez_mnemonic,
            config=config,
        )
        sdk = breez.connect(req)
        sdk.add_event_listener(_Listener())
        _sdk = sdk
        logger.info("Breez SDK connected (network=%s)", settings.breez_network)
        return _sdk


def disconnect() -> None:
    global _sdk
    if _sdk is not None:
        _sdk.disconnect()
        _sdk = None
        logger.info("Breez SDK disconnected")


def parse(destination: str) -> dict:
    """Decode a destination via the SDK and return type + embedded amount.

    Returns: {'dest_type': 'onchain'|'bolt11'|'bolt12'|'ln_address',
              'invoice_msat': int|None}  (msat only set for fixed-amount bolt11)
    Raises UnsupportedDestination for garbage input or unsupported types.
    """
    sdk = connect()
    try:
        parsed = sdk.parse(destination)
    except Exception as exc:
        raise UnsupportedDestination(f"Unparseable destination: {exc}") from exc

    if parsed.is_bitcoin_address():
        return {"dest_type": "onchain", "invoice_msat": None}
    if parsed.is_bolt11():
        invoice_msat = getattr(parsed.invoice, "amount_msat", None)
        return {"dest_type": "bolt11", "invoice_msat": invoice_msat}
    if parsed.is_bolt12_offer():
        return {"dest_type": "bolt12", "invoice_msat": None}
    if parsed.is_ln_url_pay():
        return {"dest_type": "ln_address", "invoice_msat": None}

    kind = type(parsed).__name__.removeprefix("_InputType_")
    raise UnsupportedDestination(f"Unsupported destination type: {kind}")


def classify(destination: str) -> str:
    """Infer the payment type from the destination string via the SDK parser.

    Returns one of: 'onchain', 'bolt11', 'bolt12', 'ln_address'.
    Raises UnsupportedDestination for garbage input or unsupported types.
    """
    return parse(destination)["dest_type"]


def _run_send(label: str, op: Callable[[], "breez.Payment"]) -> dict:
    """Run an SDK send operation and shape success/failure into our response dict.

    Both `send_lightning` and `send_onchain` share the same try/except shell:
    on success they emit the payment's tx_id + initial status, on failure they
    log + return a transient-failure response. This helper centralises that
    shape so the call sites only describe *what* to send.
    """
    try:
        payment = op()
        return {
            "tx_id": payment.tx_id or "",
            "status": _map_status(payment.status),
            "error": "",
            "retryable": True,
        }
    except Exception as exc:
        logger.exception("%s payment failed", label)
        return {"tx_id": "", "status": "failed", "error": str(exc), "retryable": True}


def _check_send_range(amount_sats: int, rail: str) -> dict | None:
    """Return a permanent-failure response if amount_sats is outside the rail's range.

    Pre-flight against the cached send-limits. Best-effort: if the limits
    fetch itself fails we return None and let the SDK call discover the
    constraint (the result will be a transient-looking failure, fine).
    """
    try:
        limits = get_send_limits()
    except Exception:
        logger.warning("send-range check skipped — limits fetch failed", exc_info=True)
        return None
    rail_limits = limits[f"{rail}_send"]
    if amount_sats < rail_limits["min_sat"]:
        return {
            "tx_id": "", "status": "failed",
            "error": f"amount_below_min: {amount_sats} sats < {rail_limits['min_sat']} sats {rail} minimum",
            "retryable": False,
        }
    if amount_sats > rail_limits["max_sat"]:
        return {
            "tx_id": "", "status": "failed",
            "error": f"amount_above_max: {amount_sats} sats > {rail_limits['max_sat']} sats {rail} maximum",
            "retryable": False,
        }
    return None


def _validate_bolt11_amount(sdk: breez.BindingLiquidSdk, destination: str, amount_sats: int) -> int | None:
    """Return invoice_msat (None for amountless invoices) or raise on mismatch.

    A non-matching fixed-amount invoice is a permanent error — the caller
    should bail with retryable=False without ever reaching the SDK send path.
    """
    parsed = sdk.parse(destination)
    invoice_msat = getattr(parsed.invoice, "amount_msat", None)
    if invoice_msat is not None and invoice_msat != amount_sats * 1000:
        raise UnsupportedDestination(
            f"amount_mismatch: invoice={invoice_msat // 1000} sats, requested={amount_sats} sats"
        )
    return invoice_msat


def send_lightning(destination: str, amount_sats: int, dest_type: str) -> dict:
    """Send a Lightning payment — bolt11, bolt12 offer, or LN address.

    Returns: {tx_id, status, error, retryable}. For terminal outcomes a webhook
    will follow (from the listener); the caller gets only the *initial* state.
    """
    if (rejection := _check_send_range(amount_sats, "lightning")) is not None:
        logger.warning("rejecting lightning: %s", rejection["error"])
        return rejection

    sdk = connect()

    invoice_msat: int | None = None
    if dest_type == "bolt11":
        try:
            invoice_msat = _validate_bolt11_amount(sdk, destination, amount_sats)
        except UnsupportedDestination as exc:
            logger.warning("rejecting bolt11: %s", exc)
            return {"tx_id": "", "status": "failed", "error": str(exc), "retryable": False}

    def _do() -> "breez.Payment":
        # Pass amount unless the bolt11 invoice already carries one (passing
        # both would conflict). Bolt12/ln_address always need a sender amount.
        if dest_type == "bolt11" and invoice_msat is not None:
            req = breez.PrepareSendRequest(destination=destination)
        else:
            req = breez.PrepareSendRequest(
                destination=destination,
                amount=breez.PayAmount.BITCOIN(amount_sats),
            )
        prepare = sdk.prepare_send_payment(req)
        return sdk.send_payment(breez.SendPaymentRequest(prepare_response=prepare)).payment

    return _run_send("lightning", _do)


def send_onchain(destination: str, amount_sats: int) -> dict:
    if (rejection := _check_send_range(amount_sats, "onchain")) is not None:
        logger.warning("rejecting onchain: %s", rejection["error"])
        return rejection

    sdk = connect()

    def _do() -> "breez.Payment":
        prepare = sdk.prepare_pay_onchain(
            breez.PreparePayOnchainRequest(amount=breez.PayAmount.BITCOIN(amount_sats))
        )
        return sdk.pay_onchain(
            breez.PayOnchainRequest(address=destination, prepare_response=prepare)
        ).payment

    return _run_send("onchain", _do)


_LIMITS_TTL_SEC = 60
_limits_cache: tuple[dict, float] | None = None  # (response, fetched_at_epoch)


def get_send_limits() -> dict:
    """Cached lookup of per-rail send limits in sats.

    Boltz publishes minimums that change on the order of minutes-to-hours
    (mostly tracking BTC mempool fees). Caching at 60s keeps the UI's
    page-render storm off the SDK without making the displayed minimum
    visibly stale.
    """
    global _limits_cache
    now = time.time()
    if _limits_cache and now - _limits_cache[1] < _LIMITS_TTL_SEC:
        return _limits_cache[0]

    sdk = connect()
    onchain = sdk.fetch_onchain_limits()
    lightning = sdk.fetch_lightning_limits()
    result = {
        "onchain_send": {"min_sat": onchain.send.min_sat, "max_sat": onchain.send.max_sat},
        "lightning_send": {"min_sat": lightning.send.min_sat, "max_sat": lightning.send.max_sat},
    }
    _limits_cache = (result, now)
    return result


def daily_sent_sats() -> int:
    """Sum of outbound sats over the last rolling 24 hours.

    Used by the daily-cap policy at /pay. Failures fall back to 0 so a transient
    SDK hiccup doesn't pause payouts; the cap is defense-in-depth, not the
    primary correctness guarantee.
    """
    sdk = connect()
    one_day_ago = int(time.time()) - 86400
    try:
        payments = sdk.list_payments(
            breez.ListPaymentsRequest(from_timestamp=one_day_ago)
        )
    except Exception:
        logger.exception("list_payments failed during daily-cap check")
        return 0
    total = 0
    for p in payments:
        if str(p.payment_type).upper().endswith("SEND"):
            total += int(p.amount_sat or 0)
    return total


def lookup_status(tx_id: str) -> dict | None:
    """Return the current SDK-reported status for a given tx_id, or None."""
    sdk = connect()
    try:
        payments = sdk.list_payments(breez.ListPaymentsRequest(limit=200))
    except Exception:
        logger.exception("list_payments failed")
        return None
    for p in payments:
        if p.tx_id == tx_id:
            return {
                "tx_id": tx_id,
                "status": _map_status(p.status),
                "amount_sats": p.amount_sat,
            }
    return None


def _map_status(sdk_status) -> str:
    name = str(sdk_status).lower()
    if "succeed" in name or "complete" in name:
        return "settled"
    if "fail" in name:
        return "failed"
    if "pending" in name:
        return "processing"
    return "accepted"
