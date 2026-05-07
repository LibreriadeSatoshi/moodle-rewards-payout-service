"""Outbound webhook delivery with HMAC signing and exponential backoff.

The retrier runs on a background thread. On each tick it drains any due rows
from db.webhook_outbox, POSTs them to MOODLE_WEBHOOK_URL with a signature
header, and either marks them delivered or pushes next_attempt_at forward
using capped exponential backoff. Retries forever until 2xx.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.error
import urllib.request

import breez_sdk_liquid as breez

import db
from config import settings

logger = logging.getLogger(__name__)


def event_to_terminal_status(event: breez.SdkEvent) -> str | None:
    """Map an SDK event variant to a webhook-status string, or None for events
    we don't surface (e.g. pending/syncing). Only terminal outcomes — settled,
    failed, refunded — generate a webhook."""
    if event.is_payment_succeeded():
        return "settled"
    if event.is_payment_failed() or event.is_payment_refunded():
        return "failed"
    return None


def payload_for(payment, status: str) -> dict:
    """Build the outbound webhook body. Preimage is only populated for settled
    Lightning payments; onchain payments have no preimage to attach."""
    details = getattr(payment, "details", None)
    preimage = getattr(details, "preimage", None) if details is not None else None
    return {
        "tx_id": payment.tx_id,
        "status": status,
        "amount_sats": payment.amount_sat,
        "preimage": preimage or "",
    }

_POLL_INTERVAL_SEC = 5
_BACKOFF_BASE_SEC = 5
_BACKOFF_CAP_SEC = 3600
_REQUEST_TIMEOUT_SEC = 15


def _backoff(attempts: int) -> int:
    """5, 10, 20, 40, 80, 160, ... capped at 1 hour."""
    return min(_BACKOFF_BASE_SEC * (2 ** attempts), _BACKOFF_CAP_SEC)


def _sign(body: bytes) -> str:
    digest = hmac.new(settings.webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _post(url: str, body: bytes) -> tuple[int, str]:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Signature": _sign(body),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_SEC) as resp:
            return resp.status, resp.read(2048).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(2048).decode("utf-8", "replace") if exc.fp else ""


def _deliver_one(row: dict) -> None:
    tx_id = row["tx_id"]
    # Re-sign with a fresh `timestamp` on every delivery attempt. This prevents
    # an attacker who captures one webhook from replaying it indefinitely:
    # the plugin rejects bodies whose timestamp is more than a few minutes old.
    payload = json.loads(row["payload_json"])
    payload["timestamp"] = int(time.time())
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    try:
        status, resp_text = _post(settings.moodle_webhook_url, body)
    except Exception as exc:
        err = str(exc)
        delay = _backoff(row["attempts"])
        logger.warning("webhook %s failed (network): %s — retry in %ds", tx_id, err, delay)
        db.mark_failed(tx_id, delay, err)
        return

    if 200 <= status < 300:
        logger.info("webhook %s delivered (HTTP %d)", tx_id, status)
        db.mark_delivered(tx_id)
        return

    delay = _backoff(row["attempts"])
    err = f"HTTP {status}: {resp_text[:200]}"
    logger.warning("webhook %s rejected: %s — retry in %ds", tx_id, err, delay)
    db.mark_failed(tx_id, delay, err)


class Retrier:
    """Background thread that drains the outbox."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="webhook-retrier", daemon=True)
        self._thread.start()
        logger.info("webhook retrier started (url=%s)", settings.moodle_webhook_url)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
            logger.info("webhook retrier stopped")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rows = db.claim_due(limit=20)
                for row in rows:
                    if self._stop.is_set():
                        break
                    _deliver_one(row)
            except Exception:
                logger.exception("retrier loop error")
            self._stop.wait(_POLL_INTERVAL_SEC)


retrier = Retrier()
