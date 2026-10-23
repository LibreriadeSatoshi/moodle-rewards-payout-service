"""Ask the receiver whether a payout arrived.

The Breez SDK can report a Lightning send failed although the receiver was
paid, and it never corrects a stored Failed status. So the service gets the
invoice from the student's Lightning address itself (LUD-06) and keeps its
LUD-21 `verify` URL: the receiver's server answers whether that invoice was
paid, with the preimage as proof. A failed send is only reported to Moodle as
failed once the receiver still says "not paid" after VERIFY_FAIL_AFTER_HOURS.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import db
from config import settings

logger = logging.getLogger(__name__)

_SOCKET_TIMEOUT_SEC = 10
_DEADLINE_SEC = 20  # Whole request, so a server trickling bytes can't hold the checker.
_MAX_BODY_BYTES = 65536
_USER_AGENT = "BtcPayoutService/0.1"


class InvoiceUnavailable(Exception):
    """The Lightning address server could not be reached or answered with an error."""


class InvalidInvoice(Exception):
    """The invoice does not match what was asked for: never pay it."""


class VerifyUnavailable(Exception):
    """The verify URL could not tell whether the invoice was paid."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirects are not followed", headers, fp)


_opener = urllib.request.build_opener(_NoRedirect)


def _check_url(url: str) -> None:
    """Refuse anything but https to a public address: callback and verify URLs
    come from outside, and must not reach the internal network (SSRF)."""
    parts = urllib.parse.urlparse(url)
    if parts.scheme != "https" or not parts.hostname:
        raise ValueError(f"only https URLs are fetched, got {parts.scheme or 'none'}")
    if settings.allow_private_lnurl:
        return
    try:
        addresses = {info[4][0] for info in socket.getaddrinfo(parts.hostname, parts.port or 443)}
    except socket.gaierror as exc:
        raise ValueError(f"cannot resolve {parts.hostname}: {exc}") from exc
    for address in addresses:
        if not ipaddress.ip_address(address.split("%")[0]).is_global:
            raise ValueError(f"{parts.hostname} resolves to a non-public address")


def _get_json(url: str) -> dict:
    _check_url(url)
    deadline = time.monotonic() + _DEADLINE_SEC
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
    chunks, size = [], 0
    with _opener.open(req, timeout=_SOCKET_TIMEOUT_SEC) as resp:
        while chunk := resp.read(8192):
            size += len(chunk)
            if size > _MAX_BODY_BYTES:
                raise ValueError("answer too large")
            if time.monotonic() > deadline:
                raise ValueError("answer too slow")
            chunks.append(chunk)
    body = json.loads(b"".join(chunks).decode("utf-8"))
    if not isinstance(body, dict):
        raise ValueError("answer is not a JSON object")
    return body


def request_invoice(callback: str, amount_sats: int) -> dict:
    """Ask a Lightning address callback for an invoice of amount_sats.

    Returns {"invoice": bolt11, "verify_url": str | None}. Raises
    InvoiceUnavailable when the server can't be reached or refuses.
    """
    separator = "&" if urllib.parse.urlparse(callback).query else "?"
    url = f"{callback}{separator}amount={amount_sats * 1000}"
    try:
        body = _get_json(url)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise InvoiceUnavailable(f"invoice request failed: {exc}") from exc
    if body.get("status") == "ERROR" or not body.get("pr"):
        raise InvoiceUnavailable(f"invoice refused: {body.get('reason') or 'no invoice in answer'}")
    verify = body.get("verify")
    return {"invoice": body["pr"], "verify_url": verify if isinstance(verify, str) and verify else None}


def check_invoice(details, amount_sats: int, metadata: str) -> None:
    """Check SDK-parsed bolt11 details against the request (LUD-06).

    Raises InvalidInvoice if the amount differs or the description hash does
    not commit to the Lightning address metadata.
    """
    if details.amount_msat != amount_sats * 1000:
        raise InvalidInvoice(f"invoice is for {details.amount_msat} msat, asked for {amount_sats * 1000}")
    expected = hashlib.sha256(metadata.encode("utf-8")).hexdigest()
    if details.description_hash and details.description_hash != expected:
        raise InvalidInvoice("invoice description hash does not match the Lightning address metadata")


def is_paid(verify_url: str, payment_hash: str) -> str | None:
    """The preimage if the receiver says the invoice was paid, None if not paid yet.

    A "paid" answer counts only with a preimage that hashes to payment_hash.
    Raises VerifyUnavailable if the server can't be reached or its answer
    can't be trusted either way.
    """
    try:
        body = _get_json(verify_url)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise VerifyUnavailable(f"verify request failed: {exc}") from exc
    if body.get("status") == "ERROR":
        raise VerifyUnavailable(f"verify refused: {body.get('reason')}")
    if body.get("settled") is not True:
        return None
    preimage = body.get("preimage")
    try:
        proof = hashlib.sha256(bytes.fromhex(preimage)).hexdigest()
    except (TypeError, ValueError) as exc:
        raise VerifyUnavailable("settled without a valid preimage") from exc
    if proof != payment_hash:
        raise VerifyUnavailable("preimage does not match the invoice")
    return preimage


def check_verifying() -> None:
    """Resolve payouts the SDK reported failed, from the receiver's answer.

    The receiver is asked about every invoice the payout has had. Paid (with a
    matching preimage): the payout is 'sent' and Moodle gets a settled
    webhook, also if it was already reported failed. Still not paid after
    VERIFY_FAIL_AFTER_HOURS: 'failed' and a failed webhook; failed payouts are
    still watched until their last invoice expires, since a payment in flight
    can settle until then. An answer that can't be trusted changes nothing:
    "unknown" must never become "failed".
    """
    for payout in db.verifying():
        try:
            preimage = _receiver_answer(payout)
        except VerifyUnavailable as exc:
            logger.warning("payout %s: receiver can't confirm yet: %s", payout["payout_id"], exc)
            continue
        if preimage is not None:
            if db.mark_paid(payout["payout_id"], preimage):
                logger.warning("payout %s: SDK said failed, receiver confirms paid", payout["payout_id"])
                _notify(payout, "settled", preimage)
        elif payout["state"] == "verifying" and payout["hours_unconfirmed"] >= settings.verify_fail_after_hours:
            if db.resolve_verifying(payout["payout_id"], "failed"):
                logger.info("payout %s: receiver says not paid, reporting failed", payout["payout_id"])
                _notify(payout, "failed", "")


def _receiver_answer(payout: dict) -> str | None:
    """The preimage if any of the payout's invoices was paid, None if none was.
    Raises VerifyUnavailable if one can't be answered and none was paid."""
    unknown = None
    for invoice in db.invoices(payout["payout_id"]) or [payout]:
        if not invoice["verify_url"]:
            continue
        try:
            preimage = is_paid(invoice["verify_url"], invoice["payment_hash"])
        except VerifyUnavailable as exc:
            unknown = exc
            continue
        if preimage is not None:
            return preimage
    if unknown is not None:
        raise unknown
    return None


def _notify(payout: dict, status: str, preimage: str) -> None:
    payload = {
        "tx_id": payout["tx_id"],
        "status": status,
        "amount_sats": payout["amount_sats"],
        "preimage": preimage,
    }
    if not db.enqueue(payout["tx_id"], status, payload):
        logger.info("payout %s: %s webhook for %s already queued", payout["payout_id"], status, payout["tx_id"])


class Checker:
    """Background thread running check_verifying every VERIFY_INTERVAL_SECONDS,
    apart from the webhook retrier so slow receivers never hold up webhooks."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="verify-checker", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=_DEADLINE_SEC + 5)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                check_verifying()
            except Exception:
                logger.exception("verify check error")
            self._stop.wait(settings.verify_interval_seconds)


checker = Checker()
