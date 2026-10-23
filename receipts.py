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
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

import db
from config import settings

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 15
_USER_AGENT = "BtcPayoutService/0.1"


class InvoiceUnavailable(Exception):
    """The Lightning address server could not be reached or answered with an error."""


class InvalidInvoice(Exception):
    """The invoice does not match what was asked for: never pay it."""


class VerifyUnavailable(Exception):
    """The verify URL could not tell whether the invoice was paid."""


def _get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=_TIMEOUT_SEC) as resp:
        body = json.loads(resp.read(65536).decode("utf-8"))
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

    Paid (with a matching preimage): the payout is 'sent' and Moodle gets a
    settled webhook. Still not paid after VERIFY_FAIL_AFTER_HOURS: 'failed' and
    a failed webhook. An unreachable verify URL changes nothing: the payout
    keeps waiting, because "unknown" must never become "failed".
    """
    for payout in db.verifying():
        try:
            preimage = is_paid(payout["verify_url"], payout["payment_hash"])
        except VerifyUnavailable as exc:
            logger.warning("payout %s: receiver can't confirm yet: %s", payout["payout_id"], exc)
            continue
        if preimage is not None:
            if db.resolve_verifying(payout["payout_id"], "sent", preimage):
                logger.warning("payout %s: SDK said failed, receiver confirms paid", payout["payout_id"])
                _notify(payout, "settled", preimage)
        elif payout["hours_unconfirmed"] >= settings.verify_fail_after_hours:
            if db.resolve_verifying(payout["payout_id"], "failed"):
                logger.info("payout %s: receiver says not paid, reporting failed", payout["payout_id"])
                _notify(payout, "failed", "")


def _notify(payout: dict, status: str, preimage: str) -> None:
    payload = {
        "tx_id": payout["tx_id"],
        "status": status,
        "amount_sats": payout["amount_sats"],
        "preimage": preimage,
    }
    if not db.enqueue(payout["tx_id"], status, payload):
        logger.error(
            "payout %s: a webhook for %s already exists, %s not queued: needs manual review",
            payout["payout_id"], payout["tx_id"], status,
        )
