"""Receiver-side verification: invoices from the Lightning address, LUD-21 verify, the checker."""

import asyncio
import hashlib
import importlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import breez_sdk_spark as spark

ENV = {
    "BREEZ_API_KEY": "test",
    "BREEZ_MNEMONIC": "test",
    "INTERNAL_TOKEN": "secret-token",
    "MOODLE_WEBHOOK_URL": "https://example.invalid/webhook",
    "WEBHOOK_SECRET": "test",
}
PREIMAGE = "11" * 32
PAYMENT_HASH = hashlib.sha256(bytes.fromhex(PREIMAGE)).hexdigest()
META = '[["text/identifier","student@pay.example"]]'


def _load_modules(directory: str):
    original_cwd = os.getcwd()
    with patch.dict(os.environ, ENV):
        try:
            os.chdir(directory)
            for name in ("config", "db", "receipts", "spark_client", "webhook", "rate", "app"):
                sys.modules.pop(name, None)
            db = importlib.import_module("db")
            db.DB_PATH = Path(directory) / "data" / "outbox.db"
            db._ensure_db()
            importlib.import_module("app")
        finally:
            os.chdir(original_cwd)
    return db, sys.modules["receipts"], sys.modules["spark_client"]


class ReceiptsTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db, self.receipts, self.spark_client = _load_modules(self._dir.name)

    def tearDown(self):
        patch.stopall()
        self._dir.cleanup()

    def answer(self, body):
        return patch.object(self.receipts, "_get_json", return_value=body)


class RequestInvoiceTest(ReceiptsTestCase):
    def test_asks_for_the_amount_in_millisats_and_keeps_the_verify_url(self):
        with self.answer({"pr": "lnbc1", "verify": "https://pay.example/verify/x"}) as get:
            result = self.receipts.request_invoice("https://pay.example/cb", 10)

        get.assert_called_once_with("https://pay.example/cb?amount=10000")
        self.assertEqual(result, {"invoice": "lnbc1", "verify_url": "https://pay.example/verify/x"})

    def test_callback_with_a_query_string_gets_amount_appended(self):
        with self.answer({"pr": "lnbc1"}) as get:
            result = self.receipts.request_invoice("https://pay.example/cb?user=a", 10)

        get.assert_called_once_with("https://pay.example/cb?user=a&amount=10000")
        self.assertIsNone(result["verify_url"])

    def test_refusals_and_outages_are_unavailable(self):
        for side_effect in ({"status": "ERROR", "reason": "no"}, {}, OSError("down")):
            with self.subTest(side_effect=side_effect):
                kwargs = {"side_effect": side_effect} if isinstance(side_effect, Exception) else {"return_value": side_effect}
                with patch.object(self.receipts, "_get_json", **kwargs):
                    with self.assertRaises(self.receipts.InvoiceUnavailable):
                        self.receipts.request_invoice("https://pay.example/cb", 10)


class CheckInvoiceTest(ReceiptsTestCase):
    def details(self, amount_msat=10000, description_hash=None):
        return SimpleNamespace(
            amount_msat=amount_msat,
            description_hash=description_hash or hashlib.sha256(META.encode()).hexdigest(),
        )

    def test_matching_invoice_passes(self):
        self.receipts.check_invoice(self.details(), 10, META)

    def test_wrong_amount_or_description_hash_is_never_paid(self):
        for details in (self.details(amount_msat=20000), self.details(description_hash="00" * 32)):
            with self.subTest(details=details):
                with self.assertRaises(self.receipts.InvalidInvoice):
                    self.receipts.check_invoice(details, 10, META)


class IsPaidTest(ReceiptsTestCase):
    def test_settled_with_a_matching_preimage_is_paid(self):
        with self.answer({"status": "OK", "settled": True, "preimage": PREIMAGE}):
            self.assertEqual(self.receipts.is_paid("https://v", PAYMENT_HASH), PREIMAGE)

    def test_not_settled_is_not_paid(self):
        with self.answer({"status": "OK", "settled": False, "preimage": None}):
            self.assertIsNone(self.receipts.is_paid("https://v", PAYMENT_HASH))

    def test_settled_without_proof_or_unreachable_is_unknown(self):
        cases = [
            {"return_value": {"settled": True, "preimage": "22" * 32}},
            {"return_value": {"settled": True, "preimage": None}},
            {"return_value": {"status": "ERROR", "reason": "x"}},
            {"side_effect": OSError("down")},
        ]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with patch.object(self.receipts, "_get_json", **kwargs):
                    with self.assertRaises(self.receipts.VerifyUnavailable):
                        self.receipts.is_paid("https://v", PAYMENT_HASH)


class CheckVerifyingTest(ReceiptsTestCase):
    def verifying_payout(self, hours=1.0, payout_id="site:1", tx_id="key-1"):
        self.db.start_payout(payout_id, 10, "student@pay.example", {
            "invoice": "lnbc1", "payment_hash": PAYMENT_HASH,
            "verify_url": "https://pay.example/verify/x", "expires_at": 0,
        }, tx_id=tx_id)
        self.assertTrue(self.db.mark_verifying(payout_id))
        with sqlite3.connect(self.db.DB_PATH) as conn:
            conn.execute("UPDATE payouts SET unconfirmed_since = datetime('now', ?) WHERE payout_id = ?",
                         (f"-{hours * 3600:.0f} seconds", payout_id))

    def outbox(self):
        with sqlite3.connect(self.db.DB_PATH) as conn:
            return [(r[0], r[1], json.loads(r[2])) for r in
                    conn.execute("SELECT tx_id, status, payload_json FROM webhook_outbox")]

    def test_receiver_confirms_payment_settled_webhook_with_preimage(self):
        self.verifying_payout()
        with patch.object(self.receipts, "is_paid", return_value=PREIMAGE):
            self.receipts.check_verifying()

        payout = self.db.get_payout("site:1")
        self.assertEqual(payout["state"], "sent")
        self.assertEqual(payout["preimage"], PREIMAGE)
        [(tx_id, status, payload)] = self.outbox()
        self.assertEqual((tx_id, status, payload["preimage"]), ("key-1", "settled", PREIMAGE))

    def test_not_paid_yet_keeps_waiting(self):
        self.verifying_payout(hours=23)
        with patch.object(self.receipts, "is_paid", return_value=None):
            self.receipts.check_verifying()

        self.assertEqual(self.db.get_payout("site:1")["state"], "verifying")
        self.assertEqual(self.outbox(), [])

    def test_still_not_paid_after_the_limit_is_failed(self):
        self.verifying_payout(hours=25)
        with patch.object(self.receipts, "is_paid", return_value=None):
            self.receipts.check_verifying()

        self.assertEqual(self.db.get_payout("site:1")["state"], "failed")
        [(tx_id, status, _)] = self.outbox()
        self.assertEqual((tx_id, status), ("key-1", "failed"))

    def test_unreachable_receiver_never_turns_into_failed(self):
        self.verifying_payout(hours=24 * 30)
        with patch.object(self.receipts, "is_paid", side_effect=self.receipts.VerifyUnavailable("down")):
            with self.assertLogs("receipts", level="WARNING"):
                self.receipts.check_verifying()

        self.assertEqual(self.db.get_payout("site:1")["state"], "verifying")
        self.assertEqual(self.outbox(), [])

    def test_confirmed_payout_never_goes_back_to_verifying(self):
        self.verifying_payout()
        with patch.object(self.receipts, "is_paid", return_value=PREIMAGE):
            self.receipts.check_verifying()

        self.assertFalse(self.db.mark_verifying("site:1"))


class ListenerTest(ReceiptsTestCase):
    def event(self, tx_id, failed=True):
        payment = SimpleNamespace(id=tx_id, payment_type=spark.PaymentType.SEND, amount=10)
        return SimpleNamespace(
            payment=payment,
            is_payment_succeeded=lambda: not failed,
            is_payment_failed=lambda: failed,
        )

    def deliver(self, event):
        asyncio.run(self.spark_client._Listener().on_event(event))

    def outbox_statuses(self):
        with sqlite3.connect(self.db.DB_PATH) as conn:
            return [r[0] for r in conn.execute("SELECT status FROM webhook_outbox")]

    def test_failed_event_for_a_verifiable_payout_waits_for_the_receiver(self):
        # The event can arrive before the send returns: the payout is still 'pending'.
        self.db.start_payout("site:1", 10, "student@pay.example", {
            "invoice": "lnbc1", "payment_hash": PAYMENT_HASH,
            "verify_url": "https://pay.example/verify/x", "expires_at": 0,
        }, tx_id="key-1")

        with self.assertLogs("spark_client", level="WARNING"):
            self.deliver(self.event("key-1"))

        self.assertEqual(self.db.get_payout("site:1")["state"], "verifying")
        self.assertEqual(self.outbox_statuses(), [])

    def test_failed_event_without_a_verify_url_is_sent_to_moodle(self):
        self.db.start_payout("site:1", 10, "student@pay.example", tx_id="key-1")
        self.deliver(self.event("key-1"))

        self.assertEqual(self.outbox_statuses(), ["failed"])

    def test_succeeded_event_is_sent_to_moodle(self):
        self.db.start_payout("site:1", 10, "student@pay.example", {
            "invoice": "lnbc1", "payment_hash": PAYMENT_HASH,
            "verify_url": "https://pay.example/verify/x", "expires_at": 0,
        }, tx_id="key-1")
        self.deliver(self.event("key-1", failed=False))

        self.assertEqual(self.outbox_statuses(), ["settled"])


if __name__ == "__main__":
    unittest.main()
