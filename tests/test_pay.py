"""POST /pay: pays a payout at most once, validates amounts, and keeps the daily cap closed."""

import asyncio
import hashlib
import importlib
import time
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import breez_sdk_spark as spark
import httpx

ENV = {
    "BREEZ_API_KEY": "test",
    "BREEZ_MNEMONIC": "test",
    "INTERNAL_TOKEN": "secret-token",
    "MOODLE_WEBHOOK_URL": "https://example.invalid/webhook",
    "WEBHOOK_SECRET": "test",
    "DAILY_SEND_CAP_SATS": "1000",
}
AUTH = {"X-Internal-Token": "secret-token"}
DEST = "student@pay.example"
META = '[["text/identifier","student@pay.example"]]'


def _load_modules(directory: str):
    """Import the service with settings and SQLite state inside `directory`."""
    original_cwd = os.getcwd()
    with patch.dict(os.environ, ENV):
        try:
            os.chdir(directory)
            for name in ("config", "db", "receipts", "spark_client", "webhook", "rate", "app"):
                sys.modules.pop(name, None)
            db = importlib.import_module("db")
            db.DB_PATH = Path(directory) / "data" / "outbox.db"
            db._ensure_db()
            app = importlib.import_module("app")
        finally:
            os.chdir(original_cwd)
    return app, db, sys.modules["spark_client"]


def fake_invoice(n: int, amount_sats: int) -> str:
    return f"lnbc-{n}-{amount_sats}"


def fake_details(invoice: str, expiry: int = 3600):
    _, n, amount = invoice.split("-")
    return SimpleNamespace(
        amount_msat=int(amount) * 1000,
        description_hash=hashlib.sha256(META.encode()).hexdigest(),
        payment_hash=f"hash-{n}",
        timestamp=int(time.time()),
        expiry=expiry,
    )


class PayTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.app_module, self.db, self.spark_client = _load_modules(self._dir.name)
        self.receipts = sys.modules["receipts"]
        self.spent = 0
        self.invoices = 0
        self.daily_mock = patch.object(
            self.spark_client, "daily_sent_sats", AsyncMock(side_effect=self._spent)).start()
        patch.object(self.spark_client, "pay_request", AsyncMock(return_value=SimpleNamespace(
            callback="https://pay.example/cb", min_sendable=1000, max_sendable=10**11, metadata_str=META))).start()
        self.request_mock = patch.object(self.receipts, "request_invoice", side_effect=self._invoice).start()
        patch.object(self.spark_client, "invoice_details", AsyncMock(side_effect=fake_details)).start()
        self.send_mock = patch.object(self.spark_client, "send_invoice", AsyncMock(side_effect=self._sent)).start()
        self.lookup_mock = patch.object(self.spark_client, "find_payment", AsyncMock(return_value=None)).start()

    def tearDown(self):
        patch.stopall()
        self._dir.cleanup()

    async def _spent(self):
        return self.spent

    def _invoice(self, callback, amount_sats):
        self.invoices += 1
        return {"invoice": fake_invoice(self.invoices, amount_sats),
                "verify_url": f"https://pay.example/verify/{self.invoices}"}

    async def _sent(self, invoice, idempotency_key):
        self.spent += int(invoice.split("-")[2])
        return {"tx_id": idempotency_key, "status": "processing", "error": ""}

    def sent_keys(self):
        return [call.args[1] for call in self.send_mock.await_args_list]

    def sent_invoices(self):
        return [call.args[0] for call in self.send_mock.await_args_list]

    async def pay(self, payout_id="site:1", amount=100, destination=DEST, headers=AUTH):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post("/pay", headers=headers, json={
                "payout_id": payout_id, "amount_sats": amount, "destination": destination,
            })


RAISED = {"tx_id": "", "status": "failed", "error": "timeout"}


class IdempotencyTest(PayTestCase):
    async def test_paid_payout_answers_with_its_payment_without_sending(self):
        first = await self.pay()
        self.lookup_mock.return_value = {"tx_id": first.json()["tx_id"], "status": "settled"}
        second = await self.pay()

        self.assertEqual(self.send_mock.await_count, 1)
        self.assertEqual(second.json()["tx_id"], first.json()["tx_id"])
        self.assertEqual(second.json()["status"], "settled")
        self.lookup_mock.assert_awaited_once_with(first.json()["tx_id"])

    def abandon(self, tx_id):
        """An undelivered webhook the retrier has given up on."""
        self.db.enqueue(tx_id, "settled", {"tx_id": tx_id})
        self.db.mark_failed(tx_id, "settled", 0, "HTTP 404", not_found=True)
        self.assertTrue(self.db.mark_abandoned(tx_id, "settled", "HTTP 404", 0))
        self.assertEqual(self.db.claim_due(), [])

    async def test_send_returning_an_earlier_payment_revives_its_webhook(self):
        key = self.spark_client.payment_key("site:1")
        self.abandon(key)
        await self.pay()

        self.assertEqual([r["tx_id"] for r in self.db.claim_due()], [key])

    async def test_asking_about_a_paid_payout_again_revives_its_webhook(self):
        first = await self.pay()
        tx_id = first.json()["tx_id"]
        self.abandon(tx_id)

        await self.pay()

        self.assertEqual([r["tx_id"] for r in self.db.claim_due()], [tx_id])

    async def test_payout_found_in_the_wallet_on_retry_revives_its_webhook(self):
        self.send_mock.side_effect = [RAISED]
        await self.pay()
        key = self.spark_client.payment_key("site:1")
        self.abandon(key)
        self.lookup_mock.return_value = {"tx_id": key, "status": "settled"}

        await self.pay()

        self.assertEqual([r["tx_id"] for r in self.db.claim_due()], [key])

    async def test_paid_payout_the_sdk_later_reports_failed_is_verified_not_failed(self):
        first = await self.pay()
        self.lookup_mock.return_value = {"tx_id": first.json()["tx_id"], "status": "failed"}
        response = await self.pay()

        self.assertEqual(response.json()["status"], "processing")
        self.assertTrue(response.json()["retryable"])
        self.assertEqual(self.db.get_payout("site:1")["state"], "verifying")
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_without_a_verify_url_a_failed_payment_is_final(self):
        self.request_mock.side_effect = lambda callback, amount: {
            "invoice": fake_invoice(1, amount), "verify_url": None}
        first = await self.pay()
        self.lookup_mock.return_value = {"tx_id": first.json()["tx_id"], "status": "failed"}
        response = await self.pay()

        self.assertEqual(response.json()["status"], "failed")
        self.assertFalse(response.json()["retryable"])

    async def test_payout_id_reused_for_a_different_payment_is_rejected(self):
        await self.pay(amount=100)
        response = await self.pay(amount=200)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_send_that_raised_answers_503_and_keeps_the_payout_pending(self):
        self.send_mock.side_effect = [RAISED]
        response = await self.pay()

        self.assertEqual(response.status_code, 503)
        self.assertEqual(self.db.get_payout("site:1")["state"], "pending")

    async def test_retry_after_a_raised_send_uses_the_same_key(self):
        self.send_mock.side_effect = [RAISED, {"tx_id": "tx-original", "status": "processing", "error": ""}]
        await self.pay()
        second = await self.pay()

        self.assertEqual(second.json()["tx_id"], "tx-original")
        self.assertEqual(self.db.get_payout("site:1")["state"], "sent")
        keys = self.sent_keys()
        self.assertEqual(keys, [self.spark_client.payment_key("site:1")] * 2)

    async def test_retry_finds_a_payment_that_went_out_and_does_not_send(self):
        self.send_mock.side_effect = [RAISED]
        await self.pay()
        key = self.spark_client.payment_key("site:1")
        self.lookup_mock.return_value = {"tx_id": key, "status": "processing"}
        second = await self.pay()

        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["tx_id"], key)
        self.assertEqual(self.send_mock.await_count, 1)
        self.lookup_mock.assert_awaited_once_with(key)
        self.assertEqual(self.db.get_payout("site:1")["state"], "sent")

    async def test_retry_faces_the_cap_again(self):
        self.send_mock.side_effect = [RAISED]
        await self.pay(amount=600)
        self.spent = 600
        second = await self.pay(amount=600)

        self.assertEqual(second.status_code, 429)
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_retry_when_the_wallet_cannot_be_read_does_not_send(self):
        self.send_mock.side_effect = [RAISED]
        await self.pay()
        self.lookup_mock.side_effect = self.spark_client.PaymentLookupUnavailable("sdk down")
        second = await self.pay()

        self.assertEqual(second.status_code, 503)
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_each_payout_gets_its_own_key(self):
        await self.pay("site:1")
        await self.pay("site:2")

        keys = self.sent_keys()
        self.assertNotEqual(keys[0], keys[1])

    async def test_payment_key_is_a_stable_uuid(self):
        key = self.spark_client.payment_key("site:1")
        self.assertEqual(key, self.spark_client.payment_key("site:1"))
        self.assertEqual(str(uuid.UUID(key)), key)

    async def test_payout_id_is_required(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/pay", headers=AUTH, json={"amount_sats": 100, "destination": DEST})
        self.assertEqual(response.status_code, 422)


class InvoiceTest(PayTestCase):
    async def test_new_payout_stores_its_invoice_verify_url_and_payment_id(self):
        await self.pay()

        payout = self.db.get_payout("site:1")
        self.assertEqual(payout["invoice"], fake_invoice(1, 100))
        self.assertEqual(payout["verify_url"], "https://pay.example/verify/1")
        self.assertEqual(payout["payment_hash"], "hash-1")
        self.assertEqual(payout["tx_id"], self.spark_client.payment_key("site:1"))
        self.assertEqual(self.sent_invoices(), [fake_invoice(1, 100)])

    async def test_retry_pays_the_same_invoice(self):
        self.send_mock.side_effect = [RAISED, {"tx_id": "k", "status": "processing", "error": ""}]
        await self.pay()
        await self.pay()

        self.assertEqual(self.request_mock.call_count, 1)
        self.assertEqual(self.sent_invoices(), [fake_invoice(1, 100)] * 2)

    async def test_expired_unpaid_invoice_is_replaced_before_retrying(self):
        self.send_mock.side_effect = [RAISED, {"tx_id": "k", "status": "processing", "error": ""}]
        await self.pay()
        with sqlite3.connect(self.db.DB_PATH) as conn:
            conn.execute("UPDATE payouts SET expires_at = 0")
        await self.pay()

        self.assertEqual(self.sent_invoices(), [fake_invoice(1, 100), fake_invoice(2, 100)])
        self.assertEqual(self.sent_keys()[0], self.sent_keys()[1])

    async def test_invoice_close_to_expiry_is_not_replaced(self):
        self.send_mock.side_effect = [RAISED, {"tx_id": "k", "status": "processing", "error": ""}]
        await self.pay()
        with sqlite3.connect(self.db.DB_PATH) as conn:
            conn.execute("UPDATE payouts SET expires_at = ?", (int(time.time()) + 60,))
        await self.pay()

        self.assertEqual(self.sent_invoices(), [fake_invoice(1, 100)] * 2)

    async def test_legacy_pending_payout_gets_its_payment_id_before_sending(self):
        self.db.start_payout("site:1", 100, DEST)  # Recorded before tx_id was set at creation.
        await self.pay()

        self.assertEqual(self.db.get_payout("site:1")["tx_id"], self.spark_client.payment_key("site:1"))
        self.assertEqual(len(self.db.invoices("site:1")), 1)

    async def test_invoice_that_does_not_match_is_never_paid(self):
        self.request_mock.side_effect = lambda callback, amount: {
            "invoice": fake_invoice(1, amount + 1), "verify_url": None}
        response = await self.pay()

        self.assertEqual(response.status_code, 502)
        self.send_mock.assert_not_awaited()
        self.assertIsNone(self.db.get_payout("site:1"))

    async def test_invoice_already_given_to_another_payout_is_refused(self):
        await self.pay("site:1")
        self.request_mock.side_effect = lambda callback, amount: {
            "invoice": fake_invoice(1, amount), "verify_url": "https://pay.example/verify/1"}
        response = await self.pay("site:2")

        self.assertEqual(response.status_code, 502)
        self.assertEqual(self.send_mock.await_count, 1)
        self.assertIsNone(self.db.get_payout("site:2"))

    async def test_unreachable_address_server_is_retried_later(self):
        self.request_mock.side_effect = self.receipts.InvoiceUnavailable("down")
        response = await self.pay()

        self.assertEqual(response.status_code, 503)
        self.send_mock.assert_not_awaited()

    async def test_amount_outside_the_address_limits_is_rejected(self):
        self.spark_client.pay_request.return_value = SimpleNamespace(
            callback="https://pay.example/cb", min_sendable=1000, max_sendable=50_000, metadata_str=META)
        response = await self.pay(amount=100)

        self.assertEqual(response.status_code, 400)
        self.send_mock.assert_not_awaited()


class VerificationTest(PayTestCase):
    async def test_send_reported_failed_is_verifying_not_failed(self):
        self.send_mock.side_effect = [{"tx_id": "k", "status": "failed", "error": ""}]
        response = await self.pay()

        self.assertEqual(response.json()["status"], "processing")
        self.assertTrue(response.json()["retryable"])
        self.assertEqual(self.db.get_payout("site:1")["state"], "verifying")

    async def test_verifying_payout_is_answered_without_sending(self):
        self.send_mock.side_effect = [{"tx_id": "k", "status": "failed", "error": ""}]
        await self.pay()
        response = await self.pay()

        self.assertEqual(response.json()["status"], "processing")
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_failed_after_verification_is_final(self):
        self.send_mock.side_effect = [{"tx_id": "k", "status": "failed", "error": ""}]
        await self.pay()
        self.db.resolve_verifying("site:1", "failed")
        response = await self.pay()

        self.assertEqual(response.json()["status"], "failed")
        self.assertFalse(response.json()["retryable"])
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_waiting_payouts_are_listed_for_review(self):
        self.send_mock.side_effect = [{"tx_id": "k", "status": "failed", "error": ""}]
        await self.pay()
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            body = (await client.get("/webhooks/undelivered", headers=AUTH)).json()

        self.assertEqual([p["payout_id"] for p in body["verifying"]], ["site:1"])

    async def test_receiver_confirmed_payout_stays_settled_whatever_the_sdk_says(self):
        self.send_mock.side_effect = [{"tx_id": "k", "status": "failed", "error": ""}]
        await self.pay()
        self.db.resolve_verifying("site:1", "sent", "aa" * 32)
        self.lookup_mock.return_value = {"tx_id": "k", "status": "failed"}
        response = await self.pay()

        self.assertEqual(response.json()["status"], "settled")
        self.assertEqual(self.db.get_payout("site:1")["state"], "sent")


class LimitsTest(PayTestCase):
    async def test_zero_and_negative_amounts_are_rejected(self):
        for amount in (0, -5):
            response = await self.pay(amount=amount)
            self.assertEqual(response.status_code, 400)
        self.send_mock.assert_not_awaited()

    async def test_amount_above_the_daily_cap_is_rejected(self):
        response = await self.pay(amount=1001)
        self.assertEqual(response.status_code, 400)
        self.send_mock.assert_not_awaited()

    async def test_cap_reached_returns_429(self):
        self.spent = 950
        response = await self.pay(amount=100)
        self.assertEqual(response.status_code, 429)
        self.send_mock.assert_not_awaited()

    async def test_cap_fails_closed_when_history_is_unreadable(self):
        self.daily_mock.side_effect = self.spark_client.CapCheckUnavailable("sdk down")
        response = await self.pay()

        self.assertEqual(response.status_code, 503)
        self.send_mock.assert_not_awaited()
        self.assertIsNone(self.db.get_payout("site:1"))

    async def test_concurrent_payouts_cannot_both_slip_under_the_cap(self):
        async def slow_send(invoice, idempotency_key):
            await asyncio.sleep(0.05)
            return await self._sent(invoice, idempotency_key)

        self.send_mock.side_effect = slow_send
        first, second = await asyncio.gather(self.pay("site:1", 600), self.pay("site:2", 600))

        self.assertEqual(sorted([first.status_code, second.status_code]), [200, 429])
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_request_that_waited_too_long_for_its_turn_is_not_sent(self):
        async def slow_send(invoice, idempotency_key):
            await asyncio.sleep(0.1)
            return await self._sent(invoice, idempotency_key)

        self.send_mock.side_effect = slow_send
        with patch.object(self.app_module, "MAX_LOCK_WAIT_SECONDS", 0.05):
            first, second = await asyncio.gather(self.pay("site:1"), self.pay("site:2"))

        self.assertEqual(sorted([first.status_code, second.status_code]), [200, 503])
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_wrong_token_is_rejected(self):
        response = await self.pay(headers={"X-Internal-Token": "secret-tokeX"})
        self.assertEqual(response.status_code, 401)


class DailySentTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        _, _, self.spark_client = _load_modules(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    @staticmethod
    def _payment(amount, status=spark.PaymentStatus.COMPLETED, timestamp=0, pid="p"):
        return SimpleNamespace(id=pid, amount=amount, status=status, timestamp=timestamp)

    async def test_sums_every_page_and_skips_failed_sends(self):
        pages = [
            [self._payment(10) for _ in range(100)],
            [self._payment(5), self._payment(999, spark.PaymentStatus.FAILED)],
        ]
        offsets = []

        async def list_payments(*, request):
            offsets.append(request.offset)
            return SimpleNamespace(payments=pages[len(offsets) - 1])

        sdk = SimpleNamespace(list_payments=list_payments)
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)):
            total = await self.spark_client.daily_sent_sats()

        self.assertEqual(total, 1005)
        self.assertEqual(offsets, [0, 100])

    async def test_unreadable_history_raises(self):
        async def list_payments(*, request):
            raise RuntimeError("sdk down")

        sdk = SimpleNamespace(list_payments=list_payments)
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)):
            with self.assertRaises(self.spark_client.CapCheckUnavailable):
                await self.spark_client.daily_sent_sats()


class SendInvoiceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        _, _, self.spark_client = _load_modules(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def _sdk(self, pay_error=None, payment_status=spark.PaymentStatus.PENDING):
        self.requests = []

        async def prepare_send_payment(*, request):
            self.requests.append(("prepare", request))
            return "prepared"

        async def send_payment(*, request):
            self.requests.append(("send", request))
            if pay_error:
                raise pay_error
            return SimpleNamespace(payment=SimpleNamespace(id="tx-1", status=payment_status))

        return SimpleNamespace(prepare_send_payment=prepare_send_payment, send_payment=send_payment)

    async def _send(self, sdk, key="00000000-0000-0000-0000-000000000001"):
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)), \
                patch.object(spark, "PrepareSendPaymentRequest", lambda **kw: kw), \
                patch.object(spark, "SendPaymentRequest", lambda **kw: kw):
            return await self.spark_client.send_invoice("lnbc-invoice", key)

    async def test_pays_the_invoice_with_the_idempotency_key(self):
        await self._send(self._sdk(), key="00000000-0000-0000-0000-00000000abcd")

        prepare = self.requests[0][1]
        self.assertEqual(prepare["payment_request"].input, "lnbc-invoice")
        send = self.requests[1][1]
        self.assertEqual(send["idempotency_key"], "00000000-0000-0000-0000-00000000abcd")
        self.assertEqual(send["prepare_response"], "prepared")

    async def test_sdk_error_returns_no_payment(self):
        result = await self._send(self._sdk(pay_error=RuntimeError("timeout")))
        self.assertEqual(result["tx_id"], "")
        self.assertIn("timeout", result["error"])

    async def test_failed_payment_is_reported_failed(self):
        result = await self._send(self._sdk(payment_status=spark.PaymentStatus.FAILED))
        self.assertEqual(result["status"], "failed")

    async def test_success_returns_the_payment(self):
        result = await self._send(self._sdk())
        self.assertEqual(result["tx_id"], "tx-1")
        self.assertEqual(result["status"], "processing")


class FindPaymentTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        _, _, self.spark_client = _load_modules(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    async def _find(self, get_payment):
        sdk = SimpleNamespace(get_payment=get_payment)
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)):
            return await self.spark_client.find_payment("key")

    async def test_missing_payment_is_none(self):
        async def get_payment(*, request):
            raise RuntimeError("StorageError: Query returned no rows")

        self.assertIsNone(await self._find(get_payment))

    async def test_other_errors_are_not_mistaken_for_a_missing_payment(self):
        async def get_payment(*, request):
            raise RuntimeError("database is locked")

        with self.assertRaises(self.spark_client.PaymentLookupUnavailable):
            await self._find(get_payment)

    async def test_existing_payment_is_returned(self):
        async def get_payment(*, request):
            return SimpleNamespace(payment=SimpleNamespace(id=request.payment_id, status=spark.PaymentStatus.COMPLETED))

        self.assertEqual(await self._find(get_payment), {"tx_id": "key", "status": "settled"})


if __name__ == "__main__":
    unittest.main()
