"""POST /pay: pays a payout at most once, validates amounts, and keeps the daily cap closed."""

import asyncio
import importlib
import os
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


def _load_modules(directory: str):
    """Import the service with settings and SQLite state inside `directory`."""
    original_cwd = os.getcwd()
    with patch.dict(os.environ, ENV):
        try:
            os.chdir(directory)
            for name in ("config", "db", "spark_client", "webhook", "rate", "app"):
                sys.modules.pop(name, None)
            db = importlib.import_module("db")
            db.DB_PATH = Path(directory) / "data" / "outbox.db"
            db._ensure_db()
            app = importlib.import_module("app")
        finally:
            os.chdir(original_cwd)
    return app, db, sys.modules["spark_client"]


class PayTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.app_module, self.db, self.spark_client = _load_modules(self._dir.name)
        self.spent = 0
        self.daily = patch.object(self.spark_client, "daily_sent_sats", AsyncMock(side_effect=self._spent))
        self.check = patch.object(self.spark_client, "ensure_ln_address", AsyncMock(return_value=None))
        self.send = patch.object(self.spark_client, "send_lightning", AsyncMock(side_effect=self._sent))
        self.lookup = patch.object(self.spark_client, "find_payment", AsyncMock(return_value=None))
        self.daily_mock = self.daily.start()
        self.check.start()
        self.send_mock = self.send.start()
        self.lookup_mock = self.lookup.start()

    def tearDown(self):
        patch.stopall()
        self._dir.cleanup()

    async def _spent(self):
        return self.spent

    async def _sent(self, destination, amount_sats, idempotency_key):
        self.spent += amount_sats
        return {"tx_id": f"tx-{self.send_mock.await_count}", "status": "processing", "error": ""}

    def sent_keys(self):
        return [call.args[2] for call in self.send_mock.await_args_list]

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
        self.db.mark_failed(tx_id, 0, "HTTP 404", not_found=True)
        self.assertTrue(self.db.mark_abandoned(tx_id, "HTTP 404", 0))
        self.assertEqual(self.db.claim_due(), [])

    async def test_send_returning_an_earlier_payment_revives_its_webhook(self):
        self.abandon("tx-1")
        await self.pay()

        self.assertEqual([r["tx_id"] for r in self.db.claim_due()], ["tx-1"])

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

    async def test_paid_payout_whose_payment_failed_is_final(self):
        first = await self.pay()
        self.lookup_mock.return_value = {"tx_id": first.json()["tx_id"], "status": "failed"}
        response = await self.pay()

        self.assertEqual(response.json()["status"], "failed")
        self.assertFalse(response.json()["retryable"])
        self.assertEqual(self.send_mock.await_count, 1)

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
        async def slow_send(destination, amount_sats, idempotency_key):
            await asyncio.sleep(0.05)
            return await self._sent(destination, amount_sats, idempotency_key)

        self.send_mock.side_effect = slow_send
        first, second = await asyncio.gather(self.pay("site:1", 600), self.pay("site:2", 600))

        self.assertEqual(sorted([first.status_code, second.status_code]), [200, 429])
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_request_that_waited_too_long_for_its_turn_is_not_sent(self):
        async def slow_send(destination, amount_sats, idempotency_key):
            await asyncio.sleep(0.1)
            return await self._sent(destination, amount_sats, idempotency_key)

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


class SendLightningTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        _, _, self.spark_client = _load_modules(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def _sdk(self, pay_error=None, payment_status=spark.PaymentStatus.PENDING):
        self.pay_requests = []

        class Parsed(list):
            def is_lightning_address(self):
                return True

        async def parse(*, input):
            return Parsed([SimpleNamespace(pay_request=object())])

        async def prepare_lnurl_pay(*, request):
            return object()

        async def lnurl_pay(*, request):
            self.pay_requests.append(request)
            if pay_error:
                raise pay_error
            return SimpleNamespace(payment=SimpleNamespace(id="tx-1", status=payment_status))

        return SimpleNamespace(parse=parse, prepare_lnurl_pay=prepare_lnurl_pay, lnurl_pay=lnurl_pay)

    async def _send(self, sdk, key="00000000-0000-0000-0000-000000000001"):
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)), \
                patch.object(spark, "PrepareLnurlPayRequest", lambda **kw: kw), \
                patch.object(spark, "LnurlPayRequest", lambda **kw: kw):
            return await self.spark_client.send_lightning(DEST, 100, key)

    async def test_the_idempotency_key_reaches_the_sdk(self):
        await self._send(self._sdk(), key="00000000-0000-0000-0000-00000000abcd")
        self.assertEqual(self.pay_requests[0]["idempotency_key"], "00000000-0000-0000-0000-00000000abcd")

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
