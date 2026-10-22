"""POST /pay: never pays a payout twice, validates amounts, and keeps the daily cap closed."""

import asyncio
import importlib
import os
import sys
import tempfile
import unittest
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
        self.find = patch.object(self.spark_client, "find_send", AsyncMock(return_value=None))
        self.daily_mock = self.daily.start()
        self.check.start()
        self.send_mock = self.send.start()
        self.find_mock = self.find.start()

    def tearDown(self):
        patch.stopall()
        self._dir.cleanup()

    async def _spent(self):
        return self.spent

    async def _sent(self, destination, amount_sats):
        self.spent += amount_sats
        return {"tx_id": f"tx-{self.send_mock.await_count}", "status": "processing",
                "error": "", "retryable": True, "attempted": True}

    async def pay(self, payout_id="site:1", amount=100, destination=DEST, headers=AUTH):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post("/pay", headers=headers, json={
                "payout_id": payout_id, "amount_sats": amount, "destination": destination,
            })


class IdempotencyTest(PayTestCase):
    async def test_repeated_payout_id_returns_first_payment_without_sending(self):
        first = await self.pay()
        second = await self.pay()

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(self.send_mock.await_count, 1)
        self.assertEqual(second.json()["tx_id"], first.json()["tx_id"])
        self.assertEqual(second.json()["status"], "processing")

    async def test_payout_id_reused_for_a_different_payment_is_rejected(self):
        await self.pay(amount=100)
        response = await self.pay(amount=200)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_failure_before_sending_lets_the_retry_pay(self):
        self.send_mock.side_effect = [
            {"tx_id": "", "status": "failed", "error": "lnurl down", "retryable": True, "attempted": False},
            {"tx_id": "tx-retry", "status": "processing", "error": "", "retryable": True, "attempted": True},
        ]
        first = await self.pay()
        second = await self.pay()

        self.assertTrue(first.json()["retryable"])
        self.assertEqual(second.json()["tx_id"], "tx-retry")
        self.assertEqual(self.send_mock.await_count, 2)

    async def test_send_that_raised_is_never_retried(self):
        self.send_mock.side_effect = [
            {"tx_id": "", "status": "failed", "error": "timeout", "retryable": True, "attempted": True},
        ]
        first = await self.pay()
        second = await self.pay()

        self.assertFalse(first.json()["retryable"])
        self.assertIn("outcome unknown", first.json()["error"])
        self.assertFalse(second.json()["retryable"])
        self.assertEqual(self.send_mock.await_count, 1)
        self.assertEqual(self.db.get_payout("site:1")["state"], "unknown")

    async def test_send_that_raised_but_went_out_reports_the_payment(self):
        self.send_mock.side_effect = [
            {"tx_id": "", "status": "failed", "error": "timeout", "retryable": True, "attempted": True},
        ]
        self.find_mock.return_value = {"tx_id": "tx-found", "status": "processing"}
        first = await self.pay()
        second = await self.pay()

        self.assertEqual(first.json()["tx_id"], "tx-found")
        self.assertEqual(first.json()["status"], "processing")
        self.assertEqual(second.json()["tx_id"], "tx-found")
        self.assertEqual(self.send_mock.await_count, 1)

    async def test_payout_left_mid_send_is_not_sent_again(self):
        self.db.start_payout("site:1", 100, DEST)
        response = await self.pay()

        self.assertFalse(response.json()["retryable"])
        self.send_mock.assert_not_awaited()

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
        async def slow_send(destination, amount_sats):
            await asyncio.sleep(0.05)
            return await self._sent(destination, amount_sats)

        self.send_mock.side_effect = slow_send
        first, second = await asyncio.gather(self.pay("site:1", 600), self.pay("site:2", 600))

        self.assertEqual(sorted([first.status_code, second.status_code]), [200, 429])
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

    async def test_find_send_returns_the_newest_matching_payment(self):
        payments = [
            self._payment(100, timestamp=1, pid="old"),
            self._payment(100, timestamp=2, pid="new"),
            self._payment(100, spark.PaymentStatus.FAILED, timestamp=3, pid="failed"),
            self._payment(200, timestamp=4, pid="other"),
        ]

        async def list_payments(*, request):
            return SimpleNamespace(payments=payments)

        sdk = SimpleNamespace(list_payments=list_payments)
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)):
            found = await self.spark_client.find_send(100, since=0)

        self.assertEqual(found["tx_id"], "new")


class SendLightningTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        _, _, self.spark_client = _load_modules(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def _sdk(self, prepare_error=None, pay_error=None):
        class Parsed(list):
            def is_lightning_address(self):
                return True

        async def parse(*, input):
            return Parsed([SimpleNamespace(pay_request=object())])

        async def prepare_lnurl_pay(*, request):
            if prepare_error:
                raise prepare_error
            return object()

        async def lnurl_pay(*, request):
            if pay_error:
                raise pay_error
            return SimpleNamespace(payment=SimpleNamespace(id="tx-1", status=spark.PaymentStatus.PENDING))

        return SimpleNamespace(parse=parse, prepare_lnurl_pay=prepare_lnurl_pay, lnurl_pay=lnurl_pay)

    async def _send(self, sdk):
        with patch.object(self.spark_client, "connect", AsyncMock(return_value=sdk)), \
                patch.object(spark, "PrepareLnurlPayRequest", lambda **kw: kw), \
                patch.object(spark, "LnurlPayRequest", lambda **kw: kw):
            return await self.spark_client.send_lightning(DEST, 100)

    async def test_prepare_failure_is_not_an_attempt(self):
        result = await self._send(self._sdk(prepare_error=RuntimeError("no route")))
        self.assertFalse(result["attempted"])
        self.assertEqual(result["tx_id"], "")

    async def test_pay_failure_is_an_attempt(self):
        result = await self._send(self._sdk(pay_error=RuntimeError("timeout")))
        self.assertTrue(result["attempted"])
        self.assertEqual(result["tx_id"], "")

    async def test_success_returns_the_payment(self):
        result = await self._send(self._sdk())
        self.assertEqual(result["tx_id"], "tx-1")
        self.assertEqual(result["status"], "processing")


if __name__ == "__main__":
    unittest.main()
