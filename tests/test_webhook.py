"""Webhook outbox: retry with backoff, give up after WEBHOOK_GIVE_UP_DAYS, list what's left."""

import asyncio
import importlib
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

ENV = {
    "BREEZ_API_KEY": "test",
    "BREEZ_MNEMONIC": "test",
    "INTERNAL_TOKEN": "secret-token",
    "MOODLE_WEBHOOK_URL": "https://example.invalid/webhook",
    "WEBHOOK_SECRET": "test",
}


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
            importlib.import_module("app")
        finally:
            os.chdir(original_cwd)
    return sys.modules["app"], db, sys.modules["webhook"]


class OutboxTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.app_module, self.db, self.webhook = _load_modules(self._dir.name)

    def tearDown(self):
        patch.stopall()
        self._dir.cleanup()

    def enqueue(self, tx_id="tx-1", age_days=0.0):
        self.db.enqueue(tx_id, "settled", {"tx_id": tx_id, "status": "settled", "amount_sats": 10})
        with sqlite3.connect(self.db.DB_PATH) as conn:
            conn.execute(
                "UPDATE webhook_outbox SET created_at = datetime('now', ?) WHERE tx_id = ?",
                (f"-{age_days * 86400:.0f} seconds", tx_id),
            )

    def row(self, tx_id="tx-1"):
        with sqlite3.connect(self.db.DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            return dict(conn.execute("SELECT * FROM webhook_outbox WHERE tx_id = ?", (tx_id,)).fetchone())

    def deliver_due(self, http_status=404):
        with patch.object(self.webhook, "_post", return_value=(http_status, '{"error":"tx_id not found"}')):
            for row in self.db.claim_due():
                self.webhook._deliver_one(row)


class GiveUpTest(OutboxTestCase):
    def test_recent_rejection_is_retried_later(self):
        self.enqueue(age_days=1)
        self.deliver_due()

        row = self.row()
        self.assertEqual(row["attempts"], 1)
        self.assertIsNone(row["abandoned_at"])
        self.assertIsNone(row["delivered_at"])
        self.assertIn("HTTP 404", row["last_error"])

    def test_rejection_older_than_the_limit_is_given_up_and_never_retried(self):
        self.enqueue(age_days=8)
        with self.assertLogs("webhook", level="ERROR") as logs:
            self.deliver_due()

        row = self.row()
        self.assertIsNotNone(row["abandoned_at"])
        self.assertIsNone(row["delivered_at"])
        self.assertIn("given up", logs.output[0])
        self.assertEqual(self.db.claim_due(), [])

    def test_network_failures_also_give_up_after_the_limit(self):
        self.enqueue(age_days=8)
        with patch.object(self.webhook, "_post", side_effect=OSError("connection refused")):
            with self.assertLogs("webhook", level="ERROR"):
                for row in self.db.claim_due():
                    self.webhook._deliver_one(row)

        self.assertIn("network", self.row()["last_error"])
        self.assertIsNotNone(self.row()["abandoned_at"])

    def test_zero_days_retries_forever(self):
        self.enqueue(age_days=400)
        with patch.object(self.webhook.settings, "webhook_give_up_days", 0):
            self.deliver_due()

        self.assertIsNone(self.row()["abandoned_at"])
        self.assertEqual(self.row()["attempts"], 1)

    def test_accepted_delivery_is_marked_delivered(self):
        self.enqueue(age_days=8)
        self.deliver_due(http_status=200)

        self.assertIsNotNone(self.row()["delivered_at"])
        self.assertIsNone(self.row()["abandoned_at"])


class MigrationTest(unittest.TestCase):
    def test_existing_outbox_gains_the_column_and_keeps_its_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data" / "outbox.db"
            path.parent.mkdir()
            with sqlite3.connect(path) as conn:
                conn.execute("""
                    CREATE TABLE webhook_outbox (
                        tx_id TEXT PRIMARY KEY, status TEXT NOT NULL, payload_json TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        next_attempt_at TEXT NOT NULL DEFAULT (datetime('now')),
                        delivered_at TEXT, last_error TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now'))
                    )
                """)
                conn.execute("INSERT INTO webhook_outbox (tx_id, status, payload_json) VALUES ('old', 'settled', '{}')")

            _, db, _ = _load_modules(directory)

            self.assertEqual([row["tx_id"] for row in db.claim_due()], ["old"])
            self.assertEqual(db.undelivered()["retrying"][0]["abandoned_at"], None)


class UndeliveredEndpointTest(OutboxTestCase):
    async def _get(self, headers):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/webhooks/undelivered", headers=headers)

    def test_lists_retrying_and_given_up_deliveries(self):
        self.enqueue("tx-new", age_days=1)
        self.enqueue("tx-old", age_days=8)
        self.enqueue("tx-done", age_days=1)
        with self.assertLogs("webhook", level="ERROR"):
            self.deliver_due()
        self.db.mark_delivered("tx-done")

        response = asyncio.run(self._get({"X-Internal-Token": "secret-token"}))

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual([r["tx_id"] for r in body["retrying"]], ["tx-new"])
        self.assertEqual([r["tx_id"] for r in body["abandoned"]], ["tx-old"])

    def test_requires_the_token(self):
        response = asyncio.run(self._get({}))
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
