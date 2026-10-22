"""SQLite-backed state: the webhook outbox and the payout ledger.

Outbox: each row is one terminal event (settled/failed) that must reach
Moodle. A row is considered done once `delivered_at` is set.

Payout ledger: one row per payout_id the plugin asked us to pay, so a retried
request never sends a second payment. Payment state itself lives in the SDK.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path("data/outbox.db")


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS webhook_outbox (
                tx_id            TEXT PRIMARY KEY,
                status           TEXT NOT NULL,
                payload_json     TEXT NOT NULL,
                attempts         INTEGER NOT NULL DEFAULT 0,
                next_attempt_at  TEXT NOT NULL DEFAULT (datetime('now')),
                delivered_at     TEXT,
                last_error       TEXT,
                created_at       TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_outbox_pending
            ON webhook_outbox(next_attempt_at)
            WHERE delivered_at IS NULL
        """)
        # state: 'sending' while the SDK call runs, 'sent' once it returned a
        # payment, 'unknown' when the send raised and no payment could be found.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS payouts (
                payout_id    TEXT PRIMARY KEY,
                amount_sats  INTEGER NOT NULL,
                destination  TEXT NOT NULL,
                state        TEXT NOT NULL,
                tx_id        TEXT,
                error        TEXT,
                created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)


@contextmanager
def _conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def enqueue(tx_id: str, status: str, payload: dict) -> bool:
    """Insert a delivery. Idempotent by tx_id — returns True if inserted."""
    with _conn() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO webhook_outbox (tx_id, status, payload_json) VALUES (?, ?, ?)",
            (tx_id, status, json.dumps(payload, sort_keys=True)),
        )
        return cur.rowcount > 0


def claim_due(limit: int = 20) -> list[dict]:
    """Return up-to-limit outbox rows due for delivery (delivered_at IS NULL,
    next_attempt_at <= now)."""
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT tx_id, status, payload_json, attempts
            FROM webhook_outbox
            WHERE delivered_at IS NULL AND next_attempt_at <= datetime('now')
            ORDER BY next_attempt_at
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_delivered(tx_id: str) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE webhook_outbox SET delivered_at = datetime('now') WHERE tx_id = ?",
            (tx_id,),
        )


def mark_failed(tx_id: str, backoff_seconds: int, error: str) -> None:
    """Increment attempts and push next_attempt_at into the future."""
    with _conn() as conn:
        conn.execute(
            """
            UPDATE webhook_outbox
            SET attempts = attempts + 1,
                next_attempt_at = datetime('now', ? || ' seconds'),
                last_error = ?
            WHERE tx_id = ?
            """,
            (f"+{backoff_seconds}", error[:500], tx_id),
        )


def get_payout(payout_id: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute("SELECT * FROM payouts WHERE payout_id = ?", (payout_id,)).fetchone()
    return dict(row) if row else None


def start_payout(payout_id: str, amount_sats: int, destination: str) -> None:
    """Record that a send for payout_id is about to start. Raises
    sqlite3.IntegrityError if the payout_id is already recorded."""
    with _conn() as conn:
        conn.execute(
            "INSERT INTO payouts (payout_id, amount_sats, destination, state) VALUES (?, ?, ?, 'sending')",
            (payout_id, amount_sats, destination),
        )


def finish_payout(payout_id: str, state: str, tx_id: str = "", error: str = "") -> None:
    with _conn() as conn:
        conn.execute(
            """
            UPDATE payouts
            SET state = ?, tx_id = ?, error = ?, updated_at = datetime('now')
            WHERE payout_id = ?
            """,
            (state, tx_id or None, error[:500] or None, payout_id),
        )


def forget_payout(payout_id: str) -> None:
    """Drop a payout that failed before any payment was attempted, so the
    plugin's retry can send it."""
    with _conn() as conn:
        conn.execute("DELETE FROM payouts WHERE payout_id = ?", (payout_id,))


_ensure_db()
