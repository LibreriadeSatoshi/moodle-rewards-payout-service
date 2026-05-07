"""SQLite-backed outbox for webhook deliveries to Moodle.

Only persists delivery state — payment state itself lives in the Breez SDK.
Each row represents one terminal event (settled/failed) that must reach
Moodle. A row is considered done once `delivered_at` is set.
"""

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


_ensure_db()
