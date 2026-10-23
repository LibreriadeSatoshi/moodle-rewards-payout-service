"""SQLite-backed state: the webhook outbox and the payout ledger.

Outbox: each row is one terminal event (settled/failed) that must reach
Moodle. A row is considered done once `delivered_at` is set.

Payout ledger: one row per payout_id the plugin asked us to pay, so a retry
with a different amount or destination is refused and a paid payout answers
with its payment. Payment state itself lives in the SDK.
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
        # Added after the first release. rejected_since: when Moodle first answered
        # 404 (no payout with this tx_id). abandoned_at: when the retrier gave up.
        columns = {row[1] for row in conn.execute("PRAGMA table_info(webhook_outbox)")}
        for column in ("rejected_since", "abandoned_at"):
            if column not in columns:
                conn.execute(f"ALTER TABLE webhook_outbox ADD COLUMN {column} TEXT")
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_outbox_pending
            ON webhook_outbox(next_attempt_at)
            WHERE delivered_at IS NULL
        """)
        # state: 'pending' until the SDK returned a payment for this payout,
        # then 'sent' with its tx_id. When the SDK reports that payment failed
        # and the invoice has a LUD-21 verify URL: 'verifying' (since
        # unconfirmed_since) until the receiver confirms it ('sent', with the
        # preimage) or still says "not paid" long enough ('failed').
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
        # Added after the first release: the invoice each payout pays, and
        # its receiver-side verification.
        payout_columns = {row[1] for row in conn.execute("PRAGMA table_info(payouts)")}
        for column in ("invoice", "payment_hash", "verify_url", "expires_at", "unconfirmed_since", "preimage"):
            if column not in payout_columns:
                kind = "INTEGER" if column == "expires_at" else "TEXT"
                conn.execute(f"ALTER TABLE payouts ADD COLUMN {column} {kind}")


@contextmanager
def _conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def enqueue(tx_id: str, status: str, payload: dict) -> str:
    """Queue a delivery, idempotent by tx_id. Returns "inserted", "upgraded" or "".

    A "settled" event replaces an earlier "failed" one for the same payment,
    delivered or not, and is delivered from scratch: Spark can report a
    Lightning send failed and later completed (the preimage arrives late), and
    then the money did leave. Nothing ever replaces a "settled" event.
    """
    body = json.dumps(payload, sort_keys=True)
    with _conn() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO webhook_outbox (tx_id, status, payload_json) VALUES (?, ?, ?)",
            (tx_id, status, body),
        )
        if cur.rowcount > 0:
            return "inserted"
        if status != "settled":
            return ""
        cur = conn.execute(
            """
            UPDATE webhook_outbox
            SET status = 'settled', payload_json = ?, attempts = 0,
                next_attempt_at = datetime('now'), delivered_at = NULL, last_error = NULL,
                rejected_since = NULL, abandoned_at = NULL
            WHERE tx_id = ? AND status = 'failed'
            """,
            (body, tx_id),
        )
        return "upgraded" if cur.rowcount > 0 else ""


def claim_due(limit: int = 20) -> list[dict]:
    """Return up-to-limit outbox rows due for delivery (not delivered, not
    abandoned, next_attempt_at <= now), each with the days since Moodle first
    answered 404 for it (None if it never did)."""
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT tx_id, status, payload_json, attempts,
                   julianday('now') - julianday(rejected_since) AS rejected_days
            FROM webhook_outbox
            WHERE delivered_at IS NULL AND abandoned_at IS NULL
              AND next_attempt_at <= datetime('now')
            ORDER BY next_attempt_at
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_delivered(tx_id: str, status: str) -> None:
    """Mark the `status` event for tx_id delivered. A no-op if the row was
    replaced by another status while it was being delivered."""
    with _conn() as conn:
        conn.execute(
            "UPDATE webhook_outbox SET delivered_at = datetime('now') WHERE tx_id = ? AND status = ?",
            (tx_id, status),
        )


def mark_abandoned(tx_id: str, status: str, error: str, give_up_days: float) -> bool:
    """Stop retrying a delivery Moodle has answered "tx_id not found" for at
    least give_up_days. The row stays in the outbox for manual review. Returns
    False, changing nothing, if the row no longer qualifies (delivered, or
    revived while the last attempt was in flight)."""
    with _conn() as conn:
        cur = conn.execute(
            """
            UPDATE webhook_outbox
            SET attempts = attempts + 1, abandoned_at = datetime('now'), last_error = ?
            WHERE tx_id = ? AND status = ? AND delivered_at IS NULL AND abandoned_at IS NULL
              AND rejected_since IS NOT NULL
              AND julianday('now') - julianday(rejected_since) >= ?
            """,
            (error[:500], tx_id, status, give_up_days),
        )
        return cur.rowcount > 0


def undelivered(limit: int = 50) -> dict:
    """Deliveries still being retried and deliveries given up on, oldest first."""
    query = """
        SELECT tx_id, status, attempts, created_at, rejected_since, abandoned_at, last_error
        FROM webhook_outbox
        WHERE delivered_at IS NULL AND abandoned_at IS {} NULL
        ORDER BY created_at
        LIMIT ?
    """
    with _conn() as conn:
        retrying = conn.execute(query.format(""), (limit,)).fetchall()
        abandoned = conn.execute(query.format("NOT"), (limit,)).fetchall()
    return {"retrying": [dict(r) for r in retrying], "abandoned": [dict(r) for r in abandoned]}


def mark_failed(tx_id: str, status: str, backoff_seconds: int, error: str, not_found: bool = False) -> None:
    """Increment attempts and push next_attempt_at into the future. not_found
    starts the give-up clock (rejected_since) if it isn't running yet; any other
    failure stops it, so only an unbroken run of "tx_id not found" counts."""
    with _conn() as conn:
        conn.execute(
            """
            UPDATE webhook_outbox
            SET attempts = attempts + 1,
                next_attempt_at = datetime('now', ? || ' seconds'),
                last_error = ?,
                rejected_since = CASE WHEN ? THEN coalesce(rejected_since, datetime('now'))
                                      ELSE NULL END
            WHERE tx_id = ? AND status = ?
            """,
            (f"+{backoff_seconds}", error[:500], not_found, tx_id, status),
        )


def revive(tx_id: str) -> None:
    """Retry an undelivered event from scratch: Moodle is asking about its
    payout again, so it may know the tx_id now."""
    with _conn() as conn:
        conn.execute(
            """
            UPDATE webhook_outbox
            SET abandoned_at = NULL, rejected_since = NULL, next_attempt_at = datetime('now')
            WHERE tx_id = ? AND delivered_at IS NULL
            """,
            (tx_id,),
        )


def get_payout(payout_id: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute("SELECT * FROM payouts WHERE payout_id = ?", (payout_id,)).fetchone()
    return dict(row) if row else None


def start_payout(
    payout_id: str, amount_sats: int, destination: str, invoice: dict | None = None, tx_id: str = ""
) -> None:
    """Record a new payout before its first send. Raises sqlite3.IntegrityError
    if the payout_id is already recorded. invoice: see set_invoice. tx_id: the
    payment id the send will get (the SDK uses the idempotency key), so events
    for it are matched to this payout even before the send returns."""
    with _conn() as conn:
        conn.execute(
            "INSERT INTO payouts (payout_id, amount_sats, destination, state, tx_id) VALUES (?, ?, ?, 'pending', ?)",
            (payout_id, amount_sats, destination, tx_id or None),
        )
    if invoice is not None:
        set_invoice(payout_id, invoice)


def set_invoice(payout_id: str, invoice: dict) -> None:
    """Store the invoice a payout pays: {invoice, payment_hash, verify_url, expires_at}."""
    with _conn() as conn:
        conn.execute(
            """
            UPDATE payouts
            SET invoice = ?, payment_hash = ?, verify_url = ?, expires_at = ?, updated_at = datetime('now')
            WHERE payout_id = ?
            """,
            (invoice["invoice"], invoice["payment_hash"], invoice["verify_url"], invoice["expires_at"], payout_id),
        )


def finish_payout(payout_id: str, state: str, tx_id: str = "", error: str = "") -> None:
    with _conn() as conn:
        conn.execute(
            """
            UPDATE payouts
            SET state = ?, tx_id = coalesce(?, tx_id), error = ?, updated_at = datetime('now')
            WHERE payout_id = ?
            """,
            (state, tx_id or None, error[:500] or None, payout_id),
        )


def record_sent(payout_id: str, tx_id: str) -> None:
    """A send returned a payment: 'pending' becomes 'sent'. Any other state was
    already decided (by the event listener or the verify checker) and stays."""
    with _conn() as conn:
        conn.execute(
            """
            UPDATE payouts SET state = 'sent', tx_id = ?, error = NULL, updated_at = datetime('now')
            WHERE payout_id = ? AND state = 'pending'
            """,
            (tx_id, payout_id),
        )


def get_payout_by_tx(tx_id: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute("SELECT * FROM payouts WHERE tx_id = ?", (tx_id,)).fetchone()
    return dict(row) if row else None


def mark_verifying(payout_id: str) -> bool:
    """The SDK says this payout's payment failed: wait for the receiver's word.
    Only from 'sent' or 'pending', and never once the receiver confirmed it
    (preimage stored); returns True if the row changed."""
    with _conn() as conn:
        cur = conn.execute(
            """
            UPDATE payouts
            SET state = 'verifying', unconfirmed_since = coalesce(unconfirmed_since, datetime('now')),
                updated_at = datetime('now')
            WHERE payout_id = ? AND state IN ('sent', 'pending') AND preimage IS NULL
            """,
            (payout_id,),
        )
        return cur.rowcount > 0


def resolve_verifying(payout_id: str, state: str, preimage: str = "") -> bool:
    """Close a 'verifying' payout as 'sent' (paid, with preimage) or 'failed'.
    Returns False if it was not 'verifying' any more."""
    with _conn() as conn:
        cur = conn.execute(
            """
            UPDATE payouts
            SET state = ?, preimage = coalesce(?, preimage), updated_at = datetime('now')
            WHERE payout_id = ? AND state = 'verifying'
            """,
            (state, preimage or None, payout_id),
        )
        return cur.rowcount > 0


def verifying(limit: int = 100) -> list[dict]:
    """Payouts waiting for the receiver's word, oldest first, with the hours waited."""
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT *, (julianday('now') - julianday(unconfirmed_since)) * 24 AS hours_unconfirmed
            FROM payouts WHERE state = 'verifying'
            ORDER BY unconfirmed_since
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


_ensure_db()
