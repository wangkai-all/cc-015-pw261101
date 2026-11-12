"""SQLite persistence layer.

Everything the inbox promises is written here before the HTTP 202 is returned.
SQLite runs in WAL mode and a single connection is guarded by an RLock so the
async dispatcher and request handlers can safely share it.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS sources (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    secret      TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    source              TEXT NOT NULL REFERENCES sources(id),
    event_type          TEXT NOT NULL DEFAULT '*',
    callback_url        TEXT NOT NULL,
    partition_key_field TEXT NOT NULL DEFAULT 'object_id',
    timeout_seconds     REAL NOT NULL DEFAULT 2.0,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    retry_base_seconds  REAL NOT NULL DEFAULT 0.4,
    retry_max_seconds   REAL NOT NULL DEFAULT 8.0,
    active              INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    raw_body        TEXT NOT NULL,
    event_type      TEXT,
    object_key      TEXT,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    received_at     TEXT NOT NULL,
    UNIQUE(source, event_id)
);

CREATE TABLE IF NOT EXISTS inbound_requests (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source           TEXT NOT NULL,
    event_id         TEXT,
    raw_body         TEXT NOT NULL,
    headers_json     TEXT NOT NULL,
    signature        TEXT,
    timestamp_header TEXT,
    signature_valid  INTEGER NOT NULL DEFAULT 0,
    timestamp_valid  INTEGER NOT NULL DEFAULT 0,
    outcome          TEXT NOT NULL,
    detail           TEXT,
    content_hash     TEXT,
    event_row_id     INTEGER REFERENCES events(id),
    received_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inbound_request_id ON inbound_requests(id DESC);
CREATE INDEX IF NOT EXISTS idx_inbound_event ON inbound_requests(event_row_id);

CREATE TABLE IF NOT EXISTS deliveries (
    id                  TEXT PRIMARY KEY,
    event_row_id        INTEGER NOT NULL REFERENCES events(id),
    subscription_id     TEXT NOT NULL REFERENCES subscriptions(id),
    downstream          TEXT NOT NULL,
    callback_url        TEXT NOT NULL,
    partition_key       TEXT NOT NULL,
    sequence            INTEGER NOT NULL,
    status              TEXT NOT NULL,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    manual_replay_count INTEGER NOT NULL DEFAULT 0,
    manual_pending      INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL,
    next_attempt_at     REAL,
    last_error          TEXT,
    last_status_code    INTEGER,
    idempotency_key     TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    succeeded_at        TEXT,
    dead_at             TEXT,
    UNIQUE(event_row_id, subscription_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_status_due
    ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_order
    ON deliveries(subscription_id, partition_key, sequence);
CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries(event_row_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_idem ON deliveries(idempotency_key);

CREATE TABLE IF NOT EXISTS attempts (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id          TEXT NOT NULL REFERENCES deliveries(id),
    attempt_no           INTEGER NOT NULL,
    started_at           TEXT NOT NULL,
    finished_at          TEXT,
    outcome              TEXT NOT NULL,
    status_code          INTEGER,
    error                TEXT,
    duration_ms          INTEGER,
    manual               INTEGER NOT NULL DEFAULT 0,
    response_body        TEXT,
    request_headers_json TEXT,
    UNIQUE(delivery_id, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON attempts(delivery_id, attempt_no);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None,
            timeout=30,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=30000")

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ helpers
    def fetchall(self, sql: str, params=(), conn=None):
        with self._lock:
            cur = (conn or self._conn).execute(sql, params)
            return cur.fetchall()

    def fetchone(self, sql: str, params=(), conn=None):
        with self._lock:
            cur = (conn or self._conn).execute(sql, params)
            return cur.fetchone()

    def execute(self, sql: str, params=(), conn=None):
        with self._lock:
            return (conn or self._conn).execute(sql, params)

    def executescript(self, sql: str) -> None:
        with self._lock:
            self._conn.executescript(sql)

    @contextmanager
    def transaction(self):
        """BEGIN IMMEDIATE serialises writers, including duplicate detection."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ------------------------------------------------------------------- sources
    def seed_defaults(self, sources, subscriptions) -> None:
        with self.transaction() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM sources").fetchone()["c"]:
                return
            for source_id, name, secret in sources:
                conn.execute(
                    "INSERT INTO sources (id, name, secret, active) VALUES (?, ?, ?, 1)",
                    (source_id, name, secret),
                )
            for sub in subscriptions:
                conn.execute(
                    """
                    INSERT INTO subscriptions
                        (id, name, source, event_type, callback_url, partition_key_field,
                         timeout_seconds, max_attempts, retry_base_seconds, retry_max_seconds, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0.4, 8.0, 1)
                    """,
                    (
                        sub["id"],
                        sub["name"],
                        sub["source"],
                        sub.get("event_type", "*"),
                        sub["callback_url"],
                        sub.get("partition_key_field", "object_id"),
                        sub.get("timeout_seconds", 2.0),
                        sub.get("max_attempts", 5),
                    ),
                )

    def get_source(self, source_id: str):
        return self.fetchone("SELECT * FROM sources WHERE id = ?", (source_id,))

    def active_subscriptions_for_source(self, source_id: str):
        return self.fetchall(
            "SELECT * FROM subscriptions WHERE source = ? AND active = 1 ORDER BY id",
            (source_id,),
        )

    # ------------------------------------------------------------- crash recovery
    def recover_orphans(self, now_iso: str, now_epoch: float) -> int:
        """Return in-flight work to the queue after a dispatcher restart.

        The downstream may already have handled the request; the stable
        idempotency key makes the retry safe. Attempts that were open are
        closed as ``interrupted`` instead of disappearing.
        """
        with self.transaction() as conn:
            interrupted = conn.execute(
                """
                UPDATE attempts
                   SET outcome = 'interrupted',
                       finished_at = ?,
                       error = COALESCE(error, '分发服务重启，本次尝试结果未知')
                 WHERE outcome = 'in_flight'
                """,
                (now_iso,),
            ).rowcount
            recovered = conn.execute(
                """
                UPDATE deliveries
                   SET status = 'retrying',
                       next_attempt_at = ?,
                       last_error = '分发服务重启，等待重投',
                       updated_at = ?
                 WHERE status = 'delivering'
                """,
                (now_epoch, now_iso),
            ).rowcount
        return max(interrupted, recovered)

    # ------------------------------------------------------------------ getters
    def event_by_source_event_id(self, source: str, event_id: str):
        return self.fetchone(
            "SELECT * FROM events WHERE source = ? AND event_id = ?",
            (source, event_id),
        )

    def event_by_row_id(self, event_row_id: int):
        return self.fetchone("SELECT * FROM events WHERE id = ?", (event_row_id,))

    def deliveries_for_event(self, event_row_id: int):
        return self.fetchall(
            """
            SELECT d.*, s.name AS subscription_name
              FROM deliveries d
              JOIN subscriptions s ON s.id = d.subscription_id
             WHERE d.event_row_id = ?
             ORDER BY d.created_at, d.subscription_id
            """,
            (event_row_id,),
        )

    def attempts_for_delivery(self, delivery_id: str):
        return self.fetchall(
            "SELECT * FROM attempts WHERE delivery_id = ? ORDER BY attempt_no",
            (delivery_id,),
        )

    def delivery(self, delivery_id: str):
        return self.fetchone("SELECT * FROM deliveries WHERE id = ?", (delivery_id,))
