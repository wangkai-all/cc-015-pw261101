"""At-least-once downstream dispatcher with per-object ordering.

Rules implemented here:

* one in-flight delivery per (subscription, partition/business-object),
* later events for the same object wait until every earlier delivery succeeded,
* different objects (and different subscriptions) run concurrently,
* bounded exponential backoff for timeouts / network / 5xx / 429,
* permanent 4xx responses go straight to the dead-letter state,
* the delivery id and idempotency key never change across retries or restarts.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx

from .config import Settings
from .db import Database

log = logging.getLogger("webhook.dispatcher")


def iso_now(epoch: float | None = None) -> str:
    return datetime.fromtimestamp(
        time.time() if epoch is None else epoch, tz=timezone.utc
    ).isoformat(timespec="milliseconds")


class Dispatcher:
    def __init__(self, db: Database, settings: Settings, wake: asyncio.Event | None = None):
        self.db = db
        self.settings = settings
        self.wake = wake or asyncio.Event()
        self._http = httpx.AsyncClient(
            timeout=None,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
            follow_redirects=False,
        )
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._stopping = False

    async def close(self) -> None:
        self._stopping = True
        await self._http.aclose()

    def kick(self) -> None:
        self.wake.set()

    async def run(self) -> None:
        while not self._stopping:
            try:
                await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("dispatcher cycle failed; will retry")
            try:
                await asyncio.wait_for(
                    self.wake.wait(), timeout=self.settings.worker_interval_seconds
                )
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            self.wake.clear()

    # ------------------------------------------------------------------ selection
    def _find_due(self, limit: int):
        now = time.time()
        return self.db.fetchall(
            """
            SELECT d.id
              FROM deliveries d
             WHERE d.status IN ('pending', 'retrying')
               AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
               AND NOT EXISTS (
                   SELECT 1
                     FROM deliveries p
                    WHERE p.subscription_id = d.subscription_id
                      AND p.partition_key = d.partition_key
                      AND p.sequence < d.sequence
                      AND p.status <> 'succeeded'
               )
             ORDER BY d.sequence ASC, d.created_at ASC
             LIMIT ?
            """,
            (now, limit),
        )

    def _claim(self, delivery_id: str):
        """Atomically move a queued delivery to ``delivering`` and open an attempt."""
        now = time.time()
        now_iso = iso_now(now)
        with self.db.transaction() as conn:
            row = conn.execute(
                """
                SELECT d.*, s.timeout_seconds, s.retry_base_seconds, s.retry_max_seconds,
                       s.name AS subscription_name, s.active AS subscription_active,
                       e.event_id, e.source, e.event_type, e.raw_body
                  FROM deliveries d
                  JOIN subscriptions s ON s.id = d.subscription_id
                  JOIN events e ON e.id = d.event_row_id
                 WHERE d.id = ?
                """,
                (delivery_id,),
            ).fetchone()
            if row is None or row["status"] not in ("pending", "retrying"):
                return None
            if not row["subscription_active"]:
                conn.execute(
                    """
                    UPDATE deliveries
                       SET status = 'cancelled', last_error = '订阅已停用', updated_at = ?
                     WHERE id = ?
                    """,
                    (now_iso, delivery_id),
                )
                return None

            attempt_no = row["attempt_count"] + 1
            manual = bool(row["manual_pending"])
            conn.execute(
                """
                UPDATE deliveries
                   SET status = 'delivering',
                       attempt_count = ?,
                       manual_pending = 0,
                       updated_at = ?
                 WHERE id = ?
                """,
                (attempt_no, now_iso, delivery_id),
            )
            conn.execute(
                """
                INSERT INTO attempts
                    (delivery_id, attempt_no, started_at, outcome, manual)
                VALUES (?, ?, ?, 'in_flight', ?)
                """,
                (delivery_id, attempt_no, now_iso, int(manual)),
            )
            claimed = dict(row)
            claimed["attempt_no"] = attempt_no
            claimed["manual"] = manual
            return claimed

    # ------------------------------------------------------------------ execution
    async def dispatch_once(self) -> int:
        due = self._find_due(self.settings.claim_batch_size)
        claims = [c for c in (self._claim(row["id"]) for row in due) if c]
        if not claims:
            return 0
        await asyncio.gather(*(self._deliver(c) for c in claims))
        # Successes can unblock later events in the same partition.
        self.kick()
        return len(claims)

    async def _deliver(self, claim: dict) -> None:
        async with self._semaphore:
            started = time.time()
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "webhook-dispatcher/1.0",
                "X-Event-Id": claim["event_id"],
                "X-Event-Source": claim["source"],
                "X-Event-Type": claim["event_type"] or "",
                "X-Subscription-Id": claim["subscription_id"],
                "X-Delivery-Id": claim["id"],
                "X-Idempotency-Key": claim["idempotency_key"],
                "X-Attempt": str(claim["attempt_no"]),
                "X-Event-Partition": claim["partition_key"],
            }
            timeout = float(claim["timeout_seconds"])
            error: str | None = None
            status_code: int | None = None
            response_text: str | None = None
            outcome = "network_error"

            try:
                response = await self._http.post(
                    claim["callback_url"],
                    content=claim["raw_body"].encode("utf-8"),
                    headers=headers,
                    timeout=timeout,
                )
                status_code = response.status_code
                response_text = response.text[:2000]
                if 200 <= status_code < 300:
                    outcome = "success"
                else:
                    outcome = "http_error"
                    error = f"HTTP {status_code}: {response_text[:300]}"
            except httpx.TimeoutException:
                outcome = "timeout"
                error = f"请求超时（>{timeout:g}s）"
            except httpx.HTTPError as exc:
                outcome = "network_error"
                error = f"网络/连接错误: {exc.__class__.__name__}: {exc}"
            except Exception as exc:  # defensive: a bug in one delivery must not stop the loop
                outcome = "network_error"
                error = f"{exc.__class__.__name__}: {exc}"

            duration_ms = int((time.time() - started) * 1000)
            if outcome == "success":
                self._record_success(claim, status_code, response_text, duration_ms)
            else:
                permanent = (
                    outcome == "http_error"
                    and status_code is not None
                    and 400 <= status_code < 500
                    and status_code not in (408, 425, 429)
                )
                self._record_failure(
                    claim,
                    outcome=outcome,
                    status_code=status_code,
                    error=error or outcome,
                    response_text=response_text,
                    duration_ms=duration_ms,
                    permanent=permanent,
                )

    def _record_success(
        self, claim: dict, status_code: int, response_text: str | None, duration_ms: int
    ) -> None:
        now_iso = iso_now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE deliveries
                   SET status = 'succeeded',
                       succeeded_at = ?,
                       updated_at = ?,
                       last_error = NULL,
                       last_status_code = ?,
                       consecutive_failures = 0,
                       next_attempt_at = NULL
                 WHERE id = ?
                """,
                (now_iso, now_iso, status_code, claim["id"]),
            )
            conn.execute(
                """
                UPDATE attempts
                   SET outcome = 'success',
                       finished_at = ?,
                       status_code = ?,
                       duration_ms = ?,
                       response_body = ?,
                       request_headers_json = ?
                 WHERE delivery_id = ? AND attempt_no = ?
                """,
                (
                    now_iso,
                    status_code,
                    duration_ms,
                    response_text,
                    _headers_json(claim),
                    claim["id"],
                    claim["attempt_no"],
                ),
            )
        log.info(
            "delivery %s (%s/%s) succeeded in %sms",
            claim["id"],
            claim["source"],
            claim["event_id"],
            duration_ms,
        )

    def _record_failure(
        self,
        claim: dict,
        *,
        outcome: str,
        status_code: int | None,
        error: str,
        response_text: str | None,
        duration_ms: int,
        permanent: bool,
    ) -> None:
        failures = claim["consecutive_failures"] + 1
        max_attempts = claim["max_attempts"]
        exhausted = permanent or failures >= max_attempts
        now = time.time()
        now_iso = iso_now(now)

        if exhausted:
            status = "dead"
            next_attempt = None
            if permanent:
                reason = f"{error}（永久错误，不重试）"
            else:
                reason = f"{error}（已尝试 {failures}/{max_attempts} 次，进入死信）"
        else:
            status = "retrying"
            delay = min(
                float(claim["retry_max_seconds"]),
                float(claim["retry_base_seconds"]) * (2 ** (failures - 1)),
            )
            next_attempt = now + delay
            reason = f"{error}（第 {failures}/{max_attempts} 次失败，{delay:g}s 后重试）"

        with self.db.transaction() as conn:
            if exhausted:
                conn.execute(
                    """
                    UPDATE deliveries
                       SET status = 'dead',
                           consecutive_failures = ?,
                           updated_at = ?,
                           next_attempt_at = NULL,
                           last_error = ?,
                           last_status_code = ?,
                           dead_at = ?
                     WHERE id = ?
                    """,
                    (
                        failures,
                        now_iso,
                        reason,
                        status_code,
                        now_iso,
                        claim["id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE deliveries
                       SET status = 'retrying',
                           consecutive_failures = ?,
                           updated_at = ?,
                           next_attempt_at = ?,
                           last_error = ?,
                           last_status_code = ?
                     WHERE id = ?
                    """,
                    (
                        failures,
                        now_iso,
                        next_attempt,
                        reason,
                        status_code,
                        claim["id"],
                    ),
                )
            conn.execute(
                """
                UPDATE attempts
                   SET outcome = ?,
                       finished_at = ?,
                       status_code = ?,
                       error = ?,
                       duration_ms = ?,
                       response_body = ?,
                       request_headers_json = ?
                 WHERE delivery_id = ? AND attempt_no = ?
                """,
                (
                    outcome,
                    now_iso,
                    status_code,
                    reason,
                    duration_ms,
                    response_text,
                    _headers_json(claim),
                    claim["id"],
                    claim["attempt_no"],
                ),
            )
        log.warning(
            "delivery %s (%s/%s) attempt %s failed: %s",
            claim["id"],
            claim["source"],
            claim["event_id"],
            claim["attempt_no"],
            reason,
        )

    # --------------------------------------------------------------------- replay
    def replay(self, delivery_id: str) -> tuple[bool, str]:
        """Return a dead delivery to the queue, reusing its id and idempotency key."""
        now_iso = iso_now()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
            if row is None:
                return False, "delivery not found"
            if row["status"] != "dead":
                return False, f"only dead deliveries can be replayed (status={row['status']})"
            conn.execute(
                """
                UPDATE deliveries
                   SET status = 'pending',
                       consecutive_failures = 0,
                       manual_pending = 1,
                       manual_replay_count = manual_replay_count + 1,
                       next_attempt_at = NULL,
                       last_error = NULL,
                       updated_at = ?
                 WHERE id = ?
                """,
                (now_iso, delivery_id),
            )
        self.kick()
        return True, "requeued"


def _headers_json(claim: dict) -> str:
    import json

    return json.dumps(
        {
            "X-Event-Id": claim["event_id"],
            "X-Event-Source": claim["source"],
            "X-Event-Type": claim["event_type"] or "",
            "X-Subscription-Id": claim["subscription_id"],
            "X-Delivery-Id": claim["id"],
            "X-Idempotency-Key": claim["idempotency_key"],
            "X-Attempt": str(claim["attempt_no"]),
            "X-Event-Partition": claim["partition_key"],
        },
        ensure_ascii=False,
        indent=2,
    )
