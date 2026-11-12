"""Inbound webhook verification and durable acceptance.

The order of operations is deliberate:

1. verify HMAC and timestamp (raw bytes, never a re-serialised body),
2. deduplicate by (source, event_id),
3. persist the original bytes and the full delivery plan in one transaction,
4. only then return success to the sender.

A crash before commit means no success was returned; a crash after commit means
the dispatcher can pick the deliveries up from SQLite.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from .config import Settings
from .db import Database
from .security import verify_request

SIGNATURE_HEADER = "x-webhook-signature"
TIMESTAMP_HEADER = "x-webhook-timestamp"


@dataclass
class IngestResult:
    status_code: int
    body: dict[str, Any]
    inbox_request_id: int
    event_row_id: int | None = None
    accepted: bool = False
    deliveries: list[dict[str, Any]] = field(default_factory=list)


def canonical_content_hash(payload: Any) -> str:
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def extract_path(payload: Any, path: str):
    current = payload
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _as_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


class Ingestor:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings

    def ingest(
        self,
        source_id: str,
        headers: dict[str, str],
        raw_body: bytes,
        *,
        now: float | None = None,
    ) -> IngestResult:
        now = time.time() if now is None else now
        now_iso = _iso(now)
        body_text = raw_body.decode("utf-8", errors="replace")
        headers_lower = {str(k).lower(): str(v) for k, v in headers.items()}
        headers_json = json.dumps(
            dict(sorted(headers_lower.items())), ensure_ascii=False, indent=2
        )

        source = self.db.get_source(source_id)
        if source is None:
            inbox_id = self._record(
                source=source_id,
                event_id=None,
                raw_body=body_text,
                headers_json=headers_json,
                signature=headers_lower.get(SIGNATURE_HEADER),
                timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                signature_valid=False,
                timestamp_valid=False,
                outcome="unknown_source",
                detail=f"未知来源 {source_id}，无法选择验签密钥",
                content_hash=None,
                event_row_id=None,
                received_at=now_iso,
            )
            return IngestResult(
                status_code=404,
                body={
                    "status": "unknown_source",
                    "error": f"unknown source: {source_id}",
                    "inbox_request_id": inbox_id,
                },
                inbox_request_id=inbox_id,
            )

        verification = verify_request(
            secret=source["secret"],
            timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
            signature_header=headers_lower.get(SIGNATURE_HEADER),
            raw_body=raw_body,
            now=now,
            tolerance_seconds=self.settings.signature_tolerance_seconds,
        )

        if not verification.signature_valid or not verification.timestamp_valid:
            outcome = (
                "invalid_signature"
                if not verification.signature_valid
                else "invalid_timestamp"
            )
            inbox_id = self._record(
                source=source_id,
                event_id=None,
                raw_body=body_text,
                headers_json=headers_json,
                signature=headers_lower.get(SIGNATURE_HEADER),
                timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                signature_valid=verification.signature_valid,
                timestamp_valid=verification.timestamp_valid,
                outcome=outcome,
                detail=verification.detail,
                content_hash=None,
                event_row_id=None,
                received_at=now_iso,
            )
            return IngestResult(
                status_code=401,
                body={
                    "status": outcome,
                    "error": verification.detail,
                    "inbox_request_id": inbox_id,
                },
                inbox_request_id=inbox_id,
            )

        try:
            payload = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            inbox_id = self._record(
                source=source_id,
                event_id=None,
                raw_body=body_text,
                headers_json=headers_json,
                signature=headers_lower.get(SIGNATURE_HEADER),
                timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                signature_valid=True,
                timestamp_valid=True,
                outcome="invalid_payload",
                detail=f"请求体不是合法 JSON: {exc}",
                content_hash=None,
                event_row_id=None,
                received_at=now_iso,
            )
            return IngestResult(
                status_code=400,
                body={
                    "status": "invalid_payload",
                    "error": "body is not valid JSON",
                    "inbox_request_id": inbox_id,
                },
                inbox_request_id=inbox_id,
            )

        if not isinstance(payload, dict) or not _as_text(payload.get("event_id")):
            inbox_id = self._record(
                source=source_id,
                event_id=None,
                raw_body=body_text,
                headers_json=headers_json,
                signature=headers_lower.get(SIGNATURE_HEADER),
                timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                signature_valid=True,
                timestamp_valid=True,
                outcome="invalid_payload",
                detail="JSON 对象缺少非空 event_id",
                content_hash=None,
                event_row_id=None,
                received_at=now_iso,
            )
            return IngestResult(
                status_code=400,
                body={
                    "status": "invalid_payload",
                    "error": "payload must be a JSON object with a non-empty event_id",
                    "inbox_request_id": inbox_id,
                },
                inbox_request_id=inbox_id,
            )

        event_id = _as_text(payload.get("event_id")).strip()
        event_type = _as_text(payload.get("event_type"))
        content_hash = canonical_content_hash(payload)

        # ------------------------------------------------------------------
        # The duplicate check, event insert and delivery-plan insert are one
        # IMMEDIATE transaction. No 202 is possible unless all of it committed.
        # ------------------------------------------------------------------
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM events WHERE source = ? AND event_id = ?",
                (source_id, event_id),
            ).fetchone()

            if existing is not None:
                if existing["content_hash"] == content_hash:
                    conn.execute(
                        "UPDATE events SET duplicate_count = duplicate_count + 1 WHERE id = ?",
                        (existing["id"],),
                    )
                    duplicate_count = existing["duplicate_count"] + 1
                    inbox_id = self._record_in_tx(
                        conn,
                        source=source_id,
                        event_id=event_id,
                        raw_body=body_text,
                        headers_json=headers_json,
                        signature=headers_lower.get(SIGNATURE_HEADER),
                        timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                        signature_valid=True,
                        timestamp_valid=True,
                        outcome="duplicate",
                        detail="相同来源与 event_id、相同内容，未重复投递",
                        content_hash=content_hash,
                        event_row_id=existing["id"],
                        received_at=now_iso,
                    )
                    return IngestResult(
                        status_code=200,
                        body={
                            "status": "duplicate",
                            "event_id": event_id,
                            "event_row_id": existing["id"],
                            "duplicate_count": duplicate_count,
                            "inbox_request_id": inbox_id,
                            "message": "event already accepted; delivery plan unchanged",
                        },
                        inbox_request_id=inbox_id,
                        event_row_id=existing["id"],
                    )

                inbox_id = self._record_in_tx(
                    conn,
                    source=source_id,
                    event_id=event_id,
                    raw_body=body_text,
                    headers_json=headers_json,
                    signature=headers_lower.get(SIGNATURE_HEADER),
                    timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                    signature_valid=True,
                    timestamp_valid=True,
                    outcome="conflict",
                    detail="同一来源的 event_id 已存在，但请求内容不同",
                    content_hash=content_hash,
                    event_row_id=existing["id"],
                    received_at=now_iso,
                )
                return IngestResult(
                    status_code=409,
                    body={
                        "status": "conflict",
                        "error": "event_id already exists with different content",
                        "event_id": event_id,
                        "event_row_id": existing["id"],
                        "inbox_request_id": inbox_id,
                    },
                    inbox_request_id=inbox_id,
                    event_row_id=existing["id"],
                )

            cursor = conn.execute(
                """
                INSERT INTO events
                    (source, event_id, content_hash, payload_json, raw_body,
                     event_type, object_key, duplicate_count, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    source_id,
                    event_id,
                    content_hash,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    body_text,
                    event_type,
                    _as_text(extract_path(payload, "object_id")),
                    now_iso,
                ),
            )
            event_row_id = cursor.lastrowid

            subscriptions = [
                row
                for row in conn.execute(
                    "SELECT * FROM subscriptions WHERE source = ? AND active = 1 ORDER BY id",
                    (source_id,),
                ).fetchall()
                if fnmatch.fnmatchcase(event_type or "", row["event_type"])
            ]

            deliveries: list[dict[str, Any]] = []
            for sub in subscriptions:
                partition_value = extract_path(payload, sub["partition_key_field"])
                partition_key = (
                    str(partition_value)
                    if partition_value is not None
                    else f"event:{event_id}"
                )
                delivery_id = "dlv_" + uuid.uuid4().hex
                idempotency_key = hashlib.sha256(
                    f"{source_id}\n{event_id}\n{sub['id']}".encode("utf-8")
                ).hexdigest()
                conn.execute(
                    """
                    INSERT INTO deliveries
                        (id, event_row_id, subscription_id, downstream, callback_url,
                         partition_key, sequence, status, attempt_count,
                         consecutive_failures, manual_replay_count, manual_pending,
                         max_attempts, next_attempt_at, idempotency_key,
                         created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, 0, 0,
                            ?, NULL, ?, ?, ?)
                    """,
                    (
                        delivery_id,
                        event_row_id,
                        sub["id"],
                        sub["name"],
                        sub["callback_url"],
                        partition_key,
                        event_row_id,
                        sub["max_attempts"],
                        idempotency_key,
                        now_iso,
                        now_iso,
                    ),
                )
                deliveries.append(
                    {
                        "delivery_id": delivery_id,
                        "subscription_id": sub["id"],
                        "downstream": sub["name"],
                        "partition_key": partition_key,
                        "sequence": event_row_id,
                        "idempotency_key": idempotency_key,
                    }
                )

            inbox_id = self._record_in_tx(
                conn,
                source=source_id,
                event_id=event_id,
                raw_body=body_text,
                headers_json=headers_json,
                signature=headers_lower.get(SIGNATURE_HEADER),
                timestamp_header=headers_lower.get(TIMESTAMP_HEADER),
                signature_valid=True,
                timestamp_valid=True,
                outcome="accepted",
                detail=(
                    f"已生成 {len(deliveries)} 条投递计划"
                    if deliveries
                    else "没有匹配的启用订阅，投递计划为空"
                ),
                content_hash=content_hash,
                event_row_id=event_row_id,
                received_at=now_iso,
            )

        return IngestResult(
            status_code=202,
            body={
                "status": "accepted",
                "event_id": event_id,
                "event_row_id": event_row_id,
                "inbox_request_id": inbox_id,
                "deliveries": deliveries,
                "received_at": now_iso,
            },
            inbox_request_id=inbox_id,
            event_row_id=event_row_id,
            accepted=True,
            deliveries=deliveries,
        )

    # ------------------------------------------------------------------ helpers
    def _record(self, **kwargs) -> int:
        with self.db.transaction() as conn:
            return self._record_in_tx(conn, **kwargs)

    def _record_in_tx(self, conn, **kwargs) -> int:
        columns = [
            "source",
            "event_id",
            "raw_body",
            "headers_json",
            "signature",
            "timestamp_header",
            "signature_valid",
            "timestamp_valid",
            "outcome",
            "detail",
            "content_hash",
            "event_row_id",
            "received_at",
        ]
        values = [kwargs.get(c) for c in columns]
        cursor = conn.execute(
            f"INSERT INTO inbound_requests ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            values,
        )
        return cursor.lastrowid


def _iso(epoch: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="milliseconds")
