"""FastAPI application: inbox API, demo UI hosting, and dispatcher lifecycle."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import DEFAULT_SOURCES, DEFAULT_SUBSCRIPTIONS, ROOT_DIR, Settings
from .db import Database
from .dispatcher import Dispatcher, iso_now
from .ingest import Ingestor

log = logging.getLogger("webhook.api")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def _iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="milliseconds")


def _row_to_dict(row) -> dict:
    return dict(row) if row is not None else None


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(settings.db_path)
        db.init_schema()
        db.seed_defaults(
            [
                (
                    source_id,
                    name,
                    settings.source_secrets.get(source_id, secret),
                )
                for source_id, name, secret in DEFAULT_SOURCES
            ],
            DEFAULT_SUBSCRIPTIONS,
        )
        recovered = db.recover_orphans(iso_now(), time.time())
        if recovered:
            log.warning("recovered %s interrupted delivery attempt(s)", recovered)

        wake = asyncio.Event()
        dispatcher = Dispatcher(db, settings, wake)
        app.state.db = db
        app.state.settings = settings
        app.state.dispatcher = dispatcher
        app.state.ingestor = Ingestor(db, settings)

        task = None
        if settings.dispatcher_enabled:
            task = asyncio.create_task(dispatcher.run(), name="webhook-dispatcher")

        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await dispatcher.close()
            db.close()

    app = FastAPI(title="Webhook Inbox", version="1.0.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------------ ingestion
    @app.post("/webhooks/{source_id}")
    async def receive_webhook(source_id: str, request: Request):
        raw_body = await request.body()
        headers = {k.lower(): v for k, v in request.headers.items()}
        result = app.state.ingestor.ingest(source_id, headers, raw_body)
        if result.accepted:
            app.state.dispatcher.kick()
        response_headers = {"X-Inbox-Request-Id": str(result.inbox_request_id)}
        if result.event_row_id is not None:
            response_headers["X-Event-Row-Id"] = str(result.event_row_id)
        return JSONResponse(
            status_code=result.status_code,
            content=result.body,
            headers=response_headers,
        )

    # ---------------------------------------------------------------- read models
    @app.get("/api/health")
    async def health():
        return {
            "status": "ok",
            "dispatcher_enabled": app.state.settings.dispatcher_enabled,
            "server_time": iso_now(),
        }

    @app.get("/api/overview")
    async def overview():
        db: Database = app.state.db
        inbound_counts = {
            row["outcome"]: row["c"]
            for row in db.fetchall(
                "SELECT outcome, COUNT(*) AS c FROM inbound_requests GROUP BY outcome"
            )
        }
        delivery_counts = {
            row["status"]: row["c"]
            for row in db.fetchall(
                "SELECT status, COUNT(*) AS c FROM deliveries GROUP BY status"
            )
        }
        events_total = db.fetchone("SELECT COUNT(*) AS c FROM events")["c"]
        blocked = db.fetchone(
            """
            SELECT COUNT(*) AS c
              FROM deliveries d
             WHERE d.status IN ('pending', 'retrying')
               AND EXISTS (
                   SELECT 1 FROM deliveries p
                    WHERE p.subscription_id = d.subscription_id
                      AND p.partition_key = d.partition_key
                      AND p.sequence < d.sequence
                      AND p.status <> 'succeeded'
               )
            """
        )["c"]
        sources = [
            dict(row)
            for row in db.fetchall(
                "SELECT id, name, active FROM sources ORDER BY id"
            )
        ]
        subscriptions = [
            dict(row)
            for row in db.fetchall(
                """
                SELECT s.id, s.name, s.source, s.event_type, s.callback_url,
                       s.partition_key_field, s.timeout_seconds, s.max_attempts,
                       s.retry_base_seconds, s.retry_max_seconds, s.active,
                       src.name AS source_name
                  FROM subscriptions s
                  JOIN sources src ON src.id = s.source
                 ORDER BY s.source, s.id
                """
            )
        ]
        return {
            "server_time": iso_now(),
            "dispatcher_enabled": app.state.settings.dispatcher_enabled,
            "stats": {
                "inbound_total": sum(inbound_counts.values()),
                "inbound": inbound_counts,
                "events_total": events_total,
                "deliveries": delivery_counts,
                "blocked": blocked,
            },
            "sources": sources,
            "subscriptions": subscriptions,
        }

    @app.get("/api/inbox")
    async def inbox(
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        outcome: str | None = None,
        source: str | None = None,
        event_id: str | None = None,
    ):
        db: Database = app.state.db
        where = []
        params: list = []
        if outcome:
            where.append("r.outcome = ?")
            params.append(outcome)
        if source:
            where.append("r.source = ?")
            params.append(source)
        if event_id:
            where.append("r.event_id = ?")
            params.append(event_id)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        rows = db.fetchall(
            f"""
            SELECT r.*, e.event_type, e.object_key, e.duplicate_count,
                   (SELECT COUNT(*) FROM deliveries d WHERE d.event_row_id = e.id) AS delivery_total,
                   (SELECT COUNT(*) FROM deliveries d WHERE d.event_row_id = e.id AND d.status = 'succeeded') AS delivery_succeeded,
                   (SELECT COUNT(*) FROM deliveries d WHERE d.event_row_id = e.id AND d.status = 'dead') AS delivery_dead,
                   (SELECT COUNT(*) FROM deliveries d WHERE d.event_row_id = e.id AND d.status IN ('pending','retrying','delivering')) AS delivery_open
              FROM inbound_requests r
              LEFT JOIN events e ON e.id = r.event_row_id
              {where_sql}
             ORDER BY r.id DESC
             LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        )
        total = db.fetchone(
            f"SELECT COUNT(*) AS c FROM inbound_requests r {where_sql}", tuple(params)
        )["c"]
        items = []
        for row in rows:
            item = dict(row)
            item["signature_valid"] = bool(item["signature_valid"])
            item["timestamp_valid"] = bool(item["timestamp_valid"])
            item["headers"] = json.loads(item.pop("headers_json"))
            item["event"] = (
                {
                    "row_id": item["event_row_id"],
                    "event_type": item.pop("event_type"),
                    "object_key": item.pop("object_key"),
                    "duplicate_count": item.pop("duplicate_count"),
                }
                if item["event_row_id"] is not None
                else None
            )
            item["delivery_summary"] = {
                "total": item.pop("delivery_total") or 0,
                "succeeded": item.pop("delivery_succeeded") or 0,
                "dead": item.pop("delivery_dead") or 0,
                "open": item.pop("delivery_open") or 0,
            }
            items.append(item)
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    @app.get("/api/inbox/{request_id}")
    async def inbox_detail(request_id: int):
        db: Database = app.state.db
        row = db.fetchone("SELECT * FROM inbound_requests WHERE id = ?", (request_id,))
        if row is None:
            return JSONResponse(status_code=404, content={"error": "not found"})
        item = dict(row)
        item["signature_valid"] = bool(item["signature_valid"])
        item["timestamp_valid"] = bool(item["timestamp_valid"])
        item["headers"] = json.loads(item.pop("headers_json"))
        return item

    @app.get("/api/events/{event_row_id}")
    async def event_detail(event_row_id: int):
        db: Database = app.state.db
        event = db.event_by_row_id(event_row_id)
        if event is None:
            return JSONResponse(status_code=404, content={"error": "not found"})
        event = dict(event)
        event["payload"] = json.loads(event.pop("payload_json"))
        deliveries = []
        for delivery in db.deliveries_for_event(event_row_id):
            d = dict(delivery)
            d["next_attempt_at_iso"] = _iso(d["next_attempt_at"])
            d["blocked_by"] = _blocked_by(db, d)
            d["display_status"] = (
                "blocked"
                if d["status"] in ("pending", "retrying") and d["blocked_by"] is not None
                else d["status"]
            )
            attempts = []
            for attempt in db.attempts_for_delivery(d["id"]):
                a = dict(attempt)
                a["manual"] = bool(a["manual"])
                if a.get("request_headers_json"):
                    try:
                        a["request_headers"] = json.loads(a["request_headers_json"])
                    except json.JSONDecodeError:
                        a["request_headers"] = a["request_headers_json"]
                else:
                    a["request_headers"] = None
                a.pop("request_headers_json", None)
                attempts.append(a)
            d["attempts"] = attempts
            deliveries.append(d)
        return {"event": event, "deliveries": deliveries}

    @app.get("/api/dead-letters")
    async def dead_letters(limit: int = Query(100, ge=1, le=500)):
        db: Database = app.state.db
        rows = db.fetchall(
            """
            SELECT d.*, e.event_id, e.event_type, e.object_key, e.raw_body,
                   e.payload_json, s.name AS subscription_name
              FROM deliveries d
              JOIN events e ON e.id = d.event_row_id
              JOIN subscriptions s ON s.id = d.subscription_id
             WHERE d.status = 'dead'
             ORDER BY d.dead_at DESC, d.id
             LIMIT ?
            """,
            (limit,),
        )
        items = []
        for row in rows:
            d = dict(row)
            d["payload"] = json.loads(d.pop("payload_json"))
            d["blocked_followers"] = db.fetchone(
                """
                SELECT COUNT(*) AS c FROM deliveries
                 WHERE subscription_id = ?
                   AND partition_key = ?
                   AND sequence > ?
                   AND status <> 'succeeded'
                """,
                (d["subscription_id"], d["partition_key"], d["sequence"]),
            )["c"]
            attempts = []
            for attempt in db.attempts_for_delivery(d["id"]):
                a = dict(attempt)
                a["manual"] = bool(a["manual"])
                a.pop("request_headers_json", None)
                attempts.append(a)
            d["attempts"] = attempts
            d["next_attempt_at_iso"] = _iso(d["next_attempt_at"])
            items.append(d)
        return {"items": items, "total": len(items)}

    # ------------------------------------------------------------------- actions
    @app.post("/api/deliveries/{delivery_id}/replay")
    async def replay_delivery(delivery_id: str):
        ok, message = app.state.dispatcher.replay(delivery_id)
        if not ok:
            return JSONResponse(
                status_code=409, content={"status": "rejected", "message": message}
            )
        return {
            "status": "requeued",
            "delivery_id": delivery_id,
            "message": "已沿用原投递编号与幂等键重新入队",
        }

    @app.post("/api/admin/dispatch-now")
    async def dispatch_now():
        app.state.dispatcher.kick()
        return {"status": "signalled"}

    @app.post("/api/admin/recover")
    async def recover_now():
        recovered = app.state.db.recover_orphans(iso_now(), time.time())
        app.state.dispatcher.kick()
        return {"recovered": recovered}

    # --------------------------------------------------------------- static SPA
    dist = ROOT_DIR / "frontend" / "dist"
    if dist.exists():
        app.mount("/", StaticFiles(directory=str(dist), html=True), name="frontend")
    else:
        @app.get("/")
        async def missing_frontend():
            return {
                "message": "Frontend not built. Run: cd frontend && npm install && npm run build",
                "api_docs": "/docs",
            }

    return app


def _blocked_by(db: Database, delivery: dict) -> dict | None:
    if delivery["status"] not in ("pending", "retrying"):
        return None
    row = db.fetchone(
        """
        SELECT p.id, p.status, p.sequence, e.event_id
          FROM deliveries p
          JOIN events e ON e.id = p.event_row_id
         WHERE p.subscription_id = ?
           AND p.partition_key = ?
           AND p.sequence < ?
           AND p.status <> 'succeeded'
         ORDER BY p.sequence DESC
         LIMIT 1
        """,
        (delivery["subscription_id"], delivery["partition_key"], delivery["sequence"]),
    )
    return _row_to_dict(row)


app = create_app()
