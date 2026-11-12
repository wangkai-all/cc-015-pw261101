"""Two local downstream receiver services used by the demo.

Port 9101 -- receiver-fast: always accepts quickly.
Port 9102 -- receiver-chaos: can be told to respond slowly, fail continuously,
              or process the event and then drop the connection.

Both receivers keep a processed-idempotency-key set. A retry after a timeout or
a dropped connection therefore gets a fast ``duplicate`` acknowledgement, which
is exactly the at-least-once / idempotent-consumer behaviour the dispatcher
relies on.

Per-request behaviour is selected with a ``simulate`` field in the event payload:

    {"simulate": "fail"}
    {"simulate": {"mode": "slow", "delay_seconds": 4}}
    {"simulate": {"mode": "disconnect"}}

The chaos receiver also has ``POST /admin/mode`` to set a default for all events.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

MAX_LOG = 2000


class ReceiverState:
    def __init__(self, name: str):
        self.name = name
        self.default_mode = "ok"
        self.processed: dict[str, dict] = {}
        self.received: list[dict] = []

    def record(self, entry: dict) -> dict:
        entry["seq"] = len(self.received) + 1
        if "received_at" not in entry:
            entry["received_at"] = datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            )
        self.received.append(entry)
        if len(self.received) > MAX_LOG:
            del self.received[: len(self.received) - MAX_LOG]
        return entry

    def state(self) -> dict:
        return {
            "name": self.name,
            "default_mode": self.default_mode,
            "processed_count": len(self.processed),
            "received_count": len(self.received),
            "received": list(self.received),
        }

    def reset(self) -> None:
        self.processed.clear()
        self.received.clear()
        self.default_mode = "ok"


def _headers(request: Request) -> dict:
    return {
        "idempotency_key": request.headers.get("X-Idempotency-Key"),
        "delivery_id": request.headers.get("X-Delivery-Id"),
        "event_id": request.headers.get("X-Event-Id"),
        "event_source": request.headers.get("X-Event-Source"),
        "event_type": request.headers.get("X-Event-Type"),
        "subscription_id": request.headers.get("X-Subscription-Id"),
        "attempt": request.headers.get("X-Attempt"),
        "partition": request.headers.get("X-Event-Partition"),
    }


def _simulation(payload) -> tuple[str | None, float | None]:
    simulate = payload.get("simulate") if isinstance(payload, dict) else None
    if isinstance(simulate, str):
        return simulate, None
    if isinstance(simulate, dict):
        mode = simulate.get("mode")
        delay = simulate.get("delay_seconds")
        try:
            delay = float(delay) if delay is not None else None
        except (TypeError, ValueError):
            delay = None
        return mode, delay
    return None, None


def create_receiver(name: str, chaos: bool) -> tuple[FastAPI, ReceiverState]:
    state = ReceiverState(name)
    app = FastAPI(title=f"Demo receiver ({name})")

    @app.get("/health")
    async def health():
        return {"status": "ok", "name": name, "chaos": chaos}

    @app.get("/admin/state")
    async def admin_state():
        return state.state()

    @app.post("/admin/reset")
    async def admin_reset():
        state.reset()
        return {"status": "reset", "name": name}

    @app.post("/admin/mode")
    async def admin_mode(request: Request):
        body = await request.json()
        mode = body.get("mode", "ok")
        if mode not in {"ok", "slow", "fail", "disconnect"}:
            return JSONResponse(status_code=400, content={"error": "unknown mode"})
        state.default_mode = mode
        return {"status": "ok", "default_mode": mode}

    @app.post("/hooks/orders")
    async def hook(request: Request):
        raw = await request.body()
        try:
            import json

            payload = json.loads(raw)
        except Exception:
            return JSONResponse(status_code=400, content={"error": "invalid json"})

        headers = _headers(request)
        key = headers["idempotency_key"] or ""

        # Idempotent consumer: replay of an already processed key is a fast 200.
        if key and key in state.processed:
            state.record(
                {
                    **headers,
                    "action": "duplicate",
                    "status": 200,
                    "mode": "dedup",
                }
            )
            original = state.processed[key]
            return {
                "received": True,
                "duplicate": True,
                "idempotency_key": key,
                "delivery_id": headers["delivery_id"],
                "event_id": original.get("event_id"),
                "note": "already processed; not applied twice",
            }

        mode, delay = _simulation(payload)
        if not chaos:
            mode = "ok"
        if mode is None:
            mode = state.default_mode

        common = {
            **headers,
            "mode": mode,
            "event_payload": payload,
        }

        if mode == "fail":
            state.record({**common, "action": "failed", "status": 500})
            return JSONResponse(
                status_code=500,
                content={
                    "received": False,
                    "error": "simulated downstream failure",
                    "delivery_id": headers["delivery_id"],
                    "attempt": headers["attempt"],
                },
            )

        if mode == "slow":
            delay = 4.0 if delay is None else max(0.0, delay)
            # The side effect happens; the response is deliberately slow. The
            # client times out, retries with the same key, and hits the dedup
            # path above.
            state.processed[key] = {
                "event_id": headers["event_id"],
                "delivery_id": headers["delivery_id"],
            }
            state.record(
                {**common, "action": "processed_slow_response", "status": 200}
            )
            await asyncio.sleep(delay)
            return {
                "received": True,
                "duplicate": False,
                "idempotency_key": key,
                "note": f"processed after {delay:g}s; client may have timed out",
            }

        if mode == "disconnect":
            state.processed[key] = {
                "event_id": headers["event_id"],
                "delivery_id": headers["delivery_id"],
            }
            state.record(
                {**common, "action": "processed_then_disconnect", "status": None}
            )
            if not key:
                # No idempotency key means the retry cannot be deduplicated;
                # still drop the connection to exercise the network path.
                pass

            async def broken_stream():
                yield b'{"received": true, "partial": true'
                await asyncio.sleep(0.02)
                raise ConnectionResetError("simulated disconnect after processing")

            return StreamingResponse(broken_stream(), media_type="application/json")

        # normal
        state.processed[key] = {
            "event_id": headers["event_id"],
            "delivery_id": headers["delivery_id"],
        }
        state.record({**common, "action": "processed", "status": 200})
        return {
            "received": True,
            "duplicate": False,
            "idempotency_key": key,
            "delivery_id": headers["delivery_id"],
            "event_id": headers["event_id"],
        }

    return app, state


fast_app, fast_state = create_receiver("receiver-fast", chaos=False)
chaos_app, chaos_state = create_receiver("receiver-chaos", chaos=True)
