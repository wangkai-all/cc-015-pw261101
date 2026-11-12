from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from backend.app.dispatcher import Dispatcher
from backend.app.ingest import Ingestor

from .conftest import sign_payload


class FakeDownstream(httpx.AsyncBaseTransport):
    """Records attempts and returns programmable outcomes."""

    def __init__(self):
        self.calls: list[dict] = []
        self.fail_event_ids: set[str] = set()
        self.status_by_event: dict[str, int] = {}
        self.raise_by_event: dict[str, Exception] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        event_id = payload.get("event_id")
        call = {
            "event_id": event_id,
            "object_id": payload.get("object_id"),
            "delivery_id": request.headers.get("X-Delivery-Id"),
            "idempotency_key": request.headers.get("X-Idempotency-Key"),
            "attempt": int(request.headers.get("X-Attempt", "0")),
            "subscription_id": request.headers.get("X-Subscription-Id"),
            "url": str(request.url),
        }
        self.calls.append(call)
        if event_id in self.raise_by_event:
            raise self.raise_by_event[event_id]
        if event_id in self.fail_event_ids:
            return httpx.Response(500, json={"error": "boom"}, request=request)
        status = self.status_by_event.get(event_id, 200)
        return httpx.Response(status, json={"received": True}, request=request)


def setup_chaos_only(db):
    db.execute("UPDATE subscriptions SET active = 0 WHERE id = 'shop-fast'")
    db.execute(
        """
        UPDATE subscriptions
           SET max_attempts = 3,
               retry_base_seconds = 0.01,
               retry_max_seconds = 0.02,
               timeout_seconds = 0.2
         WHERE id = 'shop-chaos'
        """
    )


def ingest_event(db, settings, event_id, object_id, *, simulate=None):
    payload = {
        "event_id": event_id,
        "event_type": "order.created",
        "object_id": object_id,
        "data": {"n": 1},
    }
    if simulate:
        payload["simulate"] = simulate
    raw, headers = sign_payload(payload)
    result = Ingestor(db, settings).ingest("shop", headers, raw)
    assert result.status_code == 202, result.body
    return result


def delivery_for(db, event_row_id):
    return db.fetchone(
        "SELECT * FROM deliveries WHERE event_row_id = ? AND subscription_id = 'shop-chaos'",
        (event_row_id,),
    )


async def make_dispatcher(db, settings, transport):
    dispatcher = Dispatcher(db, settings)
    await dispatcher._http.aclose()
    dispatcher._http = httpx.AsyncClient(transport=transport, timeout=None)
    return dispatcher


async def test_ordering_dead_letter_and_manual_replay_keeps_delivery_id(db, settings):
    setup_chaos_only(db)
    blocked = ingest_event(db, settings, "evt-block-1", "obj-block")
    waiting = ingest_event(db, settings, "evt-block-2", "obj-block")
    other = ingest_event(db, settings, "evt-other-1", "obj-other")

    transport = FakeDownstream()
    transport.fail_event_ids.add("evt-block-1")
    dispatcher = await make_dispatcher(db, settings, transport)

    await dispatcher.dispatch_once()
    # Different object may proceed immediately; the second event of the blocked
    # object must not be sent while its predecessor is unsuccessful.
    assert {c["event_id"] for c in transport.calls} == {"evt-block-1", "evt-other-1"}
    assert waiting.body["deliveries"][0]["delivery_id"] not in {
        c["delivery_id"] for c in transport.calls
    }

    # Retry until the first delivery exhausts its 3 attempts and dies.
    for _ in range(2):
        await asyncio.sleep(0.02)
        await dispatcher.dispatch_once()
    first_delivery = delivery_for(db, blocked.event_row_id)
    assert first_delivery["status"] == "dead"
    assert first_delivery["attempt_count"] == 3
    assert waiting.body["deliveries"][0]["delivery_id"] not in {
        c["delivery_id"] for c in transport.calls
    }

    # A dead predecessor still blocks the same object's later events.
    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()
    assert "evt-block-2" not in {c["event_id"] for c in transport.calls}

    # Manual replay reuses the delivery id and idempotency key, resets the retry
    # budget, and lets the queue drain in order.
    transport.fail_event_ids.clear()
    ok, _ = dispatcher.replay(first_delivery["id"])
    assert ok
    replay_row = delivery_for(db, blocked.event_row_id)
    assert replay_row["status"] == "pending"
    assert replay_row["manual_replay_count"] == 1
    assert replay_row["idempotency_key"] == first_delivery["idempotency_key"]

    await dispatcher.dispatch_once()  # evt-block-1 succeeds
    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()  # evt-block-2 now unblocked
    await dispatcher.dispatch_once()

    calls_for_block = [c for c in transport.calls if c["event_id"] == "evt-block-1"]
    calls_for_waiting = [c for c in transport.calls if c["event_id"] == "evt-block-2"]
    assert calls_for_block, calls_for_waiting
    assert all(c["delivery_id"] == first_delivery["id"] for c in calls_for_block)
    assert all(
        c["idempotency_key"] == first_delivery["idempotency_key"]
        for c in calls_for_block
    )

    # The successful replay attempt is number 4, and evt-block-2 was only sent
    # after that success.
    successful_block_attempt = calls_for_block[-1]
    assert successful_block_attempt["attempt"] == 4
    assert transport.calls.index(successful_block_attempt) < transport.calls.index(
        calls_for_waiting[0]
    )

    attempts = db.attempts_for_delivery(first_delivery["id"])
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4]
    assert [bool(a["manual"]) for a in attempts] == [False, False, False, True]
    assert attempts[-1]["outcome"] == "success"
    await dispatcher.close()


async def test_retry_attempts_track_every_delivery_and_stable_idempotency(db, settings):
    setup_chaos_only(db)
    one = ingest_event(db, settings, "evt-retry", "obj-retry")
    transport = FakeDownstream()
    transport.fail_event_ids.add("evt-retry")
    dispatcher = await make_dispatcher(db, settings, transport)

    await dispatcher.dispatch_once()
    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()
    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()

    delivery = delivery_for(db, one.event_row_id)
    assert delivery["status"] == "dead"
    attempts = db.attempts_for_delivery(delivery["id"])
    assert len(attempts) == 3
    assert all(a["status_code"] == 500 for a in attempts)
    assert all(a["outcome"] == "http_error" for a in attempts)
    assert all(a["error"] for a in attempts)
    assert {c["idempotency_key"] for c in transport.calls} == {
        delivery["idempotency_key"]
    }
    assert {c["delivery_id"] for c in transport.calls} == {delivery["id"]}
    await dispatcher.close()


async def test_permanent_4xx_goes_straight_to_dead_letter(db, settings):
    setup_chaos_only(db)
    one = ingest_event(db, settings, "evt-400", "obj-400")
    transport = FakeDownstream()
    transport.status_by_event["evt-400"] = 400
    dispatcher = await make_dispatcher(db, settings, transport)

    await dispatcher.dispatch_once()
    delivery = delivery_for(db, one.event_row_id)
    assert delivery["status"] == "dead"
    assert delivery["attempt_count"] == 1
    assert delivery["consecutive_failures"] == 1
    assert "永久错误" in delivery["last_error"]

    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()
    assert len(transport.calls) == 1  # no retry
    await dispatcher.close()


async def test_timeout_is_transient_and_retried(db, settings):
    setup_chaos_only(db)
    one = ingest_event(db, settings, "evt-timeout", "obj-timeout")
    transport = FakeDownstream()
    transport.raise_by_event["evt-timeout"] = httpx.ReadTimeout("slow")
    dispatcher = await make_dispatcher(db, settings, transport)

    await dispatcher.dispatch_once()
    delivery = delivery_for(db, one.event_row_id)
    assert delivery["status"] == "retrying"
    attempts = db.attempts_for_delivery(delivery["id"])
    assert attempts[0]["outcome"] == "timeout"

    transport.raise_by_event.clear()
    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()
    delivery = delivery_for(db, one.event_row_id)
    assert delivery["status"] == "succeeded"
    assert delivery["attempt_count"] == 2
    await dispatcher.close()


async def test_restart_recovery_requeues_inflight_without_losing_attempt(db, settings):
    setup_chaos_only(db)
    one = ingest_event(db, settings, "evt-crash", "obj-crash")
    transport = FakeDownstream()
    dispatcher = await make_dispatcher(db, settings, transport)

    delivery = delivery_for(db, one.event_row_id)
    claim = dispatcher._claim(delivery["id"])
    assert claim is not None
    # Simulate the process dying between the claim (attempt written) and the
    # HTTP response. The TCP request may have reached the downstream.
    recovered = db.recover_orphans("2026-01-01T00:00:00.000+00:00", 0.0)
    assert recovered >= 1

    delivery = delivery_for(db, one.event_row_id)
    assert delivery["status"] == "retrying"
    attempts = db.attempts_for_delivery(delivery["id"])
    assert attempts[0]["outcome"] == "interrupted"

    await dispatcher.dispatch_once()
    await asyncio.sleep(0.02)
    await dispatcher.dispatch_once()
    delivery = delivery_for(db, one.event_row_id)
    assert delivery["status"] == "succeeded"
    attempts = db.attempts_for_delivery(delivery["id"])
    assert [a["attempt_no"] for a in attempts] == [1, 2]
    assert attempts[0]["outcome"] == "interrupted"
    assert attempts[1]["outcome"] == "success"
    assert {c["idempotency_key"] for c in transport.calls} == {
        delivery["idempotency_key"]
    }
    assert {c["delivery_id"] for c in transport.calls} == {delivery["id"]}
    await dispatcher.close()


async def test_replay_only_dead_deliveries(db, settings):
    setup_chaos_only(db)
    one = ingest_event(db, settings, "evt-pending", "obj-pending")
    dispatcher = Dispatcher(db, settings)
    await dispatcher._http.aclose()
    dispatcher._http = httpx.AsyncClient(transport=FakeDownstream(), timeout=None)
    delivery = delivery_for(db, one.event_row_id)
    ok, message = dispatcher.replay(delivery["id"])
    assert not ok
    assert "only dead" in message
    await dispatcher.close()
