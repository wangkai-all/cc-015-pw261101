from __future__ import annotations

import json
import time

from backend.app.ingest import Ingestor

from .conftest import sign_payload


def payload(event_id="evt-1", object_id="order-1", event_type="order.created", **extra):
    body = {
        "event_id": event_id,
        "event_type": event_type,
        "object_id": object_id,
        "data": {"amount": 99},
    }
    body.update(extra)
    return body


def test_accept_persists_raw_body_and_delivery_plan(db, settings):
    ingestor = Ingestor(db, settings)
    p = payload()
    raw, headers = sign_payload(p)
    result = ingestor.ingest("shop", headers, raw)

    assert result.status_code == 202
    assert result.body["status"] == "accepted"
    assert result.accepted

    event = db.event_by_source_event_id("shop", "evt-1")
    assert event is not None
    assert event["raw_body"].encode() == raw
    assert event["content_hash"]

    deliveries = db.deliveries_for_event(event["id"])
    # shop has two demo subscriptions: fast + chaos
    assert {d["subscription_id"] for d in deliveries} == {"shop-fast", "shop-chaos"}
    for delivery in deliveries:
        assert delivery["status"] == "pending"
        assert delivery["sequence"] == event["id"]
        assert delivery["partition_key"] == "order-1"
        assert delivery["idempotency_key"]

    inbound = db.fetchone("SELECT * FROM inbound_requests WHERE id = ?", (result.inbox_request_id,))
    assert inbound["outcome"] == "accepted"
    assert inbound["signature_valid"] == 1
    assert inbound["timestamp_valid"] == 1
    assert inbound["raw_body"].encode() == raw


def test_identical_resend_is_duplicate_and_does_not_add_deliveries(db, settings):
    ingestor = Ingestor(db, settings)
    p = payload()
    raw, headers = sign_payload(p)
    first = ingestor.ingest("shop", headers, raw)
    assert first.status_code == 202

    second = ingestor.ingest("shop", headers, raw)
    assert second.status_code == 200
    assert second.body["status"] == "duplicate"
    assert second.body["duplicate_count"] == 1
    assert second.event_row_id == first.event_row_id

    event = db.event_by_source_event_id("shop", "evt-1")
    assert event["duplicate_count"] == 1
    deliveries = db.deliveries_for_event(event["id"])
    assert len(deliveries) == 2  # unchanged


def test_same_event_id_different_content_is_conflict(db, settings):
    ingestor = Ingestor(db, settings)
    raw, headers = sign_payload(payload(object_id="order-1"))
    assert ingestor.ingest("shop", headers, raw).status_code == 202

    changed = payload(object_id="order-1", data={"amount": 100})
    raw2, headers2 = sign_payload(changed)
    result = ingestor.ingest("shop", headers2, raw2)

    assert result.status_code == 409
    assert result.body["status"] == "conflict"
    assert "different content" in result.body["error"]

    event = db.event_by_source_event_id("shop", "evt-1")
    assert json.loads(event["payload_json"])["data"]["amount"] == 99
    assert len(db.deliveries_for_event(event["id"])) == 2
    inbound = db.fetchone("SELECT * FROM inbound_requests WHERE id = ?", (result.inbox_request_id,))
    assert inbound["outcome"] == "conflict"


def test_resend_with_reordered_json_keys_is_still_a_duplicate(db, settings):
    ingestor = Ingestor(db, settings)
    p1 = payload()
    raw1, h1 = sign_payload(p1)
    assert ingestor.ingest("shop", h1, raw1).status_code == 202

    # Canonical content hash means whitespace / key order do not matter.
    p2 = {k: p1[k] for k in reversed(list(p1.keys()))}
    raw2 = json.dumps(p2, ensure_ascii=False, indent=2).encode()
    _, h2 = sign_payload(p2, raw_body=raw2)
    result = ingestor.ingest("shop", h2, raw2)
    assert result.status_code == 200
    assert result.body["status"] == "duplicate"


def test_bad_signature_is_stored_but_not_accepted(db, settings):
    ingestor = Ingestor(db, settings)
    raw, headers = sign_payload(payload())
    headers["x-webhook-signature"] = "sha256=" + "00" * 32
    result = ingestor.ingest("shop", headers, raw)

    assert result.status_code == 401
    assert result.body["status"] == "invalid_signature"
    assert db.event_by_source_event_id("shop", "evt-1") is None
    inbound = db.fetchone("SELECT * FROM inbound_requests WHERE id = ?", (result.inbox_request_id,))
    assert inbound["outcome"] == "invalid_signature"
    assert inbound["signature_valid"] == 0
    assert inbound["raw_body"].encode() == raw


def test_stale_timestamp_is_stored_and_rejected(db, settings):
    ingestor = Ingestor(db, settings)
    raw, headers = sign_payload(payload(), timestamp=time.time() - 1000)
    result = ingestor.ingest("shop", headers, raw)

    assert result.status_code == 401
    assert result.body["status"] == "invalid_timestamp"
    inbound = db.fetchone("SELECT * FROM inbound_requests WHERE id = ?", (result.inbox_request_id,))
    assert inbound["outcome"] == "invalid_timestamp"
    assert inbound["signature_valid"] == 1
    assert inbound["timestamp_valid"] == 0
    assert db.event_by_source_event_id("shop", "evt-1") is None


def test_missing_event_id_is_a_bad_payload(db, settings):
    ingestor = Ingestor(db, settings)
    raw, headers = sign_payload({"event_type": "order.created"})
    result = ingestor.ingest("shop", headers, raw)
    assert result.status_code == 400
    assert result.body["status"] == "invalid_payload"
    assert db.fetchone("SELECT COUNT(*) AS c FROM events")["c"] == 0


def test_unknown_source(db, settings):
    ingestor = Ingestor(db, settings)
    raw, headers = sign_payload(payload())
    result = ingestor.ingest("nope", headers, raw)
    assert result.status_code == 404
    assert result.body["status"] == "unknown_source"


def test_subscription_filter_can_produce_empty_plan(db, settings):
    db.execute("UPDATE subscriptions SET event_type = 'invoice.*' WHERE source = 'shop'")
    ingestor = Ingestor(db, settings)
    raw, headers = sign_payload(payload(event_type="order.created"))
    result = ingestor.ingest("shop", headers, raw)
    assert result.status_code == 202
    assert result.body["deliveries"] == []
    event = db.event_by_source_event_id("shop", "evt-1")
    assert db.deliveries_for_event(event["id"]) == []
