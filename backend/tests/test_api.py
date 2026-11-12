from __future__ import annotations

import json

from fastapi.testclient import TestClient

from backend.app.main import create_app

from .conftest import make_settings, sign_payload


def base_payload():
    return {
        "event_id": "api-evt-1",
        "event_type": "order.created",
        "object_id": "api-order-1",
        "data": {"amount": 10},
    }


def test_inbox_and_event_api_exposes_raw_request_and_progress(tmp_path):
    settings = make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        body, headers = sign_payload(base_payload())
        response = client.post("/webhooks/shop", content=body, headers=headers)
        assert response.status_code == 202, response.text
        event_row_id = response.json()["event_row_id"]
        assert response.headers["X-Inbox-Request-Id"]

        # Identical resend -> duplicate and no extra deliveries.
        duplicate = client.post("/webhooks/shop", content=body, headers=headers)
        assert duplicate.status_code == 200
        assert duplicate.json()["status"] == "duplicate"

        changed = base_payload()
        changed["data"]["amount"] = 11
        body2, headers2 = sign_payload(changed)
        conflict = client.post("/webhooks/shop", content=body2, headers=headers2)
        assert conflict.status_code == 409
        assert conflict.json()["status"] == "conflict"

        overview = client.get("/api/overview").json()
        assert overview["stats"]["inbound"]["accepted"] == 1
        assert overview["stats"]["inbound"]["duplicate"] == 1
        assert overview["stats"]["inbound"]["conflict"] == 1
        assert overview["stats"]["events_total"] == 1
        assert overview["stats"]["deliveries"]["pending"] == 2

        inbox = client.get("/api/inbox").json()
        assert inbox["total"] == 3
        assert inbox["items"][0]["outcome"] == "conflict"
        assert inbox["items"][0]["raw_body"]
        assert "x-webhook-signature" in inbox["items"][0]["headers"]
        assert inbox["items"][1]["outcome"] == "duplicate"

        event = client.get(f"/api/events/{event_row_id}").json()
        assert event["event"]["payload"]["event_id"] == "api-evt-1"
        assert len(event["deliveries"]) == 2
        assert all(d["display_status"] == "pending" for d in event["deliveries"])
        assert event["deliveries"][0]["idempotency_key"]

        # No failures have happened yet.
        assert client.get("/api/dead-letters").json()["items"] == []


def test_dead_letter_replay_endpoint_reuses_same_delivery(tmp_path):
    settings = make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        body, headers = sign_payload(base_payload())
        accepted = client.post("/webhooks/shop", content=body, headers=headers).json()
        event_row_id = accepted["event_row_id"]
        db = app.state.db
        delivery = db.fetchone(
            "SELECT * FROM deliveries WHERE event_row_id = ? AND subscription_id = 'shop-fast'",
            (event_row_id,),
        )
        db.execute(
            """
            UPDATE deliveries
               SET status = 'dead', attempt_count = 2, consecutive_failures = 2,
                   last_error = 'boom', dead_at = '2026-01-01T00:00:00+00:00'
             WHERE id = ?
            """,
            (delivery["id"],),
        )
        dead = client.get("/api/dead-letters").json()
        assert dead["total"] == 1
        assert dead["items"][0]["id"] == delivery["id"]

        replay = client.post(f"/api/deliveries/{delivery['id']}/replay")
        assert replay.status_code == 200
        row = db.delivery(delivery["id"])
        assert row["status"] == "pending"
        assert row["manual_replay_count"] == 1
        assert row["idempotency_key"] == delivery["idempotency_key"]

        # Replay of a non-dead delivery is rejected.
        second = client.post(f"/api/deliveries/{delivery['id']}/replay")
        assert second.status_code == 409

        inbox = client.get("/api/inbox?outcome=accepted").json()
        assert inbox["total"] == 1
        detail = client.get(f"/api/inbox/{inbox['items'][0]['id']}").json()
        assert json.loads(json.dumps(detail["headers"]))["content-type"] == "application/json"


def test_invalid_signature_response_and_health(tmp_path):
    settings = make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        body, headers = sign_payload(base_payload())
        headers["x-webhook-signature"] = "sha256=" + "11" * 32
        response = client.post("/webhooks/shop", content=body, headers=headers)
        assert response.status_code == 401
        assert client.get("/api/health").json()["status"] == "ok"
        assert client.get("/api/dead-letters").json()["total"] == 0
        overview = client.get("/api/overview").json()
        assert overview["stats"]["inbound"]["invalid_signature"] == 1
        assert overview["stats"]["events_total"] == 0
