from __future__ import annotations

import hashlib
import hmac
import json
import time

import pytest

from backend.app.config import DEFAULT_SOURCES, DEFAULT_SUBSCRIPTIONS, Settings
from backend.app.db import Database


def make_settings(tmp_path, **overrides) -> Settings:
    values = dict(
        db_path=str(tmp_path / "inbox.db"),
        dispatcher_enabled=False,
        signature_tolerance_seconds=300,
        default_timeout_seconds=0.2,
        default_max_attempts=3,
        default_retry_base_seconds=0.01,
        default_retry_max_seconds=0.02,
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
def settings(tmp_path):
    return make_settings(tmp_path)


@pytest.fixture
def db(settings):
    database = Database(settings.db_path)
    database.init_schema()
    database.seed_defaults(
        [
            (source_id, name, settings.source_secrets[source_id])
            for source_id, name, secret in DEFAULT_SOURCES
        ],
        DEFAULT_SUBSCRIPTIONS,
    )
    yield database
    database.close()


def sign_payload(
    payload: dict,
    *,
    source: str = "shop",
    secret: str | None = None,
    timestamp: float | None = None,
    raw_body: bytes | None = None,
) -> tuple[bytes, dict[str, str]]:
    from backend.app.config import Settings as _Settings

    if secret is None:
        secret = _Settings.from_env().source_secrets[source]
    body = raw_body
    if body is None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ts = str(int(time.time() if timestamp is None else timestamp))
    signature = hmac.new(
        secret.encode(), ts.encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return body, {
        "content-type": "application/json",
        "x-webhook-timestamp": ts,
        "x-webhook-signature": f"sha256={signature}",
    }
