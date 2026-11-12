"""Shared HMAC signing helper for demo scripts."""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.config import Settings  # noqa: E402


def sign_payload(
    payload: dict,
    *,
    source: str = "shop",
    secret: str | None = None,
    timestamp: float | None = None,
    raw_body: bytes | None = None,
) -> tuple[bytes, dict[str, str]]:
    if secret is None:
        secret = Settings.from_env().source_secrets[source]
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
