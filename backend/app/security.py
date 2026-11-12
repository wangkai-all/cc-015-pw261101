"""HMAC signature and replay-window verification."""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass


@dataclass
class Verification:
    signature_valid: bool
    timestamp_valid: bool
    detail: str | None = None


def compute_signature(secret: str, timestamp_header: str, raw_body: bytes) -> str:
    """HMAC-SHA256 over ``<timestamp>.<raw-body>``.

    The timestamp is part of the signed bytes, so an attacker cannot combine a
    fresh timestamp with an old signature.
    """
    message = timestamp_header.encode("utf-8") + b"." + raw_body
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def verify_request(
    secret: str,
    timestamp_header: str | None,
    signature_header: str | None,
    raw_body: bytes,
    now: float,
    tolerance_seconds: int,
) -> Verification:
    ts_value: float | None = None
    if timestamp_header is not None:
        try:
            ts_value = float(timestamp_header.strip())
        except (TypeError, ValueError):
            ts_value = None

    timestamp_valid = (
        ts_value is not None and abs(now - ts_value) <= tolerance_seconds
    )

    if signature_header is None:
        signature_valid = False
    else:
        supplied = signature_header.strip()
        if supplied.lower().startswith("sha256="):
            supplied = supplied[7:]
        expected = compute_signature(secret, timestamp_header or "", raw_body)
        signature_valid = hmac.compare_digest(supplied.lower(), expected.lower())

    detail = None
    if not signature_valid:
        detail = "HMAC 签名不匹配"
    elif not timestamp_valid:
        if ts_value is None:
            detail = "X-Webhook-Timestamp 缺失或不是合法数字"
        elif abs(now - ts_value) > tolerance_seconds:
            direction = "未来" if ts_value > now else "过期"
            detail = f"时间戳{direction}（允许 ±{tolerance_seconds} 秒）"
        else:
            detail = "时间戳校验失败"

    return Verification(
        signature_valid=signature_valid,
        timestamp_valid=timestamp_valid,
        detail=detail,
    )
