from __future__ import annotations

import time

from backend.app.security import compute_signature, verify_request


def test_signature_covers_timestamp_and_raw_bytes():
    secret = "s3cret"
    body = b'{"event_id":"e1","object_id":"o1"}'
    ts = str(int(time.time()))
    sig = compute_signature(secret, ts, body)

    ok = verify_request(secret, ts, "sha256=" + sig, body, time.time(), 300)
    assert ok.signature_valid and ok.timestamp_valid

    # Changing a single byte invalidates the signature.
    bad = verify_request(secret, ts, "sha256=" + sig, body + b" ", time.time(), 300)
    assert not bad.signature_valid

    # The timestamp is part of the signed message.
    bad_ts = verify_request(secret, str(int(ts) + 1), "sha256=" + sig, body, time.time(), 300)
    assert not bad_ts.signature_valid


def test_stale_and_future_timestamps_are_rejected():
    secret = "s3cret"
    body = b"{}"
    now = time.time()
    ts = str(int(now - 1000))
    sig = compute_signature(secret, ts, body)
    result = verify_request(secret, ts, sig, body, now, 300)
    assert result.signature_valid
    assert not result.timestamp_valid
    assert "过期" in (result.detail or "")

    ts_future = str(int(now + 1000))
    sig_future = compute_signature(secret, ts_future, body)
    result = verify_request(secret, ts_future, sig_future, body, now, 300)
    assert result.signature_valid
    assert not result.timestamp_valid


def test_missing_headers_are_rejected():
    result = verify_request("s", None, None, b"{}", time.time(), 300)
    assert not result.signature_valid
    assert not result.timestamp_valid
