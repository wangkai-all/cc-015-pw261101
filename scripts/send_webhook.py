#!/usr/bin/env python3
"""Send a correctly signed demo webhook.

Example:
    .venv/bin/python scripts/send_webhook.py \
        --source shop --event-id evt-1001 --event-type order.created \
        --object-id order-42 --simulate slow
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.config import Settings  # noqa: E402


def main() -> None:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--source", default="shop")
    parser.add_argument("--event-id", default=f"evt-{int(time.time())}")
    parser.add_argument("--event-type", default="order.created")
    parser.add_argument("--object-id", default="order-42")
    parser.add_argument("--simulate", default=None, help="ok|fail|slow|disconnect")
    parser.add_argument("--delay", type=float, default=None)
    parser.add_argument("--extra", default=None, help="JSON object merged into payload")
    parser.add_argument("--json", default=None, help="raw JSON body (overrides payload fields)")
    parser.add_argument("--secret", default=None)
    parser.add_argument("--timestamp-offset", type=float, default=0.0)
    parser.add_argument("--no-sign", action="store_true")
    parser.add_argument("--print-body", action="store_true")
    args = parser.parse_args()

    if args.json:
        payload = json.loads(args.json)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    else:
        payload = {
            "event_id": args.event_id,
            "event_type": args.event_type,
            "object_id": args.object_id,
            "data": {"demo": True},
        }
        if args.simulate:
            if args.delay is not None:
                payload["simulate"] = {
                    "mode": args.simulate,
                    "delay_seconds": args.delay,
                }
            else:
                payload["simulate"] = args.simulate
        if args.extra:
            payload.update(json.loads(args.extra))
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")

    timestamp = str(int(time.time() + args.timestamp_offset))
    secret = args.secret or settings.source_secrets.get(args.source, "")
    signature = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()

    headers = {"Content-Type": "application/json", "X-Webhook-Timestamp": timestamp}
    if not args.no_sign:
        headers["X-Webhook-Signature"] = f"sha256={signature}"

    url = f"{args.url.rstrip('/')}/webhooks/{args.source}"
    if args.print_body:
        print("BODY:", body.decode())
    response = httpx.post(url, content=body, headers=headers, timeout=10)
    print("HTTP", response.status_code)
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
