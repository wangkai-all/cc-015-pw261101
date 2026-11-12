#!/usr/bin/env python3
"""Reset demo state: wipe the inbox DB and clear receiver state.

Usage:
    .venv/bin/python scripts/reset_demo.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    for suffix in ("", "-wal", "-shm"):
        path = ROOT / "data" / f"inbox.db{suffix}"
        if path.exists():
            path.unlink()
            print(f"removed {path}")
    for port in (9101, 9102):
        try:
            httpx.post(f"http://127.0.0.1:{port}/admin/reset", timeout=3)
            print(f"receiver :{port} state reset")
        except httpx.HTTPError:
            print(f"receiver :{port} not reachable (start it first?)", file=sys.stderr)
    print("restart the backend to re-seed sources/subscriptions, then run the demo")


if __name__ == "__main__":
    main()
