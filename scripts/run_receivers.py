#!/usr/bin/env python3
"""Run the two local demo receivers on ports 9101 and 9102."""
from __future__ import annotations

import multiprocessing
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _serve(module_path: str, port: int) -> None:
    import uvicorn

    uvicorn.run(module_path, host="127.0.0.1", port=port, log_level="info")


def main() -> None:
    processes = [
        multiprocessing.Process(
            target=_serve, args=("backend.app.receivers:fast_app", 9101), daemon=False
        ),
        multiprocessing.Process(
            target=_serve, args=("backend.app.receivers:chaos_app", 9102), daemon=False
        ),
    ]
    for p in processes:
        p.start()
    print("receivers: fast=http://127.0.0.1:9101  chaos=http://127.0.0.1:9102", flush=True)
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=5)


if __name__ == "__main__":
    main()
