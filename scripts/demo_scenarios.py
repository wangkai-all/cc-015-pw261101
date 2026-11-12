#!/usr/bin/env python3
"""End-to-end demo of the webhook inbox pipeline.

Prerequisites:
    1. .venv/bin/python scripts/run_receivers.py
    2. .venv/bin/uvicorn backend.app.main:app --port 8000

Then run:
    .venv/bin/python scripts/demo_scenarios.py

Every scenario prints what to look at in http://127.0.0.1:8000 afterwards.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.signing_util import sign_payload  # noqa: E402

API = "http://127.0.0.1:8000"
CHAOS = "http://127.0.0.1:9102"
client = httpx.Client(timeout=10)


def send(event_id: str, object_id: str, *, simulate=None, source="shop", extra=None):
    payload = {
        "event_id": event_id,
        "event_type": "order.created",
        "object_id": object_id,
        "data": {"demo": True},
    }
    if simulate:
        payload["simulate"] = simulate
    if extra:
        payload.update(extra)
    body, headers = sign_payload(payload, source=source)
    return client.post(f"{API}/webhooks/{source}", content=body, headers=headers)


def wait_for(description: str, predicate, timeout=15.0, interval=0.25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise SystemExit(f"TIMEOUT waiting for: {description}")


def event_detail(event_row_id: int):
    return client.get(f"{API}/api/events/{event_row_id}").json()


def chaos_delivery(event_row_id: int):
    detail = event_detail(event_row_id)
    return next(d for d in detail["deliveries"] if d["subscription_id"] == "shop-chaos")


def banner(title: str):
    print(f"\n{'=' * 68}\n{title}\n{'=' * 68}")


def main() -> None:
    for url, name in ((f"{API}/api/health", "backend"), (f"{CHAOS}/health", "chaos receiver")):
        try:
            client.get(url).raise_for_status()
        except httpx.HTTPError:
            raise SystemExit(f"{name} is not reachable at {url}; start services first")

    banner("场景 1 · 正常事件：验签 → 落库 → 两个下游并行投递")
    r = send("demo-ok-1", "order-normal")
    assert r.status_code == 202, r.text
    row = r.json()["event_row_id"]
    wait_for(
        "both deliveries succeed",
        lambda: all(
            d["display_status"] == "succeeded" for d in event_detail(row)["deliveries"]
        ),
    )
    print("✓ 两个订阅（fast/chaos）都已成功。页面：收件箱 → demo-ok-1 → 投递计划与进度")

    banner("场景 2 · 重复发送与内容冲突")
    dup = send("demo-ok-1", "order-normal")
    assert dup.status_code == 200 and dup.json()["status"] == "duplicate", dup.text
    conflict = send("demo-ok-1", "order-normal", extra={"data": {"demo": "changed"}})
    assert conflict.status_code == 409, conflict.text
    print("✓ 相同内容 → 200 duplicate（不产生新投递）；换了内容 → 409 conflict")
    print("  页面：收件箱里能看到 accepted / duplicate / conflict 三条记录")

    banner("场景 3 · 下游处理成功但响应超时（慢响应）→ 幂等去重")
    r = send("demo-slow-1", "order-slow", simulate={"mode": "slow", "delay_seconds": 4})
    row = r.json()["event_row_id"]
    delivery = wait_for(
        "chaos delivery succeeds after timeout retry",
        lambda: chaos_delivery(row)["display_status"] == "succeeded"
        and chaos_delivery(row),
        timeout=20,
    )
    outcomes = [a["outcome"] for a in delivery["attempts"]]
    assert outcomes == ["timeout", "success"], outcomes
    keys = {a for a in [delivery["idempotency_key"]]}
    assert len(keys) == 1
    print(f"✓ 尝试记录：{outcomes}，两次使用同一幂等键 {delivery['idempotency_key'][:16]}…")
    print("  页面：demo-slow-1 → receiver-chaos → 每次尝试（timeout → success）")

    banner("场景 4 · 下游处理后断开连接 → 网络错误 → 重投去重")
    r = send("demo-disc-1", "order-disc", simulate="disconnect")
    row = r.json()["event_row_id"]
    delivery = wait_for(
        "chaos delivery succeeds after disconnect",
        lambda: chaos_delivery(row)["display_status"] == "succeeded"
        and chaos_delivery(row),
        timeout=20,
    )
    outcomes = [a["outcome"] for a in delivery["attempts"]]
    assert outcomes == ["network_error", "success"], outcomes
    state = client.get(f"{CHAOS}/admin/state").json()
    processed = [x for x in state["received"] if x.get("event_id") == "demo-disc-1"]
    assert [x["action"] for x in processed] == ["processed_then_disconnect", "duplicate"]
    print("✓ 第一次连接被断开（下游已处理），第二次命中幂等去重直接 200")
    print("  页面：demo-disc-1 → receiver-chaos → network_error → success")

    banner("场景 5 · 连续失败 → 死信；同一业务对象阻塞，不同对象并行")
    client.post(f"{CHAOS}/admin/mode", json={"mode": "fail"}).raise_for_status()
    r1 = send("demo-block-1", "order-blocked")
    row1 = r1.json()["event_row_id"]
    r2 = send("demo-block-2", "order-blocked")
    row2 = r2.json()["event_row_id"]
    r3 = send("demo-free-1", "order-free")
    row3 = r3.json()["event_row_id"]

    dead1 = wait_for(
        "demo-block-1 exhausts retries and dies",
        lambda: chaos_delivery(row1)["display_status"] == "dead" and chaos_delivery(row1),
        timeout=30,
    )
    assert dead1["attempt_count"] == dead1["max_attempts"]
    blocked = chaos_delivery(row2)
    assert blocked["display_status"] == "blocked" and blocked["attempt_count"] == 0
    free = wait_for(
        "different object is attempted in parallel",
        lambda: chaos_delivery(row3)["attempt_count"] > 0 and chaos_delivery(row3),
    )
    print(f"✓ demo-block-1 连续失败 {dead1['attempt_count']} 次后进入死信")
    print("✓ demo-block-2（同一对象）显示「被前序阻塞」，0 次尝试")
    print(f"✓ demo-free-1（不同对象）不受阻塞，已尝试 {free['attempt_count']} 次")
    print("  页面：死信区能看到 demo-block-1，并提示阻塞了 1 条后续事件")

    banner("场景 6 · 修复下游 → 手动重投（沿用原投递编号）→ 队列按序恢复")
    client.post(f"{CHAOS}/admin/mode", json={"mode": "ok"}).raise_for_status()
    delivery_id = dead1["id"]
    replay = client.post(f"{API}/api/deliveries/{delivery_id}/replay")
    assert replay.status_code == 200, replay.text
    ok1 = wait_for(
        "replayed delivery succeeds",
        lambda: chaos_delivery(row1)["display_status"] == "succeeded" and chaos_delivery(row1),
    )
    ok2 = wait_for(
        "blocked follower drains in order",
        lambda: chaos_delivery(row2)["display_status"] == "succeeded" and chaos_delivery(row2),
    )
    assert ok1["id"] == delivery_id, "manual replay must reuse the delivery id"
    assert ok1["attempts"][-1]["manual"] is True
    state = client.get(f"{CHAOS}/admin/state").json()
    seq = [
        (x["seq"], x["event_id"])
        for x in state["received"]
        if x.get("event_id") in {"demo-block-1", "demo-block-2"} and x["action"] != "failed"
    ]
    assert seq[-2][1] == "demo-block-1" and seq[-1][1] == "demo-block-2", seq
    print(f"✓ 重投沿用投递编号 {delivery_id[:20]}…，最后一次尝试标记为手动")
    print(f"✓ 下游实际接收顺序：{[e for _, e in seq]}（先 block-1 后 block-2）")

    # demo-free-1 also died while the receiver was failing; replay it too.
    free_dead = chaos_delivery(row3)
    if free_dead["display_status"] == "dead":
        client.post(f"{API}/api/deliveries/{free_dead['id']}/replay").raise_for_status()
        wait_for(
            "demo-free-1 recovers",
            lambda: chaos_delivery(row3)["display_status"] == "succeeded",
        )
        print("✓ demo-free-1 的死信也已重投成功")

    banner("全部场景完成")
    stats = client.get(f"{API}/api/overview").json()["stats"]
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print("\n打开 http://127.0.0.1:8000 核对：")
    print("  · 收件箱：每条请求的验签结果、原文、重复/冲突记录")
    print("  · 事件详情：每个下游的投递进度与每一次尝试")
    print("  · 死信区：失败原因、阻塞的后续事件、手动重投")
    print("\n可选：重启后端（Ctrl+C 后重新启动 uvicorn），页面上的投递记录不丢失；")
    print("      若在投递途中重启，被中断的尝试会标记为「重启中断」并自动重投。")


if __name__ == "__main__":
    main()
