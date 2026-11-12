# Webhook 收件箱 · 可靠投递演示

外部系统推过来的 webhook 先进入**收件箱**，验签、去重、落库后再按订阅规则投递给下游。
全链路可观测：每条原始请求、验签结果、每个下游的投递进度和每一次尝试都能在页面上核对。
事件在返回接收成功之后不会悄悄丢失。

技术栈：FastAPI + SQLite（WAL）+ React（Vite），另附两个本地接收服务。

## 快速开始

```bash
# 1. 安装依赖
python3 -m venv .venv
.venv/bin/pip install -r backend/requirements.txt
cd frontend && npm install && npm run build && cd ..

# 2. 启动两个本地接收服务（9101 正常 / 9102 可模拟故障）
.venv/bin/python scripts/run_receivers.py

# 3. 另开终端，启动收件箱 + 分发服务（8000，同时托管页面）
.venv/bin/uvicorn backend.app.main:app --host 127.0.0.1 --port 8000

# 4. 打开页面
#    http://127.0.0.1:8000
```

一条命令跑完全部演示场景（正常投递、重复/冲突、慢响应、断连、连续失败→死信、
对象阻塞、手动重投、顺序恢复）：

```bash
.venv/bin/python scripts/demo_scenarios.py
```

重置演示数据：`pkill -f uvicorn; .venv/bin/python scripts/reset_demo.py`，然后重启后端。

## 发送一条 webhook

```bash
.venv/bin/python scripts/send_webhook.py \
    --source shop --event-id evt-1001 --event-type order.created \
    --object-id order-42 --simulate slow
```

`--simulate` 仅被 9102 的 chaos 接收服务识别：`ok`（默认）、`slow`、`fail`、`disconnect`。
也可以用 `POST http://127.0.0.1:9102/admin/mode {"mode":"fail"}` 给 chaos 服务设全局默认行为。

### 签名约定

```
X-Webhook-Timestamp: <unix 秒>
X-Webhook-Signature: sha256=<hex(HMAC-SHA256(secret, "<timestamp>.<raw-body>"))>
```

- 每个来源一把本地演示密钥：`shop` → `shop-demo-secret-2026`，`crm` → `crm-demo-secret-2026`
  （可用环境变量 `SOURCE_SHOP_SECRET` / `SOURCE_CRM_SECRET` 覆盖）。
- 签名覆盖**原始请求字节**和时间戳；时间戳允许 ±300 秒（`WEBHOOK_SIGNATURE_TOLERANCE`）。
- 验签失败 / 时间戳超窗 → 401，请求原文和失败原因仍留在收件箱里，不产生事件。

### 接收语义

- 按 `(source, event_id)` 去重：
  - 内容相同（按规范化 JSON 的内容哈希）→ `200 duplicate`，不新增投递；
  - 内容不同 → `409 conflict`，明确报冲突，不会当成普通重复吞掉。
- 合法事件：原文、headers、验签结果与**全部投递计划**在同一个 SQLite 事务里落库，
  提交成功后才返回 `202 accepted`。提交前崩溃 = 没返回成功；提交后崩溃 = 分发器从库里捡起来继续。

## 投递语义

- **顺序**：排序键是 `(订阅, 业务对象)`（默认取 payload 的 `object_id`，订阅可配
  `partition_key_field`，支持 `a.b` 路径）。同一业务对象严格按接收序号发送，
  前一条未成功时后一条等待（页面显示「被前序阻塞」）；不同对象、不同订阅并行。
- **重试**：超时 / 网络错误 / 5xx / 429 按指数退避（0.4s 起、×2、8s 封顶，
  `WEBHOOK_RETRY_BASE/MAX`），最多 5 次（`WEBHOOK_MAX_ATTEMPTS`）后进入死信；
  非 408/429 的 4xx 视为永久错误直接死信。
- **死信重投**：页面或 `POST /api/deliveries/{id}/replay` 手动重投，
  **沿用原投递编号和幂等编号**，尝试序号继续累加并标记「手动重投」；
  重投成功后，被阻塞的后续事件按原顺序自动放行。
- **幂等**：下游可能「处理了但响应超时/断连」，因此每次尝试都带稳定的
  `X-Idempotency-Key`、`X-Delivery-Id`、`X-Attempt`。两个演示接收服务都按幂等键去重，
  重试到达时直接返回成功（chaos 服务的 `/admin/state` 能看到 `duplicate`）。
- **重启恢复**：分发服务重启时，处于 `delivering` 的投递自动回到队列，
  未完成的尝试标记为「重启中断」，用同一幂等键重投，事件不丢、不重复执行。

## 页面能核对什么

- **收件箱**：每一次到达的请求（含验签失败、重复、冲突）的原文、请求头、验签/时间戳结果；
  点击行展开投递计划。
- **事件详情**：每个下游的状态（排队/重试/投递中/成功/死信/被前序阻塞）、顺序号、
  幂等编号、下一次重试时间、失败原因，以及**每一次尝试**（开始/结束/耗时/HTTP 状态/错误/响应）。
- **死信区**：失败原因、被它阻塞的后续事件数、完整尝试历史、手动重投按钮。
- **来源与订阅**：来源、订阅规则、回调地址、超时与重试参数。

## 手动验证三个关键场景

```bash
# A. 重启分发服务：发一条 slow 事件，趁 chaos 还在睡 4 秒时重启后端
.venv/bin/python scripts/send_webhook.py --event-id evt-r1 --object-id order-r --simulate slow
#    Ctrl+C 重启 uvicorn；页面上 evt-r1 的第一次尝试显示「重启中断」，第二次成功

# B. 重复发送相同事件
.venv/bin/python scripts/send_webhook.py --event-id evt-r1 --object-id order-r --simulate slow
#    收件箱出现 duplicate 记录，原事件的投递计划不变

# C. 阻塞一个业务对象
curl -X POST http://127.0.0.1:9102/admin/mode -d '{"mode":"fail"}' -H 'Content-Type: application/json'
.venv/bin/python scripts/send_webhook.py --event-id evt-b1 --object-id order-b
.venv/bin/python scripts/send_webhook.py --event-id evt-b2 --object-id order-b   # 显示「被前序阻塞」
.venv/bin/python scripts/send_webhook.py --event-id evt-f1 --object-id order-f   # 不同对象不受影响
curl -X POST http://127.0.0.1:9102/admin/mode -d '{"mode":"ok"}' -H 'Content-Type: application/json'
#    死信区手动重投 evt-b1 → evt-b1 成功后 evt-b2 自动按序送达
```

## 测试

```bash
.venv/bin/python -m pytest -q        # 21 个用例：验签/去重/冲突/顺序/退避/死信/重投/重启恢复/API
```

## 目录结构

```
backend/app/
  config.py       # 配置与演示种子数据（来源、订阅）
  db.py           # SQLite（WAL）schema、事务、崩溃恢复
  security.py     # HMAC 签名与时间戳窗口校验
  ingest.py       # 验签 → 去重/冲突 → 原文+投递计划同事务落库
  dispatcher.py   # 按对象排序的并发分发、退避重试、死信、手动重投
  receivers.py    # 两个本地接收服务（9101 正常 / 9102 可模拟故障）
  main.py         # API、页面托管、分发器生命周期
backend/tests/    # pytest 集成测试
frontend/         # React + Vite 页面（构建产物由 FastAPI 托管）
scripts/
  send_webhook.py     # 签名发送单条事件
  run_receivers.py    # 启动两个接收服务
  demo_scenarios.py   # 一键跑通全部演示场景
  reset_demo.py       # 清空演示数据
```

## 主要 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/webhooks/{source}` | 接收 webhook（验签/去重/落库） |
| GET | `/api/overview` | 统计、来源、订阅 |
| GET | `/api/inbox` | 收件箱列表（可按结果/来源/event_id 过滤） |
| GET | `/api/inbox/{id}` | 单条原始请求 |
| GET | `/api/events/{id}` | 事件 + 每个下游的投递与尝试 |
| GET | `/api/dead-letters` | 死信列表 |
| POST | `/api/deliveries/{id}/replay` | 手动重投（沿用原投递编号） |
| GET | `:9102/admin/state` | chaos 接收服务的处理/去重日志 |
| POST | `:9102/admin/mode` | 设置 chaos 默认行为 |
