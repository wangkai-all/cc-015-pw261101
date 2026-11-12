import { useCallback, useEffect, useMemo, useState } from "react";

const OUTCOME_LABELS = {
  accepted: "已接收",
  duplicate: "重复",
  conflict: "冲突",
  invalid_signature: "验签失败",
  invalid_timestamp: "时间戳失败",
  invalid_payload: "报文无效",
  unknown_source: "未知来源",
};

const OUTCOME_TONES = {
  accepted: "green",
  duplicate: "blue",
  conflict: "amber",
  invalid_signature: "red",
  invalid_timestamp: "red",
  invalid_payload: "red",
  unknown_source: "red",
};

const DELIVERY_LABELS = {
  pending: "排队中",
  retrying: "等待重试",
  delivering: "投递中",
  succeeded: "已成功",
  dead: "死信",
  blocked: "被前序阻塞",
  cancelled: "已取消",
};

const DELIVERY_TONES = {
  pending: "gray",
  retrying: "amber",
  delivering: "blue",
  succeeded: "green",
  dead: "red",
  blocked: "purple",
  cancelled: "gray",
};

const ATTEMPT_LABELS = {
  in_flight: "进行中",
  success: "成功",
  http_error: "HTTP 错误",
  timeout: "超时",
  network_error: "网络错误",
  interrupted: "重启中断",
};

const ATTEMPT_TONES = {
  in_flight: "blue",
  success: "green",
  http_error: "red",
  timeout: "amber",
  network_error: "amber",
  interrupted: "purple",
};

function fmtTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { hour12: false });
}

function fmtDuration(ms) {
  if (ms === null || ms === undefined) return "—";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

function pretty(text) {
  if (text === null || text === undefined) return "";
  if (typeof text !== "string") return JSON.stringify(text, null, 2);
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

async function getJSON(url) {
  const response = await fetch(url);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || data.message || `HTTP ${response.status}`);
  }
  return data;
}

function Badge({ tone = "gray", children, title }) {
  return (
    <span className={`badge badge-${tone}`} title={title}>
      {children}
    </span>
  );
}

function StatusBadge({ status }) {
  return (
    <Badge tone={DELIVERY_TONES[status] || "gray"}>
      {DELIVERY_LABELS[status] || status}
    </Badge>
  );
}

function AttemptBadge({ outcome }) {
  return (
    <Badge tone={ATTEMPT_TONES[outcome] || "gray"}>
      {ATTEMPT_LABELS[outcome] || outcome}
    </Badge>
  );
}

function Check({ ok, label }) {
  return (
    <span className={`check ${ok ? "check-ok" : "check-bad"}`} title={label}>
      {ok ? "✓" : "✗"} {label}
    </span>
  );
}

function StatCard({ label, value, tone = "default", hint }) {
  return (
    <div className={`stat stat-${tone}`}>
      <span className="stat-label">{label}</span>
      <span className="stat-value">{value ?? 0}</span>
      {hint ? <span className="stat-hint">{hint}</span> : null}
    </div>
  );
}

function Progress({ summary }) {
  if (!summary || summary.total === 0) {
    return <span className="muted">无匹配订阅</span>;
  }
  const pct = Math.round((summary.succeeded / summary.total) * 100);
  return (
    <div className="progress-wrap" title={`${summary.succeeded}/${summary.total} 已成功`}>
      <div className="progress-track">
        <div className="progress-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="progress-text">
        {summary.succeeded}/{summary.total}
        {summary.dead ? <span className="text-red"> · 死信 {summary.dead}</span> : null}
        {summary.open ? <span className="text-amber"> · 进行 {summary.open}</span> : null}
      </span>
    </div>
  );
}

function SectionTitle({ children, right }) {
  return (
    <div className="section-title">
      <h3>{children}</h3>
      {right}
    </div>
  );
}

function Meta({ label, children, mono = false, className = "" }) {
  return (
    <div className={`meta ${className}`}>
      <span className="meta-label">{label}</span>
      <span className={`meta-value ${mono ? "mono" : ""}`}>{children}</span>
    </div>
  );
}

function CodeBlock({ title, content, maxHeight = 260 }) {
  return (
    <div className="code-block">
      <div className="code-title">{title}</div>
      <pre style={{ maxHeight }}>{content || "—"}</pre>
    </div>
  );
}

function Attempts({ attempts }) {
  if (!attempts || attempts.length === 0) {
    return (
      <div className="attempt-empty">
        还没有尝试。若状态是「被前序阻塞」，它会等同一业务对象的前一条事件成功后再发送。
      </div>
    );
  }
  return (
    <div className="attempts">
      {attempts.map((attempt) => (
        <div className="attempt" key={`${attempt.delivery_id}-${attempt.attempt_no}`}>
          <div className="attempt-head">
            <span className="attempt-no">#{attempt.attempt_no}</span>
            <AttemptBadge outcome={attempt.outcome} />
            {attempt.manual ? <Badge tone="purple">手动重投</Badge> : null}
            <span className="attempt-time">{fmtTime(attempt.started_at)}</span>
            <span className="attempt-duration">{fmtDuration(attempt.duration_ms)}</span>
            {attempt.status_code !== null && attempt.status_code !== undefined ? (
              <span className="attempt-http">HTTP {attempt.status_code}</span>
            ) : null}
          </div>
          {attempt.error ? <div className="attempt-error">{attempt.error}</div> : null}
          {attempt.response_body ? (
            <details className="attempt-detail">
              <summary>下游响应</summary>
              <pre>{pretty(attempt.response_body)}</pre>
            </details>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function DeliveryCard({ delivery }) {
  return (
    <div className={`delivery delivery-${DELIVERY_TONES[delivery.display_status] || "gray"}`}>
      <div className="delivery-head">
        <div>
          <span className="delivery-name">{delivery.downstream}</span>
          <span className="delivery-sub">{delivery.subscription_id}</span>
        </div>
        <StatusBadge status={delivery.display_status} />
      </div>

      {delivery.blocked_by ? (
        <div className="blocked-note">
          顺序阻塞：等待前一条事件 <span className="mono">{delivery.blocked_by.event_id}</span>
          （投递状态：<span className="mono">{delivery.blocked_by.status}</span>）。同一订阅内相同业务对象严格按接收顺序发送。
        </div>
      ) : null}

      <div className="meta-grid">
        <Meta label="投递编号" mono className="meta-full">
          {delivery.id}
        </Meta>
        <Meta label="幂等编号" mono className="meta-full">
          {delivery.idempotency_key}
        </Meta>
        <Meta label="顺序号" mono>
          #{delivery.sequence}
        </Meta>
        <Meta label="业务对象" mono>
          {delivery.partition_key}
        </Meta>
        <Meta label="尝试次数">
          {delivery.attempt_count} / {delivery.max_attempts}（本轮失败 {delivery.consecutive_failures}）
        </Meta>
        <Meta label="手动重投">{delivery.manual_replay_count} 次</Meta>
        <Meta label="下次尝试">{fmtTime(delivery.next_attempt_at_iso)}</Meta>
        <Meta label="目标地址" mono className="meta-full">
          {delivery.callback_url}
        </Meta>
      </div>

      {delivery.last_error ? <div className="delivery-error">{delivery.last_error}</div> : null}

      <SectionTitle>每次尝试</SectionTitle>
      <Attempts attempts={delivery.attempts} />
    </div>
  );
}

function RawRequest({ item }) {
  return (
    <div className="raw-grid">
      <div className="raw-card">
        <div className="raw-head">请求头</div>
        <pre>{pretty(item.headers)}</pre>
      </div>
      <div className="raw-card">
        <div className="raw-head">原始请求体</div>
        <pre>{pretty(item.raw_body)}</pre>
      </div>
    </div>
  );
}

function Drawer({ item, detail, onClose }) {
  if (!item) return null;
  return (
    <div className="drawer-backdrop" onMouseDown={onClose}>
      <aside className="drawer" onMouseDown={(event) => event.stopPropagation()}>
        <header className="drawer-head">
          <div>
            <div className="drawer-kicker">收件箱记录 #{item.id}</div>
            <h2>
              <span className="mono">{item.event_id || "(无 event_id)"}</span>
              <Badge tone={OUTCOME_TONES[item.outcome] || "gray"}>
                {OUTCOME_LABELS[item.outcome] || item.outcome}
              </Badge>
            </h2>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭">
            ✕
          </button>
        </header>

        <div className="drawer-body">
          <div className="verify-row">
            <Check ok={item.signature_valid} label="HMAC 签名" />
            <Check ok={item.timestamp_valid} label="时间戳窗口" />
            <span className="muted">{fmtTime(item.received_at)}</span>
          </div>
          {item.detail ? <div className="detail-note">{item.detail}</div> : null}

          <div className="meta-grid">
            <Meta label="来源">{item.source}</Meta>
            <Meta label="event_id" mono>
              {item.event_id || "—"}
            </Meta>
            <Meta label="事件类型" mono>
              {item.event?.event_type || "—"}
            </Meta>
            <Meta label="业务对象" mono>
              {item.event?.object_key || "—"}
            </Meta>
            <Meta label="内容摘要" mono className="meta-full">
              {item.content_hash || "—"}
            </Meta>
            <Meta label="签名头" mono className="meta-full">
              {item.signature || "—"}
            </Meta>
          </div>

          <SectionTitle>原始请求</SectionTitle>
          <RawRequest item={item} />

          {item.event_row_id ? (
            <>
              <SectionTitle
                right={
                  detail ? (
                    <span className="muted">
                      事件 #{detail.event.id} · 重复收件 {detail.event.duplicate_count} 次
                    </span>
                  ) : null
                }
              >
                投递计划与进度
              </SectionTitle>
              {detail ? (
                <>
                  <CodeBlock title="事件原始报文" content={pretty(detail.event.raw_body)} />
                  {detail.deliveries.length === 0 ? (
                    <div className="empty-inline">该事件没有匹配的启用订阅。</div>
                  ) : (
                    detail.deliveries.map((delivery) => (
                      <DeliveryCard delivery={delivery} key={delivery.id} />
                    ))
                  )}
                </>
              ) : (
                <div className="empty-inline">正在加载投递进度…</div>
              )}
            </>
          ) : (
            <div className="empty-inline">该请求未生成事件，也没有下游投递计划。</div>
          )}
        </div>
      </aside>
    </div>
  );
}

function InboxView({ data, filters, setFilters, sources, onSelect }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>收件箱</h2>
          <p className="panel-desc">
            每一次到达的 HTTP 请求都会留档，包括验签失败、重复和内容冲突。绿色行表示原文与投递计划已落库后才返回成功。
          </p>
        </div>
        <div className="filters">
          <select
            value={filters.outcome}
            onChange={(event) => setFilters({ ...filters, outcome: event.target.value })}
          >
            <option value="">全部结果</option>
            {Object.entries(OUTCOME_LABELS).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
          <select
            value={filters.source}
            onChange={(event) => setFilters({ ...filters, source: event.target.value })}
          >
            <option value="">全部来源</option>
            {sources.map((source) => (
              <option key={source.id} value={source.id}>
                {source.id}
              </option>
            ))}
          </select>
          <input
            value={filters.eventId}
            onChange={(event) => setFilters({ ...filters, eventId: event.target.value })}
            placeholder="按 event_id 过滤"
            spellCheck={false}
          />
        </div>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>接收时间</th>
              <th>来源</th>
              <th>event_id / 类型</th>
              <th>业务对象</th>
              <th>验签</th>
              <th>结果</th>
              <th>投递进度</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {data.items.length === 0 ? (
              <tr>
                <td colSpan={8}>
                  <div className="empty-inline">暂无收件记录。用 scripts/send_webhook.py 发一条签名请求试试。</div>
                </td>
              </tr>
            ) : (
              data.items.map((item) => (
                <tr
                  key={item.id}
                  className={item.outcome === "accepted" ? "row-accepted" : ""}
                  onClick={() => onSelect(item)}
                >
                  <td className="time-cell">{fmtTime(item.received_at)}</td>
                  <td>
                    <span className="source-pill">{item.source}</span>
                  </td>
                  <td>
                    <div className="mono strong">{item.event_id || "—"}</div>
                    <div className="muted small">{item.event?.event_type || "—"}</div>
                  </td>
                  <td className="mono small">{item.event?.object_key || "—"}</td>
                  <td>
                    <div className="checks">
                      <Check ok={item.signature_valid} label="签名" />
                      <Check ok={item.timestamp_valid} label="时间戳" />
                    </div>
                  </td>
                  <td>
                    <Badge tone={OUTCOME_TONES[item.outcome] || "gray"}>
                      {OUTCOME_LABELS[item.outcome] || item.outcome}
                    </Badge>
                    {item.outcome === "duplicate" && item.event ? (
                      <div className="muted tiny">重复 ×{item.event.duplicate_count}</div>
                    ) : null}
                  </td>
                  <td>
                    <Progress summary={item.delivery_summary} />
                  </td>
                  <td>
                    <button className="link-button">查看 →</button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      <footer className="table-foot">
        显示 {data.items.length} / 共 {data.total} 条 · 点击任意行查看原始请求与每个下游的尝试记录
      </footer>
    </section>
  );
}

function DeadLettersView({ data, onReplay, replayingId, message }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>死信区</h2>
          <p className="panel-desc">
            临时错误按指数退避重试到上限后进入这里；永久 4xx 会立即进入。手动重投沿用原投递编号和幂等编号，新的尝试记录继续追加。
          </p>
        </div>
      </div>
      {message ? <div className="success-note">{message}</div> : null}
      {data.items.length === 0 ? (
        <div className="empty-inline">当前没有死信。</div>
      ) : (
        <div className="dead-list">
          {data.items.map((item) => (
            <article className="dead-card" key={item.id}>
              <header className="dead-head">
                <div>
                  <div className="dead-title">
                    <span className="mono strong">{item.event_id}</span>
                    <Badge tone="red">死信</Badge>
                  </div>
                  <div className="muted small">
                    {item.subscription_name} · {item.event_type || "—"} · 对象{" "}
                    <span className="mono">{item.partition_key}</span>
                  </div>
                </div>
                <button
                  className="primary-button"
                  disabled={replayingId === item.id}
                  onClick={() => onReplay(item.id)}
                >
                  {replayingId === item.id ? "重投中…" : "手动重投"}
                </button>
              </header>

              <div className="meta-grid">
                <Meta label="投递编号" mono className="meta-full">
                  {item.id}
                </Meta>
                <Meta label="幂等编号" mono className="meta-full">
                  {item.idempotency_key}
                </Meta>
                <Meta label="进入死信">{fmtTime(item.dead_at)}</Meta>
                <Meta label="尝试次数">
                  {item.attempt_count} / {item.max_attempts}（本轮失败 {item.consecutive_failures}）
                </Meta>
                <Meta label="手动重投">{item.manual_replay_count} 次</Meta>
                <Meta label="阻塞后续">{item.blocked_followers} 条</Meta>
              </div>

              {item.blocked_followers > 0 ? (
                <div className="blocked-note">
                  同一业务对象还有 {item.blocked_followers} 条后续事件停在队列里。重投成功或恢复后会按原接收顺序继续。
                </div>
              ) : null}
              {item.last_error ? <div className="delivery-error">{item.last_error}</div> : null}

              <SectionTitle>尝试历史</SectionTitle>
              <Attempts attempts={item.attempts} />
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function SubscriptionsView({ overview }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>来源与订阅</h2>
          <p className="panel-desc">
            每个来源使用独立演示密钥验签。订阅按 event_type 过滤；同一订阅内相同业务对象串行，不同对象并行。
          </p>
        </div>
      </div>

      <SectionTitle>来源</SectionTitle>
      <div className="source-grid">
        {overview.sources.map((source) => (
          <div className="source-card" key={source.id}>
            <div className="source-id mono">{source.id}</div>
            <div className="source-name">{source.name}</div>
            <Badge tone={source.active ? "green" : "gray"}>
              {source.active ? "启用" : "停用"}
            </Badge>
          </div>
        ))}
      </div>

      <SectionTitle>订阅规则</SectionTitle>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>来源</th>
              <th>下游</th>
              <th>事件过滤</th>
              <th>回调地址</th>
              <th>分区字段</th>
              <th>超时</th>
              <th>最大尝试</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            {overview.subscriptions.map((sub) => (
              <tr key={sub.id}>
                <td>
                  <span className="mono">{sub.source}</span>
                  <div className="muted tiny">{sub.source_name}</div>
                </td>
                <td>
                  <div className="strong">{sub.name}</div>
                  <div className="muted tiny mono">{sub.id}</div>
                </td>
                <td className="mono">{sub.event_type}</td>
                <td className="mono small">{sub.callback_url}</td>
                <td className="mono small">{sub.partition_key_field}</td>
                <td>{sub.timeout_seconds}s</td>
                <td>
                  {sub.max_attempts} 次
                  <div className="muted tiny">
                    退避 {sub.retry_base_seconds}s → {sub.retry_max_seconds}s 封顶
                  </div>
                </td>
                <td>
                  <Badge tone={sub.active ? "green" : "gray"}>
                    {sub.active ? "启用" : "停用"}
                  </Badge>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="notes">
        <h3>顺序与重试约定</h3>
        <ul>
          <li>排序键是 (订阅, 业务对象)。同一业务对象严格按接收序号发送，前一条未成功时后一条显示为「被前序阻塞」。</li>
          <li>不同业务对象、不同订阅互不等待；分发器并发投递。</li>
          <li>超时 / 网络错误 / 5xx / 429 会按 0.4s、0.8s、1.6s… 指数退避，封顶 8s，总尝试次数达到上限后进入死信。</li>
          <li>非 408/429 的 4xx 视为永久错误，直接进入死信。</li>
          <li>同一下游收到稳定的 <span className="mono">X-Idempotency-Key</span> 与 <span className="mono">X-Delivery-Id</span>；超时后重发不会重复执行。</li>
        </ul>
      </div>
    </section>
  );
}

export default function App() {
  const [overview, setOverview] = useState(null);
  const [inbox, setInbox] = useState({ items: [], total: 0 });
  const [dead, setDead] = useState({ items: [], total: 0 });
  const [tab, setTab] = useState("inbox");
  const [filters, setFilters] = useState({ outcome: "", source: "", eventId: "" });
  const [selected, setSelected] = useState(null);
  const [eventDetail, setEventDetail] = useState(null);
  const [auto, setAuto] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError] = useState(null);
  const [replayingId, setReplayingId] = useState(null);
  const [message, setMessage] = useState(null);

  const selectedEventRow = selected?.event_row_id;
  const selectedRequestId = selected?.id;

  const refresh = useCallback(async () => {
    try {
      const params = new URLSearchParams();
      if (filters.outcome) params.set("outcome", filters.outcome);
      if (filters.source) params.set("source", filters.source);
      if (filters.eventId.trim()) params.set("event_id", filters.eventId.trim());
      params.set("limit", "100");

      const [overviewData, inboxData, deadData] = await Promise.all([
        getJSON("/api/overview"),
        getJSON(`/api/inbox?${params.toString()}`),
        getJSON("/api/dead-letters"),
      ]);
      setOverview(overviewData);
      setInbox(inboxData);
      setDead(deadData);
      setLastUpdated(Date.now());
      setError(null);
      if (selectedEventRow) {
        const detail = await getJSON(`/api/events/${selectedEventRow}`);
        setEventDetail(detail);
      }
    } catch (err) {
      setError(err.message);
    }
  }, [filters.outcome, filters.source, filters.eventId, selectedEventRow]);

  useEffect(() => {
    refresh();
    if (!auto) return undefined;
    const timer = setInterval(refresh, 1500);
    return () => clearInterval(timer);
  }, [refresh, auto]);

  // Keep the open drawer's row object in sync with refreshed list data.
  useEffect(() => {
    if (!selectedRequestId) return;
    const fresh = inbox.items.find((item) => item.id === selectedRequestId);
    if (fresh) setSelected(fresh);
  }, [inbox, selectedRequestId]);

  const openRow = useCallback(
    async (item) => {
      setSelected(item);
      setEventDetail(null);
      if (item.event_row_id) {
        try {
          setEventDetail(await getJSON(`/api/events/${item.event_row_id}`));
        } catch (err) {
          setError(err.message);
        }
      }
    },
    [],
  );

  const replay = useCallback(
    async (deliveryId) => {
      setReplayingId(deliveryId);
      setMessage(null);
      try {
        const result = await getJSON(`/api/deliveries/${deliveryId}/replay`);
        setMessage(result.message || "已重新入队");
        await refresh();
      } catch (err) {
        setError(err.message);
      } finally {
        setReplayingId(null);
      }
    },
    [refresh],
  );

  const stats = overview?.stats;
  const openCount = stats
    ? (stats.deliveries.pending || 0) +
      (stats.deliveries.retrying || 0) +
      (stats.deliveries.delivering || 0)
    : 0;
  const rejectedCount = stats
    ? (stats.inbound.invalid_signature || 0) +
      (stats.inbound.invalid_timestamp || 0) +
      (stats.inbound.invalid_payload || 0) +
      (stats.inbound.unknown_source || 0)
    : 0;

  const sources = overview?.sources || [];
  const tabs = useMemo(
    () => [
      { id: "inbox", label: "收件箱", count: inbox.total },
      { id: "dead", label: "死信区", count: dead.total },
      { id: "subscriptions", label: "来源与订阅", count: null },
    ],
    [inbox.total, dead.total],
  );

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark">⌁</div>
          <div>
            <h1>Webhook 收件箱</h1>
            <p>收件 · 验签 · 去重 · 按对象顺序投递 · 死信重投</p>
          </div>
        </div>
        <div className="top-actions">
          {error ? <Badge tone="red">连接异常</Badge> : <Badge tone="green">服务在线</Badge>}
          <span className="muted small">
            {lastUpdated ? `更新于 ${new Date(lastUpdated).toLocaleTimeString("zh-CN", { hour12: false })}` : "加载中…"}
          </span>
          <label className="auto-toggle">
            <input type="checkbox" checked={auto} onChange={(event) => setAuto(event.target.checked)} />
            自动刷新
          </label>
          <button className="secondary-button" onClick={refresh}>
            立即刷新
          </button>
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <main>
        <section className="stats">
          <StatCard label="收件请求" value={stats?.inbound_total} hint="全部到达" />
          <StatCard label="已接收事件" value={stats?.inbound.accepted} tone="green" hint={`事件 ${stats?.events_total || 0}`} />
          <StatCard label="重复 / 冲突" value={(stats?.inbound.duplicate || 0) + (stats?.inbound.conflict || 0)} tone="amber" hint={`重复 ${stats?.inbound.duplicate || 0} · 冲突 ${stats?.inbound.conflict || 0}`} />
          <StatCard label="被拒绝请求" value={rejectedCount} tone="red" hint="验签 / 时间戳 / 报文" />
          <StatCard label="在途投递" value={openCount} hint="排队 + 重试 + 发送中" />
          <StatCard label="顺序阻塞" value={stats?.blocked} tone="purple" hint="等待同一对象前序事件" />
          <StatCard label="投递成功" value={stats?.deliveries.succeeded} tone="green" hint="下游已确认" />
          <StatCard label="死信" value={stats?.deliveries.dead} tone="red" hint="可手动重投" />
        </section>

        <nav className="tabs">
          {tabs.map((item) => (
            <button
              key={item.id}
              className={`tab ${tab === item.id ? "tab-active" : ""}`}
              onClick={() => setTab(item.id)}
            >
              {item.label}
              {item.count !== null ? <span className="tab-count">{item.count}</span> : null}
            </button>
          ))}
        </nav>

        {tab === "inbox" ? (
          <InboxView
            data={inbox}
            filters={filters}
            setFilters={setFilters}
            sources={sources}
            onSelect={openRow}
          />
        ) : null}
        {tab === "dead" ? (
          <DeadLettersView
            data={dead}
            onReplay={replay}
            replayingId={replayingId}
            message={message}
          />
        ) : null}
        {tab === "subscriptions" && overview ? (
          <SubscriptionsView overview={overview} />
        ) : null}
      </main>

      <Drawer item={selected} detail={eventDetail} onClose={() => setSelected(null)} />
    </div>
  );
}
