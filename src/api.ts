// 统一 fetch 封装。
// 为什么集中封装：所有请求都要带 cookie（credentials:'include'），且 401 需要统一踢回登录页，
// 分散在各页面写会漏；错误统一转成后端约定的 {"error":"人话"} 文案，页面只管展示。

export interface FeishuStatus {
  configured: boolean;
  connected: boolean;
  bot_name: string;
  owner_open_id: string;
  owner_name: string;
  last_error: string;
  connected_at?: string;
}

export interface VerifyResult {
  credentials_ok: boolean;
  bot_ok: boolean;
  bot_name: string;
  detail: string;
}

export interface ConfigResult {
  status: FeishuStatus;
  verify: VerifyResult;
}

// ---- M3 推送管道相关类型 ----

// PushScope 与后端 workflow.PushScope 对齐（B1）。M3 固化组件默认留空 = 该用户全部 active 订阅。
export interface PushScope {
  source_ids?: number[];
  top_n?: number;
}

// ScheduleSpec 是前端时间选择器编译出的中立频率结构（B2 spec_json / B7 ScheduleSpec）。
// 二选一：cron 表达式（每天/每周档位）或 every_seconds（自定义间隔）。绝不透传任意 cron。
export interface ScheduleSpec {
  cron?: string;
  every_seconds?: number;
  tz?: string;
}

// Schedule 对应 schedules 镜像表（B2）。GET /api/schedules 读 Postgres 镜像。
export interface Schedule {
  id: string;
  nl_description: string;
  spec: ScheduleSpec;
  scope: PushScope;
  status: string; // active/paused
  next_run?: string; // 可选：后端若从 Temporal Describe 补充下次触发时间则有，否则前端按 spec 推导展示
  created_at?: string;
}

// 加订阅请求体，与后端 addSubscriptionReq 对齐（api/subscriptions.go）。
// rss 只传 {url}（type 缺省即 rss，向后兼容）；exa/tikhub_xhs 传结构化搜索参数，
// 幂等合成键（exa://... / tikhub://...）由后端生成，前端不拼。
export type AddSubscriptionReq =
  | { url: string }
  | { type: "exa"; query: string; category?: string }
  | { type: "tikhub_xhs"; keyword: string };

// Source 直接复用后端 types.Source 的 JSON（entities.go）。
// 信源管理页只用到其中几列，其余字段忽略即可。
export interface Source {
  id: number;
  type: string;
  url: string;
  title: string;
  status: string; // active/disabled/paused
  fail_count: number;
  last_fetched_at?: string;
  next_fetch_at?: string;
  created_at?: string;
}

// ---- M5 Gate 可观测性（契约 §16）----
// 下面这批类型逐字对齐后端 json tag：probe.Report / probe.Result / probe.EvolveView
// （probe/probe.go）与 types/observability.go。字段名对不上不会报错，只会渲染成
// undefined——看板上表现为"没数据"，正是红线 6 警告的失败模式，故改名必须两边同步。

// ProbeStatus 三态（probe.Status）。yellow 不是"通过"，是"没验到"——
// 判定语义见 probe.go 里 Status 的注释，UI 的呈现纪律见 Observability.tsx。
export type ProbeStatus = "green" | "yellow" | "red";

export interface ProbeResult {
  id: string;
  name: string;
  contract_ref: string; // 契约条款号，如 "§16.1"
  status: ProbeStatus;
  summary: string;
  detail?: string; // 后端 omitempty
}

// 分数分布的一个桶，区间 [lo, hi)，末桶闭合到 100。后端恒返 10 个桶。
export interface ScoreBucket {
  lo: number;
  hi: number;
  count: number;
}

export interface ScoreTraceStat {
  trace_id: string;
  n: number;
  distinct_completions: number;
  started_at: string; // UTC
}

export interface ScoreQualityStat {
  ok_total: number;
  no_digit: number;
  empty_no_error: number;
  errored: number;
}

export interface ProfileInjectionStat {
  total: number;
  absent: number;
  present: number;
  unrecognized: number; // 恒应为 0，>0 表示探针字面量已漂移
}

export interface NegTailStat {
  total: number;
  intact: number;
  expected_tail: string; // 空 = 当前画像无负面句，探针不适用
}

export interface SpanDayCost {
  day: string; // UTC 日界，不是北京日（见 Observability.tsx 的 fmtUTCDay）
  span_name: string;
  calls: number;
  cost_usd: number;
}

export interface ModelUsage {
  model: string;
  calls: number;
  cost_usd: number;
}

// EvolveView 内嵌了 types.EvolveCallStat（Go 匿名嵌入 = JSON 平铺），
// 故 calls/errored/last_call_at 与外层字段同级，这里照平铺写。
export interface EvolveView {
  calls: number;
  errored: number;
  last_call_at?: string; // 不限窗口的最近一次演化调用；缺省 = 从未演化
  has_profile: boolean;
  profile_updated_at?: string;
  cursor: number; // profiles.last_evolved_feedback_id
  tag_count: number;
  summary_runes: number;
}

// BatchExitGate 对齐 types.BatchExitGate（enums.go）：pipeline 从哪个闸门提前退出。
// 空串 = 没有提前退出（跑到了 Push）。取值顺序即 pipeline 顺序。
// 注意闸门名 = 产出该结果的**上一步活动名**："dedup" 意为"Dedup 跑完后没剩下东西"。
export type BatchExitGate = "" | "fetch" | "dedup" | "score" | "select" | "cardgen";

// PipelineCounts 对齐 types.PipelineCounts（entities.go）：每一步**跑完之后还剩几条**。
//
// 字段全部可选，这是本类型存在的全部意义，别"顺手"补默认值：
// 后端是 *int + omitempty —— **缺席 = 这一步根本没跑，0 = 跑了、返回 0 条**。
// 二者是不同的事故。一个 `?? 0` 就把这份区分抹平：一次停在 dedup 闸门的运行会读成
// "打分跑了、一条都没打出来"（LLM 全军覆没的形状），而事实是打分压根没被调用。
// 后端专门用 *int 换来的这份区分，不要在渲染层扔掉（消费方见 Observability.tsx 的 Funnel）。
export interface PipelineCounts {
  fetched?: number;
  deduped?: number;
  scored?: number;
  selected?: number;
  cards?: number;
}

export interface PushBatchSummary {
  id: number;
  // types.BatchStatus。008 起真实终态是 done|failed|empty——empty 是"跑完了确实没东西
  // 可推"的**正常终态**，不是失败（failed 是"该推却推不出去"）。pending 仅在 Push 活动
  // 内部短暂存在，看到即异常。"pushing" 已于 008 从枚举里删除（从 001 起就是死值）。
  status: string;
  // 提前退出的闸门，空串 = 跑到了 Push。与 status=empty 恒同时出现：有 gate ⇔ 是空批次。
  exit_gate: BatchExitGate;
  // 各阶段跑完后剩余条数；字段缺席 = 那一步没跑（不是"跑了得 0"）。
  // 008 之前的历史行（stage_counts 列默认 '{}'）、以及成功推送的批次，这里全缺席。
  stage_counts: PipelineCounts;
  created_at: string; // UTC
  idempotency_key: string; // = workflow traceID，可据此关联 llm_calls
  delivery_count: number;
  sent_count: number;
  // 原始相关分极值，缺省 = 本批无投递。注意不是排序用的有效分（不落库）。
  max_score?: number;
  min_score?: number;
}

export interface ObservabilityReport {
  generated_at: string; // UTC
  window_hours: number;
  user_id: number;
  results: ProbeResult[];
  score_distribution: ScoreBucket[];
  score_traces: ScoreTraceStat[];
  quality: ScoreQualityStat;
  injection: ProfileInjectionStat;
  neg_tail: NegTailStat;
  costs: SpanDayCost[];
  models: ModelUsage[];
  evolve: EvolveView;
  batches: PushBatchSummary[];
}

// ---- M7 推送历史（功能 6.4）----
// 字段逐字对齐后端 store.DeliveryHistoryItem / api.deliveriesResp 的 json tag。

// 一条投递上的一次反馈动作。action 是后端 FeedbackAction 原文。
export interface DeliveryFeedback {
  action: string; // interested / not_interested / misjudged / deep_dive / question
  detail: string; // 文字反馈原文，按钮反馈为空串
  created_at: string; // UTC
}

// 推送历史一行：投递本体 + 内容摘要 + 全部反馈。
// title 后端已做空标题回退（正文头 200 字符），前端不需要再兜底；
// content_item 被删时 title/url 均为空串。
export interface DeliveryHistoryItem {
  id: number;
  batch_id: number;
  score: number;
  status: string; // pending / sent / failed
  sent_at?: string; // UTC；未发送时缺席
  created_at: string; // UTC
  title: string;
  url: string;
  feedbacks: DeliveryFeedback[];
}

export interface DeliveriesResp {
  items: DeliveryHistoryItem[];
  total: number;
  next_page_token?: string;
}

// ---- M7 成本与运行监控（功能 6.5）----
// 字段逐字对齐后端 store.SpanRunStat / api.runstatsResp 的 json tag。

// 一个 span 的窗口运行统计。cache_known 是缓存命中率的分母
// （prefix_cache_hit 非 NULL 的行数）——NULL 行既不算命中也不算未命中，
// 用 calls 当分母会把"不支持缓存的调用"算成未命中，凭空压低命中率。
export interface SpanRunStat {
  span_name: string;
  calls: number;
  errors: number;
  cost_usd: number;
  prompt_tokens: number;
  completion_tokens: number;
  avg_latency_ms: number;
  p95_latency_ms: number;
  cache_hits: number;
  cache_known: number;
}

export interface RunstatsResp {
  generated_at: string; // UTC
  window_hours: number;
  spans: SpanRunStat[];
  days: SpanDayCost[];
  models: ModelUsage[];
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

// API 基址：静态托管（OSS+CDN 国内线 / Pages 境外线）与后端不同源，生产构建注入
// 绝对地址（api.vane.zhuoqidev.com，后端已配 CORS + 凭证放行，vane#54）；
// 本地 dev 留空走 vite 代理（同源相对路径，vite.config.ts）。
const API_BASE: string = import.meta.env.VITE_API_BASE ?? "";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(API_BASE + path, { credentials: "include", ...init });
  } catch {
    // fetch 本身抛错说明网络层失败（后端没起/断网），统一转成人话，
    // 避免各页面还要单独处理 TypeError
    throw new ApiError(0, "网络错误，请确认后端服务在线后重试");
  }
  // 会话失效统一踢回登录页；登录接口自身的 401 要留给登录页展示"密码错误"，不能跳
  if (res.status === 401 && path !== "/api/auth/login") {
    location.hash = "#/login";
    throw new ApiError(401, "登录已失效，请重新登录");
  }
  if (!res.ok) {
    let msg = `请求失败（HTTP ${res.status}）`;
    try {
      const body = (await res.json()) as { error?: unknown };
      if (typeof body.error === "string" && body.error) msg = body.error;
    } catch {
      // 响应体不是 JSON 时保留默认文案即可
    }
    throw new ApiError(res.status, msg);
  }
  return (await res.json()) as T;
}

function post<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, {
    method: "POST",
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
}

// 后端 schedules 镜像表的 JSONB 列 tag 可能是 spec/spec_json（另一 agent owns types.Schedule），
// 且 pgx 序列化 JSONB 时可能是内联对象也可能是字符串——这里统一收敛，页面只面对稳定形状。
function asObject<T>(v: unknown): T {
  if (typeof v === "string") {
    try {
      return JSON.parse(v) as T;
    } catch {
      return {} as T;
    }
  }
  return (v ?? {}) as T;
}

function normalizeSchedule(raw: Record<string, unknown>): Schedule {
  return {
    id: String(raw.id ?? ""),
    nl_description: typeof raw.nl_description === "string" ? raw.nl_description : "",
    spec: asObject<ScheduleSpec>(raw.spec ?? raw.spec_json),
    scope: asObject<PushScope>(raw.scope ?? raw.scope_json),
    status: typeof raw.status === "string" ? raw.status : "active",
    next_run: (raw.next_run ?? raw.next_run_at) as string | undefined,
    created_at: raw.created_at as string | undefined,
  };
}

// Go 的 nil slice 序列化成 null 而不是 []：窗口内没跑过批次时 score_traces/costs/
// batches 全是 null，页面直接 .map 就白屏。与 normalizeSchedule 同一策略——
// 形状在 api 层收敛一次，页面只面对稳定数组。
function arr<T>(v: T[] | null | undefined): T[] {
  return v ?? [];
}

// 008 之前的后端不返 stage_counts 这个键，取 b.stage_counts.fetched 会直接抛 TypeError
// 打白屏（前端先于后端上线的那个窗口）。补成 {} 而不是补零：{} 解出来全是 undefined，
// 恰好就是"这些阶段没记录"的真话，与 008 历史行走同一条渲染路径（→ 显示"—"）。
// 补零就成了"漏斗各阶段都跑了、都是 0"——凭空编造一份观测结果，比白屏更坏。
// exit_gate 同理：缺席按 "" 处理 = "没有提前退出"，那正是 008 之前所有行的真实语义。
function normalizeBatch(b: PushBatchSummary): PushBatchSummary {
  return { ...b, exit_gate: b.exit_gate ?? "", stage_counts: b.stage_counts ?? {} };
}

function normalizeReport(raw: ObservabilityReport): ObservabilityReport {
  return {
    ...raw,
    results: arr(raw.results),
    score_distribution: arr(raw.score_distribution),
    score_traces: arr(raw.score_traces),
    costs: arr(raw.costs),
    models: arr(raw.models),
    batches: arr(raw.batches).map(normalizeBatch),
  };
}

export const api = {
  login: (password: string) => post<{ ok: boolean }>("/api/auth/login", { password }),
  logout: () => post<{ ok: boolean }>("/api/auth/logout"),
  me: () => request<{ ok: boolean }>("/api/auth/me"),
  feishuStatus: () => request<FeishuStatus>("/api/feishu/status"),
  feishuVerify: (appId: string, appSecret: string) =>
    post<VerifyResult>("/api/feishu/verify", { app_id: appId, app_secret: appSecret }),
  feishuConfig: (appId: string, appSecret: string) =>
    post<ConfigResult>("/api/feishu/config", { app_id: appId, app_secret: appSecret }),
  feishuTest: () => post<{ ok: boolean }>("/api/feishu/test"),

  // ---- M3 定时任务（B8）----
  listSchedules: () =>
    request<Record<string, unknown>[]>("/api/schedules").then((rows) =>
      (rows ?? []).map(normalizeSchedule),
    ),
  createSchedule: (spec: ScheduleSpec, scope: PushScope, nlDescription: string) =>
    post<{ schedule_id: string }>("/api/schedules", {
      spec,
      scope,
      nl_description: nlDescription,
    }),
  deleteSchedule: (id: string) =>
    request<{ ok: boolean }>(`/api/schedules/${encodeURIComponent(id)}`, { method: "DELETE" }),
  // push/now 的 body 是 {scope?}（B8）；固化"现在推一次"默认不带 scope = 推全部订阅
  pushNow: (scope?: PushScope) =>
    post<{ run_id: string }>("/api/push/now", scope ? { scope } : {}),

  // ---- M3 信源管理（B8）----
  listSubscriptions: () => request<Source[]>("/api/subscriptions"),
  addSubscription: (req: AddSubscriptionReq) =>
    post<{ source_id: number }>("/api/subscriptions", req),
  removeSubscription: (sourceId: number) =>
    request<{ ok: boolean }>(`/api/subscriptions/${sourceId}`, { method: "DELETE" }),

  // ---- M7 推送历史（功能 6.4）----
  // 键集分页：pageToken 是后端不透明游标，前端只负责原样带回。
  // items/feedbacks 用 arr 收敛 Go nil-slice 的 null（后端首页为空时 items 已保证 []，
  // 但防御性收敛零成本，与全站策略一致）。
  listDeliveries: (pageSize?: number, pageToken?: string) => {
    const params = new URLSearchParams();
    if (pageSize) params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    const qs = params.toString();
    return request<DeliveriesResp>(`/api/deliveries${qs ? "?" + qs : ""}`).then((r) => ({
      ...r,
      items: arr(r.items).map((it) => ({ ...it, feedbacks: arr(it.feedbacks) })),
    }));
  },

  // ---- M7 成本与运行监控（功能 6.5）----
  // 窗口由前端固化档位给（Costs.tsx 的 WINDOW_OPTIONS），与 observability 同策略。
  runstats: (windowHours: number) =>
    request<RunstatsResp>(
      `/api/admin/runstats?window_hours=${encodeURIComponent(windowHours)}`,
    ).then((r) => ({ ...r, spans: arr(r.spans), days: arr(r.days), models: arr(r.models) })),

  // ---- M5 Gate 可观测性（契约 §16）----
  // 只读端点，窗口由前端固化档位给（见 Observability.tsx 的 WINDOW_OPTIONS），
  // 不接受自由输入——window_hours 直接进后端 time.Duration 与全部聚合 SQL 的 since。
  observability: (windowHours: number) =>
    request<ObservabilityReport>(
      `/api/admin/observability?window_hours=${encodeURIComponent(windowHours)}`,
    ).then(normalizeReport),
};
