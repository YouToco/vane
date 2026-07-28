import type { AddSubscriptionRequest } from "@/lib/source-view";

// 统一 fetch 封装。
// 为什么集中封装：所有请求都要带 cookie（credentials:'include'），且 401 需要统一踢回登录页，
// 分散在各页面写会漏；错误统一转成后端约定的 {"error":"人话"} 文案，页面只管展示。

// PLATFORM_OWNER_TENANT_ID 必须与后端 types.SingleTenantID 保持一致：
// 后端 api/platformadmin.go 的 requirePlatformOwner 就是用 tenant_id==1 判定平台 owner，
// 前端据此决定是否显示管理面入口。判据同源，不另造一套角色标记——
// 前端猜错只会多显示/少显示入口，真正的拦截始终在后端那道闸门。
export const PLATFORM_OWNER_TENANT_ID = 1;

export interface MeResponse {
  ok: boolean;
  user_id: number;
  tenant_id: number;
  /** 用户邮箱，供界面用户块展示；无邮箱的存量飞书用户为空串（后端 COALESCE 保证）。 */
  email?: string;
}

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
  // Observation policy is compiled and approved with the task definition.  New
  // runtime snapshots use `observation`; `observation_policy` keeps the Web
  // client tolerant of the create-command representation during rollout.
  observation?: ObservationPolicy;
  observation_policy?: ObservationPolicy;
}

// Read-only projection of the versioned freshness/event policy. The backend
// owns validation and enforcement; optional fields deliberately let the UI
// show older policy revisions without inventing defaults.
export interface ObservationPolicy {
  schema?: string;
  mode?: "content" | "event" | string;
  window?: {
    kind?: "schedule_interval" | "rolling_duration" | "calendar_period" | string;
    rolling_duration_seconds?: number;
    calendar_period?: "day" | "week" | "month" | string;
  };
  evidence?: {
    requirement?: "official_required" | "trusted_allowed" | string;
    official_domains?: string[];
  };
  late_policy?: "strict" | "bounded" | string;
  allowed_lateness_seconds?: number;
  unknown_time?: "reject" | "deprioritize" | "allow" | string;
  event?: {
    subject?: string;
    event_kind?: string;
    qualification?: "official_announcement" | "general_availability" | "either" | string;
  };
  effective_at?: string;
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
  next_run_state: "scheduled" | "paused" | "none" | "unavailable";
  next_run?: string; // 只有 next_run_state=scheduled 时才是可信的 Temporal 下一次触发
  created_at?: string;
}

// 加订阅请求体，与后端 addSubscriptionReq 对齐（api/subscriptions.go）。
// 旧三种输入仍走兼容格式；web/contents 走当前 platform+capability+params 契约。
export type AddSubscriptionReq = AddSubscriptionRequest;

// Source 直接复用后端 types.Source 的 JSON（entities.go）。
// 信源管理页只用到其中几列，其余字段忽略即可。
export interface Source {
  id: number;
  type: string;
  platform: string;
  capability: string;
  url: string;
  title: string;
  config: Record<string, unknown> | string | null;
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
  // New issue feedback is a single `misjudged` row with a reason code. Older
  // rows omit this field and remain readable as their original action.
  reason_code?: string;
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

// ---- M7 任务数据面（功能 6.6/6.7，vane#142）----
// 字段逐字对齐后端 store/schedule_dashboard.go 与 api/task_dashboard.go 的 json tag。

// 任务运行概览：列表页密度升级与详情页头部共用。
// last_run_at 缺席 = 从未跑过批次（020 之前的历史批次不挂 schedule_id，同样不算）；
// last_status 空串与之恒同时出现。7d 窗口是后端固化口径，前端不传窗口参数。
export interface ScheduleRunSummary {
  schedule_id: string;
  last_run_at?: string; // UTC；缺席 = 无批次历史
  last_status: string; // done/failed/empty；无批次为空串
  last_exit_gate: string; // BatchExitGate；空串 = 没提前退出
  batches_7d: number;
  empty_batches_7d: number;
  sent_pushes_7d: number;
  source_count: number;
}

// 任务绑定的信源（schedule_sources 关联）。老任务（走账号级订阅）此列表为空，
// 空 ≠ 坏——详情页要给「本任务未绑定专属信源」的真话空态，别渲染成加载失败。
export interface ScheduleSourceInfo {
  id: number;
  platform: string;
  capability: string;
  url: string;
  title: string;
  status: string; // active/disabled/paused
  fail_count: number;
  last_fetched_at?: string; // UTC
}

// 单任务一次运行（push_batch）。stage_counts 语义同 PipelineCounts：
// 缺席 = 那一步没跑，见上方 PipelineCounts 的注释——这里同样不许补零。
export interface ScheduleBatchItem {
  id: number;
  status: string; // done/failed/empty
  exit_gate: BatchExitGate;
  stage_counts: PipelineCounts;
  deliveries: number; // 本批投递行数（含未发成的）
  sent: number; // 其中已发送成功的
  created_at: string; // UTC
}

export interface ScheduleBatchesResp {
  items: ScheduleBatchItem[];
  total: number;
  next_page_token?: string;
}

export type CanonicalRunResult = "content" | "quiet" | "failed" | "interrupted";
export type CanonicalCompleteness = "complete" | "partial";

export interface TaskBriefFeedbackState {
  preference?: "interested" | "not_interested";
  misjudged: boolean;
  deep_dive_requested: boolean;
}

export interface TaskBriefInsight {
  id: number;
  rank_position: number;
  title: string;
  body_md: string;
  source_title: string;
  source_url: string;
  published_at?: string;
  discovered_at: string;
  feedback: TaskBriefFeedbackState;
}

export interface TaskBrief {
  id: number;
  push_batch_id: number;
  generated_at: string;
  source_coverage: CanonicalCompleteness;
  processing: CanonicalCompleteness;
  insights: TaskBriefInsight[];
}

export interface TaskLatestCheck {
  finalized_at: string;
  result: CanonicalRunResult;
  source_coverage: CanonicalCompleteness;
  processing: CanonicalCompleteness;
  failure_code?: string;
}

export interface TaskBriefsResp {
  items: TaskBrief[];
  total: number;
  next_page_token?: string;
  latest_check?: TaskLatestCheck;
}

// 任务累计 LLM 成本。**只覆盖推送管道的 LLM 调用**：工具费（Exa/TikHub）的
// trace 锚在写入侧尚未统一，后端刻意不归集（vane#142 审查结论）——前端文案
// 必须写明「LLM 成本」，别标成「总成本」编造完整性。
export interface ScheduleRunCost {
  llm_cost_usd: number;
  llm_calls: number;
}

export interface SchedulePlaybook {
  content: string;
  updated_at: string; // UTC
}

// GET /api/schedules/{id} 响应。playbook 缺席 = 老任务/无手册，不是错误。
export interface ScheduleDetail {
  schedule: Schedule;
  capabilities: {
    definition_edit: boolean;
  };
  summary: ScheduleRunSummary;
  sources: ScheduleSourceInfo[];
  playbook?: SchedulePlaybook;
  cost: ScheduleRunCost;
}

// ---- Web 原生任务控制面（功能 6.8）----

export interface TaskActionPreview {
  id: string;
  kind: "create" | "edit";
  task_id?: string;
  summary: string;
}

export interface TaskActionProposal {
  reply: string;
  action?: TaskActionPreview;
}

export interface TaskActionStatus {
  id: string;
  kind: "create" | "edit";
  status: string;
  terminal: boolean;
  task_id?: string;
  summary?: string;
  message?: string;
  recovering?: boolean;
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

// 用户画像安全 DTO（M7 功能 6.3）。只包含用户可见字段；tenant/user、token
// 计量和演化游标不得从这个端点泄露给浏览器。
export interface Profile {
  industry: string;
  occupation: string;
  tags: string[];
  removed_tags: string[];
  summary: string;
  created_at: string;
  updated_at: string;
}

export interface UpdateProfileRequest {
  // null 表示用户尚无画像，要求后端以 CAS 语义安全首建。
  expected_updated_at: string | null;
  industry?: string;
  occupation?: string;
  tags?: string[];
}

export type EditableProfileField = "industry" | "occupation" | "tags";

export interface ProfileEditChange {
  field: EditableProfileField;
  before: string | string[] | null;
  after: string | string[] | null;
}

// 人工画像修改审计投影。changes 是面向用户的结构化 diff，不暴露内部画像字段；
// undoable 由后端按“当前最新 revision + 未撤销”权威判定，前端绝不自行推算。
export interface ProfileEdit {
  id: string;
  created_at: string;
  actor: "self";
  kind: "edit" | "undo";
  changes: ProfileEditChange[];
  undoable: boolean;
}

export interface ProfileEditsResponse {
  edits: ProfileEdit[];
}

export type ProfileClaimField = "industry" | "occupation" | "tag" | "summary";
export type ProfileClaimSourceState =
  | "evidence"
  | "manual"
  | "source_unavailable";

export interface ProfileClaimSource {
  state: ProfileClaimSourceState;
  ref_type?: string;
  ref?: string;
}

export interface ProfileClaim {
  id: string;
  field: ProfileClaimField;
  value: string;
  source: ProfileClaimSource;
  supersedes_id?: string;
  active: boolean;
  pinned: boolean;
  created_at: string;
}

export type ProfileClaimEventKind = "correct" | "suppress" | "pin" | "revoke";

export interface ProfileClaimEvent {
  id: string;
  kind: ProfileClaimEventKind;
  target_claim_id?: string;
  result_claim_id?: string;
  created_at: string;
  revoked: boolean;
  revocable: boolean;
}

export interface ProfileClaimsResponse {
  version: number;
  claims: ProfileClaim[];
  events: ProfileClaimEvent[];
  events_has_more?: boolean;
  events_next_cursor?: string;
}

export type ProfileClaimActionRequest =
  | {
      expected_version: number;
      action: "correct";
      claim_id: string;
      value: string;
    }
  | {
      expected_version: number;
      action: "suppress" | "pin";
      claim_id: string;
    }
  | {
      expected_version: number;
      action: "revoke";
      event_id: string;
    };

export interface ProfileClaimActionResponse {
  version: number;
  event_id: string;
  profile: Profile;
  claims: ProfileClaim[];
  claims_complete?: boolean;
}

// ---- 平台管理：邀请码 ----
// 字段逐字对齐后端 api/invites.go 的 DTO（vane#104 的接口文档）。这不是
// types.Invite 直出：used/expired 是**服务端按其口径算好的状态**——
// used = 用满（used_count >= max_uses，多用码部分使用时为 false）、
// expired 按服务端当前时刻算——前端不要自己从 used_count/expires_at 重算，
// 两边口径漂移时以服务端为准。
//
// omitempty 语义（别补默认值，缺席本身是信息）：
//   expires_at 缺席 = 永不过期（useradmin CLI 可发这种码；网页签发恒 7 天）；
//   used_by / used_at 是最近一次消费租户的 owner 邮箱与时刻，未消费、或
//   owner 是纯飞书用户（无邮箱）时缺席。
export interface Invite {
  code: string;
  created_at: string; // UTC
  expires_at?: string; // UTC
  max_uses: number;
  used_count: number;
  used: boolean;
  expired: boolean;
  used_by?: string;
  used_at?: string; // UTC
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
  if (res.status === 401 && path !== "/api/auth/login" && path !== "/api/auth/register" && path !== "/api/auth/me") {
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

export function normalizeSchedule(raw: Record<string, unknown>): Schedule {
  const nextRun =
    typeof (raw.next_run ?? raw.next_run_at) === "string"
      ? String(raw.next_run ?? raw.next_run_at)
      : undefined;
  const rawNextRunState = raw.next_run_state;
  const nextRunState: Schedule["next_run_state"] =
    rawNextRunState === "scheduled" && nextRun
      ? "scheduled"
      : rawNextRunState === "paused"
        ? "paused"
        : rawNextRunState === "none"
          ? "none"
          : "unavailable";
  return {
    id: String(raw.id ?? ""),
    nl_description: typeof raw.nl_description === "string" ? raw.nl_description : "",
    spec: asObject<ScheduleSpec>(raw.spec ?? raw.spec_json),
    scope: asObject<PushScope>(raw.scope ?? raw.scope_json),
    status: typeof raw.status === "string" ? raw.status : "active",
    next_run_state: nextRunState,
    ...(nextRunState === "scheduled" && nextRun
      ? { next_run: nextRun }
      : {}),
    created_at: raw.created_at as string | undefined,
  };
}

type RawScheduleDetail = Omit<
  ScheduleDetail,
  "schedule" | "sources" | "capabilities"
> & {
  schedule: Record<string, unknown>;
  sources?: ScheduleSourceInfo[] | null;
  capabilities?: unknown;
};

export function normalizeScheduleDetail(
  raw: RawScheduleDetail,
): ScheduleDetail {
  const capabilities = asObject<Record<string, unknown>>(raw.capabilities);
  return {
    ...raw,
    schedule: normalizeSchedule(raw.schedule),
    sources: arr(raw.sources),
    capabilities: {
      // Legacy/malformed responses must not expose an edit control which the
      // currently deployed backend may be unable to honor.
      definition_edit: capabilities.definition_edit === true,
    },
  };
}

// Go 的 nil slice 序列化成 null 而不是 []：窗口内没跑过批次时 score_traces/costs/
// batches 全是 null，页面直接 .map 就白屏。与 normalizeSchedule 同一策略——
// 形状在 api 层收敛一次，页面只面对稳定数组。
function arr<T>(v: T[] | null | undefined): T[] {
  return v ?? [];
}

const profileClaimSourceStates = new Set<ProfileClaimSourceState>([
  "evidence",
  "manual",
  "source_unavailable",
]);

function normalizeProfileClaim(claim: ProfileClaim): ProfileClaim {
  const sourceState: unknown = claim.source?.state;
  const source =
    typeof sourceState === "string" &&
    profileClaimSourceStates.has(sourceState as ProfileClaimSourceState)
      ? claim.source
      : { state: "source_unavailable" as const };
  return {
    ...claim,
    source,
    active: claim.active === true,
    pinned: claim.pinned === true,
  };
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
  // 认证改为邮箱+密码（后端决议 D2′，vane#73）。
  //
  // **必须与后端同批上线**：后端合并后旧的 {password} 形态会一律 401，
  // 而这个 bundle 是线上 Dashboard 的唯一入口——前后端脱节就是全量登不进去。
  login: (email: string, password: string) =>
    post<{ ok: boolean; tenant_id: number }>("/api/auth/login", { email, password }),

  // 注册需邀请码（后端决议 D4：邀请制是平台垫付第三方 API 成本的财务闸门）。
  register: (email: string, password: string, inviteCode: string) =>
    post<{ ok: boolean; tenant_id: number }>("/api/auth/register", {
      email,
      password,
      invite_code: inviteCode,
    }),
  logout: () => post<{ ok: boolean }>("/api/auth/logout"),
  me: () => request<MeResponse>("/api/auth/me"),
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
  // ---- M7 任务数据面（功能 6.6/6.7）----
  // 详情：schedule 走与 listSchedules 相同的 normalizeSchedule（后端是 spec_json/
  // scope_json 直出）；batches 的 stage_counts 是后端 json.RawMessage 直出，形状
  // 用 asObject 收敛（同 normalizeSchedule 的理由：JSONB 可能内联也可能字符串）。
  scheduleDetail: (id: string) =>
    request<RawScheduleDetail>(
      `/api/schedules/${encodeURIComponent(id)}`,
    ).then(normalizeScheduleDetail),
  scheduleSummaries: () =>
    request<{ items: ScheduleRunSummary[] }>("/api/schedules/summary").then((r) =>
      arr(r.items),
    ),
  scheduleBatches: (id: string, pageSize?: number, pageToken?: string) => {
    const params = new URLSearchParams();
    if (pageSize) params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    const qs = params.toString();
    return request<ScheduleBatchesResp>(
      `/api/schedules/${encodeURIComponent(id)}/batches${qs ? "?" + qs : ""}`,
    ).then((r) => ({
      ...r,
      items: arr(r.items).map((it) => ({
        ...it,
        exit_gate: it.exit_gate ?? "",
        stage_counts: asObject<PipelineCounts>(it.stage_counts),
      })),
    }));
  },
  // 单任务推送记录与全局推送历史（listDeliveries）同形状（后端复用 deliveriesResp），
  // 前端也复用 DeliveriesResp/DeliveryHistoryItem，渲染层两处共用组件。
  scheduleDeliveries: (id: string, pageSize?: number, pageToken?: string) => {
    const params = new URLSearchParams();
    if (pageSize) params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    const qs = params.toString();
    return request<DeliveriesResp>(
      `/api/schedules/${encodeURIComponent(id)}/deliveries${qs ? "?" + qs : ""}`,
    ).then((r) => ({
      ...r,
      items: arr(r.items).map((it) => ({ ...it, feedbacks: arr(it.feedbacks) })),
    }));
  },
  scheduleBriefs: (id: string, pageSize?: number, pageToken?: string) => {
    const params = new URLSearchParams();
    if (pageSize) params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    const qs = params.toString();
    return request<TaskBriefsResp>(
      `/api/schedules/${encodeURIComponent(id)}/briefs${qs ? "?" + qs : ""}`,
    ).then((r) => ({
      ...r,
      items: arr(r.items).map((brief) => ({
        ...brief,
        insights: arr(brief.insights).map((insight) => ({
          ...insight,
          feedback: {
            preference: insight.feedback?.preference,
            misjudged: Boolean(insight.feedback?.misjudged),
            deep_dive_requested: Boolean(
              insight.feedback?.deep_dive_requested,
            ),
          },
        })),
      })),
    }));
  },

  proposeTaskAction: (text: string, taskId: string | undefined, requestId: string) =>
    post<TaskActionProposal>("/api/task-actions/propose", {
      text,
      request_id: requestId,
      ...(taskId ? { task_id: taskId } : {}),
    }),
  confirmTaskAction: (id: string) =>
    post<{ message: string }>(
      `/api/task-actions/${encodeURIComponent(id)}/confirm`,
    ),
  cancelTaskAction: (id: string) =>
    post<{ message: string }>(
      `/api/task-actions/${encodeURIComponent(id)}/cancel`,
    ),
  taskActionStatus: (id: string) =>
    request<TaskActionStatus>(
      `/api/task-actions/${encodeURIComponent(id)}`,
    ),
  runScheduleNow: (id: string, idempotencyKey: string) =>
    request<{ ok: boolean }>(
      `/api/schedules/${encodeURIComponent(id)}/run`,
      {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKey },
      },
    ),
  pauseSchedule: (id: string, idempotencyKey: string) =>
    request<{ ok: boolean }>(
      `/api/schedules/${encodeURIComponent(id)}/pause`,
      {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKey },
      },
    ),
  resumeSchedule: (id: string, idempotencyKey: string) =>
    request<{ ok: boolean }>(
      `/api/schedules/${encodeURIComponent(id)}/resume`,
      {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKey },
      },
    ),

  deleteSchedule: (id: string, idempotencyKey: string) =>
    request<{ ok: boolean }>(`/api/schedules/${encodeURIComponent(id)}`, {
      method: "DELETE",
      headers: { "Idempotency-Key": idempotencyKey },
    }),
  // push/now 的 body 是 {scope?}（B8）；固化"现在推一次"默认不带 scope = 推全部订阅
  pushNow: (scope?: PushScope) =>
    post<{ run_id: string }>("/api/push/now", scope ? { scope } : {}),

  // ---- M3 信源管理（B8）----
  listSubscriptions: () => request<Source[]>("/api/subscriptions"),
  addSubscription: (req: AddSubscriptionReq) =>
    post<{ source_id: number }>("/api/subscriptions", req),
  removeSubscription: (sourceId: number) =>
    request<{ ok: boolean }>(`/api/subscriptions/${sourceId}`, { method: "DELETE" }),
  // 重新启用一个因连续抓取失败被自动停用的信源（功能 5.2）。后端清零 fail_count、立即恢复抓取。
  enableSource: (sourceId: number) =>
    request<{ ok: boolean }>(`/api/sources/${sourceId}/enable`, { method: "POST" }),

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

  // ---- M7 画像查看与人工修正（功能 6.3）----
  // 单次取数；tags/removed_tags 用 arr 收敛 Go nil-slice 的 null。
  // 画像未生成时后端回 404，调用方（Profile.tsx）按空态处理而非报错。
  profile: () =>
    request<Profile>("/api/profile").then((p) => ({
      ...p,
      tags: arr(p.tags),
      removed_tags: arr(p.removed_tags),
    })),
  // expected_updated_at 是乐观锁；409 必须交给页面要求用户重新加载，不能自动覆盖。
  updateProfile: (input: UpdateProfileRequest, idempotencyKey: string) =>
    request<Profile>("/api/profile", {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify(input),
    }).then((p) => ({
      ...p,
      tags: arr(p.tags),
      removed_tags: arr(p.removed_tags),
    })),
  profileEdits: (limit = 20) =>
    request<ProfileEditsResponse>(
      `/api/profile/edits?limit=${encodeURIComponent(limit)}`,
    ).then((r) => ({
      edits: arr(r.edits).map((item) => ({
        ...item,
        changes: arr(item.changes),
        undoable: item.undoable === true,
      })),
    })),
  undoProfileEdit: (id: string, expectedUpdatedAt: string, idempotencyKey: string) =>
    request<Profile>(`/api/profile/edits/${encodeURIComponent(id)}/undo`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify({ expected_updated_at: expectedUpdatedAt }),
    }).then((p) => ({
      ...p,
      tags: arr(p.tags),
      removed_tags: arr(p.removed_tags),
    })),
  // Roll out only after the backend accepts event_limit/event_cursor. The
  // explicit limit opts this UI into bounded pages while parameterless legacy
  // callers keep their pre-pagination response during the compatibility window.
  profileClaims: (eventCursor?: string) =>
    request<ProfileClaimsResponse>(
      eventCursor
        ? `/api/profile/claims?event_limit=20&event_cursor=${encodeURIComponent(eventCursor)}`
        : "/api/profile/claims?event_limit=20",
    ).then((r) => ({
      ...r,
      claims: arr(r.claims).map(normalizeProfileClaim),
      events: arr(r.events).map((event) => ({
        ...event,
        revoked: event.revoked === true,
        revocable: event.revocable === true,
      })),
      events_has_more:
        r.events_has_more === true &&
        typeof r.events_next_cursor === "string" &&
        r.events_next_cursor.length > 0,
      events_next_cursor:
        typeof r.events_next_cursor === "string" && r.events_next_cursor.length > 0
          ? r.events_next_cursor
          : undefined,
    })),
  applyProfileClaimAction: (
    input: ProfileClaimActionRequest,
    idempotencyKey: string,
  ) =>
    request<ProfileClaimActionResponse>("/api/profile/claims/actions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify(input),
    }).then((r) => ({
      ...r,
      profile: {
        ...r.profile,
        tags: arr(r.profile.tags),
        removed_tags: arr(r.profile.removed_tags),
      },
      claims: arr(r.claims).map(normalizeProfileClaim),
    })),

  // ---- 平台管理：邀请码（requirePlatformOwner 门控，非 owner 一律 404）----
  // 接口对齐 vane#104（feat/invite-admin-api）的文档，后端未合并前这三个请求
  // 会 404——Invites.tsx 按普通错误态展示即可，不需要特判。
  //   GET    /api/admin/invites          → {invites: Invite[]}，created_at 降序；
  //                                        后端保证空列表是 [] 不是 null，arr 只是零成本防御
  //   POST   /api/admin/invites → 201    → 新签发的 Invite（无请求体：一次性、7 天有效；
  //                                        多次使用/自定义有效期是运维特例，留在 useradmin CLI）
  //   DELETE /api/admin/invites/{code}   → {ok}。只删**从未使用**的码；已使用（含部分
  //                                        使用的多用码）后端 409——错误文案要如实展示，
  //                                        「以为码废了、实际还能用」比报错糟糕
  adminListInvites: () =>
    request<{ invites: Invite[] }>("/api/admin/invites").then((r) => arr(r.invites)),
  adminCreateInvite: () => post<Invite>("/api/admin/invites"),
  adminRevokeInvite: (code: string) =>
    request<{ ok: boolean }>(`/api/admin/invites/${encodeURIComponent(code)}`, {
      method: "DELETE",
    }),
};
