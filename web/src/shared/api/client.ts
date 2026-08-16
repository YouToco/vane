// 统一 fetch 封装。
// 为什么集中封装：所有请求都要带 cookie（credentials:'include'），且 401 需要统一踢回登录页，
// 分散在各页面写会漏；错误统一转成后端约定的 {"error":"人话"} 文案，页面只管展示。

import { apiBase } from "./base";

// PLATFORM_OWNER_TENANT_ID 必须与后端 types.SingleTenantID 保持一致：
// 前端目前只能用 tenant_id==1 决定是否显示管理面入口；后端还会重新证明
// exact owner membership。前端判断只影响可见性，真正授权始终在服务端。
export const PLATFORM_OWNER_TENANT_ID = 1;

export interface MeResponse {
  ok: boolean;
  user_id: number;
  tenant_id: number;
  /** 用户邮箱，供界面用户块展示；无邮箱的存量飞书用户为空串（后端 COALESCE 保证）。 */
  email?: string;
  role: "owner" | "admin" | "member";
  actor_type: "user" | "service_account";
  workspaces?: WorkspaceSummary[];
}

export interface WorkspaceSummary {
  id: number;
  name: string;
  kind: "personal" | "team";
  status: "active" | "suspended" | "deleting";
  plan: string;
  seat_limit: number;
  member_count: number;
  role: "owner" | "admin" | "member";
  created_at: string;
  updated_at: string;
}

export type WorkspaceRole = "owner" | "admin" | "member";

export interface WorkspaceMember {
  tenant_id: number;
  user_id: number;
  email: string;
  name: string;
  role: WorkspaceRole;
  joined_at: string;
}

export interface WorkspaceInvite {
  id: number;
  tenant_id: number;
  email: string;
  role: Exclude<WorkspaceRole, "owner">;
  issued_by: number;
  expires_at: string;
  consumed_by?: number;
  consumed_at?: string;
  revoked_at?: string;
  created_at: string;
  /** Returned exactly once by POST; list responses never contain it. */
  token?: string;
}

export type A2AScope = "assistant.chat" | "content.query";
export type A2AActorType = "user" | "service_account";

export interface A2AAccessToken {
  id: string;
  tenant_id: number;
  principal_user_id: number;
  actor_type: A2AActorType;
  service_account_label?: string;
  scopes: A2AScope[];
  issued_by: number;
  expires_at: string;
  revoked_at?: string;
  created_at: string;
  /** Returned exactly once by POST; list responses must never contain it. */
  token?: string;
}

export interface IssueA2AAccessTokenRequest {
  actor_type: A2AActorType;
  principal_user_id: number;
  service_account_label?: string;
  scopes: A2AScope[];
  expires_in_days: number;
}

// ---- 平台管理：指定用户/任务/运行的真实执行轨迹 ----

export interface AdminTraceUser {
  tenant_id: number;
  user_id: number;
  name: string;
  email: string;
  task_count: number;
}

export interface AdminTraceTask {
  task_id: string;
  title: string;
  status: string;
  run_count: number;
  last_run_at?: string;
}

export interface AdminTraceRun {
  snapshot_id: number;
  schema_version: string;
  status: string;
  result: string;
  source_coverage: string;
  processing: string;
  failure_code: string;
  failure_message: string;
  created_at: string;
  finalized_at?: string;
  model_calls: number;
  tool_calls: number;
}

export interface AdminTraceEvent {
  kind: "model" | "tool";
  created_at: string;
  span_name?: string;
  provider?: string;
  model?: string;
  system_prompt?: string;
  user_prompt?: string;
  completion?: string;
  prompt_tokens?: number;
  completion_tokens?: number;
  latency_ms?: number;
  temperature?: number;
  max_tokens?: number;
  tool_name?: string;
  tool_kind?: string;
  endpoint_path?: string;
  arguments?: unknown;
  result_preview?: string;
  result_size?: number;
  result_truncated?: boolean;
  http_status?: number;
  duration_ms?: number;
  error_type?: string;
  error?: string;
  pricing_status?: string;
  cost_amount?: number;
  cost_currency?: string;
}

export interface AdminExecutionTrace {
  run: AdminTraceRun;
  events: AdminTraceEvent[];
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

export interface TelegramStatus {
  enabled: boolean;
  ready: boolean;
  bound: boolean;
  bot_id?: number;
  bot_username?: string;
  webhook_url?: string;
  pending_update_count?: number;
  last_error_code?: string;
  blocked_reply_count?: number;
  oldest_blocked_at?: string;
  bound_at?: string;
  routes?: TelegramRoute[];
}

export interface TelegramRoute {
  id: number;
  kind: "private" | "group" | "topic" | "channel";
  chat_type: "private" | "group" | "supergroup" | "channel";
  bound_at: string;
}

export interface TelegramLink {
  deep_link: string;
  command?: string;
  expires_at: string;
}

export type DeliveryChannelSelection = "feishu" | "telegram" | "both";

export interface DeliveryChannelPreference {
  selection: DeliveryChannelSelection;
  scope: "default" | "account" | "task";
  task_id?: string;
  telegram_route_id?: number;
  explicit: boolean;
  updated_at?: string;
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

export interface CredentialStatus {
  configured: boolean;
  vault_ready: boolean;
  generation?: number;
  fingerprint?: string;
  metadata?: Record<string, unknown>;
  created_at?: string;
}

export interface TelegramCredentialInput {
  bot_token: string;
}

export interface LLMCredentialInput {
  provider: "deepseek";
  base_url: string;
  api_key: string;
  model: string;
  agent_provider: "" | "deepseek" | "kimi";
  agent_base_url: string;
  agent_api_key: string;
  agent_model: string;
  research_model: string;
  max_concurrent: number;
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
    kind?:
      "schedule_interval" | "rolling_duration" | "calendar_period" | string;
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
    qualification?:
      "official_announcement" | "general_availability" | "either" | string;
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
  delivery_channel?: DeliveryChannelPreference;
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
export type BatchExitGate =
  "" | "fetch" | "dedup" | "score" | "select" | "cardgen";

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

export interface TaskBriefStructuredClaim {
  text: string;
  excerpt: string;
  source_refs: string[];
}

export interface TaskBriefStructuredInsight {
  schema_version: "vane.cardgen-insight/v1";
  body_md: string;
  what_changed: string;
  why_it_matters: string;
  importance_reason: string;
  claims: TaskBriefStructuredClaim[];
}

export interface TaskBriefEvidenceSource {
  ref: string;
  title: string;
  source_title: string;
  platform: string;
  source_url: string;
  published_at?: string;
  discovered_at: string;
}

export interface TaskBriefEventEvidence {
  schema_version: "vane.structured-event-evidence/v1";
  sources: TaskBriefEvidenceSource[];
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
  structured?: TaskBriefStructuredInsight;
  event_evidence?: TaskBriefEventEvidence;
  feedback: TaskBriefFeedbackState;
}

export type ExecutiveDecisionState =
  "act" | "watch" | "no_action" | "insufficient_evidence";

export interface ExecutiveEvidenceRef {
  brief_id?: number;
  insight_id: number;
  claim_indexes: number[];
}

export interface ExecutiveSignal {
  kind: "opportunity" | "risk" | "change" | "trend";
  lifecycle?: "new" | "persistent" | "intensified" | "faded";
  title: string;
  summary: string;
  evidence_refs: ExecutiveEvidenceRef[];
}

export interface ExecutiveNextStep {
  kind: "deep_dive" | "monitor" | "edit_task" | "create_task";
  label: string;
  rationale: string;
  evidence_refs: ExecutiveEvidenceRef[];
}

export interface ExecutiveContent {
  headline: string;
  executive_summary: string;
  decision_state: ExecutiveDecisionState;
  why_for_you: string;
  signals: ExecutiveSignal[];
  next_steps: ExecutiveNextStep[];
}

function normalizeExecutiveContent(
  content: ExecutiveContent,
): ExecutiveContent {
  return {
    ...content,
    signals: arr(content.signals),
    next_steps: arr(content.next_steps),
  };
}

export interface ExecutiveBriefArtifact {
  generation_mode: "model" | "deterministic_fallback";
  processing: CanonicalCompleteness;
  generated_at: string;
  content: ExecutiveContent;
}

export interface TaskBrief {
  id: number;
  push_batch_id: number;
  generated_at: string;
  source_coverage: CanonicalCompleteness;
  processing: CanonicalCompleteness;
  insights: TaskBriefInsight[];
  executive?: ExecutiveBriefArtifact;
}

export interface TaskLatestCheck {
  finalized_at: string;
  result: CanonicalRunResult;
  source_coverage: CanonicalCompleteness;
  processing: CanonicalCompleteness;
  failure_code?: string;
}

export type TaskHealthState = "healthy" | "attention" | "waiting" | "never_run";
export type TaskHealthIssue =
  | "coverage_incomplete"
  | "acquisition_unavailable"
  | "quota_paused"
  | "model_temporarily_unavailable"
  | "delivery_failed"
  | "check_interrupted"
  | "check_failed";
export type TaskHealthAction =
  | "wait_for_retry"
  | "review_task"
  | "review_usage"
  | "review_delivery"
  | "run_again"
  | "contact_support";
export type CostCoverage =
  | "none"
  | "llm_only"
  | "tools_only"
  | "tools_partial"
  | "llm_and_tools"
  | "llm_and_tools_partial";
export type AcquisitionFailure =
  "timeout" | "provider_error" | "invalid_request" | "usage_limit" | "internal";
export type BudgetState =
  "not_configured" | "ok" | "warning" | "exhausted" | "incomplete";

export interface TaskHealthProjection {
  schema_version: "vane.task-health/v1";
  state: TaskHealthState;
  issue?: TaskHealthIssue;
  recommended_action?: TaskHealthAction;
  last_checked_at?: string;
  acquisition: {
    total: number;
    failing: number;
    max_fail_count: number;
    failure_reason?: AcquisitionFailure;
  };
  usage?: {
    known_cost_usd: number;
    known_costs: CurrencyCost[];
    coverage: CostCoverage;
    llm_calls?: number;
    llm_priced_calls?: number;
    llm_estimated_calls?: number;
    tool_calls?: number;
    tool_priced_calls?: number;
    tool_estimated_calls?: number;
    prompt_tokens?: number;
    prompt_cache_hit_tokens?: number;
    prompt_cache_miss_tokens?: number;
    completion_tokens?: number;
    reasoning_tokens?: number;
    window_start?: string;
    window_end?: string;
    budget_usd?: number;
    budget_state: BudgetState;
  };
  permissions: {
    role: "owner" | "admin" | "member" | "";
    can_run: boolean;
    can_pause: boolean;
    can_edit: boolean;
    can_delete: boolean;
    can_view_usage: boolean;
  };
}

export interface CurrencyCost {
  currency: "USD" | "CNY";
  amount: number;
}

export interface TaskBriefsResp {
  items: TaskBrief[];
  total: number;
  next_page_token?: string;
  latest_check?: TaskLatestCheck;
  health?: TaskHealthProjection;
}

export interface PeriodicBriefReport {
  id: number;
  cadence: "daily" | "weekly" | "monthly";
  timezone: string;
  period_start: string;
  period_end: string;
  generated_at: string;
  generation_mode: "model" | "deterministic_fallback";
  source_coverage: CanonicalCompleteness;
  processing: CanonicalCompleteness;
  content: ExecutiveContent;
}

export interface PeriodicBriefReportsResp {
  items: PeriodicBriefReport[];
  next_cursor?: string;
}

export interface BriefReportSettings {
  mode: "auto" | "manual";
  cadence: "daily" | "weekly" | "monthly";
  delivery: "important" | "always" | "web_only";
  timezone: string;
  updated_at?: string;
}

export interface BriefFollowupResponse {
  reply: string;
}

export interface BriefDeepDiveResponse {
  message: string;
  accepted: boolean;
}

export interface GroundedEvidenceBrief {
  brief_id: number;
  insights: TaskBriefInsight[];
}

export interface GroundedBriefContext {
  kind: "brief" | "report";
  id: number;
  cadence?: "daily" | "weekly" | "monthly";
  period_start?: string;
  period_end?: string;
  source_coverage: CanonicalCompleteness;
  processing: CanonicalCompleteness;
  generation_mode: "model" | "deterministic_fallback";
  content: ExecutiveContent;
  evidence: GroundedEvidenceBrief[];
}

// 任务累计成本。模型与取材分别通过 push trace 和 immutable run snapshot
// 精确归因；tool_priced_calls < tool_calls 时取材金额只是已知下限。
export interface ScheduleRunCost {
  llm_cost_usd: number;
  llm_calls: number;
  llm_priced_calls: number;
  llm_estimated_calls: number;
  prompt_tokens: number;
  prompt_cache_hit_tokens: number;
  prompt_cache_miss_tokens: number;
  completion_tokens: number;
  reasoning_tokens: number;
  tool_cost_usd: number;
  tool_calls: number;
  tool_priced_calls: number;
  tool_estimated_calls: number;
  known_costs: CurrencyCost[];
  latest_acquisition_calls: number;
  latest_acquisition_failures: number;
}

export type ProviderPriceMeter = "llm_tokens" | "request";
export type ProviderPriceCurrency = "USD" | "CNY";

export interface ProviderPriceRule {
  id: number;
  provider: string;
  resource: string;
  meter: ProviderPriceMeter;
  currency: ProviderPriceCurrency;
  input_cache_hit_per_million?: number;
  input_cache_miss_per_million?: number;
  output_per_million?: number;
  request_unit_price?: number;
  request_included_quantity?: number;
  request_additional_unit_price?: number;
  effective_from: string;
  effective_to?: string;
  source_url: string;
  note: string;
  created_by?: number;
  created_at: string;
}

export interface ReplaceProviderPriceRule {
  provider: string;
  resource: string;
  meter: ProviderPriceMeter;
  currency: ProviderPriceCurrency;
  input_cache_hit_per_million?: number;
  input_cache_miss_per_million?: number;
  output_per_million?: number;
  request_unit_price?: number;
  request_included_quantity?: number;
  request_additional_unit_price?: number;
  source_url: string;
  note: string;
}

export type CallCostKind = "llm" | "tool";
export type CallCostPricingStatus =
  | "provider_reported"
  | "calculated"
  | "estimated"
  | "unpriced"
  | "legacy";

export interface CallCostLLMUsage {
  prompt_tokens: number;
  prompt_cache_hit_tokens?: number;
  prompt_cache_miss_tokens?: number;
  completion_tokens: number;
  reasoning_tokens?: number;
}

export interface CallCostToolUsage {
  tool_name: string;
  tool_kind: string;
  endpoint_path?: string;
  usage_quantity: number;
  http_status?: number;
}

export interface CallCostLedgerItem {
  kind: CallCostKind;
  id: number;
  created_at: string;
  provider: string;
  resource: string;
  meter: ProviderPriceMeter;
  pricing_status: CallCostPricingStatus;
  cost_amount?: number;
  cost_currency?: ProviderPriceCurrency;
  pricing_rule?: ProviderPriceRule;
  llm_usage?: CallCostLLMUsage;
  tool_usage?: CallCostToolUsage;
  trace_id: string;
  task_id?: string;
  task_title?: string;
  span_name?: string;
  duration_ms: number;
  failed: boolean;
  error_type?: string;
}

export interface CallCostLedgerFilters {
  kind?: CallCostKind;
  provider?: string;
  pricing_status?: CallCostPricingStatus;
  task_id?: string;
}

export interface CallCostLedgerResponse {
  items: CallCostLedgerItem[];
  next_page_token?: string;
}

export interface SchedulePlaybook {
  content: string;
  updated_at: string; // UTC
}

// GET /api/schedules/{id} 响应。playbook 缺席 = 老任务/无手册，不是错误。
export interface ScheduleDetail {
  schedule: Schedule;
  delivery_channel: DeliveryChannelPreference;
  capabilities: {
    definition_edit: boolean;
  };
  summary: ScheduleRunSummary;
  playbook?: SchedulePlaybook;
  cost: ScheduleRunCost;
}

// ---- Web 原生任务控制面（功能 6.8）----

export interface TaskActionResult {
  message: string;
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
  "evidence" | "manual" | "source_unavailable";

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
  // Phase C authority. Older backends may omit these fields during the
  // ordered rollout; callers must fail closed instead of guessing epoch 0.
  profile_epoch?: number;
  restore_allowed?: boolean;
  claims: ProfileClaim[];
  events: ProfileClaimEvent[];
  events_has_more?: boolean;
  events_next_cursor?: string;
}

type ProfileClaimActionAuthority = {
  expected_epoch: number;
  expected_version: number;
};

export type ProfileClaimActionRequest = ProfileClaimActionAuthority &
  (
    | {
        action: "correct";
        claim_id: string;
        value: string;
      }
    | {
        action: "suppress" | "pin";
        claim_id: string;
      }
    | {
        action: "revoke";
        event_id: string;
      }
  );

export interface ProfileClaimActionResponse {
  version: number;
  event_id: string;
  profile: Profile;
  claims: ProfileClaim[];
  claims_complete?: boolean;
}

export type ProfileEpochActionRequest =
  | {
      expected_epoch: number;
      expected_version: number;
      action: "reset";
      scope: "history_learning";
    }
  | {
      expected_epoch: number;
      expected_version: number;
      action: "restore";
    };

export interface ProfileEpochActionResponse {
  action: "reset" | "restore";
  profile_epoch: number;
  version: number;
  event_id: string;
  profile: Profile;
  // This is server authority, not a condition the browser may derive from
  // timestamps, empty claims, or the action that just completed.
  restore_allowed: boolean;
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

// API 基址：静态托管（OSS+CDN 国内线 / Pages 境外线）与后端不同源；
// 生产值是公开拓扑合同，必须进入确定性制品，不依赖发布机环境变量。
// 本地 dev 留空走 Vite 代理（同源相对路径，vite.config.ts）。
const API_BASE = apiBase(import.meta.env.DEV);

export function getA2AEndpoint(): string {
  const base = API_BASE || (typeof location === "undefined" ? "" : location.origin);
  return base ? new URL("/a2a", base).toString() : "/a2a";
}

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
  if (
    res.status === 401 &&
    path !== "/api/auth/login" &&
    path !== "/api/auth/register" &&
    path !== "/api/auth/me"
  ) {
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
    headers:
      body !== undefined ? { "Content-Type": "application/json" } : undefined,
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
    nl_description:
      typeof raw.nl_description === "string" ? raw.nl_description : "",
    spec: asObject<ScheduleSpec>(raw.spec ?? raw.spec_json),
    scope: asObject<PushScope>(raw.scope ?? raw.scope_json),
    status: typeof raw.status === "string" ? raw.status : "active",
    next_run_state: nextRunState,
    ...(nextRunState === "scheduled" && nextRun ? { next_run: nextRun } : {}),
    created_at: raw.created_at as string | undefined,
    delivery_channel: raw.delivery_channel as DeliveryChannelPreference | undefined,
  };
}

type RawScheduleDetail = Omit<ScheduleDetail, "schedule" | "capabilities"> & {
  schedule: Record<string, unknown>;
  capabilities?: unknown;
};

export function normalizeScheduleDetail(
  raw: RawScheduleDetail,
): ScheduleDetail {
  const capabilities = asObject<Record<string, unknown>>(raw.capabilities);
  return {
    ...raw,
    schedule: normalizeSchedule(raw.schedule),
    capabilities: {
      // Legacy/malformed responses must not expose an edit control which the
      // currently deployed backend may be unable to honor.
      definition_edit: capabilities.definition_edit === true,
    },
  };
}

const taskHealthStates = new Set<TaskHealthState>([
  "healthy",
  "attention",
  "waiting",
  "never_run",
]);
const taskHealthIssues = new Set<TaskHealthIssue>([
  "coverage_incomplete",
  "acquisition_unavailable",
  "quota_paused",
  "model_temporarily_unavailable",
  "delivery_failed",
  "check_interrupted",
  "check_failed",
]);
const taskHealthActions = new Set<TaskHealthAction>([
  "wait_for_retry",
  "review_task",
  "review_usage",
  "review_delivery",
  "run_again",
  "contact_support",
]);
const taskHealthCostCoverage = new Set<CostCoverage>([
  "none",
  "llm_only",
  "tools_only",
  "tools_partial",
  "llm_and_tools",
  "llm_and_tools_partial",
]);
const taskHealthAcquisitionFailures = new Set<AcquisitionFailure>([
  "timeout",
  "provider_error",
  "invalid_request",
  "usage_limit",
  "internal",
]);
const taskHealthBudgetStates = new Set<BudgetState>([
  "not_configured",
  "ok",
  "warning",
  "exhausted",
  "incomplete",
]);

export function normalizeTaskHealth(
  raw: unknown,
): TaskHealthProjection | undefined {
  const value = asObject<Record<string, unknown>>(raw);
  const state = value.state as TaskHealthState;
  const issue = value.issue as TaskHealthIssue | undefined;
  const action = value.recommended_action as TaskHealthAction | undefined;
  const acquisition = asObject<Record<string, unknown>>(value.acquisition);
  const acquisitionFailure = acquisition.failure_reason as
    AcquisitionFailure | undefined;
  const permissions = asObject<Record<string, unknown>>(value.permissions);
  const role = permissions.role;
  const permissionKeys = [
    "can_run",
    "can_pause",
    "can_edit",
    "can_delete",
    "can_view_usage",
  ] as const;
  if (
    value.schema_version !== "vane.task-health/v1" ||
    !taskHealthStates.has(state) ||
    (issue !== undefined && !taskHealthIssues.has(issue)) ||
    (action !== undefined && !taskHealthActions.has(action)) ||
    !Number.isInteger(acquisition.total) ||
    !Number.isInteger(acquisition.failing) ||
    !Number.isInteger(acquisition.max_fail_count) ||
    Number(acquisition.total) < 0 ||
    Number(acquisition.failing) < 0 ||
    Number(acquisition.failing) > Number(acquisition.total) ||
    Number(acquisition.max_fail_count) < 0 ||
    (acquisitionFailure !== undefined &&
      !taskHealthAcquisitionFailures.has(acquisitionFailure)) ||
    (Number(acquisition.failing) === 0 && acquisitionFailure !== undefined) ||
    (Number(acquisition.failing) > 0 && acquisitionFailure === undefined) ||
    !(
      role === "" ||
      role === "owner" ||
      role === "admin" ||
      role === "member"
    ) ||
    permissionKeys.some((key) => typeof permissions[key] !== "boolean")
  ) {
    return undefined;
  }
  const projected: TaskHealthProjection = {
    schema_version: "vane.task-health/v1",
    state,
    ...(issue ? { issue } : {}),
    ...(action ? { recommended_action: action } : {}),
    ...(typeof value.last_checked_at === "string"
      ? { last_checked_at: value.last_checked_at }
      : {}),
    acquisition: {
      total: Number(acquisition.total),
      failing: Number(acquisition.failing),
      max_fail_count: Number(acquisition.max_fail_count),
      ...(acquisitionFailure ? { failure_reason: acquisitionFailure } : {}),
    },
    permissions: {
      role,
      can_run: permissions.can_run as boolean,
      can_pause: permissions.can_pause as boolean,
      can_edit: permissions.can_edit as boolean,
      can_delete: permissions.can_delete as boolean,
      can_view_usage: permissions.can_view_usage as boolean,
    },
  };
  const usage = asObject<Record<string, unknown>>(value.usage);
  const coverage = usage.coverage as CostCoverage;
  const budgetState = usage.budget_state as BudgetState;
  const knownCostUSDValid =
    typeof usage.known_cost_usd === "number" &&
    Number.isFinite(usage.known_cost_usd) &&
    usage.known_cost_usd >= 0;
  const llmCallsValid =
    typeof usage.llm_calls === "number" &&
    Number.isSafeInteger(usage.llm_calls) &&
    usage.llm_calls >= 0;
  const llmPricingFieldsPresent =
    usage.llm_priced_calls !== undefined ||
    usage.llm_estimated_calls !== undefined;
  const llmPricingFieldsComplete =
    usage.llm_priced_calls !== undefined &&
    usage.llm_estimated_calls !== undefined;
  const llmPricedCalls =
    !llmPricingFieldsPresent && llmCallsValid
      ? Number(usage.llm_calls)
      : Number(usage.llm_priced_calls);
  const llmEstimatedCalls =
    !llmPricingFieldsPresent && llmCallsValid
      ? 0
      : Number(usage.llm_estimated_calls);
  const llmPricingValid =
    llmCallsValid &&
    (!llmPricingFieldsPresent || llmPricingFieldsComplete) &&
    Number.isSafeInteger(llmPricedCalls) &&
    llmPricedCalls >= 0 &&
    llmPricedCalls <= Number(usage.llm_calls) &&
    Number.isSafeInteger(llmEstimatedCalls) &&
    llmEstimatedCalls >= 0 &&
    llmPricedCalls + llmEstimatedCalls <= Number(usage.llm_calls);
  const toolCallsValid =
    typeof usage.tool_calls === "number" &&
    Number.isSafeInteger(usage.tool_calls) &&
    usage.tool_calls >= 0;
  const toolPricedCallsValid =
    typeof usage.tool_priced_calls === "number" &&
    Number.isSafeInteger(usage.tool_priced_calls) &&
    usage.tool_priced_calls >= 0 &&
    toolCallsValid &&
    usage.tool_priced_calls <= Number(usage.tool_calls);
  const toolEstimatedCalls =
    usage.tool_estimated_calls === undefined && toolPricedCallsValid
      ? 0
      : Number(usage.tool_estimated_calls);
  const toolPricingValid =
    toolPricedCallsValid &&
    Number.isSafeInteger(toolEstimatedCalls) &&
    toolEstimatedCalls >= 0 &&
    Number(usage.tool_priced_calls) + toolEstimatedCalls <=
      Number(usage.tool_calls);
  const integerUsage = (
    key:
      | "prompt_tokens"
      | "prompt_cache_hit_tokens"
      | "prompt_cache_miss_tokens"
      | "completion_tokens"
      | "reasoning_tokens",
  ): number | undefined => {
    const amount = usage[key];
    return typeof amount === "number" &&
      Number.isSafeInteger(amount) &&
      amount >= 0
      ? amount
      : undefined;
  };
  const promptTokens = integerUsage("prompt_tokens");
  const promptCacheHitTokens = integerUsage("prompt_cache_hit_tokens");
  const promptCacheMissTokens = integerUsage("prompt_cache_miss_tokens");
  const completionTokens = integerUsage("completion_tokens");
  const reasoningTokens = integerUsage("reasoning_tokens");
  const tokenValues = [
    promptTokens,
    promptCacheHitTokens,
    promptCacheMissTokens,
    completionTokens,
    reasoningTokens,
  ];
  const tokenFieldsPresent = [
    "prompt_tokens",
    "prompt_cache_hit_tokens",
    "prompt_cache_miss_tokens",
    "completion_tokens",
    "reasoning_tokens",
  ].some((key) => usage[key] !== undefined);
  const tokenShapeValid =
    !tokenFieldsPresent ||
    (tokenValues.every((amount) => amount !== undefined) &&
      promptCacheHitTokens! + promptCacheMissTokens! <= promptTokens! &&
      reasoningTokens! <= completionTokens!);
  const knownCostsPresent = usage.known_costs !== undefined;
  const knownCostsShapeValid =
    !knownCostsPresent || Array.isArray(usage.known_costs);
  const knownCostsRaw: unknown[] = Array.isArray(usage.known_costs)
    ? (usage.known_costs as unknown[])
    : [];
  const parsedKnownCosts = knownCostsRaw.flatMap((rawCost) => {
    const cost = asObject<Record<string, unknown>>(rawCost);
    return (cost.currency === "USD" || cost.currency === "CNY") &&
      typeof cost.amount === "number" &&
      Number.isFinite(cost.amount) &&
      cost.amount >= 0
      ? [
          {
            currency: cost.currency,
            amount: cost.amount,
          } satisfies CurrencyCost,
        ]
      : [];
  });
  const knownCostsValid =
    knownCostsShapeValid &&
    parsedKnownCosts.length === knownCostsRaw.length &&
    new Set(parsedKnownCosts.map((cost) => cost.currency)).size ===
      parsedKnownCosts.length;
  const knownCosts = knownCostsPresent
    ? parsedKnownCosts
    : knownCostUSDValid
      ? [
          {
            currency: "USD",
            amount: Number(usage.known_cost_usd),
          } satisfies CurrencyCost,
        ]
      : [];
  const usageCoverageCoherent =
    (coverage === "none" &&
      !llmCallsValid &&
      !toolCallsValid &&
      !toolPricedCallsValid) ||
    (coverage === "llm_only" &&
      llmPricingValid &&
      !toolCallsValid &&
      !toolPricedCallsValid) ||
    (coverage === "tools_only" &&
      !llmCallsValid &&
      toolCallsValid &&
      toolPricingValid &&
      Number(usage.tool_priced_calls) === Number(usage.tool_calls) &&
      toolEstimatedCalls === 0) ||
    (coverage === "tools_partial" &&
      !llmCallsValid &&
      toolCallsValid &&
      toolPricingValid &&
      (Number(usage.tool_priced_calls) < Number(usage.tool_calls) ||
        toolEstimatedCalls > 0)) ||
    (coverage === "llm_and_tools" &&
      llmPricingValid &&
      toolCallsValid &&
      toolPricingValid &&
      Number(usage.tool_priced_calls) === Number(usage.tool_calls) &&
      toolEstimatedCalls === 0) ||
    (coverage === "llm_and_tools_partial" &&
      llmPricingValid &&
      toolCallsValid &&
      toolPricingValid &&
      (Number(usage.tool_priced_calls) < Number(usage.tool_calls) ||
        toolEstimatedCalls > 0));
  const budgetUSDValid =
    typeof usage.budget_usd === "number" &&
    Number.isFinite(usage.budget_usd) &&
    usage.budget_usd > 0;
  const budgetStateCoherent =
    !["ok", "warning", "exhausted"].includes(budgetState) ||
    (budgetUSDValid &&
      coverage === "llm_and_tools" &&
      llmPricingValid &&
      llmPricedCalls === Number(usage.llm_calls) &&
      llmEstimatedCalls === 0 &&
      toolPricingValid &&
      Number(usage.tool_priced_calls) === Number(usage.tool_calls) &&
      toolEstimatedCalls === 0);
  if (
    projected.permissions.can_view_usage &&
    knownCostUSDValid &&
    taskHealthCostCoverage.has(coverage) &&
    taskHealthBudgetStates.has(budgetState) &&
    knownCostsValid &&
    tokenShapeValid &&
    usageCoverageCoherent &&
    budgetStateCoherent
  ) {
    projected.usage = {
      known_cost_usd: Number(usage.known_cost_usd),
      known_costs: knownCosts,
      coverage,
      budget_state: budgetState,
      ...(llmCallsValid ? { llm_calls: Number(usage.llm_calls) } : {}),
      ...(llmPricingValid ? { llm_priced_calls: llmPricedCalls } : {}),
      ...(llmPricingValid ? { llm_estimated_calls: llmEstimatedCalls } : {}),
      ...(toolCallsValid ? { tool_calls: Number(usage.tool_calls) } : {}),
      ...(toolPricedCallsValid
        ? { tool_priced_calls: Number(usage.tool_priced_calls) }
        : {}),
      ...(toolPricingValid ? { tool_estimated_calls: toolEstimatedCalls } : {}),
      ...(tokenFieldsPresent
        ? {
            prompt_tokens: promptTokens!,
            prompt_cache_hit_tokens: promptCacheHitTokens!,
            prompt_cache_miss_tokens: promptCacheMissTokens!,
            completion_tokens: completionTokens!,
            reasoning_tokens: reasoningTokens!,
          }
        : {}),
      ...(typeof usage.window_start === "string"
        ? { window_start: usage.window_start }
        : {}),
      ...(typeof usage.window_end === "string"
        ? { window_end: usage.window_end }
        : {}),
      ...(budgetUSDValid ? { budget_usd: Number(usage.budget_usd) } : {}),
    };
  }
  return projected;
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
  return {
    ...b,
    exit_gate: b.exit_gate ?? "",
    stage_counts: b.stage_counts ?? {},
  };
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
    post<{ ok: boolean; tenant_id: number }>("/api/auth/login", {
      email,
      password,
    }),

  // 注册需邀请码（后端决议 D4：邀请制是平台垫付第三方 API 成本的财务闸门）。
  register: (email: string, password: string, inviteCode: string) =>
    post<{ ok: boolean; tenant_id: number }>("/api/auth/register", {
      email,
      password,
      invite_code: inviteCode,
    }),
  logout: () => post<{ ok: boolean }>("/api/auth/logout"),
  me: () => request<MeResponse>("/api/auth/me"),
  requestEmailVerification: () =>
    post<{ ok: boolean; already_verified?: boolean }>(
      "/api/auth/email-verification/request",
      {},
    ),
  verifyEmail: (token: string) =>
    post<{ ok: boolean }>("/api/auth/email-verification/verify", { token }),
  requestPasswordReset: (email: string) =>
    post<{ ok: boolean; message: string }>("/api/auth/password-reset/request", {
      email,
    }),
  completePasswordReset: (token: string, password: string) =>
    post<{ ok: boolean }>("/api/auth/password-reset/complete", {
      token,
      password,
    }),
  reauthenticate: (password: string) =>
    post<{ ok: boolean; proof: string; expires_in: number }>(
      "/api/auth/reauth",
      { password },
    ),
  logoutAll: (proof: string) =>
    request<{ ok: boolean; revoked_sessions: number }>("/api/auth/logout-all", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Vane-Reauth-Token": proof,
      },
      body: "{}",
    }),
  switchWorkspace: (tenantID: number) =>
    post<{ ok: boolean; tenant_id: number }>(
      `/api/workspaces/${tenantID}/switch`,
      {},
    ),
  listWorkspaceMembers: (tenantID: number) =>
    request<{ members: WorkspaceMember[] }>(
      `/api/workspaces/${tenantID}/members`,
    ).then((result) => arr(result.members)),
  listWorkspaceInvites: (tenantID: number) =>
    request<{ invites: WorkspaceInvite[] }>(
      `/api/workspaces/${tenantID}/invites`,
    ).then((result) => arr(result.invites)),
  issueWorkspaceInvite: (
    tenantID: number,
    email: string,
    role: Exclude<WorkspaceRole, "owner">,
  ) =>
    post<WorkspaceInvite>(`/api/workspaces/${tenantID}/invites`, {
      email,
      role,
    }),
  revokeWorkspaceInvite: (tenantID: number, inviteID: number) =>
    request<{ ok: boolean }>(
      `/api/workspaces/${tenantID}/invites/${inviteID}`,
      { method: "DELETE" },
    ),
  updateWorkspaceMemberRole: (
    tenantID: number,
    userID: number,
    role: Exclude<WorkspaceRole, "owner">,
  ) =>
    request<{ ok: boolean }>(
      `/api/workspaces/${tenantID}/members/${userID}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ role }),
      },
    ),
  removeWorkspaceMember: (tenantID: number, userID: number) =>
    request<{ ok: boolean }>(
      `/api/workspaces/${tenantID}/members/${userID}`,
      { method: "DELETE" },
    ),
  transferWorkspaceOwnership: (tenantID: number, userID: number) =>
    post<{ ok: boolean }>(
      `/api/workspaces/${tenantID}/transfer-ownership`,
      { user_id: userID },
    ),
  listA2AAccessTokens: () =>
    request<{ tokens: A2AAccessToken[] }>("/api/a2a-tokens").then((result) =>
      arr(result.tokens).map(({ token: _rawToken, ...item }) => item),
    ),
  issueA2AAccessToken: (input: IssueA2AAccessTokenRequest, reauthProof: string) =>
    request<A2AAccessToken>("/api/a2a-tokens", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Vane-Reauth-Token": reauthProof,
      },
      body: JSON.stringify(input),
    }),
  revokeA2AAccessToken: (tokenID: string) =>
    request<{ ok: boolean }>(`/api/a2a-tokens/${encodeURIComponent(tokenID)}`, {
      method: "DELETE",
    }),
  feishuStatus: () => request<FeishuStatus>("/api/feishu/status"),
  feishuVerify: (appId: string, appSecret: string) =>
    post<VerifyResult>("/api/feishu/verify", {
      app_id: appId,
      app_secret: appSecret,
    }),
  feishuConfig: (appId: string, appSecret: string) =>
    post<ConfigResult>("/api/feishu/config", {
      app_id: appId,
      app_secret: appSecret,
    }),
  feishuTest: () => post<{ ok: boolean }>("/api/feishu/test"),
  telegramStatus: () => request<TelegramStatus>("/api/telegram/status"),
  telegramLink: () => post<TelegramLink>("/api/telegram/link"),
	telegramRouteLink: () => post<TelegramLink>("/api/telegram/routes/link"),
	telegramRouteUnlink: (id: number) =>
		request<{ ok: boolean }>(`/api/telegram/routes/${id}`, { method: "DELETE" }),
  telegramUnlink: () =>
    request<{ ok: boolean }>("/api/telegram/link", { method: "DELETE" }),
  telegramTest: () => post<{ ok: boolean }>("/api/telegram/test"),
  deliveryChannelPreference: () =>
    request<DeliveryChannelPreference>("/api/channels/delivery-preference"),
  patchDeliveryChannelPreference: (selection: DeliveryChannelSelection, telegramRouteId?: number) =>
    request<DeliveryChannelPreference>("/api/channels/delivery-preference", {
      method: "PATCH",
      body: JSON.stringify({ selection, telegram_route_id: telegramRouteId }),
    }),
  taskDeliveryChannelPreference: (id: string) =>
    request<DeliveryChannelPreference>(
      `/api/schedules/${encodeURIComponent(id)}/delivery-preference`,
    ),
  patchTaskDeliveryChannelPreference: (
    id: string,
    selection: DeliveryChannelSelection,
    telegramRouteId?: number,
  ) => request<DeliveryChannelPreference>(
    `/api/schedules/${encodeURIComponent(id)}/delivery-preference`,
    {
      method: "PATCH",
      body: JSON.stringify({ selection, telegram_route_id: telegramRouteId }),
    },
  ),
  deleteTaskDeliveryChannelPreference: (id: string) =>
    request<DeliveryChannelPreference>(
      `/api/schedules/${encodeURIComponent(id)}/delivery-preference`,
      { method: "DELETE" },
    ),
  telegramCredentialStatus: () =>
    request<CredentialStatus>("/api/channels/telegram/credentials"),
  telegramRotateCredential: (input: TelegramCredentialInput) =>
    request<CredentialStatus & { activation: "active" | "restart_required" }>(
      "/api/channels/telegram/credentials",
      { method: "PUT", body: JSON.stringify(input) },
    ),
  telegramRevokeCredential: () =>
    request<{ ok: boolean; activation?: string }>(
      "/api/channels/telegram/credentials", { method: "DELETE" },
    ),
  adminLLMCredentialStatus: () =>
    request<CredentialStatus>("/api/admin/llm/credentials"),
  adminRotateLLMCredential: (input: LLMCredentialInput) =>
    request<CredentialStatus & { activation: "restart_required" }>(
      "/api/admin/llm/credentials",
      { method: "PUT", body: JSON.stringify(input) },
    ),
  adminRevokeLLMCredential: () =>
    request<{ ok: boolean }>("/api/admin/llm/credentials", { method: "DELETE" }),

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
    request<RawScheduleDetail>(`/api/schedules/${encodeURIComponent(id)}`).then(
      normalizeScheduleDetail,
    ),
  scheduleSummaries: () =>
    request<{ items: ScheduleRunSummary[] }>("/api/schedules/summary").then(
      (r) => arr(r.items),
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
  scheduleBriefs: (id: string, pageSize?: number, pageToken?: string) => {
    const params = new URLSearchParams();
    if (pageSize) params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    const qs = params.toString();
    return request<TaskBriefsResp>(
      `/api/schedules/${encodeURIComponent(id)}/briefs${qs ? "?" + qs : ""}`,
    ).then((r) => ({
      ...r,
      health: normalizeTaskHealth(r.health),
      items: arr(r.items).map((brief) => ({
        ...brief,
        executive: brief.executive
          ? {
              ...brief.executive,
              content: normalizeExecutiveContent(brief.executive.content),
            }
          : undefined,
        insights: arr(brief.insights).map((insight) => ({
          ...insight,
          event_evidence: insight.event_evidence
            ? {
                ...insight.event_evidence,
                sources: arr(insight.event_evidence.sources),
              }
            : undefined,
          feedback: {
            preference: insight.feedback?.preference,
            misjudged: Boolean(insight.feedback?.misjudged),
            deep_dive_requested: Boolean(insight.feedback?.deep_dive_requested),
          },
        })),
      })),
    }));
  },
  scheduleReports: (
    id: string,
    cadence?: "daily" | "weekly" | "monthly",
    pageSize = 10,
    cursor?: string,
  ) => {
    const params = new URLSearchParams({ page_size: String(pageSize) });
    if (cadence) params.set("cadence", cadence);
    if (cursor) params.set("cursor", cursor);
    return request<PeriodicBriefReportsResp>(
      `/api/schedules/${encodeURIComponent(id)}/reports?${params.toString()}`,
    ).then((r) => ({
      ...r,
      items: arr(r.items).map((report) => ({
        ...report,
        content: normalizeExecutiveContent(report.content),
      })),
    }));
  },
  reportSettings: (id: string) =>
    request<BriefReportSettings>(
      `/api/schedules/${encodeURIComponent(id)}/report-settings`,
    ),
  patchReportSettings: (
    id: string,
    patch: Partial<Pick<BriefReportSettings, "mode" | "cadence" | "delivery">>,
  ) =>
    request<BriefReportSettings>(
      `/api/schedules/${encodeURIComponent(id)}/report-settings`,
      { method: "PATCH", body: JSON.stringify(patch) },
    ),
  askBrief: (scheduleID: string, briefID: number, question: string) =>
    request<BriefFollowupResponse>(
      `/api/schedules/${encodeURIComponent(scheduleID)}/briefs/${briefID}/ask`,
      { method: "POST", body: JSON.stringify({ question }) },
    ),
  askReport: (scheduleID: string, reportID: number, question: string) =>
    request<BriefFollowupResponse>(
      `/api/schedules/${encodeURIComponent(scheduleID)}/reports/${reportID}/ask`,
      { method: "POST", body: JSON.stringify({ question }) },
    ),
  deepDiveBrief: (scheduleID: string, briefID: number, insightID: number) =>
    request<BriefDeepDiveResponse>(
      `/api/schedules/${encodeURIComponent(scheduleID)}/briefs/${briefID}/deep-dive`,
      { method: "POST", body: JSON.stringify({ insight_id: insightID }) },
    ),
  deepDiveReport: (scheduleID: string, reportID: number, insightID: number) =>
    request<BriefDeepDiveResponse>(
      `/api/schedules/${encodeURIComponent(scheduleID)}/reports/${reportID}/deep-dive`,
      { method: "POST", body: JSON.stringify({ insight_id: insightID }) },
    ),
  briefGrounding: (scheduleID: string, briefID: number) =>
    request<GroundedBriefContext>(
      `/api/schedules/${encodeURIComponent(scheduleID)}/briefs/${briefID}`,
    ),
  reportGrounding: (scheduleID: string, reportID: number) =>
    request<GroundedBriefContext>(
      `/api/schedules/${encodeURIComponent(scheduleID)}/reports/${reportID}`,
    ),

  executeTaskAction: (
    text: string,
    taskId: string | undefined,
    requestId: string,
  ) =>
    post<TaskActionResult>("/api/task-actions", {
      text,
      request_id: requestId,
      ...(taskId ? { task_id: taskId } : {}),
    }),
  runScheduleNow: (id: string, idempotencyKey: string) =>
    request<{ ok: boolean }>(`/api/schedules/${encodeURIComponent(id)}/run`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
    }),
  pauseSchedule: (id: string, idempotencyKey: string) =>
    request<{ ok: boolean }>(`/api/schedules/${encodeURIComponent(id)}/pause`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
    }),
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
  // ---- M7 推送历史（功能 6.4）----
  // 键集分页：pageToken 是后端不透明游标，前端只负责原样带回。
  // items/feedbacks 用 arr 收敛 Go nil-slice 的 null（后端首页为空时 items 已保证 []，
  // 但防御性收敛零成本，与全站策略一致）。
  listDeliveries: (pageSize?: number, pageToken?: string) => {
    const params = new URLSearchParams();
    if (pageSize) params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    const qs = params.toString();
    return request<DeliveriesResp>(`/api/deliveries${qs ? "?" + qs : ""}`).then(
      (r) => ({
        ...r,
        items: arr(r.items).map((it) => ({
          ...it,
          feedbacks: arr(it.feedbacks),
        })),
      }),
    );
  },

  // ---- M7 成本与运行监控（功能 6.5）----
  // 窗口由前端固化档位给（Costs.tsx 的 WINDOW_OPTIONS），与 observability 同策略。
  runstats: (windowHours: number) =>
    request<RunstatsResp>(
      `/api/admin/runstats?window_hours=${encodeURIComponent(windowHours)}`,
    ).then((r) => ({
      ...r,
      spans: arr(r.spans),
      days: arr(r.days),
      models: arr(r.models),
    })),
  adminListProviderPrices: () =>
    request<{ rules: ProviderPriceRule[] }>("/api/admin/provider-prices").then(
      (response) => arr(response.rules),
    ),
  adminReplaceProviderPrice: (
    input: ReplaceProviderPriceRule,
    idempotencyKey: string,
  ) =>
    request<ProviderPriceRule>("/api/admin/provider-prices", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify(input),
    }),
  adminListCostCalls: (
    filters: CallCostLedgerFilters = {},
    pageToken?: string,
    pageSize = 50,
  ) => {
    const params = new URLSearchParams();
    params.set("page_size", String(pageSize));
    if (pageToken) params.set("page_token", pageToken);
    if (filters.kind) params.set("kind", filters.kind);
    if (filters.provider) params.set("provider", filters.provider);
    if (filters.pricing_status) {
      params.set("pricing_status", filters.pricing_status);
    }
    if (filters.task_id?.trim()) params.set("task_id", filters.task_id.trim());
    return request<CallCostLedgerResponse>(
      `/api/admin/cost-calls?${params.toString()}`,
    ).then((response) => ({
      ...response,
      items: arr(response.items),
    }));
  },
  adminTraceUsers: () =>
    request<{ items: AdminTraceUser[] }>(
      "/api/admin/execution-traces/users",
    ).then((response) => arr(response.items)),
  adminTraceTasks: (tenantID: number, userID: number) =>
    request<{ items: AdminTraceTask[] }>(
      `/api/admin/execution-traces/tenants/${tenantID}/users/${userID}/tasks`,
    ).then((response) => arr(response.items)),
  adminTraceRuns: (tenantID: number, userID: number, taskID: string) =>
    request<{ items: AdminTraceRun[] }>(
      `/api/admin/execution-traces/tenants/${tenantID}/users/${userID}/tasks/${encodeURIComponent(taskID)}/runs`,
    ).then((response) => arr(response.items)),
  adminExecutionTrace: (
    tenantID: number,
    userID: number,
    taskID: string,
    snapshotID: number,
  ) =>
    request<AdminExecutionTrace>(
      `/api/admin/execution-traces/tenants/${tenantID}/users/${userID}/tasks/${encodeURIComponent(taskID)}/runs/${snapshotID}`,
    ).then((response) => ({
      ...response,
      events: arr(response.events),
    })),
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
  undoProfileEdit: (
    id: string,
    expectedUpdatedAt: string,
    idempotencyKey: string,
  ) =>
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
        typeof r.events_next_cursor === "string" &&
        r.events_next_cursor.length > 0
          ? r.events_next_cursor
          : undefined,
      profile_epoch:
        Number.isSafeInteger(r.profile_epoch) && Number(r.profile_epoch) >= 0
          ? r.profile_epoch
          : undefined,
      restore_allowed:
        typeof r.restore_allowed === "boolean" ? r.restore_allowed : undefined,
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
  applyProfileEpochAction: (
    input: ProfileEpochActionRequest,
    idempotencyKey: string,
  ) =>
    request<ProfileEpochActionResponse>("/api/profile/epochs/actions", {
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
      restore_allowed: r.restore_allowed === true,
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
    request<{ invites: Invite[] }>("/api/admin/invites").then((r) =>
      arr(r.invites),
    ),
  adminCreateInvite: () => post<Invite>("/api/admin/invites"),
  adminRevokeInvite: (code: string) =>
    request<{ ok: boolean }>(`/api/admin/invites/${encodeURIComponent(code)}`, {
      method: "DELETE",
    }),
};
