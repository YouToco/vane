package types

import (
	"encoding/json"
	"time"
)

// 本文件定义 9 个领域实体，字段与 store/migrations/001_init.sql
// 的 9 张表一一对应。约定：
//   - 主键 / 外键 bigserial|BIGINT → int64；
//   - 可空时间列 → *time.Time，可空数值列 → 指针；
//   - 带 DEFAULT '' 或语义上恒有值的 TEXT 列 → string（store 层写入空串而非 NULL）；
//   - JSONB 列 → json.RawMessage（延迟解析，各消费方自行定义结构）。

// User 用户（users 表）。MVP 阶段用户即飞书用户。
type User struct {
	ID int64 `json:"id"`
	// FeishuOpenID 飞书身份。migration 019 起**可空**：邮箱注册的用户没有 open_id。
	// Postgres 的 UNIQUE 视多个 NULL 为互不相同，故多行空值不冲突。
	FeishuOpenID *string   `json:"feishu_open_id,omitempty"`
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	// Email / PasswordHash 是邮箱身份（migration 019，决议 D2′）。
	// 二者同生共死（DB 约束 ck_users_email_has_password）：有邮箱必有密码。
	// **PasswordHash 绝不可出现在任何对外响应里**——它有 json tag 仅为库内扫描方便。
	Email        *string `json:"email,omitempty"`
	PasswordHash *string `json:"-"`
	// EmailVerified 邀请制下首版恒 false（邀请码即把关），为将来接发信服务预留。
	EmailVerified bool `json:"email_verified"`
}

// Source 信源（sources 表）。
// 关键设计：next_fetch_at 为预计算的下次抓取时间（替代表达式索引），
// 抓取完成时由 store 层同时更新 last_fetched_at 与 next_fetch_at；
// 调度查询 WHERE status='active' AND next_fetch_at <= now() 命中 (status, next_fetch_at) 索引。
type Source struct {
	ID                   int64           `json:"id"`
	Platform             Platform        `json:"platform"`   // 008 起取代 Type
	Capability           Capability      `json:"capability"` // 008 起取代 Type
	URL                  string          `json:"url"`
	Title                string          `json:"title"`
	Config               json.RawMessage `json:"config"` // JSONB，按 Platform+Capability 各自定义结构
	Status               SourceStatus    `json:"status"`
	FetchIntervalSeconds int             `json:"fetch_interval_seconds"`
	NextFetchAt          time.Time       `json:"next_fetch_at"`             // NOT NULL DEFAULT now()
	LastFetchedAt        *time.Time      `json:"last_fetched_at,omitempty"` // 从未抓取过时为 NULL
	FailCount            int             `json:"fail_count"`                // 连续失败计数，达阈值自动 disabled
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

// Subscription 用户订阅关系（subscriptions 表），UNIQUE(user_id, source_id)。
type Subscription struct {
	ID        int64              `json:"id"`
	UserID    int64              `json:"user_id"`
	SourceID  int64              `json:"source_id"`
	Status    SubscriptionStatus `json:"status"`
	CreatedAt time.Time          `json:"created_at"`
}

// ContentItem 抓取到的内容条目（content_items 表），UNIQUE(canonical_key)。
// content_hash 精确去重，simhash 近似去重（72h 窗口）。
//
// 007 起内容身份全局唯一（不再 per-source）：SourceID / ExternalID 语义变为「首发源」
// ——谁最先发现这条内容；"哪些源见过这条内容"由 content_sources 关联表承载。
type ContentItem struct {
	ID         int64  `json:"id"`
	SourceID   int64  `json:"source_id"`   // 首发源（007 起语义）
	ExternalID string `json:"external_id"` // 首发源给的源内 ID（RSS guid / 平台条目 ID）
	// CanonicalKey 是内容的跨源唯一身份，由 fetcher 按平台构造（web=url、xhs=note_id、
	// x=tweet_id）。不能用单一字段通吃：BBC 更新文章会换 guid（url 稳定）、
	// 小红书 url 带每次刷新的 xsec_token（note_id 稳定），两者恰好相反。
	CanonicalKey string `json:"canonical_key"`
	Kind         Kind   `json:"kind"` // 内容种类，决定 Dedup/scorer 怎么对待它

	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Content     string     `json:"content"`
	Author      string     `json:"author"`
	PublishedAt *time.Time `json:"published_at,omitempty"` // 源未提供发布时间时为 NULL
	ContentHash string     `json:"content_hash"`           // NOT NULL
	Simhash     *int64     `json:"simhash,omitempty"`      // BIGINT，可空（未计算时为 NULL）
	FetchedAt   time.Time  `json:"fetched_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

// PipelineCounts 是一次 pipeline 的漏斗快照（push_batches.stage_counts，JSONB）：
// 每一步**跑完之后还剩几条**。它把"抓到 20 条但全被去重掉"与"压根没抓到新内容"
// 这两种在库里此前完全同形（都是零行）的结局区分开。
//
// 为什么字段是 *int 而不是 int —— 这是本类型存在的全部意义，别"顺手"改成值类型：
// nil 表示**这一步根本没跑**，0 表示**跑了，返回 0 条**。二者是不同的事故。
// 若用 int 零值，一次停在 dedup 闸门的运行会写出 scored=0，读起来是
// "打分跑了、一条都没打出来"（LLM 全军覆没的形状），而事实是打分压根没被调用。
// 那正是本 PR 要消灭的那类混淆——用零值记录它等于换个地方再造一次。
//
// 为什么用一个 JSONB 列而不是 5 个 INT 列：① 上面的"没跑 vs 跑了得 0"用列表达
// 就得 5 个 nullable INT，push_batches 会被一张漏斗表撑宽；② 阶段会变（M6 信源
// 插件化在改 pipeline），JSONB 加字段不需要 ALTER，老行读出来自然是 nil = "那时
// 没这一步"，语义正确；③ 主查询键是 exit_gate（真列），counts 只是展开细节，
// 不承担过滤职责。
//
// 与 001 "JSONB → json.RawMessage（延迟解析）" 的约定不冲突：那条针对的是
// **多态**载荷（sources.config 按 type 各自定义、pending_actions.args 是模型产出），
// 消费方各不相同故不能在 types 里定死。漏斗计数恰恰相反——形状固定且全系统唯一，
// 定成结构体才能让 store/probe/前端共用一份、编译期对齐。
type PipelineCounts struct {
	Fetched  *int `json:"fetched,omitempty"`
	Deduped  *int `json:"deduped,omitempty"`
	Scored   *int `json:"scored,omitempty"`
	Selected *int `json:"selected,omitempty"`
	Cards    *int `json:"cards,omitempty"`
}

// WithFetched 等五个方法逐级填漏斗，返回副本而非就地改。
//
// 值接收者 + 返回副本是为了在 **workflow 函数体内**安全使用（Temporal 确定性约束，
// workflow/types.go:5-8）：纯计算、无共享状态、重放逐字一致。顺带把取地址收进
// 方法内部——每次调用的形参 n 都是新变量，杜绝了循环里 &i 全指同一个的经典坑。
func (c PipelineCounts) WithFetched(n int) PipelineCounts  { c.Fetched = &n; return c }
func (c PipelineCounts) WithDeduped(n int) PipelineCounts  { c.Deduped = &n; return c }
func (c PipelineCounts) WithScored(n int) PipelineCounts   { c.Scored = &n; return c }
func (c PipelineCounts) WithSelected(n int) PipelineCounts { c.Selected = &n; return c }
func (c PipelineCounts) WithCards(n int) PipelineCounts    { c.Cards = &n; return c }

// PushBatch 一次推送决策周期（push_batches 表）。
type PushBatch struct {
	ID     int64 `json:"id"`
	UserID int64 `json:"user_id"`
	// ScheduledAt 计划推送时间，可空。**从无代码写入，恒为 NULL**：即时批次的
	// 时间锚点是 created_at，批次由 Temporal Schedule 触发时刻决定。要按时间查
	// 批次一律用 created_at（001 的 idx_push_batches_status_scheduled 同样是死索引）。
	ScheduledAt *time.Time  `json:"scheduled_at,omitempty"`
	Status      BatchStatus `json:"status"`
	// IdempotencyKey 幂等键 = workflow 的确定性 traceID（004 加列）。
	// 本字段此前一直缺失——struct 从 004 起就没跟上 migration，故"按幂等键复用批次"
	// 这件事在 Go 类型上是隐形的。同时它也是关联 llm_calls.trace_id 的唯一钥匙。
	IdempotencyKey string `json:"idempotency_key"`
	// ExitGate / StageCounts 见 BatchExitGate 与 PipelineCounts（009 加列）。
	// 仅 Status=empty 的行有意义；其余行 ExitGate='' 且 StageCounts 全 nil。
	ExitGate    BatchExitGate  `json:"exit_gate"`
	StageCounts PipelineCounts `json:"stage_counts"`
	CreatedAt   time.Time      `json:"created_at"`
}

// Delivery 单条内容投递记录（deliveries 表）。
// 关键设计：content_item_id ON DELETE SET NULL —— 内容按 TTL 清理后
// 投递记录仍保留为推送历史，只是不再能追溯原始内容。
type Delivery struct {
	ID              int64           `json:"id"`
	BatchID         int64           `json:"batch_id"`
	UserID          int64           `json:"user_id"`
	ContentItemID   *int64          `json:"content_item_id,omitempty"` // 可空：原内容已被清理
	Score           float64         `json:"score"`                     // NUMERIC NOT NULL：Delivery 在打分之后才创建，恒有值
	BodyMD          string          `json:"body_md"`                   // 解读正文 markdown（含阅读原文行）
	CardJSON        json.RawMessage `json:"card_json"`                 // JSONB，飞书交互卡片
	FeishuMessageID string          `json:"feishu_message_id"`         // 发送成功后回填
	Status          DeliveryStatus  `json:"status"`
	SentAt          *time.Time      `json:"sent_at,omitempty"` // 未发送时为 NULL
	CreatedAt       time.Time       `json:"created_at"`
}

// Feedback 用户反馈（feedbacks 表）。
type Feedback struct {
	ID         int64          `json:"id"`
	UserID     int64          `json:"user_id"`
	DeliveryID int64          `json:"delivery_id"`
	Action     FeedbackAction `json:"action"` // NOT NULL
	Detail     string         `json:"detail"` // 文字反馈原文，按钮反馈为空串
	CreatedAt  time.Time      `json:"created_at"`
}

// FeedbackWithContent 演化输入行（读模型，非表映射）：Feedback JOIN 出当时打分与
// 内容最小上下文。内容被 TTL 清理时 ContentTitle/ContentExcerpt 空串。
type FeedbackWithContent struct {
	Feedback
	Score          float64 `json:"score"`
	ContentTitle   string  `json:"content_title"`
	ContentExcerpt string  `json:"content_excerpt"` // 正文前 200 字符（SQL left()）
}

// Profile 用户画像与 token 预算（profiles 表），user_id UNIQUE（1:1）。
type Profile struct {
	ID                    int64     `json:"id"`
	UserID                int64     `json:"user_id"`
	Industry              string    `json:"industry"`
	Occupation            string    `json:"occupation"`
	Tags                  []string  `json:"tags"`         // TEXT[] DEFAULT '{}'
	RemovedTags           []string  `json:"removed_tags"` // TEXT[] DEFAULT '{}'：人工删除且未加回的标签，演化不得再新增（Gate ⑧）
	Summary               string    `json:"summary"`
	TokenBudgetDaily      int       `json:"token_budget_daily"`       // NOT NULL DEFAULT 100000
	TokensUsedToday       int       `json:"tokens_used_today"`        // NOT NULL DEFAULT 0
	TokenResetAt          time.Time `json:"token_reset_at"`           // NOT NULL
	LastEvolvedFeedbackID int64     `json:"last_evolved_feedback_id"` // 演化游标：已消费到的最大 feedbacks.id
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

// LLMCall 单次 LLM 调用记录（llm_calls 表，Step 6 可观测性核心）。
// trace_id 关联同一 pipeline 的多次调用；ref_type + ref_id 多态关联业务对象。
//
// span_name 的真实取值恰好六个，没有共享常量——每个写入方把字面量硬编码在自己的
// CallMeta 里，故此处是清单副本，按 span 过滤时以写入方源码为准：
//
//	score(scorer/scorer.go:131) / cardgen(cardgen/cardgen.go:77) /
//	profile_evolve(evolver/evolver.go:116) / deep_dive(feedback/deepdive.go:218) /
//	chat_reply(feishu/handler.go:174) / agent(agent/loop.go:206)
//
// 注意一个 trace_id 会横跨多个 span（PushPipelineWorkflow 把同一 traceID 依次传给
// profile_evolve/score/cardgen 三步），故按 trace 聚合打分指标时 span_name 必须进 WHERE，
// 否则演化与卡片生成的行会混进打分统计。
type LLMCall struct {
	ID int64 `json:"id"`
	// TenantID is an internal accounting identity for prepared runs. Legacy
	// calls leave it nil and retain membership-derived attribution; compiled
	// calls pin the tenant authorized immediately before the paid request.
	TenantID         *int64    `json:"-"`
	TraceID          string    `json:"trace_id"`
	SpanName         string    `json:"span_name"`
	UserID           *int64    `json:"user_id,omitempty"` // 可空：系统级调用无归属用户；刻意不建 FK
	RefType          RefType   `json:"ref_type"`
	RefID            *int64    `json:"ref_id,omitempty"` // 可空（索引 WHERE ref_id IS NOT NULL）
	Provider         string    `json:"provider"`
	Model            string    `json:"model"`
	SystemPrompt     string    `json:"system_prompt"`
	UserPrompt       string    `json:"user_prompt"`
	Completion       string    `json:"completion"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	LatencyMs        int       `json:"latency_ms"`
	CostUSD          float64   `json:"cost_usd"`                   // NUMERIC(10,6)，MVP 精度用 float64 足够
	PrefixCacheHit   *bool     `json:"prefix_cache_hit,omitempty"` // 可空：provider 未报告缓存信息
	Temperature      *float32  `json:"temperature,omitempty"`      // REAL，可空：未显式设置
	MaxTokens        *int      `json:"max_tokens,omitempty"`       // 可空：未显式设置
	Error            string    `json:"error"`                      // 调用失败原因，成功为空串
	CreatedAt        time.Time `json:"created_at"`
}

// ToolCallKind 工具调用分类（tool_calls.tool_kind），区分三个调用面，
// 限额统计与坏端点分析按此过滤。
type ToolCallKind string

const (
	ToolCallKindStatic         ToolCallKind = "static"          // 静态白名单工具（add_source/push_now/…）
	ToolCallKindTikHubSearch   ToolCallKind = "tikhub_search"   // search_endpoints 检索元工具
	ToolCallKindTikHubEndpoint ToolCallKind = "tikhub_endpoint" // 动态注入的 TikHub 端点工具（按次计费面）
	// ToolCallKindBindingFetch 绑定引擎（调度面）的上游调用：list/enrich/probe 每次
	// 计费调用一行（endpoint-binding-contract.md §5）。compiled 调度明确记录冻结的
	// tenant/user；legacy 系统抓取仍可为空。不占 agent 免确认双限额。
	ToolCallKindBindingFetch ToolCallKind = "binding_fetch"
	// ToolCallKindExaFetch Exa API 调用（/search 与 /contents）。按次计费，
	// costDollars 由上游响应返回、落 cost_usd 列。source_id 归因到具体信源。
	ToolCallKindExaFetch ToolCallKind = "exa_fetch"
)

// 工具调用错误分类（tool_calls.error_type）。低基数硬枚举：per-tool 错误率
// 视图按它 GROUP BY，自由文本会让统计不可聚合（OTel error.type 的低基数要求）。
const (
	ToolErrTimeout        = "timeout"         // 上游超时/ctx 到期
	ToolErrHTTP           = "http_error"      // 上游非 2xx
	ToolErrInvalidArgs    = "invalid_args"    // 参数校验不过（模型可自纠）
	ToolErrBudgetExceeded = "budget_exceeded" // 单消息/每日限额拦截
	ToolErrInternal       = "internal"        // 其余基础设施错误
)

// ToolCall 一条 agent 工具调用记录（tool_calls 表，migration 015）。
// 与 LLMCall 同定位：旁路可观测性；字段语义对齐 OTel execute_tool span
// （tool_name / error_type / duration），检索留痕字段的存在理由见 015 头注。
type ToolCall struct {
	ID int64 `json:"id"`
	// TenantID is an internal post-effect accounting identity for compiled
	// runs. It is carried separately from UserID because one user may belong to
	// more than one tenant. Legacy calls leave it nil and retain the historical
	// membership-derived attribution path.
	TenantID       *int64          `json:"-"`
	TraceID        string          `json:"trace_id"`
	UserID         *int64          `json:"user_id,omitempty"`
	SessionID      *int64          `json:"session_id,omitempty"` // 可空：确认卡回调等无会话来源
	ToolName       string          `json:"tool_name"`
	ToolKind       ToolCallKind    `json:"tool_kind"`
	EndpointPath   string          `json:"endpoint_path"`         // 仅 tikhub_endpoint
	Arguments      json.RawMessage `json:"arguments,omitempty"`   // 模型产出的参数原文
	ResultPreview  string          `json:"result_preview"`        // 截断版结果（8K rune）
	ResultSize     int             `json:"result_size"`           // 截断前字节数
	HTTPStatus     *int            `json:"http_status,omitempty"` // 仅 tikhub_endpoint；非 HTTP 工具 NULL
	ErrorType      string          `json:"error_type"`            // 低基数分类，成功为空串
	Error          string          `json:"error"`                 // 详情，成功为空串
	DurationMs     int             `json:"duration_ms"`
	RetrievalQuery string          `json:"retrieval_query"`           // 仅 tikhub_search
	CandidateTools []string        `json:"candidate_tools,omitempty"` // 仅 tikhub_search
	CostUSD        *float64        `json:"cost_usd,omitempty"`        // 上游返回的花费（美元）；无计费信息时 nil
	SourceID       *int64          `json:"source_id,omitempty"`       // 抓取面调用的归属信源；agent 面为 nil
	CreatedAt      time.Time       `json:"created_at"`
}

// ScheduleStatus 调度状态（schedules.status）。
// 说明：本项目枚举通常集中在 enums.go，但 M3 store 扩展仅获准改动 entities.go，
// 故与 Schedule 结构体就近定义于此；若后续 scheduler 包也需该类型，应统一
// 收敛到 enums.go 去重（见 M3 store 报告）。
type ScheduleStatus string

const (
	ScheduleStatusActive ScheduleStatus = "active" // 生效中（Temporal Schedule 未暂停）
	ScheduleStatusPaused ScheduleStatus = "paused" // 已暂停
)

// Schedule 定时推送调度（schedules 表，M3 migration 003）。
// 真源在 Temporal（schedule_id 即本表主键 ID），本表是 Postgres 侧镜像，
// 供 /api/schedules 列表读取与对账；scheduler 在 Temporal Create 成功后写入。
// 与 001 实体一致：JSONB 列 → json.RawMessage（延迟解析），status → typed 枚举。
// 注意主键 ID 为 TEXT（Temporal schedule_id）而非 001 的 BIGSERIAL 数值主键。
type Schedule struct {
	ID string `json:"id"` // Temporal schedule_id：push-{user_id}-{uuid}
	// TenantID is an internal execution-boundary field. Public schedule APIs
	// remain user-scoped and must not acquire a new wire field just because the
	// worker needs an explicit tenant identity in its durable Action input.
	TenantID      int64           `json:"-"`
	UserID        int64           `json:"user_id"`        // 归属用户
	NLDescription string          `json:"nl_description"` // 用户原话/展示名，DEFAULT ''
	SpecJSON      json.RawMessage `json:"spec_json"`      // JSONB：{cron,tz} 或 {every_seconds}
	ScopeJSON     json.RawMessage `json:"scope_json"`     // JSONB：PushScope 序列化
	Status        ScheduleStatus  `json:"status"`         // active/paused
	// ExecutionMode is an internal Approved Definition field. It is deliberately
	// excluded from the current public schedule wire until the confirmed-control-
	// plane cutover; C2a only makes the persisted compatibility mode explicit.
	ExecutionMode ExecutionMode `json:"-"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

// SchedulePlaybook 情报任务手册（schedule_playbooks 表，migration 017）。
// 与 Schedule 一对一（ScheduleID 既是主键又是外键 → schedules(id) ON DELETE CASCADE，
// 删定时任务连带删手册）。P0：Content 为手册正文，建任务时以用户 nl 意图原文初始化，
// 只做存取、不驱动抓取。FetchPlan 为 P1 预留（NL→结构化抓取计划），P0 恒为 '{}'。
type SchedulePlaybook struct {
	ScheduleID string          `json:"schedule_id"`
	Content    string          `json:"content"`
	FetchPlan  json.RawMessage `json:"fetch_plan"` // JSONB，P1 预留，P0 为 {}
	UpdatedAt  time.Time       `json:"updated_at"`
}

// AgentSession agent 对话会话（agent_sessions 表，M4 migration 005）。
// 单 owner MVP：同一用户 TTL 窗口（默认 30 分钟）内的消息共享一个会话，
// 超时由读取路径惰性置 expired。Messages 存 OpenAI 兼容消息数组 JSON
// （含 tool_calls），延迟解析；system 消息不入库，由 agent loop 调用时动态前置。
type AgentSession struct {
	ID        int64              `json:"id"`
	TenantID  int64              `json:"tenant_id"`
	UserID    int64              `json:"user_id"`
	Status    AgentSessionStatus `json:"status"`
	Messages  json.RawMessage    `json:"messages"` // JSONB，[]llm.ChatMessage 序列化
	TurnCount int                `json:"turn_count"`
	// ActivatedTools 会话内已激活（动态注入）的 TikHub 端点名（JSONB []string，
	// migration 015）。激活顺序即注入顺序——append-only，保 FC 请求的缓存前缀稳定。
	ActivatedTools json.RawMessage `json:"activated_tools"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"` // 最后活跃时间，TTL 过期判定依据
}

// PendingAction 待确认的写工具动作（pending_actions 表，M4 migration 005）。
// 交互基调"AI 出预填、人点执行"：写工具不直接执行，先落本表并发确认卡；
// 卡片按钮 value 只携带 ID，参数以库中 Args 为准，杜绝客户端篡改（契约 §10）。
// 主键 ID 为 TEXT（uuid）而非 001 的 BIGSERIAL 数值主键。
type PendingAction struct {
	ID         string              `json:"id"`                   // uuid
	UserID     int64               `json:"user_id"`              // 归属用户，回调时必须校验一致
	SessionID  *int64              `json:"session_id,omitempty"` // 可空：产生该动作的会话
	ToolName   string              `json:"tool_name"`            // 工具注册表内的白名单名
	Args       json.RawMessage     `json:"args"`                 // JSONB，模型产出的 arguments 原始 JSON
	Summary    string              `json:"summary"`              // 卡片上展示过的人类可读摘要
	Status     PendingActionStatus `json:"status"`
	ExpiresAt  time.Time           `json:"expires_at"`            // 超过后不可再领取
	ExecutedAt *time.Time          `json:"executed_at,omitempty"` // 未执行时为 NULL
	CreatedAt  time.Time           `json:"created_at"`
}

// A2ATask 是 A2A server 任务（a2a_tasks 表，migration 013）。Task 列是 SDK a2a.Task 的
// ProtoJSON 权威载荷（store 层不解析）；ID/ContextID/Status 是提取列。SDK 类型不出 a2a/ 包
// （隔离原则，同 agent.Store 窄接口先例），store 层只见本类型。
type A2ATask struct {
	ID        string          `json:"id"` // 服务端生成 taskId
	ContextID string          `json:"context_id"`
	Status    string          `json:"status"`  // TASK_STATE_* 原文
	Task      json.RawMessage `json:"task"`    // JSONB，完整 a2a.Task ProtoJSON
	Version   int64           `json:"version"` // 乐观并发版本，从 1 起
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// A2ATaskQuery 是 ListA2ATasks（a2a-contract §4.1）的过滤条件，字段面对齐 SDK
// a2a.ListTasksRequest（v2.3.1 已核实：Tenant/ContextID/Status/PageSize/PageToken/
// HistoryLength/StatusTimestampAfter/IncludeArtifacts）。Tenant 单租户恒空不映射；
// HistoryLength/IncludeArtifacts 是 task JSONB 裁剪语义，归 a2a/taskstore.go 适配层
// （契约 §5.9），store 不感知。
type A2ATaskQuery struct {
	ContextID            string    // 空串 = 不过滤
	Status               string    // TASK_STATE_* 原文；空串 = 不过滤
	StatusTimestampAfter time.Time // 零值 = 不过滤
	PageSize             int       // <=0 → 50；钳上限 200
	PageToken            string    // (created_at,id) 键集游标，store 包编解码，调用方视为不透明串
}
