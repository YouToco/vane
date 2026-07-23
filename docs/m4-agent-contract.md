# M4 契约：最小 Agent Loop + 工具化（并行实现的对接基准）

> 本文件是 M4 并行实现的**唯一契约**。所有签名/JSON/表结构以此为准，实现中发现契约错误
> 不得自行变更——记录到交付报告，由主控裁决。
> 事实基准（均已实测）：deepseek-v4-pro function calling 60/60 全过（6 场景）；
> lark-oapi-go v3.9.9 WS 支持 OnP2CardActionTrigger（callback.CardActionTriggerEvent，
> action.Value 为 map[string]interface{}，返回 *callback.CardActionTriggerResponse 可原地更新卡片）；
> DeepSeek V4 默认思维链，结构化输出必须 thinking:{type:"disabled"}（llm.Request.DisableThinking 已存在）。

## 0. 产品行为（Boss 定的交互基调：AI 出预填、人点执行）

飞书对话（仅 owner）→ agent loop（v4-pro FC）：
- **读工具**（list_sources / list_schedules / push_now）：直接执行，结果回给模型继续多轮。
- **写工具**（add_source / remove_source / create_schedule / remove_schedule）：**不执行**，
  生成「确认卡」（展示工具名+参数摘要+确认/取消按钮）；用户点确认 → 卡片回调 → 真正执行 → 原地更新卡片为结果。
- 与订阅/推送无关的闲聊：模型直接文字回答（不调工具），行为与现 chat_reply 一致。
- maxTurns（config agent.max_turns，默认 20）内未收敛 → 回复兜底文案。
- 会话：同一 owner 30 分钟内的消息共享一个 agent 会话（多轮上下文），超时新开。

## 1. migration `store/migrations/005_agent.sql`

```sql
-- agent 会话：单 owner MVP，messages 存 OpenAI 兼容消息数组（含 tool_calls）
CREATE TABLE agent_sessions (
    id         BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    status     TEXT        NOT NULL DEFAULT 'active',  -- active / expired / closed（应用层校验）
    messages   JSONB       NOT NULL DEFAULT '[]',
    turn_count INT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_agent_sessions_user_status ON agent_sessions (user_id, status, updated_at DESC);

-- 待确认动作：确认卡按钮 value 只带 id，参数服务端存取（防客户端篡改参数）
CREATE TABLE pending_actions (
    id          TEXT        PRIMARY KEY,               -- uuid
    user_id     BIGINT      NOT NULL REFERENCES users (id),
    session_id  BIGINT      REFERENCES agent_sessions (id),
    tool_name   TEXT        NOT NULL,
    args        JSONB       NOT NULL DEFAULT '{}',
    summary     TEXT        NOT NULL DEFAULT '',       -- 卡片上展示过的人类可读摘要
    status      TEXT        NOT NULL DEFAULT 'pending', -- pending / executed / cancelled / expired
    expires_at  TIMESTAMPTZ NOT NULL,
    executed_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_pending_actions_user_status ON pending_actions (user_id, status);
```

## 2. store 新方法（`store/agent.go`，*Store 接收者，错误包装与现有一致 CodeDatabase/CodeNotFound）

```go
// GetActiveAgentSession 取该用户最近的 active 会话；updated_at 早于 since 的视为过期
// （顺带 UPDATE status='expired'），不存在/已过期返回 types.ErrNotFound 类错误。
func (s *Store) GetActiveAgentSession(ctx context.Context, userID int64, since time.Time) (*types.AgentSession, error)
// CreateAgentSession 新建 active 会话，返回完整实体。
func (s *Store) CreateAgentSession(ctx context.Context, userID int64) (*types.AgentSession, error)
// UpdateAgentSession 覆盖写 messages 与 turn_count，刷新 updated_at。
func (s *Store) UpdateAgentSession(ctx context.Context, id int64, messages json.RawMessage, turnCount int) error
// CreatePendingAction 落一条待确认动作。
func (s *Store) CreatePendingAction(ctx context.Context, a *types.PendingAction) error
// ClaimPendingAction 原子领取：status='pending' AND expires_at>now() AND user_id=$userID
// 才置为 executed 并返回实体；否则返回 ErrNotFound 类错误（含已执行/已过期/不存在/非本人，
// 幂等防双击）。归属校验进 WHERE 谓词（§10）：越权请求零副作用。
func (s *Store) ClaimPendingAction(ctx context.Context, id string, userID int64) (*types.PendingAction, error)
// CancelPendingAction pending → cancelled（同样带 user_id 谓词）；非 pending/非本人返回 ErrNotFound 类错误。
// RETURNING 返回实体：卡片回调回写通告需要 Summary。
func (s *Store) CancelPendingAction(ctx context.Context, id string, userID int64) (*types.PendingAction, error)
// AppendAgentSessionMessages 原子追加会话消息（msgs 必须是 JSON 数组，库内 jsonb || 拼接）。
// 供卡片回调回写「[卡片回调]」通告：与 saveSession 的全量覆盖写并发时只有先后、没有丢失。
// 刻意不刷 updated_at——确认卡有效期（24h）远超会话 TTL（30min），点卡不得复活超时会话。
func (s *Store) AppendAgentSessionMessages(ctx context.Context, sessionID int64, msgs json.RawMessage) error
```

## 3. types 新实体（`types/entities.go` 追加；枚举加 `types/enums.go`）

```go
type AgentSession struct {
    ID        int64           `json:"id"`
    UserID    int64           `json:"user_id"`
    Status    AgentSessionStatus `json:"status"`
    Messages  json.RawMessage `json:"messages"`
    TurnCount int             `json:"turn_count"`
    CreatedAt time.Time       `json:"created_at"`
    UpdatedAt time.Time       `json:"updated_at"`
}
type PendingAction struct {
    ID         string          `json:"id"`
    UserID     int64           `json:"user_id"`
    SessionID  *int64          `json:"session_id,omitempty"`
    ToolName   string          `json:"tool_name"`
    Args       json.RawMessage `json:"args"`
    Summary    string          `json:"summary"`
    Status     PendingActionStatus `json:"status"`
    ExpiresAt  time.Time       `json:"expires_at"`
    ExecutedAt *time.Time      `json:"executed_at,omitempty"`
    CreatedAt  time.Time       `json:"created_at"`
}
// 枚举：AgentSessionStatus{active,expired,closed}；PendingActionStatus{pending,executed,cancelled,expired}
```

## 4. llm 包多轮+tools（新文件 `llm/chat.go`，**不改动**现有 Complete/Request/Response）

```go
type ChatMessage struct {
    Role       string     `json:"role"`              // system|user|assistant|tool
    Content    string     `json:"content"`
    ToolCalls  []ToolCall `json:"tool_calls,omitempty"`   // role=assistant 且有调用时
    ToolCallID string     `json:"tool_call_id,omitempty"` // role=tool 必填
}
type ToolCall struct {
    ID        string `json:"id"`
    Name      string `json:"name"`      // function.name
    Arguments string `json:"arguments"` // function.arguments 原始 JSON 字符串
}
type ToolDef struct {
    Name        string          `json:"name"`
    Description string          `json:"description"`
    Parameters  json.RawMessage `json:"parameters"` // JSON schema
}
type ChatRequest struct {
    Model           string // 空串用 Client 默认 model；agent 传 cfg.LLM.AgentModel
    Messages        []ChatMessage
    Tools           []ToolDef
    Temperature     *float32
    MaxTokens       *int
    DisableThinking bool
}
type ChatResponse struct {
    Content          string
    ToolCalls        []ToolCall
    FinishReason     string
    PromptTokens     int
    CompletionTokens int
    CacheHitTokens   int
    CacheMissTokens  int
    Model            string
    LatencyMs        int
}
func (c *Client) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)
// DoChat = Chat + 记账（对齐 llm.Do 的行为；Completion 记 Content，若有 ToolCalls
// 则记 "tool_calls: <json>"；UserPrompt 记 messages 数组 JSON 截断到 8KB）。
func DoChat(ctx context.Context, c *Client, rec *Recorder, meta CallMeta, req ChatRequest) (*ChatResponse, error)
```
线协议（OpenAI 兼容，DeepSeek 实测格式）：请求 `tools:[{type:"function",function:{name,description,parameters}}]`；
响应 `choices[0].message.tool_calls[].{id,type,function:{name,arguments}}`；
role=tool 消息 `{role:"tool",tool_call_id,content}`；assistant 带 tool_calls 的历史消息必须原样带 tool_calls 字段回传。
空 content 且无 tool_calls 时沿用现有 WARN。信号量/错误映射复用 Complete 的实现（可提公共 helper）。

## 5. config（`config/config.go`）

```go
// LLMConfig 增加：
AgentModel string `mapstructure:"agent_model"` // 默认 "deepseek-v4-pro"
// AgentConfig 增加：
SessionTTLMinutes int `mapstructure:"session_ttl_minutes"` // 默认 30
// setDefaults: llm.agent_model=deepseek-v4-pro；agent.session_ttl_minutes=30
```

## 6. sourcespec 包（新包 `sourcespec/sourcespec.go`）——api 与 agent 工具共用的信源构造

把 `api/subscriptions.go` 的 `buildSource`/`addSubscriptionReq` 校验逻辑**原样迁移**为：
```go
package sourcespec
type Spec struct {  // 字段与原 addSubscriptionReq 一致
    Type, URL, Query, Keyword, Title, Category string
}
// Build 校验并构造待 upsert 的信源；错误返回给用户的中文文案（空串=成功）。
func Build(spec Spec) (*types.Source, string)
```
api/subscriptions.go 改为薄适配（JSON decode → sourcespec.Spec → Build）；行为与现有测试（api/subscriptions_test.go）完全等价，把 buildSource 测试同步迁到 sourcespec 包（api 保留 handler 层薄测试即可）。

## 7. agent 包（新包 `agent/`）

```go
// Tool 是 agent 可用工具。Mutating=true 的工具不由 loop 直接执行，走确认卡。
type Tool interface {
    Name() string
    Description() string
    Parameters() json.RawMessage           // JSON schema（对齐 M4 spike 里验证过的形态）
    Mutating() bool
    // Execute 返回给模型/用户看的结果文本（中文）。参数是模型产出的 arguments 原始 JSON。
    Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error)
    // Summarize 把 args 渲染成确认卡上的人类可读摘要（仅 Mutating 工具需要有意义实现）。
    Summarize(args json.RawMessage) string
}

// Deps 注入（main.go 装配）。SessionStore/ActionStore 是 store 方法的窄接口（契约 §2 签名）。
type Deps struct {
    Client   *llm.Client
    Recorder *llm.Recorder
    Store    Store   // 窄接口：§2 全部 6 个方法
    Tools    []Tool
    Model    string        // cfg.LLM.AgentModel
    MaxTurns int           // cfg.Agent.MaxTurns
    SessionTTL time.Duration // cfg.Agent.SessionTTLMinutes
}
func New(d Deps) *Loop

type Outcome struct {
    Reply   string   // 给用户的文字回复（恒非空）
    Confirm *Confirm // 非 nil 时 feishu 层追加发确认卡
}
type Confirm struct {
    ActionID string // pending_actions.id
    Summary  string // 卡片正文（工具名+参数摘要）
}
// HandleMessage 完整 agent loop：取/建会话 → 多轮 FC → 读工具直接执行、
// 首个写工具建 pending_action 并终止本轮 → 持久化会话 → 返回。
func (l *Loop) HandleMessage(ctx context.Context, userID int64, text string) (Outcome, error)
// HandleExternalContextMessage 处理已经拼入推送正文/引用消息的输入：从首轮起不读画像、
// 不声明/执行工具；用户本轮可见回答照常返回，但原始外部上下文与派生回答不持久化。
func (l *Loop) HandleExternalContextMessage(ctx context.Context, userID int64, text string) (Outcome, error)
// RunOnce 在给定历史上执行一轮多轮 FC（§7.1，2026-07-18 随 A2A PR-4 增补）：
// 不读写会话存储、不持 userMu 锁、不注入画像——历史与并发语义由调用方管理
//（A2A 侧按 contextId 重建历史，a2a-contract §12 P2）。返回追加了本轮交换的完整历史。
// 所属实例必须只注册只读工具（a2a 装配显式白名单 list_sources/list_schedules）；
// Confirm 出口在此转为错误（外部 agent 点不了确认卡，挂起即悬空）——agent/runonce_test.go 钉死。
func (l *Loop) RunOnce(ctx context.Context, userID int64, history []llm.ChatMessage, text string) (Outcome, []llm.ChatMessage, error)
// ExecuteAction 确认卡回调入口：ClaimPendingAction（原子幂等）→ 找到工具 Execute →
// 返回结果文本（用于更新卡片）。已执行/过期/不存在返回人话错误文本 + nil error。
func (l *Loop) ExecuteAction(ctx context.Context, userID int64, actionID string) (string, error)
// CancelAction 取消按钮回调。返回用于更新卡片的文本。
func (l *Loop) CancelAction(ctx context.Context, userID int64, actionID string) (string, error)
```
Loop 行为细则：
- system prompt 常量（中文）：声明角色=见微 Vane 助理、只在需要时调工具、写操作会出确认卡由用户确认、
  无关问题直接回答。外部内容注入防护措辞对齐 scorer 的写法。
  **§7.1 增补（2026-07-18 PR-4）**：`Deps.SystemPrompt` 可覆盖该常量（零值回落，飞书轨零行为变化）；
  [用户画像] 段只跟随默认 prompt 渲染（自定义 prompt 的 A2A 轨不渲染——画像是 A2A 非目标）。
- 会话消息即 []llm.ChatMessage 的 JSON；system 消息**不入库**，每次调用时动态前置。
- 模型调用：DoChat，SpanName="agent"，DisableThinking=**true**（实测思维链会与 content
  共享 MaxTokens 并导致复杂 FC 空输出；工具选择在关闭后无退化），MaxTokens 2048，
  Temperature nil（默认）。
- 模型返回未注册工具名：以 role=tool 回错误文本"工具 X 不存在"，继续循环（模型自纠）。
- 一轮多个 tool_calls：顺序处理；读工具执行并回结果；遇到**首个**写工具：建 pending_action
  （24h 过期，summary=tool.Summarize(args)），该 tool_call 的 role=tool 结果写
  "已生成确认卡，等待用户确认"，**其后未处理的 tool_calls 也各回一条 role=tool
  "本轮已挂起，等待用户确认后再操作"**（协议要求每个 tool_call 必须有对应 tool 消息），
  然后再调一次模型拿收尾文案（不带 tools，防再触发），Outcome.Reply=收尾文案、Confirm 非 nil，结束。
- `web_search`、`read_page`、动态 TikHub 端点、`read_endpoint_result` 和含外部标题的
  `list_sources` 把外部结果送入模型后，本条用户消息进入 **untrusted-result 边界**：
  后续模型请求不再附加动态画像，且不声明网络、内部数据或写工具；只有本轮动态端点确实
  生成截断句柄时，才保留本地 `read_endpoint_result` 分页能力；权限绑定到本消息实际产生的
  handle 集合（不是可猜句柄共用的一位 bool），每个 assistant 批次最多续读一次。模型即使幻觉调用画像/
  任务/信源/写工具，或尝试把上下文编码进第二个 URL/query，也只收到固定拒绝回执，
  不执行工具、不访问外网、不创建 pending_action。该边界是确定性 Go 代码，不以模型
  是否遵守 prompt 为前提。若同一个 assistant 响应并列多个 tool_call，只要批内含可执行
  的外部读取，执行前先整体分类并整批拒绝，要求下一轮把一个外部读取单独发起；不能只拦
  其他调用的执行，因为其 assistant content/arguments 仍会进入历史。单个外部读取执行后，
  内部消息先重建成最小隔离上下文：仅保留当前 user、去掉 content/arguments 的
  tool_call 协议壳及 tool result，此前会话、画像派生文本和被拒调用全部不再同屏；该结构
  只供审计、tool_call 配对与保存前清洗。若仍有本轮动态端点产生的本地缓存句柄，出站请求
  保留原生协议以支持 `read_endpoint_result` 分页；一旦工具声明为空，出站视图必须进一步
  投影为 `system + 单条 user`，不得携带 `assistant.tool_calls` 或 `role=tool`（2026-07-22
  生产证实 DeepSeek v4-pro 对“零工具 + 原生工具历史”会间歇泄漏内部 DSML 协议）。
  单条 user 以 `[外部只读结果]` 开头，后接系统生成的 JSON：
  `{"user_request":"…","external_result":"…"}`；JSON 字符串编码不得允许外部正文伪造字段边界，
  system 必须明确整段 `external_result` 都是不可信数据、本轮只输出文字且不能声称执行操作。
  类型化飞书追问/引用进入该投影时，只有包装外的真实追问/回复可进入 `user_request`，
  推送原文/引用正文必须进入 `external_result`；包装损坏时全文降为外部数据，禁止信任提升。
  `search_endpoints` 也必须独占批次：它会在执行期激活动态端点，预扫描时尚不可解析的
  同批付费端点否则会在顺序执行中穿透。
- 飞书追问包装与引用消息在**第一次**模型请求前就已混入外部正文，必须走类型化的
  `HandleExternalContextMessage`，不能等工具返回后才 taint。该入口从数据访问层就不读画像，
  首轮即不声明工具，也不加载既有 agent session 历史；即使模型幻觉调用网络、内部读取或
  写工具，运行时二次门仍固定拒绝。既有历史只在本轮结束后与固定占位重新合并保存，
  防群聊引用/恶意卡片直接要求复述旧私聊。普通消息与外部上下文的分流由 `AgentRunner`
  两个独立方法表达，不靠检查正文文案猜测。
- 含外部结果的整段 user turn 在写入 `agent_sessions` 前压成「原 user + 固定安全占位」，
  原始结果不留在 `agent_sessions`；仍可能存在于有界的 `tool_calls.result_preview` 与
  `llm_calls.user_prompt` 审计记录。加载会话时同样清洗部署前存量，防旧网页结果在
  下一条消息与画像、完整工具面重新同屏。`add_source` 确认执行期的外部 Probe 详情仍展示
  给用户，但成功与失败写回会话的都是各自固定回执（失败 Message 也可能含页面声明 URL）。
  外部上下文入口把整段合成 user turn 与模型派生回答
  一并换成固定占位；旧版追问/引用、带外部标题的反馈回调、中文 add_source Probe 回执
  在加载时迁移清洗。历史判定不依赖当前工具是否装配或仍在目录中；同时只有出现真实
  `role=tool` 外部回执才压平，单纯生成 add_source pending 确认卡不误删。清洗完成后，
  下一条明确用户消息才恢复正常工具面。
- 下一条用户消息若以强肯定命令明确要求按当前消息**直接生成任务确认卡**（如「确认创建，
  直接生成确认卡」），且没有要求先搜索、查询、检查或核对，则本消息进入
  `direct-task-creation` 缩面：从数据访问层跳过画像，模型请求只声明 `create_schedule`，
  请求消息只保留当前 user turn（原有已清洗历史在本轮结束后再合并持久化），
  且声明与执行两层都要求该注册工具的真实 effect 为 mutating；同名只读工具不得暴露或执行。
  `approved_fetch_plan.existing_source_ids` 与内部持久化字段 `sources` 在此模式下禁止出现：
  前者防模型猜 ID 触发隐藏 DB 读取，后者防模型猜 `vane://` URL/config/title。
  当前消息明确的新信源必须写入版本化 `approved_fetch_plan.source_specs`
  （`version=vane.source-specs/v1`），只提交 kind 对应的人类可读原始参数；Coordinator
  在 pending 前经 `sourcespec.Build` 原子物化，再走原有 canonical/SSRF/重复 URL 校验。
  任一 spec 非法则整份 proposal 失败，durable args 与 recovery 仍只认严格 `sources`。
  所有模型轨（不只是 direct）都在 Agent 边界禁止提交内部 `sources`；Coordinator
  保留旧 `sources` 入口只供已冻结数据、恢复流程和可信内部兼容。版本化 envelope、
  plan 与每种 spec 的字段名逐字匹配，大小写别名、转义键、重复键、未知字段和显式
  `null` 均拒绝；URL/域名还须在 pending 前拒绝十进制、八进制、十六进制及缩写 IPv4。
  运行时对任何幻觉的其他已注册工具调用也返回固定本地拒绝，不执行、不 taint。
  用户明确说「不要再搜索／直接出确认卡」
  时优先按此缩面；明确否定创建或要求先核对时不得进入。该模式只减少读取能力，不增加权限：
  `create_schedule` 仍只落 durable proposal，任务仍须用户点击确认卡才执行。任何无工具自由文本
  都会被丢弃并回到 clean baseline 自纠一次（不尝试用词法区分“追问”与“口头承诺”），连续两次
  没有真实 proposal 则返回固定的「任务尚未创建」文案；带 tool_calls 的 assistant content 也
  强制清空，避免参数校验失败时把同批的「卡已生成」承诺留进历史。整轮无论是否成功，
  会话只持久化当前 user + 确定性回复，不保留动态校验 tool result（工具审计仍走独立账本），
  避免通用 fail-closed 清洗把本地拒绝误记成外部查询。此规则避免外部发现完成后的确认消息
  又被 `list_sources` 带回 untrusted-result 边界，形成「确认→再读→隔离→口头承诺」循环。
  direct 请求 schema 只暴露且强制 `source_specs`；每个 assistant 响应必须恰有一个
  `create_schedule` 调用，多调用整批零执行。direct 模式独立限制为
  `min(agent.max_turns, 4)`；create_schedule 参数校验（含本地精确字段门）最多失败两次，
  第二次仍失败即返回固定「任务尚未创建」文案，不能把全局 20 轮当成付费猜格式预算。
- 普通 turn 达 MaxTurns：Reply="这个请求步骤太多，我先停下来了，请把需求拆小一点再试"；
  direct-task-creation 达独立四轮上限则返回固定「任务尚未创建」文案。
- 全部 LLM 错误向上抛（feishu 层 humanize）。

## 8. agent 工具集（`agent/tools.go`，构造函数 `BuildTools(st *store.Store, sched *scheduler.Scheduler, pusher PushTrigger) []Tool`）

| 工具 | Mutating | 底层调用 | 说明 |
|---|---|---|---|
| list_sources | no | store.ListSubscribedSourcesByUser | 返回 id/类型/标题/状态的中文列表文本 |
| add_source | **yes** | sourcespec.Build → store.UpsertSource + AddSubscription | 参数 schema 同 spike：{type(enum rss/exa/tikhub_xhs), url, query, keyword, title?, category?} |
| remove_source | **yes** | store.RemoveSubscription（逐 id，删除幂等） | {source_ids:[integer]}（1–20 个，去重保序，一张确认卡列全并一次确认；旧单数 {source_id} 仅为兼容存量 pending_actions 保留解析，schema 不再声明——2026-07-23 增补，批量意图不再被拆成 N 轮确认） |
| list_schedules | no | store.ListSchedulesByUser | 中文列表文本 |
| create_schedule | **yes** | CreationCoordinator.Propose → 人工确认 → creation saga | `{spec,intent,approved_fetch_plan:{existing_source_ids?,source_specs?},nl_description?,strictness?}`；`source_specs` 是 `vane.source-specs/v1` 原始规格，模型面不暴露 durable `sources/config/vane://`；cron/every 二选一且 every≥3600 |
| remove_schedule | **yes** | scheduler.DeletePush | {schedule_id:string} |
| push_now | no（低危） | PushTrigger 接口（api push/now 同款触发） | 返回 run_id 文本 |
| web_search | no | fetcher.ExaFetcher.Search（Exa /search，按次计费） | {query, num_results?(默认5/上限20), include_domains?}；一次性语义搜索，不建信源、不写内容库，结果只回当前对话（2026-07-20 增，见修订记录） |
| read_page | no | fetcher.ExaContentsFetcher.ReadPage（Exa /contents，maxAgeHours:0 活抓，按次计费） | {url}；一次性读取指定页面正文，不建信源、不写内容库（2026-07-20 增，见修订记录） |

**§8 增补（2026-07-20，Boss 拍板）**：web_search / read_page 解决「临时查一下」被迫
add_source 的形态（信源固定点反模式——一次性需求不该走订阅设施）。两工具按次计费，
Agent 工具行与 fetcher 上游行共享同一 trace_id/user_id，tenant_id 由 membership 推导；
普通对话复用本条消息 trace，确认卡执行入口在真实执行前生成独立非空 trace；
fetcher 层上游行按 `SourceID=0` 无源口径落 tool_calls，归属元数据只在本地 context 传递，
不得进入 Exa 请求体或请求头；两层旁路记账都脱离调用方取消并重新施加 5 秒 deadline，
避免取消窗口撕裂双账本或连接池故障无限阻塞；两条路径均不创建
source/content/content_sources。**双重限额**（与端点
工具同模板，对抗审查 HIGH 补齐）：单条消息 `agent.exa_msg_cap`（默认 5，消息内计数，
超限回文案）+ 滚动 24h `agent.exa_daily_cap`（默认 100，从 tool_calls 表按
tool_name IN ('web_search','read_page') COUNT，排除 invalid_args/budget_exceeded，
判定失败 fail-closed）；不进 A2A 只读白名单（显式名单仍为
list_sources/list_schedules——对外部 agent 暴露付费面是另一个决策）；
Exa key 未配置时不装配（BuildTools exa 参为 nil），system prompt 的分流引导行同样
条件注入（工具不在场不广告）；maxActivatedEndpoints 同步 13→11（16 基础 + 2 端点
工具 + 11 激活 = 29 < 30 安全线）。

`PushTrigger` 是窄接口 `TriggerPushNow(ctx, userID int64) (runID string, err error)`——查明 api/push.go
现有触发实现的真实归属（scheduler 或 api 内部），把可复用的触发逻辑收敛为 scheduler 上的导出方法后包一层；
禁止 agent 工具直接 import api 包。scheduler.CreatePush/DeletePush 的真实签名以 scheduler 包现状为准，
工具层做适配（这两个签名不在本契约锁定范围，实现者先读 scheduler/scheduler.go）。

## 9. feishu 扩展（handler.go / card.go / manager.go / main.go）

- Manager 增加字段 `agent *agent.Loop`（NewManager 增参或 SetAgent 二选一，实现者定，main.go 装配）。
- handler.handle：owner 校验后，原 chat_reply 调用替换为 `agent.HandleMessage`；
  Reply 文本用现有 BuildReplyCard 回复；Outcome.Confirm 非 nil 时**再发一张**确认卡（新消息，非回复）。
- `BuildConfirmCard(summary, actionID string) string`：JSON 2.0 卡片，正文 markdown 展示 summary，
  两个按钮：确认（callback value {"vane_action":"confirm","action_id":...}，type=primary）、
  取消（value {"vane_action":"cancel","action_id":...}）。schema 细节以飞书 JSON 2.0 卡片文档/现有 card.go 为准。
- eventDispatcher 增加 `OnP2CardActionTrigger`：
  - operator open_id ≠ owner → 返回 toast「仅主人可操作」。
  - value.vane_action=confirm → `agent.ExecuteAction`；cancel → `agent.CancelAction`。
  - 返回 `*callback.CardActionTriggerResponse` 原地把卡片更新为结果文本（沿用 BuildReplyCard 的正文样式），
    并 toast 成功/失败。SDK 响应结构实现者读 callback 包源码确认。
  - 回调处理必须 recover（与 handle 相同的 panic 兜底）+ 3 秒内响应（长执行丢 goroutine + 先回 toast「执行中」，
    完成后用 Token 更新卡片或补发消息——若 SDK 的延迟更新不可行，降级为同步执行但 ExecuteAction 内部超时 2.5s，
    超时时先回「执行中，稍后看结果消息」并异步补发结果消息）。
- main.go：构造顺序 store → llm → scheduler → tools → agent.New → manager 注入。
  push_now 工具依赖 scheduler，故 agent 装配在 scheduler 之后、manager.Start 之前。

## 10. 安全红线（deny 与边界）

- 工具注册表是唯一可调用面（白名单）；未注册名一律拒绝。
- ExecuteAction/CancelAction 必须校验 pending_action.user_id == 请求 userID。
- 确认卡按钮 value 只携带 action_id；参数以库中为准，杜绝客户端篡改。
- agent 会话 messages 上限 60 条：超过时保留最早 1 条 user + 最近 40 条（简单截断，防上下文无限膨胀）。
- 所有新增 SQL 走参数化（与现有 store 一致）；不引入新依赖。

## 11. 测试要求（每包与现有测试风格一致）

- llm/chat_test.go：httptest 断言请求体 tools/messages/tool 消息序列化、tool_calls 解析、Model 覆盖、thinking 缺省不携带。
- store：DB 门控子测试（agent_sessions CRUD、ClaimPendingAction 幂等/过期）。
- sourcespec：迁移原 buildSource 全部用例。
- agent：mock Client？——llm.Client 是具体类型，Loop 依赖收窄：Loop 内部通过 `chatFn func(ctx, llm.ChatRequest) (*llm.ChatResponse, error)` 字段调用（默认包 DoChat），测试注入假实现。覆盖：纯聊天、读工具单轮、写工具确认卡、未知工具自纠、maxTurns 兜底、ExecuteAction 幂等；恶意外部结果必须用真实外部工具分类反向证明画像读取、写 pending action 与 URL/query 上下文外带均被确定性拒绝，并覆盖跨消息、部署前存量会话、外部 Probe 成功/失败回调、本地缓存分页、同批“内部读→外部读→写”乱序及 `search_endpoints+未激活动态端点`；外部结果后的零工具出站请求必须断言只有 system+user、无原生 tool protocol，JSON 字段边界不可由外部正文伪造，且内部历史仍按原 user+固定占位持久化；外部上下文测试必须证明首轮零画像读取/零工具/零既有历史、原文零持久化且旧历史保留，扁平化时真实追问与外部正文不发生信任提升；add_source 只建 pending 或整批拒绝后重试写工具时不会被误判为已执行外部查询。
- `direct-task-creation` 必须用生产同形的「确认创建，直接生成确认卡，不要再次搜索」消息覆盖：
  即使模型先并列幻觉 `list_sources/list_schedules`、再单独重试 `list_sources`，两次都零执行、
  零 taint、画像零读取，所有模型请求只声明 `create_schedule`，随后合法调用只能产生一个 durable
  proposal；模型无工具文字（包括「确认卡稍后会出现，可以吗？」或真实追问）必须丢弃并从 clean
  baseline 自纠，连续两次无工具文字只能返回固定未创建文案；另须覆盖旧画像历史 current-turn
  隔离、读+写混合批次原子拒绝、同名非 mutating 工具零执行，以及无效 create tool_call 同批口头
  承诺不入历史、`existing_source_ids`/legacy `sources` 零 Propose、参数校验两次即停、direct
  第五轮绝不消费、同一响应多个 create 调用整批零执行。另须证明
  `source_specs → sourcespec.Build → canonical sources` 在 pending
  前完成，未知/重复/错 kind 字段、SSRF、非法裸域名、canonical 重复或批内任一坏项均整单拒绝，
  durable bytes 与 recovery 不含 `source_specs`，Confirm 逐字消费该冻结计划且不重建。
  删除请求侧缩面、运行时二次门、
  clean-baseline reset、确定性物化或无 proposal 的口头承诺门中的任一项，测试都必须变红。
- fetcher：Exa ad-hoc 上游账本必须断言 trace/user 归属与 `source_id=0`，反向断言这些本地归属元数据没有出现在第三方请求体/请求头；取消后的记账 context 仍可用且有 deadline，`statuses[].status=error` 必须在账本标为失败而不是 HTTP 200 成功。
- feishu：BuildConfirmCard JSON 结构断言；回调 owner 校验单测（能 mock 的部分）。

## 12. `create_schedule` A5/A6 耐久特化（2026-07-21，覆盖本契约旧路径）

本节只覆盖 `create_schedule`；其他历史写工具仍按 §7/§9 的 v0 行为。旧 HTTP
调度创建入口已退役，Agent 是新任务创建的唯一生产入口。

- v1 `pending_actions` 是 lease/fence/checkpoint 创建 saga。准备结果不可变，按
  `paused Temporal Schedule → 数据库完整定义 → Activate → terminal` 收敛；进程退出后
  recovery 复用原 checkpoint，不重新编译、不创建第二个 TaskID。
- 确认/取消回调必须携带原卡 `OpenMessageID`，并在接受动作的同一数据库事务中绑定
  `receipt_provider/receipt_target`。provider 含发出原卡的飞书 App 身份指纹：同 App
  换 secret 可继续，禁用或切换 App 不得沿用旧权限、也不得用新 App 修改旧卡。
- 所有新确认卡与回执卡固定 `config.update_multi=true`。终态 operation 与
  `task_creation_receipts` outbox 同事务提交；回调只给即时 toast/处理中反馈，最终结果
  由 dispatcher 对原 `message_id` 执行幂等 `Message.Patch`，禁止另发结果消息。
- outbox 使用数据库时钟、lease/fence、不可变 payload+SHA-256、会话 checkpoint 和
  sent checkpoint。Patch 超时视为结果未知：重试同一 target+同一字节，因此最多只有
  一张可见资源；进程可在任一 checkpoint 后退出并由下一实例接管。
- Agent 会话只追加固定结构化终态事实，不得写入任务 summary、信源 title/URL/config、
  provider error 或卡片正文；这些字段可能含外部内容，不能借 `[卡片回调]` 身份升级为
  下一轮模型的可信指令。会话与普通消息共用 per-user 锁，锁忙立即释放 outbox 等待重试，
  不得阻塞全局回执扫描或停机。
- terminal replay 不得用“处理中”覆盖已经 Patch 的最终卡；接受前错误或 panic 只回 toast，
  保留原卡重试能力。执行中的创建收到取消时必须明确告知“无法再取消”，但仍绑定原卡，
  以便最终成功/安全回滚结果耐久送达。
- 验证至少覆盖：终态+outbox 原子回滚、跨 Tenant/User/Task、双击/竞争、stale takeover、
  会话提交响应丢失、Patch 成功但响应丢失、sent checkpoint 前崩溃、跨 App 拒发、渠道撤销、
  外部摘要注入、panic 保卡、忙 user 锁不阻塞，以及真实 PostgreSQL `-race`。
