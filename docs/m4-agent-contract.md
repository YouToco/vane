# M4 契约：最小 Agent Loop + 工具化（并行实现的对接基准）

> **现行覆盖声明（2026-07-29）：** 本文中的 `list/add/remove/enable_source`、
> `push_now`、账户订阅、来源确认卡、通用 pending action 与 callback 执行入口
> 已由 `task-playbook-fetch-target-cutover.md` 取代，只作为冻结 v1 数据和事故背景
> 保留。当前写路径只有 `Prepare → Execute → Agent session receipt`，不得从本文
> 恢复任何交互确认阶段。
>
> 本文件是 M4 并行实现的**唯一契约**。所有签名/JSON/表结构以此为准，实现中发现契约错误
> 不得自行变更——记录到交付报告，由主控裁决。
> 事实基准（均已实测）：deepseek-v4-pro function calling 60/60 全过（6 场景）；
> lark-oapi-go v3.9.9 WS 支持 OnP2CardActionTrigger（callback.CardActionTriggerEvent，
> action.Value 为 map[string]interface{}，返回 *callback.CardActionTriggerResponse 可原地更新卡片）；
> DeepSeek V4 默认思维链，结构化输出必须 thinking:{type:"disabled"}（llm.Request.DisableThinking 已存在）。

## 0. 产品行为（当前基线：意图工具包 + 同轮 grounded research）

飞书对话（仅 owner）→ agent loop（v4-pro FC）：
- 内部只读、公开网页/社媒研究工具按当前用户意图在首个模型请求曝光；动态社媒工具保持
  `search_endpoints → 激活具体工具 → 同一用户消息内调用` 的延迟发现。
- 用户明确要求且目标唯一时，`create_schedule` / `edit_task_definition` /
  `update_profile` / `remove_schedule` / `run_task_now` 直接执行；真歧义才自然追问，
  禁止要求内部 ID。
- 用户一次点名多个任务时，Agent 先用列表工具分别解析名称/描述，再合并为一次
  `remove_schedule` / `run_task_now` 批量调用；不得只处理第一个。
- 所有写操作都遵循 `Prepare → Execute → Agent session receipt`，没有交互确认阶段；
  durable owner、幂等、补偿、恢复和审计保持不变。
- 最新事实问题允许同一消息内连续 `web_search` / `read_page` / 社媒只读查询，优先核验
  第一方页面并只引用结构化结果中真实出现的 URL；证据不足时明确说不足。
- 与订阅/推送无关的闲聊：模型直接文字回答（不调工具），行为与现 chat_reply 一致。
- 单消息由 maxTurns（默认 20）和统一 20 次工具执行熔断器保护；相同成功调用不重复，
  网络超时/429/5xx 最多自动重试两次，同一错误签名连续两次终止该分支并基于已有证据回答。
- 会话：同一 owner 30 分钟内的消息共享一个 agent 会话（多轮上下文），超时新开。

意图工具包三阶段发布已退役。生产 Owner 工具面现在由 composition root 固定为小型正交
Agent-first catalog，不再通过 shadow、owner canary 或 allow-all 配置切换；A2A 与 Web
兼容入口各自显式装配，不能回退到 Owner catalog。

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

-- 历史待确认动作兼容表：仅收敛升级前已经发出的卡片，新请求不得写入
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
// CommitAgentSessionTurn 以 base fence 原子提交整轮 legacy 投影、完整快照事件批次与 shadow audit。
// 不再暴露可绕过事件账本的公开覆盖入口。
func (s *Store) CommitAgentSessionTurn(ctx context.Context, projection agentledger.SessionProjection, batch agentledger.AppendBatch) (agentledger.ProjectionShadowAudit, error)
// CreatePendingAction 落一条待确认动作。
func (s *Store) CreatePendingAction(ctx context.Context, a *types.PendingAction) error
// ClaimPendingAction 原子领取：status='pending' AND expires_at>now() AND user_id=$userID
// 才置为 executed 并返回实体；否则返回 ErrNotFound 类错误（含已执行/已过期/不存在/非本人，
// 幂等防双击）。归属校验进 WHERE 谓词（§10）：越权请求零副作用。
func (s *Store) ClaimPendingAction(ctx context.Context, id string, userID int64) (*types.PendingAction, error)
// CancelPendingAction pending → cancelled（同样带 user_id 谓词）；非 pending/非本人返回 ErrNotFound 类错误。
// RETURNING 返回实体：卡片回调回写通告需要 Summary。
func (s *Store) CancelPendingAction(ctx context.Context, id string, userID int64) (*types.PendingAction, error)
// CommitAgentSessionAppend 供卡片回调、反馈通告等旁路写入：同一事务追加 legacy JSONB、
// 写入完整快照事件批次并校验 shadow；operationIdentity 的精确重放不重复，不同正文冲突。
// 刻意不刷 updated_at——确认卡有效期（24h）远超会话 TTL（30min），点卡不得复活超时会话。
func (s *Store) CommitAgentSessionAppend(ctx context.Context, userID int64, sessionID int64, operationIdentity string, msgs json.RawMessage) (agentledger.ProjectionShadowAudit, error)
```

### B2 耐久边界

卡片回调和反馈通告在 ingress 仍是 best-effort 异步旁路：若进程在事务开始前退出，
B2 不承诺从业务事实重新发现并补写会话。B2 保证的是一旦旁路事务提交，
legacy JSONB 与事件账本完整快照同成同败；提交响应丢失后，以稳定
`operationIdentity` 精确重试不会重复写入。该身份既是当前反重复边界，也是未来恢复接口，
不是耐久恢复本身。业务事实到会话提示的扫描、checkpoint 与断点重试属于 7.10，
不在 B2 范围内。

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

### 4.1 AssistantTurn 归一化边界（7.5，2026-07-24）

`Chat` 的 OpenAI-compatible wire choice 必须先经纯适配器归一化为
`AssistantTurn{Content, ToolCalls, StopReason}`，再投影到兼容的 `ChatResponse`；
Agent loop 本批不迁移。`StopReason` 的零值与未知 wire 值均为 `unknown`，已知值为
`stop/tool_calls/length/content_filter`。

请求声明 tools 时，只有 `stop_reason=tool_calls` 才能产生可执行 `ToolCalls`，且该 reason
必须至少有一条调用；`length/content_filter/unknown/stop` 携带调用或
`tool_calls` reason 无调用均以 `ErrToolProtocolResponse` fail-closed。每条调用必须满足：
`type=function`、非空且批内唯一的 id、非空 function name、arguments 为合法 JSON object。
任一项失败则整批调用不可见、不可执行。`Chat` 只返回 usage/model/latency 的内部 partial
供 `DoChat` 记账，`DoChat` 记账后只向业务调用方返回 error。

不声明 tools 的收尾请求没有可执行工具面：忽略 wire `tool_calls`，保留正文与归一化 stop
reason，避免 provider finish_reason 漂移误触发工具。DeepSeek V4 的全角 DSML marker 仍按
已知 provider+请求/响应 model 组合 fail-closed（包括 native+DSML 混合）；其他 provider
的普通正文不因同样 marker 被全局误杀。历史会话的 DSML 清洗规则不变。

可回放样本位于 `llm/testdata/conformance/assistant_turns.json`；新增 provider 或观察到新的
wire 漂移时先追加脱敏样本，再修改适配规则。

### 4.2 Agent 事件账本全写入口 shadow（7.7-B2，2026-07-25）

`agent_sessions.messages/turn_count/activated_tools` 仍是唯一主读与业务投影。普通飞书
`HandleMessage` 在安全清洗、消息裁剪和模型循环全部结束后，必须用
`CommitAgentSessionTurn` 在一个 PostgreSQL 事务中完成：

1. 以加载时旧投影的 canonical digest 作 base fence，拒绝跨进程或旁路写造成的陈旧覆盖；
2. 追加一个 `vane.agent-session-projection/v1 + full_snapshot` 事件 batch；
3. 更新旧 JSONB 投影；
4. 只取最后一个完整 snapshot generation 重放，与旧投影做 digest/count shadow 对账。

每个 generation 为 `turn_started → 最多 60 条 user_message/assistant_message/tool_call/
tool_result → 可选 confirmation_requested → turn_completed`，总数最多 63，低于 migration 035
冻结的 64 条上限。任一消息、payload、batch 或投影非法时整笔事务失败；不得裁掉事件后仍报告
match。日志只记录 scope、固定 reason 和数量，不记录 payload、消息、工具参数或卡片正文。

卡片确认回调、feedback `NotifyEvent`、task creation receipt 与 definition-edit receipt 也必须
在 Store 内持 exact session 根行锁后，以当前旧投影构造同样的 full snapshot generation，并在
一个事务中完成 ledger append、旧 JSONB 更新和 projector shadow audit。receipt 的
`session_recorded_at/session_messages_digest` checkpoint 与这三步同事务；锁序固定为
callback/Notify=`session`，creation/definition receipt=`receipt → session`。响应丢失重试以
持久 action+resolved verb、已落库 feedback row id 或 receipt id 为 source identity；长身份只做
带 domain separator 的 SHA-256，禁止随机数、时间或消息正文 hash 充当来源身份。相同来源+不同
消息必须 conflict，相同来源+相同消息不得重复追加。

side writer 沿用会话的 60 条裁剪纪律：在根锁事务内保留最早 user 意图，并把最近段边界推进到
user 消息，避免孤儿 tool result；不得在 Loop 预读后拼 projection。旧
`AppendAgentSessionMessages` 生产入口已经退役。definition-edit receipt 角色只额外获得
`agent_sessions.turn_count/activated_tools` 读取和 `agent_events` immutable SELECT/INSERT +
sequence USAGE，不得获得 event UPDATE/DELETE、session 其他列写入或 owner/app 绕行。
task creation receipt 则必须在首次 receipt 根锁前进入 tenant-scoped `vane_app`，复用既有
receipt/session/event RLS 与权限；不得因内部 ledger helper 而借 migration owner 绕过。

callback/feedback 的 ingress 调度仍是 best-effort：B2 不从已提交业务事实扫描并重建一次
尚未开始的旁路事务。B2 的边界是一旦事务提交，legacy 与 ledger 同成同败；提交响应丢失则
用稳定 source identity 精确重试而不重复。该 identity 是反重复与未来恢复接口，不等于
耐久恢复。业务事实→会话提示的扫描、checkpoint 与断点重试属于 7.10。

`agent_sessions.messages/turn_count/activated_tools` 仍是唯一主读；B3 切换、7.8 与 7.10 不属于
本阶段。Agent ledger 也仍不得被 Push、Temporal、task creation 或 definition edit 当成业务真相。

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
// Tool 只承载可执行实现。模型可见声明与受信执行策略由本地 ToolSpec 绑定；
// 远程 MCP/供应商注解不得充当授权源（7.6，2026-07-24）。
type Tool interface {
    // Execute 返回给模型/用户看的结果文本（中文）。参数是模型产出的 arguments 原始 JSON。
    Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error)
    // Summarize 把 args 渲染成确认卡上的人类可读摘要（ConfirmationRequired 工具须有意义实现）。
    Summarize(args json.RawMessage) string
}

// ToolSpec = 模型可见 Definition + 本地受信 Policy + 可执行 Tool。
// Policy.Confirmation=Required 的工具不由 loop 直接执行，走确认卡 / durable proposal。
// Effects 显式声明 internal_read/network_read/billable/state_write/delivery/
// durable_proposal/trust_taint/local_handle_read/activation_write；零值无效。
// 与 compiled runtimepolicy.ToolPolicyV1（空 allowlist）是不同概念，禁止混用 schema。
type ToolSpec struct {
    Tool
    Definition llm.ToolDef
    Policy     ToolPolicy
}

// Deps 注入（main.go 装配）。SessionStore/ActionStore 是 store 方法的窄接口（契约 §2 签名）。
type Deps struct {
    Client   *llm.Client
    Recorder *llm.Recorder
    Store    Store   // 窄接口：§2 全部 6 个方法
    Tools    []ToolSpec
    Model    string        // cfg.LLM.AgentModel
    MaxTurns int           // cfg.Agent.MaxTurns
    SessionTTL time.Duration // cfg.Agent.SessionTTLMinutes
}
func New(d Deps) *Loop          // 装配期校验失败则 panic
func NewChecked(d Deps) (*Loop, error)

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
- system prompt 常量（中文）：声明角色=见微 Vane 助理、只在需要时调工具；明确且唯一的
  owner 直写当轮执行，任务创建/整体编辑/画像更新只生成一次确认卡；真歧义才自然追问，
  禁止要求内部 ID；无关问题直接回答。外部内容注入防护措辞对齐 scorer 的写法。
  **§7.1 增补（2026-07-18 PR-4）**：`Deps.SystemPrompt` 可覆盖该常量（零值回落，飞书轨零行为变化）；
  [用户画像] 段只跟随默认 prompt 渲染（自定义 prompt 的 A2A 轨不渲染——画像是 A2A 非目标）。
- 会话消息即 []llm.ChatMessage 的 JSON；system 消息**不入库**，每次调用时动态前置。
- 模型调用：DoChat，SpanName="agent"，DisableThinking=**true**（实测思维链会与 content
  共享 MaxTokens 并导致复杂 FC 空输出；工具选择在关闭后无退化），MaxTokens 2048，
  Temperature nil（默认）。
- 模型返回未注册工具名：以 role=tool 回错误文本"工具 X 不存在"，继续循环（模型自纠）。
- 一轮多个纯公开只读 tool_calls 可按序执行；公开研究与内部读取/写操作混在同批时整批拒绝。
  普通 owner 直写进入各自幂等执行入口。`create_schedule` / `edit_task_definition` 由专用
  controller 冻结 exact command 并返回一张确认卡，点击后才推进同一 durable saga。
- `web_search`、`read_page`、动态 TikHub 端点、`read_endpoint_result` 和含外部标题的
  `list_sources` 把外部结果送入模型后，本条用户消息进入 **untrusted-result 边界**：
  后续模型请求不再附加动态画像，且不声明内部数据或写工具；仍可继续与当前意图相符的
  `web_search` / `read_page` / 社媒公开只读研究，以及读取本轮实际产生的
  `read_endpoint_result` handle。handle 权限绑定到本消息产生的集合。模型即使幻觉调用画像/
  任务/信源/写工具也只收到固定拒绝，不执行、不创建 pending_action。该边界是确定性 Go
  代码，不以模型是否遵守 prompt 为前提。纯公开只读批次可执行；若同批混入内部读取或
  写操作则整批拒绝。首个外部读取执行后，
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
  也不加载既有 agent session 历史；只根据包装外当前用户后缀决定是否提供绑定查询的
  `web_search`，引用正文不能控制 query、域名或工具参数。首次结果后可继续隔离的公开只读
  核验；内部读取和写工具始终关闭。既有历史只在本轮结束后与固定占位重新合并保存，
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
- 下一条用户消息若明确要求创建任务，且没有要求先搜索、查询、检查或核对，则本消息进入
  `direct-task-creation` 缩面：从数据访问层跳过画像，模型请求只声明 `create_schedule`，
  请求消息只保留当前 user turn（原有已清洗历史在本轮结束后再合并持久化），
  且声明与执行两层都要求该注册工具的真实 effect 为 mutating；同名只读工具不得暴露或执行。
  `approved_fetch_plan.existing_source_ids` 与内部持久化字段 `sources` 在此模式下禁止出现：
  前者防模型猜 ID 触发隐藏 DB 读取，后者防模型猜 `vane://` URL/config/title。
  当前消息明确的新信源必须写入版本化 `approved_fetch_plan.fetch_requirements`
  （`version=vane.fetch-requirements/v1`），只提交 kind 对应的人类可读原始参数；Coordinator
  在 pending 前经 `sourcespec.Build` 原子物化，再走原有 canonical/SSRF/重复 URL 校验。
  任一 spec 非法则整份 proposal 失败，durable args 与 recovery 仍只认严格 `sources`。
  所有模型轨（不只是 direct）都在 Agent 边界禁止提交内部 `sources`；Coordinator
  保留旧 `sources` 入口只供已冻结数据、恢复流程和可信内部兼容。版本化 envelope、
  plan 与每种 spec 的字段名逐字匹配，大小写别名、转义键、重复键、未知字段和显式
  `null` 均拒绝；URL/域名还须在 pending 前拒绝十进制、八进制、十六进制及缩写 IPv4。
  运行时对任何幻觉的其他已注册工具调用也返回固定本地拒绝，不执行、不 taint。
  用户明确说「不要再搜索／直接创建」
  时优先按此缩面；明确否定创建或要求先核对时不得进入。该模式只减少读取能力，不增加权限：
  `create_schedule` 先落 durable proposal，再返回一张确认卡。任何无工具自由文本
  都会被丢弃并回到 clean baseline 自纠一次（不尝试用词法区分“追问”与“口头承诺”），连续两次
  没有真实 proposal 则返回固定的「任务尚未创建」文案；带 tool_calls 的 assistant content 也
  强制清空，避免参数校验失败时把同批的「卡已生成」承诺留进历史。整轮无论是否成功，
  会话只持久化当前 user + 确定性回复，不保留动态校验 tool result（工具审计仍走独立账本），
  避免通用 fail-closed 清洗把本地拒绝误记成外部查询。此规则避免外部发现完成后的创建消息
  又被 `list_sources` 带回 untrusted-result 边界，形成「创建→再读→隔离→口头承诺」循环。
  direct 请求 schema 只暴露且强制 `fetch_requirements`；每个 assistant 响应必须恰有一个
  `create_schedule` 调用，多调用整批零执行。direct 模式独立限制为
  `min(agent.max_turns, 4)`；create_schedule 参数校验（含本地精确字段门）最多失败两次，
  第二次仍失败即返回固定「任务尚未创建」文案，不能把全局 20 轮当成付费猜格式预算。
- 普通 turn 达 MaxTurns：Reply="这个请求步骤太多，我先停下来了，请把需求拆小一点再试"；
  direct-task-creation 达独立四轮上限则返回固定「任务尚未创建」文案。
- 全部 LLM 错误向上抛（feishu 层 humanize）。

## 8. agent 工具集（`agent/tools.go`，构造函数 `BuildTools(...) []ToolSpec`）

决策面以 `ToolPolicy` 为准（7.6）；`Effects` 必须完整声明副作用，
`EffectDirectOwnerWrite` 表示明确 owner 意图下直接执行。A2A 只读面只按
`AuthorizationA2AReadOnly` 过滤。

| 工具 | 执行方式 | 关键 Effects | 底层调用 | 说明 |
|---|---|---|---|---|
| list_schedules | 直接 | internal_read | store.ListSchedulesByUser | 中文列表文本；A2A 可读 |
| create_schedule | Prepare → Execute | durable_proposal + state_write + direct_owner_write | CreationController.Prepare/Execute → creation saga | `{spec,intent,approved_fetch_plan:{fetch_requirements},nl_description?,strictness?}`；只接收 `vane.fetch-requirements/v1` 人类可读抓取要求 |
| remove_schedule | 直接 | state_write + direct_owner_write | scheduler.DeletePushIdempotent | `{schedule_ids:[string]}`，1–20 个，去重保序 |
| run_task_now | 直接 | delivery | TaskRunTrigger.RunTasksNow | `{schedule_ids:[string]}`；按任务冻结定义运行 |
| view_profile | 直接 | internal_read | store.GetProfile | 仅画像意图首轮曝光 |
| update_profile | 直接 | state_write + direct_owner_write | store.UpsertProfile | 首次创建画像 |
| view_task_playbook | 直接 | internal_read | playbook store | 读取选中任务手册 |
| edit_task_definition | Prepare → Execute | durable_proposal + state_write + direct_owner_write | DefinitionEditController.Prepare/Execute → edit saga | 整体编辑立即推进；语义不足时自然追问 |
| web_search | 直接 | network_read + billable + trust_taint | fetcher.ExaFetcher.Search | 一次性语义搜索，不建任务或持久抓取状态 |
| read_page | 直接 | network_read + billable + trust_taint | fetcher.ExaContentsFetcher.ReadPage | 一次性读取指定页面正文 |
| search_endpoints | 直接 | activation_write | 本地社媒工具目录 | 搜索并激活 provider-neutral 动态工具 |
| read_endpoint_result | 直接 | local_handle_read + trust_taint | 本轮结果缓存 | 仅产生绑定 handle 后曝光 |

旧模型契约 `update_schedule` / `edit_task_playbook` / `set_task_strictness` 已删除。

**§8 增补（2026-07-20，Boss 拍板）**：web_search / read_page 解决「临时查一下」被迫
add_source 的形态（信源固定点反模式——一次性需求不该走订阅设施）。两工具按次计费，
Agent 工具行与 fetcher 上游行共享同一 trace_id/user_id，tenant_id 由 membership 推导；
普通对话复用本条消息 trace，确认卡执行入口在真实执行前生成独立非空 trace；
fetcher 层上游行按 `SourceID=0` 无源口径落 tool_calls，归属元数据只在本地 context 传递，
不得进入 Exa 请求体或请求头；两层旁路记账都脱离调用方取消并重新施加 5 秒 deadline，
避免取消窗口撕裂双账本或连接池故障无限阻塞；两条路径均不创建
source/content/content_sources。滚动 24h `agent.exa_daily_cap`（默认 100，从
tool_calls 表按 tool_name IN ('web_search','read_page') COUNT，排除
invalid_args/budget_exceeded，判定失败 fail-closed）仍强制；`agent.exa_msg_cap`
已删除，单消息统一由 20 次工具熔断器管理。不进 A2A 只读白名单（显式名单仍为
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
  Reply 文本用现有 BuildReplyCard 回复。当前生产工具不得产生 `Outcome.Confirm`；
  `Confirm` 分支只用于部署前已发行卡片的兼容收敛。
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
  `fetch_requirements → sourcespec.Build → canonical sources` 在 pending
  前完成，未知/重复/错 kind 字段、SSRF、非法裸域名、canonical 重复或批内任一坏项均整单拒绝，
  durable bytes 与 recovery 不含 `fetch_requirements`，Confirm 逐字消费该冻结计划且不重建。
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

## 13. 7.7-B3 会话主读 authority 与可回滚切换（2026-07-26）

本节只定义 `agent_sessions` retained JSONB 与 `agent_events` projector 之间的主读选择，
不改变消息写入、Agent prompt、工具协议或确认后的执行语义。切换控制面只有数据库直连的
`runtimeadmin agent-session-cutover`；HTTP、飞书、Agent、A2A 与 Temporal 均不得暴露
activate/rollback 入口。

- authority 以 exact `(tenant_id,user_id,session_id)` 为作用域，只有 `legacy` 与 `ledger`
  两种解析状态。无 authority event 时等价于 `legacy`，Store 按 retained JSONB 主读；
  最新 `activate` event 解析为 `ledger`，最新 `rollback` event 解析为 `legacy`。
  rollback 只能追加不可变 event，不能删除、覆盖或倒写历史。每代记录的 ledger
  high-watermark 必须恰好落在完整 batch 末端；Store 必须重放该前缀并证明记录 digest
  等于其最新完整 snapshot，不能只检查两个存储 digest 彼此相等。
- `route=ledger` 是 fail-closed 权限，不是“尽量从 ledger 读”。零事件、批次不完整、sequence
  不连续、digest/codec 损坏、投影失败，或 ledger projector 与同事务冻结的 legacy replica
  不 exact match 时，读必须返回安全完整性错误；严禁静默回落 JSONB。
- 所有会话 writers（普通 turn、callback/feedback side writer、task creation receipt、
  definition-edit receipt）必须在同一事务内读取并锁定 authority，以 authority 选出的
  authoritative base 构造下一代完整 snapshot；不得始终拿 retained JSONB 当 base，也不得
  在 authority 检查前写任一投影。`route=ledger` 下仍原子维护 legacy replica，作为审计与
  exact rollback 前置证明，而不是主读。
- activate 仅允许当前 route 为 legacy、ledger 可完整投影且与 legacy replica exact match
  时追加 `route=ledger` 事件；rollback 仅允许当前 route 为 ledger、当前 ledger 投影仍与
  legacy replica exact match 时追加 `route=legacy` 事件。任一不匹配都保持原 authority，
  不得用 rollback 掩盖损坏。重复同一目标 action 只能 exact replay，不能额外推进 generation。
- `runtimeadmin agent-session-cutover` 强制 positive `-tenant/-user/-session` 和
  `-action status|activate|rollback`；activate/rollback 必须显式
  `-confirm-cutover`，status 反而拒绝 confirm。stdout 只输出 exact scope、route、
  generation/event 等安全 carrier；错误只输出 `AppError.Message` 或固定兜底，不得泄露
  messages、event payload、prompt、tool 结果、activated tools 或 legacy replica 正文。
- 本批明确排除 7.8 ContextBuilder/Turn Snapshot 与 7.10 durable continuation：
  不重排上下文、不新增 Agent 自动回续、不扫描业务事实、不改变 callback/feedback
  best-effort ingress，也不重放任何 provider side effect。

## 14. 7.8-A ContextBuilder v1 与 immutable Turn Snapshot shadow（2026-07-26）

本批只观察未来上下文编译结果，不切换模型主请求。`agentcontext` 是 stdlib-only、
provider-neutral 包；固定版本为 `vane.agent-context/v1`、快照
`vane.agent-turn-context-snapshot/v1`、compactor `none/v1`。

- system、实际有序 tool definition、本地可信 ToolPolicy 与 output reserve 全部进入
  确定性预算。v1 未绑定 provider tokenizer，所有文本按 UTF-8 byte length 计费，作为
  “每个 token 至少消费一个输入 byte”的可证明上界。工具定义及 policy 按当轮请求顺序完整冻结；顺序改变必须改变
  `toolset_digest` 和 candidate digest。
- history 只按完整 user turn 原子组保留或省略，不能切开 assistant/tool 协议。
  tool_call_id 孤儿、重复、缺回执或越序一律拒绝编译。历史从新到旧纳入预算；
  最初 user intent 只有在整组仍能容纳时才保留。v1 不接受或生成 summary。
  `kept_ranges/omitted_ranges` 的边界只表示 outbound request message ordinal，
  不冒充 ledger event sequence。
- `untrusted_current` 原文只参与瞬时预算与不可逆 SHA-256 绑定；候选消息只保留固定
  placeholder，`replayable=false`。快照字节、错误和日志都不得包含外部原文。
- Loop 在每次 `chatFn` 前同步构造 legacy `ChatRequest` 和纯内存 candidate，把同一个
  request 原样发给模型；仅在 `chatFn` 返回后才以 `WithoutCancel` + 2 秒独立预算异步
  best-effort seal。最多四个 seal 并发，满载时允许丢 shadow sample，绝不排无界队列，
  因而这不是完整审计日志。build、route、ledger 或 INSERT 失败只写安全结构化 warning，
  慢 Store、调用方取消及 shadow 失败均不改变 request、Outcome、工具协议或旧会话写入。
  direct-create 的历史裁面、taint
  二阶段工具集和 pending final 各自形成独立 `context_step`；该序号只标识候选，
  不等价于 `llm_calls`，pending final 是未调用模型的 synthetic post-outcome candidate。
  RunOnce/A2A 只做内存编译，
  不写 owner session 快照。
- `agent_turn_context_snapshots` 只允许在 exact session 当前为 B3 `ledger` route 时写入。
  Store 必须先锁 session root，完整验证 authority history、ledger batch 与 legacy replica，
  再自行冻结 `seal_authority_generation`、`seal_ledger_head/event id` 与
  `seal_ledger_projection_digest`。这些字段只表示 seal 执行时已耐久的水位，不证明
  candidate 的 causal input base、不声称模型已消费到该 ledger head，也严禁据此自动
  replay/resume；7.8-B 主读前必须另加付费调用前的 causal fence/authority。
  legacy route 明确 `Skipped` 零写；相同 scope/turn/step+digest exact replay，不同 digest
  conflict。表启用 restrictive tenant RLS；`vane_app` 只有 SELECT、指定列 INSERT 与
  sequence USAGE，无 UPDATE/DELETE/TRUNCATE；非空历史拒绝 Down，Tenant purge 子表先删。

7.8-B 才能把 candidate 变成模型主读；本批没有宣称 token 预算、裁剪或材料引用已影响
生产回复，也没有增加 LLM summary、长期记忆检索或 7.10 自动 continuation。

## 15. 7.10-B2/B3 DB-local durable action continuation

本节覆盖普通 Agent 新建的 `enable_source` 与 `remove_source` 确认卡。两者不再先落
execution_version=0 的 generic pending action；Agent 必须经唯一
`ActionProposalController` 在一个 PostgreSQL 事务中同时提交：

1. execution_version=2 的 pending root；
2. 完整冻结的 tool spec、ToolPolicy、canonical args、adapter 和两种终态会话事实；
3. generation-1 `durable` authority event。

相同 ActionID 的提交响应丢失只能用相同 root、冻结字节和 authority 精确只读收养；
部分行、摘要/参数漂移、租约或终态污染一律 integrity failure，不做修补或 v0 fallback。
确认/取消先由 v2 controller 判定；只有 Store 明确证明 execution_version 不是 2 才能进入
旧协议。

`enable_source` 保留 B2 的单目标 `{"source_id":N}`、adapter
`vane.enable-source/postgres/v1` 以及既有
`agent-action:enable-source:<ActionID>` 幂等身份字节。后续版本不得因 Go 工具名使用下划线
而改写这条已部署身份。

`remove_source` 使用严格 `{"source_ids":[...]}`，输入 1–20 个正整数，去重但保留首次出现
顺序；未知/重复字段、`null`、旧单数字段和非法目标都在 proposal 前拒绝。完整目标集留在
canonical args，`source_id` 只保留第一项作为兼容 carrier。冻结 adapter 为
`vane.remove-source/postgres/v1`，幂等身份为
`agent-action:remove-source:<ActionID>`。

continuator 只能在受限 `vane_agent_action_continuator` 角色和 exact tenant context 中执行。
`remove_source` 的所有目标必须用一条带 `(tenant_id,user_id)` 谓词的 DELETE 在业务 effect
事务内完成；终态 checkpoint 与固定会话事实和该 DELETE 同成同败。删到一条以上记
`removed`；全部已不存在记 `not_subscribed`，两者都属于幂等完成。信源和历史内容不删除，
其他租户或用户对同一信源的订阅不受影响。该角色只有 subscriptions DELETE，没有 INSERT、
TRUNCATE 或 proposer 权限；migration 070 有 durable remove action 时拒绝降级。

本批仍明确排除：

- `add_source` 的外部 probe、`remove_schedule` 的 Temporal 副作用等跨系统工具；这些必须先
  单独设计 durable saga；
- 动作完成后再次付费调用模型的自动回续；需先冻结 token/cost/causal authority；
- 将所有 `ConfirmationRequired` 工具自动视为 durable。支持列表必须由本地代码显式维护，
  未列入工具继续走既有协议或 fail-closed。

最低验证包括：真 PostgreSQL 18 批量删除、跨 Tenant/User、确认与投影重放、租约/fence、
会话事实冲突导致 effect 整体回滚、取消/过期/损坏、070 权限矩阵与有数据降级拒绝，以及
既有 `enable_source` 终态事实的原字节重放。

### 15.1 7.10-B4 删除动作去确认卡

Boss 于 2026-07-28 取消 `remove_source` / `remove_schedule` 的二次确认卡。用户只需
说出自己记得的标题、主题、平台、时间或任务详情，Agent 先调用只读列表工具解析内部
ID；唯一匹配时直接执行，多个合理候选才按人能看懂的名称自然追问，禁止要求用户查 ID，
也不生成 pending action 或确认卡。退订执行使用一条同时带
tenant、user 和完整 source id 集合谓词的 DELETE，整批原子、重复调用幂等；信源与
历史内容不删除。删除任务复用 scheduler durable command。`EffectDirectOwnerWrite`
只允许 owner-only、`ConfirmationNone` 的 state write，A2A 仍不可见。

B3 及更早已经发出的 `remove_source` 卡片继续按冻结的
`vane.remove-source/postgres/v1` 字节执行或重放；B4 不改写旧 action、authority、
adapter、operation identity 或终态事实。

### 15.2 7.10-B5 全写操作无确认 + 多名称批量删除

Boss 于 2026-07-28 明确取消 Agent 全阶段确认机制。`BuildTools` 中所有生产工具的
`Confirmation` 必须为 `none`；任何新增生产工具若重新引入
`ConfirmationRequired`，policy golden 必须失败。`pending_actions`、确认卡渲染和
callback API 仅用于兼容已发出的历史卡片，不得成为新请求入口。

自然语言是用户入口，内部 ID 只是模型调用列表工具后的定位结果。一个请求点名多个信源
或任务时，Agent 必须完整解析全部名称/描述并发起一个批量删除调用：

- `remove_source.source_ids`：1–20，去重保序，单条 tenant/user scoped DELETE 原子执行；
- `remove_schedule.schedule_ids`：1–20，去重保序；每个目标使用由
  `(user_id,schedule_id)` 派生的稳定 idempotency key 调用 durable schedule command，
  中途失败后的整批重试会从已完成目标安全续进；
- 某个描述有多个合理候选时只追问该歧义，展示人类可读名称，不要求 ID；无歧义目标不再
  额外确认。

`add_source`、`enable_source`、`update_profile` 作为 owner-only DB 写直接执行。
`create_schedule` 与 `edit_task_definition` 不绕过既有控制面：先 `Propose` 冻结 exact
command，再用 operation ID 构造 server-owned `agent_auto/v1` receipt 立即
`Confirm`，由同一个 durable coordinator 继续执行/恢复。终态历史写成
`[Agent执行]`，不得伪造“用户点击确认卡”。
外部内容 taint 后禁止同轮写、
A2A 只读白名单、tenant/user 归属校验、预算和幂等边界全部保持不变。
`run_task_now` 与周期调度开关是两个正交操作：它不 Patch/Trigger 周期
Schedule，而是用耐久 run command ID 启动唯一的一次性工作流。active 与 paused
任务都可手动运行；paused 时只有与该 command 精确绑定的工作流可通过快照和后续
副作用授权，周期 Schedule 的 paused 状态、cron、时区和下一次触发时间均不得改变。
响应丢失时重试同一个工作流 ID，并以 `AlreadyStarted` 作为已应用事实收敛。
