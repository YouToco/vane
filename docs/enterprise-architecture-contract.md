# 企业级三接缝契约 — 多租户 / 多推送渠道 / 信源 SDK

> 草案 2026-07-18（Mac 端）。**本文是接缝定义与迁移路径的契约,不是排期计划**。
> 依据:业内调研 5 路（OpenClaw 源码实读 + Bot Framework/Apprise/Courier/Novu/HumanLayer
> 渠道抽象 + Postgres RLS/pgx 多租户 + Airbyte/Singer/Steampipe 连接器 SDK）+ vane 现状
> file:line 级耦合面测绘。关键论断均带一手来源，见各节「依据」。
> **本文含 6 个待拍板问题（§6），未拍板前不得开工实现。**

## §0 定位:三条接缝与它们的依赖顺序

### §0.1 为什么是「接缝」而不是「功能」

多租户 / 多渠道 / 多信源是**横切**的:三者都碰 agent loop、store、feishu 包。
若按功能分工（一台机器做多租户、另一台做渠道），两台机器会同时改同一批核心文件，
必然陷入合并地狱。本契约的组织原则是:

> **先把接缝定成 interface 并合进 main,再在 interface 背后并行。**
> 并行度 = 接缝的稳定度。接缝未合并前，任何扇出都是伪并行。

### §0.2 三接缝与硬性依赖顺序

| 接缝 | 内容 | 依赖 |
|---|---|---|
| **② Tenant** | 租户身份贯穿、数据隔离、per-tenant 凭证与配额 | **无（地基，必须先行）** |
| **① Channel** | 推送/交互渠道抽象、能力协商、审批降级 | 依赖 ②（渠道配置、身份、凭证都挂租户） |
| **③ Source SDK** | 信源连接器声明式化 | 弱依赖 ②（配额归租户）；可与 ① 并行 |

**② 必须先行**的硬理由（非偏好）:
渠道配置（「这个租户开了哪些渠道」）、渠道身份（`channel_identities`）、
per-tenant 凭证（每租户自己的飞书应用/API key）、配额（跨租户 DoS 防护）
四者全部挂在租户上。先做 ① 会产出一套需要立刻返工的单租户渠道层。

**但 ② 有一个可先行的零风险子项**（见 §1.1），两台机器可在它之后立即扇出。

## §1 接缝②:多租户

### §1.1 第一刀:principal 从「全局解析」改成「从请求上下文解析」

**现状（测绘实证）**:`api/owner.go:20-49` 的 `ownerUserID` 从 `settings.feishu_owner`
读全局单行拿到 principal，**与请求身份完全无关**；同一段逻辑被逐字复述三份
（`a2a/chat.go:97-120`、`cmd/gate/main.go:182-216`，后者注释自认「与 api.ownerUserID 逐字一致」）；
八个 API 端点各自调用它（subscriptions/push/schedules/deliveries/profile/observability）。
Dashboard 只有一个共享密码（`api/auth.go:38`），无用户表参与鉴权。

**契约**:
1. principal 解析收敛为**一处** `auth.PrincipalFromContext(ctx) (Principal, error)`，
   `Principal{TenantID, UserID}`。三份复述全部删除，改为调用它。
2. 单租户期该函数返回固定 `{TenantID: 1, UserID: ownerUserID}`——**行为完全不变**，
   但调用点从此拿的是「上下文里的 principal」而非「全局 owner」。
3. 这一步**零行为变更、零迁移、零风险**，是整个接缝②里唯一可以立刻做且不阻塞任何人的动作。

> **在此之前加任何 tenant_id 列都是装饰**:列加了但 principal 仍是全局单例，隔离不成立。

### §1.2 表分级矩阵（写第一行迁移 SQL 前必须定死）

**依据**:vane 的 007 内容身份契约已论证「同一篇内容全局一份」是 dedup 与 TikHub
付费闸门（`fetcher/tikhub.go` 的 `EnrichedCanonicalKeys` 显式不带 source_id）的立论基础。

| 类别 | 表 | tenant_id | 理由 |
|---|---|---|---|
| **客观事实（共享）** | `sources`、`content_items`、`content_sources`、`page_snapshots` | **不加** | 共享抓取是设计意图。`sources.url` 全局 UNIQUE（007:19-22）正是为「多用户加同源 → 重复抓取 + 重复付费」而设；`content_items.canonical_key` 全局 UNIQUE（007:158）承载跨源去重与付费闸门 |
| **租户所有** | `users`、`subscriptions`、`push_batches`、`deliveries`、`feedbacks`、`profiles`、`schedules`、`agent_sessions`、`pending_actions`、`llm_calls`、`tool_calls`、`a2a_tasks` | **必须加** + RLS | 逐表理由见 §1.3 |
| **必须重构** | `settings` | 结构改造 | 现为 `(key PRIMARY KEY, value JSONB)` 全局单行 KV（002:11-15），须改 `PRIMARY KEY (tenant_id, key)` |

**不变量 I-T1（红线）**:
> 严禁为了多租户给 `canonical_key` 加租户前缀。那会一次性摧毁全局去重与 TikHub
> 付费闸门——同一篇小红书笔记会被 N 个租户各付费补全一次。
> 「谁能看到这条内容」由 `subscriptions`/`deliveries` 的租户维度表达，**不由内容身份表达**。

**CI 守卫**:契约附一张「表 → 隔离类别」矩阵，新建表未登记 → 构建失败。

### §1.3 隔离机制:应用层显式过滤 + RLS 兜底（双层）

**依据（一手）**:schema-per-tenant 被 Atlas/GopherCon 2025 当事人称为
「one of my biggest regrets」——迁移时长随租户数线性增长、schema 漂移是
「needle in haystack」。vane 已有 16 个 goose 迁移，扇出代价不可承受。

**选定**:单表 `tenant_id` 列（主机制，应用层显式过滤）+ Postgres RLS（兜底网）。

RLS 有**五个签名级细节**，缺一即静默失效:

```sql
-- ① FORCE：不加则表 owner（通常就是应用连接角色）完全绕过策略。
--    这是「策略写了却不生效」的头号成因。
ALTER TABLE deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE deliveries FORCE  ROW LEVEL SECURITY;

-- ② 显式 WITH CHECK：FOR INSERT 只认 WITH CHECK，缺了则租户能把行写成别人的 tenant_id。
-- ③ RESTRICTIVE：AND 语义，保证以后任何新增 PERMISSIVE 策略都无法意外放宽隔离。
-- ④ current_setting(..., true) 的 missing_ok：未设 GUC 时返回 NULL → 谓词为假 → 默认拒绝。
-- ⑤ (SELECT ...) 包裹：把逐行求值的 SubPlan 提升为只求值一次的 InitPlan。
--    PlanetScale 实测 cost 34,828 → 10,095、latency 1.96s → 102ms（~19x）。
--    注意：即使函数标了 STABLE 仍然需要包。
CREATE POLICY tenant_isolation ON deliveries AS RESTRICTIVE
  USING      (tenant_id = (SELECT current_setting('app.tenant_id', true))::bigint)
  WITH CHECK (tenant_id = (SELECT current_setting('app.tenant_id', true))::bigint);
```

**索引形状**:租户所有表的索引首列必须是 `tenant_id`。现有 `ListUnpushedByUser` /
`ListRecentSimhashesByUser` 等以 `user_id` 打头的索引改为 `(tenant_id, user_id, ...)`。
数据量小时性能差异不显著，但形状要一次改对，事后 rebuild 更贵。

**双角色双池（必做）**:
```sql
CREATE ROLE vane_admin LOGIN;                 -- 拥有表，跑 goose 迁移 + 全局抓取写共享表
CREATE ROLE vane_app   LOGIN NOBYPASSRLS;     -- 不拥有任何表，业务连接
```
现状是同一 DSN 跑迁移与业务（`store/migrate.go` 单独开连接但同角色）。
分离后「RLS 真的在生效」才可被验证。

### §1.4 连接层:只能在事务内 `set_config`，禁用 AfterConnect

**依据（一手）**:pgx 作者 jackc 本人裁定（issue #288）——`AfterConnect` 会
**永久污染连接**，等于每租户一个连接池。

```go
// package tenantdb —— 全仓唯一持有 *pgxpool.Pool 的地方
type Pool struct{ pool *pgxpool.Pool }          // 字段不导出
type Conn struct{ tx pgx.Tx; tenant TenantID }  // 只能由 InTenant 构造

func (p *Pool) InTenant(ctx context.Context, t TenantID,
    fn func(context.Context, *Conn) error) error {
    // BEGIN → SELECT set_config('app.tenant_id', $1, true) → fn → COMMIT
}
```

**另一个 pgx 专属坑**:pgx 默认缓存 prepared statement，RLS 辅助函数若误标
`IMMUTABLE` 会返回**上一个租户的数据**（issue #2007 真实事故）。一律标 `STABLE`。

### §1.5 Go 侧「忘记传 tenant」的编译期拦截

业界文章普遍只做到运行时（`GetTenant(ctx)` 失败返回错误，或直接 panic）。
Go 没有 phantom type，但**可见性**能做到真正的编译期强制:
`*pgxpool.Pool` 关进 `tenantdb` 包且字段不导出，查询方法只挂在 `*Conn` 上，
`Conn` 只能由 `InTenant` 交出 → **拿不到 Conn 就写不出查询**。

**代价与节奏（诚实说明）**:`store` 包有 ~25 个文件、上百个 `func (s *Store)` 方法，
不可能一个 PR 改完。契约规定**棘轮式分批迁移**:
1. 建 `tenantdb` 包 + `Conn`；`Store` 保留，内部改为委托；
2. 新方法一律挂 `Conn`，旧方法逐批搬；
3. CI 守卫用**只减不增的白名单**（allowlist）锁棘轮:白名单里是尚未迁移的旧方法，
   PR 只能从白名单删条目、不能加。

### §1.6 Temporal:context 不跨边界，必须用 ContextPropagator

**依据**:`context.Context` 的值**不跨 Temporal 边界**（走序列化 Header）。
SDK 机制是 `workflow.ContextPropagator` 四方法接口（`Inject` / `Extract` /
`InjectFromWorkflow` / `ExtractToWorkflow`），四个都实现值才能 Client → Workflow → Activity 全程传下去。

**最易漏的一条**:WorkflowID 必须加租户前缀。现有 push workflow 按 user 维度命名，
多租户后跨租户 WorkflowID 冲突会被 `WorkflowIDReusePolicy` **静默拒绝或复用旧执行**
——推送直接丢失，且 **RLS 兜不住**（不在 SQL 层）。

### §1.7 per-tenant 凭证与配额

**凭证**:一张 `tenant_credentials(tenant_id, provider, key_version, kek_id,
wrapped_dek, nonce, ciphertext, ...)`，信封加密 + **AAD 绑定 tenant_id**（密文行本身
不可跨租户搬运，成本为零，必做）。KEK 抽象为接口:
```go
type KEK interface {
    Wrap(ctx context.Context, dek []byte) ([]byte, error)
    Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
}
```
vane 无 AWS KMS，先用 local 实现（宿主机文件 0400），以后换阿里云 KMS 不改调用方。

**配额**:vane 栈无 Redis（Postgres + Temporal + Caddy），**不要为限流引入 Redis**。
用 Postgres 单行原子 UPDATE 做 token bucket:
```sql
CREATE TABLE tenant_quota (
  tenant_id BIGINT NOT NULL, bucket TEXT NOT NULL,   -- 'llm_tokens'|'push'|'fetch'|'tikhub_calls'
  tokens DOUBLE PRECISION NOT NULL, rate DOUBLE PRECISION NOT NULL,
  burst  DOUBLE PRECISION NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, bucket)
);
```

**最该先做的是并发隔离**（跨租户 DoS 面，测绘实证）:
- `llm/client.go` 的 `MaxConcurrent=5` 信号量被打分/出卡/agent/深挖/A2A 共用。
  `agent/loop.go` 注释已实证「5 条卡死消息即可瘫痪全部 LLM 面」——多租户下这是跨租户 DoS。
- `store.go` 的 `pgxpool MaxConns=10`:一个租户的批量推送即可占满。
- `tool_calls` 的 TikHub 每日限额（`store/toolcalls.go`）**当前是全局计数**，
  注释「单 owner MVP 下二者等价」正是本接缝要撕开的地方。
- `a2a/a2a.go` 的 `maxConcurrentExecutions=8` 注释自认「自伤，非跨租户互伤」，多对端接入即失效。

### §1.8 已知越权洞（接缝②打开当天即生效，必须同批修）

测绘发现两处**明写**的「不校验归属」:
- `api/schedules.go:102-118` `handleDeleteSchedule` —— 注释「单 owner：所有调度同属一人」，
  `DeletePush(id)` 无归属过滤。
- `agent/tools.go:552-566` `removeScheduleTool` —— 同源注释，同样无过滤。

反例（做得最好、可作范式）:`store/agent.go` 的 `ClaimPendingAction` /
`CancelPendingAction` 把归属校验放进 WHERE 谓词内，越权请求完全无副作用。

### §1.9 在线迁移五步（每步可单独回滚，不停服）

vane 的处境是**最容易的一种**:单租户 ⇒ 所有存量行 `tenant_id` 常量 1，回填是纯常量写。

1. `ADD COLUMN tenant_id BIGINT`（PG11+ 秒级，不重写表），应用双写；
2. 分批回填（按主键区间，每批单独提交；单条大 UPDATE 会长持锁 + 撑爆 WAL）；
3. `ADD CONSTRAINT ... CHECK (tenant_id IS NOT NULL) NOT VALID` → `VALIDATE CONSTRAINT`
   （**不要直接 `SET NOT NULL`**，会全表扫 + 持 AccessExclusive）；
4. `SET NOT NULL`（已有 validated CHECK 时是廉价操作）；
5. RLS 灰度开关。

**goose 两个陷阱（必踩）**:
- `CREATE INDEX CONCURRENTLY` / `VALIDATE CONSTRAINT` 需要文件头写 `-- +goose NO TRANSACTION`
  （goose 默认把每个迁移包进事务；不加则前者直接报错、后者退化为阻塞式）；
- plpgsql 函数体内的分号会被 goose 分号切分器切碎，须用 `-- +goose StatementBegin/End` 包住。

### §1.10 RLS 兜不住的三类路径（单独立契约）

- **视图**:普通视图以 owner 权限执行 ⇒ 读穿 RLS。PG15+ 必须 `WITH (security_invoker = true)`。
- **物化视图/聚合表**:「导出的数据完全失去 RLS 保护」。vane 的
  `store/observability.go`、`runstats.go`、`deliveries_history.go` 三处聚合读若做成物化视图，
  等于把跨租户数据摊在无保护表上。对策:聚合结果表自带 tenant_id + RLS，刷新语句显式 `GROUP BY tenant_id`。
- **外键**:父表被 RLS 挡住时，子表插入报「违反外键约束」而非「权限不足」。
  须把 `users` 主键扩为同时有 `UNIQUE (tenant_id, id)`，子表建复合 FK。**事后补极痛，一次做对**。

## §2 接缝①:Channel 抽象

### §2.1 现状判断:物理边界干净，语义边界脏

**物理**（好消息）:除 `feishu` 包外**零处** import lark SDK；
`pusher`/`workflow`/`feedback`/`cardgen` 已用窄接口 + 函数注入把飞书隔在外
（`FeishuSender`/`FeishuManager`/`CardBuilder`），import 环被 M5 契约 §8.2 封死。
⇒ **接缝①不需要拆包，只需给现有窄接口换概念**。

**语义**（三处真渗透，必须点名）:
1. **收件人模型 = 一个飞书 open_id**:从 `users.feishu_open_id`（001:24，UNIQUE NOT NULL，
   无 channel 维度）一路穿到 `pusher.Push(ownerOpenID)` → `FeishuManager.OwnerOpenID()` →
   `feishu.SendCard(openID)`。
2. **投递回执 = `deliveries.feishu_message_id`**:是列名（001:117）、索引名（006:23）、
   追问反查的唯一钥匙（`feedback/question.go:59`）。语义上它就是「渠道侧消息 ID」。
3. **最深的一处**:`systemPrompt`（`agent/loop.go:31-37`）用自然语言**向模型承诺了
   「会出确认卡」这个渠道能力**；`pending_actions` 表是该承诺的持久化；
   `RunOnce` 对 Confirm 直接报错——**代码已自认「没有卡片通道的渠道跑不了这套」**。

### §2.2 统一事件信封（依据:Bot Framework Activity）

```go
// package channel
type Event struct {
    Kind       EventKind   // Message | Interaction | System
    ChannelID  ChannelKind // feishu | wecom | slack | email | webhook
    TenantID   TenantID
    Conversation ConvRef    // 会话/群/线程
    From, To   IdentityRef  // (channel, external_ref) 二元组
    Text       string          // 人类可读
    Value      json.RawMessage // 中立业务载荷（程序化）
    ChannelData json.RawMessage // 渠道私有逃生舱：永不参与业务判断
    ReplyTo    *ReplyHandle
}
```

**两条硬约束**:
- `Value`（中立）与 `ChannelData`（渠道原样透传）**分家**，是挡住渠道泄漏的主边界。
  业务逻辑读 `ChannelData` 即违约。
- **`ReplyHandle` 现在就要留字段**:飞书 WS 不需要，但 Slack 的 `response_url`
  （30 分钟内最多 5 次）、webhook 渠道**每事件自带回信地址**。
  ```go
  type ReplyHandle struct{ Token string; ExpiresAt time.Time; RemainingUses int }
  ```
  飞书实现填「立即失效、次数 1」（表示只能同步回）。不留字段以后是破坏性改动。

### §2.3 能力声明:进代码，且不是 bool

**反面教材（一手）**:Bot Framework 有一份完整的 per-channel 能力矩阵，
但它**只存在于文档表格里，运行时没有任何 capability 声明或协商 API**。vane 不得重蹈。

**正面范式**:OpenClaw 的两层能力 + Apprise 的类级声明 + 基类自动降级。

```go
type Capabilities struct {
    RenderLevel     RenderLevel // Rich | Partial | Image | Text（抄 BF 四态）
    MaxActions      int         // 0 = 不支持交互
    MaxLabelLen     int
    TitleMaxLen     int         // 0 = 该渠道无标题概念（抄 Apprise，基类据此把 title 折进 body）
    BodyMaxLen      int
    SupportsMarkdown bool
    Confirm         ConfirmLevel // 见 §2.5
}
```

**`Capabilities()` 必须是方法而非结构体常量**。依据:Opsgenie 的双向短信
「除美国和欧盟外的区域不支持」，且用 sender ID 发送时**根本无法回复**——
即**同一渠道的交互能力会随区域/发送方式/凭证在运行时变化**。

### §2.4 可移植 Presentation + 判别联合 Action（依据:OpenClaw）

业务方产出渠道无关的 `Presentation{Blocks []Block}`，飞书渲染器只负责
`Presentation → card JSON`，邮件渲染器负责 `Presentation → HTML/纯文本`。

**审批必须是独立的 action 类型**，不是塞进 callback value 的字符串:
```go
type Action struct {
    Kind ActionKind // Command | Callback | Approval | Question | URL
    // Approval 专用：核心据此保证 approvalID 与 decision 永不进入降级文本
    ApprovalID string
    Decision   Decision // AllowOnce | Deny
}
```
vane 现在正是把 approval 语义编码进飞书 callback value 的（`onCardAction` 的
`confirm/cancel/fb/fbr` 四类 value 分流），多渠道后会重复三遍。

**降级归核心、原生渲染归渠道**（契约条款，非建议）:
> 若让每个 channel 自己决定「不能渲染按钮时怎么办」，安全不变量会在第三个渠道上被悄悄破坏。
> 硬规则:**不支持的控件降级，而非让整条发送失败。**
> 降级后核心须保留标签作为非交互文本——绝不出现空白消息。

### §2.5 确认能力:分级枚举 + fail-closed 兜底（本接缝最关键一条）

**不变量 I-C1（重述安全承诺，解绑渠道）**:
> 写操作必须有一个**可归属、可过期、单次消费**的 `approvalID` 被显式 approve。
> 该不变量**不要求渠道具备交互控件**。

```go
type ConfirmLevel uint8
const (
    ConfirmNone     ConfirmLevel = iota // 无任何确认通道
    ConfirmLink                         // 仅能送一次性链接（邮件）
    ConfirmKeyword                      // 可解析回复关键字
    ConfirmInteractive                  // 原生交互控件（飞书/Slack/企微）
)
```

**贫渠道确认，业内三条路（优先级明确，无银弹）**:
1. **首选 magic link**:一次性 token + 15–30 分钟过期 + 高熵不可猜 +
   执行前二次认证（邮箱可能被转发/代收，token ≠ 身份）。
   vane 已有 `pending_actions.id` 与 web dashboard，落点齐全。
   **但注意冲突**:magic link 绝不能直接暴露 `ActionID`（内部主键进邮件正文），
   须新增 `token`/`expires_at`/`consumed_at` 三列，token 与 ActionID 不复用。
2. **次选关键字回复**（Salesforce 范式）:`APPROVE/YES/REJECT/NO` **必须在正文第一行**，
   评论放第二行。规则越死越好，**别上 LLM 解析**——Salesforce 的经验是签名档就足以毁掉解析。
3. **兜底直接禁用**:Bot Framework 官方矩阵里 Email/Twilio SMS 的 card actions 就是 `None`。

**`askFallback` 三态（抄 OpenClaw，最短最关键的一条）**:
```go
type AskFallback uint8 // Deny（默认）| Allowlist | Full
```
语义:需要确认但**没有任何 UI 可达**（或提示超时）时的裁决。
**省略即 Deny，读配置失败也 Deny**（OpenClaw 的 `createFailClosedExecApprovalsFallback`
直接返回全 deny）。
⇒ 这一个枚举把「邮件渠道没有交互确认能力」从架构难题降级成 per-tenant × per-channel 配置项。

### §2.6 回调归一:交互回调不是新事件类型（依据:BF messageBack）

**一条中立规则**:按钮点击 / 邮件关键字回复 / magic link 回调 → **产生同一个中立事件**
（`Event{Kind: Interaction, Value: {"approval_id":..., "decision":...}}`）。
收益:`agent.Loop.ExecuteAction/CancelAction` **一行不用改**。这是接缝①里性价比最高的一处设计。

**同批补一个已知缺陷**:vane 的 `CancelAction` **不收拒绝理由**。
HumanLayer 的 `FunctionCall.status.comment` 在拒绝时**回灌给 LLM**——vane 现在等于
丢掉了最有价值的反馈信号。契约要求拒绝路径补 `reason` 并回灌 agent。

### §2.7 身份:一个逻辑 user 挂多条渠道身份

抄 Novu 的 `subscriber.channels[]` 形态（**不需要** Matrix 的 ghost user）:
```sql
CREATE TABLE channel_identities (
  tenant_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
  channel   TEXT   NOT NULL, external_ref TEXT NOT NULL,
  credentials JSONB, PRIMARY KEY (tenant_id, channel, external_ref)
);
```
`users.feishu_open_id` 迁入本表；`deliveries.feishu_message_id` 重命名为
`external_message_id`（语义中立化，需迁移改列名 + 索引名 + `feedback/question.go` 反查）。

**必须预留身份合并**（抄 Matrix double puppeting 的迁移动作，非 ghost 概念）:
必然出现「先在邮件渠道见到一个未知地址、后来该地址被认领为某已有 user」，
需要把孤儿 `channel_identity` 的历史 deliveries/feedback 改挂到真 user。
**该操作必须是特权路径，且必须在租户内进行——跨租户合并一律禁止。**

**provider 无状态（硬约束，抄 Novu）**:Channel 实现内**不许有自己的 DB 访问**，
`pending_actions`/`deliveries` 读写全留上层——否则多租户时每个渠道都要自己做租户隔离，必错。

### §2.8 顺手回收:已在飞书包里的渠道中立逻辑

测绘识别出以下逻辑**渠道中立但放错了包**，抽象时一并上提（非重构洁癖，它们是新渠道的共用件）:
- 入站事件按 id 去重 + TTL（`handler.go:163-179`）→ 框架级 inbound dedup
- `humanizeLLMError`（`handler.go:985-1001`）→ `types` 或 usermsg 包，零飞书成分
- 「goroutine 执行 + select 竞速同步预算 + 超时降级」**同一骨架抄了三遍**
  （`handler.go:519-581`/`645-683`/`699-739`）→ 抽 `syncOrDeferred(budget, exec, onSync, onTimeout)`
- 卡片视图模型 `buildSubtitle`/`platformEmoji`/`domainFromURL`/`relativeTime`
  （`card.go:234-304`）→ 渠道中立视图模型，各渠道 renderer 各自贴版式
- `captureOwnerIfFirst`、owner 白名单判定 → 属租户/权限面，**接缝②会直接吃掉**

**真·飞书特有（留在 adapter，不要试图抽象）**:WS 连接生命周期与代数机制、
事件 dispatcher 的 8 类订阅、卡片 2.0 schema 的全部构造、
`cardActionSyncBudget=2.5s`（唯一真正由飞书 3s 回调硬约束推导的取值）。

## §3 接缝③:Source SDK

### §3.1 形态:Steampipe 式 Go 结构体字面量，**不引入外部 DSL**

**依据**:Airbyte/n8n 需要 JSON-path 映射 DSL，是因为目标 schema 未知；
而 vane 的目标是**固定已知的 7 字段** `types.ContentItem`。
用 DSL 填 7 个固定字段只会把编译期错误变成运行期错误，还丢掉类型检查与跳转。

```go
// 连接器 = 一个包级 var，编译期类型安全、可跳转、可 grep
var XHSUserPosts = &source.Connector{
    Platform: types.PlatformXHS, Capability: types.CapUserPosts, Kind: types.KindArticle,
    Params:   []source.ParamSpec{{Name: "user_id", Required: true, ...}},
    Identity: source.IdentityFromField("note_id"),   // 声明身份，不实现去重
    Request:  ...,   // 声明请求形状
    Paginate: source.CursorFromResponse{Field: "cursor", HasMore: "has_more"},
    Extract:  func(ctx, raw []byte) ([]types.ContentItem, error) { ... }, // 唯一必写的真代码
}
```

**框架承包**（测绘实证的重复面）:HTTP client 构造 + 超时/大小兜底（**5 份逐字重复**）、
config JSONB 解析与必填校验（6 份同构）、鉴权头、HTTP 状态码分类（**6 份且已漂移**）、
body 读取与 LimitReader（5 份 + `x.go` 一处**用错**）、时间戳解析（4 种格式 4 套）、
正文截断、串号防御（2 份独立实现同一模式）、限流与付费闸门
（**只有 TikHub 一家有**，同样按次计费的 Exa 与 x/xhs_user 完全没有）、
Kind 赋值（**6 处手写字面量**，而 `sourcecatalog.Entry.Kind` 已声明过一遍）。

**安全不变量的严重发现**:SSRF 双重防护（抓取前 `LookupIP` 预检 + 传输层校验）
**只有 RSS fetcher 有**。SDK 化后由框架统一施加于所有连接器。

### §3.2 增量:`is_data_feed` 是默认且唯一模式

**vane 的全部信源都是 data feed**（不可过滤、最新在前）:RSS、X user_posts、
XHS user_posts/search、Exa search，无一支持「给我 T 之后的」。
⇒ 这不是 Airbyte 的边角选项，而应是 vane SDK 的**默认唯一增量模式**:
框架据此生成分页停止条件 + 本地过滤。

**当前最大结构性缺口（测绘实证）**:`tikhub.go` 的 `page=1`、`xhs_user.go` 的 `cursor=""`
——**vane 根本没有游标**，全部靠「抓最新一页 + 全局去重当增量」。
`xhs_user` 的 `has_more` 只进日志、不驱动循环。
推送频率高于发帖速度时能用，**一旦漏抓一轮（源挂了/调度延迟）就永久丢内容**。

**state 是黑盒（Singer/Airbyte/Fivetran 三家一致）**:
语义归连接器作者，**存储与生命周期归框架**。给 `sources` 加 `cursor_state JSONB`，
框架负责持久化与回灌，不解释内容。

### §3.3 `spec` / `check`:把「实测准入」变成可执行事实

- **`spec`**:连接器自带 `Params []ParamSpec` 成为**单一事实来源**，
  取代 `sourcespec.go` 里三个手写 `build*` switch（~150 行 if/else + 手写中文文案），
  并同时供给 `sourcecatalog` 元数据与 agent 工具参数描述——现在这是**三份分开维护**、
  靠测试锁一致性的东西。
- **`check`**:拿真配置真打一次上游，返回结构化状态。
  正面命中 vane 痛点:`sourcecatalog` 的 `Status`+`Reason` 是**人工实测一次写死的**
  （x/search 的 Reason 记着 2026-07-16 的实测结论）。`check` 让「这个源能不能用」
  变成运行时可查询、可挂探针的事实。

### §3.4 逃生舱:实现接口并在声明里点名，而非绕过框架

`web/page_watch` 完全不像「抓一列内容」（产出 `KindChange`、需要 `SnapshotStore`、
预填 `CanonicalKey`）。**任何 SDK 设计如果不能容纳 page_watch，就是在逼人绕过 SDK。**
抄 n8n 的二分法:SDK 提供 `DeclarativeConnector`（声明式）与 `ProgrammaticConnector`
（自写 Fetch）两种风格，后者仍走框架的注册/门禁/记账。

## §4 双机并行推进

### §4.1 约定放哪（三层，不可混用）

| 平面 | 载体 | 内容 |
|---|---|---|
| **协调** | 飞书多维表格（功能清单 + 开发认领）、BOARD.md | 什么/谁/什么状态。高频变、非代码 |
| **契约** | **本文 + Go interface/types（在仓库）** | 模块怎么拼。必须可 diff / PR review / CI 强制 |
| **决策日志** | journal + CLAUDE.md 重要更迭 | 为什么这么选 |

> **让并行不撞车的是契约平面，它绝不能放多维表格**——表格不可 diff、不可 review、
> 不可被 CI 强制、不随代码回滚。多维表格答「什么/谁」，仓库答「怎么拼」。

### §4.2 扇出顺序（依赖决定，非偏好）

```
第 0 步（串行，零风险）  §1.1 principal 收敛      —— 谁做都行，做完立即扇出
        ↓
第 1 步（串行，地基）    §1.2 表分级 + §1.9 迁移 1-2 步 + tenantdb 骨架
        ↓
第 2 步（并行扇出）
   ├─ 机器 A:接缝② 剩余（RLS/Temporal propagator/凭证/配额/越权洞）
   └─ 机器 B:接缝① Channel（Event 信封 + Capabilities + 飞书 adapter 就地重构）
        ↓
第 3 步（并行）
   ├─ 机器 A/B:接缝③ Source SDK（弱依赖，可与上并行）
   └─ 纵切验证:一个新渠道端到端（§6-Q3 待拍板选哪个）
```

**机器归属按「接口背后的模块」而非文件**。建议:**Mac owns 接缝②**
（有生产 VPS/DB 访问，迁移与 RLS 验证必须在真库上做）；
**Windows owns 接缝①**（全新隔离包，与 store 迁移零文件交集）。

### §4.3 纪律（比工具重要）

- **主干开发**:接口优先的小 PR 持续合 main，**禁止长命重构分支**（两机各拉一条必死）。
- **feature flag / dark-ship**:未完成的 adapter 与 tenant 层用 config 门控合入 main，
  main 永远绿、永远可部署。先例:TikHub 端点面用 key 门控 dark-ship（#56）。
- **棘轮式 CI 守卫**:白名单只减不增（§1.5），防止迁移中途回潮。

## §5 已排除方案（写明防止三个月后重提）

| 方案 | 排除理由 |
|---|---|
| **schema-per-tenant** | 迁移时长随租户线性增长、schema 漂移难查；当事人（Atlas/GopherCon 2025）称「最大遗憾」。vane 已 16 个 goose 迁移，扇出不可承受 |
| **Go 标准库 `plugin` 包** | 官方文档明列:仅 Linux/FreeBSD/macOS、工具链版本必须完全一致否则运行时崩溃、不可卸载。换来的「运行时加载」对 vane 零价值——连接器作者就是 vane 自己，走 PR + CI 是正常路径 |
| **hashicorp/go-plugin 子进程隔离** | 连接器可信（自家代码、同二进制编译）时隔离是纯成本；崩溃隔离已由 Temporal Activity 重试提供 |
| **外部 YAML/JSON-path 字段映射 DSL** | 目标 schema 固定已知（7 字段），DSL 只会把编译期错误换成运行期错误 |
| **为限流引入 Redis** | Postgres 行级锁足以做 token bucket，且天然持久化 |

**留边界**:若未来允许**租户上传自定义连接器**（接缝②的延伸），优先 WASM（wazero，
纯 Go 无 CGO，兼容单体部署）而非子进程。为此现在唯一要做的动作:
**把 SDK 接口设计成可序列化/gRPC-able 的形状**。

## §6 待拍板（未拍板不得开工）

**Q1｜多租户的商业形态是什么?** —— 这决定隔离强度，是所有问题的前置。
(a) 真 SaaS（陌生人自助注册）→ RLS 必做、凭证隔离必做、租户自带 key 必做；
(b) 多团队/多个自己人 → 可简化为「强制租户谓词 + 不做 RLS」，成本大幅下降。

**Q2｜租户与飞书应用的关系?**
(a) 每租户一个飞书应用（各自 app_id/secret，**N 条 WS 长连接**，Manager 从单例变连接池）；
(b) 共享一个应用、按群/会话分租户（连接不变，隔离靠会话映射）。
影响 `feishu/manager.go` 的改造量级（差一个数量级）。

**Q3｜第一个新渠道选哪个?**
(a) **邮件**——逼出 magic link 全套与 `askFallback`，把最难的路先走通，但出成果慢；
(b) **企微/Slack**——有原生按钮，最像飞书，最快见效，但**验证不到贫渠道降级**（真正的风险面）。

**Q4｜迁移节奏?** vane 是单 owner，存量 `tenant_id` 全等于 1，五步在线迁移的
第 2 步（回填）可一条 UPDATE 完成。是否直接走「短暂停服一次性迁移」换取简化?

**Q5｜Source SDK 的迁移策略?**
(a) 新连接器强制走 SDK + 老 fetcher 逐个搬（棘轮，推荐）；
(b) 一次性全搬（PR 巨大，但无双轨期）。

**Q6｜优先级?** 本契约主张 ② 先行。若你更急要多渠道，可先做 ① 但**必须预留租户位**
（Event 带 TenantID、渠道配置表带 tenant_id 列但暂不启用）——代价是接缝①要返工一次装配层。

---

## 附:本契约的一手依据

- **OpenClaw**（openclaw/openclaw @00eb33f, v2026.7.2）:`ChannelPlugin` 约 30 个可选
  adapter 槽位、两层 `presentationCapabilities`（带数值上限）、`MessagePresentationAction`
  判别联合（approval 一等）、7 步出站流水线与成文降级规则表、审批三档阶梯
  （原生/emoji reaction/`/approve <id>`）、`askFallback` fail-closed。
- **Bot Framework**:Activity 统一信封（`value`/`channelData` 分家、`serviceUrl` 每事件自带）、
  能力矩阵**只在文档不在运行时**的反面教材、四态渲染枚举、`messageBack` 回调归一。
- **Apprise**:类级能力声明 + 基类自动降级（`title_maxlen=0` ⇒ title 折进 body）。
- **Novu**:provider 无状态、`subscriber.channels[]` 跨渠道身份与凭证。
- **HumanLayer**:审批做成一等资源（渠道只是 contact channel）、拒绝 `comment` 回灌 LLM。
- **Postgres RLS**:FORCE / WITH CHECK / RESTRICTIVE / `missing_ok` / `(SELECT ...)` 五细节；
  PlanetScale 实测 19x latency 差异；Bytebase footgun 清单（唯一约束跨租户泄漏、视图绕过、
  物化视图失保护、FK 报错误导）。
- **pgx**:jackc 本人裁定 `AfterConnect` 永久污染连接（#288）；prepared statement 缓存 +
  误标 IMMUTABLE 返回上一租户数据（#2007）。
- **Airbyte / Singer / Fivetran / Steampipe / Benthos**:四动作协议、黑盒 state、
  `is_data_feed`、声明式分页三策略、Go 结构体字面量式连接器。
