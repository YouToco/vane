# A2A 契约：vane 作为 Agent2Agent Server（第一期 content.query）

> 本文件是 A2A 集成并行实现的**唯一契约**。所有签名/JSON/表结构以此为准，实现中发现契约错误不得自行变更——记录到交付报告，由主控裁决。
> 事实基准：worktree @ ada0f6e（origin/main，2026-07-16 重新核实全部代码事实——方案定稿后 main 又前进，migration 编号与 wantTables 欠账均与方案原文不同，见 §2/§9.5）。2026-07-17 复核 @ e2136d5：012 已被 kind_backfill 占用，A2A migration 顺延为 **013**（§2）；wantTables 欠账清单不变（012 为纯回填不建表）。
> 设计过程：多 agent 调研 → 双怀疑者对抗审查（2×CRITICAL+12×MINOR 全处置）→ Boss 拍板 8 项（§13）→ 契约起草期双怀疑者再审（5×CRITICAL+7×MINOR，全部处置，见 §14）。方案全文原引用 workmemory `work/2026/2026-07-16-自研-见微Vane-A2A协议集成调研/a2a-integration-plan.md`，经 2026-07-17 双机核查（Windows 本地含未跟踪/stash + 远端）**该文件从未落盘**——设计过程的存档以本契约与 workmemory `journal/2026/2026-07-16.md`、`journal/2026/2026-07-17.md` 的记录为准。

## 0. 背景与范围

A2A（Agent2Agent，LF 治理）是 agent 间互操作协议：server 发布 Agent Card 声明能力，client 以任务（Task）粒度发消息、轮询终态、取回产物（Artifact）。vane 第一期只做 **server**，单一 skill：

| Skill | 说明 | 实现方式 |
|---|---|---|
| `content.query`（P1 唯一） | 按关键词/时间窗检索 vane 已入库的多信源内容，返回标题、链接、发布时间、正文摘录（**不含分数、不生成摘要**） | 确定性执行：executor 直接查 store，不经 LLM——零注入面、零 token 成本 |

**能力面为什么这么窄（数据盘点结论）**：content_items（001_init.sql:66-83，007 加 canonical_key、008 加 kind）只有原始字段，无 score/summary 列；唯一持久化的分数是 deliveries.score（001_init.sql:115），且是按 owner 画像打的**个性化分**（main.go:98-99 hints 注入 scorer）——暴露它即泄露画像偏好，撞 §8 红线。故第一期诚实降级为纯内容检索。

**非目标（第一期不做）**：vane 作为 A2A client；streaming（SSE）与 push notification（card 声明 false 且 handler 显式拒绝，启用前置见 §12）；gRPC/REST binding（只做 JSON-RPC 一种即合规）；extended card / card JWS 签名 / 多 peer 差异化授权；任何 Mutating 能力（确认卡语义无法映射给外部 agent）；画像与个性化分；`assistant.chat`（P2 暂缓，§12）。

**第一个对接方**：Boss 本机 Claude Code。Claude Code 无原生 A2A client（官方互操作只有 MCP），走官方 `a2a` CLI（`go install github.com/a2aproject/a2a-go/v2/cmd/a2a@latest`）+ 本机 skill 封装，token 走 my-credentials，零开发当天可用。注意：CLI 与 server 同源 SDK，**不构成 Gate ⑦ 的"异构客户端"验证**（§10）。

## 1. 协议基线

- **A2A 规范 v1.0.1**（2026-05-28 patch；v1.0.0 为 2026-03-12）。
- **v1.0.1 changelog 核对结论（2026-07-16 联网通读 releases + 三个 PR 原文）——已核对，无影响本集成的变更**，逐条：
  1. **#1753** HTTP binding 载荷媒体类型改为优先 `application/a2a+json`——只影响 HTTP+JSON/REST binding，PR 原文**显式保留 JSON-RPC 用 `application/json`**。vane 只做 JSON-RPC，无影响。
  2. **#1627** 修正六个错误的 HTTP 状态码映射（REST/gRPC transcoding 侧），并撤销 #1600 对 JSON-RPC `error.data` 的 google.rpc.ErrorInfo 强制结构（改回允许可选结构化对象）。方向是**放宽**；vane 的错误经 SDK 序列化、不自拼 error.data，无兼容缺口。
  3. **#1801** 规范文档示例统一为正式 `TASK_STATE_*` 枚举常量并补全终态清单——纯编辑性修正，恰与 §5.5 状态映射表使用的 ProtoJSON 常量对齐。
- **SDK：官方 `github.com/a2aproject/a2a-go/v2`，go.mod 钉 v2.3.1**。理由：`NewJSONRPCHandler` 返回标准 `http.Handler` 直挂现有 mux（契合无框架风格）；官方 SDK 有跨语言互通测试，手写 v1.0 状态机/错误码/版本协商的互通成本远超省下的依赖；trpc-a2a-go 停在旧 v0.2 规范且自持 HTTP server + 重依赖，双重不匹配。
- **依赖 review checklist**（升级/引入 SDK 时逐条人工核对；单人仓库不为此加守卫测试）：
  1. go.mod 必须是 `/v2` 主线——裸 `go get github.com/a2aproject/a2a-go` 会拿到旧 v0.3.15；
  2. **push notifications 保持禁用**直到含 SSRF 修复 #373/#374 的 release 出现（#374 已于 2026-07-16 merge 进 main，列每日对账 watch）；
  3. **#351 IDOR 仍 open**（影响 v2.0.0–v2.3.1 全线）：单 token 单租户下影响有限，**升级多 peer 前必须复查**；
  4. 升级流程：读 release notes → 全量测试 + a2aclient 互通 smoke（§9.4）→ 才合并；须 pin main commit 时在 go.mod 注释注明原因与回钉计划；
  5. PR-2 引入依赖时跑 `go list -deps ./a2a/...` 实测 JSON-RPC 路径依赖面，结果记入 PR 描述（方案预期第三方仅 google/uuid，待实证）。

## 2. migration `store/migrations/013_a2a.sql`

编号 **013**：origin/main @ e2136d5 现状为 001-009 + 011 + 012（010 空缺；011=page_snapshots、012=kind_backfill——方案原文"011_a2a"与契约初稿"012_a2a"均已被占）。**合并前再核目录取最大号 +1**：goose provider 默认拒绝乱序迁移（低于库内已应用最大版本号的新迁移会让启动迁移直接报错，012_kind_backfill.sql 头注释先例），空缺号 010 不可回填。

```sql
-- 013: A2A server 任务持久化（a2a-contract §2）
-- +goose Up
CREATE TABLE a2a_tasks (
    id         TEXT PRIMARY KEY,           -- 服务端生成 taskId（SDK uuid），不可由客户端指定
    context_id TEXT NOT NULL,              -- 会话轴：同 contextId 的任务构成一段对话
    status     TEXT NOT NULL,              -- TASK_STATE_* 原文（提取列：List 过滤与探针用）
    task       JSONB NOT NULL,             -- 完整 a2a.Task（ProtoJSON），权威载荷
    version    BIGINT NOT NULL DEFAULT 1,  -- 乐观并发（SDK TaskVersion 语义）
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_a2a_tasks_context ON a2a_tasks (context_id, created_at DESC);
-- status 单列/复合索引刻意不建：验证窗口才打开的功能没有该查询量，ListA2ATasks 的
-- status 过滤（§4.1）走上述索引或顺序扫描后过滤即可；有真实查询量再补 status 维度。
-- +goose Down
DROP TABLE a2a_tasks;
```

**数据是资产不清理**：终态任务不删不 TTL，留作"谁在问 vane 什么"的需求分析素材。push notification 配置表（含 webhook 凭证）第一期不建——敏感凭证入库的问题整个后移。

## 3. types 新实体（`types/entities.go` 追加，仿 AgentSession 注释风格）

```go
// A2ATask 是 A2A server 任务（a2a_tasks 表，migration 013）。Task 列是 SDK a2a.Task 的
// ProtoJSON 权威载荷（store 层不解析）；ID/ContextID/Status 是提取列。SDK 类型不出 a2a/ 包
//（隔离原则，同 agent.Store 窄接口先例），store 层只见本类型。
type A2ATask struct {
    ID        string          `json:"id"`         // 服务端生成 taskId
    ContextID string          `json:"context_id"`
    Status    string          `json:"status"`     // TASK_STATE_* 原文
    Task      json.RawMessage `json:"task"`       // JSONB，完整 a2a.Task ProtoJSON
    Version   int64           `json:"version"`    // 乐观并发版本，从 1 起
    CreatedAt time.Time       `json:"created_at"`
    UpdatedAt time.Time       `json:"updated_at"`
}

// A2ATaskQuery 是 ListA2ATasks（§4.1）的过滤条件，字段面对齐 SDK a2a.ListTasksRequest
//（v2.3.1 已核实：Tenant/ContextID/Status/PageSize/PageToken/HistoryLength/
// StatusTimestampAfter/IncludeArtifacts）。Tenant 单租户恒空不映射；HistoryLength/
// IncludeArtifacts 是 task JSONB 裁剪语义，归 a2a/taskstore.go 适配层（§5.9），store 不感知。
type A2ATaskQuery struct {
    ContextID            string    // 空串 = 不过滤
    Status               string    // TASK_STATE_* 原文；空串 = 不过滤
    StatusTimestampAfter time.Time // 零值 = 不过滤
    PageSize             int       // <=0 → 50；钳上限 200
    PageToken            string    // (created_at,id) 键集游标，store 包编解码，调用方视为不透明串
}
```

无新枚举：status 存 SDK 的 ProtoJSON 原文，vane 不自建平行枚举（避免与规范漂移）。

## 4. store 新方法

### 4.1 `store/a2a_tasks.go`（仿 store/agent.go:16-34 的 columns 常量 + scanXxx + 参数化 SQL）

```go
// a2aTaskColumns 全列常量，SELECT 与 scanA2ATask 一一对应。
const a2aTaskColumns = `id, context_id, status, task, version, created_at, updated_at`

// CreateA2ATask 落新任务（version 走表默认 1）。id 冲突返回 CodeConflict。
func (s *Store) CreateA2ATask(ctx context.Context, t *types.A2ATask) error

// GetA2ATask 按 id 取任务；无行返回 CodeNotFound。JSONB 反序列化天然是深拷贝，满足 SDK Get 的隔离要求。
func (s *Store) GetA2ATask(ctx context.Context, id string) (*types.A2ATask, error)

// UpdateA2ATask 乐观并发条件更新：
//   UPDATE a2a_tasks SET status=$3, task=$4, version=version+1, updated_at=now()
//   WHERE id=$1 AND version=$2
// RowsAffected==0 时回查 id 区分：无行 → CodeNotFound；有行但版本已前进 → CodeConflict
//（a2a/taskstore.go 按 §5.9 完整哨兵映射表翻译）。成功后新版本恒 = expectedVersion+1。
func (s *Store) UpdateA2ATask(ctx context.Context, id string, expectedVersion int64, status string, task json.RawMessage) error

// ListA2ATasks 服务 SDK taskstore.Store 强制的 List(ctx, *a2a.ListTasksRequest)。
// 请求/响应字段面是既成事实（v2.3.1 pkg.go.dev 已核，非假设分支），本签名即终版：
//   WHERE 谓词 = ContextID（空串不过滤）AND status（空串不过滤）
//                AND updated_at > StatusTimestampAfter（零值不过滤）
//   ORDER BY created_at DESC, id DESC；PageSize 钳制后截断
//   翻页：PageToken 为 (created_at,id) 键集游标（store 包私有编解码）；items 满页时
//         next = 末行键集编码，否则空串
//   total = 同谓词 COUNT(*)（供 SDK ListTasksResponse.TotalSize）
// HistoryLength/IncludeArtifacts 的 task JSONB 裁剪归 a2a/taskstore.go 适配层（§5.9）。
func (s *Store) ListA2ATasks(ctx context.Context, q types.A2ATaskQuery) (items []types.A2ATask, total int64, next string, err error)

// CountA2ATasks 只读计数，供 Gate 探针 P-A2A（§10）：probe.Store 接口追加此一行
//（probe/probe.go:51-62，接口新增非签名重构）。
func (s *Store) CountA2ATasks(ctx context.Context) (int64, error)
```

### 4.2 `store/content_items.go` 追加 SearchContentItems（PR-2）

现有五方法（UpsertContentItem/EnrichedCanonicalKeys/GetContentItem/ListRecentSimhashesByUser/ListUnpushedByUser，已核实）**没有任何关键词/时间窗检索**，现有索引也不覆盖全局时间窗。新增：

```go
// SearchContentItems 按关键词 + 时间窗检索内容，content.query 的唯一数据面。语义：
//   (title ILIKE '%kw%' OR content ILIKE '%kw%')     -- kw 空串时省略该谓词
//   AND COALESCE(published_at, fetched_at) >= $since -- published_at 可空（001:76），NULL 回退
//                                                    -- fetched_at，不静默丢无发布时间的整类源
//   ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC LIMIT $limit
// keyword 中的 %、_、\ 必须经 escapeLike 转义后再拼 '%..%'：入站文本不可信，裸拼会让外部输入
// 携带 LIKE 通配符打穿检索语义（参数化仍守住 SQL 注入，被劫持的是语义与性能）。
// limit 由调用方（executor）钳制后传入，本方法防御性处理 limit<=0 → 20。
// 第一期刻意 ILIKE 不引分词：中文 tsvector 无效、pg_jieba/zhparser 不在默认镜像（拍板 §13.8）。
// PR-2 附真实库量基准（EXPLAIN ANALYZE + 计时记录），慢则补表达式索引或 pg_trgm——
// "<1s"不作承诺，以实测为准。
func (s *Store) SearchContentItems(ctx context.Context, keyword string, since time.Time, limit int) ([]types.ContentItem, error)

// escapeLike 是 store 包私有 helper（与 SearchContentItems 同文件；store/ 现无任何 ILIKE
// 先例，本函数为新建）：把 s 中的 \、%、_ 依序替换为 \\、\%、\_ 后返回。SQL 侧依赖
// Postgres 默认转义符 ESCAPE '\'（标准默认行为，不显式写 ESCAPE 子句）。
// §9.3 的 escapeLike 用例以它为被测对象。
func escapeLike(s string) string
```

## 5. 新包 `a2a/`（仿 api/ 形态，顶层平铺）

```
a2a/
├── a2a.go        // Deps + TaskStorage/ContentStore 窄接口 + Mount
├── auth.go       // requireBearer 中间件（常数时间比对，card 端点豁免）
├── card.go       // buildCard + 包级 capabilities 单一事实源
├── executor.go   // AgentExecutor：skill 选择约定 + content.query 确定性执行
├── taskstore.go  // 适配 SDK taskstore.Store → TaskStorage（SDK 触点，归 PR-3；哨兵映射 §5.9）
├── errors.go     // 错误卫生：对外文案唯一翻译点
└── *_test.go
```

**SDK 类型隔离原则**：`a2a-go` 的类型只出现在本包；store 只见 types.A2ATask。接口定义在消费方（本包），`*store.Store` 满足之——与 agent.Store 窄接口先例一致。

### 5.1 Deps 与窄接口（签名级）

```go
// TaskStorage 是任务持久化窄接口，*store.Store 满足（§4.1 前四方法）。
type TaskStorage interface {
    CreateA2ATask(ctx context.Context, t *types.A2ATask) error
    GetA2ATask(ctx context.Context, id string) (*types.A2ATask, error)
    UpdateA2ATask(ctx context.Context, id string, expectedVersion int64, status string, task json.RawMessage) error
    ListA2ATasks(ctx context.Context, q types.A2ATaskQuery) ([]types.A2ATask, int64, string, error)
}

// ContentStore 是 content.query 数据面窄接口，*store.Store 满足（§4.2）。
type ContentStore interface {
    SearchContentItems(ctx context.Context, keyword string, since time.Time, limit int) ([]types.ContentItem, error)
}

// Deps 由 main.go 装配（§7）。
type Deps struct {
    Storage TaskStorage  // 生产 = *store.Store
    Content ContentStore // 生产 = *store.Store
    Token   string       // cfg.A2A.Token；空值语义见 §6
    BaseURL string       // cfg.A2A.BaseURL，进 AgentCard supportedInterfaces 的 url
    Version string       // 服务版本串，进 AgentCard.version
}
```

### 5.2 Mount 签名与骨架

```go
// capabilities 是卡片声明与 handler 能力检查的单一事实源（a2a/card.go 包级变量）：
// buildCard 与 Mount 共用同一值，杜绝"卡片说不支持、handler 却放行"的分裂。
var capabilities = a2a.AgentCapabilities{Streaming: false, PushNotifications: false}

// Mount 把 A2A 端点挂到根 mux。cfg.A2A.Enabled=false 时 main.go 不调用本函数（零暴露面）。
// Token 为空 → slog.Warn 一次并照常挂载（auth 恒 401），与 config.Validate 对 dashboard.password
// 的"只告警不拒启动"语义一致（config/config.go:224-228）。
func Mount(mux *http.ServeMux, deps Deps) error {
    rh := a2asrv.NewHandler(newExecutor(deps),
        a2asrv.WithTaskStore(newTaskStore(deps.Storage)),
        a2asrv.WithCapabilityChecks(&capabilities), // 显式传入：SDK 对 streaming 方法的缺省行为无文档
                                                    // 保证（push 有兜底错误，streaming 没有等价物）
        a2asrv.WithConcurrencyConfig(...),          // 并发上限，默认值见 §5.7
        a2asrv.WithLogger(slog.Default()))
    mux.Handle("POST /a2a", requireBearer(deps.Token, a2asrv.NewJSONRPCHandler(rh)))
    // card 端点公开无认证（GET /.well-known/agent-card.json，路径用 SDK 常量不硬编码）。
    mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(buildCard(deps)))
    return nil
}
```

**card 公开是设计选择非规范强制**：规范 §8.1 的 MUST 是"必须提供卡片"，well-known 免认证公开是生态默认实践——未来加访问控制不违规；卡片内无内部 URL/密钥/owner 信息。
**关停语义**：P1 的 Execute 在 SendMessage 请求生命周期内同步完成（确定性查询，阻塞返终态），HTTP Shutdown（5s 预算，main.go:216-219）天然覆盖，无自有后台 goroutine。executor 内 DB 查询自带请求级超时（§5.7）——pgxpool.Close 阻塞等借出连接（main.go:226-228 明文警告）。

### 5.3 auth（`a2a/auth.go`）

```go
// requireBearer 校验 Authorization: Bearer <token>。比对用 crypto/subtle.ConstantTimeCompare
//（防时序侧信道）；失败 401 + WWW-Authenticate: Bearer。token 空串 → 恒 401（不认可任何请求），
// 对齐 api/session.go:26-32 的 disabled 语义。每请求认证、无会话概念，与 api/ cookie 体系零交集。
// auth 失败限流仿 api/ratelimit.go 的 loginLimiter（按 IP 计失败），阈值见 §5.7。
func requireBearer(token string, next http.Handler) http.Handler
```

### 5.4 executor（`a2a/executor.go`）——skill 选择约定与 content.query 语义

```go
func newExecutor(deps Deps) *executor // 实现 SDK a2asrv.AgentExecutor（Execute/Cancel）
```

- **协议事实（裁定依据）**：A2A v1.0 请求全链路**不存在 skill 字段**（v2.3.1 逐字段核实：
  Message 仅 ID/ContextID/Extensions/Metadata/Parts/ReferenceTasks/Role/TaskID；
  SendMessageRequest 仅 Tenant/Config/Message/Metadata）。规范里 skill 是 Agent Card 上的
  能力广告（发现用），不是 RPC 路由键——"从请求里读 skill 再分发"没有协议载体。
- **skill 选择约定（vane 自定义，写进 card skill description 供对端发现，§5.8）**：入站消息
  **首个 text part** 若解析为 JSON 对象，允许可选 `"skill"` 键；**缺省（无该键 / 非 JSON /
  纯文本）一律视为 `content.query`**——单 skill server 的自然语义。
- **REJECTED 前置判定**（仅以下两种触发，不进执行，§5.5 / §9.1 各一用例）：
  ① `skill` 键存在且 ≠ `"content.query"`；
  ② 消息不含任何 text part（纯 file/data part，content.query 无从取参）。
  executor 内做该比较的 skill 常量与 card skill id 同源（§9.5 守卫）。
- **入参**（入站消息首个 text part）：优先按 JSON 对象解析 `{"skill": string 可选, "keyword": string, "days": int, "limit": int}`；解析失败则整段文本 = keyword。缺省与钳制：days 缺省 3、钳 [1,30]；limit 缺省 10、钳 [1,25]；keyword 允许空（= 纯时间窗浏览）。since = now − days×24h。
- **产物（COMPLETED 的 Artifact）**：一个 text part（人读中文列表：标题+链接+时间）+ 一个 data part（`[{"title","url","published_at","excerpt"}]`，excerpt = 正文前 300 rune）。**不含 score、不含 summary、不含画像信号**（§8）。
- 全部 error 先经 `sanitize(err)`（§5.6）再进事件流。

### 5.5 状态映射表（含不使用项及其启用前置）

| A2A TaskState（ProtoJSON） | vane 语义 | 触发点 |
|---|---|---|
| `TASK_STATE_SUBMITTED` | SDK 收到 SendMessage 建任务 | SDK 自动 |
| `TASK_STATE_WORKING` | executor 开始执行 | Execute 首个事件 |
| `TASK_STATE_COMPLETED` | 结果以 Artifact（text+data part）交付 | Execute 正常结束 |
| `TASK_STATE_FAILED` | 内部错误，message 只含脱敏文案 | Execute yield error（经 errors.go） |
| `TASK_STATE_REJECTED` | 显式 `skill` 键 ≠ content.query，或消息无 text part（§5.4 两种触发，可构造可测试） | Execute 前置判定 |
| `TASK_STATE_CANCELED` | CancelTask | Cancel 实现（yield canceled 事件） |
| `TASK_STATE_INPUT_REQUIRED` | **不使用**：agent.Outcome 只有 Reply+Confirm（agent/loop.go:129-136），不存在"这是澄清问句"的结构化信号，映射不可实现。启用前置 = 先定义 Outcome 的结构化澄清字段（§12） | — |
| `TASK_STATE_AUTH_REQUIRED` | 不使用（单 token 模型） | — |

**不变式**：taskId/contextId 只由服务端生成（SDK 保证）；终态任务拒收消息（SDK 保证 + Gate ⑤ 实测）；Mutating 语义（Outcome.Confirm / pending_actions）在 A2A 轨**永不出现**——第一期不接 agent loop 天然成立，并以测试钉死防 P2 破坏（§9.1）。

### 5.6 错误卫生（`a2a/errors.go`，唯一翻译点）

```go
// sanitize 是 executor 全部 error 的唯一出口（先例：api.writeAppError，api/api.go:118-141）：
// AppError → 其 Message（人话，可对外）；非 AppError → 固定文案"内部错误，请稍后重试"。
// 只有 sanitize 的返回值能进 yield 的 error / FAILED 状态 message / Artifact 文案。
// 原始错误链带 taskId/contextId 落 slog（结构化，多跳排查用）。
func sanitize(err error) string
```

这是本集成最容易破红线的位置：SDK 会把 executor yield 的 error 写进协议响应。突变测试钉死（§9.1）。

### 5.7 配额默认值（Boss 拍板"契约给默认值再核"，§13.6）

| 项 | 默认值 | 依据 |
|---|---|---|
| WithConcurrencyConfig 最大并发执行 | 4 | 确定性 DB 查询无 LLM 争抢；单机小 VPS |
| auth 失败限流 | 同 IP 1 分钟 10 次失败即拒 | 对齐 loginLimiter 量级 |
| executor DB 查询超时 | 5s | pgxpool.Close 阻塞警告（main.go:226-228） |
| SearchContentItems limit 上限 | 25 | 单张 Artifact 的合理体积 |

### 5.8 Agent Card 内容草案

卡片由 SDK `a2a.AgentCard` 类型构造后序列化（ProtoJSON 形态是 SDK 的责任，不手写 JSON）。内容：

```json
{
  "name": "见微 Vane",
  "description": "AI 个性化信息推送服务。提供已抓取入库的多信源内容检索（AI 模型厂商动态等）。",
  "version": "<Deps.Version>",
  "supportedInterfaces": [
    { "url": "https://api.vane.zhuoqidev.com/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }
  ],
  "capabilities": { "streaming": false, "pushNotifications": false, "extendedAgentCard": false },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["application/json", "text/plain"],
  "securitySchemes": { "bearer": { "httpAuthSecurityScheme": { "scheme": "Bearer" } } },
  "securityRequirements": [ { "bearer": [] } ],
  "skills": [
    {
      "id": "content.query",
      "name": "内容检索",
      "description": "按关键词与时间窗检索已入库内容，返回标题/链接/发布时间/正文摘录。入参为消息首个 text part：JSON 对象 {\"skill\"(可选，缺省即本 skill),\"keyword\",\"days\",\"limit\"}，或纯文本直接作为 keyword。",
      "tags": ["news", "ai-models", "digest"],
      "inputModes": ["text/plain"],
      "outputModes": ["application/json", "text/plain"],
      "examples": ["查询最近 3 天 Anthropic 相关内容", "{\"keyword\":\"GPT\",\"days\":7,\"limit\":10}"]
    }
  ]
}
```

- **securityRequirements 必填，与 securitySchemes 缺一不可**（v2.3.1 核实：AgentCard 有独立
  字段 `SecurityRequirements SecurityRequirementsOptions`）：securitySchemes 只是"可用方案
  声明"，**不构成访问要求**；官方 a2aclient 的 AuthInterceptor 按卡片 security requirements
  决定是否附凭证——缺了它，卡片驱动的客户端（恰是首个对接方路径：a2a CLI / `NewFromCard`）
  判定"无认证要求"裸发 SendMessage，被 requireBearer 恒 401，Gate ③④ 直接卡死。buildCard
  必须设置该字段（语义 `[{"bearer": []}]`）。scheme 值用 IANA 注册形态 `"Bearer"`
  （RFC 7235 规定 scheme 大小写不敏感，但对端实现未必遵守）。
- **本草案是语义草案，非 SDK 序列化逐字形态**：AgentCapabilities 三个 bool 的 json tag 均
  `omitempty`——false 时字段整段消失，静态 card 实际输出为 `"capabilities": {}`；
  SecurityRequirementsOptions 有自定义 Marshaler，实际包装形态以 SDK 输出为准。
  PR-3 跑一次 SDK 真实序列化回填本节（标注"SDK 序列化实测形态"）；在此之前 card golden
  （§9.1）与 Gate ①（§10）按**语义等价**比对（缺省字段 = false），不逐字比对本草案。

skill 粒度 = 一个可独立授权的能力边界，不按内部函数枚举。

### 5.9 taskstore 适配（`a2a/taskstore.go`）——SDK 哨兵错误映射（完整表）

SDK `taskstore.Store` 的接口文档（v2.3.1 逐方法核实）对返回错误有明确语义要求，适配层是
唯一翻译点，**三行映射全部要有 §9.1 用例**（按方法区分，无歧义）：

| store 返回 | 场景 | 适配层必须返回的 SDK 哨兵 |
|---|---|---|
| CodeConflict | CreateA2ATask：id 已存在 | `taskstore.ErrTaskAlreadyExists`（接口文档 "should return"） |
| CodeNotFound | GetA2ATask 无行；UpdateA2ATask 回查无行 | `a2a.ErrTaskNotFound`（Get/Update 文档均要求；§9.2 的 -32001 断言依赖本行） |
| CodeConflict | UpdateA2ATask：版本已前进 | `taskstore.ErrConcurrentModification`（接口文档 **MUST**） |

版本语义集中写清：适配层 Create 返回 `TaskVersion(1)`（表默认值，§2）；Update 成功返回
`TaskVersion(expectedVersion+1)`（§4.1 UPDATE 语句保证恒成立）。List 适配：Status/ContextID/
StatusTimestampAfter/PageSize/PageToken 直传 `types.A2ATaskQuery`（§3）；TotalSize/NextPageToken
取 store 的 total/next 返回值；HistoryLength/IncludeArtifacts 在本层对 task JSONB 做裁剪
（history 截断 / artifacts 剥除），store 不感知。

## 6. config（`config/config.go`）

```go
// A2AConfig 是 A2A server 配置（Config 结构体 Dashboard 字段之后追加 A2A A2AConfig）。
type A2AConfig struct {
    // Enabled 默认 false：未显式开启时 main.go 不 Mount，零新增暴露面。
    Enabled bool `mapstructure:"enabled"`
    // Token 环境变量 VANE_A2A_TOKEN；本体存 YouToco/my-credentials，绝不入库。
    // 为空时照常挂载、auth 恒 401、Mount 时 slog.Warn 一次（Dashboard Password 先例）。
    Token string `mapstructure:"token"`
    // BaseURL 是对外 A2A endpoint，进 AgentCard。
    BaseURL string `mapstructure:"base_url"`
}
```

**三处缺一不可**（config 既有陷阱，config/config.go:113-123 注释）：① Config 结构体加段；② setDefaults：`v.SetDefault("a2a.enabled", false)`、`v.SetDefault("a2a.base_url", "https://api.vane.zhuoqidev.com/a2a")`——有默认值的键 AutomaticEnv 才认识对应环境变量；③ sensitiveKeys 追加 `"a2a.token"`——token 无默认值，不 BindEnv 则纯 env 部署漏读。config.example.yaml 同步补 a2a 段。Validate 不新增拒启动项。

## 7. 装配（cmd/server/main.go）

插入点：`api.Mount(...)`（main.go:176-181）之后、`srv := &http.Server{...}`（:183）之前：

```go
if cfg.A2A.Enabled {
    if err := a2a.Mount(mux, a2a.Deps{
        Storage: st, Content: st,
        Token: cfg.A2A.Token, BaseURL: cfg.A2A.BaseURL,
        Version: vaneVersion, // PR-3 **新增**的 main 包常量（cmd/ 现无任何 version 常量），
                              // 值 = 合并时 CHANGELOG 最上方**已发布**版本号（当前 0.4.0；
                              // 若 PR-3 随发版则同步为新号）；不为此新增 ldflags 基建
    }); err != nil { /* 与其余装配失败同款：逆序拆栈后返回错误拒绝启动 */ }
}
```

- `enabled=false` 时 `/a2a` 与 card 路径都是 404（mux 上根本没注册）——可测断言（§9.5）。
- P1 不触碰 agent/scheduler/feishu 装配序；P2 若桥接 agent，构造须在 `agentLoop := agent.New(...)`（main.go:143）之后（M4 契约 §9 装配序先例）。关停零新增（§5.2）。

## 8. 安全红线（deny 与边界）

1. **错误卫生**：sanitize 是唯一翻译点，原始错误链（pgx/SQL/路径）一个字节不得进协议响应；**突变测试强制**（§9.1）：把坏写法（直接 yield err）放回去必须变红。
2. **入站消息 = 不可信输入**（协议官方免责声明原话）。P1 不经 LLM，注入面为零；keyword 经 escapeLike 转义（§4.2）。a2a_tasks 里的入站原文**不得回流进任何 LLM prompt**——第一期天然成立，钉为红线防 P2/分析脚本破坏。
3. **不暴露**：画像（profiles）、个性化分（deliveries.score）、推送历史、任何 Mutating 工具。executor 数据面窄接口只有 SearchContentItems，编译期封死。
4. **card 公开是设计选择非规范强制**（§5.2）：卡片内无内部 URL、无密钥、无 owner 信息。
5. token 比对常数时间；token 本体只存 my-credentials；auth 失败限流（§5.7）。
6. 全部 SQL 参数化；除 SDK 外不引入新依赖。
7. GetTask 需过 Bearer 认证，单 token 单租户下无横向越权面（多 peer 前必须复查 SDK #351，§1）。

## 9. 测试要求（vane 惯例：标准库 testing、手写 fake、DATABASE_URL 门控、无 build tag）

### 9.1 纯单测（无 DB，`go test -race` 必过）

- **card_test.go**：buildCard 经 SDK 序列化后 golden 断言（skill id、supportedInterfaces url、**securityRequirements 非空且含 bearer**；capabilities 按语义等价断言——三 bool 均 omitempty，字段缺省 = false，见 §5.8）；**断言 buildCard 与 WithCapabilityChecks 同源**（同一包级变量）。
- **auth_test.go**：拒绝矩阵表驱动（无头/错 token/空配置 token 恒 401/正确 token/card 豁免），仿 api/session_test.go。
- **executor_test.go**：手写 fakeContentStore（错误注入位+调用留痕，仿 agent loop_test.go fakeStore）；状态映射表驱动（§5.5 每行一用例，**REJECTED 两种触发各一用例**：显式 skill≠content.query、消息无 text part）；入参解析表驱动（JSON/纯文本/skill 键缺省/钳制边界）；**错误卫生突变测试**：fake 注入含 `pgx: connection refused` 的原始错误，断言 Execute 产出的**全部事件**序列化后逐字不含该串。
- **taskstore_test.go**：§5.9 哨兵映射表驱动（三行各一用例：fake TaskStorage 注入 CodeConflict/CodeNotFound，断言按方法翻译为对应 SDK 哨兵）；Create 返回 TaskVersion(1)、Update 返回 TaskVersion(expectedVersion+1)；List 的 HistoryLength/IncludeArtifacts 裁剪。
- **errors_test.go**：sanitize 对 AppError/裸 error/包装链的翻译逐字钉死。

### 9.2 httptest 层

- Mount 后的真 mux：card 端点无认证 200 + Content-Type；`POST /a2a` 401 矩阵；合法 JSON-RPC SendMessage（fake storage）走通回 COMPLETED；查不存在 taskId 得 -32001（依赖 §5.9 的 Get→`a2a.ErrTaskNotFound` 映射）。
- **streaming 拒绝钉子**：发 streaming 方法请求，断言收到能力类错误而非 SSE——验证 WithCapabilityChecks 生效；若 SDK 缺省已拒，保留为 SDK 行为钉子，升级 SDK 先跑。
- Deps 最小化，意外触库当场 panic（api/observability_test.go 哲学）。

### 9.3 DB 门控集成（store 包）

- a2a_tasks CRUD；**乐观并发**：两个并发 UpdateA2ATask 一胜一得 CodeConflict（channel 编排时序）；NotFound 与 Conflict 的回查区分；ListA2ATasks：ContextID/Status/StatusTimestampAfter 三谓词过滤、排序、PageSize 钳制、**键集游标翻页**（next 续查不重不漏、末页 next 空串）、total 与谓词一致。
- **SearchContentItems**：关键词命中 title/content 两路；时间窗（含 published_at NULL 回退 fetched_at）；limit；**escapeLike**（正文含 `%`/`_` 的行不被裸通配误伤）；基准计时留档。
- testsupport 三件套 + uuid 后缀 + FK 逆序清理（#26/#28 教训照单全收）。

### 9.4 协议合规与互通（CI 内只留一层）

- 官方 **a2aclient**（同 SDK，零新依赖）互通 smoke：`NewFromCard` 读本进程 httptest server 的 card → SendMessage → GetTask 轮询终态 → 断言 Artifact。随 `go test ./...` 跑。client 侧只配测试 token 凭证、**不手动加 Authorization 头**——AuthInterceptor 按卡片 securityRequirements 附 Bearer，顺带实证 §5.8 卡片驱动认证成立（缺 securityRequirements 时本 smoke 必 401 变红）。
- a2a-inspector / Python a2a-sdk 异构互通**不进 CI**，收敛为 Gate 真人项 ⑦（§10）。

### 9.5 守卫层

- **wantTables 补账**（store/migrate_test.go 现停在 11 张表，实际欠 4 张——比方案原文多出 007 的 content_sources）：补 `agent_sessions`、`pending_actions`（005）、`content_sources`（007）、`page_snapshots`（011），并加 `a2a_tasks`（013；012=kind_backfill 纯回填不建表）；**加对账守卫**：扫 store/migrations/*.sql 的 `CREATE TABLE` 名集合与 wantTables 集合比对，漏一张 CI 红——守卫自 M4 起失守，一次性堵死。
- probe/literals_test.go 模式：正则钉 card skill id 与 executor 的 skill 比较常量（§5.4 REJECTED 判定用的 `content.query` 字面量）一致。
- enabled=false 的 404 断言 + 装配项守卫（workflow/registration_test.go 反射守卫先例）。

## 10. Gate 清单

**服务端探针**（probe/ 扩展一条）：probe 包定位是纯 DB 只读（probe/probe.go:1-13 头注释），probe.Run 拿不到 config、cmd/gate 明文拒走 HTTP，故只进一条 DB 项：

- **P-A2A**：`CountA2ATasks` 查询成功（含 0 行）= green（migration 落位、表可读）；查询报错 = red。无 yellow——表存在性与数据量无关。**实现注记（刻意偏离既有模式）**：现有 probe.Run 对 Store 报错一律 `return (rep, err)` 中断整轮（probe/probe.go:145-178），cmd/gate 计为 exit 2 = "探针坏了"（cmd/gate/main.go:19-21）；P-A2A 的 CountA2ATasks 报错**不中断 Run，就地记一条 StatusRed 的 Result**（exit 1）——表缺失/不可读正是本探针要报告的产品事实，不是探针自身故障。两个实现者不得各写一版。
- 终态分布探针**推迟**：默认关闭的功能"无数据→yellow"是永久黄灯，把告警训练成背景噪音；待 Boss 决定常开时随启用动作一起加。card 自检不进探针 → 部署 smoke curl + 真人①；配置一致性不进探针 → Mount 启动期一次性 slog.Warn（§5.2）。

**真人实测清单**（编号制，Boss/外部机器执行）：

| # | 项 | 判定 |
|---|---|---|
| ① | 公网 `curl https://vane.zhuoqidev.com/.well-known/agent-card.json` | 字段与 §5.8 草案**语义等价**（omitempty 缺省 = false，§5.8；兼作 card 自检）。依赖 PR-infra 已合并（§11）；未合并时以 api 子域 curl 代跑、主域项待其合并后补验 |
| ② | 无 token 调 `POST /a2a` | 401 + WWW-Authenticate |
| ③ | 带 token SendMessage `content.query` | 返回**真实库内内容**，对照飞书当日推送交叉验证——防空转假象（M5 教训：闭环各环节"跑通"但全程消费空数据） |
| ④ | GetTask 以 taskId 轮询 | 到 COMPLETED，Artifact 含 text+data part |
| ⑤ | CancelTask + 终态任务再发消息 | 语义正确、终态拒收 |
| ⑥ | 制造非法参数与内部错误 | 错误文案无任何内部错误链（§8.1 的线上验证） |
| ⑦ | **异构客户端**（a2a-inspector 或 Python a2a-sdk）全流程 | **仅首次对外启用或协议大版本升级时执行**；官方 a2a CLI 与 server 同源 SDK **不算异构**，不得以 CLI 通过充抵本项 |
| ⑧ | 重启 vane 进程后旧 taskId 仍可 GetTask | 持久化生效（TaskStore 非内存实现的实证）。P2 生效后语义扩展：关停时在飞任务留 WORKING、重启后可查可 Cancel（§12） |

①-⑥⑧ 全过 → 发版；⑦ 按其触发条件独立执行。

## 11. 里程碑与 PR 拆分（4 个可独立合并 PR + 1 个 infra PR）

| PR | 范围 | 验收标准 | 对抗审查 |
|---|---|---|---|
| **PR-1 契约** | 本文档 | changelog 核对结论在案（§1）；全部签名级接口就位；拍板记录在案（§13） | **是**（契约级，仿 M5 双怀疑者） |
| **PR-2 存储** | migration 013 + types.A2ATask + store/a2a_tasks.go + SearchContentItems + 基准 + wantTables 补账（4 张欠账 + a2a_tasks + 对账守卫）+ §9.3 门控测试 | 门控测试全绿；基准数据附 PR；**纯 store 层，零 SDK import**；无公网面变化 | 否（常规 review） |
| **PR-3 服务端** | a2a/ 包全部（含 taskstore.go 适配）+ config 段 + main.go 装配 + probe P-A2A + `go list -deps` 实测记录 | §9.1/9.2/9.4/9.5 全绿；enabled=false 时 `/a2a` 与 card 404 + 守卫绿 + 既有测试全绿；enabled=true 验证窗口：VPS 部署后探针绿 + 真人 ①-⑥⑧ 过 | **是**（公网暴露面 + 认证 + 错误卫生） |
| **PR-infra Caddyfile** | 主域 vane.zhuoqidev.com 块内、SPA handle 之前加 `handle /.well-known/* { reverse_proxy localhost:8080 }`（现状 try_files 把该路径回落 index.html） | 合并后主域 card 可达、SPA 路由不回归；CI 自动上传 infra、合并即生效，不与代码 PR 搭车 | 否 |
| **PR-4 agent 桥接** | **暂缓**（拍板 §13.2）：assistant.chat + agent 中等重构 + M4 契约修订，范围见 §12 | P1 连通后再议 | **是**（触碰 M4 契约核心） |

PR-2 与 PR-3 无同包文件耦合可真并行（SDK 触点全在 PR-3）；PR-3 合并依赖 PR-2。**PR-infra 须在 PR-3 的 enabled=true 验证窗口开启前合并**——真人 ① 的主域 curl 依赖它（Caddyfile 现状 try_files 把 `/.well-known/*` 回落 index.html，deploy/Caddyfile:13-17）；若届时未合并，① 按其判定列的降级方式代跑。PR-4 砍掉不影响前三个的价值闭环。

## 12. 演进与已知取舍（记录在案）

- **streaming 启用前置**：仅需 `/a2a` 路由用 `http.ResponseController.SetWriteDeadline` 逐路由放宽 WriteTimeout=30s（main.go:188，唯一服务端障碍）+ 补 streaming 测试。**Caddy 零工作**：当前 Caddy 对 text/event-stream 自动逐段直通。
- **push notification 启用前置**（三条全满足才解禁）：SDK 升级到含 #373/#374 的 release + 自建 webhook 域名白名单 + 拒 RFC1918/localhost/云 metadata。启用时补 push 配置表（凭证入库问题届时正面处理）。
- **INPUT_REQUIRED 启用前置**：先给 agent.Outcome 定义结构化澄清字段（"这是澄清问句"的判定来源），无它则映射不可实现（§5.5）。
- **P2 `assistant.chat`（暂缓，连通后再议）**：不复用 agent_sessions、不共享 owner Loop 实例（写工具挂起等确认卡外部 agent 点不了；GetActiveAgentSession 会把 A2A 消息混进 owner 飞书上下文；userMu 让两轨互相排队）。改造范围（触碰 M4 契约 §7 签名面，PR-4 时修订 M4 契约）：
  ```go
  // agent 包新增（HandleMessage 签名不变，内部改为 load→RunOnce→save）：
  // RunOnce 在给定历史上执行一轮多轮 FC，不碰 store 会话。userID 必须入参（Tool.Execute 带
  // userID，agent/loop.go:91）；A2A 轨填 owner userID = 外部 agent 以 owner 身份读订阅/计划，
  // 此数据边界已随拍板 §13.2 确认。
  func (l *Loop) RunOnce(ctx context.Context, userID int64, history []llm.ChatMessage, text string) (Outcome, []llm.ChatMessage, error)
  ```
  三项前置改造：消息类型用 `llm.ChatMessage`（llm/chat.go:24，llm 包无 Message 类型）；**system prompt 参数化**（现为包级 const 写死飞书语境，agent/loop.go:31，withSystem 无条件前置 :595-603——改 `Deps.SystemPrompt string` 零值回落现常量，飞书轨零行为变化）；sessionID 传 0 并测试钉死"A2A 轨永不产生 pending_actions"（工具子集全只读：list_sources/list_schedules）。
  **P2 关停纪律**：`returnImmediately` 下 Execute goroutine 脱离请求生命周期——PR-4 开工前实测 SDK goroutine 宿主 ctx；a2a 包维护在飞 WaitGroup 有界等待；超预算任务留 WORKING 不强写终态（重启后可查可 Cancel，进 Gate ⑧ 扩展语义）；executor 预算 120s（对齐 chatCallTimeout，agent/loop.go:75-77）；DB 写自带超时。
- **vane 作为 A2A client：不做**（无对接对象，完全独立的工程面）。
- **多 peer 触发点**：需要第二个 token 时再加 peer 列 + 复查 SDK #351 IDOR（§1）。
- **不引入 Temporal**：A2A 任务粒度是"一次查询"，由 SDK + Postgres TaskStore 承载，与 PushPipelineWorkflow 无交集；"workflow 即 agent"（订阅式监控长任务）留未来。
- **MCP 薄桥（观察后可选）**：官方 `modelcontextprotocol/go-sdk` stdio server → a2aclient，300-500 行，让 vane skills 以原生 MCP tools 出现在 Claude Code。现成 MCP↔A2A 桥一律不用（生态碎片化 + v1.0 方法名从 `message/send` 硬断裂为 `SendMessage`，pre-1.0 桥大概率不通）。
- 检索是 ILIKE 顺序扫描起步：库量增长后按 PR-2 基准数据决定 pg_trgm/分词升级（拍板 §13.8）。

## 13. 拍板记录（2026-07-16 Boss 拍板 8 项）

| # | 决策点 | 结果 |
|---|---|---|
| 1 | 上线策略 | 第一个真实 peer = Boss 本机 Claude Code（官方 a2a CLI 路径）。验证窗口跑 Gate 后可为自用保持开启（dogfooding，Bearer 兜底） |
| 2 | PR-4 做不做 | **暂缓**，先解决 Claude Code 连通；P1 连通后再议 |
| 3 | card 主域/子域 | **主域**（PR-infra 独立合并；card 内 endpoint 仍指 api 子域） |
| 4 | 单 token vs 每 peer | 单 token；a2a_tasks 不加 peer 列，多 peer 是升级触发点（§12） |
| 5 | 数据边界 | **只做纯内容检索**；"owner 推送历史 + 个性化分"显式否决不暴露 |
| 6 | 配额阈值 | 契约给默认值再核（§5.7） |
| 7 | SDK issue 跟踪 | #374 release / #351 IDOR 列每日对账 watch |
| 8 | 检索语义 | ILIKE + PR-2 实测基准，慢了再升级（不动 Docker 镜像） |

> 走 A2A 而非直连 DB/SSH 的价值（诚实备注）：用真实消费者 dogfooding 协议面本身（card 发现、认证、任务生命周期全链路），为未来真正的外部 agent 对接踩平道路。

## 14. 起草期审查处置记录（2026-07-16）

契约初稿经双怀疑者对抗审查（A：协议/SDK 事实；B：vane 工程契合），全部 SDK 断言已对照
pkg.go.dev v2.3.1 独立复核（Message/SendMessageRequest/ListTasksRequest/ListTasksResponse
字段面、AgentCard.SecurityRequirements、taskstore.Store 哨兵语义、AgentCapabilities omitempty），
本地断言已在 worktree ada0f6e 复核（cmd/ 无 version 常量、store/ 无 escapeLike/ILIKE、
probe.Run 报错中断模式、cmd/gate 退出码语义、CHANGELOG 最新已发布 0.4.0、Caddyfile try_files）。

| # | 级别 | 议题 | 处置 |
|---|---|---|---|
| A-C1 | CRITICAL | skill 分发无协议载体（请求全链路无 skill 字段） | [已修复] §5.4 裁定为显式选择约定：首个 text part JSON 可选 `"skill"` 键、缺省 = content.query；REJECTED 收敛为两种可构造触发（显式 skill≠content.query / 无 text part）；§5.5/§5.8 skill description/§9.1/§9.5 四处同步 |
| A-C2 | CRITICAL | Agent Card 缺 securityRequirements，卡片驱动客户端不附 Bearer → Gate ③④ 必 401 | [已修复] §5.8 草案增 `"securityRequirements": [{"bearer": []}]` 并注明 buildCard 必须设置；scheme 改 IANA 形态 `"Bearer"`；§9.1 card golden、§9.4 smoke（不手动加头、实证卡片驱动认证）、Gate ① 三处同步 |
| A-C3 | CRITICAL | ListA2ATasks 签名对 SDK List 字段面可知不足，且与 §2 status 列声明矛盾 | [已修复] §4.1 签名当场定终版 `ListA2ATasks(ctx, q types.A2ATaskQuery) (items, total, next, err)`；§3 新增 A2ATaskQuery（Status/StatusTimestampAfter/PageSize/键集 PageToken）；删除"若 SDK 要求游标分页记勘误"假设分支与"PR-3 对照落地"字样；HistoryLength/IncludeArtifacts 归 §5.9 适配层；§5.1/§9.3 同步 |
| A-M1 | MINOR | taskstore 哨兵映射只写 1/3 | [已修复] 新增 §5.9 完整三行映射表（Create→ErrTaskAlreadyExists；Get/Update 无行→a2a.ErrTaskNotFound；Update 版本前进→ErrConcurrentModification）+ TaskVersion 语义集中写清；§9.1 增 taskstore_test.go 表驱动用例；§9.2 的 -32001 断言标注依赖 |
| A-M2 | MINOR | §5.8 草案与 SDK 序列化形态不一致（omitempty），golden/Gate ① 逐字比对假红 | [已修复] §5.8 增"语义草案非逐字形态"注记（capabilities false 缺省、SecurityRequirements 自定义 Marshaler）；PR-3 回填 SDK 实测形态；§9.1 与 Gate ① 改按语义等价比对 |
| B-C1 | CRITICAL | 同 A-C3（另指出 §2 status 注释矛盾与"字段面已知却留条件分支"） | [已修复] 同 A-C3；§2 索引注释同步说明 List status 过滤的执行方式与 status 维度索引的补建触发点 |
| B-C2 | CRITICAL | 同 A-C1（另指出"解析失败=整段当 keyword"使 REJECTED 永不触发、用例写不出） | [已修复] 同 A-C1；采纳其"无 text part → REJECTED"作为第二触发点，§9.1 两用例均可构造 |
| B-M1 | MINOR | 同 A-M1（另要求集中写清 TaskVersion 返回值） | [已修复] 同 A-M1，§5.9 已含 Create=TaskVersion(1)/Update=TaskVersion(expectedVersion+1) |
| B-M2 | MINOR | P-A2A "报错=red" 与 probe.Run 中断模式/cmd/gate exit 2 语义冲突未注明 | [已修复] §10 P-A2A 增实现注记：CountA2ATasks 报错不中断 Run、就地记 StatusRed（exit 1），并写明这是对既有模式的刻意偏离及理由 |
| B-M3 | MINOR | vaneVersion 读起来像既有常量，取值规则悬空 | [已修复] §7 改为"PR-3 新增 main 包常量，值 = 合并时 CHANGELOG 最上方已发布版本号（当前 0.4.0，随 PR-3 发版则同步）" |
| B-M4 | MINOR | escapeLike 三处引用但无归属文件与签名 | [已修复] §4.2 补 store 包私有 `func escapeLike(s string) string` 签名、替换规则（\/%/_）、依赖 Postgres 默认 ESCAPE '\' 的说明；§9.3 用例以它为被测对象 |
| B-M5 | MINOR | Gate ① 依赖 PR-infra 但 §11 依赖关系缺失 | [已修复] §11 增"PR-infra 须在 PR-3 验证窗口前合并"；Gate ① 增未合并时的降级跑法（api 子域代跑、主域补验） |

无驳回项：12 条意见的事实基础全部复核属实。
