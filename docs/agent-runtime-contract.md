# Agent Runtime 双轨执行契约

> 状态：正式演进契约，2026-07-21 起按 C0→C4 小步落地。本文描述运行时边界，
> 不新增公开 HTTP/A2A wire contract。任务创建仍以 Agent 确认卡和 A5/A6 saga 为唯一生产写入口。

## 1. 产品语义

Vane 按用户确认的意图选择两种内部执行模式：

- `compiled`：稳定监控任务。创建或编辑时编译并确认计划，每次触发直接运行已批准计划。
- `discover_at_run`：动态研究任务。每次触发先运行受限 `PlanFetch` Activity，再把合法计划交给固定流水线。
- `unknown`：只作零值和损坏数据哨兵，任何运行路径都必须 fail-closed，绝不能隐式退化为 `compiled`。

存量任务由兼容解析器**显式**映射为 `compiled`。模式是 Approved Definition 的一部分；
系统确认时须用用户语言说明，用户可用自然语言改选并再次确认。

## 2. Approved Definition 与 Adaptive State

两类状态必须物理和语义分离：

- Approved Definition：主题、范围、日程、预算、渠道、长期信源、执行模式和呈现要求。
  只能经用户确认改变。
- Adaptive State：同一已批准意图内的查询变体、只读能力排序、运行统计、健康状态和
  可回滚故障恢复状态。只能在受限规则内自动更新。last-known-good 指针只能指向已批准计划，
  或经固定代码判定为同主体、同 canonical domain 的等价恢复；动态单次发现计划不得跨 run 晋升。

动态发现的新源默认仅本次运行可用。除同一主体、同一 canonical domain 的等价端点恢复外，
长期新增信源、订阅、主题、日程、预算、渠道、账号或任何写操作都必须重新确认；用户不在线时拒绝改变。

**C2 持久化不变量**：`discover_at_run` 必须带精确的 Approved Definition head；只有
`compiled` 可作为兼容期 headless 状态。每次 Adaptive 写入都携带其运行快照消费的 definition
version+digest fence，并把该 basis 随行持久化；head 已变化的旧 run 一律冲突。C3 建立等价恢复
证据前，LKG 只能等于当前 exact `approved_plan` basis，`legacy_subscriptions` 不得写 Adaptive 或
成为 LKG。C2c 构造运行快照时必须在同一数据库事务内读取 definition、Adaptive 与 head，不能在
内存里拼接两个独立查询。

### 2.1 C2b 定义写控制面与跨系统编辑

C2b 按不可跳级的子列车落地：C2b-1 让创建双写 legacy/Approved 并关闭危险旧编辑旁路，C2b-2 以
exact base version+digest CAS 追加 Approved Definition，C2b-3 才把确认、PostgreSQL quiesce、定义提交和
Temporal Schedule 更新编排成可恢复事务。确认票必须同时绑定认证 actor、Tenant/User/Task scope、
base head、候选 definition 原始字节及 digest，以及 base/target 两份完整 Temporal 表示；确认后不得再
调用 LLM、重新编译或按进程当前配置重建候选。旧 generic pending action、HTTP PATCH 和分段 Store 写入
都不是合法旁路。

C2b-3 编辑时先在 PostgreSQL 保存原状态并把 active task 置为 paused，阻断已派发 run 的后续付费与写
副作用；然后用 raw Temporal `UpdateSchedule` 和刚刚 `Describe` 取得的 exact conflict token，依次完成
base-active→base-paused、base-paused→target-paused。每个 phase 的 request ID 必须由冻结 operation 与
payload 确定性派生，每次写后都用 detached bounded `Describe` 严格核对完整 spec/action/policy/state，
不能把 RPC success 当成提交证明，也不能使用 SDK 的无条件 Update/Pause/Unpause。原本 paused 的任务
保持 paused；原本 active 的任务先把 Temporal 恢复为 exact target-active，最后才 CAS 恢复 PostgreSQL
active。C2b3-1 只交付这些 raw CAS 原语并保持零生产调用点，持久 operation、确认入口与 receipt/outbox
分别由后续子列车接线。

C2b3-2 继续按故障域拆成四个不可跳级的子列车：

- **C2b3-2a frozen protocol**：只新增 edit-specific operation/receipt schema、强类型状态和 frozen proposal
  codec；每任务单一非终态约束、删除后仍保留审计、actor/target scope、base/target head、完整 Temporal
  表示与原始 canonical definition bytes 都在这一层固化；schema 可先预留 lease/fence、schedule marker、phase
  checkpoint 和 receipt outbox 列，但此阶段没有 Store mutation API，也没有生产调用点。
- **C2b3-2b durable Store substrate**：实现受数据库时钟 lease/fence、schedule operation marker、逐阶段原始
  字节 checkpoint 与 edit-specific terminal receipt outbox 约束的 Store API。定义 head/legacy projection 的推进必须与 operation
  `definition_committed` checkpoint 在同一 PostgreSQL 事务；恢复数据库状态与 terminal/outbox 也必须同事务。
  此阶段仍保持零生产调用点。
- **C2b3-2c dark coordinator**：唯一允许调用 C2b3-1 raw API 的内部 coordinator，按一次 attempt 最多推进
  一个远端 phase 的规则执行，并提供启动时和周期性的 tenant-sharded bounded recovery。只有真 PostgreSQL 18
  与真 Temporal 的全部 kill point 收敛后才可结束本阶段；Agent、HTTP、飞书入口仍不可达。
- **C2b3-2d authenticated wiring**：新增唯一 definition-edit Agent 工具/控制器；旧
  `update_schedule`、`edit_task_playbook`、`set_task_strictness`、generic pending v0 与 HTTP PATCH 永久保持
  退役。冻结票显式保存 actor tenant/user 与 target tenant/user/task，并在原卡点击时绑定 Feishu App
  fingerprint + message ID。terminal outbox 原地 Patch 同一张卡，session 只写固定终态事实。真卡 Gate 通过前
  默认关闭，不得声称用户编辑已恢复。开闸前还必须让 legacy `ReconcileActions` 与 edit operation 串行化：
  reconcile 不得持有 active 快照跨过 quiesce/recovery 后再写回 Temporal；须等待首次恢复完成，或在每次写前
  以 operation marker/status/fence 重新授权，并以并发真 Temporal Gate 证明旧 Action 不会覆盖已完成编辑。

这里的 **attempt** 专指一次代码级 `runTaskDefinitionEditAttempt` 调用：它从一个已持久化 phase 开始，最多
调用一次 Pause/Apply/Restore raw phase，并在一次本地事务推进或一次远端 checkpoint 后立即返回。同一条仍
有效的 lease/fence 可以顺序执行多个这样的代码级 attempt。operation 行的 `attempt` 列只审计首次 acquisition
与过期 lease takeover 的代次，不是远端 phase 预算；不得为了推进下一个 phase 主动放弃 lease 或伪造 takeover。

持久 phase 与允许的系统状态如下；表中任一不匹配都不得猜测修复：

| operation phase | PostgreSQL head | PostgreSQL status | Temporal 可接受表示 |
|---|---|---|---|
| `proposal_sealed` | exact base | frozen original | exact base-original |
| `db_quiesced` | exact base | paused + exact operation marker | exact base-original；原本 active 的 RPC 结果未知时也可能已是本 operation 的 base-paused |
| `temporal_base_paused` | exact base | paused + marker | 原本 active：exact base-paused；原本 paused：仅复核 exact base-original |
| `definition_committed` | exact target | paused + marker | 原本 active：exact base-paused；原本 paused：exact base-original；apply 结果未知时也可能已到各自 target 表示 |
| `temporal_target_applied` | exact target | paused + marker | 原本 active：exact target-paused；原本 paused：直接 exact target-final |
| `temporal_target_restored` | exact target | paused + marker | exact target-final |
| terminal completed | exact target | frozen original、marker 已清 | exact target-final |

`status` 表示 pending/executing/terminal，`phase` 始终保留最后一个已持久化的 progress checkpoint；进入
cancelled/expired/blocked/superseded 不得用同名“终态 phase”覆盖进度。每个 progress phase 必须与其
pause/apply/restore canonical snapshot 前缀完全一致；cancelled/expired 只能停在 `proposal_sealed`，
completed 只能停在 `temporal_target_restored`。

当前生产 `DATABASE_URL` 同时承担 migration 与 runtime，连接角色是新表 owner；owner 对表有隐式 DML 且默认
绕过 RLS。因此 2a 的 dark 保证只来自“无 Store API + AST 零引用守卫”，不能宣称数据库权限级不可写。
在 2d authenticated wiring 前，必须完成 migration/admin 与 runtime 权限分离，或在所有 edit 事务中可靠
激活 scoped restricted role + tenant context，并以真生产同形连接证明；033 不给 `vane_app` 显式 DML 只是
为该切换保留最小授权面，不是当前生产安全边界。

033 同时把 `schedules` 原有的 table-level INSERT/UPDATE 收窄为 legacy 列 allowlist，明确排除
`definition_edit_operation_id/definition_edit_fence`；2b 必须用 coordinator 专用受限角色/事务路径写 marker。
普通 runtime 仍保留产品既有的整行 DELETE；这是“用户删任务优先、marker 随行消失”的明确生命周期语义，
不是 marker 权限级不可清除。operation 独立保存 base 与 target 两份 exact canonical definition bytes，且分别由
base/target head digest 绑定；schedule 与 Approved history 被级联删除后，恢复仍可严格解码自身 checkpoint，
但必须以 schedule missing/Temporal `NotFound` 进入 blocked/quarantine，绝不能重建或继续远端写。operation/receipt
独立留存只用于审计和终态收敛，不把“字节自包含”误当成资源仍存在。
operation 身份、proposal、target/prepared/base snapshot 可通过不授 UPDATE 固化；pause/apply/restore 位于同一行，
其 NULL→首次写、之后只允许 exact replay 的不可变性来自带 expected phase+lease+fence 的 Store CAS，而不是普通
SHA 或列级 grant。若未来要求 DB 权限本身证明 append-only，应把 phase receipt 拆成只授 INSERT/SELECT 的子表。

每个 Temporal RPC 前须在独立短事务中重新验证 active lease/fence、expected phase、schedule marker 和对应
head；RPC 后只允许同一 fence checkpoint exact canonical snapshot。现有 C2b-2
`CommitApprovedDefinitionEdit` 的历史 replay 只表示“该 confirmation 曾产生过这个 immutable version”，不授予
任何远端写权限：current head 高于 target 时必须 `superseded` 且零 Temporal mutation；同 version 不同 digest、
foreign remote representation、损坏 checkpoint 或 Temporal NotFound 一律 blocked/quarantine，数据库保持 paused。

Prepared wire 内的 creation ownership 只能来自同 scope、已经终态成功的 create-schedule v1 operation 所保存的
`prepared_schedule`；不得从当前 legacy 行、当前配置或调用方自报字段重建。2a codec 只能证明字节内部闭包，不能
替代 2b Store 对该 append-only provenance 的真库核验。

definition-edit/v1 在 prepared wire 中分别固化 base/target canonical projection digest；Decode 只核 exact
Approved head、projection digest、creation ownership 与 phase checkpoint，不重新执行会演进的 spec compiler。
只有 Build/封票入口运行 current target writer，并把 current 编译出的 timing 与 prepared target 严格对照；旧 base
使用 retained v1 compiler，因此策略收紧只会拒绝新票，不得卡死已确认 operation 的恢复。v1 还显式冻结 task ID
scheme、operation ID 字节上限、timing reader，以及共享 `ScheduleSpec`/`PushScope`/`PushParams`/prepared schedule
布局；这些类型演进时必须新建 wire 并保留 v1 reader，不能直接修改 guard。

隐式 `temporal-default-json-v1` converter ID 由 exact PushParams payload golden 约束，且禁止用显式自定义 converter
重绑同一 ID；SDK 升级若改变 payload，必须 bump converter ID 并注册旧 decoder。另一个部署不变量是：存在
nonterminal edit operation 时不得直接把唯一 Scheduler namespace 从 A 切到 B；必须先 drain/终结，或先实现按 sealed
namespace+namespace ID 路由的 retained client。namespace 同名重建仍由 ID mismatch fail closed。两项均属于 2d
接线/部署 Gate。

恢复或迟到 replay 只有在 current Approved head **恰好等于该 operation 的 target head**，且 Temporal
仍是该 operation 的 exact base/target pre-state 时才可继续。C2b-2 返回历史成功记录不等于获得远端写
权限；current head 已更高时旧 operation 必须 superseded，绝不能刷新 conflict token 后覆盖新版本。
Temporal `NotFound` 表示删除胜出，禁止重建；任何不属于冻结 base/target 的远端表示都进入 blocked/
quarantine 并保持数据库 paused，不能猜测或收养。

## 3. Run Snapshot：每次运行只有一个事实版本

每个 Temporal run 在任何网络、LLM、数据库写入或推送副作用前创建一次不可变运行快照，冻结：

- Tenant/User/Task 与 execution mode；
- definition、plan、adaptive state 的版本和实际消费字节；
- capability catalog、tool policy、prompt policy、model policy、quota policy 和 planner budget envelope；
- Temporal WorkflowID/RunID。

快照必须先以 `(TenantID, UserID, TaskID, Temporal WorkflowID, Temporal RunID)` 在 PostgreSQL 中
`CreateOrGet`，再向 Workflow 返回只含身份、ID 和 SHA-256 digest 的安全引用。引用自身有覆盖全部
安全字段的 `reference_digest`；每次消费都必须按期望的 WorkflowID/RunID/RunKind/Tenant/User/Task
逐字段核对并以常量时间重算 digest。策略标识是内容寻址、append-only 的 digest，不是可被原地改写的标签。
Store 必须从持久化的 canonical 策略内容自行计算 digest，不能接受一串没有对应内容的调用方声明；
reference schema version 也必须随行持久化，旧快照不得被新代码常量重新解释。真实密钥永不进入快照，
只允许非敏感策略和受控 secret reference/version；运行时再从既有 secret store 解析权限范围内的密钥。

**C1 生产接线硬门槛**：五类 policy 必须由 typed non-secret DTO 构建，只暴露 allowlist、模型名与参数、
配额规则和 secret reference/version，DTO 中不得存在 secret value 字段；credential ref 必须与用途绑定
（LLM/Exa/TikHub 不可互换），implementation/endpoint 只能取受控版本 ID；解码使用未知、重复、缺失字段及
非法 `null` 拒绝，并以
API key/token/password 注入反向测试证明其无法进入。不得把通用 config map 或凭证对象直接序列化给 C0 Store。
C1a 的 raw JSON 原语仅包内可见，typed adapter 也保持零生产调用点；C1 接线前必须由五类
typed non-secret policy builder 生成输入，
DTO 不含 secret value 字段并拒绝未知字段。禁止直接序列化应用 config，也不以不完备的敏感键 denylist
冒充密钥泄漏证明。

C1 开始写生产快照前还必须把 payload decode/canonical/validation 做成按 schema version 分派的 reader。
v1 reader 不得继续调用会随业务演进的“current”计划结构或校验器；新增计划字段或规则时保留旧 reader、
写入新版本，不能让部署后的新校验反向拒绝或重新解释历史 v1 字节。

当前单一 Temporal namespace 内，`Temporal RunID` 另有全局唯一防串号约束：同一 execution 不得因
错误 scope 或 WorkflowID 被创建成第二份快照。该约束只用于冲突检测，所有业务读取仍必须从
Tenant/User/Task scope 开始；冲突只返回统一错误，不暴露已存在行的归属。

手册正文、URL、source config、凭证或其他外部内容不得进入**运行开始快照引用或 PlanFetch 引用**的
Temporal history payload。现有固定流水线仍会把抓取内容作为 Activity 结果传递；那是独立的 history
体积与敏感内容治理债，不在 C0/C1 借改 wire shape 一并处理。

`CreateOrGet` 是 first-writer-wins：首次提交成功但 Activity 响应丢失时，重试必须读取原行，
不得按已变化的任务定义、worker 配置或模型重新生成。digest 损坏、scope 不符或引用缺失一律 fail-closed。
后续 Activity 只按精确 snapshot ref 读取，禁止再读 current definition 覆盖本次运行。

快照提交是本 run 的定义/策略冻结点，不是永久授权票。C1 从 PrepareRun 后开始，在每个付费调用或写副作用
Activity 紧邻执行前，以当前 tenant active、membership 存在、task active 做独立 fail-closed kill check；
数据库检查失败也拒绝。定义编辑只影响下一 run，但 pause/delete、停租或撤成员必须阻止本 run 后续副作用。
因此响应丢失重试即使在任务删除后仍可取回原审计 ref，消费步骤也不会据此继续花钱或写入。
其中 expected WorkflowID/RunID 必须来自当前 ActivityInfo，Task/User 来自受信调度输入；不得从 ref 自身
反推 expected，否则另一 run 的一枚合法 ref 会退化为 bearer token。ref 的授权校验也必须按其持久化
schema version 分派到固定 reader；不得先调用 current DTO validator，避免未来规则反向拒绝历史 run。

运行快照是 tenant-owned 审计数据：所有 API/索引都以 Tenant/User/Task scope 开头，应用层没有更新接口；
删除任务不连带删除历史快照，租户到期硬删除时才随 tenant 清除。当前不凭空增加 TTL；未来保留策略须作为
独立数据决策。全库 RLS 激活时，本表必须与其他 tenant-owned 表同批纳入 FORCE RLS/tenantdb，而不是留下例外。

运行健康、due time、fail count 等 Adaptive 事实可在执行时更新；但 URL、query、config、长期源集合等
任务身份只能来自快照。配置变更只影响下一个 Temporal run。

### 3.1 存量 Compiled 范围

存量任务必须显式区分：

- `approved_plan`：按已批准 fetch_plan 和精确 schedule_sources 执行；二者不一致即拒绝运行。
  fetch_plan 中的 platform/capability/title/URL/config 是执行身份真相源，schedule_sources 与全局
  sources 只提供稳定 SourceID 和可变健康状态；共享源元数据变化不得改写用户已批准的下一 run。
- `legacy_subscriptions`：仅为存量兼容，快照创建时冻结当次订阅源身份，不能在运行中重新展开。

`discover_at_run` 的空计划永远不得进入 legacy“抓全部订阅源”分支。

### 3.2 C0/C1 的主体边界

C0/C1 只为有持久 TaskID 的 scheduled run 建快照。现有 `push_now` 暂走 legacy compiled 路径，
在设计出可审计的 synthetic task scope 前不得开启 `discover_at_run`，也不得借用其他任务的记忆、
checkpoint 或 last-known-good。

## 4. Temporal 确定性边界

- Workflow 只允许编排、纯计算、Temporal API 与 `SideEffect`；不得直接调用 DB、LLM、网络、Agent 或系统时钟。
- 新的命令序列必须用 `workflow.GetVersion` 接入并通过现有多代 history replay。
- Activity 名称、顺序和 wire shape 变更必须有负控，证明错误变更会触发 Temporal nondeterminism。
- Schedule Action 只保存稳定标识符；run-start snapshot 不能写进创建时冻结的 Action 参数。

## 5. DiscoverAtRun / PlanFetch

`PlanFetch` 是独立、只读、最小权限的 Agent Loop 实例，不复用对话 Agent 的写工具图。输入只含：

- 本 run 的最小 Tenant/User/Task 记忆切片；
- Approved Definition 与当前受限 Adaptive State；
- 显式 capability allowlist 和预算。

输出只能是结构化 Execution Plan：只引用已注册 capability 和合法参数，禁止代码、SQL、密钥、
任意 MCP 连接或直接副作用。固定 Go 代码必须再次校验类型、URL/重定向/SSRF、租户权限、参数、
工具数、token、费用和总时限，再进入 Fetch→Dedup→Score→Select→CardGen→Push。

校验通过的计划必须先以 `(TenantID, UserID, TaskID, RunID, StepID)` 写入不可变 checkpoint，
`PlanFetch` Activity 只向 Workflow 返回带身份和 digest 的安全 `PlanRef`；原始 Execution Plan 不得作为
Activity result 进入 Temporal history。首版 planner 硬上限为 8 轮、16 次工具、32,768 token、
1,000,000 microUSD 和 300,000 ms；动态规划五项都必须为正，费用覆盖 planner LLM 与工具调用。
`compiled` 的 planner budget 全零只表示“不运行 planner”，租户付费配额仍由冻结的 quota policy 执行。

## 6. 付费步骤、重试与 last-known-good

每个付费步骤使用 `(TenantID, UserID, TaskID, RunID, StepID)` 的持久检查点与不可变 request digest。

- 调用前先落 `started`；成功结果和真实成本落终态后才能复用。
- 上游支持幂等键时，重试沿用同一键。
- 上游不支持幂等键且 `started` 后失联时，结果属于 indeterminate：禁止自动再次付费，
  应转 last-known-good 或 blocked。
- lease/fence 使用数据库时钟；陈旧 owner 不得提交结果。
- 预算从持久步骤汇总，不能依赖 Activity 内存计数。

规划失败、预算耗尽或不确定结果时，只能使用最近一次**已批准且校验通过**的计划，或已被固定代码
授权为同主体、同 canonical domain 等价恢复的计划；动态单次计划和临时新源不得成为跨 run LKG。
没有有效计划则记录 blocked 并通知。禁止回退为抓取用户全部订阅源。

## 7. 能力、记忆与自动进化

- 记忆分为会话、用户、任务、运行和全局能力目录；检索必须带 Tenant/User/Task 范围与来源版本。
- capability catalog 是受信注册表，按匹配度、历史成功率、延迟和成本排序；Agent 无权安装工具、
  连接任意 MCP server、扩大权限或获得任务写入/删除/直接推送/任意代码执行能力。
- critic 只在固定指标异常时产生带证据建议。只有批准意图内的低风险 Adaptive State 可自动持久化，
  其余转确认动作。

## 8. 发布列车

| 阶段 | 交付 | 生产行为 |
|---|---|---|
| C0 | `ExecutionMode` + scheduled-only immutable `task_run_snapshots` 真库原语 | 零调用点，只部署 schema/API 地基 |
| C1a | 强类型非敏感 policy DTO + 固定 v1 payload reader | 零调用点；先封住密钥边界与历史重解释风险 |
| C1b | versioned `PrepareRun` + Compiled 全链按 snapshot ref 消费 | 存量仍 Compiled，行为等价且单 run 不漂移 |
| C2a | mode + Approved/Adaptive schema、冻结 wire 与 fenced Store | 零调用点；默认 Compiled，动态模式仍关闭 |
| C2b-1 | 创建双写 legacy/Approved + 关闭旧编辑旁路 | 新建任务安全停靠，仍不重开编辑、不切运行读取 |
| C2b-2 | exact base version+digest definition CAS | 零生产调用点；证明单库原子提交与 exact replay |
| C2b-3 | 2a 冻结协议 → 2b fenced Store → 2c dark coordinator → 2d authenticated wiring | 全部 kill point 过 Gate 后才重开唯一编辑入口，仍不切运行读取 |
| C2c | 存量适配、shadow 对账并切 immutable head 读取 | 动态模式仍 feature flag 关闭 |
| C3 | RunID/StepID 检查点、LKG、bounded `PlanFetch` | 仅 shadow，无用户推送影响 |
| C4 | 两条首批竖切的 Boss 单任务 canary | 逐步放量，可回滚 Compiled/LKG |

首批竖切：

1. “监控 Anthropic 状态”但不给 URL：Agent 找官方候选→确认→稳定 `compiled` 任务。
2. “每天找全球 AI 热点”：Temporal 触发 bounded Agent→固定流水线推送。

模型、prompt、检索策略、能力说明或自动学习规则变更必须经过 replay→shadow→canary→发布，并可按版本回滚。

## 9. 最低验收

- 跨 Tenant/User/Task、Unknown mode、暂停/删除/失效成员、计划与链接不一致均在付费或外部副作用前拒绝。
- Activity 响应丢失、并发同 RunID、定义在运行中改变、worker 重启和 digest 损坏有反向测试。
- 外部网页不能改变权限、读取其他记忆、生成写 pending action 或把上下文外带到新 URL/query。
- 新模型/策略只影响下一 run；本 run 的模型、能力和预算必须由 snapshot 实际驱动，不得只做日志字段。
- 无 LKG 的动态失败可见且 blocked，外部调用为零；绝不抓全部订阅源兜底。
