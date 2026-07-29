# 任务手册（Task Playbook）设计方案 · RFC

> **现行覆盖声明（2026-07-29）：** 当前材料化链路为
> `task manual → fetchspec → capabilitycatalog → fetch_targets →
> task_fetch_targets`，以 `task-playbook-fetch-target-cutover.md` 为准。下文
> `sourcespec` / `sources` 是本 RFC 落地时的历史名，不得用于新代码。

> 状态：**正式契约（7 个开放问题全部拍板，见 §10）· P0、P1a、P1b、P1c 均已实现**。
> - **P0 + D-2 + D-5**：迁移 017 `schedule_playbooks`、store DAO、`view/edit_task_playbook` 工具、
>   `create_schedule` 创建即初始化、`include_domains` 接进 sourcespec 配置+幂等键、M6 §5.3 lookback 提级。
> - **P1a 编译层**：`create_schedule`/`edit_task_playbook`
>   在存下手册正文后各发一次 LLM 调用，把正文**编译**成结构化 `fetch_plan`（逐源经 sourcespec 校验、
>   归一化落库），`view_task_playbook` 渲染计划。
> - **P1b 运行时消费**：Fetch 按本任务 `fetch_plan`/`task_fetch_targets` 取材；候选取材按任务隔离，
>   用户级去重避免同一内容跨任务重复轰炸。
> - **P1c 打分/出卡注入**：Score/CardGen 各自在 Activity 扇出前按属主读取一次已确认手册，
>   经统一 promptguard 消毒和 800-rune 上限后追加到锁定 user prompt 末尾；默认暗发布，先单任务 canary。
> 关联审计项：D-2（include_domains 半实现）、D-5（lookback_days 契约分歧）。
> 提出背景：Boss 希望「每个定时任务绑一份可编辑手册，任务跑时 AI 先读手册，据此决定
> 从哪些信源抓什么内容；agent 有工具能用自然语言改手册；删任务时连带删手册」。
>
> ## 决策记录（Boss 已拍板）
> - **D1（架构）**：**先 A 后 B**——稳定任务用方案 A（编辑期把手册翻译成结构化抓取配置、
>   确定性运行）；动态研究任务的方案 B 已列为后续 Agent Runtime 主航道。
> - **D2（创建即初始化）**：用户对 vane 智能体说"新建一个情报任务"时，**任务手册必须随任务创建
>   一起初始化**——即 `create_schedule` 升级为原子动作「建调度 spec + 从自然语言意图初始化手册与
>   抓取计划」。**一个定时任务 = 一份情报简报，自带手册定义它抓什么**（见 §6、§10#3 的连带影响）。

---

## 1. 目标与非目标

**目标**
- 每个定时推送任务（schedule）绑定一份**自然语言手册**，描述"这个任务要什么"。
- 稳定任务在创建/编辑时由 AI 编译手册，每次运行按已批准计划决定**抓哪些信源、要什么样的内容**；
  动态研究任务后续由受限 `PlanFetch` Activity 在运行时规划。
- 用户可用自然语言让 agent 改手册（"以后这个早报只要官方源"），无需改代码、无需重配源。
- 顺带解决 D-2 / D-5：把 Exa 的 `include_domains` / `lookback_days` 变成 agent 可按手册填的抓取参数。

**非目标**
- 不做手册的版本历史 / 回滚。
- 不做多用户共享手册（单 owner MVP）。
- 不改 M5 的锁定 system prompt、基础 user prompt、打分参数或演化契约；P1c 仅在命中任务手册时
  向 scorer/cardgen 的基础 user prompt 尾部追加受限任务指令块。

---

## 2. 与现有架构的关系（关键背景）

当前定时推送仍是一条**全确定性** Temporal 流水线，**抓取阶段没有运行期 agent/LLM 参与**：

```
Schedule 触发 → PushPipelineWorkflow
  → EvolveProfile → Fetch(按本任务已批准计划取材) → Dedup → Score(LLM逐条) → Select → CardGen(LLM) → Push
```

- `Fetch` 活动根据 `schedule_id` 消费已编译计划与材料化 `task_fetch_targets`；没有任务范围的
  运行直接拒绝。**目标与参数在创建/编辑期已批准**，运行时不临时扩大范围。
- LLM 只出现在 Score / CardGen / Evolve（逐条、确定性关进 Activity）。

Boss 的愿景把抓取阶段从"抓全部订阅源"演进为"创建/编辑期由 agent 编译手册，运行期按批准计划抓"。
P1c 再让相同任务定义约束评分与呈现；动态任务的运行期规划由后续 Agent Runtime 承接。

**可复用的地基**：`agent.Loop.RunOnce`（#57 为 A2A 轨新增）已能"在给定历史上跑一轮多轮 FC"。
后续 `PlanFetch` 可复用循环核心，但必须装配成独立、只读、最小记忆切片与显式能力白名单的实例，
不能把对话 Agent 的完整权限直接带进定时任务。

---

## 3. 核心架构决策（已拍板）

**手册"影响抓取"有两种落法，成本/复杂度差很多：**

### 方案 A：手册参数化确定性抓取（轻，推荐作为 P1）
手册在**编辑时**由 agent 解析成结构化配置（这个任务启用哪些源、每个源的 Exa 参数如 include_domains/lookback），存进 DB。任务运行时**不调 LLM**，直接按这份结构化配置确定性抓取。
- ✅ 运行时零额外 LLM 成本、零非确定性、不碰 Temporal 重放。
- ✅ "AI 读手册"发生在**用户改手册那一刻**（agent 把自然语言手册翻译成结构化抓取配置），符合"自然语言调整任务"。
- ⚠️ 不是"每次跑都现读现判"——但对"从哪些源抓什么"这类**相对稳定**的意图，编辑时定好就够了。

### 方案 B：运行时 agentic 抓取（重，P2/未来）
定时任务运行时**引入一个 agent 步骤**（新 Activity 包 `RunOnce`）：agent 读手册 + 用 `search_endpoints`/抓取工具**动态**决定这次抓什么。
- ✅ 最贴合"每次跑时 AI 现读手册再抓"，能处理"今天有什么热点就抓什么"这类**动态**意图。
- ⚠️ 每次定时跑多一次（多轮）LLM 调用：成本、延迟、非确定性；LLM 必须关进 Activity（Temporal 确定性）；失败要 fail-open（沿用 M6 §10.6 红线）。
- ⚠️ 与现有"订阅源"模型如何共存要想清楚（手册抓的内容 vs 用户订阅的源，是替代还是叠加）。

> ✅ **Boss 已拍板：先 A 后 B（决策 D1）**。P1 用方案 A 把"手册 + 编辑工具 + Exa 参数 + 编辑期翻译成抓取配置"跑通；方案 B 已从可选设想升级为动态研究任务的后续主航道。本 RFC 其余章节以 A/P1 为主，B 的完整能力、预算与记忆契约另案定义。
>
> 注：方案 A 下"AI 读手册"发生在**用户建/改任务那一刻**（agent 把自然语言意图翻译成结构化抓取计划），与决策 D2「创建即初始化手册」天然契合——建情报任务时手册即被 agent 初始化好，之后每次定时跑按计划确定性抓取。

---

## 4. 手册"作用于哪一层"

✅ **Boss 已拍板（#2）：手册是一个情报任务的完整定义——既管"抓什么信息"，也管"推送时怎么呈现"。** 三层都作用：

| 层 | 手册是否作用 | 说明 |
|---|---|---|
| **抓取选材**（抓哪些源、什么内容、Exa 过滤） | ✅ | 方案 A 下 = 建/改任务时翻译成结构化抓取配置（fetch_plan）。 |
| **打分 scorer** | ✅ | 手册的"想要什么信息"注入打分：这个任务偏好/排除什么主题。 |
| **出卡 cardgen** | ✅ | 手册的"呈现形式"注入出卡：这个简报要长/短、要不要要点化等。 |

> **P1c 已按 M5 红线分层实现**：scorer/cardgen 的 system prompt 与基础布局继续**逐字锁定**；
> 黄金测试守着"画像空+无负反馈+空任务指令 = M3 逐字节一致"等不变量。手册注入只**在锁定 prompt 之外
> 追加一段"任务级指令"**，不改锁定部分。具体运行契约见 §8：任何未命中手册的路径都保留
> 旧请求逐字节一致；命中时也只追加有界数据块，不改变 system prompt、模型参数或工具权限。

---

## 5. 数据模型

新增迁移（下一个可用号，注意避开并行占号）：

```sql
CREATE TABLE schedule_playbooks (
    schedule_id  TEXT PRIMARY KEY            -- 1:1 绑定 schedules.schedule_id（Temporal schedule id）
                 REFERENCES schedules(schedule_id) ON DELETE CASCADE,  -- 删任务连带删手册
    content      TEXT NOT NULL DEFAULT '',   -- 自然语言手册（markdown）
    -- 方案 A：手册翻译出的结构化抓取配置（JSONB）。方案 B 不需要这列。
    fetch_plan   JSONB NOT NULL DEFAULT '{}',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- `ON DELETE CASCADE`：满足"只有删定时任务时才删手册"。
- `fetch_plan`（方案 A）：agent 编辑手册时产出的结构化配置，形如
  `{"sources":[{"platform":"web","capability":"search","params":{"query":"...","include_domains":[...],"lookback_days":3}}, ...]}`。
  运行时 Fetch 读它、确定性抓取。方案 B 则运行时由 agent 现算，不落这列。
- 校验、幂等键规则沿用 sourcespec（§7）。

**开放问题**：`schedules` 的镜像表主键是 Temporal schedule_id（TEXT）。手册与它 1:1 FK 没问题；但注意 schedules 真源在 Temporal，镜像行由 scheduler 维护——删任务时要保证先删 Temporal schedule、镜像行随之删、CASCADE 带走手册（顺序见 §8）。

---

## 6. Agent 工具（编辑手册）

**创建即初始化（决策 D2）**：`create_schedule` 工具升级为原子动作——建情报任务时，
除 spec（cron/interval）外，agent 把用户的自然语言意图（"每天早八，AI 官方新闻，只要权威源"）
**同时**初始化成手册 `content` + 翻译出 `fetch_plan`，与 schedule 一并落库。不存在"有任务没手册"
的中间态。（实现上：create_schedule 建 Temporal schedule + 镜像行后，同一路径写 schedule_playbooks 行。）

编辑/查看两个工具（M4 标准：读工具直接执行、写工具走确认卡）：

- `view_task_playbook(schedule_id)`：读工具，返回手册当前内容 + 翻译出的抓取计划摘要。
- `edit_task_playbook(schedule_id, content)`：写工具（Mutating，确认卡）。Execute 里：
  1. 存 `content`；
  2. **（方案 A）** 立刻由一次 LLM 调用把 `content` 翻译成 `fetch_plan`（结构化），经 sourcespec 校验后落库；翻译失败回自纠文案不落库。
  3. 归属校验：schedule 必须属于当前 owner（单 owner MVP 下天然成立，仍按 user 维度校验）。

> 自然语言全生命周期闭环：**建**——"帮我建个每天早八的 AI 官方新闻情报任务" → agent 调 create_schedule（spec + 初始化手册/计划）；**改**——"这个早报以后也带上 Anthropic 官博" → agent 调 edit_task_playbook → 确认卡 → 手册与计划更新；**删**——删任务连带删手册（§8）。

---

## 7. Exa 参数（含 D-2 / D-5 的解决）

无论 A/B，`include_domains` 与 `lookback_days` 都要成为 **web/search 源的正式抓取参数**：

- **`include_domains`（D-2 修复 + Exa 追新的主手段）**：接进 `sourcespec` 的 web/search 分支——进 `config`、**进幂等键**（契约 §5.2 规则 B 要求），修掉"exa.go 消费但无配置入口、又不入键"的半实现。**这是 Exa 追新的推荐路径**（§0.3 实测解药，15/15）。
- **`lookback_days`（D-5 ✅ 已定）**：**Exa 追新不依赖日期过滤**（§0.3 实测：startPublishedDate 把无 HTML 日期的官方页全删，0/15 vs 去掉后 13/15）。故对 **Exa（web/search）**：lookback **默认关闭、手册明确不建议**，仅保留为"用户死活要按日期"的逃生阀（不是推荐路径）。对 **RSS（web/feed）**：lookback **保持有效**——RSS 的 `pubDate` 是结构化必填字段（不是猜的，契约 §0.4 已确认），日期过滤对 feed 是对的。

**lookback_days 是什么、用在哪、为何慎用**（回答 Boss 的问题）：
- 是 Exa 搜索 API 的参数，限定只返回"最近 N 天发布"的结果（Exa 侧 `startPublishedDate`）。
- 用在 `fetcher/exa.go` 抓 web/search 源时：config 里 `lookback_days>0` 就把日期下限发给 Exa。
- 慎用原因：Exa 的"发布日期"是从 HTML 猜的，**官方/权威站点经常猜不出（null）**，按日期过滤会把这些"没日期"的结果连带删掉——实测官方站从 15 条掉到 0 条。所以"要最新"优先用 `include_domains` 限定优质源，只有用户明确"只要最近 N 天"时才开 lookback。
- **这正是手册要固化的判断**：手册模板内置一条"追新优先 include_domains，慎用 lookback"，agent 翻译时据此选参数。

**契约动作**：改 M6 §5.3——从"web/search 彻底不支持 lookback"改为"**默认关闭、手册强烈不建议**、仅作显式逃生阀；Exa 追新以 include_domains 为准"。`exa_test.go` 保留"显式传才生效"的用例、新增"include_domains 入幂等键"的用例；追新推荐路径的黄金用例钉在 include_domains 而非日期。RSS 的 lookback 语义不动。

---

## 8. 与现有 pipeline 的接线 / 迁移

**方案 A 的运行时改动（小）**：
- `Fetch` 活动：若该 schedule 有 `fetch_plan`，按计划抓（用计划里的源+参数）而非 `ListDueSourcesByUser` 的全部订阅；无手册的任务保持现状（全订阅源）——**向后兼容**。
- `PushParams` 已带 `schedule_id`，Fetch 据此查手册/计划。

**P1c 的 Score/CardGen 接线（已实现）**：
- `PushParams.ScheduleID` 只作为标识符进入 `ScoreIn`/`CardGenIn`；手册正文不进入 Temporal history。
- 两个 Activity 各自在并发扇出前调用一次 owner-scoped `GetSchedulePlaybook(user_id, schedule_id)`；
  Score 的 50 路与 CardGen 的 5 路各自复用同一次读取结果，不按条查库。
- scorer/cardgen 先构造原有 user prompt，再对非空正文执行
  `StripInvisible → legacy Sanitize → 任务手册专用定界符消毒 → 800 rune 截断 → TrimSpace`，最后追加系统生成的
  `【任务手册…】…【任务手册结束】` 块；只有确有手册时，旧 user prompt 中外部内容伪造的
  `【任务手册` 前缀也会被专用消毒，防它冒充“用户确认”块；无手册时仍逐字不动。system prompt 不变。
- 无 `ScheduleID`、灰度关闭、未命中 canary、NotFound、nil/空正文或 DB 读取错误都使用空指令，
  最终 LLM 请求回到逐字节旧路径。DB 错误只 WARN，不能把旁路增强升级成整批停摆。
- 日志只记录 stage、属主、任务/trace 标识、状态与 rune 数，绝不记录手册正文；不可解析 completion
  与上游错误响应也只记长度/摘要或结构化错误码，不回流原文。完整 LLM 请求仍按既有规则进入
  `llm_calls` 审计账本。
- 灰度配置默认关闭：`enabled=false` 为精确回滚；`enabled=true + 非空 schedule ID` 只开单任务；
  canary 通过后才用 `enabled=true + 空 ID + allow_all=true` 全量开启。启用时，空 ID 却未显式
  `allow_all`、仅空白 ID、或 canary 与 `allow_all` 同开都会拒绝启动，防漏配造成误放量；关闭时
  始终优先作为回滚开关。

**Temporal 兼容结论**：新增可选 JSON 字段不需要 `workflow.GetVersion`。旧历史已调度的 Activity
继续使用历史中旧载荷；尚未调度及后续运行使用含 `ScheduleID` 的新载荷。SDK replay 匹配
Activity ID/type 而非 input，post-P1b 与 post-A5 两代冻结历史均已重放通过，且测试直接核验两代
历史的 Score/CardGen JSON 物理上没有 `schedule_id`；回滚时旧结构会忽略未知 JSON 字段。
Activity type 负控必须触发 `TMPRL1100`，用于证明 replay 测试不是恒绿。

**方案 B 的运行时改动（大，预留）**：
- 新增 `PlanFetch` 活动：独立只读 Agent 读手册并产出只引用注册能力的结构化 Execution Plan，
  由固定 Go 代码做类型、URL、SSRF、权限和预算校验后再进入 Fetch。LLM 只在 Activity 内；规划失败
  或预算耗尽时复用最近一次已批准有效计划，没有有效计划则记录 blocked 并通知，**不得**回退抓用户全部订阅源。

**删除顺序**（删任务）：scheduler.DeletePush 先删 Temporal schedule → 删 schedules 镜像行 → CASCADE 带走 schedule_playbooks。需在 DeletePush 里保证镜像行确实删除（现有实现镜像删除失败只 slog，要评估是否会留下孤儿手册——建议 playbook 的 FK 指向镜像表并允许 orphan 清理任务兜底）。

---

## 9. 分期建议

| 阶段 | 内容 | 价值 / 风险 | 状态 |
|---|---|---|---|
| **P0** | 迁移建表 + `view/edit_task_playbook` 工具 + `create_schedule` 创建即初始化手册（决策 D2）+ 手册存取（暂不影响抓取） | 能建情报任务并带手册、能改能看，零运行时风险 | ✅ 已实现（#68） |
| **P1a 编译层** | create/edit 时把手册正文一次 LLM 翻译成 `fetch_plan`（逐源 sourcespec 校验+归一化落库）+ `view` 渲染计划 + Exa 参数入 sourcespec（修 D-2）+ 改 M6 §5.3（定 D-5） | "AI 读手册出计划"的可见落点；零 M5/Temporal 风险；后续接抓取的地基 | ✅ 已实现（本轮） |
| **P1b 运行时消费** | `PushParams` 带 `schedule_id` + Fetch 按本任务 `fetch_plan` 抓（决策 §10#3）+ 候选池取材按任务归属（去重后改为用户级，见 §10#7 决策 A） | 情报任务真正按自己的手册决定抓什么；碰内容/候选模型 | ✅ 已实现（b1-b3 + reconcile；去重口径按决策 A） |
| **P1c 打分/出卡注入** | 手册的"想要什么/怎么呈现"注入 scorer/cardgen（§4，锁定 prompt 外追加任务级指令 + 无手册=逐字节一致的黄金测试） | 手册管全三层；默认暗发布、单任务 canary 后放量 | ✅ 已实现 |
| **P2（方案 B）** | 运行时受限 `PlanFetch` Agent 产出结构化 Execution Plan，再交固定流水线执行 | 动态热点类需求；后续 Agent Runtime 主航道，具体契约另案 | ⏳ 后续阶段 |

**P1a 编译层实现要点**（本轮，代码见 `agent/playbook_translate.go`）：
- **翻译**：一次 `llm.Do`（`DisableThinking:true`、温度 0，client 默认模型，走 recorder 记账），
  system prompt 锚定可用 platform/capability 词汇（严格对齐 sourcespec 消费面）+ D-5 追新用
  `include_domains` 不用日期过滤的铁律 + 宁缺毋滥不编造。
- **校验+落库形态**：模型输出由独立的任务 fetch-target 编译参数承载，
  逐目标经 `sourcespec.Build` 校验，**坏目标丢弃并 warn**；落库存**归一化后**的
  `PlannedSource{platform,capability,title,url(幂等键),config}`（即将来运行时抓取要消费的形态）。
- **best-effort 铁律**：翻译/落库任何失败只 slog、绝不影响主效果（调度已建 / 手册正文已存）；
  `SetFetchPlan` 只改 `fetch_plan` 列不动 `content`，且用 `UPDATE…FROM schedules` 依附**已存在**手册行
  （不建"有计划无正文"的孤儿行），归属+存在性进 SQL WHERE。
- **纯函数核心** `compilePlan`（解析+校验，不碰 LLM/DB）单测覆盖全部分支；`SetFetchPlan` 归属/归一化/
  依附由 store 集成测试覆盖。

---

## 10. 决策记录（原开放问题，均已拍板）

1. ~~§3 A/B~~ ✅ **D1：先 A 后 B**（稳定任务编辑期编译并确定性运行；动态任务的受限运行期规划为后续主航道）。
2. ~~§4 作用层~~ ✅ **手册管三层**：抓取选材 + 打分 + 出卡（手册 = 情报任务完整定义："要什么信息 + 怎么呈现"）。打分/出卡碰 M5，按 §4 的"锁定 prompt 外追加任务级指令"实现，P1 单列一节对齐 M5。
3. ~~手册 vs 订阅源~~ ✅ **只按本任务手册抓**（情报任务自包含、互不干扰）。
4. ~~无手册老任务~~ ✅ **逐步要求都建手册**：老任务暂保持现状（抓全部订阅），提供迁移/引导逐步补手册，最终所有任务都是自包含情报任务。
5. ~~D-5 lookback~~ ✅ **Exa 不依赖日期过滤**（实测 §0.3）：追新用 include_domains；Exa 的 lookback 默认关+手册不建议+留逃生阀；RSS 的 lookback 保持有效。改 M6 §5.3。D-2 的 include_domains 一并接进配置入口+幂等键。
6. ~~成本护栏~~ ✅ 方案 A 下只在建/改任务时做低频"意图→计划"翻译；方案 B 必须按 RunID/StepID
   复用付费检查点，并设置轮数、工具数、token、费用与总时限，具体数值在 Runtime 阶段冻结。
7. ~~候选去重口径~~ ✅ **决策 A（2026-07-19 拍板）：取材按任务隔离、去重按用户级**。b3 最初用 per-schedule 投递账本（`deliveries JOIN push_batches WHERE schedule_id`）追求「任务自包含互不干扰」，但任务从用户级转隔离时该账本为空 → 本任务源里用户**早已在全局推送里看过**的存量积压全被当成「没推过」重推一遍（实测：转隔离首日 47 条候选里 40 条是用户已读）。改成用户级去重（排除该用户已投递过的全部内容，任意批次/任意状态）后，隔离任务永不把用户看过的内容再推一遍。代价（已知、接受）：两个任务共享同一源时，同一内容只被先触发的那个推一次——轻微弱化「任务自包含」，换来「同一条永不重复轰炸用户」，对单用户产品是更优权衡。

**结论：7 个开放问题已全部拍板，本文已提级为正式契约；P0、P1a、P1b、P1c 均已落地。**
P1c 的生产启用仍必须遵循单任务 canary → 显式 `allow_all` 全量的灰度顺序；运行中编辑可能让 Score 与 CardGen
分别读到不同版本，这是当前诚实边界，留给后续 Approved Definition 运行快照收口。
