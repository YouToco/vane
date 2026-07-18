# 任务手册（Task Playbook）设计方案 · RFC

> 状态：**草案（RFC）· Boss 已拍板核心两条（见下），其余待定**。
> 关联审计项：D-2（include_domains 半实现）、D-5（lookback_days 契约分歧）。
> 提出背景：Boss 希望「每个定时任务绑一份可编辑手册，任务跑时 AI 先读手册，据此决定
> 从哪些信源抓什么内容；agent 有工具能用自然语言改手册；删任务时连带删手册」。
>
> ## 决策记录（Boss 已拍板）
> - **D1（架构）**：**先 A 后 B**——P1 用方案 A（编辑期把手册翻译成结构化抓取配置、确定性运行），
>   待确有"动态热点"需求再上方案 B（运行期 agentic 抓取）。
> - **D2（创建即初始化）**：用户对 vane 智能体说"新建一个情报任务"时，**任务手册必须随任务创建
>   一起初始化**——即 `create_schedule` 升级为原子动作「建调度 spec + 从自然语言意图初始化手册与
>   抓取计划」。**一个定时任务 = 一份情报简报，自带手册定义它抓什么**（见 §6、§10#3 的连带影响）。

---

## 1. 目标与非目标

**目标**
- 每个定时推送任务（schedule）绑定一份**自然语言手册**，描述"这个任务要什么"。
- 任务每次运行时，**在抓取之前**由 AI 读手册，据此决定**抓哪些信源、要什么样的内容**。
- 用户可用自然语言让 agent 改手册（"以后这个早报只要官方源"），无需改代码、无需重配源。
- 顺带解决 D-2 / D-5：把 Exa 的 `include_domains` / `lookback_days` 变成 agent 可按手册填的抓取参数。

**非目标（本期不做）**
- 不做手册的版本历史 / 回滚。
- 不做多用户共享手册（单 owner MVP）。
- 不改打分/演化的核心契约（M5）——手册只在"抓取选材"层面起作用（见 §4 的落点讨论）。

---

## 2. 与现有架构的关系（关键背景）

当前定时推送是一条**全确定性** Temporal 流水线，**抓取阶段没有任何 agent/LLM 参与**：

```
Schedule 触发 → PushPipelineWorkflow
  → EvolveProfile → Fetch(抓该用户全部到期订阅源) → Dedup → Score(LLM逐条) → Select → CardGen(LLM) → Push
```

- `Fetch` 活动 = `ListDueSourcesByUser` 捞出用户**全部**到期订阅源，逐个抓。**源是预配置的**（add_source），抓取不做任何"这次该抓什么"的判断。
- LLM 只出现在 Score / CardGen / Evolve（逐条、确定性关进 Activity）。

Boss 的愿景改变了抓取阶段的性质：**从"抓全部订阅源"变成"agent 读手册后决定抓什么"**。这是本 RFC 的核心架构决策（§3）。

**可复用的地基**：`agent.Loop.RunOnce`（#57 为 A2A 轨新增）已能"在给定历史上跑一轮多轮 FC"，且带只读工具护栏。定时任务里的"agent 读手册→调抓取工具"完全可以复用它，不必新造 agent 运行时。

---

## 3. 核心架构决策（**需 Boss 拍板**）

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

> ✅ **Boss 已拍板：先 A 后 B（决策 D1）**。P1 用方案 A 把"手册 + 编辑工具 + Exa 参数 + 编辑期翻译成抓取配置"跑通，拿到 90% 价值且零运行时风险；待确有"动态热点"类需求，再上方案 B 的运行时 agentic 抓取。本 RFC 其余章节以 A 为主线，B 的接口预留在 §8。
>
> 注：方案 A 下"AI 读手册"发生在**用户建/改任务那一刻**（agent 把自然语言意图翻译成结构化抓取计划），与决策 D2「创建即初始化手册」天然契合——建情报任务时手册即被 agent 初始化好，之后每次定时跑按计划确定性抓取。

---

## 4. 手册"作用于哪一层"

澄清一个容易混的点：手册可能影响两层，本期**只做抓取选材层**：

| 层 | 手册是否作用 | 说明 |
|---|---|---|
| **抓取选材**（抓哪些源、什么内容、Exa 过滤） | ✅ 本期 | Boss 明确的诉求。方案 A 下 = 编辑时翻译成结构化抓取配置。 |
| 打分 scorer | ⛔ 本期不碰 | M5 契约锁定；画像已承担"这个用户偏好什么"。若将来要"这个任务偏好什么"再议。 |
| 出卡 cardgen | ⛔ 本期不碰 | 同上。 |

> 若 Boss 认为手册也该影响打分/出卡（"这个早报的卡片要更简短"），列为后续，不进本期，避免动 M5 红线。

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

- **`include_domains`（D-2 修复）**：接进 `sourcespec` 的 web/search 分支——进 `config`、**进幂等键**（契约 §5.2 规则 B 要求），修掉"exa.go 消费但无配置入口、又不入键"的半实现。
- **`lookback_days`（D-5 定夺）**：见下方"这个参数是什么/用在哪"，Boss 已问；本 RFC 建议**认可显式支持**（改 M6 §5.3），但**手册教 agent 慎用**。

**lookback_days 是什么、用在哪、为何慎用**（回答 Boss 的问题）：
- 是 Exa 搜索 API 的参数，限定只返回"最近 N 天发布"的结果（Exa 侧 `startPublishedDate`）。
- 用在 `fetcher/exa.go` 抓 web/search 源时：config 里 `lookback_days>0` 就把日期下限发给 Exa。
- 慎用原因：Exa 的"发布日期"是从 HTML 猜的，**官方/权威站点经常猜不出（null）**，按日期过滤会把这些"没日期"的结果连带删掉——实测官方站从 15 条掉到 0 条。所以"要最新"优先用 `include_domains` 限定优质源，只有用户明确"只要最近 N 天"时才开 lookback。
- **这正是手册要固化的判断**：手册模板内置一条"追新优先 include_domains，慎用 lookback"，agent 翻译时据此选参数。

**契约动作**：改 M6 §5.3——从"web/search 彻底不支持 lookback"改为"支持但默认不开、由手册/agent 显式设定，并注明 Exa 日期缺失的坑"。删 `exa_test.go` 里钉死"不支持"的用例，改为钉死"显式传才生效 + include_domains 入键"。

---

## 8. 与现有 pipeline 的接线 / 迁移

**方案 A 的运行时改动（小）**：
- `Fetch` 活动：若该 schedule 有 `fetch_plan`，按计划抓（用计划里的源+参数）而非 `ListDueSourcesByUser` 的全部订阅；无手册的任务保持现状（全订阅源）——**向后兼容**。
- `PushParams` 需带上 `schedule_id`（现在只有 UserID+Scope），Fetch 据此查手册/计划。

**方案 B 的运行时改动（大，预留）**：
- 新增 `PlanFetch` 活动：内部 `RunOnce` 让 agent 读手册 + 调抓取工具产出候选内容，替代/前置于确定性 Fetch。LLM 关进 Activity；fail-open（手册解析失败退回全订阅源抓取，不空跑）。

**删除顺序**（删任务）：scheduler.DeletePush 先删 Temporal schedule → 删 schedules 镜像行 → CASCADE 带走 schedule_playbooks。需在 DeletePush 里保证镜像行确实删除（现有实现镜像删除失败只 slog，要评估是否会留下孤儿手册——建议 playbook 的 FK 指向镜像表并允许 orphan 清理任务兜底）。

---

## 9. 分期建议

| 阶段 | 内容 | 价值 / 风险 |
|---|---|---|
| **P0** | 迁移建表 + `view/edit_task_playbook` 工具 + `create_schedule` 创建即初始化手册（决策 D2）+ 手册存取（暂不影响抓取） | 能建情报任务并带手册、能改能看，零运行时风险 |
| **P1（方案 A）** | 编辑/创建时把意图翻译成 `fetch_plan` + Fetch 按本任务计划抓（决策 D2/§10#3）+ Exa 参数入 sourcespec（修 D-2）+ 改 M6 §5.3（定 D-5） | 拿到主要价值：情报任务按自己的手册决定抓什么；确定性、低成本 |
| **P2（方案 B，可选）** | 运行时 `PlanFetch` agentic 抓取 | 动态热点类需求；成本/复杂度显著上升 |

---

## 10. 开放问题

1. ~~**§3 A/B**~~ ✅ **已定（D1）：先 A 后 B。**
2. **§4 作用层**：本期只作用于抓取选材（推荐），还是也要影响打分/出卡（碰 M5）？
3. **手册 vs 订阅源**（**当前最需拍板**）：决策 D2 的"情报任务 = 自带手册定义抓什么"模型下，
   有手册的任务倾向 **A) 只按本任务手册的 fetch_plan 抓**（任务自包含、互不干扰，推荐）；
   备选 **B) 手册优先 + 该用户其它订阅源兜底**。选 A 意味着"订阅源"更多是 add_source 的临时/全局
   信源池，而每个情报任务从中（或直接由手册）选定自己的一组——需确认这与现有全局订阅模型如何并存。
4. **无手册的老任务**：保持现状（抓全部订阅源）——确认此向后兼容行为可接受。
5. **D-5**：确认改 M6 §5.3 认可 lookback 显式支持（手册教慎用），而非彻底禁。
6. **成本护栏**：`create_schedule`/`edit_task_playbook` 各触发一次"意图→计划"的 LLM 翻译
   （低频、可接受）；方案 B 才是"每次定时跑多一轮 LLM"（P2 再议限额，连回审计 R-1 token 预算）。

---

*本 RFC 落地前不写实现代码。Boss 在以上 §10 拍板后，我据此更新为正式契约（docs/）并分期实现。*
