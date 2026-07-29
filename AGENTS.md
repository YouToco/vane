# 见微 Vane — AI 协作入口

> Boss 自研的 AI 个性化信息推送产品（Go + Temporal + Postgres + 飞书）。
> 本文件是薄入口：只放指针、红线和流程约定，内容以 docs/ 契约为准。

## 必读文档（按任务对号入座）

- 分支/提交/PR 规范：[docs/git-workflow.md](docs/git-workflow.md)（trunk-based、squash merge、Conventional Commits）
- 内容身份与去重：[docs/content-identity-contract.md](docs/content-identity-contract.md)（身份是 canonical_key，不是 external_id）
- M4 agent loop 历史契约：[docs/m4-agent-contract.md](docs/m4-agent-contract.md)（冻结 v1 读协议）
- 当前任务/Tool 运行契约：[docs/task-manual-tool-runtime.md](docs/task-manual-tool-runtime.md)
- 任务级 Source 隔离迁移：[docs/task-source-isolation-migration.md](docs/task-source-isolation-migration.md)
- M5 画像+反馈闭环契约：[docs/m5-profile-contract.md](docs/m5-profile-contract.md)（17 节签名级；第 16 节是 Gate 验证清单）
- 双轨 Agent Runtime：[docs/agent-runtime-contract.md](docs/agent-runtime-contract.md)（ExecutionMode、运行快照、PlanFetch、预算与发布列车）
- Phase 2 结构化 Insight：[docs/structured-insight-contract.md](docs/structured-insight-contract.md)（单次 CardGen、引用校验、Brief 冻结、Temporal/rollout 边界）
- Phase 2 事件证据地基：[docs/structured-event-evidence-contract.md](docs/structured-event-evidence-contract.md)（零调用点 observed-event provenance、first-writer replay 与后续接线边界）

## 构建 / 测试 / 部署

- `make build` / `make test`（全仓 -race）/ `make lint` 是合并与发布入口；开发过程中按下方 S/A/B
  风险分级先跑受影响包，不得每次小改都机械重跑全仓。
- push 到 main 自动 CI → 部署 VPS（systemd `vane.service`，`/opt/vane`，Docker 栈：Postgres 18 / Temporal / Caddy）
- VPS：ssh alias `vane`（凭证在 my-credentials，不在本仓库）；域名 https://vane.zhuoqidev.com
- 每日推送调度：00:30 UTC（北京 08:30；VPS 本地 EDT 为前一天 20:30）
- LLM 调用明细（打分 prompt/completion/token）落 DB `llm_calls` 表，不在系统日志里

## 踩坑红线（均为历史真实事故，违反必炸）

1. **DeepSeek V4 结构化输出必须 `thinking: disabled`** — 默认 reasoning 吃光小 max_tokens 预算致 content 恒空，曾造成三批假 50 分静默照推（M3 事故）。
2. **Temporal `ExecuteWorkflow` 对运行中同 ID workflow 默认静默 attach 不报错** — 需显式 `WorkflowExecutionErrorWhenAlreadyStarted: true`（M4 Gate ⑦ 假"已触发"事故）。
3. **错误卫生**：原始 error 链（可能含连接串、Temporal 服务端原文）不得喂进模型上下文或用户文案，只落 `AppError.Message`。
4. **任务写操作只走 Prepare → Execute → Agent 会话回执**；不得重新引入确认卡、轮询确认资源或通用 pending action。历史 `[卡片回调]` 只能由只读兼容层解释。
5. **数据是资产不清理**：内容表无 TTL，不要加"过期清理"逻辑。
6. **查 VPS 日志**：`journalctl --since` 按主机本地时区（EDT）解释，DB 是 UTC，北京时间差 12 小时——先 `date` 核对再查；"grep 计数为 0"下结论前先用已知存在的记录验证查询窗口。
7. **核对 DB 行数只认 `count(*)`，不认 `pg_stat_user_tables.n_live_tup`** — 后者是 autovacuum 维护的估算值，会谎报：#28 验证测试库残留时它报 `users=1`，实际为 0。与第 6 条同源——计数类结论落地前先验证来源本身可信。

## 风险分级验证与审查（2026-07-23）

目标是把严格审查留给真正可能造成越权、数据损坏、重复付费或不可恢复副作用的改动；不得因为历史上
某个高风险 PR 用过全套流程，就把全套流程复制到所有后续改动。

开始编码前先按**语义影响面**标 S/A/B，并写出本次验收条件；文件数、代码行数和“跨包”本身不自动升档。
一个 PR/turn 按其中最高风险的语义改动定级，不能把 S/A 级功能附带的文档或测试夹具单独降成 B 级。
开发中若触发更高风险条件再升级，不得为追求形式完整主动扩大范围。

| 等级 | 适用范围 | 必需 Gate | 默认不做 |
|---|---|---|---|
| **S：安全/一致性关键** | migration/RLS/租户与权限、鉴权、配额计费；Temporal lease/fence/recovery；outbox/幂等/跨系统事务；确认后执行不可逆副作用 | 受影响包 + 全仓 race；相关时真 PG/Temporal 故障测试；直接证明不变量的最小 mutation；两名不同视角独立审查；完整 CI；部署探针与对应生产 Gate | 第三名及更多审查者，除非前两名存在未解决分歧或已确认 HIGH |
| **A：核心产品行为** | Agent 工具路由、模型协议与 prompt；任务 prepare/编辑；acquisition Tool；打分、推送、反馈等可恢复业务行为 | 受影响包定向 race；至少一条行为级集成/replay 测试；一名独立审查者；合并前全仓 CI 一次；用户可见变化做一次针对性 smoke | 多轮 mutation、双终审、真故障矩阵、完整生产 Gate，除非测试或首轮审查给出具体高风险证据 |
| **B：低风险** | UI 样式/文案/i18n、文档、测试夹具、生成物、已证明不改行为的局部重构 | 相关单测、lint 或 build；一次自审或普通 review | 真 DB/Temporal、mutation、多 agent 审查、全套生产 Gate |

### 收口与停止规则

1. **首轮独立审查无 HIGH 即停止加审**；S 级固定两名，只有明确分歧或确认的 HIGH 才增加视角。
2. mutation 必须绑定一个具体不变量，且能说明“改哪一处应当变红”；禁止为了有 mutation 记录而批量造变体。
3. 文档、注释或测试夹具修正后，只重跑受影响 Gate；全仓测试在合并前跑一次，不在每次编辑后重复。
4. 定向红测转绿后立即 checkpoint commit/报告；实现、全面回归、部署和真人验收可以拆成不同 turn，
   不得把所有阶段塞进一个数小时无停靠点的 turn。
5. 审查发现必须带代码路径、反例或可执行验证；纯推测先标问题，不得据此扩架构或升档。
6. 真人 Gate 用于里程碑或真实用户交互边界，不要求每个内部 PR 都重复一遍。
7. 当前任务若从原验收条件扩成协议迁移、架构重构或额外产品功能，先停下报告并另开批次，不能顺手吞并。

## 其他流程约定

- **里程碑收官走 Gate**：真人实测清单 + 服务端探针（清单在对应契约文档），全过才打版本 tag。
- 密钥/token 一律不入库。
