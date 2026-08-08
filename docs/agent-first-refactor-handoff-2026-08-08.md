# Agent-first 重构换机交接（2026-08-08）

本文只记录可由 Git 远端恢复的开发事实和不含凭证的生产状态。完整设计与安全边界见
[Agent-first 用户情报与证据契约](agent-first-intelligence-contract.md)，V3 操作约束见
[Research V3 shadow runbook](research-v3-shadow-runbook.md)。

## 远端恢复入口

- 仓库：`https://github.com/YouToco/vane.git`
- 接续分支：`agent-first-finalize-20260808`
- 创建本机 worktree：

```powershell
git clone https://github.com/YouToco/vane.git D:\dev\vane
git -C D:\dev\vane fetch origin --prune
git -C D:\dev\vane worktree add D:\dev\vane-worktrees\agent-first-finalize-20260808 -b agent-first-finalize-20260808 origin/agent-first-finalize-20260808
```

worktree 是本机目录，不会被 GitHub 直接同步；远端分支、提交和本文才是换机恢复点。
不要从旧本机 `D:\dev\vane` 的 dirty `main` 继续开发。

## 已完成并进入 `origin/main`

截至基线 `180c8c92d8f9f89265f280851145caeb67fad3d2`（PR #305）：

- `query_my_intelligence` 已成为固定用户情报查询入口，覆盖任务、运行、Observation、Brief、
  Agent turn、Tool evidence、画像与反馈，并由认证 scope、RLS、限额和签名游标隔离。
- `manage_tasks` 已收敛创建、编辑、运行和批量删除；按自然名称定位，不要求用户提供 ID，
  明确指令直接执行，含糊指令自然追问，不再生成确认卡。
- owner Agent 的窄读写工具、关键词路由和编辑专用状态机已从生产工具面移除；
  `update_profile` 和公开研究工具保留在正交工具面。
- 不可变 Agent turn/tool evidence、V3 历史投影、公开证据隔离综合和管理后台审计投影已落地。
- `vane.task-approved-definition/v3`、动态研究规划、receipt-backed LLM 调用、delivery-dark shadow、
  exact-task cutover/rollback 与逐任务 authority 已落地。
- 新建/编辑 V1/V2 已在应用与数据库边界封禁；已执行 migration 和历史解码/replay 兼容仍保留。
- Kimi 已完成首个真实 V3 切流及 quiet-run 验证，因此老板 Gate 1 已满足。

## 2026-08-08 生产事实

- `vane.service` 和 Research gateway 正常，`/readyz` 返回 `ok`。
- 持久 server canary 仍指向 Kimi 任务；一次性 A 任务暗跑环境已从 `/run` 清除，未修改持久环境文件。
- 两条待迁移正式任务 A/B 都保持 `paused`，调度仍为 `0 9 * * 1`、`Asia/Shanghai`；
  本轮没有改 ScheduleSpec、next run 或周一 09:00 正式调度。
- A 任务已有 prepared V3 sidecar；B 尚未 prepare。
- A 的 delivery-dark shadow 已真实执行 Planner 和 Tool 阶段，但在
  `SynthesizeResearchBriefV3` 返回 `CONFLICT`。Workflow ID：
  `research-v3-shadow-06d438356daa12d36477339c750e11cf4c839d830ce31195b859877d0f5511c2`，
  Run ID：`019fe086-df4b-7441-816f-dff2a8b6df45`。
- 该失败没有产生 delivery，也没有执行 A cutover；不能把它记成暗跑通过。
- 飞书曾把 Provider 失败错误归因为“本租户 LLM 额度已用尽”。老板确认 Kimi 和 DeepSeek
  账户余额充足；当前没有证据支持额度耗尽。该问题按要求仅记录为待诊断的错误归因，
  不在本次同步中修改。

## 下一位 Codex 的顺序

1. 从远端接续分支创建全新 worktree，先 `fetch --prune` 并核对 `origin/main`，不要复用旧 dirty worktree。
2. 只读检查 A shadow 的 service journal、`research_brief_syntheses`、run snapshot、LLM reservation/
   receipt 和 Evidence，定位 synthesis conflict；不要用新幂等 key 盲重跑。
3. 若需代码修复，按风险等级完成测试、审查、CI、合并和部署；若只是可恢复运行状态，证明后复用稳定 key。
4. A delivery-dark shadow 必须得到 finalized Brief、零 delivery，才允许 preflight/cutover；随后按同一门槛 prepare、
   shadow、cutover B。两条任务始终保留原暂停状态和周一 09:00 调度。
5. A/B 完成后验收远端 `cleanup/agent-first-runtime`，在旧 pending、活跃 V1/V2 workflow 和 retention Gate
   均满足后才删除旧生产路径；migration、历史数据和必要 replay 解码器不可删除。
6. 最后执行真实飞书 UAT：昨天/历史结论、为什么这么判断、今日变化、七日重大更新、自然名称批量操作、
   含糊写入追问、外部提示注入隔离，以及“无重大更新不推送”。

## 不可从 Git 恢复的内容

Git 不承载生产数据库、Temporal history、systemd 运行态、机器凭证、未提交文件或本机 worktree 布局。
新机器必须重新配置 SSH/部署凭证并对生产做只读核验；禁止把凭证写入本文或仓库。
