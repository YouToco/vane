# 见微 Vane — AI 协作入口

> Boss 自研的 AI 个性化信息推送产品（Go + Temporal + Postgres + 飞书）。
> 本文件是薄入口：只放指针、红线和流程约定，内容以 docs/ 契约为准。

## 必读文档（按任务对号入座）

- 分支/提交/PR 规范：[docs/git-workflow.md](docs/git-workflow.md)（trunk-based、squash merge、Conventional Commits）
- 内容身份与去重：[docs/content-identity-contract.md](docs/content-identity-contract.md)（身份是 canonical_key，不是 external_id）
- M4 agent loop 契约：[docs/m4-agent-contract.md](docs/m4-agent-contract.md)（工具、确认卡、回调链路）
- M5 画像+反馈闭环契约：[docs/m5-profile-contract.md](docs/m5-profile-contract.md)（17 节签名级；第 16 节是 Gate 验证清单）

## 构建 / 测试 / 部署

- `make build` / `make test`（-race 必须过）/ `make lint`
- push 到 main 自动 CI → 部署 VPS（systemd `vane.service`，`/opt/vane`，Docker 栈：Postgres 18 / Temporal / Caddy）
- VPS：ssh alias `vane`（凭证在 my-credentials，不在本仓库）；域名 https://vane.zhuoqidev.com
- 每日推送调度：00:30 UTC（北京 08:30；VPS 本地 EDT 为前一天 20:30）
- LLM 调用明细（打分 prompt/completion/token）落 DB `llm_calls` 表，不在系统日志里

## 踩坑红线（均为历史真实事故，违反必炸）

1. **DeepSeek V4 结构化输出必须 `thinking: disabled`** — 默认 reasoning 吃光小 max_tokens 预算致 content 恒空，曾造成三批假 50 分静默照推（M3 事故）。
2. **Temporal `ExecuteWorkflow` 对运行中同 ID workflow 默认静默 attach 不报错** — 需显式 `WorkflowExecutionErrorWhenAlreadyStarted: true`（M4 Gate ⑦ 假"已触发"事故）。
3. **错误卫生**：原始 error 链（可能含连接串、Temporal 服务端原文）不得喂进模型上下文或用户文案，只落 `AppError.Message`。
4. **卡片回调路径的动作必须回写 agent 会话**（`[卡片回调]` user 消息），否则模型对已处理动作产生状态幻觉。
5. **数据是资产不清理**：内容表无 TTL，不要加"过期清理"逻辑。
6. **查 VPS 日志**：`journalctl --since` 按主机本地时区（EDT）解释，DB 是 UTC，北京时间差 12 小时——先 `date` 核对再查；"grep 计数为 0"下结论前先用已知存在的记录验证查询窗口。
7. **核对 DB 行数只认 `count(*)`，不认 `pg_stat_user_tables.n_live_tup`** — 后者是 autovacuum 维护的估算值，会谎报：#28 验证测试库残留时它报 `users=1`，实际为 0。与第 6 条同源——计数类结论落地前先验证来源本身可信。

## 流程约定

- **对抗审查按风险分级**：核心路径（推送 pipeline / 打分 / 回调 / 演化）或跨包契约变更的 PR 上全流程（多 agent 并行实现 + 双怀疑者审查 + 突变体实验）；外围改动单轮 review 即可。
- **里程碑收官走 Gate**：真人实测清单 + 服务端探针（清单在对应契约文档），全过才打版本 tag。
- 密钥/token 一律不入库。
