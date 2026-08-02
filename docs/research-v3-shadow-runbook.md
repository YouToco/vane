# Research V3 shadow runbook

Research V3 shadow 是正式切流前的独立技术验证路径。它读取 delivery-dark prepared sidecar 中的 V3 任务定义，生成真实的 Planner、工具证据与 Brief，但在 Workflow 层强制不调用投递 Activity。Prepare 不改变正式 Schedule 的 head、mode、Action 或 next run。

## 安全边界

- `pipeline.research_v3_shadow_canary_schedule_id` 只允许一个精确任务 ID；空值是硬关闭。
- `pipeline.research_v3_authority_canary_schedule_id` 默认必须为空；非空时必须与 shadow ID 完全相同，且没有 allow-all。
- Authority 非空时，`pipeline.push_effect_recovery_canary_schedule_id` 必须配置为同一个精确任务。Server 会在 Temporal worker 和任何飞书入站启动前同步完成 outbound route 预热与首轮 durable recovery；失败则整个服务启动 fail-closed。
- Authority 配置本身不会修改任何 Action。普通创建、编辑、reconcile 也永远不读取它来切 V3；只有 `researchcutover` 的持久化 saga 可以替换 exact-task Action。
- 切流后的每次启动 reconcile 都会在 tenant/user RLS 事务中把正式 Action token 的哈希与同任务 `enabled` authority 实时比对；token 被替换或 authority 已撤销时启动 fail-closed，不能把故障拖到下一次周一运行。
- Shadow 使用独立 Workflow ID，不读取、不更新、不触发原 Schedule；原 cron、时区、Overlap、Action 和下次周一 09:00 执行时间均保持不变。
- Scheduler 再核验任务 owner、active 状态与 exact prepared `vane.task-approved-definition/v3` sidecar；不会回退读取正式 head。
- Coordinator 在任何 Store、模型或网络副作用前再次核验精确任务授权。
- 即使 coordinator 错误返回 `delivery_allowed=true`，Shadow Workflow 仍跳过投递。

## 执行

先部署 migration、gateway、server 和 worker，并只配置 shadow ID。不要配置 authority ID。

用显式 policy JSON 准备 sidecar；任务名称、完整手册与调度 spec 由 Store 从当前 owner 投影读取，policy 文件不能覆盖它们：

```json
{
  "notification": {"minimum_significance": "major_updates_only", "suppress_empty": true},
  "output": {"language": "zh-CN", "format": "executive_brief", "instructions": "", "include_evidence_links": true},
  "planner_budget": {"max_planner_rounds": 8, "max_tool_calls": 16, "max_tokens": 32768, "max_cost_micro_usd": 1000000, "duration_ms": 300000}
}
```

```powershell
$env:VANE_MIGRATION_DB_URL = "<one-shot migration owner DSN>"
/opt/vane/bin/vane-research-prepare -operation prepare -task-id <schedule-id> -user-id <owner-user-id> -idempotency-key <stable-prepare-key> -policy-file <policy.json>
```

Prepare 重试必须复用同一个 key。相同 key 但 task projection/policy 不同会 fail-closed；旧 immutable definition 不会覆盖或删除。
`vane-research-prepare rollback` 只允许在尚未创建切流 journal 时执行；一旦
cutover 进入 prepared、pause 或后续恢复阶段，sidecar 会被硬围栏保留，必须先由
`vane-research-cutover rollback` 收敛到 `rolled_back`，避免调度已暂停但定义被删除。

```powershell
go run ./cmd/researchshadow -task-id <schedule-id> -user-id <owner-user-id> -idempotency-key <stable-key>
```

重试必须复用同一个 idempotency key；Temporal 只创建一次 shadow Workflow。

## 技术验收

1. Planner 仅依据当前任务手册与冻结工具目录生成计划。
   Planner 输入必须显式携带输出字段契约（顶层 `schema_version`、`steps`，step 内
   `invocation_id`、`tool_name`、`arguments`）；只给私有 schema 名称不构成可执行契约。
   `v3.1` 解码器拒绝重复键、未知键和缺失字段，但接受等价的空白与字段顺序；持久化层再
   生成 canonical plan，不能要求远端模型猜测 Go 编码器的字节表示。
   LLM receipt 只与模型实际拥有的 `schema_version + steps` 响应投影做 JSON 语义绑定；
   runtime 注入的 definition/catalog/Tool-policy digests 由 run snapshot admission fence
   独立约束。不得把两种不同所有权的信封做整份 JSON 相等比较，也不得为绕过 receipt
   而放松步骤投影的一致性。
   Prompt、correction 与 decoder 必须按快照冻结的 renderer 分派：历史 `v3` 原字节回放，
   `v3.1` 使用显式字段契约；未知版本 fail-closed。
2. 每个 Tool 只有一个 first-writer provider effect，Evidence 保存的是模型实际可见结果。
3. Brief 引用当前 Evidence，并按历史 Observation 做对比。
4. 无重大更新时 Brief 为 quiet，且没有 delivery 记录、飞书消息或推送副作用。
5. 原任务 ScheduleSpec、时区、Action 与 next run 在执行前后逐字一致。

满足以上条件后，提交老板 Gate。Cutover 入口还会从不可变的 V3 run snapshot 与 finalized Brief 重建“当前 definition 已成功完成 delivery-dark shadow 且没有 delivery”的持久证据；仅改配置或仅启动过 Workflow 不能绕过。未获批准前不得设置 authority ID。

## 老板 Gate 1：首个真实任务切流

Gate 获批后，把 shadow、authority 与 push-effect recovery ID 配置为同一个已验收任务并部署。Server 必须同时装配独立 Research runtime 与 receipt-backed delivery；任何 user/open_id、App 或 P2P chat 不一致都会在 provider 调用前 fail-closed。

配置不会改变周一 09:00 调度。使用稳定 key 执行唯一切流入口：

```powershell
$env:VANE_MIGRATION_DB_URL = "<one-shot migration owner DSN>"
go run ./cmd/researchcutover -operation cutover -task-id <schedule-id> -user-id <owner-user-id> -idempotency-key <stable-cutover-key>
```

`researchcutover` 是一次性 operator 控制面，只接受 migration owner credential（也支持 systemd `CREDENTIALS_DIRECTORY/migration_db_url`）；长期运行的 `vane_server_runtime` 永远不获得 cutover operator 身份。

该 saga 按“冻结完整 Schedule 与原 DB head/mode → CAS 暂停 → 原子 promote prepared head/mode → 仅替换 Action → 恢复原暂停状态”执行；cron、时区、Overlap、Workflow ID、Task Queue 与其他策略必须逐字保持。命令重试必须复用同一个 key。

切流后只验收一次临时真实运行：官方原文交叉核验、历史对比、专业卡片，以及无重大更新时零 delivery/零飞书消息。正式周一 09:00 不做临时改期。

## 回滚

先回滚，再清除 authority 配置；不能反序，否则正式 V3 Workflow 会先失去 runtime/delivery 依赖。

```powershell
go run ./cmd/researchcutover -operation rollback -task-id <schedule-id> -user-id <owner-user-id> -idempotency-key <same-stable-cutover-key>
```

Rollback 首先撤销数据库投递 authority，然后暂停、恢复冻结的旧 Action，并在仍暂停时原子恢复旧 DB head/mode，最后恢复原暂停状态。若发现外部紧急暂停且无法证明由 saga 持有，状态进入 `manual_intervention`，不会擅自恢复调度。只有输出 phase=`rolled_back` 后才能清除 authority ID；shadow ID 可保留用于无投递复测。

若只需撤销尚未切流的 prepared sidecar：

```powershell
/opt/vane/bin/vane-research-prepare -operation rollback -task-id <schedule-id> -user-id <owner-user-id> -idempotency-key <same-stable-prepare-key>
```
