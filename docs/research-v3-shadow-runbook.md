# Research V3 shadow runbook

Research V3 shadow 是正式切流前的独立技术验证路径。它读取 delivery-dark prepared sidecar 中的 V3 任务定义，生成真实的 Planner、工具证据与 Brief，但在 Workflow 层强制不调用投递 Activity。Prepare 不改变正式 Schedule 的 head、mode、Action 或 next run。

## 安全边界

- 正常 server 部署中的 `pipeline.research_v3_shadow_canary_schedule_id` 与
  `pipeline.research_v3_authority_canary_schedule_id` 保持为空。Prepare、shadow、preflight、
  cutover、verify、rollback 只在一次性 operator 进程中通过
  `VANE_RESEARCH_OPERATOR_EXACT_TASK_ID` 获得一个精确任务的临时控制面能力。
- `pipeline.research_v3_runtime_enabled` 只装载长期 V3 worker/runtime/delivery 能力，不授予任何任务执行权。Agent-first server 因无条件提供原生 V3 `manage_tasks create`，要求它在首个 Store、worker 或 ingress 前即为 `true`；每个正式任务仍必须携带与 tenant/user/task 绑定、数据库状态为 `enabled` 的 authority token。首个正式切流前通过数据库零 enabled authority 保持任务 hard-dark。
- Server 从数据库中动态发现所有 `enabled` V3 authority，并在 Temporal worker 和任何
  飞书入站启动前同步完成 outbound route 预热与首轮 durable recovery；失败则整个服务
  启动 fail-closed。旧的 `pipeline.push_effect_recovery_canary_schedule_id` 只保留单任务
  兼容用途，不能作为已切流任务的长期恢复清单。
- Authority 配置本身不会修改任何 Action。普通创建、编辑、reconcile 也永远不读取它来切 V3；只有 `researchcutover` 的持久化 saga 可以替换 exact-task Action。
- 切流后的每次启动 reconcile、日常手动运行和 Server runtime 都会在 tenant/user RLS 事务中把正式 Action 的身份与 token 哈希和同任务 `enabled` authority 实时比对；token 被替换、跨任务复用或 authority 已撤销时 fail-closed。reconcile 与手动运行只校验/执行，不改 Schedule spec、status 或 Action。
- Shadow 使用独立 Workflow ID，不读取、不更新、不触发原 Schedule；原 cron、时区、Overlap、Action 和下次周一 09:00 执行时间均保持不变。
- Scheduler 再核验任务 owner、`active|paused` 状态与 exact prepared
  `vane.task-approved-definition/v3` sidecar；不会回退读取正式 head。暂停任务的 shadow
  只在 prepared-shadow capability 分支放行，正式运行的暂停/手动运行规则不变。
- Coordinator 在任何 policy 构造、Store 写入、模型或网络副作用前再次核验 Action tenant/user/task 与 enabled authority token。当前 authority canary ID 可以移到下一条待切流任务，已切流任务不会因此失去正式运行权。
- 每个 Shadow Tool first-writer 还会在数据库内重新核验：exact shadow Workflow、不可变
  snapshot、owner/active 租户与任务、当前 prepared head 及其 prepare journal 必须完全一致；
  正式 Schedule 可保持 `compiled`。撤销 sidecar 后只允许已有 ordinal 的幂等恢复，禁止任何
  新 Tool effect。正式运行仍逐字沿用 `discover_at_run`/manual-run authority 原规则。
- Shadow snapshot 创建与 Tool first-writer 都只在 exact-task advisory fence 下读取 owner、
  tenant、Schedule 和 prepared head；撤权写入使用同一独占 fence。读取不再叠加授权行锁，
  避免与 row-then-advisory 的撤权触发器形成死锁；正式 snapshot 分支保持原行锁规则。
- 即使 coordinator 错误返回 `delivery_allowed=true`，Shadow Workflow 仍跳过投递。

## 执行

先部署 migration、gateway、server 和 worker；长期 server 的 shadow/authority canary 保持为空。

用显式 policy JSON 准备 sidecar；任务名称、完整手册与调度 spec 由 Store 从当前 owner 投影读取，policy 文件不能覆盖它们：

```json
{
  "notification": {"minimum_significance": "major_updates_only", "suppress_empty": true},
  "output": {"language": "zh-CN", "format": "executive_brief", "instructions": "", "include_evidence_links": true},
  "planner_budget": {"max_planner_rounds": 8, "max_tool_calls": 16, "max_tokens": 32768, "max_cost_micro_usd": 1000000, "duration_ms": 300000}
}
```

```powershell
$env:VANE_RESEARCH_OPERATOR_EXACT_TASK_ID = "<schedule-id>"
$env:VANE_MIGRATION_DB_URL = "<one-shot migration owner DSN>"
/opt/vane/bin/vane-research-prepare -operation prepare -task-id <schedule-id> -idempotency-key <stable-prepare-key> -policy-file <policy.json>
```

Prepare 重试必须复用同一个 key。相同 key 但 task projection/policy 不同会 fail-closed；旧 immutable definition 不会覆盖或删除。
Prepare 会同时冻结当时的 `active|paused` 状态。paused 任务必须在保持 paused 时完成
prepare、shadow、preflight 与 cutover；active prepare 后出现的独立暂停不会被当作 shadow
授权，需在确认暂停原因后用新 key 重新 prepare。
`vane-research-prepare rollback` 只允许在尚未创建切流 journal 时执行；一旦
cutover 进入 prepared、pause 或后续恢复阶段，sidecar 会被硬围栏保留，必须先由
`vane-research-cutover rollback` 收敛到 `rolled_back`，避免调度已暂停但定义被删除。

```powershell
go run ./cmd/researchshadow -task-id <schedule-id> -idempotency-key <stable-key>
```

重试必须复用同一个 idempotency key；Temporal 只创建一次 shadow Workflow。命令会等待
Workflow 结束，并从数据库重建 finalized Brief、snapshot 与零 delivery 的暗跑证据；只收到
Temporal start receipt 不算成功。

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

满足以上条件后，提交老板 Gate。Cutover 入口还会从不可变的 V3 run snapshot 与 finalized Brief 重建“当前 definition 已成功完成 delivery-dark shadow 且没有 delivery”的持久证据；仅改配置或仅启动过 Workflow 不能绕过。未获批准前不得运行 cutover。

## 老板 Gate 1：首个真实任务切流

Gate 获批后，不修改 server 的 shadow/authority canary 配置；继续使用同一个一次性进程
精确任务环境。`pipeline.research_v3_runtime_enabled` 已是 Agent-first server 的启动前置条件，
不能等到此时才开启。Server 必须同时装配独立 Research runtime、动态 authority recovery
与 receipt-backed delivery；任何 user/open_id、App 或 P2P chat 不一致都会在 provider 调用前 fail-closed。

配置不会改变周一 09:00 调度。使用稳定 key 执行唯一切流入口：

```powershell
$env:VANE_MIGRATION_DB_URL = "<one-shot migration owner DSN>"
go run ./cmd/researchcutover -operation preflight -task-id <schedule-id> -idempotency-key <stable-cutover-key>
go run ./cmd/researchcutover -operation cutover -task-id <schedule-id> -idempotency-key <stable-cutover-key> -plan-digest <exact-preflight-plan-digest>
```

`researchcutover` 是一次性 operator 控制面，只接受 migration owner credential（也支持 systemd `CREDENTIALS_DIRECTORY/migration_db_url`）；长期运行的 `vane_server_runtime` 永远不获得 cutover operator 身份。

切完一个任务后清除一次性进程环境；`research_v3_runtime_enabled` 在 Agent-first server 生命周期内始终保持开启。数据库中的逐任务 authority 是正式运行与动态恢复的唯一授权，因此多个已切流 V3 任务可以并存。使用 `status` 查看 journal，使用 `verify` 重新校验数据库与 Temporal 的终态；两者都不会推进 saga。

该 saga 按“冻结完整 Schedule 与原 DB head/mode → CAS 暂停 → 原子 promote prepared head/mode → 仅替换 Action → 恢复原暂停状态”执行；cron、时区、Overlap、Workflow ID、Task Queue 与其他策略必须逐字保持。命令重试必须复用同一个 key。

切流后只验收一次临时真实运行：官方原文交叉核验、历史对比、专业卡片，以及无重大更新时零 delivery/零飞书消息。正式周一 09:00 不做临时改期。

## 回滚

回滚时重新设置同一个一次性 exact-task 环境，不修改长期 server 配置。即使已无 enabled
V3 任务，Agent-first server 的 `research_v3_runtime_enabled` 仍保持开启；任务 hard-dark 由
逐任务数据库 authority 保证。

```powershell
$env:VANE_RESEARCH_OPERATOR_EXACT_TASK_ID = "<schedule-id>"
go run ./cmd/researchcutover -operation rollback -task-id <schedule-id> -idempotency-key <same-stable-cutover-key>
```

Rollback 首先撤销数据库投递 authority，然后暂停、恢复冻结的旧 Action，并在仍暂停时原子恢复旧 DB head/mode，最后恢复原暂停状态。若发现外部紧急暂停且无法证明由 saga 持有，状态进入 `manual_intervention`，不会擅自恢复调度。只有输出 phase=`rolled_back` 且 `verify` 成功后，才能清除一次性 exact-task 环境。

若只需撤销尚未切流的 prepared sidecar：

```powershell
/opt/vane/bin/vane-research-prepare -operation rollback -task-id <schedule-id> -idempotency-key <same-stable-prepare-key>
```
