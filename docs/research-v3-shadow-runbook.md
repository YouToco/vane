# Research V3 shadow runbook

Research V3 shadow 是正式切流前的独立技术验证路径。它读取同一个已批准的 V3 任务定义，生成真实的 Planner、工具证据与 Brief，但在 Workflow 层强制不调用投递 Activity。

## 安全边界

- `pipeline.research_v3_shadow_canary_schedule_id` 只允许一个精确任务 ID；空值是硬关闭。
- `pipeline.research_v3_authority_canary_schedule_id` 默认必须为空。非空意味着修改该任务的 durable Schedule Action，属于老板“首个真实 V3 任务切流”Gate。
- Shadow 使用独立 Workflow ID，不读取、不更新、不触发原 Schedule；原 cron、时区、Overlap、Action 和下次周一 09:00 执行时间均保持不变。
- Scheduler 再核验任务 owner、active 状态与 current canonical `vane.task-approved-definition/v3`。
- Coordinator 在任何 Store、模型或网络副作用前再次核验精确任务授权。
- 即使 coordinator 错误返回 `delivery_allowed=true`，Shadow Workflow 仍跳过投递。

## 执行

先部署 migration、gateway、server 和 worker，并只配置 shadow ID。不要配置 authority ID。

```powershell
go run ./cmd/researchshadow -task-id <schedule-id> -user-id <owner-user-id> -idempotency-key <stable-key>
```

重试必须复用同一个 idempotency key；Temporal 只创建一次 shadow Workflow。

## 技术验收

1. Planner 仅依据当前任务手册与冻结工具目录生成计划。
2. 每个 Tool 只有一个 first-writer provider effect，Evidence 保存的是模型实际可见结果。
3. Brief 引用当前 Evidence，并按历史 Observation 做对比。
4. 无重大更新时 Brief 为 quiet，且没有 delivery 记录、飞书消息或推送副作用。
5. 原任务 ScheduleSpec、时区、Action 与 next run 在执行前后逐字一致。

满足以上条件后，提交老板 Gate。未获批准前不得设置 authority ID。
