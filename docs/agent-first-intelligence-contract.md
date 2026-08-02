# Agent-first 用户情报与证据契约

> 状态：`vane.intelligence-catalog/v1` 数据层与默认关闭的 Agent-first owner canary 已落地；V3 运行时按发布列车继续推进。

## 1. 产品边界

用户用任务名称、主题、用途和自然时间查询自己的情报，不需要知道内部任务 ID。模型只得到一个内部只读工具 `query_my_intelligence`，不能提交 SQL，也不能提交 `tenant_id`、`user_id` 或定时运行的任务范围。

身份由认证入口注入。交互 Agent 绑定当前 tenant/user；定时研究 Agent 额外绑定 exact task。任何数据集都先经过这三个范围，再应用模型提供的关系条件。

## 2. 固定语义目录

目录版本 `vane.intelligence-catalog/v1` 只包含：

- `tasks`：任务名称、手册、调度、状态；
- `runs`：不可变运行身份及终态；
- `observations`：本任务、本次运行的 exact Observation；
- `briefs`：不可变 Brief 与生成时间；
- `agent_turns`：用户原话、最终回复、引用的调用与动作回执；
- `tool_calls`：模型实际看到的参数和结果；
- `profile`：当前来源化用户画像。

查询只接受列选择、参数化过滤、分组、固定聚合、排序、limit 与签名游标。单次只访问一个数据集；跨数据集问题由 Agent 发起多次只读查询，最后在无工具阶段综合。

## 3. 时间、分页与资源上限

- `today`、`yesterday`、`last_7_days` 由 Store 使用 exact task 的调度时区解析。
- 未先定位任务且用户名下存在零个或多个时区时，查询拒绝，不以 UTC 或服务器时区猜测。
- limit 为 1–100，返回 JSON 总量不超过 64 KiB，数据库预算 2 秒。
- 游标使用数据库持久、带版本且可轮换的 HMAC 密钥签名，并绑定 tenant/user/task、查询摘要和第一页的 `as_of` 水位。多实例/重启可继续验证旧版本密钥；跨身份、跨查询或篡改均 fail-closed。
- 分页使用最后一行的不可变排序值与记录引用组成 keyset，不使用 OFFSET；两页之间即使已读任务被硬删除，也不会跳过下一条。任务名称、状态、`updated_at` 等可编辑字段若需要第二页，Store 直接拒绝。

## 4. 可审计证据

`AgentToolEvidenceV1` 保存规范化参数、模型实际看到的 UTF-8 结果、原始大小、截断状态、信任类型与 SHA-256。模型可见结果最多 256 KiB。

`AgentTurnRecordV1` 保存当前用户原话、最终回复、turn/trace、引用的 invocation 与结构化动作回执。它不保存系统提示词、隐藏策略、凭证或思维链。

二者在一个事务中提交，以 `(tenant,user,trace,invocation_id)` 和 `(tenant,user,session,turn_id)` 幂等；同一身份不同字节返回冲突。使用工具的回复只有在该事务提交后才能交给用户。

现有 `tool_calls` 只保留 8 KiB preview 的记录以 `legacy_preview` 暴露；无法可靠关联旧回复与证据时，Agent turn coverage 为 `unavailable`，禁止猜测性回填。

## 5. 安全与保留

- 用户证据、turn 与普通查询审计三张表启用 tenant 与 user 双重 restrictive RLS。
- `vane_app` 只有所需列的 INSERT/SELECT；没有 UPDATE、DELETE、TRUNCATE。
- 查询审计只记录数据集、字段/操作形状、耗时、行数、状态与截断，不复制过滤值或结果。
- 无效 tenant/user 组合无法满足普通审计的 membership 外键，因此另写入不带业务结果、且不向运行角色开放的 owner-only 越权拒绝账本；拒绝审计失败时查询也失败。
- 固定语义 SQL 在 `NOLOGIN NOINHERIT NOBYPASSRLS` 的专用 reader 下执行。迁移会拒绝已有对象所有权、角色连线、参数权限或本库 ACL 污染，再按列授予最小读取面。
- 证据是用户业务资产，没有 TTL。只有显式 tenant hard purge 按 FK 子表顺序删除。
- Store SQL 来自版本化固定目录；模型值始终是参数，不进入标识符、表名或表达式。

## 6. 发布列车

1. 数据层：migration 085、通用查询编译器、证据原语与真实 PostgreSQL 隔离 Gate。
2. Agent 面：接入 `query_my_intelligence` 与 `manage_tasks`，移除窄读工具和意图/编辑状态机。
3. 运行面：`vane.task-approved-definition/v3` 与 `ResearchRunWorkflowV3` shadow/canary。
4. 清理面：生产审计证明没有旧 pending/V1/V2 活跃工作流后，删除旧确认/Source/工具生产路径。

整个列车不修改既有周一 9:00 正式调度。老板 Gate 只保留 V3 首个真实任务切流与全部旧运行路径删除。

## 7. Agent-first owner canary

- canary 同时要求本地开关与一个精确 `user_id`；进程内其他用户的新工具在声明面和执行面都不可用。
- canary 普通聊天只暴露 `query_my_intelligence`、`manage_tasks`、`update_profile` 与已装配的公开研究工具；八个旧任务读写工具只留给明确的 Web/历史兼容通道。
- Agent-first 不再隐式读取或注入画像；需要画像时模型显式查询 `profile`，因此画像影响会进入 exact tool evidence。
- `manage_tasks` 在 authenticated scope 内重新解析全部目标，再调用只看本轮原话、动作、changes 和可读目标摘要的 `authorize_owner_action`。外部结果与历史不会进入裁决。
- `run`/`delete` 强制使用每个任务的耐久幂等命令。批量部分失败仍继续处理其他目标，并把 completed/failed 写入动作回执；用户只看到可读名称。
- 开启 exact evidence 后，任何 provider call identity、scope 或 session 不变量不匹配都会中止回复，不能回退到 legacy preview。presentation guard 和内部引用脱敏都发生在最终 turn 提交之前。

## 8. V3 运行权限与付费调用回执

`app.tenant_id` / `app.user_id` 只用于兼容过滤，不能作为 V3 的授权根。每个 V3
快照在控制面事务中同时登记一个 exact-run capability hash；Activity 根据不可变快照引用和
进程密钥环重新派生 bearer，并只用 `SET LOCAL` 安装到单个受限事务。bearer 不进入
Temporal 参数、日志、错误或 JSON。能力绑定 snapshot、tenant、user、task、workflow、run
和 reference digest；active key 可轮换，retired derivation key 必须保留到最长 Temporal
恢复窗口之后。默认能力寿命为 90 天，部署值不得短于 Temporal retention。

模型调用采用三层职责分离：

1. 普通 V3 executor 只能申请不可变 reservation，不能直接写 `llm_calls`、结算或读取网关密钥；
2. 独立 OS 进程和 gateway login 在真正发 HTTP 前原子写入 `send_started`，模型返回后对
   exact request/response/usage/outcome 生成 HMAC 回执；主进程只可通过校验 peer UID 的
   Unix Socket 提交 reservation、digest 与 opaque capability，不能提交 prompt、模型或 usage；
3. PostgreSQL verifier 在 claim 时校验 live run capability，并在 terminal settlement 校验
   reservation、已授权 send marker、签名、时间窗和冻结 prompt，再原子写入调用证据与成本。
   capability 后续过期/撤销只能阻止新 claim，不能阻断已付费 effect 的结算。Plan/Brief 只接受
   `verified_gateway` 回执。

Provider 路由同样属于冻结快照：gateway 从数据库取得 provider、endpoint ID/generation 与
credential ID/generation，再由其私有 retained-route registry 解析具体 HTTPS endpoint 和
systemd credential。轮换必须新增 generation 并保留旧 route 到最长 Temporal 恢复窗口之后；
缺少旧 generation 时在网络请求前 fail-closed，绝不回退到“当前 key”。

恢复语义是保守且不重复付费：没有 `send_started` 的 reservation 可以安全继续；存在 marker
却没有完整签名回执时，禁止再次调用 provider；同一 Activity 用 exact binding 轮询，十分钟
恢复门槛后由 gateway 根据数据库中的冻结请求骨架签发 `indeterminate` 回执并保留全额预留。
Activity 单次预算必须大于该门槛；Provider 完成后的相同 settlement 会做有界幂等重试。
低报、零报、缺 usage 或执行器伪造的用量均不能
触发退款；真实用量高于预留时记入债务并阻止后续超额轮次。

Tool 调用同样采用 reservation floor：原子 admission 扣下的 `exa_calls` 是最低收费，后续
`attempted=false`、零用量或无 Tool call 的 settlement 只能记录结果，不能补回配额。096
之前已经发生的退款保持历史原样，不做猜测性回填；恢复重放只读取同一不可变 settlement，
因此既不会再次扣减，也不会产生补偿。完整 Tool provider receipt/gateway 属于后续独立边界。

当前生产开关继续 fail-closed：在独立 runtime/gateway 连接、能力密钥环、真实 PostgreSQL
攻击测试和 receipt-first coordinator 全部装配前，V3 Activity 虽已注册但没有可执行 runtime；
影子运行也不得借用旧 executor 或绕过签名结算。
