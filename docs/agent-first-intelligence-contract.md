# Agent-first 用户情报与证据契约

> 状态：`vane.intelligence-catalog/v2`、唯一 Agent-first owner 工具面、V3 原生创建与原生编辑已落地；V3 真实任务迁移和旧路径物理删除按发布列车继续推进。

## 1. 产品边界

用户用任务名称、主题、用途和自然时间查询自己的情报，不需要知道内部任务 ID。模型只得到一个内部只读工具 `query_my_intelligence`，不能提交 SQL，也不能提交 `tenant_id`、`user_id` 或定时运行的任务范围。

身份由认证入口注入。交互 Agent 绑定当前 tenant/user；定时研究 Agent 额外绑定 exact task。任何数据集都先经过这三个范围，再应用模型提供的关系条件。

## 2. 固定语义目录

目录版本 `vane.intelligence-catalog/v2` 包含 v1 的全部只读数据集，并新增 canonical 反馈查询：

- `tasks`：任务名称、手册、调度、状态；
- `runs`：不可变运行身份及终态；
- `observations`：本任务、本次运行的 exact Observation；
- `briefs`：不可变 Brief 与生成时间；
- `agent_turns`：用户原话、最终回复、引用的调用与动作回执；
- `tool_calls`：模型实际看到的参数和结果；
- `profile`：当前来源化用户画像。
- `feedbacks`：推送后的追加式反馈事实、问题原因和当前有效态度；通过投递批次绑定任务与运行。

v1 已封存到 `AgentToolEvidenceV1` 的历史结果保持原字节和原版本，不做猜测性重写。v2 对七个 v1 数据集保持字段和关系查询语义兼容；旧签名游标仍可继续同一查询，下一页会如实标记当前 catalog 为 v2。

`feedbacks` 直接读取既有 canonical `feedbacks`，不复制成 Agent turn，也不创建专用反馈工具。`is_effective_attitude` 仅对 `interested/not_interested` 有值，并按当前画像 epoch 与 canonical supersession 规则确定；其他动作返回 null。旧 push-now 投递允许缺少 `task_ref/run_snapshot_id`，owner 仍可读取，定时 Agent 则由 exact-task fence 自动排除。`delivered_summary` 最多 2,000 字符，只用于把“刚才那条”关联回具体结论：migration 061 后已 sealed 的 canonical delivery 有不可变证据，旧/open delivery 仅是 `mixed` 的历史展示快照，不能宣称为 exact。Harness 会在通用查询返回前把该字段从可信反馈行删除，转成 historical public evidence sidecar；只有 Tools:nil 的公开摘要阶段能看到原文，最终无工具综合只看到来源绑定的降权摘要。数据集不返回内部 delivery ID、卡片 JSON 或原始网页正文。

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

## 7. Agent-first owner 工具面

- 生产飞书 owner chat 由 composition root 显式选择 `OwnerAgent` lane；它不是配置开关、灰度比例或用户 ID canary，不能因环境变量缺失回退旧工具。
- 因 Owner catalog 无条件提供原生 V3 `manage_tasks create`，server 启动必须显式设置 `pipeline.research_v3_runtime_enabled=true`；该 Gate 在首个 Store、worker 或 ingress 之前失败。此开关只保证运行能力持续在线，不授予任何任务执行权；正式运行仍逐任务校验数据库 authority token，shadow/cutover 仍保持 exact-task 语义。
- owner 普通聊天只装配 `query_my_intelligence`、`manage_tasks`、经统一裁决的 `update_profile` 与已配置的公开研究工具。构造时缺少 exact evidence writer、任一必需工具或混入八个旧任务工具都会拒绝启动。
- `list_schedules`、`view_task_playbook`、`view_task_latest_run`、`view_profile`、`create_schedule`、`edit_task_definition`、`run_task_now`、`remove_schedule` 不再进入 owner 或 A2A catalog，其 Go handler 和专用状态机已物理删除。Web `POST /api/task-actions` 与飞书共用唯一 Owner Loop；浏览器 `request_id+digest` 派生稳定 trace，已完成请求从 `AgentTurnRecordV1` 重放，不再调模型或重复写入。
- Web route 还会把可信 mode/selected task 写入仅存在于本轮 context 的 capability：create route 只能执行一次 `manage_tasks create`；edit route 只能 `edit` server 选中的唯一 task ref。`update_profile`、run/delete、跨目标 edit 和任何其他 mutating tool 都在 authorizer、Store 查询及副作用之前双层拒绝；模型提示词不承担这个安全边界。
- 旧 `intent_toolkits_shadow/owner_canary/allow_all` 配置、关键词首轮分类和 shadow diff 已删除。强模型始终看到完整的小型正交工具面；授权仍在工具边界执行。
- Owner Agent 不再隐式读取或注入画像；需要画像时模型显式查询 `profile`，因此画像影响会进入 exact tool evidence。
- `manage_tasks` 在 authenticated scope 内重新解析全部目标，再调用只看本轮原话、动作、changes 和可读目标摘要的 `authorize_owner_action`。创建提交完整 owner-visible 定义；编辑只提交用户明确要求改变的字段，由 V3 coordinator 在同一次最新 head 读取上保留其余字段并生成完整目标，避免模型猜旧配置和 read-then-write 覆盖。外部结果与历史不会进入裁决。
- `run`/`delete` 强制使用每个任务的耐久幂等命令。批量部分失败仍继续处理其他目标，并把 completed/failed 写入动作回执；用户只看到可读名称。
- `create` 的 action ID 只由认证 tenant/user/turn trace 派生，不包含模型生成的任务字段；浏览器响应丢失后即使模型把等价手册重新措辞，也只能恢复同一次创建，不能生成第二个任务。
- 开启 exact evidence 后，任何 provider call identity、scope 或 session 不变量不匹配都会中止回复，不能回退到 legacy preview。presentation guard 和内部引用脱敏都发生在最终 turn 提交之前。

### 7.1 内部历史与当前公开证据隔离综合

Agent-first 同一轮需要比较历史与当前网页时，必须先完成
`query_my_intelligence`，并在首个公开读取执行前冻结模型实际看到的 exact
内部证据、认证 scope 与集合摘要。公开正文进入后，内部查询和全部写工具继续由
Harness 确定性关闭；外部正文只能在隔离上下文中继续公开研究，不能改写已经冻结的
内部查询。`EffectActivationWrite` 同样关闭：动态端点只能在首个外部结果前完成发现，
不能由外部正文触发新的持久化 activation。

`tool_calls` 数据集必须逐行携带 `trust_type` provenance。`local` 行可以留在受信主
Agent；`external`、未知或缺失 provenance 的 `model_visible_result` 在工具返回前移出，
其 arguments、raw trace 与 raw invocation 同样不得留在主投影，只留下严格系统元数据
与 tenant/user/trace/invocation/arguments/result/coverage 共同派生的不可变
`public_evidence_ref`。arguments/trace/invocation 任一敏感字段的查询都必须自动补齐逐行
provenance 后再投影。历史记录、当前网页、动态 API、社媒及
`read_endpoint_result` 统一使用这一引用；URL 只是由已知 Tool 参数或结构化结果产生的
可选展示元数据，不参与证据身份。缺失 trace 的 legacy 行明确标记 `unbound_trace` 并
移除原文与原始标识；非 exact 的 local legacy 行同样 fail-closed。历史
`query_my_intelligence` 的 exact local wrapper 可保留本地参数，但其 result 可能嵌套旧
external 原文，因无法递归证明 provenance 而丢弃，不得冻结为受信内部证据。展示 URL
必须绑定到产生该 ref 的单次 invocation，不得从本轮累计搜索结果串入其他 ref。

`observations` 数据集虽然属于用户自己的历史记录，其中的 URL、标题、作者与正文仍是
外部公开证据，不因进入数据库而升级为受信指令。查询原始 Observation 时，Harness
必须把完整行移入既有 public evidence sidecar，主 Agent 只见严格元数据与
`public_evidence_ref/status`，并设置仅限本轮的 historical-public pending 状态。这个状态
不能依赖关键词判断：若主 Agent 下一步调用当前公开工具，Harness 在调用前冻结安全投影
并进入正常隔离研究；若主 Agent 直接文字收敛，Harness 丢弃该文字并转入历史公开证据的
隔离摘要与无工具综合出口。exact local、`legacy_local_unavailable`、`unbound_trace` 与
`unavailable` 不得设置 pending；状态保持到本轮可见 final，下一用户 turn 自然清零。
tasks/profile/runs、严格 Brief 与 AgentTurn 结论仍按受信内部证据处理。

公开研究结束后，模型先在隔离上下文输出严格的
`vane.public-evidence-summary/v1`。送入摘要器的 bundle 为每个既有 ref 携带完整、未静默
截断的 arguments JSON 与模型可见 result；arguments 必须是有效 UTF-8/JSON 且不超过
64 KiB，arguments 与 result 合计受本轮 512 KiB 预算约束。ref 身份继续绑定完整
arguments、可见 result、original size、truncation 与 provenance。摘要只接受固定字段、受限长度与结构化工具结果中
真实存在的 `public_evidence_ref`；Markdown 包装、未知字段、伪造 ref、正文 URL 或无效
`as_of` 均拒绝。最终综合
请求固定 `Tools:nil`，只包含当前用户原话、冻结的内部 exact evidence 与公开摘要。
原始网页正文和原生 Tool 协议不得进入最终综合请求或 `agent_sessions` 后续历史；完整
模型可见工具结果仍按 `AgentToolEvidenceV1` 独立留存。最终 turn 在返回前继续原子封存
内部与公开工具证据，且不得向用户暴露内部引用或证据摘要。最终模型不得自由输出 URL；
Harness 只根据摘要已采纳 ref 的可选规范化展示 URL 渲染链接。

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
