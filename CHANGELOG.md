# Changelog

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 SemVer。
里程碑与 tag 对应关系见 [docs/git-workflow.md](docs/git-workflow.md)。

## [Unreleased]

### Added
- **通用来源兜底解析**（功能清单 1.5，`feat/generic-fallback-fetcher`）：「试跑=准入」从绑定能力
  推广到 URL 类 web 能力（web/feed、web/contents）——`add_source` 确认后先真调一次，全过才落库，
  消除「冷门 URL 解析失败却回『已添加』、用户误以为已订阅」的假装成功。web/feed 试跑失败于
  「不是 feed」时走兜底解析（`fetcher/resolve.go`）：从页面 HTML 嗅探 autodiscovery 声明
  （`<link rel="alternate" type="application/rss+xml">`），发现真实 feed 地址则进拒绝话术建议
  改用它；未发现则建议 web/contents 页面监控或 web/search 关键词订阅。只建议、不静默改道
  （M6 §2.2；确认卡上是什么 URL 就订什么 URL）。试跑分派收敛在 `fetcher.Multi.Probe`
  （web/search 无试跑门返回 nil——入参是关键词无「来源解析」概念）；拒绝话术为 ProbeRejection
  人话（红线 3），瞬态失败不翻译（保持「稍后再试」语义）。运行期「不无限重试」由既有
  fail_count 链承载（3 次告警/10 次自动停用），零新增。
- **信源连续失败自动停用 + 重新启用**（功能 5.2 补全，`feat/source-auto-disable`）：在既有
  「连续失败 3 次发预警卡」之上，新增连续失败达 `disableFetchFailThreshold=10` 次自动置
  `disabled`（停止抓取），并发一张与预警卡区分的「已暂停 + 如何重新启用」卡。Boss 拍板
  「告警后再宽限」：3→10 之间继续抓取，短暂宕机的站点恢复即清零、不误停。重新启用两条入口
  （Boss 拍板「两者都要」）——① agent 工具 `enable_source`（飞书里对 AI 说「重新启用信源 X」，
  走确认卡）；② 后端 `POST /api/sources/{id}/enable`（供前端信源页「重新启用」按钮，前端 PR 另提）。
  store 新增 `DisableSourceIfActive`（仅 active→disabled 幂等翻转，一次性告警）与 `EnableSource`
  （归属校验进 SQL WHERE + 清零 fail_count + `next_fetch_at=now()` 立即恢复）。

### Changed
- **代码审计整改批次一**（`chore/audit-cleanup-batch`）：
  - **D-3** 移除 agent `add_source` 的 legacy `type` enum（`rss/exa/tikhub_xhs`）与相随的
    BuildLegacy/SourceType 分支——违反 M6 §13.1【硬约束】（legacy 只服务 HTTP api 的
    vane-web 兼容，绝不进 agent，否则模型会说「已添加 tikhub_xhs 源」）。HTTP 侧 BuildLegacy 保留。
  - **D-4** 修正 `PushScope.SourceIDs` 注释：诚实说明它只约束抓取、不过滤候选（非「只推这些源」）。
  - **R-5** 删除死方法 `scheduler.UpdatePushSpec` / `TriggerNow` 及 `api.Scheduler` 接口对应声明
    （无路由无调用，且 UpdatePushSpec 埋着「只改 Temporal 不回写镜像」的漂移隐患）。
  - **M-3** 补 agent 工具测试：抽出纯函数 `specFromArgs` 并逐 capability 守住入参→params 映射
    （此前 `add_source` 持具体 store 不可 fake、映射零测试），另补 6 个工具的 Summarize 覆盖。

### Removed
- **下线 `web/page_watch` 页面变化监控能力**（`refactor/drop-page-watch`）：改由 Exa `/contents`
  fetch API 覆盖，不再在 Go 侧自建抓取 + 基线 diff + LLM 重要性门。移除范围——
  `fetcher/pagewatch.go`、`store/pagesnapshots.go`、`fetcher.SnapshotStore` 接口、
  `types.CapPageWatch/KindChange/SnapshotVerdict/RefTypeSource/PageSnapshot`、scorer 的
  change 打分 prompt、workflow Dedup 的 change 豁免、sourcecatalog 的 `web/page_watch` 条目、
  agent `add_source` 工具对 `page_watch` 的暴露；迁移 016 DROP `page_snapshots` 表。
  `kind` 列与 `KindArticle` 保留（承载全内容；当前唯一种类）。M6 契约 §10 标注为已下线的历史设计记录。
  连带消解审计发现的 3 项（page_watch 幂等键裸 URL 源劫持、LLM 门 SettleSnapshot 死路径、
  PageWatchFetcher.Fetch 主流程零测试）。`go vet`/全量单测（23 包）绿。

### 已知取舍（M2 审查记录，后续里程碑处理）
- logout 仅清 cookie，无状态 HMAC token 到期前（30 天）理论上仍有效——泄漏 token 无法即时吊销。
  收紧方案：缩短 TTL 或签名里混入服务端 "valid-after" 时间戳。
- 优雅关停不等待飞书消息处理 goroutine：关停瞬间在途消息的 LLM 记账可能丢失一条（仅日志）。
  收紧方案：Manager 加处理中 goroutine 的 WaitGroup，st.Close() 前 Wait。

### 已知取舍（M4 审查记录）
- 卡片回调回写是异步旁路：工具执行的 ~30s 窗口内用户抢先发消息，该条消息可能暂看不到
  「[卡片回调]」通告（下一条自愈，有界最终一致）。

### 待办（跟进项）
- api/ratelimit.go 的 clientIP 取 X-Forwarded-For 最左段（同 A2A HIGH 的 CWE-348）——
  dashboard 登录限流可被伪造 XFF 绕过，待按 a2a/auth.go 的单跳可信反代逻辑修。
- 主域 vane.zhuoqidev.com/.well-known/ 未生效：后端部署未 reload 运行中的 Caddy，
  Caddyfile 改动悬空（api 子域正常，A2A 不阻塞）。

## [0.5.1] - 2026-07-17 — A2A server 上线 + M7 数据面

**A2A server（第一期 content.query）自用上线**：Gate ①-⑥⑧ 生产实测全过（enabled=true）——
① card 发现（api 子域）② 无 token 401 ③ 带 token 返真实库内内容（防空转，独立查库命中）
④ GetTask 轮询 COMPLETED ⑤ 终态 Cancel/拒收 ⑥ 错误文案无内部链 ⑧ 重启后 taskId 可查（DB 持久化）。

### Added
- **A2A server（#46，a2a-contract PR-3）**：`a2a/` 包（Deps/窄接口 + Bearer 认证 + Agent Card +
  content.query executor + SDK taskstore 适配 + 错误卫生）+ config A2AConfig（Enabled 默认 false /
  VANE_A2A_TOKEN / BaseURL）+ main.go enabled 门控装配 + 探针 P-A2A。SDK 钉 a2a-go/v2 v2.3.1，
  仅 JSON-RPC binding；确定性检索不经 LLM（零注入面、零 token）；不暴露画像/个性化分（§8 红线）
- **M7 只读数据端点**：`GET /api/deliveries` 推送历史（#43，功能 6.4 数据面）、
  `GET /api/admin/runstats` 运行统计（#47，功能 6.5 数据面，基于 llm_calls）

### Fixed
- 探针假击穿（首采后 24h）：§16.4 注入统计（#44）、§16.5 保尾统计（#45）窗口起点钳到画像创建时刻——
  画像存在前的打分写「暂无」/不含负面句是正确行为，不再误判红线
- A2A 对抗审查修复（随 #46）：X-Forwarded-For 最左段信任（CWE-348，改单跳可信反代取最右段）、
  taskstore 错误未消毒（SDK 把 Error() 逐字进 JSON-RPC response）、适配层 DB 超时、装配守卫

### 版本一致性
- `cmd/server/main.go` 的 `vaneVersion` 同步为 0.5.1，A2A AgentCard.version 与发布版本一致。

## [0.5.0] - 2026-07-17 — M5 画像+反馈闭环（越用越准）

**Gate：八项真人实测全过（2026-07-17）**——①画像采集 ②态度翻转 ③误判（👎+form 填原因，
misjudged+detail 落库、7 秒链路）④深度解读 ⑤追问 ⑥负反馈快通道（prompt 实批验证）
⑦画像演化（LLM 消费反馈+游标推进）⑧人工修正恒赢（重删标签演化不回加）。
③⑥⑧ 首测暴露的 4 个真 bug（#38/#39/#40/#41）当日修复并生产重验。

### Added
- **M5 画像+反馈闭环**（契约 #10 / 实现 #11）：画像首采确认卡 + `update_profile` 人工修正；
  打分画像化（profilehint 注入 + 负偏好句式）；负反馈快通道（14 天窗【近期不感兴趣】进
  score prompt）；演化慢通道（evolver：批量消费反馈 → LLM 全量重写 summary/tags，
  (updated_at, 游标) 双条件 CAS + 标签只增不减守门 + promptguard 定界消毒）
- **removed_tags 黑名单**（#41，migration 014，Gate ⑧）：人工删的标签入黑名单、人工加回出列
  （单语句集合运算）；演化三道防线（prompt 明令 + 黑名单渲染 + 代码硬过滤）——
  「人工删掉/加回的标签，演化无权再动」自此有数据层承载
- **卡片改版**（规格 #14 / 实现 1a08fdb）：emoji 按钮 + header 元信息 + 👎 后 form 原因输入；
  富文本 post 消息支持（#9，粘贴不再被拒）
- **M6 信源超前交付**（清单外补记）：三轴信源模型 + migration 008 + x/user_posts（#18）、
  Twitter fetcher 多态加固（#21/#22/#24/#32）、RSS categories 过滤（#27）+ lookback 防老文
  洪水（#13）、page_watch pipeline + KindChange（#30/#33）、kind 空串治理三件套（#31）
- **可观测性**：M5 Gate 服务端探针固化 + 只读看板端点（#15）；空批次落库并记录退出闸门（#25）
- **A2A server 基建**（服务端实现排 v0.5.0 后）：集成契约（#34）、Caddy `/.well-known/*`
  反代（#36）、tasks store + 内容检索 + migration 013（#37，含真实库量基准 6.3ms/278 行）
- feishu：打字指示器 + 机器人菜单命令（ada0f6e）、新增 5 个事件处理器（af2f5e9）、
  引用/回复消息拉取被引内容传给 agent（8262263）

### Fixed
- **卡片回调 200673 真因**（#38）：👎 后重建卡 form 提交按钮缺 `name` + `form_action_type`，
  整卡被飞书拒收——独立应用受控实验二分定位；误判提交后收起表单防重复输入（#39）
- SDK WS 模式卡片回调静默丢弃 patch（5a26823）
- 负反馈快通道空标题盲区（#40）：X 官号类无标题内容的 👎 曾对打分 prompt 不可见，
  空标题回退正文前 200 字符
- 迁移并发竞态：会话级 advisory lock（#35）；008 goose StatementBegin/End（#20）；
  xmax 扫描 Postgres 18 兼容（cb89495/e122c32）
- 小红书正文抓全文 + 证据不足禁止编造（#12）

### Docs
- M5 契约 §16 修订记录续 ×3（探针固化 / 空批次可见化 / Gate ⑧ removed_tags）；
  卡片改版设计存档（#14）；CLAUDE.md AI 协作薄入口 + count(*) 核对红线（#29）

## [0.4.0] - 2026-07-15 — M4 最小 agent loop

### Added
- agent/：最小 agent loop——飞书消息 → deepseek-v4-pro 原生 function calling 多轮 →
  7 工具（list/add/remove_source、list/create/remove_schedule、push_now）；
  会话 30min TTL + 60 条截断 + per-user 串行化；agent 关思维链（防打分事故复现）
- 写工具确认卡（AI 出预填、人点执行）：pending_actions 24h 有效 + 原子 Claim 防双击 +
  卡片回调（card.action.trigger 长连接）原地更新；owner 白名单纵深防御
- store/：migration 005（agent_sessions + pending_actions）+ 原子 append 会话消息
- 护栏：agent 全链路超时（单调用 120s / 每消息 5min）、push_now 确定性 workflow ID
  钉死并发=1、v4-pro 计价入账

### Fixed
- V4 默认思维链吃光 scorer/cardgen 的小 max_tokens 预算致 content 恒空、打分静默回退
  中位 50 分（三个批次全假分）——thinking:disabled + 空 content 记 WARN（#6）
- push_now 重复触发返回假"已触发"：SDK 对运行中同 ID workflow 默认静默 attach 不报错，
  显式 WorkflowExecutionErrorWhenAlreadyStarted 后"已有一次推送正在进行"文案生效（#8）
- 确认卡点击结果不回写会话致模型把已处理动作说成"等待确认"——「[卡片回调]」通告
  原子回写（独立 goroutine + 拿锁后另起 DB 预算；不续 TTL 防点卡复活超时会话）（#8）
- 错误卫生：失败通告/拒绝文案只落 AppError.Message，错误链（连接串、Temporal 服务端
  原文）不进模型上下文（#8）

### 过程
M4 spike：v4-pro function calling 6 场景×10 次 60/60 全过后定原生 FC 地基。契约先行
（docs/m4-agent-contract.md）5 包并行实现一次统编全绿；27 agent 对抗审查 22 发现 →
8 确认全处置（#7）。Gate 修复（#8）：16 agent 实现+审查（1 major：锁等待耗尽回调 ctx
致回写必败）+ 双怀疑者突变体实验补 4 个测试缺口（互锁并发/WithoutCancel/裸 error 回退/
already-started 分流），全程 -race 稳定。Gate：七项真人实测通过（2026-07-15，含防重复
触发与状态回写重测）。

## [0.3.1] - 2026-07-14 — M3.5 多源信息接入

### Added
- fetcher.Multi 按 src.Type 分发：RSS + Exa 搜索 + TikHub 小红书关键词三源并存；
  query/keyword 即信源（合成 URL 幂等键）；Fetch 只抓到期源（重试不重复计费付费 API）
- 跨源去重 per-source → per-user；候选窗口 per-source 配额（防单源饿死其他源）
- CRITICAL 修复：content 字节截断切裂中文 UTF-8 → Postgres 22021 拒写（改 rune 边界回退）

### 过程
契约实测先行（Exa/TikHub 真实 key 逐字段核对响应）；21 agent 对抗审查 17 发现 →
13 确认全处置（#5）。Gate：三源生产实测 71 条入库、5 张小红书解读卡送达（Boss 截图确认）。

## [0.3.0] - 2026-07-14 — M3 推送管道

### Added
- workflow/：Temporal PushPipelineWorkflow（Fetch→Dedup→Score→Select→CardGen→Push）+
  Temporal Schedule 定时调度（每日 08:30 Asia/Shanghai 推送今日精选）
- dedup/（simhash）、scorer/、selector/（TopN）、cardgen/、pusher/（飞书解读卡）
- POST /api/push/now 手动触发 + push_batches 幂等批次

### Fixed
- Dedup 自撞：fetcher 抓取时已写 simhash，查历史不排除本批 ID → 每条与自己相撞 →
  整批删光（M3 阻塞根因，#4 + DB 门控回归测试）

### 过程
Gate：push→202→33 条 BBC→5 张卡片飞书送达（Boss 截图确认）。

## [0.2.0] - 2026-07-14 — M2 LLM + 飞书通道 + Dashboard

### Added
- llm/：DeepSeek v4-flash 客户端（OpenAI 兼容，纯标准库）+ 信号量限流 + 调用记账
  （trace_id / span_name / cache 命中三态 / cost_usd 计算，写 llm_calls 表）
- feishu/：lark-oapi-go v3.9.9 WS 长连接管理器（连接/重配/状态）+ 消息处理链
  （飞书消息 → DeepSeek → 交互卡片回复）+ 凭证校验 + owner 授权白名单
- api/：HMAC 无状态会话 + 登录（限流 + body 限制 + 常数时间对比）+ 飞书接入管理端点
- store/：migration 002（settings 表）+ settings/llmcalls/users 数据访问
- Web Dashboard（vane-web）：登录 + 状态总览 + 飞书接入五步向导
- 部署：Caddy 主域名托管 SPA + 反代 /api；vane-web CI 自动部署 dist 到 VPS
- 安全加固：VPS sshd 关闭密码登录（仅 key 认证）

### 过程
6 agent 并行实现（先 1 个研究 agent 实测 lark SDK/DeepSeek v4 事实基准）+ 3 视角对抗审查
（含独立 verify 层，9 confirmed / 0 误报）：修复 1 major（reload 并发泄漏活 WS 连接）+
5 minor（登录限流/body 限制/owner 授权白名单/owner 捕获 race/迁移注释）+ 1 info（key 常量导出）。

## [0.1.0] - 2026-07-14 — M1 行走骨架

### Added
- types/：9 个领域实体 + 7 组枚举 + AppError 错误体系（自定义 Is、哨兵映射、可重试默认表）
- config/：Viper 配置加载，VANE_ 环境变量覆盖，敏感键显式 BindEnv
- store/：pgxpool 连接层 + goose 嵌入式迁移 001（9 张表）+ 启动期数据库可达等待
- cmd/server：/healthz + /readyz 探针、JSON 日志、优雅关停
- CI/CD：部署改 SSH key 认证，部署后 readyz 健康验证（消灭假绿）
- systemd 最小加固（NoNewPrivileges / ProtectSystem=strict / ProtectHome / PrivateTmp）

### 过程
4 agent 并行实现 + 3 视角对抗审查（24 findings，8 major 全修复：
4 处 Go/SQL 可空性契约冲突、llm_calls 补 span_name/user_id、
环境变量命名错位、部署无健康验证、SSH 密码认证）。
