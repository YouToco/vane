# TikHub 端点注册表契约 — 检索式工具发现 + 动态注入 + 全量调用记账

> 定稿 2026-07-18。决策链：Boss 四项拍板（架构分层征询后改判动态注入）＋业内调研
> （Anthropic Tool Search Tool / OpenAI tool_search / langgraph-bigtool / RAG-MCP 等，
> 全部关键论断经二次核验，来源见 §0.2）。
> 实现：`tikhubcatalog/` + `tikhubinvoke/` + `agent/endpoint_tools.go` + migration 015。

## §0 背景与定位

### §0.1 要解决什么

TikHub 提供 1000+ 社媒数据端点（抖音/TikTok/小红书/微博/B站/知乎/快手/YouTube/
Instagram/Twitter 等 20 平台）。Boss 要求"全支持弄成 tool，agent 需要什么信源只用
搜索即可"，且**每次调用必须有记录**（否则之后优化调用逻辑、排查问题无从下手）。

### §0.2 为什么是「注册表 + 搜索 + 动态注入」

业内已收敛的结论（2026-07-18 调研，关键数字均经打开原文二次核验）：

- 工具数量直接打击选择准确率：候选 <30 时成功率 >90%，>100 急剧衰减；全量塞入
  1000 工具 ≈ 20 万 token 且成功率跌到 ~14%（RAG-MCP, arXiv 2505.03275）。
  Anthropic 官方口径：>30–50 个工具开始退化；OpenAI 官方建议单轮 <20。
- 两大 provider 已把解法做成原生特性：Anthropic Tool Search Tool（token -85%，
  MCP 评测 Opus 4 准确率 49%→74%）、OpenAI tool_search（defer_loading）。二者的
  client-executed 变体证明该流程可在自建 loop（DeepSeek FC）完全复刻。
- 两流派：检索后**动态注入**一等工具（Anthropic/OpenAI/langgraph-bigtool）vs
  **元工具转发** search+execute（Composio/Cloudflare Code Mode）。注入的参数准确率
  上限更好（per-endpoint schema 进 FC 声明）；代价是每轮 tools 数组变化。
  **Boss 拍板：动态注入**；缓存代价用 §4.3 的顺序纪律压到最低。

### §0.3 与 sourcecatalog 的分界（两层注册表，各守各的准入）

| | sourcecatalog（订阅信源层） | tikhubcatalog（lookup 层，本契约） |
|---|---|---|
| 用途 | 追新入库 → 打分 → 推送 | 一次性查询，结果回给模型阅读 |
| 准入 | 实测准入（M6 契约 §2，Available 是质量承诺） | spec 里存在即收录（未逐个实测） |
| 归一化 | 手写 fetcher 进 content_items | 零归一化，原文（截断）回模型 |
| 静默失败风险 | 有（坏源无声流进推送）→ 必须实测 | 无（错误被模型和用户直接看到） |

准入门槛差异的唯一依据是**静默失败通道是否存在**。端点查询结果**永不**写入
content_items；某端点若证明适合做订阅信源，走 sourcecatalog 实测准入另行实现。

> 【2026-07-18 修订】「另行实现」已落地：endpoint-binding-contract.md。lookup 通道
> 的「永不写入」不变；注册表端点可经绑定+试跑准入成为订阅信源（走信源通道与绑定
> 引擎归一化）。本节分界表述以该契约 §0 为准。

## §1 注册表数据（tikhubcatalog）

- **来源**：`catalog.json` 由 `tikhubcatalog/gen` 从 TikHub OpenAPI 3.1 spec 生成
  后**提交入库**（go:embed）。上游 spec 变更只能经 re-gen + code review 进入生产——
  不做运行时拉取，上游一次发布不得静默改变 agent 能调什么。
- **收录范围**（Boss 拍板）：排除平台管理类 6 个 tag（TikHub-User/Downloader/Demo/
  Health-Check/Temp-Mail/iOS-Shortcut），其余全收（含星图/DouPlus/广告等营销数据类
  ——对行业情报有用；营销类多需广告主账号授权，Boss 拍板先全留、靠调用日志实证哪些
  真能用）。**另按 path-slug 精确排除单个端点**（gen 的 excludedEndpoints）两类：
  ① 改第三方平台状态的写端点（刷播放 ×2、刷浏览、注册设备 ×2）——lookup 层免确认直调，
  混进来 = agent 可无确认刷量，与 §3「查询不改系统状态」冲突；② 越界/社工风险端点
  （生成发私信唤起链接 ×2，只读但偏门+风险，Boss 拍板排除）。精确匹配而非关键词，避免
  误伤「获取点赞/关注/收藏列表」这类只读 fetch_ 端点。当前 995 个端点、20 平台。
- **工具命名**：path-slug（`/api/v1/tiktok/web/fetch_post_detail` →
  `tiktok_web_fetch_post_detail`）。不用 operationId：FastAPI 生成的 operationId
  561/1024 超 FC 64 字符上限；path-slug 全量唯一且最长 61（实测）。gen 硬校验
  命名合法性与唯一性，违例直接生成失败，绝不静默截断。
- **瘦身**：description 截 600 rune、参数描述截 200 rune、响应 schema 全丢弃。
  POST body 的 `$ref` 解析一层（FastAPI Body_* 皆平铺 object）；`anyOf[T, null]`
  归一为 T；产物按 name 排序保 re-gen 确定性。

## §2 检索（BM25，embedding 留二期）

- 单一检索算法 BM25（k1=1.2, b=0.75，Lucene 非负 IDF 变体），倒排索引启动时建。
  依据：Anthropic 官方开箱只给 regex/BM25，工具目录短文本语料词法检索已被验证
  够用；DeepSeek 无 embedding API，语义检索需引第三方依赖，MVP 不值得。
- 分词：ASCII 字母/数字连续段小写成词 + CJK 连续段相邻双字 bigram（单字成词）。
  检索域 = 工具名 + 平台 + tag + summary + description + 参数名 + 参数描述
  （与 Anthropic Tool Search 检索面对齐）。
- topK=5（Anthropic 默认值同），platform 过滤是硬约束、大小写不敏感。
- **升级 embedding 的决策依据是数据不是感觉**：tool_calls 的 retrieval_query +
  candidate_tools 留痕（§6）攒够样本后再评估召回质量。

## §3 工具面（search_endpoints + 动态端点工具）

- `search_endpoints`（静态白名单成员，读工具）：`{query, platform?}` → top-5 端点，
  返回文本含端点名/方法路径/摘要/参数表，并把命中端点**激活**进会话。
  零命中给自纠指引；未知平台给平台清单。
- 动态端点工具：按 catalog Entry 即时构造，Parameters 从注册表参数生成 JSON
  schema（enum/default/required 齐全；未知类型退化 string——schema 是给模型的提示，
  权威校验在 Execute 参数校验 + 上游 422）。注入描述 = summary + description 截
  300 rune + 计费提醒（600 全文只出现在检索结果文本里，不随每轮请求重复发送）。
- **端点工具全部 Mutating=false**：查询不改系统状态，免确认卡（Boss 拍板）；
  推论：永不进 pending_actions，ExecuteAction 路径只需静态白名单。
- system prompt 仅在装配了端点工具面时追加能力说明段（可搜索平台清单 + 计费/
  限额提示 + "持续追新用 add_source 不用端点查询"）。

### §3.5 大结果：缓存句柄 + read_endpoint_result（2026-07-18 增补）

- **背景**：原先超限只硬截 6000 rune + 一行提示，被截部分无取回通道（Boss 在
  B 站评论查询实测撞死路）。业界调研（Claude Code / Pi / OpenClaw 源码级 +
  Codex 反例）结论：没有一家裸移除上限，共识 = **cap + 全量留存 + 句柄 + 取回
  工具**；Codex 是唯一只截不给取回的，其社区正开 issue 求这套方案。
- **机制**：2xx 响应超 6000 rune 时，全量进进程内 LRU 缓存（句柄 `res-N`、
  TTL 30min、上限 16 条 ×2MiB 封顶、**绑定 userID** 防跨用户句柄探测、重启失效
  ——零持久化零 migration，丢了就重查）；截断提示 = 大小 + 句柄 + **JSON 结构
  摘要**（深度 2 前 10 键，模型免读全文直接写路径）+ 强命令式取回指引 + 预览
  混淆压制话术（Claude Code 社区实测模型 ~70% 概率拿预览当全文，必须显式压）。
- **内联上限与预算（2026-07-18 定档，业界横比后由 6000 提到 40000）**：

  | | 单次内联上限 | 目标模型上下文 |
  |---|---|---|
  | Claude Code | MCP 25k tokens(≈100k chars) / Bash 30k chars / 落盘线 50k chars | ~200k |
  | Pi | 50KB（2000 行双限） | ~200k |
  | OpenClaw | 按窗口分档 16k/32k/64k chars，且 ≤ 窗口 30% | 按模型 |
  | Codex | 10k tokens（≈40k bytes） | ~272k |
  | **vane 旧值** | **6000 rune** | **1M（deepseek-v4-pro）** |

  旧值是「无取回通道」时代的产物（截了即丢，只能靠小上限少丢点），而我们窗口最大、
  上限最小，差一个数量级。新档：**单次 40000 rune**（对齐 Claude Code 的 MCP 档，
  同为「工具返回任意 JSON」场景；中文 JSON 约 1.5–2 char/token ≈ 20–27k token，
  占 1M 窗口 ~2.5%）。**放大的前提是句柄取回已存在**——上限从「数据生死线」降为
  「首屏预算」，超出部分随时可取。
- **每消息累计预算 120000 rune + 保底 4000 rune/次**：单次上限管不住 msgCap=10 次
  调用的总量（10×40000=400k），更贵的是 FC 多轮历史重发（MaxTurns=20 时按轮数乘，
  前缀缓存打折但不改数量级）。故按曲线分配：前几次给满、预算耗尽后降到保底
  （仍带句柄，不是数据丢失，只是首屏变窄）。续读 offset 用**本次实际展示量**，
  不是常量上限——降级后用常量会让模型从错位置续读、跳过中间一段。
- `read_endpoint_result`（静态白名单成员，读工具，**本地缓存读取不打上游、
  不计费、不占 §7 双限额**）：`{handle, path?, offset?, limit?}`；path 支持
  `name`/`name[i]`/`name[i:j]` 点路径取子树（sub-Turing，与绑定引擎同纪律），
  优先于 offset 分页；单次返回同样封顶 6000 rune，超限给下一步 offset。
  句柄 miss（过期/重启/非本人）统一「不存在或已过期，请重查」，不区分口径。

## §4 动态注入与白名单扩展

### §4.1 激活集
- 会话新增 `activated_tools JSONB`（migration 015）：激活的端点名数组，随会话
  持久化——TTL（30min）内跨消息有效，进程重启不丢；新会话从空集开始。
- 上限 14【2026-07-18 修订，原 15】：静态面已增至 15（13 业务工具 +
  search_endpoints + read_endpoint_result，§3.5），15 + 14 = 29 < 30（业内在场
  工具数安全线）；原「静态 10」算术早已失真。再加静态工具须先降本上限。满员 FIFO
  逐出最早激活者，检索结果文本明示被逐出的端点（重新检索即恢复）。

### §4.2 白名单语义（M4 契约 §10 的扩展）
可调用面 = 静态工具 ∪ **会话已激活端点**。两个硬规则：
- 注册表里存在但未激活的端点名 → 拒绝（"工具 X 不存在"自纠文案）。跳过检索直呼
  端点名是绕过检索留痕的旁门，必须堵死。
- 已激活但注册表已无（re-gen 下线）→ Defs 跳过注入、Resolve 同步拒绝，两处口径
  必须一致（防"声明发了却调不了"或反之）。

### §4.3 顺序纪律（缓存前缀）
每轮请求 tools = 静态声明（进程内恒定，恒在前）+ 激活声明（激活顺序，append-only）。
会话内已注入的前缀恒稳定，DeepSeek 前缀缓存按最长公共前缀命中，新增只作废尾部。
禁止重排、禁止中途移除（FIFO 逐出是唯一例外，发生频度低）。

### §4.4 状态传递
Tool 接口签名由 M4 契约固定且工具实例是全局单例，per-message 状态（激活集、
消息内计数、记账记录）一律经 ctx 旁路传递（先例：loop.go chatMetaKey）。
工具在 ctx 无状态时必须安全退化（检索照常、不激活、不 panic）。

## §5 调用器（tikhubinvoke）

- 按 Entry 元数据装配请求：path 参数替换、query 参数（数组重复键展开、整数不带
  小数点）、POST 恒发 JSON body（空参发 `{}`——FastAPI 对声明了 body 的端点收空
  body 回 422）。未提供的可选参数**不发送**（让上游用自己的默认值，不做默认值快照）。
- **大整数保真**：社媒雪花 ID（TikTok/抖音 uid ~6.8e18 > 2^53）。agent 侧
  validateEndpointArgs 用 `json.Number`（UseNumber）解析、invoker toString 原样透传
  其十进制串——普通 float64 会静默丢低位精度，向上游查错对象。
- 错误分层：key 未配置/装配失败 → CodeValidation；超时 → CodeFetchTimeout；
  其余传输失败 → CodeInternal。**非 2xx 不是 error**：状态码与原文随 Result 返回，
  由端点工具带状态码回给模型（4xx 原文是模型自纠的关键输入）。
- 护栏：单次 20s 超时；响应体读取上限 2 MiB（内存护栏）；回模型另有 6000 rune
  截断（token 护栏），两层各管各的。固定可信主机 api.tikhub.io，无 SSRF 面。

## §6 全量调用记账（tool_calls，Boss 硬需求）

- **范围**：agent 全部工具调用（静态 9 个 + search + 端点），单点拦截在 loop 的
  execRecorded（读工具直执与确认后执行两条路径共用）——不改 9 个工具，新工具自动
  被覆盖。
- 表结构对齐 OTel GenAI execute_tool 语义：tool_name / tool_kind（static /
  tikhub_search / tikhub_endpoint）/ error_type（低基数硬枚举）/ duration_ms /
  trace_id（与 llm_calls 同源，可 JOIN 回放整条消息链路）。
- **元数据全量、内容截断**：arguments 全存（非法 JSON 降级为字符串保存——排查
  恰恰需要看残缺原文）；result_preview 截 8K rune + result_size 存真实体量。
  上游响应可重取，不是本库资产，不入全文。
- **入库前净化**（sanitizeForDB）：上游响应可能含非法 UTF-8（GBK 错误页/二进制残片）
  或 NUL，两者都会让 result_preview 的 TEXT 列与 messages 的 JSONB 列**整行插入失败**
  （Postgres 22021/22P05），Boss「每次调用必须有记录」被数据内容静默击穿、限额随之
  漏计。净化在 execRecorded 这唯一汇聚点做（result 同时流向会话消息与 result_preview，
  一处覆盖两 sink）：剔 NUL + 非法 UTF-8 换 U+FFFD；arguments 侧 normalizeArgsJSON 同办。
- **检索留痕**（优化检索的唯一数据源）：retrieval_query + candidate_tools，
  零命中也记。端点调用另记 endpoint_path + http_status。
- 记账纪律与 llm.Recorder 完全一致：同步写、失败只记日志，绝不放大成业务失败。

## §7 成本护栏（免确认的代价，Boss 拍板：双重限额）

- 单条消息上限 `agent.endpoint_msg_cap`（默认 10）：消息内计数，超限回文案。
- 滚动 24h 上限 `agent.endpoint_daily_cap`（默认 200）：从 tool_calls COUNT——
  限额与账本同源。口径：打到上游的都算（含 HTTP 错误/超时，失败同样计费）；
  排除 invalid_args / budget_exceeded（没发请求，且不排除会把限额越顶越死）。
- 计数时机：参数校验通过之后、发请求之前（校验失败不打上游不吃限额）。
- **fail-closed**：每日限额判定不可用（DB 故障）时拒绝调用而非放行——护栏失效
  即放开计费面，故障期间宁可少查。

## §8 装配与降级

`fetch.tikhub_api_key` 未配置 → EndpointTools 不装配（nil）：无 search_endpoints
工具、无 system prompt 说明段、动态解析关闭，agent 工具面与本特性上线前逐字节一致。
nil 的 ToolCallRecorder 全程安全（测试免装配）。

## §9 测试锁定的不变量

- catalog：数量下限 900（re-gen 误伤守卫）/ 命名合法唯一 / 排除 tag 不在表 /
  golden 检索 8 例 / 平台过滤硬约束 / 结果确定性。
- agent：激活 FIFO+去重+上限 / encode-decode 保序 / 损坏自愈 / 未激活拒绝 /
  下线端点 Defs-Resolve 口径一致 / 消息限额与每日限额（含 fail-closed）/
  校验失败不打上游不吃限额 / 检索→注入→调用→持久化→记账全链路 / 跨消息激活 /
  静态声明恒在动态之前 / system prompt 两态。
- invoker：GET/POST/path 装配 / 数组重复键 / 整数无小数点 / 可选参数不发送 /
  空参 POST 发 {} / 非 2xx 透传 / key 缺失与超时错误码。
- store：迁移幂等 + wantTables 对账 / tool_calls 全字段往返 / 每日限额口径
  （static、invalid_args、budget_exceeded 不计，HTTP 错误计）。

## §10 显式非目标（本期不做，改动它们须先改本契约）

1. 端点查询结果不进 content_items / 打分 / 推送管道（§0.3 的分界线）。
   【2026-07-18 收窄】限 lookup 通道；绑定通道见 endpoint-binding-contract.md §0。
2. 不做订阅信源准入迁移：xhs 三缺口信源（热榜/话题流/收藏流）等仍走 sourcecatalog
   实测路线。【2026-07-18 作废】准入迁移即 endpoint-binding-contract.md（三缺口为首批）。
3. embedding 检索、检索词改写、端点描述 LLM 增强 → 二期，凭 §6 留痕数据立项。
4. per-endpoint 价格表与 cost_usd 落列：TikHub 计价元数据未接入，本期以调用次数
   为成本代理（限额单位=次）；接入后在 tool_calls 补列。
5. agent 之外的调用面（API/A2A 暴露端点查询）不在本期。
   【2026-07-18 收窄】调度绑定引擎成为非 agent 调用面，记账口径见
   endpoint-binding-contract.md §5（kind=binding_fetch，不占本契约 §7 双限额）。
