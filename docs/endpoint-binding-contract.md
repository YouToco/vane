# 端点绑定契约（注册表端点 → 订阅信源）

> 2026-07-18 定稿。Boss 拍板「统一前门」：发现/绑定环节用 agent + 注册表，定时运行环节
> 跑固化绑定（不每轮重新找工具）。本契约是 tikhub-endpoint-registry-contract.md §0.3
> 所述「某端点若证明适合做订阅信源，走 sourcecatalog 实测准入**另行实现**」的那个另行实现，
> 并修订该契约 §10.1/§10.2/§10.5 与 M6 契约 §2/§5.3/§18-I2 的对应条款（见 §10 修订清单）。
> 同日 Boss 拍板：被通用引擎替代的 bespoke fetcher **删除干净**（§6）。

## §0 分界怎么变（对 §0.3 的修订）

不变的部分：lookup 层（agent 会话内动态端点调用）的查询结果**仍然永不**直接写入
content_items——那条通道没有准入。变的部分：注册表端点可以经过**绑定 + 准入**成为
订阅信源，此后它的数据经绑定引擎（§3）归一化后进 content_items——走的是信源通道，
受信源准入与运行期护栏约束。判据不变：**静默失败通道是否存在**决定准入门槛；
绑定把端点接进了静默通道，所以绑定必须带准入（§2）与运行期反静默护栏（§4）。

## §1 绑定模型

**绑定 = 端点 + 参数 + 字段映射 + 身份槽 + Kind + 时间格式（+ 可选 enrich/unwrap）**：

```go
type Binding struct {
    Endpoint  string            // tikhubcatalog Entry.Name（必须 Lookup 命中，§3.1）
    Params    map[string]string // 静态参数模板；值可引用 config 键（"$user_id"）
    ItemsPath string            // 响应里条目数组的点路径（如 "data.data.items"）
    Fields    FieldMap          // 条目字段 → ContentItem 字段的点路径映射
    Kind      types.Kind
    Enrich    *EnrichSpec       // 可选：付费详情补全（xhs/search 迁移用）
    Unwrap    string            // 可选：转发解包路径（x 迁移用，如 "retweeted_status"）
}
type FieldMap struct {
    ID, Title, Content, Author, URL string // 点路径；URL 可为模板 "https://…/{id}"
    Time       string                      // 点路径；空 = 无时间戳（PublishedAt=nil）
    TimeFormat string                      // unix_s | unix_ms | ruby_date（严格按声明解析，类型漂移→计数）
}
```

完整特性清单（迁移三旧 fetcher 逐项对照后定，2026-07-18）——每个特性都是**引擎级
一次性实现、模板声明式开关**，模板里不出现任何函数/逻辑（sub-Turing 在代码模板同样遵守，
这是行为集中可审的架构约束，不只是 agent 红线）：
字段回退链（title→display_title、author→$screen_name）、条目过滤+下钻
（model_type=="note" → 进 note 子对象）、URL 模板+条件查询组（xsec_token 空则整组略去）、
enrich 触发条件（MinRunes+必备字段）+计费闸门+实例级限速+预算+串号校验+空值保旧、
unwrap 尝试链（retweeted_tweet→retweeted，取到含 id 的对象才替换）、param 对照校验
（user.userid vs $user_id，空则宽容）、信封断言表（code==200；xhs 族加 data.success）、
参数规格（config 引用/默认值/常量/omit-empty）。表达力仍不够的端点就写 bespoke fetcher
（本清单之外不再扩>——扩前先改本契约）。

1. **映射语言 sub-Turing（M6 §12.4 红线）**：只允许点路径（`a.b.c`，无索引、无谓词）、
   URL/Content 常量模板（`{id}`/`{title}` 等字段占位替换）、时间格式**枚举**。
   禁止 regex / XPath / JS-eval / 条件逻辑。表达力不够的端点就不绑，写 bespoke fetcher。
2. **解析顺序**：本期绑定**只存在于代码内模板注册表** `fetcher.bindingTemplates`
   （key = platform+capability，value = Binding）。`sources.config` 只存用户参数
   （page_id / user_id 等），模板通过 `$键名` 引用。**config.binding 命名空间保留给二期**
   （agent 自主绑定任意端点），本期不实现、不读取（§9.1）——避免未测攻击面上线。
3. **I2 的修订（M6 §18）**：模板绑定把 vendor 专属信息（TikHub 端点名）留在代码里，
   config 顶层继续无 vendor 字段——比 provider_hints 更干净，I2 对模板绑定源**自动成立**。
   二期 config.binding 落地时按「诚实泄漏」条款单独修订。
4. **IdemKey（I-S2/Rule B）**：绑定能力的 key 沿用 `vane://<platform>/<capability>?<用户参数>`
   手拼定序。模板内部固定值（如 topic_feed 恒 sort=time）是能力语义的一部分，随代码
   版本化，不进 key——与 RSS 解析器内部行为不进 key 同理。新增用户参数必须同步进
   key 并在 sourcespec/invariant_test.go 加 case（该测试是清单不是网）。

## §2 准入（试跑 = 准入，及其诚实边界）

1. **能力级准入**（进 sourcecatalog）：新绑定能力仍须**真实测**后才 Available——
   本契约首批三能力的实测证据见 §7。实测口径升级为可复跑：每个绑定能力的
   fixture 单测（真实响应样本）+ 时序单调检查（若声明 Time）。
2. **源实例级准入（试跑）**：绑定能力的 `add_source` 在确认后的 Execute 里先**真调一次
   上游**（probe），全过才 `UpsertSource`+`AddSubscription`：
   - 非 2xx / 超时 → 拒，AppError.Message 给人话（红线 3：原始错误链不进模型/用户面）；
   - ItemsPath 解析不到或 0 条 → 拒（提示参数可能有误 / 收藏可能未公开）；
   - 身份槽为空的条目 >0 → 计数；**全部**为空 → 拒（否则 finalize 静默全丢，
     probe-green production-dead）；
   - 声明了 Time 的，时间解析失败 → 拒；**时序非降序检查只对声明了 OrderCheck 的模板**
     （topic_feed：sort=time 语义承诺降序，x/search 乱序教训的可检面）——faved_notes
     豁免（收藏序≠创建序，2026-07-18 实测非单调，检了必误拒）、hot_list 豁免（无条目
     时间戳）、xhs/search 豁免（sort_type 用户可选，非恒时序）；
   - 回执写明首批统计（N 条、最新一条时间、标题样例）——用户看得见试跑结果。
   probe 调用计账（§5），probe 失败不落任何行。
3. **诚实条款**：单次试跑弱于人肉多轮实测（x/search 单次 probe 全绿、多轮才暴露乱序）。
   补偿三道，缺一不可：(a) sourcecatalog **Unavailable veto**——同 (platform,capability)
   标了 Unavailable 的，绑定与试跑一律拒绝，Reason 原样给出（M6 §2.2 的「不静默改道」）；
   (b) §2.2 的时序单调检查；(c) 运行期反静默护栏（§4）。
4. probe 实现落点：add_source Execute（确认后）内、独立 10s 超时。飞书卡片回调
   已有「2.5s 同步预算超时 → 30s 执行预算 + 结果异步补发」机制（handler 既有），
   probe 不会阻塞回调，失败不静默。
5. **已知豁免（诚实记录）**：任务手册编译路径（fetch_plan，P1）产出的绑定源
   不经 probe——该路径 best-effort、坏源由运行期护栏（§4 + fail_count 链）兜底；
   若实践中成为坏源主要入口，再把 probe 前移进编译层。

## §3 运行（确定性引擎）

1. **端点解析**：引擎每轮 `tikhubcatalog.Lookup(binding.Endpoint)`——命中才调用。
   miss（re-gen 后端点消失/改名）→ `CodeValidation`「绑定失效：端点已从注册表移除」，
   fail_count++，走既有告警链（==3 告警卡，>=10 自动停用）。**绝不静默跳过**。
   附带效果：gen 期排除的写操作/风险端点天然不可绑（引擎只认 Lookup 命中的 Entry）。
2. **参数校验**：每轮按**当前** Entry.Params 校验绑定参数（必填齐全、无未知参数名）。
   校验不过 → `CodeValidation`。这是防 tikhubinvoke.buildRequest「静默丢弃未声明参数 →
   上游用默认值返回 200 但数据错误」的唯一防线（漂移场景：re-gen 改了参数名）。
3. **调用**：复用 `tikhubinvoke.Invoker`（无状态并发安全）。非 2xx → `CodeFetchFailed`
   类映射（fail_count 链路）。大整数经 json.Number（UseNumber）全程不过 float64。
4. **归一化**：映射产物必须填 ExternalID（身份槽）；CanonicalKey 按**平台既有规则**
   构造（I1 不变：xhs=裸 note_id/条目 id，x=裸 tweet_id）；Kind 来自 Binding.Kind；
   最后过 `finalize`（既有守卫复用）。平台内撞击分析：热榜 item_id 为十进制数字串，
   与 24-hex note_id 形状不相交（2026-07-18 实测结论，经验性而非结构性——新绑定能力
   的身份形状对照写进 §7 表格，是准入检查项）。
5. **提取失败语义（M6 §10.5：静默空是最坏失败）**：ItemsPath 解析失败 → `CodeValidation`
   （fail_count++），不是空成功；条目级身份缺失 → drop + slog 计数；**提取到条目但全部
   drop → `CodeValidation`**。注意区分：提取为 0 是失败，**去重后**入库 0 是正常（追新无新条目）。
6. **enrich 阶段**（xhs/search 迁移）：声明式 `{Endpoint, KeyParam, Fields, RateMs, BudgetMs}`；
   计费闸门语义照旧（SeenChecker 已见即跳过）、实例级限速与预算照旧；enrich 响应的
   条目 id 与请求 id 对照（串号防御引擎级通用化）。
7. **Temporal 零改动**：引擎只是 `fetcher.Multi` 分派表的一个分支，活在既有 Fetch
   Activity 内；不加新 Activity（workflow 函数体确定性红线）。

## §4 漂移与反静默护栏

| 漂移场景 | 护栏 | 表现 |
|---|---|---|
| re-gen 后端点消失/改名 | §3.1 Lookup 每轮校验 + **模板引用完整性测试**（CI：所有模板 Endpoint 必须 Lookup 命中，re-gen 破坏绑定直接红） | CodeValidation，告警链 |
| re-gen 改参数名/必填 | §3.2 每轮参数校验 | CodeValidation，告警链 |
| 上游响应结构变化 | §3.5 提取失败=失败；身份/时间字段解析失败计数 | CodeValidation，告警链 |
| 上游 200 但语义腐坏（乱序等） | 声明 Time 的绑定每轮做时序降序断言（失败→CodeValidation） | 告警链 |
| 慢性零产出（提取正常、恒无新内容） | 既有 next_fetch_at/fail_count 不覆盖此形态；**本期不加棘轮**，依赖管理员视图观察（探针 ⑪ 风格楔子留二期） | 记录为已知缺口 |

## §5 记账（Boss 硬需求：每次调用有记录）

- 引擎与 probe 的**每次**上游调用（list / enrich / probe）写 `tool_calls`：
  新增 `ToolCallKind = "binding_fetch"`；`endpoint_path`/`http_status`/`error_type`/
  `duration_ms` 照 §6（tikhub 契约）口径；`trace_id` = 调度运行的 workflow traceID
  （probe = 会话 trace）；`user_id`/`session_id`/`tenant_id` 均 NULL——源是跨租户共享
  客观事实（I-T1），其抓取是系统行为。
- **额度**：binding_fetch 不占 agent 免确认双限额（那是会话面的护栏）；调度面本期不设
  独立限额——频率天然受 1h floor × ≤20 active sources 约束；管理员视图凭 kind 聚合
  观察成本，超预期再立项限额（数据已在）。
- 无需 DB migration：tool_calls 的 tool_kind 是自由文本列，nullable 列齐备。

## §6 多余 fetcher 删除（Boss 拍板：删除干净）

**删除**：`fetcher/tikhub.go`、`fetcher/xhs_user.go`、`fetcher/x.go` 及其配套测试文件
（tikhub_test.go / xhs_user_test.go / xhs_user_live_test.go / x_test.go），三个能力
（xhs/search、xhs/user_posts、x/user_posts）改由绑定引擎模板承载。
**保留**：exa.go / exa_contents.go / fetcher.go(RSS)——非 TikHub，不在「多余」范围。

迁移等价义务（每项都有测试承载，缺一不得合并）：
1. **IdemKey 金串不动**：`vane://xhs/user_posts?user_id=…`、`vane://xhs/search?keyword=…`、
   `vane://x/user_posts?screen_name=…` 既有金串测试原样保持绿——sourcespec 不因迁移改
   一个字节，生产 sources 行零迁移、零重复。
2. **canonical_key 字节等价**：引擎对同一响应样本提取的 ExternalID/CanonicalKey 与旧
   map 函数一致（fixture 对照测试；xhs=note_id、x=tweet_id 裸值，TRIM 口径一致）。
   等价则旧内容不重推（M6 §4.1 两步证明的第 1 步；第 2 步生产前置 SQL 于部署时执行）。
3. **config 键名不变**：user_id / keyword / sort_type / note_type / screen_name 原样。
4. **行为参数对齐**：xhs_user 100-rune desc 截断及其理由（原文件头注迁入模板注释）、
   xhs/search enrich（计费闸门/1.1s 限速/40s 预算/串号防御）、x retweet unwrap、
   noRedirect 凭证防外带、desc 字节上限。
5. **共享符号安置**：SeenChecker、parseUnixSeconds、fixture 等被兄弟文件引用的符号
   迁至存续文件；kind_test.go 的 Kind 一致性锁改为走引擎模板逐能力断言（锁不许删）。

## §7 首批能力与实测记录（2026-07-18，全部 code=200 真调）

| 能力 (platform/capability) | 端点 (Entry.Name) | 用户参数 | 身份槽 | 时间 | 实测发现 |
|---|---|---|---|---|---|
| xhs/hot_list | xiaohongshu_web_v3_fetch_hot_list | 无（全局一份，IdemKey=`vane://xhs/hot_list`） | item_id（十进制串，20/20 在位） | **无条目时间戳**（data.updated_at 实测坏值：滞后 date 字段 41 天，禁用） | 正文=标题+热度+趋势合成，薄正文打分风险见 §7.1 |
| xhs/topic_feed | xiaohongshu_app_v2_get_topic_feed | page_id | 条目 id（24-hex note_id） | create_time unix_ms，sort=time 实测**严格降序**（模板固定 sort=time） | 条目即完整笔记（title+desc 真身） |
| xhs/faved_notes | xiaohongshu_app_v2_get_user_faved_notes | user_id | 条目 id（24-hex note_id） | create_time unix_s；**序列非单调**（收藏序≠创建序，OrderCheck 关） | 收藏未公开→0 条（probe §2.2 会拒并提示）；有 cursor，本期只拉首页 |

- **时间表示三端点三样**（topic 毫秒 / faved 秒 / hot 无）——FieldMap.TimeFormat 枚举的直接依据。
- §7.1 **hot_list 打分风险**：条目无正文，M5 scorer 对薄正文给 0-20 且提示词字节锁死。
  本期以「标题+热度+趋势」合成 Content 上线并观察；若长期分数压底不推送，走 M6 §8.2
  Kind 分派提示词路径二期解决（**不改** M5 §5 字节锁）。
- §7.2 **topic_feed page_id 获取 UX 缺口**：上游无按名搜话题端点；page_id 可从笔记
  hash_tag 深链取得（agent 会话内可用 search_notes+get_image_note_detail 挖到）。
  add_source 工具描述注明；专用 helper 二期。
- 分页拍板：三能力都**只拉首页**——增量追新首页足够，避免首次添加灌历史（RSS lookback
  同款教训）；cursor 翻页二期。

## §8 测试不变量（新增/改造）

1. 模板引用完整性：所有 bindingTemplates.Endpoint 必须 tikhubcatalog.Lookup 命中（CI 卡 re-gen）。
2. 三真实 fixture（脱敏样本）驱动引擎单测：提取数、身份、时间解析、时序断言。
3. §6.1 金串 + §6.2 字节等价对照 + §6.5 Kind 锁改造。
4. 漂移模拟：参数改名 → CodeValidation；ItemsPath 失配 → CodeValidation（非空成功）。
5. 大整数（>2^53）经映射全程无损（UseNumber 链路）。
6. multi_test「Available 必接线」不变量对绑定能力成立；引擎测试 baseURL 走死服务器覆盖。
7. probe 单测：0 条拒 / 身份全空拒 / 非降序拒 / 全过落库。

## §9 显式非目标（动它先改本契约）

1. **config.binding + agent 自主绑定工具**（bind_endpoint_source）：语义已留位（§1.2），
   工具面、注入防御（模型从 probe 响应提映射的上游可影响面）、startup reconcile 均二期。
2. 新 Platform（douyin/tiktok/微博…）绑定：每个新平台先做 M6 §7.3 撞击分析 Gate。
3. cursor 翻页 / 历史回灌；慢性零产出棘轮（§4 表末行）；per-endpoint 价格表。
4. 调度面独立调用限额（§5 留观察）。

## §10 对既有契约的修订清单（本契约生效即修订）

- tikhub-endpoint-registry-contract.md §0.3：分界表述按本契约 §0 收窄（lookup 通道不变，
  绑定通道新增）；§10.1 收窄（「端点查询结果不进 content_items」限 lookup 通道）；
  §10.2 作废（准入迁移即本契约）；§10.5 收窄（新增调度绑定调用面，记账口径 §5）。
- m6-source-plugin-contract.md：§2 准入新增「绑定能力实测口径」（本契约 §2.1）；
  §5.3 config schema 增补三能力行（hot_list `{}` / topic_feed `{page_id}` /
  faved_notes `{user_id}`）；§18-I2 增补模板绑定条款（本契约 §1.3）。
- 两份契约在对应小节就地加注日期指针，正文详情以本契约为准（避免双份漂移）。
