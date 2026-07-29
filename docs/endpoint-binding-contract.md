# 端点绑定契约（注册表端点 → 订阅信源）

> **现行覆盖声明（2026-07-29）：** 本文的固定运行期绑定引擎仍有效；实例级
> `add_source` probe、来源确认卡和 `sourcecatalog` 准入已退役。当前能力准入归
> `capabilitycatalog`，目标实例只由批准后的任务手册经 `fetchspec` 材料化。下文
> 出现的 `sourcecatalog`、`sourcespec`、`sources` 与账户订阅均是切换前历史名，
> 不得作为新入口。见 `task-playbook-fetch-target-cutover.md`。
>
> 2026-07-18 定稿。Boss 拍板「统一前门」：发现/绑定环节用 agent + 注册表，定时运行环节
> 跑固化绑定（不每轮重新找工具）。本契约是 tikhub-endpoint-registry-contract.md §0.3
> 所述「某端点若证明适合做订阅信源，走 sourcecatalog 实测准入**另行实现**」的那个另行实现，
> 并修订该契约 §10.1/§10.2/§10.5 与 M6 契约 §2/§5.3/§18-I2 的对应条款（见 §10 修订清单）。
> 同日 Boss 拍板：被通用引擎替代的 bespoke fetcher **删除干净**（§6）。

> **2026-07-29 X 供应商政策（现行）**：X 平台能力保留，但 X 专属 provider / 能力
> 的生产数据访问只允许通过 TikHub（`https://api.tikhub.io`）。禁止接入官方 X/Twitter API、syndication、第三方
> X API，也禁止在 TikHub 失败时降级到这些供应商。TikHub key 缺失、鉴权失败、限流、
> 超时或响应漂移均须显式失败。`x.com/<author>/status/<id>` 只用于内容证据链接，不是
> 抓取 API；通用 Web 搜索偶然命中公开 x.com 页面也不构成 X 信源 provider。CI 通过
> 固定绑定端点、运行策略、credential、禁止生产覆盖 TikHub base URL 和故障不回退测试
> 证明主链；域名/凭证 denylist 仅是辅助 tripwire，不冒充网络出口白名单。

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
enrich 触发条件（MinRunes+必备字段）+计费闸门+实例级限速+预算+串号校验+空值保旧
（enrich 端点同样每轮 Lookup+参数校验，漂移时降级跳过不静默丢参）、
unwrap 尝试链（retweeted_tweet→retweeted，取到含 id 的对象才替换）、param 对照校验
（user.userid vs $user_id，空则宽容）、信封断言表（code==200；xhs 族加 data.success）、
参数规格（config 引用/默认值/常量/omit-empty）、正文截断上限 MaxContentBytes
（xhs 族 4000 字节成本护栏；**x 恒 0 不截断**——旧实现存推文全文，迁移等价）。
超时与响应上限对齐旧抓取器口径（cfg 可配，兜底 20s/5MB，超限显式报错非静默截断）。
【2026-07-23 增补，随 weibo/wechat_mp 三能力落地】两项引擎级扩展：
①**每模板超时覆盖** `TimeoutSeconds`（wechat_mp 上游明示 30s，否则「已扣费但收不到
响应」）——http.Client 级超时只设全模板最大值当保险丝，每次调用的真实预算由 ctx 按
「模板声明值或配置默认」控制，否则声明 30s 的模板会被 client 级 20s 抢先掐断。
已知取舍：ctx 超时只能收紧不能放宽外层 deadline——add_source 试跑面受飞书卡片执行
预算约束（probeBudget=25s），wechat_mp probe 实际上限 25s，超时可能已扣费（probe
话术如实告知费用面）；周期抓取（Fetch activity 120s）30s 完整生效；
②**URL 模板任意字段占位**——`{id}`/`{author}` 之外允许 `{a.b.c}` 点路径占位从条目
数据取值（weibo 桌面权威链接需要 `{user.idstr}/{mblogid}` 双段），仍是纯字段替换、
sub-Turing 红线不动。
表达力仍不够的端点就写 bespoke fetcher（本清单之外不再扩——扩前先改本契约）。

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
   上游**（probe），全过才 `UpsertSource`+`AddSubscription`。拒绝话术分两类出口（红线 3
   的实现，ProbeRejection 标记）：**准入拒绝**（0 条/时序/缺参数）话术为人话、原样透出；
   **其余失败**（漂移/网络/鉴权——内嵌端点名与上游 body）按错误码映射固定话术，
   原文只进 slog 与 tool_calls：
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
   对 `x/user_posts`，该 Invoker 是唯一允许的 provider；失败后不得尝试 Exa、RSS、
   官方 X/Twitter API 或其他 X 数据接口。
4. **归一化**：映射产物必须填 ExternalID（身份槽）；CanonicalKey 按**平台既有规则**
   构造（I1 不变：xhs=裸 note_id/条目 id，x=裸 tweet_id）；Kind 来自 Binding.Kind；
   最后过 `finalize`（既有守卫复用）。平台内撞击分析：热榜 item_id 为十进制数字串，
   与 24-hex note_id 形状不相交（2026-07-18 实测结论，经验性而非结构性——新绑定能力
   的身份形状对照写进 §7 表格，是准入检查项）。
5. **提取失败语义（M6 §10.5：静默空是最坏失败）**：ItemsPath **键缺失** → `CodeValidation`
   （fail_count++），不是空成功；键在但值为 JSON null、或数组为空 → 合法静默轮
   （X 静默账号实测形态，旧 x.go 同语义——null 与缺键严格区分）；条目级身份缺失 →
   drop + slog 计数（probe 报告携带该计数）；**提取到条目但候选全灭 → `CodeValidation`**，
   其中「全部被 ItemFilter 挡掉」豁免（多态流合法），ItemRoot 下钻失败与身份缺失**不豁免**
   （都是漂移证据）。已知盲区：faved 收藏转私密后表现为持续合法空轮，管理员视图观察（§9.3）。
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
| 上游 200 但语义腐坏（乱序等） | **声明 OrderCheck 的模板**（topic_feed）probe + fetch 每轮做时序非增断言（失败→CodeValidation）；Time 有但序无承诺的（faved）不检 | 告警链 |
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

### §7.4 增补能力（2026-07-23，全部 code=200 真调）

| 能力 (platform/capability) | 端点 (Entry.Name) | 用户参数 | 身份槽 | 时间 | 实测发现 |
|---|---|---|---|---|---|
| weibo/user_posts | weibo_web_v2_fetch_user_posts | uid（纯数字；主页链接 `weibo.com/u/<uid>` 可抽取） | mblogid（base62 短串，如 `Ra1N24Tm5`） | created_at ruby_date（+0800，与 Twitter 同格式） | 转发条目 `retweeted_status` 结构与 x 同形 → unwrap 拆包（身份/作者/URL 随原帖；**刻意不设 VerifyParam**，拆包后归属是原作者）；`isLongText` 长文 text_raw 被上游截断（实测 9/20），详情补全留二期（enrich 触发语义「过长=截断」与「过短需补全」相反）；模板固定 feature=0（10 条基础档，上游文档「性能最佳」）|
| weibo/hot_list | weibo_web_v2_fetch_hot_search | 无（全局一份，IdemKey=`vane://weibo/hot_list`） | word（热搜词本身；榜单条目无 id 字段，实测 id 仅广告条目有） | **无条目时间戳** | `data.realtime` 混入 `is_ad=1` 商业推广位（实测 1/51）→ ItemFilter 按「无 is_ad 键才保留」过滤；正文=词+榜位+热度合成（xhs/hot_list 薄正文同款，§7.1 风险同样适用） |
| wechat_mp/user_posts | wechat_mp_v2_fetch_account_articles | username（`gh_` 原始 ID；名称解析不了，拒绝话术指路获取方式） | `{app_msg_id}_{idx}` 复合模板（文章 URL 含每次抓取会变的 chksm 签名参数，**不能当身份**） | create_time unix_s | POST JSON body 端点（invoker 既有能力）；raw=false 精简结构；digest 实测恒空（10/10）→ 正文退回标题，详情补全留二期；上游明示 timeout 30s → 模板 TimeoutSeconds=30（§1 扩展①，probe 面受卡片预算压到 25s、超时可能已扣费，见 §1 已知取舍）；**唯一在描述里标价的端点**（$0.010/次），weibo 两端点未标价按兜底最高档记账，待上游放出价目表校正 |

- **跨平台撞击分析（M6 §7.3 Gate，2026-07-23 做）**：weibo mblogid 是 base62 短串
  （含大小写字母，与 24-hex note_id / 纯十进制 tweet_id / 恒含 `://` 的 url 均不同形）；
  weibo 热搜词是中文短语；wechat_mp 复合键恒含下划线且两段皆数字。与既有三平台
  **互不同形**，经验结论口径与 §3.4 一致（新平台再加时须重做）。
- probe/时序：三能力 OrderCheck 均关——weibo/user_posts 有置顶微博（置顶=旧帖排首位，
  检了必误拒，x/user_posts 同款取舍）；hot_list 无时间戳；wechat_mp 列表实测降序但
  上游无排序承诺，保守不检。

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
2. 新 Platform（douyin/tiktok…）绑定：每个新平台先做 M6 §7.3 撞击分析 Gate
   （微博/微信公众号已于 2026-07-23 过 Gate 落地，见 §7.4）。
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
