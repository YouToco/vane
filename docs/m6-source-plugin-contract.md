# M6 契约：信源插件化（平台 × 能力 × 内容种类）

> **历史设计记录，不是当前能力清单。** 本文保留 `web/page_watch`、
> `page_snapshots`、`KindChange`、`SnapshotStore` 等内容用于解释当时的设计与下线决策；
> 这些能力现已退役，不能据本文中的旧 schema、Gate 或排期重新实现。页面内容监控的现行能力是
> `web/contents`：由 Exa `POST /contents` 抓取，统一走 data-feed 管道。当前代码真相源见
> `sourcecatalog/`、`types/`、`sourcespec/` 与 `fetcher/exa_contents.go`。
>
> 事实基准：生产库 + 真实 API key 实测 2026-07-16。所有「实测」标注的数字都可复现。
> 设计动机见 Boss 决策：**用户关注的是「来源平台」和「这个平台能给我什么功能」，
> 而不是 tikhub 还是 exa 这种实现细节**。
>
> 精度标准对齐 [m5-profile-contract.md](m5-profile-contract.md)（签名级）。
> §17 是 Gate 验证清单。

---

## 0. 为什么要改

### 0.1 根因：现有枚举**在三个不同的轴上命名**

| 现有枚举 | 平台 | 能力 | 供应商 |
|---|---|---|---|
| `rss` | 开放网页 | feed 订阅 | 直连 |
| `exa` | 开放网页 | 语义搜索 | **Exa** |
| `tikhub_xhs` | **小红书** | 关键词搜索 | **TikHub** |

`rss` 命名了能力，`exa` 命名了供应商，`tikhub_xhs` 命名了供应商+平台。
**每个值都在不同的轴上命名**——这就是它无法扩展的结构性原因，也是
「X 平台能不能搜关键词」这类问题**在现有模型里无处安放**的原因。

### 0.2 实证：现状正在把传闻站以 85 分推给 Boss

生产 source id=9（`{"query":"Anthropic Claude new model release announcement","category":"news","num_results":15}`）
实测抓回 15 条，**只有 1 条来自 anthropic.com**，还是 `https://www.anthropic.com/news` 这个
**索引页本身**（title="Newsroom"）。其余全是内容农场与传闻站。**且真的推送了**：

| delivery | score | status | title | host |
|---|---|---|---|---|
| 57 | **85.0** | **sent** | Claude Opus 5 Next Week? Inside the Honeycomb Leak | vallettasoftware.com |
| 56 | **85.0** | **sent** | Anthropic Set to Release Its Most Powerful Model, Mythos | xix.ai |

打分器 prompt 里**没有信源权威性概念**（只有画像 + 内容），分不出官方公告与传闻。

### 0.3 但根因**不是** Exa，是 Vane 自己的 bug（实测消融）

Exa 官方 OpenAPI spec 对 `publishedDate` 的**原文定义**：

> "An estimate of the creation date, **from parsing HTML content**."

即**从页面 HTML 猜的**。实测：`openai.com` 系列 8 个官方页 publishedDate **全部为 None**；
无日期过滤搜 Anthropic 时 25 条里 11 条为空，**而这 11 条恰恰是
`anthropic.com/claude/opus`、`platform.claude.com/docs/.../pricing` 这类最权威的页**。

消融实验（复刻生产 config + `exa.go` 自动附加的参数）：

| 实验 | `startPublishedDate` | `category:news` | **官方站占比** |
|---|:---:|:---:|:---:|
| ① **完全复刻生产** | ✓ | ✓ | **0/15** |
| ② 只去掉 startPublishedDate | ✗ | ✓ | **13/15** |
| ③ 只去掉 category | ✓ | ✗ | 3/15 |
| ④ 两个都去掉 | ✗ | ✗ | 13/15 |
| ⑥ ④ + `includeDomains:["anthropic.com"]` | ✗ | ✗ | **15/15** |

**唯一起作用的变量是 `startPublishedDate`；`category:news` 是无辜的**（②④ 同为 13/15）。
代码定位：`fetcher/exa.go:143-151`，`exaDefaultLookbackDays = 7`，`lookback == 0` 时取默认 7；
生产 config 没写 `lookback_days` → 0 → 落入默认 7 → 毒药生效。

> **⚠️ 这条与 M6 架构工作可解耦，且应当先做。见 §19.0 热修。**

### 0.4 这条实证直接否掉了一个"自然"的抽象

`fetcher/fetcher.go:196-199`（`rssSourceConfig.LookbackDays` 的字段注释）写着：

> 「语义与 exaSourceConfig.LookbackDays 一致——**同一个 config 键在不同源类型下含义相同**，
>   配源的人不必记住例外」

**这条注释表达的"统一"是错的**：
- `lookback_days` 对 **RSS 是对的**——feed 的 `pubDate` 是**结构化必填字段**（PR #13 已落地，正确）
- 对 **Exa 是毒药**——`publishedDate` 是**从 HTML 猜的**，官方页普遍猜不出

> **⚠️ 但它不是事故的根源，时间上也不可能是**（对抗审查的事实核查者纠正，已 `git log -S` 复验）：
>
> | commit | 日期 | 内容 |
> |---|---|---|
> | `83fc3a8` | **2026-07-14** | 引入 `exaDefaultLookbackDays = 7` 与 `lookback==0 → 7`（**毒药**） |
> | `7321efd` | **2026-07-16** | 加入上述注释（PR #13） |
>
> 注释比毒药**晚两天** ⇒ 它是**症状**（事后把一个错误的统一写成了规范），不是**病因**。
> 病因是 `exa.go` 的默认值本身。本节初稿曾把该注释同时挂在 `exa.go:71` 名下并称其
> "正是本次事故的根源"——**两处都是错的**（`exa.go:71` 只是 `LookbackDays` 的字段注释，无此文），
> 已改正。**记录此事本身**：签名级契约的误引会被照抄进代码注释，变成后人读到的谎。

→ **真正的设计含义**：平台×能力模型天然会想把 `lookback_days` 提升为公共 source 级配置项——
   **那才是把这个 bug 固化进架构**（注释已经是这个念头的第一次显形）。本契约 §5.3 显式拒绝这种统一。

### 0.5 三个业务目标与现有模型的差距

| Boss 要什么 | 现有模型 |
|---|---|
| X 官号发布（@OpenAI / @AnthropicAI / @GoogleDeepMind） | **做不到**（TikHub 支持 Twitter，Vane 没实现） |
| 官网 blog/news | OpenAI/Google 有 RSS ✓；**Anthropic 无官方 RSS**（16 个 URL 实测 404 + autodiscovery 标签枚举 `rel="alternate"` 数量为 0） |
| price 页 / 文档页的**变化** | **三种 type 都做不了** |

---

## 1. 抽象模型：三轴

```
Source = Platform × Capability × Params
              │
              ├─→ registry 查表（不进用户配置）─→ Provider（供应商实现）
              └─→ registry 查表 ─────────────────→ Kind（内容种类，物化到 content_items）
```

| 轴 | 定义 | 决定什么 |
|---|---|---|
| **Platform** 平台 | 一个有**自己的身份空间与访问协议**的内容生态 | `canonical_key` 规则 |
| **Capability** 能力 | 从这个平台**拿什么** | 用哪个 Provider + 产出什么 Kind |
| **Kind** 内容种类 | 产出的**是哪一类东西** | **下游 pipeline 怎么对待它** |
| ~~Provider 供应商~~ | 谁去拿 | **实现细节，不进用户配置、不进身份** |

**为什么 Platform 是「身份空间 + 访问协议」而不是「网站」**：`anthropic.com` 不是平台，
它是 `web` 平台上的一个站点。为它写专用解析器 = 站点专用适配器 = **正是本次要消灭的东西**
（详见 §18 被否决分支 D3）。

### 1.1 第三轴（Kind）不是分类学洁癖——它是被实测逼出来的

**这是本次设计最重要的发现，来自对抗审查的对照组（其任务本是论证"不必重构"）。**

已逐行复验 `workflow/activities.go:305-311`：

```go
sh := dedup.Simhash(item.Title + " " + item.Content)
candidates := append(append([]int64{}, hist...), batchSeen...)
if dedup.IsNearDup(sh, candidates, simhashThreshold) {   // simhashThreshold = 3
    continue
}
```

`types.ContentItem`（`entities.go:56-73`）**没有 Type/Kind 字段**，`ListUnpushedByUser`
也不返回源类型 → **Dedup 步无从豁免任何内容，无条件对全部内容跑近似去重。**

而 `dedup/dedup.go` 的包注释把设计意图写死了：

> `simhash：64-bit 局部敏感哈希，近似去重（**改动少量文字仍判为重复**）`

**页面监控的信号恰恰是「改动少量文字」**（`$30.00` → `$24.00`）→ **变化被静默丢弃**，
表现不是报错而是 pipeline「去重后无内容」早退——**与 §0.2、AGENTS.md 红线 1 同构的静默失败**。

#### 实测（用真实 `dedup` 包跑的探针，不是推理）

机制（已逐行复验）：
- `dedup/dedup.go:97-105` 的 `tokenize`：`default: flush()` —— **非字母/数字/CJK 一律当分隔符**
  ⇒ unified diff 的 `+` / `-` / `@@` / `|` / `$` / `.` **贡献零 token**
- `dedup/dedup.go:38-50` 的 `Simhash`：`for _, tok := range tokens { v[i]++ / v[i]-- }`
  —— **可交换累加，与顺序无关，纯 bag-of-tokens**

```
tokenize("- a | $30.00") = ["a" "30" "00"]
tokenize("+ a | $30.00") = ["a" "30" "00"]      ← 完全相同：+ 与 - 根本不进 token
```

| 场景 | 实测汉明距离 | 阈值 `simhashThreshold=3` |
|---|---|---|
| ① 降价 diff **vs 它的反向**（价格又涨回去） | **0** | **必被丢弃** |
| ② 连续两次改同一行价（`$30→$24` vs `$24→$22`） | **1** | **必被丢弃** |
| ③ 两条无关的变化（不同模型、不同列） | **25** | 正常区分 |

**① 是必然而非概率**：一段 diff 与其反向 diff 的 token 多重集**完全相同** ⇒ 指纹完全相同。
在 simhash 眼里「新增一行」与「删除一行」**没有区别**。

> ③ 的 25 证明 **simhash 本身没坏**——它只是**对 change 这类内容在结构上不适用**。
> 这也是为什么修法是**豁免**（§8.1）而不是调阈值：调阈值会同时废掉它对 article 的正当用途。

> **注意：把 `content` 改成 diff 文本也躲不掉**（②：汉明距离 1）。
> **两层防线缺一不可**：转移键（§10.4）挡**存储层**的键碰撞；Kind 豁免（§8.1）挡**投递层**的近似去重。
> 只做前者，「价格又降回去了」会从另一条通道原样消失（① 的 hamming=0），且无报错无日志。

**第二处**（已复验 `scorer/scorer.go:74`）：

> 「【待评估内容】的正文信息过少（为空、仅有话题标签、或短到看不出实质内容）时给低分（0-20）」

一段 diff 天然就短 → 命中"正文信息过少" → 0-20 分 → selector 淘汰 → **页面监控推不出来**。

→ **整条 pipeline（Dedup / scorer prompt / cardgen）都隐含假设内容是"一篇文章"。**
→ Kind 必须是一等概念，且**必须物化在 `content_items` 上**（理由见 §3.3）。

**对照组对重构派的诘问，本契约采纳**：

> 「registry 也不修这条。缺的抽象不是 (平台×能力)，是 content kind——**谁的方案里没有这一轴，谁一样炸**。」

补一句：三轴模型让 Kind **有地方推导**（capability → kind 是 registry 的一列），
而最小方案里 `web_watch` 只存在于 `sources.type`，**内容侧完全看不见它**。
**两者都要做，但三轴模型让"忘了做"这件事更难发生。**

### 1.2 能力的两种 kind：source vs lookup —— 这不是分类学，它决定同一个端点能不能用

Boss 列的能力：「关键词搜索、**用户搜索**、用户新文章详情」。它们**不是同一类东西**：

| Boss 的说法 | 落在哪个面 | 为什么 |
|---|---|---|
| 关键词搜索 | **信源能力**（sources 表） | 周期性产出内容流 |
| 用户新文章 | **信源能力** | 周期性产出内容流 |
| **用户搜索** | **lookup**（agent 工具） | 一次性查询——**用来把「我想追 OpenAI 的推特」变成一个合法的 user_posts 源**，它自己不产出流 |
| 单帖详情 | **provider 内部行为** | 已存在（小红书详情补全），是 provider 的实现细节 |

**实测证据说明这个区分有牙齿**：TikHub 的 `fetch_search_timeline` 在 X 上
**作为信源不可用**（`search_type=Latest` 实测返回 2023–2026 乱序、与 `Top` 有 18/20 重合，
**无法追新**），但**作为 lookup 完全合理**（"X 上关于 XX 都在聊什么"用相关性排序反而对）。

→ **同一个端点，作为 source 是坏的，作为 lookup 是好的。** 把两者混为一谈会导致
  要么丢掉一个可用能力，要么 ship 一个坏掉的源。

---

## 2. 能力注册表

### 2.1 信源能力（落 sources 表，被 pipeline 周期抓）

| Platform | Capability | Kind | Provider | 状态 | 依据 |
|---|---|---|---|---|---|
| `web` | `feed` | `article` | `direct`（gofeed） | 迁移自 `rss` | — |
| `web` | `search` | `article` | `exa` | 迁移自 `exa`，**必须同时修 §0.3 毒药 + 加 `include_domains`** | §0.3 |
| ~~`web`~~ | ~~`page_watch`~~ | ~~`change`~~ | ~~`direct`~~ | **已下线**（改用 Exa fetch，见 §10 顶部） | §10 |
| `x` | `user_posts` | `article` | `tikhub` | **新增** | §9 |
| `x` | `search` | — | — | **`Unavailable`** | 见下 |
| `xhs` | `search` | `article` | `tikhub` | 迁移自 `tikhub_xhs` | — |
| `xhs` | `user_posts` | `article` | — | **未实现**（registry 缺席） | — |

> 【2026-07-18 增补】表格快照滞后于代码（xhs/user_posts 早已 Available）。新增绑定
> 能力 xhs/hot_list、xhs/topic_feed、xhs/faved_notes 经 2026-07-18 真实测准入
> （证据与 config schema：endpoint-binding-contract.md §7/§10）；绑定能力的实测口径
> 升级为可复跑（fixture 单测 + 时序检查），准入与运行护栏以该契约为准。
> tikhub Provider 的三能力（xhs/search、xhs/user_posts、x/user_posts）自同日起由
> 绑定引擎模板承载，bespoke fetcher 删除（等价义务：该契约 §6）。

### 2.2 `Unavailable` 是一等条目，不是缺席 —— registry 唯一严格优于最小方案的地方

```go
// Entry 是能力注册表的一行。Unavailable 条目**必须留在表里**而不是删掉：
// 缺席不带理由，而理由正是防止后人重新踩坑的唯一载体。
type Entry struct {
    Platform   Platform
    Capability Capability
    Kind       Kind
    Provider   Provider  // Unavailable 时为 nil
    Status     Status    // Available | Unavailable | Unimplemented
    Reason     string    // Status != Available 时必填，**会进 agent 工具 description**
}
```

`(x, search)` 的条目：

```go
{Platform: PlatformX, Capability: CapSearch, Status: StatusUnavailable,
 Reason: "上游 search_type=Latest 排序不可靠（2026-07-16 实测返回 2023–2026 乱序、" +
         "与 Top 有 18/20 重合），无法用于追新。追 X 官号请用 user_posts。"}
```

**为什么这值得一个类型**（对照组的论证，本契约采纳）：

> 最小方案里它**只能靠枚举缺席表达，缺席不带理由**。三个月后有人看到 TikHub 有
> `fetch_search_timeline` 且只要 $0.001，会理直气壮加上去，ship 一个 `code=200`、
> `params` 老实回显、报文毫无异常的**教科书级静默失败**源。

现状代码的应对模式是**在 const 上写注释**（`tikhub.go:42` 记 `get_video_note_detail` 串号、
`tikhub.go:43` 记 `general` 排序坏）——**能用，但只对读代码的人生效，对 agent 不生效**。

带 `Status + Reason` 的条目是**机器可读**的：agent 能主动回答"X 关键词搜索暂不支持（原因…）"，
而不是**静默改用 Exa 去搜**——而 Exa 搜出来的正是 §0.2 那些被打 85 分推出去的传闻站。

### 2.3 lookup 能力（agent 工具，不建源）

| Platform | Lookup | Provider | 用途 | 分期 |
|---|---|---|---|---|
| `x` | `resolve_user` | tikhub `fetch_user_profile` | 把"OpenAI"确认成 `@OpenAI` | P3 |
| `x` | `search_once` | tikhub `fetch_search_timeline`（`Top` 排序） | "X 上都在聊什么"，**不建源** | P3 |
| `xhs` | `search_users` | tikhub | 同上 | P3 |

---

## 3. types 变更

### 3.1 `types/enums.go`

```go
// Platform 内容平台（sources.platform）。平台的定义是「有自己的身份空间与访问协议的
// 内容生态」——anthropic.com 不是平台，它是 web 平台上的一个站点。
type Platform string

const (
    PlatformWeb Platform = "web" // 开放网页：身份=url，协议=HTTP
    PlatformX   Platform = "x"   // X / Twitter：身份=tweet_id
    PlatformXHS Platform = "xhs" // 小红书：身份=note_id
)

// Capability 从平台拿什么（sources.capability）。
type Capability string

const (
    CapFeed      Capability = "feed"       // 订阅一个 RSS/Atom feed
    CapSearch    Capability = "search"     // 关键词/语义搜索
    CapUserPosts Capability = "user_posts" // 某账号的新发布
    CapPageWatch Capability = "page_watch" // 页面变化监控
)

// Kind 内容种类（content_items.kind）。决定**下游 pipeline 怎么对待它**，
// 而不只是给人看的标签——见契约 §1.1：Dedup 的 simhash 近似去重对 change 是
// 灾难性的（simhash 的设计目的「改动少量文字仍判为重复」与 change 的信号
// 「改动少量文字」直接对立）。
//
// Kind 由 capability 推导（registry 的一列），但**物化在 content_items 上**：
// 007 起内容全局唯一、source_id 只是首发源，内容的种类是**内容自己的属性**，
// 不该由"碰巧先发现它的那个源"的类型决定；且 Dedup 拿到的是 []types.ContentItem，
// 没有 JOIN 的余地。
type Kind string

const (
    KindArticle Kind = "article" // 一篇内容（默认；存量 231 条全是这个）
    KindChange  Kind = "change"  // 一次变化事件（page_watch 产出）
)
```

**`SourceType` 退役**：`types/enums.go` 的 `SourceType` 及三个常量删除。
向仓库外前端的兼容由 api 层派生（§13.2），**不在 DB 保留**。

### 3.2 `types/entities.go`：`Source`

```go
type Source struct {
    ID         int64      `json:"id"`
    Platform   Platform   `json:"platform"`   // 008 新增，取代 Type
    Capability Capability `json:"capability"` // 008 新增，取代 Type
    // URL 自 008 起语义收窄为**幂等键**（UpsertSource 的 ON CONFLICT 目标）。
    // 只有两种形态，且两者命名空间**必须不相交**（见 §5.2）：
    //   - web/feed          → 真实 http(s) 地址（它同时是天然键、fetcher 要 GET 的地址、
    //                         前端要渲染成 <a> 的链接——三个理由都指向"别动它"）
    //   - 其余全部 capability → vane://<platform>/<capability>?<params>
    URL string `json:"url"`
    // ... 其余字段不变（Title/Config/Status/FetchIntervalSeconds/NextFetchAt/...）
}
```

### 3.3 `types/entities.go`：`ContentItem`

```go
type ContentItem struct {
    ID       int64 `json:"id"`
    SourceID int64 `json:"source_id"` // 首发源（007 起语义）
    // Kind 是内容种类，决定下游 pipeline 怎么对待它（契约 §1.1）。
    // **必须由 fetcher 填**（它知道 capability）；空值在 finalize 处被拒（见 §7.2）。
    // 落在内容上而非源上：007 起内容全局唯一，同一条内容可被多个源命中，
    // 它是不是"一次变化"是它自己的属性。
    Kind Kind `json:"kind"`
    // ... 其余字段不变
}

// PageSnapshot 页面快照（page_snapshots 表，只被 web/page_watch 使用，见 §10.3/§10.4）。
type PageSnapshot struct {
    ID            int64           `json:"id"`
    SourceID      int64           `json:"source_id"`
    CanonicalKey  string          `json:"canonical_key"`  // = watchKey(...)，由 Go 构造后写入
    ContentHash   string          `json:"content_hash"`   // = sha256(extracted_text)，Baseline 比对用
    ExtractedText string          `json:"extracted_text"` // 抽取后的结构化文本（§10.2），**绝非裸 HTML**
    Verdict       SnapshotVerdict `json:"verdict"`        // 见 §10.4：settled 与否决定基准是否前进
    FirstSeenAt   time.Time       `json:"first_seen_at"`
}
```

### 3.3.1 【强制语义】Kind 必须活着走完 **DB 往返**

> **这一条是对抗审查抓到的头号致命缺陷，三个怀疑者独立命中。初稿只加了 Go 字段与 DB 列，
> 没改 store 层任何一条 SQL——照初稿逐字实现，§8.1 的豁免恒不触发，page_watch 的变化
> 依然被 simhash 静默吞掉，第三轴（本契约自称的两大真实收益之一）完全不工作。**

已复验的事实链：

1. `workflow/activities.go:241` —— **`Fetch` 返回的是 `ListUnpushedByUser(...)` 的结果，
   不是本次抓到的 items**（注释自陈"修 #3 重试丢内容"）。
2. `store/content_items.go:35-42` —— `UpsertContentItem` 的 INSERT 是**显式列清单**，无 `kind`。
3. `store/content_items.go:242-247` —— `ListUnpushedByUser` 的 SELECT（**内外两层**）是显式列清单，
   无 `kind`，`rows.Scan` 也不扫它。

⇒ fetcher 填 `Kind=change` → INSERT 不写 → DB 落 `DEFAULT 'article'` → SELECT 不读
→ Dedup 拿到 `item.Kind == ""` → 豁免不触发 → **静默吞掉**。

**强制语义（写进 `types.Kind` 的 doc 注释）**：

```go
// **任何返回 []types.ContentItem 或 *types.ContentItem 的 store 方法，
//   其 SELECT 列清单与 Scan 都必须带 kind。** 漏一处的后果不是编译错误、不是运行时错误，
//   而是 Dedup/scorer 读回零值 "" → 按 article 处理 → 页面变化被 simhash 静默丢弃，
//   且 §17 里查 content_items 的探针**照样是绿的**（那些行在 Dedup 之前就已写入）。
//   这是本契约在对抗审查中真实犯过的错，留此为戒。
```

**§5.4 逐条钉死受影响的 SQL。**

---

## 4. migration 008

> **迁移号以落地时实际空号为准**：并行分支 `feat/gate-probes-observability` 可能先占 008，
> 届时本迁移顺延为 009。

### 4.1 存量处置证明

**命题**：映射 `rss|exa → web`、`tikhub_xhs → xhs` 后，
同一条内容重抓时算出的 `canonical_key` 与库里已存的**逐字节相等** ⇒ **零重写、零重复**。

> **证明分两步，缺一不可**（对抗审查纠正了初稿：初稿只给了第 2 步的 SQL 并称其为"证明"，
> 而那段 SQL **逐字复算的正是 007 回填用的 CASE**——它测的是**库的自洽性**，
> 对任何 `CanonicalKey` 改动都恒返回 231/231。**它不是安全网**，
> 尤其不能拿它给 §18 D5（url 归一化）背书：那天它照样全绿而全库 BBC 重新长一份。）

**第 1 步：代码级恒等（这才是主证明）**

新旧 `CanonicalKey` 对同一条内容是**同一个表达式**：

| 旧（按 `src.Type`） | 新（按 `src.Platform`） | 表达式 |
|---|---|---|
| `case rss, exa:` | `case Platform == web:` | `strings.TrimSpace(item.URL)` |
| `case tikhub_xhs:` | `case Platform == xhs:` | `xhsKey(item.ExternalID)` |

映射 `{rss, exa} → web`、`{tikhub_xhs} → xhs` 是**全序、单射到分派分支**，
故对**任意** item，`CanonicalKey_new ≡ CanonicalKey_old` —— **按构造成立，与库里有什么无关**。

**第 2 步：前置条件检查（确认无存量行偏离规则）**

第 1 步只保证"新旧算法一致"。还需确认：**库里不存在 canonical_key 偏离该规则的行**
——若有（例如某行的键既不是 url 也不是 note_id），重抓它会算出**新键** → 重复。
这正是下面这段 SQL 的作用：**它是前置条件检查，不是主证明**。

生产库实测：

```sql
SELECT CASE WHEN s.type IN ('rss','exa') THEN 'web' ELSE 'xhs' END AS new_platform,
       count(*) AS rows,
       count(*) FILTER (WHERE ci.canonical_key = CASE
           WHEN s.type IN ('rss','exa') THEN trim(ci.url)         -- web → url
           WHEN s.type = 'tikhub_xhs'   THEN trim(ci.external_id) -- xhs → note_id
       END) AS key_matches_new_rule
FROM content_items ci JOIN sources s ON s.id = ci.source_id GROUP BY 1;
```

| new_platform | rows | key_matches_new_rule |
|---|---|---|
| web | 102 | **102** |
| xhs | 129 | **129** |

**231/231 全中。** 三步推理：

1. 008 **只改 `sources`，绝不触碰 `content_items.canonical_key`**——它是已物化的列，
   无任何代码路径重算存量行的键（`UpsertContentItem` 只对本次抓取到的 item 算键）。
2. 改制后 runtime 规则与改制前**一一对应**（`rss|exa→url` ≡ `web→url`；
   `tikhub_xhs→note_id` ≡ `xhs→note_id`）→ 同一条内容重抓时算出的键逐字节相同
   → `ON CONFLICT (canonical_key) DO NOTHING` → 不新增行。上表就是这一步的实测。
3. 007 的回填 CASE 已在 version 7 执行完毕；`store/migrate.go` 是 `goose.Provider.Up`，
   按 version 单调推进**不重跑**。

→ **content_items 变更 0 行（除新增 kind 列的 DEFAULT）、content_sources 0 行、deliveries 0 行。**

**幂等键前缀替换的可行性**（同样实测）：

| type | rows | `exa://search?` 开头 | `tikhub://xhs/search?` 开头 | 真实 http(s) url |
|---|---|---|---|---|
| exa | 2 | **2** | 0 | 0 |
| rss | 3 | 0 | 0 | **3** |
| tikhub_xhs | 4 | 0 | **4** | 0 |

**无一例外** ⇒ `substring()` 前缀替换安全。

### 4.2 SQL

```sql
-- +goose Up

-- ① 三轴的前两轴落列。DEFAULT '' 只为让 ALTER 在非空表上成立，回填后立刻收紧为非空语义
-- （不加 CHECK：001 起的约定是"应用层校验"，且 sources.type 从来没有 CHECK——
--  加了反而会让未来新增 platform 必须改 schema，与插件化目标相悖）。
ALTER TABLE sources ADD COLUMN platform   TEXT NOT NULL DEFAULT '';
ALTER TABLE sources ADD COLUMN capability TEXT NOT NULL DEFAULT '';

-- ② 回填。**不按 status 过滤**——9 行里 6 行是 disabled，它们同样要迁
--    （disabled 只是不抓，不是不存在；将来重新启用时必须已是新制）。
UPDATE sources SET platform='web', capability='feed'   WHERE type='rss';        -- id 1,7,8
UPDATE sources SET platform='web', capability='search' WHERE type='exa';        -- id 2,9
UPDATE sources SET platform='xhs', capability='search' WHERE type='tikhub_xhs'; -- id 3,4,5,6

-- ③ 幂等键去供应商化：**纯前缀替换，不重新转义**。
--    绝不能改用 url.Values.Encode() 重新生成——它按字母序排键（category= 会排到 q= 前面），
--    生成不同的字符串 → 与 Build() 算出的键不一致 → 重复源 → 双倍付费。
--    前端 syntheticParam 是 url.split("?")[1] + URLSearchParams（**与 scheme 无关**，
--    已复验），故换前缀对 vane-web 的展示零影响。
UPDATE sources SET url = 'vane://web/search?' || substring(url from length('exa://search?') + 1)
 WHERE type = 'exa';
UPDATE sources SET url = 'vane://xhs/search?' || substring(url from length('tikhub://xhs/search?') + 1)
 WHERE type = 'tikhub_xhs';
-- web/feed 的 url 不动：它同时是天然幂等键、FetchRSS 要 GET 的地址、前端要渲染的链接。

-- ③' 【后置守卫】把最贵的静默失败变成响亮失败。
--    没有它时的失败模式：某行的真实串与假设形态不符 → substring 产出垃圾（**不报错**）
--    或前缀残留 → 新 Build() 算出 vane://… → UpsertSource 的 ON CONFLICT (url) **不冲突**
--    → 直接 INSERT 新行 → **重复源 + 每轮双倍付费，全程零错误**。
--    ②的 platform/capability 回填有 NOT NULL 兜着，风险更高的 url 交换反而裸奔——补上。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sources WHERE url LIKE 'exa://%' OR url LIKE 'tikhub://%') THEN
        RAISE EXCEPTION '008: 幂等键去供应商化未覆盖全部行，仍有 exa:// 或 tikhub:// 残留';
    END IF;
    IF EXISTS (SELECT 1 FROM sources WHERE platform = '' OR capability = '') THEN
        RAISE EXCEPTION '008: 存在未映射的 sources 行（platform/capability 为空）';
    END IF;
END $$;

-- ④ type 列退役。前端兼容由 api 层派生（§13.2），不在 DB 保留双份真相。
ALTER TABLE sources DROP COLUMN type;

-- ⑤ 内容种类（契约 §1.1）。存量 231 条全部落 'article'——而它们**确实全都是文章**，
--    DEFAULT 即正确语义，无需回填判断。
--    ⚠️ 加列只是第一步：kind 必须活着走完 DB 往返（§3.3.1 / §5.4.2），否则本列恒为 'article'。
ALTER TABLE content_items ADD COLUMN kind TEXT NOT NULL DEFAULT 'article';

-- ⑥ 页面快照（只被 web/page_watch 使用，见 §10.3）。
--    canonical_key 列在此存一份**由 Go 构造后写入的键**，而不是让 SQL 重新拼——
--    键的构造若分散两处，任何一边多做一步归一化都会让 §10.4 的基准判据全 miss，
--    表现不是报错而是"这个页面从此不再报变化"（identity.go 的教训）。
CREATE TABLE page_snapshots (
    id             BIGSERIAL   PRIMARY KEY,
    source_id      BIGINT      NOT NULL,
    canonical_key  TEXT        NOT NULL,  -- = watchKey(url, prevHash, hash)，Go 构造
    content_hash   TEXT        NOT NULL,  -- = sha256(extracted_text)，Baseline 比对用
    -- extracted_text 是**抽取后的结构化文本**（§10.2 的 ` | ` 压平行），**绝非裸 HTML**：
    -- 存原始 HTML 一年 550MB/页 × 4 页 = 2.2GB 且红线 5 禁止清理；压平行是原文的 0.2%。
    -- §17.1 探针⑩ 可执行地守着这一条。
    extracted_text TEXT        NOT NULL,
    -- verdict 把「门的判定」与「崩溃」拆成两个可区分的状态（§10.4）。
    -- 没有 'reported'：门判重要时的 settled 由 content_items 的存在性证明。
    verdict        TEXT        NOT NULL DEFAULT 'pending'
                   CHECK (verdict IN ('pending','baseline','suppressed')),
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_page_snapshots_source FOREIGN KEY (source_id) REFERENCES sources (id) ON DELETE CASCADE,
    CONSTRAINT uq_page_snapshots UNIQUE (source_id, canonical_key)
);
-- Baseline 的查询走 (source_id, id DESC)；verdict 谓词在其上过滤。
CREATE INDEX idx_page_snapshots_source ON page_snapshots (source_id, id DESC);

-- +goose Down
-- 按 007 约定：Down 只需让旧代码能跑，不必还原数据。
ALTER TABLE sources ADD COLUMN type TEXT NOT NULL DEFAULT 'rss';
UPDATE sources SET type = CASE
    WHEN platform='web' AND capability='feed'   THEN 'rss'
    WHEN platform='web' AND capability='search' THEN 'exa'
    WHEN platform='xhs' AND capability='search' THEN 'tikhub_xhs'
    ELSE 'rss'  -- x/user_posts、web/page_watch 无对应旧值；落 'rss' 会让旧 FetchRSS
                -- 拿 vane:// 当 URL 去抓 → CodeValidation → 单源失败不拖垮整批
                -- （activities.go 已保证）。**刻意不删行：数据是资产（红线 5）。**
END;
UPDATE sources SET url = 'exa://search?' || substring(url from length('vane://web/search?') + 1)
 WHERE platform='web' AND capability='search';
UPDATE sources SET url = 'tikhub://xhs/search?' || substring(url from length('vane://xhs/search?') + 1)
 WHERE platform='xhs' AND capability='search';
DROP TABLE IF EXISTS page_snapshots;
ALTER TABLE content_items DROP COLUMN IF EXISTS kind;
ALTER TABLE sources DROP COLUMN IF EXISTS capability;
ALTER TABLE sources DROP COLUMN IF EXISTS platform;
```

### 4.3 部署窗口

- **后端内部无错位窗口**（实测）：`cmd/server/main.go` 先 `store.Migrate` 成功再起服务；
  CI 是 `systemctl restart vane`（Type=simple，stop→start），单二进制无蓝绿 → 可一步到位改列。
- **跨仓库有错位窗口**：见 §13.2。

---

## 5. sourcespec 重构与幂等键

### 5.1 `sourcespec.Spec`

```go
// Spec 是信源构造入参。Platform + Capability 决定必填 Params。
// Params 用 map[string]string 而非扁平字段：扁平字段（现状的 URL/Query/Keyword/Category）
// 每加一个能力就要加一个字段，且字段之间的"哪个能力用哪几个"关系只存在于注释里。
type Spec struct {
    Platform   string
    Capability string
    Params     map[string]string
    Title      string
}

// Build 校验并构造待 upsert 的信源；校验失败返回给用户的中文文案（空串=成功）。
// 未注册的 (platform, capability) 组合、或 Status != Available 的组合，
// 返回带 Reason 的文案（§2.2）——这是"X 不支持关键词搜索"能被 agent 说出口的地方。
func Build(spec Spec) (*types.Source, string)

// BuildLegacy 接受 M6 前的扁平入参（type + url/query/keyword/category），
// 映射到 (platform, capability, params) 后转 Build。
// **存在的唯一理由是仓库外前端 vane-web 的兼容窗口**（§13.2），
// 删除里程碑：vane-web 切到新字段后的下一个后端 minor。
func BuildLegacy(typ, url, query, keyword, title, category string) (*types.Source, string)
```

### 5.2 幂等键：命名空间不相交是**约束**，不是运气

已复验 `store/sources.go:161-166`：

```sql
INSERT INTO sources (...) VALUES (...)
ON CONFLICT (url) DO UPDATE
SET type = EXCLUDED.type, config = EXCLUDED.config, ...
```

→ **`DO UPDATE SET config = EXCLUDED.config` 意味着：任何两个 capability 若产出同一个幂等键，
后添加的那个会静默改写前一个的配置。** 现状三类型的键空间（`https://` / `exa://` / `tikhub://`）
不相交**纯属运气**——`page_watch` 若图省事用裸 `https://openai.com/...` 当键，
而该 url 已被某 `web/feed` 占用 → **第二次添加静默劫持他人的源**。

**规则 A（scheme 不相交，必须有测试守）**：

```go
// IdemKey 构造 sources.url（幂等键）。
//
// 只有两个 scheme，且**必须不相交**：
//   - web/feed        → 真实 http(s) 地址（原样，不合成）
//   - 其余全部        → "vane://" + platform + "/" + capability + "?" + 参数
//
// 不相交的证明：vane:// 与 http(s):// 前缀互斥；params 一律 url.QueryEscape，
// 故被包进 params 的 url（如 page_watch 的 url）不会在顶层产生 "://"。
//
// **参数拼接必须逐字节稳定**：手工按**固定顺序**拼接，绝不用 url.Values.Encode()
// ——它按字母序排键，会让 exa 的 category= 排到 q= 前面，产出与 008 回填不同的字符串
// → 重复源 → 双倍付费。改这里必须同步改 008 的回填，反之亦然（identity.go 的教训）。
func IdemKey(platform types.Platform, cap types.Capability, params map[string]string) string
```

**规则 B（键必须蕴含全部改变抓取语义的参数）——初稿漏了这条，对抗审查抓到**：

初稿只把风险面框在「两个 capability 之间」，但**真正的碰撞发生在同一个 capability 的
两个不同 config 之间**。现有 `sourcespec.go:77-79` 早就写下了这条规则：

> 「category 参与幂等键：**它改变抓取语义**（news 与不限类别是两个信源），
>   不入键会让同 query 不同 category 撞同一行、**config 被静默覆盖**。」

**每个 capability 的键参数白名单（顺序即拼接顺序，不可重排）**：

| capability | 入键参数（按此序） | 不入键 | 理由 |
|---|---|---|---|
| `web/feed` | （键 = 原始 url）+ `categories`（fragment 判别位，2026-07-18 起） | `lookback_days` | 见下方【死结已解开】 |
| `web/search` | `q`, `category`, **`include_domains`**（排序后逗号 join） | `num_results` | 前三个改变**结果集**；`num_results` 只改条数不改语义 |
| `web/page_watch` | `url`, **`selector`** | `min_rows`, `min_prices` | selector 改变**抽取什么**；闸门阈值只改灵敏度 |
| `x/user_posts` | `screen_name` | `include_retweets`, `include_replies`, `lookback_days` | 后三者是**同一条流上的过滤器**，不是不同的流 |
| `xhs/search` | `keyword`, `sort_type`, `note_type` | — | 与现状一致 |

> **`include_domains` 必须入键**：不入键 ⇒ Boss 先订
> `{q:"Claude model release", include_domains:["anthropic.com"]}`（§0.3 的解药），
> 三周后 agent 再订同 query 不带域名 ⇒ 撞同一行 ⇒ `DO UPDATE SET config`
> ⇒ **解药被静默抹掉，传闻站当天回归**。

#### 【结构性死结】`web/feed` + `categories`——记录在案，不假装解决

`web/feed` 的幂等键**就是那个 feed 的原始 url**（§3.2：三个理由都指向"别动它"）。
⇒ 同一个 feed url 在结构上**只能存在一份 `categories` 过滤器**。
Boss 先订 `openai.com/news/rss.xml` + `categories:["Product","Research"]`，
三周后说"也想看政策动态" → agent 调 `add_source` 带 `categories:["Global Affairs"]`
→ 同一个 url → `ON CONFLICT (url) DO UPDATE SET config` → **前一份过滤器被覆盖**。

**原取舍（刻意，非疏忽）**：
- 把 `categories` 塞进键 ⇒ 键变成 `vane://web/feed?url=…&categories=…`
  ⇒ 前端 `Sources.tsx` 的 RSS 分支 `<a href={s.url}>` **变成死链**（已复验的真实回归）
  ⇒ 且 fetcher 要 GET 的地址得从 config 另取。**代价大于收益。**
- 故保留「一个 feed = 一个源 = 一份配置」，这在语义上也站得住：
  **想要同一个 feed 的两种过滤 = 想要两个源，而 Vane 的模型里过滤器长在源上。**
- **但"静默"必须消除**：`UpsertSource` 返回 `updated bool`（§5.4.1），
  `add_source` 命中既有源时必须回报「已**更新**既有源的配置（原 categories: X → 新: Y）」
  而不是「已添加」。§17.3 ⑪ 真人验这一条。

> #### 【2026-07-18 推翻】死结已解开——`categories` 改为**入键**（承载于 URL fragment）
>
> 上面那段取舍在**单 owner** 前提下成立，多租户下**结构性失效**，且它据以权衡的代价
> 已被一个当时没考虑到的第三选项消除。两点分别说明：
>
> **1. 缓解措施在多租户下够不着受害者。** "消除静默"是把配置变更回报给**写入者**。
> 单 owner 时写入者与受害者是同一个人（Boss 覆盖了自己的过滤器、被告知），成立；
> 多租户下是 B 覆盖 A 的过滤器、而被告知的是 **B**——A 的信源从此抓回别人要的东西，
> 且 A 永远不会收到任何提示。回报机制在结构上到不了受害者，因此不再构成缓解。
> （况且它本就只落实了一半：agent 只说"已更新既有信源"、从未回报「原 X → 新 Y」；
> API 订阅路径 `api/subscriptions.go` 直接丢弃 `updated`，那条路完全静默。）
>
> **2. 两条代价都只针对"合成 vane:// url"，fragment 方案不沾。**
> 键写成 `https://openai.com/news/rss.xml#vane-categories=ai,research`：
> - 前端 `<a href={s.url}>` 点开仍是真实 RSS 地址，**不是死链**（fragment 只是锚点）；
> - fetcher **零改动**：Go 的 http 客户端不把 fragment 发到线上
>   （`(*url.URL).RequestURI()` 不含 Fragment，已实测），`src.URL` 照抓不误。
>
> 于是「想要同一个 feed 的两种过滤 = 想要两个源」这句语义判断被**如实实现**了：
> 两种过滤真的得到两行 source，各自抓各自的。内容层面仍由 canonical_key 去重，
> 同一篇文章不会因此存两份；多出的成本只是同一个 RSS 多抓一次（RSS 抓取不计费）。
>
> **归一化口径**（与 `fetcher.applyCategories` 的匹配口径逐字对齐，两边不一致会
> 让行为相同的两组分类建出两行源、或让行为不同的两组共用一行）：
> `TrimSpace + 小写 + 去空 + 去重 + 升序`，逐项 `QueryEscape` 后逗号 join。
> 排序是幂等键正确性的关键——分类是无序集合，集合相同、顺序不同必须是同一个源。
>
> **无 `categories` 时 url 逐字节保持原始地址**，存量 feed 源的幂等键不漂移。
>
> 守卫：`sourcespec/invariant_test.go`（不变量 I-S2）。
> 遗留（不阻塞）：前端展示 `s.url` 时会连 `#vane-categories=…` 一起显示，属观感问题，
> 待 vane-web 侧在渲染时剥掉判别位。

### 5.3 config schema —— **`lookback_days` 显式拒绝跨能力统一**（§0.4）

| Platform/Capability | 字段 | 类型 | 默认 | 必填 | 入键 |
|---|---|---|---|---|---|
| **web/feed** | `url` | string | — | **是** | 键即它 |
| | `lookback_days` | int | 7（0=默认，<0=不限） | 否 | 否 |
| | `categories` | []string | nil（不限） | 否 | **✓**（fragment 判别位，见 §5.2【死结已解开】） |
| **web/search** | `query` | string | — | **是** | ✓ |
| | `include_domains` | []string | nil | 否 ← **§0.3 的解药** | **✓ 必须** |
| | `num_results` | int | 10（上限 100） | 否 | 否 |
| | `provider_hints` | object | `{}` | 否 ← **见下** | ✓（`category` 项） |
| | `lookback_days` | int | **0=关闭（默认）**；>0=逃生阀 | 否（**逃生阀·手册强烈不建议**，见下） | 否 |
| **web/page_watch** | `url` | string | — | **是** | ✓ |
| | `selector` | string | ""（全页 `<tr>` 压平） | 否 | **✓ 必须** |
| | `min_rows` / `min_prices` | int | 5 / 3（§10.5） | 否 | 否 |
| **x/user_posts** | `screen_name` | string | — | **是** | ✓ |
| | `include_retweets` | bool | **true**（拆包，§9.4） | 否 | 否 |
| | `include_replies` | bool | **true** | 否 | 否 |
| | `lookback_days` | int | 7 | 否 | 否 |
| **xhs/search** | `keyword` | string | — | **是** | ✓ |
| | `sort_type` | string | `time_descending` | 否 | ✓ |
| | `note_type` | string | ""（不限） | 否 | ✓ |

#### `provider_hints`：让不变量 I2 真的成立（对抗审查抓到初稿违反了自己的不变量）

初稿把 Exa 的 `category`（私有分类法 `research paper` / `personal site` / `financial report`）
与 `type`（`neural`/`keyword`/`fast`）**原样摆进用户 config**——那是**供应商本体**，
换 Exa→Brave/Tavily 时这些键全部失效 ⇒ 用户配置必须改 ⇒ **不变量 I2 被我自己的 schema 违反**。

```jsonc
// web/search 的 config：
{
  "query": "Claude model release announcement",   // 平台语义：任何搜索供应商都有
  "include_domains": ["anthropic.com", "claude.com"],  // 平台语义：Brave/Tavily 也有域名过滤
  "num_results": 15,                              // 平台语义
  "provider_hints": {                             // ← 供应商专属逃生舱
    "category": "news"                            //   Exa 私有分类法
  }
}
```

**`provider_hints` 的契约**：
- 里面的键**只对当前 provider 有意义**，换供应商时**明确失效并记 WARN**（而不是静默失效）
- 它**参与幂等键**（因为它改变抓取语义），但**排序后逐字节拼接**
- provider **必须自校验**：Exa 实测**不校验 `category`**（传 `__invalid__` 静默忽略照常返回）
  → 拼错一个字母过滤器就无声失效 → **本地白名单拒掉非法值**。
  合法值：`company / research paper / publication / news / personal site / financial report / people`
- **这是诚实的泄漏，不是消除的泄漏**：I2 的真实表述是
  「**config 的顶层不出现供应商专属字段；供应商专属的一律进 `provider_hints` 并接受它会随供应商失效**」

**`web/search` 的 `lookback_days` 默认关闭、仅作显式逃生阀、手册强烈不建议**（本契约的核心决定之一；追新以 `include_domains` 为准）：

```go
// web/search 的 lookback_days **默认关闭（0 或 <0 = 不发 startPublishedDate），
// 仅作显式逃生阀，手册强烈不建议**。追新以 include_domains 为准
// （§0.3 实测：无日期过滤 13/15 官方站，再加域名白名单 15/15）。
//
// 理由（2026-07-16 实测，见契约 §0.3）：Exa 的 publishedDate 是「从 HTML 内容解析出的
// 创建日期**估计值**」（官方 OpenAPI spec 原文），而官方页普遍解析不出——
// openai.com 系列 8 个官方页全部为 None；搜 Anthropic 时 25 条里 11 条为空，
// 而这 11 条恰恰是最权威的页。故 startPublishedDate 会**精准地删掉你最想要的东西**：
// 消融实验里加日期过滤后官方站占比 0/15，去掉后 13/15。**因此默认必须关闭**——
// 已落地：exa.go 的毒药 exaDefaultLookbackDays 已删，lookback_days<=0 时不发
// startPublishedDate。**但保留字段作显式逃生阀**：用户明确写 lookback_days>0 时仍生效
// （少数场景：追一个自身 publishedDate 可靠的窄域），这是下策，手册不主动建议。
//
// 这与 web/feed 的 lookback_days **默认语义相反**：feed 的 pubDate 是结构化必填字段，
// 默认过滤（7 天）是对的（PR #13）；Exa 的 publishedDate 是从 HTML 猜的，默认必须关。
// **同名概念在两个能力下的默认含义不同——刻意不统一默认值，但两边都保留字段。**
//
// fetcher.go:196-199 现有注释声称"同一个 config 键在不同源类型下含义相同"——那个"统一"是错的。
// 但注意它**不是**病因（git log -S 已复验）：该注释来自 7321efd(2026-07-16)，
// 而毒药 exaDefaultLookbackDays 来自 83fc3a8(2026-07-14)，**晚两天**。
// 它是症状（事后把错误的统一写成了规范），病因是 exa.go 的默认值本身。见契约 §0.4。
// 可选清理：把 fetcher.go 那句"语义与 exaSourceConfig.LookbackDays 一致"改为
// "本键对 web/feed 默认开启（7 天）、对 web/search 默认关闭仅作逃生阀，见 m6 契约 §5.3"。
//
// 存量 config 里若出现 lookback_days（source id=2/9 目前没有）：>0 按逃生阀生效，<=0 忽略。
```

**`web/feed` 新增 `categories`**（实测支撑）：`openai.com/news/rss.xml` 的 item 带 `<category>`
（实测分布 `Research 193 / Company 190 / Product 142 / Global Affairs 99 / Story 66 / ...`）
→ 这是把 1038 条 feed 收窄到"模型发布"的**最好维度**，且是 feed 的结构化字段（与 lookback 同性质，安全）。

---

### 5.4 store 变更（**初稿完全漏掉本节，是头号致命缺陷的落点**）

### 5.4.1 `store/sources.go`：`type` 列退役的连带

```go
// sourceColumns（现 line 17）去 s.type、加 s.platform, s.capability：
const sourceColumns = `s.id, s.platform, s.capability, s.url, s.title, s.config, s.status,
	s.fetch_interval_seconds, s.next_fetch_at, s.last_fetched_at, s.fail_count,
	s.created_at, s.updated_at`

// scanSource 同步：&src.Type → &src.Platform, &src.Capability

// UpsertSource 改签名，多返回一个 updated——**这不是可选的**（§5.3 的 categories 死结）：
//   命中既有源时 DO UPDATE 会**静默改写 config**（含 include_domains 这个 §0.3 的解药、
//   web/feed 的 categories 过滤器）。调用方（agent add_source / API）必须能据此回报
//   「已**更新**既有源的配置」而不是「已添加」，否则用户永远不知道自己覆盖了什么。
//   判据用 `xmax = 0`（INSERT）/ `xmax <> 0`（UPDATE）——Postgres 上 ON CONFLICT DO UPDATE
//   的系统列，一次往返即得，不必额外 SELECT。
func (s *Store) UpsertSource(ctx context.Context, src *types.Source) (id int64, updated bool, err error)
//   INSERT INTO sources (platform, capability, url, title, config) VALUES ($1,$2,$3,$4,$5)
//   ON CONFLICT (url) DO UPDATE
//   SET platform = EXCLUDED.platform, capability = EXCLUDED.capability,
//       title = COALESCE(NULLIF(EXCLUDED.title, ''), sources.title),
//       config = EXCLUDED.config, updated_at = now()
//   RETURNING id, (xmax <> 0) AS updated
```

### 5.4.2 `store/content_items.go`：**kind 必须进 INSERT、必须进每一个 SELECT**

```go
// ① UpsertContentItem 的 INSERT 列清单加 kind（现 line 35-42，共 10 列 → 11 列）：
//   INSERT INTO content_items (
//       source_id, external_id, canonical_key, url, title, content, author,
//       published_at, content_hash, simhash, kind
//   ) VALUES ($1,...,$11)   ← 实参补 item.Kind
//   ON CONFLICT (canonical_key) DO NOTHING RETURNING id
//
// ② 「更长的正文赢」的 UPDATE（现 line 71-85）**不动**——它只碰 content/content_hash/simhash。
//    kind 是内容的**种类**不是版本，跨版本恒定；让它参与覆盖只会制造"同一条内容种类会变"的怪状态。
//
// ③ ListUnpushedByUser 的**内外两层 SELECT** 与 rows.Scan 都要加 kind（现 line 242-247）：
//    外层 `SELECT id, source_id, ..., created_at` → 追加 `, kind`
//    内层 `SELECT ci.id, ci.source_id, ..., ci.created_at,` → 追加 `ci.kind,`（在 ROW_NUMBER 之前）
//    rows.Scan(...) → 追加 &ci.Kind
//    **漏内层或漏 Scan 都不会编译报错**（列数对不上才会 runtime 报错，而两层都改才对得上）——
//    这正是它容易被漏的原因，故 §16 有定向用例。
//
// ④ GetContentItem 同理（deep_dive 取原文也要知道种类）。
//
// ⑤ 凡将来新增的、返回 ContentItem 的方法，一律适用 §3.3.1 的强制语义。
```

### 5.4.3 `store/pagesnapshots.go`（新文件，§10.4）

```go
// Baseline 返回该源的 diff 基准 = **最近一个已 settled 的快照**；一个都没有时返回 nil。
// settled 的定义见 §10.4（verdict 已定 或 已落成 content_item）——**不是"最近一次快照"**。
func (s *Store) Baseline(ctx context.Context, sourceID int64) (*types.PageSnapshot, error)

// PutSnapshot 追加快照，ON CONFLICT (source_id, canonical_key) DO NOTHING（重试幂等）。
// verdict 传 SnapshotVerdictPending：它会在 SettleSnapshot 或内容项落库后才变 settled。
func (s *Store) PutSnapshot(ctx context.Context, snap *types.PageSnapshot) error

// SettleSnapshot 由 provider 在 LLM 门判「不重要」后调用，把 verdict 置为 suppressed。
// 门判「重要」时**不调用它**——那条路径的 settled 由 content_items 的存在性证明（§10.4）。
func (s *Store) SettleSnapshot(ctx context.Context, sourceID int64, canonicalKey string, v types.SnapshotVerdict) error
```

## 6. fetcher：registry + Provider

```go
// fetcher/registry.go

// Provider 是一个「平台×能力」的具体实现。供应商是它的内部细节，
// 既不出现在 types.Source 里，也不参与 canonical_key（§7）。
type Provider interface {
    // Fetch 抓取一个源。失败语义沿用现状：
    //   非法 config/缺 key → CodeValidation（不可重试）；超时 → CodeFetchTimeout；
    //   429 → CodeFetchRateLimit；非 2xx 按 5xx/4xx 定 Retryable。
    Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error)
}

type key struct {
    Platform   types.Platform
    Capability types.Capability
}

// Registry 是「平台×能力」→ 条目的**全系统唯一事实来源**。
// Unavailable/Unimplemented 条目**必须留在表里**（§2.2）：缺席不带理由。
type Registry map[key]Entry

// Multi 取代原 multi.go 的 switch 分派。
type Multi struct{ reg Registry }

// Fetch 按 (platform, capability) 查表分派。
//   - 未注册         → CodeValidation（数据问题，重试无益，同现状）
//   - Status != Available → CodeValidation，**Message 带 Entry.Reason**
//     （让"为什么这个源不工作"进到用户/agent 看得见的地方，而不是只在 const 注释里）
func (m *Multi) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error)

// KindOf 返回该能力产出的内容种类。fetcher 在 finalize 时用它填 ContentItem.Kind（§7.2）。
func (m *Multi) KindOf(p types.Platform, c types.Capability) (types.Kind, bool)
```

**Multi 的构造**：`NewMulti(cfg config.FetchConfig, seen SeenChecker, snaps SnapshotStore) *Multi`
——注册表在此装配。未配置 key 的 provider 仍注册（构造零成本），
等真有该源被抓时才返回配置缺失错误（沿用现状语义）。

---

## 7. 身份规则（`fetcher/identity.go`）

### 7.1 按 (platform, capability) 分派

```go
// CanonicalKey 算出内容的全局身份（content_items.canonical_key，UNIQUE）。
//
// 按 **platform** 分派而非 provider：这正是"换供应商不产生重复内容"的承载者
// （契约 §18 不变量 I1）。tweet_id 是 X 平台的事实，不是 TikHub 的事实——
// 换成官方 X API 或 syndication 旁路，同一条推算出的键逐字节相同。
//
// **键是裸值，不加命名空间前缀**（web=url 原文、xhs=note_id 原文）：007 迁移的回填
// 写的就是裸值，加前缀会让存量 231 条的键与运行时算出的键永不相等，全库内容立刻
// 重新长出一份。改这里必须同步改 007 的回填 CASE，反之亦然。
//
// page_watch 是**唯一按 capability 分派**的例外：它产出的不是"一篇内容"而是
// "一次变化事件"，身份自然是「哪个页面、从什么变成了什么」——详见 §7.3。
func CanonicalKey(src types.Source, item types.ContentItem) string {
    switch {
    case src.Platform == types.PlatformWeb && src.Capability == types.CapPageWatch:
        return "" // 由 page_watch provider 自己构造（§10.4），此处不猜
    case src.Platform == types.PlatformWeb:
        return strings.TrimSpace(item.URL)          // feed / search：url。存量 102 条不变
    case src.Platform == types.PlatformXHS:
        return xhsKey(item.ExternalID)              // note_id。存量 129 条不变
    case src.Platform == types.PlatformX:
        return strings.TrimSpace(item.ExternalID)   // tweet_id（转推取被转推那条，§9.4）
    default:
        return ""                                    // 未知平台不猜身份字段
    }
}
```

### 7.2 `finalize` 的两处改动

#### (a) 【致命】身份**不得覆盖 provider 已填的值**

已复验 `fetcher/fetcher.go:347` 现状是**无条件赋值后判空丢弃**：

```go
item.CanonicalKey = CanonicalKey(src, *item)   // ← 无条件覆盖调用方已填的值
if item.CanonicalKey == "" { slog.Warn(...); return false }
```

而 §7.1 让 `CanonicalKey` 对 page_watch 返回 `""`、§10.4 让 provider 自己填转移键
——两者在此**正面相撞**：provider 填好的 `watchKey(...)` 被擦成空串 → `return false`
→ **每一条 change 都被丢弃**，且 `markFetchResult(ok=true)`（抓取本身没失败）
→ fail_count 不涨 → **Gate ⑧「抓取失败可见」也看不见**，只剩一行原因还写错了的 `slog.Warn`。

```go
// 改为「已填则不覆盖」：
// 身份优先由 provider 自己构造——page_watch 的转移键含 prevHash，是**历史的函数**，
// identity.go 只拿到 (src, item) 无从推导。未填时才按 platform 分派（§7.1）。
// 改这里必须同步看 §7.1 的 page_watch 分支，反之亦然。
if item.CanonicalKey == "" {
    item.CanonicalKey = CanonicalKey(src, *item)
}
if item.CanonicalKey == "" {
    slog.Warn("fetcher: 内容缺少身份字段（web=url / xhs=note_id / x=tweet_id；page_watch 由 provider 构造），跳过该条", ...)
    return false
}
```

> **顺序仍不可重排**（现有注释的教训）：`content_hash` 先算（`external_id` 兜底要用它）；
> `canonical_key` 必须在 `external_id` 兜底**之前**定好。

#### (b) Kind 必须非空

```go
// 做成"写不出来"而非"注释提醒"——Kind 零值 "" 会让 Dedup 按 article 处理
// → 页面变化被 simhash 静默吞掉（§1.1），**无任何错误信号**。
if item.Kind == "" {
    slog.Warn("fetcher: 内容缺少 kind，跳过该条", "source_id", src.ID, ...)
    return false
}
```

> **⚠️ finalize 的校验挡不住 §3.3.1 的病**：它只保证**写入前**的内存值非空。
> Kind 是否活着走完 DB 往返，是 §5.4.2 的事。**两处都要做。**

### 7.3 跨平台裸值键的碰撞分析（**已实测，但记录为经验结论而非结构保证**）

| platform | 实测形态 | 长度 |
|---|---|---|
| web | `https://www.bbc.co.uk/news/articles/clyxyzlp9p2o?at_medium=RSS&...` | 恒含 `://` |
| xhs | `6a5648b0000000000702ef1c` | 24 字符小写 hex |
| x | `2077807977193714080` | ~19 位十进制 string |

三者**不可能逐字节相等**（url 恒含 `://`；24 字符 hex 与 19 位十进制长度不同）。
→ 裸值键在当前三平台下不碰撞。

> **⚠️ 这是基于当前 ID 格式的经验结论，不是结构保证。** 第四个平台若产出裸 url 形态的 ID
> 仍会撞，而碰撞的后果是**把两篇无关内容合并成一篇（不可逆）**。
> **新增平台时必须重做本节的碰撞分析**，这是 §17 Gate 的一条。
> 彻底修需全库加前缀 = 007 同级身份重构，与 §18 D5 的 url 归一化同属被显式推迟的课题。

---

## 8. 内容种类与下游 pipeline

### 8.1 `workflow/activities.go` Dedup —— change 豁免近似去重

```go
for _, item := range in.Items {
    // change 类内容豁免 simhash 近似去重（契约 §1.1）。
    //
    // 不是"优化"，是**正确性**：simhash 的设计目的写在 dedup 包注释里——
    // 「改动少量文字仍判为重复」。而 change 的信号恰恰是改动少量文字
    // （$30.00 → $24.00 汉明距离 ≈ 0-1，必 ≤ simhashThreshold=3）。
    // 不豁免的话，页面变化会被静默丢弃，表现为 pipeline「去重后无内容」早退
    // ——与红线 1 的 M3 事故同构的静默失败。
    //
    // change 的精确去重由 canonical_key 的 UNIQUE 承担（§10.4），不依赖 simhash。
    if item.Kind == types.KindChange {
        s := dedup.Simhash(item.Title + " " + item.Content)
        item.Simhash = &s   // 仍回填：Push 建 Delivery 时要用，且留作分析
        kept = append(kept, item)
        continue            // **刻意不进 batchSeen**：change 不该成为别人的近重复判据
    }
    // ... 原逻辑不变
}
```

### 8.2 `scorer` —— change 走独立 prompt

已复验 `scorer/scorer.go:74` 的现行 prompt 会给 diff 打 0-20 分（"正文信息过少"）。

```go
// buildScoreUser 按 item.Kind 分派 prompt（契约 §1.1）：
//   KindArticle → 现行 scoreSystemPrompt（**与 M5 逐字节一致，不得改动**——
//                 M5 §15 有"空画像+空反馈+空任务指令与 M3 逐字节一致"的黄金用例守着；
//                 P1c 命中时会消毒外部内容伪造的任务手册前缀，再在尾部追加任务块）
//   KindChange  → scoreChangeSystemPrompt：把"正文信息过少给低分"替换为
//                 "【待评估内容】是一次页面变化的 diff，短是正常的；
//                  按**这次变化对该用户的重要性**打分"
```

> **取舍**：这让 scorer 有两条 prompt 路径，M5 的"四象限黄金输出"用例只覆盖 article。
> 接受理由：不分派的话 change 恒 0-20 分、功能完全不工作；而共用一条 prompt 会污染
> article 路径（M5 Gate ①②③ 的探针都建立在 article 的分数分布上）。

### 8.3 `cardgen`

`KindChange` 的 bodyMD 直接用 diff 生成（LLM 门已在 §10.6 判过重要性）；
标题模板 `「<页面标题> 有变化」`。**不改 cardgen 的注入防护措辞**（M5 §15 有断言守）。

---

## 9. `x/user_posts` 实现（`fetcher/x.go`）

> 全节依据 2026-07-16 真实 key 实测（11 个请求，报文级）。
> **每一条"不要照抄小红书"都对应一个实测到的不兼容。**

### 9.1 端点与入参

```
GET https://api.tikhub.io/api/v1/twitter/web/fetch_user_post_tweet?screen_name=OpenAI
Authorization: Bearer <key>
```
- `screen_name` **直接认名**（实测 `OpenAI`/`AnthropicAI`/`GoogleDeepMind` 全通）
  → **不需要 user_id 解析步骤**：少一个端点、少一次计费、少一层状态
- `cursor` 翻页：`next_cursor` 确实返回，但**未实测** → MVP 单页，靠 canonical_key 增量去重（同 xhs 现状）

### 9.2 **不要复用 `tikhubEnvelope`**——三处实测不兼容

| | 小红书 | Twitter |
|---|---|---|
| 成功标志 | `data.success` (bool) | **不存在**；只有 `data.status == "ok"` (string) |
| 数据位置 | `data.data.items[].note` | **`data.timeline[]`**（扁平，少一层） |
| 错误外壳 | 同上 | **`{"detail":{"code":400,...}}`，无顶层 code** |

```go
// 判据是 code==200 && len(data.timeline)>0，**不依赖 data.status**：
// 三个号实测均为 "ok"，但**未见过非 ok 的样本**——不知道失败时它是变值还是缺失。
// 拿一个只见过单一取值的字段当判据，就是在赌它的失败形态。
```

### 9.3 **不要搬详情补全那套闸门**

实测：**原创推文一次给全文，不截断**（三号最长 534/475/303 字符，无一条以 `…` 结尾）。
→ `SeenChecker` / `enrichDescs` / `tikhubDetailInterval` / `tikhubEnrichBudget` 整套
$0.01 付费闸门**在 X 侧没有存在理由**（M5 缺陷 1 的根因是 XHS 上游截到 60 rune，X 不截）。

**陷阱**：搜索端点的 `display_text_range` **不能用来切 text**——实测 20/20 与 `len(text)` 不符
（`[0,273]` vs `len=469`，切出来是半句话）。**正是 M5 缺陷 1「证据不足→模型编造」的翻版。**

### 9.4 转推**拆包**，不是过滤 —— 这条来自两份实测报告的交叉

**若按直觉「`retweeted` 键存在即排除」，会正好丢掉 Anthropic 的产品发布公告。**

- 实测（信源核实）：`anthropic.com` 挂 `@AnthropicAI`、`claude.com` 挂 `@claudeai`；
  **@AnthropicAI 时间线实测含 `RT @claudeai: We're introducing Claude for Teachers...`**
  → **公司号转发产品号做产品发布**是真实模式
- 实测（TikHub）：RT 的 `text` **被截到 140 字符**（OpenAI 5 条 RT 全部恰好 140 + U+2026），
  但**全文在 `retweeted_tweet.text`**（同条实测 140 vs 215）

```go
// 转推拆包（契约 §9.4）：转推不是新内容，**被转推的那条才是**。
//   Content     = rt.RetweetedTweet.Text            // 全文，不是 140 截断的 text
//   Author      = rt.RetweetedTweet.Author.ScreenName // 真实原作者
//   PublishedAt = rt.RetweetedTweet.CreatedAt
//   ExternalID  = rt.RetweetedTweet.TweetID         // ← 身份取被转推那条
//
// **副产品**：同时订 @AnthropicAI 与 @claudeai 时，同一条公告经两个源抓到 →
// canonical_key 相同 → 归并成一份 content_item + 两行 content_sources。
// 这将是 **007 跨源归并机制第一次在生产真正触发**（实测当前 multi_source_items=0），
// 也是「首发源 + 出现时间差 = 信源质量分析」的第一个真实样本。
```

**原创/转推/引用/回复的判据**（实测）：

| 类型 | 判据 | 处置 |
|---|---|---|
| 转推 | `retweeted` **且** `retweeted_tweet` 存在 | **拆包**（`include_retweets` 默认 true） |
| 引用 | `quoted` 存在 | **保留**（`text` 是官号自己的原创评论且完整） |
| 回复 | `reply_to` 存在（string） | **保留**（`include_replies` 默认 true，见下） |
| 纯原创 | 三键都不存在 | 保留 |

**致命细节（实测，必须写进代码注释）**：

```
RT 那条: author.screen_name == "OpenAI"       ← 仍是转推者，不是原作者！
        conversation_id == tweet_id → True    ← 与原创推文完全一样！
```
→ **用 `author.screen_name` 或 `conversation_id == tweet_id` 判原创，会把 5 条 RT 全当原创收进来。**

`include_replies` 默认 **true**：实测 19 条里 7 条 `reply_to`，**全是官号自己回自己（自建 thread）**，
属原创内容；无差别过滤会丢掉 thread 后续楼层。
> **⚠️ 未验证**：本端点是否会返回"回复别人"的推文（样本 0 条）。若会，默认 true 会把官号回路人的
> 闲聊推给用户。**Gate 须实测这一条**（§17）。

### 9.5 Go 建模陷阱（全部实测）

```go
// media / entities 是**多态**的：空时 []（数组），非空时 {}（对象）。
// 实测 media 13/19 空、entities 8/19 空。静态 struct 会直接报
// "cannot unmarshal array into Go value of type ..."。**小红书没这个坑，照抄会炸。**
Media    json.RawMessage `json:"media"`
Entities json.RawMessage `json:"entities"`

// views 是 string（"93400"）不是 int；tweet_id/conversation_id/author.rest_id 全是 string。
// source 只在 19 条里的 9 条出现 → 必须 optional。

// created_at 是 **Twitter 原生字符串**："Thu Jul 16 17:30:00 +0000 2026"
// → time.Parse("Mon Jan 02 15:04:05 -0700 2006", ...)
// 这是继 XHS timestamp(秒) / note_card.time(毫秒) 之后的**第三种时间表示**。

// 无 URL/permalink 字段 → 自拼 https://x.com/<screen_name>/status/<tweet_id>
// data.user 里**没有 screen_name**（profile 为 None）→ 回校用 author.screen_name 或 params 回显
```

### 9.6 响应结构的两个坑（3/3 号复现）

```go
// ① 忽略 data.pinned：置顶推同时出现在 pinned 和 timeline[]，且是长期不变的老帖每轮都返回。
//    只读 data.timeline（其内部 tweet_id 实测无重复）。

// ② timeline **不是严格倒序**：实测排序为「按 thread 根倒序、自 thread 回复紧跟其根」
//    #0 17:30:00 ORIG / #1 17:30:01 REPLY ← 比上一条晚 1 秒
//    → lookback 过滤**绝不能写成"遇到第一条超期就 break"**，必须逐条判 created_at。
//    （PR #13 给 RSS 加的 applyLookback 是逐条判，正确；照抄时别"优化"成 early-break。）
```

### 9.7 计费与限速

- $0.001/请求（**比 XHS 便宜 10 倍**）；不需详情补全 → 三个官号一轮 **$0.003**
- **不赌元数据的 `10/second`**：同一份元数据对 XHS 也写 10/second，
  而生产实测超速直接 429（`tikhub.go:55-58` 因此硬编码 1.1s 串行）。
  **同一字段在同一平台已被证伪过一次** → 复用现有实例级节流闸门。
  三个官号一轮仅 3 个请求，**没有任何理由去赌**。

### 9.8 回校（比小红书充裕）

顶层 `params` **原样回显入参**（天然锚点）、`data.user.rest_id`、每条 `author.screen_name`、
`router` 回显路径、`request_id` + `cache_url`（24h 免费复查原始报文，事故复盘利器）。
- 实测 3/3 号**未复现** XHS 式串号，但**只测了 3 个账号，不能证否** → 保留 `params` 回校。

---

## 10. `web/page_watch` 实现（`fetcher/pagewatch.go`）

> **⚠️ 本节整节已下线（`refactor/drop-page-watch`）。** `web/page_watch` 能力、
> `fetcher/pagewatch.go`、`store/pagesnapshots.go`、`page_snapshots` 表（迁移 016 DROP）、
> `types.CapPageWatch/KindChange/SnapshotVerdict/RefTypeSource/PageSnapshot` 均已从代码移除。
> 页面变化监控改由 **Exa `/contents` fetch API** 覆盖（§10.1.1 早已给出这条方向），不再在
> Go 侧自建抓取+基线 diff+LLM 门。**本节以下内容保留为历史设计记录**（解释当初为何这样设计、
> 以及那些至今仍成立的通用不变量如 SSRF 栈、转移键思路），但不再对应任何在库代码。
> 下游连带影响：§8.1 的 Kind 豁免、§8.2 的 change 打分 prompt 一并移除；`kind` 列与
> `KindArticle` 保留（承载全内容，将来若引入新内容种类可复用）。

### 10.1 抓取

```go
// **必须复用 Fetcher 的 SSRF 栈**：LookupIP 预检 + Dialer.Control（防 DNS rebinding）
// + blockedCIDRs + noRedirect。exa/tikhub 因目标是固定可信主机才豁免；
// page_watch 抓的是**用户提供的 url**。自己 new 一个 http.Client 就是一个 SSRF 洞。

// **不要设浏览器 User-Agent**（实测，反直觉）：
//   ai.google.dev  不设 UA → 200 拿到 400 个价格
//                  设 Chrome UA → 302 跳 Google OAuth(prompt=none&auto_signin=True) → 无限重定向
// 现有 fetcher.go:143 给 RSS 设的 "Vane/0.3" **不可照搬到 page_watch**。
```

### 10.1.1 抓取方式的另一选项：Exa `/contents`（2026-07-17 补充，Boss 拍板方向）

**拍板方向**：无需登录态的公开页面，不维护自己的 SSRF 防护 + `http.Client` 抓取代码，
直接用 Exa 的 `/contents` 工具抓；需要登录会话的页面（若未来接入）仍走 §10.1 自建客户端
——两者不是互斥关系，是按"要不要认证"分流。

**实测依据**（2026-07-17，对比目标 `platform.claude.com/docs/en/about-claude/pricing`，
即 §10.3 选定的正确 URL，真表格 14 个 `<table>` / 95 个 `<tr>`）：

- **⚠️ 致命坑，必须写死**：Exa `/contents` **默认（不传 `maxAgeHours`）优先吃缓存**
  ——实测 `statuses[].source == "cached"`，0.4s 返回。**必须显式传 `maxAgeHours: 0`**
  才会强制活抓（实测 `source == "crawled"`，3–4s）。漏了这一行，page_watch 会拿旧快照
  跟上次快照比较，**变化监控静默失效**且不报错——比 §10.2 那条 go-readability 的坑更隐蔽，
  因为**请求本身 200 成功、没有任何错误信号**。
- **确定性**：同一 URL 相隔数秒两次独立 `maxAgeHours:0` 活抓，返回文本**逐字节相同**
  （sha256 一致，25943 字符）。对基于 hash 的变化检测（§10.4）没有引入噪声——
  但这是 Exa 内部实现细节，不受 Vane 控制版本，**将来 Exa 若改抽取算法，此结论需要重测**。
- **结构保留优于预期**：真表格页上，Exa 把 `<table>` 转成 markdown 表格行，
  且实测**没有 `CellPaddingBehaviorAligned` 那种对齐填充**（各行分隔符统一是单空格
  `| 单元格 | 单元格 |`，不因单元格长度补空格），单元格值变化理论上仍是单行 diff——
  **和 §10.2 goquery 压平表格行想要的效果基本等价**，且自带表头 `| Model | ... |`
  免去 §10.2 提到的"Google 模型名在表外要手动拼接"那类特殊处理。
  未验证项：markdown 转换是否在所有目标站点都稳定保留"一行一模型"粒度
  （§10.2 的 goquery 方案是 Vane 自己的代码、行为可测试锁定；Exa 的转换逻辑不透明，
  没有 SLA，需要在正式接入前对 §10.3 列出的三个目标站点都跑一遍这个粒度检查）。
- **成本**：两次测试（26K/28K 字符页面）`costDollars.total` 均为 $0.001/次。
  按当前唯一在跑源 `fetch_interval_seconds=1800` 折算约 $0.05/天/源，量级可忽略，
  但客户量上来后要按源数重新估算，不能假设一直是这个数量级。
- **不适用场景**：§10.1 的 SSRF 防护栈是防**用户提供的任意 URL**打 Vane 自己的内网；
  换成 Exa 抓取后，请求源变成 Exa 的基础设施而不是 Vane 服务器，**Vane 侧 SSRF 风险归零**，
  但没有做过滥用测试的前提下，不代表可以对 Exa 不做任何 URL 校验就透传——仍需和 §10.1
  一样做 scheme/host 层面的基本合法性检查，只是不再需要 `lookupIP`/`isBlocked` 那层。

**顺带发现（与本节决策无关，但同一次实测暴露）**：生产库当前唯一在跑的 page_watch 源
（`sources.id=11`，`https://www.anthropic.com/pricing`）用的正是 §10.3 表格里明确标了
❌ 的那个 marketing 页——零 `<table>`/`<tr>`，只能靠 `body.Text()` 兜底，抽取质量远低于
本契约设计的目标。**这是本契约 §10.3 结论尚未回填到生产配置的独立遗留问题，建议单独修，
不要和这次 Exa 抓取方式的决策混在一次改动里。**

#### §10.1.2 拍板追加（2026-07-17）：与 Exa 重复的自建代码——删除范围

Boss 拍板：不再手动维护与 Exa `/contents` 功能重复的抓取/解析代码。全局核查后
（`grep os/exec` 全仓 0 命中、goquery 唯一使用方是 pagewatch）划定边界如下：

| 代码 | 处置 | 理由 |
|---|---|---|
| `pagewatch.go` HTTP 抓取段（UA/403/塌缩前的 body 读取，§10.1） | **删，换 Exa `/contents`** | 与 Exa 抓取完全重复 |
| `pagewatch.go` `extractTableText`（goquery 压平，§10.2） | **删，用 Exa 的 markdown 表格输出** | 实测输出粒度等价（§10.1.1），且免 Google 拼 heading 的特殊处理 |
| `go.mod` 的 `PuerkitoBio/goquery` 依赖 | **删** | 全仓唯一使用方就是上一行 |
| `pagewatch.go` 快照对比 / 塌缩检测 / `simpleDiff` / `watchKey` / `min_rows` 闸门 | **留** | Vane 业务逻辑，Exa 不提供；闸门改为作用在 Exa 返回文本上 |
| `fetcher.go` SSRF 栈（`lookupIP`/`isBlocked`/`Dialer.Control`） | **留** | RSS 仍自建抓取（Exa `/contents` 返回正文 markdown、给不了原始 XML feed），SSRF 栈的第一用户是 RSS 不是 page_watch |
| `exa.go`（search 能力） | **留** | 是 search 信源本体，与 /contents 抓取是两回事 |
| `tikhub.go` / `x.go` | **留** | 平台 API 通道，Exa 不可替代 |

实现时机：**M5 收官优先的主线决策（2026-07-16）不变**，本节实现随 M6 落地，
不插队。落地时按 §10.1.1 的待验证项先跑三站点粒度检查。

### 10.2 抽取：**goquery 压平表格行**，**不用 readability**

**实测：`go-readability` 在 Anthropic 价格页静默失败**——返回 `err == nil`、
`Title == "Pricing"`（正确！）、**264 字符正文、0 个价格**（朴素抽取同页 26,691 字符 / 144 个价格）。
**不报错、不 panic、标题还对** → 监控会安静地 hash 那 264 个字符，**永远不触发**。
（且 `go-shiori/go-readability` **已归档**，最后一次提交就是 deprecation notice。）
`go-trafilatura` 更差：**静默丢掉每一个订阅档**（$20 Pro、$100/$200 Max）——
定价页按它的启发式**就是 boilerplate**；且 15.8MB / 76 模块，内嵌 wazero（WASM 运行时）。

**选定 `goquery`**（14,976★，v1.12.0，5.8MB/15 模块，API 明确稳定），把 `<tr>` 压平：

```
gpt-5.6-sol | $5.00 | $0.50 | $6.25 | $30.00 | $10.00 | $1.00 | $12.50 | $45.00
Claude Opus 4.8 | $5 / MTok | $6.25 / MTok | $10 / MTok | $0.50 / MTok | $25 / MTok
```

**为什么压平是关键**（实测）——它决定 diff 可不可读，而不是 diff 库决定：

```
朴素抽取 + unified_diff(n=3)：        goquery 压平 + 行级 diff：
  $6.25                                - gpt-5.6-sol | $5.00 | ... | $30.00
 -$30.00                               + gpt-5.6-sol | $5.00 | ... | $24.00
 +$24.00                               ↑ 一行、自带模型名，LLM 可直接写出
  $10.00                                 "gpt-5.6-sol 输出价 $30→$24"
 ↑ 整个窗口没有模型名、没有列名
```

- **分隔符必须是 ` | ` 无对齐填充**：实测 `html-to-markdown` 的默认 `CellPaddingBehaviorAligned`
  会让**一个单元格变化引发 5 行里 5 行都变**（改 `None` 后只有 1 行变）
  → **若有人图省事改用 markdown 表格渲染，diff 会从 1 行炸成整表。**
- **Google 需特殊处理**（实测）：它一个模型一张表、模型名在表**外**的标题里，
  压出来是 `Input price | Free of charge | $1.50`——**行内没有模型名**
  → 必须**把最近的前置 heading 拼到每行前面**。

### 10.3 目标 URL 必须钉死（实测，"看起来更对"的那个是错的）

| 目标 | ✅ 用这个 | ❌ 不要用 |
|---|---|---|
| OpenAI | `developers.openai.com/api/docs/pricing`（305 个价格） | `openai.com/api/pricing/`（**403 challenge**；且价格 39 个里只有 2 个在 DOM，其余在 RSC payload） |
| Anthropic | `platform.claude.com/docs/en/about-claude/pricing`（302 个价格） | `claude.com/pricing`（marketing 页，div 布局，仅 13-20 个价格） |
| Google | `ai.google.dev/gemini-api/docs/pricing`（400 个价格） | `ai.google.dev/pricing`（**OAuth 死循环，50 跳耗尽**） |

**marketing 页 vs docs 页是本节最容易踩的坑**：marketing 页无 `<table>`、价格在 RSC payload 里
→ 任何 DOM 抽取器只拿到 ~5%。**docs 页才有真表格。**

**运维炸点（三份独立报告三次命中）**：`openai.com` 的 HTML 页会触发
`Cf-Mitigated: challenge` + 403 且**间歇性**（200 → 403 → 200），
但 `openai.com/news/rss.xml` 与 `developers.openai.com` **豁免**。
VPS 固定 IP 会更快被拉黑。

### 10.4 身份与**重试自愈**——本节是全案最容易埋雷的地方

**若沿用「web → url」的规则，同一页面的第二次变化会与第一次撞 UNIQUE 被静默丢弃
——这个页面从此永远不再报变化。** 故 page_watch 是唯一按 capability 分派身份的例外。

```go
// watchKey 构造变化事件的身份：watch://<url>#<prevHash>-><newHash>
//
// 为什么带 prevHash（转移键）而不只是 newHash：
//   只用 newHash 时，页面 A→B→A 回退到曾出现过的状态会撞已有键 → **回退永远不报**。
//   转移键让 A→B 与 B→A 是两条不同的记录（回退能报），而 A→B→A→B 的第二轮 A→B
//   会撞第一轮 → **2-cycle 抖动（A/B 测试）自动收敛到最多 2 次推送后静音**。
//   这恰好是我们想要的：真回退要报，抖动要闭嘴。
//
// 碰撞安全：hash 是定长 hex，故 "url1#h1->h2" == "url2#h3->h4" ⟺ url 相等且两 hash 相等。
// 与 web/feed 的 url 键不相交（watch:// vs http(s)://）。
func watchKey(url, prevHash, newHash string) string
```

**重试自愈（这条必须逐字看）**：

> **初稿在此有一个致命的自相矛盾**（对抗审查抓到）：初稿把「基准前进」绑在
> 「content_items 里有对应行」上，而 §10.6 的 LLM 门**有权不产出 ContentItem**。
> 于是同一个 DB 状态（**有快照、无内容项**）被赋予两个互斥语义——
> §10.4 读作「崩溃了，重试我」，§10.6 读作「门判了不重要，往前走」，**设计上无法区分**。
> 更直接的证伪：初稿步骤 3「首轮建基线**不产出**」本身就制造了一个**永久孤儿快照**
> ⇒「孤儿 ⇒ 崩溃」这个不变量**从第一轮就不成立**，基准会永远卡在 H1、diff 无限累积。
>
> **修法：把「门的判定」物化成列，不要让它和「崩溃」共用一个状态。**

```go
// SnapshotVerdict 决定该快照是否已 settled（= 可以充当基准）。
type SnapshotVerdict string
const (
    SnapshotVerdictPending    SnapshotVerdict = "pending"    // 刚写入，命运未定 → **未 settled**
    SnapshotVerdictBaseline   SnapshotVerdict = "baseline"   // 首轮基线，刻意不产出 → settled
    SnapshotVerdictSuppressed SnapshotVerdict = "suppressed" // LLM 门判「不重要」 → settled
    // 注意没有 "reported"：门判「重要」时的 settled **由 content_items 的存在性证明**，
    // 而不是再写一个列——provider 不知道内容项有没有落库（那是 workflow 的下一步），
    // 让它写 "reported" 就是在撒它不知道的谎，也正是初稿矛盾的根源。
)

// Baseline 返回该源的 diff 基准 = **最近一个已 settled 的快照**；一个都没有时返回 nil。
//
//   settled ⟺ verdict IN ('baseline','suppressed')            -- provider 自己能定的
//          ∨ EXISTS(content_items WHERE canonical_key = ps.canonical_key)  -- 门判重要且已落库
//
// 为什么不是"最近一次快照"（**这是本设计最容易写错的地方，突变体 M4 逃逸过**）：
//   Fetch 活动的 120s StartToCloseTimeout **由全部到期源共享**，进程也可能被重启。
//   若基准取"最近一次快照"，则在「快照已写入、内容项尚未入库」之间被掐断并重试时，
//   重试会认为"没变化" → **那次变化永久丢失且无任何告警**。
//
// 为什么 pending 不算 settled：pending ⟺「产出了 ContentItem 但还没确认落库」
//   ⇒ 正是崩溃窗口 ⇒ 基准不前进 ⇒ 重试重新产出同键 ⇒ 收敛。
//
// 这与 store.EnrichedCanonicalKeys 的判据选择是同一个教训：
//   判据必须选"正文长度"而非"行是否存在"，否则一次瞬时 429 让那条笔记终身 60 字。
type SnapshotStore interface {
    Baseline(ctx context.Context, sourceID int64) (*types.PageSnapshot, error)
    PutSnapshot(ctx context.Context, snap *types.PageSnapshot) error
    SettleSnapshot(ctx context.Context, sourceID int64, canonicalKey string, v types.SnapshotVerdict) error
}
```

对应 SQL（**键由 Go 构造后存入 `page_snapshots.canonical_key`，SQL 只做等值 JOIN，
不重新拼键**——键的构造分散两处必然漂移，这是 identity.go 的教训）：

```sql
-- Baseline：最近一个已 settled 的快照。一条查询，无回退分支。
SELECT ps.id, ps.canonical_key, ps.content_hash, ps.extracted_text, ps.verdict
FROM page_snapshots ps
WHERE ps.source_id = $1
  AND (ps.verdict IN ('baseline','suppressed')
       OR EXISTS (SELECT 1 FROM content_items ci WHERE ci.canonical_key = ps.canonical_key))
ORDER BY ps.id DESC LIMIT 1;
```

**流程**：

```
1. 抓 → 合理性闸门（§10.5）→ 抽取 → text → newHash = sha256(text)
2. base := Baseline(sourceID)
3. base == nil → PutSnapshot{key: watchKey(url,"",newHash), verdict: baseline}
                 → **首轮建基线，不产出** → return nil
4. base.ContentHash == newHash → 无变化 → return nil
5. key := watchKey(url, base.ContentHash, newHash)
   PutSnapshot{key, verdict: pending}                    // ON CONFLICT DO NOTHING
6. diff := Diff(base.ExtractedText, text)
7. LLM 门（§10.6）：
     不重要 → SettleSnapshot(key, suppressed) → return nil     // 基准前进，不推送
     重要   → return ContentItem{Kind: change, CanonicalKey: key, Content: diff}
              // 保持 pending；settled 由 content_items 的存在性证明
     门失败 → fail-open，按「重要」走（§10.6 红线 1）
```

**逐态验证**：

| 场景 | 结果 |
|---|---|
| 首轮 | 建基线 H1（`baseline`=settled），不推送 ✓（否则整页当"变化"推） |
| 次轮无变化 | base=H1，newHash=H1 → return nil ✓ |
| 次轮变 H2，门判重要 | base=H1 → key=`#H1->H2`（`pending`）→ 入库 → EXISTS 成立 ⇒ **H2 settled** ✓ |
| **产出后崩溃重试** | H2 是 `pending` 且无内容项 ⇒ **未 settled** ⇒ base 仍是 H1 ⇒ **重新产出同键** ⇒ ON CONFLICT 去重 ✓ **自愈** |
| 次轮变 H2，门判**不重要** | `SettleSnapshot(suppressed)` ⇒ **H2 settled** ⇒ 基准前进 ⇒ **噪音不会让 diff 永远对着 H1** ✓ |
| H2→H3 | base=最近 settled=H2 → key=`#H2->H3` ✓ |
| H3→H2 回退 | base=H3 → key=`#H3->H2` ≠ 任何已有键 → **回退能报** ✓ |
| A/B 抖动第 3 轮 | key 撞第 1 轮 → ON CONFLICT DO NOTHING → **静音** ✓ |

> **关于 `ON CONFLICT DO NOTHING` 的精确表述**（初稿此处引用不精确，事实核查者纠正）：
> `UpsertContentItem`（`store/content_items.go:71-85`）在 `DO NOTHING` **之后还有一条
> 「更长的正文赢」的条件 UPDATE**（`WHERE char_length(content) < char_length($2)`）。
> 对 change 无害：转移键让两次不同变化的键天然不同（不冲突）；重试时内容逐字节相同
> ⇒ `char_length` 相等 ⇒ UPDATE 空转。**但 §16 必须有用例钉死这一点**，
> 否则将来有人改动那条 UPDATE 的谓词时，change 的正文会被静默改写。

### 10.5 合理性闸门：**静默返回空是最坏的失败模式**

实测证据：`urlwatch` 的 selector 匹配不到时**静默返回空串并当作"全部内容被删除"上报**
（维护者称 by design），是该项目**头号误报源**；`changedetection.io` 的 include filter 抛
`FilterNotFoundInResponse`，**但它自己的 subtractive selector 又是静默的**——
**同一个项目里两种策略并存**，说明这事极易做错。

```go
// 抽取后必须过不变量断言，**不满足 = 报错（fail_count++），不是"变化"**：
//   - HTTP 403 challenge → CodeFetchTimeout + **Retryable = true**
//   - 其余非 2xx → CodeFetchTimeout，Retryable 按 5xx/4xx（沿用 fetcher.go 现状）
//   - 抽出行数 < min_rows（默认 5；**散文文档页应设 0 关闭本闸门**，见 §18）→ CodeValidation
//   - 抽出价格数 < min_prices（默认 3，仅当上次快照有价格时）→ CodeValidation
//   - 相对上次快照**塌缩超过 50%** → CodeValidation，**且不得 PutSnapshot**
//     （塌缩文本若成了基准，页面恢复时会再报一次假变化）
//
// **403 的错误码与 exa/tikhub 刻意不同，别"对齐"**（对抗审查指出的冲突点）：
//   exa.go / tikhub.go 把 401/403 归 CodeValidation，因为那是**鉴权失败**——
//   "key 配错是本方配置问题，不可重试"。
//   而 page_watch 抓的是**无需鉴权的公开页**，它的 403 是 **bot challenge**：
//   实测 openai.com 呈现 200 → 403 → 200 的**间歇性**（三份独立报告三次命中）
//   ⇒ 是瞬态，Retryable = true；判成 CodeValidation 会让这个源被当成"配置错"永久搁置。
//
// 为什么这是硬要求（实测）：openai.com 的 challenge 页只有 9,929B，
// 真实页 582,777B。不设防的话 challenge 页会被抽取成完全不同的文本 →
// **报一个巨大的假变化并推送垃圾**。selector 匹配不到、页面改版同理。
```

### 10.6 降噪：**抽取层就是主力降噪器，不要过度设计**

实测同一 URL 连抓两次（**零真实变化**）：

| 方案 | platform.claude.com/pricing | models/overview |
|---|:---:|:---:|
| 原始 HTML 全文 hash | **【误报】** | **【误报】** |
| **抽取文本 hash** | ✅ 稳定 | ✅ 稳定 |

噪音源已精确定位：Anthropic 页用 Radix UI，每次渲染生成随机组件 ID
（`radix-_R_1babupffj2mdb_`，pricing 页 5 个、models/overview 页 17 个）
——**这些 ID 活在 HTML 属性里，一做文本抽取就消失了**。

→ **不需要 changedetection.io 那套复杂 ignore 规则**。广告位/A-B/cookie 横幅的顾虑
  **在官方 docs 站上基本不存在**（这不是电商页）。

**LLM 门**（`span_name = "page_diff"`，**`ref_type = "source"` / `ref_id = <source_id>`**）：

> **`types.RefType` 需新增 `RefTypeSource = "source"`**（对抗审查指出）：
> 已复验 `llm_calls` **无 source_id 列**，多态关联只有 `ref_type + ref_id`，
> 而现有 RefType 只有 `push_batch / feedback / content_item / profile`。
> 不加这一项，§17.1 的 page_diff 探针**无从按源聚合**——
> "这个源的门跑了 N 次却一条都没产出"（楔死告警）就写不出来。

```go
// 只在 hash 已变之后才跑；只喂 diff hunk（几行）不喂整页 → 一次几百 token，远低于 $0.001。
//
// 三条红线：
//   1. **fail-open**：LLM 挂了/预算耗尽/解析失败 → 一律当"重要"照推。
//      静默吞掉才是红线 1（M3 三批假 50 分静默照推）的翻版。
//      （changedetection.io 的 llm_intent 也是 fail-open：{'important': True}）
//   2. **DeepSeek V4 结构化输出必须 thinking: disabled**（红线 1）
//   3. 只喂 diff，不喂原始 error 链（红线 3）
//
// **无论判定如何，快照都已推进**（步骤 5 在门之前）：否则噪音会让 diff 永远对着旧基准、
// 每轮重跑 LLM。判定只决定要不要产出 ContentItem。
```

**可选增强**（照抄 changedetection.io 的 `_annotate_moved_lines`，纯正则零成本）：
把「同时出现在 `+` 和 `-` 两侧的行」标为移动、把 `^\d+\s+\w+\s+ago$` 标为相对时间戳，
一举干掉"重排序"与"X 天前"两大噪音类。**P2 可选，不阻塞。**

### 10.7 存储：**正因为红线 5 不能清理，才更不能存原始 HTML**

| 方案 | 可生成可读 diff | 存储/页/年（每日一次） |
|---|:---:|---:|
| 原始 HTML 全文 | ✓ | **~550MB** |
| 只存 hash | ✗ **无法 diff** | ~25KB |
| **压平表格行** | ✓ | **~3.5MB**（原文的 0.2%），且未变化的天天然 dedup≈0 |

4 个页面存原始 HTML = 一年 2.2GB **且永远不删**（红线 5）。
`page_snapshots` 只在 hash 变化时插入（§10.4 步骤 5），未变化的天零增长。

---

## 11. `web/search` 修复（`fetcher/exa.go`）

1. **删除 lookback 的默认值（毒药 exaDefaultLookbackDays）**，改默认关闭；`lookback_days`>0 时仍产出 `startPublishedDate`（显式逃生阀，手册不建议，§0.3、§5.3）
2. **新增 `include_domains` → `includeDomains`**（上限 1200 域名）
   - **绝不能与 `startPublishedDate` 并用**：实测两者并用返回 9/9 官方却全是边角料
     （`ben-bernanke`、`hard-questions`），**恰恰漏掉 `claude-sonnet-5` / `opus-4-8`**
     ——因为模型发布公告的 publishedDate 正好是 null
3. **保留 `contents.text=true`**：实测**免费搭售**（$0.007 带不带一分钱不差），
   别因为怕收费去掉它
4. **不用 `excludeDomains`**：实测是打地鼠（排掉 5 个农场后 `cryptobriefing.com` 立刻补位）
5. **`category` 传前自校验**：实测 Exa **不做校验**，传 `__invalid__` 静默忽略照常返回
   → 拼错一个字母过滤器就无声失效。合法值：
   `company / research paper / publication / news / personal site / financial report / people`
6. **潜伏 bug 记录（本次不修，见 §18 D6）**：`exa.go` 只看 `resp.StatusCode`，**从不看 `statuses[]`**
   → livecrawl 超时是 `HTTP 200 + results:[] + statuses[0].status="error"`，会伪装成"搜索返回 0 条"。
   当前未用 livecrawl 故潜伏。

---

## 12. promptguard 扩展与注入防护

### 12.1 威胁面的量变（实测）

| | 现状（rss/exa/xhs） | `web/page_watch` |
|---|---|---|
| 文本来源 | feed 作者写的 description | **整个页面的 DOM** |
| 攻击者 | 需控制 feed 内容 | 任何能改页面的人（含第三方 script/广告/评论区） |
| 含 HTML 标签 | **2/63**（rss，实测） | **100%** |
| 不可见字符 | **0/231**（实测） | 结构性存在 |

### 12.2 `promptguard` 新增

```go
// StripInvisible 剥除对人不可见、对模型是 token 的字符：
//   零宽（U+200B-200D）、BOM（U+FEFF）、双向控制符（U+202A-202E, U+2066-2069）、
//   **Unicode Tags 块（U+E0000–U+E007F）**，以及其余 Cf 类。
//
// 存量实测 0/231 命中 → **对现有内容是恒等变换**，可安全全局启用（不会改动既有指纹）。
//
// 这既是安全要求也是**正确性要求**：不可见字符若进入正文，
// 攻击者每轮插入一个随机零宽字符即可让 content_hash/simhash 每轮不同
// ——对 page_watch 就是**无限假变化 = 无限推送**（其身份含内容 hash）。
func StripInvisible(s string) string
```

### 12.3 【重写】**任何进入指纹的文本都必须是抽取后的文本**

> **初稿把地标钉在「finalize 之前」上，是错的**（对抗审查纠正，已复验）：
> - `dedup.ContentHash` 实测 = `sha256(item.Title + "\n" + item.URL)` —— **根本不含正文**。
>   初稿说"content_hash 会随 nonce 抖动"是错的。
> - **`page_watch` 的 `newHash = sha256(text)` 根本不经 `finalize`**。
>   ⇒ 实现者按初稿字面照做（"我确实放在 finalize 之前了"）**仍然可以把 newHash 算在裸 HTML 上**。
> **地标应钉在"指纹"上，不是钉在某个函数上。**

**规则**：HTML 剥离（含注释、`<script>`/`<style>`/`<template>`/`<noscript>`、
`display:none`/`aria-hidden` 子树）+ `StripInvisible` **必须先于任何指纹计算完成**。

**全部指纹点（穷举，新增指纹点必须登记到此清单）**：

| 指纹 | 位置 | 输入 | 约束 |
|---|---|---|---|
| `simhash` | `finalize`（`fetcher.go:342`） | `item.Title + " " + item.Content` | **Content 必须已剥离** |
| `content_hash` | `finalize`（`fetcher.go:340`） | `Title + "\n" + URL` | 不含正文，**本条与它无关** |
| **`newHash`** | `pagewatch`（§10.4 步骤 1） | `sha256(extracted_text)` | **text 必须是 §10.2 的压平结果，绝非裸 HTML** |

**为什么这是正确性要求而不只是安全要求**：正文若是裸 HTML，`simhash` / `newHash` 会随
属性顺序与 nonce 抖动（**实测**：Radix UI 每次渲染生成随机组件 ID，
`platform.claude.com/pricing` 5 个、`models/overview` 17 个）→ 每轮都判"变了"
→ **红线 5 禁止 TTL 清理 ⇒ content_items / page_snapshots 无限膨胀且不可回收**。

**做成"写不出来"而非"注释提醒"**（与 §7.2 同构的哲学）：

```go
// finalize 加护栏：正文含裸 HTML 标签即拒绝，不让它进指纹。
if htmlTagRe.MatchString(item.Content) {   // `<[a-zA-Z/!]`
    slog.Warn("fetcher: 正文含裸 HTML，抽取未在指纹之前完成（契约 §12.3），跳过该条", ...)
    return false
}
```

> ⚠️ **护栏会改变 `web/feed` 的现状**：实测 2/63 条 RSS 内容含裸 HTML 标签。
> 上线护栏前必须先落地 §12.3 的 feed 剥离（否则这 2 类条目会被丢弃而非剥离）。
> **顺序：先剥离，再护栏。** §16 有"干净文本原样通过"的黄金用例守着剥离是恒等的。

**顺带修一个既有小洞**：`web/feed` 的 content 也过 HTML 剥离
（实测 2/63 条 RSS 内容含裸 HTML 标签直送 LLM prompt）。
黄金用例守着"干净文本原样通过"（OpenAI feed 实测就是干净文本，剥离应为恒等）。

### 12.4 Boss 的「不让 agent 写脚本」

**现状已满足**：agent 只填 `add_source` 的 JSON schema，抓取是固定 Go 实现。
**本契约的边界**：`page_watch` config 允许 **CSS selector**（参数化抽取，声明式），
**禁 XPath / JS-eval / 正则回溯**——图灵完备等于写脚本。

### 12.5 与 M5 §17 的衔接（**不假装解决了**）

M5 §17 记录的取舍原样成立：

> 「注入防护是 prompt 级非硬隔离（定界+消毒+输出面收窄三层）；爆破半径限于画像文本与单条分数。」

**本次的变化是风险等级，不是防护等级**：`page_watch` 把攻击者完全可控的页面正文以
**100% 比例**送进 scorer，爆破半径需重算。三轴模型**挡不住这个**——
它是内容来源性质的变化，不是架构能消除的。**诚实记录，不粉饰。**

---

## 13. agent 工具面与 api

### 13.1 `add_source` schema

```jsonc
{
  "type": "object",
  "properties": {
    "platform":   {"type":"string","enum":["web","x","xhs"],
                   "description":"来源平台：web=开放网页；x=X/Twitter；xhs=小红书"},
    "capability": {"type":"string","enum":["feed","search","user_posts","page_watch"],
                   "description":"要这个平台的什么能力。注意：X 仅支持 user_posts（官号时间线）；不支持 search（上游 Latest 排序不可靠，2026-07-16 实测）"},
    "params":     {"type":"object","description":"随 platform+capability 而定：web/feed→{url}；web/search→{query,include_domains?}；web/page_watch→{url}；x/user_posts→{screen_name}；xhs/search→{keyword}"},
    "title":      {"type":"string"}
  },
  "required": ["platform","capability","params"]
}
```

**`description` 里写 Unavailable 理由**是刻意的：`tikhub.go:42-43` 那种 const 注释
**只对读代码的人生效，对模型不生效**。模型真会读 description。

> **【硬约束】agent 面只有新 schema，legacy 垫片不得进 agent**（对抗审查指出的风险）：
> §13.2 的 `BuildLegacy` **只服务 HTTP API**（为仓库外前端 vane-web 的兼容窗口）。
> **绝不能同时把 legacy `type` 暴露给 agent 的 `add_source`**——那等于在 DeepSeek V4
> 结构化输出（**红线 1 的地盘**）上给模型**两条重叠的表达路径**，模型会选描述更全的那条。
> 后果是 Boss 依然听见「已为你添加 tikhub_xhs 源」——**Boss 的唯一诉求当场落空**。
>
> 实测现状（`agent/tools.go:130`）：enum 是 `rss / exa / tikhub_xhs`，`Description()` 原文
> 「支持三种类型：rss（提供 url）、exa（提供 query 搜索词）、tikhub_xhs（提供 keyword 小红书关键词）」
> ——**这是 Boss 加源的主路径（飞书 agent，M4 确认卡）**。
> P1 必须把它整体换成上面的新 schema，**不留 legacy 分支**。§17.3 ① 真人验这一条。

**新增读工具 `list_capabilities`**（无参）：吐 registry 的可读投影，含 Unavailable 条目与 Reason。
→ 这是"用户不用关注实现，只关注这个平台能给我什么功能"在 agent 面的直接落地。

### 13.2 api 与**跨仓库兼容垫片**

**已复验**：仓库外前端 `vane-web` 硬编码了 type 字面量——
`src/pages/Sources.tsx:14` `type SrcType = "rss"|"exa"|"tikhub_xhs"`、
`src/api.ts:60` 的可辨识联合、`Sources.tsx:66` `if (s.type === "tikhub_xhs")`。
VPS 打包产物复验含 4 处 ``===`rss`` + 4 处 `tikhub_xhs`。

**跨仓库错位窗口真实存在**（与后端内部相反）：后端 push main → CI 自动部署 VPS；
前端走 CDN 独立发布 → 中间 dashboard 是坏的。
`docs/git-workflow.md:45` 正有对应约定（「前后端版本独立演进，不强制同步；
**API 破坏性变更时后端 minor +1 并在 Release notes 标注需要的前端最低版本**」）。

**已复验的关键区分：改 url 安全，改 type 会破**——
`Sources.tsx:51` 的 `syntheticParam` 是 `url.split("?")[1]` + `URLSearchParams`，
**与 scheme 无关** → 幂等键换前缀对前端零影响。**垫片只需管 `type`。**

```go
// GET /api/subscriptions 的响应体在过渡期**同时**含新旧字段：
//   platform / capability  ← 新，vane-web 迁移后用
//   type                   ← legacy 派生别名，**由 platform+capability 算出**，DB 不存
//                             web/feed→"rss"、web/search→"exa"、xhs/search→"tikhub_xhs"
//                             新组合（x/user_posts、web/page_watch）→ "<platform>_<capability>"
//                             旧前端 typeMeta 对未知 type 返回 null → 降级成链接渲染
//                             （难看但不崩），待 vane-web 跟进
//
// POST /api/subscriptions 同时接受：
//   新体 {platform, capability, params, title}
//   旧体 {type, url|query|keyword, category, title} → sourcespec.BuildLegacy
//
// **删除里程碑写进 CHANGELOG**：vane-web 切新字段后的下一个后端 minor 删垫片。
// 不写死删除条件的兼容层就是永久债。
```

---

## 14. config 与装配

`config.FetchConfig` **零新增键**（TikHub Twitter 复用 `TikhubAPIKey`——同一个供应商同一把 key）。
`cmd/server/main.go`：`fetcher.NewMulti(cfg.Fetch, st, st)` —— 第二个 `st` 是 `SnapshotStore`。

---

## 15. 安全红线

- **SSRF**：`page_watch` 必须复用 `Fetcher` 的 `LookupIP` + `Dialer.Control` + `blockedCIDRs` + `noRedirect`。
- **凭证外带**：`noRedirect` 对 TikHub Twitter 同样必须（`Authorization: Bearer` 会被 30x 原样带走）。
- **抽取在 finalize 之前**（§12.3）——安全 + 正确性双要求。
- **合理性闸门**（§10.5）：静默返回空是最坏的失败模式。
- **LLM 门 fail-open**（§10.6）——静默吞掉是红线 1 的翻版。
- **错误卫生**（红线 3）：TikHub / Exa 的原始 error 链不得进模型上下文，只落 `AppError.Message`。
- **幂等键命名空间不相交**（§5.2）——否则 `ON CONFLICT (url) DO UPDATE` 会静默劫持他人的源。
- 全部 SQL 参数化；新增依赖仅 `goquery`（+ `sergi/go-diff`），**不引 chromedp / trafilatura / readability**。

---

## 16. 测试要求

- **sourcespec**：每个 (platform, capability) 的必填校验；`IdemKey` **黄金字符串**用例
  ——`Build()` 算出的键必须与 008 回填的**逐字节相等**（含 exa 的 `q` 在 `category` 之前的**顺序**）；
  未注册组合与 Unavailable 组合返回带 Reason 的文案；`BuildLegacy` 三个旧 type 的等价性。
- **fetcher/identity**：四种 (platform, capability) 的键构造；**存量等价性黄金用例**
  （给定 M6 前的 item + 新制 Source，算出的键与 007 规则逐字节相同）；空 key 跳过；Kind 空跳过。
- **fetcher/x**：envelope 解析（**无 `data.success`**）；`media`/`entities` 空数组与对象**双形态**；
  `views` string；`created_at` 三种时间表示；**转推拆包**（身份取 `retweeted_tweet.tweet_id`、
  正文取全文而非 140 截断）；**RT 的 `author.screen_name`=="OpenAI" 仍被正确识别为转推**（定向用例）；
  `data.pinned` 被忽略；400 不计费路径；
  🔴 **timeline 乱序时 lookback 不得 early-break**（突变体 M11 逃逸）——
  **初稿指定的 fixture（实测的「#0 ORIG 17:30:00 / #1 REPLY 17:30:01」）抓不住这个突变体**：
  那两条**都在窗口内**，early-break 根本不会触发。能抓住的 fixture 必须让
  **超期项的下标 < 新鲜项的下标**：

  ```go
  // 真实对应场景（§9.6 的排序规则「thread 根倒序、自回复紧跟其根」直接蕴含它）：
  // @OpenAI 今天给 8 天前自建的 thread 加了一楼 → 那条新鲜的自回复排在已超期的根之后。
  // §9.4 实测 19 条里 7 条正是自建 thread 的楼层，且 include_replies 默认 true。
  func TestX_LookbackNoEarlyBreak(t *testing.T) {
      // 钉死时钟（否则 fixture 会随真实时间流逝而无故变红——fetcher.go 的 now 字段就是为此）
      now := mustParse("Thu Jul 23 12:00:00 +0000 2026")   // lookback_days=7 → cutoff=07-16
      timeline := []tweet{
          {id: "1", createdAt: "Wed Jul 15 10:00:00 +0000 2026"},  // 超期（thread 根）
          {id: "2", createdAt: "Thu Jul 23 09:00:00 +0000 2026"},  // 新鲜（今天加的那楼）
      }
      got := applyLookback(timeline, now, 7)
      // early-break 实现会在 #0 就 break → got == []，#1 被吞
      require.Equal(t, []string{"2"}, ids(got))   // ← 保留集**不是**前缀，early-break 必红
  }
  ```
- **fetcher/pagewatch**：**Baseline 语义七态**（下表逐条，突变体逃逸过的用 🔴 标）：

  | 态 | 断言 | 抓哪个突变体 |
  |---|---|---|
  | 首轮 | 建基线（`verdict=baseline`），**产出 0 条** | — |
  | 无变化（**nonce fixture**：两次渲染正文逐字相同、仅 5 处 `radix-_R_<随机>_` 不同） | 抽取文本逐字节相等 ∧ 产出 0 条 ∧ **`PutSnapshot` 未被调用** | **M8** |
  | 变化 + 门判重要 | 产出 1 条，key=`#H1->H2`，`verdict=pending` | — |
  | 🔴 **变化 + 门判不重要** | `verdict=suppressed`，**base 前进到 H2**（下轮 diff 不得对着 H1） | §10.4/§10.6 矛盾 |
  | 🔴 **回退后稳态** | 序列 `H1(基线)→H2→H3→H2(回退)→H2(不变)`，三条断言见下 | **M10** |
  | A/B 抖动第 3 轮 | key 撞第 1 轮 → 静音 | — |

  🔴 **「回退后稳态」的三条断言**（M10 的逃逸分析给的，**A1 刻意断言不变量而非格式**）：
  - **A1**：`R4[0].CanonicalKey != R2[0].CanonicalKey` ——「**两次到达同一内容 hash，身份必须不同**」。
    `#newHash`-only 在构造上不可能满足。**比黄金字符串强**：实现者改键格式也逃不掉。
  - **A2**：回退轮 `PutSnapshot` **必须新增一行**（快照行数 3→4）。
  - **A3**：第 5 轮页面仍 H2 → 产出 **0 条** 且 `page_diff` 门调用 **0 次**（fake 计数器）。

  > **🔴 前置硬要求**：fake `SnapshotStore` **必须真实模拟 `UNIQUE(source_id, canonical_key)`
  > 的 ON CONFLICT DO NOTHING**。用 map 直接覆写的 fake 会**把 bug 一起藏掉**。

  > **🔴 「产出后崩溃重试自愈」不在本层测——那是恒真的复印件**（终审纠正）：
  > fetcher 层的 fake `SnapshotStore` 会实现你写进它的任何 Baseline 语义，
  > 用它测 reportedness **等于用实现测实现**。**该断言必须在 store 层跑真 DB**（下方 store 条目）。
  > 本层的四态因此退化为三态；分工照抄现有 `fakeSeen` 的注释写法写清楚，
  > 让「忘了在 store 层测」这件事更难发生。

  🔴 **合理性闸门必须有「塌缩独占」用例**（突变体 M7 逃逸）：现有两条（非 2xx / 行数不足）
  会**遮蔽**塌缩闸门，使它形同虚设。造一个**只有塌缩能抓**的 fixture：
  基线 60 行/302 价 → 本轮 httptest 返回 **`http.StatusOK` + 20 行/20 价**
  ⇒ 2xx 过、`min_rows(5)` 过、`min_prices(3)` 过 ⇒ **只有「相对基线塌缩 > 50%」能响**。
  （真实对应场景：docs 页把部分 `<table>` 改成 div、selector 失配后仍匹配到子集。）

  其余：challenge 页 9,929B → **报错而非变化**；`watchKey` 定长 hash 的碰撞安全；
  🔴 **「更长的正文赢」对 change 无害**（同键重试时 `char_length` 相等 ⇒ UPDATE 空转，
  钉死这一点，防将来有人改那条谓词时静默改写 change 正文）。

- **fetcher/exa**：**断言请求体不含 `startPublishedDate`**（定向回归用例，防 §0.3 复发）；
  `includeDomains` 透传且**入幂等键**；`provider_hints.category` 非法值被**本地**拒绝
  （Exa 自己不校验）。
- **workflow/Dedup**：**KindChange 豁免近似去重**（定向用例：用 §1.1 实测的
  **hamming=0 反向 diff 对** —— `KindArticle` 时第二条被吞、`KindChange` 时两条都留。
  用 hamming=0 而非 ≈1 的样本：它是**必然命中**，用例不会因 simhash 实现微调而变绿）；
  change 不进 `batchSeen`。
- 🔴 **端到端往返用例（本契约头号致命缺陷的守卫，§3.3.1）**：
  `UpsertContentItem(Kind=change)` → `ListUnpushedByUser` → **断言读回的 `item.Kind == KindChange`**。
  DB 门控。**这一条抓的是"契约漏掉了 store 层 SQL"这件事本身**——它是初稿真实犯过的错。
- **scorer**：`KindArticle` 的 system prompt 与基础 user prompt 保持 M5 **逐字节一致**；P1c 未命中时
  整体请求仍逐字一致，命中时会专用消毒外部内容伪造的任务手册前缀并追加尾部任务块；
  `KindChange` 走新 prompt。
- **promptguard**：`StripInvisible` 覆盖四类字符 + **Unicode Tags 块**；对干净文本恒等。
- **store（DB 门控，CI 已带 postgres）**：🔴 **`Baseline` 的判别性 fixture ——
  这是杀死突变体 M4 的唯一一条断言，也是「产出后崩溃重试自愈」的真正测试位置**：

  ```go
  // TestBaseline_跳过比已报告快照更新的未报告快照
  // fixture（同一 source S，三行快照）：
  //   id=1 key="watch://u#->H1"   verdict=baseline    —— 首轮基线
  //   id=2 key="watch://u#H1->H2" verdict=pending  + **插 content_items(canonical_key=该键)**
  //   id=3 key="watch://u#H2->H3" verdict=pending  + **不插 content_item**（崩溃窗口）
  got, _ := st.Baseline(ctx, S)
  // 必须是 id=2（最近的**已 settled** 快照）；
  // 突变体的 `ORDER BY id DESC LIMIT 1` 会返回 id=3 → 红。
  require.Equal(t, "watch://u#H1->H2", got.CanonicalKey)
  ```

  🔴 **`verdict=suppressed` 也算 settled**（同 fixture，把 id=3 的 verdict 改成 suppressed
  → `Baseline` 必须返回 id=3）——这条守着 §10.4 与 §10.6 不再矛盾。

  🔴 **无 settled 快照时返回 nil**（≥2 行 fixture，全 `pending` 且无 content_item）
  ——单行 fixture 证明不了方向，必须 ≥2 条。

  其余：`PutSnapshot` ON CONFLICT 幂等；`SettleSnapshot` 只改 verdict 不动别的。
- **migration 008**：在**有存量的库**上跑完后 —— canonical_key **231 条逐字节未变**（快照比对）；
  9 行 source 的 platform/capability 正确且 **6 行 disabled 同样被迁**；
  幂等键前缀替换后 `Build()` 复算一致；`kind` 全为 `article`；Down 后旧代码能跑（DB 门控）。

---

## 17. Gate 验证清单

### 17.1 服务端探针（部署前存基线，部署后当天与次日复跑）

> **对抗审查的突变体实验证明初稿有 3 条探针是装饰品**（其中 2 条**结构上恒绿**、
> 1 条**在 bug 活着时读数最高**）。以下是修正版，每条都注明"它能抓住什么"。

1. **存量零重写**：`SELECT count(*) FROM content_items WHERE canonical_key <> <迁移前快照>` = **0**。
   非 0 → 立即回滚。
2. **无重复源**：`SELECT url, count(*) FROM sources GROUP BY 1 HAVING count(*)>1` 为空；
   且 `SELECT count(*) FROM sources` 仍为 9（+ 新加的）。
3. **幂等键无供应商名**：`SELECT count(*) FROM sources WHERE url LIKE 'exa://%' OR url LIKE 'tikhub://%'` = **0**。
   （008 的 `RAISE EXCEPTION` 已在迁移期挡一道；此处是部署后的复核。）

4. **【重写】§0.3 回归探针 —— 直接查因，不查果**

   > **初稿的「官方域名占比 ≥ 80%」是装饰品，且方向是反的**：§11 实测「毒药 + `includeDomains`
   > 并用」返回 **9/9 = 100% 官方**——**bug 活着时这条探针读数最高**，而它恰恰漏掉了
   > `claude-sonnet-5` / `opus-4-8` 这些真正的模型发布。**探针在事故最严重时最绿。**

   改为查**因**：Exa 的 `publishedDate` 是从 HTML 猜的，**最权威的官方页恰恰猜不出**
   （实测 `openai.com` 8 个官方页全为 None）。若 `startPublishedDate` 活着，
   这些 null-date 的条目会被**全部过滤掉**：

   ```sql
   -- 期望 > 0。= 0 ⇒ startPublishedDate 复活了（或 Exa 改了行为，同样要查）
   SELECT count(*) FROM content_items ci
   JOIN sources s ON s.id = ci.source_id
   WHERE s.platform = 'web' AND s.capability = 'search'
     AND ci.published_at IS NULL;
   ```
   配合 §16 的代码级用例（断言请求体不含 `startPublishedDate`）双保险。

5. **Kind 正确**：`SELECT kind, count(*) FROM content_items GROUP BY 1` —— 存量 231 条全 `article`；
   page_watch 上线后 `change` 只来自 page_watch 源。
   **这条只证明写路径通了，不证明读路径通了** → 看探针 ⑥。

6. **【重写】change 活着走完了 pipeline —— 查 deliveries，不查 content_items**

   > **初稿的「content_items 里 change 行数 = 变化次数」结构上恒绿**（突变体 M3 逃逸）：
   > 那些行是 **Dedup 之前**由 Fetch 写入的，而 **Dedup 只过滤内存切片、不删行**。
   > 它测不出它以之命名的那个洞。

   ```sql
   -- change 必须能走到 deliveries（Dedup + Score + Select 之后才有 delivery 行）
   -- 期望：page_watch 源有 N 次真实变化 ⇒ 这里 = N。= 0 或 = 1 ⇒ §1.1 的洞复活。
   SELECT count(*) FROM deliveries d
   JOIN content_items ci ON ci.id = d.content_item_id
   WHERE ci.kind = 'change';
   ```
   **同时**（§3.3.1 的可执行证明——kind 有没有活着走完 DB 往返）：
   ```sql
   -- 期望 = 0：page_watch 源产出的内容，其 kind 必须真的是 'change' 而不是 DEFAULT 'article'
   SELECT count(*) FROM content_items ci
   JOIN sources s ON s.id = ci.source_id
   WHERE s.capability = 'page_watch' AND ci.kind <> 'change';
   ```

7. **成本**：日成本环比涨幅 < $0.01（M5 红线沿用）。X 三个官号一轮应 = $0.003。
8. **抓取失败可见**：`SELECT id, fail_count FROM sources WHERE fail_count > 0` —— challenge/改版
   应表现为 fail_count 上涨，**不是**静默的假变化。

9. **【新增】§12.3 的可执行证明 —— 抽取必须在 finalize 之前**

   > 突变体 M8 逃逸：把抽取挪到 finalize 之后**保留了全部用户可见行为**
   > （LLM 看到的、卡片显示的仍是干净文本）→ **每一条行为断言都绿**。
   > 错的只有指纹，而指纹的错是静默的（simhash 算在裸 HTML 上 → Radix 随机 ID
   > 让同一页每轮"变了" → 红线 5 下 content_items 无限膨胀且不可回收）。
   > **唯一能抓住它的是查存储，不是查行为。**

   ```sql
   -- 期望 = 0。非 0 ⇒ 抽取跑在了指纹/存储之后，§12.3 被违反 → 立即回滚
   -- 用正则而非 LIKE '%<%'：压平文本里可能有合法的 "<"（如 "<1ms"），
   -- `<[a-zA-Z/!]` 才是标签的形态。
   SELECT count(*) FROM page_snapshots WHERE extracted_text ~ '<[a-zA-Z/!]';
   SELECT count(*) FROM content_items WHERE content ~ '<[a-zA-Z/!]' AND kind = 'change';
   ```

   **配套探针「静止页零增长」**（次日复跑；页面确未真实变化时）：
   ```sql
   -- 期望 new_snaps = 0。≥1 且人工比对抽取文本逐字相同 ⇒ 指纹算在了裸 HTML 上
   SELECT s.id, count(ps.id) AS new_snaps
   FROM sources s JOIN page_snapshots ps ON ps.source_id = s.id
   WHERE s.capability = 'page_watch' AND ps.id > <部署后基线快照 id>
   GROUP BY 1;
   ```

10. **【新增】快照存的是压平文本不是裸 HTML**（红线 5 的成本护栏，§10.7）

    ```sql
    -- 期望：远小于 100KB。压平行实测 ~26KB/页；裸 HTML 是 582KB-1.4MB。
    SELECT max(length(extracted_text)), avg(length(extracted_text)) FROM page_snapshots;
    ```

11. **【新增】page_watch 楔死告警 —— 门跑了但一条都没产出**（§10.4 自愈失效的唯一线上信号）

    ```sql
    -- 断言 NOT (gate_calls_3d >= 3 AND emitted_3d = 0)
    -- 门连跑 3 天却零产出 ⇒ base 卡住了（Baseline 取了未 settled 的快照，或转移键退化）。
    -- 依赖 §10.6 的 ref_type='source' / ref_id=<source_id> 记账。
    SELECT l.ref_id AS source_id,
           count(*) FILTER (WHERE l.created_at > now() - interval '3 days') AS gate_calls_3d,
           (SELECT count(*) FROM content_items ci
             WHERE ci.source_id = l.ref_id AND ci.kind = 'change'
               AND ci.created_at > now() - interval '3 days')            AS emitted_3d
    FROM llm_calls l
    WHERE l.span_name = 'page_diff' AND l.ref_type = 'source'
    GROUP BY l.ref_id;
    ```

12. **【新增】已报告的 change 键必须首尾相接成链**（断链 ⇒ 有一次变化永久丢失）

    ```sql
    -- 期望 0 行。转移键 watch://<url>#<prev>-><new> 天然构成链：
    -- 每条 change 的 prev_hash 必须是前一条 change 的 new_hash，或是首轮基线。
    WITH ch AS (
      SELECT cs.source_id,
             split_part(split_part(ci.canonical_key, '#', 2), '->', 1) AS prev_hash,
             split_part(split_part(ci.canonical_key, '#', 2), '->', 2) AS new_hash
      FROM content_items ci
      JOIN content_sources cs ON cs.content_item_id = ci.id
      WHERE ci.kind = 'change'
    )
    SELECT * FROM ch a
    WHERE NOT EXISTS (SELECT 1 FROM ch b
                      WHERE b.source_id = a.source_id AND b.new_hash = a.prev_hash)
      AND NOT EXISTS (SELECT 1 FROM page_snapshots ps
                      WHERE ps.source_id = a.source_id
                        AND ps.canonical_key LIKE '%#->' || a.prev_hash);  -- 首轮基线
    ```

### 17.2 供应商可替换性——**这条能真验，不是纸面承诺**

实测发现 `(x, user_posts)` **至少有两个可用供应商**：TikHub（$0.001/次）与
`syndication.twitter.com/srv/timeline-profile/screen-name/<handle>`（**免鉴权**，
实测 200 + `__NEXT_DATA__` 解析出 24 条结构化推文，含 `id_str`/`full_text`/`created_at`）。
两者**身份空间相同**（都是 tweet id）。

> **Gate ⑨（度量已修正）**：用同一个 handle 分别跑两个 provider，断言 canonical_key 的**子集关系**：
>
> ```
> 较小的键集合 ⊆ 较大的键集合
> ```
>
> **初稿的「交并比 ≥ 0.9」是个注定失败的指标**（对抗审查用事实基准自己的数字算出来的）：
> AnthropicAI 实测 TikHub 返回 **13 条**、syndication 返回 **24 条**
> → 交并比**上界 = 13/24 ≈ 0.54** → **正确的实现也会报红** → 下一步必然是调阈值 → **调成恒绿**。
> 一个"永远不可能过"的 Gate 与"永远绿"的 Gate 一样没用，且更危险（它教会人忽略红灯）。
>
> 子集关系才是 I1 的正确表述：**两个 provider 对同一条推文必须算出同一个键**。
> 条数差异来自时间窗/分页，不是身份不一致——子集断言对条数差异免疫，对身份漂移零容忍。
>
> **前置**：需先补一条 syndication 侧的实测（其 `id_str` 与 TikHub 的 `tweet_id` 是否同一空间、
> 转推在 syndication 里的字段形态）。**未补此实测前，I1 仍是纸面承诺**（记录在 §18 遗留）。

### 17.3 真人实测清单（Boss 飞书操作）

① 对话说"订 OpenAI 的推特" → agent 走 `x/user_posts` 确认卡 → 确认后 `list_sources` 可见，
  **展示里不出现 tikhub 字样**；
② 对话说"X 上搜 GPT-5 的讨论" → agent **明确回答不支持并给出理由**（§2.2），
  **不得静默改用 Exa 搜**；
③ push_now → @AnthropicAI 的转推公告被**拆包**：卡片作者是 @claudeai（原作者）、正文是全文（非 140 截断）；
④ 同时订 @AnthropicAI 与 @claudeai → 同一条公告**只推一次**，`content_sources` 两行
  （**007 跨源归并首次真实触发**）；
⑤ 加 `web/page_watch` 盯 `platform.claude.com/docs/en/about-claude/pricing`
  → **首轮无推送**（建基线，agent 应主动说明）；
⑥ 人为改一次基线（或等真实变化）→ 收到卡片，正文含**具体哪个模型哪一列变了**
  （"gpt-5.6-sol 输出价 $30→$24"），不是一堆裸价格；
⑦ 把 watch 指向 `openai.com/api/pricing/`（会 403 challenge）→ **源 fail_count 上涨、
  不产生假变化推送**；
⑧ Anthropic 动态：`web/search` + `include_domains:["anthropic.com","claude.com"]`
  → push_now → **推的是 anthropic.com 官方文章，不再是 xix.ai / vallettasoftware.com**；
⑨ Gate ⑨（§17.2）跑通；
⑩ **vane-web 未更新时** dashboard 的 Sources 页仍能正常展示三个存量源、仍能加源（垫片生效）；
⑪ **配置覆盖必须可见**（§5.2 死结的真人验）：先订 `openai.com/news/rss.xml` +
  `categories:["Product"]`，再对 agent 说"我也想看 OpenAI 的政策动态" →
  agent 必须回报**「已更新既有源的配置（Product → Global Affairs）」**，
  **不得**说"已添加"（那意味着 Product 被静默抹掉了）；
⑫ **门判不重要后基准要前进**（§10.4 与 §10.6 的真人验）：盯一个有每轮变动 build id 的页面
  → 连续两天都不应收到推送，且**第三天真实变化时 diff 只含真实变化**、
  不含前两天累积的噪音（若含，说明基准卡在首轮基线上）；
⑬ **注入故障的真人验**（§17.3 ①-⑫ 全是 happy path，而 §10.4 整节只为故障路径存在）：
  在 page_watch 产出后、内容项入库前**人为杀进程** → 下一轮**必须重新产出同一个 key 的
  change 并送达卡片**，且 `SELECT count(*) FROM content_items WHERE canonical_key='watch://…#H1->H2'` = **1**。
  判「无变化」或该 key 计数为 0 ⇒ Baseline 取了未 settled 的快照；
⑭ **回退的真人验**（M10 的判据）：把页面改回**上一个已推送过的状态**（**不是基线态**
  ——基线态不产出条目，会假阴性）→ 必须收到「改回去了」的卡片，
  且**再下一轮不得再收到任何卡片**（后半句才是楔死的判据）。

全过 → 打 tag（版本号见 §19）。

---

## 18. 已知取舍与遗留

### 不变量（**这些是"供应商可替换"的真正承载者，不是类型系统**）

- **I1**：`canonical_key` 只依赖 **platform + 内容本身**，**绝不依赖 provider**。
  → Gate ⑨ 可执行验证。
- **I2**：`sources.config` 的**顶层**不出现供应商专属字段；供应商专属的一律进 `provider_hints`，
  并**接受它会随供应商失效**（§5.3）。
  【2026-07-18 增补】绑定能力（endpoint-binding-contract.md）把供应商专属信息（端点名、
  字段映射）留在**代码内模板**、config 只存平台语义参数——I2 对模板绑定源自动成立；
  二期 config.binding 落地时按该契约 §1.3 单独修订。这是**诚实的泄漏，不是消除的泄漏**——
  初稿的 I2 表述是「config 里不出现供应商专属字段」，而初稿自己的 schema 就把 Exa 的
  `category`/`type` 摆在顶层，**违反了自己的不变量**（对抗审查抓到）。
  （`sort_type` 是 xhs **平台**语义不是 TikHub 语义，留在顶层是对的。）
- **I3**：幂等键的两个 scheme 命名空间**不相交**（§5.2 规则 A），
  且**键蕴含全部改变抓取语义的参数**（§5.2 规则 B）。
- **I4**：**Kind 活着走完 DB 往返**（§3.3.1）——任何返回 `ContentItem` 的 store 方法都 SELECT kind。
  这条是被对抗审查从血里捞出来的，§16 有端到端用例守。

### 对照组的结论——**必须诚实记录**

对抗审查的对照组（任务是论证"不必重构"）的核心发现，本契约**采纳而非驳回**：

> **换供应商的代价现状已经是 0**：1 个文件 + 0 行 SQL + 0 条用户配置 + 0 条内容重生。
> 因为解耦的真正承载者是 **canonical_key 的供应商无关性 + config 的 JSONB 自由度**，
> **这两样现状已经有了**。**约束「供应商可替换」今天就已满足，只是没人写下来。**

→ **本次重构不能拿"换供应商"当卖点**——那是既得的。重构的真实收益只有两条：
  1. **Kind 这一轴**（§1.1）——没有它，`page_watch` 会被 simhash 静默吞掉。**这是刚需。**
  2. **`Unavailable` 条目的机器可读性**（§2.2）——让 agent 说得出"X 不支持关键词搜索"，
     而不是静默改用 Exa 去搜出传闻站。折算下来是**一个 agent description 字符串 + 一个 registry 列**。

  其余（枚举改名、正交化）是**表达力与可读性的改善，不是能力的增加**。
  Boss 应当知道自己在为什么买单。

#### 【必须记录】收益 1 的论据被本契约自己证伪了一半

初稿在此写过：「三轴模型让 Kind 有地方推导，**让"忘了做"这件事更难发生**」。

**对抗审查的三个怀疑者独立发现：这份契约自己就忘了做**——初稿只加了 Go 字段（§3.3）
与 DB 列（§4.2⑤），**完全漏掉了 store 层的三条 SQL**（§5.4.2），照初稿逐字实现出来
`Kind` 过不了 DB 往返、§8.1 的豁免恒不触发、page_watch 的变化依然被静默吞掉——
**即"这是刚需"的那条收益，在初稿里是不工作的**。反例就在文内。

**这不推翻收益 1（Kind 这一轴仍是刚需），但它推翻了那句自我表扬**：
- 三轴模型**不会**让人自动想起去改 `ListUnpushedByUser` 的 SELECT 列清单
- 真正让"忘了做"被抓住的，是 **§16 那条端到端往返用例 + §17.1 探针⑥的第二段 SQL**
  ——**是测试，不是抽象**

→ 修正后的表述：**三轴模型给 Kind 一个正确的位置；让"忘了做"被抓住的是测试。
  别把前者的功劳记在后者头上。**（这也是 §17 那三条装饰性探针的教训：
  **探针要能失败才叫探针**。）

### 被否决的分支

- **D1 只加枚举值不重构**（对照组方案）：在 Kind 这一轴上崩了（§1.1）。
  但它的 §7/§8 论证被采纳（上条）。
- **D2 全库 canonical_key 加命名空间前缀**：会让存量 231 条的键与运行时算出的键永不相等
  → 全库重新长出一份。§7.3 的碰撞分析显示当前三平台不碰撞，故**推迟到真有碰撞风险时**
  （与 007 同级的身份重构，需独立迁移与实测）。
- **D3 为 anthropic.com 写专用 RSC 解析器**：实测 `anthropic.com/news` 的 `self.__next_f`
  flight payload 含 **255 条 post**（比可见 DOM 多 20 倍）且带官方 `subjects` tag
  （`product`/`announcements`）——**最富的数据源**。**但它是站点专用适配器，正是本次要消灭的东西**，
  且 RSC flight 是 Next.js 内部格式、无版本承诺、换框架即碎。
  **§0.3 修好后 `web/search` + `include_domains` 实测 15/15 全官方，Anthropic 的缺口已被填上**
  → 不需要它。
- **D4 `web/sitemap_feed`**（sitemap.xml 新 URL → 抓正文）：实测 `anthropic.com/sitemap.xml`
  有 **497 个 loc / 243 个 `/news/` / 100% lastmod 覆盖**，是标准格式、免费、稳健。
  **但 §0.3 修好后已无必需性**。**记录为 M7+ 的通用能力候选**（"给没有 RSS 的站点做伪 feed"）。
  ⚠️ 若采用须记住实测坑：**`lastmod` ≠ 发布时间**（同一篇 `claude-for-teachers`：
  RSC `publishedOn`=07-14、sitemap `lastmod`=07-15、RSSHub `pubDate`=07-13 三源三值，
  且 lastmod 有 bulk 迁移尖峰）→ **可靠信号是"出现新 URL"而非 lastmod 排序**。
- **D5 url 归一化**（剥 tracking 参数）：实测 RSS 的 canonical_key **52/63 带 `?`**（全是 BBC 的
  `?at_medium=RSS&at_campaign=...`），Exa **0/39** → 007 契约声称的"Exa 与 RSS 同文可归并"
  **对 BBC 类 feed 实际不成立**（生产 `multi_source_items` 实测 = **0**，与此一致）。
  **本次不做**：它会改写存量 canonical_key，属 007 同级身份重构；
  与"平台能力重构"绑进一个 PR 会把两个独立风险捆在一起。**记录在案。**
- **D6 `exa.go` 的 `statuses[]` 未检查**：livecrawl 超时 = `HTTP 200 + results:[] + statuses[0].status="error"`
  会伪装成"搜索返回 0 条"。当前未用 livecrawl 故潜伏，**接 `/contents` 时必须先修**。
- **D7 chromedp / 无头浏览器**：实测目标 docs 页**全部 SSR**、裸 GET 可拿到 302-400 个价格
  → 纯属白烧内存与运维（chromedp 仍 v0.x、177 未关 issue、镜像 143MB、
  `#1627` 长跑内存增长、`#1441` 泄漏 60KB/s、`#1492` 僵尸进程且镜像不带 init）。
- **D8 `excludeDomains` / `startCrawlDate` / `findSimilar` / Exa Monitors**：均实测不可用或退化（§11）。

### 遗留

- **注入防护仍是 prompt 级非硬隔离**（M5 §17 原样成立），而 `page_watch` 把风险等级抬高了
  （攻击者可控正文 100% 比例）。**架构消除不了，诚实记录。**
- **`(x, user_posts)` 是否会返回"回复他人"的推文未验证**（样本 0 条）→ `include_replies` 默认 true
  有风险，Gate 须实测。
- **note tweet（X >1000 字符长文）未验证**：报文无 `note_tweet`/`full_text` 字段，最长只见 534。
  若存在长文截断，会是 M5 缺陷 1 的翻版。
- **TikHub Twitter 真实 QPS 未验证**（元数据称 10/s，同字段对 XHS 已被证伪）。
- **`blog.google` 系 feed 只有 20 条滚动窗口**（最早仅 6 天）→ 每日一抓，某天 >20 条即漏。
  且生产 id=8 订的 `blog.google/products/gemini/rss/` 实测**信噪比差**
  （"Here's how Gemini can help you avoid jetlag"）→ **建议换 `deepmind.google/blog/rss.xml`**
  （100 条，实测标题全是 `Introducing Gemini Omni` 这类模型发布）。**属配置调整，不入本 PR。**
- **【诚实修正】目标 ③ 在本契约里被缩成了「price 页」，「文档页」没有解**
  （对抗审查指出初稿用「四个目标页都有表格，够用」把这半个目标悄悄抹掉了）：
  - Boss 的原话是「price 页 / **官方文档 URL** 的变化」。
  - `<tr>` 压平只对**有 `<table>` 的页**有效。**散文型文档页（如 API 指南、模型卡正文）没有表格**
    → 落到朴素抽取 → §10.5 的 `min_rows` 闸门**每轮 CodeValidation** → `fail_count` 永远涨
    → **那不是"diff 可读性下降"，那是这个源根本跑不起来**。
  - **处置**：`min_rows` 对无表格页必须可关（设为 0 = 不启用行数闸门，只保留 HTTP + 塌缩两道）；
    但**可读 diff 的承诺仅限表格页**——散文页的 diff 是段落级的，卡片可读性未验证。
  - **本契约不解决散文文档页的 diff 可读性**。若 Boss 确实要盯散文文档，
    候选是「LLM 门直接读 diff 生成人话」（§10.6 已有该环节，只是未针对散文验证）
    → **留 P3 实测后再定，不在此承诺**。
- **第四个平台的碰撞分析**是人工纪律（§7.3），没有机器强制。
- **I1 在补上 syndication 侧实测前仍是纸面承诺**（§17.2）：已实测 syndication 返回
  `id_str`/`full_text`/`created_at`，但**未验证**它与 TikHub 的 `tweet_id` 是否同一空间、
  转推在它那里的字段形态。Gate ⑨ 落地前必须补这条实测。
- **`min_rows` 闸门与散文文档页不兼容**（见上条目标 ③ 修正）。

### 未来方向拍板（2026-07-17）：agent 代码执行沙箱 = microVM

**现状**：agent 无任何命令执行能力（全仓 `os/exec` 零命中，9 个 tool 全是参数化
结构化调用）——这是刻意设计（能力工具化，不让 agent 写脚本），也是当前最强的隔离：
攻击面为零。本契约的 lookup 能力（§2.3）延续此路线。

**拍板**：若未来出现"agent 需要动态执行代码"的真实需求（如客户要监控的内容形态
超出所有已注册能力），沙箱采用业内金标准 **microVM 一族**（Firecracker / Kata /
smolvm），形态为**每任务新建、用完即毁**（ephemeral，不做长驻复用，状态零残留）。
托管沙箱 SaaS（E2B 等，底层同为 Firecracker）作为不想自运维时的备选，届时按运维意愿定。

**可行性已实测**（2026-07-17）：ByteVirt VPS `/dev/kvm` 存在、`systemd-detect-virt`
返回 kvm——自建 microVM 的硬件条件具备，无需换机。

**边界**：只定方向，不含实现。触发条件是真实需求出现，本契约与 M6 排期均不包含它。


## 19. 分期与排期

### 19.0 【先做，与 M6 解耦】§0.3 热修

```sql
-- 零部署止血：Boss 每天都在被推传闻站
UPDATE sources SET config = config || '{"lookback_days":-1}'::jsonb WHERE id = 9;
```
代码侧的正解（删 `web/search` 的 lookback **默认** + 加 `include_domains`）随 P2 落地。
> 按 AGENTS.md「对抗审查按风险分级」，`fetcher/exa.go` 属核心抓取路径 → 该修改上全流程审查。

### 19.1 分期（**已按对抗审查的诘问重排**）

> **怀疑者的诘问，成立且必须回答**：「目标 ② 的解法（删 lookback 默认 + 加 include_domains）
> **一个文件、零 SQL、不需要任何 M6 架构**，却被排进依赖 P1 的 P2——**M6 对目标 ② 的贡献是 0**，
> 而 Boss 每天都在挨 §0.2 的事故。」
>
> **采纳**：把 `web/search` 的修复从 P2 提到 **P0，与 M6 完全解耦、独立发**。

| 期 | 内容 | 解决 Boss 的 | 依赖 | 与 M6 的关系 |
|---|---|---|---|---|
| **P0a** | §19.0 的 JSONB 热修 | 立刻止血 | — | **零** |
| **P0b** | `web/search` 修复（§11）：删 lookback 默认 + `include_domains` + `statuses[]` 检查 | **目标 ②**（含 Anthropic，实测 15/15 官方） | — | **零——不要等 M6** |
| **P1** | 三轴模型 + migration 008 + registry + `x/user_posts` + api 垫片 + **§5.4 store 层** | **目标 ①**（X 官号） | — | 核心 |
| **P2** | `web/feed` 的 `categories`（OpenAI 1038 条收窄到 Product/Research） | 目标 ② 的增强 | P1 | 弱 |
| **P3** | `web/page_watch`（§10） + **Kind 下游（§8）** + promptguard（§12） | **目标 ③**（价格页） | P1 | **强——Kind 是刚需** |
| **P4** | lookup 工具面（§2.3） | Boss 的「用户搜索」 | P1 | 中 |

> **P0b 用 M6 前的形态实现**（改 `exaSourceConfig` + `exa.go`），M6 落地时随 §5.3 迁移到
> `provider_hints` 形态。**多一次小改动，换 Boss 少挨几天传闻站——值。**
>
> **P3 的 Kind 下游改动（§8.1/§8.2/§5.4.2）跨 workflow/scorer/store/types 四包**，
> 按 AGENTS.md 属跨包契约变更 → **上全流程对抗审查**。P1 的 migration 同理（数据面入口）。
>
> **P1/P3 开工前必须先补齐 §5.4 与 §16 的往返用例**——它们是本契约在对抗审查中真实
> 漏掉过的东西，不是纸面要求。

### 19.2 里程碑命名问题（**需 Boss 拍板**）

- `docs/git-workflow.md:64` 定义 **M6 = 加固**；M5 §17 已把若干项挂在 M6 名下
  （token 预算统一、自然语言指代、reaction 反馈、误判撤销、负反馈 per-source 维度）。
- **本工作是功能不是加固**，且 **v0.5.0 尚未打 tag**（现有 tag 只到 v0.4.0，
  M5 Gate 正由并行会话固化探针中）。
- **建议**：要么把 M6 重定义为「信源插件化 + 加固」并同步更新 git-workflow.md 的里程碑表，
  要么本工作另立 M6.5 / M7 而把原 M6 顺延。**不要让两个 M6 并存。**

---

## 20. 并行会话边界（2026-07-16 核实）

- 并行分支 `feat/gate-probes-observability`：`store/observability.go` + `types/observability.go`
  + `/api/admin/observability` 端点；vane-web 侧已提交 `34b381f feat(dashboard): 管理员可观测性看板页`。
- **与本契约无文件重叠**，但两点需注意：
  1. **migration 号可能撞车** → 本迁移以落地时实际空号为准（§4）。
  2. **两边都会碰 api 包的路由注册，且两个工作流同时在改 vane-web**（对方 Observability 页、
     本方 Sources 页）→ 合入顺序上先到先得，后者 rebase。
- 顺带记录（**不在本 PR 改**）：当时的 `CLAUDE.md:19`（现为 `AGENTS.md`）写「LLM 调用明细…落 DB `llm_traces` 表」，
  **实际表名是 `llm_calls`**（001 建表、`store/llmcalls.go`、`types.LLMCall` 全是 llm_calls；
  当时全仓库 `llm_traces` 只在该协作入口一处）。属并行会话（可观测性）地盘，交由其修正。

---

## 21. 对抗审查记录（2026-07-16）

按 AGENTS.md「核心路径 / 跨包契约变更上全流程」执行。**记录在案是为了让后人知道
哪些结论是被攻击过的、哪些只是没人攻击**。

| 阶段 | 规模 | 关键产出 |
|---|---|---|
| 事实核实 | 4 路并行（含真实 TikHub/Exa key 实测） | 推翻简报 5 条假设；发现 §0.3 生产事故根因 |
| 并行设计 | 4 个对立视角 + 3 维评审团 × 4 | **对照组**（论证"不必重构"）找到 Kind 轴；**migration-first** 的评审挖出 simhash 反向 diff 碰撞 |
| 对抗审查 | 3 怀疑者 + 12 突变体 + 终审 | **4 条致命 + 2 条事实错误 + 3 条装饰性 Gate** |

**最有价值的两个发现都来自"输掉"的视角**：
- **对照组**（任务是论证不必重构）找到了 §1.1 的 Kind 轴——最小方案与朴素重构**都修不了**的崩点；
  同时证明「换供应商代价已经是 0」，**否掉了本次重构最诱人的那个卖点**（§18）。
- 评审团在挑 `migration-first` 方案的错时，翻出了 `dedup.tokenize` 吃掉 `+`/`-`、
  `Simhash` 是可交换累加 → **反向 diff 指纹 hamming = 0（必然）**，把 §1.1 的论证从
  "≈0-1（概率）"加强成"= 0（必然）"。

**对抗审查在本契约初稿上抓到的真错误**（全部已修，逐条亲自复验）：

| 严重度 | 问题 | 修在 |
|---|---|---|
| **致命** | Kind 过不了 DB 往返（三个怀疑者独立命中）——**"这是刚需"的那条收益在初稿里不工作** | §3.3.1 / §5.4.2 / §16 |
| **致命** | `finalize:347` 无条件覆盖 CanonicalKey → 每条 change 被丢弃且 fail_count 不涨 | §7.2(a) |
| **致命** | §10.4 与 §10.6 正面矛盾——「有快照无内容项」被赋予两个互斥语义 | §10.4 的 `verdict` 列 |
| **致命** | `include_domains` 不入幂等键 → 静默抹掉 §0.3 的解药 | §5.2 规则 B |
| **事实错误** | §0.4 张冠李戴（注释在 `fetcher.go` 不在 `exa.go:71`）+ 时间线倒错（注释比毒药**晚两天**） | §0.4 |
| **事实错误** | §13.2 引 `git-workflow.md:63`，实际在第 45 行 | §13.2 |
| **装饰品** | Gate ④ **在 bug 活着时读数最高**（毒药+includeDomains 实测 9/9=100%） | §17.1 ④ 改查因 |
| **装饰品** | Gate ⑥ **结构上恒绿**（查 content_items，而那些行在 Dedup 之前就已写入） | §17.1 ⑥ 改查 deliveries |
| **装饰品** | Gate ⑨ 的 Jaccard ≥0.9 **正确实现也过不了**（上界 0.54）→ 必被调成恒绿 | §17.2 改子集断言 |
| **自我表扬** | 「三轴让『忘了做』更难发生」——**而本契约自己就忘了做** | §18 |

**突变体实验：12 个突变体，5 个逃逸**（即初稿的测试/Gate 清单约四成是装饰品）。
逃逸的都已补上定向用例（§16 标 🔴）+ 5 条新探针（§17.1 ⑨⑩⑪⑫）。

最阴的是 **M8**：把抽取挪到 `finalize` 之后**保留了全部用户可见行为**
（LLM 看到的、卡片显示的仍是干净文本）→ 每条行为断言都绿
→ **只有查存储的 SQL 探针能抓住它**（§17.1 ⑨）。
它顺带证伪了初稿 §12.3 本身：地标钉在 `finalize` 上是错的
（`ContentHash` 实测不含正文；`page_watch` 的 `newHash` 根本不经 finalize），
**实现者按字面照做仍能把 newHash 算在裸 HTML 上** → §12.3 已重写为"钉在指纹上"。

**M4 还揭穿了一个更隐蔽的问题：测试放错了层就是恒真的复印件。**
初稿把「产出后崩溃重试自愈」放在 `fetcher/pagewatch` 的用例里，
而那一层的 fake `SnapshotStore` **会实现你写进它的任何 Baseline 语义**
——用它测 reportedness 等于用实现测实现。该断言已挪到 store 层跑真 DB（§16）。

> **给后人的两条方法论**：
> 1. 本次 5 个逃逸里有 3 个的共同点是——**契约用注释警告了，但没有会失败的测试**。
>    注释不是防线。突变体实验的判定标准就该是「有没有一条**会红**的用例」，
>    而不是「有没有人写过这件事很危险」。
> 2. **测试所在的层决定它能不能证伪**。凡是"被测对象的行为由 fake 定义"的地方，
>    那条用例就是装饰品——它只会重复你已经相信的东西。

> **给后人的一条方法论**：本次 5 个逃逸里有 3 个的共同点是——
> **契约用注释警告了，但没有会失败的测试**。注释不是防线。
> 突变体实验的判定标准就该是「有没有一条**会红**的用例」，
> 而不是「有没有人写过这件事很危险」。

---
