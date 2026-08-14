// 绑定引擎（endpoint-binding-contract.md）：把「TikHub 注册表端点 + 声明式字段映射」
// 执行成一个任务抓取目标执行器。它取代了逐端点手写 HTTP/信封/映射的 bespoke fetcher
// （原 tikhub.go / xhs_user.go / x.go，2026-07-18 删除），三份重复的请求装配与错误
// 分类收敛到 tikhubinvoke + 本文件各一份。
//
// 架构约束（契约 §1.1）：模板是**纯数据**——所有行为（过滤/下钻/回退链/补全/校验）
// 都是引擎级一次性实现，模板只能开关与传参，不允许出现函数。表达力不够的端点
// 就写 bespoke fetcher，不扩表达力（扩前先改契约）。
//
// 反静默三防线（契约 §3/§4，M6 §10.5「静默返回空是最坏失败」）：
//  1. 端点每轮 Lookup + 参数对照当前 Entry —— 注册表 re-gen 漂移显式失败，
//     不会「静默丢参数 → 上游用默认值 → 200 但数据错误」；
//  2. ItemsPath 解析不到 / 候选全灭 → CodeValidation 走 fail_count 告警链，不是空成功；
//  3. 声明了 Time 的字段解析失败逐条计数，全灭同样显式失败。
package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
)

// ────────── 模板 spec 类型（纯数据，契约 §1）──────────

// bindingSpec 描述一个 (platform, capability) 如何由注册表端点承载。
type bindingSpec struct {
	Endpoint string         // tikhubcatalog Entry.Name；每轮 Lookup，miss = 绑定失效
	Params   []bindingParam // 请求参数装配规格
	Envelope []envelopeCheck
	MsgPath  string // 业务失败时取上游 msg 的点路径（进错误信息，截断）

	ItemsPath  string       // 条目数组的点路径（自响应根）；解析不到 = 结构漂移
	ItemFilter *fieldEquals // 可选：条目级过滤（不满足即静默跳过——多态流的正常形态）
	ItemRoot   string       // 可选：过滤后下钻到子对象（search 的 {model_type, note}）
	Unwrap     []string     // 可选：尝试链，取到「含非空 ID 的对象」则替换条目（x 转推拆包）

	VerifyParam *verifyParam // 可选：条目字段与 config 参数对照，不符即丢（串号防御）
	Fields      bindingFields
	Kind        types.Kind
	OrderCheck  bool // probe + fetch 每轮断言 Time 序列非增（仅模板语义承诺时序时开，契约 §2.2/§4）
	// MaxContentBytes 正文字节上限（0=不截断）。xhs 族沿用 4000（成本护栏）；
	// x 不截断——旧 x.go 存推文全文，长文推被截是迁移回归（对抗审查 parity-4）。
	MaxContentBytes int
	// TimeoutSeconds 单端点调用超时覆盖（0=用 cfg/兜底 20s）。wechat_mp 官方文档明示
	// 「请将 timeout 设置为 30 秒；设置过小会造成已扣费但收不到响应」——按次计费面
	// 超时即白花钱，此字段是把上游的超时契约声明进模板（契约 §1 特性清单 2026-07-23 增补）。
	// 已知取舍：本值只能**收紧**外层 ctx、不能放宽——任务抓取受上层执行
	// 预算约束（agent probeBudget=25s），wechat_mp 的 probe 实际上限 25s，超时可能已扣费
	// （probe 话术已如实告知）；周期抓取（Fetch activity 120s）不受此限，30s 完整生效。
	TimeoutSeconds int

	Enrich *enrichSpec // 可选：付费详情补全
}

type bindingParam struct {
	Key        string // 请求参数名
	FromConfig string // 从 config 取值的键名；空表示常量参数
	Const      string // FromConfig 为空时的常量值（可为 ""，如 cursor）
	Default    string // config 值为空时的默认值
	Required   bool   // config 必填（缺失 → CodeValidation）
	OmitEmpty  bool   // 解析后为空则整个参数不发送
}

// envelopeCheck 对解码后的响应做业务层断言，比较按标量字符串化后进行
// （json.Number "200"、bool "true"——见 scalarString）。
type envelopeCheck struct{ Path, Want string }

// fieldEquals 条目过滤：默认按 Want 等值比较（字符串化口径见 scalarString）；
// WantAbsent=true 时改为「键不存在才保留」——用 resolvePath 的 ok 位判断，
// 不再靠「miss → ""」与 Want "" 撞相等（那无法区分「无键」与「键在但值为 0/false」）。
// 注意 WantAbsent 语义下显式 is_ad:0 也会被过滤——按上游当前形态（正常条目**无**该键）
// 刻意选定，夹具案例钉住；若上游改为全量显式 0/1，靠下方「全量过滤」告警可见。
type fieldEquals struct {
	Path       string
	Want       string
	WantAbsent bool
}

// verifyParam 把条目字段与 config 参数对照：字段非空且不等 → 丢弃该条
// （xhs/user_posts 串号防御的通用化；字段为空宽容保留，靠身份兜底）。
type verifyParam struct {
	Path      string
	ConfigKey string
}

type bindingFields struct {
	ID      []string // 回退链：依次取第一个非空
	Title   []string
	Content []string // 路径回退链；或单元素 "tmpl:…"（占位符为条目原始字段点路径）
	Author  []string // 路径回退链；"$键名" 表示取 config 参数（x 的 screen_name 兜底）

	URLPaths []string // 直接从条目取 URL（hot_list）；与 URLTemplate 二选一
	// URLTemplate 模板：{id} 取提取后 ID（PathEscape），{author} 取提取后 Author（原样）；
	// 其余 {点路径} 占位符从条目原始字段取值（renderTemplate，2026-07-23 增补——weibo 桌面
	// 链接需要 uid+mblogid 双段：https://weibo.com/{user.idstr}/{id}）。字段占位符不做
	// URL 转义，只用于 id 形态的字段（数字/base62），不用于自由文本。
	URLTemplate string
	URLQuery    []urlQueryParam

	Time       []string
	TimeFormat string // unix_s | unix_ms | ruby_date
}

// urlQueryParam 是 URL 的条件查询组：组内任一 FromField 解析为空 → 整组略去
// （xhs 直链的 xsec_token 语义：没有 token 时连 xsec_source 也不拼）。
// 按声明顺序手拼（不走 url.Values.Encode 的字典序），与旧 fetcher 逐字节一致。
type urlQueryParam struct {
	Key       string
	FromField string // 条目字段点路径
	Const     string // FromField 为空时的常量
}

const (
	tfUnixS    = "unix_s"
	tfUnixMS   = "unix_ms"
	tfRubyDate = "ruby_date" // Twitter 原生格式（第三种时间表示：RSS=RFC3339, XHS=Unix 秒）
)

// enrichSpec 付费详情补全（契约 §3.6）。全部行为引擎实现：触发条件、计费闸门
// （SeenChecker）、实例级限速、预算、串号校验、空值保旧。
type enrichSpec struct {
	Endpoint   string            // 详情端点 Entry.Name（同样每轮 Lookup + 参数校验）
	KeyParam   string            // 接收条目 ID 的请求参数名
	ItemParams map[string]string // 额外请求参数 ← 条目字段点路径（xsec_token）
	Envelope   []envelopeCheck
	MsgPath    string

	RespItemsPath string // 详情载荷数组路径；取第一个元素
	RespRoot      string // 元素内下钻（note_card）
	VerifyIDPath  string // 下钻后 ID 字段，必须与请求的条目 ID 相等（串号防御）
	DescPath      string // 下钻后正文字段；空值保旧（纯图笔记别把 60 字覆盖成空）

	MinRunes         int    // 触发阈值：正文 rune 数 ≥ 此值视为被截断（60 恰是上游截断信号）
	RequireItemField string // 条目必备字段（xsec_token 缺失稳吃 400 还计费）
	RateMs           int    // 实例级限速间隔（上游 1 req/s 实测超速 429，留 10% 余量）
	BudgetMs         int    // 单次 Fetch 的补全总预算（Fetch 活动 120s 由全部到期源共享）
}

// ────────── 模板注册表（契约 §7 首批三能力 + §6 三迁移能力）──────────

type bindingKey struct {
	P types.Platform
	C types.Capability
}

var xhsEnvelope = []envelopeCheck{{Path: "code", Want: "200"}, {Path: "data.success", Want: "true"}}

// bindingTemplatesV1 is the retained fetcher.binding/v1 response contract.
// Existing entries are immutable; a semantic change adds a V2 implementation
// and keeps this map for snapshots already sealed to V1.
var bindingTemplatesV1 = map[bindingKey]bindingSpec{
	// ── 迁移自 fetcher/tikhub.go（2026-07-14 实测契约见 git 史该文件头注）──
	// desc 上游截 60 rune，详情补全是把「证据不足→模型编造」从根上消灭的唯一手段，
	// 代价按次计费，故有 SeenChecker 闸门只为新笔记付费。
	{types.PlatformXHS, types.CapSearch}: {
		Endpoint: "xiaohongshu_app_v2_search_notes",
		Params: []bindingParam{
			{Key: "keyword", FromConfig: "keyword", Required: true},
			{Key: "page", Const: "1"}, // MVP 单页；周期抓取靠 canonical_key 增量去重
			// time_descending：推送要「最新动态」，相关性排序反复返回同批高赞旧帖。
			{Key: "sort_type", FromConfig: "sort_type", Default: "time_descending"},
			{Key: "note_type", FromConfig: "note_type", OmitEmpty: true},
		},
		Envelope:   xhsEnvelope,
		MsgPath:    "data.msg",
		ItemsPath:  "data.data.items",
		ItemFilter: &fieldEquals{Path: "model_type", Want: "note"}, // 广告位/用户卡片跳过
		ItemRoot:   "note",
		Fields: bindingFields{
			ID:      []string{"id"},
			Title:   []string{"title"},
			Content: []string{"desc"},
			Author:  []string{"user.nickname"},
			// xsec_token 是每次搜索新发的临时票据：URL 必须拼它（2024 起直链缺 token
			// 404），但它**不能当身份用**——身份是 note_id（ID 字段）。
			URLTemplate: "https://www.xiaohongshu.com/explore/{id}",
			URLQuery: []urlQueryParam{
				{Key: "xsec_token", FromField: "xsec_token"},
				{Key: "xsec_source", Const: "pc_search"},
			},
			Time:       []string{"timestamp"},
			TimeFormat: tfUnixS,
		},
		Kind:            types.KindArticle,
		MaxContentBytes: tikhubMaxDescBytes,
		Enrich: &enrichSpec{
			// 只用 web_v3 这一个详情端点：app_v2 get_video_note_detail 对图文笔记会
			// **静默返回别人的笔记**（200+success=true 照常计费，实测）——拿错比报错
			// 危险得多。即便如此仍要 VerifyIDPath 校验返回的 note_id。
			Endpoint:         "xiaohongshu_web_v3_fetch_note_detail",
			KeyParam:         "note_id",
			ItemParams:       map[string]string{"xsec_token": "xsec_token"},
			Envelope:         xhsEnvelope,
			MsgPath:          "data.msg",
			RespItemsPath:    "data.data.items",
			RespRoot:         "note_card",
			VerifyIDPath:     "note_id",
			DescPath:         "desc",
			MinRunes:         60, // 上游正截到 60 rune，「恰好 60」就是被截断的信号
			RequireItemField: "xsec_token",
			RateMs:           1100,
			BudgetMs:         40000,
		},
	},

	// ── 迁移自 fetcher/xhs_user.go（2026-07-17 实测契约见 git 史该文件头注）──
	// desc 上游截 100 rune 且拿不到 xsec_token：不做详情补全（web_v3 详情必填 token；
	// app_v2 详情有静默串号前科），URL 也不拼 token——note_id 是稳定引用，已知取舍。
	{types.PlatformXHS, types.CapUserPosts}: {
		Endpoint: "xiaohongshu_app_v2_get_user_posted_notes",
		Params: []bindingParam{
			{Key: "user_id", FromConfig: "user_id", Required: true},
			{Key: "cursor", Const: ""}, // 与旧实现逐字节同请求：显式空 cursor
		},
		Envelope:  xhsEnvelope,
		MsgPath:   "data.msg",
		ItemsPath: "data.data.notes",
		// 串号防御：user.userid 非空且 ≠ 所订 user_id 的笔记不是我们要的，
		// 丢弃而非安在错误的源上（上游有串号前科，廉价校验值回票价）。
		VerifyParam: &verifyParam{Path: "user.userid", ConfigKey: "user_id"},
		Fields: bindingFields{
			ID:          []string{"id"},
			Title:       []string{"title", "display_title"}, // 部分笔记 title 空、display_title 有值
			Content:     []string{"desc"},
			Author:      []string{"user.nickname"},
			URLTemplate: "https://www.xiaohongshu.com/explore/{id}",
			Time:        []string{"create_time"},
			TimeFormat:  tfUnixS,
		},
		Kind:            types.KindArticle,
		MaxContentBytes: tikhubMaxDescBytes,
	},

	// ── 迁移自 fetcher/x.go（响应结构契约 §9，多态字段实测）──
	{types.PlatformX, types.CapUserPosts}: {
		Endpoint: "twitter_web_fetch_user_post_tweet",
		Params: []bindingParam{
			{Key: "screen_name", FromConfig: "screen_name", Required: true},
		},
		Envelope:  []envelopeCheck{{Path: "code", Want: "200"}}, // twitter 外壳无 data.success
		ItemsPath: "data.timeline",
		// 转推拆包：转推不是新内容，被转推的那条才是。ExternalID 取被转推条的
		// tweet_id → 同一原创推经多号转发只落一行 content_item。
		Unwrap: []string{"retweeted_tweet", "retweeted"},
		Fields: bindingFields{
			ID:          []string{"tweet_id"},
			Title:       nil, // 推文无标题
			Content:     []string{"text"},
			Author:      []string{"author.screen_name", "$screen_name"}, // 作者缺失时回退所订账号
			URLTemplate: "https://x.com/{author}/status/{id}",
			Time:        []string{"created_at"},
			TimeFormat:  tfRubyDate,
		},
		Kind: types.KindArticle,
	},

	// ── 首批新能力（2026-07-18 实测准入，契约 §7）──
	{types.PlatformXHS, types.CapHotList}: {
		Endpoint:  "xiaohongshu_web_v3_fetch_hot_list",
		Params:    nil,                                          // 无参数：全局一份热榜
		Envelope:  []envelopeCheck{{Path: "code", Want: "200"}}, // web_v3 外壳无 data.success
		ItemsPath: "data.items",
		Fields: bindingFields{
			ID:    []string{"item_id"}, // 十进制数字串，与 24-hex note_id 形状不相交（契约 §3.4）
			Title: []string{"title"},
			// 无正文：用热度元数据合成，给打分器一点上下文（薄正文风险见契约 §7.1）。
			Content:  []string{"tmpl:{title}（小红书热榜第 {rank} 位，热度 {hot}，趋势 {trend}）"},
			URLPaths: []string{"url"}, // 上游给的搜索落地页链接
			// 无 Time：data.updated_at 实测坏值（滞后 date 字段 41 天），禁用。
		},
		Kind:            types.KindArticle,
		MaxContentBytes: tikhubMaxDescBytes,
	},
	{types.PlatformXHS, types.CapTopicFeed}: {
		Endpoint: "xiaohongshu_app_v2_get_topic_feed",
		Params: []bindingParam{
			{Key: "page_id", FromConfig: "page_id", Required: true},
			{Key: "sort", Const: "time"}, // 追新唯一正确排序；能力语义的一部分，不进 IdemKey
		},
		Envelope:  xhsEnvelope,
		MsgPath:   "data.msg",
		ItemsPath: "data.data.items",
		Fields: bindingFields{
			ID:          []string{"id"},
			Title:       []string{"title", "display_title"},
			Content:     []string{"desc"},
			Author:      []string{"user.nickname"},
			URLTemplate: "https://www.xiaohongshu.com/explore/{id}",
			Time:        []string{"create_time"},
			TimeFormat:  tfUnixMS, // 顶层 create_time 是毫秒（note_time.create_time 才是秒）
		},
		Kind:            types.KindArticle,
		OrderCheck:      true, // sort=time 承诺降序，probe+每轮 fetch 均断言（2026-07-18 实测严格降序）
		MaxContentBytes: tikhubMaxDescBytes,
	},
	{types.PlatformXHS, types.CapFavedNotes}: {
		Endpoint: "xiaohongshu_app_v2_get_user_faved_notes",
		Params: []bindingParam{
			{Key: "user_id", FromConfig: "user_id", Required: true},
		},
		Envelope:  xhsEnvelope,
		MsgPath:   "data.msg",
		ItemsPath: "data.data.notes",
		Fields: bindingFields{
			ID:          []string{"id"},
			Title:       []string{"title", "display_title"},
			Content:     []string{"desc"},
			Author:      []string{"user.nickname"},
			URLTemplate: "https://www.xiaohongshu.com/explore/{id}",
			Time:        []string{"create_time"},
			TimeFormat:  tfUnixS,
			// OrderCheck 关：收藏序≠创建序，实测非单调（契约 §7），检了必误拒。
		},
		Kind:            types.KindArticle,
		MaxContentBytes: tikhubMaxDescBytes,
	},

	// ── 微博 + 公众号（2026-07-23 实测准入，契约 §7 增补；1.4 信源扩展）──
	// 身份形状对照（M6 §7.3 新平台撞击分析）：weibo 帖=mblogid（9-10 位 base62 含大小写，
	// 如 "Ra1N24Tm5"）、weibo 热搜=中文短语、wechat_mp=app_msg_id_idx（"2667023086_1"）——
	// 三者与既有 web(恒含 ://)/xhs(24 位小写 hex)/x(19 位十进制) 均不可逐字节相等。
	{types.PlatformWeibo, types.CapUserPosts}: {
		Endpoint: "weibo_web_v2_fetch_user_posts",
		Params: []bindingParam{
			{Key: "uid", FromConfig: "uid", Required: true},
			// feature=0：10 条基础数据（上游文档「性能最佳」）。增量追新首页足够，
			// 与三 xhs 能力同款「只拉首页」拍板（契约 §7 分页条款）。
			{Key: "feature", Const: "0"},
		},
		Envelope:  []envelopeCheck{{Path: "code", Want: "200"}, {Path: "data.ok", Want: "1"}},
		ItemsPath: "data.data.list",
		// 转发拆包（x retweet 同款语义）：转发不是新内容，被转发的那条才是——
		// ExternalID 取原帖 mblogid，同一原帖经多号转发只落一行 content_item。
		// 刻意不设 VerifyParam：拆包后条目归属是原作者，按所订 uid 对照会把转发全丢。
		Unwrap: []string{"retweeted_status"},
		Fields: bindingFields{
			ID:      []string{"mblogid"},
			Title:   nil,                  // 微博无标题（x 同款）
			Content: []string{"text_raw"}, // 纯文本；text 是 HTML，不用
			Author:  []string{"user.screen_name"},
			// 桌面权威链接需要 uid+mblogid 双段；{user.idstr} 是字段占位符（拆包后
			// 指向原作者 uid，链接与内容一致）。
			URLTemplate: "https://weibo.com/{user.idstr}/{id}",
			Time:        []string{"created_at"},
			TimeFormat:  tfRubyDate, // "Thu Jul 23 17:55:27 +0800 2026"，与 Twitter 同格式（实测）
		},
		Kind: types.KindArticle,
		// OrderCheck 关：账号可设置置顶微博（置顶=旧帖排首位），检了必误拒——x/user_posts 同款取舍。
		// 已知取舍：isLongText 的长文 text_raw 被上游截断（实测 人民日报 9/20 条），
		// 本期不做详情补全（enrich 现有触发语义是「过长=被截断信号」，与「过短需补全」相反，
		// 且微博新闻体首段即要点）；若长期打分失真再按契约 §9 立项。
		MaxContentBytes: tikhubMaxDescBytes,
	},
	{types.PlatformWeibo, types.CapHotList}: {
		Endpoint:  "weibo_web_v2_fetch_hot_search",
		Params:    nil,                                          // 无参数：全局一份热搜榜
		Envelope:  []envelopeCheck{{Path: "code", Want: "200"}}, // 实测响应无 data.ok 字段
		ItemsPath: "data.realtime",
		// 广告位过滤：实测榜单混入 is_ad=1 的商业推广条目（无 realpos）；正常条目
		// **没有 is_ad 键**（1/51 实测）→ WantAbsent 语义「键不存在才保留」。
		ItemFilter: &fieldEquals{Path: "is_ad", WantAbsent: true},
		Fields: bindingFields{
			ID:    []string{"word"}, // 榜单条目无 id 字段（实测 id 仅广告条目有），热搜词即身份
			Title: []string{"word"},
			// 无正文：用榜位+热度合成，给打分器上下文（xhs/hot_list 薄正文先例，契约 §7.1）。
			Content:     []string{"tmpl:{word}（微博热搜第 {realpos} 位，热度 {num}）"},
			URLTemplate: "https://s.weibo.com/weibo",
			URLQuery:    []urlQueryParam{{Key: "q", FromField: "word"}},
			// 无 Time：榜单条目无时间戳（xhs/hot_list 同形态）。
		},
		Kind:            types.KindArticle,
		MaxContentBytes: tikhubMaxDescBytes,
	},
	{types.PlatformWechatMP, types.CapUserPosts}: {
		Endpoint: "wechat_mp_v2_fetch_account_articles",
		Params: []bindingParam{
			{Key: "username", FromConfig: "username", Required: true},
			// raw=false：精简解析结构（data.articles，snake_case）。字符串 "false" 经上游
			// FastAPI coerce 为 bool（2026-07-23 实测 200 且 params.raw=false 回显）。
			// 已知依赖：catalog 声明 raw 类型是 boolean，此处发 JSON 字符串依赖上游 lax
			// coercion；上游若切 strict validation 会 422（显式 HTTP 错误走 fail_count 链，
			// 可见非静默）——re-gen 后该端点若 422 从此查起。
			{Key: "raw", Const: "false"},
			// page_size/offset/item_show_type 不发送：用上游默认（20 条/首页/文章栏目）。
		},
		Envelope:  []envelopeCheck{{Path: "code", Want: "200"}},
		ItemsPath: "data.articles",
		Fields: bindingFields{
			// 复合身份：文章 URL 含每次抓取都会变的 chksm 签名参数，不能当身份；
			// mid(app_msg_id)+idx（一次群发多篇文章的位次）才是微信文章的稳定锚点。
			ID:      []string{"tmpl:{app_msg_id}_{idx}"},
			Title:   []string{"title"},
			Content: []string{"digest", "title"}, // digest 常为空（实测 人民日报 10/10 空）→ 退回标题
			// Author 无字段可取：响应只有 gh_ 原始 ID（在 data 层非条目层），不硬造。
			URLPaths:   []string{"url"},
			Time:       []string{"create_time"},
			TimeFormat: tfUnixS,
		},
		Kind: types.KindArticle,
		// OrderCheck 关：列表语义是发文历史（实测降序），但上游无排序承诺，保守不检。
		// 已知取舍：正文=digest（常为空）；详情补全需按 URL 调 fetch_article_detail 且触发
		// 语义与现有 enrich（过长=截断）相反，留二期（契约 §9）。
		MaxContentBytes: tikhubMaxDescBytes,
		// 上游文档明示：微信服务器慢，timeout 须设 30s，否则「已扣费但收不到响应」。
		TimeoutSeconds: 30,
	},
}

// IsBindingBacked 报告 (platform, capability) 是否由绑定引擎承载
// （agent 的试跑准入只对绑定能力生效，rss/exa 路径行为不变）。
func IsBindingBacked(p types.Platform, c types.Capability) bool {
	_, ok := bindingTemplatesV1[bindingKey{p, c}]
	return ok
}

// ────────── 记账 ──────────

// BindingCallRecorder 把绑定引擎的每次上游调用写进 tool_calls（契约 §5，Boss 硬需求：
// 每次计费调用有记录）。生产实现是 store 适配器；nil 合法（测试/未装配时只是不记账，
// 记账失败也绝不放大成抓取失败——与 agent ToolCallRecorder 同一纪律）。
type BindingCallRecorder interface {
	RecordBindingCall(ctx context.Context, rec *types.ToolCall)
}

// bindingTraceKey / bindingTenantIDKey / bindingUserIDKey 从 ctx 取本次上游
// 调用的账本归属。Agent ad-hoc 调用塞 trace + userID，保留 membership 推导；
// compiled workflow 塞冻结的 trace + tenantID + userID，避免多租户用户在网络调用
// 完成后被当前 membership 重新归属。这里只传本地记账元数据，不会序列化给上游。
type bindingTraceKeyT struct{}
type bindingTenantIDKeyT struct{}
type bindingUserIDKeyT struct{}
type bindingRunSnapshotIDKeyT struct{}

var bindingTraceKey bindingTraceKeyT
var bindingTenantIDKey bindingTenantIDKeyT
var bindingUserIDKey bindingUserIDKeyT
var bindingRunSnapshotIDKey bindingRunSnapshotIDKeyT

const bindingRecordTimeout = 5 * time.Second

// WithBindingTrace 把本轮抓取的 trace_id 注入 ctx（Fetch Activity 调用）。
func WithBindingTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, bindingTraceKey, traceID)
}

// WithBindingAttribution 把 Agent 发起的上游调用归到同一会话 trace 与用户。
// userID 显式必填；tenant_id 不在 fetcher 猜，由 store.InsertToolCall 按 memberships
// 的唯一规则推导。只作为本地记账元数据，不会被 HTTP 客户端序列化给第三方。
func WithBindingAttribution(ctx context.Context, traceID string, userID int64) context.Context {
	ctx = WithBindingTrace(ctx, traceID)
	return context.WithValue(ctx, bindingUserIDKey, userID)
}

// WithBindingRunAttribution pins a compiled fetch receipt to the immutable
// run's exact tenant and user. Values deliberately survive context.WithoutCancel
// so an upstream call that already happened can still be durably accounted
// after cancellation or membership revocation.
func WithBindingRunAttribution(
	ctx context.Context,
	traceID string,
	tenantID int64,
	userID int64,
	runSnapshotID int64,
) context.Context {
	ctx = WithBindingAttribution(ctx, traceID, userID)
	ctx = context.WithValue(ctx, bindingTenantIDKey, tenantID)
	if runSnapshotID > 0 {
		ctx = context.WithValue(ctx, bindingRunSnapshotIDKey, runSnapshotID)
	}
	return ctx
}

// BindingAttributionFromContext 读取上游账本归属。hasUser=false 是合法的系统/调度
// 调用形态；Agent ad-hoc 调用必须为 true。导出是为了让跨包调用方的行为测试能钉住
// “确实注入了归属”，而不是只测 fetcher 收到归属后会使用。
func BindingAttributionFromContext(ctx context.Context) (traceID string, userID int64, hasUser bool) {
	traceID, _ = ctx.Value(bindingTraceKey).(string)
	userID, hasUser = ctx.Value(bindingUserIDKey).(int64)
	return traceID, userID, hasUser
}

// BindingRunAttributionFromContext additionally exposes the optional exact
// tenant used by compiled runs. The older three-result helper above remains
// stable for Agent callers and their tests.
func BindingRunAttributionFromContext(
	ctx context.Context,
) (traceID string, tenantID int64, hasTenant bool, userID int64, hasUser bool) {
	traceID, userID, hasUser = BindingAttributionFromContext(ctx)
	tenantID, hasTenant = ctx.Value(bindingTenantIDKey).(int64)
	return traceID, tenantID, hasTenant, userID, hasUser
}

func bindingAttribution(ctx context.Context) (
	traceID string,
	tenantID *int64,
	userID *int64,
	runSnapshotID *int64,
) {
	traceID, uid, ok := BindingAttributionFromContext(ctx)
	if ok {
		userID = &uid
	}
	tid, hasTenant := ctx.Value(bindingTenantIDKey).(int64)
	if hasTenant {
		tenantID = &tid
	}
	snapshotID, hasSnapshot := ctx.Value(bindingRunSnapshotIDKey).(int64)
	if hasSnapshot && snapshotID > 0 {
		runSnapshotID = &snapshotID
	}
	return traceID, tenantID, userID, runSnapshotID
}

// detachedBindingRecordContext 让“已经打到上游”的调用即使随后被调用方取消也能
// 留下账本，同时重新加 5s 上限，避免裸 WithoutCancel 在连接池故障时无限阻塞。
func detachedBindingRecordContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), bindingRecordTimeout)
}

// ────────── 引擎 ──────────

// BindingFetcher 执行绑定模板。除 enrich 限速状态外无可变状态，可并发复用。
type BindingFetcher struct {
	inv       *tikhubinvoke.Invoker
	seen      SeenChecker
	rec       BindingCallRecorder
	maxBody   int64 // 响应体上限（invoke 读 cap+1，超出即显式报错）
	apiKeySet bool
	// callTimeout 默认单次调用超时（cfg/兜底 20s）。invoker 的 http.Client.Timeout 只设
	// 全模板最大值当保险丝，真正的每次调用预算由 ctx 按「模板 TimeoutSeconds 或本默认值」
	// 控制——否则声明了 30s 的模板会被 client 级 20s 抢先掐断，超时覆盖形同虚设。
	callTimeout time.Duration

	// enrich 限速是**实例级**闸门（跨源、跨 Fetch 调用共享）：Multi 只持有一个
	// BindingFetcher，Fetch 活动串行遍历到期源——若每次调用各自计数，源 B 的首条
	// 详情会紧贴源 A 的末条发出，稳吃 429（原 tikhub.go 同款设计，理由原样成立）。
	rateMu       sync.Mutex
	lastDetailAt time.Time
	// detailInterval/enrichBudget 抽成字段仅为可测：生产恒为模板值。
	detailInterval time.Duration
	enrichBudget   time.Duration
}

// NewBinding 构造绑定引擎。seen 为 nil 时跳过 enrich（无从判断新旧就不重复付费）；
// rec 为 nil 时不记账。invOpts 供测试注入 baseURL（生产不传）。
// 超时/响应上限对齐旧抓取器口径（cfg 可配，兜底 20s / 5MB——对抗审查 parity-5：
// 不能静默降为 lookup 面的 2MiB 且超限必须显式报错而非截断喂给 JSON 解码器）。
func NewBinding(cfg config.FetchConfig, seen SeenChecker, rec BindingCallRecorder, invOpts ...tikhubinvoke.Option) *BindingFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	// client 级超时抬到「默认与全部模板覆盖值的最大者」：它只是保险丝，每次调用的
	// 真实预算在 callAndDecode 由 ctx 控制（默认 timeout，模板声明了 TimeoutSeconds
	// 用声明值）。不抬的话模板级 30s 声明会被 client 级 20s 抢先掐断。
	clientTimeout := timeout
	for _, spec := range bindingTemplatesV1 {
		if d := time.Duration(spec.TimeoutSeconds) * time.Second; d > clientTimeout {
			clientTimeout = d
		}
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}
	maxBody := int64(maxMB) * 1024 * 1024
	opts := append([]tikhubinvoke.Option{
		tikhubinvoke.WithTimeout(clientTimeout),
		tikhubinvoke.WithBodyCap(maxBody),
	}, invOpts...)
	return &BindingFetcher{
		inv:         tikhubinvoke.New(cfg, opts...),
		seen:        seen,
		rec:         rec,
		maxBody:     maxBody,
		apiKeySet:   cfg.TikhubAPIKey != "",
		callTimeout: timeout,
	}
}

// Fetch 实现订阅信源抓取（workflow.Fetcher 分派到此）。
func (b *BindingFetcher) Fetch(ctx context.Context, src types.FetchTarget) ([]types.ContentItem, error) {
	return b.fetchWithEffectGate(ctx, src, nil)
}

func (b *BindingFetcher) fetchWithEffectGate(
	ctx context.Context,
	src types.FetchTarget,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	return b.run(ctx, src, beforeEffect)
}

func (b *BindingFetcher) run(
	ctx context.Context,
	src types.FetchTarget,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	spec, ok := bindingTemplatesV1[bindingKey{src.Platform, src.Capability}]
	if !ok {
		// Multi 只对模板里有的能力分派到此；走到这里是装配漂移，不是数据问题。
		return nil, types.NewAppError(types.CodeInternal,
			fmt.Sprintf("能力 %q/%q 无绑定模板（装配漂移，source_id=%d）", src.Platform, src.Capability, src.ID), nil)
	}
	// 反漂移防线 1：端点每轮对注册表校验（契约 §3.1/§3.2）。
	entry, ok := tikhubcatalog.Lookup(spec.Endpoint)
	if !ok {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("绑定失效：端点 %s 已从注册表移除（re-gen 漂移，source_id=%d）", spec.Endpoint, src.ID), nil)
	}
	var enrichEntry *tikhubcatalog.Entry
	if spec.Enrich != nil {
		if resolved, found := tikhubcatalog.Lookup(spec.Enrich.Endpoint); found {
			enrichEntry = &resolved
		}
	}
	return b.runResolved(
		ctx, src, spec, entry, enrichEntry, beforeEffect)
}

// fetchWithRetainedRouteV1 executes an exact binding template and transport
// entry retained by fetcher.binding/v1. It never consults bindingTemplatesV1
// or the current generated TikHub catalog.
func (b *BindingFetcher) fetchWithRetainedRouteV1(
	ctx context.Context,
	src types.FetchTarget,
	spec bindingSpec,
	entry tikhubcatalog.Entry,
	enrichEntry *tikhubcatalog.Entry,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	return b.runResolved(
		ctx, src, spec, entry, enrichEntry, beforeEffect)
}

func (b *BindingFetcher) runResolved(
	ctx context.Context,
	src types.FetchTarget,
	spec bindingSpec,
	entry tikhubcatalog.Entry,
	enrichEntry *tikhubcatalog.Entry,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	if !b.apiKeySet {
		return nil, types.NewAppError(types.CodeValidation,
			"TikHub 信源需要配置 VANE_FETCH_TIKHUB_API_KEY，当前为空", nil)
	}
	cfgMap, err := decodeConfigMap(src.Config)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("解析信源 config 失败（source_id=%d）", src.ID), err)
	}
	params, err := resolveParams(spec.Params, cfgMap, src)
	if err != nil {
		return nil, err
	}
	if err := validateAgainstEntry(entry, params, src); err != nil {
		return nil, err
	}

	root, err := b.callAndDecode(
		ctx, entry, params, spec.Envelope, spec.MsgPath, src, beforeEffect, b.specTimeout(spec))
	if err != nil {
		return nil, err
	}

	// 反漂移防线 2：条目数组必须解析得到（契约 §3.5）。
	rawItems, ok := resolveList(root, spec.ItemsPath)
	if !ok {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("响应结构漂移：路径 %q 不是数组（endpoint=%s，source_id=%d）",
				spec.ItemsPath, spec.Endpoint, src.ID), nil)
	}

	type extracted struct {
		item    types.ContentItem
		raw     any // 条目原始对象（enrich 的 ItemParams / RequireItemField 从这取）
		id      string
		content string
	}
	var (
		cands           []extracted
		filtered        int // 仅 ItemFilter 不匹配（多态流合法形态，不是漂移）
		rootMisses      int // ItemRoot 下钻失败（过滤已通过——这是结构漂移信号，对抗审查 HIGH-1）
		identityMissing int // 身份字段为空的条目（契约 §3.5 要求计数可见）
		timeFailures    int
	)
	now := time.Now().UTC()
	for _, ri := range rawItems {
		it := ri
		// 条目过滤：多态流（广告位/用户卡片）不是漂移，静默跳过是正确行为。
		if spec.ItemFilter != nil {
			v, ok := resolvePath(it, spec.ItemFilter.Path)
			keep := scalarString(v) == spec.ItemFilter.Want
			if spec.ItemFilter.WantAbsent {
				keep = !ok
			}
			if !keep {
				filtered++
				continue
			}
		}
		if spec.ItemRoot != "" {
			sub, ok := resolvePath(it, spec.ItemRoot)
			if !ok || sub == nil {
				// 过滤已通过却下钻失败：不是多态流，是「note 子对象改名」级的结构
				// 漂移信号——**不并入 filtered**，否则候选全灭防线被整体绕过。
				rootMisses++
				continue
			}
			it = sub
		}
		// 转推拆包：尝试链取到「含非空 ID 的对象」才替换，否则保留原条目。
		for _, p := range spec.Unwrap {
			sub, ok := resolvePath(it, p)
			if !ok {
				continue
			}
			if m, isObj := sub.(map[string]any); isObj {
				if id, _ := resolvePath(m, spec.Fields.ID[0]); strings.TrimSpace(scalarString(id)) != "" {
					it = m
					break
				}
			}
		}
		// 串号防御：字段非空且与所订参数不符 → 这条不是我们要的。
		if vp := spec.VerifyParam; vp != nil {
			got := strings.TrimSpace(scalarString(first(resolvePath(it, vp.Path))))
			want := cfgMap[vp.ConfigKey]
			if got != "" && want != "" && got != want {
				slog.Warn("binding: 条目归属与所订参数不符，疑似上游串号，跳过",
					"source_id", src.ID, "endpoint", spec.Endpoint, "path", vp.Path, "got", got, "want", want)
				continue
			}
		}

		id := strings.TrimSpace(chainString(it, spec.Fields.ID, cfgMap, true))
		if id == "" {
			identityMissing++ // 无身份的条目由 finalize 统一拒绝口径，这里早退省事。
			continue
		}
		content := chainString(it, spec.Fields.Content, cfgMap, false)
		author := chainString(it, spec.Fields.Author, cfgMap, false)

		var pub *time.Time
		if len(spec.Fields.Time) > 0 {
			v, _ := resolvePath(it, spec.Fields.Time[0])
			pub, err = parseBindingTime(v, spec.Fields.TimeFormat)
			if err != nil {
				timeFailures++
				pub = nil // 单条时间坏不丢内容（与旧 parseUnixSeconds(0)=nil 口径一致），全坏才算漂移。
			}
		}

		itemURL := ""
		if len(spec.Fields.URLPaths) > 0 {
			itemURL = strings.TrimSpace(chainString(it, spec.Fields.URLPaths, cfgMap, false))
		} else if spec.Fields.URLTemplate != "" {
			itemURL = strings.ReplaceAll(spec.Fields.URLTemplate, "{id}", url.PathEscape(id))
			itemURL = strings.ReplaceAll(itemURL, "{author}", author)
			// 剩余 {点路径} 占位符从条目原始字段取值（weibo 桌面链接的 {user.idstr}）。
			// 只用于 id 形态字段、不做转义（见 bindingFields.URLTemplate 注释）；
			// 既有模板 URL 无其他花括号，此步对它们是恒等的。任一占位符缺失 → 整个
			// URL 置空（推送卡无链接优于 weibo.com//xxx 死链），条目本身保留。
			if strings.Contains(itemURL, "{") {
				rendered, rok := renderTemplate(itemURL, it)
				if !rok {
					rendered = ""
				}
				itemURL = rendered
			}
			if itemURL != "" {
				itemURL += buildURLQuery(it, spec.Fields.URLQuery)
			}
		}

		cands = append(cands, extracted{
			item: types.ContentItem{
				SourceID:    src.ID,
				ExternalID:  id,
				URL:         itemURL,
				Title:       chainString(it, spec.Fields.Title, cfgMap, false),
				Author:      author,
				PublishedAt: pub,
				FetchedAt:   now,
				Kind:        spec.Kind,
			},
			raw:     it,
			id:      id,
			content: content,
		})
	}

	// 反漂移防线 3：有条目但候选全灭 = 身份/结构漂移，不是安静的空轮（契约 §3.5）。
	// **只有** ItemFilter 挡掉的不算（一屏全是广告位是多态流的合法形态）；
	// rootMisses/identityMissing 都是漂移证据，必须触发防线而非豁免。
	if len(rawItems) > 0 && len(cands) == 0 && filtered < len(rawItems) {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("条目全部无法提取（%d 条：下钻失败 %d、身份缺失 %d，endpoint=%s，source_id=%d）——疑似响应结构漂移",
				len(rawItems), rootMisses, identityMissing, spec.Endpoint, src.ID), nil)
	}
	// 「整屏被过滤」不是漂移（多态流合法形态，上面刻意豁免），但值得可观测：
	// 若上游把过滤字段从「仅特殊条目有」改成「全量显式 0/1」（WantAbsent 语义的已知
	// 脆弱面），存量订阅会从此每轮走到这里而非报错——这条 Warn 是唯一信号。
	if len(rawItems) > 0 && filtered == len(rawItems) {
		slog.Warn("binding: 本轮条目被 ItemFilter 全量过滤",
			"source_id", src.ID, "endpoint", spec.Endpoint, "filtered", filtered)
	}
	if identityMissing > 0 {
		slog.Warn("binding: 部分条目身份字段为空已丢弃",
			"source_id", src.ID, "endpoint", spec.Endpoint, "identity_missing", identityMissing)
	}
	if len(spec.Fields.Time) > 0 && len(cands) > 0 && timeFailures == len(cands) {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("时间字段 %q 全部解析失败（格式 %s，endpoint=%s，source_id=%d）——疑似响应结构漂移",
				spec.Fields.Time[0], spec.Fields.TimeFormat, spec.Endpoint, src.ID), nil)
	}
	// 运行期时序断言（契约 §4 表第 4 行，对抗审查 HIGH-2）：OrderCheck 模板的
	// sort=time 语义承诺降序；准入后上游排序腐坏（x/search 教训本尊：probe 单次
	// 全绿、多轮才暴露）唯一可检面就是每轮这条——失败走 fail_count 告警链。
	// probe 路径由 buildProbeReport 做同一断言（用户话术版），此处只管 fetch 轮次。
	if spec.OrderCheck && len(spec.Fields.Time) > 0 {
		var prev *time.Time
		for i := range cands {
			t := cands[i].item.PublishedAt
			if t == nil {
				continue
			}
			if prev != nil && t.After(*prev) {
				return nil, types.NewAppError(types.CodeValidation,
					fmt.Sprintf("条目时间序列非降序（endpoint=%s，source_id=%d）——上游排序疑似腐坏，无法继续追新",
						spec.Endpoint, src.ID), nil)
			}
			prev = t
		}
	}

	// enrich 在指纹计算（finalize）之前：content_hash/simhash 必须基于全文，
	// 否则跨批去重会把「同一条笔记的不同截断」当成新内容（原 tikhub.go 同款顺序）。
	if spec.Enrich != nil {
		contents := make([]*string, len(cands))
		ids := make([]string, len(cands))
		raws := make([]any, len(cands))
		for i := range cands {
			contents[i] = &cands[i].content
			ids[i] = cands[i].id
			raws[i] = cands[i].raw
		}
		if err := b.enrich(
			ctx, src, spec.Enrich, enrichEntry,
			ids, raws, contents, beforeEffect,
		); err != nil {
			return nil, err
		}
	}

	out := make([]types.ContentItem, 0, len(cands))
	for i := range cands {
		item := cands[i].item
		item.Content = cands[i].content
		if spec.MaxContentBytes > 0 {
			item.Content = truncateUTF8(item.Content, spec.MaxContentBytes)
		}
		// 刻意不在此累计 dropTally、也不加全灭防线：binding 路径已有「反漂移防线 3」
		// （见上方 len(rawItems)>0 && len(cands)==0 的判断），它的语义比通用版更细
		// ——额外排除了 ItemFilter 挡掉的条目（一屏全是广告位是多态流的合法形态）。
		// 套通用防线会对同一故障双重报错，且会把 ItemFilter 的正常过滤误判成漂移。
		if finalize(src, &item) != dropNone {
			continue
		}
		out = append(out, item)
	}
	slog.Info("binding: 抓取完成",
		"source_id", src.ID, "endpoint", spec.Endpoint,
		"raw_count", len(rawItems), "items_count", len(out), "filtered", filtered)
	return out, nil
}

// specTimeout 取本次调用的 ctx 预算：模板声明了 TimeoutSeconds 用声明值，否则用默认。
func (b *BindingFetcher) specTimeout(spec bindingSpec) time.Duration {
	if spec.TimeoutSeconds > 0 {
		return time.Duration(spec.TimeoutSeconds) * time.Second
	}
	return b.callTimeout
}

// callAndDecode 调一次端点：记账 → 状态分类 → UseNumber 解码 → 信封断言。
// timeout 是本次调用的 ctx 预算（0=不加，调用方自管）；client 级超时只是全模板
// 最大值的保险丝，见 NewBinding。
func (b *BindingFetcher) callAndDecode(
	ctx context.Context,
	entry tikhubcatalog.Entry,
	params map[string]any,
	checks []envelopeCheck,
	msgPath string,
	src types.FetchTarget,
	beforeEffect func(context.Context) error,
	timeout time.Duration,
) (any, error) {
	if err := checkEffectGate(ctx, beforeEffect); err != nil {
		return nil, err
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	res, err := b.inv.Invoke(ctx, entry, params)
	b.record(ctx, entry, params, res, err, src)
	if err != nil {
		// invoke 把传输层失败归 CodeInternal（lookup 面的口径）；对信源抓取而言
		// 网络失败是普通抓取失败（可重试），不是装配漂移——CodeInternal 在本管线
		// 专指「注册表↔装配漂移」（multi.go 兜底 + 探针语义），必须改判。
		var ae *types.AppError
		if errors.As(err, &ae) && ae.Code == types.CodeInternal {
			// cause 用 ae 的底层错误而非 ae 本身：留着 CodeInternal 在链上会让
			// errors.Is(err, ErrInternal) 命中，探针与 wired 不变量把它当装配漂移。
			wrapped := types.NewAppError(types.CodeFetchTimeout,
				fmt.Sprintf("TikHub 端点 %s 网络调用失败（source_id=%d）", entry.Name, src.ID), ae.Unwrap())
			wrapped.Retryable = true
			return nil, wrapped
		}
		return nil, err // 其余已分类（Validation=缺 key / FetchTimeout=超时）
	}

	switch {
	case res.Status == http.StatusTooManyRequests:
		return nil, types.NewAppError(types.CodeFetchRateLimit,
			fmt.Sprintf("TikHub 端点 %s 被限流(429)（source_id=%d）", entry.Name, src.ID), nil)
	case res.Status == http.StatusUnauthorized || res.Status == http.StatusForbidden:
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("TikHub 鉴权失败（HTTP %d），请检查 API key 与 scopes", res.Status), nil)
	case res.Status < 200 || res.Status >= 300:
		// body 摘要进错误信息：上游 4xx/5xx 的 body 通常直接写明原因，
		// 没有它每次断供都要本地重放才能定位（x.go 2026-07-17 生产 400 的教训）。
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("TikHub 端点 %s 返回 HTTP %d（source_id=%d）: %s",
				entry.Name, res.Status, src.ID, truncateUTF8(string(res.Body), 200)), nil)
		ae.Retryable = res.Status >= 500 // 5xx 瞬态可重试，4xx 确定性不可重试
		return nil, ae
	}

	if int64(len(res.Body)) > b.maxBody {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("TikHub 端点 %s 响应体超过 %d 字节上限", entry.Name, b.maxBody), nil)
	}

	root, err := decodeUseNumber(res.Body)
	if err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("解析 TikHub 端点 %s 响应失败", entry.Name), err)
		ae.Retryable = false
		return nil, ae
	}
	for _, c := range checks {
		v, _ := resolvePath(root, c.Path)
		if scalarString(v) != c.Want {
			msg := ""
			if msgPath != "" {
				if mv, ok := resolvePath(root, msgPath); ok {
					msg = truncateUTF8(scalarString(mv), 200)
				}
			}
			ae := types.NewAppError(types.CodeFetchTimeout,
				fmt.Sprintf("TikHub 端点 %s 业务失败（%s=%s，期望 %s，msg=%s，source_id=%d）",
					entry.Name, c.Path, scalarString(v), c.Want, msg, src.ID), nil)
			ae.Retryable = false // 业务层失败按确定性处理；瞬态故障通常直接表现为 HTTP 5xx
			return nil, ae
		}
	}
	return root, nil
}

// record 写一行 tool_calls（契约 §5）。失败只记日志，绝不放大成抓取失败。
// 两处加固（对抗审查 parity-7 / cs-13）：
//   - WithoutCancel：超时/取消的调用恰恰最该记账，不能因业务 ctx 已死而丢行；
//   - 参数净化：config 值可能带 NUL（Postgres JSONB 拒收 \x00），落库前剥离。
func (b *BindingFetcher) record(ctx context.Context, entry tikhubcatalog.Entry, params map[string]any, res *tikhubinvoke.Result, callErr error, src types.FetchTarget) {
	if b.rec == nil {
		return
	}
	ctx, cancel := detachedBindingRecordContext(ctx)
	defer cancel()
	trace, tenantID, userID, runSnapshotID := bindingAttribution(ctx)
	clean := make(map[string]any, len(params))
	for k, v := range params {
		if s, isStr := v.(string); isStr {
			clean[k] = strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
		} else {
			clean[k] = v
		}
	}
	args, _ := json.Marshal(clean)
	rec := &types.ToolCall{
		RunSnapshotID: runSnapshotID,
		TraceID:       trace,
		TenantID:      tenantID,
		UserID:        userID,
		ToolName:      "binding:" + entry.Name,
		ToolKind:      types.ToolCallKindBindingFetch,
		Provider:      "tikhub",
		EndpointPath:  entry.Path,
		Arguments:     args,
		UsageQuantity: 1,
	}
	if src.ID > 0 {
		srcID := src.ID
		rec.SourceID = &srcID
	}
	if res != nil {
		status := res.Status
		rec.HTTPStatus = &status
		rec.DurationMs = res.DurationMs
		rec.ResultSize = len(res.Body)
		rec.ResultPreview = toolResultPreview(res.Body)
		if res.Status < 200 || res.Status >= 300 {
			rec.ErrorType = types.ToolErrHTTP
		}
	}
	if callErr != nil {
		ae, _ := callErr.(*types.AppError)
		if ae != nil && ae.Code == types.CodeFetchTimeout {
			rec.ErrorType = types.ToolErrTimeout
		} else {
			rec.ErrorType = types.ToolErrInternal
		}
		rec.Error = strings.ToValidUTF8(strings.ReplaceAll(truncateUTF8(callErr.Error(), 500), "\x00", ""), "�")
	}
	b.rec.RecordBindingCall(ctx, rec)
}

// ────────── enrich（付费详情补全）──────────

// enrich 就地把被截断的正文替换为详情接口的全文。普通上游失败仍按原铁律
// best-effort 降级；唯一向上返回的是紧邻调用的 live-authorization 失败，避免
// 撤权被误当成一条可吞的详情失败后继续调用后续付费端点。
func (b *BindingFetcher) enrich(
	ctx context.Context,
	src types.FetchTarget,
	es *enrichSpec,
	retainedEntry *tikhubcatalog.Entry,
	ids []string,
	raws []any,
	contents []*string,
	beforeEffect func(context.Context) error,
) error {
	if b.seen == nil {
		return nil // 无从判断新旧就不补：宁可不补，也不为一库老笔记重复付费。
	}
	if retainedEntry == nil {
		slog.Warn("binding: 详情端点已从注册表移除，跳过本轮补全",
			"source_id", src.ID, "endpoint", es.Endpoint)
		return nil
	}
	entry := *retainedEntry
	// 与主调用同一道反漂移防线（契约 §3.6「同样每轮 Lookup + 参数校验」）：
	// re-gen 改了详情端点参数名时显式跳过（降级不失败），而不是静默丢参白花钱。
	nameProbe := map[string]any{es.KeyParam: ""}
	for k := range es.ItemParams {
		nameProbe[k] = ""
	}
	if err := validateAgainstEntry(entry, nameProbe, src); err != nil {
		slog.Warn("binding: 详情端点参数与注册表不符（re-gen 漂移？），跳过本轮补全",
			"source_id", src.ID, "endpoint", es.Endpoint, "err", err)
		return nil
	}

	// 候选：被截断（≥MinRunes 即上游截断信号）且必备字段在手。
	type cand struct {
		i  int
		id string
	}
	var cands []cand
	for i := range ids {
		if utf8.RuneCountInString(*contents[i]) < es.MinRunes {
			continue // 未被截断，已是完整正文
		}
		if es.RequireItemField != "" {
			v, _ := resolvePath(raws[i], es.RequireItemField)
			if strings.TrimSpace(scalarString(v)) == "" {
				continue // 详情必填字段缺失，调了必 400 还计费
			}
		}
		cands = append(cands, cand{i, ids[i]})
	}
	if len(cands) == 0 {
		return nil
	}

	// 计费闸门按 canonical_key（全局身份，不带 source_id）：别的源/用户补全过的
	// 笔记直接命中跳过，同一篇不为多个订阅者重复付费（M5 多用户重构的关键）。
	keys := make([]string, 0, len(cands))
	for _, c := range cands {
		keys = append(keys, CanonicalKey(src, types.ContentItem{ExternalID: c.id}))
	}
	seen, err := b.seen.EnrichedCanonicalKeys(ctx, keys, es.MinRunes)
	if err != nil {
		slog.Warn("binding: 查询已补全 canonical_key 失败，跳过本轮详情补全",
			"source_id", src.ID, "candidates", len(cands), "err", err)
		return nil
	}

	interval := b.detailInterval
	if interval <= 0 {
		interval = time.Duration(es.RateMs) * time.Millisecond
	}
	budget := b.enrichBudget
	if budget <= 0 {
		budget = time.Duration(es.BudgetMs) * time.Millisecond
	}
	deadline := time.Now().Add(budget)
	sent := 0
	for _, c := range cands {
		if _, ok := seen[CanonicalKey(src, types.ContentItem{ExternalID: c.id})]; ok {
			continue
		}
		if time.Now().After(deadline) {
			slog.Warn("binding: 详情补全预算用尽，剩余条目保留截断正文待下轮",
				"source_id", src.ID, "enriched", sent, "budget", budget)
			return nil
		}
		if !b.waitDetailSlot(ctx, interval) {
			slog.Warn("binding: 上下文取消，剩余条目保留截断正文", "source_id", src.ID, "enriched", sent)
			return nil
		}
		sent++

		desc, derr := b.fetchDetail(
			ctx, src, entry, es, c.id, raws[c.i], beforeEffect)
		if derr != nil {
			if isEffectGateError(derr) {
				return derr
			}
			slog.Warn("binding: 详情补全失败，保留截断正文",
				"source_id", src.ID, "item_id", c.id, "err", derr)
			continue
		}
		if desc == "" {
			continue // 详情正文为空（纯图笔记）：截断正文至少还有点内容，别覆盖成空。
		}
		*contents[c.i] = desc
	}
	return nil
}

// waitDetailSlot 拿详情限速槽位；持锁跨越 sleep 是刻意的（上游限"每秒 1 次"，
// 并发放行会直接破坏该约束；补全本就是串行低频路径）。
func (b *BindingFetcher) waitDetailSlot(ctx context.Context, interval time.Duration) bool {
	b.rateMu.Lock()
	defer b.rateMu.Unlock()
	if wait := interval - time.Since(b.lastDetailAt); wait > 0 && !b.lastDetailAt.IsZero() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
	b.lastDetailAt = time.Now()
	return true
}

// fetchDetail 取单条详情正文。任何异常返回 error 交调用方降级，不重试
// （补全尽力而为，重试只会加剧限流并翻倍成本）。
func (b *BindingFetcher) fetchDetail(
	ctx context.Context,
	src types.FetchTarget,
	entry tikhubcatalog.Entry,
	es *enrichSpec,
	id string,
	raw any,
	beforeEffect func(context.Context) error,
) (string, error) {
	params := map[string]any{es.KeyParam: id}
	for k, path := range es.ItemParams {
		v, _ := resolvePath(raw, path)
		params[k] = scalarString(v)
	}
	root, err := b.callAndDecode(
		ctx, entry, params, es.Envelope, es.MsgPath, src, beforeEffect, b.callTimeout)
	if err != nil {
		return "", err
	}
	list, ok := resolveList(root, es.RespItemsPath)
	if !ok || len(list) == 0 {
		return "", fmt.Errorf("详情响应无 items（path=%s）", es.RespItemsPath)
	}
	node := list[0]
	if es.RespRoot != "" {
		sub, ok := resolvePath(node, es.RespRoot)
		if !ok {
			return "", fmt.Errorf("详情响应缺 %s", es.RespRoot)
		}
		node = sub
	}
	// 防静默污染：确认拿回来的确实是请求的那条（上游有"200+正常外壳+别人的笔记"前科）。
	if gotID, _ := resolvePath(node, es.VerifyIDPath); strings.TrimSpace(scalarString(gotID)) != id {
		return "", fmt.Errorf("详情返回的 id=%q 与请求的 %q 不符，疑似上游串号", scalarString(gotID), id)
	}
	v, _ := resolvePath(node, es.DescPath)
	return scalarString(v), nil
}

// ────────── 参数装配与校验 ──────────

func decodeConfigMap(raw json.RawMessage) (map[string]string, error) {
	out := map[string]string{}
	if len(raw) == 0 {
		return out, nil
	}
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber() // 雪花 ID 级参数经 float64 会丢精度（大整数红线）
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	for k, v := range m {
		out[k] = scalarString(v)
	}
	return out, nil
}

func resolveParams(specs []bindingParam, cfg map[string]string, src types.FetchTarget) (map[string]any, error) {
	out := map[string]any{}
	for _, p := range specs {
		var val string
		if p.FromConfig != "" {
			val = strings.TrimSpace(cfg[p.FromConfig])
			if val == "" {
				if p.Required {
					return nil, types.NewAppError(types.CodeValidation,
						fmt.Sprintf("信源缺少必填参数 %s（source_id=%d）", p.FromConfig, src.ID), nil)
				}
				val = p.Default
			}
		} else {
			val = p.Const
		}
		if val == "" && p.OmitEmpty {
			continue
		}
		out[p.Key] = val
	}
	return out, nil
}

// validateAgainstEntry 按**当前**注册表 Entry 校验参数（契约 §3.2，反漂移防线）：
// 发送的参数必须都在 Entry.Params 里（防 buildRequest 静默丢参 → 200 但数据错误），
// Entry 的必填参数必须都在发送集里。
func validateAgainstEntry(entry tikhubcatalog.Entry, params map[string]any, src types.FetchTarget) error {
	known := map[string]bool{}
	for _, p := range entry.Params {
		known[p.Name] = true
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				return types.NewAppError(types.CodeValidation,
					fmt.Sprintf("端点 %s 必填参数 %s 缺失（注册表参数漂移？source_id=%d）",
						entry.Name, p.Name, src.ID), nil)
			}
		}
	}
	for k := range params {
		if !known[k] {
			return types.NewAppError(types.CodeValidation,
				fmt.Sprintf("端点 %s 不认识参数 %s（注册表参数漂移？source_id=%d）",
					entry.Name, k, src.ID), nil)
		}
	}
	return nil
}

// ────────── 点路径 / 标量 / 模板 ──────────

// resolvePath 沿点路径下钻（只支持对象键，无索引/谓词——sub-Turing，契约 §1.1）。
func resolvePath(root any, path string) (any, bool) {
	cur := root
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func resolveList(root any, path string) ([]any, bool) {
	v, ok := resolvePath(root, path)
	if !ok {
		return nil, false // 键不存在：字段被改名/移除，是结构漂移
	}
	if v == nil {
		// 显式 JSON null：字段还在、这轮没有内容（X 静默账号的 timeline 实测形态，
		// 旧 x.go 对此返回空成功）。与「键缺失」严格区分——null 合法，缺失才是漂移。
		return nil, true
	}
	l, ok := v.([]any)
	return l, ok
}

func first(v any, _ bool) any { return v }

// scalarString 把标量节点转字符串；json.Number 保原始十进制串（雪花 ID 精度）。
func scalarString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// chainString 依次尝试回退链，取第一个非空值。"$键名" 取 config 参数，
// "tmpl:…" 按模板渲染（占位符 = 条目原始字段点路径）。
// chainString 依次取回退链第一个非空值。strictTmpl 只影响 "tmpl:" 元素：true 时任一
// 占位符缺失/为空 → 该元素整体作废（取下一回退项）。**ID 链必须 strict**——宽松渲染会把
// 缺 app_msg_id 的条目拼成 "_1" 这类非空垃圾身份：既绕过 identityMissing 反漂移防线
// （契约 §3.5 防线 3 对该端点的身份漂移失明），又经裸 CanonicalKey 让所有账号同 idx 的
// 文章全局撞键、被 ON CONFLICT 静默归并。Content 合成保持宽松（少个榜位数字不该整条判废）。
func chainString(item any, chain []string, cfg map[string]string, strictTmpl bool) string {
	for _, src := range chain {
		var v string
		switch {
		case strings.HasPrefix(src, "$"):
			v = cfg[src[1:]]
		case strings.HasPrefix(src, "tmpl:"):
			s, ok := renderTemplate(src[len("tmpl:"):], item)
			if !strictTmpl || ok {
				v = s
			}
		default:
			raw, _ := resolvePath(item, src)
			v = scalarString(raw)
		}
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// renderTemplate 把 {点路径} 占位符替换为条目字段值（缺失 → 空串）。
// ok 报告是否所有占位符都解析出了非空值——身份/URL 用途必须检查 ok
// （缺一段的复合身份或双斜杠死链都不该带着「看起来有值」的外形过关）。
func renderTemplate(tmpl string, item any) (string, bool) {
	var sb strings.Builder
	ok := true
	rest := tmpl
	for {
		i := strings.IndexByte(rest, '{')
		if i < 0 {
			sb.WriteString(rest)
			return sb.String(), ok
		}
		j := strings.IndexByte(rest[i:], '}')
		if j < 0 {
			sb.WriteString(rest)
			return sb.String(), ok
		}
		sb.WriteString(rest[:i])
		v, found := resolvePath(item, rest[i+1:i+j])
		s := scalarString(v)
		if !found || strings.TrimSpace(s) == "" {
			ok = false
		}
		sb.WriteString(s)
		rest = rest[i+j+1:]
	}
}

// buildURLQuery 拼条件查询组：任一 FromField 为空 → 整组略去。按声明顺序手拼
// （不走 url.Values.Encode 的字典序），与旧 fetcher 的 URL 逐字节一致。
func buildURLQuery(item any, qs []urlQueryParam) string {
	if len(qs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(qs))
	for _, q := range qs {
		val := q.Const
		if q.FromField != "" {
			v, _ := resolvePath(item, q.FromField)
			val = scalarString(v)
			if val == "" {
				return "" // 条件组：字段缺失时整组不拼
			}
		}
		parts = append(parts, q.Key+"="+url.QueryEscape(val))
	}
	return "?" + strings.Join(parts, "&")
}

func decodeUseNumber(body []byte) (any, error) {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// parseBindingTime 严格按声明格式解析（类型漂移要可检——宽容解析会把漂移吞成 nil）。
func parseBindingTime(v any, format string) (*time.Time, error) {
	switch format {
	case tfUnixS:
		n, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("期望 unix 秒数值，得到 %T", v)
		}
		sec, err := n.Int64()
		if err != nil {
			return nil, err
		}
		return parseUnixSeconds(sec), nil
	case tfUnixMS:
		n, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("期望 unix 毫秒数值，得到 %T", v)
		}
		ms, err := n.Int64()
		if err != nil {
			return nil, err
		}
		if ms <= 0 {
			return nil, nil
		}
		t := time.UnixMilli(ms).UTC()
		return &t, nil
	case tfRubyDate:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("期望时间字符串，得到 %T", v)
		}
		t, err := time.Parse(twitterTimeLayout, s)
		if err != nil {
			return nil, err
		}
		u := t.UTC()
		return &u, nil
	default:
		return nil, fmt.Errorf("未知时间格式 %q", format)
	}
}

// parseUnixSeconds 把 Unix 秒转为 *time.Time；0 或负值视为未提供（列可空）。
// （迁移自原 tikhub.go，xhs 族与 enrich 判据共用。）
func parseUnixSeconds(sec int64) *time.Time {
	if sec <= 0 {
		return nil
	}
	t := time.Unix(sec, 0).UTC()
	return &t
}

// twitterTimeLayout Twitter 原生时间格式（迁移自原 x.go）。
const twitterTimeLayout = "Mon Jan 02 15:04:05 -0700 2006"

// tikhubMaxDescBytes 正文字节上限，防超长内容打爆打分 token（成本护栏）。
// 截断按 rune 边界回退（truncateUTF8），绝不产生非法 UTF-8。（迁移自原 tikhub.go。）
const tikhubMaxDescBytes = 4000

// SeenChecker 报告哪些 canonical_key **已入库且正文已补全**（生产实现是 *store.Store）。
// 详情接口按次计费，已拿到全文的笔记不该每轮重复付费；判据是**正文长度**而非
// "行是否存在"——补全会失败（429/抖动），失败的笔记仍以截断摘要落库，若按"存在"
// 跳过，一次瞬时 429 就让它终身停在摘要且再无自愈路径。
// 按 canonical_key（全局身份）而非 (source_id, external_id) 查是 M5 多用户重构的
// 关键：跨源命中同一篇时第一个源补全、其余全部跳过，不重复付费。
// （迁移自原 tikhub.go，语义原样。）
type SeenChecker interface {
	EnrichedCanonicalKeys(ctx context.Context, keys []string, minRunes int) (map[string]struct{}, error)
}
