// 内容身份（canonical_key）的构造，契约 §5。
//
// 为什么要有这一层（M5 多用户重构的直接动机）：数据一律留存做深层需求分析与信源
// 质量评估，而分析的前提是**同一篇内容在库里只有一份**——存 N 份会让"这篇被几个
// 用户看过""这个信源出稿质量如何"之类的统计口径全错。原先的身份是
// UNIQUE(source_id, external_id)，它在两个方向上都不够：
//
//   - **源内挡不住**：BBC 给同一篇文章发多个 guid（生产 219 行里 13 组冗余，
//     13 组的 url 全部相同、external_id 各不同，有 1 篇存了 3 份）。
//   - **跨源挡不住**（多用户才暴露）：用户 A 订「AI编程」、B 订「AI工具」，
//     同一篇笔记命中两个源 → per-source 唯一无从比较 → 存两份，且小红书详情
//     补全按 (source_id, external_id) 查闸门时会被重复付费（$0.01/次）。
//
// 身份由 fetcher 构造而非 store：只有 fetcher 知道源类型，而规则按类型分派（契约 §1）。
package fetcher

import (
	"strings"

	"github.com/YouToco/vane/types"
)

// CanonicalKey 算出内容的全局身份（content_items.canonical_key，UNIQUE）。
//
// 按 Platform 分派（008 起取代 src.Type）：
//   - web → url：BBC 换 guid 重发同一篇文章时 url 逐字不变，url 才是"这篇文章"。
//   - xhs → note_id：小红书 url 带 xsec_token（每次搜索新发的临时票据），只有 note_id 稳定。
//   - x   → tweet_id：推文 ID 是推特身份的唯一稳定锚点。
//
// 键是裸值（url 原文 / note_id 原文 / tweet_id 原文），不加命名空间前缀：
// 007 回填写的就是裸值，加前缀会让存量行的键与运行时算出的不相等 → 全库重复。
//
// 返回空串表示这条内容没有可用身份，调用方必须丢弃（见 finalize）。
func CanonicalKey(src types.Source, item types.ContentItem) string {
	switch src.Platform {
	case types.PlatformXHS:
		return xhsKey(item.ExternalID)

	case types.PlatformX:
		return strings.TrimSpace(item.ExternalID)

	// weibo → mblogid（帖，9-10 位 base62 含大小写）/ 热搜词（榜，中文短语）；
	// wechat_mp → app_msg_id_idx（"2667023086_1"，模板 tmpl 合成的复合身份——
	// 文章 URL 含每次抓取会变的 chksm 参数，不能当身份）。
	// 两平台的键形状与既有三平台不可逐字节相等（M6 §7.3 撞击分析，2026-07-23 实测），
	// 详见 endpoint-binding-contract.md §7 的身份形状对照。
	case types.PlatformWeibo, types.PlatformWechatMP:
		return strings.TrimSpace(item.ExternalID)

	case types.PlatformWeb:
		return strings.TrimSpace(item.URL)

	default:
		return ""
	}
}

// xhsKey 由 note_id 构造小红书内容的身份。
//
// 单独抽出来是为了让"落库用的键"（CanonicalKey）与"查付费闸门用的键"（enrichDescs）
// **不可能漂移**：两处各写一遍的话，任何一边多做一步归一化，闸门就会全部 miss——
// 表现不是报错，而是每轮为整库老笔记重付一遍详情费（$0.01/条）且无任何告警。
//
// TrimSpace 是唯一的归一化，且只为让"空白即无身份"成立：note_id 恒为十六进制串，
// 真实值不含空白，故这一步在生产数据上是恒等的，不会与 007 回填的裸值漂移。
func xhsKey(noteID string) string {
	return strings.TrimSpace(noteID)
}
