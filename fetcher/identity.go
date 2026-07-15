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
// **没有单一字段能通吃**，两条实测结论方向恰好相反（契约 §1）：
//   - rss/exa → url：BBC 换 guid 重发同一篇文章时 url 逐字不变，url 才是"这篇文章"。
//   - tikhub_xhs → note_id：小红书 url 带 xsec_token（每次搜索新发的临时票据），
//     同一笔记两次搜到 url 不同，只有 note_id 稳定。
//
// 键是**裸值**（url 原文 / note_id 原文），不加命名空间前缀：007 迁移的回填
// （`CASE WHEN s.type='tikhub_xhs' THEN ci.external_id ELSE ci.url END`）写的就是裸值，
// 这里加前缀会让存量行的键与运行时算出的键永不相等，全库内容立刻重新长出一份重复。
// 改这里必须同步改 007 的回填 CASE，反之亦然。
//
// 返回空串表示**这条内容没有可用身份**，调用方必须丢弃（见 finalize）。刻意不兜底成
// 别的键：兜底键要么含 source_id（跨源不同 → 归并失效，等于没重构），要么含正文摘要
// （内容一改就变 → 同一篇长出多份），两种都是"看着有身份、其实没有"，比直接丢一条更坏。
func CanonicalKey(src types.Source, item types.ContentItem) string {
	switch src.Type {
	case types.SourceTypeTikHubXHS:
		// 身份是 note_id，即 mapTikhubNotes 填进 ExternalID 的 n.ID。刻意不碰
		// item.URL：它带 xsec_token，拿它当身份等于同一笔记每轮都是新内容。
		return xhsKey(item.ExternalID)

	case types.SourceTypeRSS, types.SourceTypeExa:
		// url 优先于 external_id：guid 只是"源内唯一"，url 才是"这篇文章"。
		// 这也让 rss 与 exa 同属 url 派——Exa 搜到用户 RSS 源里的同一篇文章时能归并。
		return strings.TrimSpace(item.URL)

	default:
		// 未知类型不猜身份字段：猜错会把两篇无关内容合并成一篇（不可逆）。Multi 分发时
		// 已拒掉未知类型，这里返回空串是防"新增了类型却忘了加 case"时静默按 url 归并。
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
