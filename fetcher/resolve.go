// 通用来源兜底解析（功能清单 1.5）：用户给了一个 URL、但它解析不出 feed 时，
// 从页面 HTML 里嗅探 autodiscovery 声明（<link rel="alternate" type="application/rss+xml">）
// 找出站点真正的 feed 地址，让试跑拒绝话术能给出替代建议，而不是干巴巴的「不是 feed」。
//
// 边界（m6 契约 §18 被否决分支 D3/D4 划下的线）：
//   - 这里只做「发现并建议」，绝不静默改道——用户提交的 URL 是 A、实际订成 B 会破坏
//     确认卡语义（M4 契约），也违反 sourcecatalog 契约 §2.2「不静默改用别的能力凑合」。
//     发现的 feed 地址进拒绝话术，由用户确认后走第二次 add_source（新确认卡展示新 URL）。
//   - 不做站点专用解析器（D3）、不做 sitemap 伪 feed（D4）：嗅探失败就落到
//     web/contents（Exa /contents 页面监控）与 web/search 的建议上，那是通用能力。
//
// 安全（对抗审查 B-HIGH/B-MED/B-LOW）：嗅探出的 URL 由**攻击者可控的页面**声明，
// 而它会被拼进拒绝话术、经 add_source 回执写进 agent 的 [卡片回调] 上下文。因此
// 本文件只输出**结构上安全**的 URL（http(s)、无 userinfo、限长），**文本层消毒**
// （定界符伪造、隐藏字符）由话术拼接处的 promptguard 兜底（见 probe.go probeReject）。
package fetcher

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

const (
	// sniffFeedMax 最多带几条发现的 feed 地址进建议话术。多数站点只声明 1-2 个
	// （RSS + Atom 各一），上限防站点声明几十个分类 feed 时把回执撑爆。
	sniffFeedMax = 3
	// maxFeedURLBytes 单条嗅探 URL 的长度上限（对抗审查 B-MED DoS）：正常 feed
	// 地址不过百字节，超 512 的多半是攻击者塞进 href 的注入载荷（可达 MB 级，
	// 会经 add_source 回执灌进 agent 会话消息、且 truncateMessages 按条数不按体量
	// 裁剪 → 会话级 DoS）。超限直接**拒收该条**（不截断——截断的 URL 用户也用不了）。
	maxFeedURLBytes = 512
	// maxSniffBytes 嗅探只解析 HTML 的前若干字节（对抗审查 B-LOW 内存放大）：
	// autodiscovery 声明按规范只在 <head> 里，几十 KB 足够；对 5MB 恶意 HTML 全量
	// 建 DOM 是 10-30× 内存放大。取 512KB 给 <head> 充裕余量。
	maxSniffBytes = 512 * 1024
)

// feedMIMETypes 是 autodiscovery 约定的 feed MIME 类型（大小写不敏感，去参数后比较）。
// application/feed+json（JSON Feed）**收**：gofeed v1.4.0 实测支持解析
// （FeedType=json，正确提取 title/link/content），web/feed 能直接订阅——2026-07-19
// 实测纠正了初版「gofeed 不吃 JSON Feed」的错误断言（对抗审查 A-F7）。
var feedMIMETypes = map[string]struct{}{
	"application/rss+xml":  {},
	"application/atom+xml": {},
	"application/feed+json": {},
}

// sniffFeedLinks 从 HTML 里提取 autodiscovery 声明的 feed 绝对地址（≤ sniffFeedMax，
// 按文档出现顺序，去重）。best-effort：body 不是 HTML、无声明、解析失败都返回 nil——
// 嗅探不到不是错误路径，只是回退到「无发现」话术。
//
// base 应传**重定向后的最终 URL**（对抗审查 A-F2/B-LOW）：相对 href 用原始 URL 做基
// 会在尾斜杠 301、跨域重定向后指向错误目录/主机，让「建议地址」失真——伤的正是 1.5
// 的核心卖点。文档内 <base href> 若存在则进一步覆盖（HTML 规范：首个带 href 的 base）。
//
// rel/type 匹配按 HTML 规范而非精确串比较（对抗审查 A-F6）：rel 是空白分隔的 token
// 列表（rel="alternate stylesheet" 合法）、type 可带 MIME 参数（"application/rss+xml;
// charset=utf-8"）——两者都用宽松匹配，否则真实网页里有声明也识别不出。
func sniffFeedLinks(body []byte, base *url.URL) []string {
	if len(body) > maxSniffBytes {
		body = body[:maxSniffBytes]
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	// <base href> 覆盖相对路径基准（若存在且可解析为绝对地址）。
	if href, ok := doc.Find("base[href]").First().Attr("href"); ok {
		if ref, perr := url.Parse(strings.TrimSpace(href)); perr == nil {
			base = base.ResolveReference(ref)
		}
	}

	var out []string
	seen := map[string]struct{}{}
	doc.Find("link").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		if !relHasAlternate(s.AttrOr("rel", "")) {
			return true
		}
		if !isFeedMIME(s.AttrOr("type", "")) {
			return true
		}
		href := strings.TrimSpace(s.AttrOr("href", ""))
		if href == "" {
			return true
		}
		ref, perr := url.Parse(href)
		if perr != nil {
			return true
		}
		abs := base.ResolveReference(ref)
		if !safeFeedURL(abs) {
			return true
		}
		u := abs.String()
		if len(u) > maxFeedURLBytes {
			return true // 超长 URL 拒收（DoS/注入载荷），见 maxFeedURLBytes。
		}
		if _, dup := seen[u]; dup {
			return true
		}
		seen[u] = struct{}{}
		out = append(out, u)
		return len(out) < sniffFeedMax
	})
	return out
}

// relHasAlternate 报告 rel 属性（空白分隔 token 列表）是否含 alternate（大小写不敏感）。
func relHasAlternate(rel string) bool {
	for _, tok := range strings.Fields(rel) {
		if strings.EqualFold(tok, "alternate") {
			return true
		}
	}
	return false
}

// isFeedMIME 去掉 MIME 参数（"; charset=…"）后查表判是否 feed 类型。
func isFeedMIME(typ string) bool {
	mime := typ
	if i := strings.IndexByte(mime, ';'); i >= 0 {
		mime = mime[:i]
	}
	_, ok := feedMIMETypes[strings.ToLower(strings.TrimSpace(mime))]
	return ok
}

// safeFeedURL 是嗅探输出的**结构性**安全闸（对抗审查 B-HIGH/B-LOW）：
//   - 只放行 http(s)：其它 scheme（javascript:、data:）既订不上也不该进回执；
//   - 拒绝带 userinfo（user:pass@host）：正常 feed 声明不含它，而它是「可信域伪装」
//     升级面（https://trusted.com@evil.com 里真实 host 是 evil.com），用户照建议加就
//     订到钓鱼源。
//
// 文本层消毒（定界符伪造、零宽字符）不在此做——那是话术拼接处 promptguard 的职责，
// 分层清晰：这里保证「URL 结构合法」，promptguard 保证「文本嵌入 LLM 上下文安全」。
func safeFeedURL(u *url.URL) bool {
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if u.User != nil {
		return false
	}
	return true
}
