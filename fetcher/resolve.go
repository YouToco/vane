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
package fetcher

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// sniffFeedMax 最多带几条发现的 feed 地址进建议话术。多数站点只声明 1-2 个
// （RSS + Atom 各一），上限防站点声明几十个分类 feed 时把回执撑爆。
const sniffFeedMax = 3

// feedMIMETypes 是 autodiscovery 约定的 feed MIME 类型（大小写不敏感比较）。
// JSON Feed（application/feed+json）刻意不收：gofeed 的 Parse 不吃 JSON Feed，
// 建议了用户也订不上，那是假建议。
var feedMIMETypes = map[string]struct{}{
	"application/rss+xml":  {},
	"application/atom+xml": {},
}

// sniffFeedLinks 从 HTML 里提取 autodiscovery 声明的 feed 绝对地址（≤ sniffFeedMax，
// 按文档出现顺序，去重）。best-effort：body 不是 HTML、无声明、解析失败都返回 nil——
// 嗅探不到不是错误路径，只是回退到「无发现」话术。
//
// 属性匹配手工遍历而非 CSS 选择器：cascadia 的属性值匹配大小写敏感，
// 而真实网页里 rel="Alternate"、type="APPLICATION/RSS+XML" 都出现过，EqualFold 才稳。
func sniffFeedLinks(body []byte, base *url.URL) []string {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	var out []string
	seen := map[string]struct{}{}
	doc.Find("link").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		rel, _ := s.Attr("rel")
		if !strings.EqualFold(strings.TrimSpace(rel), "alternate") {
			return true
		}
		typ, _ := s.Attr("type")
		if _, ok := feedMIMETypes[strings.ToLower(strings.TrimSpace(typ))]; !ok {
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
		// 只收 http(s)：feed 地址会进建议话术、被用户拿去二次 add_source，
		// 其它 scheme（javascript:、data:）既订不上也不该出现在回执里。
		if abs.Scheme != "http" && abs.Scheme != "https" {
			return true
		}
		u := abs.String()
		if _, dup := seen[u]; dup {
			return true
		}
		seen[u] = struct{}{}
		out = append(out, u)
		return len(out) < sniffFeedMax
	})
	return out
}
