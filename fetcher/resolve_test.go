package fetcher

import (
	"net/url"
	"strings"
	"testing"
)

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("解析基准 URL 失败: %v", err)
	}
	return u
}

// TestSniffFeedLinks_标准声明 覆盖 autodiscovery 的常见形态：RSS 与 Atom 各一、
// 相对路径解析为绝对地址、按文档顺序返回。
func TestSniffFeedLinks_标准声明(t *testing.T) {
	body := `<!DOCTYPE html><html><head>
		<link rel="alternate" type="application/rss+xml" title="RSS" href="/feed.xml">
		<link rel="alternate" type="application/atom+xml" title="Atom" href="https://blog.example.com/atom.xml">
		<link rel="stylesheet" href="/style.css">
	</head><body>hi</body></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://blog.example.com/posts/"))
	want := []string{"https://blog.example.com/feed.xml", "https://blog.example.com/atom.xml"}
	if len(got) != len(want) {
		t.Fatalf("期望 %d 条，实得 %v", len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 条期望 %q，实得 %q", i, want[i], got[i])
		}
	}
}

// TestSniffFeedLinks_JSONFeed 对抗审查 A-F7：gofeed v1.4.0 实测支持 JSON Feed，
// application/feed+json 声明应被收（此前被错误排除、引向付费的 web/contents）。
func TestSniffFeedLinks_JSONFeed(t *testing.T) {
	body := `<html><head><link rel="alternate" type="application/feed+json" href="/feed.json"></head></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://example.com/"))
	if len(got) != 1 || got[0] != "https://example.com/feed.json" {
		t.Fatalf("JSON Feed 声明应被收，实得 %v", got)
	}
}

// TestSniffFeedLinks_宽松匹配 对抗审查 A-F6：rel 是空白分隔 token 列表、type 可带
// MIME 参数——真实网页里两者都出现过，精确串比较会漏识别有声明的站点。
func TestSniffFeedLinks_宽松匹配(t *testing.T) {
	body := `<html><head>
		<link rel="alternate stylesheet" type="application/rss+xml; charset=utf-8" href="/a.xml">
		<link rel="Alternate" type="APPLICATION/ATOM+XML" href="/b.xml">
	</head></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://example.com/"))
	want := []string{"https://example.com/a.xml", "https://example.com/b.xml"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rel 多 token / type 带参数应命中，实得 %v", got)
	}
}

// TestSniffFeedLinks_去重 同地址声明两次只返回一次。
func TestSniffFeedLinks_去重(t *testing.T) {
	body := `<html><head>
		<link rel="alternate" type="application/rss+xml" href="/feed.xml">
		<link rel="alternate" type="application/rss+xml" href="/feed.xml">
	</head></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://example.com/"))
	if len(got) != 1 {
		t.Fatalf("应去重为 1 条，实得 %v", got)
	}
}

// TestSniffFeedLinks_base覆盖 对抗审查 A-F2：<base href> 存在时相对 href 以它为基准。
func TestSniffFeedLinks_base覆盖(t *testing.T) {
	body := `<html><head>
		<base href="https://cdn.example.com/blog/">
		<link rel="alternate" type="application/rss+xml" href="feed.xml">
	</head></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://example.com/posts/"))
	if len(got) != 1 || got[0] != "https://cdn.example.com/blog/feed.xml" {
		t.Fatalf("<base href> 应覆盖相对基准，实得 %v", got)
	}
}

// TestSniffFeedLinks_安全过滤 对抗审查 B-HIGH/B-LOW：javascript:/data: 伪协议、
// 空 href、带 userinfo（可信域伪装）、超长（DoS 载荷）一律不收。
func TestSniffFeedLinks_安全过滤(t *testing.T) {
	longURL := "https://example.com/" + strings.Repeat("a", maxFeedURLBytes)
	body := `<html><head>` +
		`<link rel="alternate" type="application/rss+xml" href="javascript:alert(1)">` +
		`<link rel="alternate" type="application/rss+xml" href="">` +
		`<link rel="alternate" type="application/rss+xml" href="https://user:pass@evil.com/feed.xml">` +
		`<link rel="alternate" type="application/rss+xml" href="` + longURL + `">` +
		`<link rel="alternate" type="application/rss+xml" href="/ok.xml">` +
		`</head></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://example.com/"))
	if len(got) != 1 || got[0] != "https://example.com/ok.xml" {
		t.Fatalf("伪协议/空/userinfo/超长应全部拒收，只剩 ok.xml，实得 %v", got)
	}
}

// TestSniffFeedLinks_上限 超过 sniffFeedMax 截断。
func TestSniffFeedLinks_上限(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<html><head>`)
	for _, p := range []string{"/a.xml", "/b.xml", "/c.xml", "/d.xml"} {
		b.WriteString(`<link rel="alternate" type="application/rss+xml" href="` + p + `">`)
	}
	b.WriteString(`</head></html>`)
	got := sniffFeedLinks([]byte(b.String()), mustParse(t, "https://example.com/"))
	if len(got) != sniffFeedMax {
		t.Fatalf("期望截断到 %d 条，实得 %v", sniffFeedMax, got)
	}
}

// TestSniffFeedLinks_无声明与烂输入 都返回 nil——嗅探不到不是错误路径。
func TestSniffFeedLinks_无声明与烂输入(t *testing.T) {
	base := mustParse(t, "https://example.com/")
	for name, body := range map[string]string{
		"无声明的正常页": `<html><head><title>hi</title></head><body>text</body></html>`,
		"非HTML":    `{"json": true}`,
		"截断的烂HTML": `<html><head><link rel="alternate" type="application/rss`,
	} {
		if got := sniffFeedLinks([]byte(body), base); len(got) != 0 {
			t.Errorf("%s：应返回空，实得 %v", name, got)
		}
	}
}
