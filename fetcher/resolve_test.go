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

// TestSniffFeedLinks_大小写与去重 真实网页里 rel="Alternate"、type 全大写都出现过；
// cascadia 属性值匹配大小写敏感，所以实现必须手工 EqualFold——本用例锁住这一点。
// 同地址声明两次只返回一次。
func TestSniffFeedLinks_大小写与去重(t *testing.T) {
	body := `<html><head>
		<link rel="Alternate" type="APPLICATION/RSS+XML" href="/feed.xml">
		<link rel="alternate" type="application/rss+xml" href="/feed.xml">
	</head></html>`
	got := sniffFeedLinks([]byte(body), mustParse(t, "https://example.com/"))
	if len(got) != 1 || got[0] != "https://example.com/feed.xml" {
		t.Fatalf("大小写变体应命中且去重为 1 条，实得 %v", got)
	}
}

// TestSniffFeedLinks_上限与非法项 超过 sniffFeedMax 截断；javascript: 伪协议、
// 空 href、JSON Feed（gofeed 订不上，建议了也是假建议）一律不收。
func TestSniffFeedLinks_上限与非法项(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<html><head>`)
	b.WriteString(`<link rel="alternate" type="application/rss+xml" href="javascript:alert(1)">`)
	b.WriteString(`<link rel="alternate" type="application/rss+xml" href="">`)
	b.WriteString(`<link rel="alternate" type="application/feed+json" href="/feed.json">`)
	for _, p := range []string{"/a.xml", "/b.xml", "/c.xml", "/d.xml"} {
		b.WriteString(`<link rel="alternate" type="application/rss+xml" href="` + p + `">`)
	}
	b.WriteString(`</head></html>`)
	got := sniffFeedLinks([]byte(b.String()), mustParse(t, "https://example.com/"))
	if len(got) != sniffFeedMax {
		t.Fatalf("期望截断到 %d 条，实得 %v", sniffFeedMax, got)
	}
	if got[0] != "https://example.com/a.xml" {
		t.Errorf("非法项应被跳过、从 a.xml 开始，实得 %v", got)
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
