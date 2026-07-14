package api

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestBuildSource_RSSDefault(t *testing.T) {
	src, msg := buildSource(addSubscriptionReq{URL: "https://example.com/feed.xml"})
	if msg != "" {
		t.Fatalf("合法 RSS 请求不应报错: %s", msg)
	}
	if src.Type != types.SourceTypeRSS || src.URL != "https://example.com/feed.xml" {
		t.Errorf("缺省 type 应为 rss，实际 %+v", src)
	}
}

func TestBuildSource_RSSRejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "ftp://x.com/feed", "exa://search?q=x", "not-a-url"} {
		if _, msg := buildSource(addSubscriptionReq{URL: u}); msg == "" {
			t.Errorf("URL %q 应被拒绝", u)
		}
	}
}

func TestBuildSource_ExaCategoryInIdempotencyKey(t *testing.T) {
	// 回归（审查 #幂等键）：category 改变抓取语义，必须参与合成键——
	// 否则同 query 不同 category 撞同一 sources 行，config 被静默覆盖。
	a, msg := buildSource(addSubscriptionReq{Type: "exa", Query: "AI", Category: "news"})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	b, msg := buildSource(addSubscriptionReq{Type: "exa", Query: "AI", Category: "research paper"})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	c, msg := buildSource(addSubscriptionReq{Type: "exa", Query: "AI"})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	if a.URL == b.URL || a.URL == c.URL || b.URL == c.URL {
		t.Errorf("不同 category 应生成不同幂等键: %q / %q / %q", a.URL, b.URL, c.URL)
	}
	// 完全相同的 (query, category) 仍应幂等命中同一键。
	a2, _ := buildSource(addSubscriptionReq{Type: "exa", Query: "AI", Category: "news"})
	if a.URL != a2.URL {
		t.Errorf("同 (query,category) 应生成同一幂等键: %q vs %q", a.URL, a2.URL)
	}
}

func TestBuildSource_TrimsWhitespace(t *testing.T) {
	// 回归（审查 #归一化）："AI" 与 "AI " 必须收敛到同一幂等键；全空白应被拒。
	a, _ := buildSource(addSubscriptionReq{Type: "exa", Query: "AI"})
	b, _ := buildSource(addSubscriptionReq{Type: "exa", Query: "  AI  "})
	if a.URL != b.URL {
		t.Errorf("首尾空白应被归一化: %q vs %q", a.URL, b.URL)
	}
	if _, msg := buildSource(addSubscriptionReq{Type: "exa", Query: "   "}); msg == "" {
		t.Error("全空白 query 应被拒绝（否则建出永久失败的源）")
	}
	if _, msg := buildSource(addSubscriptionReq{Type: "tikhub_xhs", Keyword: "\t \n"}); msg == "" {
		t.Error("全空白 keyword 应被拒绝")
	}
}

func TestBuildSource_RejectsOverlongParams(t *testing.T) {
	long := strings.Repeat("长", maxSourceParamRunes+1)
	if _, msg := buildSource(addSubscriptionReq{Type: "exa", Query: long}); msg == "" {
		t.Error("超长 query 应被拒绝")
	}
	if _, msg := buildSource(addSubscriptionReq{Type: "tikhub_xhs", Keyword: long}); msg == "" {
		t.Error("超长 keyword 应被拒绝")
	}
}

func TestBuildSource_TikhubKeyword(t *testing.T) {
	src, msg := buildSource(addSubscriptionReq{Type: "tikhub_xhs", Keyword: "AI 创业"})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	if src.Type != types.SourceTypeTikHubXHS {
		t.Errorf("Type 应为 tikhub_xhs，实际 %q", src.Type)
	}
	if !strings.HasPrefix(src.URL, "tikhub://xhs/search?keyword=") {
		t.Errorf("合成 URL 前缀不符: %q", src.URL)
	}
	if src.Title != "小红书: AI 创业" {
		t.Errorf("默认 Title 不符: %q", src.Title)
	}
	if _, msg := buildSource(addSubscriptionReq{Type: "tikhub_xhs"}); msg == "" {
		t.Error("缺 keyword 应被拒绝")
	}
}

func TestBuildSource_UnknownType(t *testing.T) {
	if _, msg := buildSource(addSubscriptionReq{Type: "carrier_pigeon", URL: "https://x.com"}); msg == "" {
		t.Error("未知 type 应被拒绝")
	}
}
