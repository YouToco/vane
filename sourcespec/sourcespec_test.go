package sourcespec

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestBuild_WebFeed(t *testing.T) {
	src, msg := Build(Spec{
		Platform:   "web",
		Capability: "feed",
		Params:     map[string]string{"url": "https://example.com/feed.xml"},
	})
	if msg != "" {
		t.Fatalf("合法 web/feed 请求不应报错: %s", msg)
	}
	if src.Platform != types.PlatformWeb || src.Capability != types.CapFeed {
		t.Errorf("应为 web/feed，实际 %s/%s", src.Platform, src.Capability)
	}
	if src.URL != "https://example.com/feed.xml" {
		t.Errorf("URL 不符: %q", src.URL)
	}
}

func TestBuild_WebFeedRejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "ftp://x.com/feed", "vane://web/search?q=x", "not-a-url"} {
		if _, msg := Build(Spec{
			Platform:   "web",
			Capability: "feed",
			Params:     map[string]string{"url": u},
		}); msg == "" {
			t.Errorf("URL %q 应被拒绝", u)
		}
	}
}

func TestBuild_WebFeedWithCategories(t *testing.T) {
	catJSON, _ := json.Marshal([]string{"Product", "Research"})
	src, msg := Build(Spec{
		Platform:   "web",
		Capability: "feed",
		Params:     map[string]string{"url": "https://openai.com/news/rss.xml", "categories": string(catJSON)},
	})
	if msg != "" {
		t.Fatalf("合法请求不应报错: %s", msg)
	}
	if src.URL != "https://openai.com/news/rss.xml" {
		t.Errorf("URL 应为原始地址（categories 不入键）: %q", src.URL)
	}
	var cfg struct {
		Categories []string `json:"categories"`
	}
	if err := json.Unmarshal(src.Config, &cfg); err != nil {
		t.Fatalf("config 应包含 categories: %v", err)
	}
	if len(cfg.Categories) != 2 || cfg.Categories[0] != "Product" || cfg.Categories[1] != "Research" {
		t.Errorf("categories 不符: %v", cfg.Categories)
	}
}

func TestBuild_WebFeedCategoriesNotInKey(t *testing.T) {
	catJSON, _ := json.Marshal([]string{"Product"})
	a, _ := Build(Spec{Platform: "web", Capability: "feed", Params: map[string]string{"url": "https://openai.com/rss.xml"}})
	b, _ := Build(Spec{Platform: "web", Capability: "feed", Params: map[string]string{"url": "https://openai.com/rss.xml", "categories": string(catJSON)}})
	if a.URL != b.URL {
		t.Errorf("categories 不应影响幂等键: %q vs %q", a.URL, b.URL)
	}
}

func TestBuild_WebSearchCategoryInIdempotencyKey(t *testing.T) {
	a, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "AI", "category": "news"}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	b, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "AI", "category": "research paper"}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	c, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "AI"}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	if a.URL == b.URL || a.URL == c.URL || b.URL == c.URL {
		t.Errorf("不同 category 应生成不同幂等键: %q / %q / %q", a.URL, b.URL, c.URL)
	}
	a2, _ := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "AI", "category": "news"}})
	if a.URL != a2.URL {
		t.Errorf("同 (query,category) 应生成同一幂等键: %q vs %q", a.URL, a2.URL)
	}
}

func TestBuild_TrimsWhitespace(t *testing.T) {
	a, _ := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "AI"}})
	b, _ := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "  AI  "}})
	if a.URL != b.URL {
		t.Errorf("首尾空白应被归一化: %q vs %q", a.URL, b.URL)
	}
	if _, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": "   "}}); msg == "" {
		t.Error("全空白 query 应被拒绝")
	}
	if _, msg := Build(Spec{Platform: "xhs", Capability: "search", Params: map[string]string{"keyword": "\t \n"}}); msg == "" {
		t.Error("全空白 keyword 应被拒绝")
	}
}

func TestBuild_RejectsOverlongParams(t *testing.T) {
	long := strings.Repeat("长", maxSourceParamRunes+1)
	if _, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{"query": long}}); msg == "" {
		t.Error("超长 query 应被拒绝")
	}
	if _, msg := Build(Spec{Platform: "xhs", Capability: "search", Params: map[string]string{"keyword": long}}); msg == "" {
		t.Error("超长 keyword 应被拒绝")
	}
}

func TestBuild_XHSSearch(t *testing.T) {
	src, msg := Build(Spec{Platform: "xhs", Capability: "search", Params: map[string]string{"keyword": "AI 创业"}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	if src.Platform != types.PlatformXHS || src.Capability != types.CapSearch {
		t.Errorf("应为 xhs/search，实际 %s/%s", src.Platform, src.Capability)
	}
	if !strings.HasPrefix(src.URL, "vane://xhs/search?keyword=") {
		t.Errorf("合成 URL 前缀不符: %q", src.URL)
	}
	if src.Title != "小红书: AI 创业" {
		t.Errorf("默认 Title 不符: %q", src.Title)
	}
	if _, msg := Build(Spec{Platform: "xhs", Capability: "search", Params: map[string]string{}}); msg == "" {
		t.Error("缺 keyword 应被拒绝")
	}
}

func TestBuild_XUserPosts(t *testing.T) {
	src, msg := Build(Spec{Platform: "x", Capability: "user_posts", Params: map[string]string{"screen_name": "@elonmusk"}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	if src.Platform != types.PlatformX || src.Capability != types.CapUserPosts {
		t.Errorf("应为 x/user_posts，实际 %s/%s", src.Platform, src.Capability)
	}
	if !strings.HasPrefix(src.URL, "vane://x/user_posts?screen_name=") {
		t.Errorf("合成 URL 前缀不符: %q", src.URL)
	}
	if strings.Contains(src.URL, "%40") || strings.Contains(src.URL, "@") {
		t.Errorf("screen_name 中的 @ 应被去除: %q", src.URL)
	}
	if src.Title != "X: @elonmusk" {
		t.Errorf("默认 Title 不符: %q", src.Title)
	}
	if _, msg := Build(Spec{Platform: "x", Capability: "user_posts", Params: map[string]string{}}); msg == "" {
		t.Error("缺 screen_name 应被拒绝")
	}
}

func TestBuild_XHSUserPosts(t *testing.T) {
	const uid = "6a5578b3000000000e03cc00"
	src, msg := Build(Spec{Platform: "xhs", Capability: "user_posts", Params: map[string]string{"user_id": uid}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	if src.Platform != types.PlatformXHS || src.Capability != types.CapUserPosts {
		t.Errorf("应为 xhs/user_posts，实际 %s/%s", src.Platform, src.Capability)
	}
	if src.URL != "vane://xhs/user_posts?user_id="+uid {
		t.Errorf("合成 URL 不符: %q", src.URL)
	}
	if src.Title != "小红书用户: "+uid {
		t.Errorf("默认 Title 不符: %q", src.Title)
	}
	// config 里应存归一化后的 user_id。
	if !strings.Contains(string(src.Config), uid) {
		t.Errorf("config 未含 user_id: %s", src.Config)
	}

	// profile_url 应能抽出同一个 user_id，且幂等键与直填 user_id 完全相同
	// （契约 §5.2：同一博主无论怎么加，键必须相同）。
	src2, msg := Build(Spec{Platform: "xhs", Capability: "user_posts", Params: map[string]string{
		"profile_url": "https://www.xiaohongshu.com/user/profile/" + uid + "?xsec_token=abc",
	}})
	if msg != "" {
		t.Fatalf("profile_url 意外报错: %s", msg)
	}
	if src2.URL != src.URL {
		t.Errorf("profile_url 与 user_id 应产出同一幂等键，实际 %q vs %q", src2.URL, src.URL)
	}

	// 两者皆缺 → 拒绝。
	if _, msg := Build(Spec{Platform: "xhs", Capability: "user_posts", Params: map[string]string{}}); msg == "" {
		t.Error("缺 user_id / profile_url 应被拒绝")
	}
}

// TestBuild_XSearchUnavailable：x/search 在 sourcecatalog 里标记 Unavailable，
// Build 应直接回其 Reason 而非构造坏源，且 Reason 里应指路到 user_posts。
func TestBuild_XSearchUnavailable(t *testing.T) {
	_, msg := Build(Spec{Platform: "x", Capability: "search", Params: map[string]string{"query": "AI"}})
	if msg == "" {
		t.Fatal("x/search 应被拒绝（Unavailable）")
	}
	if !strings.Contains(msg, "user_posts") {
		t.Errorf("拒绝理由应指路 user_posts，实际: %s", msg)
	}
}

func TestBuild_UnknownPlatform(t *testing.T) {
	if _, msg := Build(Spec{Platform: "carrier_pigeon", Capability: "user_posts"}); msg == "" {
		t.Error("未知 platform 应被拒绝")
	}
}

func TestBuildLegacy_RSS(t *testing.T) {
	src, msg := BuildLegacy("rss", "https://example.com/feed.xml", "", "", "", "")
	if msg != "" {
		t.Fatalf("合法 rss legacy 请求不应报错: %s", msg)
	}
	if src.Platform != types.PlatformWeb || src.Capability != types.CapFeed {
		t.Errorf("legacy rss 应映射到 web/feed，实际 %s/%s", src.Platform, src.Capability)
	}
}

func TestBuildLegacy_EmptyTypeDefaultsToRSS(t *testing.T) {
	src, msg := BuildLegacy("", "https://example.com/feed.xml", "", "", "", "")
	if msg != "" {
		t.Fatalf("type 为空应默认 rss: %s", msg)
	}
	if src.Platform != types.PlatformWeb || src.Capability != types.CapFeed {
		t.Errorf("空 type 应映射到 web/feed，实际 %s/%s", src.Platform, src.Capability)
	}
}

func TestBuildLegacy_Exa(t *testing.T) {
	src, msg := BuildLegacy("exa", "", "AI", "", "", "news")
	if msg != "" {
		t.Fatalf("合法 exa legacy 请求不应报错: %s", msg)
	}
	if src.Platform != types.PlatformWeb || src.Capability != types.CapSearch {
		t.Errorf("legacy exa 应映射到 web/search，实际 %s/%s", src.Platform, src.Capability)
	}
}

func TestBuildLegacy_TikHubXHS(t *testing.T) {
	src, msg := BuildLegacy("tikhub_xhs", "", "", "AI 创业", "", "")
	if msg != "" {
		t.Fatalf("合法 tikhub_xhs legacy 请求不应报错: %s", msg)
	}
	if src.Platform != types.PlatformXHS || src.Capability != types.CapSearch {
		t.Errorf("legacy tikhub_xhs 应映射到 xhs/search，实际 %s/%s", src.Platform, src.Capability)
	}
}

func TestBuildLegacy_UnknownType(t *testing.T) {
	if _, msg := BuildLegacy("carrier_pigeon", "https://x.com", "", "", "", ""); msg == "" {
		t.Error("未知 type 应被拒绝")
	}
}

// page_watch 已下线（改由 Exa fetch 覆盖）：web 平台不再接受该能力，Build 应拒绝。
func TestBuild_WebPageWatchRemoved(t *testing.T) {
	if _, msg := Build(Spec{
		Platform:   "web",
		Capability: "page_watch",
		Params:     map[string]string{"url": "https://example.com/pricing"},
	}); msg == "" {
		t.Error("page_watch 已下线，web/page_watch 请求应被拒绝")
	}
}
