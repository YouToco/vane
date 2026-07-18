package sourcespec

import (
	"encoding/json"
	"net/url"
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
	// 2026-07-18 起 categories **入键**，承载于 URL fragment（契约 §5.2【死结已解开】）：
	// 原设计让同一 feed 的不同过滤共用一行 source，后订阅者会静默改掉先订阅者的过滤条件；
	// 单 owner 下靠"回报配置变更"缓解，多租户下回报够不着受害者（被告知的是覆盖者）。
	// fragment 承载判别位，既进幂等键、又不破坏前端 <a href> 与 fetcher 抓取。
	if src.URL != "https://openai.com/news/rss.xml#vane-categories=product,research" {
		t.Errorf("URL 应为原始地址 + categories 判别位: %q", src.URL)
	}
	var cfg struct {
		Categories []string `json:"categories"`
	}
	if err := json.Unmarshal(src.Config, &cfg); err != nil {
		t.Fatalf("config 应包含 categories: %v", err)
	}
	// 归一化为小写+升序，与 fetcher.applyCategories 的匹配口径（ToLower+TrimSpace）对齐。
	if len(cfg.Categories) != 2 || cfg.Categories[0] != "product" || cfg.Categories[1] != "research" {
		t.Errorf("categories 应归一化为小写+升序: %v", cfg.Categories)
	}
}

// TestBuild_WebFeedCategoriesInKey 取代原 TestBuild_WebFeedCategoriesNotInKey。
//
// 原用例断言 categories **不**入键——那是契约 §5.2 记录在案的【结构性死结】的产物，
// 2026-07-18 推翻：该取舍在单 owner 下成立，多租户下 B 会静默改掉 A 的过滤条件，
// 而当时用以缓解的"回报配置变更"只到得了覆盖者、到不了受害者。原取舍权衡的两条代价
// （前端死链、fetcher 需另取地址）都只针对合成 vane:// url，fragment 方案两条都不沾。
func TestBuild_WebFeedCategoriesInKey(t *testing.T) {
	catJSON, _ := json.Marshal([]string{"Product"})
	a, _ := Build(Spec{Platform: "web", Capability: "feed", Params: map[string]string{"url": "https://openai.com/rss.xml"}})
	b, _ := Build(Spec{Platform: "web", Capability: "feed", Params: map[string]string{"url": "https://openai.com/rss.xml", "categories": string(catJSON)}})
	if a.URL == b.URL {
		t.Errorf("categories 必须影响幂等键，否则两份过滤器共用一行 source、后写者赢: %q", a.URL)
	}
	// 判别位之外的部分必须仍是那个真实地址：前端 <a href> 不能变死链，fetcher 照抓。
	if got, want := a.URL, "https://openai.com/rss.xml"; got != want {
		t.Errorf("无 categories 时 url 必须逐字节保持原样（存量源幂等键不得漂移）: %q", got)
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

// TestBuild_WebSearchIncludeDomains 守 D-2：include_domains 进 config（JSON 数组，与
// exa.go 消费端对齐）+ 进幂等键（§5.2 规则 B）；集合语义（排序/去重/大小写归一）。
func TestBuild_WebSearchIncludeDomains(t *testing.T) {
	mk := func(domainsJSON string) *types.Source {
		p := map[string]string{"query": "Claude release"}
		if domainsJSON != "" {
			p["include_domains"] = domainsJSON
		}
		src, msg := Build(Spec{Platform: "web", Capability: "search", Params: p})
		if msg != "" {
			t.Fatalf("意外报错: %s", msg)
		}
		return src
	}

	// (a) config 含 include_domains 数组（供 exa.go 消费）+ 进幂等键。
	src := mk(`["anthropic.com","claude.com"]`)
	var cfg struct {
		Query          string   `json:"query"`
		IncludeDomains []string `json:"include_domains"`
	}
	if err := json.Unmarshal(src.Config, &cfg); err != nil {
		t.Fatalf("config 解析失败: %v", err)
	}
	if len(cfg.IncludeDomains) != 2 || cfg.IncludeDomains[0] != "anthropic.com" || cfg.IncludeDomains[1] != "claude.com" {
		t.Errorf("config.include_domains 不符（应排序）: %v", cfg.IncludeDomains)
	}
	if !strings.Contains(src.URL, "include_domains=") {
		t.Errorf("幂等键应含 include_domains: %q", src.URL)
	}

	// (b) 有域名 vs 无域名 = 不同源。
	if none := mk(""); none.URL == src.URL {
		t.Errorf("有/无 include_domains 应产出不同幂等键: %q vs %q", none.URL, src.URL)
	}

	// (c) 同 query 不同域名集 = 不同源（解药不会被静默抹掉）。
	if other := mk(`["openai.com"]`); other.URL == src.URL {
		t.Errorf("不同 include_domains 应产出不同幂等键: %q vs %q", other.URL, src.URL)
	}

	// (d) 集合语义：顺序不同 + 大小写不同 + 重复项 → 同一幂等键。
	if reordered := mk(`["Claude.com","ANTHROPIC.com","claude.com"]`); reordered.URL != src.URL {
		t.Errorf("域名集合相同（乱序/大小写/去重后）应产出同一幂等键: %q vs %q", reordered.URL, src.URL)
	}

	// (e) 参数顺序：q < category < include_domains（§5.2 规则 B 不可重排）。
	full, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{
		"query": "AI", "category": "news", "include_domains": `["anthropic.com"]`,
	}})
	if msg != "" {
		t.Fatalf("意外报错: %s", msg)
	}
	qi := strings.Index(full.URL, "q=")
	ci := strings.Index(full.URL, "category=")
	di := strings.Index(full.URL, "include_domains=")
	if !(qi >= 0 && ci > qi && di > ci) {
		t.Errorf("幂等键参数顺序应为 q<category<include_domains: %q", full.URL)
	}
}

// TestBuild_WebSearchIncludeDomainsRejectsBadJSON 守非法入参可见拒绝（不静默丢解药）。
func TestBuild_WebSearchIncludeDomainsRejectsBadJSON(t *testing.T) {
	if _, msg := Build(Spec{Platform: "web", Capability: "search", Params: map[string]string{
		"query": "AI", "include_domains": "anthropic.com", // 裸串非 JSON 数组
	}}); msg == "" {
		t.Error("非 JSON 数组的 include_domains 应被拒绝")
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

func TestBuild_WebContents(t *testing.T) {
	src, msg := Build(Spec{Platform: "web", Capability: "contents",
		Params: map[string]string{"url": "https://x.com/pricing", "title": "定价监控"}})
	if msg != "" {
		t.Fatalf("合法 web/contents 请求不应报错: %s", msg)
	}
	if src.Platform != types.PlatformWeb || src.Capability != types.CapContents {
		t.Errorf("应映射到 web/contents，实际 %s/%s", src.Platform, src.Capability)
	}
	if src.URL != "vane://web/contents?url="+url.QueryEscape("https://x.com/pricing") {
		t.Errorf("幂等键不符: %s", src.URL)
	}
	var cfg map[string]string
	if json.Unmarshal(src.Config, &cfg); cfg["url"] != "https://x.com/pricing" || cfg["title"] != "定价监控" {
		t.Errorf("config 不符: %s", src.Config)
	}
	// 缺 url 应报错。
	if _, msg := Build(Spec{Platform: "web", Capability: "contents", Params: map[string]string{}}); msg == "" {
		t.Error("web/contents 缺 url 应被拒绝")
	}
	// 非法 url 应报错。
	if _, msg := Build(Spec{Platform: "web", Capability: "contents", Params: map[string]string{"url": "not-a-url"}}); msg == "" {
		t.Error("web/contents 非法 url 应被拒绝")
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
