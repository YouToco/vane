package sourcespec

import (
	"encoding/json"
	"testing"
)

// 不变量 I-S2：**凡是影响抓取结果集的入参，都必须改变幂等键（url）。**
//
// 为什么这条值得一个专门的守卫，而不是靠 review 记住：
//
// sources 是跨租户共享的客观事实表（不变量 I-T1），一行 source 由 url 唯一确定。
// 如果两组语义不同的入参映射到同一个 url，它们就被迫共用一行——于是"谁后写谁说了算"，
// 一个用户的抓取意图被另一个用户支配。而这个失败**完全静默**：不报错、不崩溃，
// 只是某人的信源从此抓回来的是别人要的东西。
//
// 这正是 categories 曾经的样子：web/feed 的 url 是真实 RSS 地址、不含 categories，
// 于是 A 要 [ai]、B 要 [区块链]，后者一订阅就把前者的过滤条件改掉了。
//
// 本测试逐条列出"会改变结果集的入参"，断言改动它必然改变 url。新增这类入参时，
// 忘了进幂等键就会在这里红——而不是等到线上有人发现自己的信源不对劲。
func TestInvariant_FetchAffectingParamsChangeIdempotencyKey(t *testing.T) {
	cases := []struct {
		name     string
		platform string
		cap      string
		base     map[string]string
		// variant 与 base 只差一个"会改变抓取结果集"的入参。
		variant map[string]string
	}{
		{
			name:     "web/feed 的 categories（过滤掉一部分条目）",
			platform: "web", cap: "feed",
			base:    map[string]string{"url": "https://example.com/rss.xml"},
			variant: map[string]string{"url": "https://example.com/rss.xml", "categories": `["ai"]`},
		},
		{
			name:     "web/feed 的 categories 取不同集合",
			platform: "web", cap: "feed",
			base:    map[string]string{"url": "https://example.com/rss.xml", "categories": `["ai"]`},
			variant: map[string]string{"url": "https://example.com/rss.xml", "categories": `["blockchain"]`},
		},
		{
			name: "web/search 的 query", platform: "web", cap: "search",
			base:    map[string]string{"query": "AI"},
			variant: map[string]string{"query": "区块链"},
		},
		{
			name: "web/search 的 category", platform: "web", cap: "search",
			base:    map[string]string{"query": "AI"},
			variant: map[string]string{"query": "AI", "category": "news"},
		},
		{
			name: "web/search 的 include_domains", platform: "web", cap: "search",
			base:    map[string]string{"query": "AI"},
			variant: map[string]string{"query": "AI", "include_domains": `["anthropic.com"]`},
		},
		{
			name: "web/contents 的 url", platform: "web", cap: "contents",
			base:    map[string]string{"url": "https://example.com/a"},
			variant: map[string]string{"url": "https://example.com/b"},
		},
		{
			name: "x/user_posts 的 screen_name", platform: "x", cap: "user_posts",
			base:    map[string]string{"screen_name": "AnthropicAI"},
			variant: map[string]string{"screen_name": "OpenAI"},
		},
		{
			name: "xhs/search 的 keyword", platform: "xhs", cap: "search",
			base:    map[string]string{"keyword": "手冲咖啡"},
			variant: map[string]string{"keyword": "机械键盘"},
		},
		{
			name: "xhs/user_posts 的 user_id", platform: "xhs", cap: "user_posts",
			base:    map[string]string{"user_id": "aaa"},
			variant: map[string]string{"user_id": "bbb"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := mustBuild(t, tc.platform, tc.cap, tc.base)
			b := mustBuild(t, tc.platform, tc.cap, tc.variant)
			if a.URL == b.URL {
				t.Errorf("不变量 I-S2 被破坏：改变会影响抓取结果集的入参后 url 没变，"+
					"两组语义不同的配置将共用同一行 source，后写者会静默改掉前写者的抓取行为。\n"+
					"  url = %s\n  base    config = %s\n  variant config = %s",
					a.URL, a.Config, b.Config)
			}
		})
	}
}

// TestCategoriesNormalizationMatchesMatcher 锁住 categories 的归一化口径。
//
// 幂等键里的归一化必须与 fetcher.applyCategories 的匹配口径（ToLower+TrimSpace）一致：
//   - 若键比匹配更严（如键区分大小写），["AI"] 与 ["ai"] 会建出两行 source，
//     抓取行为却完全相同——白白重复抓取、重复付费。
//   - 若键比匹配更松，两组行为不同的配置会共用一行——就是 I-S2 要防的那个洞。
//
// 排序同理：分类是无序集合，集合相同、顺序不同必须是同一个源。
func TestCategoriesNormalizationMatchesMatcher(t *testing.T) {
	const rss = "https://example.com/rss.xml"
	canonical := mustBuild(t, "web", "feed", map[string]string{
		"url": rss, "categories": `["ai","research"]`,
	})

	equivalents := []struct {
		name  string
		input string
	}{
		{"大小写不同", `["AI","Research"]`},
		{"顺序不同", `["research","ai"]`},
		{"前后空白", `["  ai  ","research"]`},
		{"含重复项", `["ai","research","ai"]`},
		{"含空串", `["ai","","research"]`},
	}
	for _, eq := range equivalents {
		t.Run(eq.name, func(t *testing.T) {
			got := mustBuild(t, "web", "feed", map[string]string{"url": rss, "categories": eq.input})
			if got.URL != canonical.URL {
				t.Errorf("行为等价的分类集合产生了不同幂等键 ⇒ 会重复建源、重复抓取\n  期望 %s\n  实得 %s",
					canonical.URL, got.URL)
			}
			if string(got.Config) != string(canonical.Config) {
				t.Errorf("config 未归一化到同一形态\n  期望 %s\n  实得 %s", canonical.Config, got.Config)
			}
		})
	}

	// 无分类时 url 必须与原始地址逐字节相同——否则存量 feed 源的幂等键会漂移，
	// 上线当天所有既有 RSS 源都会被判成新源、重新建行。
	plain := mustBuild(t, "web", "feed", map[string]string{"url": rss})
	if plain.URL != rss {
		t.Errorf("无 categories 时 url 必须保持原样，实得 %q（存量源幂等键会漂移）", plain.URL)
	}

	// 判别位放在 fragment 里：Go 的 http 客户端不会把它发到线上（RequestURI() 不含
	// Fragment），所以 url 列既是幂等键、又仍是可直接抓取的真实地址。
	var cfg struct {
		Categories []string `json:"categories"`
	}
	if err := json.Unmarshal(canonical.Config, &cfg); err != nil {
		t.Fatalf("解析 config 失败: %v", err)
	}
	if len(cfg.Categories) != 2 || cfg.Categories[0] != "ai" || cfg.Categories[1] != "research" {
		t.Errorf("config.categories 未按「小写+升序」归一化，实得 %v", cfg.Categories)
	}
}

// TestCategoriesMalformedIsRejected：打错的 categories 必须报错，而不是静默当成「不过滤」。
// 旧实现 `if json.Unmarshal(...) == nil` 会把解析失败当无事发生——用户以为设了过滤、
// 实际收到全量，且没有任何提示。
func TestCategoriesMalformedIsRejected(t *testing.T) {
	_, errMsg := Build(Spec{
		Platform: "web", Capability: "feed",
		Params: map[string]string{"url": "https://example.com/rss.xml", "categories": `ai,research`},
	})
	if errMsg == "" {
		t.Error("categories 传了非 JSON 数组却被静默忽略——用户会以为过滤生效了")
	}
}

func mustBuild(t *testing.T, platform, capability string, params map[string]string) *struct {
	URL    string
	Config []byte
} {
	t.Helper()
	src, errMsg := Build(Spec{Platform: platform, Capability: capability, Params: params})
	if errMsg != "" {
		t.Fatalf("Build(%s/%s, %v) 失败: %s", platform, capability, params, errMsg)
	}
	return &struct {
		URL    string
		Config []byte
	}{URL: src.URL, Config: src.Config}
}
