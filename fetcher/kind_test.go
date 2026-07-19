package fetcher

// 本文件守卫 M6 契约 §7.2(b) 的 Kind 语义：Kind 由抓取器在**构造 item 处**显式
// 赋值，finalize 只校验不兜底。钉死两件事：
//   1. 每个 Available 能力实际产出的 Kind 与 sourcecatalog 登记一致（一致性锁）；
//   2. finalize 对空 Kind 一律拒绝（单条跳过，不炸整批）。
// 背景是 2026-07-16 生产事故：008 加列后全链路无人赋值，Go 零值 "" 被显式 INSERT
// 覆盖 DB 的 DEFAULT 'article'，全部新内容 kind 落成空串（存量由 012 回填）。
//
// 绑定引擎迁移（2026-07-18）后，TikHub 系能力的产出统一走 BindingFetcher：一致性锁
// 对这些能力改为「模板 Kind vs 登记 Kind」（TestBindingTemplates_Integrity 已锁）+
// 「引擎真产出 vs 登记 Kind」（本文件，走 fixture 全链路，防模板对了引擎丢字段）。

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/mmcdole/gofeed"

	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/types"
)

// requireAllKindArticle 断言产出的每条 item 都是 Kind=article。空批直接 Fatal：
// 空产出会让"全部都是 article"空洞地为真（vacuous truth），必须先排除。
func requireAllKindArticle(t *testing.T, items []types.ContentItem) {
	t.Helper()
	if len(items) == 0 {
		t.Fatal("fixture 未产出任何 item，断言无从谈起（检查 fixture 是否被上游过滤）")
	}
	for i, it := range items {
		if it.Kind != types.KindArticle {
			t.Errorf("第 %d 条 Kind 应为 %q，实际 %q（空串会覆盖 DB 的 DEFAULT 'article'）",
				i, types.KindArticle, it.Kind)
		}
	}
}

func TestMapItems_RSSProducesKindArticle(t *testing.T) {
	items := mapItems(rssSource(1), []*gofeed.Item{
		{Link: bbcURL, Title: "标题", Content: "正文"},
	})
	requireAllKindArticle(t, items)
}

func TestMapExaResults_ProducesKindArticle(t *testing.T) {
	items := mapExaResults(exaSource(1), []exaResult{
		{ID: "exa-1", URL: "https://news.example/a", Title: "标题", Text: "正文"},
	})
	requireAllKindArticle(t, items)
}

// TestCatalogKindMatchesFetcherEmittedKind 是 sourcecatalog 登记的 Kind 与各抓取器
// **实际产出**的 Kind 之间的一致性锁。手写 map 函数的能力（rss/exa/contents）直接调
// map 函数；绑定能力走引擎全链路（fixture 假服务端），确保「模板声明的 Kind」真的
// 落到了产出条目上，而不是只在声明层一致。
func TestCatalogKindMatchesFetcherEmittedKind(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathSearch] = sampleTikhubResponse
	up.bodies[pathUserPost] = sampleXHSUserResponse
	up.bodies[pathTwitter] = sampleTwitterResponse
	up.bodies[pathHotList] = sampleHotListResponse
	up.bodies[pathTopic] = sampleTopicFeedResponse
	up.bodies[pathFaved] = sampleFavedNotesResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()
	b := newTestBinding(srv.URL, nil, nil)

	cases := []struct {
		platform   types.Platform
		capability types.Capability
		items      func(t *testing.T) []types.ContentItem
	}{
		{types.PlatformWeb, types.CapFeed, func(*testing.T) []types.ContentItem {
			return mapItems(rssSource(1), []*gofeed.Item{{Link: bbcURL, Title: "标题", Content: "正文"}})
		}},
		{types.PlatformWeb, types.CapSearch, func(*testing.T) []types.ContentItem {
			return mapExaResults(exaSource(1), []exaResult{{ID: "exa-1", URL: "https://news.example/a", Title: "标题", Text: "正文"}})
		}},
		{types.PlatformWeb, types.CapContents, func(*testing.T) []types.ContentItem {
			return contentsItems(
				types.Source{ID: 1, Platform: types.PlatformWeb, Capability: types.CapContents},
				[]exaContentsResult{{ID: "c1", URL: "https://x.example/pricing", Title: "定价", Text: "正文"}})
		}},
		{types.PlatformXHS, types.CapSearch, bindingItems(b, types.PlatformXHS, types.CapSearch, `{"keyword":"k"}`)},
		{types.PlatformXHS, types.CapUserPosts, bindingItems(b, types.PlatformXHS, types.CapUserPosts, `{"user_id":"6a5578b3000000000e03cc00"}`)},
		{types.PlatformX, types.CapUserPosts, bindingItems(b, types.PlatformX, types.CapUserPosts, `{"screen_name":"OpenAI"}`)},
		{types.PlatformXHS, types.CapHotList, bindingItems(b, types.PlatformXHS, types.CapHotList, `{}`)},
		{types.PlatformXHS, types.CapTopicFeed, bindingItems(b, types.PlatformXHS, types.CapTopicFeed, `{"page_id":"6301c499df9bea0001dc6f47"}`)},
		{types.PlatformXHS, types.CapFavedNotes, bindingItems(b, types.PlatformXHS, types.CapFavedNotes, `{"user_id":"69bfda630000000034019ee8"}`)},
	}
	covered := map[string]bool{}
	for _, tc := range cases {
		covered[string(tc.platform)+"/"+string(tc.capability)] = true
		want, ok := sourcecatalog.KindOf(tc.platform, tc.capability)
		if !ok {
			t.Errorf("%s/%s 在 sourcecatalog 里不可用，无从比对 Kind", tc.platform, tc.capability)
			continue
		}
		items := tc.items(t)
		if len(items) == 0 {
			t.Fatalf("%s/%s 的 fixture 未产出 item，断言空洞", tc.platform, tc.capability)
		}
		for i, it := range items {
			if it.Kind != want {
				t.Errorf("%s/%s 第 %d 条产出 Kind=%q，但 sourcecatalog.KindOf=%q，二者漂移",
					tc.platform, tc.capability, i, it.Kind, want)
			}
		}
	}
	// 完备性：sourcecatalog 每个 Available 能力都必须有比对用例——新增能力忘了配
	// fixture 时在此变红，而不是让一致性锁静默漏一个能力。
	for _, e := range sourcecatalog.List() {
		if e.Available() && !covered[string(e.Platform)+"/"+string(e.Capability)] {
			t.Errorf("Available 能力 %s/%s 没有 Kind 一致性比对用例", e.Platform, e.Capability)
		}
	}
}

// bindingItems 用引擎全链路产出条目（fixture 假服务端）。
func bindingItems(b *BindingFetcher, p types.Platform, c types.Capability, cfg string) func(t *testing.T) []types.ContentItem {
	return func(t *testing.T) []types.ContentItem {
		t.Helper()
		items, err := b.Fetch(context.Background(), types.Source{
			ID: 1, Platform: p, Capability: c, Config: json.RawMessage(cfg),
		})
		if err != nil {
			t.Fatalf("%s/%s 引擎抓取失败: %v", p, c, err)
		}
		return items
	}
}

// TestFinalize_DropsItemWithoutKind 固定契约 §7.2(b)：Kind 空即拒，粒度是**单条**
// （返回 false、调用方 continue），不把整批打死——批里其余合法条目不该陪葬。
// item 带合法身份（url），确保拒绝确实发生在 Kind 校验、而非更早的身份校验。
func TestFinalize_DropsItemWithoutKind(t *testing.T) {
	src := rssSource(1)
	item := types.ContentItem{SourceID: 1, URL: bbcURL, Title: "有身份没 kind", Content: "正文"}

	if finalize(src, &item) {
		t.Fatalf("空 Kind 的条目必须被拒绝（否则空串覆盖 DB DEFAULT，重演 012 回填的污染），实得 Kind=%q", item.Kind)
	}
	if item.CanonicalKey == "" {
		t.Error("拒绝应发生在 Kind 校验（身份校验之后）——canonical_key 为空说明死在了身份校验，用例没测到目标分支")
	}
}

// contentsItems 把 mapExaContents 的 (item, ok) 适配成切片，供 kind 一致性锁复用。
func contentsItems(src types.Source, results []exaContentsResult) []types.ContentItem {
	it, ok := mapExaContents(src, "https://x.example/pricing", "", results)
	if !ok {
		return nil
	}
	return []types.ContentItem{it}
}
