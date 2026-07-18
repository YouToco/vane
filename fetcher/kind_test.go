package fetcher

// 本文件守卫 M6 契约 §7.2(b) 的 Kind 语义：Kind 由各抓取器在**构造 item 处**显式
// 赋值，finalize 只校验不兜底。钉死两件事：
//   1. 四个 article 抓取器（rss/exa/tikhub_xhs/x）产出的每条 item 都是 Kind=article；
//   2. finalize 对空 Kind 一律拒绝（单条跳过，不炸整批）。
// 背景是 2026-07-16 生产事故：008 加列后全链路无人赋值，Go 零值 "" 被显式 INSERT
// 覆盖 DB 的 DEFAULT 'article'，全部新内容 kind 落成空串（存量由 012 回填）。
// 这组用例保证同类污染在单测层就变红，而不是等 DB 里长出脏数据才被发现。

import (
	"encoding/json"
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

func TestMapTikhubNotes_ProducesKindArticle(t *testing.T) {
	items := mapTikhubNotes(xhsSource(1), []tikhubSearchItem{
		{ModelType: "note", Note: &tikhubNote{ID: xhsNoteID, Title: "笔记", Desc: "正文"}},
	})
	requireAllKindArticle(t, items)
}

// TestTweetToItem_ProducesKindArticle 覆盖 tweetToItem 的**两个**构造分支：
// 转推拆包分支与原创分支各有一个独立的 ContentItem 字面量，漏赋任何一个都会让
// 那一类推文带着空 Kind 入库。
func TestTweetToItem_ProducesKindArticle(t *testing.T) {
	original := twitterTweet{
		TweetID:   "1001",
		Text:      "原创推文",
		CreatedAt: "Thu Jul 16 12:00:00 +0000 2026",
		Author:    twitterAuthor{ScreenName: "someone"},
	}
	retweet := twitterTweet{
		TweetID:   "1002",
		Text:      "RT @orig: 截断的 140 字",
		CreatedAt: "Thu Jul 16 12:00:01 +0000 2026",
		Author:    twitterAuthor{ScreenName: "reposter"},
		RetweetedTweet: json.RawMessage(
			`{"tweet_id":"2001","text":"被转推的全文","created_at":"Thu Jul 16 11:00:00 +0000 2026","author":{"screen_name":"orig"}}`),
	}

	for _, tc := range []struct {
		name string
		tw   twitterTweet
	}{
		{"原创分支", original},
		{"转推拆包分支", retweet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := tweetToItem(tc.tw, "fallback")
			if item == nil {
				t.Fatal("fixture 推文不该被丢弃")
			}
			if item.Kind != types.KindArticle {
				t.Errorf("Kind 应为 %q，实际 %q", types.KindArticle, item.Kind)
			}
		})
	}
}

// TestCatalogKindMatchesFetcherEmittedKind 是 sourcecatalog 登记的 Kind 与各抓取器**实际
// 产出**的 Kind 之间的一致性锁（sourcecatalog.go 的 Entry.Kind 注释所承诺的那把锁）。
//
// 为什么单靠 requireAllKindArticle + sourcecatalog_test 的"非空"断言不够（对抗审查 CONFIRMED）：
// 前者把 fetcher 产出钉在硬编码的 KindArticle，后者只查 catalog 里 Kind 非空——两头各自钉在
// 同一个常量上，却从不互相比对。有人把某个 article 能力的 catalog Kind 误改成 change，两处测试
// 照样全绿，catalog 与真实产出静默漂移。本用例对每个 article 能力跑其 map 函数、拿产出的 Kind
// 与 sourcecatalog.KindOf 逐条比，把这条漂移路径焊死。
func TestCatalogKindMatchesFetcherEmittedKind(t *testing.T) {
	cases := []struct {
		platform   types.Platform
		capability types.Capability
		items      []types.ContentItem
	}{
		{types.PlatformWeb, types.CapFeed, mapItems(rssSource(1),
			[]*gofeed.Item{{Link: bbcURL, Title: "标题", Content: "正文"}})},
		{types.PlatformWeb, types.CapSearch, mapExaResults(exaSource(1),
			[]exaResult{{ID: "exa-1", URL: "https://news.example/a", Title: "标题", Text: "正文"}})},
		{types.PlatformXHS, types.CapSearch, mapTikhubNotes(xhsSource(1),
			[]tikhubSearchItem{{ModelType: "note", Note: &tikhubNote{ID: xhsNoteID, Title: "笔记", Desc: "正文"}}})},
		{types.PlatformXHS, types.CapUserPosts, mapXHSUserNotes(
			types.Source{ID: 1, Platform: types.PlatformXHS, Capability: types.CapUserPosts}, "u1",
			[]xhsUserNote{{ID: xhsNoteID, Title: "笔记", Desc: "正文", User: xhsUserAuthor{UserID: "u1"}}})},
	}
	for _, tc := range cases {
		want, ok := sourcecatalog.KindOf(tc.platform, tc.capability)
		if !ok {
			t.Errorf("%s/%s 在 sourcecatalog 里不可用，无从比对 Kind", tc.platform, tc.capability)
			continue
		}
		if len(tc.items) == 0 {
			t.Fatalf("%s/%s 的 fixture 未产出 item，断言空洞", tc.platform, tc.capability)
		}
		for i, it := range tc.items {
			if it.Kind != want {
				t.Errorf("%s/%s 第 %d 条产出 Kind=%q，但 sourcecatalog.KindOf=%q，二者漂移",
					tc.platform, tc.capability, i, it.Kind, want)
			}
		}
	}

	// x/user_posts 走 tweetToItem（无独立 map 函数），单独比对。
	tw := tweetToItem(twitterTweet{
		TweetID: "1", Text: "推文", CreatedAt: "Thu Jul 16 12:00:00 +0000 2026",
		Author: twitterAuthor{ScreenName: "a"},
	}, "a")
	wantX, ok := sourcecatalog.KindOf(types.PlatformX, types.CapUserPosts)
	if tw == nil {
		t.Fatal("fixture 推文不该被丢弃")
	} else if !ok || tw.Kind != wantX {
		t.Errorf("x/user_posts 产出 Kind=%q，sourcecatalog.KindOf=%q(ok=%v)，二者漂移", tw.Kind, wantX, ok)
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
