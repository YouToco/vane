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
