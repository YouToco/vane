package fetcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// sampleTwitterResponse 按契约 §9 实测报文字段构造（含原创 + 转推 + 引用 + 回复）。
const sampleTwitterResponse = `{
  "code": 200,
  "data": {
    "status": "ok",
    "pinned": [
      {"tweet_id": "pin1", "text": "pinned tweet", "created_at": "Thu Jul 10 12:00:00 +0000 2026", "author": {"screen_name": "OpenAI"}}
    ],
    "timeline": [
      {
        "tweet_id": "t1",
        "text": "We are releasing GPT-5 today. Check it out!",
        "created_at": "Wed Jul 15 17:30:00 +0000 2026",
        "conversation_id": "t1",
        "views": "93400",
        "author": {"screen_name": "OpenAI", "name": "OpenAI", "rest_id": "123"},
        "media": [],
        "entities": []
      },
      {
        "tweet_id": "t2",
        "text": "RT @claudeai: We're introducing Claude for Te...",
        "created_at": "Wed Jul 15 16:00:00 +0000 2026",
        "conversation_id": "t2",
        "retweeted": true,
        "retweeted_tweet": {
          "tweet_id": "rt_orig_1",
          "text": "We're introducing Claude for Teachers — a free tool designed to help educators bring AI into the classroom responsibly.",
          "created_at": "Wed Jul 15 14:00:00 +0000 2026",
          "author": {"screen_name": "claudeai", "name": "Claude", "rest_id": "456"},
          "media": {},
          "entities": {}
        },
        "author": {"screen_name": "AnthropicAI", "name": "Anthropic", "rest_id": "789"},
        "media": [],
        "entities": []
      },
      {
        "tweet_id": "t3",
        "text": "This is a great analysis of our latest paper.",
        "created_at": "Wed Jul 15 15:00:00 +0000 2026",
        "quoted": {
          "tweet_id": "q1",
          "text": "Original quoted tweet text"
        },
        "author": {"screen_name": "OpenAI"},
        "media": [],
        "entities": []
      },
      {
        "tweet_id": "t4",
        "text": "More details in this thread...",
        "created_at": "Wed Jul 15 17:30:01 +0000 2026",
        "reply_to": "t1",
        "author": {"screen_name": "OpenAI"},
        "media": [],
        "entities": []
      }
    ]
  }
}`

func newTestTwitter(srvURL string) *TwitterFetcher {
	f := NewTwitter(config.FetchConfig{
		TimeoutSeconds: 10,
		MaxResponseMB:  1,
		TikhubAPIKey:   "test-key",
	})
	f.baseURL = srvURL
	return f
}

func twitterSrc(id int64, screenName string) types.Source {
	cfg, _ := json.Marshal(map[string]string{"screen_name": screenName})
	return types.Source{
		ID:         id,
		Platform:   types.PlatformX,
		Capability: types.CapUserPosts,
		Config:     cfg,
	}
}

func TestTwitterFetch_MapsTimeline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("缺少 Bearer 鉴权头")
		}
		if r.URL.Query().Get("screen_name") != "OpenAI" {
			t.Errorf("screen_name 参数不符: %s", r.URL.Query().Get("screen_name"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(sampleTwitterResponse))
	}))
	defer srv.Close()

	f := newTestTwitter(srv.URL)
	items, err := f.Fetch(context.Background(), twitterSrc(1, "OpenAI"))
	if err != nil {
		t.Fatalf("Fetch 出错: %v", err)
	}

	// 4 条 timeline → 4 个 item（pinned 被忽略）
	if len(items) != 4 {
		t.Fatalf("应返回 4 条，实际 %d", len(items))
	}

	// t1: 原创
	if items[0].ExternalID != "t1" {
		t.Errorf("第 1 条 ExternalID 应为 t1: %s", items[0].ExternalID)
	}
	if items[0].SourceID != 1 {
		t.Errorf("SourceID 应为 1: %d", items[0].SourceID)
	}
	if items[0].CanonicalKey != "t1" {
		t.Errorf("CanonicalKey 应为 tweet_id: %s", items[0].CanonicalKey)
	}
	if items[0].Author != "OpenAI" {
		t.Errorf("第 1 条 Author 应为 OpenAI: %s", items[0].Author)
	}
	if items[0].URL != "https://x.com/OpenAI/status/t1" {
		t.Errorf("第 1 条 URL 不符: %s", items[0].URL)
	}
	if items[0].PublishedAt == nil {
		t.Error("第 1 条 PublishedAt 应被解析")
	}

	// t2: 转推——应拆包取被转推那条的数据（§9.4）
	if items[1].ExternalID != "rt_orig_1" {
		t.Errorf("转推应拆包到被转推的 tweet_id: got %s", items[1].ExternalID)
	}
	if items[1].Author != "claudeai" {
		t.Errorf("转推拆包后 Author 应为原作者 claudeai: got %s", items[1].Author)
	}
	if items[1].Content != "We're introducing Claude for Teachers — a free tool designed to help educators bring AI into the classroom responsibly." {
		t.Errorf("转推拆包后 Content 应为被转推的全文，实际被截断或不符")
	}
	if items[1].URL != "https://x.com/claudeai/status/rt_orig_1" {
		t.Errorf("转推 URL 应指向原作者: %s", items[1].URL)
	}

	// t3: 引用推文——保留原推内容
	if items[2].ExternalID != "t3" {
		t.Errorf("引用推文应保留自身 tweet_id: got %s", items[2].ExternalID)
	}

	// t4: 回复——保留
	if items[3].ExternalID != "t4" {
		t.Errorf("回复推文应保留: got %s", items[3].ExternalID)
	}
}

func TestTwitterFetch_SkipsPinned(t *testing.T) {
	resp := `{"code":200,"data":{"status":"ok","pinned":[{"tweet_id":"pin","text":"pinned","created_at":"Thu Jul 10 12:00:00 +0000 2026","author":{"screen_name":"A"}}],"timeline":[]}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(resp))
	}))
	defer srv.Close()

	f := newTestTwitter(srv.URL)
	items, err := f.Fetch(context.Background(), twitterSrc(1, "TestUser"))
	if err != nil {
		t.Fatalf("不应报错: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("timeline 为空时应返回 0 条，实际 %d", len(items))
	}
}

func TestTwitterFetch_MissingAPIKey(t *testing.T) {
	f := NewTwitter(config.FetchConfig{})
	_, err := f.Fetch(context.Background(), twitterSrc(1, "OpenAI"))
	if err == nil {
		t.Fatal("缺少 API key 应报错")
	}
}

func TestTwitterFetch_MissingScreenName(t *testing.T) {
	f := NewTwitter(config.FetchConfig{TikhubAPIKey: "k"})
	src := types.Source{
		ID:         1,
		Platform:   types.PlatformX,
		Capability: types.CapUserPosts,
		Config:     json.RawMessage(`{}`),
	}
	_, err := f.Fetch(context.Background(), src)
	if err == nil {
		t.Fatal("缺少 screen_name 应报错")
	}
}

func TestTwitterFetch_BusinessError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"code":400,"data":{}}`))
	}))
	defer srv.Close()

	f := newTestTwitter(srv.URL)
	_, err := f.Fetch(context.Background(), twitterSrc(1, "BadUser"))
	if err == nil {
		t.Fatal("业务 code 非 200 应报错")
	}
}

func TestParseTwitterTime(t *testing.T) {
	p := parseTwitterTime("Wed Jul 15 17:30:00 +0000 2026")
	if p == nil {
		t.Fatal("应解析成功")
	}
	if p.Year() != 2026 || p.Month() != 7 || p.Day() != 15 {
		t.Errorf("日期不符: %v", p)
	}

	if parseTwitterTime("invalid") != nil {
		t.Error("无效格式应返回 nil")
	}
}

func TestTwitterFetch_PolymorphicFields(t *testing.T) {
	// 验证 media/entities 为 {} (对象) 时也能正确解析（§9.5）
	resp := `{"code":200,"data":{"status":"ok","timeline":[{
		"tweet_id":"p1","text":"with media",
		"created_at":"Thu Jul 16 12:00:00 +0000 2026",
		"author":{"screen_name":"Test"},
		"media":{"type":"photo","url":"https://x.com/photo/1"},
		"entities":{"urls":[{"expanded_url":"https://example.com"}]}
	}]}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(resp))
	}))
	defer srv.Close()

	f := newTestTwitter(srv.URL)
	items, err := f.Fetch(context.Background(), twitterSrc(1, "Test"))
	if err != nil {
		t.Fatalf("多态字段（对象形态）不应报错: %v", err)
	}
	if len(items) != 1 || items[0].ExternalID != "p1" {
		t.Errorf("应正常解析: items=%d", len(items))
	}
}
