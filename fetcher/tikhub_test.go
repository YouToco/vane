package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// sampleTikhubResponse 按 2026-07-14 实测的 search_notes 响应结构构造：
// 外壳 code/data.success，笔记在 data.data.items[].note，混入一个非 note 项验证过滤。
const sampleTikhubResponse = `{
  "code": 200,
  "data": {
    "success": true,
    "msg": null,
    "data": {
      "items": [
        {
          "model_type": "note",
          "note": {
            "id": "69ca2af0000000001b020a10",
            "title": "分享几个AI创业方向",
            "desc": "去年开始有创业的想法…",
            "timestamp": 1783670775,
            "xsec_token": "ABtoken=",
            "user": {"nickname": "Zimablue"}
          }
        },
        {
          "model_type": "recommend_query",
          "note": null
        },
        {
          "model_type": "note",
          "note": {
            "id": "",
            "title": "空 id 应被跳过",
            "desc": "x",
            "timestamp": 0,
            "xsec_token": "",
            "user": {"nickname": ""}
          }
        }
      ]
    }
  }
}`

// newTestTikHub 构造指向 httptest.Server 的 TikHubFetcher。
func newTestTikHub(srvURL string) *TikHubFetcher {
	f := NewTikHub(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, TikhubAPIKey: "test-key"})
	f.baseURL = srvURL
	return f
}

// xhsSrc 构造一个带 keyword 的小红书信源。
func xhsSrc(id int64, cfg string) types.Source {
	return types.Source{ID: id, Type: types.SourceTypeTikHubXHS, Config: json.RawMessage(cfg)}
}

func TestTikHubFetch_MapsNotes(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sampleTikhubResponse))
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL)
	items, err := f.Fetch(context.Background(), xhsSrc(11, `{"keyword":"AI 创业"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	// 非 note 项与空 id 项都应被跳过，只剩 1 条。
	if len(items) != 1 {
		t.Fatalf("期望 1 条，实际 %d", len(items))
	}

	got := items[0]
	if got.SourceID != 11 {
		t.Errorf("SourceID: 期望 11，实际 %d", got.SourceID)
	}
	if got.ExternalID != "69ca2af0000000001b020a10" {
		t.Errorf("ExternalID: 期望 note.id，实际 %q", got.ExternalID)
	}
	if got.Title != "分享几个AI创业方向" || got.Author != "Zimablue" {
		t.Errorf("字段映射不符: %+v", got)
	}
	if !strings.HasPrefix(got.URL, "https://www.xiaohongshu.com/explore/69ca2af0000000001b020a10?xsec_token=") {
		t.Errorf("URL 应拼 explore 直链 + xsec_token，实际 %q", got.URL)
	}
	if got.PublishedAt == nil || !got.PublishedAt.Equal(time.Unix(1783670775, 0)) {
		t.Errorf("PublishedAt 应为 Unix 秒 1783670775，实际 %v", got.PublishedAt)
	}
	if got.ContentHash == "" || got.Simhash == nil {
		t.Error("ContentHash/Simhash 应由 finalize 补齐")
	}

	// 请求侧断言：路径、鉴权、关键 query 参数（含默认 sort_type）。
	if gotPath != tikhubSearchPath {
		t.Errorf("请求路径: 期望 %s，实际 %s", tikhubSearchPath, gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization: 期望 Bearer test-key，实际 %q", gotAuth)
	}
	if !strings.Contains(gotQuery, "sort_type=time_descending") || !strings.Contains(gotQuery, "page=1") {
		t.Errorf("query 参数缺失: %s", gotQuery)
	}
}

func TestTikHubFetch_MissingKey(t *testing.T) {
	f := NewTikHub(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}) // 无 key
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 key 应判 ErrValidation，实际 %v", err)
	}
}

func TestTikHubFetch_MissingKeyword(t *testing.T) {
	f := newTestTikHub("http://unused.invalid")
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 keyword 应判 ErrValidation，实际 %v", err)
	}
}

func TestTikHubFetch_AuthFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("401 应判 ErrValidation（key 问题），实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("鉴权失败不应可重试")
	}
}

func TestTikHubFetch_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if types.CodeOf(err) != types.CodeFetchRateLimit {
		t.Errorf("429 期望 CodeFetchRateLimit，实际 %s", types.CodeOf(err))
	}
}

func TestTikHubFetch_BusinessFailure(t *testing.T) {
	// HTTP 200 但业务失败（success=false）：应报错且不可重试。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":{"success":false,"msg":"keyword blocked","data":{"items":[]}}}`))
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if err == nil {
		t.Fatal("业务失败应返回错误")
	}
	if types.IsRetryable(err) {
		t.Error("业务失败按确定性处理，不应可重试")
	}
}

func TestTikHubFetch_SortTypeOverride(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"code":200,"data":{"success":true,"data":{"items":[]}}}`))
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x","sort_type":"general"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if !strings.Contains(gotQuery, "sort_type=general") {
		t.Errorf("config 指定 sort_type 应覆盖默认，实际 query: %s", gotQuery)
	}
}
