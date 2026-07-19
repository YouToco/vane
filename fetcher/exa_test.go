package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// sampleExaResponse 按 2026-07-14 实测的 Exa /search 响应字段构造。
const sampleExaResponse = `{
  "requestId": "req-123",
  "results": [
    {
      "id": "https://example.com/ai-news-1",
      "title": "AI Weekly Digest",
      "url": "https://example.com/ai-news-1",
      "publishedDate": "2026-07-13T08:30:00.000Z",
      "author": "Jane Doe",
      "text": "This week in AI..."
    },
    {
      "id": "",
      "title": "No ID Result",
      "url": "https://example.com/no-id",
      "publishedDate": "",
      "author": "",
      "text": "body"
    },
    {
      "id": "",
      "title": "",
      "url": "",
      "text": "ghost entry without url or title"
    }
  ]
}`

// newTestExa 构造指向 httptest.Server 的 ExaFetcher（不记账）。
func newTestExa(srvURL string) *ExaFetcher {
	e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "test-key"}, nil)
	e.searchURL = srvURL
	return e
}

// exaSrc 构造一个带 query 的 Exa 信源。
func exaSrc(id int64, cfg string) types.Source {
	return types.Source{ID: id, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(cfg)}
}

func TestExaFetch_MapsResults(t *testing.T) {
	var gotBody []byte
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotAuth = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sampleExaResponse))
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	items, err := e.Fetch(context.Background(), exaSrc(9, `{"query":"AI 周报","category":"news"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	// 第三条无 url 无 title 应被跳过。
	if len(items) != 2 {
		t.Fatalf("期望 2 条，实际 %d", len(items))
	}

	got := items[0]
	if got.SourceID != 9 {
		t.Errorf("SourceID: 期望 9，实际 %d", got.SourceID)
	}
	if got.ExternalID != "https://example.com/ai-news-1" {
		t.Errorf("ExternalID: 期望结果 id，实际 %q", got.ExternalID)
	}
	if got.Title != "AI Weekly Digest" || got.Author != "Jane Doe" || got.Content != "This week in AI..." {
		t.Errorf("字段映射不符: %+v", got)
	}
	if got.PublishedAt == nil {
		t.Error("publishedDate 应被解析出来")
	}
	if got.ContentHash == "" {
		t.Error("ContentHash 应由 finalize 补齐")
	}
	if got.Simhash == nil {
		t.Error("Simhash 应由 finalize 补齐（否则 Dedup 自撞修复失效）")
	}
	// 第二条无 id：external_id 应兜底为 content_hash。
	if items[1].ExternalID == "" {
		t.Error("无 id 时 ExternalID 应兜底 content_hash，实际为空")
	}
	if items[1].ExternalID != items[1].ContentHash {
		t.Errorf("无 id 时 ExternalID 应等于 ContentHash，实际 %q", items[1].ExternalID)
	}

	// 请求侧断言：鉴权头与关键请求体字段。
	if gotAuth != "test-key" {
		t.Errorf("x-api-key: 期望 test-key，实际 %q", gotAuth)
	}
	var req map[string]any
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("请求体不是合法 JSON: %v", err)
	}
	if req["query"] != "AI 周报" || req["category"] != "news" || req["type"] != "auto" {
		t.Errorf("请求体字段不符: %v", req)
	}
	if req["numResults"] != float64(exaDefaultNumResults) {
		t.Errorf("numResults 默认应为 %d，实际 %v", exaDefaultNumResults, req["numResults"])
	}
	if _, has := req["startPublishedDate"]; has {
		t.Error("默认不应带 startPublishedDate（Exa 的 publishedDate 是 HTML 猜的，官方页普遍为空）")
	}
	contents, _ := req["contents"].(map[string]any)
	if contents == nil || contents["text"] != true {
		t.Errorf("contents.text 应为 true，实际 %v", req["contents"])
	}
}

func TestExaFetch_IncludeDomains(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(sampleExaResponse))
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	cfg := `{"query":"Anthropic","include_domains":["anthropic.com","docs.anthropic.com"]}`
	_, err := e.Fetch(context.Background(), exaSrc(1, cfg))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	var req map[string]any
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("请求体不是合法 JSON: %v", err)
	}
	domains, ok := req["includeDomains"].([]any)
	if !ok || len(domains) != 2 {
		t.Fatalf("includeDomains 期望 2 个域名，实际 %v", req["includeDomains"])
	}
	if domains[0] != "anthropic.com" || domains[1] != "docs.anthropic.com" {
		t.Errorf("includeDomains 内容不符: %v", domains)
	}
}

func TestExaFetch_ExplicitLookbackDays(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(sampleExaResponse))
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	_, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x","lookback_days":30}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	var req map[string]any
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("请求体不是合法 JSON: %v", err)
	}
	if s, _ := req["startPublishedDate"].(string); s == "" {
		t.Error("显式 lookback_days=30 应带 startPublishedDate")
	}
}

func TestExaFetch_MissingKey(t *testing.T) {
	e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}, nil) // 无 key
	_, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 key 应判 ErrValidation，实际 %v", err)
	}
}

func TestExaFetch_MissingQuery(t *testing.T) {
	e := newTestExa("http://unused.invalid")
	_, err := e.Fetch(context.Background(), exaSrc(1, `{}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 query 应判 ErrValidation，实际 %v", err)
	}
}

func TestExaFetch_BadConfig(t *testing.T) {
	e := newTestExa("http://unused.invalid")
	_, err := e.Fetch(context.Background(), exaSrc(1, `{"query": 123`)) // 非法 JSON
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("非法 config 应判 ErrValidation，实际 %v", err)
	}
}

func TestExaFetch_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	_, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x"}`))
	if types.CodeOf(err) != types.CodeFetchRateLimit {
		t.Errorf("429 期望 CodeFetchRateLimit，实际 %s", types.CodeOf(err))
	}
	if !types.IsRetryable(err) {
		t.Error("限流应可重试")
	}
}

func TestExaFetch_AuthFailed(t *testing.T) {
	// 401/403 归 CodeValidation（与 TikHub 对齐）：key 配错是本方配置问题。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	_, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("401 应判 ErrValidation（key 问题），实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("鉴权失败不应可重试")
	}
}

func TestExaFetch_Non2xxRetryability(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
	}{
		{http.StatusInternalServerError, true}, // 5xx 瞬态可重试
		{http.StatusNotFound, false},           // 4xx 确定性不可重试
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
		}))
		e := newTestExa(srv.URL)
		_, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x"}`))
		if err == nil {
			t.Errorf("status %d 应返回错误", tc.status)
		}
		if got := types.IsRetryable(err); got != tc.retryable {
			t.Errorf("status %d: Retryable 期望 %v，实际 %v", tc.status, tc.retryable, got)
		}
		srv.Close()
	}
}

func TestExaFetch_TruncatesLongText(t *testing.T) {
	long := strings.Repeat("a", exaMaxTextBytes+100)
	resp := `{"results":[{"id":"x","title":"t","url":"https://e.com/1","text":"` + long + `"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(resp))
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	items, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if len(items) != 1 || len(items[0].Content) != exaMaxTextBytes {
		t.Errorf("正文应截断到 %d 字节，实际 %d", exaMaxTextBytes, len(items[0].Content))
	}
}

func TestExaFetch_TruncatesCJKAtRuneBoundary(t *testing.T) {
	// 回归（审查 CRITICAL）：中文 3 字节/字，4000 不是 3 的倍数——裸字节切片必然
	// 切裂 rune 产生非法 UTF-8，Postgres 22021 拒绝入库、条目每轮抓取永久丢失。
	long := strings.Repeat("汉", (exaMaxTextBytes/3)+100) // > 4000 字节纯中文
	resp := `{"results":[{"id":"x","title":"中文","url":"https://e.com/1","text":"` + long + `"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(resp))
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	items, err := e.Fetch(context.Background(), exaSrc(1, `{"query":"x"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("期望 1 条，实际 %d", len(items))
	}
	got := items[0].Content
	if !utf8.ValidString(got) {
		t.Error("截断后必须是合法 UTF-8（否则 Postgres 22021 拒绝入库）")
	}
	if len(got) > exaMaxTextBytes {
		t.Errorf("截断后不应超过 %d 字节，实际 %d", exaMaxTextBytes, len(got))
	}
	if len(got) < exaMaxTextBytes-utf8.UTFMax {
		t.Errorf("截断不应回退超过一个 rune（%d 字节），实际 %d", utf8.UTFMax, len(got))
	}
}

// fakeCallRecorder 收集 RecordBindingCall 的入参（错误路径记账断言用）。
type fakeCallRecorder struct{ calls []*types.ToolCall }

func (f *fakeCallRecorder) RecordBindingCall(_ context.Context, rec *types.ToolCall) {
	f.calls = append(f.calls, rec)
}

// TestExaFetch_ErrorPathsRecordToolCall：429/非 2xx 也必须进 tool_calls
// （bug 狩猎 2026-07-19 MEDIUM，两路独立发现）：此前只有成功路径记账，
// Exa 限流一整天账本零行、故障在 tool_calls 上隐形。与 TikHub 成败都记对齐。
func TestExaFetch_ErrorPathsRecordToolCall(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantErrTyp string
	}{
		{"429 限流", http.StatusTooManyRequests, `{"error":"rate limited"}`, types.ToolErrInternal},
		{"500 服务端错误", http.StatusInternalServerError, `boom`, types.ToolErrInternal},
		{"200 但 JSON 非法", http.StatusOK, `{not-json`, types.ToolErrInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			rec := &fakeCallRecorder{}
			e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
			e.searchURL = srv.URL

			_, err := e.Fetch(context.Background(), exaSrc(9, `{"query":"q"}`))
			if err == nil {
				t.Fatal("错误路径应返回 error")
			}
			if len(rec.calls) != 1 {
				t.Fatalf("错误路径应记恰好 1 条 tool_call，实得 %d", len(rec.calls))
			}
			c := rec.calls[0]
			if c.HTTPStatus == nil || *c.HTTPStatus != tt.status {
				t.Errorf("HTTPStatus 应为 %d，实为 %v", tt.status, c.HTTPStatus)
			}
			if c.Error == "" {
				t.Error("错误路径的 tool_call 应携带错误文本")
			}
			if c.CostUSD != nil {
				t.Errorf("错误路径不得记成本，实为 %v", *c.CostUSD)
			}
		})
	}
}
