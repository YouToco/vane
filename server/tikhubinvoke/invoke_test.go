package tikhubinvoke

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/types"
)

// newTestInvoker 指向 httptest server（同包可触私有字段，生产构造仍只走 New）。
func newTestInvoker(srvURL string) *Invoker {
	return &Invoker{
		hc:      &http.Client{Timeout: 2 * time.Second},
		baseURL: srvURL,
		apiKey:  "test-key",
	}
}

func TestInvoke_GETQueryAssembly(t *testing.T) {
	var gotReq *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r.Clone(context.Background())
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	entry := tikhubcatalog.Entry{
		Name: "t_get", Method: "GET", Path: "/api/v1/test/search",
		Params: []tikhubcatalog.Param{
			{Name: "keyword", In: "query", Required: true, Type: "string"},
			{Name: "page", In: "query", Type: "integer"},
			{Name: "tags", In: "query", Type: "array:string"},
			{Name: "optional_missing", In: "query", Type: "string"},
		},
	}
	res, err := newTestInvoker(srv.URL).Invoke(context.Background(), entry, map[string]any{
		"keyword": "AI 编程",
		"page":    float64(2), // JSON 数字恒为 float64
		"tags":    []any{"a", "b"},
	})
	if err != nil {
		t.Fatalf("Invoke 报错: %v", err)
	}
	if res.Status != 200 || string(res.Body) != `{"ok":true}` {
		t.Fatalf("结果不符: %+v", res)
	}
	q := gotReq.URL.Query()
	if q.Get("keyword") != "AI 编程" {
		t.Errorf("keyword = %q", q.Get("keyword"))
	}
	if q.Get("page") != "2" {
		t.Errorf("整数参数应无小数点: page=%q", q.Get("page"))
	}
	if got := q["tags"]; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("数组参数应重复键展开: %v", got)
	}
	if q.Has("optional_missing") {
		t.Error("未提供的可选参数不应出现在请求里")
	}
	if auth := gotReq.Header.Get("Authorization"); auth != "Bearer test-key" {
		t.Errorf("鉴权头不符: %q", auth)
	}
}

func TestInvoke_POSTBodyAndMixedParams(t *testing.T) {
	var gotBody []byte
	var gotReq *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotReq = r.Clone(context.Background())
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	entry := tikhubcatalog.Entry{
		Name: "t_post", Method: "POST", Path: "/api/v1/test/feed",
		Params: []tikhubcatalog.Param{
			{Name: "count", In: "body", Type: "integer"},
			{Name: "cookie", In: "body", Type: "string"},
			{Name: "debug", In: "query", Type: "boolean"},
		},
	}
	_, err := newTestInvoker(srv.URL).Invoke(context.Background(), entry, map[string]any{
		"count": float64(15),
		"debug": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(gotBody, &body); err != nil {
		t.Fatalf("body 非法 JSON: %s", gotBody)
	}
	if body["count"] != float64(15) {
		t.Errorf("body 参数不符: %v", body)
	}
	if _, has := body["cookie"]; has {
		t.Error("未提供的 body 参数不应出现（让上游用自己的默认值）")
	}
	if gotReq.URL.Query().Get("debug") != "true" {
		t.Errorf("POST 的 query 参数应照常装配: %q", gotReq.URL.RawQuery)
	}
	if ct := gotReq.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

// TestInvoke_BigIntIDPrecision：雪花级大 ID（>2^53）经 json.Number 原样透传，
// 不被 float64 舍入（对抗审查 HIGH 缺陷）。TikTok/抖音 uid 都是这个量级。
func TestInvoke_BigIntIDPrecision(t *testing.T) {
	var gotReq *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r.Clone(context.Background())
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	entry := tikhubcatalog.Entry{
		Name: "t", Method: "GET", Path: "/p",
		Params: []tikhubcatalog.Param{{Name: "user_id", In: "query", Required: true, Type: "integer"}},
	}
	// 模拟 agent 侧 UseNumber 解析后传入的 json.Number（十进制原串）。
	_, err := newTestInvoker(srv.URL).Invoke(context.Background(), entry, map[string]any{
		"user_id": json.Number("6829164342857171974"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := gotReq.URL.Query().Get("user_id"); got != "6829164342857171974" {
		t.Errorf("大 ID 应逐位保真，实得 %q（float64 会舍成 ...171968）", got)
	}
}

// TestInvoke_POSTAlwaysSendsBody：无参数 POST 也发 {}——FastAPI 对声明了
// requestBody 的端点收到空 body 回 422。
func TestInvoke_POSTAlwaysSendsBody(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	entry := tikhubcatalog.Entry{Name: "t", Method: "POST", Path: "/p"}
	if _, err := newTestInvoker(srv.URL).Invoke(context.Background(), entry, nil); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(gotBody)) != "{}" {
		t.Errorf("空参 POST 应发 {}，实得 %q", gotBody)
	}
}

func TestInvoke_PathParamSubstitution(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	entry := tikhubcatalog.Entry{
		Name: "t", Method: "GET", Path: "/api/v1/note/{note_id}/detail",
		Params: []tikhubcatalog.Param{{Name: "note_id", In: "path", Required: true, Type: "string"}},
	}
	if _, err := newTestInvoker(srv.URL).Invoke(context.Background(), entry, map[string]any{"note_id": "abc123"}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/note/abc123/detail" {
		t.Errorf("path 参数替换不符: %q", gotPath)
	}
}

// TestInvoke_Non2xxIsNotError：4xx/5xx 不是 error——状态码随 Result 返回，
// 由 agent 端点工具决定怎么回给模型（4xx 原文是模型自纠的关键输入）。
func TestInvoke_Non2xxIsNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		w.Write([]byte(`{"detail":"bad"}`))
	}))
	defer srv.Close()
	res, err := newTestInvoker(srv.URL).Invoke(context.Background(),
		tikhubcatalog.Entry{Name: "t", Method: "GET", Path: "/p"}, nil)
	if err != nil {
		t.Fatalf("非 2xx 不应报 error: %v", err)
	}
	if res.Status != 422 || !strings.Contains(string(res.Body), "bad") {
		t.Fatalf("状态与原文应透传: %+v", res)
	}
}

func TestInvoke_MissingKeyFailsClosed(t *testing.T) {
	inv := &Invoker{hc: http.DefaultClient, baseURL: "http://127.0.0.1:1", apiKey: ""}
	_, err := inv.Invoke(context.Background(), tikhubcatalog.Entry{Name: "t", Method: "GET", Path: "/p"}, nil)
	var ae *types.AppError
	if err == nil || !typesAsAppError(err, &ae) || ae.Code != types.CodeValidation {
		t.Fatalf("key 缺失应回 CodeValidation: %v", err)
	}
}

func TestInvoke_TimeoutMapsToFetchTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()
	inv := &Invoker{hc: &http.Client{Timeout: 50 * time.Millisecond}, baseURL: srv.URL, apiKey: "k"}
	_, err := inv.Invoke(context.Background(), tikhubcatalog.Entry{Name: "t", Method: "GET", Path: "/p"}, nil)
	var ae *types.AppError
	if err == nil || !typesAsAppError(err, &ae) || ae.Code != types.CodeFetchTimeout {
		t.Fatalf("client 超时应映射 CodeFetchTimeout: %v", err)
	}
}

// TestNew_UsesFetchConfigKey：生产构造复用 fetch 配置里的 key。
func TestNew_UsesFetchConfigKey(t *testing.T) {
	inv := New(config.FetchConfig{TikhubAPIKey: "k1"})
	if inv.apiKey != "k1" || inv.baseURL != defaultBaseURL {
		t.Fatalf("构造不符: %+v", inv)
	}
}

// typesAsAppError 是 errors.As 的本地小包装（避免测试里重复 import errors 样板）。
func typesAsAppError(err error, target **types.AppError) bool {
	for e := err; e != nil; {
		if ae, ok := e.(*types.AppError); ok {
			*target = ae
			return true
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}
