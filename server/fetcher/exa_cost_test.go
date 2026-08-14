package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/types"
)

// mockRecorder captures tool_calls written by the Exa fetchers.
type mockRecorder struct {
	mu          sync.Mutex
	calls       []*types.ToolCall
	contextErr  error
	hasDeadline bool
}

func (m *mockRecorder) RecordBindingCall(ctx context.Context, rec *types.ToolCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, rec)
	m.contextErr = ctx.Err()
	_, m.hasDeadline = ctx.Deadline()
}

func (m *mockRecorder) last() *types.ToolCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return nil
	}
	return m.calls[len(m.calls)-1]
}

func (m *mockRecorder) contextState() (error, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.contextErr, m.hasDeadline
}

// ────────── Exa /search cost recording ──────────

const sampleExaResponseWithCost = `{
  "requestId": "req-1",
  "results": [
    {"id":"r1","title":"AI News","url":"https://example.com/1","text":"body"}
  ],
  "costDollars": {"total": 0.007, "search": {"neural": 0.007}}
}`

func TestExaFetch_RecordsCost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(sampleExaResponseWithCost))
	}))
	defer srv.Close()

	rec := &mockRecorder{}
	e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
	e.searchURL = srv.URL

	src := types.FetchTarget{ID: 42, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(`{"query":"x"}`)}
	_, err := e.Fetch(context.Background(), src)
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("recorder 未收到调用记录")
	}
	if got.ToolName != "exa:search" {
		t.Errorf("ToolName: 期望 exa:search，实际 %q", got.ToolName)
	}
	if got.ToolKind != types.ToolCallKindExaFetch {
		t.Errorf("ToolKind: 期望 %q，实际 %q", types.ToolCallKindExaFetch, got.ToolKind)
	}
	if got.EndpointPath != "/search" {
		t.Errorf("EndpointPath: 期望 /search，实际 %q", got.EndpointPath)
	}
	if got.SourceID == nil || *got.SourceID != 42 {
		t.Errorf("SourceID: 期望 42，实际 %v", got.SourceID)
	}
	if got.CostUSD == nil || *got.CostUSD != 0.007 {
		t.Errorf("CostUSD: 期望 0.007，实际 %v", got.CostUSD)
	}
	if got.DurationMs < 0 {
		t.Errorf("DurationMs 应 >= 0，实际 %d", got.DurationMs)
	}
	if got.ErrorType != "" {
		t.Errorf("成功调用不应有 ErrorType，实际 %q", got.ErrorType)
	}
}

func TestExaFetch_NoCostField_RecordsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"results":[{"id":"r1","title":"t","url":"https://e.com/1","text":"b"}]}`))
	}))
	defer srv.Close()

	rec := &mockRecorder{}
	e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
	e.searchURL = srv.URL

	_, err := e.Fetch(context.Background(), types.FetchTarget{ID: 7, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(`{"query":"x"}`)})
	if err != nil {
		t.Fatalf("响应里没有 costDollars 时不应报错: %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("recorder 未收到调用记录")
	}
	if got.CostUSD != nil {
		t.Errorf("无 costDollars 时 CostUSD 应为 nil（不记 0），实际 %v", *got.CostUSD)
	}
}

func TestExaFetch_NilRecorder_NoPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(sampleExaResponseWithCost))
	}))
	defer srv.Close()

	e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, nil)
	e.searchURL = srv.URL

	_, err := e.Fetch(context.Background(), types.FetchTarget{ID: 1, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(`{"query":"x"}`)})
	if err != nil {
		t.Fatalf("recorder 为 nil 时不应影响 Fetch: %v", err)
	}
}

// ────────── Exa /contents cost recording ──────────

const sampleExaContentsWithCost = `{
  "requestId": "r2",
  "results": [{"id":"c1","url":"https://x.com/pricing","title":"Pricing","text":"gpt-5 $5"}],
  "statuses": [{"status":"success","source":"crawled"}],
  "costDollars": {"total": 0.001, "contents": {"text": 0.001}}
}`

func TestExaContents_RecordsCost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(sampleExaContentsWithCost))
	}))
	defer srv.Close()

	rec := &mockRecorder{}
	e := NewExaContents(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
	e.contentURL = srv.URL

	src := types.FetchTarget{ID: 99, Platform: types.PlatformWeb, Capability: types.CapContents, Config: json.RawMessage(`{"url":"https://x.com/pricing"}`)}
	_, err := e.Fetch(context.Background(), src)
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("recorder 未收到调用记录")
	}
	if got.ToolName != "exa:contents" {
		t.Errorf("ToolName: 期望 exa:contents，实际 %q", got.ToolName)
	}
	if got.ToolKind != types.ToolCallKindExaFetch {
		t.Errorf("ToolKind: 期望 %q，实际 %q", types.ToolCallKindExaFetch, got.ToolKind)
	}
	if got.EndpointPath != "/contents" {
		t.Errorf("EndpointPath: 期望 /contents，实际 %q", got.EndpointPath)
	}
	if got.SourceID == nil || *got.SourceID != 99 {
		t.Errorf("SourceID: 期望 99，实际 %v", got.SourceID)
	}
	if got.CostUSD == nil || *got.CostUSD != 0.001 {
		t.Errorf("CostUSD: 期望 0.001，实际 %v", got.CostUSD)
	}
	if got.DurationMs < 0 {
		t.Errorf("DurationMs 应 >= 0，实际 %d", got.DurationMs)
	}
}

func TestExaContents_NoCostField_RecordsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"results":[{"id":"c1","url":"https://x.com/p","title":"P","text":"hi"}],"statuses":[{"status":"success","source":"crawled"}]}`))
	}))
	defer srv.Close()

	rec := &mockRecorder{}
	e := NewExaContents(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
	e.contentURL = srv.URL

	_, err := e.Fetch(context.Background(), types.FetchTarget{ID: 5, Platform: types.PlatformWeb, Capability: types.CapContents, Config: json.RawMessage(`{"url":"https://x.com/p"}`)})
	if err != nil {
		t.Fatalf("无 costDollars 不应报错: %v", err)
	}

	got := rec.last()
	if got == nil {
		t.Fatal("recorder 未收到调用记录")
	}
	if got.CostUSD != nil {
		t.Errorf("无 costDollars 时 CostUSD 应为 nil，实际 %v", *got.CostUSD)
	}
}

func TestExaContents_NilRecorder_NoPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(sampleExaContentsWithCost))
	}))
	defer srv.Close()

	e := NewExaContents(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, nil)
	e.contentURL = srv.URL

	_, err := e.Fetch(context.Background(), types.FetchTarget{ID: 1, Platform: types.PlatformWeb, Capability: types.CapContents, Config: json.RawMessage(`{"url":"https://x.com/p"}`)})
	if err != nil {
		t.Fatalf("recorder 为 nil 时不应影响 Fetch: %v", err)
	}
}

// TestExaAdHoc_RecordsAttributionWithoutSendingContext H-1 双账本金丝雀：
// Agent 注入的 trace/user 要进入本地上游账本，但不得被序列化到 Exa 请求体/请求头。
func TestExaAdHoc_RecordsAttributionWithoutSendingContext(t *testing.T) {
	const traceID = "private-context-sentinel"
	ctx := WithBindingAttribution(context.Background(), traceID, 7007)

	assertAttribution := func(t *testing.T, got *types.ToolCall) {
		t.Helper()
		if got == nil {
			t.Fatal("recorder 未收到调用记录")
		}
		if got.TraceID != traceID || got.UserID == nil || *got.UserID != 7007 {
			t.Fatalf("上游账本归属不完整: trace=%q user=%v", got.TraceID, got.UserID)
		}
		if got.SourceID != nil {
			t.Fatalf("ad-hoc 调用必须省略 source_id，实得 %v", got.SourceID)
		}
	}

	t.Run("search", func(t *testing.T) {
		var outbound string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			outbound = string(body) + "\n" + fmt.Sprint(r.Header)
			_, _ = w.Write([]byte(sampleExaResponseWithCost))
		}))
		defer srv.Close()

		rec := &mockRecorder{}
		e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
		e.searchURL = srv.URL
		if _, err := e.Search(ctx, "Vane H-1", 1, nil); err != nil {
			t.Fatal(err)
		}
		assertAttribution(t, rec.last())
		if strings.Contains(outbound, traceID) || strings.Contains(outbound, "7007") {
			t.Fatalf("本地归属不得泄露给 Exa: %s", outbound)
		}
	})

	t.Run("contents", func(t *testing.T) {
		var outbound string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			outbound = string(body) + "\n" + fmt.Sprint(r.Header)
			_, _ = w.Write([]byte(sampleExaContentsWithCost))
		}))
		defer srv.Close()

		rec := &mockRecorder{}
		e := NewExaContents(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
		e.contentURL = srv.URL
		if _, _, _, err := e.ReadPage(ctx, "https://x.com/pricing"); err != nil {
			t.Fatal(err)
		}
		assertAttribution(t, rec.last())
		if strings.Contains(outbound, traceID) || strings.Contains(outbound, "7007") {
			t.Fatalf("本地归属不得泄露给 Exa: %s", outbound)
		}
	})
}

func TestExaRecordCall_DetachesCancellationWithBoundedDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(WithBindingAttribution(context.Background(), "trace-cancel", 7))
	cancel()

	rec := &mockRecorder{}
	e := NewExa(config.FetchConfig{}, rec)
	e.recordCall(ctx, types.FetchTarget{}, nil, nil, 200, 0, 0, 0, 10, nil)

	got := rec.last()
	if got == nil || got.TraceID != "trace-cancel" || got.UserID == nil || *got.UserID != 7 {
		t.Fatalf("取消后仍应保留归属并写上游账本: %+v", got)
	}
	if ctxErr, hasDeadline := rec.contextState(); ctxErr != nil || !hasDeadline {
		t.Fatalf("上游记账应脱离取消且保持有界: err=%v hasDeadline=%v", ctxErr, hasDeadline)
	}
}

func TestExaContents_StatusErrorRecordedAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"results":[],
			"statuses":[{"status":"error","source":"crawled"}],
			"costDollars":{"total":0.003}
		}`))
	}))
	defer srv.Close()

	rec := &mockRecorder{}
	e := NewExaContents(config.FetchConfig{
		TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k",
	}, rec)
	e.contentURL = srv.URL
	_, _, _, err := e.ReadPage(context.Background(), "https://example.com/unreachable")
	if err == nil {
		t.Fatal("statuses.error 必须返回错误")
	}
	got := rec.last()
	if got == nil || got.ErrorType != types.ToolErrInternal || got.CostUSD == nil || *got.CostUSD != 0.003 {
		t.Fatalf("上游账本必须同时记录失败与真实成本: %+v", got)
	}
}

// ────────── 反向验证：解析逻辑如果改坏，测试要红 ──────────

func TestExaFetch_CostParsing_BreaksIfFieldRemoved(t *testing.T) {
	resp := `{"results":[{"id":"r1","title":"t","url":"https://e.com/1","text":"b"}],"costDollars":{"total":0.123}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(resp))
	}))
	defer srv.Close()

	rec := &mockRecorder{}
	e := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "k"}, rec)
	e.searchURL = srv.URL

	_, _ = e.Fetch(context.Background(), types.FetchTarget{ID: 1, Platform: types.PlatformWeb, Capability: types.CapSearch, Config: json.RawMessage(`{"query":"x"}`)})

	got := rec.last()
	if got == nil {
		t.Fatal("recorder 未收到记录")
	}
	if got.CostUSD == nil || *got.CostUSD != 0.123 {
		t.Fatalf("costDollars.total=0.123 应被精确解析到 CostUSD，实际 %v（如果此测试失败说明解析逻辑被改坏）", got.CostUSD)
	}
}
