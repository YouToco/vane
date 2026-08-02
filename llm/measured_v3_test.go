package llm

import (
	"context"
	"errors"
	"go/ast"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestInvariant_DoMeasuredV3HasOneUpstreamAttemptAndNoLegacyAccounting(t *testing.T) {
	_, file := parseLLMFile(t, "measured_v3.go")
	fn := findFunc(t, file, "DoMeasuredV3")
	completeCalls := 0
	forbidden := map[string]bool{
		"Record":                              true,
		"CheckQuota":                          true,
		"BeforeSpend":                         true,
		"TryConsumeForUser":                   true,
		"AdjustForUser":                       true,
		"finishCallAccountingWithReservation": true,
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch callee := call.Fun.(type) {
		case *ast.Ident:
			name = callee.Name
		case *ast.SelectorExpr:
			name = callee.Sel.Name
		}
		if name == "Complete" {
			completeCalls++
		}
		if forbidden[name] {
			t.Errorf("DoMeasuredV3 calls forbidden legacy accounting API %s", name)
		}
		return true
	})
	if completeCalls != 1 {
		t.Fatalf("DoMeasuredV3 Complete calls = %d, want exactly 1", completeCalls)
	}
}

func TestDoMeasuredV3_SuccessPreservesExactCallMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"provider-model",
			"choices":[{"message":{"content":"answer"}}],
			"usage":{
				"prompt_tokens":100,
				"completion_tokens":25,
				"prompt_cache_hit_tokens":40,
				"prompt_cache_miss_tokens":60,
				"completion_tokens_details":{"reasoning_tokens":10}
			}
		}`))
	}))
	t.Cleanup(srv.Close)

	temperature := float32(0.4)
	maxTokens := 321
	tenantID, userID, refID := int64(7), int64(8), int64(9)
	ctx := WithRunSnapshotAttribution(t.Context(), 91)
	result, err := DoMeasuredV3(ctx, newTestClient(srv.URL, 1), CallMeta{
		TraceID: "trace", TenantID: &tenantID, SpanName: "research_planner",
		UserID: &userID, RefType: types.RefType("task"), RefID: &refID,
	}, Request{
		System: "system", User: "user", Model: "requested-model",
		Temperature: &temperature, MaxTokens: &maxTokens, DisableThinking: true,
	})
	if err != nil {
		t.Fatalf("DoMeasuredV3() error = %v", err)
	}
	if result.Response == nil || result.Response.Content != "answer" {
		t.Fatalf("response = %+v, want answer", result.Response)
	}
	if !result.Attempted || !result.UsageKnown || result.DefinitelyNotAttempted ||
		result.DefinitelyZeroUsage || !result.DisableThinking {
		t.Fatalf("measurement flags = %+v", result)
	}
	call := result.Call
	if call.RunSnapshotID == nil || *call.RunSnapshotID != 91 ||
		call.TenantID != &tenantID || call.UserID != &userID || call.RefID != &refID ||
		call.TraceID != "trace" || call.SpanName != "research_planner" ||
		call.Provider == "" || call.Model != "provider-model" ||
		call.SystemPrompt != "system" || call.UserPrompt != "user" ||
		call.Completion != "answer" || call.PromptTokens != 100 ||
		call.CompletionTokens != 25 || call.Temperature != &temperature ||
		call.MaxTokens != &maxTokens || call.Error != "" || call.LatencyMs < 0 {
		t.Fatalf("call metadata = %+v", call)
	}
	if call.PromptCacheHitTokens == nil || *call.PromptCacheHitTokens != 40 ||
		call.PromptCacheMissTokens == nil || *call.PromptCacheMissTokens != 60 ||
		call.PrefixCacheHit == nil || !*call.PrefixCacheHit ||
		call.ReasoningTokens == nil || *call.ReasoningTokens != 10 {
		t.Fatalf("call usage breakdown = %+v", call)
	}
}

func TestDoMeasuredV3_BeforeSendFailureIsDefinitelyUnattempted(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	t.Cleanup(srv.Close)
	wantErr := errors.New("durable claim rejected")
	result, err := DoMeasuredV3(t.Context(), newTestClient(srv.URL, 1), CallMeta{}, Request{
		User: "user", BeforeSend: func(context.Context) error { return wantErr },
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if result.Attempted || result.UsageKnown || !result.DefinitelyNotAttempted ||
		!result.DefinitelyZeroUsage || result.Response != nil || requests.Load() != 0 {
		t.Fatalf("result = %+v, requests = %d", result, requests.Load())
	}
	if result.Call.Error == "" {
		t.Fatal("failure omitted from LLMCall")
	}
}

func TestDoMeasuredV3_ConcurrentQueueCancellationIsDefinitelyUnattempted(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	t.Cleanup(srv.Close)
	client := newTestClient(srv.URL, 1)
	firstDone := make(chan error, 1)
	go func() {
		_, err := client.Complete(t.Context(), Request{User: "occupy"})
		firstDone <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	var beforeSend atomic.Int32
	result, err := DoMeasuredV3(ctx, client, CallMeta{}, Request{
		User: "queued", BeforeSend: func(context.Context) error {
			beforeSend.Add(1)
			return nil
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if result.Attempted || !result.DefinitelyNotAttempted || !result.DefinitelyZeroUsage ||
		result.UsageKnown || beforeSend.Load() != 0 {
		t.Fatalf("result = %+v, beforeSend = %d", result, beforeSend.Load())
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("occupying call error = %v", err)
	}
}

func TestDoMeasuredV3_HTTPAndProtocolFailureClassification(t *testing.T) {
	tests := []struct {
		name               string
		status             int
		body               string
		wantCode           types.ErrCode
		wantUsageKnown     bool
		wantDefinitelyZero bool
	}{
		{name: "http 400", status: http.StatusBadRequest, body: "bad", wantCode: types.CodeLLMBadRequest, wantDefinitelyZero: true},
		{name: "http 429", status: http.StatusTooManyRequests, body: "slow", wantCode: types.CodeLLMRateLimit, wantDefinitelyZero: true},
		{name: "http 500", status: http.StatusInternalServerError, body: "failed", wantCode: types.CodeLLMUnavailable},
		{name: "invalid json", status: http.StatusOK, body: "{", wantCode: types.CodeLLMUnavailable},
		{
			name: "invalid shape with usage", status: http.StatusOK,
			body:     `{"model":"actual","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":3}}`,
			wantCode: types.CodeLLMUnavailable, wantUsageKnown: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(srv.Close)
			result, err := DoMeasuredV3(t.Context(), newTestClient(srv.URL, 1),
				CallMeta{}, Request{User: "user", Model: "requested"})
			if types.CodeOf(err) != tt.wantCode {
				t.Fatalf("error = %v, code = %q, want %q", err, types.CodeOf(err), tt.wantCode)
			}
			if !result.Attempted || result.DefinitelyNotAttempted ||
				result.UsageKnown != tt.wantUsageKnown ||
				result.DefinitelyZeroUsage != tt.wantDefinitelyZero {
				t.Fatalf("result flags = %+v", result)
			}
			if result.Call.Error == "" || result.Call.Completion != "" {
				t.Fatalf("failure call = %+v", result.Call)
			}
			if tt.wantUsageKnown {
				if result.Response == nil || result.Call.Model != "actual" ||
					result.Call.PromptTokens != 12 || result.Call.CompletionTokens != 3 {
					t.Fatalf("metadata response/call = %+v / %+v", result.Response, result.Call)
				}
			} else if result.Response != nil {
				t.Fatalf("response = %+v, want nil", result.Response)
			}
		})
	}
}

func TestDoMeasuredV3_TimeoutAfterSendIsIndeterminate(t *testing.T) {
	entered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-time.After(time.Second)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	result, err := DoMeasuredV3(ctx, newTestClient(srv.URL, 1), CallMeta{}, Request{User: "user"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	<-entered
	if !result.Attempted || result.UsageKnown || result.DefinitelyNotAttempted ||
		result.DefinitelyZeroUsage || result.Response != nil {
		t.Fatalf("result = %+v", result)
	}
	if result.Call.Error == "" || result.Call.LatencyMs < 1 {
		t.Fatalf("timeout call = %+v", result.Call)
	}
}

func TestDoMeasuredV3_HTTP200IncompleteUsageIsIndeterminate(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing usage", body: `{"model":"provider-model","choices":[{"message":{"content":"answer"}}]}`},
		{name: "empty usage", body: `{"model":"provider-model","choices":[{"message":{"content":"answer"}}],"usage":{}}`},
		{name: "missing completion", body: `{"model":"provider-model","choices":[{"message":{"content":"answer"}}],"usage":{"prompt_tokens":12}}`},
		{name: "zero total", body: `{"model":"provider-model","choices":[{"message":{"content":"answer"}}],"usage":{"prompt_tokens":0,"completion_tokens":0}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(srv.Close)
			result, err := DoMeasuredV3(t.Context(), newTestClient(srv.URL, 1),
				CallMeta{}, Request{User: "user"})
			if types.CodeOf(err) != types.CodeLLMUnavailable {
				t.Fatalf("error = %v, want unavailable", err)
			}
			if !result.Attempted || result.UsageKnown || result.DefinitelyZeroUsage ||
				result.Response == nil || result.Response.UsageReported ||
				result.Response.Content != "" || result.Call.Error == "" ||
				result.Call.Completion != "" || result.Call.PromptTokens != 0 ||
				result.Call.CompletionTokens != 0 {
				t.Fatalf("incomplete usage was not fail-closed: %+v", result)
			}
		})
	}
}
