package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const productionDSMLLeak = `<｜｜DSML｜｜tool_calls>
<｜｜DSML｜｜invoke name="create_schedule">
<｜｜DSML｜｜parameter name="name" string="true">AI 行业早报</｜｜DSML｜｜parameter>
<｜｜DSML｜｜parameter name="task_ids" array="true">["task-26"]</｜｜DSML｜｜parameter>
<｜｜DSML｜｜parameter name="schedule" string="true">每天 08:30</｜｜DSML｜｜parameter>
</｜｜DSML｜｜invoke>
</｜｜DSML｜｜tool_calls>`

func newDSMLResponseClientWithFinish(t *testing.T, responseModel, finishReason, content string, native []wireToolCall) *Client {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"model": responseModel,
		"choices": []any{map[string]any{
			"finish_reason": finishReason,
			"message": map[string]any{
				"role":       "assistant",
				"content":    content,
				"tool_calls": native,
			},
		}},
		"usage": map[string]any{
			"prompt_tokens":            20,
			"completion_tokens":        8,
			"prompt_cache_hit_tokens":  12,
			"prompt_cache_miss_tokens": 8,
		},
	})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(body); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return newTestClient(srv.URL, 5)
}

func newDSMLResponseClient(t *testing.T, responseModel, content string, native []wireToolCall) *Client {
	t.Helper()
	finishReason := "stop"
	if len(native) > 0 {
		finishReason = "tool_calls"
	}
	return newDSMLResponseClientWithFinish(t, responseModel, finishReason, content, native)
}

func chatWithDSMLResponse(t *testing.T, content string, native []wireToolCall, tools []ToolDef) (*ChatResponse, error) {
	t.Helper()
	c := newDSMLResponseClient(t, "deepseek-v4-pro", content, native)
	return c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "创建任务"}},
		Tools:    tools,
	})
}

func TestRedactLeakedDSMLContent(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		want       string
		wantChange bool
	}{
		{name: "observed doubled token", content: productionDSMLLeak, want: dsmlSafeContent, wantChange: true},
		{name: "canonical fullwidth token", content: `<｜DSML｜tool_calls>`, want: dsmlSafeContent, wantChange: true},
		{name: "ASCII prose", content: "DSML is a format", want: "DSML is a format"},
		{name: "ASCII pipes", content: `<||DSML||tool_calls>`, want: `<||DSML||tool_calls>`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := RedactLeakedDSMLContent(tt.content)
			if got != tt.want || changed != tt.wantChange {
				t.Fatalf("redact = (%q, %v), want (%q, %v)", got, changed, tt.want, tt.wantChange)
			}
		})
	}
}

func TestChatRejectsProductionDSMLLeakWithoutExecuting(t *testing.T) {
	resp, err := chatWithDSMLResponse(t, productionDSMLLeak, nil, []ToolDef{{Name: "create_schedule"}})
	if err == nil {
		t.Fatalf("expected fixed fail-closed error, got response %+v", resp)
	}
	if resp == nil {
		t.Fatal("Chat must return safe billing metadata with the error")
	}
	if resp.Content != "" || len(resp.ToolCalls) != 0 {
		t.Fatalf("unsafe partial response: %+v", resp)
	}
	if resp.Model != "deepseek-v4-pro" || resp.PromptTokens != 20 ||
		resp.CompletionTokens != 8 || resp.CacheHitTokens != 12 || resp.CacheMissTokens != 8 {
		t.Fatalf("billing metadata was lost: %+v", resp)
	}
	if code := types.CodeOf(err); code != types.CodeLLMUnavailable {
		t.Errorf("error code = %s, want %s", code, types.CodeLLMUnavailable)
	}
	if types.IsRetryable(err) {
		t.Error("DSML protocol error must be non-retryable")
	}
	if !errors.Is(err, ErrToolProtocolResponse) {
		t.Fatalf("DSML error is not classifiable by callers: %v", err)
	}
	const want = "LLM_UNAVAILABLE: llm: 上游工具调用格式异常"
	if err.Error() != want {
		t.Errorf("error = %q, want fixed %q", err, want)
	}
	if strings.Contains(err.Error(), "DSML") || strings.Contains(err.Error(), "create_schedule") {
		t.Errorf("error leaks protocol details: %q", err)
	}
}

func TestChatRejectsDSMLMarkerWithSurroundingText(t *testing.T) {
	content := "I will create it now.\n" + productionDSMLLeak + "\ndone"
	resp, err := chatWithDSMLResponse(t, content, nil, []ToolDef{{Name: "create_schedule"}})
	if err == nil || resp == nil || resp.Content != "" || len(resp.ToolCalls) != 0 {
		t.Fatalf("DSML marker with prose must fail closed: resp=%+v err=%v", resp, err)
	}
}

func TestChatMixedNativeToolCallsAndDSMLFailsClosed(t *testing.T) {
	native := []wireToolCall{{
		ID:       "call_native",
		Type:     "function",
		Function: wireFunctionCall{Name: "view_profile", Arguments: `{}`},
	}}
	malicious := `<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name="delete_everything">`
	resp, err := chatWithDSMLResponse(t, malicious, native, []ToolDef{{Name: "view_profile"}})
	if err == nil {
		t.Fatalf("mixed native+DSML response must fail closed: %+v", resp)
	}
	if resp == nil || resp.Content != "" || len(resp.ToolCalls) != 0 {
		t.Fatalf("mixed response retained executable or raw content: %+v", resp)
	}
}

func TestChatHarmlessDSMLProseIsPreserved(t *testing.T) {
	content := "DSML is an internal tool-call markup format."
	resp, err := chatWithDSMLResponse(t, content, nil, []ToolDef{{Name: "view_profile"}})
	if err != nil {
		t.Fatalf("harmless prose must not be rejected: %v", err)
	}
	if resp.Content != content || len(resp.ToolCalls) != 0 {
		t.Fatalf("harmless prose was changed: %+v", resp)
	}
}

func TestChatToolFreeDSMLFailsClosedWithoutDurableState(t *testing.T) {
	resp, err := chatWithDSMLResponse(t, productionDSMLLeak, nil, nil)
	if err == nil {
		t.Fatalf("tool-free DSML without caller-owned durable state must fail closed: %+v", resp)
	}
	if resp == nil || resp.Content != "" || len(resp.ToolCalls) != 0 {
		t.Fatalf("tool-free DSML retained executable or raw content: %+v", resp)
	}
	if types.IsRetryable(err) {
		t.Error("deterministic protocol leak must be non-retryable")
	}
	if !errors.Is(err, ErrToolProtocolResponse) {
		t.Fatalf("tool-free DSML error is not classifiable by callers: %v", err)
	}
}

func TestChatDSMLWorkaroundIsProviderAndModelScoped(t *testing.T) {
	tests := []struct {
		name          string
		provider      string
		requestModel  string
		responseModel string
	}{
		{name: "other provider", provider: "openai", requestModel: "deepseek-v4-pro", responseModel: "deepseek-v4-pro"},
		{name: "other model", provider: "deepseek", requestModel: "deepseek-v3", responseModel: "deepseek-v3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newDSMLResponseClient(t, tt.responseModel, productionDSMLLeak, nil)
			c.provider = tt.provider
			resp, err := c.Chat(t.Context(), ChatRequest{
				Model:    tt.requestModel,
				Messages: []ChatMessage{{Role: "user", Content: "explain"}},
				Tools:    []ToolDef{{Name: "create_schedule"}},
			})
			if err != nil {
				t.Fatalf("out-of-scope response was rejected: %v", err)
			}
			if resp.Content != productionDSMLLeak {
				t.Fatalf("out-of-scope content was changed: %q", resp.Content)
			}
		})
	}
}

func TestChatDeepSeekV4ToolCallFinishReasonInvariant(t *testing.T) {
	tests := []struct {
		name         string
		finishReason string
		native       []wireToolCall
	}{
		{name: "finish says tool calls but native absent", finishReason: "tool_calls"},
		{name: "native present but finish says stop", finishReason: "stop", native: []wireToolCall{{
			ID: "call_native", Type: "function", Function: wireFunctionCall{Name: "view_profile", Arguments: `{}`},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newDSMLResponseClientWithFinish(t, "deepseek-v4-pro", tt.finishReason, "ordinary", tt.native)
			resp, err := c.Chat(t.Context(), ChatRequest{
				Messages: []ChatMessage{{Role: "user", Content: "list"}},
				Tools:    []ToolDef{{Name: "view_profile"}},
			})
			if err == nil || resp == nil || resp.Content != "" || len(resp.ToolCalls) != 0 {
				t.Fatalf("mismatch did not fail closed: resp=%+v err=%v", resp, err)
			}
			if types.IsRetryable(err) {
				t.Error("deterministic protocol mismatch must be non-retryable")
			}
		})
	}
}

func TestChatToolFreeIgnoresFinishReasonDrift(t *testing.T) {
	c := newDSMLResponseClientWithFinish(t, "deepseek-v4-pro", "tool_calls", "final confirmation", nil)
	resp, err := c.Chat(t.Context(), ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "finish"}}})
	if err != nil {
		t.Fatalf("tool-free finalizer must not be orphaned: %v", err)
	}
	if resp.Content != "final confirmation" || len(resp.ToolCalls) != 0 {
		t.Fatalf("tool-free response changed: %+v", resp)
	}
}

func TestChatScrubsLegacyDSMLHistoryAcrossRoles(t *testing.T) {
	var gotBody map[string]any
	srv := newChatCaptureServer(t, chatOKBody, &gotBody)
	t.Cleanup(srv.Close)
	c := newTestClient(srv.URL, 1)
	_, err := c.Chat(t.Context(), ChatRequest{Messages: []ChatMessage{
		{Role: "user", Content: productionDSMLLeak},
		{Role: "assistant", Content: productionDSMLLeak, ToolCalls: []ToolCall{{
			ID: "legacy-native", Name: "view_profile", Arguments: `{}`,
		}}},
		{Role: "tool", ToolCallID: "legacy-native", Content: productionDSMLLeak},
		{Role: "user", Content: "继续"},
	}})
	if err != nil {
		t.Fatalf("Chat returned error: %v", err)
	}
	messages, ok := gotBody["messages"].([]any)
	if !ok || len(messages) != 4 {
		t.Fatalf("captured messages = %#v", gotBody["messages"])
	}
	for i := range 3 {
		message, _ := messages[i].(map[string]any)
		if message["content"] != dsmlSafeContent || strings.Contains(fmt.Sprint(message["content"]), "DSML") {
			t.Fatalf("legacy DSML role %d was resent: %#v", i, message)
		}
	}
	assistant, _ := messages[1].(map[string]any)
	if calls, ok := assistant["tool_calls"].([]any); !ok || len(calls) != 1 {
		t.Fatalf("legacy native call chain was broken: %#v", assistant["tool_calls"])
	}
	toolMessage, _ := messages[2].(map[string]any)
	if toolMessage["tool_call_id"] != "legacy-native" {
		t.Fatalf("legacy ToolCallID was lost: %#v", toolMessage)
	}
}

type dsmlAccountingStore struct {
	call     *types.LLMCall
	estimate float64
	adjust   float64
}

func (s *dsmlAccountingStore) InsertLLMCall(_ context.Context, call *types.LLMCall) (int64, error) {
	clone := *call
	s.call = &clone
	return 1, nil
}

func (s *dsmlAccountingStore) TryConsumeForUser(_ context.Context, _ int64, _ store.QuotaBucket, amount float64) error {
	s.estimate = amount
	return nil
}

func (s *dsmlAccountingStore) AdjustForUser(_ context.Context, _ int64, _ store.QuotaBucket, delta float64) error {
	s.adjust = delta
	return nil
}

func TestDoChatAccountsRejectedDSMLWithoutLeakingIt(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"model": "deepseek-v4-pro",
		"choices": []any{map[string]any{
			"finish_reason": "stop",
			"message":       map[string]any{"role": "assistant", "content": productionDSMLLeak},
		}},
		"usage": map[string]any{
			"prompt_tokens": 20, "completion_tokens": 8,
			"prompt_cache_hit_tokens": 12, "prompt_cache_miss_tokens": 8,
		},
	})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write(body); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	st := &dsmlAccountingStore{}
	recorder := &Recorder{st: st}
	userID := int64(7)
	resp, err := DoChat(t.Context(), newTestClient(srv.URL, 1), recorder,
		CallMeta{TraceID: "dsml-accounting", SpanName: "agent", UserID: &userID},
		ChatRequest{
			Messages: []ChatMessage{
				{Role: "user", Content: productionDSMLLeak},
				{Role: "assistant", Content: productionDSMLLeak},
				{Role: "tool", ToolCallID: "legacy", Content: productionDSMLLeak},
				{Role: "user", Content: "创建任务"},
			},
			Tools: []ToolDef{{Name: "create_schedule"}},
		})
	if err == nil || resp != nil {
		t.Fatalf("DoChat must expose only the error: resp=%+v err=%v", resp, err)
	}
	if !errors.Is(err, ErrToolProtocolResponse) {
		t.Fatalf("DoChat lost the protocol classification needed by Agent recovery: %v", err)
	}
	if st.call == nil {
		t.Fatal("DSML protocol error was not recorded")
	}
	if st.call.PromptTokens != 20 || st.call.CompletionTokens != 8 || st.call.CostUSD <= 0 {
		t.Fatalf("paid usage was lost: %+v", st.call)
	}
	if st.call.Model != "deepseek-v4-pro" || st.call.Completion != "" {
		t.Fatalf("unsafe or incomplete ledger row: %+v", st.call)
	}
	if strings.Contains(st.call.UserPrompt, "DSML") || strings.Contains(st.call.UserPrompt, "[26]") ||
		!strings.Contains(st.call.UserPrompt, dsmlSafeContent) {
		t.Fatalf("ledger UserPrompt retained legacy DSML: %q", st.call.UserPrompt)
	}
	const safeError = "LLM_UNAVAILABLE: llm: 上游工具调用格式异常"
	if st.call.Error != safeError || strings.Contains(st.call.Error, "DSML") ||
		strings.Contains(st.call.Completion, "DSML") {
		t.Fatalf("ledger leaked raw protocol text: completion=%q error=%q", st.call.Completion, st.call.Error)
	}
	const actualTokens = 28
	if st.adjust != st.estimate-actualTokens {
		t.Fatalf("quota reconciled as zero usage: estimate=%v adjust=%v want=%v",
			st.estimate, st.adjust, st.estimate-actualTokens)
	}
}
