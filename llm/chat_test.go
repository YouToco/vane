package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// chatOKBody 模拟纯文本回复（无 tool_calls）的正常响应。
const chatOKBody = `{
	"model": "deepseek-v4-pro",
	"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "好的，已了解。"}}],
	"usage": {
		"prompt_tokens": 20,
		"completion_tokens": 8,
		"prompt_cache_hit_tokens": 12,
		"prompt_cache_miss_tokens": 8
	}
}`

// chatToolCallsBody 模拟 function calling 响应：content 为空、
// tool_calls 两条（DeepSeek 实测形态：嵌套 function 对象 + type 字段）。
const chatToolCallsBody = `{
	"model": "deepseek-v4-pro",
	"choices": [{
		"finish_reason": "tool_calls",
		"message": {
			"role": "assistant",
			"content": "",
			"tool_calls": [
				{"id": "call_abc", "type": "function", "function": {"name": "view_profile", "arguments": "{}"}},
				{"id": "call_def", "type": "function", "function": {"name": "remove_schedule", "arguments": "{\"task_ids\":[\"task-3\"]}"}}
			]
		}
	}],
	"usage": {
		"prompt_tokens": 30,
		"completion_tokens": 12,
		"prompt_cache_hit_tokens": 0,
		"prompt_cache_miss_tokens": 30
	}
}`

// newChatCaptureServer 返回记录请求体的 httptest server，响应固定 body。
func newChatCaptureServer(t *testing.T, respBody string, gotBody *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(gotBody); err != nil {
			t.Errorf("解析请求体失败: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(respBody))
	}))
}

// TestChatMessagesAndToolsSerialization 断言线协议请求形态（契约 §4）：
// tools 用 {type:"function",function:{name,description,parameters}} 包装；
// assistant 历史消息的 tool_calls 原样回传（嵌套 function、arguments 原文）；
// role=tool 消息携带 tool_call_id。
func TestChatMessagesAndToolsSerialization(t *testing.T) {
	var gotBody map[string]any
	srv := newChatCaptureServer(t, chatOKBody, &gotBody)
	defer srv.Close()

	schema := json.RawMessage(`{"type":"object","properties":{"task_ids":{"type":"array","items":{"type":"string"}}},"required":["task_ids"]}`)
	c := newTestClient(srv.URL, 5)
	if _, err := c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{
			{Role: "system", Content: "你是见微助理"},
			{Role: "user", Content: "删掉源 3"},
			{Role: "assistant", Content: "", ToolCalls: []ToolCall{
				{ID: "call_abc", Name: "remove_schedule", Arguments: `{"task_ids":["task-3"]}`},
			}},
			{Role: "tool", ToolCallID: "call_abc", Content: "已删除"},
		},
		Tools: []ToolDef{{Name: "remove_schedule", Description: "删除一个任务", Parameters: schema}},
	}); err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}

	// Model 未覆盖时用 Client 默认。
	if gotBody["model"] != "deepseek-v4-flash" {
		t.Errorf("model = %v, 期望默认 deepseek-v4-flash", gotBody["model"])
	}

	// tools 序列化形态。
	tools, ok := gotBody["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v, 期望长度 1 的数组", gotBody["tools"])
	}
	tool0, _ := tools[0].(map[string]any)
	if tool0["type"] != "function" {
		t.Errorf("tools[0].type = %v, 期望 function", tool0["type"])
	}
	fn, _ := tool0["function"].(map[string]any)
	if fn["name"] != "remove_schedule" || fn["description"] != "删除一个任务" {
		t.Errorf("tools[0].function = %v, name/description 不符", fn)
	}
	params, _ := fn["parameters"].(map[string]any)
	if params["type"] != "object" {
		t.Errorf("tools[0].function.parameters = %v, 期望 JSON schema 原样携带", fn["parameters"])
	}

	msgs, ok := gotBody["messages"].([]any)
	if !ok || len(msgs) != 4 {
		t.Fatalf("messages 长度 = %d, 期望 4", len(msgs))
	}

	// assistant 历史消息必须原样带 tool_calls（嵌套 function 形态）。
	asst, _ := msgs[2].(map[string]any)
	if asst["role"] != "assistant" {
		t.Fatalf("messages[2].role = %v, 期望 assistant", asst["role"])
	}
	tcs, ok := asst["tool_calls"].([]any)
	if !ok || len(tcs) != 1 {
		t.Fatalf("messages[2].tool_calls = %v, 期望长度 1 的数组", asst["tool_calls"])
	}
	tc0, _ := tcs[0].(map[string]any)
	if tc0["id"] != "call_abc" || tc0["type"] != "function" {
		t.Errorf("tool_calls[0] id/type = %v/%v, 期望 call_abc/function", tc0["id"], tc0["type"])
	}
	tcFn, _ := tc0["function"].(map[string]any)
	if tcFn["name"] != "remove_schedule" {
		t.Errorf("tool_calls[0].function.name = %v, 期望 remove_schedule", tcFn["name"])
	}
	if tcFn["arguments"] != `{"task_ids":["task-3"]}` {
		t.Errorf("tool_calls[0].function.arguments = %v, 期望原始 JSON 字符串原样携带", tcFn["arguments"])
	}

	// role=tool 消息必须带 tool_call_id 与结果 content。
	toolMsg, _ := msgs[3].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "call_abc" {
		t.Errorf("messages[3] = %v, 期望 role=tool 且 tool_call_id=call_abc", toolMsg)
	}
	if toolMsg["content"] != "已删除" {
		t.Errorf("messages[3].content = %v, 期望 已删除", toolMsg["content"])
	}
}

// TestChatDefaultsOmitOptional 未设置的可选项一律不携带：thinking、tools、
// temperature、max_tokens 都不得出现在请求体（空 tools 数组对部分兼容实现非法）。
func TestChatDefaultsOmitOptional(t *testing.T) {
	var gotBody map[string]any
	srv := newChatCaptureServer(t, chatOKBody, &gotBody)
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	if _, err := c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	}); err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}

	for _, field := range []string{
		"thinking", "tools", "tool_choice", "temperature", "max_tokens",
	} {
		if _, present := gotBody[field]; present {
			t.Errorf("未设置时不应携带 %s 字段，实际 %v", field, gotBody[field])
		}
	}
}

func TestChatSerializesRequiredToolChoice(t *testing.T) {
	var gotBody map[string]any
	srv := newChatCaptureServer(t, chatOKBody, &gotBody)
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	if _, err := c.Chat(context.Background(), ChatRequest{
		Messages:   []ChatMessage{{Role: "user", Content: "edit it"}},
		Tools:      []ToolDef{{Name: "edit_task_definition"}},
		ToolChoice: ToolChoiceRequired,
	}); err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}

	if gotBody["tool_choice"] != "required" {
		t.Fatalf("tool_choice=%v, want required", gotBody["tool_choice"])
	}
}

// TestChatDisableThinking DisableThinking=true 必须序列化为 thinking:{type:"disabled"}。
func TestChatDisableThinking(t *testing.T) {
	var gotBody map[string]any
	srv := newChatCaptureServer(t, chatOKBody, &gotBody)
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	if _, err := c.Chat(context.Background(), ChatRequest{
		Messages:        []ChatMessage{{Role: "user", Content: "hi"}},
		DisableThinking: true,
	}); err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}

	th, ok := gotBody["thinking"].(map[string]any)
	if !ok {
		t.Fatalf("thinking 字段缺失或类型错误: %v", gotBody["thinking"])
	}
	if th["type"] != "disabled" {
		t.Errorf("thinking.type = %v, 期望 disabled", th["type"])
	}
}

func TestChatKimiK26UsesStrictToolSchema(t *testing.T) {
	const kimiToolCallBody = `{
		"model": "kimi-k2.6",
		"choices": [{
			"finish_reason": "tool_calls",
			"message": {
				"role": "assistant",
				"content": "",
				"tool_calls": [{
					"id": "call_kimi",
					"type": "function",
					"function": {"name": "create_schedule", "arguments": "{\"intent\":\"weekly AI updates\"}"}
				}]
			}
		}],
		"usage": {"prompt_tokens": 20, "completion_tokens": 8}
	}`

	var gotBody map[string]any
	srv := newChatCaptureServer(t, kimiToolCallBody, &gotBody)
	defer srv.Close()

	c := New(config.LLMConfig{
		Provider:      "kimi",
		BaseURL:       srv.URL,
		APIKey:        "test-key",
		Model:         "kimi-k2.6",
		MaxConcurrent: 1,
	})
	legacyTemp := float32(0.3)
	resp, err := c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "create it"}},
		Tools: []ToolDef{{
			Name:       "create_schedule",
			Parameters: json.RawMessage(`{"type":"object","properties":{"intent":{"type":"string"}},"required":["intent"],"additionalProperties":false}`),
		}},
		Temperature:     &legacyTemp,
		DisableThinking: true,
	})
	if err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "create_schedule" {
		t.Fatalf("ToolCalls = %+v", resp.ToolCalls)
	}

	tools, ok := gotBody["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v", gotBody["tools"])
	}
	fn, _ := tools[0].(map[string]any)["function"].(map[string]any)
	if fn["strict"] != true {
		t.Fatalf("Kimi tool schema must set strict=true, got %v", fn["strict"])
	}
	thinking, _ := gotBody["thinking"].(map[string]any)
	if thinking["type"] != "disabled" {
		t.Fatalf("Kimi K2.6 should disable thinking for deterministic FC, got %v", gotBody["thinking"])
	}
	if _, present := gotBody["temperature"]; present {
		t.Fatalf("Kimi K2.6 has a provider-fixed temperature; client must omit caller value, got %v",
			gotBody["temperature"])
	}
}

// TestChatModelOverride ChatRequest.Model 非空时必须替代 Client 默认模型发请求。
func TestChatModelOverride(t *testing.T) {
	var gotBody map[string]any
	srv := newChatCaptureServer(t, chatOKBody, &gotBody)
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	resp, err := c.Chat(context.Background(), ChatRequest{
		Model:    "deepseek-v4-pro",
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}
	if gotBody["model"] != "deepseek-v4-pro" {
		t.Errorf("model = %v, 期望覆盖为 deepseek-v4-pro", gotBody["model"])
	}
	if resp.Model != "deepseek-v4-pro" {
		t.Errorf("resp.Model = %q, 期望上游回报的 deepseek-v4-pro", resp.Model)
	}
}

// TestChatToolCallsParsing 响应中的 tool_calls 必须解析为扁平 ToolCall
// （id / function.name / function.arguments），并透传 finish_reason 与 usage。
func TestChatToolCallsParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(chatToolCallsBody))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	resp, err := c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "列出我的信源，然后删掉源 3"}},
		Tools: []ToolDef{
			{Name: "view_profile", Parameters: json.RawMessage(`{"type":"object"}`)},
			{Name: "remove_schedule", Parameters: json.RawMessage(`{"type":"object"}`)},
		},
	})
	if err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}

	if resp.FinishReason != "tool_calls" {
		t.Errorf("FinishReason = %q, 期望 tool_calls", resp.FinishReason)
	}
	if resp.Content != "" {
		t.Errorf("Content = %q, 期望空（FC 响应）", resp.Content)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("ToolCalls 长度 = %d, 期望 2", len(resp.ToolCalls))
	}
	if tc := resp.ToolCalls[0]; tc.ID != "call_abc" || tc.Name != "view_profile" || tc.Arguments != "{}" {
		t.Errorf("ToolCalls[0] = %+v, 期望 {call_abc view_profile {}}", tc)
	}
	if tc := resp.ToolCalls[1]; tc.ID != "call_def" || tc.Name != "remove_schedule" || tc.Arguments != `{"task_ids":["task-3"]}` {
		t.Errorf("ToolCalls[1] = %+v, 期望 arguments 原始 JSON 字符串", tc)
	}
	if resp.PromptTokens != 30 || resp.CompletionTokens != 12 {
		t.Errorf("tokens = (%d, %d), 期望 (30, 12)", resp.PromptTokens, resp.CompletionTokens)
	}
	if resp.CacheHitTokens != 0 || resp.CacheMissTokens != 30 {
		t.Errorf("cache tokens = (%d, %d), 期望 (0, 30)", resp.CacheHitTokens, resp.CacheMissTokens)
	}
	if resp.Model != "deepseek-v4-pro" {
		t.Errorf("Model = %q, 期望 deepseek-v4-pro", resp.Model)
	}
}

// TestChatPlainContent 纯文本回复解析：Content 非空、无 ToolCalls。
func TestChatPlainContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(chatOKBody))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	resp, err := c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "你好"}},
	})
	if err != nil {
		t.Fatalf("Chat 返回错误: %v", err)
	}
	if resp.Content != "好的，已了解。" {
		t.Errorf("Content = %q", resp.Content)
	}
	if len(resp.ToolCalls) != 0 {
		t.Errorf("ToolCalls = %v, 期望空", resp.ToolCalls)
	}
	if resp.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, 期望 stop", resp.FinishReason)
	}
}

// TestChat429RateLimit Chat 与 Complete 共用错误映射：429 → CodeLLMRateLimit。
func TestChat429RateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"rate limit exceeded"}}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	_, err := c.Chat(context.Background(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("期望错误，实际为 nil")
	}
	if code := types.CodeOf(err); code != types.CodeLLMRateLimit {
		t.Errorf("错误码 = %s, 期望 %s", code, types.CodeLLMRateLimit)
	}
}

// TestDoChatPassthrough DoChat 成功路径透传 Chat 结果；rec 为 nil 时
// 记账静默跳过（Recorder.Record 的 nil 防御），不影响返回。
func TestDoChatPassthrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(chatToolCallsBody))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	resp, err := DoChat(context.Background(), c, nil, CallMeta{SpanName: "agent"}, ChatRequest{
		Model:    "deepseek-v4-pro",
		Messages: []ChatMessage{{Role: "user", Content: "列出信源"}},
		Tools: []ToolDef{
			{Name: "view_profile", Parameters: json.RawMessage(`{"type":"object"}`)},
			{Name: "remove_schedule", Parameters: json.RawMessage(`{"type":"object"}`)},
		},
	})
	if err != nil {
		t.Fatalf("DoChat 返回错误: %v", err)
	}
	if len(resp.ToolCalls) != 2 {
		t.Errorf("ToolCalls 长度 = %d, 期望 2", len(resp.ToolCalls))
	}
}

// TestDoChatErrorPropagates DoChat 失败路径：错误原样上抛（记账旁路不吞错）。
func TestDoChatErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 5)
	_, err := DoChat(context.Background(), c, nil, CallMeta{SpanName: "agent"}, ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("期望错误，实际为 nil")
	}
	if code := types.CodeOf(err); code != types.CodeLLMUnavailable {
		t.Errorf("错误码 = %s, 期望 %s", code, types.CodeLLMUnavailable)
	}
}

func TestDoChatUnknownUsageFailureRetainsConservativeReservation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream failed after accepting request", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	userID := int64(42)
	st := &capturingRecorderStore{}
	rec := &Recorder{st: st}
	_, err := DoChat(t.Context(), newTestClient(srv.URL, 5), rec,
		CallMeta{TraceID: "unknown-usage", SpanName: "agent", UserID: &userID},
		ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "research"}}})
	if types.CodeOf(err) != types.CodeLLMUnavailable {
		t.Fatalf("error code = %s, want %s", types.CodeOf(err), types.CodeLLMUnavailable)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.legacyTryCalls != 1 {
		t.Fatalf("quota reservations = %d, want 1", st.legacyTryCalls)
	}
	if st.legacyAdjustCalls != 0 {
		t.Fatalf("unknown usage refunds = %d, want 0", st.legacyAdjustCalls)
	}
	if len(st.calls) != 1 || st.calls[0].Error == "" {
		t.Fatalf("ledger calls = %#v, want one explicit error row", st.calls)
	}
}

func TestDoChatExplicitRateLimitRefundsReservation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"rate limit exceeded"}}`, http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	userID := int64(42)
	st := &capturingRecorderStore{}
	rec := &Recorder{st: st}
	_, err := DoChat(t.Context(), newTestClient(srv.URL, 5), rec,
		CallMeta{TraceID: "rate-limit-refund", SpanName: "agent", UserID: &userID},
		ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "research"}}})
	if types.CodeOf(err) != types.CodeLLMRateLimit {
		t.Fatalf("error code = %s, want %s", types.CodeOf(err), types.CodeLLMRateLimit)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.legacyTryCalls != 1 || st.legacyAdjustCalls != 1 {
		t.Fatalf("quota reserve/adjust = %d/%d, want 1/1",
			st.legacyTryCalls, st.legacyAdjustCalls)
	}
	if len(st.legacyAdjustments) != 1 || st.legacyAdjustments[0] <= 0 {
		t.Fatalf("refund adjustments = %v, want one positive refund", st.legacyAdjustments)
	}
}

func TestDoChatSemaphoreWaitCancellationNeverReservesOrCallsUpstream(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(chatOKBody))
	}))
	t.Cleanup(srv.Close)

	c := newTestClient(srv.URL, 1)
	c.sem <- struct{}{} // occupy the only provider slot before DoChat reaches its gate
	defer func() { <-c.sem }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	userID := int64(42)
	st := &capturingRecorderStore{}
	_, err := DoChat(ctx, c, &Recorder{st: st},
		CallMeta{TraceID: "queue-cancel", SpanName: "agent", UserID: &userID},
		ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "research"}}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if hits != 0 {
		t.Fatalf("upstream calls = %d, want 0", hits)
	}
	if st.legacyTryCalls != 0 || st.legacyAdjustCalls != 0 {
		t.Fatalf("quota reserve/adjust = %d/%d, want 0/0",
			st.legacyTryCalls, st.legacyAdjustCalls)
	}
}

func TestDoChatCancellationAfterReservationRefundsBeforeSend(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(chatOKBody))
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(t.Context())
	userID := int64(42)
	st := &capturingRecorderStore{onTryConsume: cancel}
	_, err := DoChat(ctx, newTestClient(srv.URL, 1), &Recorder{st: st},
		CallMeta{TraceID: "post-reserve-cancel", SpanName: "agent", UserID: &userID},
		ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "research"}}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if hits != 0 {
		t.Fatalf("upstream calls = %d, want 0", hits)
	}
	if st.legacyTryCalls != 1 || st.legacyAdjustCalls != 1 {
		t.Fatalf("quota reserve/adjust = %d/%d, want 1/1",
			st.legacyTryCalls, st.legacyAdjustCalls)
	}
	if len(st.legacyAdjustments) != 1 || st.legacyAdjustments[0] <= 0 {
		t.Fatalf("refund adjustments = %v, want one positive refund", st.legacyAdjustments)
	}
}

// TestChatMessageRoundtripJSON ChatMessage 的 json tag 是 agent 会话持久化
// 格式（契约 §1/§7）：序列化再反序列化必须无损，且扁平 ToolCall 形态稳定。
func TestChatMessageRoundtripJSON(t *testing.T) {
	in := []ChatMessage{
		{Role: "user", Content: "删掉源 3"},
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "call_abc", Name: "remove_schedule", Arguments: `{"task_ids":["task-3"]}`}}},
		{Role: "tool", ToolCallID: "call_abc", Content: "已删除"},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal 失败: %v", err)
	}
	var out []ChatMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal 失败: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("长度 = %d, 期望 3", len(out))
	}
	if out[1].ToolCalls[0] != in[1].ToolCalls[0] {
		t.Errorf("ToolCall 往返不一致: %+v != %+v", out[1].ToolCalls[0], in[1].ToolCalls[0])
	}
	if out[2].ToolCallID != "call_abc" {
		t.Errorf("ToolCallID 往返丢失: %+v", out[2])
	}
}
