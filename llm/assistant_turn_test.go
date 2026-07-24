package llm

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/YouToco/vane/types"
)

type assistantTurnFixture struct {
	Name           string                  `json:"name"`
	Provider       string                  `json:"provider"`
	RequestModel   string                  `json:"request_model"`
	ResponseModel  string                  `json:"response_model"`
	ToolsDeclared  bool                    `json:"tools_declared"`
	FinishReason   string                  `json:"finish_reason"`
	Content        string                  `json:"content"`
	ToolCalls      []wireAssistantToolCall `json:"tool_calls"`
	WantStopReason string                  `json:"want_stop_reason"`
	WantToolCalls  int                     `json:"want_tool_calls"`
	WantError      bool                    `json:"want_error"`
}

func TestAdaptAssistantTurnConformance(t *testing.T) {
	raw, err := os.ReadFile("testdata/conformance/assistant_turns.json")
	if err != nil {
		t.Fatalf("read conformance fixtures: %v", err)
	}
	var fixtures []assistantTurnFixture
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatalf("decode conformance fixtures: %v", err)
	}
	if len(fixtures) == 0 {
		t.Fatal("conformance fixtures must not be empty")
	}

	names := make(map[string]struct{}, len(fixtures))
	for _, fixture := range fixtures {
		if _, exists := names[fixture.Name]; exists {
			t.Fatalf("duplicate conformance fixture name %q", fixture.Name)
		}
		names[fixture.Name] = struct{}{}
		t.Run(fixture.Name, func(t *testing.T) {
			turn, err := adaptAssistantTurn(wireChatChoice{
				FinishReason: wireString{Value: fixture.FinishReason, Valid: true},
				Message: wireAssistantMessage{
					Content:   wireNullableString{Value: fixture.Content, Valid: true},
					ToolCalls: fixture.ToolCalls,
				},
			}, assistantTurnOptions{
				Provider:      fixture.Provider,
				RequestModel:  fixture.RequestModel,
				ResponseModel: fixture.ResponseModel,
				ToolsDeclared: fixture.ToolsDeclared,
			})
			if fixture.WantError {
				if !errors.Is(err, ErrToolProtocolResponse) {
					t.Fatalf("error = %v, want ErrToolProtocolResponse", err)
				}
				if turn.Content != "" || len(turn.ToolCalls) != 0 ||
					turn.StopReason != StopReasonUnknown {
					t.Fatalf("unsafe turn escaped on error: %+v", turn)
				}
				return
			}
			if err != nil {
				t.Fatalf("adaptAssistantTurn returned error: %v", err)
			}
			if got := turn.StopReason.String(); got != fixture.WantStopReason {
				t.Fatalf("StopReason = %q, want %q", got, fixture.WantStopReason)
			}
			if turn.Content != fixture.Content {
				t.Fatalf("Content = %q, want %q", turn.Content, fixture.Content)
			}
			if len(turn.ToolCalls) != fixture.WantToolCalls {
				t.Fatalf("ToolCalls = %+v, want length %d", turn.ToolCalls, fixture.WantToolCalls)
			}
		})
	}
}

func TestStopReasonZeroValueIsUnknown(t *testing.T) {
	var reason StopReason
	if reason != StopReasonUnknown || reason.String() != "unknown" {
		t.Fatalf("zero StopReason = %d/%q, want unknown", reason, reason.String())
	}
}

func TestChatMalformedChoicePreservesAccountingMetadata(t *testing.T) {
	const body = `{
		"model":"gpt-compatible",
		"choices":[{
			"finish_reason":"tool_calls",
			"message":{
				"content":null,
				"tool_calls":[{
					"id":"call_1",
					"type":"function",
					"function":{"name":"list_sources","arguments":{}}
				}]
			}
		}],
		"usage":{
			"prompt_tokens":31,
			"completion_tokens":7,
			"prompt_cache_hit_tokens":11,
			"prompt_cache_miss_tokens":20
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte(body)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	resp, err := newTestClient(srv.URL, 1).Chat(t.Context(), ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "list"}},
		Tools:    []ToolDef{{Name: "list_sources"}},
	})
	if !errors.Is(err, ErrToolProtocolResponse) {
		t.Fatalf("error = %v, want ErrToolProtocolResponse", err)
	}
	if code := types.CodeOf(err); code != types.CodeLLMUnavailable || types.IsRetryable(err) {
		t.Fatalf("error classification = %s retryable=%v", code, types.IsRetryable(err))
	}
	if resp == nil {
		t.Fatal("Chat must retain a metadata-only partial response")
	}
	if resp.Content != "" || len(resp.ToolCalls) != 0 {
		t.Fatalf("unsafe response fields escaped: %+v", resp)
	}
	if resp.Model != "gpt-compatible" || resp.PromptTokens != 31 ||
		resp.CompletionTokens != 7 || resp.CacheHitTokens != 11 ||
		resp.CacheMissTokens != 20 || resp.LatencyMs < 0 {
		t.Fatalf("accounting metadata was lost: %+v", resp)
	}
}
