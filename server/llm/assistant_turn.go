package llm

import (
	"encoding/json"
	"strings"
)

// StopReason is the provider-neutral reason an assistant turn ended.
// Its zero value is deliberately Unknown so missing or new wire values never
// gain executable semantics.
type StopReason uint8

const (
	StopReasonUnknown StopReason = iota
	StopReasonStop
	StopReasonToolCalls
	StopReasonLength
	StopReasonContentFilter
)

func (r StopReason) String() string {
	switch r {
	case StopReasonStop:
		return "stop"
	case StopReasonToolCalls:
		return "tool_calls"
	case StopReasonLength:
		return "length"
	case StopReasonContentFilter:
		return "content_filter"
	default:
		return "unknown"
	}
}

// AssistantTurn is the provider-neutral, validated result of one assistant
// choice. ToolCalls are executable only after the wire adapter accepts the
// complete turn.
type AssistantTurn struct {
	Content    string
	ToolCalls  []ToolCall
	StopReason StopReason
}

// wireString intentionally never fails JSON unmarshalling. It records whether
// the wire value was a string so malformed choice fields reach the adapter,
// where they become ErrToolProtocolResponse without discarding usage metadata.
type wireString struct {
	Value string
	Valid bool
}

func (s *wireString) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		*s = wireString{}
		return nil
	}
	*s = wireString{Value: value, Valid: true}
	return nil
}

type wireNullableString struct {
	Value string
	Valid bool
}

func (s *wireNullableString) UnmarshalJSON(raw []byte) error {
	if string(raw) == "null" {
		*s = wireNullableString{Valid: true}
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		*s = wireNullableString{}
		return nil
	}
	*s = wireNullableString{Value: value, Valid: true}
	return nil
}

type wireAssistantMessage struct {
	Content   wireNullableString      `json:"content"`
	ToolCalls []wireAssistantToolCall `json:"tool_calls"`
}

func (m *wireAssistantMessage) UnmarshalJSON(raw []byte) error {
	type message wireAssistantMessage
	decoded := message{
		// OpenAI-compatible tool responses may use null or omit content.
		Content: wireNullableString{Valid: true},
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*m = wireAssistantMessage(decoded)
	return nil
}

type wireAssistantToolCall struct {
	ID       wireString                `json:"id"`
	Type     wireString                `json:"type"`
	Function wireAssistantFunctionCall `json:"function"`
}

type wireAssistantFunctionCall struct {
	Name      wireString `json:"name"`
	Arguments wireString `json:"arguments"`
}

type wireChatChoice struct {
	FinishReason wireString           `json:"finish_reason"`
	Message      wireAssistantMessage `json:"message"`
}

type assistantTurnOptions struct {
	Provider      string
	RequestModel  string
	ResponseModel string
	ToolsDeclared bool
}

// adaptAssistantTurn is the single pure wire-to-domain adapter for chat
// choices. Any contradictory executable shape returns a zero turn and a
// classified, non-retryable protocol error.
func adaptAssistantTurn(choice wireChatChoice, opts assistantTurnOptions) (AssistantTurn, error) {
	if !choice.FinishReason.Valid || !choice.Message.Content.Valid {
		return AssistantTurn{}, newToolProtocolResponseError()
	}
	if isDeepSeekV4DSML(
		opts.Provider,
		choice.Message.Content.Value,
		opts.RequestModel,
		opts.ResponseModel,
	) {
		return AssistantTurn{}, newToolProtocolResponseError()
	}

	reason := normalizeStopReason(choice.FinishReason.Value)
	if !opts.ToolsDeclared {
		// A tool-free finalizer has no executable tool surface. Preserve its
		// content and normalized stop reason, but discard any provider drift.
		return AssistantTurn{
			Content:    choice.Message.Content.Value,
			StopReason: reason,
		}, nil
	}

	wireCalls := choice.Message.ToolCalls
	if (len(wireCalls) > 0) != (reason == StopReasonToolCalls) {
		return AssistantTurn{}, newToolProtocolResponseError()
	}

	calls := make([]ToolCall, 0, len(wireCalls))
	ids := make(map[string]struct{}, len(wireCalls))
	for _, call := range wireCalls {
		if !call.Type.Valid || call.Type.Value != "function" ||
			!call.ID.Valid || strings.TrimSpace(call.ID.Value) == "" ||
			!call.Function.Name.Valid || strings.TrimSpace(call.Function.Name.Value) == "" ||
			!call.Function.Arguments.Valid ||
			!isJSONObject(call.Function.Arguments.Value) {
			return AssistantTurn{}, newToolProtocolResponseError()
		}
		if _, exists := ids[call.ID.Value]; exists {
			return AssistantTurn{}, newToolProtocolResponseError()
		}
		ids[call.ID.Value] = struct{}{}
		calls = append(calls, ToolCall{
			ID:        call.ID.Value,
			Name:      call.Function.Name.Value,
			Arguments: call.Function.Arguments.Value,
		})
	}

	return AssistantTurn{
		Content:    choice.Message.Content.Value,
		ToolCalls:  calls,
		StopReason: reason,
	}, nil
}

func normalizeStopReason(reason string) StopReason {
	switch reason {
	case "stop":
		return StopReasonStop
	case "tool_calls":
		return StopReasonToolCalls
	case "length":
		return StopReasonLength
	case "content_filter":
		return StopReasonContentFilter
	default:
		return StopReasonUnknown
	}
}

func isJSONObject(raw string) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal([]byte(raw), &object) == nil && object != nil
}
