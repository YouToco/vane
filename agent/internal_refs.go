package agent

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/store"
)

var internalReferenceFields = map[string]struct{}{
	"record_id": {}, "task_ref": {}, "run_snapshot_id": {}, "delivery_ref": {},
	"task_id": {}, "task_refs": {}, "completed_task_refs": {}, "failed_task_refs": {},
	"schedule_id": {}, "schedule_ids": {}, "tool_invocation_ids": {},
	"temporal_workflow_id": {}, "temporal_run_id": {},
	"turn_id": {}, "trace_id": {}, "invocation_id": {},
	"operation_id": {}, "tool_call_id": {}, "session_id": {},
	"invocation_digest": {}, "observation_digest": {},
}

func rememberIntelligenceResultReferences(ctx context.Context, result *store.IntelligenceQueryResult) {
	state := runStateFrom(ctx)
	if state == nil || result == nil {
		return
	}
	for _, row := range result.Rows {
		walkInternalReferenceValue(state, "", row)
	}
}

func walkInternalReferenceValue(state *toolRunState, field string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			walkInternalReferenceValue(state, key, child)
		}
	case []any:
		for _, child := range typed {
			walkInternalReferenceValue(state, field, child)
		}
	case string:
		if _, sensitive := internalReferenceFields[field]; sensitive {
			rememberInternalReference(state, typed)
			return
		}
		if field == "arguments" || field == "action_receipts" {
			var decoded any
			if json.Unmarshal([]byte(typed), &decoded) == nil {
				walkInternalReferenceValue(state, field, decoded)
			}
		}
	case json.RawMessage:
		var decoded any
		if json.Unmarshal(typed, &decoded) == nil {
			walkInternalReferenceValue(state, field, decoded)
		}
	case []byte:
		var decoded any
		if json.Unmarshal(typed, &decoded) == nil {
			walkInternalReferenceValue(state, field, decoded)
		}
	}
}

func rememberInternalReference(state *toolRunState, value string) {
	value = strings.TrimSpace(value)
	if state == nil || len(value) < 4 || len(value) > 512 {
		return
	}
	if state.internalReferences == nil {
		state.internalReferences = make(map[string]struct{})
	}
	state.internalReferences[value] = struct{}{}
}

func appendAgentActionReceipt(state *toolRunState, receipt json.RawMessage) {
	if state == nil || len(receipt) == 0 {
		return
	}
	var decoded any
	if json.Unmarshal(receipt, &decoded) == nil {
		walkInternalReferenceValue(state, "action_receipts", decoded)
	}
	state.actionReceipts = append(state.actionReceipts,
		append(json.RawMessage(nil), receipt...))
}

func redactInternalReferences(reply string, state *toolRunState) string {
	if state == nil || len(state.internalReferences) == 0 || reply == "" {
		return reply
	}
	references := make([]string, 0, len(state.internalReferences))
	for reference := range state.internalReferences {
		if strings.Contains(reply, reference) {
			references = append(references, reference)
		}
	}
	sort.Slice(references, func(i, j int) bool { return len(references[i]) > len(references[j]) })
	for _, reference := range references {
		reply = strings.ReplaceAll(reply, reference, "[内部引用已隐藏]")
	}
	return reply
}

func replaceLastAssistantReply(messages []llm.ChatMessage, old, replacement string) {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == "assistant" && messages[index].Content == old {
			messages[index].Content = replacement
			return
		}
	}
}
