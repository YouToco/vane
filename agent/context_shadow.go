package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/llm"
)

var errAgentContextShadow = errors.New("agent: context shadow unavailable")

type agentTurnContextSnapshotStore interface {
	SealAgentTurnContextSnapshot(
		context.Context,
		agentcontext.Scope,
		agentcontext.CandidateSnapshot,
	) (agentcontext.SealResult, error)
}

func (l *Loop) shadowAgentContext(
	ctx context.Context,
	request llm.ChatRequest,
	state *toolRunState,
	modelStep int,
) {
	meta, ok := ctx.Value(chatMetaKey{}).(chatMeta)
	if !ok || strings.TrimSpace(meta.traceID) == "" {
		return
	}
	candidate, err := l.buildShadowAgentContext(
		meta, request, state, modelStep,
	)
	if err != nil {
		l.logContextShadowFailure(meta, modelStep, "build")
		return
	}
	// RunOnce/A2A deliberately compiles the same in-memory candidate but has no
	// owner session scope and therefore cannot write the owner snapshot table.
	if meta.scope == (agentcontext.Scope{}) {
		return
	}
	sealer, ok := l.store.(agentTurnContextSnapshotStore)
	if !ok {
		return
	}
	if _, err := sealer.SealAgentTurnContextSnapshot(
		ctx, meta.scope, candidate,
	); err != nil {
		l.logContextShadowFailure(meta, modelStep, "seal")
	}
}

func (l *Loop) shadowFinalPendingContext(
	ctx context.Context,
	messages []llm.ChatMessage,
	state *toolRunState,
	modelStep int,
) {
	meta, ok := ctx.Value(chatMetaKey{}).(chatMeta)
	if !ok || meta.scope == (agentcontext.Scope{}) {
		return
	}
	request := llm.ChatRequest{
		Model: l.model,
		Messages: withSystem(
			l.sys, messages, "", false,
		),
		MaxTokens:       iptr(replyMaxTokens),
		DisableThinking: true,
	}
	candidate, err := l.buildShadowAgentContext(
		meta, request, state, modelStep,
	)
	if err != nil {
		l.logContextShadowFailure(meta, modelStep, "build_final")
		return
	}
	sealer, ok := l.store.(agentTurnContextSnapshotStore)
	if !ok {
		return
	}
	if _, err := sealer.SealAgentTurnContextSnapshot(
		ctx, meta.scope, candidate,
	); err != nil {
		l.logContextShadowFailure(meta, modelStep, "seal_final")
	}
}

func (l *Loop) buildShadowAgentContext(
	meta chatMeta,
	request llm.ChatRequest,
	state *toolRunState,
	modelStep int,
) (agentcontext.CandidateSnapshot, error) {
	system, groups := shadowMessageGroups(request.Messages, state)
	if len(groups) == 0 {
		return agentcontext.CandidateSnapshot{},
			agentContextShadowError()
	}
	history := groups[:len(groups)-1]
	current := groups[len(groups)-1]
	maxOutput := 0
	if request.MaxTokens != nil {
		maxOutput = *request.MaxTokens
	}
	tools := make([]agentcontext.Tool, 0, len(request.Tools))
	for _, definition := range request.Tools {
		spec, ok := l.resolveTool(definition.Name, state)
		if !ok {
			return agentcontext.CandidateSnapshot{},
				agentContextShadowError()
		}
		tools = append(tools, agentcontext.Tool{
			Definition: agentcontext.ToolDefinition{
				Name: definition.Name, Description: definition.Description,
				Parameters: append(json.RawMessage(nil), definition.Parameters...),
			},
			Policy: agentcontext.PolicySnapshot{
				Version:       agentcontext.PolicyVersion,
				Effects:       uint16(spec.Policy.Effects),
				Authorization: uint8(spec.Policy.Authorization),
				Confirmation:  uint8(spec.Policy.Confirmation),
				Budget:        uint8(spec.Policy.Budget),
				Retry:         uint8(spec.Policy.Retry),
				Concurrency:   uint8(spec.Policy.Concurrency),
			},
		})
	}
	result, err := agentcontext.Build(agentcontext.BuildInput{
		Scope: meta.scope, TurnID: meta.traceID, ModelStep: modelStep,
		Model: request.Model, SystemPrompt: system,
		Tools: tools, History: history, Current: current,
		ContextWindowTokens: llm.ContextWindowTokens(request.Model),
		MaxOutputTokens:     maxOutput,
	})
	if err != nil {
		return agentcontext.CandidateSnapshot{},
			agentContextShadowError()
	}
	return result.Candidate, nil
}

func shadowMessageGroups(
	messages []llm.ChatMessage,
	state *toolRunState,
) (string, []agentcontext.AtomicGroup) {
	if len(messages) == 0 || messages[0].Role != "system" {
		return "", nil
	}
	system := messages[0].Content
	var groups []agentcontext.AtomicGroup
	var current []agentcontext.Message
	var firstOrdinal int64 = 1
	var ordinal int64
	flush := func() {
		if len(current) == 0 {
			return
		}
		trust := agentcontext.TrustTrusted
		if shadowMessagesSanitized(current) {
			trust = agentcontext.TrustSanitizedPlaceholder
		}
		groups = append(groups, agentcontext.AtomicGroup{
			FirstMessageOrdinal: firstOrdinal,
			LastMessageOrdinal:  ordinal,
			Trust:               trust,
			Messages:            current,
		})
		current = nil
		firstOrdinal = ordinal + 1
	}
	for _, message := range messages[1:] {
		if message.Role == "user" && len(current) > 0 {
			flush()
		}
		ordinal++
		current = append(current, shadowMessage(message))
	}
	flush()
	if len(groups) == 0 {
		return "", nil
	}
	currentGroup := &groups[len(groups)-1]
	if state != nil && state.untrustedExternalResult {
		currentGroup.Trust = agentcontext.TrustUntrustedCurrent
	}
	return system, groups
}

func shadowMessage(message llm.ChatMessage) agentcontext.Message {
	var toolCalls []agentcontext.ToolCall
	if message.ToolCalls != nil {
		toolCalls = make([]agentcontext.ToolCall, len(message.ToolCalls))
		for i, call := range message.ToolCalls {
			toolCalls[i] = agentcontext.ToolCall{
				ID: call.ID, Name: call.Name, Arguments: call.Arguments,
			}
		}
	}
	return agentcontext.Message{
		Role: message.Role, Content: message.Content,
		ToolCalls: toolCalls, ToolCallID: message.ToolCallID,
	}
}

func shadowMessagesSanitized(messages []agentcontext.Message) bool {
	for _, message := range messages {
		if message.Content == untrustedHistoryPlaceholder ||
			message.Content == untrustedCallbackPlaceholder ||
			message.Content == untrustedFailurePlaceholder ||
			message.Content == untrustedInputHistoryUser ||
			message.Content == untrustedNoticePlaceholder {
			return true
		}
	}
	return false
}

func (l *Loop) logContextShadowFailure(
	meta chatMeta,
	modelStep int,
	phase string,
) {
	slog.Warn("agent: context shadow unavailable",
		"tenant_id", meta.scope.TenantID,
		"user_id", meta.userID,
		"session_id", meta.scope.SessionID,
		"model_step", modelStep,
		"phase", phase)
}

func agentContextShadowError() error {
	// Fixed error text: malformed request history can contain external source
	// text and must never be formatted into logs or snapshot errors.
	return errAgentContextShadow
}
