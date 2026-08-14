package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/YouToco/vane/server/agentcontext"
	"github.com/YouToco/vane/server/llm"
)

var errAgentContextShadow = errors.New("agent: context shadow unavailable")

const (
	agentContextShadowSealTimeout = 2 * time.Second
	agentContextShadowConcurrency = 4
)

type agentTurnContextSnapshotStore interface {
	SealAgentTurnContextSnapshot(
		context.Context,
		agentcontext.Scope,
		agentcontext.CandidateSnapshot,
	) (agentcontext.SealResult, error)
}

type preparedAgentContextShadow struct {
	meta        chatMeta
	candidate   agentcontext.CandidateSnapshot
	contextStep int
	phase       string
}

func (l *Loop) prepareAgentContextShadow(
	ctx context.Context,
	request llm.ChatRequest,
	state *toolRunState,
	contextStep int,
) *preparedAgentContextShadow {
	meta, ok := ctx.Value(chatMetaKey{}).(chatMeta)
	if !ok || strings.TrimSpace(meta.traceID) == "" {
		return nil
	}
	candidate, err := l.buildShadowAgentContext(
		meta, request, state, contextStep,
	)
	if err != nil {
		l.logContextShadowFailure(meta, contextStep, "build")
		return nil
	}
	// RunOnce/A2A deliberately compiles the same in-memory candidate but has no
	// owner session scope and therefore cannot write the owner snapshot table.
	if meta.scope == (agentcontext.Scope{}) {
		return nil
	}
	return &preparedAgentContextShadow{
		meta: meta, candidate: candidate, contextStep: contextStep, phase: "seal",
	}
}

// sealPreparedAgentContextShadow is deliberately post-chat, bounded and
// best-effort. A slow Store must never consume the legacy chat context,
// delay the next model/tool step or change its Outcome. A small process-local
// slot set admits adjacent steps while bounding goroutines and Store/root-lock
// pressure; DrainSessionWrites owns
// the accepted goroutine's shutdown lifecycle.
func (l *Loop) sealPreparedAgentContextShadow(
	ctx context.Context,
	prepared *preparedAgentContextShadow,
) {
	if l == nil || prepared == nil {
		return
	}
	sealer, ok := l.store.(agentTurnContextSnapshotStore)
	if !ok {
		return
	}
	select {
	case l.contextShadowSlots <- struct{}{}:
	default:
		l.logContextShadowFailure(
			prepared.meta, prepared.contextStep, "seal_busy",
		)
		return
	}

	l.sessionWriteMu.Lock()
	if !l.sessionWriteAccepting {
		l.sessionWriteMu.Unlock()
		<-l.contextShadowSlots
		return
	}
	l.sessionWriteWG.Add(1)
	l.sessionWriteMu.Unlock()

	go func() {
		defer l.sessionWriteWG.Done()
		defer func() { <-l.contextShadowSlots }()
		defer func() {
			if recover() != nil {
				l.logContextShadowFailure(
					prepared.meta, prepared.contextStep, "seal_panic",
				)
			}
		}()
		sealCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), agentContextShadowSealTimeout,
		)
		defer cancel()
		if _, err := sealer.SealAgentTurnContextSnapshot(
			sealCtx, prepared.meta.scope, prepared.candidate,
		); err != nil {
			l.logContextShadowFailure(
				prepared.meta, prepared.contextStep, prepared.phase,
			)
		}
	}()
}

func (l *Loop) shadowFinalPendingContext(
	ctx context.Context,
	messages []llm.ChatMessage,
	state *toolRunState,
	contextStep int,
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
		EnableThinking:  true,
		ReasoningEffort: llm.ReasoningEffortHigh,
	}
	candidate, err := l.buildShadowAgentContext(
		meta, request, state, contextStep,
	)
	if err != nil {
		l.logContextShadowFailure(meta, contextStep, "build_final")
		return
	}
	l.sealPreparedAgentContextShadow(ctx, &preparedAgentContextShadow{
		meta: meta, candidate: candidate, contextStep: contextStep,
		phase: "seal_final",
	})
}

func (l *Loop) buildShadowAgentContext(
	meta chatMeta,
	request llm.ChatRequest,
	state *toolRunState,
	contextStep int,
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
				Version:        agentcontext.PolicyVersion,
				Effects:        uint16(spec.Policy.Effects),
				Authorization:  uint8(spec.Policy.Authorization),
				Confirmation:   0, // retained only in immutable v1 shadow bytes
				Budget:         uint8(spec.Policy.Budget),
				Retry:          uint8(spec.Policy.Retry),
				Concurrency:    uint8(spec.Policy.Concurrency),
				Exposure:       uint8(spec.Policy.Exposure),
				Intents:        uint16(spec.Policy.Intents),
				ResultTrust:    uint8(spec.Policy.ResultTrust),
				DirectExplicit: spec.Policy.DirectOnExplicitIntent,
			},
		})
	}
	result, err := agentcontext.Build(agentcontext.BuildInput{
		Scope: meta.scope, TurnID: meta.traceID, ContextStep: contextStep,
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
	contextStep int,
	phase string,
) {
	slog.Warn("agent: context shadow unavailable",
		"tenant_id", meta.scope.TenantID,
		"user_id", meta.userID,
		"session_id", meta.scope.SessionID,
		"context_step", contextStep,
		"phase", phase)
}

func agentContextShadowError() error {
	// Fixed error text: malformed request history can contain external source
	// text and must never be formatted into logs or snapshot errors.
	return errAgentContextShadow
}
