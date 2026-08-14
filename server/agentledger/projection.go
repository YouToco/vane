package agentledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/YouToco/vane/internal/strictjson"
)

const (
	// ProjectionSchemaVersion freezes the shadow projection payload. A later
	// session representation must introduce a new reader instead of changing
	// how historical snapshot generations are interpreted.
	ProjectionSchemaVersion = "vane.agent-session-projection/v1"
	ProjectionFullSnapshot  = "full_snapshot"

	maxProjectionMessages        = 60
	maxProjectionTurnID          = 128
	keepRecentProjectionMessages = 40
)

var ErrInvalidProjection = errors.New("agent ledger: invalid session projection")

// SessionProjection is the legacy agent_sessions projection represented by
// the latest complete semantic snapshot batch. Raw JSON is kept at the
// boundary so agentledger does not depend on the model/provider package.
type SessionProjection struct {
	Messages       json.RawMessage
	TurnCount      int
	ActivatedTools json.RawMessage
}

// ProjectionShadowAudit is bounded telemetry from the transactional dual
// write. PriorState attributes drift caused before this normal turn; Match
// reports the post-commit shadow comparison.
type ProjectionShadowAudit struct {
	Match              bool
	PriorState         string
	Reason             string
	LegacyDigest       string
	EventDigest        string
	LegacyMessageCount int
	EventMessageCount  int
}

// ProjectionSnapshotInput is one complete normal Agent turn after all
// trust-boundary scrubbing and message truncation have run.
type ProjectionSnapshotInput struct {
	Scope                Scope
	TurnID               string
	BaseProjectionDigest string
	Messages             json.RawMessage
	TurnCount            int
	ActivatedTools       json.RawMessage
}

type projectionToolCallV1 struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type projectionMessageV1 struct {
	Role       string                 `json:"role"`
	Content    string                 `json:"content"`
	ToolCalls  []projectionToolCallV1 `json:"tool_calls,omitempty"`
	ToolCallID string                 `json:"tool_call_id,omitempty"`
}

type projectionStartedV1 struct {
	SchemaVersion        string `json:"schema_version"`
	Generation           string `json:"generation"`
	TurnID               string `json:"turn_id"`
	BaseProjectionDigest string `json:"base_projection_digest"`
}

type projectionMessageBodyV1 struct {
	SchemaVersion string              `json:"schema_version"`
	TurnID        string              `json:"turn_id"`
	Message       projectionMessageV1 `json:"message"`
}

type projectionConfirmationV1 struct {
	SchemaVersion string `json:"schema_version"`
	TurnID        string `json:"turn_id"`
	ActionID      string `json:"action_id"`
}

type projectionCompletedV1 struct {
	SchemaVersion  string   `json:"schema_version"`
	Generation     string   `json:"generation"`
	TurnID         string   `json:"turn_id"`
	TurnCount      int      `json:"turn_count"`
	ActivatedTools []string `json:"activated_tools"`
	Outcome        string   `json:"outcome"`
}

// BuildProjectionSnapshotBatch emits one self-contained full snapshot
// generation. Projectors deliberately select only the latest complete batch;
// they never append full-history generations together.
func BuildProjectionSnapshotBatch(
	input ProjectionSnapshotInput,
) (AppendBatch, error) {
	if input.Scope.TenantID <= 0 || input.Scope.UserID <= 0 ||
		input.Scope.SessionID <= 0 {
		return AppendBatch{}, invalidProjection("scope is invalid")
	}
	if !validProjectionTurnID(input.TurnID) {
		return AppendBatch{}, invalidProjection("turn id is invalid")
	}
	if !validDigest(input.BaseProjectionDigest) {
		return AppendBatch{}, invalidProjection("base projection digest is invalid")
	}
	messages, err := decodeProjectionMessages(input.Messages)
	if err != nil {
		return AppendBatch{}, err
	}
	activatedTools, err := decodeActivatedTools(input.ActivatedTools)
	if err != nil {
		return AppendBatch{}, err
	}
	if input.TurnCount < 0 {
		return AppendBatch{}, invalidProjection("turn count is invalid")
	}

	eventCount := len(messages) + 2
	// This mirrors migration 035's immutable batch bound. Refuse the complete
	// write instead of silently dropping semantic events and reporting match.
	if eventCount > 64 {
		return AppendBatch{}, invalidProjection("snapshot event count exceeds the batch limit")
	}

	events := make([]Input, 0, eventCount)
	started, err := projectionBody(projectionStartedV1{
		SchemaVersion:        ProjectionSchemaVersion,
		Generation:           ProjectionFullSnapshot,
		TurnID:               input.TurnID,
		BaseProjectionDigest: input.BaseProjectionDigest,
	})
	if err != nil {
		return AppendBatch{}, err
	}
	events = append(events, Input{Kind: KindTurnStarted, Body: started})

	for i := range messages {
		kind, err := projectionMessageKind(messages[i])
		if err != nil {
			return AppendBatch{}, err
		}
		body, err := projectionBody(projectionMessageBodyV1{
			SchemaVersion: ProjectionSchemaVersion,
			TurnID:        input.TurnID,
			Message:       messages[i],
		})
		if err != nil {
			return AppendBatch{}, err
		}
		events = append(events, Input{Kind: kind, Body: body})
	}

	completed, err := projectionBody(projectionCompletedV1{
		SchemaVersion:  ProjectionSchemaVersion,
		Generation:     ProjectionFullSnapshot,
		TurnID:         input.TurnID,
		TurnCount:      input.TurnCount,
		ActivatedTools: activatedTools,
		Outcome:        "reply",
	})
	if err != nil {
		return AppendBatch{}, err
	}
	events = append(events, Input{Kind: KindTurnCompleted, Body: completed})

	return AppendBatch{
		Scope:          input.Scope,
		IdempotencyKey: "turn." + input.TurnID,
		Events:         events,
	}, nil
}

// ProjectionSnapshotBaseDigest extracts the expected legacy projection digest
// bound into a canonical full-snapshot batch.
func ProjectionSnapshotBaseDigest(event CanonicalEvent) (string, error) {
	if event.Kind() != KindTurnStarted {
		return "", invalidProjection("snapshot does not start with turn_started")
	}
	var started *projectionStartedV1
	if err := strictjson.Decode(event.Body(), &started); err != nil ||
		started == nil || started.SchemaVersion != ProjectionSchemaVersion ||
		started.Generation != ProjectionFullSnapshot ||
		!validProjectionTurnID(started.TurnID) ||
		!validDigest(started.BaseProjectionDigest) {
		return "", invalidProjection("snapshot base digest is invalid")
	}
	return started.BaseProjectionDigest, nil
}

// ProjectionSnapshotTurnID extracts the request-bound turn identity from a
// canonical full-snapshot generation. Side-writer exact replay uses this
// identity to prove that the same durable operation is retrying the same
// appended message body without logging or separately persisting that body.
func ProjectionSnapshotTurnID(event CanonicalEvent) (string, error) {
	if event.Kind() != KindTurnStarted {
		return "", invalidProjection("snapshot does not start with turn_started")
	}
	started, err := decodeCanonicalProjectionStarted(event)
	if err != nil {
		return "", err
	}
	return started.TurnID, nil
}

// AppendProjectionMessages applies the retained session-history truncation
// contract to one side-writer append. It preserves the first user intent and
// advances the recent-history boundary to a user message so a tool result is
// never retained without its initiating user turn.
func AppendProjectionMessages(
	current json.RawMessage,
	appended json.RawMessage,
) (json.RawMessage, error) {
	base, err := decodeProjectionMessages(current)
	if err != nil {
		return nil, err
	}
	additions, err := decodeProjectionMessages(appended)
	if err != nil {
		return nil, err
	}
	if len(additions) == 0 {
		return nil, invalidProjection("appended messages must not be empty")
	}
	messages := append(base, additions...)
	if len(messages) > maxProjectionMessages {
		cut := len(messages) - keepRecentProjectionMessages
		for cut < len(messages) && messages[cut].Role != "user" {
			cut++
		}
		if cut >= len(messages) {
			cut = len(messages)
		}
		truncated := make([]projectionMessageV1, 0, len(messages)-cut+1)
		for i := 0; i < cut; i++ {
			if messages[i].Role == "user" {
				truncated = append(truncated, messages[i])
				break
			}
		}
		messages = append(truncated, messages[cut:]...)
	}
	raw, err := json.Marshal(messages)
	if err != nil {
		return nil, invalidProjection("appended messages cannot be encoded")
	}
	return raw, nil
}

// ProjectLatestSessionSnapshot rebuilds the latest complete full-snapshot
// batch. Earlier generations are integrity history, not deltas to concatenate.
func ProjectLatestSessionSnapshot(events []Event) (SessionProjection, error) {
	var latest []Event
	for index := 0; index < len(events); {
		event := events[index]
		if event.BatchIndex != 0 || event.BatchSize <= 0 ||
			index+event.BatchSize > len(events) {
			return SessionProjection{}, invalidProjection("event batches are incomplete")
		}
		batch := events[index : index+event.BatchSize]
		for batchIndex := range batch {
			if batch[batchIndex].BatchIndex != batchIndex ||
				batch[batchIndex].BatchSize != len(batch) ||
				batch[batchIndex].IdempotencyKey != event.IdempotencyKey {
				return SessionProjection{}, invalidProjection("event batch metadata is inconsistent")
			}
		}
		if isProjectionSnapshotBatch(batch) {
			latest = batch
		}
		index += event.BatchSize
	}
	if len(latest) == 0 {
		return SessionProjection{}, invalidProjection("complete snapshot generation is unavailable")
	}
	return projectSnapshotBatch(latest)
}

// ProjectionDigest returns a content digest safe for mismatch telemetry.
// Callers log only this digest and bounded counts, never message bodies.
func ProjectionDigest(projection SessionProjection) (string, error) {
	messages, err := decodeProjectionMessages(projection.Messages)
	if err != nil {
		return "", err
	}
	activatedTools, err := decodeActivatedTools(projection.ActivatedTools)
	if err != nil {
		return "", err
	}
	if projection.TurnCount < 0 {
		return "", invalidProjection("turn count is invalid")
	}
	wire, err := json.Marshal(struct {
		SchemaVersion  string                `json:"schema_version"`
		Messages       []projectionMessageV1 `json:"messages"`
		TurnCount      int                   `json:"turn_count"`
		ActivatedTools []string              `json:"activated_tools"`
	}{
		SchemaVersion:  ProjectionSchemaVersion,
		Messages:       messages,
		TurnCount:      projection.TurnCount,
		ActivatedTools: activatedTools,
	})
	if err != nil {
		return "", invalidProjection("projection digest cannot be encoded")
	}
	sum := sha256.Sum256(wire)
	return hex.EncodeToString(sum[:]), nil
}

func isProjectionSnapshotBatch(batch []Event) bool {
	if len(batch) < 2 || batch[0].Kind != KindTurnStarted ||
		batch[len(batch)-1].Kind != KindTurnCompleted {
		return false
	}
	canonical, err := Decode(batch[0].Payload, batch[0].PayloadDigest)
	if err != nil {
		return false
	}
	var started *projectionStartedV1
	if err := strictjson.Decode(canonical.Body(), &started); err != nil ||
		started == nil {
		return false
	}
	return started.SchemaVersion == ProjectionSchemaVersion &&
		started.Generation == ProjectionFullSnapshot &&
		validDigest(started.BaseProjectionDigest)
}

// ProjectCanonicalSessionSnapshot validates and projects exactly one
// self-contained full-snapshot generation. Unlike ProjectLatestSessionSnapshot,
// it never falls back to an earlier valid batch; dual-write callers use it to
// prove the newly supplied batch itself represents the desired projection.
func ProjectCanonicalSessionSnapshot(
	batch []CanonicalEvent,
) (SessionProjection, error) {
	if len(batch) < 2 || batch[0].Kind() != KindTurnStarted ||
		batch[len(batch)-1].Kind() != KindTurnCompleted {
		return SessionProjection{},
			invalidProjection("snapshot lifecycle is incomplete")
	}
	started, err := decodeCanonicalProjectionStarted(batch[0])
	if err != nil {
		return SessionProjection{}, err
	}
	messages := make([]projectionMessageV1, 0, len(batch)-2)
	confirmationSeen := false
	for i := 1; i < len(batch)-1; i++ {
		event := batch[i]
		switch event.Kind() {
		case KindUserMessage, KindAssistantMessage, KindToolCall, KindToolResult:
			if confirmationSeen {
				return SessionProjection{}, invalidProjection("message follows confirmation")
			}
			message, turnID, err := decodeCanonicalProjectionMessage(event)
			if err != nil || turnID != started.TurnID {
				return SessionProjection{}, invalidProjection("snapshot message is invalid")
			}
			expectedKind, err := projectionMessageKind(message)
			if err != nil || expectedKind != event.Kind() {
				return SessionProjection{}, invalidProjection("snapshot message kind does not match")
			}
			messages = append(messages, message)
		case KindConfirmationRequested:
			if confirmationSeen {
				return SessionProjection{}, invalidProjection("snapshot has duplicate confirmation")
			}
			confirmation, err := decodeCanonicalProjectionConfirmation(event)
			if err != nil || confirmation.TurnID != started.TurnID {
				return SessionProjection{}, invalidProjection("snapshot confirmation is invalid")
			}
			confirmationSeen = true
		default:
			return SessionProjection{}, invalidProjection("snapshot contains an unsupported event")
		}
	}
	completed, err := decodeCanonicalProjectionCompleted(batch[len(batch)-1])
	if err != nil || completed.TurnID != started.TurnID ||
		completed.SchemaVersion != ProjectionSchemaVersion ||
		completed.Generation != ProjectionFullSnapshot {
		return SessionProjection{}, invalidProjection("snapshot completion is invalid")
	}
	expectedOutcome := "reply"
	if confirmationSeen {
		expectedOutcome = "confirmation_requested"
	}
	if completed.Outcome != expectedOutcome || completed.TurnCount < 0 {
		return SessionProjection{}, invalidProjection("snapshot outcome is inconsistent")
	}
	if len(messages) > maxProjectionMessages {
		return SessionProjection{}, invalidProjection("snapshot has too many messages")
	}
	messagesRaw, err := json.Marshal(messages)
	if err != nil {
		return SessionProjection{}, invalidProjection("snapshot messages cannot be encoded")
	}
	activatedRaw, err := json.Marshal(completed.ActivatedTools)
	if err != nil {
		return SessionProjection{}, invalidProjection("snapshot activated tools cannot be encoded")
	}
	return SessionProjection{
		Messages:       messagesRaw,
		TurnCount:      completed.TurnCount,
		ActivatedTools: activatedRaw,
	}, nil
}

func projectSnapshotBatch(batch []Event) (SessionProjection, error) {
	canonical := make([]CanonicalEvent, len(batch))
	for i := range batch {
		event, err := Decode(batch[i].Payload, batch[i].PayloadDigest)
		if err != nil || event.Kind() != batch[i].Kind {
			return SessionProjection{},
				invalidProjection("event payload integrity failed")
		}
		canonical[i] = event
	}
	return ProjectCanonicalSessionSnapshot(canonical)
}

func decodeCanonicalProjectionStarted(
	event CanonicalEvent,
) (projectionStartedV1, error) {
	var body *projectionStartedV1
	if err := decodeCanonicalProjectionBody(
		event, KindTurnStarted, &body,
	); err != nil ||
		body == nil || body.SchemaVersion != ProjectionSchemaVersion ||
		body.Generation != ProjectionFullSnapshot ||
		!validProjectionTurnID(body.TurnID) ||
		!validDigest(body.BaseProjectionDigest) {
		return projectionStartedV1{}, invalidProjection("snapshot start is invalid")
	}
	return *body, nil
}

func decodeCanonicalProjectionMessage(
	event CanonicalEvent,
) (projectionMessageV1, string, error) {
	var body *projectionMessageBodyV1
	if err := decodeCanonicalProjectionBody(
		event, event.Kind(), &body,
	); err != nil ||
		body == nil || body.SchemaVersion != ProjectionSchemaVersion ||
		!validProjectionTurnID(body.TurnID) {
		return projectionMessageV1{}, "", invalidProjection("snapshot message body is invalid")
	}
	return body.Message, body.TurnID, nil
}

func decodeCanonicalProjectionConfirmation(
	event CanonicalEvent,
) (projectionConfirmationV1, error) {
	var body *projectionConfirmationV1
	if err := decodeCanonicalProjectionBody(
		event, KindConfirmationRequested, &body,
	); err != nil || body == nil ||
		body.SchemaVersion != ProjectionSchemaVersion ||
		!validProjectionTurnID(body.TurnID) ||
		!validProjectionActionID(body.ActionID) {
		return projectionConfirmationV1{}, invalidProjection("snapshot confirmation body is invalid")
	}
	return *body, nil
}

func decodeCanonicalProjectionCompleted(
	event CanonicalEvent,
) (projectionCompletedV1, error) {
	var body *projectionCompletedV1
	if err := decodeCanonicalProjectionBody(
		event, KindTurnCompleted, &body,
	); err != nil ||
		body == nil {
		return projectionCompletedV1{}, invalidProjection("snapshot completion body is invalid")
	}
	activated, err := decodeActivatedToolsFromSlice(body.ActivatedTools)
	if err != nil {
		return projectionCompletedV1{}, err
	}
	body.ActivatedTools = activated
	return *body, nil
}

func decodeCanonicalProjectionBody(
	event CanonicalEvent,
	kind Kind,
	target any,
) error {
	if event.Kind() != kind {
		return invalidProjection("event kind is inconsistent")
	}
	if err := strictjson.Decode(event.Body(), target); err != nil {
		return invalidProjection("event body schema is invalid")
	}
	return nil
}

func decodeProjectionMessages(raw json.RawMessage) ([]projectionMessageV1, error) {
	if len(raw) == 0 {
		raw = json.RawMessage("[]")
	}
	var messages *[]projectionMessageV1
	if err := strictjson.Decode(raw, &messages); err != nil || messages == nil {
		return nil, invalidProjection("messages must be a strict json array")
	}
	if len(*messages) > maxProjectionMessages {
		return nil, invalidProjection("messages exceed the projection limit")
	}
	for i := range *messages {
		if _, err := projectionMessageKind((*messages)[i]); err != nil {
			return nil, err
		}
	}
	return slices.Clone(*messages), nil
}

func projectionMessageKind(message projectionMessageV1) (Kind, error) {
	switch message.Role {
	case "user":
		if len(message.ToolCalls) != 0 || message.ToolCallID != "" {
			return "", invalidProjection("user message has tool protocol fields")
		}
		return KindUserMessage, nil
	case "assistant":
		if message.ToolCallID != "" {
			return "", invalidProjection("assistant message has a tool result id")
		}
		if len(message.ToolCalls) == 0 {
			return KindAssistantMessage, nil
		}
		seen := make(map[string]struct{}, len(message.ToolCalls))
		for i := range message.ToolCalls {
			call := message.ToolCalls[i]
			if call.ID == "" || call.Name == "" || !json.Valid([]byte(call.Arguments)) {
				return "", invalidProjection("assistant tool call is invalid")
			}
			var arguments map[string]json.RawMessage
			if err := strictjson.Decode(
				[]byte(call.Arguments), &arguments,
			); err != nil || arguments == nil {
				return "", invalidProjection("assistant tool arguments are invalid")
			}
			if _, duplicate := seen[call.ID]; duplicate {
				return "", invalidProjection("assistant tool call id is duplicated")
			}
			seen[call.ID] = struct{}{}
		}
		return KindToolCall, nil
	case "tool":
		if message.ToolCallID == "" || len(message.ToolCalls) != 0 {
			return "", invalidProjection("tool result message is invalid")
		}
		return KindToolResult, nil
	default:
		return "", invalidProjection("message role is unsupported")
	}
}

func decodeActivatedTools(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		raw = json.RawMessage("[]")
	}
	var values *[]string
	if err := strictjson.Decode(raw, &values); err != nil || values == nil {
		return nil, invalidProjection("activated tools must be a strict json array")
	}
	return decodeActivatedToolsFromSlice(*values)
}

func decodeActivatedToolsFromSlice(values []string) ([]string, error) {
	// Keep the durable projection decoder aligned with the interactive Agent's
	// per-session dynamic-tool authority. A forged event must not preserve more
	// tools than the runtime can ever activate.
	if len(values) > 16 {
		return nil, invalidProjection("activated tools exceed the session limit")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || len(value) > 255 || strings.TrimSpace(value) != value {
			return nil, invalidProjection("activated tool name is invalid")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, invalidProjection("activated tool name is duplicated")
		}
		seen[value] = struct{}{}
	}
	return slices.Clone(values), nil
}

func projectionBody(value any) (json.RawMessage, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, invalidProjection("event body cannot be encoded")
	}
	return body, nil
}

func validProjectionTurnID(value string) bool {
	if value == "" || len(value) > maxProjectionTurnID {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'A' && character <= 'Z') ||
			(character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			character == '-' {
			continue
		}
		return false
	}
	return true
}

func validProjectionActionID(value string) bool {
	return validProjectionTurnID(value)
}

func invalidProjection(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidProjection, message)
}
