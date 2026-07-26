// Package agentcontext builds provider-neutral, deterministic Agent context
// candidates. It deliberately has no provider SDK or application dependency.
package agentcontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	CompilerVersion         = "vane.agent-context/v1"
	SnapshotSchemaVersion   = "vane.agent-turn-context-snapshot/v1"
	CompactorVersion        = "none/v1"
	PolicyVersion           = "vane.agent-context-policy/v1"
	untrustedPlaceholder    = "[untrusted current-turn content omitted]"
	defaultContextWindow    = 8192
	messageFramingTokens    = 8
	toolDefinitionOverhead  = 16
	contextEnvelopeOverhead = 24
)

type TrustLabel string

const (
	TrustTrusted              TrustLabel = "trusted"
	TrustUntrustedCurrent     TrustLabel = "untrusted_current"
	TrustSanitizedPlaceholder TrustLabel = "sanitized_placeholder"
)

func (t TrustLabel) valid() bool {
	return t == TrustTrusted || t == TrustUntrustedCurrent ||
		t == TrustSanitizedPlaceholder
}

type Scope struct {
	TenantID  int64 `json:"tenant_id"`
	UserID    int64 `json:"user_id"`
	SessionID int64 `json:"session_id"`
}

type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type AtomicGroup struct {
	FirstMessageOrdinal int64      `json:"first_message_ordinal"`
	LastMessageOrdinal  int64      `json:"last_message_ordinal"`
	Trust               TrustLabel `json:"trust"`
	Messages            []Message  `json:"messages"`
}

type MaterialRef struct {
	Kind      string    `json:"kind"`
	Version   string    `json:"version"`
	Digest    string    `json:"digest"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type PolicySnapshot struct {
	Version       string `json:"version"`
	Effects       uint16 `json:"effects"`
	Authorization uint8  `json:"authorization"`
	Confirmation  uint8  `json:"confirmation"`
	Budget        uint8  `json:"budget"`
	Retry         uint8  `json:"retry"`
	Concurrency   uint8  `json:"concurrency"`
}

type Tool struct {
	Definition ToolDefinition `json:"definition"`
	Policy     PolicySnapshot `json:"policy"`
}

type BuildInput struct {
	Scope               Scope
	TurnID              string
	ContextStep         int
	Model               string
	SystemPrompt        string
	Profile             []MaterialRef
	Playbooks           []MaterialRef
	Tools               []Tool
	History             []AtomicGroup
	Current             AtomicGroup
	ContextWindowTokens int
	MaxOutputTokens     int
}

type Range struct {
	FirstMessageOrdinal int64  `json:"first_message_ordinal"`
	LastMessageOrdinal  int64  `json:"last_message_ordinal"`
	SourceMessageCount  int    `json:"source_message_count"`
	DurableMessageCount int    `json:"durable_message_count"`
	Reason              string `json:"reason"`
}

type CandidateSnapshot struct {
	SchemaVersion        string        `json:"schema_version"`
	CompilerVersion      string        `json:"compiler_version"`
	Scope                Scope         `json:"scope"`
	TurnID               string        `json:"turn_id"`
	ContextStep          int           `json:"context_step"`
	Model                string        `json:"model"`
	ContextWindowTokens  int           `json:"context_window_tokens"`
	MaxOutputTokens      int           `json:"max_output_tokens"`
	EstimatedInputTokens int           `json:"estimated_input_tokens"`
	CandidateMessages    []Message     `json:"candidate_messages"`
	CurrentMessageOffset int           `json:"current_message_offset"`
	CurrentMessageCount  int           `json:"current_message_count"`
	KeptRanges           []Range       `json:"kept_ranges"`
	OmittedRanges        []Range       `json:"omitted_ranges"`
	Materials            []MaterialRef `json:"materials"`
	Tools                []Tool        `json:"tools"`
	ToolsetDigest        string        `json:"toolset_digest"`
	PolicyVersion        string        `json:"policy_version"`
	CompactorVersion     string        `json:"compactor_version"`
	Replayable           bool          `json:"replayable"`
	UntrustedDigest      string        `json:"untrusted_digest,omitempty"`
	Digest               string        `json:"digest"`
}

type TurnSnapshot struct {
	CandidateSnapshot
	SealAuthorityGeneration    int64  `json:"seal_authority_generation"`
	SealLedgerHeadSequence     int64  `json:"seal_ledger_head_sequence"`
	SealLedgerHeadEventID      int64  `json:"seal_ledger_head_event_id"`
	SealLedgerProjectionDigest string `json:"seal_ledger_projection_digest"`
	SnapshotDigest             string `json:"snapshot_digest"`
}

type BuildResult struct {
	Candidate CandidateSnapshot
}

type SealResult struct {
	Sealed   bool
	Skipped  bool
	Snapshot TurnSnapshot
}

func Build(in BuildInput) (BuildResult, error) {
	if err := validateInput(in); err != nil {
		return BuildResult{}, err
	}
	window := in.ContextWindowTokens
	if window <= 0 {
		window = defaultContextWindow
	}
	reserved := in.MaxOutputTokens
	if reserved < 0 {
		return BuildResult{}, errors.New("agentcontext: max output tokens is negative")
	}

	tools, toolDigest, toolTokens, err := normalizeTools(in.Tools)
	if err != nil {
		return BuildResult{}, err
	}
	materials, err := normalizeMaterials(in.Profile, in.Playbooks)
	if err != nil {
		return BuildResult{}, err
	}
	baseTokens := contextEnvelopeOverhead + conservativeTokens(in.SystemPrompt) +
		messageFramingTokens + toolTokens + reserved
	current, currentDigest, replayable, err := durableGroup(in.Current, true)
	if err != nil {
		return BuildResult{}, err
	}
	// Redaction can expand a short/empty tainted source into the fixed durable
	// placeholder. Charge the larger shape so neither the provider input nor
	// the persisted candidate can exceed the declared window.
	currentTokens := max(groupTokens(in.Current), groupTokens(current))
	if baseTokens+currentTokens > window {
		return BuildResult{}, errors.New("agentcontext: required current turn exceeds context window")
	}

	remaining := window - baseTokens - currentTokens
	keep := make([]bool, len(in.History))
	for i := len(in.History) - 1; i >= 0; i-- {
		cost := groupTokens(in.History[i])
		if cost <= remaining {
			keep[i] = true
			remaining -= cost
		}
	}
	firstIntent := firstUserIntentGroup(in.History)
	if firstIntent >= 0 && !keep[firstIntent] {
		cost := groupTokens(in.History[firstIntent])
		if cost <= remaining {
			keep[firstIntent] = true
			remaining -= cost
		}
	}

	messages := []Message{{Role: "system", Content: in.SystemPrompt}}
	kept := make([]Range, 0, len(in.History)+1)
	omitted := make([]Range, 0, len(in.History))
	estimated := baseTokens - reserved
	var untrustedDigests []string
	for i, group := range in.History {
		if !keep[i] {
			omitted = append(omitted, groupRange(group, "budget"))
			continue
		}
		durable, digest, groupReplayable, buildErr := durableGroup(group, false)
		if buildErr != nil {
			return BuildResult{}, buildErr
		}
		messages = append(messages, durable.Messages...)
		kept = append(kept, groupRange(group, "history"))
		estimated += groupTokens(group)
		replayable = replayable && groupReplayable
		if digest != "" {
			untrustedDigests = append(untrustedDigests, digest)
		}
	}
	currentOffset := len(messages)
	messages = append(messages, current.Messages...)
	currentRange := groupRange(in.Current, "current")
	currentRange.DurableMessageCount = len(current.Messages)
	kept = append(kept, currentRange)
	estimated += currentTokens
	if currentDigest != "" {
		untrustedDigests = append(untrustedDigests, currentDigest)
	}
	// Per-group checks protect atomicity while this whole-candidate check keeps
	// tool-call identities globally unique across retained turns. Build and
	// VerifyCandidate must accept exactly the same protocol surface.
	if err := validateToolProtocol(messages); err != nil {
		return BuildResult{},
			errors.New("agentcontext: candidate message protocol is invalid")
	}

	candidate := CandidateSnapshot{
		SchemaVersion: SnapshotSchemaVersion, CompilerVersion: CompilerVersion,
		Scope: in.Scope, TurnID: in.TurnID, ContextStep: in.ContextStep,
		Model: in.Model, ContextWindowTokens: window,
		MaxOutputTokens: reserved, EstimatedInputTokens: estimated,
		CandidateMessages:    messages,
		CurrentMessageOffset: currentOffset,
		CurrentMessageCount:  len(current.Messages),
		KeptRanges:           kept, OmittedRanges: omitted,
		Materials: materials, Tools: tools, ToolsetDigest: toolDigest,
		PolicyVersion: PolicyVersion, CompactorVersion: CompactorVersion,
		Replayable: replayable,
	}
	if len(untrustedDigests) > 0 {
		candidate.UntrustedDigest = digestStrings(untrustedDigests)
	}
	digest, err := candidateDigest(candidate)
	if err != nil {
		return BuildResult{}, err
	}
	candidate.Digest = digest
	return BuildResult{Candidate: candidate}, nil
}

func validateInput(in BuildInput) error {
	scopePositive := in.Scope.TenantID > 0 && in.Scope.UserID > 0 &&
		in.Scope.SessionID > 0
	scopeTransient := in.Scope == (Scope{})
	if !scopePositive && !scopeTransient {
		return errors.New("agentcontext: scope is invalid")
	}
	if strings.TrimSpace(in.TurnID) == "" || len(in.TurnID) > 128 ||
		in.ContextStep <= 0 {
		return errors.New("agentcontext: turn identity is invalid")
	}
	if strings.TrimSpace(in.Model) == "" ||
		strings.TrimSpace(in.SystemPrompt) == "" {
		return errors.New("agentcontext: model and system prompt are required")
	}
	if !utf8.ValidString(in.SystemPrompt) {
		return errors.New("agentcontext: system prompt is invalid utf-8")
	}
	var previousLast int64
	var anchoredHistory bool
	var unanchoredHistory bool
	for i, group := range in.History {
		if err := validateGroup(group, false); err != nil {
			return fmt.Errorf("agentcontext: history group %d: %w", i, err)
		}
		if group.FirstMessageOrdinal == 0 {
			unanchoredHistory = true
			continue
		}
		anchoredHistory = true
		if (previousLast == 0 && group.FirstMessageOrdinal != 1) ||
			(previousLast > 0 &&
				group.FirstMessageOrdinal != previousLast+1) {
			return errors.New("agentcontext: history message ranges are not contiguous")
		}
		previousLast = group.LastMessageOrdinal
	}
	if anchoredHistory && unanchoredHistory {
		return errors.New("agentcontext: history mixes anchored and unanchored groups")
	}
	if err := validateGroup(in.Current, true); err != nil {
		return fmt.Errorf("agentcontext: current group: %w", err)
	}
	if unanchoredHistory && in.Current.FirstMessageOrdinal > 0 {
		return errors.New(
			"agentcontext: unanchored history cannot precede an anchored current range",
		)
	}
	if !anchoredHistory && !unanchoredHistory &&
		in.Current.FirstMessageOrdinal > 0 &&
		in.Current.FirstMessageOrdinal != 1 {
		return errors.New("agentcontext: first anchored current range must start at one")
	}
	if anchoredHistory && in.Current.FirstMessageOrdinal > 0 &&
		in.Current.FirstMessageOrdinal != previousLast+1 {
		return errors.New("agentcontext: current message range is not contiguous with history")
	}
	return nil
}

func validateGroup(group AtomicGroup, current bool) error {
	if !group.Trust.valid() || len(group.Messages) == 0 {
		return errors.New("invalid trust or empty messages")
	}
	if current {
		hasNoOrdinals := group.FirstMessageOrdinal == 0 &&
			group.LastMessageOrdinal == 0
		hasValidOrdinals := group.FirstMessageOrdinal > 0 &&
			group.LastMessageOrdinal >= group.FirstMessageOrdinal
		if !hasNoOrdinals && !hasValidOrdinals {
			return errors.New("current message range is invalid")
		}
	} else if !((group.FirstMessageOrdinal == 0 &&
		group.LastMessageOrdinal == 0) ||
		(group.FirstMessageOrdinal > 0 &&
			group.LastMessageOrdinal >= group.FirstMessageOrdinal)) {
		return errors.New("history message range is invalid")
	}
	if group.FirstMessageOrdinal > 0 &&
		group.LastMessageOrdinal-group.FirstMessageOrdinal+1 !=
			int64(len(group.Messages)) {
		return errors.New("message range length does not match messages")
	}
	if group.Trust == TrustUntrustedCurrent && !current {
		return errors.New("untrusted_current is only valid for current turn")
	}
	return validateToolProtocol(group.Messages)
}

func validateToolProtocol(messages []Message) error {
	pending := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, message := range messages {
		switch message.Role {
		case "system", "user":
			if len(pending) != 0 {
				return errors.New("tool calls are missing replies before a new instruction")
			}
			if len(message.ToolCalls) != 0 || message.ToolCallID != "" {
				return errors.New("non-assistant message carries tool calls")
			}
		case "assistant":
			if len(pending) != 0 || message.ToolCallID != "" {
				return errors.New("assistant tool protocol is out of order")
			}
			for _, call := range message.ToolCalls {
				if strings.TrimSpace(call.ID) == "" ||
					strings.TrimSpace(call.Name) == "" {
					return errors.New("tool call identity is empty")
				}
				if _, ok := seen[call.ID]; ok {
					return errors.New("duplicate tool call id")
				}
				seen[call.ID] = struct{}{}
				pending[call.ID] = struct{}{}
			}
		case "tool":
			if len(message.ToolCalls) != 0 ||
				strings.TrimSpace(message.ToolCallID) == "" {
				return errors.New("tool reply is malformed")
			}
			if _, ok := pending[message.ToolCallID]; !ok {
				return errors.New("orphan or duplicate tool reply")
			}
			delete(pending, message.ToolCallID)
		default:
			return errors.New("message role is invalid")
		}
		if !utf8.ValidString(message.Content) {
			return errors.New("message content is invalid utf-8")
		}
	}
	if len(pending) != 0 {
		return errors.New("tool calls are missing replies")
	}
	return nil
}

func durableGroup(group AtomicGroup, current bool) (AtomicGroup, string, bool, error) {
	out := group
	out.Messages = cloneMessages(group.Messages)
	if group.Trust != TrustUntrustedCurrent {
		return out, "", true, nil
	}
	if !current {
		return AtomicGroup{}, "", false,
			errors.New("agentcontext: untrusted history cannot be durable")
	}
	raw, err := json.Marshal(group.Messages)
	if err != nil {
		return AtomicGroup{}, "", false,
			errors.New("agentcontext: encode untrusted current turn")
	}
	sum := sha256.Sum256(raw)
	out.Trust = TrustSanitizedPlaceholder
	out.Messages = []Message{{Role: "user", Content: untrustedPlaceholder}}
	return out, hex.EncodeToString(sum[:]), false, nil
}

func normalizeTools(tools []Tool) ([]Tool, string, int, error) {
	out := slices.Clone(tools)
	for i := range out {
		tool := &out[i]
		if strings.TrimSpace(tool.Definition.Name) == "" ||
			strings.TrimSpace(tool.Definition.Description) == "" ||
			strings.TrimSpace(tool.Policy.Version) == "" {
			return nil, "", 0, errors.New("agentcontext: tool definition or policy is incomplete")
		}
		canonical, err := canonicalJSON(tool.Definition.Parameters)
		if err != nil {
			return nil, "", 0, errors.New("agentcontext: tool parameters are invalid")
		}
		tool.Definition.Parameters = canonical
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, "", 0, errors.New("agentcontext: encode tools")
	}
	sum := sha256.Sum256(raw)
	tokens := toolDefinitionOverhead
	for _, tool := range out {
		policyRaw, policyErr := json.Marshal(tool.Policy)
		if policyErr != nil {
			return nil, "", 0,
				errors.New("agentcontext: encode tool policy")
		}
		tokens += conservativeTokens(tool.Definition.Name)
		tokens += conservativeTokens(tool.Definition.Description)
		tokens += conservativeTokens(string(tool.Definition.Parameters))
		tokens += conservativeTokens(string(policyRaw)) + 12
	}
	return out, hex.EncodeToString(sum[:]), tokens, nil
}

func normalizeMaterials(groups ...[]MaterialRef) ([]MaterialRef, error) {
	var out []MaterialRef
	for _, group := range groups {
		out = append(out, group...)
	}
	for _, material := range out {
		if strings.TrimSpace(material.Kind) == "" ||
			strings.TrimSpace(material.Version) == "" ||
			!validDigest(material.Digest) ||
			material.UpdatedAt.IsZero() {
			return nil, errors.New("agentcontext: material reference is invalid")
		}
	}
	slices.SortFunc(out, func(a, b MaterialRef) int {
		if c := strings.Compare(a.Kind, b.Kind); c != 0 {
			return c
		}
		if c := strings.Compare(a.Version, b.Version); c != 0 {
			return c
		}
		return strings.Compare(a.Digest, b.Digest)
	})
	return out, nil
}

func groupTokens(group AtomicGroup) int {
	total := 0
	for _, message := range group.Messages {
		total += messageFramingTokens + conservativeTokens(message.Role)
		total += conservativeTokens(message.Content)
		for _, call := range message.ToolCalls {
			total += conservativeTokens(call.ID) + conservativeTokens(call.Name)
			total += conservativeTokens(call.Arguments) + 8
		}
		total += conservativeTokens(message.ToolCallID)
	}
	return total
}

func conservativeTokens(value string) int {
	// Without binding v1 snapshots to a provider tokenizer, UTF-8 byte length
	// is the portable upper bound: every non-empty token must consume at least
	// one input byte. This intentionally over-reserves ASCII, CJK and emoji.
	return len(value)
}

func firstUserIntentGroup(history []AtomicGroup) int {
	for i, group := range history {
		for _, message := range group.Messages {
			if message.Role == "user" {
				return i
			}
		}
	}
	return -1
}

func groupRange(group AtomicGroup, reason string) Range {
	if reason == "history" && group.FirstMessageOrdinal == 0 {
		reason = "history_unanchored"
	}
	return Range{
		FirstMessageOrdinal: group.FirstMessageOrdinal,
		LastMessageOrdinal:  group.LastMessageOrdinal,
		SourceMessageCount:  len(group.Messages),
		DurableMessageCount: len(group.Messages),
		Reason:              reason,
	}
}

func cloneMessages(messages []Message) []Message {
	out := make([]Message, len(messages))
	for i, message := range messages {
		out[i] = message
		out[i].ToolCalls = slices.Clone(message.ToolCalls)
	}
	return out
}

func candidateDigest(candidate CandidateSnapshot) (string, error) {
	candidate.Digest = ""
	raw, err := json.Marshal(candidate)
	if err != nil {
		return "", errors.New("agentcontext: encode candidate")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func TurnSnapshotDigest(snapshot TurnSnapshot) (string, error) {
	snapshot.SnapshotDigest = ""
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return "", errors.New("agentcontext: encode turn snapshot")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func VerifyCandidate(candidate CandidateSnapshot) error {
	if candidate.SchemaVersion != SnapshotSchemaVersion ||
		candidate.CompilerVersion != CompilerVersion ||
		candidate.CompactorVersion != CompactorVersion ||
		candidate.PolicyVersion != PolicyVersion ||
		candidate.Scope.TenantID <= 0 ||
		candidate.Scope.UserID <= 0 ||
		candidate.Scope.SessionID <= 0 ||
		len(candidate.TurnID) == 0 || len(candidate.TurnID) > 128 ||
		candidate.ContextStep <= 0 ||
		strings.TrimSpace(candidate.Model) == "" ||
		candidate.ContextWindowTokens <= 0 ||
		candidate.MaxOutputTokens < 0 ||
		candidate.CurrentMessageOffset < 1 ||
		candidate.CurrentMessageCount <= 0 ||
		candidate.CurrentMessageOffset+candidate.CurrentMessageCount !=
			len(candidate.CandidateMessages) ||
		!validDigest(candidate.ToolsetDigest) ||
		!validDigest(candidate.Digest) {
		return errors.New("agentcontext: candidate envelope is invalid")
	}
	normalizedTools, toolsetDigest, toolTokens, err :=
		normalizeTools(candidate.Tools)
	if err != nil || toolsetDigest != candidate.ToolsetDigest {
		return errors.New("agentcontext: candidate toolset is invalid")
	}
	storedTools, marshalErr := json.Marshal(candidate.Tools)
	canonicalTools, canonicalErr := json.Marshal(normalizedTools)
	if marshalErr != nil || canonicalErr != nil ||
		!bytes.Equal(storedTools, canonicalTools) {
		return errors.New("agentcontext: candidate tools are not canonical")
	}
	normalizedMaterials, err := normalizeMaterials(candidate.Materials)
	if err != nil {
		return errors.New("agentcontext: candidate materials are invalid")
	}
	storedMaterials, marshalErr := json.Marshal(candidate.Materials)
	canonicalMaterials, canonicalErr := json.Marshal(normalizedMaterials)
	if marshalErr != nil || canonicalErr != nil ||
		!bytes.Equal(storedMaterials, canonicalMaterials) {
		return errors.New("agentcontext: candidate materials are not canonical")
	}
	if err := validateToolProtocol(candidate.CandidateMessages); err != nil {
		return errors.New("agentcontext: candidate message protocol is invalid")
	}
	minimumInputTokens := contextEnvelopeOverhead + toolTokens +
		messageFramingTokens +
		conservativeTokens(candidate.CandidateMessages[0].Content) +
		groupTokens(AtomicGroup{
			Messages: candidate.CandidateMessages[1:],
		})
	if candidate.EstimatedInputTokens < minimumInputTokens ||
		candidate.EstimatedInputTokens <= 0 ||
		candidate.EstimatedInputTokens+candidate.MaxOutputTokens >
			candidate.ContextWindowTokens {
		return errors.New("agentcontext: candidate token budget is invalid")
	}
	if err := verifyCandidateRanges(candidate); err != nil {
		return err
	}
	digest, err := candidateDigest(candidate)
	if err != nil || digest != candidate.Digest {
		return errors.New("agentcontext: candidate digest mismatch")
	}
	if candidate.UntrustedDigest != "" &&
		!validDigest(candidate.UntrustedDigest) {
		return errors.New("agentcontext: untrusted digest is invalid")
	}
	if (!candidate.Replayable && candidate.UntrustedDigest == "") ||
		(candidate.Replayable && candidate.UntrustedDigest != "") {
		return errors.New("agentcontext: replayability evidence is invalid")
	}
	if !candidate.Replayable &&
		!exactRedactedCurrent(candidate) {
		return errors.New("agentcontext: non-replayable candidate is not redacted")
	}
	return nil
}

func verifyCandidateRanges(candidate CandidateSnapshot) error {
	if len(candidate.KeptRanges) == 0 ||
		candidate.KeptRanges[len(candidate.KeptRanges)-1].Reason != "current" {
		return errors.New("agentcontext: candidate current range is invalid")
	}
	totalKept := 0
	for i, kept := range candidate.KeptRanges {
		if kept.SourceMessageCount <= 0 ||
			kept.DurableMessageCount <= 0 ||
			(kept.Reason != "history" &&
				kept.Reason != "history_unanchored" &&
				kept.Reason != "current") ||
			(kept.Reason == "current" &&
				i != len(candidate.KeptRanges)-1) {
			return errors.New("agentcontext: candidate kept ranges are invalid")
		}
		if kept.SourceMessageCount != kept.DurableMessageCount &&
			(kept.Reason != "current" || candidate.Replayable ||
				kept.DurableMessageCount != 1) {
			return errors.New(
				"agentcontext: candidate durable/source range counts mismatch",
			)
		}
		totalKept += kept.DurableMessageCount
	}
	if totalKept != len(candidate.CandidateMessages)-1 ||
		candidate.KeptRanges[len(candidate.KeptRanges)-1].
			DurableMessageCount !=
			candidate.CurrentMessageCount {
		return errors.New("agentcontext: candidate kept range counts mismatch")
	}
	historyKept := totalKept - candidate.CurrentMessageCount
	if candidate.CurrentMessageOffset != 1+historyKept {
		return errors.New("agentcontext: candidate current offset mismatch")
	}

	current := candidate.KeptRanges[len(candidate.KeptRanges)-1]
	history := append(
		slices.Clone(candidate.KeptRanges[:len(candidate.KeptRanges)-1]),
		candidate.OmittedRanges...,
	)
	var keptLast, omittedLast int64
	for _, item := range candidate.KeptRanges[:len(candidate.KeptRanges)-1] {
		if item.FirstMessageOrdinal > 0 {
			if keptLast > 0 && item.FirstMessageOrdinal <= keptLast {
				return errors.New(
					"agentcontext: kept history ranges are out of order",
				)
			}
			keptLast = item.LastMessageOrdinal
		}
	}
	for _, item := range candidate.OmittedRanges {
		if item.Reason != "budget" ||
			item.SourceMessageCount != item.DurableMessageCount {
			return errors.New("agentcontext: omitted ranges are invalid")
		}
		if item.FirstMessageOrdinal > 0 {
			if omittedLast > 0 && item.FirstMessageOrdinal <= omittedLast {
				return errors.New(
					"agentcontext: omitted history ranges are out of order",
				)
			}
			omittedLast = item.LastMessageOrdinal
		}
	}
	var anchoredHistory, unanchoredHistory bool
	for _, item := range append(slices.Clone(history), current) {
		if item.SourceMessageCount <= 0 ||
			item.DurableMessageCount <= 0 ||
			(item.Reason != "history" &&
				item.Reason != "history_unanchored" &&
				item.Reason != "current" &&
				item.Reason != "budget") {
			return errors.New("agentcontext: candidate ranges are invalid")
		}
		if item.FirstMessageOrdinal == 0 && item.LastMessageOrdinal == 0 {
			if item.Reason == "history" {
				return errors.New("agentcontext: unanchored history reason is invalid")
			}
			if item.Reason != "current" {
				unanchoredHistory = true
			}
			continue
		}
		if item.FirstMessageOrdinal <= 0 ||
			item.LastMessageOrdinal-item.FirstMessageOrdinal+1 !=
				int64(item.SourceMessageCount) {
			return errors.New("agentcontext: candidate range length mismatch")
		}
		if item.Reason != "current" {
			anchoredHistory = true
		}
	}
	if anchoredHistory && unanchoredHistory {
		return errors.New(
			"agentcontext: candidate history mixes anchor modes",
		)
	}
	if unanchoredHistory && current.FirstMessageOrdinal > 0 {
		return errors.New(
			"agentcontext: anchored current follows unanchored history",
		)
	}
	anchored := make([]Range, 0, len(history))
	for _, item := range history {
		if item.FirstMessageOrdinal > 0 {
			anchored = append(anchored, item)
		}
	}
	slices.SortFunc(anchored, func(a, b Range) int {
		switch {
		case a.FirstMessageOrdinal < b.FirstMessageOrdinal:
			return -1
		case a.FirstMessageOrdinal > b.FirstMessageOrdinal:
			return 1
		default:
			return 0
		}
	})
	var expected int64 = 1
	for _, item := range anchored {
		if item.FirstMessageOrdinal != expected {
			return errors.New("agentcontext: candidate ranges are not contiguous")
		}
		expected = item.LastMessageOrdinal + 1
	}
	if current.FirstMessageOrdinal > 0 &&
		current.FirstMessageOrdinal != expected {
		return errors.New(
			"agentcontext: current range is not contiguous with history",
		)
	}
	return nil
}

func exactRedactedCurrent(candidate CandidateSnapshot) bool {
	if candidate.CurrentMessageCount != 1 {
		return false
	}
	current := candidate.CandidateMessages[candidate.CurrentMessageOffset]
	return current.Role == "user" &&
		current.Content == untrustedPlaceholder &&
		len(current.ToolCalls) == 0 &&
		current.ToolCallID == ""
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing json")
	}
	return json.Marshal(value)
}

func digestStrings(values []string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}
