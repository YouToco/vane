package agentcontext

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildKeepsAtomicTurnsNewestFirstAndFirstIntent(t *testing.T) {
	input := validBuildInput()
	input.ContextWindowTokens = 1200
	input.History = []AtomicGroup{
		group(1, 2, "first intent", "first answer"),
		group(3, 4, strings.Repeat("middle", 140), "middle answer"),
		group(5, 6, "recent question", "recent answer"),
	}
	result, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	got := result.Candidate
	if len(got.KeptRanges) != 3 {
		t.Fatalf("kept ranges = %+v, want first + recent + current", got.KeptRanges)
	}
	if len(got.OmittedRanges) != 1 ||
		got.OmittedRanges[0].FirstMessageOrdinal != 3 {
		t.Fatalf("omitted ranges = %+v, want middle atomic group", got.OmittedRanges)
	}
	for _, message := range got.CandidateMessages {
		if strings.Contains(message.Content, "middlemiddle") {
			t.Fatal("builder cut or retained the oversized middle group")
		}
	}
}

func TestBuildRejectsBrokenToolProtocol(t *testing.T) {
	tests := []struct {
		name     string
		messages []Message
	}{
		{
			name: "orphan",
			messages: []Message{
				{Role: "user", Content: "q"},
				{Role: "tool", Content: "x", ToolCallID: "missing"},
			},
		},
		{
			name: "duplicate id",
			messages: []Message{
				{Role: "user", Content: "q"},
				{Role: "assistant", ToolCalls: []ToolCall{
					{ID: "call-1", Name: "a", Arguments: `{}`},
					{ID: "call-1", Name: "b", Arguments: `{}`},
				}},
			},
		},
		{
			name: "missing reply",
			messages: []Message{
				{Role: "user", Content: "q"},
				{Role: "assistant", ToolCalls: []ToolCall{
					{ID: "call-1", Name: "a", Arguments: `{}`},
				}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validBuildInput()
			input.Current.Messages = test.messages
			if _, err := Build(input); err == nil {
				t.Fatal("Build() succeeded with broken tool protocol")
			}
		})
	}
}

func TestBuildUntrustedCurrentNeverPersistsRawAndIsNotReplayable(t *testing.T) {
	const attack = "SECRET-EXTERNAL-RAW-IGNORE-POLICY"
	input := validBuildInput()
	input.Current = AtomicGroup{
		Trust: TrustUntrustedCurrent,
		Messages: []Message{{
			Role: "user", Content: attack,
		}},
	}
	result, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	candidate := result.Candidate
	raw, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Replayable || candidate.UntrustedDigest == "" {
		t.Fatalf("candidate replayable=%v digest=%q", candidate.Replayable, candidate.UntrustedDigest)
	}
	if strings.Contains(string(raw), attack) {
		t.Fatal("untrusted original content reached durable candidate bytes")
	}
	if err := VerifyCandidate(candidate); err != nil {
		t.Fatal(err)
	}

	changed := input
	changed.Current.Messages[0].Content += "-changed"
	other, err := Build(changed)
	if err != nil {
		t.Fatal(err)
	}
	if other.Candidate.Digest == candidate.Digest ||
		other.Candidate.UntrustedDigest == candidate.UntrustedDigest {
		t.Fatal("untrusted input change did not change opaque digests")
	}
}

func TestBuildIsByteStableAndToolOrderSensitive(t *testing.T) {
	input := validBuildInput()
	first, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	firstRaw, _ := json.Marshal(first.Candidate)
	replayRaw, _ := json.Marshal(replay.Candidate)
	if string(firstRaw) != string(replayRaw) {
		t.Fatalf("same input was not byte stable:\n%s\n%s", firstRaw, replayRaw)
	}

	input.Tools[0], input.Tools[1] = input.Tools[1], input.Tools[0]
	second, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	secondRaw, _ := json.Marshal(second.Candidate)
	if string(firstRaw) == string(secondRaw) ||
		first.Candidate.ToolsetDigest == second.Candidate.ToolsetDigest {
		t.Fatalf("ordered tool snapshots did not change:\n%s\n%s", firstRaw, secondRaw)
	}
}

func TestBuildUnknownModelUsesConservativeWindow(t *testing.T) {
	input := validBuildInput()
	input.Model = "future-provider-model"
	input.ContextWindowTokens = 0
	got, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Candidate.ContextWindowTokens != defaultContextWindow {
		t.Fatalf("window=%d, want %d", got.Candidate.ContextWindowTokens, defaultContextWindow)
	}
}

func TestBuildRejectsRequiredOverflow(t *testing.T) {
	input := validBuildInput()
	input.ContextWindowTokens = 40
	if _, err := Build(input); err == nil {
		t.Fatal("Build() truncated required current/system/tool budget")
	}
}

func validBuildInput() BuildInput {
	return BuildInput{
		Scope:  Scope{TenantID: 1, UserID: 2, SessionID: 3},
		TurnID: "turn-1", ModelStep: 1, Model: "model",
		SystemPrompt: "system policy",
		Tools: []Tool{
			{
				Definition: ToolDefinition{
					Name: "b", Description: "tool b",
					Parameters: json.RawMessage(`{"properties":{},"type":"object"}`),
				},
				Policy: validPolicy(),
			},
			{
				Definition: ToolDefinition{
					Name: "a", Description: "tool a",
					Parameters: json.RawMessage(`{"type":"object","properties":{}}`),
				},
				Policy: validPolicy(),
			},
		},
		History: []AtomicGroup{group(1, 2, "old question", "old answer")},
		Current: AtomicGroup{
			Trust:    TrustTrusted,
			Messages: []Message{{Role: "user", Content: "current question"}},
		},
		ContextWindowTokens: 4096,
		MaxOutputTokens:     256,
	}
}

func validPolicy() PolicySnapshot {
	return PolicySnapshot{
		Version: PolicyVersion, Effects: 1, Authorization: 1,
		Confirmation: 1, Budget: 1, Retry: 1, Concurrency: 1,
	}
}

func group(first, last int64, user, assistant string) AtomicGroup {
	return AtomicGroup{
		FirstMessageOrdinal: first, LastMessageOrdinal: last,
		Trust: TrustTrusted,
		Messages: []Message{
			{Role: "user", Content: user},
			{Role: "assistant", Content: assistant},
		},
	}
}
