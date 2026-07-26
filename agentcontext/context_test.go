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

func TestBuildRejectsToolCallIDReusedAcrossGroups(t *testing.T) {
	callAndReply := func(user string) []Message {
		return []Message{
			{Role: "user", Content: user},
			{Role: "assistant", ToolCalls: []ToolCall{{
				ID: "reused-call", Name: "read", Arguments: `{}`,
			}}},
			{Role: "tool", Content: "result", ToolCallID: "reused-call"},
		}
	}
	input := validBuildInput()
	input.History = []AtomicGroup{{
		FirstMessageOrdinal: 1, LastMessageOrdinal: 3,
		Trust: TrustTrusted, Messages: callAndReply("old"),
	}}
	input.Current = AtomicGroup{
		Trust: TrustTrusted, Messages: callAndReply("current"),
	}
	if _, err := Build(input); err == nil {
		t.Fatal("Build() accepted a tool call ID reused across retained groups")
	}
}

func TestBuildRejectsFabricatedOrdinalRanges(t *testing.T) {
	tests := map[string]func(*BuildInput){
		"group length mismatch": func(input *BuildInput) {
			input.History[0].LastMessageOrdinal = 3
		},
		"history gap": func(input *BuildInput) {
			input.History = append(input.History,
				group(4, 5, "gap question", "gap answer"))
		},
		"anchored current gap": func(input *BuildInput) {
			input.Current.FirstMessageOrdinal = 4
			input.Current.LastMessageOrdinal = 4
		},
		"first history anchor is not one": func(input *BuildInput) {
			input.History[0].FirstMessageOrdinal = 2
			input.History[0].LastMessageOrdinal = 3
		},
		"unanchored history before anchored current": func(input *BuildInput) {
			input.History[0].FirstMessageOrdinal = 0
			input.History[0].LastMessageOrdinal = 0
			input.Current.FirstMessageOrdinal = 3
			input.Current.LastMessageOrdinal = 3
		},
		"first anchored current is not one": func(input *BuildInput) {
			input.History = nil
			input.Current.FirstMessageOrdinal = 2
			input.Current.LastMessageOrdinal = 2
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			input := validBuildInput()
			mutate(&input)
			if _, err := Build(input); err == nil {
				t.Fatal("Build() accepted fabricated ordinal ranges")
			}
		})
	}
}

func TestVerifyCandidateRejectsRangeTamperWithRecomputedDigest(t *testing.T) {
	result, err := Build(validBuildInput())
	if err != nil {
		t.Fatal(err)
	}
	candidate := result.Candidate
	candidate.KeptRanges[0].LastMessageOrdinal++
	candidate.Digest, err = candidateDigest(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCandidate(candidate); err == nil {
		t.Fatal("candidate with recomputed digest hid a fabricated message range")
	}
}

func TestVerifyCandidateRejectsRangeOrderAndAnchorModeMutations(t *testing.T) {
	t.Run("kept history reorder", func(t *testing.T) {
		input := validBuildInput()
		input.History = []AtomicGroup{
			group(1, 2, "first", "answer"),
			group(3, 4, "second", "answer"),
		}
		result, err := Build(input)
		if err != nil {
			t.Fatal(err)
		}
		candidate := result.Candidate
		candidate.KeptRanges[0], candidate.KeptRanges[1] =
			candidate.KeptRanges[1], candidate.KeptRanges[0]
		candidate.Digest, err = candidateDigest(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyCandidate(candidate); err == nil {
			t.Fatal("reordered kept ranges passed with recomputed digest")
		}
	})
	t.Run("mixed history anchor modes", func(t *testing.T) {
		input := validBuildInput()
		input.History = []AtomicGroup{
			{
				Trust: TrustTrusted,
				Messages: []Message{
					{Role: "user", Content: "first"},
					{Role: "assistant", Content: "answer"},
				},
			},
			{
				Trust: TrustTrusted,
				Messages: []Message{
					{Role: "user", Content: "second"},
					{Role: "assistant", Content: "answer"},
				},
			},
		}
		result, err := Build(input)
		if err != nil {
			t.Fatal(err)
		}
		candidate := result.Candidate
		candidate.KeptRanges[0].FirstMessageOrdinal = 1
		candidate.KeptRanges[0].LastMessageOrdinal = 2
		candidate.KeptRanges[0].Reason = "history"
		candidate.Digest, err = candidateDigest(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyCandidate(candidate); err == nil {
			t.Fatal("mixed anchored/unanchored history passed")
		}
	})
}

func TestVerifyCandidateRejectsUnderreportedTokenBudget(t *testing.T) {
	result, err := Build(validBuildInput())
	if err != nil {
		t.Fatal(err)
	}
	for name, estimated := range map[string]int{
		"negative":              -1,
		"below durable minimum": 1,
	} {
		t.Run(name, func(t *testing.T) {
			candidate := result.Candidate
			candidate.EstimatedInputTokens = estimated
			candidate.Digest, err = candidateDigest(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyCandidate(candidate); err == nil {
				t.Fatal("underreported token budget passed")
			}
		})
	}
}

func TestByteUpperBoundBudgetHandlesMultibyteExactWindow(t *testing.T) {
	for _, content := range []string{
		"😀🧪罕见字符𠮷",
		"混合-ASCII-🚀-文本",
	} {
		t.Run(content, func(t *testing.T) {
			input := validBuildInput()
			input.History = nil
			input.Current = AtomicGroup{
				Trust:    TrustTrusted,
				Messages: []Message{{Role: "user", Content: content}},
			}
			input.ContextWindowTokens = 1 << 20
			result, err := Build(input)
			if err != nil {
				t.Fatal(err)
			}
			exact := result.Candidate.EstimatedInputTokens +
				input.MaxOutputTokens
			input.ContextWindowTokens = exact
			if _, err := Build(input); err != nil {
				t.Fatalf("exact byte upper-bound window rejected: %v", err)
			}
			input.ContextWindowTokens = exact - 1
			if _, err := Build(input); err == nil {
				t.Fatal("max+1 required byte budget unexpectedly fit")
			}
		})
	}
}

func TestUntrustedPlaceholderExpansionIsBudgetedAtExactWindow(t *testing.T) {
	input := validBuildInput()
	input.History = nil
	input.Current = AtomicGroup{
		Trust:    TrustUntrustedCurrent,
		Messages: []Message{{Role: "user", Content: ""}},
	}
	input.ContextWindowTokens = 1 << 20
	result, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	durableCurrent := AtomicGroup{
		Messages: result.Candidate.CandidateMessages[result.Candidate.CurrentMessageOffset:],
	}
	if result.Candidate.EstimatedInputTokens <
		groupTokens(durableCurrent) {
		t.Fatal("estimated input does not cover expanded durable placeholder")
	}
	exact := result.Candidate.EstimatedInputTokens + input.MaxOutputTokens
	input.ContextWindowTokens = exact
	if _, err := Build(input); err != nil {
		t.Fatalf("exact expanded-placeholder window rejected: %v", err)
	}
	input.ContextWindowTokens = exact - 1
	if _, err := Build(input); err == nil {
		t.Fatal("expanded placeholder exceeded max+1 budget without rejection")
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

func TestVerifyCandidateRejectsFakePlaceholderShieldingRawCurrent(t *testing.T) {
	input := validBuildInput()
	input.Current = AtomicGroup{
		Trust: TrustUntrustedCurrent,
		Messages: []Message{{
			Role: "user", Content: "RAW-EXTERNAL-CURRENT",
		}},
	}
	result, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	candidate := result.Candidate
	candidate.CandidateMessages[0].Content += "\n" + untrustedPlaceholder
	candidate.CandidateMessages[candidate.CurrentMessageOffset] = Message{
		Role: "user", Content: "RAW-EXTERNAL-CURRENT",
	}
	candidate.Digest, err = candidateDigest(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCandidate(candidate); err == nil {
		t.Fatal("system-level fake placeholder shielded raw current content")
	}
}

func TestBuildVerifyMultiMessageUntrustedCurrent(t *testing.T) {
	input := validBuildInput()
	input.History = nil
	input.Current = AtomicGroup{
		FirstMessageOrdinal: 1, LastMessageOrdinal: 3,
		Trust: TrustUntrustedCurrent,
		Messages: []Message{
			{Role: "user", Content: "request"},
			{Role: "assistant", ToolCalls: []ToolCall{{
				ID: "external-1", Name: "read", Arguments: `{"q":"raw"}`,
			}}},
			{Role: "tool", ToolCallID: "external-1", Content: "RAW-EXTERNAL"},
		},
	}
	result, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	candidate := result.Candidate
	currentRange := candidate.KeptRanges[len(candidate.KeptRanges)-1]
	if currentRange.SourceMessageCount != 3 ||
		currentRange.DurableMessageCount != 1 ||
		candidate.CurrentMessageCount != 1 {
		t.Fatalf("redacted current shape=%+v candidate=%+v",
			currentRange, candidate)
	}
	if err := VerifyCandidate(candidate); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(candidate)
	if strings.Contains(string(raw), "RAW-EXTERNAL") {
		t.Fatal("multi-message untrusted raw reached candidate")
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
		TurnID: "turn-1", ContextStep: 1, Model: "model",
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
