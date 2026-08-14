package types

import (
	"strings"
	"testing"
)

func TestResearchLLMGatewayCanonicalPayloadBindsExactEvidence(t *testing.T) {
	receipt := ResearchLLMGatewayReceiptV3{
		SchemaVersion: ResearchLLMGatewayReceiptSchemaV3,
		KeyID:         "key-1", SignedAtUnixMillis: 42, ReservationID: 7,
		RequestDigest: strings.Repeat("a", 64),
		Call: LLMCall{Provider: "deepseek", Model: "model", SystemPrompt: "s",
			UserPrompt: "u", Completion: "c", PromptTokens: 1, CompletionTokens: 2},
		Attempted: true, UsageKnown: true, Outcome: "completed",
	}
	base, err := receipt.CanonicalPayload()
	if err != nil {
		t.Fatal(err)
	}
	mutated := receipt
	mutated.Call.Completion = "C"
	other, err := mutated.CanonicalPayload()
	if err != nil {
		t.Fatal(err)
	}
	if string(base) == string(other) {
		t.Fatal("completion mutation did not change canonical payload")
	}
}
