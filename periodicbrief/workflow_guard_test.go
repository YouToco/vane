package periodicbrief

import (
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/store"
)

func TestPeriodicSynthesisSpendClaimPrecedesProviderCall(t *testing.T) {
	raw, err := os.ReadFile("workflow.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func (a *Activities) SynthesizePeriodicBriefV1(")
	if start < 0 {
		t.Fatal("periodic synthesis Activity is missing")
	}
	body := source[start:]
	claim := strings.Index(body, "a.store.ClaimPeriodicSynthesisSpendV1(")
	provider := strings.Index(body, "llm.Do(")
	if claim < 0 || provider < 0 || claim >= provider {
		t.Fatalf("spend fence order changed: claim=%d provider=%d", claim, provider)
	}
	if !strings.Contains(source, "MaximumAttempts: 1") {
		t.Fatal("periodic synthesis Activity retry fence was removed")
	}
}

func TestPeriodicRequestDigestBindsFrozenPolicy(t *testing.T) {
	base := store.PeriodicSynthesisPolicyV1{
		RunSnapshotID: 9,
		Renderer:      "periodic.render/v1",
		PolicyDigest:  strings.Repeat("a", 64),
		ModelCall: runtimepolicy.ModelCallV1{
			Model: "model-a",
		},
	}
	first, err := synthesisRequestDigestV1(
		strings.Repeat("1", 64), strings.Repeat("2", 64), base)
	if err != nil {
		t.Fatal(err)
	}
	base.PolicyDigest = strings.Repeat("b", 64)
	second, err := synthesisRequestDigestV1(
		strings.Repeat("1", 64), strings.Repeat("2", 64), base)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("frozen policy mutation did not change request identity")
	}
}
