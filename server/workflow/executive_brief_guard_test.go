package workflow

import (
	"os"
	"strings"
	"testing"
)

func TestExecutiveSynthesisSpendClaimPrecedesProviderCall(t *testing.T) {
	raw, err := os.ReadFile("executive_brief_activities.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(
		source, "func (a *Activities) SynthesizeExecutiveBriefV1(")
	if start < 0 {
		t.Fatal("executive synthesis Activity is missing")
	}
	body := source[start:]
	claim := strings.Index(
		body, "a.executiveBriefStore.ClaimExecutiveSynthesisSpendV1(")
	provider := strings.Index(body, "llm.Do(")
	if claim < 0 || provider < 0 || claim >= provider {
		t.Fatalf("executive spend fence order changed: claim=%d provider=%d",
			claim, provider)
	}

	workflowRaw, err := os.ReadFile("workflow.go")
	if err != nil {
		t.Fatal(err)
	}
	options := string(workflowRaw)
	optionsStart := strings.Index(
		options, "func executiveSynthesisActivityOptions()")
	if optionsStart < 0 {
		t.Fatal("executive synthesis Activity options are missing")
	}
	optionsBody := options[optionsStart:]
	optionsEnd := strings.Index(
		optionsBody, "func quickActivityOptions()")
	if optionsEnd < 0 ||
		!strings.Contains(
			optionsBody[:optionsEnd], "MaximumAttempts:        1") {
		t.Fatal("executive synthesis Activity retry fence was removed")
	}
}
