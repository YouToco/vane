package periodicbrief

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/store"
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
	intent := store.PeriodicBriefIntentV1{
		TaskID: "task-v1", Cadence: "daily", Timezone: "UTC",
		PeriodStart: time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC),
	}
	first, err := synthesisRequestDigestV1(
		intent,
		strings.Repeat("1", 64), strings.Repeat("2", 64), base)
	if err != nil {
		t.Fatal(err)
	}
	base.PolicyDigest = strings.Repeat("b", 64)
	second, err := synthesisRequestDigestV1(
		intent,
		strings.Repeat("1", 64), strings.Repeat("2", 64), base)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("frozen policy mutation did not change request identity")
	}
}

func TestPeriodicRequestDigestBindsTaskAndPeriod(t *testing.T) {
	start := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)
	inputDigest, profileDigest :=
		strings.Repeat("1", 64), strings.Repeat("2", 64)
	intent := store.PeriodicBriefIntentV1{
		TaskID: "task-v1", Cadence: "daily", Timezone: "UTC",
		PeriodStart: start, PeriodEnd: end,
	}
	base, err := synthesisRequestDigestV1(
		intent, inputDigest, profileDigest,
		store.PeriodicSynthesisPolicyV1{})
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []struct {
		mutate func(*store.PeriodicBriefIntentV1)
	}{
		{mutate: func(value *store.PeriodicBriefIntentV1) {
			value.TaskID = "task-v2"
		}},
		{mutate: func(value *store.PeriodicBriefIntentV1) {
			value.Cadence = "weekly"
		}},
		{mutate: func(value *store.PeriodicBriefIntentV1) {
			value.Timezone = "Asia/Shanghai"
		}},
		{mutate: func(value *store.PeriodicBriefIntentV1) {
			value.PeriodStart = start.Add(time.Minute)
		}},
		{mutate: func(value *store.PeriodicBriefIntentV1) {
			value.PeriodEnd = end.Add(time.Minute)
		}},
	} {
		mutated := intent
		scope.mutate(&mutated)
		got, digestErr := synthesisRequestDigestV1(
			mutated, inputDigest, profileDigest,
			store.PeriodicSynthesisPolicyV1{})
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		if got == base {
			t.Fatalf("scope mutation retained request digest: %+v", mutated)
		}
	}
}
