package types

import (
	"strings"
	"testing"
	"time"
)

func testExecutiveContentV1(periodic bool) ExecutiveBriefContentV1 {
	ref := ExecutiveEvidenceRefV1{
		InsightID: 11, ClaimIndexes: []int{0},
	}
	if periodic {
		ref.BriefID = 7
	}
	content := ExecutiveBriefContentV1{
		Headline:         "本期有一项值得关注的变化",
		ExecutiveSummary: "供应商调整了关键能力，可能影响当前产品路线。",
		DecisionState:    ExecutiveDecisionWatch,
		WhyForYou:        "与你负责的 AI 产品规划直接相关。",
		Signals: []ExecutiveSignalV1{{
			Kind: ExecutiveSignalChange, Title: "产品能力变化",
			Summary: "能力边界发生变化。", EvidenceRefs: []ExecutiveEvidenceRefV1{ref},
		}},
		NextSteps: []ExecutiveNextStepV1{{
			Kind: ExecutiveNextStepDeepDive, Label: "深入了解",
			Rationale: "核对对现有路线的影响。", EvidenceRefs: []ExecutiveEvidenceRefV1{ref},
		}},
	}
	if periodic {
		content.Signals[0].Lifecycle = ExecutiveSignalNew
	}
	return content
}

func TestExecutiveBriefArtifactV1SealAndValidate(t *testing.T) {
	now := time.Date(2026, 7, 28, 1, 2, 3, 456789000, time.UTC)
	draft := ExecutiveBriefArtifactDraftV1{
		SchemaVersion: ExecutiveBriefSchemaVersionV1,
		RunOutcomeID:  1, RunSnapshotID: 2, PushBatchID: 3,
		TenantID: 4, UserID: 5, TaskID: "task-a",
		ProfileEpoch: 6, ProfileVersion: 7,
		ProfileDigest:  strings.Repeat("a", 64),
		InputDigest:    strings.Repeat("b", 64),
		GenerationMode: ExecutiveGenerationModel,
		Processing:     RunCompletenessComplete, GeneratedAt: now,
		Content: testExecutiveContentV1(false),
	}
	artifact, err := draft.Seal(8, 9)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifact.Validate(); err != nil {
		t.Fatal(err)
	}
	replay, err := draft.Seal(8, 9)
	if err != nil || replay.Digest != artifact.Digest {
		t.Fatalf("seal replay = %+v, %v", replay, err)
	}
}

func TestExecutiveBriefArtifactV1RejectsUngroundedAndModeMismatch(t *testing.T) {
	base := ExecutiveBriefArtifactDraftV1{
		SchemaVersion: ExecutiveBriefSchemaVersionV1,
		RunOutcomeID:  1, RunSnapshotID: 2, PushBatchID: 3,
		TenantID: 4, UserID: 5, TaskID: "task-a",
		ProfileDigest:  strings.Repeat("a", 64),
		InputDigest:    strings.Repeat("b", 64),
		GenerationMode: ExecutiveGenerationModel,
		Processing:     RunCompletenessComplete,
		GeneratedAt:    time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC),
		Content:        testExecutiveContentV1(false),
	}
	ungrounded := base
	ungrounded.Content.Signals[0].EvidenceRefs = nil
	if err := ungrounded.Validate(); err == nil {
		t.Fatal("ungrounded synthesis was accepted")
	}
	fallbackComplete := base
	fallbackComplete.GenerationMode = ExecutiveGenerationFallback
	if err := fallbackComplete.Validate(); err == nil {
		t.Fatal("complete fallback was accepted")
	}
}

func TestExecutiveBriefContentBindsCanonicalBriefWithoutMutatingInput(
	t *testing.T,
) {
	content := testExecutiveContentV1(false)
	bound, err := content.BindBriefID(77)
	if err != nil {
		t.Fatal(err)
	}
	if content.Signals[0].EvidenceRefs[0].BriefID != 0 ||
		bound.Signals[0].EvidenceRefs[0].BriefID != 77 ||
		bound.NextSteps[0].EvidenceRefs[0].BriefID != 77 {
		t.Fatalf("bound content = %+v, original = %+v", bound, content)
	}
	if _, err := bound.BindBriefID(78); err == nil {
		t.Fatal("rebound content to a different Brief identity")
	}
}

func TestPeriodicBriefReportV1CanonicalizesInputsAndRequiresBriefRefs(t *testing.T) {
	now := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	draft := PeriodicBriefReportDraftV1{
		SchemaVersion: PeriodicBriefSchemaVersionV1,
		TenantID:      1, UserID: 2, TaskID: "task-a",
		Cadence: "weekly", Timezone: "Asia/Shanghai",
		PeriodStart: now, PeriodEnd: now.Add(7 * 24 * time.Hour),
		GeneratedAt:   now.Add(7 * 24 * time.Hour),
		ProfileDigest: strings.Repeat("a", 64),
		InputDigest:   strings.Repeat("b", 64),
		Inputs: []PeriodicBriefInputV1{
			{BriefID: 9, Digest: strings.Repeat("c", 64)},
			{BriefID: 7, Digest: strings.Repeat("d", 64)},
		},
		RunOutcomeIDs: []int64{6, 4},
		OutcomeDigest: strings.Repeat("e", 64),
		GenerationMode: ExecutiveGenerationModel,
		SourceCoverage: RunCompletenessComplete,
		Processing:     RunCompletenessComplete,
		Content:        testExecutiveContentV1(true),
	}
	report, err := draft.Seal(10)
	if err != nil {
		t.Fatal(err)
	}
	if report.Inputs[0].BriefID != 7 ||
		report.RunOutcomeIDs[0] != 4 || report.Validate() != nil {
		t.Fatalf("periodic report = %+v", report)
	}
	bad := draft
	bad.Content.Signals[0].EvidenceRefs[0].BriefID = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("periodic synthesis without brief identity was accepted")
	}
}
