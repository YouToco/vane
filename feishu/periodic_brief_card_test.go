package feishu

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func periodicCardReportFixture(t *testing.T) types.PeriodicBriefReportV1 {
	t.Helper()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	draft := types.PeriodicBriefReportDraftV1{
		SchemaVersion: types.PeriodicBriefSchemaVersionV1,
		TenantID:      1,
		UserID:        2,
		TaskID:        "task-v1",
		Cadence:       "weekly",
		Timezone:      "America/Los_Angeles",
		PeriodStart:   now.AddDate(0, 0, -7),
		PeriodEnd:     now,
		GeneratedAt:   now,
		ProfileDigest: strings.Repeat("a", 64),
		InputDigest:   strings.Repeat("b", 64),
		Inputs: []types.PeriodicBriefInputV1{{
			BriefID: 3, Digest: strings.Repeat("c", 64),
		}},
		RunOutcomeIDs: []int64{4},
		OutcomeDigest: strings.Repeat("d", 64),
		GenerationMode: types.ExecutiveGenerationModel,
		SourceCoverage: types.RunCompletenessComplete,
		Processing:     types.RunCompletenessComplete,
		Content: types.ExecutiveBriefContentV1{
			Headline:         "本周应关注供应风险",
			ExecutiveSummary: "两个独立来源显示交付周期延长。",
			DecisionState:    types.ExecutiveDecisionAct,
			WhyForYou:        "这会影响你负责的采购窗口。",
			Signals: []types.ExecutiveSignalV1{{
				Kind:      types.ExecutiveSignalRisk,
				Lifecycle: types.ExecutiveSignalIntensified,
				Title:     "交付周期增强",
				Summary:   "延迟从个别现象扩展到多个供应商。",
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					BriefID: 3, InsightID: 4, ClaimIndexes: []int{0},
				}},
			}},
			NextSteps: []types.ExecutiveNextStepV1{{
				Kind:      types.ExecutiveNextStepDeepDive,
				Label:     "深入了解",
				Rationale: "核对供应商证据",
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					BriefID: 3, InsightID: 4, ClaimIndexes: []int{0},
				}},
			}},
		},
	}
	report, err := draft.Seal(5)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestPeriodicBriefCardIsBoundedAndPreservesActionQuery(t *testing.T) {
	report := periodicCardReportFixture(t)
	webURL := "https://vane.example/#/tasks/task-v1?brief_period=weekly&report_id=5"
	card := BuildPeriodicBriefCardV1(report, webURL)
	if card == "" || len(card) > periodicBriefCardMaxBytesV1 {
		t.Fatalf("periodic card bytes=%d", len(card))
	}
	var decoded any
	if err := json.Unmarshal([]byte(card), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"本周应关注供应风险",
		"交付周期增强",
		"brief_action=deep_dive",
		"brief_action=edit_task",
		"brief_period=weekly",
		"report_id=5",
	} {
		if !strings.Contains(card, want) {
			t.Fatalf("periodic card missing %q: %s", want, card)
		}
	}
}
