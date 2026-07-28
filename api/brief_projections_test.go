package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestExecutiveBriefTaskEnabledUsesExactRolloutScope(t *testing.T) {
	s := &server{deps: Deps{
		ExecutiveBriefEnabled:          true,
		ExecutiveBriefCanaryScheduleID: "task-canary",
	}}
	if !s.executiveBriefTaskEnabled("task-canary") ||
		s.executiveBriefTaskEnabled("task-other") {
		t.Fatal("exact executive Brief rollout scope drifted")
	}
	s.deps.ExecutiveBriefAllowAll = true
	s.deps.ExecutiveBriefCanaryScheduleID = ""
	if !s.executiveBriefTaskEnabled("task-other") ||
		s.executiveBriefTaskEnabled("") {
		t.Fatal("allow-all executive Brief rollout scope drifted")
	}
	s.deps.ExecutiveBriefEnabled = false
	if s.executiveBriefTaskEnabled("task-other") {
		t.Fatal("disabled executive Brief rollout remained visible")
	}
}

func TestBriefPublicProjectionsDoNotExposeIntegrityMetadata(t *testing.T) {
	page := store.TaskBriefPageV1{
		Items: []store.TaskBriefItemV1{{
			ID: 1,
			Executive: &types.ExecutiveBriefArtifactV1{
				ID: 99,
				ExecutiveBriefArtifactDraftV1: types.ExecutiveBriefArtifactDraftV1{
					TenantID:      7,
					UserID:        8,
					RunSnapshotID: 9,
					ProfileDigest: strings.Repeat("a", 64),
					InputDigest:   strings.Repeat("b", 64),
					Content: types.ExecutiveBriefContentV1{
						Headline: "public",
					},
				},
			},
		}},
	}
	raw, err := json.Marshal(publicTaskBriefPageV1(page))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{
		"tenant_id", "user_id", "run_snapshot_id", "run_outcome_id",
		"profile_digest", "input_digest", "request_digest",
		"artifact_digest",
	} {
		if strings.Contains(text, `"`+forbidden+`"`) {
			t.Fatalf("public Brief response leaked %q: %s", forbidden, text)
		}
	}

	reportRaw, err := json.Marshal(publicPeriodicBriefReportPageV1(
		store.PeriodicBriefReportPageV1{
			Items: []types.PeriodicBriefReportV1{{
				ID: 2,
				PeriodicBriefReportDraftV1: types.PeriodicBriefReportDraftV1{
					TenantID:      7,
					UserID:        8,
					ProfileDigest: strings.Repeat("c", 64),
					InputDigest:   strings.Repeat("d", 64),
				},
			}},
		}))
	if err != nil {
		t.Fatal(err)
	}
	text = string(reportRaw)
	for _, forbidden := range []string{
		"tenant_id", "user_id", "profile_epoch", "profile_version",
		"profile_digest", "input_digest", "inputs", "digest",
	} {
		if strings.Contains(text, `"`+forbidden+`"`) {
			t.Fatalf("public report response leaked %q: %s", forbidden, text)
		}
	}
}

func TestGroundedContextDeepDiveRequiresExactFrozenReference(t *testing.T) {
	contextValue := store.GroundedBriefContextV1{
		Content: types.ExecutiveBriefContentV1{
			NextSteps: []types.ExecutiveNextStepV1{{
				Kind: types.ExecutiveNextStepDeepDive,
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					InsightID: 41, ClaimIndexes: []int{0},
				}},
			}, {
				Kind: types.ExecutiveNextStepEditTask,
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					InsightID: 42, ClaimIndexes: []int{0},
				}},
			}},
		},
		Evidence: []store.GroundedEvidenceBriefV1{{
			BriefID: 7,
			Insights: []store.TaskBriefInsightV1{
				{ID: 41}, {ID: 42},
			},
		}},
	}
	if !groundedContextAllowsDeepDiveV1(contextValue, 41) {
		t.Fatal("exact frozen deep-dive evidence was rejected")
	}
	for _, denied := range []int64{0, 42, 99} {
		if groundedContextAllowsDeepDiveV1(contextValue, denied) {
			t.Fatalf("non-deep-dive Insight %d was accepted", denied)
		}
	}
	contextValue.Evidence = nil
	if groundedContextAllowsDeepDiveV1(contextValue, 41) {
		t.Fatal("missing immutable evidence accepted deep-dive")
	}
}
