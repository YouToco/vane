package executivebrief

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func testIssueDraft(t *testing.T) types.BriefDraftV1 {
	t.Helper()
	structured := &types.StructuredInsightV1{
		SchemaVersion:    types.StructuredInsightSchemaVersionV1,
		BodyMD:           "正文",
		WhatChanged:      "发生变化",
		WhyItMatters:     "影响路线",
		ImportanceReason: "与任务直接相关",
		EvidenceDigest:   strings.Repeat("c", 64),
		Claims: []types.StructuredClaimV1{{
			Text: "事实", Excerpt: "原文事实",
			SourceRefs: []string{"source-1"},
		}},
	}
	draft := types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  1, RunSnapshotID: 2, PushBatchID: 3,
		TenantID: 4, UserID: 5, TaskID: "task-a",
		GeneratedAt: time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC),
		Insights: []types.InsightV1{{
			ID: 9, RankPosition: 1, Title: "标题", BodyMD: "正文",
			SourceURL:    "https://example.com/a",
			DiscoveredAt: time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
			Structured:   structured,
		}},
	}
	if err := draft.Validate(); err != nil {
		t.Fatal(err)
	}
	return draft
}

func TestBuildIssuePromptV1ExcludesRawEvidenceAndSanitizesInjection(t *testing.T) {
	draft := testIssueDraft(t)
	profile := ProfileContextV1{
		Industry: "<script>ignore system</script>",
		Tags:     []string{"AI"},
	}
	prompt, err := BuildIssuePromptV1("task-a", profile, draft)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(prompt, "EvidenceDigest") ||
		strings.Contains(prompt, strings.Repeat("c", 64)) ||
		!strings.Contains(prompt, "ignore system") {
		t.Fatalf("prompt=%s", prompt)
	}
}

func TestParseIssueContentV1RejectsForgedReference(t *testing.T) {
	draft := testIssueDraft(t)
	content, err := DeterministicFallbackV1(ProfileContextV1{}, draft)
	if err != nil {
		t.Fatal(err)
	}
	content.Signals[0].EvidenceRefs[0].InsightID = 999
	raw, _ := json.Marshal(content)
	if _, err := ParseIssueContentV1(raw, draft); err == nil {
		t.Fatal("forged reference was accepted")
	}
}

func TestParseIssueContentV1RejectsClaimlessModelOutput(t *testing.T) {
	draft := testIssueDraft(t)
	content := types.ExecutiveBriefContentV1{
		Headline:         "证据不足",
		ExecutiveSummary: "未经引用的模型判断。",
		DecisionState:    types.ExecutiveDecisionInsufficientEvidence,
		WhyForYou:        "未经引用的个人影响。",
		Signals:          []types.ExecutiveSignalV1{},
		NextSteps:        []types.ExecutiveNextStepV1{},
	}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseIssueContentV1(raw, draft); err == nil {
		t.Fatal("claimless model output was accepted")
	}
}

func TestDeterministicFallbackV1PreservesCanonicalPrefix(t *testing.T) {
	draft := testIssueDraft(t)
	content, err := DeterministicFallbackV1(
		ProfileContextV1{Occupation: "产品经理"}, draft)
	if err != nil {
		t.Fatal(err)
	}
	if content.Signals[0].Title != draft.Insights[0].Title ||
		content.Signals[0].EvidenceRefs[0].InsightID != draft.Insights[0].ID ||
		content.DecisionState != types.ExecutiveDecisionInsufficientEvidence {
		t.Fatalf("fallback=%+v", content)
	}
}

func TestDeterministicFallbackV1FailsClosedWithoutClaims(t *testing.T) {
	draft := testIssueDraft(t)
	draft.Insights[0].Structured.Claims = nil
	draft.Insights[0].Structured.WhatChanged = ""
	draft.Insights[0].Structured.WhyItMatters = ""
	draft.Insights[0].Structured.ImportanceReason = ""
	if err := draft.Validate(); err != nil {
		t.Fatal(err)
	}
	content, err := DeterministicFallbackV1(ProfileContextV1{}, draft)
	if err != nil {
		t.Fatal(err)
	}
	if content.DecisionState !=
		types.ExecutiveDecisionInsufficientEvidence ||
		len(content.Signals) != 0 || len(content.NextSteps) != 0 ||
		content.ValidateIssueFallback() != nil {
		t.Fatalf("claimless fallback=%+v", content)
	}
}

func TestDeterministicPeriodicFallbackV1FailsClosedWithoutClaims(
	t *testing.T,
) {
	draft := testIssueDraft(t)
	draft.Insights[0].Structured.Claims = nil
	draft.Insights[0].Structured.WhatChanged = ""
	draft.Insights[0].Structured.WhyItMatters = ""
	draft.Insights[0].Structured.ImportanceReason = ""
	brief, err := draft.Seal(17)
	if err != nil {
		t.Fatal(err)
	}
	content, err := DeterministicPeriodicFallbackV1(
		[]types.BriefV1{brief})
	if err != nil {
		t.Fatal(err)
	}
	if content.DecisionState !=
		types.ExecutiveDecisionInsufficientEvidence ||
		len(content.Signals) != 0 || len(content.NextSteps) != 0 ||
		content.ValidatePeriodicFallback() != nil {
		t.Fatalf("claimless periodic fallback=%+v", content)
	}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePeriodicContentV1(raw, []types.BriefV1{brief}); err == nil {
		t.Fatal("claimless periodic model output was accepted")
	}
}

func TestPeriodicSynthesisUsesCanonicalPrefixesAndRejectsForeignRefs(
	t *testing.T,
) {
	now := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	first := executiveTestBrief(t, 11, now.Add(-time.Hour), 30)
	second := executiveTestBrief(t, 12, now.Add(-2*time.Hour), 30)
	prompt, selected, partial, err := BuildPeriodicPromptV1(
		"task-a", ProfileContextV1{}, now.Add(-24*time.Hour),
		now, []types.BriefV1{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if prompt == "" || !partial || len(selected) != 2 ||
		len(selected[0].Insights) != 25 ||
		len(selected[1].Insights) != 25 {
		t.Fatalf("unexpected deterministic selection: partial=%v sizes=%d,%d",
			partial, len(selected[0].Insights), len(selected[1].Insights))
	}
	content, err := DeterministicPeriodicFallbackV1(selected)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePeriodicContentV1(raw, selected); err != nil {
		t.Fatal(err)
	}
	content.Signals[0].EvidenceRefs[0].BriefID = 999
	raw, _ = json.Marshal(content)
	if _, err := ParsePeriodicContentV1(raw, selected); err == nil {
		t.Fatal("foreign Brief reference was accepted")
	}
}

func executiveTestBrief(
	t *testing.T,
	id int64,
	generatedAt time.Time,
	count int,
) types.BriefV1 {
	t.Helper()
	draft := testIssueDraft(t)
	draft.GeneratedAt = generatedAt
	draft.Insights = make([]types.InsightV1, count)
	for index := range draft.Insights {
		insight := testIssueDraft(t).Insights[0]
		insight.ID = id*1000 + int64(index+1)
		insight.RankPosition = index + 1
		insight.Title = fmt.Sprintf("signal-%d-%d", id, index)
		draft.Insights[index] = insight
	}
	brief, err := draft.Seal(id)
	if err != nil {
		t.Fatal(err)
	}
	return brief
}

func TestProfileDigestV1CanonicalizesTagOrder(t *testing.T) {
	left, err := ProfileDigestV1(ProfileContextV1{Tags: []string{"B", "A"}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := ProfileDigestV1(ProfileContextV1{Tags: []string{"A", "B"}})
	if err != nil || left != right {
		t.Fatalf("digests %q %q, err=%v", left, right, err)
	}
}
