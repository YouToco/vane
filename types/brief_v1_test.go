package types

import (
	"strings"
	"testing"
	"time"
)

func TestRunOutcomeV1SealBindsIndependentCompleteness(t *testing.T) {
	finalized := time.Date(2026, 7, 27, 11, 30, 0, 123456789, time.FixedZone("x", 3600))
	outcome, err := (RunOutcomeV1{
		RunOutcomeMarkerV1: RunOutcomeMarkerV1{
			ID: 7, SchemaVersion: RunOutcomeSchemaVersionV1,
			RunSnapshotID: 11, TenantID: 2, UserID: 3, TaskID: "task-a",
		},
		Result: RunResultQuiet, SourceCoverage: RunCompletenessPartial,
		Processing: RunCompletenessComplete, FinalizedAt: finalized,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := outcome.Validate(); err != nil {
		t.Fatal(err)
	}
	if outcome.FinalizedAt.Location() != time.UTC ||
		outcome.FinalizedAt.Nanosecond()%1000 != 0 {
		t.Fatalf("timestamp was not canonicalized: %s", outcome.FinalizedAt)
	}
	tampered := outcome
	tampered.SourceCoverage = RunCompletenessComplete
	if err := tampered.Validate(); err == nil {
		t.Fatal("completeness mutation did not invalidate outcome digest")
	}
}

func TestRunOutcomeV1RejectsDishonestFailureShape(t *testing.T) {
	base := RunOutcomeV1{
		RunOutcomeMarkerV1: RunOutcomeMarkerV1{
			ID: 7, SchemaVersion: RunOutcomeSchemaVersionV1,
			RunSnapshotID: 11, TenantID: 2, UserID: 3, TaskID: "task-a",
		},
		Result: RunResultFailed, SourceCoverage: RunCompletenessPartial,
		Processing: RunCompletenessPartial, FinalizedAt: time.Now(),
	}
	if _, err := base.Seal(); err == nil {
		t.Fatal("failed outcome without failure code was accepted")
	}
	base.FailureCode = "activity_failed"
	base.FailureMessage = "sanitized failure"
	if _, err := base.Seal(); err != nil {
		t.Fatalf("valid failed outcome rejected: %v", err)
	}
	base.Result = RunResultContent
	if _, err := base.Seal(); err == nil {
		t.Fatal("content outcome carrying failure fields was accepted")
	}
}

func TestBriefV1FreezesRankIdentityAndCanonicalSource(t *testing.T) {
	published := time.Date(2026, 7, 27, 8, 0, 0, 999, time.UTC)
	draft := BriefDraftV1{
		SchemaVersion: BriefSchemaVersionV1,
		RunOutcomeID:  5, RunSnapshotID: 11, PushBatchID: 17,
		TenantID: 2, UserID: 3, TaskID: "task-a",
		GeneratedAt: time.Date(2026, 7, 27, 9, 0, 0, 1234, time.Local),
		Insights: []InsightV1{
			{
				ID: 31, RankPosition: 1, Title: "First",
				BodyMD:      "What changed\n\nWhy it matters",
				SourceTitle: "Official", SourceURL: "https://example.com/a",
				PublishedAt: &published, DiscoveredAt: time.Now(),
			},
			{
				ID: 29, RankPosition: 2, Title: "Second",
				BodyMD: "A second item", SourceURL: "http://example.com/b",
				DiscoveredAt: time.Now(),
			},
		},
	}
	requestDigest, err := draft.RequestDigest()
	if err != nil {
		t.Fatal(err)
	}
	brief, err := draft.Seal(101)
	if err != nil {
		t.Fatal(err)
	}
	if err := brief.Validate(); err != nil {
		t.Fatal(err)
	}
	retryDigest, err := brief.BriefDraftV1.RequestDigest()
	if err != nil || retryDigest != requestDigest {
		t.Fatalf("request digest drifted: %q %v", retryDigest, err)
	}
	if brief.Insights[0].ID != 31 || brief.Insights[1].ID != 29 {
		t.Fatal("seal reordered insights by delivery id")
	}
	tampered := brief
	tampered.Insights = append([]InsightV1(nil), brief.Insights...)
	tampered.Insights[0].RankPosition = 2
	if err := tampered.Validate(); err == nil {
		t.Fatal("rank mutation did not fail validation")
	}
}

func TestBriefV1RejectsUnsafeOrNonCanonicalInputs(t *testing.T) {
	base := BriefDraftV1{
		SchemaVersion: BriefSchemaVersionV1,
		RunOutcomeID:  5, RunSnapshotID: 11, PushBatchID: 17,
		TenantID: 2, UserID: 3, TaskID: "task-a",
		GeneratedAt: time.Now(),
		Insights: []InsightV1{{
			ID: 31, RankPosition: 1, Title: "First", BodyMD: "body",
			SourceURL: "https://example.com/a", DiscoveredAt: time.Now(),
		}},
	}
	cases := map[string]func(*BriefDraftV1){
		"javascript URL": func(d *BriefDraftV1) {
			d.Insights[0].SourceURL = "javascript:alert(1)"
		},
		"credential URL": func(d *BriefDraftV1) {
			d.Insights[0].SourceURL = "https://user:pass@example.com/a"
		},
		"duplicate delivery": func(d *BriefDraftV1) {
			d.Insights = append(d.Insights, d.Insights[0])
			d.Insights[1].RankPosition = 2
		},
		"rank gap": func(d *BriefDraftV1) {
			d.Insights[0].RankPosition = 2
		},
		"oversized body": func(d *BriefDraftV1) {
			d.Insights[0].BodyMD = strings.Repeat("x", maxBriefBodyBytes+1)
		},
		"oversized escaped payload": func(d *BriefDraftV1) {
			body := strings.Repeat("\\", maxBriefBodyBytes)
			d.Insights = make([]InsightV1, 64)
			for i := range d.Insights {
				d.Insights[i] = base.Insights[0]
				d.Insights[i].ID = int64(i + 1)
				d.Insights[i].RankPosition = i + 1
				d.Insights[i].BodyMD = body
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.Insights = append([]InsightV1(nil), base.Insights...)
			mutate(&candidate)
			if _, err := candidate.Seal(1); err == nil {
				t.Fatal("unsafe brief was accepted")
			}
		})
	}
}
