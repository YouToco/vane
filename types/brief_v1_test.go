package types

import (
	"encoding/json"
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

func TestBriefV1FreezesOptionalStructuredInsight(t *testing.T) {
	structured := &StructuredInsightV1{
		SchemaVersion: StructuredInsightSchemaVersionV1,
		BodyMD:        "**变化**", WhatChanged: "价格下降",
		WhyItMatters: "降低成本", ImportanceReason: "直接影响单位经济性",
		Claims: []StructuredClaimV1{{
			Text: "下降 20%", Excerpt: "价格下降 20%",
			SourceRefs: []string{"source-1"},
		}},
	}
	draft := BriefDraftV1{
		SchemaVersion: BriefSchemaVersionV1,
		RunOutcomeID:  1, RunSnapshotID: 2, PushBatchID: 3,
		TenantID: 4, UserID: 5, TaskID: "task-a",
		GeneratedAt: time.Now(),
		Insights: []InsightV1{{
			ID: 6, RankPosition: 1, Title: "标题", BodyMD: "**变化**",
			SourceURL: "https://example.com/source", DiscoveredAt: time.Now(),
			Structured: structured,
		}},
	}
	brief, err := draft.Seal(7)
	if err != nil {
		t.Fatal(err)
	}
	structured.Claims[0].SourceRefs[0] = "mutated"
	if got := brief.Insights[0].Structured.Claims[0].SourceRefs[0]; got != "source-1" {
		t.Fatalf("sealed structured source mutated through caller alias: %q", got)
	}
	tampered := brief
	tampered.Insights = append([]InsightV1(nil), brief.Insights...)
	copyStructured := *brief.Insights[0].Structured
	copyStructured.WhatChanged = "另一变化"
	tampered.Insights[0].Structured = &copyStructured
	if err := tampered.Validate(); err == nil {
		t.Fatal("structured mutation did not invalidate Brief digest")
	}
}

func TestBriefV1StructuredBodyMustMatchCanonicalBody(t *testing.T) {
	draft := BriefDraftV1{
		SchemaVersion: BriefSchemaVersionV1,
		RunOutcomeID:  1, RunSnapshotID: 2, PushBatchID: 3,
		TenantID: 4, UserID: 5, TaskID: "task-a", GeneratedAt: time.Now(),
		Insights: []InsightV1{{
			ID: 6, RankPosition: 1, Title: "标题", BodyMD: "legacy body",
			SourceURL: "https://example.com/source", DiscoveredAt: time.Now(),
			Structured: &StructuredInsightV1{
				SchemaVersion: StructuredInsightSchemaVersionV1,
				BodyMD:        "different body",
			},
		}},
	}
	if _, err := draft.Seal(7); err == nil {
		t.Fatal("structured body drift was accepted")
	}
}

func TestStructuredInsightEvidenceBindsExactSourcesAndEveryRef(t *testing.T) {
	insight := StructuredInsightV1{
		SchemaVersion: StructuredInsightSchemaVersionV1,
		BodyMD:        "正文", WhatChanged: "变化",
		WhyItMatters: "原因", ImportanceReason: "依据",
		Claims: []StructuredClaimV1{{
			Text: "事实", Excerpt: "共同原文",
			SourceRefs: []string{"source-1", "source-2"},
		}},
	}
	sources := map[string]string{
		"source-1": "第一份共同原文证据",
		"source-2": "第二份共同原文证据",
	}
	sealed, err := SealStructuredInsightEvidenceV1(insight, sources)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.EvidenceDigest == "" ||
		ValidateStructuredInsightEvidenceV1(sealed, sources) != nil {
		t.Fatalf("sealed evidence = %+v", sealed)
	}
	tampered := map[string]string{
		"source-1": sources["source-1"],
		"source-2": "第二份没有该摘录",
	}
	if err := ValidateStructuredInsightEvidenceV1(
		sealed, tampered); err == nil {
		t.Fatal("one unmatched cited source was accepted")
	}
	if err := ValidateStructuredInsightEvidenceV1(
		sealed, map[string]string{"source-1": sources["source-1"]},
	); err == nil {
		t.Fatal("missing cited source was accepted")
	}
}

func TestGeneratedLegacyInsightJSONOmitsStructuredExtension(t *testing.T) {
	payload, err := json.Marshal(InsightV1{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "structured") {
		t.Fatalf("legacy Insight wire shape gained structured field: %s", payload)
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

func TestRunOutcomeClaimV1SealsOnlyWithStoreTimeAndMatchesSemantics(t *testing.T) {
	claim := RunOutcomeClaimV1{
		RunOutcomeMarkerV1: RunOutcomeMarkerV1{
			ID: 1, SchemaVersion: RunOutcomeSchemaVersionV1,
			RunSnapshotID: 2, TenantID: 3, UserID: 4, TaskID: "task-1",
		},
		Result: RunResultContent, SourceCoverage: RunCompletenessPartial,
		Processing: RunCompletenessComplete,
	}
	if err := claim.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	at := time.Date(2026, 7, 27, 12, 34, 56, 123456789, time.FixedZone("x", 3600))
	outcome, err := claim.SealAt(at)
	if err != nil {
		t.Fatalf("SealAt() error = %v", err)
	}
	if want := at.UTC().Truncate(time.Microsecond); !outcome.FinalizedAt.Equal(want) {
		t.Fatalf("FinalizedAt = %v, want %v", outcome.FinalizedAt, want)
	}
	if !claim.Matches(outcome) {
		t.Fatal("claim does not match its sealed outcome")
	}
	outcome.Processing = RunCompletenessPartial
	if claim.Matches(outcome) {
		t.Fatal("claim matched a different terminal semantic")
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

func TestObservedEventProvenanceV1SealsCanonicalEvidence(t *testing.T) {
	occurredAt := time.Date(
		2026, 7, 28, 8, 30, 0, 123456789, time.FixedZone("test", 8*60*60))
	first, err := SealObservedEventProvenanceV1(
		41,
		strings.Repeat("a", 64),
		strings.Repeat("b", 64),
		"model_release",
		"OpenAI models",
		occurredAt,
		json.RawMessage(`{"content_ids":[7,9],"subject":"OpenAI"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
	if first.OccurredAt.Location() != time.UTC ||
		first.OccurredAt.Nanosecond()%1000 != 0 {
		t.Fatalf("occurred_at not canonical: %s", first.OccurredAt)
	}
	reordered := json.RawMessage(
		"{\n \"subject\":\"OpenAI\", \"content_ids\" : [7,9] }")
	if !first.MatchesEvidenceJSON(reordered) {
		t.Fatal("canonical-equivalent evidence did not match")
	}
	if first.MatchesEvidenceJSON(
		json.RawMessage(`{"content_ids":[7],"subject":"OpenAI"}`),
	) {
		t.Fatal("evidence mutation matched sealed provenance")
	}
}

func TestObservedEventProvenanceV1RejectsInvalidInputs(t *testing.T) {
	valid := func() ObservedEventProvenanceV1 {
		provenance, err := SealObservedEventProvenanceV1(
			1,
			strings.Repeat("a", 64),
			strings.Repeat("b", 64),
			"release",
			"subject",
			time.Now(),
			json.RawMessage(`{"content_ids":[1]}`),
		)
		if err != nil {
			t.Fatal(err)
		}
		return provenance
	}
	cases := map[string]func(*ObservedEventProvenanceV1){
		"zero id": func(p *ObservedEventProvenanceV1) {
			p.ID = 0
		},
		"bad schema": func(p *ObservedEventProvenanceV1) {
			p.SchemaVersion = "vane.observed-event-provenance/v999"
		},
		"bad policy digest": func(p *ObservedEventProvenanceV1) {
			p.PolicyDigest = strings.Repeat("A", 64)
		},
		"bad event key": func(p *ObservedEventProvenanceV1) {
			p.EventKey = "bad"
		},
		"empty subject": func(p *ObservedEventProvenanceV1) {
			p.Subject = ""
		},
		"noncanonical time": func(p *ObservedEventProvenanceV1) {
			p.OccurredAt = p.OccurredAt.Add(time.Nanosecond)
		},
		"bad evidence digest": func(p *ObservedEventProvenanceV1) {
			p.EvidenceDigest = "bad"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := valid()
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid provenance was accepted")
			}
		})
	}
	for _, raw := range []json.RawMessage{
		nil,
		json.RawMessage(`{"content_ids":[1]} trailing`),
		json.RawMessage{0xff},
	} {
		if _, err := SealObservedEventProvenanceV1(
			1,
			strings.Repeat("a", 64),
			strings.Repeat("b", 64),
			"release",
			"subject",
			time.Now(),
			raw,
		); err == nil {
			t.Fatalf("invalid evidence %q was accepted", raw)
		}
	}
}
