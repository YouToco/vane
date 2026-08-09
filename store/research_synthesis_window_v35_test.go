package store

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func scopedResearchBriefFixtureV35(t *testing.T, result []byte) researchBriefFixtureV3 {
	t.Helper()
	scope := &taskstate.ResearchScopeV3{
		Mode:            taskstate.ResearchScopeEventWindowV3,
		LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3,
	}
	st := tenantTestStore(t)
	return newResearchBriefFixtureWithStoreWorkflowModelAndScopeV3(
		t, st, taskstate.NotificationThresholdMajorV3, true, result, "", "",
		testResearchGroundingModelPolicyV1(t), scope, 0)
}

func testResearchWindowV33(t *testing.T) researchScopeWindowV33 {
	t.Helper()
	end := time.Date(2026, 8, 9, 8, 45, 50, 329463000, time.UTC)
	return researchScopeWindowV33{
		Mode:            taskstate.ResearchScopeEventWindowV3,
		LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3,
		StartUTC:        end.Add(-7 * 24 * time.Hour).Format(time.RFC3339Nano),
		EndUTC:          end.Format(time.RFC3339Nano), Boundary: "(start,end]",
	}
}

func canonicalWindowDocumentsV33(t *testing.T, documents ...researchWindowDocumentV33) string {
	t.Helper()
	payload, err := json.Marshal(documents)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func TestProjectResearchWindowEvidenceV33UsesOpenStartClosedEnd(t *testing.T) {
	window := testResearchWindowV33(t)
	start, _ := time.Parse(time.RFC3339Nano, window.StartUTC)
	end, _ := time.Parse(time.RFC3339Nano, window.EndUTC)
	full := canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "at-start", URL: "https://example.com/start", PublishedAt: start.Format(time.RFC3339Nano), Text: "excluded"},
		researchWindowDocumentV33{Title: "after-start", URL: "https://example.com/after", PublishedAt: start.Add(time.Nanosecond).Format(time.RFC3339Nano), Text: "included"},
		researchWindowDocumentV33{Title: "at-end", URL: "https://example.com/end", PublishedAt: end.Format(time.RFC3339Nano), Text: "included"},
		researchWindowDocumentV33{Title: "after-end", URL: "https://example.com/future", PublishedAt: end.Add(time.Nanosecond).Format(time.RFC3339Nano), Text: "excluded"},
		researchWindowDocumentV33{Title: "missing", URL: "https://example.com/missing", Text: "excluded"},
	)
	filtered, eligible, err := projectResearchWindowEvidenceV33("web_search", full, window)
	if err != nil || !eligible || strings.Contains(filtered, "at-start") ||
		strings.Contains(filtered, "after-end") || strings.Contains(filtered, "missing") ||
		!strings.Contains(filtered, "after-start") || !strings.Contains(filtered, "at-end") {
		t.Fatalf("filtered=%s eligible=%v err=%v", filtered, eligible, err)
	}
}

func TestProjectResearchWindowEvidenceV33FailsClosedAndDoesNotParseNonWeb(t *testing.T) {
	window := testResearchWindowV33(t)
	for name, full := range map[string]string{
		"invalid json":    `[{"title":"x"`,
		"unknown field":   `[{"title":"x","url":"u","text":"x","published_at":"2026-08-09T00:00:00Z","extra":true}]`,
		"duplicate field": `[{"title":"x","title":"y","url":"u","text":"x","published_at":"2026-08-09T00:00:00Z"}]`,
		"non canonical":   `[ {"title":"x","url":"u","published_at":"2026-08-09T00:00:00Z","text":"x"} ]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := projectResearchWindowEvidenceV33("web_contents", full, window); err == nil {
				t.Fatal("tampered web Evidence was admitted")
			}
		})
	}
	if got, eligible, err := projectResearchWindowEvidenceV33("web_search",
		`[{"title":"x","url":"u","published_at":"not-time","text":"x"}]`, window); err != nil || eligible || got != "" {
		t.Fatalf("invalid document date was not excluded: got=%q eligible=%v err=%v", got, eligible, err)
	}
	if got, eligible, err := projectResearchWindowEvidenceV33(
		"web_product_status", `not-json`, window); err != nil || eligible || got != "" {
		t.Fatalf("non-web projection=%q eligible=%v err=%v", got, eligible, err)
	}
}

func TestProjectResearchWindowEvidenceV33FiltersBeforeContextBudget(t *testing.T) {
	window := testResearchWindowV33(t)
	start, _ := time.Parse(time.RFC3339Nano, window.StartUTC)
	end, _ := time.Parse(time.RFC3339Nano, window.EndUTC)
	full := canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "old", URL: "https://example.com/old", PublishedAt: start.Add(-time.Nanosecond).Format(time.RFC3339Nano), Text: strings.Repeat("o", 64<<10)},
		researchWindowDocumentV33{Title: "new", URL: "https://example.com/new", PublishedAt: end.Format(time.RFC3339Nano), Text: strings.Repeat("新", 8<<10)},
	)
	filtered, eligible, err := projectResearchWindowEvidenceV33("web_search", full, window)
	if err != nil || !eligible || strings.Contains(filtered, "old") {
		t.Fatalf("filtered-before-budget failed: bytes=%d eligible=%v err=%v", len(filtered), eligible, err)
	}
	digest := researchRunSHA256([]byte(full))
	items := []researchEvidenceContextItemV3{{
		researchEvidenceManifestItemV3: researchEvidenceManifestItemV3{ResultDigest: digest},
		SynthesisVisibleText:           filtered, ContextStoredSize: len(filtered),
	}}
	if err := projectResearchEvidenceContextV32(items); err != nil ||
		items[0].ResultDigest != digest || !items[0].ContextTruncated ||
		items[0].ContextVisibleDigest != researchRunSHA256([]byte(items[0].SynthesisVisibleText)) {
		t.Fatalf("projection=%+v err=%v", items[0], err)
	}
}

func TestValidateResearchBriefCitationsV33UsesEligibleContextIDs(t *testing.T) {
	contextPayload, _ := json.Marshal(researchSynthesisContextV3{
		SchemaVersion:       researchSynthesisContextSchemaV33,
		ResearchScopeWindow: func() *researchScopeWindowV33 { value := testResearchWindowV33(t); return &value }(),
		CurrentEvidence:     []researchEvidenceContextItemV3{{researchEvidenceManifestItemV3: researchEvidenceManifestItemV3{EvidenceID: 8}}},
		History:             researchHistoryContextV3{HistoryThroughUTC: testResearchWindowV33(t).EndUTC},
	})
	evidencePayload, _ := json.Marshal(researchEvidenceManifestV3{
		SchemaVersion: researchEvidenceManifestSchemaV3,
		Items:         []researchEvidenceManifestItemV3{{EvidenceID: 7}, {EvidenceID: 8}},
	})
	historyPayload, _ := json.Marshal(researchHistoryManifestV3{SchemaVersion: researchHistoryManifestSchemaV3})
	brief := types.ResearchBriefPayloadV3{Citations: []types.ResearchBriefCitationV3{{
		Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "7",
	}}}
	if err := validateResearchBriefCitationsV3(brief, contextPayload, evidencePayload, historyPayload); err == nil {
		t.Fatal("filtered Evidence id remained citeable")
	}
	brief.Citations[0].Ref = "8"
	if err := validateResearchBriefCitationsV3(brief, contextPayload, evidencePayload, historyPayload); err != nil {
		t.Fatalf("eligible Evidence id rejected: %v", err)
	}
}

func TestScopedResearchBriefV35FiltersFullEvidenceBeforeProjection(t *testing.T) {
	now := time.Now().UTC()
	result := []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "outside", URL: "https://example.com/old", PublishedAt: now.Add(-8 * 24 * time.Hour).Format(time.RFC3339Nano), Text: strings.Repeat("old", 20<<10)},
		researchWindowDocumentV33{Title: "inside <&>", URL: "https://example.com/new", PublishedAt: now.Add(-time.Hour).Format(time.RFC3339Nano), Text: "eligible fact\u2028line"},
		researchWindowDocumentV33{Title: "missing", URL: "https://example.com/missing", Text: "not eligible"},
	))
	f := scopedResearchBriefFixtureV35(t, result)
	seal, err := f.st.LoadResearchRunSnapshotV3(t.Context(), f.identity, f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	if seal.ResearchModel.Synthesis.RendererVersion != "research-synthesis.render/v3.5" {
		t.Fatalf("renderer=%q", seal.ResearchModel.Synthesis.RendererVersion)
	}
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var context researchSynthesisContextV3
	if err := json.Unmarshal(prepared.Synthesis.ContextPayload, &context); err != nil {
		t.Fatal(err)
	}
	if context.SchemaVersion != researchSynthesisContextSchemaV33 ||
		context.ResearchScopeWindow == nil || len(context.CurrentEvidence) != 1 {
		t.Fatalf("context=%+v", context)
	}
	visible := context.CurrentEvidence[0].SynthesisVisibleText
	if strings.Contains(visible, "outside") || strings.Contains(visible, "missing") ||
		!strings.Contains(visible, "inside") || !strings.Contains(visible, "eligible fact") {
		t.Fatalf("visible scoped Evidence=%q", visible)
	}
	var manifest researchEvidenceManifestV3
	if err := json.Unmarshal(prepared.Synthesis.EvidenceManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Items) != 1 || manifest.Items[0].ResultDigest != researchRunSHA256(result) ||
		context.CurrentEvidence[0].ResultDigest != manifest.Items[0].ResultDigest {
		t.Fatalf("manifest=%+v context=%+v", manifest, context.CurrentEvidence)
	}
	tampered := context
	tampered.CurrentEvidence[0].SynthesisVisibleText = "forged eligible fact"
	tamperedPayload, err := json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE research_brief_syntheses SET context_payload=$2 WHERE id=$1`,
		prepared.Synthesis.ID, tamperedPayload); err == nil {
		t.Fatal("database admitted forged scoped visible Evidence")
	}
}

func TestScopedResearchBriefV35RejectsExplicitlyTruncatedEvidence(t *testing.T) {
	now := time.Now().UTC()
	result := []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "inside", URL: "https://example.com/new", PublishedAt: now.Add(-time.Hour).Format(time.RFC3339Nano), Text: "eligible"},
	))
	scope := &taskstate.ResearchScopeV3{Mode: taskstate.ResearchScopeEventWindowV3,
		LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3}
	st := tenantTestStore(t)
	f := newResearchBriefFixtureWithStoreWorkflowModelAndScopeV3(
		t, st, taskstate.NotificationThresholdMajorV3, true, result, "", "",
		testResearchGroundingModelPolicyV1(t), scope, len(result)+1)
	if _, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f)); err == nil {
		t.Fatalf("truncated scoped prepare err=%v", err)
	}
	var count int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_brief_syntheses WHERE temporal_run_id=$1`,
		f.identity.TemporalRunID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("truncated synthesis count=%d err=%v", count, err)
	}
}

func TestScopedResearchBriefV35AllFilteredFailsBeforeSynthesisAndLLM(t *testing.T) {
	now := time.Now().UTC()
	result := []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "outside", URL: "https://example.com/old", PublishedAt: now.Add(-8 * 24 * time.Hour).Format(time.RFC3339Nano), Text: "old"},
		researchWindowDocumentV33{Title: "invalid", URL: "https://example.com/invalid", PublishedAt: "yesterday", Text: "invalid"},
	))
	f := scopedResearchBriefFixtureV35(t, result)
	if _, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f)); err == nil ||
		!strings.Contains(err.Error(), "no eligible in-window Evidence") {
		t.Fatalf("all-filtered prepare err=%v", err)
	}
	for table, query := range map[string]string{
		"synthesis": `SELECT count(*) FROM research_brief_syntheses WHERE temporal_run_id=$1`,
		"llm":       `SELECT count(*) FROM research_run_llm_spend_reservations WHERE run_snapshot_id=$1 AND stage='synthesis'`,
	} {
		var count int
		argument := any(f.identity.TemporalRunID)
		if table == "llm" {
			argument = f.snapshotRef.SnapshotID
		}
		if err := f.st.pool.QueryRow(t.Context(), query, argument).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
}

func TestUnscopedResearchBriefPreservesV34V32ReplayBytes(t *testing.T) {
	st := tenantTestStore(t)
	f := newResearchBriefFixtureWithStoreWorkflowAndModelV3(
		t, st, taskstate.NotificationThresholdMajorV3, true, nil, "", "",
		testResearchGroundingModelPolicyV1(t))
	seal, err := f.st.LoadResearchRunSnapshotV3(t.Context(), f.identity, f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	if seal.ResearchModel.Synthesis.RendererVersion != "research-synthesis.render/v3.4" {
		t.Fatalf("renderer=%q", seal.ResearchModel.Synthesis.RendererVersion)
	}
	first, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Synthesis.ContextPayload, second.Synthesis.ContextPayload) ||
		first.Synthesis.ContextDigest != second.Synthesis.ContextDigest {
		t.Fatal("unscoped context replay changed bytes")
	}
	var context researchSynthesisContextV3
	if err := json.Unmarshal(first.Synthesis.ContextPayload, &context); err != nil {
		t.Fatal(err)
	}
	if context.SchemaVersion != researchSynthesisContextSchemaV32 || context.ResearchScopeWindow != nil {
		t.Fatalf("unscoped context=%+v", context)
	}
}
