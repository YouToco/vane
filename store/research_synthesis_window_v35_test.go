package store

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/runtimepolicy"
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
		testScopedResearchGroundingModelPolicyV36Base(t), scope, 0, nil)
}

func testScopedResearchGroundingModelPolicyV36Base(
	t *testing.T,
) runtimepolicy.ResearchModelPolicyV3 {
	t.Helper()
	model := testResearchGroundingModelPolicyV1(t)
	model.GroundingVerifier.RendererVersion =
		runtimepolicy.ResearchGroundingVerifierRendererVersionV12
	model, err := runtimepolicy.BuildResearchModelPolicyV3(model)
	if err != nil {
		t.Fatal(err)
	}
	return model
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
	if seal.ResearchModel.Synthesis.RendererVersion != "research-synthesis.render/v3.6" ||
		seal.ResearchModel.GroundingCorrector == nil {
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
	for _, boundary := range []string{"start", "end"} {
		t.Run("reject "+boundary+" plus 1ns", func(t *testing.T) {
			changed := context
			window := *context.ResearchScopeWindow
			value := window.StartUTC
			if boundary == "end" {
				value = window.EndUTC
			}
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "start" {
				window.StartUTC = parsed.Add(time.Nanosecond).Format(time.RFC3339Nano)
			} else {
				window.EndUTC = parsed.Add(time.Nanosecond).Format(time.RFC3339Nano)
			}
			changed.ResearchScopeWindow = &window
			payload, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.st.pool.Exec(t.Context(),
				`UPDATE research_brief_syntheses SET context_payload=$2 WHERE id=$1`,
				prepared.Synthesis.ID, payload); err == nil {
				t.Fatalf("database admitted %s+1ns window", boundary)
			}
		})
	}
	var withExtra map[string]any
	if err := json.Unmarshal(prepared.Synthesis.ContextPayload, &withExtra); err != nil {
		t.Fatal(err)
	}
	withExtra["research_scope_window"].(map[string]any)["untrusted"] = true
	extraPayload, err := json.Marshal(withExtra)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE research_brief_syntheses SET context_payload=$2 WHERE id=$1`,
		prepared.Synthesis.ID, extraPayload); err == nil {
		t.Fatal("database admitted extra research_scope_window key")
	}
	delete(withExtra["research_scope_window"].(map[string]any), "untrusted")
	withExtra["research_scope_window"].(map[string]any)["lookback_seconds"] = "604800"
	stringLookback, err := json.Marshal(withExtra)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE research_brief_syntheses SET context_payload=$2 WHERE id=$1`,
		prepared.Synthesis.ID, stringLookback); err == nil {
		t.Fatal("database admitted string research_scope_window lookback")
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
		testScopedResearchGroundingModelPolicyV36Base(t), scope, len(result)+1, nil)
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

func TestScopedResearchBriefV35GroundedFinalizationPostgres(t *testing.T) {
	now := time.Now().UTC()
	f := scopedResearchBriefFixtureV35(t, []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "inside", URL: "https://example.com/in",
			PublishedAt: now.Add(-time.Hour).Format(time.RFC3339Nano), Text: "eligible"},
	)))
	synthesis, handle, candidate, grounding := prepareGroundingCandidateV1(t, f)
	settled := settleGroundingVerifierV1(t, f, synthesis, handle, grounding,
		types.ResearchGroundingGroundedV1)
	ref, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        candidate, GroundingVerificationID: settled.ID,
		})
	if err != nil || !ref.DeliveryRequired {
		t.Fatalf("scoped finalization ref=%+v err=%v", ref, err)
	}
}

func TestScopedResearchBriefV35DatabaseRejectsFilteredCitationMutation(t *testing.T) {
	now := time.Now().UTC()
	oldResult := []byte(canonicalWindowDocumentsV33(t, researchWindowDocumentV33{
		Title: "filtered", URL: "https://example.com/old",
		PublishedAt: now.Add(-8 * 24 * time.Hour).Format(time.RFC3339Nano), Text: "old"},
	))
	eligibleResult := []byte(canonicalWindowDocumentsV33(t, researchWindowDocumentV33{
		Title: "eligible", URL: "https://example.com/new",
		PublishedAt: now.Add(-time.Hour).Format(time.RFC3339Nano), Text: "new"},
	))
	scope := &taskstate.ResearchScopeV3{Mode: taskstate.ResearchScopeEventWindowV3,
		LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3}
	st := tenantTestStore(t)
	f := newResearchBriefFixtureWithStoreWorkflowModelAndScopeV3(
		t, st, taskstate.NotificationThresholdMajorV3, true, oldResult, "", "",
		testScopedResearchGroundingModelPolicyV36Base(t), scope, 0, eligibleResult)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var manifest researchEvidenceManifestV3
	var context researchSynthesisContextV3
	if err := json.Unmarshal(prepared.Synthesis.EvidenceManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(prepared.Synthesis.ContextPayload, &context); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Items) != 2 || len(context.CurrentEvidence) != 1 ||
		manifest.Items[0].EvidenceID == context.CurrentEvidence[0].EvidenceID {
		t.Fatalf("manifest=%+v context=%+v", manifest.Items, context.CurrentEvidence)
	}
	eligibleID := context.CurrentEvidence[0].EvidenceID
	filteredID := manifest.Items[0].EvidenceID
	encodeCandidate := func(ref int64) []byte {
		payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
			SchemaVersion: types.ResearchBriefPayloadSchemaV3,
			Headline:      "eligible change", Summary: "eligible evidence only",
			Significance: types.ResearchBriefSignificanceMajorV3,
			Citations: []types.ResearchBriefCitationV3{{
				Kind: types.ResearchBriefCitationCurrentEvidenceV3,
				Ref:  strconv.FormatInt(ref, 10),
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	eligiblePayload := encodeCandidate(eligibleID)
	handle, reservation := claimResearchBriefWithPendingReceiptV3(t, f, prepared.Synthesis)
	settleResearchBriefReceiptV3(t, f, reservation, prepared.Synthesis, eligiblePayload)
	grounding, err := f.st.PrepareOrGetResearchBriefGroundingV1(t.Context(),
		PrepareResearchBriefGroundingV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			CandidateBriefPayload:               eligiblePayload,
		})
	if err != nil {
		t.Fatal(err)
	}
	settled := settleGroundingVerifierV1(t, f, prepared.Synthesis, handle,
		grounding.Grounding, types.ResearchGroundingGroundedV1)
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle, BriefPayload: eligiblePayload,
			GroundingVerificationID: settled.ID,
		}); err != nil {
		t.Fatalf("eligible citation did not finalize: %v", err)
	}
	tx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(), `ALTER TABLE research_brief_syntheses DISABLE TRIGGER USER;
		ALTER TABLE research_brief_syntheses ENABLE TRIGGER research_scope_window_v33`); err != nil {
		t.Fatal(err)
	}
	eligibleDigest := researchRunSHA256(eligiblePayload)
	if _, err := tx.Exec(t.Context(), `UPDATE research_brief_syntheses
		SET brief_payload=$2,brief_digest=$3 WHERE id=$1`, prepared.Synthesis.ID,
		eligiblePayload, eligibleDigest); err != nil {
		t.Fatalf("scope trigger rejected eligible finalized citation: %v", err)
	}
	filteredPayload := encodeCandidate(filteredID)
	if _, err := tx.Exec(t.Context(), `UPDATE research_brief_syntheses
		SET brief_payload=$2,brief_digest=$3 WHERE id=$1`, prepared.Synthesis.ID,
		filteredPayload, researchRunSHA256(filteredPayload)); err == nil ||
		!strings.Contains(err.Error(), "final Brief cites ineligible scoped Evidence") {
		t.Fatalf("filtered finalized citation mutation err=%v", err)
	}
}

func TestScopedResearchBriefV35DatabaseRejectsScopeSnapshotTampering(t *testing.T) {
	now := time.Now().UTC()
	f := scopedResearchBriefFixtureV35(t, []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "inside", URL: "https://example.com/in",
			PublishedAt: now.Add(-time.Hour).Format(time.RFC3339Nano), Text: "eligible"},
	)))
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var frozen []byte
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload FROM task_run_snapshots WHERE id=$1`, f.snapshotRef.SnapshotID).Scan(&frozen); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(map[string]any){
		"missing renderer": func(snapshot map[string]any) {
			delete(snapshot["research_model"].(map[string]any)["synthesis"].(map[string]any), "renderer_version")
		},
		"missing mode": func(snapshot map[string]any) {
			delete(snapshot["definition"].(map[string]any)["research_scope"].(map[string]any), "mode")
		},
		"string lookback": func(snapshot map[string]any) {
			snapshot["definition"].(map[string]any)["research_scope"].(map[string]any)["lookback_seconds"] = "604800"
		},
		"missing manual": func(snapshot map[string]any) {
			delete(snapshot["definition"].(map[string]any), "task_manual")
		},
		"missing digest": func(snapshot map[string]any) {
			delete(snapshot["definition"].(map[string]any)["research_scope"].(map[string]any), "task_manual_digest")
		},
		"tampered manual": func(snapshot map[string]any) {
			snapshot["definition"].(map[string]any)["task_manual"] = "different manual"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			var snapshot map[string]any
			if err := json.Unmarshal(frozen, &snapshot); err != nil {
				t.Fatal(err)
			}
			mutate(snapshot)
			payload, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := f.st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(t.Context()) }()
			if _, err := tx.Exec(t.Context(), `ALTER TABLE task_run_snapshots DISABLE TRIGGER USER;
				ALTER TABLE research_brief_syntheses DISABLE TRIGGER USER;
				ALTER TABLE research_brief_syntheses ENABLE TRIGGER research_scope_window_v33`); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(),
				`UPDATE task_run_snapshots SET payload=$2 WHERE id=$1`,
				f.snapshotRef.SnapshotID, payload); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(),
				`UPDATE research_brief_syntheses SET context_payload=context_payload WHERE id=$1`,
				prepared.Synthesis.ID); err == nil {
				t.Fatal("database admitted tampered scoped snapshot")
			}
		})
	}
}
