package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

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
