package feedback

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestCanonicalInsightBodyMDV1StructuredProjection(t *testing.T) {
	insight := types.InsightV1{
		BodyMD: "legacy fallback",
		Structured: &types.StructuredInsightV1{
			SchemaVersion:    types.StructuredInsightSchemaVersionV1,
			BodyMD:           "legacy fallback",
			WhatChanged:      "产品发布了新版本。",
			WhyItMatters:     "命中当前任务关注范围。",
			ImportanceReason: "原文明确列出了新增能力。",
			Claims: []types.StructuredClaimV1{{
				Text:       "发布新版本",
				Excerpt:    "发布新版本",
				SourceRefs: []string{"source-1"},
			}},
		},
	}
	const want = "**发生了什么**\n产品发布了新版本。" +
		"\n\n**为什么重要**\n命中当前任务关注范围。" +
		"\n\n**重要性依据**\n原文明确列出了新增能力。"
	if got := CanonicalInsightBodyMDV1(insight); got != want {
		t.Fatalf("structured projection = %q, want %q", got, want)
	}
}

func TestCanonicalInsightBodyMDV1Fallbacks(t *testing.T) {
	tests := []struct {
		name       string
		structured *types.StructuredInsightV1
	}{
		{name: "legacy"},
		{name: "body only", structured: &types.StructuredInsightV1{
			SchemaVersion: types.StructuredInsightSchemaVersionV1,
			BodyMD:        "fallback",
		}},
		{name: "body mismatch", structured: &types.StructuredInsightV1{
			SchemaVersion:    types.StructuredInsightSchemaVersionV1,
			BodyMD:           "different",
			WhatChanged:      "changed",
			WhyItMatters:     "matters",
			ImportanceReason: "reason",
		}},
		{name: "invalid partial trio", structured: &types.StructuredInsightV1{
			SchemaVersion: types.StructuredInsightSchemaVersionV1,
			BodyMD:        "fallback",
			WhatChanged:   "changed",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			insight := types.InsightV1{
				BodyMD: "fallback", Structured: test.structured,
			}
			if got := CanonicalInsightBodyMDV1(insight); got != "fallback" {
				t.Fatalf("fallback projection = %q", got)
			}
		})
	}
}

func TestCanonicalInsightEvidenceSourcesV1(t *testing.T) {
	structured, err := types.SealStructuredInsightEvidenceV1(
		types.StructuredInsightV1{
			SchemaVersion:    types.StructuredInsightSchemaVersionV1,
			BodyMD:           "body",
			WhatChanged:      "change",
			WhyItMatters:     "relevance",
			ImportanceReason: "reason",
			Claims: []types.StructuredClaimV1{{
				Text: "claim", Excerpt: "shared excerpt",
				SourceRefs: []string{"source-2", "source-1"},
			}},
		},
		map[string]string{
			"source-1": "first shared excerpt",
			"source-2": "second shared excerpt",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := types.SealObservedEventProvenanceV1(
		5, strings.Repeat("a", 64), strings.Repeat("b", 64),
		"release", "subject", time.Date(
			2026, 7, 28, 12, 0, 0, 0, time.UTC),
		json.RawMessage(`{"evidence_content_ids":[1,2]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	published := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	insight := types.InsightV1{
		Structured: &structured,
		EventEvidence: &types.StructuredEventEvidenceV1{
			SchemaVersion:  types.StructuredEventEvidenceSchemaVersionV1,
			Provenance:     provenance,
			EvidenceDigest: structured.EvidenceDigest,
			Sources: []types.StructuredEvidenceSourceV1{
				{
					Ref: "source-1", Title: "first",
					SourceTitle: "one", Platform: "web",
					SourceURL:   "https://example.com/first",
					PublishedAt: &published, DiscoveredAt: published,
				},
				{
					Ref: "source-2", Title: "second",
					SourceTitle: "two", Platform: "rss",
					SourceURL:    "https://example.com/second",
					DiscoveredAt: published.Add(time.Hour),
				},
			},
		},
	}
	sources := CanonicalInsightEvidenceSourcesV1(insight)
	if len(sources) != 2 ||
		sources[0].Ref != "source-1" ||
		sources[1].Ref != "source-2" ||
		sources[0].PublishedAt == nil ||
		!sources[0].PublishedAt.Equal(published) {
		t.Fatalf("canonical evidence sources = %+v", sources)
	}

	unresolved := structured
	unresolved.Claims = append(
		[]types.StructuredClaimV1(nil), structured.Claims...)
	unresolved.Claims[0].SourceRefs = []string{"source-3"}
	insight.Structured = &unresolved
	if sources := CanonicalInsightEvidenceSourcesV1(insight); sources != nil {
		t.Fatalf("unresolved evidence sources = %+v", sources)
	}
	if sources := CanonicalInsightEvidenceSourcesV1(
		types.InsightV1{}); sources != nil {
		t.Fatalf("legacy evidence sources = %+v", sources)
	}
}
