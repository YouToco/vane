package feedback

import (
	"testing"

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
