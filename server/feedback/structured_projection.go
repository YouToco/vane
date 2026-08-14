package feedback

import (
	"strings"

	"github.com/YouToco/vane/server/types"
)

// CanonicalInsightBodyMDV1 projects one immutable Brief insight into the
// Feishu body used by both initial delivery and callback rebuilds. Structured
// claims remain available in the Web projection; the compact Feishu prefix
// shows the three user-facing fields and keeps the source link alongside them.
//
// Invalid or body-only extensions fail closed to the already-validated
// backwards-compatible BodyMD. Readers must keep this fallback after rollout
// is disabled because previously frozen structured Briefs remain immutable.
func CanonicalInsightBodyMDV1(insight types.InsightV1) string {
	structured := insight.Structured
	if structured == nil || structured.Validate() != nil ||
		structured.BodyMD != insight.BodyMD ||
		structured.WhatChanged == "" ||
		structured.WhyItMatters == "" ||
		structured.ImportanceReason == "" {
		return insight.BodyMD
	}

	var body strings.Builder
	body.Grow(
		len(structured.WhatChanged) +
			len(structured.WhyItMatters) +
			len(structured.ImportanceReason) + 64,
	)
	body.WriteString("**发生了什么**\n")
	body.WriteString(structured.WhatChanged)
	body.WriteString("\n\n**为什么重要**\n")
	body.WriteString(structured.WhyItMatters)
	body.WriteString("\n\n**重要性依据**\n")
	body.WriteString(structured.ImportanceReason)
	return body.String()
}

// CanonicalInsightEvidenceSourcesV1 exposes only the ordered inventory-owned
// source presentation already sealed in the Brief. Invalid optional evidence
// fails closed to the legacy single-source card/API presentation.
func CanonicalInsightEvidenceSourcesV1(
	insight types.InsightV1,
) []CanonicalEvidenceSourceV1 {
	sources, err := types.ProjectInsightEvidenceSourcesV1(insight)
	if err != nil {
		return nil
	}
	return sources
}
