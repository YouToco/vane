package cardgen

import (
	"strings"
	"testing"
)

func TestParseStructuredInsightV1(t *testing.T) {
	raw := []byte(`{
		"schema_version":"vane.cardgen-insight/v1",
		"body_md":"**核心变化**",
		"what_changed":"价格下降",
		"why_it_matters":"影响当前成本",
		"importance_reason":"直接改变单位经济性",
		"claims":[{"text":"价格下降 20%","excerpt":"价格下降了 20%","source_refs":["source-1"]}]
	}`)
	got, err := ParseStructuredInsightV1(raw, map[string]string{
		"source-1": "公告称： 价格下降了   20% ，即日生效。",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.WhatChanged != "价格下降" || len(got.Claims) != 1 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestParseStructuredInsightV1RejectsUntrustedOutput(t *testing.T) {
	valid := `{"schema_version":"vane.cardgen-insight/v1","body_md":"正文","what_changed":"变化","why_it_matters":"原因","importance_reason":"依据","claims":[]}`
	tests := map[string]string{
		"unknown field":      strings.Replace(valid, `"claims":[]`, `"claims":[],"extra":true`, 1),
		"duplicate field":    strings.Replace(valid, `"body_md":"正文"`, `"body_md":"正文","body_md":"替换"`, 1),
		"trailing object":    valid + `{}`,
		"partial projection": strings.Replace(valid, `"why_it_matters":"原因"`, `"why_it_matters":""`, 1),
		"forged ref": strings.Replace(valid, `"claims":[]`,
			`"claims":[{"text":"主张","excerpt":"原文","source_refs":["forged"]}]`, 1),
		"excerpt mismatch": strings.Replace(valid, `"claims":[]`,
			`"claims":[{"text":"主张","excerpt":"不存在","source_refs":["source-1"]}]`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseStructuredInsightV1([]byte(raw), map[string]string{
				"source-1": "原文内容",
			}); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestParseStructuredInsightV1AllowsBodyOnlyFallback(t *testing.T) {
	raw := []byte(`{"schema_version":"vane.cardgen-insight/v1","body_md":"安全正文","what_changed":"","why_it_matters":"","importance_reason":"","claims":[]}`)
	got, err := ParseStructuredInsightV1(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.BodyMD != "安全正文" || got.WhatChanged != "" {
		t.Fatalf("unexpected fallback: %+v", got)
	}
}
