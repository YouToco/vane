package cardgen

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
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
		"unknown field":   strings.Replace(valid, `"claims":[]`, `"claims":[],"extra":true`, 1),
		"duplicate field": strings.Replace(valid, `"body_md":"正文"`, `"body_md":"正文","body_md":"替换"`, 1),
		"trailing object": valid + `{}`,
		"bad schema":      strings.Replace(valid, StructuredInsightSchemaV1, "vane.cardgen-insight/v999", 1),
		"schema wrong type": strings.Replace(
			valid, `"schema_version":"vane.cardgen-insight/v1"`, `"schema_version":1`, 1),
		"empty body": strings.Replace(valid, `"body_md":"正文"`, `"body_md":""`, 1),
		"body wrong type": strings.Replace(
			valid, `"body_md":"正文"`, `"body_md":{}`, 1),
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

func TestParseStructuredInsightV1FallsBackToValidBody(t *testing.T) {
	valid := `{"schema_version":"vane.cardgen-insight/v1","body_md":"安全正文","what_changed":"变化","why_it_matters":"原因","importance_reason":"依据","claims":[]}`
	tests := map[string]string{
		"partial projection": strings.Replace(
			valid, `"why_it_matters":"原因"`, `"why_it_matters":""`, 1),
		"forged ref": strings.Replace(valid, `"claims":[]`,
			`"claims":[{"text":"主张","excerpt":"原文","source_refs":["forged"]}]`, 1),
		"excerpt mismatch": strings.Replace(valid, `"claims":[]`,
			`"claims":[{"text":"主张","excerpt":"不存在","source_refs":["source-1"]}]`, 1),
		"optional number": strings.Replace(
			valid, `"what_changed":"变化"`, `"what_changed":123`, 1),
		"optional object": strings.Replace(
			valid, `"why_it_matters":"原因"`, `"why_it_matters":{}`, 1),
		"optional array": strings.Replace(
			valid, `"importance_reason":"依据"`, `"importance_reason":[]`, 1),
		"claims wrong type": strings.Replace(
			valid, `"claims":[]`, `"claims":"invalid"`, 1),
		"claim wrong type": strings.Replace(
			valid, `"claims":[]`, `"claims":[123]`, 1),
		"claim inner wrong type": strings.Replace(
			valid, `"claims":[]`,
			`"claims":[{"text":"主张","excerpt":"原文","source_refs":"source-1"}]`, 1),
		"claim unknown field": strings.Replace(
			valid, `"claims":[]`,
			`"claims":[{"text":"主张","excerpt":"原文","source_refs":["source-1"],"extra":true}]`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseStructuredInsightV1(
				[]byte(raw), map[string]string{"source-1": "原文内容"})
			if err != nil {
				t.Fatal(err)
			}
			if got.BodyMD != "安全正文" || got.WhatChanged != "" ||
				got.WhyItMatters != "" || got.ImportanceReason != "" ||
				len(got.Claims) != 0 || got.EvidenceDigest == "" {
				t.Fatalf("unsafe projection did not fall back: %+v", got)
			}
		})
	}
}

func TestParseStructuredInsightV1AllowsBodyOnlyFallback(t *testing.T) {
	tests := map[string]string{
		"empty": `{"schema_version":"vane.cardgen-insight/v1","body_md":"安全正文","what_changed":"","why_it_matters":"","importance_reason":"","claims":[]}`,
		"null":  `{"schema_version":"vane.cardgen-insight/v1","body_md":"安全正文","what_changed":null,"why_it_matters":null,"importance_reason":null,"claims":null}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseStructuredInsightV1(
				[]byte(raw), map[string]string{"source-1": "正文"})
			if err != nil {
				t.Fatal(err)
			}
			if got.BodyMD != "安全正文" || got.WhatChanged != "" ||
				got.EvidenceDigest == "" {
				t.Fatalf("unexpected fallback: %+v", got)
			}
		})
	}
}

func TestGenerateStructuredWithPolicyV2UsesOneFrozenCall(t *testing.T) {
	prompts, models := validPolicyV1(t, true)
	prompts.CardGen = StructuredPromptStageV2()
	wantCall := StructuredModelCallV2("structured-model")
	for index := range models.Calls {
		if models.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			models.Calls[index] = wantCall
		}
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		t.Fatal(err)
	}
	reply := `{"schema_version":"vane.cardgen-insight/v1","body_md":"**降价**","what_changed":"价格下降","why_it_matters":"成本降低","importance_reason":"影响单位成本","claims":[{"text":"下降 20%","excerpt":"下降 20%","source_refs":["source-1"]}]}`
	cg, captured := newTestCardGen(t, http.StatusOK, reply, nil)
	got, err := cg.GenerateStructuredWithPolicyV2(
		t.Context(), 0, 1,
		types.ScoredItem{Item: types.ContentItem{
			ID: 7, Title: "价格公告", Content: "官方价格下降 20% 即日生效",
		}},
		"trace-structured", "只关注成本", policy, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.WhatChanged != "价格下降" || captured.callCount() != 1 {
		t.Fatalf("result=%+v calls=%d", got, captured.callCount())
	}
	system, user := captured.snapshot()
	if system != structuredSystemPromptV1 ||
		!strings.Contains(user, "来源标签：source-1\n标题：价格公告\n正文：官方价格下降 20% 即日生效") ||
		!strings.Contains(user, "只关注成本") {
		t.Fatalf("unexpected frozen prompts:\nsystem=%q\nuser=%q", system, user)
	}
	maxTokens, temperature, thinking := captured.paramsSnapshot()
	if maxTokens == nil || *maxTokens != 900 ||
		temperature == nil || *temperature != 0.2 || thinking != "disabled" {
		t.Fatalf("unexpected request params: max=%v temp=%v thinking=%q",
			maxTokens, temperature, thinking)
	}
}

func TestGenerateStructuredWithPolicyV2FallsBackWithoutRepairCall(t *testing.T) {
	prompts, models := validPolicyV1(t, false)
	prompts.CardGen = StructuredPromptStageV2()
	for index := range models.Calls {
		if models.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			models.Calls[index] = StructuredModelCallV2("structured-model")
		}
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		t.Fatal(err)
	}
	reply := `{"schema_version":"vane.cardgen-insight/v1","body_md":"正文","what_changed":"变化","why_it_matters":"原因","importance_reason":"依据","claims":[{"text":"伪造","excerpt":"不存在","source_refs":["source-1"]}]}`
	cg, captured := newTestCardGen(t, http.StatusOK, reply, nil)
	got, err := cg.GenerateStructuredWithPolicyV2(
		t.Context(), 0, 1,
		types.ScoredItem{Item: types.ContentItem{Title: "标题", Content: "真实正文"}},
		"trace-invalid", "", policy, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.BodyMD != "正文" || got.WhatChanged != "" ||
		len(got.Claims) != 0 || captured.callCount() != 1 {
		t.Fatalf("fallback=%+v calls=%d", got, captured.callCount())
	}
}

func TestGenerateStructuredWithPolicyV2DoesNotUseTitleAsEvidence(t *testing.T) {
	prompts, models := validPolicyV1(t, false)
	prompts.CardGen = StructuredPromptStageV2()
	for index := range models.Calls {
		if models.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			models.Calls[index] = StructuredModelCallV2("structured-model")
		}
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		t.Fatal(err)
	}
	reply := `{"schema_version":"vane.cardgen-insight/v1","body_md":"正文","what_changed":"收购","why_it_matters":"影响市场","importance_reason":"交易规模","claims":[{"text":"已收购","excerpt":"以3万亿美元收购Apple","source_refs":["source-1"]}]}`
	cg, _ := newTestCardGen(t, http.StatusOK, reply, nil)
	got, err := cg.GenerateStructuredWithPolicyV2(
		t.Context(), 0, 1,
		types.ScoredItem{Item: types.ContentItem{
			Title:   "OpenAI 以3万亿美元收购Apple",
			Content: "官方否认该传闻",
		}},
		"trace-title-evidence", "", policy, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.WhatChanged != "" || len(got.Claims) != 0 {
		t.Fatalf("title was admitted as factual evidence: %+v", got)
	}
}

func TestGenerateStructuredWithPolicyV2RejectsZeroPolicyBeforeCall(t *testing.T) {
	cg, captured := newTestCardGen(t, http.StatusOK, `{}`, nil)
	_, err := cg.GenerateStructuredWithPolicyV2(
		t.Context(), 0, 1, types.ScoredItem{}, "trace-zero", "",
		PolicyV2{}, nil,
	)
	if err == nil || captured.callCount() != 0 {
		t.Fatalf("err=%v calls=%d", err, captured.callCount())
	}
}

func TestGenerateStructuredWithPolicyV2RejectsEmptyEvidenceBeforeCall(
	t *testing.T,
) {
	prompts, models := validPolicyV1(t, false)
	prompts.CardGen = StructuredPromptStageV2()
	for index := range models.Calls {
		if models.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			models.Calls[index] = StructuredModelCallV2("structured-model")
		}
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{
		" \n\t ",
		strings.Repeat(" ", 800) + "正文",
	} {
		cg, captured := newTestCardGen(t, http.StatusOK, `{}`, nil)
		_, err = cg.GenerateStructuredWithPolicyV2(
			t.Context(), 0, 1,
			types.ScoredItem{Item: types.ContentItem{
				ID: 7, Title: "标题", Content: content,
			}},
			"trace-empty-evidence", "", policy, nil,
		)
		if err == nil || captured.callCount() != 0 {
			t.Fatalf("err=%v calls=%d", err, captured.callCount())
		}
	}
}

func TestGenerateStructuredWithEvidencePolicyV3UsesOneMultiSourceCall(
	t *testing.T,
) {
	prompts, models := validPolicyV1(t, true)
	prompts.CardGen = StructuredPromptStageV2()
	for index := range models.Calls {
		if models.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			models.Calls[index] = StructuredModelCallV2("structured-model")
		}
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 123456789, time.UTC)
	items := []types.ContentItem{
		{
			ID: 7, Title: "公告一", URL: "https://example.com/one",
			Content: "第一份共同证据确认发布", CreatedAt: now,
		},
		{
			ID: 8, Title: "公告二", URL: "https://example.com/two",
			Content: "第二份共同证据补充细节", CreatedAt: now.Add(time.Minute),
		},
	}
	sources := make([]EventEvidenceSourceV1, len(items))
	for index, item := range items {
		sources[index], err = NewEventEvidenceSourceV1(
			index, item, runcontext.SourceV1{
				SourceID: int64(index + 10), Platform: types.PlatformWeb,
				Title: "官方源",
			})
		if err != nil {
			t.Fatal(err)
		}
	}
	reply := `{"schema_version":"vane.cardgen-insight/v1","body_md":"**已发布**","what_changed":"正式发布","why_it_matters":"影响当前任务","importance_reason":"两份来源交叉确认","claims":[{"text":"已确认发布","excerpt":"共同证据","source_refs":["source-1","source-2"]}]}`
	cg, captured := newTestCardGen(t, http.StatusOK, reply, nil)
	got, err := cg.GenerateStructuredWithEvidencePolicyV3(
		t.Context(), 2, 3, types.ScoredItem{Item: items[0]},
		sources, "trace-event", "关注正式发布", policy, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.WhatChanged != "正式发布" || len(got.Claims) != 1 ||
		captured.callCount() != 1 {
		t.Fatalf("result=%+v calls=%d", got, captured.callCount())
	}
	system, user := captured.snapshot()
	if system != structuredEventEvidenceSystemPromptV1 ||
		!strings.Contains(system,
			"单一来源事实只能引用含有该 excerpt 的标签") ||
		!strings.Contains(system,
			"禁止为了表示交叉验证而加入不含该逐字 excerpt 的来源") ||
		!strings.Contains(user, "来源标签：source-1") ||
		!strings.Contains(user, "来源标签：source-2") ||
		strings.Contains(user, `"content_item_id"`) ||
		strings.Contains(user, "\n7\n") || strings.Contains(user, "\n8\n") {
		t.Fatalf("unexpected multi-source prompts:\nsystem=%q\nuser=%q",
			system, user)
	}
}

func TestGenerateStructuredWithEvidencePolicyV3RejectsInventoryBeforeCall(
	t *testing.T,
) {
	prompts, models := validPolicyV1(t, false)
	prompts.CardGen = StructuredPromptStageV2()
	for index := range models.Calls {
		if models.Calls[index].Stage == runtimepolicy.ModelStageCardGen {
			models.Calls[index] = StructuredModelCallV2("structured-model")
		}
	}
	policy, err := PreparePolicyV2(prompts, models)
	if err != nil {
		t.Fatal(err)
	}
	cg, captured := newTestCardGen(t, http.StatusOK, `{}`, nil)
	source := EventEvidenceSourceV1{
		ContentItemID: 7,
		Metadata: types.StructuredEvidenceSourceV1{
			Ref: "source-2", Title: "标题", Platform: "web",
			SourceURL:    "https://example.com/source",
			DiscoveredAt: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		},
		EvidenceText: "正文",
	}
	_, err = cg.GenerateStructuredWithEvidencePolicyV3(
		t.Context(), 2, 3,
		types.ScoredItem{Item: types.ContentItem{ID: 7}},
		[]EventEvidenceSourceV1{source}, "trace-invalid", "", policy, nil,
	)
	if err == nil || captured.callCount() != 0 {
		t.Fatalf("err=%v calls=%d", err, captured.callCount())
	}
}

func TestEventEvidenceSourceV1RejectsEmptyEvidence(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	_, err := NewEventEvidenceSourceV1(
		0,
		types.ContentItem{
			ID: 7, Title: "公告", URL: "https://example.com/one",
			Content: " \n\t ", CreatedAt: now,
		},
		runcontext.SourceV1{
			SourceID: 10, Platform: types.PlatformWeb, Title: "官方源",
		},
	)
	if err == nil {
		t.Fatal("expected empty normalized evidence to be rejected")
	}
}
