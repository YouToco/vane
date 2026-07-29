package taskstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/YouToco/vane/types"
)

const goldenApprovedDefinitionV1 = `{"schema_version":"vane.task-approved-definition/v1","tenant_id":7,"user_id":11,"task_id":"task-approved-v1","intent":"追踪 \"AI\" 🚀","nl_description":"每天 08:00\n推送","spec_json":{"cron":"0 8 * * *","tz":"Asia/Shanghai"},"scope_json":{"emoji":"🌬️","max_items":3},"playbook_content":"只选官方\t来源。","source_scope":"approved_plan","fetch_plan":{"sources":[{"platform":"web","capability":"search","title":"搜索: AI","url":"vane://web/search?q=AI","config":{"query":"AI"}}]},"strictness":"normal","sources":[{"source_id":1,"platform":"web","capability":"search","title":"搜索: AI","url":"vane://web/search?q=AI","config":{"query":"AI"}}],"execution_mode":"compiled","delivery_policy":"owner_feishu","budget_policy":"inherit_tenant_quota"}`

const goldenApprovedDefinitionV1Digest = "19fd5270220d915b793c53d8f0e8c1e8f5f9dfd1785553ffeaba34e4808a7e30"

func TestApprovedDefinitionV1_ExactCanonicalGolden(t *testing.T) {
	t.Parallel()
	input := validApprovedDefinitionInputV1()
	input.Intent = `追踪 "AI" 🚀`
	input.NLDescription = "每天 08:00\n推送"
	input.PlaybookContent = "只选官方\t来源。"
	input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 8 * * *"}`)
	input.ScopeJSON = json.RawMessage(`{"max_items":3,"emoji":"🌬️"}`)
	definition, err := BuildApprovedDefinitionV1(input)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != goldenApprovedDefinitionV1 {
		t.Fatalf("approved V1 canonical wire drifted:\n got: %s\nwant: %s",
			canonical, goldenApprovedDefinitionV1)
	}
	digest, err := DigestApprovedDefinitionV1(definition)
	if err != nil || digest != goldenApprovedDefinitionV1Digest {
		t.Fatalf("approved V1 digest=%q err=%v want=%q",
			digest, err, goldenApprovedDefinitionV1Digest)
	}
	decoded, err := DecodeApprovedDefinitionV1([]byte(goldenApprovedDefinitionV1))
	if err != nil {
		t.Fatalf("decode retained approved V1 golden: %v", err)
	}
	reencoded, err := EncodeApprovedDefinitionV1(decoded)
	if err != nil || string(reencoded) != goldenApprovedDefinitionV1 {
		t.Fatalf("retained approved V1 reencode=%s err=%v", reencoded, err)
	}
}

func TestApprovedDefinitionV1_CanonicalRoundTripAndDigest(t *testing.T) {
	t.Parallel()
	input := validApprovedDefinitionInputV1()
	input.SpecJSON = json.RawMessage(`{ "tz":"Asia/Shanghai", "cron":"0 8 * * *" }`)
	input.ScopeJSON = json.RawMessage(`{ "z":2, "a":1 }`)
	input.FetchPlan = json.RawMessage(`{
		"sources":[
			{"platform":"web","capability":"search","title":"搜索: Z","url":"vane://web/search?q=Z","config":{"query":"Z"}},
			{"platform":"web","capability":"search","title":"搜索: A","url":"vane://web/search?q=A","config":{ "query":"A" }}
		]
	}`)
	input.Sources = []ApprovedSourceV1{
		approvedSearchSourceV1(22, "Z"),
		approvedSearchSourceV1(11, "A"),
	}

	definition, err := BuildApprovedDefinitionV1(input)
	if err != nil {
		t.Fatalf("BuildApprovedDefinitionV1() error = %v", err)
	}
	if got := []string{definition.Sources[0].URL, definition.Sources[1].URL}; !slices.Equal(got, []string{"vane://web/search?q=A", "vane://web/search?q=Z"}) {
		t.Fatalf("canonical source order = %v", got)
	}
	var plan FetchPlanV1
	if err := json.Unmarshal(definition.FetchPlan, &plan); err != nil {
		t.Fatalf("decode normalized plan: %v", err)
	}
	if got := []string{plan.Sources[0].URL, plan.Sources[1].URL}; !slices.Equal(got, []string{"vane://web/search?q=Z", "vane://web/search?q=A"}) {
		t.Fatalf("approved plan order changed = %v", got)
	}

	canonical, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatalf("EncodeApprovedDefinitionV1() error = %v", err)
	}
	decoded, err := DecodeApprovedDefinitionV1(canonical)
	if err != nil {
		t.Fatalf("DecodeApprovedDefinitionV1() error = %v", err)
	}
	reencoded, err := EncodeApprovedDefinitionV1(decoded)
	if err != nil {
		t.Fatalf("re-encode error = %v", err)
	}
	if !bytes.Equal(canonical, reencoded) {
		t.Fatalf("canonical bytes drifted:\nfirst:  %s\nsecond: %s", canonical, reencoded)
	}
	if !bytes.Contains(canonical, []byte(`"spec_json":{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`)) ||
		!bytes.Contains(canonical, []byte(`"scope_json":{"a":1,"z":2}`)) {
		t.Fatalf("json islands were not canonicalized: %s", canonical)
	}

	digest, err := DigestApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatalf("DigestApprovedDefinitionV1() error = %v", err)
	}
	if len(digest) != 64 || strings.ToLower(digest) != digest {
		t.Fatalf("digest = %q", digest)
	}
	equivalentInput := input
	equivalentInput.SpecJSON = json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`)
	equivalentInput.ScopeJSON = json.RawMessage(`{"a":1,"z":2}`)
	equivalentInput.Sources = slices.Clone(input.Sources)
	slices.Reverse(equivalentInput.Sources)
	equivalent, err := BuildApprovedDefinitionV1(equivalentInput)
	if err != nil {
		t.Fatalf("build equivalent definition: %v", err)
	}
	equivalentDigest, err := DigestApprovedDefinitionV1(equivalent)
	if err != nil || equivalentDigest != digest {
		t.Fatalf("equivalent digest = %q, %v; want %q", equivalentDigest, err, digest)
	}

	// Caller-owned buffers and slices must not alias the built value.
	input.Sources[0].Title = "mutated"
	input.SpecJSON[2] = 'x'
	if definition.Sources[1].Title == "mutated" || !json.Valid(definition.SpecJSON) {
		t.Fatal("built definition aliases caller-owned input")
	}
}

func TestApprovedDefinitionV1_StrictReader(t *testing.T) {
	t.Parallel()
	definition, err := BuildApprovedDefinitionV1(validApprovedDefinitionInputV1())
	if err != nil {
		t.Fatal(err)
	}
	valid, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "unknown root field", payload: insertBeforeLastObjectByte(valid, `,"extra":true`)},
		{name: "null root", payload: []byte(`null`)},
		{name: "duplicate root field", payload: insertBeforeLastObjectByte(valid, `,"task_id":"other"`)},
		{name: "case folded root field", payload: bytes.Replace(valid, []byte(`"task_id"`), []byte(`"Task_ID"`), 1)},
		{name: "missing root field", payload: bytes.Replace(valid, []byte(`,"budget_policy":"inherit_tenant_quota"`), nil, 1)},
		{name: "null sources", payload: replaceJSONFieldValue(valid, "sources", `null`)},
		{name: "null spec", payload: replaceJSONFieldValue(valid, "spec_json", `null`)},
		{name: "unpaired surrogate", payload: replaceJSONFieldValue(valid, "intent", `"\ud800"`)},
		{name: "escaped line separator", payload: bytes.Replace(valid, []byte(`"task_id":"task-approved-v1"`), []byte(`"task_id":"task\u2028approved"`), 1)},
		{name: "duplicate nested config", payload: bytes.Replace(valid, []byte(`"config":{"query":"AI"}`), []byte(`"config":{"query":"AI","query":"other"}`), 1)},
		{name: "unknown nested plan field", payload: bytes.Replace(valid, []byte(`"config":{"query":"AI"}`), []byte(`"config":{"query":"AI"},"write":true`), 1)},
		{name: "missing nested plan field", payload: bytes.Replace(valid, []byte(`"title":"搜索: AI",`), nil, 1)},
		{name: "null plan source list", payload: replaceJSONFieldValue(valid, "fetch_plan", `{"sources":null}`)},
		{name: "trailing value", payload: append(slices.Clone(valid), []byte(` {}`)...)},
		{name: "invalid utf8", payload: append(slices.Clone(valid[:len(valid)-1]), 0xff, '}')},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeApprovedDefinitionV1(testCase.payload); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("DecodeApprovedDefinitionV1() error = %v", err)
			}
		})
	}
}

func TestApprovedDefinitionV1_SourceScopeAndWriterGate(t *testing.T) {
	t.Parallel()

	legacy := validApprovedDefinitionInputV1()
	legacy.SourceScope = SourceScopeLegacySubscriptions
	legacy.FetchPlan = json.RawMessage(` { } `)
	legacy.Sources = []ApprovedSourceV1{}
	if _, err := BuildApprovedDefinitionV1(legacy); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("current builder accepted reader-only legacy scope: %v", err)
	}

	current, err := BuildApprovedDefinitionV1(validApprovedDefinitionInputV1())
	if err != nil {
		t.Fatal(err)
	}
	historicalPayload, err := EncodeApprovedDefinitionV1(current)
	if err != nil {
		t.Fatal(err)
	}
	historicalPayload = bytes.Replace(
		historicalPayload,
		[]byte(`"source_scope":"approved_plan"`),
		[]byte(`"source_scope":"legacy_subscriptions"`),
		1,
	)
	historicalPayload = replaceJSONFieldValue(historicalPayload, "fetch_plan", `{}`)
	historicalPayload = replaceJSONFieldValue(historicalPayload, "sources", `[]`)
	historical, err := DecodeApprovedDefinitionV1(historicalPayload)
	if err != nil {
		t.Fatalf("retained reader rejected historical v1: %v", err)
	}
	if err := historical.Validate(); err != nil {
		t.Fatalf("frozen validator rejected historical v1: %v", err)
	}
	if err := ValidateApprovedDefinitionV1ForWrite(historical); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("current writer gate accepted reader-only historical v1: %v", err)
	}

	compiled, err := BuildApprovedDefinitionV1(validApprovedDefinitionInputV1())
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := EncodeApprovedDefinitionV1(compiled)
	if err != nil {
		t.Fatal(err)
	}
	discoverPayload := bytes.Replace(
		canonical,
		[]byte(`"execution_mode":"compiled"`),
		[]byte(`"execution_mode":"discover_at_run"`),
		1,
	)
	discover, err := DecodeApprovedDefinitionV1(discoverPayload)
	if err != nil {
		t.Fatalf("retained reader rejected discover_at_run: %v", err)
	}
	if err := discover.Validate(); err != nil {
		t.Fatalf("Validate() rejected discover_at_run: %v", err)
	}
	if _, err := EncodeApprovedDefinitionV1(discover); err != nil {
		t.Fatalf("frozen encoder rejected discover_at_run: %v", err)
	}
	if err := ValidateApprovedDefinitionV1ForWrite(discover); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("current writer gate accepted discover_at_run: %v", err)
	}
	input := validApprovedDefinitionInputV1()
	input.ExecutionMode = types.ExecutionModeDiscoverAtRun
	if _, err := BuildApprovedDefinitionV1(input); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("current builder created discover_at_run: %v", err)
	}
}

func TestApprovedDefinitionV1_FrozenReaderDoesNotCallCurrentSourceRegistry(t *testing.T) {
	t.Parallel()
	definition, err := BuildApprovedDefinitionV1(validApprovedDefinitionInputV1())
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}
	retainedFutureBytes := bytes.ReplaceAll(
		canonical,
		[]byte(`"config":{"query":"AI"}`),
		[]byte(`"config":{"future_option":true,"query":"AI"}`),
	)
	decoded, err := DecodeApprovedDefinitionV1(retainedFutureBytes)
	if err != nil {
		t.Fatalf("frozen reader consulted current source registry: %v", err)
	}
	if _, err := EncodeApprovedDefinitionV1(decoded); err != nil {
		t.Fatalf("frozen encoder consulted current source registry: %v", err)
	}
	if err := ValidateApprovedDefinitionV1ForWrite(decoded); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("current writer gate accepted unknown source config: %v", err)
	}
}

func TestApprovedDefinitionV1_RejectsInvalidFieldsAndSourceDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ApprovedDefinitionInputV1)
	}{
		{name: "tenant missing", mutate: func(i *ApprovedDefinitionInputV1) { i.TenantID = 0 }},
		{name: "task control rune", mutate: func(i *ApprovedDefinitionInputV1) { i.TaskID = "task\nother" }},
		{name: "task line separator", mutate: func(i *ApprovedDefinitionInputV1) { i.TaskID = "task\u2028other" }},
		{name: "intent whitespace only", mutate: func(i *ApprovedDefinitionInputV1) { i.Intent = " \t" }},
		{name: "description missing", mutate: func(i *ApprovedDefinitionInputV1) { i.NLDescription = "" }},
		{name: "strictness invalid", mutate: func(i *ApprovedDefinitionInputV1) { i.Strictness = "extreme" }},
		{name: "unknown mode", mutate: func(i *ApprovedDefinitionInputV1) { i.ExecutionMode = types.ExecutionModeUnknown }},
		{name: "unknown delivery", mutate: func(i *ApprovedDefinitionInputV1) { i.DeliveryPolicy = "webhook" }},
		{name: "unknown budget", mutate: func(i *ApprovedDefinitionInputV1) { i.BudgetPolicy = "unlimited" }},
		{name: "unknown source scope", mutate: func(i *ApprovedDefinitionInputV1) { i.SourceScope = "subscriptions" }},
		{name: "spec is array", mutate: func(i *ApprovedDefinitionInputV1) { i.SpecJSON = json.RawMessage(`[]`) }},
		{name: "scope duplicate key", mutate: func(i *ApprovedDefinitionInputV1) { i.ScopeJSON = json.RawMessage(`{"a":1,"a":2}`) }},
		{name: "empty approved plan", mutate: func(i *ApprovedDefinitionInputV1) {
			i.FetchPlan = json.RawMessage(`{"sources":[]}`)
			i.Sources = []ApprovedSourceV1{}
		}},
		{name: "source id not positive", mutate: func(i *ApprovedDefinitionInputV1) { i.Sources[0].SourceID = 0 }},
		{name: "duplicate source id", mutate: func(i *ApprovedDefinitionInputV1) {
			i.Sources = append(i.Sources, approvedSearchSourceV1(1, "other"))
			i.FetchPlan = twoSearchPlanV1("AI", "other")
		}},
		{name: "duplicate plan url", mutate: func(i *ApprovedDefinitionInputV1) { i.FetchPlan = twoSearchPlanV1("AI", "AI") }},
		{name: "unregistered config field", mutate: func(i *ApprovedDefinitionInputV1) {
			i.FetchPlan = json.RawMessage(`{"sources":[{"platform":"web","capability":"search","title":"搜索: AI","url":"vane://web/search?q=AI","config":{"query":"AI","api_key":"secret"}}]}`)
			i.Sources[0].Config = json.RawMessage(`{"query":"AI","api_key":"secret"}`)
		}},
		{name: "identity config drift", mutate: func(i *ApprovedDefinitionInputV1) {
			i.FetchPlan = json.RawMessage(`{"sources":[{"platform":"web","capability":"search","title":"搜索: AI","url":"vane://web/search?q=AI","config":{"query":"other"}}]}`)
			i.Sources[0].Config = json.RawMessage(`{"query":"other"}`)
		}},
		{name: "plan and materialized source differ", mutate: func(i *ApprovedDefinitionInputV1) { i.Sources[0].Title = "different" }},
		{name: "source title paragraph separator", mutate: func(i *ApprovedDefinitionInputV1) {
			i.FetchPlan = json.RawMessage(`{"sources":[{"platform":"web","capability":"search","title":"搜索\u2029AI","url":"vane://web/search?q=AI","config":{"query":"AI"}}]}`)
			i.Sources[0].Title = "搜索\u2029AI"
		}},
		{name: "invalid utf8 intent", mutate: func(i *ApprovedDefinitionInputV1) { i.Intent = string([]byte{0xff}) }},
		{name: "oversize description", mutate: func(i *ApprovedDefinitionInputV1) { i.NLDescription = strings.Repeat("x", maxDescriptionBytes+1) }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			input := validApprovedDefinitionInputV1()
			testCase.mutate(&input)
			if _, err := BuildApprovedDefinitionV1(input); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("BuildApprovedDefinitionV1() error = %v", err)
			}
		})
	}
}

func TestApprovedDefinitionV1_NormalizesLegacyEmptyStrictness(t *testing.T) {
	t.Parallel()
	input := validApprovedDefinitionInputV1()
	input.Strictness = ""
	definition, err := BuildApprovedDefinitionV1(input)
	if err != nil {
		t.Fatalf("BuildApprovedDefinitionV1() error = %v", err)
	}
	if definition.Strictness != types.PushStrictness("loose") {
		t.Fatalf("strictness = %q, want frozen V1 loose", definition.Strictness)
	}
	payload, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}
	legacyPayload := bytes.Replace(
		payload,
		[]byte(`"strictness":"loose"`),
		[]byte(`"strictness":""`),
		1,
	)
	decoded, err := DecodeApprovedDefinitionV1(legacyPayload)
	if err != nil {
		t.Fatalf("DecodeApprovedDefinitionV1() error = %v", err)
	}
	if decoded.Strictness != types.PushStrictness("loose") {
		t.Fatalf("decoded strictness = %q, want frozen V1 loose", decoded.Strictness)
	}
}

func validApprovedDefinitionInputV1() ApprovedDefinitionInputV1 {
	return ApprovedDefinitionInputV1{
		TenantID:        7,
		UserID:          11,
		TaskID:          "task-approved-v1",
		Intent:          "监控 AI 官方动态",
		NLDescription:   "每天 08:00 推送 AI 官方动态",
		SpecJSON:        json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
		ScopeJSON:       json.RawMessage(`{}`),
		PlaybookContent: "只选官方来源。",
		SourceScope:     SourceScopeApprovedPlan,
		FetchPlan:       oneSearchPlanV1("AI"),
		Strictness:      types.StrictnessNormal,
		Sources:         []ApprovedSourceV1{approvedSearchSourceV1(1, "AI")},
		ExecutionMode:   types.ExecutionModeCompiled,
		DeliveryPolicy:  DeliveryPolicyOwnerFeishu,
		BudgetPolicy:    BudgetPolicyInheritTenantQuota,
	}
}

func approvedSearchSourceV1(sourceID int64, query string) ApprovedSourceV1 {
	return ApprovedSourceV1{
		SourceID: sourceID, Platform: types.PlatformWeb, Capability: types.CapSearch,
		Title: "搜索: " + query, URL: "vane://web/search?q=" + query,
		Config: json.RawMessage(`{"query":"` + query + `"}`),
	}
}

func oneSearchPlanV1(query string) json.RawMessage {
	return json.RawMessage(`{"sources":[{"platform":"web","capability":"search","title":"搜索: ` +
		query + `","url":"vane://web/search?q=` + query + `","config":{"query":"` + query + `"}}]}`)
}

func twoSearchPlanV1(first, second string) json.RawMessage {
	return json.RawMessage(`{"sources":[` + string(oneSearchPlanV1(first)[12:len(oneSearchPlanV1(first))-2]) + `,` +
		string(oneSearchPlanV1(second)[12:len(oneSearchPlanV1(second))-2]) + `]}`)
}

func insertBeforeLastObjectByte(payload []byte, suffix string) []byte {
	result := slices.Clone(payload[:len(payload)-1])
	result = append(result, suffix...)
	return append(result, '}')
}

func replaceJSONFieldValue(payload []byte, field, replacement string) []byte {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		panic(err)
	}
	object[field] = json.RawMessage(replacement)
	result, err := json.Marshal(object)
	if err != nil {
		panic(err)
	}
	return result
}

func FuzzDecodeApprovedDefinitionV1(f *testing.F) {
	input := validApprovedDefinitionInputV1()
	definition, err := BuildApprovedDefinitionV1(input)
	if err != nil {
		f.Fatal(err)
	}
	canonical, err := EncodeApprovedDefinitionV1(definition)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add([]byte(`null`))
	f.Add([]byte(`{"schema_version":"vane.task-approved-definition/v1"}`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		decoded, err := DecodeApprovedDefinitionV1(payload)
		if err != nil {
			return
		}
		if !utf8.Valid(payload) {
			t.Fatal("decoder accepted invalid utf8")
		}
		if err := decoded.Validate(); err != nil {
			t.Fatalf("decoded value failed validation: %v", err)
		}
	})
}
