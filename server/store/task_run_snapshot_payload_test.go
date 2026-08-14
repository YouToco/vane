package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/observation"
)

const goldenTaskRunSnapshotPayloadV1 = `{"schema_version":"vane.task-run-snapshot-payload/v1","tenant_id":7,"user_id":11,"task_id":"golden-task","run_kind":"scheduled","mode":"compiled","adaptive_version":0,"policies":{"capability_catalog":{"capabilities":["web/search"]},"tool_policy":{"allow":["fetch"]},"prompt_policy":{"score":"v1"},"model_policy":{"model":"m1","provider":"test"},"quota_policy":{"bucket":"fetch","limit":7}},"budget":{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0},"definition":{"task_id":"golden-task","tenant_id":7,"user_id":11,"nl_description":"monitor status","spec_json":{"cron":"0 8 * * *","tz":"UTC"},"scope_json":{"max_items":3},"playbook_content":"trusted only","strictness":"normal","source_scope":"approved_plan","fetch_plan":{"sources":[{"platform":"web","capability":"search","title":"Official","url":"https://example.test/status","config":{"query":"status"}}]},"sources":[{"source_id":42,"platform":"web","capability":"search","title":"Official","url":"https://example.test/status","config":{"query":"status"}}]},"reference_schema_version":"vane.run-snapshot-ref/v1"}`

const goldenLegacyTaskRunSnapshotPayloadV1 = `{"schema_version":"vane.task-run-snapshot-payload/v1","tenant_id":13,"user_id":17,"task_id":"legacy-golden-task","run_kind":"scheduled","mode":"compiled","adaptive_version":0,"policies":{"capability_catalog":{"capabilities":["rss/read"]},"tool_policy":{"allow":["fetch"]},"prompt_policy":{"score":"legacy-v1"},"model_policy":{"model":"m0","provider":"test"},"quota_policy":{"bucket":"fetch","limit":3}},"budget":{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0},"definition":{"task_id":"legacy-golden-task","tenant_id":13,"user_id":17,"nl_description":"legacy monitor","spec_json":{"cron":"0 9 * * *"},"scope_json":{},"playbook_content":"legacy trusted","strictness":"strict","source_scope":"legacy_subscriptions","fetch_plan":{},"sources":[{"source_id":84,"platform":"rss","capability":"read","title":"Legacy official","url":"https://legacy.example.test/feed","config":{}}]},"reference_schema_version":"vane.run-snapshot-ref/v1"}`

// This is a hand-pinned v1 wire artifact, not an expected value produced by
// the current writer. Changing a field/tag/order without bumping and retaining
// the old schema reader must fail here before persisted BYTEA rows are shipped.
func TestTaskRunSnapshotPayloadV1Golden(t *testing.T) {
	const goldenSHA256 = "6d19f3dd9212b73c0cf26f724bddd41adfd927de4e21deffe76631f9435c4e31"
	const goldenDefinitionSHA256 = "1400efbc29b987bbe27c33b856fd8729bf66c713b0bd21edb717806eb72b400a"
	const goldenPlanSHA256 = "8f899709eebbe045adf15c92cac89ccdb3193fea7e4db450551ec03c375a2c92"
	wantPolicyDigests := taskRunPolicyDigestSet{
		CapabilityCatalog: "f94675f8557e3af71274b5add91448bc78ecbc7989338029e007ea02ba12429d",
		ToolPolicy:        "bbfd5ed1895d8b9c13eb95ac370375247506cf3ac5000fb0a67332104d6f4861",
		PromptPolicy:      "391a1a6bccfd0e793213783beb308973ff1e61923b0a4944e1c27d15282e5f67",
		ModelPolicy:       "75217606846875cc6bd5aa9de611328a4e3813213162cdb8be7274499257ba06",
		QuotaPolicy:       "761530d28b590fbf02a135756910b9047e5eb4439cc6930cb74723fceb9fa093",
	}

	decoded, err := readTaskRunSnapshotPayload([]byte(goldenTaskRunSnapshotPayloadV1))
	if err != nil {
		t.Fatalf("pinned reader must retain v1 compatibility: %v", err)
	}
	canonical := decoded.Canonical
	if string(canonical) != goldenTaskRunSnapshotPayloadV1 {
		t.Fatalf("v1 payload bytes drifted without a schema bump:\n got %s\nwant %s",
			canonical, goldenTaskRunSnapshotPayloadV1)
	}
	if got := sha256Hex(canonical); got != goldenSHA256 {
		t.Fatalf("v1 payload SHA drifted: got %s want %s", got, goldenSHA256)
	}
	if decoded.DefinitionDigest != goldenDefinitionSHA256 {
		t.Fatalf("v1 definition SHA drifted: got %s want %s",
			decoded.DefinitionDigest, goldenDefinitionSHA256)
	}
	if decoded.PlanDigest != goldenPlanSHA256 {
		t.Fatalf("v1 plan SHA drifted: got %s want %s", decoded.PlanDigest, goldenPlanSHA256)
	}
	if decoded.PolicyDigests != wantPolicyDigests {
		t.Fatalf("v1 policy digest domain drifted:\n got %+v\nwant %+v",
			decoded.PolicyDigests, wantPolicyDigests)
	}
}

func TestTaskRunSnapshotPayloadV1ObservationRolloutIsDigestBound(t *testing.T) {
	decoded, err := readTaskRunSnapshotPayload(
		[]byte(goldenTaskRunSnapshotPayloadV1))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Payload.ObservationRollout != "" {
		t.Fatalf("historical rollout = %q, want omitted/off",
			decoded.Payload.ObservationRollout)
	}
	oldDigest := sha256Hex(decoded.Canonical)
	decoded.Payload.ObservationRollout = string(observation.RolloutShadow)
	withRollout, err := canonicalizeTaskRunPayloadV1(decoded.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(withRollout.Canonical,
		[]byte(`"observation_rollout":"shadow"`)) {
		t.Fatalf("new rollout omitted from canonical payload: %s",
			withRollout.Canonical)
	}
	if got := sha256Hex(withRollout.Canonical); got == oldDigest {
		t.Fatal("observation rollout did not change payload digest")
	}
}

func TestTaskRunSnapshotPayloadV1RejectsPlanMaterializedURLMismatch(t *testing.T) {
	var payload *taskRunSnapshotPayloadV1
	if err := strictjson.DecodeExact([]byte(goldenTaskRunSnapshotPayloadV1), &payload); err != nil || payload == nil {
		t.Fatalf("decode pinned v1 fixture: payload=%+v err=%v", payload, err)
	}
	var plan *taskRunFetchPlanV1
	if err := strictjson.DecodeExact(payload.Definition.FetchPlan, &plan); err != nil || plan == nil {
		t.Fatalf("decode pinned v1 plan: plan=%+v err=%v", plan, err)
	}
	plan.Sources[0].URL = "https://different.example.test/status"
	var err error
	payload.Definition.FetchPlan, err = json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalizeTaskRunPayloadV1(payload); err == nil {
		t.Fatal("v1 reader accepted equal-count plan/materialized sources with different URLs")
	}
}

// Legacy rows use a different frozen definition digest domain. This fixture
// is hand-authored so changing either legacy semantics or any policy digest
// domain breaks compatibility visibly instead of regenerating the oracle.
func TestTaskRunSnapshotPayloadV1LegacyGolden(t *testing.T) {
	const goldenSHA256 = "ad88f2a2719df5a43601e8e555933da209d4f4ce0c7ff1fb88e2e11a86d913b8"
	const goldenDefinitionSHA256 = "f88cdf149ee8b98d6aa817a9125e5584ef280287edba6b63bfd45bdd2cdcc17f"
	const goldenPlanSHA256 = "fa6547312a82355a8ef1979393209d04aca27876abb53053b36f347f2e818f67"
	wantPolicyDigests := taskRunPolicyDigestSet{
		CapabilityCatalog: "1ee4f17ac8e9401b1c4789b74f75d7a9ce5a0443cc1db7a9b6bad92935d5d2eb",
		ToolPolicy:        "bbfd5ed1895d8b9c13eb95ac370375247506cf3ac5000fb0a67332104d6f4861",
		PromptPolicy:      "3ef5a43e3faafc4134f4f4c46677d48081c7deec298c82fd5ae53cc97c438886",
		ModelPolicy:       "d8c0df3f21070af217e232d70d91b231aefc615445ce76941c7aa504741fded6",
		QuotaPolicy:       "8cabb2333e0b86365b367c4df518de9efcfc902004a07fb671d03bd4d9069d7e",
	}

	decoded, err := readTaskRunSnapshotPayload([]byte(goldenLegacyTaskRunSnapshotPayloadV1))
	if err != nil {
		t.Fatalf("read hand-pinned legacy v1 payload: %v", err)
	}
	if string(decoded.Canonical) != goldenLegacyTaskRunSnapshotPayloadV1 {
		t.Fatalf("legacy v1 payload bytes drifted:\n got %s\nwant %s",
			decoded.Canonical, goldenLegacyTaskRunSnapshotPayloadV1)
	}
	gotSHA := sha256Hex(decoded.Canonical)
	if gotSHA != goldenSHA256 || decoded.DefinitionDigest != goldenDefinitionSHA256 ||
		decoded.PlanDigest != goldenPlanSHA256 || decoded.PolicyDigests != wantPolicyDigests {
		t.Fatalf("legacy v1 digest domain drifted:\n payload=%s\n definition=%s\n plan=%s\n policies=%+v",
			gotSHA, decoded.DefinitionDigest, decoded.PlanDigest, decoded.PolicyDigests)
	}
}

func TestTaskRunSnapshotPayloadV1LegalBoundaryCompatibility(t *testing.T) {
	jsonObjectAtSize := func(size int) json.RawMessage {
		const prefix = `{"pad":"`
		const suffix = `"}`
		if size < len(prefix)+len(suffix) {
			t.Fatalf("JSON object target %d is too small", size)
		}
		return json.RawMessage(prefix + strings.Repeat("a", size-len(prefix)-len(suffix)) + suffix)
	}
	build := func() *taskRunSnapshotPayloadV1 {
		planSources := make([]taskRunPlanSourceV1, maxTaskRunSourcesV1)
		identities := make([]taskRunSourceIdentityV1, maxTaskRunSourcesV1)
		for i := range maxTaskRunSourcesV1 {
			url := fmt.Sprintf("https://boundary-%03d.example.test/feed", i)
			planSources[i] = taskRunPlanSourceV1{
				Platform: "web", Capability: "feed", URL: url,
			}
			identities[i] = taskRunSourceIdentityV1{
				SourceID: int64(i + 1), Platform: "web", Capability: "feed",
				Title: "", URL: url, Config: json.RawMessage("{}"),
			}
		}
		plan, err := json.Marshal(taskRunFetchPlanV1{Sources: planSources})
		if err != nil {
			t.Fatalf("marshal boundary plan: %v", err)
		}
		budget := taskRunBudgetV1{}
		return &taskRunSnapshotPayloadV1{
			SchemaVersion: taskRunSnapshotPayloadSchemaV1,
			TenantID:      7,
			UserID:        11,
			TaskID:        strings.Repeat("t", maxTaskRunTaskIDBytesV1),
			RunKind:       "scheduled",
			Mode:          "compiled",
			Policies: &taskRunPolicyPayloadsV1{
				CapabilityCatalog: json.RawMessage(`{"capabilities":["web/feed"]}`),
				ToolPolicy:        json.RawMessage(`{"allow":[]}`),
				PromptPolicy:      json.RawMessage(`{"score":"v1"}`),
				ModelPolicy:       json.RawMessage(`{"model":"m1"}`),
				QuotaPolicy:       json.RawMessage(`{"bucket":"fetch"}`),
			},
			Budget: &budget,
			Definition: &taskRunDefinitionPayloadV1{
				TaskID:   strings.Repeat("t", maxTaskRunTaskIDBytesV1),
				TenantID: 7, UserID: 11,
				NLDescription:   "",
				SpecJSON:        json.RawMessage("{}"),
				ScopeJSON:       json.RawMessage("{}"),
				PlaybookContent: "",
				Strictness:      "",
				SourceScope:     taskRunApprovedSourceScopeV1,
				FetchPlan:       plan,
				Sources:         identities,
			},
			ReferenceSchemaVersion: taskRunReferenceSchemaV1,
		}
	}

	boundary := build()
	decoded, err := canonicalizeTaskRunPayloadV1(boundary)
	if err != nil {
		t.Fatalf("legal v1 boundary became unreadable: %v", err)
	}
	if len(decoded.Payload.Definition.Sources) != maxTaskRunSourcesV1 {
		t.Fatalf("source count = %d, want %d",
			len(decoded.Payload.Definition.Sources), maxTaskRunSourcesV1)
	}
	fetchPlan := string(decoded.Payload.Definition.FetchPlan)
	if strings.Contains(fetchPlan, `"title"`) ||
		!strings.Contains(fetchPlan, `"config":{}`) {
		t.Fatalf("empty-title/config v1 wire semantics drifted: %s", fetchPlan)
	}

	overSources := build()
	overSources.Definition.Sources = append(overSources.Definition.Sources,
		taskRunSourceIdentityV1{
			SourceID: 65, Platform: "web", Capability: "feed", Title: "",
			URL: "https://boundary-064.example.test/feed", Config: json.RawMessage("{}"),
		})
	var overPlan taskRunFetchPlanV1
	if err := strictjson.DecodeExact(overSources.Definition.FetchPlan, &overPlan); err != nil {
		t.Fatal(err)
	}
	overPlan.Sources = append(overPlan.Sources, taskRunPlanSourceV1{
		Platform: "web", Capability: "feed", URL: "https://boundary-064.example.test/feed",
	})
	overSources.Definition.FetchPlan, err = json.Marshal(overPlan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalizeTaskRunPayloadV1(overSources); err == nil {
		t.Fatal("v1 reader accepted 65 sources")
	}

	overTaskID := build()
	overTaskID.TaskID += "x"
	overTaskID.Definition.TaskID = overTaskID.TaskID
	if _, err := canonicalizeTaskRunPayloadV1(overTaskID); err == nil {
		t.Fatal("v1 reader accepted 256-byte task id")
	}

	singleSource := func() *taskRunSnapshotPayloadV1 {
		payload := build()
		payload.Definition.Sources = payload.Definition.Sources[:1]
		var plan taskRunFetchPlanV1
		if err := strictjson.DecodeExact(payload.Definition.FetchPlan, &plan); err != nil {
			t.Fatal(err)
		}
		plan.Sources = plan.Sources[:1]
		var err error
		payload.Definition.FetchPlan, err = json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}

	maxText := singleSource()
	maxText.TaskID = "boundary-text"
	maxText.Definition.TaskID = maxText.TaskID
	maxText.Definition.NLDescription = strings.Repeat("d", maxTaskRunDescriptionBytesV1)
	maxText.Definition.PlaybookContent = strings.Repeat("p", maxTaskRunPlaybookBytesV1)
	maxText.Definition.Sources[0].Platform = strings.Repeat("p", maxTaskRunSourceTextBytesV1)
	maxText.Definition.Sources[0].Capability = strings.Repeat("c", maxTaskRunSourceTextBytesV1)
	maxText.Definition.Sources[0].Title = strings.Repeat("t", maxTaskRunSourceTextBytesV1)
	maxText.Definition.Sources[0].URL = strings.Repeat("u", maxTaskRunSourceURLBytesV1)
	maxPlan := taskRunFetchPlanV1{Sources: []taskRunPlanSourceV1{{
		Platform:   maxText.Definition.Sources[0].Platform,
		Capability: maxText.Definition.Sources[0].Capability,
		Title:      maxText.Definition.Sources[0].Title,
		URL:        maxText.Definition.Sources[0].URL,
		Config:     json.RawMessage("{}"),
	}}}
	maxText.Definition.FetchPlan, err = json.Marshal(maxPlan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalizeTaskRunPayloadV1(maxText); err != nil {
		t.Fatalf("legal v1 text maxima became unreadable: %v", err)
	}

	overflowTextTests := []struct {
		name   string
		mutate func(*taskRunSnapshotPayloadV1)
	}{
		{"description", func(p *taskRunSnapshotPayloadV1) { p.Definition.NLDescription += "x" }},
		{"playbook", func(p *taskRunSnapshotPayloadV1) { p.Definition.PlaybookContent += "x" }},
		{"platform", func(p *taskRunSnapshotPayloadV1) {
			p.Definition.Sources[0].Platform += "x"
		}},
		{"capability", func(p *taskRunSnapshotPayloadV1) {
			p.Definition.Sources[0].Capability += "x"
		}},
		{"title", func(p *taskRunSnapshotPayloadV1) { p.Definition.Sources[0].Title += "x" }},
		{"url", func(p *taskRunSnapshotPayloadV1) { p.Definition.Sources[0].URL += "x" }},
	}
	for _, tt := range overflowTextTests {
		t.Run("max+1 "+tt.name, func(t *testing.T) {
			payload := singleSource()
			payload.TaskID = maxText.TaskID
			payload.Definition.TaskID = payload.TaskID
			payload.Definition.NLDescription = maxText.Definition.NLDescription
			payload.Definition.PlaybookContent = maxText.Definition.PlaybookContent
			payload.Definition.Sources = append([]taskRunSourceIdentityV1(nil),
				maxText.Definition.Sources...)
			payload.Definition.FetchPlan = append(json.RawMessage(nil),
				maxText.Definition.FetchPlan...)
			tt.mutate(payload)
			if _, err := canonicalizeTaskRunPayloadV1(payload); err == nil {
				t.Fatalf("v1 reader accepted %s at max+1", tt.name)
			}
		})
	}

	maxJSONObject := singleSource()
	maxJSONObject.TaskID = "boundary-json"
	maxJSONObject.Definition.TaskID = maxJSONObject.TaskID
	maxJSONObject.Definition.SpecJSON = jsonObjectAtSize(maxTaskRunJSONBytesV1)
	if _, err := canonicalizeTaskRunPayloadV1(maxJSONObject); err != nil {
		t.Fatalf("legal v1 JSON-object maximum became unreadable: %v", err)
	}
	overJSONObject := singleSource()
	overJSONObject.TaskID = "boundary-json-over"
	overJSONObject.Definition.TaskID = overJSONObject.TaskID
	overJSONObject.Definition.SpecJSON = jsonObjectAtSize(maxTaskRunJSONBytesV1 + 1)
	if _, err := canonicalizeTaskRunPayloadV1(overJSONObject); err == nil {
		t.Fatal("v1 reader accepted JSON object at max+1")
	}

	maxPlanPayload := singleSource()
	maxPlanPayload.TaskID = "boundary-plan"
	maxPlanPayload.Definition.TaskID = maxPlanPayload.TaskID
	planAtLimit := taskRunFetchPlanV1{Sources: []taskRunPlanSourceV1{{
		Platform: "web", Capability: "feed", URL: "https://plan-limit.example.test",
		Config: json.RawMessage("{}"),
	}}}
	basePlan, err := json.Marshal(planAtLimit)
	if err != nil {
		t.Fatal(err)
	}
	configSize := len(planAtLimit.Sources[0].Config) + maxTaskRunJSONBytesV1 - len(basePlan)
	planAtLimit.Sources[0].Config = jsonObjectAtSize(configSize)
	maxPlanPayload.Definition.Sources[0].URL = planAtLimit.Sources[0].URL
	maxPlanPayload.Definition.Sources[0].Config = append(json.RawMessage(nil),
		planAtLimit.Sources[0].Config...)
	maxPlanPayload.Definition.FetchPlan, err = json.Marshal(planAtLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(maxPlanPayload.Definition.FetchPlan) != maxTaskRunJSONBytesV1 {
		t.Fatalf("plan fixture length = %d, want %d",
			len(maxPlanPayload.Definition.FetchPlan), maxTaskRunJSONBytesV1)
	}
	if _, err := canonicalizeTaskRunPayloadV1(maxPlanPayload); err != nil {
		t.Fatalf("legal v1 fetch-plan maximum became unreadable: %v", err)
	}
	overPlanPayload := singleSource()
	overPlanPayload.TaskID = maxPlanPayload.TaskID
	overPlanPayload.Definition.TaskID = overPlanPayload.TaskID
	overPlanPayload.Definition.Sources = append([]taskRunSourceIdentityV1(nil),
		maxPlanPayload.Definition.Sources...)
	overLimitPlan := planAtLimit
	overLimitPlan.Sources = append([]taskRunPlanSourceV1(nil), planAtLimit.Sources...)
	overLimitPlan.Sources[0].Config = jsonObjectAtSize(configSize + 1)
	overPlanPayload.Definition.Sources[0].Config = append(json.RawMessage(nil),
		overLimitPlan.Sources[0].Config...)
	overPlanPayload.Definition.FetchPlan, err = json.Marshal(overLimitPlan)
	if err != nil {
		t.Fatal(err)
	}
	if len(overPlanPayload.Definition.FetchPlan) != maxTaskRunJSONBytesV1+1 {
		t.Fatalf("over-limit plan fixture length = %d, want %d",
			len(overPlanPayload.Definition.FetchPlan), maxTaskRunJSONBytesV1+1)
	}
	if _, err := canonicalizeTaskRunPayloadV1(overPlanPayload); err == nil {
		t.Fatal("v1 reader accepted fetch plan at max+1")
	}

	zeroLegacy := singleSource()
	zeroLegacy.TaskID = "legacy-zero-sources"
	zeroLegacy.Definition.TaskID = zeroLegacy.TaskID
	zeroLegacy.Definition.SourceScope = taskRunLegacySourceScopeV1
	zeroLegacy.Definition.FetchPlan = json.RawMessage("{}")
	zeroLegacy.Definition.Sources = []taskRunSourceIdentityV1{}
	zeroLegacyRead, err := canonicalizeTaskRunPayloadV1(zeroLegacy)
	if err != nil {
		t.Fatalf("legal zero-source legacy v1 payload became unreadable: %v", err)
	}
	if !bytes.Contains(zeroLegacyRead.Canonical, []byte(`"sources":[]`)) {
		t.Fatalf("legacy zero-source wire drifted: %s", zeroLegacyRead.Canonical)
	}

	// Build a fully valid canonical artifact exactly at the outer 2 MiB limit.
	// Four policy objects, two legacy source configs, and the text maxima leave
	// the fifth policy as a one-byte-adjustable pad, so no nested limit is bent.
	buildOuterLimit := func() *taskRunSnapshotPayloadV1 {
		budget := taskRunBudgetV1{}
		objectMax := jsonObjectAtSize(maxTaskRunJSONBytesV1)
		return &taskRunSnapshotPayloadV1{
			SchemaVersion: taskRunSnapshotPayloadSchemaV1,
			TenantID:      7, UserID: 11, TaskID: "outer-limit",
			RunKind: "scheduled", Mode: "compiled",
			Policies: &taskRunPolicyPayloadsV1{
				CapabilityCatalog: append(json.RawMessage(nil), objectMax...),
				ToolPolicy:        append(json.RawMessage(nil), objectMax...),
				PromptPolicy:      append(json.RawMessage(nil), objectMax...),
				ModelPolicy:       append(json.RawMessage(nil), objectMax...),
				QuotaPolicy:       json.RawMessage("{}"),
			},
			Budget: &budget,
			Definition: &taskRunDefinitionPayloadV1{
				TaskID: "outer-limit", TenantID: 7, UserID: 11,
				NLDescription: strings.Repeat("d", maxTaskRunDescriptionBytesV1),
				SpecJSON:      json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
				PlaybookContent: strings.Repeat("p", maxTaskRunPlaybookBytesV1),
				Strictness:      "", SourceScope: taskRunLegacySourceScopeV1,
				FetchPlan: json.RawMessage("{}"),
				Sources: []taskRunSourceIdentityV1{
					{SourceID: 1, Platform: "web", Capability: "feed", Title: "",
						URL: "https://outer-1.example.test", Config: append(json.RawMessage(nil), objectMax...)},
					{SourceID: 2, Platform: "web", Capability: "feed", Title: "",
						URL: "https://outer-2.example.test", Config: append(json.RawMessage(nil), objectMax...)},
				},
			},
			ReferenceSchemaVersion: taskRunReferenceSchemaV1,
		}
	}
	outer := buildOuterLimit()
	baseOuter, err := json.Marshal(outer)
	if err != nil {
		t.Fatal(err)
	}
	quotaSize := len(outer.Policies.QuotaPolicy) + maxTaskRunPayloadBytesV1 - len(baseOuter)
	if quotaSize <= 0 || quotaSize > maxTaskRunJSONBytesV1 {
		t.Fatalf("outer-limit quota pad = %d, want 1..%d", quotaSize, maxTaskRunJSONBytesV1)
	}
	outer.Policies.QuotaPolicy = jsonObjectAtSize(quotaSize)
	outerBytes, err := json.Marshal(outer)
	if err != nil {
		t.Fatal(err)
	}
	if len(outerBytes) != maxTaskRunPayloadBytesV1 {
		t.Fatalf("outer fixture length = %d, want %d", len(outerBytes), maxTaskRunPayloadBytesV1)
	}
	if _, err := readTaskRunSnapshotPayload(outerBytes); err != nil {
		t.Fatalf("legal v1 outer payload maximum became unreadable: %v", err)
	}
	if _, err := readTaskRunSnapshotPayload(append(outerBytes, ' ')); err == nil {
		t.Fatal("v1 reader accepted outer payload at max+1")
	}
}

func TestTaskRunSnapshotPayloadV1WireTagsArePinned(t *testing.T) {
	tests := []struct {
		typ  reflect.Type
		tags []string
	}{
		{reflect.TypeFor[taskRunBudgetV1](), []string{
			"max_planner_rounds", "max_tool_calls", "max_tokens", "max_cost_micro_usd", "duration_ms",
		}},
		{reflect.TypeFor[taskRunPolicyPayloadsV1](), []string{
			"capability_catalog", "tool_policy", "prompt_policy", "model_policy", "quota_policy",
		}},
		{reflect.TypeFor[taskRunPolicyDigestEnvelopeV1](), []string{"version", "kind", "payload"}},
		{reflect.TypeFor[taskRunSourceIdentityV1](), []string{
			"source_id", "platform", "capability", "title", "url", "config",
		}},
		{reflect.TypeFor[taskRunPlanSourceV1](), []string{
			"platform", "capability", "title,omitempty", "url", "config,omitempty",
		}},
		{reflect.TypeFor[taskRunFetchPlanV1](), []string{"sources"}},
		{reflect.TypeFor[taskRunDefinitionPayloadV1](), []string{
			"task_id", "tenant_id", "user_id", "nl_description", "spec_json", "scope_json",
			"playbook_content", "strictness", "source_scope", "fetch_plan", "sources",
		}},
		{reflect.TypeFor[taskRunSnapshotPayloadV1](), []string{
			"schema_version", "tenant_id", "user_id", "task_id", "run_kind", "mode",
			"adaptive_version", "observation_rollout,omitempty", "policies", "budget",
			"definition", "reference_schema_version",
		}},
		{reflect.TypeFor[taskRunApprovedDefinitionDigestEnvelopeV1](), []string{
			"version", "task_id", "tenant_id", "user_id", "nl_description", "spec_json",
			"scope_json", "playbook_content", "fetch_plan", "strictness",
		}},
		{reflect.TypeFor[taskRunLegacyDefinitionEnvelopeV1](), []string{"version", "definition"}},
		{reflect.TypeFor[taskRunPlanDigestEnvelopeV1](), []string{
			"version", "source_scope", "fetch_plan", "sources",
		}},
	}
	for _, tt := range tests {
		if tt.typ.NumField() != len(tt.tags) {
			t.Errorf("%s field count = %d, want %d", tt.typ, tt.typ.NumField(), len(tt.tags))
			continue
		}
		for i, want := range tt.tags {
			if got := tt.typ.Field(i).Tag.Get("json"); got != want {
				t.Errorf("%s.%s json tag = %q, want %q",
					tt.typ, tt.typ.Field(i).Name, got, want)
			}
		}
	}
}

func TestReadTaskRunSnapshotPayload_DispatchesAndRejectsWireDrift(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "pinned v1", raw: goldenTaskRunSnapshotPayloadV1, ok: true},
		{
			name: "unknown schema version",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"vane.task-run-snapshot-payload/v1"`,
				`"vane.task-run-snapshot-payload/v2"`, 1),
		},
		{
			name: "v2 top-level field under v1 version",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"tenant_id":7`, `"future_v2_field":true,"tenant_id":7`, 1),
		},
		{
			name: "v2 definition field under v1 version",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"task_id":"golden-task","tenant_id":7,"user_id":11,"nl_description"`,
				`"task_id":"golden-task","tenant_id":7,"user_id":11,"future_v2_field":true,"nl_description"`, 1),
		},
		{
			name: "v2 plan field under v1 version",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"fetch_plan":{"sources"`, `"fetch_plan":{"future_v2_field":true,"sources"`, 1),
		},
		{
			name: "duplicate top-level key",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"tenant_id":7`, `"tenant_id":7,"tenant_id":8`, 1),
		},
		{
			name: "case-folded top-level alias",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"tenant_id":7`, `"TENANT_ID":7`, 1),
		},
		{
			name: "escaped schema key alias",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"schema_version"`, `"\u0073chema_version"`, 1),
		},
		{
			name: "case-folded plan field alias",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"platform":"web"`, `"PLATFORM":"web"`, 1),
		},
		{
			name: "duplicate nested raw-message key",
			raw: strings.Replace(goldenTaskRunSnapshotPayloadV1,
				`"config":{"query":"status"}`,
				`"config":{"query":"status","query":"other"}`, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := readTaskRunSnapshotPayload([]byte(tt.raw))
			if tt.ok {
				if err != nil {
					t.Fatalf("read pinned v1: %v", err)
				}
				if decoded.Payload == nil || string(decoded.Canonical) != goldenTaskRunSnapshotPayloadV1 {
					t.Fatalf("unexpected decoded v1: %+v", decoded)
				}
				return
			}
			if err == nil {
				t.Fatalf("wire drift must fail closed: %+v", decoded)
			}
		})
	}
}

func TestTaskRunSnapshotPayloadV1ReaderIsPinnedByAST(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate payload test")
	}
	sourceFile := strings.TrimSuffix(thisFile, "_test.go") + ".go"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse payload reader: %v", err)
	}

	forbiddenIdentifiers := []string{
		"compiledFetchPlan",
		"validatePausedCompiledTaskDefinition",
		"DigestPausedCompiledTaskDefinition",
		"maxCompiledTaskJSONBytes",
		"maxCompiledTaskPlaybookBytes",
		"maxCompiledTaskDescriptionBytes",
		"maxCompiledTaskSources",
		"maxCompiledTaskSourceURLBytes",
		"maxCompiledTaskSourceTextBytes",
		"taskRunSnapshotPayloadVersion",
		"taskRunLegacyDefinitionVersion",
		"taskRunPlanDigestVersion",
		"taskRunPolicyDigestVersion",
	}
	expectedPinnedConstants := map[string]string{
		"taskRunSnapshotPayloadSchemaV1":    `"vane.task-run-snapshot-payload/v1"`,
		"taskRunReferenceSchemaV1":          `"vane.run-snapshot-ref/v1"`,
		"taskRunApprovedDefinitionDigestV1": `"vane.paused-compiled-task-definition/v1"`,
		"taskRunLegacyDefinitionDigestV1":   `"vane.task-run-legacy-definition/v1"`,
		"taskRunPlanDigestV1":               `"vane.task-run-execution-plan/v1"`,
		"taskRunPolicyDigestV1":             `"vane.runtime-policy-digest/v1"`,
		"taskRunApprovedSourceScopeV1":      `"approved_plan"`,
		"taskRunLegacySourceScopeV1":        `"legacy_subscriptions"`,
		"maxTaskRunPayloadBytesV1":          `2 << 20`,
		"maxTaskRunJSONBytesV1":             `256 << 10`,
		"maxTaskRunPlaybookBytesV1":         `256 << 10`,
		"maxTaskRunDescriptionBytesV1":      `16 << 10`,
		"maxTaskRunSourcesV1":               `64`,
		"maxTaskRunSourceURLBytesV1":        `4096`,
		"maxTaskRunSourceTextBytesV1":       `4096`,
		"maxTaskRunTaskIDBytesV1":           `255`,
	}
	seenPinnedConstants := make(map[string]bool, len(expectedPinnedConstants))
	slices.Sort(forbiddenIdentifiers)
	var violations []string
	inspect := func(root ast.Node) {
		ast.Inspect(root, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				if _, found := slices.BinarySearch(forbiddenIdentifiers, n.Name); found {
					pos := fset.Position(n.Pos())
					violations = append(violations, pos.String()+": "+n.Name)
				}
			case *ast.CallExpr:
				selector, isSelector := n.Fun.(*ast.SelectorExpr)
				if isSelector && selector.Sel.Name == "Valid" {
					pos := fset.Position(selector.Pos())
					violations = append(violations, pos.String()+": current Valid method")
				}
			}
			return true
		})
	}

	for _, declaration := range file.Decls {
		switch declaration := declaration.(type) {
		case *ast.FuncDecl:
			if declaration.Body != nil &&
				(declaration.Name.Name == "readTaskRunSnapshotPayload" ||
					strings.HasSuffix(declaration.Name.Name, "V1")) {
				inspect(declaration.Body)
			}
		case *ast.GenDecl:
			if declaration.Tok == token.CONST {
				for _, spec := range declaration.Specs {
					valueSpec, isValue := spec.(*ast.ValueSpec)
					if !isValue {
						continue
					}
					for i, name := range valueSpec.Names {
						want, pinned := expectedPinnedConstants[name.Name]
						if !pinned || i >= len(valueSpec.Values) {
							continue
						}
						var encoded bytes.Buffer
						if err := format.Node(&encoded, fset, valueSpec.Values[i]); err != nil {
							t.Fatalf("format pinned constant %s: %v", name.Name, err)
						}
						seenPinnedConstants[name.Name] = true
						if encoded.String() != want {
							pos := fset.Position(name.Pos())
							violations = append(violations,
								pos.String()+": "+name.Name+" = "+encoded.String()+", want "+want)
						}
					}
				}
			}
			for _, spec := range declaration.Specs {
				typeSpec, isType := spec.(*ast.TypeSpec)
				if isType && strings.HasSuffix(typeSpec.Name.Name, "V1") {
					if typeSpec.Assign.IsValid() {
						pos := fset.Position(typeSpec.Pos())
						violations = append(violations,
							pos.String()+": pinned v1 type must not be an alias")
					}
					inspect(typeSpec.Type)
				}
			}
		}
	}
	for name := range expectedPinnedConstants {
		if !seenPinnedConstants[name] {
			violations = append(violations, "missing pinned v1 constant: "+name)
		}
	}
	if len(violations) != 0 {
		t.Fatalf("pinned v1 reader depends on current schema rules: %v", violations)
	}
}
