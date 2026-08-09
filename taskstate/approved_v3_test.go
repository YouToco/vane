package taskstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

func validApprovedDefinitionInputV3() ApprovedDefinitionInputV3 {
	return ApprovedDefinitionInputV3{
		TenantID: 7, UserID: 42, TaskID: "task-research-v3",
		TaskName:      "Kimi 套餐可购买状态",
		TaskManual:    "每周一 9:00 检查 Kimi 官方套餐页。以官方原文为主并交叉核验；和历史结论比较，没有重大更新不推送。",
		SpecJSON:      json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 9 * * 1"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: NotificationPolicyV3{
			MinimumSignificance: NotificationThresholdMajorV3,
			SuppressEmpty:       true,
		},
		Output: OutputPreferenceV3{
			Language: OutputLanguageZhCNV3, Format: OutputFormatExecutiveBriefV3,
			Instructions: "先写结论，再列官方依据和变化。", IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768,
			MaxCostMicroUSD: 1_000_000, DurationMs: 300_000,
		},
		DeliveryPolicy:     DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: BudgetPolicyInheritTenantQuota,
	}
}

func TestApprovedDefinitionV3RoundTripContainsOnlyDurableResearchIntent(t *testing.T) {
	definition, err := BuildApprovedDefinitionV3(validApprovedDefinitionInputV3())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(definition.SpecJSON); got != `{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}` {
		t.Fatalf("canonical spec=%s", got)
	}
	payload, err := EncodeApprovedDefinitionV3(definition)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeApprovedDefinitionV3(payload)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeApprovedDefinitionV3(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, reencoded) {
		t.Fatalf("round trip drift\nfirst=%s\nsecond=%s", payload, reencoded)
	}
	digest, err := DigestApprovedDefinitionV3(decoded)
	if err != nil || len(digest) != 64 {
		t.Fatalf("digest=%q err=%v", digest, err)
	}
	for _, forbidden := range []string{`"tool_calls"`, `"fetch_plan"`, `"sources"`} {
		if bytes.Contains(payload, []byte(forbidden)) {
			t.Fatalf("V3 payload contains retired execution state %q: %s", forbidden, payload)
		}
	}
}

func TestApprovedDefinitionV3OptionalResearchScopePreservesAbsentBytes(t *testing.T) {
	input := validApprovedDefinitionInputV3()
	retained, err := BuildApprovedDefinitionV3(input)
	if err != nil {
		t.Fatal(err)
	}
	retainedBytes, _ := EncodeApprovedDefinitionV3(retained)
	if strings.Contains(string(retainedBytes), "research_scope") {
		t.Fatalf("absent scope changed retained bytes: %s", retainedBytes)
	}
	input.ResearchScope = &ResearchScopeV3{Mode: ResearchScopeEventWindowV3, LookbackSeconds: ResearchScopeWeekSecondsV3}
	scoped, err := BuildApprovedDefinitionV3(input)
	if err != nil || scoped.ResearchScope == nil {
		t.Fatalf("scoped definition=%+v err=%v", scoped, err)
	}
	scopedBytes, _ := EncodeApprovedDefinitionV3(scoped)
	if !strings.Contains(string(scopedBytes), `"research_scope":{"mode":"event_window","lookback_seconds":604800}`) {
		t.Fatalf("scope bytes=%s", scopedBytes)
	}
	input.ResearchScope.LookbackSeconds--
	if _, err := BuildApprovedDefinitionV3(input); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("unsupported lookback error=%v", err)
	}
}

func TestApprovedDefinitionV3FailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ApprovedDefinitionInputV3)
	}{
		{name: "compiled mode", mutate: func(input *ApprovedDefinitionInputV3) { input.ExecutionMode = types.ExecutionModeCompiled }},
		{name: "empty manual", mutate: func(input *ApprovedDefinitionInputV3) { input.TaskManual = "" }},
		{name: "empty delivery allowed", mutate: func(input *ApprovedDefinitionInputV3) { input.Notification.SuppressEmpty = false }},
		{name: "unknown threshold", mutate: func(input *ApprovedDefinitionInputV3) { input.Notification.MinimumSignificance = "importantish" }},
		{name: "unbounded planner", mutate: func(input *ApprovedDefinitionInputV3) { input.PlannerBudget.MaxToolCalls = 17 }},
		{name: "unsupported delivery", mutate: func(input *ApprovedDefinitionInputV3) { input.DeliveryPolicy = "webhook" }},
		{name: "spec smuggles tool calls", mutate: func(input *ApprovedDefinitionInputV3) {
			input.SpecJSON = json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai","tool_calls":[]}`)
		}},
		{name: "spec smuggles sources", mutate: func(input *ApprovedDefinitionInputV3) {
			input.SpecJSON = json.RawMessage(`{"cron":"0 9 * * 1","sources":[]}`)
		}},
		{name: "sub-hour interval", mutate: func(input *ApprovedDefinitionInputV3) {
			input.SpecJSON = json.RawMessage(`{"every_seconds":3599}`)
		}},
		{name: "invalid cron", mutate: func(input *ApprovedDefinitionInputV3) {
			input.SpecJSON = json.RawMessage(`{"cron":"60 9 * * 1","tz":"Asia/Shanghai"}`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validApprovedDefinitionInputV3()
			test.mutate(&input)
			if _, err := BuildApprovedDefinitionV3(input); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("err=%v, want ErrInvalidState", err)
			}
		})
	}

	definition, err := BuildApprovedDefinitionV3(validApprovedDefinitionInputV3())
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := EncodeApprovedDefinitionV3(definition)
	payload = bytes.Replace(payload, []byte(`"task_name":`), []byte(`"unexpected":true,"task_name":`), 1)
	if _, err := DecodeApprovedDefinitionV3(payload); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("unknown field err=%v", err)
	}
}

func TestApprovedDefinitionV3StaticGuardHasNoFrozenExecutionModel(t *testing.T) {
	typeOf := reflect.TypeOf(ApprovedDefinitionV3{})
	forbiddenFields := map[string]bool{
		"ToolCalls": true, "Sources": true, "FetchPlan": true,
	}
	for index := 0; index < typeOf.NumField(); index++ {
		if forbiddenFields[typeOf.Field(index).Name] {
			t.Fatalf("V3 definition exposes frozen execution field %s", typeOf.Field(index).Name)
		}
	}
	source, err := os.ReadFile("approved_v3.go")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(string(source))
	for _, forbidden := range []string{
		"toolinvocationv1", "fetch_targets", "task_fetch_targets", "subscriptions",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("approved_v3.go references retired runtime symbol %q", forbidden)
		}
	}
}
