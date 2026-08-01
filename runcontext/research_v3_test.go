package runcontext

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestResearchSnapshotV3CanonicalRoundTrip(t *testing.T) {
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: 7, UserID: 42, TaskID: "task-v3", TaskName: "Kimi pricing",
		TaskManual:    "检查 Kimi 官方套餐并交叉核验；没有重大更新不推送。",
		SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3, SuppressEmpty: true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language: taskstate.OutputLanguageZhCNV3,
			Format:   taskstate.OutputFormatExecutiveBriefV3, IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := SealResearchSnapshotV3(ResearchSnapshotV3{
		Identity: types.RunIdentity{
			TemporalWorkflowID: "workflow-v3", TemporalRunID: "run-v3",
			RunKind: types.RunSnapshotKindScheduled, TenantID: 7, UserID: 42,
			TaskID: "task-v3",
		},
		DefinitionVersion: 3, HistoryThroughUTC: "2026-08-01T12:34:56.123Z",
		Definition: definition, Policy: testPolicyBundleV1(t),
		ResearchTools: testResearchToolPolicyV3(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResearchSnapshotPayloadV3(sealed.CanonicalPayload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sealed.CanonicalPayload, decoded.CanonicalPayload) ||
		sealed.PayloadDigest != decoded.PayloadDigest ||
		sealed.DefinitionDigest != decoded.DefinitionDigest {
		t.Fatal("snapshot round trip drifted")
	}
	tampered := bytes.Replace(sealed.CanonicalPayload,
		[]byte(`"history_through_utc":"2026-08-01T12:34:56.123Z"`),
		[]byte(`"history_through_utc":"2026-08-01T12:34:56.123+00:00"`), 1)
	if _, err := DecodeResearchSnapshotPayloadV3(tampered); err == nil {
		t.Fatal("tampered cutoff passed")
	}
}

func TestResearchExecutionPlanV3CanonicalRoundTrip(t *testing.T) {
	digest := strings.Repeat("a", 64)
	canonicalizerCalls := 0
	plan, err := BuildResearchExecutionPlanV3(digest, digest, digest, []ResearchPlanStepV3{
		{InvocationID: "official-page", ToolName: "read_page", Arguments: json.RawMessage(`{"url":"https://example.com/official"}`)},
		{InvocationID: "cross-check", ToolName: "web_search", Arguments: json.RawMessage(`{"query":"Kimi pricing official update"}`)},
	}, func(_ string, arguments json.RawMessage) (json.RawMessage, error) {
		canonicalizerCalls++
		return append(json.RawMessage(nil), arguments...), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if canonicalizerCalls != 2 {
		t.Fatalf("canonicalizer calls=%d", canonicalizerCalls)
	}
	payload, err := EncodeResearchExecutionPlanV3(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResearchExecutionPlanV3(payload)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, _ := EncodeResearchExecutionPlanV3(decoded)
	if !bytes.Equal(payload, reencoded) {
		t.Fatalf("round trip drift\n%s\n%s", payload, reencoded)
	}
	digestValue, err := DigestResearchExecutionPlanV3(decoded)
	if err != nil || len(digestValue) != 64 {
		t.Fatalf("digest=%q err=%v", digestValue, err)
	}
}

func TestResearchExecutionPlanV3RejectsUncanonicalOrUnknownInput(t *testing.T) {
	digest := strings.Repeat("a", 64)
	_, err := BuildResearchExecutionPlanV3(digest, digest, digest, []ResearchPlanStepV3{{
		InvocationID: "one", ToolName: "web_search",
		Arguments: json.RawMessage(`{"query":"x", "limit":1}`),
	}}, func(_ string, arguments json.RawMessage) (json.RawMessage, error) {
		return arguments, nil
	})
	if err == nil {
		t.Fatal("non-canonical arguments passed")
	}

	payload := `{"schema_version":"vane.research-execution-plan/v3","definition_digest":"` + digest +
		`","capability_catalog_digest":"` + digest +
		`","tool_policy_digest":"` + digest +
		`","steps":[{"invocation_id":"one","tool_name":"web_search","arguments":{}}],"secret":"x"}`
	if _, err := DecodeResearchExecutionPlanV3([]byte(payload)); err == nil {
		t.Fatal("unknown plan field passed")
	}
}

func testResearchToolPolicyV3(t *testing.T) runtimepolicy.ResearchToolPolicyV3 {
	t.Helper()
	tools := make([]runtimepolicy.ResearchToolDefinitionV3, 0, 2)
	for _, item := range []struct {
		name           string
		implementation runtimepolicy.ResearchToolImplementationV3
	}{
		{"web_search", runtimepolicy.ResearchToolExaSearchV3},
		{"web_contents", runtimepolicy.ResearchToolExaContentsV3},
	} {
		tools = append(tools, runtimepolicy.ResearchToolDefinitionV3{
			Name: item.name, Description: "Test scheduled public read",
			Parameters:     json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}`),
			Implementation: item.implementation, ImplementationGeneration: 1,
			Provider: "exa", Effects: []runtimepolicy.ResearchToolEffectV3{
				runtimepolicy.ResearchToolEffectBillableV3,
				runtimepolicy.ResearchToolEffectNetworkReadV3,
				runtimepolicy.ResearchToolEffectTrustTaintV3,
			}, ResultTrust: runtimepolicy.ResearchToolTrustExternalV3,
			BudgetBucket: "exa_calls", CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
			}, MaxCostMicroUSD: 10_000,
		})
	}
	policy, err := runtimepolicy.BuildResearchToolPolicyV3(tools)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
