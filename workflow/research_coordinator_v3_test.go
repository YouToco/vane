package workflow

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/researchgateway"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimeconfig"
	"github.com/YouToco/vane/runtimepolicy"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type coordinatorStoreFakeV3 struct {
	researchRuntimeStoreV3
	createSnapshot func(context.Context, types.RunIdentity, runtimepolicy.BundleV1,
		runtimepolicy.ResearchToolPolicyV3, runtimepolicy.ResearchModelPolicyV3, string) (types.ResearchRunSnapshotRefV3, error)
	beginStep func(context.Context, types.RunIdentity, int64,
		types.ResearchRunPlanRefV3, int) (storepkg.ResearchRunStepExecutionV3, error)
	loadResolution func(context.Context, types.RunIdentity, int64,
		types.ResearchRunPlanRefV3, int) (storepkg.ResearchRunStepResolutionV3, error)
}

func (f *coordinatorStoreFakeV3) CreateOrGetResearchRunSnapshotWithAuthorityV3(
	ctx context.Context, identity types.RunIdentity, policy runtimepolicy.BundleV1,
	tools runtimepolicy.ResearchToolPolicyV3, model runtimepolicy.ResearchModelPolicyV3,
	authorityToken string,
) (types.ResearchRunSnapshotRefV3, error) {
	return f.createSnapshot(ctx, identity, policy, tools, model, authorityToken)
}

func (f *coordinatorStoreFakeV3) BeginResearchRunStepV3(
	ctx context.Context, identity types.RunIdentity, snapshotID int64,
	plan types.ResearchRunPlanRefV3, ordinal int,
) (storepkg.ResearchRunStepExecutionV3, error) {
	return f.beginStep(ctx, identity, snapshotID, plan, ordinal)
}

func (f *coordinatorStoreFakeV3) LoadResearchRunStepResolutionV3(
	ctx context.Context, identity types.RunIdentity, snapshotID int64,
	plan types.ResearchRunPlanRefV3, ordinal int,
) (storepkg.ResearchRunStepResolutionV3, error) {
	return f.loadResolution(ctx, identity, snapshotID, plan, ordinal)
}

type coordinatorGatewayFakeV3 struct{}

func (coordinatorGatewayFakeV3) Execute(
	context.Context, researchgateway.ExecuteRequestV1,
) (researchgateway.ExecuteResponseV1, error) {
	panic("gateway must not be called")
}

type coordinatorExecutorFakeV3 struct{ calls int }

func (f *coordinatorExecutorFakeV3) ExecuteOnceV3(
	context.Context, fetcher.ResearchExecutionRequestV3,
) fetcher.ResearchExecutionReceiptV3 {
	f.calls++
	panic("provider must not be called")
}

func TestProductionResearchRuntimeV3IsDeliveryHardDark(t *testing.T) {
	identity, snapshot, plan, _ := researchBridgeFixtureV3(t)
	policy := validToolRuntimePolicyV2(t)
	tools := coordinatorResearchToolsV3(t)
	model := coordinatorResearchModelV3(t)
	store := &coordinatorStoreFakeV3{createSnapshot: func(
		_ context.Context, got types.RunIdentity, gotPolicy runtimepolicy.BundleV1,
		gotTools runtimepolicy.ResearchToolPolicyV3, gotModel runtimepolicy.ResearchModelPolicyV3,
		_ string,
	) (types.ResearchRunSnapshotRefV3, error) {
		if got != identity || gotPolicy.Validate() != nil ||
			gotTools.Validate() != nil || gotModel.Validate() != nil {
			t.Fatal("Prepare did not pass the trusted runtime policy set")
		}
		return snapshot, nil
	}}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, &coordinatorExecutorFakeV3{},
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return policy, tools, model, nil
		},
		func(got types.RunIdentity) bool { return got == identity },
	)
	if err != nil {
		t.Fatal(err)
	}
	gotSnapshot, authorized, deliveryAllowed, err := runtime.Prepare(t.Context(), identity, "")
	if err != nil || !authorized || deliveryAllowed || gotSnapshot != snapshot {
		t.Fatalf("Prepare snapshot=%+v authorized=%v delivery=%v err=%v",
			gotSnapshot, authorized, deliveryAllowed, err)
	}
	if _, err := runtime.Deliver(t.Context(), identity, snapshot, plan,
		types.ResearchBriefRefV3{}, "trace"); types.CodeOf(err) != types.CodeValidation || types.IsRetryable(err) {
		t.Fatalf("Deliver error=%v, want non-retryable hard-dark rejection", err)
	}
}

func TestProductionResearchRuntimeV3FreezesDedicatedNonThinkingModelPolicy(t *testing.T) {
	identity, snapshot, _, _ := researchBridgeFixtureV3(t)
	current, err := runtimeconfig.BuildResearchRuntimeV3(
		runtimeconfig.CurrentCompiledV1Input{
			Model: "cheap-pipeline-model", ResearchModel: "strong-research-model",
			ModelEndpointGeneration: 1, ModelCredentialGeneration: 1,
			ExaCredentialGeneration: 1, TikHubCredentialGeneration: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorStoreFakeV3{createSnapshot: func(
		_ context.Context, gotIdentity types.RunIdentity, _ runtimepolicy.BundleV1,
		_ runtimepolicy.ResearchToolPolicyV3, model runtimepolicy.ResearchModelPolicyV3,
		_ string,
	) (types.ResearchRunSnapshotRefV3, error) {
		if gotIdentity != identity || model.Planner.Model != "strong-research-model" ||
			model.Synthesis.Model != "strong-research-model" ||
			!model.Planner.DisableThinking || !model.Synthesis.DisableThinking {
			t.Fatalf("unsafe V3 model policy crossed the Store boundary: %+v", model)
		}
		return snapshot, nil
	}}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, &coordinatorExecutorFakeV3{},
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return current.Bundle, current.Tools, current.Model, nil
		}, func(got types.RunIdentity) bool { return got == identity })
	if err != nil {
		t.Fatal(err)
	}
	if _, authorized, delivery, err := runtime.Prepare(
		t.Context(), identity, ""); err != nil || !authorized || delivery {
		t.Fatalf("Prepare authorized=%v delivery=%v err=%v", authorized, delivery, err)
	}
}

func TestProductionResearchRuntimeV3FreezesExactActionAuthority(t *testing.T) {
	identity, snapshot, _, _ := researchBridgeFixtureV3(t)
	snapshot.AuthorityGeneration = 7
	snapshot.TargetActionDigest = strings.Repeat("8", 64)
	snapshot.ActionAuthorizationDigest = strings.Repeat("9", 64)
	snapshot.ReferenceDigest = ""
	var err error
	snapshot, err = types.SealResearchRunSnapshotRefV3(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	wantToken := "research-v3-action-authority-token-1234567890"
	store := &coordinatorStoreFakeV3{createSnapshot: func(
		_ context.Context, got types.RunIdentity, _ runtimepolicy.BundleV1,
		_ runtimepolicy.ResearchToolPolicyV3, _ runtimepolicy.ResearchModelPolicyV3,
		token string,
	) (types.ResearchRunSnapshotRefV3, error) {
		if got != identity || token != wantToken {
			t.Fatalf("authority input identity=%+v token=%q", got, token)
		}
		return snapshot, nil
	}}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, &coordinatorExecutorFakeV3{},
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return validToolRuntimePolicyV2(t), coordinatorResearchToolsV3(t),
				coordinatorResearchModelV3(t), nil
		}, func(types.RunIdentity) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	got, authorized, deliveryAllowed, err := runtime.Prepare(t.Context(), identity, wantToken)
	if err != nil || !authorized || !deliveryAllowed || got != snapshot {
		t.Fatalf("Prepare authority snapshot=%+v authorized=%v delivery=%v err=%v",
			got, authorized, deliveryAllowed, err)
	}
}

func TestProductionResearchRuntimeV3RecoveryNeverRepeatsPaidToolEffect(t *testing.T) {
	identity, snapshot, plan, execution := researchBridgeFixtureV3(t)
	executor := &coordinatorExecutorFakeV3{}
	store := &coordinatorStoreFakeV3{
		beginStep: func(context.Context, types.RunIdentity, int64,
			types.ResearchRunPlanRefV3, int) (storepkg.ResearchRunStepExecutionV3, error) {
			execution.FirstWriter = false
			execution.Arguments = nil
			return execution, nil
		},
		loadResolution: func(context.Context, types.RunIdentity, int64,
			types.ResearchRunPlanRefV3, int) (storepkg.ResearchRunStepResolutionV3, error) {
			return storepkg.ResearchRunStepResolutionV3{
				Phase: storepkg.ResearchRunStepCompletedV3,
				Receipt: storepkg.ResearchRunStepReceiptV3{
					StepID: 77, Ordinal: 0, Phase: storepkg.ResearchRunStepCompletedV3,
					InvocationID: execution.InvocationID, ToolName: execution.ToolName,
					RequestDigest: execution.RequestDigest,
					ResultDigest:  strings.Repeat("4", 64),
				},
				Evidence: &storepkg.ResearchRunEvidenceV3{EvidenceID: 88},
			}, nil
		},
	}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, executor,
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return runtimepolicy.BundleV1{}, runtimepolicy.ResearchToolPolicyV3{}, runtimepolicy.ResearchModelPolicyV3{}, nil
		},
		func(types.RunIdentity) bool { return true },
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := runtime.ExecuteStep(t.Context(), identity, snapshot, plan, 0, "trace")
	if err != nil || receipt.StepID != 77 || receipt.EvidenceID != 88 || executor.calls != 0 {
		t.Fatalf("recovery receipt=%+v provider_calls=%d err=%v",
			receipt, executor.calls, err)
	}
}

func TestProductionResearchRuntimeV3UnauthorizedPrepareHasNoDependenciesOrEffects(t *testing.T) {
	identity, _, _, _ := researchBridgeFixtureV3(t)
	policyCalls, storeCalls := 0, 0
	store := &coordinatorStoreFakeV3{createSnapshot: func(
		context.Context, types.RunIdentity, runtimepolicy.BundleV1,
		runtimepolicy.ResearchToolPolicyV3, runtimepolicy.ResearchModelPolicyV3, string,
	) (types.ResearchRunSnapshotRefV3, error) {
		storeCalls++
		return types.ResearchRunSnapshotRefV3{}, nil
	}}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, &coordinatorExecutorFakeV3{},
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			policyCalls++
			return runtimepolicy.BundleV1{}, runtimepolicy.ResearchToolPolicyV3{},
				runtimepolicy.ResearchModelPolicyV3{}, nil
		},
		func(types.RunIdentity) bool { return false },
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, authorized, deliveryAllowed, err := runtime.Prepare(t.Context(), identity, "")
	if err != nil || authorized || deliveryAllowed ||
		snapshot != (types.ResearchRunSnapshotRefV3{}) ||
		policyCalls != 0 || storeCalls != 0 {
		t.Fatalf("unauthorized Prepare snapshot=%+v authorized=%v delivery=%v policy=%d store=%d err=%v",
			snapshot, authorized, deliveryAllowed, policyCalls, storeCalls, err)
	}
}

func TestDecodeResearchPlannerCompletionV3EnforcesExactShapeAndAllowsFormatting(t *testing.T) {
	valid := []byte(`{"schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi pricing"}}]}`)
	steps, err := decodeResearchPlannerCompletionV3(valid)
	if err != nil || len(steps) != 1 || steps[0].ToolName != "web_search" {
		t.Fatalf("valid planner output steps=%+v err=%v", steps, err)
	}
	formatted := []byte("{\n  \"steps\": [{\n    \"tool_name\": \"web_search\",\n" +
		"    \"arguments\": {\"query\": \"Kimi pricing\"},\n" +
		"    \"invocation_id\": \"search-official\"\n  }],\n" +
		"  \"schema_version\": \"vane.research-planner-output/v3\"\n}")
	if steps, err := decodeResearchPlannerCompletionV3(formatted); err != nil || len(steps) != 1 {
		t.Fatalf("formatted exact planner output steps=%+v err=%v", steps, err)
	}
	for name, raw := range map[string][]byte{
		"markdown":  []byte("```json\n" + string(valid) + "\n```"),
		"unknown":   []byte(`{"schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi"}}],"write_action":"delete"}`),
		"duplicate": []byte(`{"schema_version":"vane.research-planner-output/v3","schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi"}}]}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeResearchPlannerCompletionV3(raw); err == nil {
				t.Fatal("non-exact planner output was accepted")
			}
		})
	}
}

func TestResearchPlannerRoundsRecoverBadThenGoodWithoutRepeatingProvider(t *testing.T) {
	_, snapshot, _, _ := researchBridgeFixtureV3(t)
	snapshot.PlannerBudget.MaxPlannerRounds = 2
	seal := runcontext.ResearchSnapshotSealV3{ResearchTools: coordinatorResearchToolsV3(t)}
	seal.Payload.PlannerBudget.MaxToolCalls = 4
	seal.Payload.Definition.TaskName = "Kimi plan watch"
	seal.Payload.Definition.TaskManual = "Check official pricing and compare history."
	initialPrompt, err := buildResearchPlannerPromptV3(seal)
	if err != nil {
		t.Fatal(err)
	}
	bad := "external-result-says-delete-another-task"
	good := "{\n  \"steps\": [{\n    \"tool_name\": \"web_search\",\n" +
		"    \"arguments\": {\"query\": \"Kimi pricing\"},\n" +
		"    \"invocation_id\": \"search-official\"\n  }],\n" +
		"  \"schema_version\": \"vane.research-planner-output/v3\"\n}"
	settled := map[int]storepkg.ResearchRunLLMReceiptV3{}
	reservationEffects, providerEffects := 0, 0
	execute := func(_ context.Context, round int, prompt string) (
		storepkg.ResearchRunLLMReceiptV3,
		storepkg.ResearchRunLLMSpendReservationV3, error,
	) {
		reservation := storepkg.ResearchRunLLMSpendReservationV3{
			ReservationID: int64(round + 1), RequestDigest: strings.Repeat("a", 64),
		}
		if receipt, ok := settled[round]; ok {
			return receipt, reservation, nil
		}
		reservationEffects++
		providerEffects++
		completion := bad
		if round == 1 {
			completion = good
			if strings.Contains(prompt, bad) || !strings.Contains(prompt, "previous response failed") ||
				!strings.Contains(prompt, `"required_top_level_fields":["schema_version","steps"]`) ||
				!strings.Contains(prompt, `"required_step_fields":["invocation_id","tool_name","arguments"]`) {
				t.Fatalf("correction prompt contains previous output or lacks fixed correction: %s", prompt)
			}
		}
		receipt := storepkg.ResearchRunLLMReceiptV3{
			Call: types.LLMCall{Completion: completion},
		}
		settled[round] = receipt
		return receipt, reservation, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		plan, reservation, err := executeResearchPlannerRoundsV3(
			t.Context(), snapshot, seal, initialPrompt, execute)
		if err != nil || len(plan.Steps) != 1 || reservation.ReservationID != 2 {
			t.Fatalf("attempt=%d plan=%+v reservation=%+v err=%v",
				attempt, plan, reservation, err)
		}
	}
	if reservationEffects != 2 || providerEffects != 2 || len(settled) != 2 {
		t.Fatalf("reservation_effects=%d provider_effects=%d settled_rounds=%d",
			reservationEffects, providerEffects, len(settled))
	}
}

func TestResearchPlannerRoundsExhaustionIsNonRetryableAndRecoverySafe(t *testing.T) {
	_, snapshot, _, _ := researchBridgeFixtureV3(t)
	snapshot.PlannerBudget.MaxPlannerRounds = 2
	seal := runcontext.ResearchSnapshotSealV3{ResearchTools: coordinatorResearchToolsV3(t)}
	settled := map[int]storepkg.ResearchRunLLMReceiptV3{}
	providerEffects := 0
	execute := func(_ context.Context, round int, _ string) (
		storepkg.ResearchRunLLMReceiptV3,
		storepkg.ResearchRunLLMSpendReservationV3, error,
	) {
		reservation := storepkg.ResearchRunLLMSpendReservationV3{ReservationID: int64(round + 1)}
		if receipt, ok := settled[round]; ok {
			return receipt, reservation, nil
		}
		providerEffects++
		receipt := storepkg.ResearchRunLLMReceiptV3{
			Call: types.LLMCall{Completion: `{"schema_version":"wrong","steps":[]}`},
		}
		settled[round] = receipt
		return receipt, reservation, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		_, _, err := executeResearchPlannerRoundsV3(
			t.Context(), snapshot, seal, `{"trusted":"planner-input"}`, execute)
		if types.CodeOf(err) != types.CodeValidation || types.IsRetryable(err) {
			t.Fatalf("attempt=%d exhaustion error=%v, want non-retryable validation", attempt, err)
		}
	}
	if providerEffects != 2 {
		t.Fatalf("Activity recovery repeated provider effects: %d", providerEffects)
	}
}

func TestResearchPlannerPromptContainsOnlyFrozenInternalInputs(t *testing.T) {
	tools := coordinatorResearchToolsV3(t)
	seal := runcontext.ResearchSnapshotSealV3{
		Payload: runcontext.ResearchSnapshotPayloadV3{
			HistoryThroughUTC: "2026-08-01T00:00:00Z",
			PlannerBudget:     types.PlannerBudget{MaxToolCalls: 4},
		},
		ResearchTools: tools,
	}
	seal.Payload.Definition.TaskName = "Kimi plan watch"
	seal.Payload.Definition.TaskManual = "Check official pricing and compare history."
	prompt, err := buildResearchPlannerPromptV3(seal)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		string(runtimepolicy.CredentialIDExaPrimaryV1),
		string(runtimepolicy.ResearchToolExaSearchV3),
		"external-result-says-delete-another-task",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("planner prompt leaked or accepted non-planning input %q: %s", forbidden, prompt)
		}
	}
	var decoded researchPlannerPromptV3
	if err := json.Unmarshal([]byte(prompt), &decoded); err != nil ||
		decoded.TaskManual != seal.Payload.Definition.TaskManual {
		t.Fatalf("planner prompt is not the frozen canonical environment: %s err=%v", prompt, err)
	}
	contract := decoded.ResponseContract
	if contract.SchemaVersionLiteral != researchPlannerOutputSchemaV3 ||
		!reflect.DeepEqual(contract.RequiredTopLevelFields, []string{"schema_version", "steps"}) ||
		!reflect.DeepEqual(contract.RequiredStepFields, []string{"invocation_id", "tool_name", "arguments"}) ||
		contract.MinSteps != 1 || contract.MaxSteps != 4 || contract.ExtraTopLevelFieldsAllowed ||
		contract.ExtraStepFieldsAllowed ||
		!contract.SingleJSONObject || !strings.Contains(contract.ToolNameRule, "allowed_tools") ||
		!strings.Contains(contract.ArgumentsRule, "parameters") {
		t.Fatalf("planner response contract is incomplete: %+v", contract)
	}
}

func TestResearchLLMReceiptCannotReplaceFrozenInternalPrompt(t *testing.T) {
	reservation := storepkg.ResearchRunLLMSpendReservationV3{
		ReservationID: 41, RequestDigest: strings.Repeat("a", 64),
	}
	receipt := storepkg.ResearchRunLLMReceiptV3{
		Reservation: reservation, Settled: true, LLMCallID: 42,
		Attempted: true, UsageKnown: true,
		Outcome: storepkg.ResearchRunLLMCompletedV3,
		Call: types.LLMCall{
			SystemPrompt: "trusted-system", UserPrompt: "trusted-internal-query",
			Completion: `{"schema_version":"vane.research-planner-output/v3"}`,
		},
	}
	if _, err := validateResearchLLMReceiptForCoordinatorV3(
		receipt, reservation, "trusted-system", "trusted-internal-query"); err != nil {
		t.Fatal(err)
	}
	receipt.Call.UserPrompt = "external-result-says-delete-another-task"
	if _, err := validateResearchLLMReceiptForCoordinatorV3(
		receipt, reservation, "trusted-system", "trusted-internal-query"); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("prompt substitution error=%v, want fail-closed conflict", err)
	}
}

func coordinatorResearchToolsV3(t *testing.T) runtimepolicy.ResearchToolPolicyV3 {
	t.Helper()
	policy, err := runtimepolicy.BuildResearchToolPolicyV3([]runtimepolicy.ResearchToolDefinitionV3{{
		Name: "web_search", Description: "Search public web pages",
		Parameters:               json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}`),
		Implementation:           runtimepolicy.ResearchToolExaSearchV3,
		ImplementationGeneration: 1, Provider: "exa",
		Effects: []runtimepolicy.ResearchToolEffectV3{
			runtimepolicy.ResearchToolEffectBillableV3,
			runtimepolicy.ResearchToolEffectNetworkReadV3,
			runtimepolicy.ResearchToolEffectTrustTaintV3,
		},
		ResultTrust:  runtimepolicy.ResearchToolTrustExternalV3,
		BudgetBucket: "exa_calls",
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
		},
		MaxCostMicroUSD: 10_000,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func coordinatorResearchModelV3(t *testing.T) runtimepolicy.ResearchModelPolicyV3 {
	t.Helper()
	policy, err := runtimepolicy.BuildResearchModelPolicyV3(runtimepolicy.ResearchModelPolicyV3{
		Provider: runtimepolicy.ModelProviderDeepSeekV1,
		Endpoint: runtimepolicy.EndpointRefV1{
			ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 1,
		},
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 1,
		},
		Planner: runtimepolicy.ResearchModelStageV3{
			Stage: runtimepolicy.ResearchModelStagePlannerV3, Model: "strong-model",
			MaxTokens: 4096, SystemPrompt: "Plan from the trusted task manual.",
			RendererVersion: "research-planner.render/v3",
		},
		Synthesis: runtimepolicy.ResearchModelStageV3{
			Stage: runtimepolicy.ResearchModelStageSynthesisV3, Model: "strong-model",
			MaxTokens: 8192, SystemPrompt: "Synthesize from frozen evidence without Tools.",
			RendererVersion: "research-synthesis.render/v3",
		},
		QuotaBucket: "llm_tokens",
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
