package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	loadSnapshot func(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3) (runcontext.ResearchSnapshotSealV3, error)
	prepareBrief func(context.Context, storepkg.PrepareResearchBriefSynthesisV3Params) (
		storepkg.PrepareResearchBriefSynthesisV3Result, error)
	beginLLMSpend func(context.Context, storepkg.BeginResearchRunLLMSpendV3Params) (
		storepkg.ResearchRunLLMSpendReservationV3, error)
	claimBrief func(context.Context, storepkg.ClaimResearchBriefSynthesisV3Params) (
		storepkg.ClaimResearchBriefSynthesisV3Result, error)
	loadLLMReceipt func(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3, string, int) (storepkg.ResearchRunLLMReceiptV3, bool, error)
	prepareGrounding func(context.Context, storepkg.PrepareResearchBriefGroundingV1Params) (
		storepkg.PrepareResearchBriefGroundingV1Result, error)
	settleGrounding func(context.Context, storepkg.SettleResearchBriefGroundingV1Params) (
		storepkg.ResearchBriefGroundingV1, error)
	prepareCorrection func(context.Context,
		storepkg.PrepareResearchBriefGroundingCorrectionV1Params) (
		storepkg.PrepareResearchBriefGroundingCorrectionV1Result, error)
	settleCorrectionCandidate func(context.Context,
		storepkg.SettleResearchBriefGroundingCorrectionCandidateV1Params) (
		storepkg.ResearchBriefGroundingCorrectionV1, error)
	settleCorrectionVerification func(context.Context,
		storepkg.SettleResearchBriefGroundingCorrectionVerificationV1Params) (
		storepkg.ResearchBriefGroundingCorrectionV1, error)
	finalizeBrief func(context.Context, storepkg.FinalizeResearchBriefSynthesisV3Params) (
		types.ResearchBriefRefV3, error)
}

func (f *coordinatorStoreFakeV3) LoadResearchRunSnapshotV3(
	ctx context.Context, identity types.RunIdentity, snapshot types.ResearchRunSnapshotRefV3,
) (runcontext.ResearchSnapshotSealV3, error) {
	return f.loadSnapshot(ctx, identity, snapshot)
}

func (f *coordinatorStoreFakeV3) PrepareOrGetResearchBriefSynthesisV3(
	ctx context.Context, params storepkg.PrepareResearchBriefSynthesisV3Params,
) (storepkg.PrepareResearchBriefSynthesisV3Result, error) {
	return f.prepareBrief(ctx, params)
}

func (f *coordinatorStoreFakeV3) BeginResearchRunLLMSpendV3(
	ctx context.Context, params storepkg.BeginResearchRunLLMSpendV3Params,
) (storepkg.ResearchRunLLMSpendReservationV3, error) {
	return f.beginLLMSpend(ctx, params)
}

func (f *coordinatorStoreFakeV3) ClaimResearchBriefSynthesisV3(
	ctx context.Context, params storepkg.ClaimResearchBriefSynthesisV3Params,
) (storepkg.ClaimResearchBriefSynthesisV3Result, error) {
	return f.claimBrief(ctx, params)
}

func (f *coordinatorStoreFakeV3) LoadResearchRunLLMReceiptV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, stage string, round int,
) (storepkg.ResearchRunLLMReceiptV3, bool, error) {
	return f.loadLLMReceipt(ctx, identity, snapshot, stage, round)
}

func (f *coordinatorStoreFakeV3) PrepareOrGetResearchBriefGroundingV1(
	ctx context.Context, params storepkg.PrepareResearchBriefGroundingV1Params,
) (storepkg.PrepareResearchBriefGroundingV1Result, error) {
	return f.prepareGrounding(ctx, params)
}

func (f *coordinatorStoreFakeV3) SettleResearchBriefGroundingV1(
	ctx context.Context, params storepkg.SettleResearchBriefGroundingV1Params,
) (storepkg.ResearchBriefGroundingV1, error) {
	return f.settleGrounding(ctx, params)
}

func (f *coordinatorStoreFakeV3) PrepareOrGetResearchBriefGroundingCorrectionV1(
	ctx context.Context, params storepkg.PrepareResearchBriefGroundingCorrectionV1Params,
) (storepkg.PrepareResearchBriefGroundingCorrectionV1Result, error) {
	return f.prepareCorrection(ctx, params)
}

func (f *coordinatorStoreFakeV3) SettleResearchBriefGroundingCorrectionCandidateV1(
	ctx context.Context, params storepkg.SettleResearchBriefGroundingCorrectionCandidateV1Params,
) (storepkg.ResearchBriefGroundingCorrectionV1, error) {
	return f.settleCorrectionCandidate(ctx, params)
}

func (f *coordinatorStoreFakeV3) SettleResearchBriefGroundingCorrectionVerificationV1(
	ctx context.Context, params storepkg.SettleResearchBriefGroundingCorrectionVerificationV1Params,
) (storepkg.ResearchBriefGroundingCorrectionV1, error) {
	return f.settleCorrectionVerification(ctx, params)
}

func (f *coordinatorStoreFakeV3) FinalizeResearchBriefSynthesisV3(
	ctx context.Context, params storepkg.FinalizeResearchBriefSynthesisV3Params,
) (types.ResearchBriefRefV3, error) {
	return f.finalizeBrief(ctx, params)
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
		func(_ context.Context, got types.RunIdentity, _ string) (bool, error) {
			return got == identity, nil
		},
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
		}, func(_ context.Context, got types.RunIdentity, _ string) (bool, error) {
			return got == identity, nil
		})
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
		}, func(context.Context, types.RunIdentity, string) (bool, error) {
			return true, nil
		})
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
		func(context.Context, types.RunIdentity, string) (bool, error) { return true, nil },
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

func TestProductionResearchRuntimeV3RecoveryReturnsSealedFailureWithoutProviderReplay(t *testing.T) {
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
				Phase: storepkg.ResearchRunStepIndeterminateV3,
				Receipt: storepkg.ResearchRunStepReceiptV3{
					StepID: 77, Ordinal: 0, Phase: storepkg.ResearchRunStepIndeterminateV3,
					InvocationID: execution.InvocationID, ToolName: execution.ToolName,
					RequestDigest: execution.RequestDigest,
					ErrorCode:     string(fetcher.ResearchExecutionProviderUncertainV3),
				},
			}, nil
		},
	}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, executor,
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return runtimepolicy.BundleV1{}, runtimepolicy.ResearchToolPolicyV3{},
				runtimepolicy.ResearchModelPolicyV3{}, nil
		},
		func(context.Context, types.RunIdentity, string) (bool, error) { return true, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := runtime.ExecuteStep(t.Context(), identity, snapshot, plan, 0, "trace")
	if err != nil || receipt.StepID != 77 ||
		receipt.Phase != string(storepkg.ResearchRunStepIndeterminateV3) ||
		receipt.ErrorCode != string(fetcher.ResearchExecutionProviderUncertainV3) ||
		receipt.EvidenceID != 0 || receipt.ResultDigest != "" || executor.calls != 0 {
		t.Fatalf("recovery receipt=%+v provider_calls=%d err=%v",
			receipt, executor.calls, err)
	}
}

func TestProductionResearchRuntimeV3LegacySynthesisRendererRejectsPartialCoverageBeforeSpend(t *testing.T) {
	identity, snapshot, plan, _ := researchBridgeFixtureV3(t)
	tools := coordinatorResearchToolsV3(t)
	model := coordinatorResearchModelV3(t)
	model.Synthesis.RendererVersion = runtimepolicy.ResearchSynthesisRendererVersionV3
	seal := runcontext.ResearchSnapshotSealV3{
		DefinitionDigest: snapshot.DefinitionDigest,
		PolicyDigests: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: snapshot.CapabilityCatalogDigest,
		},
		ResearchToolPolicyDigest:  snapshot.ToolPolicyDigest,
		ResearchModelPolicyDigest: snapshot.ModelPolicyDigest,
		PayloadDigest:             snapshot.PayloadDigest,
		Payload: runcontext.ResearchSnapshotPayloadV3{
			TenantID: identity.TenantID, UserID: identity.UserID,
			TaskID: identity.TaskID, TemporalWorkflowID: identity.TemporalWorkflowID,
			TemporalRunID: identity.TemporalRunID, PlannerBudget: snapshot.PlannerBudget,
		},
		ResearchTools: tools, ResearchModel: model,
	}
	spendCalls := 0
	store := &coordinatorStoreFakeV3{
		prepareBrief: func(context.Context, storepkg.PrepareResearchBriefSynthesisV3Params) (
			storepkg.PrepareResearchBriefSynthesisV3Result, error,
		) {
			return storepkg.PrepareResearchBriefSynthesisV3Result{
				PartialCoverage: true,
				Synthesis: storepkg.ResearchBriefSynthesisV3{
					ID: 91, Status: storepkg.ResearchBriefSynthesisPreparedV3,
					ContextPayload: []byte(`{"schema_version":"vane.research-synthesis-context/v3.1"}`),
				},
			}, nil
		},
		loadSnapshot: func(context.Context, types.RunIdentity,
			types.ResearchRunSnapshotRefV3) (runcontext.ResearchSnapshotSealV3, error) {
			return seal, nil
		},
		beginLLMSpend: func(context.Context, storepkg.BeginResearchRunLLMSpendV3Params) (
			storepkg.ResearchRunLLMSpendReservationV3, error,
		) {
			spendCalls++
			return storepkg.ResearchRunLLMSpendReservationV3{}, nil
		},
	}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, &coordinatorExecutorFakeV3{},
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return runtimepolicy.BundleV1{}, runtimepolicy.ResearchToolPolicyV3{},
				runtimepolicy.ResearchModelPolicyV3{}, nil
		},
		func(context.Context, types.RunIdentity, string) (bool, error) { return true, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Synthesize(t.Context(), identity, snapshot, plan, "trace"); types.CodeOf(err) != types.CodeConflict || spendCalls != 0 {
		t.Fatalf("legacy partial synthesis error=%v spend_calls=%d", err, spendCalls)
	}
}

func TestProductionResearchRuntimeV36ExecutesOneCorrectionAndFinalVerifier(t *testing.T) {
	identity, snapshot, plan, _ := researchBridgeFixtureV3(t)
	tools := coordinatorResearchToolsV3(t)
	retained := coordinatorResearchModelV3(t)
	retained.Synthesis.RendererVersion = runtimepolicy.ResearchSynthesisRendererVersionV34
	verifier := retained.Synthesis
	verifier.Stage = runtimepolicy.ResearchModelStageGroundingVerifierV3
	verifier.Temperature = 0
	verifier.SystemPrompt = "Verify only cited evidence."
	verifier.RendererVersion = runtimepolicy.ResearchGroundingVerifierRendererVersionV12
	retained.GroundingVerifier = &verifier
	retained, err := runtimepolicy.BuildResearchModelPolicyV3(retained)
	if err != nil {
		t.Fatal(err)
	}
	model, err := runtimepolicy.WithExplicitEventWindowV36(retained)
	if err != nil {
		t.Fatal(err)
	}
	seal := runcontext.ResearchSnapshotSealV3{
		DefinitionDigest: snapshot.DefinitionDigest,
		PolicyDigests: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: snapshot.CapabilityCatalogDigest,
		},
		ResearchToolPolicyDigest:  snapshot.ToolPolicyDigest,
		ResearchModelPolicyDigest: snapshot.ModelPolicyDigest,
		PayloadDigest:             snapshot.PayloadDigest,
		Payload: runcontext.ResearchSnapshotPayloadV3{
			TenantID: identity.TenantID, UserID: identity.UserID,
			TaskID: identity.TaskID, TemporalWorkflowID: identity.TemporalWorkflowID,
			TemporalRunID: identity.TemporalRunID, PlannerBudget: snapshot.PlannerBudget,
		},
		ResearchTools: tools, ResearchModel: model,
	}
	citation := types.ResearchBriefCitationV3{
		Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "1",
	}
	initial, err := json.Marshal(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV3,
		Headline:      "OpenAI 向免费用户开放 GPT-5.6 Luna 并移除文本聊天限制",
		Summary: "OpenAI 官方扩大 GPT-5.6 Luna 的免费访问；" +
			"第三方报道据此称免费用户已无限使用文本聊天。",
		Significance: types.ResearchBriefSignificanceMajorV3,
		Citations:    []types.ResearchBriefCitationV3{citation},
	})
	if err != nil {
		t.Fatal(err)
	}
	corrected, err := json.Marshal(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV31,
		Assessment:    types.ResearchBriefAssessmentUnknownV31,
		Headline:      "当前证据不足",
		Summary:       "当前冻结证据不足以形成符合任务手册的可验证结论。",
		Significance:  types.ResearchBriefSignificanceNoneV3,
		Citations:     []types.ResearchBriefCitationV3{},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := func(payload []byte) string {
		sum := sha256.Sum256(payload)
		return hex.EncodeToString(sum[:])
	}
	initialDigest := digest(initial)
	correctedDigest := digest(corrected)
	firstVerdict, err := json.Marshal(types.ResearchGroundingVerdictPayloadV1{
		SchemaVersion:   types.ResearchGroundingVerdictSchemaV1,
		CandidateDigest: initialDigest, Verdict: types.ResearchGroundingUnsupportedV1,
		Issues: []types.ResearchGroundingIssueV1{{
			Field: "summary", Claim: "第三方报道据此称免费用户已无限使用文本聊天。",
			Refs:   []types.ResearchBriefCitationV3{citation},
			Reason: "官方 citation 只直接支持 expanding access，不能证明 unlimited text chats。",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	finalVerdict, err := json.Marshal(types.ResearchGroundingVerdictPayloadV1{
		SchemaVersion:   types.ResearchGroundingVerdictSchemaV1,
		CandidateDigest: correctedDigest, Verdict: types.ResearchGroundingGroundedV1,
		Issues: []types.ResearchGroundingIssueV1{},
	})
	if err != nil {
		t.Fatal(err)
	}
	completions := map[int][]byte{0: initial, 1: firstVerdict, 2: corrected, 3: finalVerdict}
	reservations := make(map[int]storepkg.ResearchRunLLMSpendReservationV3)
	prompts := make(map[int][2]string)
	var rounds []int
	const synthesisID int64 = 91
	store := &coordinatorStoreFakeV3{
		prepareBrief: func(context.Context, storepkg.PrepareResearchBriefSynthesisV3Params) (
			storepkg.PrepareResearchBriefSynthesisV3Result, error,
		) {
			return storepkg.PrepareResearchBriefSynthesisV3Result{
				FirstWriter: true,
				Synthesis: storepkg.ResearchBriefSynthesisV3{
					ID: synthesisID, Status: storepkg.ResearchBriefSynthesisPreparedV3,
					RequestDigest: strings.Repeat("3", 64), ContextPayload: []byte(`{"context":"frozen"}`),
				},
			}, nil
		},
		loadSnapshot: func(context.Context, types.RunIdentity,
			types.ResearchRunSnapshotRefV3) (runcontext.ResearchSnapshotSealV3, error) {
			return seal, nil
		},
		beginLLMSpend: func(_ context.Context, params storepkg.BeginResearchRunLLMSpendV3Params) (
			storepkg.ResearchRunLLMSpendReservationV3, error,
		) {
			if params.RoundOrdinal == 2 {
				for _, contract := range []string{
					"不得把 access 扩大写成默认或无限使用",
					"删除包含它的整句以及 headline 中对应分句",
					"重新审计 headline、summary 和 significance",
					"initial_verdict=unsupported 时禁止原样返回 original candidate",
					`{"schema_version":"vane.research-brief/v3.1","assessment":"unknown","headline":"当前证据不足","summary":"当前冻结证据不足以形成符合任务手册的可验证结论。","significance":"none","citations":[]}`,
				} {
					if !strings.Contains(params.SystemPrompt, contract) {
						t.Fatalf("round-2 production correction lacks %q: %s",
							contract, params.SystemPrompt)
					}
				}
			}
			rounds = append(rounds, params.RoundOrdinal)
			reservation := storepkg.ResearchRunLLMSpendReservationV3{
				ReservationID: int64(100 + params.RoundOrdinal), Stage: params.Stage,
				RoundOrdinal: params.RoundOrdinal, SubjectID: params.SubjectID,
				RequestDigest: strings.Repeat(string(rune('a'+params.RoundOrdinal)), 64),
			}
			reservations[params.RoundOrdinal] = reservation
			prompts[params.RoundOrdinal] = [2]string{params.SystemPrompt, params.UserPrompt}
			return reservation, nil
		},
		claimBrief: func(context.Context, storepkg.ClaimResearchBriefSynthesisV3Params) (
			storepkg.ClaimResearchBriefSynthesisV3Result, error,
		) {
			return storepkg.ClaimResearchBriefSynthesisV3Result{
				Claimed: true,
				Synthesis: storepkg.ResearchBriefSynthesisV3{
					ID: synthesisID, Status: storepkg.ResearchBriefSynthesisSpendingV3,
				},
			}, nil
		},
		loadLLMReceipt: func(_ context.Context, _ types.RunIdentity,
			_ types.ResearchRunSnapshotRefV3, _ string, round int,
		) (storepkg.ResearchRunLLMReceiptV3, bool, error) {
			reservation, found := reservations[round]
			if !found {
				return storepkg.ResearchRunLLMReceiptV3{}, false, nil
			}
			prompt := prompts[round]
			return storepkg.ResearchRunLLMReceiptV3{
				Reservation: reservation, Settled: true, LLMCallID: int64(200 + round),
				Attempted: true, UsageKnown: true, Outcome: storepkg.ResearchRunLLMCompletedV3,
				Call: types.LLMCall{SystemPrompt: prompt[0], UserPrompt: prompt[1],
					Completion: string(completions[round])},
			}, true, nil
		},
		prepareGrounding: func(_ context.Context, params storepkg.PrepareResearchBriefGroundingV1Params) (
			storepkg.PrepareResearchBriefGroundingV1Result, error,
		) {
			if !reflect.DeepEqual(params.CandidateBriefPayload, initial) {
				t.Fatalf("initial candidate=%s", params.CandidateBriefPayload)
			}
			return storepkg.PrepareResearchBriefGroundingV1Result{
				FirstWriter: true, Grounding: storepkg.ResearchBriefGroundingV1{
					ID: 501, Status: storepkg.ResearchBriefGroundingPreparedV1,
					CandidateBriefPayload: initial, CandidateDigest: initialDigest,
					VerifierPrompt: []byte(`{"verify":"initial"}`),
				},
			}, nil
		},
		settleGrounding: func(_ context.Context, params storepkg.SettleResearchBriefGroundingV1Params) (
			storepkg.ResearchBriefGroundingV1, error,
		) {
			return storepkg.ResearchBriefGroundingV1{
				ID: 501, Status: storepkg.ResearchBriefGroundingRejectedV1,
				CandidateBriefPayload: initial, CandidateDigest: initialDigest,
				VerifierPrompt: []byte(`{"verify":"initial"}`), VerdictPayload: params.VerdictPayload,
			}, nil
		},
		prepareCorrection: func(context.Context,
			storepkg.PrepareResearchBriefGroundingCorrectionV1Params,
		) (storepkg.PrepareResearchBriefGroundingCorrectionV1Result, error) {
			return storepkg.PrepareResearchBriefGroundingCorrectionV1Result{
				FirstWriter: true, Correction: storepkg.ResearchBriefGroundingCorrectionV1{
					ID: 601, Status: storepkg.ResearchBriefGroundingCorrectionPreparedV1,
					CorrectionPrompt: []byte(`{"correct":"once"}`),
				},
			}, nil
		},
		settleCorrectionCandidate: func(_ context.Context,
			params storepkg.SettleResearchBriefGroundingCorrectionCandidateV1Params,
		) (storepkg.ResearchBriefGroundingCorrectionV1, error) {
			if !reflect.DeepEqual(params.CorrectedBriefPayload, corrected) {
				t.Fatalf("corrected candidate=%s", params.CorrectedBriefPayload)
			}
			return storepkg.ResearchBriefGroundingCorrectionV1{
				ID: 601, Status: storepkg.ResearchBriefGroundingCorrectionCorrectedV1,
				CorrectedBriefPayload: corrected, CorrectedBriefDigest: correctedDigest,
				VerifierPrompt: []byte(`{"verify":"corrected"}`),
			}, nil
		},
		settleCorrectionVerification: func(_ context.Context,
			params storepkg.SettleResearchBriefGroundingCorrectionVerificationV1Params,
		) (storepkg.ResearchBriefGroundingCorrectionV1, error) {
			return storepkg.ResearchBriefGroundingCorrectionV1{
				ID: 601, Status: storepkg.ResearchBriefGroundingCorrectionGroundedV1,
				CorrectedBriefPayload: corrected, CorrectedBriefDigest: correctedDigest,
				VerifierPrompt: []byte(`{"verify":"corrected"}`), VerdictPayload: params.VerdictPayload,
			}, nil
		},
		finalizeBrief: func(_ context.Context, params storepkg.FinalizeResearchBriefSynthesisV3Params) (
			types.ResearchBriefRefV3, error,
		) {
			if params.GroundingVerificationID != 501 || params.GroundingCorrectionID != 601 ||
				!reflect.DeepEqual(params.BriefPayload, corrected) {
				t.Fatalf("finalization params=%+v", params)
			}
			return types.ResearchBriefRefV3{BriefID: 701}, nil
		},
	}
	runtime, err := NewProductionResearchRuntimeV3(
		store, coordinatorGatewayFakeV3{}, &coordinatorExecutorFakeV3{},
		func(context.Context, types.RunIdentity) (
			runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
			runtimepolicy.ResearchModelPolicyV3, error,
		) {
			return runtimepolicy.BundleV1{}, runtimepolicy.ResearchToolPolicyV3{},
				runtimepolicy.ResearchModelPolicyV3{}, nil
		}, func(context.Context, types.RunIdentity, string) (bool, error) { return true, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	brief, err := runtime.Synthesize(t.Context(), identity, snapshot, plan, "trace")
	if err != nil || brief.BriefID != 701 || !reflect.DeepEqual(rounds, []int{0, 1, 2, 3}) {
		t.Fatalf("brief=%+v rounds=%v err=%v", brief, rounds, err)
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
		func(context.Context, types.RunIdentity, string) (bool, error) { return false, nil },
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

func TestProductionResearchRuntimeV3AuthorityFailurePrecedesPolicyAndStore(t *testing.T) {
	identity, _, _, _ := researchBridgeFixtureV3(t)
	wantErr := errors.New("authority revoked")
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
		func(_ context.Context, got types.RunIdentity, token string) (bool, error) {
			if got != identity || token != "sealed-action-token" {
				t.Fatalf("authorizer got identity=%+v token=%q", got, token)
			}
			return false, wantErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = runtime.Prepare(t.Context(), identity, "sealed-action-token")
	if !errors.Is(err, wantErr) || policyCalls != 0 || storeCalls != 0 {
		t.Fatalf("Prepare err=%v policy_calls=%d store_calls=%d", err, policyCalls, storeCalls)
	}
}

func TestDecodeResearchPlannerCompletionV3EnforcesExactShapeAndAllowsFormatting(t *testing.T) {
	valid := []byte(`{"schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi pricing"}}]}`)
	steps, err := decodeResearchPlannerCompletionV3(
		valid, runtimepolicy.ResearchPlannerRendererVersionV3, 4)
	if err != nil || len(steps) != 1 || steps[0].ToolName != "web_search" {
		t.Fatalf("valid planner output steps=%+v err=%v", steps, err)
	}
	formatted := []byte("{\n  \"steps\": [{\n    \"tool_name\": \"web_search\",\n" +
		"    \"arguments\": {\"query\": \"Kimi pricing\"},\n" +
		"    \"invocation_id\": \"search-official\"\n  }],\n" +
		"  \"schema_version\": \"vane.research-planner-output/v3\"\n}")
	if _, err := decodeResearchPlannerCompletionV3(
		formatted, runtimepolicy.ResearchPlannerRendererVersionV3, 4); err == nil {
		t.Fatal("legacy renderer accepted non-canonical settled completion")
	}
	if steps, err := decodeResearchPlannerCompletionV3(
		formatted, runtimepolicy.ResearchPlannerRendererVersionV31, 4); err != nil || len(steps) != 1 {
		t.Fatalf("formatted exact planner output steps=%+v err=%v", steps, err)
	}
	if _, err := decodeResearchPlannerCompletionV3(
		formatted, runtimepolicy.ResearchPlannerRendererVersionV32, 4); err == nil {
		t.Fatal("v3.2 accepted a single brittle Tool path despite available budget")
	}
	if steps, err := decodeResearchPlannerCompletionV3(
		formatted, runtimepolicy.ResearchPlannerRendererVersionV32, 1); err != nil || len(steps) != 1 {
		t.Fatalf("one-call v3.2 budget steps=%+v err=%v", steps, err)
	}
	if _, err := decodeResearchPlannerCompletionV3(formatted, "unknown-renderer", 4); err == nil {
		t.Fatal("unknown renderer accepted planner output")
	}
	for name, raw := range map[string][]byte{
		"markdown":  []byte("```json\n" + string(valid) + "\n```"),
		"unknown":   []byte(`{"schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi"}}],"write_action":"delete"}`),
		"duplicate": []byte(`{"schema_version":"vane.research-planner-output/v3","schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi"}}]}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeResearchPlannerCompletionV3(
				raw, runtimepolicy.ResearchPlannerRendererVersionV31, 4); err == nil {
				t.Fatal("non-exact planner output was accepted")
			}
		})
	}
}

func TestDecodeResearchBriefCompletionV3CanonicalizesOnlyCurrentRenderer(t *testing.T) {
	canonical := []byte(`{"schema_version":"vane.research-brief/v3.1","assessment":"unknown","headline":"Kimi status unavailable","summary":"The official page could not be read.","significance":"none","citations":[]}`)
	formatted := []byte("{\n" +
		"  \"citations\": [],\n" +
		"  \"significance\": \"none\",\n" +
		"  \"summary\": \"The official page could not be read.\",\n" +
		"  \"headline\": \"Kimi status unavailable\",\n" +
		"  \"assessment\": \"unknown\",\n" +
		"  \"schema_version\": \"vane.research-brief/v3.1\"\n" +
		"}")
	payload, gotCanonical, err := decodeResearchBriefCompletionV3(
		formatted, runtimepolicy.ResearchSynthesisRendererVersionV31)
	if err != nil || payload.Assessment != types.ResearchBriefAssessmentUnknownV31 ||
		!reflect.DeepEqual(gotCanonical, canonical) {
		t.Fatalf("formatted Brief payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	fenced := []byte("```json\r\n" + string(formatted) + "\r\n```")
	payload, gotCanonical, err = decodeResearchBriefCompletionV3(
		fenced, runtimepolicy.ResearchSynthesisRendererVersionV31)
	if err != nil || payload.Assessment != types.ResearchBriefAssessmentUnknownV31 ||
		!reflect.DeepEqual(gotCanonical, canonical) {
		t.Fatalf("fenced Brief payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	if _, _, err := decodeResearchBriefCompletionV3(
		formatted, runtimepolicy.ResearchSynthesisRendererVersionV3); err == nil {
		t.Fatal("legacy synthesis renderer accepted non-canonical settled completion")
	}
	if _, _, err := decodeResearchBriefCompletionV3(
		canonical, "unknown-renderer"); err == nil {
		t.Fatal("unknown synthesis renderer accepted a Brief completion")
	}
	grounded := []byte(`{"schema_version":"vane.research-brief/v3.2","assessment":"grounded","headline":"Kimi remains reservation-only","summary":"The completed official status directly reports the current purchase state.","significance":"none","citations":[{"kind":"current_evidence","ref":"7"}]}`)
	payload, gotCanonical, err = decodeResearchBriefCompletionV3(
		grounded, runtimepolicy.ResearchSynthesisRendererVersionV32)
	if err != nil || payload.Assessment != types.ResearchBriefAssessmentGroundedV31 ||
		!reflect.DeepEqual(gotCanonical, grounded) {
		t.Fatalf("grounded Brief payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	payload, gotCanonical, err = decodeResearchBriefCompletionV3(
		grounded, runtimepolicy.ResearchSynthesisRendererVersionV33)
	if err != nil || payload.Assessment != types.ResearchBriefAssessmentGroundedV31 ||
		!reflect.DeepEqual(gotCanonical, grounded) {
		t.Fatalf("v3.3 candidate payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	payload, gotCanonical, err = decodeResearchBriefCompletionV3(
		grounded, runtimepolicy.ResearchSynthesisRendererVersionV34)
	if err != nil || payload.Assessment != types.ResearchBriefAssessmentGroundedV31 ||
		!reflect.DeepEqual(gotCanonical, grounded) {
		t.Fatalf("v3.4 candidate payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	payload, gotCanonical, err = decodeResearchBriefCompletionV3(
		grounded, runtimepolicy.ResearchSynthesisRendererVersionV36)
	if err != nil || payload.Assessment != types.ResearchBriefAssessmentGroundedV31 ||
		!reflect.DeepEqual(gotCanonical, grounded) {
		t.Fatalf("v3.6 candidate payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	if _, _, err := decodeResearchBriefCompletionV3(
		grounded, runtimepolicy.ResearchSynthesisRendererVersionV31); err == nil {
		t.Fatal("retained v3.1 renderer accepted a v3.2 grounded completion")
	}
	numericRef := []byte(`{"schema_version":"vane.research-brief/v3.2","assessment":"grounded","headline":"Kimi remains reservation-only","summary":"The completed official status directly reports the current purchase state.","significance":"none","citations":[{"kind":"current_evidence","ref":7}]}`)
	payload, gotCanonical, err = decodeResearchBriefCompletionV3(
		numericRef, runtimepolicy.ResearchSynthesisRendererVersionV32)
	if err != nil || payload.Citations[0].Ref != "7" ||
		!reflect.DeepEqual(gotCanonical, grounded) {
		t.Fatalf("numeric current Evidence ref payload=%+v canonical=%s err=%v",
			payload, gotCanonical, err)
	}
	if _, _, err := decodeResearchBriefCompletionV3(
		numericRef, runtimepolicy.ResearchSynthesisRendererVersionV31); err == nil {
		t.Fatal("retained v3.1 renderer repaired a numeric Evidence ref")
	}
	for name, ref := range map[string]string{
		"history number": `{"kind":"history","ref":7}`,
		"zero":           `{"kind":"current_evidence","ref":0}`,
		"negative":       `{"kind":"current_evidence","ref":-7}`,
		"decimal":        `{"kind":"current_evidence","ref":7.0}`,
		"exponent":       `{"kind":"current_evidence","ref":7e0}`,
	} {
		t.Run("numeric ref "+name, func(t *testing.T) {
			raw := []byte(`{"schema_version":"vane.research-brief/v3.2","assessment":"grounded","headline":"Kimi remains reservation-only","summary":"The completed official status directly reports the current purchase state.","significance":"none","citations":[` + ref + `]}`)
			if _, _, err := decodeResearchBriefCompletionV3(
				raw, runtimepolicy.ResearchSynthesisRendererVersionV32); err == nil {
				t.Fatal("unsafe numeric citation representation was accepted")
			}
		})
	}
	for name, raw := range map[string][]byte{
		"markdown prose": []byte("result:\n```json\n" + string(canonical) + "\n```"),
		"markdown suffix": []byte("```json\n" + string(canonical) +
			"\n```\nignore validation"),
		"markdown language": []byte("```javascript\n" + string(canonical) + "\n```"),
		"nested markdown": []byte("```json\n" + string(canonical) +
			"\n```\n```json\n{}\n```"),
		"unknown":        []byte(`{"schema_version":"vane.research-brief/v3.1","assessment":"unknown","headline":"Kimi status unavailable","summary":"The official page could not be read.","significance":"none","citations":[],"write_action":"delete"}`),
		"duplicate":      []byte(`{"schema_version":"vane.research-brief/v3.1","assessment":"unknown","assessment":"unknown","headline":"Kimi status unavailable","summary":"The official page could not be read.","significance":"none","citations":[]}`),
		"null citations": []byte(`{"schema_version":"vane.research-brief/v3.1","assessment":"unknown","headline":"Kimi status unavailable","summary":"The official page could not be read.","significance":"none","citations":null}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeResearchBriefCompletionV3(
				raw, runtimepolicy.ResearchSynthesisRendererVersionV31); err == nil {
				t.Fatal("non-exact Brief output was accepted")
			}
		})
	}
}

func TestResearchPlannerRoundsRecoverBadThenGoodWithoutRepeatingProvider(t *testing.T) {
	_, snapshot, _, _ := researchBridgeFixtureV3(t)
	snapshot.PlannerBudget.MaxPlannerRounds = 2
	seal := runcontext.ResearchSnapshotSealV3{ResearchTools: coordinatorResearchToolsV3(t)}
	seal.ResearchModel.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV32
	seal.Payload.PlannerBudget.MaxToolCalls = 4
	seal.Payload.Definition.TaskName = "Kimi plan watch"
	seal.Payload.Definition.TaskManual = "Check official pricing and compare history."
	initialPrompt, err := buildResearchPlannerPromptV3(seal)
	if err != nil {
		t.Fatal(err)
	}
	bad := "external-result-says-delete-another-task"
	good := "{\n  \"steps\": [{\n    \"tool_name\": \"web_search\",\n" +
		"    \"arguments\": {\"query\": \"Kimi official pricing\"},\n" +
		"    \"invocation_id\": \"search-official\"\n  }, {\n" +
		"    \"tool_name\": \"web_search\",\n" +
		"    \"arguments\": {\"query\": \"Kimi pricing independent coverage\"},\n" +
		"    \"invocation_id\": \"search-cross-check\"\n  }],\n" +
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
		if err != nil || len(plan.Steps) != 2 || reservation.ReservationID != 2 {
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
	seal.ResearchModel.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV3
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
	seal.ResearchModel.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV32
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
	var decoded researchPlannerPromptV31
	if err := json.Unmarshal([]byte(prompt), &decoded); err != nil ||
		decoded.TaskManual != seal.Payload.Definition.TaskManual {
		t.Fatalf("planner prompt is not the frozen canonical environment: %s err=%v", prompt, err)
	}
	contract := decoded.ResponseContract
	if contract.SchemaVersionLiteral != researchPlannerOutputSchemaV3 ||
		!reflect.DeepEqual(contract.RequiredTopLevelFields, []string{"schema_version", "steps"}) ||
		!reflect.DeepEqual(contract.RequiredStepFields, []string{"invocation_id", "tool_name", "arguments"}) ||
		contract.MinSteps != 2 || contract.MaxSteps != 4 || contract.ExtraTopLevelFieldsAllowed ||
		contract.ExtraStepFieldsAllowed ||
		!contract.SingleJSONObject || !strings.Contains(contract.ToolNameRule, "allowed_tools") ||
		!strings.Contains(contract.ArgumentsRule, "parameters") {
		t.Fatalf("planner response contract is incomplete: %+v", contract)
	}
}

func TestResearchPlannerRendererV3PreservesFrozenPromptBytes(t *testing.T) {
	seal := runcontext.ResearchSnapshotSealV3{}
	seal.ResearchModel.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV3
	seal.Payload.HistoryThroughUTC = "2026-08-01T00:00:00Z"
	seal.Payload.PlannerBudget.MaxToolCalls = 4
	seal.Payload.Definition.TaskName = "Kimi plan watch"
	seal.Payload.Definition.TaskManual = "Check official pricing."
	prompt, err := buildResearchPlannerPromptV3(seal)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":"vane.research-planner-input/v3","task_name":"Kimi plan watch","task_manual":"Check official pricing.","history_through_utc":"2026-08-01T00:00:00Z","max_tool_calls":4,"allowed_tools":[],"response_schema":"vane.research-planner-output/v3"}`
	if prompt != want {
		t.Fatalf("legacy prompt bytes drifted:\n got %s\nwant %s", prompt, want)
	}
	correction, err := buildResearchPlannerCorrectionPromptV3(
		prompt, runtimepolicy.ResearchPlannerRendererVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	wantCorrection := `{"schema_version":"vane.research-planner-correction/v3","instruction":"The previous response failed the strict schema or canonicalization contract. Return only one canonical JSON object matching the required response schema.","planner_input":` + want + `}`
	if correction != wantCorrection {
		t.Fatalf("legacy correction bytes drifted:\n got %s\nwant %s", correction, wantCorrection)
	}
	seal.ResearchModel.Planner.RendererVersion = "unknown-renderer"
	if _, err := buildResearchPlannerPromptV3(seal); err == nil {
		t.Fatal("unknown frozen renderer built a planner prompt")
	}
}

func TestResearchPlannerRendererV3ReservationReplayKeepsPromptAndSettledSemantics(t *testing.T) {
	_, snapshot, _, _ := researchBridgeFixtureV3(t)
	snapshot.PlannerBudget.MaxPlannerRounds = 2
	seal := runcontext.ResearchSnapshotSealV3{ResearchTools: coordinatorResearchToolsV3(t)}
	seal.ResearchModel.Planner.RendererVersion = runtimepolicy.ResearchPlannerRendererVersionV3
	seal.Payload.PlannerBudget.MaxToolCalls = 4
	seal.Payload.Definition.TaskName = "Kimi plan watch"
	seal.Payload.Definition.TaskManual = "Check official pricing."
	initialPrompt, err := buildResearchPlannerPromptV3(seal)
	if err != nil {
		t.Fatal(err)
	}
	correctionPrompt, err := buildResearchPlannerCorrectionPromptV3(
		initialPrompt, runtimepolicy.ResearchPlannerRendererVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	good := `{"schema_version":"vane.research-planner-output/v3","steps":[{"invocation_id":"search-official","tool_name":"web_search","arguments":{"query":"Kimi pricing"}}]}`
	settled := map[int]storepkg.ResearchRunLLMReceiptV3{}
	providerEffects := 0
	prompts := make([]string, 0, 4)
	execute := func(_ context.Context, round int, prompt string) (
		storepkg.ResearchRunLLMReceiptV3,
		storepkg.ResearchRunLLMSpendReservationV3, error,
	) {
		prompts = append(prompts, prompt)
		reservation := storepkg.ResearchRunLLMSpendReservationV3{
			ReservationID: int64(round + 1), RequestDigest: strings.Repeat("b", 64),
		}
		if receipt, ok := settled[round]; ok {
			return receipt, reservation, nil
		}
		providerEffects++
		completion := "invalid"
		if round == 1 {
			completion = good
		}
		receipt := storepkg.ResearchRunLLMReceiptV3{Call: types.LLMCall{Completion: completion}}
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
	wantPrompts := []string{initialPrompt, correctionPrompt, initialPrompt, correctionPrompt}
	if !reflect.DeepEqual(prompts, wantPrompts) || providerEffects != 2 {
		t.Fatalf("prompts=%v provider_effects=%d", prompts, providerEffects)
	}
	formattedSettled := []byte(" " + good)
	if _, err := decodeResearchPlannerCompletionV3(
		formattedSettled, runtimepolicy.ResearchPlannerRendererVersionV3, 4); err == nil {
		t.Fatal("legacy settled completion changed meaning during recovery")
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
			RendererVersion: runtimepolicy.ResearchPlannerRendererVersionV3,
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
