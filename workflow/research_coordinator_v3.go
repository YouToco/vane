package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/researchgateway"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const researchPlannerOutputSchemaV3 = "vane.research-planner-output/v3"

// ResearchRuntimePolicyBuilderV3 returns the current, trusted runtime
// environment. The Store freezes it with the approved task definition before
// any model or network effect is admitted.
type ResearchRuntimePolicyBuilderV3 func(
	context.Context, types.RunIdentity,
) (runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
	runtimepolicy.ResearchModelPolicyV3, error)

// ResearchRuntimeAuthorizerV3 is the hard pre-effect cutover boundary. It must
// be backed by an exact task allow-set; a broad "all tasks" authorizer is not
// part of this coordinator's API.
type ResearchRuntimeAuthorizerV3 func(types.RunIdentity) bool

type researchRuntimeStoreV3 interface {
	CreateOrGetResearchRunSnapshotV3(context.Context, types.RunIdentity,
		runtimepolicy.BundleV1, runtimepolicy.ResearchToolPolicyV3,
		runtimepolicy.ResearchModelPolicyV3) (types.ResearchRunSnapshotRefV3, error)
	LoadResearchRunSnapshotV3(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3) (runcontext.ResearchSnapshotSealV3, error)
	LoadResearchRunPlanRefV3(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3) (types.ResearchRunPlanRefV3, bool, error)
	CreateOrGetResearchRunPlanV3(context.Context,
		storepkg.CreateOrGetResearchRunPlanV3Params) (types.ResearchRunPlanRefV3, error)
	BeginResearchRunStepV3(context.Context, types.RunIdentity, int64,
		types.ResearchRunPlanRefV3, int) (storepkg.ResearchRunStepExecutionV3, error)
	LoadResearchRunStepResolutionV3(context.Context, types.RunIdentity, int64,
		types.ResearchRunPlanRefV3, int) (storepkg.ResearchRunStepResolutionV3, error)
	CommitResearchRunStepV3(context.Context,
		storepkg.CommitResearchRunStepV3Params) (storepkg.ResearchRunStepReceiptV3, error)
	CommitResearchRunStepEvidenceV3(context.Context,
		storepkg.CommitResearchRunStepEvidenceV3Params) (storepkg.ResearchRunStepEvidenceReceiptV3, error)
	BeginResearchRunLLMSpendV3(context.Context,
		storepkg.BeginResearchRunLLMSpendV3Params) (storepkg.ResearchRunLLMSpendReservationV3, error)
	LoadResearchRunLLMReceiptV3(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3, string, int) (storepkg.ResearchRunLLMReceiptV3, bool, error)
	ResolveResearchLLMProcessGatewayBindingV1(context.Context, types.RunIdentity,
		types.ResearchRunSnapshotRefV3, int64) (storepkg.ResearchLLMProcessGatewayBindingV1, error)
	PrepareOrGetResearchBriefSynthesisV3(context.Context,
		storepkg.PrepareResearchBriefSynthesisV3Params) (storepkg.PrepareResearchBriefSynthesisV3Result, error)
	ClaimResearchBriefSynthesisV3(context.Context,
		storepkg.ClaimResearchBriefSynthesisV3Params) (storepkg.ClaimResearchBriefSynthesisV3Result, error)
	FinalizeResearchBriefSynthesisV3(context.Context,
		storepkg.FinalizeResearchBriefSynthesisV3Params) (types.ResearchBriefRefV3, error)
	FailResearchBriefSynthesisV3(context.Context,
		storepkg.FailResearchBriefSynthesisV3Params) (storepkg.ResearchBriefSynthesisV3, error)
}

type researchGatewayCallerV3 interface {
	Execute(context.Context, researchgateway.ExecuteRequestV1) (researchgateway.ExecuteResponseV1, error)
}

type researchExecutorV3 interface {
	ExecuteOnceV3(context.Context, fetcher.ResearchExecutionRequestV3) fetcher.ResearchExecutionReceiptV3
}

// ProductionResearchRuntimeV3 is deliberately delivery-dark. It can prepare,
// plan, execute and synthesize immutable V3 artifacts, but it cannot acquire
// notification authority. Enabling delivery requires a separate reviewed
// implementation and is therefore impossible through constructor options.
type ProductionResearchRuntimeV3 struct {
	store         researchRuntimeStoreV3
	gateway       researchGatewayCallerV3
	executor      researchExecutorV3
	policyBuilder ResearchRuntimePolicyBuilderV3
	authorize     ResearchRuntimeAuthorizerV3
}

func NewProductionResearchRuntimeV3(
	store researchRuntimeStoreV3,
	gateway researchGatewayCallerV3,
	executor researchExecutorV3,
	policyBuilder ResearchRuntimePolicyBuilderV3,
	authorize ResearchRuntimeAuthorizerV3,
) (*ProductionResearchRuntimeV3, error) {
	if researchDependencyNilV3(store) || researchDependencyNilV3(gateway) ||
		researchDependencyNilV3(executor) || policyBuilder == nil || authorize == nil {
		return nil, errors.New("research V3 coordinator dependencies are unavailable")
	}
	return &ProductionResearchRuntimeV3{
		store: store, gateway: gateway, executor: executor,
		policyBuilder: policyBuilder, authorize: authorize,
	}, nil
}

func researchDependencyNilV3(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (r *ProductionResearchRuntimeV3) Prepare(
	ctx context.Context, identity types.RunIdentity,
) (types.ResearchRunSnapshotRefV3, bool, bool, error) {
	if r == nil || identity.Validate() != nil ||
		identity.RunKind != types.RunSnapshotKindScheduled {
		return types.ResearchRunSnapshotRefV3{}, false, false,
			researchCoordinatorValidationV3("research V3 preparation is invalid")
	}
	if !r.authorize(identity) {
		return types.ResearchRunSnapshotRefV3{}, false, false, nil
	}
	policy, tools, model, err := r.policyBuilder(ctx, identity)
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, false, false, err
	}
	if policy.Validate() != nil || tools.Validate() != nil || model.Validate() != nil {
		return types.ResearchRunSnapshotRefV3{}, false, false,
			researchCoordinatorValidationV3("research V3 runtime policy is invalid")
	}
	snapshot, err := r.store.CreateOrGetResearchRunSnapshotV3(
		ctx, identity, policy, tools, model)
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, false, false, err
	}
	if err := snapshot.ValidateFor(identity); err != nil {
		return types.ResearchRunSnapshotRefV3{}, false, false,
			researchCoordinatorValidationV3("research V3 snapshot is invalid")
	}
	// Hard-dark invariant: artifact authority never implies delivery authority.
	return snapshot, true, false, nil
}

func (r *ProductionResearchRuntimeV3) Plan(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, _ string,
) (types.ResearchRunPlanRefV3, error) {
	if r == nil || snapshot.ValidateFor(identity) != nil {
		return types.ResearchRunPlanRefV3{},
			researchCoordinatorValidationV3("research V3 planning scope is invalid")
	}
	// Recovery always wins before prompts are opened or another paid effect is
	// considered.
	if recovered, found, err := r.store.LoadResearchRunPlanRefV3(
		ctx, identity, snapshot); err != nil {
		return types.ResearchRunPlanRefV3{}, err
	} else if found {
		if err := recovered.ValidateFor(identity, snapshot.SnapshotID); err != nil {
			return types.ResearchRunPlanRefV3{},
				researchCoordinatorValidationV3("recovered research plan is invalid")
		}
		return recovered, nil
	}

	seal, err := r.store.LoadResearchRunSnapshotV3(ctx, identity, snapshot)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	if err := validateResearchSnapshotSealForCoordinatorV3(seal, snapshot); err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	userPrompt, err := buildResearchPlannerPromptV3(seal)
	if err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	plan, reservation, err := executeResearchPlannerRoundsV3(
		ctx, snapshot, seal, userPrompt,
		func(ctx context.Context, round int, roundPrompt string) (
			storepkg.ResearchRunLLMReceiptV3,
			storepkg.ResearchRunLLMSpendReservationV3, error,
		) {
			return r.executeLLMStageV3(ctx, identity, snapshot,
				storepkg.ResearchRunLLMStagePlannerV3, round, 0,
				seal.ResearchModel.Planner.SystemPrompt, roundPrompt)
		})
	if err != nil {
		return types.ResearchRunPlanRefV3{}, err
	}
	return r.store.CreateOrGetResearchRunPlanV3(ctx,
		storepkg.CreateOrGetResearchRunPlanV3Params{
			Identity: identity, RunSnapshotID: snapshot.SnapshotID,
			PlannerLLMReservationID: reservation.ReservationID, Plan: plan,
		})
}

type researchPlannerRoundExecutorV3 func(
	context.Context, int, string,
) (storepkg.ResearchRunLLMReceiptV3, storepkg.ResearchRunLLMSpendReservationV3, error)

func executeResearchPlannerRoundsV3(
	ctx context.Context, snapshot types.ResearchRunSnapshotRefV3,
	seal runcontext.ResearchSnapshotSealV3, initialPrompt string,
	execute researchPlannerRoundExecutorV3,
) (runcontext.ResearchExecutionPlanV3, storepkg.ResearchRunLLMSpendReservationV3, error) {
	maxRounds := snapshot.PlannerBudget.MaxPlannerRounds
	if execute == nil || maxRounds <= 0 || maxRounds > 8 || initialPrompt == "" {
		return runcontext.ResearchExecutionPlanV3{},
			storepkg.ResearchRunLLMSpendReservationV3{},
			researchCoordinatorValidationV3("research planner correction budget is invalid")
	}
	correctionPrompt, err := buildResearchPlannerCorrectionPromptV3(initialPrompt)
	if err != nil {
		return runcontext.ResearchExecutionPlanV3{},
			storepkg.ResearchRunLLMSpendReservationV3{}, err
	}
	for round := 0; round < maxRounds; round++ {
		roundPrompt := initialPrompt
		if round > 0 {
			roundPrompt = correctionPrompt
		}
		receipt, reservation, err := execute(ctx, round, roundPrompt)
		if err != nil {
			return runcontext.ResearchExecutionPlanV3{},
				storepkg.ResearchRunLLMSpendReservationV3{}, err
		}
		plan, err := researchPlanFromCompletionV3(snapshot, seal,
			[]byte(receipt.Call.Completion))
		if err == nil {
			return plan, reservation, nil
		}
	}
	return runcontext.ResearchExecutionPlanV3{},
		storepkg.ResearchRunLLMSpendReservationV3{},
		researchCoordinatorValidationV3("research planner exhausted its strict output correction budget")
}

func researchPlanFromCompletionV3(
	snapshot types.ResearchRunSnapshotRefV3,
	seal runcontext.ResearchSnapshotSealV3, completion []byte,
) (runcontext.ResearchExecutionPlanV3, error) {
	steps, err := decodeResearchPlannerCompletionV3(completion)
	if err != nil {
		return runcontext.ResearchExecutionPlanV3{}, err
	}
	allowed := make(map[string]struct{}, len(seal.ResearchTools.AllowedTools))
	for _, tool := range seal.ResearchTools.AllowedTools {
		allowed[tool.Name] = struct{}{}
	}
	for _, step := range steps {
		if _, ok := allowed[step.ToolName]; !ok {
			return runcontext.ResearchExecutionPlanV3{}, researchCoordinatorValidationV3(
				"research planner selected a Tool outside the frozen grant")
		}
	}
	return runcontext.BuildResearchExecutionPlanV3(
		snapshot.DefinitionDigest, snapshot.CapabilityCatalogDigest,
		snapshot.ToolPolicyDigest, steps, acquisitiontool.CanonicalizeToolArgumentsV1)
}

func (r *ProductionResearchRuntimeV3) ExecuteStep(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	ordinal int, _ string,
) (ResearchStepReceiptV3, error) {
	if r == nil || snapshot.ValidateFor(identity) != nil ||
		plan.ValidateFor(identity, snapshot.SnapshotID) != nil ||
		ordinal < 0 || ordinal >= plan.StepCount {
		return ResearchStepReceiptV3{},
			researchCoordinatorValidationV3("research V3 execution scope is invalid")
	}
	execution, err := r.store.BeginResearchRunStepV3(
		ctx, identity, snapshot.SnapshotID, plan, ordinal)
	if err != nil {
		return ResearchStepReceiptV3{}, err
	}
	if !execution.FirstWriter {
		resolution, loadErr := r.store.LoadResearchRunStepResolutionV3(
			ctx, identity, snapshot.SnapshotID, plan, ordinal)
		if loadErr != nil {
			return ResearchStepReceiptV3{}, loadErr
		}
		return researchStepReceiptFromResolutionV3(resolution)
	}

	seal, err := r.store.LoadResearchRunSnapshotV3(ctx, identity, snapshot)
	if err != nil {
		return ResearchStepReceiptV3{}, err
	}
	if err := validateResearchSnapshotSealForCoordinatorV3(seal, snapshot); err != nil {
		return ResearchStepReceiptV3{}, err
	}
	tool, found := frozenResearchToolV3(seal.ResearchTools, execution.ToolName)
	if !found {
		return ResearchStepReceiptV3{}, researchCoordinatorValidationV3(
			"research step Tool is outside the frozen grant")
	}
	providerReceipt := r.executor.ExecuteOnceV3(ctx, fetcher.ResearchExecutionRequestV3{
		FirstWriter: true, Identity: identity, RunSnapshotID: snapshot.SnapshotID,
		PlanDigest: plan.PlanDigest, Ordinal: ordinal,
		InvocationID: execution.InvocationID, Tool: tool, Arguments: execution.Arguments,
	})
	persistence, err := mapResearchExecutionReceiptV3(
		identity, snapshot, plan, execution, providerReceipt)
	if err != nil {
		return ResearchStepReceiptV3{}, err
	}
	if persistence.Evidence != nil {
		receipt, err := r.store.CommitResearchRunStepEvidenceV3(ctx, *persistence.Evidence)
		if err != nil {
			return ResearchStepReceiptV3{}, err
		}
		return researchStepReceiptFromEvidenceV3(receipt), nil
	}
	if persistence.Terminal == nil {
		return ResearchStepReceiptV3{}, researchCoordinatorValidationV3(
			"research step produced no terminal receipt")
	}
	if _, err := r.store.CommitResearchRunStepV3(ctx, *persistence.Terminal); err != nil {
		return ResearchStepReceiptV3{}, err
	}
	return ResearchStepReceiptV3{}, types.NewAppError(types.CodeConflict,
		"research Tool step ended without usable evidence", types.ErrConflict)
}

func (r *ProductionResearchRuntimeV3) Synthesize(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	_ string,
) (ResearchBriefRefV3, error) {
	if r == nil || snapshot.ValidateFor(identity) != nil ||
		plan.ValidateFor(identity, snapshot.SnapshotID) != nil {
		return ResearchBriefRefV3{},
			researchCoordinatorValidationV3("research V3 synthesis scope is invalid")
	}
	prepared, err := r.store.PrepareOrGetResearchBriefSynthesisV3(ctx,
		storepkg.PrepareResearchBriefSynthesisV3Params{
			Identity: identity, SnapshotRef: snapshot, PlanRef: plan,
		})
	if err != nil {
		return ResearchBriefRefV3{}, err
	}
	if prepared.Synthesis.Status == storepkg.ResearchBriefSynthesisFinalizedV3 {
		return researchBriefRefFromCoordinatorSynthesisV3(prepared.Synthesis)
	}
	if prepared.Synthesis.Status != storepkg.ResearchBriefSynthesisPreparedV3 &&
		prepared.Synthesis.Status != storepkg.ResearchBriefSynthesisSpendingV3 {
		return ResearchBriefRefV3{}, types.NewAppError(types.CodeConflict,
			"research Brief synthesis is terminal", types.ErrConflict)
	}
	seal, err := r.store.LoadResearchRunSnapshotV3(ctx, identity, snapshot)
	if err != nil {
		return ResearchBriefRefV3{}, err
	}
	if err := validateResearchSnapshotSealForCoordinatorV3(seal, snapshot); err != nil {
		return ResearchBriefRefV3{}, err
	}
	userPrompt := string(prepared.Synthesis.ContextPayload)
	reservation, err := r.store.BeginResearchRunLLMSpendV3(ctx,
		storepkg.BeginResearchRunLLMSpendV3Params{
			Identity: identity, SnapshotRef: snapshot,
			Stage: storepkg.ResearchRunLLMStageSynthesisV3, RoundOrdinal: 0,
			SubjectID:    prepared.Synthesis.ID,
			SystemPrompt: seal.ResearchModel.Synthesis.SystemPrompt,
			UserPrompt:   userPrompt,
		})
	if err != nil {
		return ResearchBriefRefV3{}, err
	}
	claimParams := storepkg.ClaimResearchBriefSynthesisV3Params{
		Identity: identity, SnapshotRef: snapshot, PlanRef: plan,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
		SynthesisLLMReservationID: reservation.ReservationID,
	}
	claim, err := r.store.ClaimResearchBriefSynthesisV3(ctx, claimParams)
	if err != nil {
		return ResearchBriefRefV3{}, err
	}
	if claim.Synthesis.Status == storepkg.ResearchBriefSynthesisFinalizedV3 {
		return researchBriefRefFromCoordinatorSynthesisV3(claim.Synthesis)
	}
	if claim.ReceiptState == storepkg.ResearchBriefLLMReceiptFailedV3 ||
		claim.ReceiptState == storepkg.ResearchBriefLLMReceiptIndeterminateV3 {
		if failErr := r.failResearchSynthesisForReceiptStateV3(
			ctx, claimParams, claim.ReceiptState); failErr != nil {
			return ResearchBriefRefV3{}, failErr
		}
		return ResearchBriefRefV3{}, types.NewAppError(types.CodeConflict,
			"research Brief model effect did not produce a usable completion",
			types.ErrConflict)
	}
	receipt, err := r.loadOrExecuteLLMReservationV3(ctx, identity, snapshot,
		storepkg.ResearchRunLLMStageSynthesisV3, 0, reservation,
		seal.ResearchModel.Synthesis.SystemPrompt, userPrompt)
	if err != nil {
		if settled, found, loadErr := r.store.LoadResearchRunLLMReceiptV3(
			ctx, identity, snapshot, storepkg.ResearchRunLLMStageSynthesisV3, 0,
		); loadErr == nil && found && settled.Settled &&
			settled.Outcome != storepkg.ResearchRunLLMCompletedV3 {
			state := storepkg.ResearchBriefLLMReceiptFailedV3
			if settled.Outcome == storepkg.ResearchRunLLMIndeterminateV3 {
				state = storepkg.ResearchBriefLLMReceiptIndeterminateV3
			}
			if failErr := r.failResearchSynthesisForReceiptStateV3(
				ctx, claimParams, state); failErr != nil {
				return ResearchBriefRefV3{}, failErr
			}
		}
		return ResearchBriefRefV3{}, err
	}
	_, canonical, decodeErr := types.DecodeResearchBriefPayloadV3(
		[]byte(receipt.Call.Completion))
	if decodeErr != nil {
		_, failErr := r.store.FailResearchBriefSynthesisV3(ctx,
			storepkg.FailResearchBriefSynthesisV3Params{
				ClaimResearchBriefSynthesisV3Params: claimParams,
				Status:                              storepkg.ResearchBriefSynthesisFailedV3,
				FailureCode:                         "invalid_model_output",
			})
		if failErr != nil {
			return ResearchBriefRefV3{}, failErr
		}
		return ResearchBriefRefV3{}, decodeErr
	}
	return r.store.FinalizeResearchBriefSynthesisV3(ctx,
		storepkg.FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: claimParams,
			BriefPayload:                        canonical,
		})
}

func (r *ProductionResearchRuntimeV3) failResearchSynthesisForReceiptStateV3(
	ctx context.Context, claim storepkg.ClaimResearchBriefSynthesisV3Params,
	receiptState storepkg.ResearchBriefLLMReceiptStateV3,
) error {
	status := storepkg.ResearchBriefSynthesisFailedV3
	failureCode := "model_failed"
	if receiptState == storepkg.ResearchBriefLLMReceiptIndeterminateV3 {
		status = storepkg.ResearchBriefSynthesisAmbiguousV3
		failureCode = "model_outcome_indeterminate"
	} else if receiptState != storepkg.ResearchBriefLLMReceiptFailedV3 {
		return researchCoordinatorValidationV3("research Brief receipt state is invalid")
	}
	_, err := r.store.FailResearchBriefSynthesisV3(ctx,
		storepkg.FailResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: claim,
			Status:                              status, FailureCode: failureCode,
		})
	return err
}

func (r *ProductionResearchRuntimeV3) Deliver(
	context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
	types.ResearchRunPlanRefV3, ResearchBriefRefV3, string,
) (ResearchDeliveryReceiptV3, error) {
	return ResearchDeliveryReceiptV3{}, types.NewAppError(types.CodeValidation,
		"research V3 delivery authority is disabled", types.ErrValidation)
}

func (r *ProductionResearchRuntimeV3) executeLLMStageV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, stage string, round int, subjectID int64,
	systemPrompt, userPrompt string,
) (storepkg.ResearchRunLLMReceiptV3, storepkg.ResearchRunLLMSpendReservationV3, error) {
	reservation, err := r.store.BeginResearchRunLLMSpendV3(ctx,
		storepkg.BeginResearchRunLLMSpendV3Params{
			Identity: identity, SnapshotRef: snapshot, Stage: stage,
			RoundOrdinal: round, SubjectID: subjectID,
			SystemPrompt: systemPrompt, UserPrompt: userPrompt,
		})
	if err != nil {
		return storepkg.ResearchRunLLMReceiptV3{},
			storepkg.ResearchRunLLMSpendReservationV3{}, err
	}
	receipt, err := r.loadOrExecuteLLMReservationV3(ctx, identity, snapshot,
		stage, round, reservation, systemPrompt, userPrompt)
	return receipt, reservation, err
}

func (r *ProductionResearchRuntimeV3) loadOrExecuteLLMReservationV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, stage string, round int,
	reservation storepkg.ResearchRunLLMSpendReservationV3,
	systemPrompt, userPrompt string,
) (storepkg.ResearchRunLLMReceiptV3, error) {
	if receipt, found, err := r.store.LoadResearchRunLLMReceiptV3(
		ctx, identity, snapshot, stage, round); err != nil {
		return storepkg.ResearchRunLLMReceiptV3{}, err
	} else if found && receipt.Settled {
		return validateResearchLLMReceiptForCoordinatorV3(
			receipt, reservation, systemPrompt, userPrompt)
	}
	binding, err := r.store.ResolveResearchLLMProcessGatewayBindingV1(
		ctx, identity, snapshot, reservation.ReservationID)
	if err != nil {
		return storepkg.ResearchRunLLMReceiptV3{}, err
	}
	reservationID, requestDigest, bearer, err := binding.OpenForProcessGatewayV1()
	if err != nil {
		return storepkg.ResearchRunLLMReceiptV3{}, researchCoordinatorValidationV3(
			"research model gateway binding is invalid")
	}
	response, err := r.gateway.Execute(ctx, researchgateway.ExecuteRequestV1{
		ReservationID: reservationID, RequestDigest: requestDigest,
		RunCapability: bearer,
	})
	if err != nil {
		return storepkg.ResearchRunLLMReceiptV3{}, types.NewAppError(
			types.CodeLLMUnavailable, "research model gateway is unavailable", err)
	}
	if response.Status != researchgateway.StatusSettledV1 {
		return storepkg.ResearchRunLLMReceiptV3{}, types.NewAppError(
			types.CodeLLMUnavailable, "research model gateway has not settled", nil)
	}
	receipt, found, err := r.store.LoadResearchRunLLMReceiptV3(
		ctx, identity, snapshot, stage, round)
	if err != nil {
		return storepkg.ResearchRunLLMReceiptV3{}, err
	}
	if !found || !receipt.Settled {
		return storepkg.ResearchRunLLMReceiptV3{}, types.NewAppError(
			types.CodeLLMUnavailable, "research model receipt is unavailable", nil)
	}
	return validateResearchLLMReceiptForCoordinatorV3(
		receipt, reservation, systemPrompt, userPrompt)
}

func validateResearchLLMReceiptForCoordinatorV3(
	receipt storepkg.ResearchRunLLMReceiptV3,
	reservation storepkg.ResearchRunLLMSpendReservationV3,
	systemPrompt, userPrompt string,
) (storepkg.ResearchRunLLMReceiptV3, error) {
	if !receipt.Settled || receipt.Reservation.ReservationID != reservation.ReservationID ||
		receipt.Reservation.RequestDigest != reservation.RequestDigest ||
		receipt.Outcome != storepkg.ResearchRunLLMCompletedV3 ||
		!receipt.Attempted || !receipt.UsageKnown || receipt.LLMCallID <= 0 ||
		receipt.Call.Error != "" || receipt.Call.SystemPrompt != systemPrompt ||
		receipt.Call.UserPrompt != userPrompt || strings.TrimSpace(receipt.Call.Completion) == "" {
		return storepkg.ResearchRunLLMReceiptV3{}, types.NewAppError(
			types.CodeConflict, "research model receipt is not a usable completion", types.ErrConflict)
	}
	return receipt, nil
}

type researchPlannerOutputV3 struct {
	SchemaVersion string                          `json:"schema_version"`
	Steps         []runcontext.ResearchPlanStepV3 `json:"steps"`
}

func decodeResearchPlannerCompletionV3(raw []byte) ([]runcontext.ResearchPlanStepV3, error) {
	if len(raw) < 2 || len(raw) > 256<<10 {
		return nil, researchCoordinatorValidationV3("research planner output is invalid")
	}
	var output researchPlannerOutputV3
	if err := strictjson.DecodeExact(raw, &output); err != nil ||
		output.SchemaVersion != researchPlannerOutputSchemaV3 ||
		len(output.Steps) == 0 || len(output.Steps) > 16 {
		return nil, researchCoordinatorValidationV3("research planner output is invalid")
	}
	canonical, err := json.Marshal(output)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, researchCoordinatorValidationV3(
			"research planner output must be canonical JSON")
	}
	steps := make([]runcontext.ResearchPlanStepV3, len(output.Steps))
	copy(steps, output.Steps)
	return steps, nil
}

type researchPlannerPromptToolV3 struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type researchPlannerPromptV3 struct {
	SchemaVersion     string                        `json:"schema_version"`
	TaskName          string                        `json:"task_name"`
	TaskManual        string                        `json:"task_manual"`
	HistoryThroughUTC string                        `json:"history_through_utc"`
	MaxToolCalls      int                           `json:"max_tool_calls"`
	AllowedTools      []researchPlannerPromptToolV3 `json:"allowed_tools"`
	ResponseSchema    string                        `json:"response_schema"`
}

type researchPlannerCorrectionPromptV3 struct {
	SchemaVersion string          `json:"schema_version"`
	Instruction   string          `json:"instruction"`
	PlannerInput  json.RawMessage `json:"planner_input"`
}

func buildResearchPlannerPromptV3(seal runcontext.ResearchSnapshotSealV3) (string, error) {
	tools := make([]researchPlannerPromptToolV3, len(seal.ResearchTools.AllowedTools))
	for index, tool := range seal.ResearchTools.AllowedTools {
		tools[index] = researchPlannerPromptToolV3{
			Name: tool.Name, Description: tool.Description,
			Parameters: append(json.RawMessage(nil), tool.Parameters...),
		}
	}
	payload, err := json.Marshal(researchPlannerPromptV3{
		SchemaVersion:     "vane.research-planner-input/v3",
		TaskName:          seal.Payload.Definition.TaskName,
		TaskManual:        seal.Payload.Definition.TaskManual,
		HistoryThroughUTC: seal.Payload.HistoryThroughUTC,
		MaxToolCalls:      seal.Payload.PlannerBudget.MaxToolCalls,
		AllowedTools:      tools,
		ResponseSchema:    researchPlannerOutputSchemaV3,
	})
	if err != nil || len(payload) < 2 || len(payload) > 2<<20 {
		return "", researchCoordinatorValidationV3("research planner prompt is invalid")
	}
	return string(payload), nil
}

func buildResearchPlannerCorrectionPromptV3(initialPrompt string) (string, error) {
	var plannerInput json.RawMessage
	if strictjson.DecodeExact([]byte(initialPrompt), &plannerInput) != nil {
		return "", researchCoordinatorValidationV3(
			"research planner correction input is invalid")
	}
	canonicalInput, err := json.Marshal(plannerInput)
	if err != nil || !bytes.Equal(canonicalInput, []byte(initialPrompt)) {
		return "", researchCoordinatorValidationV3(
			"research planner correction input must be canonical")
	}
	payload, err := json.Marshal(researchPlannerCorrectionPromptV3{
		SchemaVersion: "vane.research-planner-correction/v3",
		Instruction:   "The previous response failed the strict schema or canonicalization contract. Return only one canonical JSON object matching the required response schema.",
		PlannerInput:  plannerInput,
	})
	if err != nil || len(payload) < 2 || len(payload) > 2<<20 {
		return "", researchCoordinatorValidationV3(
			"research planner correction prompt is invalid")
	}
	return string(payload), nil
}

func validateResearchSnapshotSealForCoordinatorV3(
	seal runcontext.ResearchSnapshotSealV3, snapshot types.ResearchRunSnapshotRefV3,
) error {
	if seal.PayloadDigest != snapshot.PayloadDigest ||
		seal.DefinitionDigest != snapshot.DefinitionDigest ||
		seal.PolicyDigests.CapabilityCatalogDigest != snapshot.CapabilityCatalogDigest ||
		seal.ResearchToolPolicyDigest != snapshot.ToolPolicyDigest ||
		seal.ResearchModelPolicyDigest != snapshot.ModelPolicyDigest ||
		seal.Payload.PlannerBudget != snapshot.PlannerBudget ||
		seal.ResearchTools.Validate() != nil || seal.ResearchModel.Validate() != nil ||
		seal.Payload.TenantID != snapshot.TenantID ||
		seal.Payload.UserID != snapshot.UserID || seal.Payload.TaskID != snapshot.TaskID ||
		seal.Payload.TemporalWorkflowID != snapshot.TemporalWorkflowID ||
		seal.Payload.TemporalRunID != snapshot.TemporalRunID {
		return researchCoordinatorValidationV3("research snapshot payload is inconsistent")
	}
	return nil
}

func frozenResearchToolV3(
	policy runtimepolicy.ResearchToolPolicyV3, name string,
) (runtimepolicy.ResearchToolDefinitionV3, bool) {
	for _, tool := range policy.AllowedTools {
		if tool.Name == name {
			return tool, true
		}
	}
	return runtimepolicy.ResearchToolDefinitionV3{}, false
}

func researchStepReceiptFromEvidenceV3(
	receipt storepkg.ResearchRunStepEvidenceReceiptV3,
) ResearchStepReceiptV3 {
	return ResearchStepReceiptV3{
		StepID: receipt.StepID, Ordinal: receipt.Ordinal,
		InvocationID: receipt.InvocationID, ToolName: receipt.ToolName,
		RequestDigest: receipt.RequestDigest, ResultDigest: receipt.ResultDigest,
		EvidenceID: receipt.EvidenceID,
	}
}

func researchStepReceiptFromResolutionV3(
	resolution storepkg.ResearchRunStepResolutionV3,
) (ResearchStepReceiptV3, error) {
	if resolution.Phase != storepkg.ResearchRunStepCompletedV3 ||
		resolution.Evidence == nil {
		return ResearchStepReceiptV3{}, types.NewAppError(types.CodeConflict,
			"research Tool step has no recoverable evidence", types.ErrConflict)
	}
	return ResearchStepReceiptV3{
		StepID: resolution.Receipt.StepID, Ordinal: resolution.Receipt.Ordinal,
		InvocationID:  resolution.Receipt.InvocationID,
		ToolName:      resolution.Receipt.ToolName,
		RequestDigest: resolution.Receipt.RequestDigest,
		ResultDigest:  resolution.Receipt.ResultDigest,
		EvidenceID:    resolution.Evidence.EvidenceID,
	}, nil
}

func researchBriefRefFromCoordinatorSynthesisV3(
	row storepkg.ResearchBriefSynthesisV3,
) (types.ResearchBriefRefV3, error) {
	if row.Status != storepkg.ResearchBriefSynthesisFinalizedV3 ||
		row.DeliveryRequired == nil {
		return types.ResearchBriefRefV3{}, researchCoordinatorValidationV3(
			"research Brief synthesis is not finalized")
	}
	ref, err := types.SealResearchBriefRefV3(types.ResearchBriefRefV3{
		BriefID: row.ID, RunSnapshotID: row.RunSnapshotID, PlanID: row.PlanID,
		TemporalWorkflowID: row.TemporalWorkflowID, TemporalRunID: row.TemporalRunID,
		TenantID: row.TenantID, UserID: row.UserID, TaskID: row.TaskID,
		DefinitionDigest: row.DefinitionDigest, PlanDigest: row.PlanDigest,
		RequestDigest: row.RequestDigest, BriefDigest: row.BriefDigest,
		EvidenceDigest: row.EvidenceDigest, HistoryDigest: row.HistoryDigest,
		NotificationThreshold: row.NotificationThreshold,
		Significance:          row.Significance, Decision: row.Decision,
		DeliveryRequired: *row.DeliveryRequired,
	})
	if err != nil {
		return types.ResearchBriefRefV3{}, researchCoordinatorValidationV3(
			"research Brief reference is invalid")
	}
	return ref, nil
}

func researchCoordinatorValidationV3(message string) error {
	return types.NewAppError(types.CodeValidation, message, types.ErrValidation)
}

var _ ResearchRuntimeCoordinatorV3 = (*ProductionResearchRuntimeV3)(nil)
