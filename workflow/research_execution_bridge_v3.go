package workflow

import (
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/runtimepolicy"
	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// researchStepPersistenceV3 is the only adapter from a provider execution
// receipt to the Store's immutable terminal contracts. Exactly one branch is
// populated. Keeping this mapping in production code prevents a coordinator
// from accidentally treating over-cap or unknown-cost responses as evidence.
type researchStepPersistenceV3 struct {
	Evidence *storepkg.CommitResearchRunStepEvidenceV3Params
	Terminal *storepkg.CommitResearchRunStepV3Params
}

func mapResearchExecutionReceiptV3(
	identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3,
	plan types.ResearchRunPlanRefV3,
	execution storepkg.ResearchRunStepExecutionV3,
	receipt fetcher.ResearchExecutionReceiptV3,
) (researchStepPersistenceV3, error) {
	if err := snapshot.ValidateFor(identity); err != nil {
		return researchStepPersistenceV3{}, types.NewAppError(
			types.CodeValidation, "research execution snapshot is invalid", err)
	}
	if err := plan.ValidateFor(identity, snapshot.SnapshotID); err != nil ||
		!execution.FirstWriter || execution.StepID <= 0 ||
		execution.SpendReservationID <= 0 || execution.Ordinal < 0 ||
		execution.ReservedQuotaUnits != 1 || execution.ReservedCostMicroUSD < 0 ||
		execution.InvocationID == "" || execution.ToolName == "" ||
		execution.RequestDigest == "" {
		return researchStepPersistenceV3{}, types.NewAppError(
			types.CodeValidation, "research execution claim is invalid", types.ErrValidation)
	}
	if err := receipt.Validate(); err != nil {
		return researchStepPersistenceV3{}, err
	}
	if receipt.Attempted {
		expectedTrace, err := fetcher.ResearchExecutionTraceV3(
			identity, snapshot.SnapshotID, plan.PlanDigest,
			execution.Ordinal, execution.InvocationID)
		if err != nil || receipt.TraceID != expectedTrace {
			return researchStepPersistenceV3{}, types.NewAppError(
				types.CodeValidation, "research execution receipt does not match its claim",
				types.ErrValidation)
		}
	}

	provider := mapResearchProviderCallV3(receipt)
	resultTrust := receipt.ResultTrust
	if resultTrust == "" && receipt.Provider == "exa" {
		resultTrust = runtimepolicy.ResearchToolTrustExternalV3
	}
	base := storepkg.CommitResearchRunStepV3Params{
		Identity: identity, RunSnapshotID: snapshot.SnapshotID,
		PlanRef: plan, Ordinal: execution.Ordinal,
		ErrorCode: string(receipt.ErrorCode), ProviderCall: provider,
	}
	switch receipt.Status {
	case fetcher.ResearchExecutionSuccessV3:
		if receipt.CostMicroUSD > execution.ReservedCostMicroUSD {
			return researchStepPersistenceV3{}, types.NewAppError(
				types.CodeValidation, "completed research spend exceeds its frozen Tool cap",
				types.ErrValidation)
		}
		return researchStepPersistenceV3{Evidence: &storepkg.CommitResearchRunStepEvidenceV3Params{
			Identity: identity, RunSnapshotID: snapshot.SnapshotID,
			PlanRef: plan, Ordinal: execution.Ordinal,
			Result: receipt.Result, OriginalSize: receipt.NormalizedResultSize,
			TrustType: string(resultTrust), CostMicroUSD: receipt.CostMicroUSD,
			ProviderCall: provider,
		}}, nil
	case fetcher.ResearchExecutionDefiniteFailureV3:
		base.Phase = storepkg.ResearchRunStepFailedV3
	case fetcher.ResearchExecutionIndeterminateV3:
		base.Phase = storepkg.ResearchRunStepIndeterminateV3
	default:
		return researchStepPersistenceV3{}, types.NewAppError(
			types.CodeValidation, "research execution status is invalid", types.ErrValidation)
	}
	if receipt.Attempted && !receipt.CostKnown {
		base.CostMicroUSD = execution.ReservedCostMicroUSD
	} else {
		base.CostMicroUSD = receipt.CostMicroUSD
	}
	return researchStepPersistenceV3{Terminal: &base}, nil
}

func mapResearchProviderCallV3(
	receipt fetcher.ResearchExecutionReceiptV3,
) storepkg.ResearchProviderCallV3 {
	if !receipt.Attempted {
		return storepkg.ResearchProviderCallV3{}
	}
	call := storepkg.ResearchProviderCallV3{
		TraceID: receipt.TraceID, Provider: receipt.Provider,
		UsageQuantity: receipt.UsageQuantity, QuotaUnits: 1,
		HTTPStatus: receipt.HTTPStatus, DurationMS: receipt.DurationMS,
		Attempted: true, CostKnown: receipt.CostKnown,
		CostMicroUSD: receipt.CostMicroUSD,
	}
	if receipt.CostKnown {
		call.PricingStatus = "provider_reported"
		if receipt.Provider == "kimi" {
			call.PricingStatus = "calculated"
		}
		call.CostCurrency = "USD"
	} else {
		call.PricingStatus = "unpriced"
	}
	return call
}
