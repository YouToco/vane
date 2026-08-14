package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	storepkg "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// ResearchRuntimeCoordinatorV3 is the non-Temporal implementation boundary.
// Every method must finish its immutable Store commit before returning; the
// Activity result contains only safe references and digests.
type ResearchRuntimeCoordinatorV3 interface {
	Prepare(context.Context, types.RunIdentity, string) (
		snapshot types.ResearchRunSnapshotRefV3,
		authorized, deliveryAllowed bool,
		err error,
	)
	Plan(context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3, string) (types.ResearchRunPlanRefV3, error)
	ExecuteStep(context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3, int, string) (ResearchStepReceiptV3, error)
	Synthesize(context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3, string) (ResearchBriefRefV3, error)
	Deliver(context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3, types.ResearchRunPlanRefV3, ResearchBriefRefV3, string) (ResearchDeliveryReceiptV3, error)
}

type PrepareResearchRunV3Result struct {
	Authorized      bool                           `json:"authorized"`
	DeliveryAllowed bool                           `json:"delivery_allowed"`
	Snapshot        types.ResearchRunSnapshotRefV3 `json:"snapshot"`
}

func (r PrepareResearchRunV3Result) ValidateFor(identity types.RunIdentity) error {
	if !r.Authorized {
		if r.DeliveryAllowed || r.Snapshot != (types.ResearchRunSnapshotRefV3{}) {
			return types.NewAppError(types.CodeValidation,
				"unauthorized research preparation returned runtime authority", nil)
		}
		return nil
	}
	return r.Snapshot.ValidateFor(identity)
}

type ResearchRunV3Input struct {
	TenantID int64                          `json:"tenant_id"`
	UserID   int64                          `json:"user_id"`
	TaskID   string                         `json:"task_id"`
	TraceID  string                         `json:"trace_id"`
	Snapshot types.ResearchRunSnapshotRefV3 `json:"snapshot"`
}

type PlanResearchRunV3Result struct {
	Plan types.ResearchRunPlanRefV3 `json:"plan"`
}

func (r PlanResearchRunV3Result) ValidateFor(
	identity types.RunIdentity, snapshotID int64,
) error {
	return r.Plan.ValidateFor(identity, snapshotID)
}

type ExecuteResearchStepV3Input struct {
	ResearchRunV3Input
	Plan    types.ResearchRunPlanRefV3 `json:"plan"`
	Ordinal int                        `json:"ordinal"`
}

type ResearchStepReceiptV3 struct {
	StepID        int64  `json:"step_id"`
	Ordinal       int    `json:"ordinal"`
	Phase         string `json:"phase"`
	InvocationID  string `json:"invocation_id"`
	ToolName      string `json:"tool_name"`
	RequestDigest string `json:"request_digest"`
	ResultDigest  string `json:"result_digest,omitempty"`
	EvidenceID    int64  `json:"evidence_id,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
}

func (r ResearchStepReceiptV3) Validate(ordinal int) error {
	if r.StepID <= 0 || r.Ordinal != ordinal ||
		!researchV3Text(r.InvocationID, 255) || !researchV3Text(r.ToolName, 255) ||
		!researchV3Digest(r.RequestDigest) {
		return types.NewAppError(types.CodeValidation,
			"research step receipt is invalid", nil)
	}
	switch r.Phase {
	case "":
		// Activity results written before the terminal-outcome union had no
		// phase field. Only the old exact success shape is replay-compatible;
		// an empty phase can never authorize a failed/indeterminate receipt.
		if r.EvidenceID <= 0 || !researchV3Digest(r.ResultDigest) || r.ErrorCode != "" {
			return types.NewAppError(types.CodeValidation,
				"legacy research completed step receipt is invalid", nil)
		}
	case string(storepkg.ResearchRunStepCompletedV3):
		if r.EvidenceID <= 0 || !researchV3Digest(r.ResultDigest) || r.ErrorCode != "" {
			return types.NewAppError(types.CodeValidation,
				"research completed step receipt is invalid", nil)
		}
	case string(storepkg.ResearchRunStepFailedV3),
		string(storepkg.ResearchRunStepIndeterminateV3):
		if r.EvidenceID != 0 || r.ResultDigest != "" || !researchV3Text(r.ErrorCode, 128) {
			return types.NewAppError(types.CodeValidation,
				"research failed step receipt is invalid", nil)
		}
	default:
		return types.NewAppError(types.CodeValidation,
			"research step receipt phase is invalid", nil)
	}
	return nil
}

type SynthesizeResearchBriefV3Input struct {
	ResearchRunV3Input
	Plan types.ResearchRunPlanRefV3 `json:"plan"`
}

type ResearchBriefRefV3 = types.ResearchBriefRefV3

type DeliverResearchBriefV3Input struct {
	ResearchRunV3Input
	Plan  types.ResearchRunPlanRefV3 `json:"plan"`
	Brief ResearchBriefRefV3         `json:"brief"`
}

type ResearchDeliveryReceiptV3 struct {
	DeliveryID    int64  `json:"delivery_id"`
	BriefID       int64  `json:"brief_id"`
	ReceiptDigest string `json:"receipt_digest"`
}

func (r ResearchDeliveryReceiptV3) Validate(briefID int64) error {
	if r.DeliveryID <= 0 || r.BriefID != briefID ||
		!researchV3Digest(r.ReceiptDigest) {
		return types.NewAppError(types.CodeValidation,
			"research delivery receipt is invalid", nil)
	}
	return nil
}

func WithResearchRuntimeV3(runtime ResearchRuntimeCoordinatorV3) ActivitiesOption {
	return func(a *Activities) { a.researchRuntimeV3 = runtime }
}

// WithResearchDeliveryV3 installs the independently reviewed receipt-backed
// provider boundary. Omitting it keeps production delivery hard-dark even when
// the artifact runtime is present.
func WithResearchDeliveryV3(delivery interface {
	Deliver(context.Context, types.RunIdentity, types.ResearchRunSnapshotRefV3,
		types.ResearchRunPlanRefV3, ResearchBriefRefV3, string) (ResearchDeliveryReceiptV3, error)
}) ActivitiesOption {
	return func(a *Activities) { a.researchDeliveryV3 = delivery }
}

func (a *Activities) PrepareResearchRunV3(
	ctx context.Context, p ResearchScheduledInputV3,
) (PrepareResearchRunV3Result, error) {
	identity, err := researchActivityIdentityV3(ctx, p.TenantID, p.UserID, p.TaskID)
	if err != nil || a.researchRuntimeV3 == nil {
		return PrepareResearchRunV3Result{}, researchV3ActivityError(ctx, "prepare", types.NewAppError(
			types.CodeValidation, "research V3 preparation is unavailable", err))
	}
	ref, authorized, deliveryAllowed, err := a.researchRuntimeV3.Prepare(
		ctx, identity, p.ActionAuthorizationToken)
	if err != nil {
		return PrepareResearchRunV3Result{}, researchV3ActivityError(ctx, "prepare", err)
	}
	result := PrepareResearchRunV3Result{
		Authorized: authorized, DeliveryAllowed: deliveryAllowed,
	}
	if authorized {
		result.Snapshot = ref
	}
	if err := result.ValidateFor(identity); err != nil {
		return PrepareResearchRunV3Result{}, researchV3ActivityError(ctx, "prepare", err)
	}
	return result, nil
}

func (a *Activities) PlanResearchRunV3(
	ctx context.Context, in ResearchRunV3Input,
) (PlanResearchRunV3Result, error) {
	identity, err := validateResearchRunV3Input(ctx, in)
	if err != nil || a.researchRuntimeV3 == nil {
		return PlanResearchRunV3Result{}, researchV3ActivityError(ctx, "plan", types.NewAppError(
			types.CodeValidation, "research V3 planning is unavailable", err))
	}
	plan, err := a.researchRuntimeV3.Plan(ctx, identity, in.Snapshot, in.TraceID)
	if err != nil {
		return PlanResearchRunV3Result{}, researchV3ActivityError(ctx, "plan", err)
	}
	result := PlanResearchRunV3Result{Plan: plan}
	if err := result.ValidateFor(identity, in.Snapshot.SnapshotID); err != nil {
		return PlanResearchRunV3Result{}, researchV3ActivityError(ctx, "plan", err)
	}
	return result, nil
}

func (a *Activities) ExecuteResearchStepV3(
	ctx context.Context, in ExecuteResearchStepV3Input,
) (ResearchStepReceiptV3, error) {
	identity, err := validateResearchRunV3Input(ctx, in.ResearchRunV3Input)
	if err != nil || a.researchRuntimeV3 == nil || in.Ordinal < 0 || in.Ordinal >= 16 ||
		in.Plan.ValidateFor(identity, in.Snapshot.SnapshotID) != nil {
		return ResearchStepReceiptV3{}, researchV3ActivityError(ctx, "execute_step", types.NewAppError(
			types.CodeValidation, "research V3 step is unavailable", err))
	}
	receipt, err := a.researchRuntimeV3.ExecuteStep(
		ctx, identity, in.Snapshot, in.Plan, in.Ordinal, in.TraceID)
	if err != nil {
		return ResearchStepReceiptV3{}, researchV3ActivityError(ctx, "execute_step", err)
	}
	if err := receipt.Validate(in.Ordinal); err != nil {
		return ResearchStepReceiptV3{}, researchV3ActivityError(ctx, "execute_step", err)
	}
	return receipt, nil
}

func (a *Activities) SynthesizeResearchBriefV3(
	ctx context.Context, in SynthesizeResearchBriefV3Input,
) (ResearchBriefRefV3, error) {
	identity, err := validateResearchRunV3Input(ctx, in.ResearchRunV3Input)
	if err != nil || a.researchRuntimeV3 == nil ||
		in.Plan.ValidateFor(identity, in.Snapshot.SnapshotID) != nil {
		return ResearchBriefRefV3{}, researchV3ActivityError(ctx, "synthesize", types.NewAppError(
			types.CodeValidation, "research V3 synthesis is unavailable", err))
	}
	brief, err := a.researchRuntimeV3.Synthesize(
		ctx, identity, in.Snapshot, in.Plan, in.TraceID)
	if err != nil {
		return ResearchBriefRefV3{}, researchV3ActivityError(ctx, "synthesize", err)
	}
	if err := brief.ValidateFor(identity, in.Snapshot.SnapshotID, in.Plan.PlanID); err != nil {
		return ResearchBriefRefV3{}, researchV3ActivityError(ctx, "synthesize", err)
	}
	return brief, nil
}

func (a *Activities) DeliverResearchBriefV3(
	ctx context.Context, in DeliverResearchBriefV3Input,
) (ResearchDeliveryReceiptV3, error) {
	identity, err := validateResearchRunV3Input(ctx, in.ResearchRunV3Input)
	if err != nil || a.researchRuntimeV3 == nil || a.researchDeliveryV3 == nil ||
		!in.Brief.DeliveryRequired ||
		in.Plan.ValidateFor(identity, in.Snapshot.SnapshotID) != nil ||
		in.Brief.ValidateFor(identity, in.Snapshot.SnapshotID, in.Plan.PlanID) != nil {
		return ResearchDeliveryReceiptV3{}, researchV3ActivityError(ctx, "deliver", types.NewAppError(
			types.CodeValidation, "research V3 delivery is unavailable", err))
	}
	receipt, err := a.researchDeliveryV3.Deliver(ctx, identity, in.Snapshot, in.Plan,
		in.Brief, in.TraceID)
	if err != nil {
		return ResearchDeliveryReceiptV3{}, researchV3ActivityError(ctx, "deliver", err)
	}
	if err := receipt.Validate(in.Brief.BriefID); err != nil {
		return ResearchDeliveryReceiptV3{}, researchV3ActivityError(ctx, "deliver", err)
	}
	return receipt, nil
}

func validateResearchRunV3Input(
	ctx context.Context, in ResearchRunV3Input,
) (types.RunIdentity, error) {
	identity, err := researchActivityIdentityV3(ctx, in.TenantID, in.UserID, in.TaskID)
	if err != nil || !researchV3Text(in.TraceID, 255) ||
		in.Snapshot.ValidateFor(identity) != nil {
		return types.RunIdentity{}, types.NewAppError(types.CodeValidation,
			"research V3 Activity input is invalid", err)
	}
	return identity, nil
}

func researchActivityIdentityV3(
	ctx context.Context, tenantID, userID int64, taskID string,
) (types.RunIdentity, error) {
	if !activity.IsActivity(ctx) || tenantID <= 0 || userID <= 0 ||
		!researchV3Text(taskID, 255) {
		return types.RunIdentity{}, types.NewAppError(types.CodeValidation,
			"research V3 Activity identity is invalid", nil)
	}
	info := activity.GetInfo(ctx)
	identity := types.RunIdentity{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		TemporalRunID:      info.WorkflowExecution.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID, UserID: userID, TaskID: taskID,
	}
	if err := identity.Validate(); err != nil {
		return types.RunIdentity{}, err
	}
	return identity, nil
}

func researchV3Text(value string, max int) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= max
}

func researchV3Digest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validResearchV3PushParams(p PushParams) bool {
	return p.RunKind == PushRunKindScheduled && p.TenantID > 0 && p.UserID > 0 &&
		researchV3Text(p.ScheduleID, 255) &&
		p.ExecutionMode == types.ExecutionModeDiscoverAtRun &&
		IsResearchRuntimeV3(p.RuntimeVersion) && p.Snapshot == nil &&
		len(p.Scope.SourceIDs) == 0 && p.Scope.TopN == 0 && p.NLDesc == ""
}

// researchV3ActivityError is the sole V3 Activity failure boundary. Temporal
// history receives only a fixed phase, a low-cardinality code and retryability;
// coordinator messages and causes may contain prompts, Tool payloads or secrets
// and must never enter the durable failure chain. The digest is correlation-only.
func researchV3ActivityError(ctx context.Context, phase string, err error) error {
	code := researchV3SafeErrorCode(err)
	digest := sha256.Sum256([]byte(err.Error()))
	activity.GetLogger(ctx).Error("research V3 activity failed",
		"phase", phase, "code", code, "error_digest", hex.EncodeToString(digest[:]))
	return temporal.NewApplicationErrorWithOptions(
		"research V3 "+phase+" failed", string(code),
		temporal.ApplicationErrorOptions{
			NonRetryable: !types.IsRetryable(err),
		},
	)
}

func researchV3SafeErrorCode(err error) types.ErrCode {
	code := types.CodeOf(err)
	switch code {
	case types.CodeNotFound, types.CodeConflict, types.CodeValidation,
		types.CodeDatabase, types.CodeInternal, types.CodeLLMRateLimit,
		types.CodeLLMBadRequest, types.CodeLLMUnavailable,
		types.CodeQuotaExceeded, types.CodeFetchTimeout,
		types.CodeFetchRateLimit, types.CodePushFailed,
		types.CodeDBDeadlock, types.CodeDBConnLost, types.CodeDBConstraint:
		return code
	default:
		return types.CodeInternal
	}
}
