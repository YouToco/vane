package task

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// CreationV3ServerPolicy contains only policy selected by the trusted server
// deployment. It is not part of the Agent/model-facing create request.
type CreationV3ServerPolicy struct {
	PlannerBudget types.PlannerBudget
}

// CreationCoordinatorOption configures trusted control-plane behavior without
// changing retained V1 creation semantics.
type CreationCoordinatorOption func(*CreationCoordinator)

// WithResearchV3CreationPolicy enables native V3 proposal persistence. An
// invalid policy leaves the path disabled so startup wiring fails closed.
func WithResearchV3CreationPolicy(policy CreationV3ServerPolicy) CreationCoordinatorOption {
	return func(coordinator *CreationCoordinator) {
		if coordinator == nil ||
			policy.PlannerBudget.ValidateForMode(types.ExecutionModeDiscoverAtRun) != nil {
			return
		}
		copy := policy
		coordinator.v3Policy = &copy
	}
}

// ResearchV3CreationProposalInput is the complete model-facing durable intent.
// Budget and delivery policy are intentionally absent and injected by the
// coordinator. It contains no source entity or frozen execution plan.
type ResearchV3CreationProposalInput struct {
	ActionID     string
	UserID       int64
	SessionID    *int64
	TaskName     string
	TaskManual   string
	SpecJSON     json.RawMessage
	Notification taskstate.NotificationPolicyV3
	Output       taskstate.OutputPreferenceV3
	ExpiresAt    time.Time
}

type researchV3CreationOperationStore interface {
	CreateResearchTaskCreationOperationV3(
		context.Context,
		types.CreateResearchTaskCreationOperationV3Params,
	) (*types.TaskCreationOperation, error)
}

// PrepareResearchV3 validates and freezes a native V3 task proposal in the
// existing CreationCoordinator operation journal using execution version 2.
// This checkpoint deliberately does not expose an execution entrypoint yet;
// the Temporal lifecycle driver is added separately after its recovery path is
// complete, so no half-created production task can escape this batch.
func (c *CreationCoordinator) PrepareResearchV3(
	ctx context.Context,
	in ResearchV3CreationProposalInput,
) (CreationProposal, error) {
	if err := ctx.Err(); err != nil {
		return CreationProposal{}, err
	}
	if c == nil {
		return CreationProposal{}, errors.New("task: native V3 creation is not configured")
	}
	store, ok := c.store.(researchV3CreationOperationStore)
	if !ok || store == nil || c.v3Policy == nil {
		return CreationProposal{}, errors.New("task: native V3 creation is not configured")
	}
	if !validResearchV3CreationOperationID(in.ActionID) ||
		in.UserID <= 0 || in.ExpiresAt.IsZero() {
		return CreationProposal{}, creationValidation("V3 任务创建请求不完整", nil)
	}
	tenantID, err := c.resolveActiveTenant(ctx, in.UserID)
	if err != nil {
		return CreationProposal{}, err
	}
	taskID, err := scheduler.TaskIDForOperation(tenantID, in.UserID, in.ActionID)
	if err != nil {
		return CreationProposal{}, creationValidation("V3 任务标识无法生成", err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: in.UserID, TaskID: taskID,
		TaskName: in.TaskName, TaskManual: in.TaskManual, SpecJSON: in.SpecJSON,
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification:  in.Notification, Output: in.Output,
		PlannerBudget:      c.v3Policy.PlannerBudget,
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		return CreationProposal{}, creationValidation("V3 任务方案未通过校验", err)
	}
	payload, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil {
		return CreationProposal{}, creationValidation("V3 任务方案无法规范化", err)
	}
	params := types.CreateResearchTaskCreationOperationV3Params{
		ID: in.ActionID, TenantID: tenantID, UserID: in.UserID,
		SessionID: in.SessionID, Args: payload, Summary: definition.TaskName,
		ExpiresAt: in.ExpiresAt.UTC().Truncate(time.Microsecond),
	}
	op, err := store.CreateResearchTaskCreationOperationV3(ctx, params)
	if err != nil {
		return CreationProposal{}, err
	}
	if op == nil || op.ExecutionVersion != types.TaskCreationExecutionVersionV2 ||
		op.ID != params.ID || op.TenantID != params.TenantID || op.UserID != params.UserID ||
		op.ToolName != "manage_tasks" || op.Summary != params.Summary ||
		!researchV3CreationDefinitionsEqual(op.Args, params.Args) {
		return CreationProposal{}, types.NewAppError(
			types.CodeConflict, "V3 任务创建操作与已保存内容冲突", types.ErrConflict)
	}
	return CreationProposal{ID: op.ID, Summary: op.Summary}, nil
}

func researchV3CreationDefinitionsEqual(left, right json.RawMessage) bool {
	leftDefinition, err := taskstate.DecodeApprovedDefinitionV3(left)
	if err != nil {
		return false
	}
	rightDefinition, err := taskstate.DecodeApprovedDefinitionV3(right)
	if err != nil {
		return false
	}
	leftDigest, err := taskstate.DigestApprovedDefinitionV3(leftDefinition)
	if err != nil {
		return false
	}
	rightDigest, err := taskstate.DigestApprovedDefinitionV3(rightDefinition)
	return err == nil && leftDigest == rightDigest
}

func validResearchV3CreationOperationID(value string) bool {
	if len(value) == 0 || len(value) > 255 || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("-_.:", char) {
			continue
		}
		return false
	}
	return true
}
