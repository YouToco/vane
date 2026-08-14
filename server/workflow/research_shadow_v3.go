package workflow

import (
	"github.com/google/uuid"
	"go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/server/types"
)

// ResearchShadowInputV3 identifies one already-approved scheduled task. It is
// deliberately smaller than PushParams: a shadow caller cannot smuggle a
// Source scope, legacy description, snapshot, or delivery authority into
// Temporal history.
type ResearchShadowInputV3 struct {
	TenantID int64  `json:"tenant_id"`
	UserID   int64  `json:"user_id"`
	TaskID   string `json:"task_id"`
}

// ResearchShadowWorkflowV3 evaluates the V3 runtime independently of the
// task's durable Schedule Action. It never calls the delivery Activity, even
// if a buggy coordinator returns DeliveryAllowed=true and the Brief requests
// delivery. Starting this Workflow therefore does not change the Schedule
// spec, next fire time, authoritative Action, or user-visible delivery path.
func ResearchShadowWorkflowV3(ctx workflow.Context, in ResearchShadowInputV3) error {
	identity := types.RunIdentity{
		TemporalWorkflowID: "shadow-validation-only",
		TemporalRunID:      "shadow-validation-only",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           in.TenantID,
		UserID:             in.UserID,
		TaskID:             in.TaskID,
	}
	if err := identity.Validate(); err != nil {
		return types.NewAppError(types.CodeValidation,
			"research V3 shadow identity is invalid", nil)
	}
	var traceID string
	if err := workflow.SideEffect(ctx, func(workflow.Context) any {
		return uuid.NewString()
	}).Get(&traceID); err != nil {
		return err
	}
	var activities *Activities
	return runResearchPipelineV3(ctx, ResearchScheduledInputV3{
		TenantID: in.TenantID, UserID: in.UserID, TaskID: in.TaskID,
	}, traceID, activities, false)
}
