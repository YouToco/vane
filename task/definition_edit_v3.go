package task

import (
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/taskstate"
)

// ResearchV3DefinitionEditInput is the complete target requested by the
// authenticated manage_tasks boundary. Tenant/User/Task are trusted internal
// references resolved before authorization; the model supplies only the five
// owner-visible target fields. There is intentionally no Source, fetch plan,
// ToolCalls, budget, delivery route, or confirmation-card identity.
type ResearchV3DefinitionEditInput struct {
	TenantID     int64
	UserID       int64
	TaskID       string
	TaskName     string
	TaskManual   string
	SpecJSON     json.RawMessage
	Notification taskstate.NotificationPolicyV3
	Output       taskstate.OutputPreferenceV3
}

// BuildResearchV3DefinitionEditTarget preserves every trusted server policy
// from the exact base and replaces the complete owner-visible target. This is
// a full-definition writer: omitted/zero fields fail validation instead of
// inheriting a model-guessed partial patch.
func BuildResearchV3DefinitionEditTarget(
	base taskstate.ApprovedDefinitionV3,
	in ResearchV3DefinitionEditInput,
) (taskstate.ApprovedDefinitionV3, error) {
	if err := base.Validate(); err != nil {
		return taskstate.ApprovedDefinitionV3{}, errors.New(
			"task: native V3 edit base definition is invalid")
	}
	if in.TenantID <= 0 || in.UserID <= 0 || in.TaskID == "" ||
		base.TenantID != in.TenantID || base.UserID != in.UserID ||
		base.TaskID != in.TaskID {
		return taskstate.ApprovedDefinitionV3{}, errors.New(
			"task: native V3 edit scope differs from the exact base")
	}
	target, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: in.TenantID, UserID: in.UserID, TaskID: in.TaskID,
			TaskName: in.TaskName, TaskManual: in.TaskManual,
			SpecJSON: in.SpecJSON, ExecutionMode: base.ExecutionMode,
			Notification: in.Notification, Output: in.Output,
			PlannerBudget:      base.PlannerBudget,
			DeliveryPolicy:     base.DeliveryPolicy,
			TenantBudgetPolicy: base.TenantBudgetPolicy,
		},
	)
	if err != nil {
		return taskstate.ApprovedDefinitionV3{}, creationValidation(
			"V3 任务完整编辑未通过校验", err)
	}
	return target, nil
}
