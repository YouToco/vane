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

// ResearchV3DefinitionChanges is the exact owner-visible change set accepted
// by manage_tasks. Nil fields are preserved from the immutable base loaded by
// the coordinator; present fields replace the complete nested value. Keeping
// this merge inside the coordinator prevents the model from guessing omitted
// policy and prevents a read-then-write race from overwriting concurrent edits.
type ResearchV3DefinitionChanges struct {
	TaskName     *string
	TaskManual   *string
	SpecJSON     json.RawMessage
	Notification *taskstate.NotificationPolicyV3
	Output       *taskstate.OutputPreferenceV3
}

func ApplyResearchV3DefinitionChanges(
	base taskstate.ApprovedDefinitionV3,
	changes ResearchV3DefinitionChanges,
) (taskstate.ApprovedDefinitionV3, error) {
	if err := base.Validate(); err != nil {
		return taskstate.ApprovedDefinitionV3{}, errors.New(
			"task: native V3 edit base definition is invalid")
	}
	if changes.TaskName == nil && changes.TaskManual == nil &&
		len(changes.SpecJSON) == 0 && changes.Notification == nil &&
		changes.Output == nil {
		return taskstate.ApprovedDefinitionV3{}, creationValidation(
			"V3 任务编辑至少需要一项明确变更", nil)
	}
	target := ResearchV3DefinitionEditInput{
		TenantID: base.TenantID, UserID: base.UserID, TaskID: base.TaskID,
		TaskName: base.TaskName, TaskManual: base.TaskManual,
		SpecJSON: base.SpecJSON, Notification: base.Notification,
		Output: base.Output,
	}
	if changes.TaskName != nil {
		target.TaskName = *changes.TaskName
	}
	if changes.TaskManual != nil {
		target.TaskManual = *changes.TaskManual
	}
	if len(changes.SpecJSON) != 0 {
		target.SpecJSON = changes.SpecJSON
	}
	if changes.Notification != nil {
		target.Notification = *changes.Notification
	}
	if changes.Output != nil {
		target.Output = *changes.Output
	}
	return BuildResearchV3DefinitionEditTarget(base, target)
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
	if base.ResearchScope != nil && in.TaskManual != base.TaskManual {
		return taskstate.ApprovedDefinitionV3{}, errors.New(
			"task: scoped V3 task manual changed; explicit operator prepare is required")
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
			ResearchScope:      base.ResearchScope,
		},
	)
	if err != nil {
		return taskstate.ApprovedDefinitionV3{}, creationValidation(
			"V3 任务完整编辑未通过校验", err)
	}
	return target, nil
}
