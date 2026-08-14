package taskstate

import "github.com/YouToco/vane/server/types"

// ResearchV3DefinitionPrepareParams carries authenticated operation scope and
// explicit V3 policies. Task text and timing are deliberately absent: Store
// compiles those from the current locked owner projection.
type ResearchV3DefinitionPrepareParams struct {
	TenantID       int64
	UserID         int64
	TaskID         string
	IdempotencyKey string
	Notification   NotificationPolicyV3
	Output         OutputPreferenceV3
	PlannerBudget  types.PlannerBudget
	ResearchScope  *ResearchScopeV3
}
