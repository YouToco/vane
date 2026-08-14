package runcontext

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/YouToco/vane/types"
)

// ResearchExecutionTraceV3 seals one provider attempt to the complete trusted
// run identity and the exact frozen plan step. Ordinal is intentionally part
// of the claim even though invocation IDs are unique within a valid plan: the
// Store must be able to reject a coordinator that re-labels a receipt as a
// different step while retaining the original invocation ID.
func ResearchExecutionTraceV3(
	identity types.RunIdentity,
	runSnapshotID int64,
	planDigest string,
	ordinal int,
	invocationID string,
) (string, error) {
	if identity.Validate() != nil || runSnapshotID <= 0 ||
		!validResearchDigest(planDigest) || ordinal < 0 || ordinal >= maxResearchPlanSteps ||
		!validResearchText(invocationID, maxResearchInvocationBytes) {
		return "", invalidResearchPlan("execution trace input is invalid")
	}
	payload, err := json.Marshal(struct {
		TemporalWorkflowID string                `json:"temporal_workflow_id"`
		TemporalRunID      string                `json:"temporal_run_id"`
		RunKind            types.RunSnapshotKind `json:"run_kind"`
		TenantID           int64                 `json:"tenant_id"`
		UserID             int64                 `json:"user_id"`
		TaskID             string                `json:"task_id"`
		RunSnapshotID      int64                 `json:"run_snapshot_id"`
		PlanDigest         string                `json:"plan_digest"`
		Ordinal            int                   `json:"ordinal"`
		InvocationID       string                `json:"invocation_id"`
	}{
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID, RunKind: identity.RunKind,
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		RunSnapshotID: runSnapshotID, PlanDigest: planDigest,
		Ordinal: ordinal, InvocationID: invocationID,
	})
	if err != nil {
		return "", types.NewAppError(types.CodeInternal,
			"research V3 execution trace could not be sealed", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
