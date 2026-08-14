package store

import (
	"bytes"

	"github.com/YouToco/vane/server/types"
)

func validateStoredTaskRunSnapshotV2(
	snapshot *taskRunSnapshot,
	p CreateOrGetTaskRunSnapshotParams,
) error {
	if snapshot == nil || snapshot.CreatedAt.IsZero() ||
		snapshot.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 ||
		snapshot.V2CutoverEventID != nil ||
		snapshot.RunKind != types.RunSnapshotKindScheduled ||
		snapshot.Mode != types.ExecutionModeCompiled ||
		snapshot.AdaptiveVersion <= 0 {
		return taskRunIntegrityError()
	}
	if snapshot.TenantID != p.TenantID || snapshot.UserID != p.UserID ||
		snapshot.TaskID != p.TaskID ||
		snapshot.TemporalWorkflowID != p.TemporalWorkflowID ||
		snapshot.TemporalRunID != p.TemporalRunID {
		return types.NewAppError(types.CodeConflict,
			"task run snapshot identity differs from the committed run", nil)
	}
	ref, err := snapshot.safeRefV2()
	if err != nil {
		return taskRunIntegrityError()
	}
	expected := types.RunIdentity{
		TemporalWorkflowID: p.TemporalWorkflowID,
		TemporalRunID:      p.TemporalRunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           p.TenantID,
		UserID:             p.UserID,
		TaskID:             p.TaskID,
	}
	if validateTaskRunSnapshotReferenceForExpectedV2(ref, expected) != nil {
		return taskRunIntegrityError()
	}
	decoded, err := readTaskRunSnapshotPayloadV2(snapshot.Payload)
	if err != nil {
		return taskRunIntegrityError()
	}
	payload := decoded.Payload
	storedBudget, budgetJSON, err := readTaskRunBudgetV1(snapshot.BudgetJSON)
	if err != nil || storedBudget != (taskRunBudgetV1{}) ||
		payload.Budget != (taskRunBudget{}) ||
		payload.TenantID != snapshot.TenantID ||
		payload.UserID != snapshot.UserID ||
		payload.TaskID != snapshot.TaskID ||
		payload.RunKind != snapshot.RunKind ||
		payload.Mode != snapshot.Mode ||
		payload.AdaptiveVersion != snapshot.AdaptiveVersion ||
		payload.ReferenceSchemaVersion != snapshot.ReferenceSchemaVersion ||
		!constantTimeDigestEqual(
			decoded.PolicyDigests.CapabilityCatalog,
			snapshot.CapabilityCatalogDigest) ||
		!constantTimeDigestEqual(
			decoded.PolicyDigests.ToolPolicy, snapshot.ToolPolicyDigest) ||
		!constantTimeDigestEqual(
			decoded.PolicyDigests.PromptPolicy, snapshot.PromptPolicyDigest) ||
		!constantTimeDigestEqual(
			decoded.PolicyDigests.ModelPolicy, snapshot.ModelPolicyDigest) ||
		!constantTimeDigestEqual(
			decoded.PolicyDigests.QuotaPolicy, snapshot.QuotaPolicyDigest) ||
		!constantTimeDigestEqual(
			payload.DefinitionDigest, snapshot.DefinitionDigest) ||
		!constantTimeDigestEqual(decoded.PlanDigest, snapshot.PlanDigest) ||
		!bytes.Equal(decoded.Canonical, snapshot.Payload) ||
		!constantTimeDigestEqual(
			sha256Hex(snapshot.Payload), snapshot.PayloadDigest) {
		return taskRunIntegrityError()
	}
	snapshot.Payload = decoded.Canonical
	snapshot.BudgetJSON = budgetJSON
	return nil
}
