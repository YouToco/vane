package store

import (
	"crypto/subtle"

	"github.com/YouToco/vane/server/types"
)

func validateTaskRunSnapshotReferenceForExpectedV2(
	ref types.RunSnapshotRefV2,
	expected types.RunIdentity,
) error {
	if ref.SchemaVersion != types.RunSnapshotSchemaVersionV2 ||
		validateTaskRunExpectedIdentityV1(expected) != nil ||
		ref.Mode != types.ExecutionModeCompiled ||
		ref.AdaptiveVersion <= 0 ||
		ref.PlannerBudget != (types.PlannerBudget{}) ||
		ref.ValidateFor(expected) != nil {
		return taskRunValidationError("task run v2 snapshot reference is invalid")
	}
	return nil
}

func (s *taskRunSnapshot) safeRefV2() (types.RunSnapshotRefV2, error) {
	if s == nil || s.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 ||
		s.AdaptiveVersion <= 0 {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	budget, canonicalBudget, err := readTaskRunBudgetV1(s.BudgetJSON)
	if err != nil || budget != (taskRunBudgetV1{}) {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	ref := types.RunSnapshotRefV2{
		SchemaVersion:      types.RunSnapshotSchemaVersionV2,
		SnapshotID:         s.ID,
		TemporalWorkflowID: s.TemporalWorkflowID,
		TemporalRunID:      s.TemporalRunID,
		RunKind:            s.RunKind,
		TenantID:           s.TenantID,
		UserID:             s.UserID,
		TaskID:             s.TaskID,
		Mode:               s.Mode,
		DefinitionDigest:   s.DefinitionDigest,
		PlanDigest:         s.PlanDigest,
		AdaptiveVersion:    s.AdaptiveVersion,
		Policy: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: s.CapabilityCatalogDigest,
			ToolPolicyDigest:        s.ToolPolicyDigest,
			PromptPolicyDigest:      s.PromptPolicyDigest,
			ModelPolicyDigest:       s.ModelPolicyDigest,
			QuotaPolicyDigest:       s.QuotaPolicyDigest,
		},
		PlannerBudget: types.PlannerBudget{},
		PayloadDigest: s.PayloadDigest,
	}
	sealed, err := ref.Seal()
	if err != nil || subtle.ConstantTimeCompare(
		[]byte(sealed.ReferenceDigest), []byte(s.ReferenceDigest)) != 1 {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	sealed.ReferenceDigest = s.ReferenceDigest
	if err := sealed.Validate(); err != nil {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	s.BudgetJSON = canonicalBudget
	return sealed, nil
}

func sealTaskRunSnapshotReferenceV2(
	snapshot *taskRunSnapshot,
) (types.RunSnapshotRefV2, error) {
	if snapshot == nil ||
		snapshot.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	ref := types.RunSnapshotRefV2{
		SchemaVersion:      types.RunSnapshotSchemaVersionV2,
		SnapshotID:         snapshot.ID,
		TemporalWorkflowID: snapshot.TemporalWorkflowID,
		TemporalRunID:      snapshot.TemporalRunID,
		RunKind:            snapshot.RunKind,
		TenantID:           snapshot.TenantID,
		UserID:             snapshot.UserID,
		TaskID:             snapshot.TaskID,
		Mode:               snapshot.Mode,
		DefinitionDigest:   snapshot.DefinitionDigest,
		PlanDigest:         snapshot.PlanDigest,
		AdaptiveVersion:    snapshot.AdaptiveVersion,
		Policy: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: snapshot.CapabilityCatalogDigest,
			ToolPolicyDigest:        snapshot.ToolPolicyDigest,
			PromptPolicyDigest:      snapshot.PromptPolicyDigest,
			ModelPolicyDigest:       snapshot.ModelPolicyDigest,
			QuotaPolicyDigest:       snapshot.QuotaPolicyDigest,
		},
		PlannerBudget: types.PlannerBudget{},
		PayloadDigest: snapshot.PayloadDigest,
	}
	sealed, err := ref.Seal()
	if err != nil {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	return sealed, nil
}
