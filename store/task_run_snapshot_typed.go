package store

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// CreateOrGetCompiledTaskRunSnapshotV1Params is the only production-safe C1
// input to the generic snapshot persistence primitive. Identity is complete
// and scheduled; Policy can only be constructed from the versioned,
// non-secret runtime DTOs. Compiled V1 has no planner, so callers cannot supply
// a budget, execution mode, or adaptive version independently.
type CreateOrGetCompiledTaskRunSnapshotV1Params struct {
	Identity types.RunIdentity
	Policy   runtimepolicy.BundleV1
}

// CreateOrGetCompiledRunSnapshotV1 is the interface-friendly production
// facade used by PrepareRun. Keeping the parameter object behind the store
// boundary lets workflow depend on an identity+policy contract rather than a
// concrete store request type; all persistence still enters the single typed
// adapter below.
func (s *Store) CreateOrGetCompiledRunSnapshotV1(
	ctx context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
) (types.RunSnapshotRef, error) {
	return s.CreateOrGetCompiledTaskRunSnapshotV1(ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: identity, Policy: policy})
}

// CreateOrGetCompiledTaskRunSnapshotV1 freezes one compiled scheduled run from
// typed policy input. It deliberately owns serialization of all five policy
// bodies so a future Activity cannot bypass the DTO boundary with arbitrary
// JSON or accidentally pass an application config containing credentials.
//
// C1a kept this method at zero production call points. C1b exposes it only
// through CreateOrGetCompiledRunSnapshotV1, whose sole production consumer is
// PrepareRun after snapshot readers and live authorization landed together.
func (s *Store) CreateOrGetCompiledTaskRunSnapshotV1(
	ctx context.Context,
	p CreateOrGetCompiledTaskRunSnapshotV1Params,
) (types.RunSnapshotRef, error) {
	if err := validateTaskRunExpectedIdentityV1(p.Identity); err != nil {
		return types.RunSnapshotRef{}, taskRunValidationError(
			"compiled task run identity is invalid")
	}
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID:           p.Identity.TenantID,
		UserID:             p.Identity.UserID,
		TaskID:             p.Identity.TaskID,
		TemporalWorkflowID: p.Identity.TemporalWorkflowID,
		TemporalRunID:      p.Identity.TemporalRunID,
	}
	capabilityCatalog, capabilityErr := runtimepolicy.EncodeCapabilityCatalogV1(
		p.Policy.CapabilityCatalog)
	toolPolicy, toolErr := runtimepolicy.EncodeToolPolicyV1(p.Policy.ToolPolicy)
	promptPolicy, promptErr := runtimepolicy.EncodePromptPolicyV1(p.Policy.PromptPolicy)
	modelPolicy, modelErr := runtimepolicy.EncodeModelPolicyV1(p.Policy.ModelPolicy)
	quotaPolicy, quotaErr := runtimepolicy.EncodeQuotaPolicyV1(p.Policy.QuotaPolicy)
	if capabilityErr != nil || toolErr != nil || promptErr != nil ||
		modelErr != nil || quotaErr != nil ||
		p.Policy.SchemaVersion != runtimepolicy.BundleSchemaVersionV1 {
		// An invalid/obsolete caller still waits behind the same RunID fence.
		// This preserves response-lost recovery without creating an outer
		// lookup/inner-create gap: a valid first writer owns the raw primitive's
		// fence continuously through its exact lookup and insert.
		if existing, found, loadErr := s.loadTaskRunSnapshotBehindFence(
			ctx, lookup); loadErr != nil {
			return types.RunSnapshotRef{}, loadErr
		} else if found {
			return existing.safeRef()
		}
		return types.RunSnapshotRef{}, invalidTypedTaskRunPolicy()
	}

	budget, err := json.Marshal(types.PlannerBudget{})
	if err != nil {
		return types.RunSnapshotRef{}, taskRunIntegrityError()
	}
	lookup.Mode = types.ExecutionMode(taskRunReferenceModeV1)
	lookup.AdaptiveVersion = 0
	lookup.CapabilityCatalogJSON = capabilityCatalog
	lookup.ToolPolicyJSON = toolPolicy
	lookup.PromptPolicyJSON = promptPolicy
	lookup.ModelPolicyJSON = modelPolicy
	lookup.QuotaPolicyJSON = quotaPolicy
	lookup.BudgetJSON = budget
	snapshot, err := s.createOrGetTaskRunSnapshot(ctx, lookup)
	if err != nil {
		return types.RunSnapshotRef{}, err
	}
	return snapshot.safeRef()
}

// loadTaskRunSnapshotBehindFence waits for any in-flight writer of the same
// Temporal RunID before deciding that no immutable winner exists. Read
// Committed is intentional: the advisory-lock statement may wait for another
// transaction to commit, and the following SELECT must take a newer snapshot.
func (s *Store) loadTaskRunSnapshotBehindFence(
	ctx context.Context,
	p CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, bool, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, false, taskRunDatabaseError(
			"begin task run snapshot lookup transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockTaskRunSnapshotRun(ctx, tx, p.TemporalRunID); err != nil {
		return nil, false, err
	}
	existing, found, err := loadTaskRunSnapshot(ctx, tx, p)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, taskRunDatabaseError(
			"commit task run snapshot lookup transaction", err)
	}
	return existing, found, nil
}

func invalidTypedTaskRunPolicy() error {
	return taskRunValidationError("compiled task run policy is invalid")
}
