package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// CreateOrGetCompiledTaskRunSnapshotV2Params freezes one Source-free task run.
// Policy contains only versioned, non-secret runtime DTOs. CredentialRef fields
// are opaque generation references; credential values never enter the snapshot.
type CreateOrGetCompiledTaskRunSnapshotV2Params struct {
	Identity           types.RunIdentity
	Policy             runtimepolicy.BundleV1
	ObservationRollout observation.RolloutMode
}

func (s *Store) CreateOrGetCompiledRunSnapshotV2(
	ctx context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
	observationRollout ...observation.RolloutMode,
) (types.RunSnapshotRefV2, error) {
	rollout, err := observationRolloutArgument(observationRollout)
	if err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	return s.CreateOrGetCompiledTaskRunSnapshotV2(ctx,
		CreateOrGetCompiledTaskRunSnapshotV2Params{
			Identity: identity, Policy: policy, ObservationRollout: rollout,
		})
}

func (s *Store) CreateOrGetCompiledTaskRunSnapshotV2(
	ctx context.Context,
	p CreateOrGetCompiledTaskRunSnapshotV2Params,
) (types.RunSnapshotRefV2, error) {
	if validateTaskRunExpectedIdentityV1(p.Identity) != nil {
		return types.RunSnapshotRefV2{}, taskRunValidationError(
			"compiled task run v2 identity is invalid")
	}
	lookup := taskRunLookupFromIdentity(p.Identity)
	policies, policyDigests, policy, policyErr :=
		encodeTaskRunPolicyBundleV1(p.Policy)
	rollout, rolloutErr := normalizeObservationRollout(
		[]observation.RolloutMode{p.ObservationRollout})
	if policyErr != nil || rolloutErr != nil {
		existing, found, err := s.loadTaskRunSnapshotBehindFenceV2(ctx, lookup)
		if err != nil {
			return types.RunSnapshotRefV2{}, err
		}
		if found {
			if existing.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 {
				return types.RunSnapshotRefV2{}, taskRunIntegrityError()
			}
			return existing.safeRefV2()
		}
		return types.RunSnapshotRefV2{}, invalidTypedTaskRunPolicy()
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return types.RunSnapshotRefV2{}, taskRunDatabaseError(
			"begin task run v2 snapshot transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setTaskRunTenantContext(ctx, tx, p.Identity.TenantID); err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	if err := lockTaskRunSnapshotRun(ctx, tx, p.Identity.TemporalRunID); err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	if existing, found, err := loadTaskRunSnapshot(ctx, tx, lookup); err != nil {
		return types.RunSnapshotRefV2{}, err
	} else if found {
		if existing.ReferenceSchemaVersion != types.RunSnapshotSchemaVersionV2 {
			return types.RunSnapshotRefV2{}, taskRunIntegrityError()
		}
		return existing.safeRefV2()
	}
	approved, err := loadCurrentToolApprovedDefinitionTx(
		ctx, tx, p.Identity)
	if err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	if err := lockTaskRunMembership(
		ctx, tx, p.Identity.TenantID, p.Identity.UserID); err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	adaptive, err := loadCurrentToolAdaptiveStateTx(ctx, tx, approved)
	if err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	bindings, err := buildTaskRunToolBindingsV1(approved.Definition, policy)
	if err != nil {
		return types.RunSnapshotRefV2{}, taskRunValidationError(
			"runtime policy does not cover the approved Tool plan")
	}
	budget := taskRunBudget{}
	budgetJSON, err := json.Marshal(budget)
	if err != nil {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	prepared, err := encodeTaskRunSnapshotPayloadV2(taskRunSnapshotPayloadV2{
		SchemaVersion:                  taskRunSnapshotPayloadSchemaV2,
		TenantID:                       p.Identity.TenantID,
		UserID:                         p.Identity.UserID,
		TaskID:                         p.Identity.TaskID,
		RunKind:                        types.RunSnapshotKindScheduled,
		Mode:                           types.ExecutionModeCompiled,
		DefinitionVersion:              approved.Version,
		DefinitionDigest:               approved.Digest,
		AdaptiveVersion:                adaptive.Version,
		AdaptiveDigest:                 adaptive.Digest,
		AdaptiveBasisDefinitionVersion: adaptive.BasisDefinitionVersion,
		AdaptiveBasisDefinitionDigest:  adaptive.BasisDefinitionDigest,
		ObservationRollout:             rollout,
		Policies:                       policies,
		Budget:                         budget,
		Definition:                     approved.Definition,
		Adaptive:                       adaptive.State,
		ToolBindings:                   bindings,
		ReferenceSchemaVersion:         types.RunSnapshotSchemaVersionV2,
	})
	if err != nil {
		return types.RunSnapshotRefV2{}, taskRunIntegrityError()
	}
	var snapshotID int64
	if err := tx.QueryRow(ctx,
		`SELECT nextval('task_run_snapshots_id_seq')`).Scan(&snapshotID); err != nil {
		return types.RunSnapshotRefV2{}, taskRunDatabaseError(
			"allocate task run v2 snapshot id", err)
	}
	candidate := &taskRunSnapshot{
		ID: snapshotID, TenantID: p.Identity.TenantID,
		UserID: p.Identity.UserID, TaskID: p.Identity.TaskID,
		TemporalWorkflowID:      p.Identity.TemporalWorkflowID,
		TemporalRunID:           p.Identity.TemporalRunID,
		RunKind:                 types.RunSnapshotKindScheduled,
		Mode:                    types.ExecutionModeCompiled,
		AdaptiveVersion:         adaptive.Version,
		CapabilityCatalogDigest: policyDigests.CapabilityCatalog,
		ToolPolicyDigest:        policyDigests.ToolPolicy,
		PromptPolicyDigest:      policyDigests.PromptPolicy,
		ModelPolicyDigest:       policyDigests.ModelPolicy,
		QuotaPolicyDigest:       policyDigests.QuotaPolicy,
		DefinitionDigest:        approved.Digest,
		PlanDigest:              prepared.PlanDigest,
		PayloadDigest:           sha256Hex(prepared.Canonical),
		ReferenceSchemaVersion:  types.RunSnapshotSchemaVersionV2,
		Payload:                 prepared.Canonical,
		BudgetJSON:              budgetJSON,
	}
	ref, err := sealTaskRunSnapshotReferenceV2(candidate)
	if err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	candidate.ReferenceDigest = ref.ReferenceDigest
	inserted, err := scanTaskRunSnapshot(tx.QueryRow(ctx,
		`INSERT INTO task_run_snapshots (
			id, tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version, capability_catalog_digest,
			tool_policy_digest, prompt_policy_digest, model_policy_digest,
			quota_policy_digest, definition_digest, plan_digest, payload_digest,
			reference_digest, reference_schema_version, payload, budget
		 ) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20, $21
		 ) ON CONFLICT (temporal_run_id) DO NOTHING
		 RETURNING `+taskRunSnapshotColumns,
		candidate.ID, candidate.TenantID, candidate.UserID, candidate.TaskID,
		candidate.TemporalWorkflowID, candidate.TemporalRunID,
		string(candidate.RunKind), string(candidate.Mode), candidate.AdaptiveVersion,
		candidate.CapabilityCatalogDigest, candidate.ToolPolicyDigest,
		candidate.PromptPolicyDigest, candidate.ModelPolicyDigest,
		candidate.QuotaPolicyDigest, candidate.DefinitionDigest,
		candidate.PlanDigest, candidate.PayloadDigest, candidate.ReferenceDigest,
		candidate.ReferenceSchemaVersion, candidate.Payload, candidate.BudgetJSON,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			rollbackCompiledTaskTx(ctx, tx)
			winner, found, loadErr :=
				s.loadTaskRunSnapshotBehindFenceV2(ctx, lookup)
			if loadErr != nil {
				return types.RunSnapshotRefV2{}, loadErr
			}
			if found &&
				winner.ReferenceSchemaVersion ==
					types.RunSnapshotSchemaVersionV2 {
				return winner.safeRefV2()
			}
			return types.RunSnapshotRefV2{}, types.NewAppError(types.CodeConflict,
				"task run snapshot execution identity conflicts with an existing run", nil)
		}
		return types.RunSnapshotRefV2{}, taskRunDatabaseError(
			"insert task run v2 snapshot", err)
	}
	if err := validateStoredTaskRunSnapshotV2(inserted, lookup); err != nil {
		return types.RunSnapshotRefV2{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		recovered, found, recoveryErr :=
			s.loadTaskRunSnapshotBehindFenceV2(recoveryCtx, lookup)
		if recoveryErr != nil {
			return types.RunSnapshotRefV2{}, recoveryErr
		}
		if found && recovered.ReferenceSchemaVersion == types.RunSnapshotSchemaVersionV2 {
			return recovered.safeRefV2()
		}
		return types.RunSnapshotRefV2{}, taskRunDatabaseError(
			"commit task run v2 snapshot transaction", err)
	}
	return inserted.safeRefV2()
}

func taskRunLookupFromIdentity(identity types.RunIdentity) CreateOrGetTaskRunSnapshotParams {
	return CreateOrGetTaskRunSnapshotParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID, TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID: identity.TemporalRunID,
	}
}

func encodeTaskRunPolicyBundleV1(
	policy runtimepolicy.BundleV1,
) (taskRunPolicyPayloads, taskRunPolicyDigestSet, runtimepolicy.BundleV1, error) {
	payloads, sharedDigests, decoded, err :=
		runcontext.EncodePolicyBundleV1(policy)
	if err != nil {
		return taskRunPolicyPayloads{}, taskRunPolicyDigestSet{},
			runtimepolicy.BundleV1{}, err
	}
	return payloads, taskRunPolicyDigestSet{
		CapabilityCatalog: sharedDigests.CapabilityCatalogDigest,
		ToolPolicy:        sharedDigests.ToolPolicyDigest,
		PromptPolicy:      sharedDigests.PromptPolicyDigest,
		ModelPolicy:       sharedDigests.ModelPolicyDigest,
		QuotaPolicy:       sharedDigests.QuotaPolicyDigest,
	}, decoded, nil
}

func loadCurrentToolApprovedDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	identity types.RunIdentity,
) (ToolApprovedDefinitionVersionRecord, error) {
	record, err := scanToolApprovedDefinitionVersion(tx.QueryRow(ctx,
		`SELECT d.version, d.schema_version, d.execution_mode,
		        d.definition_digest, d.payload, d.operation_ref, d.created_at
		   FROM schedules s
		   JOIN task_approved_definition_versions d
		     ON d.tenant_id=s.tenant_id AND d.user_id=s.user_id AND d.task_id=s.id
		    AND d.version=s.approved_definition_version
		    AND d.definition_digest=s.approved_definition_digest
		    AND d.execution_mode=s.execution_mode
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND (
		      s.status=$4 OR (
		        s.status=$7 AND authorize_manual_task_run_v1(
		          s.tenant_id, s.user_id, s.id, $8
		        )
		      )
		    )
		    AND s.execution_mode=$5
		    AND d.schema_version=$6 AND `+matureSchedulePredicate+`
		  FOR SHARE OF s, d`,
		identity.TenantID, identity.UserID, identity.TaskID,
		types.ScheduleStatusActive, types.ExecutionModeCompiled,
		taskstate.ApprovedDefinitionSchemaVersionV2,
		types.ScheduleStatusPaused, identity.TemporalWorkflowID,
	),
		identity.TenantID, identity.UserID, identity.TaskID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return ToolApprovedDefinitionVersionRecord{}, taskRunNotFound()
		}
		return ToolApprovedDefinitionVersionRecord{}, err
	}
	return record, nil
}

func loadCurrentToolAdaptiveStateTx(
	ctx context.Context,
	tx pgx.Tx,
	approved ToolApprovedDefinitionVersionRecord,
) (ToolAdaptiveStateRecord, error) {
	record, err := scanToolAdaptiveState(tx.QueryRow(ctx,
		`SELECT a.version, a.schema_version, a.payload_digest, a.payload,
		        a.basis_definition_version, a.basis_definition_digest,
		        a.last_known_good_definition_version, a.created_at, a.updated_at
		   FROM task_adaptive_states a
		  WHERE a.tenant_id=$1 AND a.user_id=$2 AND a.task_id=$3
		    AND a.schema_version=$4
		    AND a.basis_definition_version=$5
		    AND a.basis_definition_digest=$6
		  FOR SHARE OF a`,
		approved.Definition.TenantID, approved.Definition.UserID,
		approved.Definition.TaskID, taskstate.AdaptiveStateSchemaVersionV2,
		approved.Version, approved.Digest),
		approved.Definition.TenantID, approved.Definition.UserID,
		approved.Definition.TaskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ToolAdaptiveStateRecord{}, taskRunNotFound()
		}
		return ToolAdaptiveStateRecord{}, err
	}
	return record, nil
}

func (s *Store) loadTaskRunSnapshotBehindFenceV2(
	ctx context.Context,
	lookup CreateOrGetTaskRunSnapshotParams,
) (*taskRunSnapshot, bool, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, false, taskRunDatabaseError(
			"begin task run v2 recovery transaction", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setTaskRunTenantContext(ctx, tx, lookup.TenantID); err != nil {
		return nil, false, err
	}
	if err := lockTaskRunSnapshotRun(ctx, tx, lookup.TemporalRunID); err != nil {
		return nil, false, err
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, taskRunDatabaseError(
			"commit task run v2 recovery transaction", err)
	}
	return snapshot, found, nil
}
