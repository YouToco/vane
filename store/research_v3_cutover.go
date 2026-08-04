package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func (s *Store) LoadCurrentResearchApprovedDefinitionV3Head(
	ctx context.Context, tenantID, userID int64, taskID string,
) (types.ResearchV3DefinitionHead, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return types.ResearchV3DefinitionHead{}, err
	}
	return s.LoadPreparedResearchV3DefinitionHead(ctx, tenantID, userID, taskID)
}

// LoadPreparedResearchApprovedDefinitionV3Head returns the delivery-dark
// sidecar selected by prepare. It never falls back to the production head.
func (s *Store) LoadPreparedResearchApprovedDefinitionV3Head(
	ctx context.Context, tenantID, userID int64, taskID string,
) (types.ResearchV3DefinitionHead, error) {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return types.ResearchV3DefinitionHead{}, err
	}
	return s.LoadPreparedResearchV3DefinitionHead(ctx, tenantID, userID, taskID)
}

// VerifyEnabledResearchV3ActionAuthorization makes startup reconciliation a
// live check against the durable exact-task authority. A syntactically valid
// but stale or tampered Schedule bearer token must stop startup rather than
// leave the next scheduled run to fail silently.
func (s *Store) VerifyEnabledResearchV3ActionAuthorization(
	ctx context.Context, tenantID, userID int64, taskID, token string,
) error {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return err
	}
	if len(token) != 64 || token != strings.ToLower(token) {
		return types.NewAppError(types.CodeConflict,
			"research V3 Schedule Action authorization is invalid", types.ErrConflict)
	}
	if _, err := hex.DecodeString(token); err != nil {
		return types.NewAppError(types.CodeConflict,
			"research V3 Schedule Action authorization is invalid", types.ErrConflict)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return taskStateDatabaseError("begin research V3 Action authority verification", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return taskStateDatabaseError("bind research V3 Action authority scope", err)
	}
	var expected string
	err = tx.QueryRow(ctx,
		`SELECT action_authorization_digest
		   FROM research_v3_delivery_authorities
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND status='enabled'`,
		tenantID, userID, taskID).Scan(&expected)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeConflict,
			"enabled research V3 Action authority is unavailable", types.ErrConflict)
	}
	if err != nil {
		return taskStateDatabaseError("load research V3 Action authority", err)
	}
	digest := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(digest[:])), []byte(expected)) != 1 {
		return types.NewAppError(types.CodeConflict,
			"research V3 Schedule Action authorization does not match durable authority",
			types.ErrConflict)
	}
	return nil
}

// RequireSuccessfulResearchV3ShadowPreflight proves that the exact current
// definition has already completed synthesis through the delivery-dark shadow
// workflow. The proof is rebuilt from immutable snapshot and Brief rows; a
// Temporal start receipt or an in-memory success flag is never sufficient.
func (s *Store) RequireSuccessfulResearchV3ShadowPreflight(
	ctx context.Context, tenantID, userID int64, taskID string,
	head types.ResearchV3DefinitionHead,
) error {
	if err := validateTaskStateScope(tenantID, userID, taskID); err != nil {
		return err
	}
	if head.Version <= 0 || !validDigestSyntaxV3(head.Digest) {
		return types.NewAppError(types.CodeValidation,
			"research V3 shadow preflight head is invalid", types.ErrValidation)
	}
	row, err := scanTaskRunSnapshot(s.pool.QueryRow(ctx,
		`SELECT `+taskRunSnapshotColumns+`
		   FROM task_run_snapshots
		  WHERE id=(
		        SELECT synthesis.run_snapshot_id
		          FROM research_brief_syntheses synthesis
		          JOIN task_run_snapshots candidate
		            ON candidate.id=synthesis.run_snapshot_id
		           AND candidate.tenant_id=synthesis.tenant_id
		           AND candidate.user_id=synthesis.user_id
		           AND candidate.task_id=synthesis.task_id
		           AND candidate.temporal_workflow_id=synthesis.temporal_workflow_id
		           AND candidate.temporal_run_id=synthesis.temporal_run_id
		         WHERE synthesis.tenant_id=$1 AND synthesis.user_id=$2
		           AND synthesis.task_id=$3 AND synthesis.definition_digest=$4
		           AND synthesis.status='finalized' AND synthesis.finalized_at IS NOT NULL
		           AND candidate.reference_schema_version=$5
		           AND candidate.definition_digest=$4
		           AND candidate.temporal_workflow_id LIKE 'research-v3-shadow-%'
		           AND NOT EXISTS (
		               SELECT 1 FROM research_brief_deliveries delivery
		                WHERE delivery.tenant_id=synthesis.tenant_id
		                  AND delivery.user_id=synthesis.user_id
		                  AND delivery.task_id=synthesis.task_id
		                  AND delivery.brief_id=synthesis.id)
		         ORDER BY synthesis.finalized_at DESC,synthesis.id DESC
		         LIMIT 1)`,
		tenantID, userID, taskID, head.Digest, types.ResearchRunSnapshotRefSchemaV3))
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeConflict,
			"successful delivery-dark research V3 shadow is unavailable", types.ErrConflict)
	}
	if err != nil {
		return taskStateDatabaseError("load research V3 shadow preflight", err)
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: row.TemporalWorkflowID,
		TemporalRunID:      row.TemporalRunID,
		RunKind:            row.RunKind,
		TenantID:           row.TenantID,
		UserID:             row.UserID,
		TaskID:             row.TaskID,
	}
	ref, err := validateStoredResearchRunSnapshotV3(identity, row)
	if err != nil {
		return err
	}
	shadowSuffix := strings.TrimPrefix(ref.TemporalWorkflowID, "research-v3-shadow-")
	if shadowSuffix == ref.TemporalWorkflowID || !validDigestSyntaxV3(shadowSuffix) ||
		ref.DefinitionVersion != head.Version || ref.DefinitionDigest != head.Digest ||
		ref.AuthorityGeneration != 0 || ref.TargetActionDigest != "" ||
		ref.ActionAuthorizationDigest != "" {
		return types.NewAppError(types.CodeConflict,
			"research V3 shadow preflight evidence is not eligible", types.ErrConflict)
	}
	return nil
}

// BeginResearchV3Cutover stages a new exact-task authority and its immutable
// recovery journal. It re-validates the canonical V3 head in the same DB
// transaction that allocates the generation; no delivery claim is authorized
// while the authority remains staged.
func (s *Store) BeginResearchV3Cutover(
	ctx context.Context, p types.BeginResearchV3CutoverParams,
) (types.ResearchV3CutoverOperation, error) {
	if err := validateResearchV3CutoverBegin(p); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("begin V3 cutover", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, p.TenantID, p.UserID); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", p.TenantID, p.UserID, p.TaskID)); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("lock V3 cutover", err)
	}
	if existing, found, loadErr := loadResearchV3CutoverTx(
		ctx, tx, p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey,
	); loadErr != nil {
		return types.ResearchV3CutoverOperation{}, loadErr
	} else if found {
		if !researchV3CutoverMatches(existing, p) {
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 cutover key has different immutable input", types.ErrConflict)
		}
		if existing.Phase == types.ResearchV3CutoverPrepared ||
			existing.Phase == types.ResearchV3CutoverPauseRequested ||
			existing.Phase == types.ResearchV3CutoverPaused {
			binding, bindErr := loadPreparedResearchV3BindingTx(
				ctx, tx, p.TenantID, p.UserID, p.TaskID, false,
				researchV3ExpectBaseHead)
			if bindErr != nil {
				return types.ResearchV3CutoverOperation{}, bindErr
			}
			if binding.Target != existing.Definition ||
				binding.ScheduleStatus != existing.OriginalScheduleStatus ||
				binding.SourceBaselineDigest != existing.SourceBaselineDigest ||
				binding.BaseMode != existing.OriginalExecutionMode ||
				!researchV3HeadsEqual(binding.BaseHead, existing.OriginalDefinition) {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 task changed after cutover began", types.ErrConflict)
			}
		}
		return existing, nil
	}
	binding, err := loadPreparedResearchV3BindingTx(ctx, tx, p.TenantID, p.UserID,
		p.TaskID, false, researchV3ExpectBaseHead)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if binding.Target != p.Definition || binding.ScheduleStatus != p.OriginalScheduleStatus {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeConflict, "research V3 definition changed before cutover", types.ErrConflict)
	}
	var generation int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(max(generation),0)+1
		   FROM research_v3_delivery_authorities
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		p.TenantID, p.UserID, p.TaskID).Scan(&generation); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("allocate V3 cutover generation", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO research_v3_delivery_authorities
		 (tenant_id,user_id,task_id,generation,definition_version,
		  definition_digest,target_action_digest,action_authorization_digest,status)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'staged')`,
		p.TenantID, p.UserID, p.TaskID, generation, p.Definition.Version,
		p.Definition.Digest, p.TargetActionDigest, p.ActionAuthorizationDigest); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("stage V3 delivery authority", err)
	}
	originalMode := string(binding.BaseMode)
	var originalVersion *int64
	var originalDigest *string
	if binding.BaseHead != nil {
		originalVersion, originalDigest = &binding.BaseHead.Version, &binding.BaseHead.Digest
	}
	var op types.ResearchV3CutoverOperation
	op, err = scanResearchV3Cutover(tx.QueryRow(ctx,
		`INSERT INTO research_v3_cutover_operations
		 (tenant_id,user_id,task_id,idempotency_key,generation,
		  definition_version,definition_digest,frozen_schedule,
		  frozen_schedule_digest,frozen_conflict_token,conflict_token_digest,
		  target_action,target_action_digest,action_authorization_digest,original_paused,
		  original_schedule_status,preflight_digest,phase,
		  original_execution_mode,original_definition_version,original_definition_digest,
		  source_baseline_digest)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'prepared',$18,$19,$20,$21)
		 RETURNING id,tenant_id,user_id,task_id,idempotency_key,generation,
		  definition_version,definition_digest,frozen_schedule,
		  frozen_schedule_digest,frozen_conflict_token,conflict_token_digest,
		  rollback_conflict_token,rollback_token_digest,
		  target_action,target_action_digest,action_authorization_digest,
		  original_paused,original_schedule_status,preflight_digest,
		  original_execution_mode,original_definition_version,
		  original_definition_digest,source_baseline_digest,phase,created_at,updated_at`,
		p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey, generation,
		p.Definition.Version, p.Definition.Digest, p.FrozenSchedule,
		p.FrozenScheduleDigest, p.FrozenConflictToken, p.ConflictTokenDigest,
		p.TargetAction, p.TargetActionDigest, p.ActionAuthorizationDigest,
		p.OriginalPaused, p.OriginalScheduleStatus, p.PreflightDigest,
		originalMode, originalVersion, originalDigest,
		binding.SourceBaselineDigest))
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("insert V3 cutover journal", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("commit V3 cutover journal", err)
	}
	return op, nil
}

func (s *Store) LoadResearchV3Cutover(
	ctx context.Context, tenantID, userID int64, taskID, key string,
) (types.ResearchV3CutoverOperation, bool, error) {
	return loadResearchV3CutoverTx(ctx, s.pool, tenantID, userID, taskID, key)
}

// LoadResearchV3CutoverAuthorityStatus returns only the durable state bound to
// one immutable cutover generation. It never exposes the Action bearer token.
func (s *Store) LoadResearchV3CutoverAuthorityStatus(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) (string, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", taskStateDatabaseError("begin V3 authority status", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(
		ctx, tx, op.TenantID, op.UserID); err != nil {
		return "", err
	}
	var status string
	if err := tx.QueryRow(ctx, `
		SELECT status FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=$4
		   AND definition_version=$5 AND definition_digest=$6
		   AND target_action_digest=$7 AND action_authorization_digest=$8`,
		op.TenantID, op.UserID, op.TaskID, op.Generation,
		op.Definition.Version, op.Definition.Digest, op.TargetActionDigest,
		op.ActionAuthorizationDigest).Scan(&status); err != nil {
		return "", taskStateDatabaseError("load V3 authority status", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", taskStateDatabaseError("commit V3 authority status", err)
	}
	return status, nil
}

// VerifyResearchV3CutoverDatabaseState proves the terminal DB half of the
// saga without advancing it. Temporal is verified separately from Describe.
func (s *Store) VerifyResearchV3CutoverDatabaseState(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) error {
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return taskStateDatabaseError("begin V3 cutover verification", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(
		ctx, tx, op.TenantID, op.UserID); err != nil {
		return err
	}
	var status, mode string
	var version *int64
	var digest *string
	if err := tx.QueryRow(ctx, `
		SELECT status,execution_mode,approved_definition_version,
		       approved_definition_digest
		  FROM schedules
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		op.TenantID, op.UserID, op.TaskID).Scan(
		&status, &mode, &version, &digest); err != nil {
		return taskStateDatabaseError("load V3 cutover verification head", err)
	}
	if status != string(op.OriginalScheduleStatus) {
		return types.NewAppError(types.CodeConflict,
			"research V3 cutover changed schedule status", types.ErrConflict)
	}
	var authorityStatus string
	if err := tx.QueryRow(ctx, `
		SELECT status FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=$4
		   AND definition_version=$5 AND definition_digest=$6
		   AND target_action_digest=$7 AND action_authorization_digest=$8`,
		op.TenantID, op.UserID, op.TaskID, op.Generation,
		op.Definition.Version, op.Definition.Digest, op.TargetActionDigest,
		op.ActionAuthorizationDigest).Scan(&authorityStatus); err != nil {
		return taskStateDatabaseError("load V3 cutover verification authority", err)
	}
	switch op.Phase {
	case types.ResearchV3CutoverActive:
		if mode != string(types.ExecutionModeDiscoverAtRun) || version == nil || digest == nil ||
			*version != op.Definition.Version || *digest != op.Definition.Digest ||
			authorityStatus != "enabled" {
			return types.NewAppError(types.CodeConflict,
				"research V3 active database state differs", types.ErrConflict)
		}
	case types.ResearchV3CutoverRolledBack, types.ResearchV3CutoverAborted:
		if mode != string(op.OriginalExecutionMode) ||
			!nullableHeadEqual(version, digest, op.OriginalDefinition) ||
			authorityStatus != "revoked" {
			return types.NewAppError(types.CodeConflict,
				"research V3 rollback database state differs", types.ErrConflict)
		}
	default:
		return types.NewAppError(types.CodeConflict,
			"research V3 cutover is not terminal", types.ErrConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskStateDatabaseError("commit V3 cutover verification", err)
	}
	return nil
}

// RecheckResearchV3CutoverDefinition closes the pause-to-CAS definition race.
func (s *Store) RecheckResearchV3CutoverDefinition(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) error {
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, op.TenantID, op.UserID); err != nil {
		return err
	}
	expectation := researchV3ExpectBaseHead
	if op.Phase == types.ResearchV3CutoverDefinitionPromoted ||
		op.Phase == types.ResearchV3CutoverActionSwapped ||
		op.Phase == types.ResearchV3CutoverActive {
		expectation = researchV3ExpectTargetHead
	}
	binding, err := loadPreparedResearchV3BindingTx(ctx, tx, op.TenantID, op.UserID,
		op.TaskID, false, expectation)
	if err != nil {
		if types.CodeOf(err) == types.CodeDatabase || types.CodeOf(err) == types.CodeInternal {
			return err
		}
		return researchV3CutoverDrift("research V3 definition is no longer admissible")
	}
	if binding.Target != op.Definition || binding.ScheduleStatus != op.OriginalScheduleStatus ||
		binding.SourceBaselineDigest != op.SourceBaselineDigest ||
		binding.BaseMode != op.OriginalExecutionMode || !researchV3HeadsEqual(binding.BaseHead, op.OriginalDefinition) {
		return researchV3CutoverDrift("research V3 definition changed during cutover")
	}
	if err := tx.Commit(ctx); err != nil {
		return taskStateDatabaseError("commit research V3 definition recheck", err)
	}
	return nil
}

// PromoteResearchV3PreparedDefinition performs the production DB-head switch
// and its durable checkpoint in one transaction. The coordinator calls this
// only after it has proved that Temporal is paused.
func (s *Store) PromoteResearchV3PreparedDefinition(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) (types.ResearchV3CutoverOperation, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("begin V3 definition promotion", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, op.TenantID, op.UserID); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", op.TenantID, op.UserID, op.TaskID)); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("lock V3 definition promotion", err)
	}
	binding, err := loadPreparedResearchV3BindingTx(ctx, tx, op.TenantID, op.UserID,
		op.TaskID, false, researchV3ExpectBaseHead)
	if err != nil || binding.Target != op.Definition ||
		binding.ScheduleStatus != op.OriginalScheduleStatus ||
		binding.SourceBaselineDigest != op.SourceBaselineDigest ||
		binding.BaseMode != op.OriginalExecutionMode ||
		!researchV3HeadsEqual(binding.BaseHead, op.OriginalDefinition) {
		if err != nil {
			if types.CodeOf(err) == types.CodeDatabase || types.CodeOf(err) == types.CodeInternal {
				return types.ResearchV3CutoverOperation{}, err
			}
			return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
				"prepared V3 definition is no longer admissible")
		}
		return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
			"prepared V3 definition changed before promotion")
	}
	if err := requireResearchV3ProductionHeadTx(ctx, tx, op, false); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	command, err := tx.Exec(ctx,
		`UPDATE schedules SET execution_mode='discover_at_run',
		 approved_definition_version=$1,approved_definition_digest=$2
		 WHERE tenant_id=$3 AND user_id=$4 AND id=$5 AND execution_mode=$6
		 AND approved_definition_version IS NOT DISTINCT FROM $7
		 AND approved_definition_digest IS NOT DISTINCT FROM $8`,
		op.Definition.Version, op.Definition.Digest, op.TenantID, op.UserID, op.TaskID,
		op.OriginalExecutionMode, nullableDefinitionVersion(op.OriginalDefinition),
		nullableDefinitionDigest(op.OriginalDefinition))
	if err != nil || command.RowsAffected() != 1 {
		if err == nil {
			return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
				"production definition changed before V3 promotion")
		}
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("promote V3 production definition", err)
	}
	command, err = tx.Exec(ctx,
		`UPDATE research_v3_cutover_operations SET phase='definition_promoted'
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		 AND generation=$5 AND phase='paused'`,
		op.ID, op.TenantID, op.UserID, op.TaskID, op.Generation)
	if err != nil || command.RowsAffected() != 1 {
		if err == nil {
			err = types.ErrConflict
		}
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("checkpoint V3 definition promotion", err)
	}
	updated, found, err := loadResearchV3CutoverTx(ctx, tx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.ErrNotFound
		}
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("commit V3 definition promotion", err)
	}
	return updated, nil
}

// RestoreResearchV3OriginalDefinition restores the exact pre-cutover DB head
// while Temporal remains paused. It also handles rollback before promotion.
func (s *Store) RestoreResearchV3OriginalDefinition(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) (types.ResearchV3CutoverOperation, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("begin V3 definition restore", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, op.TenantID, op.UserID); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`, fmt.Sprintf("%d/%d/%s", op.TenantID, op.UserID, op.TaskID)); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	var mode string
	var version *int64
	var digest *string
	if err := tx.QueryRow(ctx, `SELECT execution_mode,approved_definition_version,approved_definition_digest FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3 FOR UPDATE`, op.TenantID, op.UserID, op.TaskID).Scan(&mode, &version, &digest); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("lock V3 production definition restore", err)
	}
	isTarget := mode == string(types.ExecutionModeDiscoverAtRun) && version != nil && digest != nil && *version == op.Definition.Version && *digest == op.Definition.Digest
	isOriginal := mode == string(op.OriginalExecutionMode) && nullableHeadEqual(version, digest, op.OriginalDefinition)
	if !isTarget && !isOriginal {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(types.CodeConflict, "production definition changed before V3 restore", types.ErrConflict)
	}
	if isTarget {
		command, execErr := tx.Exec(ctx, `UPDATE schedules SET execution_mode=$1,approved_definition_version=$2,approved_definition_digest=$3 WHERE tenant_id=$4 AND user_id=$5 AND id=$6 AND execution_mode='discover_at_run' AND approved_definition_version=$7 AND approved_definition_digest=$8`, op.OriginalExecutionMode, nullableDefinitionVersion(op.OriginalDefinition), nullableDefinitionDigest(op.OriginalDefinition), op.TenantID, op.UserID, op.TaskID, op.Definition.Version, op.Definition.Digest)
		if execErr != nil || command.RowsAffected() != 1 {
			if execErr == nil {
				execErr = types.ErrConflict
			}
			return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("restore V3 production definition", execErr)
		}
	}
	command, err := tx.Exec(ctx, `UPDATE research_v3_cutover_operations SET phase='definition_restored' WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4 AND generation=$5 AND phase='rollback_paused'`, op.ID, op.TenantID, op.UserID, op.TaskID, op.Generation)
	if err != nil || command.RowsAffected() != 1 {
		if err == nil {
			err = types.ErrConflict
		}
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("checkpoint V3 definition restore", err)
	}
	updated, found, err := loadResearchV3CutoverTx(ctx, tx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.ErrNotFound
		}
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("commit V3 definition restore", err)
	}
	return updated, nil
}

func requireResearchV3ProductionHeadTx(ctx context.Context, tx pgx.Tx, op types.ResearchV3CutoverOperation, promoted bool) error {
	var mode string
	var version *int64
	var digest *string
	if err := tx.QueryRow(ctx, `SELECT execution_mode,approved_definition_version,approved_definition_digest FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3 FOR SHARE`, op.TenantID, op.UserID, op.TaskID).Scan(&mode, &version, &digest); err != nil {
		return taskStateDatabaseError("load V3 production definition head", err)
	}
	if promoted {
		if mode != string(types.ExecutionModeDiscoverAtRun) || version == nil || digest == nil || *version != op.Definition.Version || *digest != op.Definition.Digest {
			return researchV3CutoverDrift("promoted V3 definition changed")
		}
		return nil
	}
	if mode != string(op.OriginalExecutionMode) || !nullableHeadEqual(version, digest, op.OriginalDefinition) {
		return researchV3CutoverDrift("production definition changed before V3 promotion")
	}
	return nil
}

func researchV3CutoverDrift(message string) error {
	return types.NewAppError(types.CodeConflict, message, types.ErrResearchV3CutoverDrift)
}

func nullableDefinitionVersion(head *types.ResearchV3DefinitionHead) any {
	if head == nil {
		return nil
	}
	return head.Version
}
func nullableDefinitionDigest(head *types.ResearchV3DefinitionHead) any {
	if head == nil {
		return nil
	}
	return head.Digest
}
func nullableHeadEqual(version *int64, digest *string, head *types.ResearchV3DefinitionHead) bool {
	if head == nil {
		return version == nil && digest == nil
	}
	return version != nil && digest != nil && *version == head.Version && *digest == head.Digest
}

func researchV3HeadsEqual(left, right *types.ResearchV3DefinitionHead) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Version == right.Version && subtle.ConstantTimeCompare(
		[]byte(left.Digest), []byte(right.Digest)) == 1
}

func (s *Store) AdvanceResearchV3Cutover(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	expected, next types.ResearchV3CutoverPhase,
) (types.ResearchV3CutoverOperation, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("begin V3 cutover checkpoint", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, op.TenantID, op.UserID); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if next == types.ResearchV3CutoverPauseRequested || next == types.ResearchV3CutoverPaused || next == types.ResearchV3CutoverActionSwapped ||
		next == types.ResearchV3CutoverActive {
		promoted := next == types.ResearchV3CutoverActionSwapped || next == types.ResearchV3CutoverActive
		if !promoted {
			binding, headErr := loadPreparedResearchV3BindingTx(
				ctx, tx, op.TenantID, op.UserID, op.TaskID, false,
				researchV3ExpectBaseHead)
			if headErr != nil || binding.Target != op.Definition ||
				binding.ScheduleStatus != op.OriginalScheduleStatus ||
				binding.SourceBaselineDigest != op.SourceBaselineDigest ||
				binding.BaseMode != op.OriginalExecutionMode ||
				!researchV3HeadsEqual(binding.BaseHead, op.OriginalDefinition) {
				if headErr != nil {
					if types.CodeOf(headErr) == types.CodeDatabase || types.CodeOf(headErr) == types.CodeInternal {
						return types.ResearchV3CutoverOperation{}, headErr
					}
					return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
						"prepared V3 definition is no longer admissible")
				}
				return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
					"prepared V3 definition changed during cutover")
			}
		} else {
			binding, headErr := loadPreparedResearchV3BindingTx(
				ctx, tx, op.TenantID, op.UserID, op.TaskID, false,
				researchV3ExpectTargetHead)
			if headErr != nil || binding.Target != op.Definition ||
				binding.ScheduleStatus != op.OriginalScheduleStatus ||
				binding.SourceBaselineDigest != op.SourceBaselineDigest {
				if headErr != nil {
					if types.CodeOf(headErr) == types.CodeDatabase || types.CodeOf(headErr) == types.CodeInternal {
						return types.ResearchV3CutoverOperation{}, headErr
					}
					return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
						"prepared V3 baseline is no longer admissible")
				}
				return types.ResearchV3CutoverOperation{}, researchV3CutoverDrift(
					"prepared V3 baseline changed after promotion")
			}
		}
		if headErr := requireResearchV3ProductionHeadTx(ctx, tx, op, promoted); headErr != nil {
			return types.ResearchV3CutoverOperation{}, headErr
		}
	}
	command, err := tx.Exec(ctx,
		`UPDATE research_v3_cutover_operations
		    SET phase=$1
		  WHERE id=$2 AND tenant_id=$3 AND user_id=$4 AND task_id=$5
		    AND generation=$6 AND phase=$7`,
		next, op.ID, op.TenantID, op.UserID, op.TaskID, op.Generation, expected)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("advance V3 cutover checkpoint", err)
	}
	if command.RowsAffected() != 1 {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeConflict, "research V3 cutover checkpoint changed", types.ErrConflict)
	}
	if next == types.ResearchV3CutoverActive {
		command, err = tx.Exec(ctx,
			`UPDATE research_v3_delivery_authorities
			    SET status='enabled',enabled_at=clock_timestamp()
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=$4
			    AND definition_version=$5 AND definition_digest=$6
			    AND target_action_digest=$7 AND status='staged'`,
			op.TenantID, op.UserID, op.TaskID, op.Generation,
			op.Definition.Version, op.Definition.Digest, op.TargetActionDigest)
		if err != nil || command.RowsAffected() != 1 {
			if err == nil {
				err = types.ErrConflict
			}
			return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("enable V3 delivery authority", err)
		}
	}
	updated, found, err := loadResearchV3CutoverTx(
		ctx, tx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.ErrNotFound
		}
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("commit V3 cutover checkpoint", err)
	}
	return updated, nil
}

func (s *Store) BeginResearchV3RollbackPause(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	conflictToken []byte, tokenDigest string,
) (types.ResearchV3CutoverOperation, error) {
	digest := sha256.Sum256(conflictToken)
	if len(conflictToken) == 0 || len(conflictToken) > 4096 ||
		!validDigestSyntaxV3(tokenDigest) || subtle.ConstantTimeCompare(
		[]byte(tokenDigest), []byte(hex.EncodeToString(digest[:]))) != 1 {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeValidation, "research V3 rollback pause token is invalid", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("begin V3 rollback pause", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, op.TenantID, op.UserID); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", op.TenantID, op.UserID, op.TaskID)); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("lock V3 rollback pause", err)
	}
	command, err := tx.Exec(ctx,
		`UPDATE research_v3_cutover_operations operation
		    SET phase='rollback_pause_requested',rollback_conflict_token=$1,
		        rollback_token_digest=$2
		  WHERE operation.id=$3 AND operation.tenant_id=$4 AND operation.user_id=$5
		    AND operation.task_id=$6 AND operation.generation=$7
		    AND operation.phase IN ('active','action_swapped')
		    AND operation.rollback_conflict_token IS NULL
		    AND EXISTS (SELECT 1 FROM research_v3_delivery_authorities authority
		                 WHERE authority.tenant_id=operation.tenant_id
		                   AND authority.user_id=operation.user_id
		                   AND authority.task_id=operation.task_id
		                   AND authority.generation=operation.generation
		                   AND authority.status='revoked')`,
		conflictToken, tokenDigest, op.ID, op.TenantID, op.UserID, op.TaskID, op.Generation)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("checkpoint V3 rollback pause request", err)
	}
	if command.RowsAffected() != 1 {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeConflict, "research V3 rollback pause checkpoint changed", types.ErrConflict)
	}
	updated, found, err := loadResearchV3CutoverTx(
		ctx, tx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.ErrNotFound
		}
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3CutoverOperation{}, taskStateDatabaseError("commit V3 rollback pause request", err)
	}
	return updated, nil
}

// RevokeResearchV3DeliveryAuthority is deliberately the first rollback step.
// New effect claims fail immediately after this transaction commits; receipt
// settlement for an already-sent effect does not call this admission helper.
func (s *Store) RevokeResearchV3DeliveryAuthority(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) error {
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return taskStateDatabaseError("begin V3 authority revocation", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, op.TenantID, op.UserID); err != nil {
		return err
	}
	command, err := tx.Exec(ctx,
		`UPDATE research_v3_delivery_authorities
		    SET status='revoked',revoked_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=$4
		    AND target_action_digest=$5 AND status IN ('staged','enabled')`,
		op.TenantID, op.UserID, op.TaskID, op.Generation, op.TargetActionDigest)
	if err != nil {
		return taskStateDatabaseError("revoke V3 delivery authority", err)
	}
	if command.RowsAffected() == 1 {
		if err := tx.Commit(ctx); err != nil {
			return taskStateDatabaseError("commit V3 authority revocation", err)
		}
		return nil
	}
	var status string
	err = tx.QueryRow(ctx,
		`SELECT status FROM research_v3_delivery_authorities
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=$4
		    AND target_action_digest=$5`,
		op.TenantID, op.UserID, op.TaskID, op.Generation, op.TargetActionDigest).Scan(&status)
	if err == nil && status == "revoked" {
		if err := tx.Commit(ctx); err != nil {
			return taskStateDatabaseError("commit V3 authority revocation", err)
		}
		return nil
	}
	return types.NewAppError(types.CodeConflict,
		"research V3 delivery authority cannot be revoked", types.ErrConflict)
}

func bindResearchV3CutoverOperatorTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) error {
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
		return taskStateDatabaseError("bind V3 cutover scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_research_v3_cutover_operator`); err != nil {
		return taskStateDatabaseError("bind V3 cutover operator", err)
	}
	return nil
}

// RequireResearchV3DeliveryAuthorityForClaimTx is the delivery adapter's
// same-transaction fence. The caller must invoke it in the transaction that
// changes a prepared effect into its provider claim. Never call it when merely
// settling a receipt for an effect whose provider send was already recorded.
func RequireResearchV3DeliveryAuthorityForClaimTx(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3,
) (types.ResearchV3DeliveryAuthority, error) {
	if tx == nil || snapshot.ValidateFor(identity) != nil {
		return types.ResearchV3DeliveryAuthority{}, types.NewAppError(
			types.CodeValidation, "research V3 delivery claim scope is invalid", types.ErrValidation)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.user_id',$1,true)`,
		fmt.Sprint(identity.UserID)); err != nil {
		return types.ResearchV3DeliveryAuthority{}, taskStateDatabaseError(
			"bind V3 delivery authority user", err)
	}
	authority := types.ResearchV3DeliveryAuthority{
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
	}
	// Migration 101 takes the same exact-task lock for authority and Schedule
	// head transitions. It remains held through the caller's effect claim,
	// closing revoke/edit races while the coordinator stays subject to RLS.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", identity.TenantID, identity.UserID, identity.TaskID)); err != nil {
		return types.ResearchV3DeliveryAuthority{}, taskStateDatabaseError(
			"lock V3 delivery authority", err)
	}
	err := tx.QueryRow(ctx,
		`SELECT authority.generation,authority.definition_version,
		        authority.definition_digest,authority.target_action_digest,
		        authority.action_authorization_digest
		   FROM research_v3_delivery_authorities authority
		   JOIN schedules schedule
		     ON schedule.tenant_id=authority.tenant_id
		    AND schedule.user_id=authority.user_id
		    AND schedule.id=authority.task_id
		   JOIN task_run_snapshots snapshot ON snapshot.id=$4
		  WHERE authority.tenant_id=$1 AND authority.user_id=$2
		    AND authority.task_id=$3 AND authority.status='enabled'
		    AND authority.definition_version=$5
		    AND authority.definition_digest=$6
		    AND schedule.status='active'
		    AND schedule.execution_mode='discover_at_run'
		    AND schedule.approved_definition_version=authority.definition_version
		    AND schedule.approved_definition_digest=authority.definition_digest
		    AND snapshot.tenant_id=authority.tenant_id
		    AND snapshot.user_id=authority.user_id
		    AND snapshot.task_id=authority.task_id
		    AND snapshot.temporal_workflow_id=$7
		    AND snapshot.temporal_run_id=$8
		    AND snapshot.definition_digest=authority.definition_digest
		    AND snapshot.reference_digest=$9
		    AND snapshot.reference_schema_version=$10
		    AND snapshot.created_at>=authority.enabled_at`,
		identity.TenantID, identity.UserID, identity.TaskID, snapshot.SnapshotID,
		snapshot.DefinitionVersion, snapshot.DefinitionDigest,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		snapshot.ReferenceDigest, types.ResearchRunSnapshotRefSchemaV3).Scan(
		&authority.Generation, &authority.DefinitionVersion,
		&authority.DefinitionDigest, &authority.TargetActionDigest,
		&authority.ActionAuthorizationDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ResearchV3DeliveryAuthority{}, types.NewAppError(
			types.CodeConflict, "research V3 delivery authority is unavailable", types.ErrConflict)
	}
	if err != nil {
		return types.ResearchV3DeliveryAuthority{}, taskStateDatabaseError("claim V3 delivery authority", err)
	}
	if snapshot.AuthorityGeneration <= 0 ||
		authority.Generation != snapshot.AuthorityGeneration ||
		subtle.ConstantTimeCompare([]byte(authority.TargetActionDigest),
			[]byte(snapshot.TargetActionDigest)) != 1 ||
		subtle.ConstantTimeCompare([]byte(authority.ActionAuthorizationDigest),
			[]byte(snapshot.ActionAuthorizationDigest)) != 1 {
		return types.ResearchV3DeliveryAuthority{}, types.NewAppError(
			types.CodeConflict, "research V3 delivery authority is unavailable", types.ErrConflict)
	}
	authority.Enabled = true
	return authority, nil
}

func loadCanonicalResearchV3HeadTx(
	ctx context.Context, q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}, tenantID, userID int64, taskID string, lock bool,
) (types.ResearchV3DefinitionHead, error) {
	lockSQL := ""
	if lock {
		lockSQL = " FOR SHARE OF schedule,definition"
	}
	var version int64
	var digest, schemaVersion, mode string
	var payload []byte
	err := q.QueryRow(ctx,
		`SELECT definition.version,definition.definition_digest,
		        definition.schema_version,definition.execution_mode,definition.payload
		   FROM schedules schedule
		   JOIN tenants tenant ON tenant.id=schedule.tenant_id
		   JOIN memberships membership
		     ON membership.tenant_id=schedule.tenant_id AND membership.user_id=schedule.user_id
		   JOIN task_approved_definition_versions definition
		     ON definition.tenant_id=schedule.tenant_id AND definition.user_id=schedule.user_id
		    AND definition.task_id=schedule.id
		    AND definition.version=schedule.approved_definition_version
		    AND definition.definition_digest=schedule.approved_definition_digest
		  WHERE schedule.tenant_id=$1 AND schedule.user_id=$2 AND schedule.id=$3
		    AND schedule.status IN ('active','paused') AND tenant.status='active'
		    AND tenant.deleted_at IS NULL`+lockSQL,
		tenantID, userID, taskID).Scan(&version, &digest, &schemaVersion, &mode, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ResearchV3DefinitionHead{}, types.NewAppError(
			types.CodeNotFound, "research V3 task head is unavailable", types.ErrNotFound)
	}
	if err != nil {
		return types.ResearchV3DefinitionHead{}, taskStateDatabaseError("load V3 cutover definition", err)
	}
	definition, decodeErr := taskstate.DecodeApprovedDefinitionV3(payload)
	canonical, encodeErr := taskstate.EncodeApprovedDefinitionV3(definition)
	if decodeErr != nil || encodeErr != nil || !bytes.Equal(canonical, payload) ||
		schemaVersion != taskstate.ApprovedDefinitionSchemaVersionV3 ||
		mode != string(types.ExecutionModeDiscoverAtRun) ||
		definition.TenantID != tenantID || definition.UserID != userID ||
		definition.TaskID != taskID || definition.ExecutionMode != types.ExecutionModeDiscoverAtRun ||
		len(digest) != 64 || subtle.ConstantTimeCompare([]byte(digest), []byte(taskstateDigest(payload))) != 1 {
		return types.ResearchV3DefinitionHead{}, taskStateIntegrity()
	}
	return types.ResearchV3DefinitionHead{Version: version, Digest: digest}, nil
}

func taskstateDigest(payload []byte) string {
	// constantTimeDigestMatches is the package's single canonical SHA-256
	// implementation; expose its expected value without duplicating validation.
	definition, err := taskstate.DecodeApprovedDefinitionV3(payload)
	if err != nil {
		return ""
	}
	digest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil {
		return ""
	}
	return digest
}

func loadResearchV3CutoverTx(
	ctx context.Context, q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}, tenantID, userID int64, taskID, key string,
) (types.ResearchV3CutoverOperation, bool, error) {
	op, err := scanResearchV3Cutover(q.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,task_id,idempotency_key,generation,
		        definition_version,definition_digest,frozen_schedule,
		        frozen_schedule_digest,frozen_conflict_token,conflict_token_digest,
		        rollback_conflict_token,rollback_token_digest,
		        target_action,target_action_digest,action_authorization_digest,
		        original_paused,original_schedule_status,preflight_digest,
		        original_execution_mode,original_definition_version,
		        original_definition_digest,source_baseline_digest,phase,created_at,updated_at
		   FROM research_v3_cutover_operations
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND idempotency_key=$4`,
		tenantID, userID, taskID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ResearchV3CutoverOperation{}, false, nil
	}
	if err != nil {
		return types.ResearchV3CutoverOperation{}, false, taskStateDatabaseError("load V3 cutover journal", err)
	}
	return op, true, nil
}

func scanResearchV3Cutover(row pgx.Row) (types.ResearchV3CutoverOperation, error) {
	var op types.ResearchV3CutoverOperation
	var rollbackDigest *string
	var originalVersion *int64
	var originalDigest *string
	err := row.Scan(&op.ID, &op.TenantID, &op.UserID, &op.TaskID,
		&op.IdempotencyKey, &op.Generation, &op.Definition.Version,
		&op.Definition.Digest, &op.FrozenSchedule, &op.FrozenScheduleDigest,
		&op.FrozenConflictToken, &op.ConflictTokenDigest,
		&op.RollbackConflictToken, &rollbackDigest,
		&op.TargetAction, &op.TargetActionDigest, &op.ActionAuthorizationDigest,
		&op.OriginalPaused, &op.OriginalScheduleStatus, &op.PreflightDigest,
		&op.OriginalExecutionMode, &originalVersion, &originalDigest,
		&op.SourceBaselineDigest, &op.Phase, &op.CreatedAt, &op.UpdatedAt)
	if err != nil {
		return op, err
	}
	if (originalVersion == nil) != (originalDigest == nil) ||
		(op.OriginalScheduleStatus != types.ScheduleStatusActive &&
			op.OriginalScheduleStatus != types.ScheduleStatusPaused) ||
		!validDigestSyntaxV3(op.PreflightDigest) ||
		!validDigestSyntaxV3(op.SourceBaselineDigest) {
		return op, taskStateIntegrity()
	}
	if rollbackDigest != nil {
		op.RollbackTokenDigest = *rollbackDigest
	}
	if originalVersion != nil {
		op.OriginalDefinition = &types.ResearchV3DefinitionHead{
			Version: *originalVersion, Digest: *originalDigest,
		}
	}
	return op, nil
}

func validateResearchV3CutoverBegin(p types.BeginResearchV3CutoverParams) error {
	// Digest syntax is checked here; equality to payload bytes is checked by
	// the scheduler before calling Store and by database CHECK constraints.
	if p.TenantID <= 0 || p.UserID <= 0 || p.TaskID == "" || strings.TrimSpace(p.TaskID) != p.TaskID ||
		len(p.TaskID) > 255 || p.IdempotencyKey == "" || strings.TrimSpace(p.IdempotencyKey) != p.IdempotencyKey ||
		len(p.IdempotencyKey) > 512 || p.Definition.Version <= 0 ||
		!validDigestSyntaxV3(p.Definition.Digest) || !validDigestSyntaxV3(p.FrozenScheduleDigest) ||
		!validDigestSyntaxV3(p.ConflictTokenDigest) || !validDigestSyntaxV3(p.TargetActionDigest) ||
		!validDigestSyntaxV3(p.ActionAuthorizationDigest) ||
		!validDigestSyntaxV3(p.PreflightDigest) ||
		(p.OriginalScheduleStatus != types.ScheduleStatusActive &&
			p.OriginalScheduleStatus != types.ScheduleStatusPaused) ||
		len(p.FrozenSchedule) == 0 || len(p.FrozenConflictToken) == 0 || len(p.FrozenConflictToken) > 4096 ||
		len(p.FrozenSchedule) > 1<<20 || len(p.TargetAction) == 0 || len(p.TargetAction) > 1<<19 {
		return types.NewAppError(types.CodeValidation,
			"research V3 cutover input is invalid", types.ErrValidation)
	}
	frozenDigest := sha256.Sum256(p.FrozenSchedule)
	conflictDigest := sha256.Sum256(p.FrozenConflictToken)
	targetDigest := sha256.Sum256(p.TargetAction)
	if subtle.ConstantTimeCompare(
		[]byte(p.FrozenScheduleDigest), []byte(hex.EncodeToString(frozenDigest[:]))) != 1 ||
		subtle.ConstantTimeCompare(
			[]byte(p.ConflictTokenDigest), []byte(hex.EncodeToString(conflictDigest[:]))) != 1 ||
		subtle.ConstantTimeCompare(
			[]byte(p.TargetActionDigest), []byte(hex.EncodeToString(targetDigest[:]))) != 1 {
		return types.NewAppError(types.CodeValidation,
			"research V3 cutover artifact digest is invalid", types.ErrValidation)
	}
	return nil
}

func validDigestSyntaxV3(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func researchV3CutoverMatches(
	op types.ResearchV3CutoverOperation, p types.BeginResearchV3CutoverParams,
) bool {
	return op.Definition == p.Definition && op.OriginalPaused == p.OriginalPaused &&
		op.OriginalScheduleStatus == p.OriginalScheduleStatus &&
		op.PreflightDigest == p.PreflightDigest &&
		op.FrozenScheduleDigest == p.FrozenScheduleDigest &&
		op.ConflictTokenDigest == p.ConflictTokenDigest &&
		op.TargetActionDigest == p.TargetActionDigest &&
		op.ActionAuthorizationDigest == p.ActionAuthorizationDigest &&
		bytes.Equal(op.FrozenSchedule, p.FrozenSchedule) &&
		bytes.Equal(op.FrozenConflictToken, p.FrozenConflictToken) &&
		bytes.Equal(op.TargetAction, p.TargetAction)
}
