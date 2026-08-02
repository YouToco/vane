package store

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func (s *Store) PrepareResearchV3Definition(
	ctx context.Context, p taskstate.ResearchV3DefinitionPrepareParams,
) (types.ResearchV3DefinitionPrepareOperation, error) {
	if err := validateResearchV3DefinitionPrepare(p); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("begin V3 definition prepare", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, p.TenantID, p.UserID); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`,
		fmt.Sprintf("%d/%d/%s", p.TenantID, p.UserID, p.TaskID)); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("lock V3 definition prepare", err)
	}

	var name, manual, rawMode string
	var spec []byte
	var mainVersion *int64
	var mainDigest *string
	err = tx.QueryRow(ctx,
		`SELECT schedule.nl_description,playbook.content,schedule.spec_json,
		        schedule.execution_mode,schedule.approved_definition_version,
		        schedule.approved_definition_digest
		   FROM schedules schedule
		   JOIN tenants tenant ON tenant.id=schedule.tenant_id
		   JOIN memberships membership
		     ON membership.tenant_id=schedule.tenant_id AND membership.user_id=schedule.user_id
		   JOIN schedule_playbooks playbook ON playbook.schedule_id=schedule.id
		  WHERE schedule.tenant_id=$1 AND schedule.user_id=$2 AND schedule.id=$3
		    AND schedule.status IN ('active','paused') AND tenant.status='active'
		    AND tenant.deleted_at IS NULL AND membership.role='owner'
		`, p.TenantID, p.UserID, p.TaskID).Scan(
		&name, &manual, &spec, &rawMode, &mainVersion, &mainDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(
			types.CodeNotFound, "owner research task is unavailable", types.ErrNotFound)
	}
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("load V3 prepare projection", err)
	}
	originalMode := types.ExecutionMode(rawMode)
	if (originalMode == types.ExecutionModeCompiled && (mainVersion != nil || mainDigest != nil)) ||
		(originalMode == types.ExecutionModeDiscoverAtRun && (mainVersion == nil || mainDigest == nil)) {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateIntegrity()
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: p.TenantID, UserID: p.UserID, TaskID: p.TaskID,
		TaskName: name, TaskManual: manual, SpecJSON: spec,
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification:  p.Notification, Output: p.Output, PlannerBudget: p.PlannerBudget,
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(
			types.CodeValidation, "current task cannot be compiled as V3", err)
	}
	payload, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateIntegrity()
	}
	digest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateIntegrity()
	}
	if existing, found, loadErr := loadResearchV3DefinitionPrepareTx(
		ctx, tx, p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey,
	); loadErr != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, loadErr
	} else if found {
		var stored []byte
		loadErr = tx.QueryRow(ctx,
			`SELECT payload FROM task_approved_definition_versions
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=$4
			    AND definition_digest=$5`, p.TenantID, p.UserID, p.TaskID,
			existing.Target.Version, existing.Target.Digest).Scan(&stored)
		if loadErr != nil || existing.Phase != types.ResearchV3DefinitionPrepared ||
			subtle.ConstantTimeCompare([]byte(existing.Target.Digest), []byte(digest)) != 1 ||
			!bytes.Equal(stored, payload) {
			return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(
				types.CodeConflict, "V3 definition prepare key has different immutable input", types.ErrConflict)
		}
		var currentVersion int64
		var currentDigest string
		if err := tx.QueryRow(ctx,
			`SELECT definition_version,definition_digest
			   FROM research_v3_prepared_definition_heads
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			p.TenantID, p.UserID, p.TaskID).Scan(&currentVersion, &currentDigest); err != nil ||
			currentVersion != existing.Target.Version || currentDigest != existing.Target.Digest {
			return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(
				types.CodeConflict, "V3 prepared definition head has advanced", types.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("commit V3 prepare replay", err)
		}
		return existing, nil
	}

	var previousVersion *int64
	var previousDigest *string
	err = tx.QueryRow(ctx,
		`SELECT definition_version,definition_digest
		   FROM research_v3_prepared_definition_heads
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 FOR UPDATE`,
		p.TenantID, p.UserID, p.TaskID).Scan(&previousVersion, &previousDigest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("load previous V3 prepared head", err)
	}
	var operationID, version int64
	if err := tx.QueryRow(ctx, `SELECT nextval('research_v3_definition_prepare_operations_id_seq')`).Scan(&operationID); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("allocate V3 prepare operation", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(max(version),0)+1 FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		p.TenantID, p.UserID, p.TaskID).Scan(&version); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("allocate V3 definition version", err)
	}
	operationRef := fmt.Sprintf("research-v3-prepare/%d", operationID)
	if _, err := tx.Exec(ctx,
		`INSERT INTO task_approved_definition_versions
		 (tenant_id,user_id,task_id,version,schema_version,execution_mode,
		  definition_digest,payload,operation_ref)
		 VALUES ($1,$2,$3,$4,$5,'discover_at_run',$6,$7,$8)`,
		p.TenantID, p.UserID, p.TaskID, version,
		taskstate.ApprovedDefinitionSchemaVersionV3, digest, payload, operationRef); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("insert immutable V3 definition", err)
	}
	var op types.ResearchV3DefinitionPrepareOperation
	op, err = scanResearchV3DefinitionPrepare(tx.QueryRow(ctx,
		`INSERT INTO research_v3_definition_prepare_operations
		 (id,tenant_id,user_id,task_id,idempotency_key,target_definition_version,
		  target_definition_digest,previous_definition_version,previous_definition_digest,
		  original_execution_mode,original_definition_version,original_definition_digest)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 RETURNING id,tenant_id,user_id,task_id,idempotency_key,
		  target_definition_version,target_definition_digest,
		  previous_definition_version,previous_definition_digest,
		  original_execution_mode,original_definition_version,original_definition_digest,
		  phase,created_at,updated_at`, operationID, p.TenantID, p.UserID, p.TaskID,
		p.IdempotencyKey, version, digest, previousVersion, previousDigest,
		rawMode, mainVersion, mainDigest))
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("insert V3 prepare journal", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO research_v3_prepared_definition_heads
		 (tenant_id,user_id,task_id,definition_version,definition_digest,prepare_operation_id)
		 VALUES ($1,$2,$3,$4,$5,$6)
		 ON CONFLICT (tenant_id,user_id,task_id) DO UPDATE SET
		 definition_version=EXCLUDED.definition_version,
		 definition_digest=EXCLUDED.definition_digest,
		 prepare_operation_id=EXCLUDED.prepare_operation_id,
		 updated_at=clock_timestamp()`, p.TenantID, p.UserID, p.TaskID,
		version, digest, operationID); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("publish V3 prepared head", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("commit V3 definition prepare", err)
	}
	return op, nil
}

func (s *Store) RollbackResearchV3DefinitionPrepare(
	ctx context.Context, tenantID, userID int64, taskID, idempotencyKey string,
) (types.ResearchV3DefinitionPrepareOperation, error) {
	if tenantID <= 0 || userID <= 0 || taskID == "" || idempotencyKey == "" {
		return types.ResearchV3DefinitionPrepareOperation{}, types.ErrValidation
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("begin V3 prepare rollback", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3CutoverOperatorTx(ctx, tx, tenantID, userID); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,101))`, fmt.Sprintf("%d/%d/%s", tenantID, userID, taskID)); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	op, found, err := loadResearchV3DefinitionPrepareTx(ctx, tx, tenantID, userID, taskID, idempotencyKey)
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	if !found {
		return types.ResearchV3DefinitionPrepareOperation{}, types.ErrNotFound
	}
	if op.Phase == types.ResearchV3DefinitionRolledBack {
		_ = tx.Commit(ctx)
		return op, nil
	}
	var currentVersion int64
	var currentDigest string
	if err := tx.QueryRow(ctx, `SELECT definition_version,definition_digest FROM research_v3_prepared_definition_heads WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 FOR UPDATE`, tenantID, userID, taskID).Scan(&currentVersion, &currentDigest); err != nil || currentVersion != op.Target.Version || currentDigest != op.Target.Digest {
		return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(types.CodeConflict, "V3 prepared head changed before rollback", types.ErrConflict)
	}
	if op.PreviousPreparedHead == nil {
		_, err = tx.Exec(ctx, `DELETE FROM research_v3_prepared_definition_heads WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND definition_version=$4 AND definition_digest=$5`, tenantID, userID, taskID, op.Target.Version, op.Target.Digest)
	} else {
		_, err = tx.Exec(ctx, `UPDATE research_v3_prepared_definition_heads SET definition_version=$1,definition_digest=$2,prepare_operation_id=(SELECT id FROM research_v3_definition_prepare_operations WHERE tenant_id=$3 AND user_id=$4 AND task_id=$5 AND target_definition_version=$1 AND target_definition_digest=$2 ORDER BY id DESC LIMIT 1),updated_at=clock_timestamp() WHERE tenant_id=$3 AND user_id=$4 AND task_id=$5 AND definition_version=$6 AND definition_digest=$7`, op.PreviousPreparedHead.Version, op.PreviousPreparedHead.Digest, tenantID, userID, taskID, op.Target.Version, op.Target.Digest)
	}
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("restore V3 prepared head", err)
	}
	op, err = scanResearchV3DefinitionPrepare(tx.QueryRow(ctx, `UPDATE research_v3_definition_prepare_operations SET phase='rolled_back' WHERE id=$1 AND phase='prepared' RETURNING id,tenant_id,user_id,task_id,idempotency_key,target_definition_version,target_definition_digest,previous_definition_version,previous_definition_digest,original_execution_mode,original_definition_version,original_definition_digest,phase,created_at,updated_at`, op.ID))
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("checkpoint V3 prepare rollback", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, taskStateDatabaseError("commit V3 prepare rollback", err)
	}
	return op, nil
}

func (s *Store) LoadPreparedResearchV3DefinitionHead(ctx context.Context, tenantID, userID int64, taskID string) (types.ResearchV3DefinitionHead, error) {
	return loadPreparedResearchV3HeadTx(ctx, s.pool, tenantID, userID, taskID, false)
}

func loadPreparedResearchV3HeadTx(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenantID, userID int64, taskID string, lock bool) (types.ResearchV3DefinitionHead, error) {
	// Sidecar writers already hold the exact-task advisory lock and approved
	// definition rows are immutable. A SQL row lock here would only broaden the
	// operator's UPDATE privileges without adding a useful fence.
	_ = lock
	var head types.ResearchV3DefinitionHead
	var schema, mode string
	var payload []byte
	err := q.QueryRow(ctx, `SELECT head.definition_version,head.definition_digest,definition.schema_version,definition.execution_mode,definition.payload FROM research_v3_prepared_definition_heads head JOIN schedules schedule ON schedule.tenant_id=head.tenant_id AND schedule.user_id=head.user_id AND schedule.id=head.task_id JOIN tenants tenant ON tenant.id=head.tenant_id JOIN memberships membership ON membership.tenant_id=head.tenant_id AND membership.user_id=head.user_id JOIN task_approved_definition_versions definition ON definition.tenant_id=head.tenant_id AND definition.user_id=head.user_id AND definition.task_id=head.task_id AND definition.version=head.definition_version AND definition.definition_digest=head.definition_digest AND definition.execution_mode=head.execution_mode WHERE head.tenant_id=$1 AND head.user_id=$2 AND head.task_id=$3 AND schedule.status IN ('active','paused') AND tenant.status='active' AND tenant.deleted_at IS NULL`, tenantID, userID, taskID).Scan(&head.Version, &head.Digest, &schema, &mode, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return head, types.NewAppError(types.CodeNotFound, "prepared research V3 definition is unavailable", types.ErrNotFound)
	}
	if err != nil {
		return head, taskStateDatabaseError("load prepared research V3 definition", err)
	}
	definition, decodeErr := taskstate.DecodeApprovedDefinitionV3(payload)
	canonical, encodeErr := taskstate.EncodeApprovedDefinitionV3(definition)
	if decodeErr != nil || encodeErr != nil || !bytes.Equal(canonical, payload) || schema != taskstate.ApprovedDefinitionSchemaVersionV3 || mode != string(types.ExecutionModeDiscoverAtRun) || definition.TenantID != tenantID || definition.UserID != userID || definition.TaskID != taskID || definition.ExecutionMode != types.ExecutionModeDiscoverAtRun || subtle.ConstantTimeCompare([]byte(head.Digest), []byte(taskstateDigest(payload))) != 1 {
		return types.ResearchV3DefinitionHead{}, taskStateIntegrity()
	}
	return head, nil
}

func loadResearchV3DefinitionPrepareTx(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenantID, userID int64, taskID, key string) (types.ResearchV3DefinitionPrepareOperation, bool, error) {
	op, err := scanResearchV3DefinitionPrepare(q.QueryRow(ctx, `SELECT id,tenant_id,user_id,task_id,idempotency_key,target_definition_version,target_definition_digest,previous_definition_version,previous_definition_digest,original_execution_mode,original_definition_version,original_definition_digest,phase,created_at,updated_at FROM research_v3_definition_prepare_operations WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND idempotency_key=$4`, tenantID, userID, taskID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return op, false, nil
	}
	if err != nil {
		return op, false, taskStateDatabaseError("load V3 definition prepare", err)
	}
	return op, true, nil
}

func scanResearchV3DefinitionPrepare(row pgx.Row) (types.ResearchV3DefinitionPrepareOperation, error) {
	var op types.ResearchV3DefinitionPrepareOperation
	var previousVersion, originalVersion *int64
	var previousDigest, originalDigest *string
	err := row.Scan(&op.ID, &op.TenantID, &op.UserID, &op.TaskID, &op.IdempotencyKey,
		&op.Target.Version, &op.Target.Digest, &previousVersion, &previousDigest,
		&op.OriginalMode, &originalVersion, &originalDigest, &op.Phase, &op.CreatedAt, &op.UpdatedAt)
	if err != nil {
		return op, err
	}
	if (previousVersion == nil) != (previousDigest == nil) || (originalVersion == nil) != (originalDigest == nil) {
		return op, taskStateIntegrity()
	}
	if previousVersion != nil {
		op.PreviousPreparedHead = &types.ResearchV3DefinitionHead{Version: *previousVersion, Digest: *previousDigest}
	}
	if originalVersion != nil {
		op.OriginalHead = &types.ResearchV3DefinitionHead{Version: *originalVersion, Digest: *originalDigest}
	}
	return op, nil
}

func validateResearchV3DefinitionPrepare(p taskstate.ResearchV3DefinitionPrepareParams) error {
	if p.TenantID <= 0 || p.UserID <= 0 || p.TaskID == "" || strings.TrimSpace(p.TaskID) != p.TaskID || len(p.TaskID) > 255 || p.IdempotencyKey == "" || strings.TrimSpace(p.IdempotencyKey) != p.IdempotencyKey || len(p.IdempotencyKey) > 512 {
		return types.NewAppError(types.CodeValidation, "V3 definition prepare input is invalid", types.ErrValidation)
	}
	return nil
}
