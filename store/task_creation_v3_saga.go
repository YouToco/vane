package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

// LoadResearchTaskCreationOperationV3 reads only the manage_tasks/V2 protocol.
// It never falls back to V1 even when an operation ID collides.
func (s *Store) LoadResearchTaskCreationOperationV3(
	ctx context.Context, id string, tenantID, userID int64,
) (*types.TaskCreationOperation, error) {
	if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) ||
		tenantID <= 0 || userID <= 0 {
		return nil, taskCreationValidation("invalid native V3 operation scope")
	}
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(s.pool.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM task_creation_operations
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND tool_name='manage_tasks' AND execution_version=$4`,
		id, tenantID, userID, types.TaskCreationExecutionVersionV2), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskCreationNotFound()
	}
	if err != nil {
		return nil, taskCreationDatabaseError("load native V3 operation", err)
	}
	return &op, nil
}

func (s *Store) LoadResearchTaskCreationOperationV3ByUser(
	ctx context.Context, id string, userID int64,
) (*types.TaskCreationOperation, error) {
	if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) ||
		len(id) > 255 || userID <= 0 {
		return nil, taskCreationValidation("invalid native V3 operation lookup")
	}
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(s.pool.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM task_creation_operations
		  WHERE id=$1 AND user_id=$2
		    AND tool_name='manage_tasks' AND execution_version=$3`,
		id, userID, types.TaskCreationExecutionVersionV2), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskCreationNotFound()
	}
	if err != nil {
		return nil, taskCreationDatabaseError("load native V3 operation by user", err)
	}
	return &op, nil
}

// AcquireResearchTaskCreationOperationV3 mirrors the retained lease/fence
// protocol while remaining exactly bound to manage_tasks/V2.
func (s *Store) AcquireResearchTaskCreationOperationV3(
	ctx context.Context, p types.AcquireTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if err := validateAcquireTaskCreationParams(p); err != nil {
		return nil, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin native V3 acquisition", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, now, err := loadScopedResearchTaskCreationOperationV3ForUpdate(ctx, tx,
		types.TaskCreationLease{ID: p.ID, TenantID: p.TenantID, UserID: p.UserID})
	if err != nil {
		return nil, err
	}
	if op.TombstonedAt != nil || taskCreationStatusIsTerminal(op.Status) {
		return nil, taskCreationTerminal()
	}
	if op.ReceiptProvider != "" || op.ReceiptTarget != "" {
		if op.ReceiptProvider != p.ReceiptProvider || op.ReceiptTarget != p.ReceiptTarget {
			return nil, taskCreationConflict("native V3 receipt target differs")
		}
	} else if op.Status == types.TaskOperationStatusPending &&
		(p.ReceiptProvider == "" || p.ReceiptTarget == "") {
		return nil, taskCreationValidation("pending native V3 operation requires a receipt target")
	}

	switch op.Status {
	case types.TaskOperationStatusPending:
		if !pendingTaskCreationOperationPristine(op) {
			return nil, taskCreationConflict("pending native V3 operation has saga state")
		}
		if !now.Before(op.ExpiresAt) {
			return s.expirePendingResearchTaskCreationV3(ctx, tx, p, op.Fence)
		}
		return s.acquirePendingResearchTaskCreationV3(ctx, tx, p, op.Fence)
	case types.TaskOperationStatusExecuting:
		if op.LeaseUntil == nil || op.TakeoverNotBefore == nil || op.LeaseOwner == "" ||
			op.Fence <= 0 || op.Attempt <= 0 {
			return nil, taskCreationConflict("executing native V3 operation has incomplete lease")
		}
		if op.ReceiptProvider == "" && p.ReceiptProvider != "" {
			err = scanTaskCreationOperation(tx.QueryRow(ctx, `
				UPDATE task_creation_operations
				   SET receipt_provider=$5,receipt_target=$6,updated_at=clock_timestamp()
				 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
				   AND tool_name='manage_tasks' AND execution_version=$4
				   AND status=$7 AND tombstoned_at IS NULL AND fence=$8
				   AND receipt_provider='' AND receipt_target=''
				 RETURNING `+taskCreationOperationColumns,
				p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV2,
				p.ReceiptProvider, p.ReceiptTarget, types.TaskOperationStatusExecuting,
				op.Fence), op)
			if err != nil {
				return nil, taskCreationDatabaseError("bind native V3 receipt target", err)
			}
		}
		if now.Before(*op.LeaseUntil) {
			if err := tx.Commit(ctx); err != nil {
				return nil, taskCreationDatabaseError("commit native V3 receipt binding", err)
			}
			if op.LeaseOwner == p.LeaseOwner {
				return op, nil
			}
			return op, taskCreationBusy()
		}
		if now.Before(*op.TakeoverNotBefore) {
			if err := tx.Commit(ctx); err != nil {
				return nil, taskCreationDatabaseError("commit native V3 receipt binding", err)
			}
			return op, taskCreationBusy()
		}
		return s.takeOverResearchTaskCreationV3(ctx, tx, p, op.Fence)
	default:
		return nil, taskCreationTerminal()
	}
}

func (s *Store) acquirePendingResearchTaskCreationV3(
	ctx context.Context, tx pgx.Tx, p types.AcquireTaskCreationOperationParams, oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx, `
		UPDATE task_creation_operations
		   SET status=$6,phase=$7,lease_owner=$5,
		       receipt_provider=$12,receipt_target=$13,
		       lease_until=clock_timestamp()+($8*interval '1 microsecond'),
		       takeover_not_before=clock_timestamp()+($9*interval '1 microsecond'),
		       fence=fence+1,attempt=attempt+1,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$4
		   AND status=$10 AND expires_at>clock_timestamp() AND tombstoned_at IS NULL
		   AND fence=$11 AND receipt_provider='' AND receipt_target=''
		 RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV2,
		p.LeaseOwner, types.TaskOperationStatusExecuting, types.TaskCreationPhaseClaimed,
		p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskCreationTakeoverSafetyGrace).Microseconds(),
		types.TaskOperationStatusPending, oldFence, p.ReceiptProvider, p.ReceiptTarget), &op)
	if err != nil {
		return nil, taskCreationDatabaseError("acquire pending native V3 operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit native V3 acquisition", err)
	}
	return &op, nil
}

func (s *Store) takeOverResearchTaskCreationV3(
	ctx context.Context, tx pgx.Tx, p types.AcquireTaskCreationOperationParams, oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx, `
		UPDATE task_creation_operations
		   SET lease_owner=$5,
		       receipt_provider=CASE WHEN receipt_provider='' AND receipt_target='' THEN $10 ELSE receipt_provider END,
		       receipt_target=CASE WHEN receipt_provider='' AND receipt_target='' THEN $11 ELSE receipt_target END,
		       lease_until=clock_timestamp()+($6*interval '1 microsecond'),
		       takeover_not_before=clock_timestamp()+($7*interval '1 microsecond'),
		       fence=fence+1,attempt=attempt+1,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$4
		   AND status=$8 AND tombstoned_at IS NULL AND fence=$9
		   AND takeover_not_before<=clock_timestamp()
		 RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV2,
		p.LeaseOwner, p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskCreationTakeoverSafetyGrace).Microseconds(),
		types.TaskOperationStatusExecuting, oldFence,
		p.ReceiptProvider, p.ReceiptTarget), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskCreationBusy()
	}
	if err != nil {
		return nil, taskCreationDatabaseError("take over native V3 operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit native V3 takeover", err)
	}
	return &op, nil
}

func (s *Store) expirePendingResearchTaskCreationV3(
	ctx context.Context, tx pgx.Tx, p types.AcquireTaskCreationOperationParams, oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx, `
		UPDATE task_creation_operations
		   SET status=$5,phase=$6,receipt_provider=$9,receipt_target=$10,
		       lease_owner='',lease_until=NULL,takeover_not_before=NULL,
		       tombstoned_at=clock_timestamp(),updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$4
		   AND status=$7 AND phase='' AND tombstoned_at IS NULL AND fence=$8
		 RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV2,
		types.TaskOperationStatusExpired, types.TaskCreationPhaseExpired,
		types.TaskOperationStatusPending, oldFence, p.ReceiptProvider, p.ReceiptTarget), &op)
	if err != nil {
		return nil, taskCreationDatabaseError("expire native V3 operation", err)
	}
	if err := insertResearchTaskCreationReceiptV3(ctx, tx, p.ID, p.TenantID, p.UserID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit native V3 expiry", err)
	}
	return nil, taskCreationTerminal()
}

func (s *Store) CheckpointResearchTaskCreationPreparedV3(
	ctx context.Context, lease types.TaskCreationLease, definition []byte,
	digest string, prepared []byte, taskID string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if len(definition) == 0 || len(prepared) == 0 || !validSHA256Digest(digest) {
		return taskCreationValidation("native V3 prepared evidence is incomplete")
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin native V3 prepared checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadLeasedResearchTaskCreationOperationV3(ctx, tx, lease)
	if err != nil {
		return err
	}
	if op.Phase != types.TaskCreationPhaseClaimed {
		if taskCreationPhaseRank(op.Phase) >= taskCreationPhaseRank(types.TaskCreationPhaseSchedulePrepared) &&
			bytes.Equal(op.CompiledDefinition, definition) && op.CompiledDigest == digest &&
			bytes.Equal(op.PreparedSchedule, prepared) && op.TaskID == taskID {
			return tx.Commit(ctx)
		}
		return taskCreationConflict("native V3 prepared checkpoint differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_operations
		   SET normalized_command=$6,compiled_definition=$6,compiled_digest=$7,
		       prepared_schedule=$8,task_id=$9,phase=$10,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$11
		   AND lease_owner=$4 AND fence=$5 AND lease_until>clock_timestamp()
		   AND status=$12 AND phase=$13`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		definition, digest, prepared, taskID, types.TaskCreationPhaseSchedulePrepared,
		types.TaskCreationExecutionVersionV2, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseClaimed)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			err = taskCreationLeaseLost()
		}
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CheckpointResearchTaskCreationEnsureReceiptV3(
	ctx context.Context, lease types.TaskCreationLease, receipt []byte, taskID string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if len(receipt) == 0 || len(receipt) > maxTaskCreationReceiptBytes {
		return taskCreationValidation("native V3 ensure receipt is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin native V3 ensure checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadLeasedResearchTaskCreationOperationV3(ctx, tx, lease)
	if err != nil {
		return err
	}
	if op.TaskID != taskID {
		return taskCreationConflict("native V3 ensure task differs")
	}
	if op.Phase != types.TaskCreationPhaseSchedulePrepared {
		if taskCreationPhaseRank(op.Phase) >= taskCreationPhaseRank(types.TaskCreationPhaseScheduleEnsured) &&
			bytes.Equal(op.EnsureReceipt, receipt) {
			return tx.Commit(ctx)
		}
		return taskCreationConflict("native V3 ensure checkpoint differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_operations
		   SET ensure_receipt=$6,phase=$7,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$8
		   AND lease_owner=$4 AND fence=$5 AND lease_until>clock_timestamp()
		   AND status=$9 AND phase=$10`, lease.ID, lease.TenantID, lease.UserID,
		lease.LeaseOwner, lease.Fence, receipt, types.TaskCreationPhaseScheduleEnsured,
		types.TaskCreationExecutionVersionV2, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseSchedulePrepared)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			err = taskCreationLeaseLost()
		}
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CompleteResearchTaskCreationOperationV3(
	ctx context.Context, lease types.TaskCreationLease, taskID string, result json.RawMessage,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return err
	}
	if len(result) == 0 || len(result) > maxTaskCreationResultBytes || strictjson.Validate(result) != nil {
		return taskCreationValidation("native V3 result is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin native V3 completion", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadScopedResearchTaskCreationOperationV3ForUpdate(ctx, tx, lease)
	if err != nil {
		return err
	}
	if op.Status == types.TaskOperationStatusExecuted && op.Phase == types.TaskCreationPhaseCompleted &&
		op.TaskID == taskID && taskCreationJSONEqual(op.Result, result) {
		if err := verifyTaskCreationReceiptForTerminal(ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if op.Status != types.TaskOperationStatusExecuting || op.Phase != types.TaskCreationPhaseActivated ||
		op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence || op.LeaseUntil == nil {
		return taskCreationConflict("native V3 completion lease or phase differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_operations
		   SET status=$6,phase=$7,result=$8,executed_at=clock_timestamp(),
		       tombstoned_at=clock_timestamp(),lease_owner='',lease_until=NULL,
		       takeover_not_before=NULL,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$9
		   AND lease_owner=$4 AND fence=$5 AND lease_until>clock_timestamp()
		   AND status=$10 AND phase=$11`, lease.ID, lease.TenantID, lease.UserID,
		lease.LeaseOwner, lease.Fence, types.TaskOperationStatusExecuted,
		types.TaskCreationPhaseCompleted, result, types.TaskCreationExecutionVersionV2,
		types.TaskOperationStatusExecuting, types.TaskCreationPhaseActivated)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			err = taskCreationLeaseLost()
		}
		return err
	}
	if err := insertResearchTaskCreationReceiptV3(ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CleanupResearchTaskCreationOperationV3 is called only after DeleteTask has
// proved the exact Temporal schedule absent. The database aggregate and
// terminal receipt then converge atomically behind migration 109's definer.
func (s *Store) CleanupResearchTaskCreationOperationV3(
	ctx context.Context, lease types.TaskCreationLease, taskID, code, message string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return err
	}
	if strings.TrimSpace(code) == "" || code != strings.TrimSpace(code) || len(code) > 128 ||
		strings.TrimSpace(message) == "" || message != strings.TrimSpace(message) || len(message) > 4096 {
		return taskCreationValidation("native V3 cleanup reason is invalid")
	}
	tx, err := s.beginNativeResearchCreationV3Tx(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	if _, err := tx.Exec(ctx, `SELECT cleanup_native_research_task_creation_v3_v1(
		$1,$2,$3,$4,$5,$6,$7,$8,$9)`, lease.ID, lease.TenantID, lease.UserID,
		lease.LeaseOwner, lease.Fence, taskID, code, message,
		types.TaskCreationExecutionVersionV2); err != nil {
		return taskCreationDatabaseError("cleanup native V3 creation aggregate", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit native V3 creation cleanup", err)
	}
	return nil
}

// BlockResearchTaskCreationOperationV3 retains an active/ambiguous aggregate
// for operator reconciliation. matureSchedulePredicate keeps it invisible and
// all run/delivery admission gates reject the unfinished operation.
func (s *Store) BlockResearchTaskCreationOperationV3(
	ctx context.Context, lease types.TaskCreationLease, taskID, code, message string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return err
	}
	if strings.TrimSpace(code) == "" || strings.TrimSpace(message) == "" {
		return taskCreationValidation("native V3 block reason is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin native V3 quarantine", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadScopedResearchTaskCreationOperationV3ForUpdate(ctx, tx, lease)
	if err != nil {
		return err
	}
	if op.Status == types.TaskOperationStatusBlocked && op.Phase == types.TaskCreationPhaseBlocked &&
		op.TaskID == taskID && op.ErrorCode == code && op.ErrorMessage == message {
		if err := verifyTaskCreationReceiptForTerminal(ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if op.Status != types.TaskOperationStatusExecuting || op.TombstonedAt != nil ||
		op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence || op.TaskID != taskID {
		return taskCreationConflict("native V3 quarantine lease differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_operations
		   SET status=$6,phase=$7,error_code=$8,error_message=$9,
		       tombstoned_at=clock_timestamp(),lease_owner='',lease_until=NULL,
		       takeover_not_before=NULL,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$10
		   AND lease_owner=$4 AND fence=$5 AND status=$11 AND tombstoned_at IS NULL`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskOperationStatusBlocked, types.TaskCreationPhaseBlocked, code, message,
		types.TaskCreationExecutionVersionV2, types.TaskOperationStatusExecuting)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			err = taskCreationLeaseLost()
		}
		return err
	}
	if err := insertResearchTaskCreationReceiptV3(ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListStaleResearchTaskCreationTenantIDsV3(
	ctx context.Context, before time.Time, afterTenantID int64, limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 || limit > 1000 {
		return nil, taskCreationValidation("invalid stale native V3 tenant query")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT tenant_id FROM task_creation_operations
		 WHERE tool_name='manage_tasks' AND execution_version=$1 AND status=$2
		   AND tenant_id>$3 AND tombstoned_at IS NULL
		   AND lease_owner<>'' AND fence>0 AND attempt>0
		   AND lease_until<=clock_timestamp()
		   AND takeover_not_before<=LEAST($4,clock_timestamp())
		 ORDER BY tenant_id LIMIT $5`, types.TaskCreationExecutionVersionV2,
		types.TaskOperationStatusExecuting, afterTenantID, before, limit)
	if err != nil {
		return nil, taskCreationDatabaseError("list stale native V3 tenants", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) ListStaleResearchTaskCreationOperationsV3(
	ctx context.Context, tenantID int64, before time.Time, limit int,
) ([]types.TaskCreationOperation, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, taskCreationValidation("invalid stale native V3 operation query")
	}
	rows, err := s.pool.Query(ctx, `SELECT `+taskCreationOperationColumns+`
		 FROM task_creation_operations
		 WHERE tenant_id=$1 AND tool_name='manage_tasks' AND execution_version=$2
		   AND status=$3 AND tombstoned_at IS NULL AND lease_owner<>''
		   AND fence>0 AND attempt>0 AND lease_until<=clock_timestamp()
		   AND takeover_not_before<=LEAST($4,clock_timestamp())
		 ORDER BY takeover_not_before,id LIMIT $5`, tenantID,
		types.TaskCreationExecutionVersionV2, types.TaskOperationStatusExecuting,
		before, limit)
	if err != nil {
		return nil, taskCreationDatabaseError("list stale native V3 operations", err)
	}
	defer rows.Close()
	var out []types.TaskCreationOperation
	for rows.Next() {
		var op types.TaskCreationOperation
		if err := scanTaskCreationOperation(rows, &op); err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func loadScopedResearchTaskCreationOperationV3ForUpdate(
	ctx context.Context, tx pgx.Tx, lease types.TaskCreationLease,
) (*types.TaskCreationOperation, time.Time, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx, `SELECT `+taskCreationOperationColumns+`
		 FROM task_creation_operations
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=$4 FOR UPDATE`,
		lease.ID, lease.TenantID, lease.UserID, types.TaskCreationExecutionVersionV2), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, taskCreationNotFound()
	}
	if err != nil {
		return nil, time.Time{}, taskCreationDatabaseError("load scoped native V3 operation", err)
	}
	now, err := taskCreationDatabaseClock(ctx, tx)
	return &op, now, err
}

func loadLeasedResearchTaskCreationOperationV3(
	ctx context.Context, tx pgx.Tx, lease types.TaskCreationLease,
) (*types.TaskCreationOperation, time.Time, error) {
	op, now, err := loadScopedResearchTaskCreationOperationV3ForUpdate(ctx, tx, lease)
	if err != nil {
		return nil, time.Time{}, err
	}
	if op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence ||
		op.Status != types.TaskOperationStatusExecuting || op.TombstonedAt != nil ||
		op.LeaseUntil == nil || !now.Before(*op.LeaseUntil) {
		return nil, time.Time{}, taskCreationLeaseLost()
	}
	return op, now, nil
}

func insertResearchTaskCreationReceiptV3(
	ctx context.Context, tx pgx.Tx, operationID string, tenantID, userID int64,
) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO task_creation_receipts (
		 operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
		 status,next_attempt_at,failure_class,blocked_at)
		SELECT p.id,p.tenant_id,p.user_id,p.session_id,p.receipt_provider,p.receipt_target,
		       md5('vane/task-creation-receipt/v1:'||p.id)::uuid,
		       CASE WHEN p.receipt_provider='' OR p.receipt_target='' THEN $4 ELSE $5 END,
		       clock_timestamp()+interval '4 seconds',
		       CASE WHEN p.receipt_provider='' OR p.receipt_target='' THEN $6 ELSE '' END,
		       CASE WHEN p.receipt_provider='' OR p.receipt_target='' THEN clock_timestamp() ELSE NULL END
		  FROM task_creation_operations p
		 WHERE p.id=$1 AND p.tenant_id=$2 AND p.user_id=$3
		   AND p.tool_name='manage_tasks' AND p.execution_version=2
		   AND p.tombstoned_at IS NOT NULL
		   AND p.status IN ('executed','cancelled','expired','blocked','failed')
		ON CONFLICT (operation_id) DO NOTHING`, operationID, tenantID, userID,
		types.TaskCreationReceiptStatusBlocked, types.TaskCreationReceiptStatusPending,
		types.TaskCreationReceiptFailureTargetUnbound)
	if err != nil {
		return taskCreationDatabaseError("insert native V3 terminal receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationConflict("native V3 terminal receipt already exists")
	}
	return nil
}
