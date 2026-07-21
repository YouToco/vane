package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const taskCreationOperationColumns = `
	id, tenant_id, user_id, session_id, tool_name, args, summary, status,
	expires_at, executed_at, created_at, execution_version, phase,
	lease_owner, lease_until, takeover_not_before, fence, attempt, normalized_command,
	compiled_definition, compiled_digest, prepared_schedule, ensure_receipt,
	task_id, result, error_code, error_message, updated_at, tombstoned_at`

const (
	maxTaskCreationLease            = 24 * time.Hour
	taskCreationTakeoverSafetyGrace = 30 * time.Second
	maxTaskCreationErrorCode        = 64
	maxTaskCreationErrorText        = 512
	taskCreationRollbackLimit       = 2 * time.Second
)

func scanTaskCreationOperation(row pgx.Row, op *types.TaskCreationOperation) error {
	return row.Scan(
		&op.ID, &op.TenantID, &op.UserID, &op.SessionID, &op.ToolName,
		&op.Args, &op.Summary, &op.Status, &op.ExpiresAt, &op.ExecutedAt,
		&op.CreatedAt, &op.ExecutionVersion, &op.Phase, &op.LeaseOwner,
		&op.LeaseUntil, &op.TakeoverNotBefore, &op.Fence, &op.Attempt, &op.NormalizedCommand,
		&op.CompiledDefinition, &op.CompiledDigest, &op.PreparedSchedule,
		&op.EnsureReceipt, &op.TaskID, &op.Result, &op.ErrorCode,
		&op.ErrorMessage, &op.UpdatedAt, &op.TombstonedAt,
	)
}

// AcquireTaskCreationOperation atomically claims a v1 create_schedule action.
// The caller creates LeaseOwner before entering this method. Replaying the same
// owner while its lease is active returns the original fence without extending
// it or incrementing Attempt; this closes the committed-response-lost window.
// Another owner may take over only after the persisted TakeoverNotBefore
// boundary. The fixed safety grace is written with each lease and cannot be
// shortened by a competing caller; A5 must keep every external RPC deadline
// below this grace (including clock/network skew).
func (s *Store) AcquireTaskCreationOperation(
	ctx context.Context,
	p types.AcquireTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if err := validateAcquireTaskCreationParams(p); err != nil {
		return nil, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin acquisition", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	var op types.TaskCreationOperation
	err = scanTaskCreationOperation(tx.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM pending_actions
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		  FOR UPDATE`,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV1,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskCreationNotFound()
		}
		return nil, taskCreationDatabaseError("load acquisition target", err)
	}
	databaseNow, err := taskCreationDatabaseClock(ctx, tx)
	if err != nil {
		return nil, err
	}

	if op.TombstonedAt != nil || taskCreationStatusIsTerminal(op.Status) {
		return nil, taskCreationTerminal()
	}

	switch op.Status {
	case types.PendingActionStatusPending:
		if !databaseNow.Before(op.ExpiresAt) {
			return nil, taskCreationTerminal()
		}
		if op.Phase != "" || op.LeaseOwner != "" || op.LeaseUntil != nil ||
			op.TakeoverNotBefore != nil {
			return nil, taskCreationConflict("pending operation has saga state")
		}
		return s.acquirePendingTaskCreation(ctx, tx, p, op.Fence)

	case types.PendingActionStatusExecuting:
		if op.LeaseUntil == nil || op.TakeoverNotBefore == nil || op.LeaseOwner == "" ||
			op.Fence <= 0 || op.Attempt <= 0 {
			return nil, taskCreationConflict("executing operation has incomplete lease metadata")
		}
		if databaseNow.Before(*op.LeaseUntil) {
			if op.LeaseOwner == p.LeaseOwner {
				return &op, nil
			}
			return nil, taskCreationBusy()
		}

		// A just-expired worker gets a bounded grace period for an in-flight
		// external RPC to return. This cannot fence a request already sent to
		// Temporal/LLM, so takeover before the grace would create duplicates.
		if databaseNow.Before(*op.TakeoverNotBefore) {
			return nil, taskCreationBusy()
		}
		return s.takeOverTaskCreation(ctx, tx, p, op.Fence)

	default:
		return nil, taskCreationTerminal()
	}
}

func (s *Store) acquirePendingTaskCreation(
	ctx context.Context,
	tx pgx.Tx,
	p types.AcquireTaskCreationOperationParams,
	oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`UPDATE pending_actions
		    SET status = $6, phase = $7, lease_owner = $5,
		        lease_until = clock_timestamp() + ($8 * interval '1 microsecond'),
		        takeover_not_before = clock_timestamp() + ($9 * interval '1 microsecond'),
		        fence = fence + 1, attempt = attempt + 1, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		    AND status = $10 AND expires_at > clock_timestamp() AND tombstoned_at IS NULL
		    AND fence = $11
		  RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV1,
		p.LeaseOwner, types.PendingActionStatusExecuting,
		types.TaskCreationPhaseClaimed, p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskCreationTakeoverSafetyGrace).Microseconds(),
		types.PendingActionStatusPending, oldFence,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskCreationConflict("operation changed during acquisition")
		}
		return nil, taskCreationDatabaseError("acquire pending operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit acquisition", err)
	}
	return &op, nil
}

func (s *Store) takeOverTaskCreation(
	ctx context.Context,
	tx pgx.Tx,
	p types.AcquireTaskCreationOperationParams,
	oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`UPDATE pending_actions
		    SET lease_owner = $5,
		        lease_until = clock_timestamp() + ($6 * interval '1 microsecond'),
		        takeover_not_before = clock_timestamp() + ($7 * interval '1 microsecond'),
		        fence = fence + 1, attempt = attempt + 1, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		    AND status = $8 AND tombstoned_at IS NULL AND fence = $9
		    AND takeover_not_before IS NOT NULL
		    AND takeover_not_before <= clock_timestamp()
		  RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV1,
		p.LeaseOwner, p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskCreationTakeoverSafetyGrace).Microseconds(),
		types.PendingActionStatusExecuting, oldFence,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskCreationBusy()
		}
		return nil, taskCreationDatabaseError("take over operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit takeover", err)
	}
	return &op, nil
}

// LoadTaskCreationOperation reads one v1 create_schedule operation under its
// tenant/user scope. Terminal rows remain loadable as permanent audit
// tombstones, but are never returned by Acquire or the stale scanner.
func (s *Store) LoadTaskCreationOperation(
	ctx context.Context,
	id string,
	tenantID int64,
	userID int64,
) (*types.TaskCreationOperation, error) {
	if id == "" || tenantID <= 0 || userID <= 0 {
		return nil, taskCreationValidation("invalid operation scope")
	}
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(s.pool.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM pending_actions
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4`,
		id, tenantID, userID, types.TaskCreationExecutionVersionV1,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskCreationNotFound()
		}
		return nil, taskCreationDatabaseError("load operation", err)
	}
	return &op, nil
}

// RenewTaskCreationLease extends only a still-active lease with the exact
// tenant/user/owner/fence identity. An expired lease cannot be resurrected;
// callers must reacquire it (and receive a new fence) after takeover grace.
func (s *Store) RenewTaskCreationLease(
	ctx context.Context,
	lease types.TaskCreationLease,
	duration time.Duration,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateTaskCreationDuration(duration, maxTaskCreationLease, "lease duration"); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin lease renewal", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	if _, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions
		    SET lease_until = clock_timestamp() + ($6 * interval '1 microsecond'),
		        takeover_not_before = clock_timestamp() + ($7 * interval '1 microsecond'),
		        updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $8
		    AND status = $9 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		duration.Microseconds(),
		(duration + taskCreationTakeoverSafetyGrace).Microseconds(),
		types.TaskCreationExecutionVersionV1,
		types.PendingActionStatusExecuting,
	)
	if err != nil {
		return taskCreationDatabaseError("renew operation lease", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit lease renewal", err)
	}
	return nil
}

// SealTaskCreationCommand freezes the normalized user command before any paid
// translation. Same-byte replay is success; different bytes can never replace
// the command that the user approved.
func (s *Store) SealTaskCreationCommand(
	ctx context.Context,
	lease types.TaskCreationLease,
	command []byte,
) error {
	if len(command) == 0 {
		return taskCreationValidation("normalized command is empty")
	}
	return s.checkpointTaskCreationBytes(ctx, lease, command, taskCreationCheckpoint{
		from: types.TaskCreationPhaseClaimed,
		to:   types.TaskCreationPhaseCommandSealed,
		get:  func(op *types.TaskCreationOperation) []byte { return op.NormalizedCommand },
		update: `UPDATE pending_actions
		             SET normalized_command = $6, phase = $7, updated_at = clock_timestamp()
		           WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		             AND tool_name = 'create_schedule' AND execution_version = $8
		             AND status = $9 AND tombstoned_at IS NULL
		             AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		             AND phase = $10 AND normalized_command IS NULL`,
	})
}

// BeginTaskCreationTranslation is the paid-call intent checkpoint. Exactly the
// caller that observes started=true may invoke the compiler. A replay after a
// lost response returns started=false, forcing fail-closed recovery instead of
// charging for a second translation whose first outcome is ambiguous.
func (s *Store) BeginTaskCreationTranslation(
	ctx context.Context,
	lease types.TaskCreationLease,
) (bool, error) {
	if err := validateTaskCreationLease(lease); err != nil {
		return false, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, taskCreationDatabaseError("begin translation checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return false, err
	}
	if len(op.NormalizedCommand) == 0 {
		return false, taskCreationConflict("translation has no sealed command")
	}
	if taskCreationPhaseAtLeast(op.Phase, types.TaskCreationPhaseTranslationStarted) {
		return false, nil
	}
	if op.Phase != types.TaskCreationPhaseCommandSealed {
		return false, taskCreationConflict("translation phase is invalid")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions
		    SET phase = $6, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $7
		    AND status = $8 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $9 AND normalized_command IS NOT NULL`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskCreationPhaseTranslationStarted,
		types.TaskCreationExecutionVersionV1, types.PendingActionStatusExecuting,
		types.TaskCreationPhaseCommandSealed,
	)
	if err != nil {
		return false, taskCreationDatabaseError("write translation checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return false, taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		// A commit error is intentionally never reported as started=true: the
		// database may have committed, so the only safe choice is no paid call.
		return false, taskCreationDatabaseError("commit translation checkpoint", err)
	}
	return true, nil
}

// CheckpointTaskCreationDefinition freezes the compiled A2 definition and its
// SHA-256 digest after the one authorized translation call.
func (s *Store) CheckpointTaskCreationDefinition(
	ctx context.Context,
	lease types.TaskCreationLease,
	definition []byte,
	digest string,
) error {
	if len(definition) == 0 {
		return taskCreationValidation("compiled definition is empty")
	}
	if !validSHA256Digest(digest) {
		return taskCreationValidation("compiled digest is invalid")
	}
	sum := sha256.Sum256(definition)
	if hex.EncodeToString(sum[:]) != digest {
		return taskCreationValidation("compiled digest does not match definition")
	}
	return s.checkpointTaskCreationDefinition(ctx, lease, definition, digest)
}

// CheckpointTaskCreationSchedule freezes A3's immutable Prepared schedule
// envelope. It is still side-effect free: EnsurePausedTaskSchedule belongs to
// A5 and records its receipt in the next checkpoint.
func (s *Store) CheckpointTaskCreationSchedule(
	ctx context.Context,
	lease types.TaskCreationLease,
	prepared []byte,
) error {
	if len(prepared) == 0 {
		return taskCreationValidation("prepared schedule is empty")
	}
	return s.checkpointTaskCreationBytes(ctx, lease, prepared, taskCreationCheckpoint{
		from: types.TaskCreationPhaseDefinitionCompiled,
		to:   types.TaskCreationPhaseSchedulePrepared,
		get:  func(op *types.TaskCreationOperation) []byte { return op.PreparedSchedule },
		update: `UPDATE pending_actions
		             SET prepared_schedule = $6, phase = $7, updated_at = clock_timestamp()
		           WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		             AND tool_name = 'create_schedule' AND execution_version = $8
		             AND status = $9 AND tombstoned_at IS NULL
		             AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		             AND phase = $10 AND prepared_schedule IS NULL`,
	})
}

// CheckpointTaskCreationEnsureReceipt freezes A5's proof that the deterministic
// paused Temporal schedule exists. It is provided in A4 for the saga substrate
// but has no production caller until A5.
func (s *Store) CheckpointTaskCreationEnsureReceipt(
	ctx context.Context,
	lease types.TaskCreationLease,
	receipt []byte,
	taskID string,
) error {
	if len(receipt) == 0 || strings.TrimSpace(taskID) == "" || taskID != strings.TrimSpace(taskID) {
		return taskCreationValidation("ensure receipt is incomplete")
	}
	if len(taskID) > 255 {
		return taskCreationValidation("task id is too long")
	}
	return s.checkpointTaskCreationEnsureReceipt(ctx, lease, receipt, taskID)
}

// BlockTaskCreationOperation is the fail-closed terminal for an ambiguous paid
// translation. Failed and completed operations use the same fenced tombstone
// transition; none can ever be acquired or scanned again.
func (s *Store) BlockTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	errorCode string,
	errorMessage string,
) error {
	return s.terminateTaskCreationOperation(ctx, lease, taskCreationTermination{
		status:       types.PendingActionStatusBlocked,
		phase:        types.TaskCreationPhaseBlocked,
		errorCode:    errorCode,
		errorMessage: errorMessage,
	})
}

// FailTaskCreationOperation permanently records a deterministic, non-ambiguous
// failure. Retryable failures should release through lease expiry instead.
func (s *Store) FailTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	errorCode string,
	errorMessage string,
) error {
	return s.terminateTaskCreationOperation(ctx, lease, taskCreationTermination{
		status:       types.PendingActionStatusFailed,
		phase:        types.TaskCreationPhaseFailed,
		errorCode:    errorCode,
		errorMessage: errorMessage,
	})
}

// CompleteTaskCreationOperation creates the permanent success tombstone.
func (s *Store) CompleteTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	result json.RawMessage,
) error {
	if strings.TrimSpace(taskID) == "" || taskID != strings.TrimSpace(taskID) || len(taskID) > 255 {
		return taskCreationValidation("task id is invalid")
	}
	if len(result) == 0 || !json.Valid(result) {
		return taskCreationValidation("operation result is invalid")
	}
	return s.terminateTaskCreationOperation(ctx, lease, taskCreationTermination{
		status: types.PendingActionStatusExecuted,
		phase:  types.TaskCreationPhaseCompleted,
		taskID: taskID,
		result: result,
	})
}

// ListStaleTaskCreationOperations returns only recoverable v1 operations for a
// single tenant. Terminal/tombstoned rows and all historical v0 actions are
// excluded both by the query and the migration's partial index.
func (s *Store) ListStaleTaskCreationOperations(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]types.TaskCreationOperation, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, taskCreationValidation("invalid stale operation query")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM pending_actions
		  WHERE tenant_id = $1 AND tool_name = 'create_schedule'
		    AND execution_version = $2 AND status = $3
		    AND tombstoned_at IS NULL AND takeover_not_before IS NOT NULL
		    AND takeover_not_before <= LEAST($4, clock_timestamp())
		  ORDER BY takeover_not_before, id
		  LIMIT $5`,
		tenantID, types.TaskCreationExecutionVersionV1,
		types.PendingActionStatusExecuting, before, limit,
	)
	if err != nil {
		return nil, taskCreationDatabaseError("list stale operations", err)
	}
	defer rows.Close()

	operations := make([]types.TaskCreationOperation, 0)
	for rows.Next() {
		var op types.TaskCreationOperation
		if err := scanTaskCreationOperation(rows, &op); err != nil {
			return nil, taskCreationDatabaseError("scan stale operation", err)
		}
		operations = append(operations, op)
	}
	if err := rows.Err(); err != nil {
		return nil, taskCreationDatabaseError("iterate stale operations", err)
	}
	return operations, nil
}

type taskCreationCheckpoint struct {
	from   types.TaskCreationPhase
	to     types.TaskCreationPhase
	get    func(*types.TaskCreationOperation) []byte
	update string
}

func (s *Store) checkpointTaskCreationBytes(
	ctx context.Context,
	lease types.TaskCreationLease,
	payload []byte,
	cp taskCreationCheckpoint,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin immutable checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	existing := cp.get(op)
	if len(existing) != 0 {
		if bytes.Equal(existing, payload) && taskCreationPhaseAtLeast(op.Phase, cp.to) {
			return nil
		}
		return taskCreationConflict("immutable checkpoint differs")
	}
	if op.Phase != cp.from {
		return taskCreationConflict("checkpoint phase is invalid")
	}

	tag, err := tx.Exec(ctx, cp.update,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		payload, cp.to, types.TaskCreationExecutionVersionV1,
		types.PendingActionStatusExecuting, cp.from,
	)
	if err != nil {
		return taskCreationDatabaseError("write immutable checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit immutable checkpoint", err)
	}
	return nil
}

func (s *Store) checkpointTaskCreationDefinition(
	ctx context.Context,
	lease types.TaskCreationLease,
	definition []byte,
	digest string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin definition checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	if len(op.CompiledDefinition) != 0 || op.CompiledDigest != "" {
		if bytes.Equal(op.CompiledDefinition, definition) && op.CompiledDigest == digest &&
			taskCreationPhaseAtLeast(op.Phase, types.TaskCreationPhaseDefinitionCompiled) {
			return nil
		}
		return taskCreationConflict("immutable definition differs")
	}
	if op.Phase != types.TaskCreationPhaseTranslationStarted {
		return taskCreationConflict("definition checkpoint phase is invalid")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions
		    SET compiled_definition = $6, compiled_digest = $7,
		        phase = $8, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $9
		    AND status = $10 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $11 AND compiled_definition IS NULL AND compiled_digest = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		definition, digest, types.TaskCreationPhaseDefinitionCompiled,
		types.TaskCreationExecutionVersionV1, types.PendingActionStatusExecuting,
		types.TaskCreationPhaseTranslationStarted,
	)
	if err != nil {
		return taskCreationDatabaseError("write definition checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit definition checkpoint", err)
	}
	return nil
}

func (s *Store) checkpointTaskCreationEnsureReceipt(
	ctx context.Context,
	lease types.TaskCreationLease,
	receipt []byte,
	taskID string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin ensure checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	if len(op.EnsureReceipt) != 0 || op.TaskID != "" {
		if bytes.Equal(op.EnsureReceipt, receipt) && op.TaskID == taskID &&
			taskCreationPhaseAtLeast(op.Phase, types.TaskCreationPhaseScheduleEnsured) {
			return nil
		}
		return taskCreationConflict("immutable ensure receipt differs")
	}
	if op.Phase != types.TaskCreationPhaseSchedulePrepared {
		return taskCreationConflict("ensure checkpoint phase is invalid")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions
		    SET ensure_receipt = $6, task_id = $7, phase = $8, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $9
		    AND status = $10 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $11 AND ensure_receipt IS NULL AND task_id = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		receipt, taskID, types.TaskCreationPhaseScheduleEnsured,
		types.TaskCreationExecutionVersionV1, types.PendingActionStatusExecuting,
		types.TaskCreationPhaseSchedulePrepared,
	)
	if err != nil {
		return taskCreationDatabaseError("write ensure checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit ensure checkpoint", err)
	}
	return nil
}

type taskCreationTermination struct {
	status       types.PendingActionStatus
	phase        types.TaskCreationPhase
	errorCode    string
	errorMessage string
	taskID       string
	result       json.RawMessage
}

func (s *Store) terminateTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	termination taskCreationTermination,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if len(termination.errorCode) > maxTaskCreationErrorCode ||
		len(termination.errorMessage) > maxTaskCreationErrorText {
		return taskCreationValidation("terminal error metadata is too long")
	}
	if termination.status != types.PendingActionStatusExecuted && termination.errorCode == "" {
		return taskCreationValidation("terminal error code is empty")
	}
	if termination.status != types.PendingActionStatusExecuted &&
		(termination.errorCode != strings.TrimSpace(termination.errorCode) ||
			termination.errorMessage != strings.TrimSpace(termination.errorMessage)) {
		return taskCreationValidation("terminal error metadata is not normalized")
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin terminal checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, databaseNow, err := loadScopedTaskCreationOperationForUpdate(ctx, tx, lease)
	if err != nil {
		return err
	}
	if op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence {
		return taskCreationLeaseLost()
	}
	if op.TombstonedAt != nil || taskCreationStatusIsTerminal(op.Status) {
		if op.TombstonedAt == nil || op.LeaseUntil != nil || op.TakeoverNotBefore != nil {
			return taskCreationConflict("terminal tombstone is incomplete")
		}
		return adoptTaskCreationTermination(op, termination)
	}
	if op.Status != types.PendingActionStatusExecuting || op.LeaseUntil == nil ||
		op.TakeoverNotBefore == nil ||
		!databaseNow.Before(*op.LeaseUntil) {
		return taskCreationLeaseLost()
	}
	if err := validateTaskCreationTerminationPhase(op.Phase, termination.status); err != nil {
		return err
	}
	if termination.status == types.PendingActionStatusExecuted &&
		op.TaskID != "" && op.TaskID != termination.taskID {
		return taskCreationConflict("completed task id differs from ensure checkpoint")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions
		    SET status = $6, phase = $7,
		        task_id = CASE WHEN $6 = 'executed' THEN $8 ELSE task_id END,
		        result = CASE WHEN $6 = 'executed' THEN $9 ELSE result END,
		        error_code = CASE WHEN $6 = 'executed' THEN '' ELSE $10 END,
		        error_message = CASE WHEN $6 = 'executed' THEN '' ELSE $11 END,
		        executed_at = CASE WHEN $6 = 'executed' THEN clock_timestamp() ELSE executed_at END,
		        lease_until = NULL, takeover_not_before = NULL,
		        tombstoned_at = clock_timestamp(), updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $12
		    AND status = $13 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND ($6 <> 'executed' OR task_id = '' OR task_id = $8)`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		termination.status, termination.phase, termination.taskID,
		termination.result, termination.errorCode, termination.errorMessage,
		types.TaskCreationExecutionVersionV1, types.PendingActionStatusExecuting,
	)
	if err != nil {
		return taskCreationDatabaseError("write terminal tombstone", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit terminal checkpoint", err)
	}
	return nil
}

// adoptTaskCreationTermination closes the commit-response-lost window for the
// final durable result. The first transition uses the fully fenced executing
// UPDATE above. A retry holds the same row lock and exact-adopts only the same
// lease and same terminal payload.
func adoptTaskCreationTermination(
	op *types.TaskCreationOperation,
	termination taskCreationTermination,
) error {
	if op.Status != termination.status || op.Phase != termination.phase {
		return taskCreationTerminal()
	}
	switch termination.status {
	case types.PendingActionStatusExecuted:
		if op.ExecutedAt != nil && op.ErrorCode == "" && op.ErrorMessage == "" &&
			op.TaskID == termination.taskID && taskCreationJSONEqual(op.Result, termination.result) {
			return nil
		}
	case types.PendingActionStatusBlocked, types.PendingActionStatusFailed:
		if op.ExecutedAt == nil && len(op.Result) == 0 &&
			op.ErrorCode == termination.errorCode && op.ErrorMessage == termination.errorMessage {
			return nil
		}
	}
	return taskCreationConflict("terminal checkpoint differs")
}

func loadLeasedTaskCreationOperation(
	ctx context.Context,
	tx pgx.Tx,
	lease types.TaskCreationLease,
) (*types.TaskCreationOperation, time.Time, error) {
	op, databaseNow, err := loadScopedTaskCreationOperationForUpdate(ctx, tx, lease)
	if err != nil {
		return nil, time.Time{}, err
	}
	if op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence {
		return nil, time.Time{}, taskCreationLeaseLost()
	}
	if op.TombstonedAt != nil || taskCreationStatusIsTerminal(op.Status) {
		return nil, time.Time{}, taskCreationTerminal()
	}
	if op.Status != types.PendingActionStatusExecuting || op.LeaseUntil == nil ||
		op.TakeoverNotBefore == nil ||
		!databaseNow.Before(*op.LeaseUntil) {
		return nil, time.Time{}, taskCreationLeaseLost()
	}
	return op, databaseNow, nil
}

func loadScopedTaskCreationOperationForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	lease types.TaskCreationLease,
) (*types.TaskCreationOperation, time.Time, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM pending_actions
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		  FOR UPDATE`,
		lease.ID, lease.TenantID, lease.UserID, types.TaskCreationExecutionVersionV1,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, time.Time{}, taskCreationNotFound()
		}
		return nil, time.Time{}, taskCreationDatabaseError("load leased operation", err)
	}
	databaseNow, err := taskCreationDatabaseClock(ctx, tx)
	if err != nil {
		return nil, time.Time{}, err
	}
	return &op, databaseNow, nil
}

func validateAcquireTaskCreationParams(p types.AcquireTaskCreationOperationParams) error {
	if p.ID == "" || p.TenantID <= 0 || p.UserID <= 0 ||
		strings.TrimSpace(p.LeaseOwner) == "" || p.LeaseOwner != strings.TrimSpace(p.LeaseOwner) {
		return taskCreationValidation("invalid acquisition scope")
	}
	if len(p.LeaseOwner) > 255 {
		return taskCreationValidation("lease owner is too long")
	}
	if err := validateTaskCreationDuration(p.LeaseDuration, maxTaskCreationLease, "lease duration"); err != nil {
		return err
	}
	return nil
}

func validateTaskCreationLease(lease types.TaskCreationLease) error {
	if lease.ID == "" || lease.TenantID <= 0 || lease.UserID <= 0 ||
		strings.TrimSpace(lease.LeaseOwner) == "" ||
		lease.LeaseOwner != strings.TrimSpace(lease.LeaseOwner) || lease.Fence <= 0 {
		return taskCreationValidation("invalid operation lease")
	}
	return nil
}

func validateTaskCreationDuration(duration, maximum time.Duration, label string) error {
	if duration <= 0 || duration > maximum || duration.Microseconds() <= 0 {
		return taskCreationValidation(label + " is invalid")
	}
	return nil
}

func validateTaskCreationTerminationPhase(
	phase types.TaskCreationPhase,
	status types.PendingActionStatus,
) error {
	switch status {
	case types.PendingActionStatusExecuted:
		if phase != types.TaskCreationPhaseActivated {
			return taskCreationConflict("operation is not activated")
		}
	case types.PendingActionStatusBlocked, types.PendingActionStatusFailed:
		rank := taskCreationPhaseRank(phase)
		if rank < taskCreationPhaseRank(types.TaskCreationPhaseClaimed) ||
			rank > taskCreationPhaseRank(types.TaskCreationPhaseSchedulePrepared) {
			return taskCreationConflict("operation requires side-effect cleanup")
		}
	default:
		return taskCreationConflict("terminal status is invalid")
	}
	return nil
}

func taskCreationJSONEqual(left, right json.RawMessage) bool {
	leftValue, err := decodeTaskCreationJSON(left)
	if err != nil {
		return false
	}
	rightValue, err := decodeTaskCreationJSON(right)
	return err == nil && equalTaskCreationJSON(leftValue, rightValue)
}

func decodeTaskCreationJSON(payload json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func equalTaskCreationJSON(left, right any) bool {
	switch leftValue := left.(type) {
	case nil:
		return right == nil
	case bool:
		rightValue, ok := right.(bool)
		return ok && leftValue == rightValue
	case string:
		rightValue, ok := right.(string)
		return ok && leftValue == rightValue
	case json.Number:
		rightValue, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftNumber, leftOK := new(big.Rat).SetString(leftValue.String())
		rightNumber, rightOK := new(big.Rat).SetString(rightValue.String())
		return leftOK && rightOK && leftNumber.Cmp(rightNumber) == 0
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for i := range leftValue {
			if !equalTaskCreationJSON(leftValue[i], rightValue[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, leftItem := range leftValue {
			rightItem, exists := rightValue[key]
			if !exists || !equalTaskCreationJSON(leftItem, rightItem) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Rollback must not inherit a cancelled request context. A cancelled parent is
// a common reason a method exits early; using it again would make cleanup a
// no-op and leave the connection pinned in a transaction until pool teardown.
func rollbackTaskCreationTransaction(parent context.Context, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), taskCreationRollbackLimit)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}

// PostgreSQL evaluates a SELECT target list before a conflicting FOR UPDATE
// lock has necessarily been acquired. Even clock_timestamp() in that target
// list can therefore describe the instant before a long lock wait. Read the
// clock in a second statement after the row lock is ours.
func taskCreationDatabaseClock(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return time.Time{}, taskCreationDatabaseError("read database clock", err)
	}
	return databaseNow, nil
}

func validSHA256Digest(digest string) bool {
	if len(digest) != 64 || digest != strings.ToLower(digest) {
		return false
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32
}

func taskCreationStatusIsTerminal(status types.PendingActionStatus) bool {
	switch status {
	case types.PendingActionStatusExecuted,
		types.PendingActionStatusCancelled,
		types.PendingActionStatusExpired,
		types.PendingActionStatusBlocked,
		types.PendingActionStatusFailed:
		return true
	default:
		return false
	}
}

func taskCreationPhaseAtLeast(current, target types.TaskCreationPhase) bool {
	return taskCreationPhaseRank(current) >= taskCreationPhaseRank(target)
}

func taskCreationPhaseRank(phase types.TaskCreationPhase) int {
	switch phase {
	case types.TaskCreationPhaseClaimed:
		return 1
	case types.TaskCreationPhaseCommandSealed:
		return 2
	case types.TaskCreationPhaseTranslationStarted:
		return 3
	case types.TaskCreationPhaseDefinitionCompiled:
		return 4
	case types.TaskCreationPhaseSchedulePrepared:
		return 5
	case types.TaskCreationPhaseScheduleEnsured:
		return 6
	case types.TaskCreationPhaseDefinitionCommitted:
		return 7
	case types.TaskCreationPhaseActivationStarted:
		return 8
	case types.TaskCreationPhaseActivated:
		return 9
	case types.TaskCreationPhaseCleanupPending:
		return 10
	case types.TaskCreationPhaseCompleted,
		types.TaskCreationPhaseBlocked,
		types.TaskCreationPhaseFailed:
		return 11
	default:
		return 0
	}
}

func taskCreationValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func taskCreationConflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func taskCreationDatabaseError(action string, err error) error {
	return types.NewAppError(types.CodeDatabase, "task creation: "+action, err)
}

func taskCreationNotFound() error {
	return fmt.Errorf("%w: task creation operation is unavailable", types.ErrNotFound)
}

func taskCreationBusy() error {
	return fmt.Errorf("%w: task creation operation has an active owner", types.ErrTaskCreationBusy)
}

func taskCreationTerminal() error {
	return fmt.Errorf("%w: task creation operation is terminal", types.ErrTaskCreationTerminal)
}

func taskCreationLeaseLost() error {
	return fmt.Errorf("%w: task creation lease is no longer valid", types.ErrTaskCreationLeaseLost)
}
