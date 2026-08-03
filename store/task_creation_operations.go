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
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const taskCreationOperationColumns = `
	id, tenant_id, user_id, session_id, tool_name, args, summary, status,
	expires_at, executed_at, created_at, execution_version, phase,
	lease_owner, lease_until, takeover_not_before, fence, attempt, normalized_command,
	compiled_definition, compiled_digest, prepared_schedule, ensure_receipt,
	task_id, result, error_code, error_message, receipt_provider, receipt_target,
	updated_at, tombstoned_at`

const (
	maxTaskCreationLease            = 24 * time.Hour
	taskCreationTakeoverSafetyGrace = 30 * time.Second
	maxTaskCreationErrorCode        = 64
	maxTaskCreationErrorText        = 512
	maxTaskCreationArgsBytes        = 64 << 10
	maxTaskCreationSummaryBytes     = 4 << 10
	maxTaskCreationCommandBytes     = 64 << 10
	maxTaskCreationDefinitionBytes  = 1 << 20
	maxTaskCreationPreparedBytes    = 256 << 10
	maxTaskCreationReceiptBytes     = 256 << 10
	maxTaskCreationResultBytes      = 64 << 10
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
		&op.ErrorMessage, &op.ReceiptProvider, &op.ReceiptTarget,
		&op.UpdatedAt, &op.TombstonedAt,
	)
}

// CreateTaskCreationOperation inserts a v1 create_schedule operation under
// an explicit active tenant membership. Legacy CreatePendingAction remains v0
// and is intentionally untouched. Store owns every protocol field so callers
// cannot smuggle an already-claimed phase, fence, or terminal status into the
// durable saga.
func (s *Store) CreateTaskCreationOperation(
	ctx context.Context,
	p types.CreateTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if err := validateCreateTaskCreationParams(p); err != nil {
		return nil, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin operation creation", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	existing, found, err := loadTaskCreationOperationForCreationReplay(
		ctx, tx, p.ID, p.TenantID, p.UserID,
	)
	if err != nil {
		return nil, err
	}
	if found {
		if err := validateTaskCreationOperationCreationScope(ctx, tx, p); err != nil {
			return nil, err
		}
		if !taskCreationCreationRequestEqual(existing, p) {
			return nil, taskCreationConflict("operation id already exists")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, taskCreationDatabaseError("commit operation creation replay", err)
		}
		return existing, nil
	}
	if s.legacyAdmissionIsClosed() {
		return nil, legacyAdmissionClosed("task creation v1")
	}

	if err := lockValidMembership(ctx, tx, p.TenantID, p.UserID); err != nil {
		return nil, err
	}
	if err := validateTaskCreationOperationSession(ctx, tx, p); err != nil {
		return nil, err
	}
	databaseNow, err := taskCreationDatabaseClock(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !databaseNow.Before(p.ExpiresAt) {
		return nil, taskCreationValidation("operation expiry is not in the future")
	}

	var op types.TaskCreationOperation
	err = scanTaskCreationOperation(tx.QueryRow(ctx,
		`INSERT INTO task_creation_operations
			(id, tenant_id, user_id, session_id, tool_name, args, summary, status,
			 expires_at, execution_version)
		 VALUES ($1, $2, $3, $4, 'create_schedule', $5, $6, $7, $8, $9)
		 ON CONFLICT (id) DO NOTHING
		 RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, p.SessionID, []byte(p.Args), p.Summary,
		types.TaskOperationStatusPending, p.ExpiresAt,
		types.TaskCreationExecutionVersionV1,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return nil, taskCreationDatabaseError(
					"release creation scope before replay", rollbackErr,
				)
			}
			return s.loadTaskCreationOperationCreationReplay(ctx, p)
		}
		return nil, taskCreationDatabaseError("insert operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit operation creation", err)
	}
	return &op, nil
}

func loadTaskCreationOperationForCreationReplay(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	tenantID int64,
	userID int64,
) (*types.TaskCreationOperation, bool, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM task_creation_operations
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		  FOR SHARE /* task creation replay operation lock order */`,
		id, tenantID, userID, types.TaskCreationExecutionVersionV1,
	), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, taskCreationDatabaseError(
			"load operation creation replay", err,
		)
	}
	return &op, true, nil
}

func validateTaskCreationOperationCreationScope(
	ctx context.Context,
	tx pgx.Tx,
	p types.CreateTaskCreationOperationParams,
) error {
	if err := lockValidMembership(ctx, tx, p.TenantID, p.UserID); err != nil {
		return err
	}
	if err := validateTaskCreationOperationSession(ctx, tx, p); err != nil {
		return err
	}
	databaseNow, err := taskCreationDatabaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if !databaseNow.Before(p.ExpiresAt) {
		return taskCreationValidation("operation expiry is not in the future")
	}
	return nil
}

func validateTaskCreationOperationSession(
	ctx context.Context,
	tx pgx.Tx,
	p types.CreateTaskCreationOperationParams,
) error {
	if p.SessionID == nil {
		return nil
	}
	var valid bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM agent_sessions
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		  FOR SHARE`,
		*p.SessionID, p.TenantID, p.UserID,
	).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return taskCreationValidation("agent session is outside operation scope")
	}
	if err != nil {
		return taskCreationDatabaseError("validate operation session", err)
	}
	return nil
}

func (s *Store) loadTaskCreationOperationCreationReplay(
	ctx context.Context,
	p types.CreateTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin operation creation replay", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, found, err := loadTaskCreationOperationForCreationReplay(
		ctx, tx, p.ID, p.TenantID, p.UserID,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, taskCreationConflict("operation id conflict disappeared")
	}
	if err := validateTaskCreationOperationCreationScope(ctx, tx, p); err != nil {
		return nil, err
	}
	if !taskCreationCreationRequestEqual(op, p) {
		return nil, taskCreationConflict("operation id already exists")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit operation creation replay", err)
	}
	return op, nil
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
		   FROM task_creation_operations
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
	if op.ReceiptProvider != "" || op.ReceiptTarget != "" {
		if op.ReceiptProvider != p.ReceiptProvider || op.ReceiptTarget != p.ReceiptTarget {
			return nil, taskCreationConflict("operation receipt target differs")
		}
	} else if op.Status == types.TaskOperationStatusPending &&
		(p.ReceiptProvider == "" || p.ReceiptTarget == "") {
		return nil, taskCreationValidation("pending operation requires a receipt target")
	}
	switch op.Status {
	case types.TaskOperationStatusPending:
		if !pendingTaskCreationOperationPristine(&op) {
			return nil, taskCreationConflict("pending operation has saga state")
		}
		if !databaseNow.Before(op.ExpiresAt) {
			return expirePendingTaskCreationOperation(ctx, tx, p, op.Fence)
		}
		return s.acquirePendingTaskCreation(ctx, tx, p, op.Fence)

	case types.TaskOperationStatusExecuting:
		if op.LeaseUntil == nil || op.TakeoverNotBefore == nil || op.LeaseOwner == "" ||
			op.Fence <= 0 || op.Attempt <= 0 {
			return nil, taskCreationConflict("executing operation has incomplete lease metadata")
		}
		receiptBoundNow := false
		if op.ReceiptProvider == "" && p.ReceiptProvider != "" {
			err = scanTaskCreationOperation(tx.QueryRow(ctx, `
				UPDATE task_creation_operations
				   SET receipt_provider = $5, receipt_target = $6,
				       updated_at = clock_timestamp()
				 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
				   AND tool_name = 'create_schedule' AND execution_version = $4
				   AND status = $7 AND tombstoned_at IS NULL AND fence = $8
				   AND receipt_provider = '' AND receipt_target = ''
				 RETURNING `+taskCreationOperationColumns,
				p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV1,
				p.ReceiptProvider, p.ReceiptTarget,
				types.TaskOperationStatusExecuting, op.Fence,
			), &op)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, taskCreationConflict("operation changed during receipt binding")
				}
				return nil, taskCreationDatabaseError("bind executing operation receipt target", err)
			}
			receiptBoundNow = true
		}
		commitReceiptBinding := func() error {
			if !receiptBoundNow {
				return nil
			}
			if err := tx.Commit(ctx); err != nil {
				return taskCreationDatabaseError("commit executing operation receipt target", err)
			}
			return nil
		}
		if databaseNow.Before(*op.LeaseUntil) {
			if op.LeaseOwner == p.LeaseOwner {
				if err := commitReceiptBinding(); err != nil {
					return nil, err
				}
				return &op, nil
			}
			if err := commitReceiptBinding(); err != nil {
				return nil, err
			}
			return &op, taskCreationBusy()
		}

		// A just-expired worker gets a bounded grace period for an in-flight
		// external RPC to return. This cannot fence a request already sent to
		// Temporal or another remote system, so an early takeover could duplicate it.
		if databaseNow.Before(*op.TakeoverNotBefore) {
			if err := commitReceiptBinding(); err != nil {
				return nil, err
			}
			return &op, taskCreationBusy()
		}
		return s.takeOverTaskCreation(ctx, tx, p, op.Fence)

	default:
		return nil, taskCreationTerminal()
	}
}

func expirePendingTaskCreationOperation(
	ctx context.Context,
	tx pgx.Tx,
	p types.AcquireTaskCreationOperationParams,
	oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`UPDATE task_creation_operations
		    SET status = $5, phase = $6,
		        receipt_provider = $9, receipt_target = $10,
		        lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
		        result = NULL, error_code = '', error_message = '', executed_at = NULL,
		        tombstoned_at = clock_timestamp(), updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		    AND status = $7 AND phase = '' AND tombstoned_at IS NULL
		    AND lease_owner = '' AND lease_until IS NULL AND takeover_not_before IS NULL
		    AND fence = $8 AND expires_at <= clock_timestamp()
		  RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV1,
		types.TaskOperationStatusExpired, types.TaskCreationPhaseExpired,
		types.TaskOperationStatusPending, oldFence,
		p.ReceiptProvider, p.ReceiptTarget,
	), &op)
	if err != nil {
		return nil, taskCreationDatabaseError("write expiry tombstone", err)
	}
	if err := insertTaskCreationReceiptForTerminal(
		ctx, tx, p.ID, p.TenantID, p.UserID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit expiry tombstone", err)
	}
	return nil, taskCreationTerminal()
}

func (s *Store) acquirePendingTaskCreation(
	ctx context.Context,
	tx pgx.Tx,
	p types.AcquireTaskCreationOperationParams,
	oldFence int64,
) (*types.TaskCreationOperation, error) {
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`UPDATE task_creation_operations
		    SET status = $6, phase = $7, lease_owner = $5,
		        receipt_provider = $12, receipt_target = $13,
		        lease_until = clock_timestamp() + ($8 * interval '1 microsecond'),
		        takeover_not_before = clock_timestamp() + ($9 * interval '1 microsecond'),
		        fence = fence + 1, attempt = attempt + 1, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		    AND status = $10 AND expires_at > clock_timestamp() AND tombstoned_at IS NULL
		    AND fence = $11
		    AND receipt_provider = '' AND receipt_target = ''
		  RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, types.TaskCreationExecutionVersionV1,
		p.LeaseOwner, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseClaimed, p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskCreationTakeoverSafetyGrace).Microseconds(),
		types.TaskOperationStatusPending, oldFence,
		p.ReceiptProvider, p.ReceiptTarget,
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
		`UPDATE task_creation_operations
		    SET lease_owner = $5,
		        receipt_provider = CASE WHEN receipt_provider = '' AND receipt_target = ''
		                                THEN $10 ELSE receipt_provider END,
		        receipt_target = CASE WHEN receipt_provider = '' AND receipt_target = ''
		                              THEN $11 ELSE receipt_target END,
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
		types.TaskOperationStatusExecuting, oldFence,
		p.ReceiptProvider, p.ReceiptTarget,
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
		   FROM task_creation_operations
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

// LoadTaskCreationOperationByUser resolves the durable tenant scope from the
// v1 operation itself. It deliberately does not route through the user's
// current/active memberships: a suspended or multi-membership user must still
// be able to resume or clean up the exact operation they started.
// Wrong-user, v0, wrong-tool and missing rows are indistinguishable.
func (s *Store) LoadTaskCreationOperationByUser(
	ctx context.Context,
	id string,
	userID int64,
) (*types.TaskCreationOperation, error) {
	if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) ||
		len(id) > 255 || userID <= 0 {
		return nil, taskCreationValidation("invalid operation lookup scope")
	}
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(s.pool.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM task_creation_operations
		  WHERE id = $1 AND user_id = $2
		    AND tool_name = 'create_schedule' AND execution_version = $3`,
		id, userID, types.TaskCreationExecutionVersionV1,
	), &op)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskCreationNotFound()
		}
		return nil, taskCreationDatabaseError("load operation by user", err)
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
		`UPDATE task_creation_operations
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
		types.TaskOperationStatusExecuting,
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

// SealTaskCreationCommand freezes the normalized approved command before the
// deterministic definition build. Same-byte replay is success; different bytes
// can never replace the command that the user approved.
func (s *Store) SealTaskCreationCommand(
	ctx context.Context,
	lease types.TaskCreationLease,
	command []byte,
) error {
	if len(command) == 0 || len(command) > maxTaskCreationCommandBytes {
		return taskCreationValidation("normalized command size is invalid")
	}
	return s.checkpointTaskCreationBytes(ctx, lease, command, taskCreationCheckpoint{
		from: types.TaskCreationPhaseClaimed,
		to:   types.TaskCreationPhaseCommandSealed,
		get:  func(op *types.TaskCreationOperation) []byte { return op.NormalizedCommand },
		update: `UPDATE task_creation_operations
		             SET normalized_command = $6, phase = $7, updated_at = clock_timestamp()
		           WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		             AND tool_name = 'create_schedule' AND execution_version = $8
		             AND status = $9 AND tombstoned_at IS NULL
		             AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		             AND phase = $10 AND normalized_command IS NULL`,
	})
}

// BeginTaskCreationTranslation keeps the historical phase/API name but now
// marks the start of a deterministic definition build from approved_fetch_plan.
// started=true means this call advanced the phase; started=false means recovery
// should load the existing compiled checkpoint or safely rebuild if it is not
// present. No paid model authorization is represented by this method.
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
		`UPDATE task_creation_operations
		    SET phase = $6, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $7
		    AND status = $8 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $9 AND normalized_command IS NOT NULL`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskCreationPhaseTranslationStarted,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseCommandSealed,
	)
	if err != nil {
		return false, taskCreationDatabaseError("write translation checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return false, taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		// A commit error is intentionally never reported as started=true because
		// the database may have committed. Recovery reloads the durable phase.
		return false, taskCreationDatabaseError("commit translation checkpoint", err)
	}
	return true, nil
}

// CheckpointTaskCreationDefinition freezes the deterministically compiled A2
// definition and its SHA-256 digest.
func (s *Store) CheckpointTaskCreationDefinition(
	ctx context.Context,
	lease types.TaskCreationLease,
	definition []byte,
	digest string,
) error {
	if len(definition) == 0 || len(definition) > maxTaskCreationDefinitionBytes {
		return taskCreationValidation("compiled definition size is invalid")
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
	if len(prepared) == 0 || len(prepared) > maxTaskCreationPreparedBytes {
		return taskCreationValidation("prepared schedule size is invalid")
	}
	return s.checkpointTaskCreationBytes(ctx, lease, prepared, taskCreationCheckpoint{
		from: types.TaskCreationPhaseDefinitionCompiled,
		to:   types.TaskCreationPhaseSchedulePrepared,
		get:  func(op *types.TaskCreationOperation) []byte { return op.PreparedSchedule },
		update: `UPDATE task_creation_operations
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
	if len(receipt) == 0 || len(receipt) > maxTaskCreationReceiptBytes ||
		strings.TrimSpace(taskID) == "" || taskID != strings.TrimSpace(taskID) {
		return taskCreationValidation("ensure receipt is incomplete")
	}
	if len(taskID) > 255 {
		return taskCreationValidation("task id is too long")
	}
	return s.checkpointTaskCreationEnsureReceipt(ctx, lease, receipt, taskID)
}

// BlockTaskCreationOperation is the fail-closed terminal for an operation whose
// outcome cannot be proven safe. Failed and completed operations use the same
// fenced tombstone transition; none can ever be acquired or scanned again.
func (s *Store) BlockTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	errorCode string,
	errorMessage string,
) error {
	return s.terminateTaskCreationOperation(ctx, lease, taskCreationTermination{
		status:       types.TaskOperationStatusBlocked,
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
		status:       types.TaskOperationStatusFailed,
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
	if len(result) == 0 || len(result) > maxTaskCreationResultBytes ||
		strictjson.Validate(result) != nil {
		return taskCreationValidation("operation result is invalid")
	}
	return s.terminateTaskCreationOperation(ctx, lease, taskCreationTermination{
		status: types.TaskOperationStatusExecuted,
		phase:  types.TaskCreationPhaseCompleted,
		taskID: taskID,
		result: result,
	})
}

// ListStaleTaskCreationTenantIDs enumerates tenant shards which contain at
// least one truly takeover-safe v1 operation. afterTenantID is an exclusive,
// stable keyset cursor; the coordinator wraps it to zero at the end. Ordering
// by tenant identity (rather than repeatedly taking the oldest top-N page)
// guarantees that a permanently failing early shard cannot starve later ones.
// The caller-supplied boundary is clamped to the database clock, so a
// skewed/future process clock cannot make a still-owned operation recoverable
// early.
func (s *Store) ListStaleTaskCreationTenantIDs(
	ctx context.Context,
	before time.Time,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 || limit > 1000 {
		return nil, taskCreationValidation("invalid stale tenant query")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT tenant_id
		   FROM task_creation_operations
		  WHERE tool_name = 'create_schedule'
		    AND execution_version = $1 AND status = $2
		    AND tenant_id > $4
		    AND tombstoned_at IS NULL
		    AND lease_owner <> '' AND fence > 0 AND attempt > 0
		    AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL
		    AND lease_until <= clock_timestamp()
		    AND takeover_not_before <= LEAST($3, clock_timestamp())
		  ORDER BY tenant_id
		  LIMIT $5`,
		types.TaskCreationExecutionVersionV1,
		types.TaskOperationStatusExecuting, before, afterTenantID, limit,
	)
	if err != nil {
		return nil, taskCreationDatabaseError("list stale tenant shards", err)
	}
	defer rows.Close()

	tenantIDs := make([]int64, 0)
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			return nil, taskCreationDatabaseError("scan stale tenant shard", err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		return nil, taskCreationDatabaseError("iterate stale tenant shards", err)
	}
	return tenantIDs, nil
}

// ListStaleTaskCreationOperations returns only recoverable v1 operations for a
// single tenant. Terminal/tombstoned rows, incomplete/corrupt lease rows, and
// all historical v0 actions are excluded.
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
		   FROM task_creation_operations
		  WHERE tenant_id = $1 AND tool_name = 'create_schedule'
		    AND execution_version = $2 AND status = $3
		    AND tombstoned_at IS NULL
		    AND lease_owner <> '' AND fence > 0 AND attempt > 0
		    AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL
		    AND lease_until <= clock_timestamp()
		    AND takeover_not_before <= LEAST($4, clock_timestamp())
		  ORDER BY takeover_not_before, id
		  LIMIT $5`,
		tenantID, types.TaskCreationExecutionVersionV1,
		types.TaskOperationStatusExecuting, before, limit,
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
		types.TaskOperationStatusExecuting, cp.from,
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
		`UPDATE task_creation_operations
		    SET compiled_definition = $6, compiled_digest = $7,
		        phase = $8, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $9
		    AND status = $10 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $11 AND compiled_definition IS NULL AND compiled_digest = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		definition, digest, types.TaskCreationPhaseDefinitionCompiled,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
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
		`UPDATE task_creation_operations
		    SET ensure_receipt = $6, task_id = $7, phase = $8, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $9
		    AND status = $10 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $11 AND ensure_receipt IS NULL AND task_id = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		receipt, taskID, types.TaskCreationPhaseScheduleEnsured,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
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
	status       types.TaskOperationStatus
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
	if termination.status != types.TaskOperationStatusExecuted && termination.errorCode == "" {
		return taskCreationValidation("terminal error code is empty")
	}
	if termination.status != types.TaskOperationStatusExecuted &&
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
		if err := adoptTaskCreationTermination(op, termination); err != nil {
			return err
		}
		return verifyTaskCreationReceiptForTerminal(
			ctx, tx, lease.ID, lease.TenantID, lease.UserID)
	}
	if op.Status != types.TaskOperationStatusExecuting || op.LeaseUntil == nil ||
		op.TakeoverNotBefore == nil ||
		!databaseNow.Before(*op.LeaseUntil) {
		return taskCreationLeaseLost()
	}
	if err := validateTaskCreationTerminationPhase(op.Phase, termination.status); err != nil {
		return err
	}
	if termination.status == types.TaskOperationStatusExecuted {
		row, found, err := loadCreationScheduleForUpdate(ctx, tx, termination.taskID)
		if err != nil {
			return err
		}
		if !found || row.tenantID != lease.TenantID || row.userID != lease.UserID ||
			row.status != types.ScheduleStatusActive {
			return taskCreationConflict("completed operation has no exact active aggregate")
		}
	}
	if termination.status == types.TaskOperationStatusExecuted &&
		op.TaskID != "" && op.TaskID != termination.taskID {
		return taskCreationConflict("completed task id differs from ensure checkpoint")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE task_creation_operations
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
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
	)
	if err != nil {
		return taskCreationDatabaseError("write terminal tombstone", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := insertTaskCreationReceiptForTerminal(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
		return err
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
	case types.TaskOperationStatusExecuted:
		if op.ExecutedAt != nil && op.ErrorCode == "" && op.ErrorMessage == "" &&
			op.TaskID == termination.taskID && taskCreationJSONEqual(op.Result, termination.result) {
			return nil
		}
	case types.TaskOperationStatusBlocked, types.TaskOperationStatusFailed:
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
	if op.Status != types.TaskOperationStatusExecuting || op.LeaseUntil == nil ||
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
		   FROM task_creation_operations
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $4
		  FOR UPDATE /* task creation operation lock order */`,
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
	if p.ReceiptProvider != "" || p.ReceiptTarget != "" {
		if err := validateTaskCreationReceiptTarget(p.ReceiptProvider, p.ReceiptTarget); err != nil {
			return err
		}
	}
	if err := validateTaskCreationDuration(p.LeaseDuration, maxTaskCreationLease, "lease duration"); err != nil {
		return err
	}
	return nil
}

func validateCreateTaskCreationParams(p types.CreateTaskCreationOperationParams) error {
	if strings.TrimSpace(p.ID) == "" || p.ID != strings.TrimSpace(p.ID) ||
		len(p.ID) > 255 || !utf8.ValidString(p.ID) {
		return taskCreationValidation("operation id is invalid")
	}
	if p.TenantID <= 0 || p.UserID <= 0 {
		return taskCreationValidation("operation tenant/user scope is invalid")
	}
	if p.SessionID != nil && *p.SessionID <= 0 {
		return taskCreationValidation("operation session id is invalid")
	}
	if p.ExpiresAt.IsZero() {
		return taskCreationValidation("operation expiry is missing")
	}
	if len(p.Args) == 0 || len(p.Args) > maxTaskCreationArgsBytes || !utf8.Valid(p.Args) {
		return taskCreationValidation("operation args size or encoding is invalid")
	}
	if strings.TrimSpace(p.Summary) == "" || p.Summary != strings.TrimSpace(p.Summary) ||
		len(p.Summary) > maxTaskCreationSummaryBytes || !utf8.ValidString(p.Summary) {
		return taskCreationValidation("operation summary size or encoding is invalid")
	}
	var args map[string]json.RawMessage
	if err := strictjson.Decode(p.Args, &args); err != nil || args == nil {
		return types.NewAppError(types.CodeValidation,
			"task creation: args must be a strict JSON object", err)
	}
	return nil
}

func taskCreationCreationRequestEqual(
	op *types.TaskCreationOperation,
	p types.CreateTaskCreationOperationParams,
) bool {
	if op == nil || op.ID != p.ID || op.TenantID != p.TenantID || op.UserID != p.UserID ||
		op.ToolName != "create_schedule" ||
		op.ExecutionVersion != types.TaskCreationExecutionVersionV1 ||
		op.Status != types.TaskOperationStatusPending || op.Phase != "" ||
		op.LeaseOwner != "" || op.LeaseUntil != nil || op.TakeoverNotBefore != nil ||
		op.Fence != 0 || op.Attempt != 0 || op.ReceiptProvider != "" ||
		op.ReceiptTarget != "" || op.TombstonedAt != nil ||
		op.Summary != p.Summary || !op.ExpiresAt.Equal(p.ExpiresAt.Truncate(time.Microsecond)) ||
		!taskCreationJSONEqual(op.Args, p.Args) {
		return false
	}
	if op.SessionID == nil || p.SessionID == nil {
		return op.SessionID == nil && p.SessionID == nil
	}
	return *op.SessionID == *p.SessionID
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
	status types.TaskOperationStatus,
) error {
	switch status {
	case types.TaskOperationStatusExecuted:
		if phase != types.TaskCreationPhaseActivated {
			return taskCreationConflict("operation is not activated")
		}
	case types.TaskOperationStatusBlocked, types.TaskOperationStatusFailed:
		rank := taskCreationPhaseRank(phase)
		if rank < taskCreationPhaseRank(types.TaskCreationPhaseClaimed) ||
			rank > taskCreationPhaseRank(types.TaskCreationPhaseDefinitionCompiled) {
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

func taskCreationStatusIsTerminal(status types.TaskOperationStatus) bool {
	switch status {
	case types.TaskOperationStatusExecuted,
		types.TaskOperationStatusCancelled,
		types.TaskOperationStatusExpired,
		types.TaskOperationStatusBlocked,
		types.TaskOperationStatusFailed:
		return true
	default:
		return false
	}
}

func pendingTaskCreationOperationPristine(op *types.TaskCreationOperation) bool {
	return op != nil && op.Status == types.TaskOperationStatusPending &&
		op.Phase == "" && op.LeaseOwner == "" && op.LeaseUntil == nil &&
		op.TakeoverNotBefore == nil && op.Fence == 0 && op.Attempt == 0 &&
		len(op.NormalizedCommand) == 0 && len(op.CompiledDefinition) == 0 &&
		op.CompiledDigest == "" && len(op.PreparedSchedule) == 0 &&
		len(op.EnsureReceipt) == 0 && op.TaskID == "" && len(op.Result) == 0 &&
		op.ErrorCode == "" && op.ErrorMessage == "" && op.ReceiptProvider == "" &&
		op.ReceiptTarget == "" && op.ExecutedAt == nil && op.TombstonedAt == nil
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
		types.TaskCreationPhaseCancelled,
		types.TaskCreationPhaseExpired,
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
