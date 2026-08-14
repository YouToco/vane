package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/types"
)

const (
	maxTaskDefinitionEditReceiptLease       = 24 * time.Hour
	maxTaskDefinitionEditReceiptPayload     = 2 << 20
	maxTaskDefinitionEditReceiptMessages    = 64 << 10
	maxTaskDefinitionEditReceiptProvider    = 64
	maxTaskDefinitionEditReceiptTarget      = 512
	maxTaskDefinitionEditProviderMessageID  = 512
	maxTaskDefinitionEditReceiptRetryWindow = 30 * 24 * time.Hour
	taskDefinitionEditReceiptTakeoverGrace  = 30 * time.Second
)

const taskDefinitionEditReceiptColumns = `
	r.id, r.operation_id, r.tenant_id, r.user_id, r.session_id,
	r.provider, r.target, r.provider_key::text, r.status,
	r.lease_owner, r.lease_until, r.takeover_not_before, r.fence, r.attempt,
	r.next_attempt_at, r.payload, r.payload_digest, r.session_recorded_at,
	r.session_messages_digest, r.provider_message_id, r.failure_class,
	r.ambiguous_since, r.sent_at, r.blocked_at, r.created_at, r.updated_at,
	o.status, o.phase, o.task_id, o.result, o.error_code, o.error_message`

func scanTaskDefinitionEditReceipt(
	row pgx.Row,
	receipt *types.TaskDefinitionEditReceipt,
) error {
	return row.Scan(
		&receipt.ID, &receipt.OperationID, &receipt.TenantID, &receipt.UserID,
		&receipt.SessionID, &receipt.Provider, &receipt.Target, &receipt.ProviderKey,
		&receipt.Status, &receipt.LeaseOwner, &receipt.LeaseUntil,
		&receipt.TakeoverNotBefore, &receipt.Fence, &receipt.Attempt,
		&receipt.NextAttemptAt, &receipt.Payload, &receipt.PayloadDigest,
		&receipt.SessionRecordedAt, &receipt.SessionMessagesDigest,
		&receipt.ProviderMessageID, &receipt.FailureClass, &receipt.AmbiguousSince,
		&receipt.SentAt, &receipt.BlockedAt, &receipt.CreatedAt, &receipt.UpdatedAt,
		&receipt.OperationStatus, &receipt.OperationPhase, &receipt.TaskID,
		&receipt.Result, &receipt.ErrorCode, &receipt.ErrorMessage,
	)
}

func taskDefinitionEditReceiptSelect(suffix string) string {
	return `SELECT ` + taskDefinitionEditReceiptColumns + `
		FROM task_definition_edit_receipts r
		JOIN task_definition_edit_operations o
		  ON o.id = r.operation_id
		 AND o.tenant_id = r.tenant_id
		 AND o.user_id = r.user_id
		 AND o.session_id = r.session_id
		 AND o.receipt_provider = r.provider
		 AND o.receipt_target = r.target ` + suffix
}

func (s *Store) LoadTaskDefinitionEditReceiptByOperation(
	ctx context.Context,
	operationID string,
	tenantID int64,
	userID int64,
) (*types.TaskDefinitionEditReceipt, error) {
	if !validTaskDefinitionEditReceiptReference(operationID, 512) ||
		tenantID <= 0 || userID <= 0 {
		return nil, taskDefinitionEditReceiptValidation(
			"task definition edit receipt operation scope is invalid")
	}
	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, tenantID)
	if err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("begin receipt load", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	var receipt types.TaskDefinitionEditReceipt
	err = scanTaskDefinitionEditReceipt(tx.QueryRow(ctx,
		taskDefinitionEditReceiptSelect(`
		 WHERE r.operation_id = $1 AND r.tenant_id = $2 AND r.user_id = $3`),
		operationID, tenantID, userID,
	), &receipt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskDefinitionEditReceiptNotFound()
		}
		return nil, taskDefinitionEditReceiptDatabaseError("load receipt", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("commit receipt load", err)
	}
	return &receipt, nil
}

func (s *Store) ListDueTaskDefinitionEditReceipts(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]types.TaskDefinitionEditReceipt, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, taskDefinitionEditReceiptValidation(
			"task definition edit due receipt query is invalid")
	}
	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, tenantID)
	if err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("begin due receipt list", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	rows, err := tx.Query(ctx, taskDefinitionEditReceiptSelect(`
		 WHERE r.tenant_id = $1 AND r.status = $2
		   AND r.provider <> '' AND r.target <> ''
		   AND r.next_attempt_at <= LEAST($3, clock_timestamp())
		   AND (r.lease_until IS NULL OR
		        (r.takeover_not_before IS NOT NULL AND
		         r.takeover_not_before <= LEAST($3, clock_timestamp())))
		 ORDER BY r.next_attempt_at, r.id
		 LIMIT $4`),
		tenantID, types.TaskDefinitionEditReceiptStatusPending, before, limit)
	if err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("list due receipts", err)
	}
	defer rows.Close()

	receipts := make([]types.TaskDefinitionEditReceipt, 0)
	for rows.Next() {
		var receipt types.TaskDefinitionEditReceipt
		if err := scanTaskDefinitionEditReceipt(rows, &receipt); err != nil {
			return nil, taskDefinitionEditReceiptDatabaseError("scan due receipt", err)
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("iterate due receipts", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("commit due receipt list", err)
	}
	return receipts, nil
}

func (s *Store) AcquireTaskDefinitionEditReceipt(
	ctx context.Context,
	p types.AcquireTaskDefinitionEditReceiptParams,
) (*types.TaskDefinitionEditReceipt, error) {
	if err := validateAcquireTaskDefinitionEditReceiptParams(p); err != nil {
		return nil, err
	}
	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, p.TenantID)
	if err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("begin receipt acquisition", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	receipt, databaseNow, err := loadTaskDefinitionEditReceiptForUpdate(
		ctx, tx, p.ID, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != types.TaskDefinitionEditReceiptStatusPending {
		return nil, taskDefinitionEditReceiptTerminal()
	}
	if receipt.Provider == "" || receipt.Target == "" ||
		databaseNow.Before(receipt.NextAttemptAt) {
		return nil, taskDefinitionEditReceiptBusy()
	}
	if receipt.LeaseUntil != nil {
		if databaseNow.Before(*receipt.LeaseUntil) {
			if receipt.LeaseOwner == p.LeaseOwner {
				if err := tx.Commit(ctx); err != nil {
					return nil, taskDefinitionEditReceiptDatabaseError(
						"commit receipt acquisition replay", err)
				}
				return receipt, nil
			}
			return nil, taskDefinitionEditReceiptBusy()
		}
		if receipt.TakeoverNotBefore == nil ||
			databaseNow.Before(*receipt.TakeoverNotBefore) {
			return nil, taskDefinitionEditReceiptBusy()
		}
	}

	tag, err := tx.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET lease_owner = $4,
		       lease_until = clock_timestamp() + ($6 * interval '1 microsecond'),
		       takeover_not_before = clock_timestamp() + ($7 * interval '1 microsecond'),
		       fence = fence + 1,
		       attempt = attempt + 1,
		       updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND status = $8 AND fence = $5
		   AND next_attempt_at <= clock_timestamp()
		   AND (lease_until IS NULL OR takeover_not_before <= clock_timestamp())`,
		p.ID, p.TenantID, p.UserID, p.LeaseOwner, receipt.Fence,
		p.LeaseDuration.Microseconds(),
		(p.LeaseDuration + taskDefinitionEditReceiptTakeoverGrace).Microseconds(),
		types.TaskDefinitionEditReceiptStatusPending)
	if err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("acquire receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, taskDefinitionEditReceiptBusy()
	}
	receipt, _, err = loadTaskDefinitionEditReceiptForUpdate(
		ctx, tx, p.ID, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditReceiptDatabaseError("commit receipt acquisition", err)
	}
	return receipt, nil
}

func (s *Store) CheckpointTaskDefinitionEditReceiptPayload(
	ctx context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	payload []byte,
	digest string,
) error {
	if len(payload) == 0 || len(payload) > maxTaskDefinitionEditReceiptPayload ||
		!utf8.Valid(payload) || !validTaskDefinitionEditReceiptDigest(digest) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt payload checkpoint is invalid")
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != digest {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt payload digest differs")
	}
	if err := validateTaskDefinitionEditReceiptLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("begin receipt payload checkpoint", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	receipt, _, err := loadLeasedTaskDefinitionEditReceipt(ctx, tx, lease)
	if err != nil {
		return err
	}
	if len(receipt.Payload) != 0 || receipt.PayloadDigest != "" {
		if bytes.Equal(receipt.Payload, payload) && receipt.PayloadDigest == digest {
			if err := tx.Commit(ctx); err != nil {
				return taskDefinitionEditReceiptDatabaseError(
					"commit receipt payload replay", err)
			}
			return nil
		}
		return taskDefinitionEditReceiptConflict(
			"immutable task definition edit receipt payload differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET payload = $6, payload_digest = $7, updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND lease_owner = $4 AND fence = $5 AND status = $8
		   AND lease_until > clock_timestamp()
		   AND payload IS NULL AND payload_digest = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		payload, digest, types.TaskDefinitionEditReceiptStatusPending)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("write receipt payload checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditReceiptLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditReceiptDatabaseError("commit receipt payload checkpoint", err)
	}
	return nil
}

func (s *Store) RecordTaskDefinitionEditReceiptSessionMessages(
	ctx context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	messages json.RawMessage,
) error {
	if len(messages) == 0 || len(messages) > maxTaskDefinitionEditReceiptMessages ||
		!utf8.Valid(messages) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt session messages are invalid")
	}
	var messageItems []json.RawMessage
	if err := strictjson.Decode(messages, &messageItems); err != nil || messageItems == nil {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt session messages must be a strict JSON array")
	}
	if err := validateTaskDefinitionEditReceiptLease(lease); err != nil {
		return err
	}
	sum := sha256.Sum256(messages)
	digest := hex.EncodeToString(sum[:])

	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("begin receipt session checkpoint", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	receipt, _, err := loadLeasedTaskDefinitionEditReceipt(ctx, tx, lease)
	if err != nil {
		return err
	}
	if receipt.SessionRecordedAt != nil || receipt.SessionMessagesDigest != "" {
		if receipt.SessionRecordedAt != nil &&
			receipt.SessionMessagesDigest == digest {
			_, _, replayErr := commitAgentSessionAppendTx(
				ctx,
				tx,
				lease.TenantID,
				lease.UserID,
				receipt.SessionID,
				fmt.Sprintf("definition-edit-receipt:%d", lease.ID),
				messages,
				true,
			)
			if replayErr != nil {
				return taskDefinitionEditReceiptDatabaseError(
					"replay receipt session ledger checkpoint",
					replayErr,
				)
			}
			if err := tx.Commit(ctx); err != nil {
				return taskDefinitionEditReceiptDatabaseError(
					"commit receipt session replay", err)
			}
			return nil
		}
		return taskDefinitionEditReceiptConflict(
			"immutable task definition edit receipt session messages differ")
	}

	_, _, err = commitAgentSessionAppendTx(
		ctx,
		tx,
		lease.TenantID,
		lease.UserID,
		receipt.SessionID,
		fmt.Sprintf("definition-edit-receipt:%d", lease.ID),
		messages,
		false,
	)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError(
			"append receipt session projection and ledger", err,
		)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET session_recorded_at = clock_timestamp(),
		       session_messages_digest = $6,
		       updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND lease_owner = $4 AND fence = $5 AND status = $7
		   AND lease_until > clock_timestamp()
		   AND session_recorded_at IS NULL AND session_messages_digest = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		digest, types.TaskDefinitionEditReceiptStatusPending)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("write receipt session checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditReceiptLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditReceiptDatabaseError("commit receipt session checkpoint", err)
	}
	return nil
}

func (s *Store) MarkTaskDefinitionEditReceiptSent(
	ctx context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	providerMessageID string,
) error {
	if err := validateTaskDefinitionEditReceiptLease(lease); err != nil {
		return err
	}
	if !validTaskDefinitionEditReceiptReference(
		providerMessageID, maxTaskDefinitionEditProviderMessageID) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt provider message id is invalid")
	}
	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("begin receipt sent checkpoint", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	receipt, databaseNow, err := loadTaskDefinitionEditReceiptForUpdate(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	if receipt.Status == types.TaskDefinitionEditReceiptStatusSent {
		if receipt.ProviderMessageID == providerMessageID && receipt.SentAt != nil &&
			receipt.LeaseOwner == "" && receipt.LeaseUntil == nil &&
			len(receipt.Payload) != 0 && receipt.PayloadDigest != "" &&
			receipt.SessionRecordedAt != nil && receipt.SessionMessagesDigest != "" {
			if err := tx.Commit(ctx); err != nil {
				return taskDefinitionEditReceiptDatabaseError(
					"commit receipt sent replay", err)
			}
			return nil
		}
		return taskDefinitionEditReceiptConflict(
			"sent task definition edit receipt checkpoint differs")
	}
	if err := validateActiveTaskDefinitionEditReceiptLease(
		receipt, lease, databaseNow); err != nil {
		return err
	}
	if len(receipt.Payload) == 0 || receipt.PayloadDigest == "" ||
		receipt.SessionRecordedAt == nil || receipt.SessionMessagesDigest == "" {
		return taskDefinitionEditReceiptConflict(
			"task definition edit receipt delivery checkpoints are incomplete")
	}

	tag, err := tx.Exec(ctx, `
		UPDATE task_definition_edit_receipts
		   SET status = $6,
		       provider_message_id = $7,
		       sent_at = clock_timestamp(),
		       blocked_at = NULL,
		       lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
		       failure_class = '', ambiguous_since = NULL,
		       updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND lease_owner = $4 AND fence = $5 AND status = $8
		   AND lease_until > clock_timestamp()
		   AND payload IS NOT NULL AND payload_digest <> ''
		   AND session_recorded_at IS NOT NULL AND session_messages_digest <> ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskDefinitionEditReceiptStatusSent, providerMessageID,
		types.TaskDefinitionEditReceiptStatusPending)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("write receipt sent checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditReceiptLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditReceiptDatabaseError("commit receipt sent checkpoint", err)
	}
	return nil
}

func (s *Store) RecordTaskDefinitionEditReceiptSendFailure(
	ctx context.Context,
	p types.RecordTaskDefinitionEditReceiptSendFailureParams,
) error {
	if err := validateTaskDefinitionEditReceiptLease(p.Lease); err != nil {
		return err
	}
	if !validTaskDefinitionEditReceiptFailureClass(p.Class) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt failure class is invalid")
	}
	retryable := p.Class == types.TaskDefinitionEditReceiptFailureRetryable ||
		p.Class == types.TaskDefinitionEditReceiptFailureAmbiguous
	if (retryable && (p.RetryAfter <= 0 ||
		p.RetryAfter > maxTaskDefinitionEditReceiptRetryWindow ||
		p.RetryAfter.Microseconds() <= 0)) ||
		(!retryable && p.RetryAfter != 0) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt retry boundary is invalid")
	}

	tx, err := s.beginTaskDefinitionEditReceiptTx(ctx, p.Lease.TenantID)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("begin receipt failure checkpoint", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	receipt, databaseNow, err := loadTaskDefinitionEditReceiptForUpdate(
		ctx, tx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		return err
	}
	if receipt.Status == types.TaskDefinitionEditReceiptStatusBlocked {
		if p.Class == types.TaskDefinitionEditReceiptFailurePermanent &&
			receipt.FailureClass == p.Class && receipt.BlockedAt != nil &&
			receipt.LeaseOwner == "" && receipt.LeaseUntil == nil {
			if err := tx.Commit(ctx); err != nil {
				return taskDefinitionEditReceiptDatabaseError(
					"commit blocked receipt replay", err)
			}
			return nil
		}
		return taskDefinitionEditReceiptConflict(
			"blocked task definition edit receipt checkpoint differs")
	}
	if err := validateActiveTaskDefinitionEditReceiptLease(
		receipt, p.Lease, databaseNow); err != nil {
		return err
	}
	if receipt.AmbiguousSince != nil &&
		p.Class == types.TaskDefinitionEditReceiptFailurePermanent {
		return taskDefinitionEditReceiptConflict(
			"ambiguous task definition edit receipt requires exact sent proof")
	}

	if retryable {
		tag, err := tx.Exec(ctx, `
			UPDATE task_definition_edit_receipts
			   SET failure_class = CASE
			           WHEN failure_class = $8 OR $6 = $8 THEN $8
			           ELSE $6
			       END,
			       next_attempt_at = clock_timestamp() + ($7 * interval '1 microsecond'),
			       ambiguous_since = CASE
			           WHEN failure_class = $8 OR $6 = $8
			           THEN COALESCE(ambiguous_since, clock_timestamp())
			           ELSE NULL
			       END,
			       lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
			       updated_at = clock_timestamp()
			 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
			   AND lease_owner = $4 AND fence = $5 AND status = $9
			   AND lease_until > clock_timestamp()`,
			p.Lease.ID, p.Lease.TenantID, p.Lease.UserID,
			p.Lease.LeaseOwner, p.Lease.Fence, p.Class,
			p.RetryAfter.Microseconds(),
			types.TaskDefinitionEditReceiptFailureAmbiguous,
			types.TaskDefinitionEditReceiptStatusPending)
		if err != nil {
			return taskDefinitionEditReceiptDatabaseError("write retryable receipt failure", err)
		}
		if tag.RowsAffected() != 1 {
			return taskDefinitionEditReceiptLeaseLost()
		}
	} else {
		tag, err := tx.Exec(ctx, `
			UPDATE task_definition_edit_receipts
			   SET status = $6,
			       failure_class = $7,
			       blocked_at = clock_timestamp(), sent_at = NULL,
			       ambiguous_since = NULL,
			       lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
			       updated_at = clock_timestamp()
			 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
			   AND lease_owner = $4 AND fence = $5 AND status = $8
			   AND lease_until > clock_timestamp()`,
			p.Lease.ID, p.Lease.TenantID, p.Lease.UserID,
			p.Lease.LeaseOwner, p.Lease.Fence,
			types.TaskDefinitionEditReceiptStatusBlocked, p.Class,
			types.TaskDefinitionEditReceiptStatusPending)
		if err != nil {
			return taskDefinitionEditReceiptDatabaseError("write permanent receipt failure", err)
		}
		if tag.RowsAffected() != 1 {
			return taskDefinitionEditReceiptLeaseLost()
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditReceiptDatabaseError("commit receipt failure checkpoint", err)
	}
	return nil
}

// insertTaskDefinitionEditReceiptForTerminal is called only after the
// coordinator has locked schedule -> operation and made the operation
// terminal in the same transaction. The coordinator role can insert the
// immutable outbox identity but cannot mutate any later delivery checkpoint.
func insertTaskDefinitionEditReceiptForTerminal(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
	tenantID int64,
	userID int64,
) error {
	if !validTaskDefinitionEditReceiptReference(operationID, 512) ||
		tenantID <= 0 || userID <= 0 {
		return taskDefinitionEditReceiptValidation(
			"task definition edit terminal receipt scope is invalid")
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO task_definition_edit_receipts (
			operation_id, tenant_id, user_id, session_id, provider, target,
			provider_key, status, next_attempt_at, provider_message_id,
			failure_class, sent_at
		)
		SELECT o.id, o.tenant_id, o.user_id, o.session_id,
		       o.receipt_provider, o.receipt_target,
		       substr(encode(sha256(convert_to(
		           'vane/task-definition-edit-receipt/v1:' || o.id, 'UTF8'
		       )), 'hex'), 1, 32)::uuid,
		       CASE WHEN o.receipt_provider = '' AND o.receipt_target = ''
		            THEN $4 ELSE $5 END,
		       clock_timestamp() + interval '4 seconds',
		       CASE WHEN o.receipt_provider = '' AND o.receipt_target = ''
		            THEN $6 ELSE '' END,
		       CASE WHEN o.receipt_provider = '' AND o.receipt_target = ''
		            THEN $7 ELSE '' END,
		       CASE WHEN o.receipt_provider = '' AND o.receipt_target = ''
		            THEN clock_timestamp() ELSE NULL END
		  FROM task_definition_edit_operations o
		 WHERE o.id = $1 AND o.tenant_id = $2 AND o.user_id = $3
		   AND o.tombstoned_at IS NOT NULL
		   AND o.status IN ('completed', 'blocked', 'superseded', 'cancelled', 'expired')
		ON CONFLICT (operation_id) DO NOTHING`,
		operationID, tenantID, userID,
		types.TaskDefinitionEditReceiptStatusSuppressed,
		types.TaskDefinitionEditReceiptStatusPending,
		"target-unbound-suppressed",
		types.TaskDefinitionEditReceiptFailureTargetUnbound)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("insert terminal receipt", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return verifyTaskDefinitionEditReceiptForTerminal(
		ctx, tx, operationID, tenantID, userID)
}

// verifyTaskDefinitionEditReceiptForTerminal proves immutable adoption after
// an operation-id conflict. Bound receipts may already have progressed to
// sent/blocked; only their immutable origin is compared. Suppressed receipts
// never dispatch, so their complete terminal marker is compared as well.
func verifyTaskDefinitionEditReceiptForTerminal(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
	tenantID int64,
	userID int64,
) error {
	if !validTaskDefinitionEditReceiptReference(operationID, 512) ||
		tenantID <= 0 || userID <= 0 {
		return taskDefinitionEditReceiptValidation(
			"task definition edit terminal receipt scope is invalid")
	}
	var count int
	err := tx.QueryRow(ctx, `
		SELECT count(*)
		  FROM task_definition_edit_receipts r
		  JOIN task_definition_edit_operations o
		    ON o.id = r.operation_id
		   AND o.tenant_id = r.tenant_id
		   AND o.user_id = r.user_id
		   AND o.session_id = r.session_id
		 WHERE r.operation_id = $1 AND r.tenant_id = $2 AND r.user_id = $3
		   AND o.tombstoned_at IS NOT NULL
		   AND o.status IN ('completed', 'blocked', 'superseded', 'cancelled', 'expired')
		   AND r.provider = o.receipt_provider
		   AND r.target = o.receipt_target
		   AND r.provider_key =
		       substr(encode(sha256(convert_to(
		           'vane/task-definition-edit-receipt/v1:' || o.id, 'UTF8'
		       )), 'hex'), 1, 32)::uuid
		   AND (
		       (o.receipt_provider <> '' AND o.receipt_target <> '')
		       OR
		       (o.receipt_provider = '' AND o.receipt_target = ''
		        AND r.status = $4
		        AND r.failure_class = $5
		        AND r.provider_message_id = $6
		        AND r.sent_at IS NOT NULL AND r.blocked_at IS NULL
		        AND r.lease_owner = '' AND r.lease_until IS NULL
		        AND r.takeover_not_before IS NULL
		        AND r.fence = 0 AND r.attempt = 0
		        AND r.payload IS NULL AND r.payload_digest = ''
		        AND r.session_recorded_at IS NULL
		        AND r.session_messages_digest = ''
		        AND r.ambiguous_since IS NULL)
		   )`,
		operationID, tenantID, userID,
		types.TaskDefinitionEditReceiptStatusSuppressed,
		types.TaskDefinitionEditReceiptFailureTargetUnbound,
		"target-unbound-suppressed").Scan(&count)
	if err != nil {
		return taskDefinitionEditReceiptDatabaseError("verify terminal receipt", err)
	}
	if count != 1 {
		return taskDefinitionEditReceiptConflict(
			"task definition edit terminal receipt is missing or differs")
	}
	return nil
}

func loadTaskDefinitionEditReceiptForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	id int64,
	tenantID int64,
	userID int64,
) (*types.TaskDefinitionEditReceipt, time.Time, error) {
	var receipt types.TaskDefinitionEditReceipt
	err := scanTaskDefinitionEditReceipt(tx.QueryRow(ctx,
		taskDefinitionEditReceiptSelect(`
		 WHERE r.id = $1 AND r.tenant_id = $2 AND r.user_id = $3
		 FOR UPDATE OF r`),
		id, tenantID, userID,
	), &receipt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, time.Time{}, taskDefinitionEditReceiptNotFound()
		}
		return nil, time.Time{}, taskDefinitionEditReceiptDatabaseError("lock receipt", err)
	}

	// PostgreSQL may evaluate a SELECT target list before waiting for a row
	// lock. Read DB time only after FOR UPDATE has returned, so a lock wait that
	// crosses lease expiry cannot authorize from a stale pre-wait timestamp.
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return nil, time.Time{}, taskDefinitionEditReceiptDatabaseError(
			"read receipt database clock", err)
	}
	return &receipt, databaseNow, nil
}

func loadLeasedTaskDefinitionEditReceipt(
	ctx context.Context,
	tx pgx.Tx,
	lease types.TaskDefinitionEditReceiptLease,
) (*types.TaskDefinitionEditReceipt, time.Time, error) {
	receipt, databaseNow, err := loadTaskDefinitionEditReceiptForUpdate(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID)
	if err != nil {
		return nil, time.Time{}, err
	}
	if err := validateActiveTaskDefinitionEditReceiptLease(
		receipt, lease, databaseNow); err != nil {
		return nil, time.Time{}, err
	}
	return receipt, databaseNow, nil
}

func validateActiveTaskDefinitionEditReceiptLease(
	receipt *types.TaskDefinitionEditReceipt,
	lease types.TaskDefinitionEditReceiptLease,
	databaseNow time.Time,
) error {
	if receipt.Status != types.TaskDefinitionEditReceiptStatusPending {
		return taskDefinitionEditReceiptTerminal()
	}
	if receipt.LeaseOwner != lease.LeaseOwner || receipt.Fence != lease.Fence ||
		receipt.LeaseUntil == nil || !databaseNow.Before(*receipt.LeaseUntil) {
		return taskDefinitionEditReceiptLeaseLost()
	}
	return nil
}

func validateAcquireTaskDefinitionEditReceiptParams(
	p types.AcquireTaskDefinitionEditReceiptParams,
) error {
	if p.ID <= 0 || p.TenantID <= 0 || p.UserID <= 0 ||
		!validTaskDefinitionEditReceiptReference(p.LeaseOwner, 255) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt acquisition scope is invalid")
	}
	if p.LeaseDuration <= 0 ||
		p.LeaseDuration > maxTaskDefinitionEditReceiptLease ||
		p.LeaseDuration.Microseconds() <= 0 {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt lease duration is invalid")
	}
	return nil
}

func validateTaskDefinitionEditReceiptLease(
	lease types.TaskDefinitionEditReceiptLease,
) error {
	if lease.ID <= 0 || lease.TenantID <= 0 || lease.UserID <= 0 ||
		!validTaskDefinitionEditReceiptReference(lease.LeaseOwner, 255) ||
		lease.Fence <= 0 {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt lease is invalid")
	}
	return nil
}

func validateTaskDefinitionEditReceiptTarget(
	provider string,
	target string,
	allowEmpty bool,
) error {
	if provider == "" || target == "" {
		if allowEmpty && provider == "" && target == "" {
			return nil
		}
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt target is incomplete")
	}
	if !validTaskDefinitionEditReceiptReference(
		provider, maxTaskDefinitionEditReceiptProvider) ||
		!validTaskDefinitionEditReceiptReference(target, maxTaskDefinitionEditReceiptTarget) {
		return taskDefinitionEditReceiptValidation(
			"task definition edit receipt target is invalid")
	}
	return nil
}

func validTaskDefinitionEditReceiptReference(value string, maxBytes int) bool {
	return validTaskDefinitionEditReference(value, maxBytes)
}

func validTaskDefinitionEditReceiptDigest(digest string) bool {
	if len(digest) != 64 || digest != strings.ToLower(digest) {
		return false
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size
}

func validTaskDefinitionEditReceiptFailureClass(
	class types.TaskDefinitionEditReceiptFailureClass,
) bool {
	switch class {
	case types.TaskDefinitionEditReceiptFailureRetryable,
		types.TaskDefinitionEditReceiptFailureAmbiguous,
		types.TaskDefinitionEditReceiptFailurePermanent:
		return true
	default:
		return false
	}
}

func taskDefinitionEditReceiptValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func taskDefinitionEditReceiptConflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func taskDefinitionEditReceiptNotFound() error {
	return types.NewAppError(types.CodeNotFound,
		"task definition edit receipt is unavailable", nil)
}

func taskDefinitionEditReceiptDatabaseError(action string, cause error) error {
	return taskStateDatabaseError("task definition edit receipt: "+action, cause)
}

func taskDefinitionEditReceiptBusy() error {
	return fmt.Errorf("%w: task definition edit receipt has an active owner or is not due",
		types.ErrTaskDefinitionEditReceiptBusy)
}

func taskDefinitionEditReceiptTerminal() error {
	return fmt.Errorf("%w: task definition edit receipt is terminal",
		types.ErrTaskDefinitionEditReceiptTerminal)
}

func taskDefinitionEditReceiptLeaseLost() error {
	return fmt.Errorf("%w: task definition edit receipt lease is no longer valid",
		types.ErrTaskDefinitionEditReceiptLeaseLost)
}
