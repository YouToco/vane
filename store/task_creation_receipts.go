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

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	maxTaskCreationReceiptLease       = 24 * time.Hour
	maxTaskCreationReceiptPayload     = 256 << 10
	maxTaskCreationReceiptMessages    = 64 << 10
	maxTaskCreationReceiptProvider    = 64
	maxTaskCreationReceiptTarget      = 512
	maxTaskCreationProviderMessageID  = 512
	maxTaskCreationReceiptRetryWindow = 30 * 24 * time.Hour
)

const taskCreationReceiptColumns = `
	r.id, r.operation_id, r.tenant_id, r.user_id, r.session_id,
	r.provider, r.target, r.provider_key::text, r.status,
	r.lease_owner, r.lease_until, r.takeover_not_before, r.fence, r.attempt,
	r.next_attempt_at, r.payload, r.payload_digest, r.session_recorded_at,
	r.session_messages_digest, r.provider_message_id, r.failure_class,
	r.ambiguous_since, r.sent_at, r.blocked_at, r.created_at, r.updated_at,
	p.summary, p.status, p.phase, p.task_id,
	p.result, p.error_code, p.error_message`

func scanTaskCreationReceipt(row pgx.Row, receipt *types.TaskCreationReceipt) error {
	return row.Scan(
		&receipt.ID, &receipt.OperationID, &receipt.TenantID, &receipt.UserID,
		&receipt.SessionID, &receipt.Provider, &receipt.Target, &receipt.ProviderKey,
		&receipt.Status, &receipt.LeaseOwner, &receipt.LeaseUntil,
		&receipt.TakeoverNotBefore, &receipt.Fence, &receipt.Attempt,
		&receipt.NextAttemptAt, &receipt.Payload, &receipt.PayloadDigest,
		&receipt.SessionRecordedAt, &receipt.SessionMessagesDigest,
		&receipt.ProviderMessageID, &receipt.FailureClass, &receipt.AmbiguousSince,
		&receipt.SentAt, &receipt.BlockedAt, &receipt.CreatedAt, &receipt.UpdatedAt,
		&receipt.OperationSummary,
		&receipt.OperationStatus, &receipt.OperationPhase, &receipt.TaskID,
		&receipt.Result, &receipt.ErrorCode, &receipt.ErrorMessage,
	)
}

func taskCreationReceiptSelect(suffix string) string {
	return `SELECT ` + taskCreationReceiptColumns + `
		FROM task_creation_receipts r
		JOIN task_creation_operations p ON p.id = r.operation_id ` + suffix
}

func (s *Store) LoadTaskCreationReceiptByOperation(
	ctx context.Context,
	operationID string,
	tenantID int64,
	userID int64,
) (*types.TaskCreationReceipt, error) {
	if strings.TrimSpace(operationID) == "" || operationID != strings.TrimSpace(operationID) ||
		tenantID <= 0 || userID <= 0 {
		return nil, taskCreationValidation("invalid receipt operation scope")
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, tenantID, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, taskCreationDatabaseError("begin task creation receipt load", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	var receipt types.TaskCreationReceipt
	err = scanTaskCreationReceipt(tx.QueryRow(ctx,
		taskCreationReceiptSelect(`
		 WHERE r.operation_id = $1 AND r.tenant_id = $2 AND r.user_id = $3`),
		operationID, tenantID, userID,
	), &receipt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskCreationNotFound()
		}
		return nil, taskCreationDatabaseError("load task creation receipt", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit task creation receipt load", err)
	}
	return &receipt, nil
}

func (s *Store) ListDueTaskCreationReceipts(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]types.TaskCreationReceipt, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, taskCreationValidation("invalid due receipt query")
	}
	tx, err := s.beginRecoveryTenantRead(ctx, tenantID, "vane_app")
	if err != nil {
		return nil, err
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	rows, err := tx.Query(ctx, taskCreationReceiptSelect(`
		 WHERE r.tenant_id = $1 AND r.status = $2
		   AND r.provider <> '' AND r.target <> ''
		   AND r.next_attempt_at <= LEAST($3, clock_timestamp())
		   AND (r.lease_until IS NULL OR
		        (r.takeover_not_before IS NOT NULL AND
		         r.takeover_not_before <= LEAST($3, clock_timestamp())))
		 ORDER BY r.next_attempt_at, r.id LIMIT $4`),
		tenantID, types.TaskCreationReceiptStatusPending, before, limit)
	if err != nil {
		return nil, taskCreationDatabaseError("list due task creation receipts", err)
	}
	defer rows.Close()
	receipts := make([]types.TaskCreationReceipt, 0)
	for rows.Next() {
		var receipt types.TaskCreationReceipt
		if err := scanTaskCreationReceipt(rows, &receipt); err != nil {
			return nil, taskCreationDatabaseError("scan due task creation receipt", err)
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, taskCreationDatabaseError("iterate due task creation receipts", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit due receipt list", err)
	}
	return receipts, nil
}

func (s *Store) AcquireTaskCreationReceipt(
	ctx context.Context,
	p types.AcquireTaskCreationReceiptParams,
) (*types.TaskCreationReceipt, error) {
	if err := validateAcquireTaskCreationReceiptParams(p); err != nil {
		return nil, err
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, p.TenantID, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin receipt acquisition", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	receipt, databaseNow, err := loadTaskCreationReceiptForUpdate(
		ctx, tx, p.ID, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != types.TaskCreationReceiptStatusPending {
		return nil, taskCreationReceiptTerminal()
	}
	if receipt.Provider == "" || receipt.Target == "" ||
		databaseNow.Before(receipt.NextAttemptAt) {
		return nil, taskCreationReceiptBusy()
	}
	if receipt.LeaseUntil != nil {
		if databaseNow.Before(*receipt.LeaseUntil) {
			if receipt.LeaseOwner == p.LeaseOwner {
				return receipt, nil
			}
			return nil, taskCreationReceiptBusy()
		}
		if receipt.TakeoverNotBefore == nil || databaseNow.Before(*receipt.TakeoverNotBefore) {
			return nil, taskCreationReceiptBusy()
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET lease_owner = $4,
		       lease_until = clock_timestamp() + ($6 * interval '1 microsecond'),
		       takeover_not_before = clock_timestamp() + ($7 * interval '1 microsecond'),
		       fence = fence + 1, attempt = attempt + 1,
		       updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND status = $8 AND fence = $5
		   AND next_attempt_at <= clock_timestamp()
		   AND (lease_until IS NULL OR takeover_not_before <= clock_timestamp())`,
		p.ID, p.TenantID, p.UserID, p.LeaseOwner, receipt.Fence,
		p.LeaseDuration.Microseconds(),
		(p.LeaseDuration + taskCreationTakeoverSafetyGrace).Microseconds(),
		types.TaskCreationReceiptStatusPending)
	if err != nil {
		return nil, taskCreationDatabaseError("acquire task creation receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, taskCreationReceiptBusy()
	}
	receipt, _, err = loadTaskCreationReceiptForUpdate(ctx, tx, p.ID, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit receipt acquisition", err)
	}
	return receipt, nil
}

func (s *Store) CheckpointTaskCreationReceiptPayload(
	ctx context.Context,
	lease types.TaskCreationReceiptLease,
	payload []byte,
	digest string,
) error {
	if len(payload) == 0 || len(payload) > maxTaskCreationReceiptPayload ||
		!utf8.Valid(payload) || !validSHA256Digest(digest) {
		return taskCreationValidation("receipt payload checkpoint is invalid")
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != digest {
		return taskCreationValidation("receipt payload digest differs")
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin receipt payload checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	receipt, _, err := loadLeasedTaskCreationReceipt(ctx, tx, lease)
	if err != nil {
		return err
	}
	if len(receipt.Payload) != 0 || receipt.PayloadDigest != "" {
		if bytes.Equal(receipt.Payload, payload) && receipt.PayloadDigest == digest {
			return nil
		}
		return taskCreationConflict("immutable receipt payload differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET payload = $6, payload_digest = $7, updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND lease_owner = $4 AND fence = $5 AND status = $8
		   AND lease_until > clock_timestamp()
		   AND payload IS NULL AND payload_digest = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		payload, digest, types.TaskCreationReceiptStatusPending)
	if err != nil {
		return taskCreationDatabaseError("write receipt payload checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationReceiptLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit receipt payload checkpoint", err)
	}
	return nil
}

func (s *Store) RecordTaskCreationReceiptSessionMessages(
	ctx context.Context,
	lease types.TaskCreationReceiptLease,
	messages json.RawMessage,
) error {
	if err := validateTaskCreationReceiptLease(lease); err != nil {
		return err
	}
	if len(messages) == 0 || len(messages) > maxTaskCreationReceiptMessages ||
		!utf8.Valid(messages) {
		return taskCreationValidation("receipt session messages are invalid")
	}
	var messageItems []json.RawMessage
	if err := strictjson.Decode(messages, &messageItems); err != nil || messageItems == nil {
		return types.NewAppError(types.CodeValidation,
			"task creation: receipt session messages must be a strict JSON array", err)
	}
	sum := sha256.Sum256(messages)
	digest := hex.EncodeToString(sum[:])
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin receipt session checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	// Enter the normal tenant-scoped runtime role before the receipt root lock.
	// The transaction then follows receipt -> session and every receipt,
	// projection, and event-ledger read/write remains under RLS/least privilege.
	if err := setAgentEventRuntimeContext(
		ctx, tx, lease.TenantID,
	); err != nil {
		return taskCreationDatabaseError(
			"enter receipt session runtime scope", err,
		)
	}
	receipt, _, err := loadLeasedTaskCreationReceipt(ctx, tx, lease)
	if err != nil {
		return err
	}
	if receipt.SessionRecordedAt != nil || receipt.SessionMessagesDigest != "" {
		if receipt.SessionRecordedAt != nil && receipt.SessionMessagesDigest == digest {
			if receipt.SessionID != nil {
				_, _, replayErr := commitAgentSessionAppendTx(
					ctx,
					tx,
					lease.TenantID,
					lease.UserID,
					*receipt.SessionID,
					fmt.Sprintf("creation-receipt:%d", lease.ID),
					messages,
					true,
				)
				if replayErr != nil {
					return taskCreationDatabaseError(
						"replay receipt session ledger checkpoint",
						replayErr,
					)
				}
			}
			return nil
		}
		return taskCreationConflict("immutable receipt session messages differ")
	}
	if receipt.SessionID != nil {
		_, _, err := commitAgentSessionAppendTx(
			ctx,
			tx,
			lease.TenantID,
			lease.UserID,
			*receipt.SessionID,
			fmt.Sprintf("creation-receipt:%d", lease.ID),
			messages,
			false,
		)
		if err != nil {
			return taskCreationDatabaseError(
				"append receipt session projection and ledger", err,
			)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET session_recorded_at = clock_timestamp(),
		       session_messages_digest = $6, updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND lease_owner = $4 AND fence = $5 AND status = $7
		   AND lease_until > clock_timestamp()
		   AND session_recorded_at IS NULL AND session_messages_digest = ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		digest, types.TaskCreationReceiptStatusPending)
	if err != nil {
		return taskCreationDatabaseError("write receipt session checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationReceiptLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit receipt session checkpoint", err)
	}
	return nil
}

func (s *Store) MarkTaskCreationReceiptSent(
	ctx context.Context,
	lease types.TaskCreationReceiptLease,
	providerMessageID string,
) error {
	if err := validateTaskCreationReceiptLease(lease); err != nil {
		return err
	}
	if strings.TrimSpace(providerMessageID) == "" ||
		providerMessageID != strings.TrimSpace(providerMessageID) ||
		len(providerMessageID) > maxTaskCreationProviderMessageID ||
		!utf8.ValidString(providerMessageID) {
		return taskCreationValidation("provider message id is invalid")
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin receipt sent checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	receipt, databaseNow, err := loadTaskCreationReceiptForUpdate(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	if receipt.Status == types.TaskCreationReceiptStatusSent {
		if receipt.ProviderMessageID == providerMessageID && receipt.SentAt != nil &&
			receipt.LeaseOwner == "" && receipt.LeaseUntil == nil {
			return nil
		}
		return taskCreationConflict("sent receipt checkpoint differs")
	}
	if err := validateActiveTaskCreationReceiptLease(receipt, lease, databaseNow); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_creation_receipts
		   SET status = $6, provider_message_id = $7,
		       sent_at = clock_timestamp(), blocked_at = NULL,
		       lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
		       failure_class = '', ambiguous_since = NULL,
		       updated_at = clock_timestamp()
		 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		   AND lease_owner = $4 AND fence = $5 AND status = $8
		   AND lease_until > clock_timestamp()`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskCreationReceiptStatusSent, providerMessageID,
		types.TaskCreationReceiptStatusPending)
	if err != nil {
		return taskCreationDatabaseError("write receipt sent checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationReceiptLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit receipt sent checkpoint", err)
	}
	return nil
}

func (s *Store) RecordTaskCreationReceiptSendFailure(
	ctx context.Context,
	p types.RecordTaskCreationReceiptSendFailureParams,
) error {
	if err := validateTaskCreationReceiptLease(p.Lease); err != nil {
		return err
	}
	if !validTaskCreationReceiptFailureClass(p.Class) {
		return taskCreationValidation("receipt failure class is invalid")
	}
	retryable := p.Class == types.TaskCreationReceiptFailureRetryable ||
		p.Class == types.TaskCreationReceiptFailureAmbiguous
	if (retryable && (p.RetryAfter <= 0 ||
		p.RetryAfter > maxTaskCreationReceiptRetryWindow || p.RetryAfter.Microseconds() <= 0)) ||
		(!retryable && p.RetryAfter != 0) {
		return taskCreationValidation("receipt retry boundary is invalid")
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, p.Lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin receipt failure checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	receipt, databaseNow, err := loadTaskCreationReceiptForUpdate(
		ctx, tx, p.Lease.ID, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		return err
	}
	if receipt.Status == types.TaskCreationReceiptStatusBlocked {
		if receipt.FailureClass == p.Class && receipt.BlockedAt != nil {
			return nil
		}
		return taskCreationConflict("blocked receipt checkpoint differs")
	}
	if err := validateActiveTaskCreationReceiptLease(receipt, p.Lease, databaseNow); err != nil {
		return err
	}
	if retryable {
		tag, err := tx.Exec(ctx, `
			UPDATE task_creation_receipts
			   SET failure_class = $6,
			       next_attempt_at = clock_timestamp() + ($7 * interval '1 microsecond'),
			       ambiguous_since = CASE WHEN $6 = $8
			                              THEN COALESCE(ambiguous_since, clock_timestamp())
			                              ELSE ambiguous_since END,
			       lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
			       updated_at = clock_timestamp()
			 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
			   AND lease_owner = $4 AND fence = $5 AND status = $9
			   AND lease_until > clock_timestamp()`,
			p.Lease.ID, p.Lease.TenantID, p.Lease.UserID,
			p.Lease.LeaseOwner, p.Lease.Fence, p.Class, p.RetryAfter.Microseconds(),
			types.TaskCreationReceiptFailureAmbiguous,
			types.TaskCreationReceiptStatusPending)
		if err != nil {
			return taskCreationDatabaseError("write retryable receipt failure", err)
		}
		if tag.RowsAffected() != 1 {
			return taskCreationReceiptLeaseLost()
		}
	} else {
		tag, err := tx.Exec(ctx, `
			UPDATE task_creation_receipts
			   SET status = $6, failure_class = $7,
			       blocked_at = clock_timestamp(), sent_at = NULL,
			       lease_owner = '', lease_until = NULL, takeover_not_before = NULL,
			       updated_at = clock_timestamp()
			 WHERE id = $1 AND tenant_id = $2 AND user_id = $3
			   AND lease_owner = $4 AND fence = $5 AND status = $8
			   AND lease_until > clock_timestamp()`,
			p.Lease.ID, p.Lease.TenantID, p.Lease.UserID,
			p.Lease.LeaseOwner, p.Lease.Fence,
			types.TaskCreationReceiptStatusBlocked, p.Class,
			types.TaskCreationReceiptStatusPending)
		if err != nil {
			return taskCreationDatabaseError("write permanent receipt failure", err)
		}
		if tag.RowsAffected() != 1 {
			return taskCreationReceiptLeaseLost()
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit receipt failure checkpoint", err)
	}
	return nil
}

func insertTaskCreationReceiptForTerminal(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
	tenantID int64,
	userID int64,
) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO task_creation_receipts (
			operation_id, tenant_id, user_id, session_id, provider, target,
			provider_key, status, next_attempt_at, failure_class, blocked_at
		)
		SELECT p.id, p.tenant_id, p.user_id, p.session_id,
		       p.receipt_provider, p.receipt_target,
		       md5('vane/task-creation-receipt/v1:' || p.id)::uuid,
		       CASE WHEN p.receipt_provider = '' OR p.receipt_target = ''
		            THEN $4 ELSE $5 END,
		       clock_timestamp() + interval '4 seconds',
		       CASE WHEN p.receipt_provider = '' OR p.receipt_target = ''
		            THEN $6 ELSE '' END,
		       CASE WHEN p.receipt_provider = '' OR p.receipt_target = ''
		            THEN clock_timestamp() ELSE NULL END
		  FROM task_creation_operations p
		 WHERE p.id = $1 AND p.tenant_id = $2 AND p.user_id = $3
		   AND p.tool_name = 'create_schedule' AND p.execution_version = 1
		   AND p.tombstoned_at IS NOT NULL
		   AND p.status IN ('executed', 'cancelled', 'expired', 'blocked', 'failed')
		ON CONFLICT (operation_id) DO NOTHING`,
		operationID, tenantID, userID,
		types.TaskCreationReceiptStatusBlocked,
		types.TaskCreationReceiptStatusPending,
		types.TaskCreationReceiptFailureTargetUnbound)
	if err != nil {
		return taskCreationDatabaseError("insert terminal task creation receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationConflict("terminal receipt already exists")
	}
	return nil
}

func verifyTaskCreationReceiptForTerminal(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
	tenantID int64,
	userID int64,
) error {
	var count int
	err := tx.QueryRow(ctx, `
		SELECT count(*)
		  FROM task_creation_receipts r
		  JOIN task_creation_operations p ON p.id = r.operation_id
		 WHERE r.operation_id = $1 AND r.tenant_id = $2 AND r.user_id = $3
		   AND r.provider = p.receipt_provider AND r.target = p.receipt_target`,
		operationID, tenantID, userID).Scan(&count)
	if err != nil {
		return taskCreationDatabaseError("verify terminal task creation receipt", err)
	}
	if count != 1 {
		return taskCreationConflict("terminal receipt is missing or differs")
	}
	return nil
}

func loadTaskCreationReceiptForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	id int64,
	tenantID int64,
	userID int64,
) (*types.TaskCreationReceipt, time.Time, error) {
	var receipt types.TaskCreationReceipt
	err := scanTaskCreationReceipt(tx.QueryRow(ctx,
		taskCreationReceiptSelect(`
		 WHERE r.id = $1 AND r.tenant_id = $2 AND r.user_id = $3
		 FOR UPDATE OF r`), id, tenantID, userID), &receipt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, time.Time{}, taskCreationNotFound()
		}
		return nil, time.Time{}, taskCreationDatabaseError("lock task creation receipt", err)
	}
	databaseNow, err := taskCreationDatabaseClock(ctx, tx)
	if err != nil {
		return nil, time.Time{}, err
	}
	return &receipt, databaseNow, nil
}

func loadLeasedTaskCreationReceipt(
	ctx context.Context,
	tx pgx.Tx,
	lease types.TaskCreationReceiptLease,
) (*types.TaskCreationReceipt, time.Time, error) {
	if err := validateTaskCreationReceiptLease(lease); err != nil {
		return nil, time.Time{}, err
	}
	receipt, databaseNow, err := loadTaskCreationReceiptForUpdate(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID)
	if err != nil {
		return nil, time.Time{}, err
	}
	if err := validateActiveTaskCreationReceiptLease(receipt, lease, databaseNow); err != nil {
		return nil, time.Time{}, err
	}
	return receipt, databaseNow, nil
}

func validateActiveTaskCreationReceiptLease(
	receipt *types.TaskCreationReceipt,
	lease types.TaskCreationReceiptLease,
	databaseNow time.Time,
) error {
	if receipt.Status != types.TaskCreationReceiptStatusPending {
		return taskCreationReceiptTerminal()
	}
	if receipt.LeaseOwner != lease.LeaseOwner || receipt.Fence != lease.Fence ||
		receipt.LeaseUntil == nil || !databaseNow.Before(*receipt.LeaseUntil) {
		return taskCreationReceiptLeaseLost()
	}
	return nil
}

func validateAcquireTaskCreationReceiptParams(
	p types.AcquireTaskCreationReceiptParams,
) error {
	if p.ID <= 0 || p.TenantID <= 0 || p.UserID <= 0 ||
		strings.TrimSpace(p.LeaseOwner) == "" ||
		p.LeaseOwner != strings.TrimSpace(p.LeaseOwner) || len(p.LeaseOwner) > 255 {
		return taskCreationValidation("invalid receipt acquisition scope")
	}
	return validateTaskCreationDuration(
		p.LeaseDuration, maxTaskCreationReceiptLease, "receipt lease duration")
}

func validateTaskCreationReceiptLease(lease types.TaskCreationReceiptLease) error {
	if lease.ID <= 0 || lease.TenantID <= 0 || lease.UserID <= 0 ||
		strings.TrimSpace(lease.LeaseOwner) == "" ||
		lease.LeaseOwner != strings.TrimSpace(lease.LeaseOwner) ||
		len(lease.LeaseOwner) > 255 || lease.Fence <= 0 {
		return taskCreationValidation("invalid task creation receipt lease")
	}
	return nil
}

func validateTaskCreationReceiptTarget(provider, target string) error {
	if strings.TrimSpace(provider) == "" || provider != strings.TrimSpace(provider) ||
		len(provider) > maxTaskCreationReceiptProvider || !utf8.ValidString(provider) ||
		strings.TrimSpace(target) == "" || target != strings.TrimSpace(target) ||
		len(target) > maxTaskCreationReceiptTarget || !utf8.ValidString(target) {
		return taskCreationValidation("task creation receipt target is invalid")
	}
	return nil
}

func validTaskCreationReceiptFailureClass(class types.TaskCreationReceiptFailureClass) bool {
	switch class {
	case types.TaskCreationReceiptFailureRetryable,
		types.TaskCreationReceiptFailureAmbiguous,
		types.TaskCreationReceiptFailurePermanent:
		return true
	default:
		return false
	}
}

func taskCreationReceiptBusy() error {
	return fmt.Errorf("%w: task creation receipt has an active owner or is not due",
		types.ErrTaskCreationReceiptBusy)
}

func taskCreationReceiptTerminal() error {
	return fmt.Errorf("%w: task creation receipt is terminal",
		types.ErrTaskCreationReceiptTerminal)
}

func taskCreationReceiptLeaseLost() error {
	return fmt.Errorf("%w: task creation receipt lease is no longer valid",
		types.ErrTaskCreationReceiptLeaseLost)
}
