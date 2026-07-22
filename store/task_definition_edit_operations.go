package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const taskDefinitionEditOperationColumns = `
	id, tenant_id, user_id, target_tenant_id, target_user_id, task_id,
	session_id, approval_ref, status, phase, expires_at, confirmed_at,
	original_status, base_definition_version, base_definition_digest,
	base_definition, target_definition_version, target_definition_digest,
	target_definition, canonical_proposal, proposal_digest, prepared_edit,
	prepared_edit_digest, base_snapshot, base_snapshot_digest, pause_snapshot,
	pause_snapshot_digest, apply_snapshot, apply_snapshot_digest,
	restore_snapshot, restore_snapshot_digest, lease_owner, lease_until,
	takeover_not_before, fence, attempt, receipt_provider, receipt_target,
	result, error_code, error_message, created_at, updated_at, tombstoned_at`

const (
	maxTaskDefinitionEditLease            = 24 * time.Hour
	taskDefinitionEditTakeoverSafetyGrace = 30 * time.Second
	maxTaskDefinitionEditLeaseOwner       = 255
	maxTaskDefinitionEditResultBytes      = 64 << 10
)

type taskDefinitionEditScheduleRow struct {
	Status      types.ScheduleStatus
	Mode        types.ExecutionMode
	Version     *int64
	Digest      *string
	OperationID *string
	Fence       *int64
}

func scanTaskDefinitionEditOperation(
	row pgx.Row,
	op *types.TaskDefinitionEditOperation,
) error {
	return row.Scan(
		&op.ID, &op.TenantID, &op.UserID, &op.TargetTenantID,
		&op.TargetUserID, &op.TaskID, &op.SessionID, &op.ApprovalRef,
		&op.Status, &op.Phase, &op.ExpiresAt, &op.ConfirmedAt,
		&op.OriginalStatus, &op.BaseDefinitionVersion,
		&op.BaseDefinitionDigest, &op.BaseDefinition,
		&op.TargetDefinitionVersion, &op.TargetDefinitionDigest,
		&op.TargetDefinition, &op.CanonicalProposal, &op.ProposalDigest,
		&op.PreparedEdit, &op.PreparedEditDigest, &op.BaseSnapshot,
		&op.BaseSnapshotDigest, &op.PauseSnapshot,
		&op.PauseSnapshotDigest, &op.ApplySnapshot,
		&op.ApplySnapshotDigest, &op.RestoreSnapshot,
		&op.RestoreSnapshotDigest, &op.LeaseOwner, &op.LeaseUntil,
		&op.TakeoverNotBefore, &op.Fence, &op.Attempt,
		&op.ReceiptProvider, &op.ReceiptTarget, &op.Result,
		&op.ErrorCode, &op.ErrorMessage, &op.CreatedAt, &op.UpdatedAt,
		&op.TombstonedAt,
	)
}

func cloneTaskDefinitionEditOperation(
	op *types.TaskDefinitionEditOperation,
) *types.TaskDefinitionEditOperation {
	if op == nil {
		return nil
	}
	clone := *op
	clone.BaseDefinition = bytes.Clone(op.BaseDefinition)
	clone.TargetDefinition = bytes.Clone(op.TargetDefinition)
	clone.CanonicalProposal = bytes.Clone(op.CanonicalProposal)
	clone.PreparedEdit = bytes.Clone(op.PreparedEdit)
	clone.BaseSnapshot = bytes.Clone(op.BaseSnapshot)
	clone.PauseSnapshot = bytes.Clone(op.PauseSnapshot)
	clone.ApplySnapshot = bytes.Clone(op.ApplySnapshot)
	clone.RestoreSnapshot = bytes.Clone(op.RestoreSnapshot)
	clone.Result = bytes.Clone(op.Result)
	return &clone
}

func taskDefinitionEditDatabaseClock(
	ctx context.Context,
	tx pgx.Tx,
) (time.Time, error) {
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return time.Time{}, taskDefinitionEditDatabaseError("read database clock", err)
	}
	return databaseNow, nil
}

func taskDefinitionEditValidation(message string) error {
	return types.NewAppError(types.CodeValidation, "task definition edit: "+message, nil)
}

func taskDefinitionEditConflict(message string) error {
	return types.NewAppError(types.CodeConflict, "task definition edit: "+message, nil)
}

func taskDefinitionEditIntegrity() error {
	return types.NewAppError(types.CodeInternal,
		"task definition edit: durable state integrity check failed", nil)
}

func taskDefinitionEditDatabaseError(action string, cause error) error {
	return taskStateDatabaseError("task definition edit: "+action, cause)
}

func taskDefinitionEditNotFound() error {
	return fmt.Errorf("%w: task definition edit operation is unavailable", types.ErrNotFound)
}

func taskDefinitionEditBusy() error {
	return fmt.Errorf("%w: task definition edit operation has an active owner",
		types.ErrTaskDefinitionEditBusy)
}

func taskDefinitionEditTerminal() error {
	return fmt.Errorf("%w: task definition edit operation is terminal",
		types.ErrTaskDefinitionEditTerminal)
}

func taskDefinitionEditLeaseLost() error {
	return fmt.Errorf("%w: task definition edit lease is no longer valid",
		types.ErrTaskDefinitionEditLeaseLost)
}

func validTaskDefinitionEditScope(scope types.TaskDefinitionEditScope) bool {
	return validTaskDefinitionEditReference(scope.ID, 512) &&
		scope.TenantID > 0 && scope.UserID > 0 &&
		scope.TargetTenantID > 0 && scope.TargetUserID > 0 &&
		scope.TenantID == scope.TargetTenantID &&
		scope.UserID == scope.TargetUserID &&
		validTaskDefinitionEditReference(scope.TaskID, 255)
}

func validTaskDefinitionEditReference(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return false
		}
	}
	return true
}

func validateTaskDefinitionEditAcquire(
	p types.AcquireTaskDefinitionEditOperationParams,
) error {
	if !validTaskDefinitionEditScope(p.Scope) ||
		!validTaskDefinitionEditReference(p.LeaseOwner, maxTaskDefinitionEditLeaseOwner) {
		return taskDefinitionEditValidation("acquisition scope is invalid")
	}
	if p.LeaseDuration <= 0 || p.LeaseDuration > maxTaskDefinitionEditLease ||
		p.LeaseDuration.Microseconds() <= 0 {
		return taskDefinitionEditValidation("lease duration is invalid")
	}
	if err := validateTaskDefinitionEditReceiptTarget(
		p.ReceiptProvider, p.ReceiptTarget, true); err != nil {
		return err
	}
	return nil
}

func validateTaskDefinitionEditLease(lease types.TaskDefinitionEditLease) error {
	if !validTaskDefinitionEditScope(types.TaskDefinitionEditScope{
		ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
		TargetTenantID: lease.TargetTenantID,
		TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
	}) || !validTaskDefinitionEditReference(lease.LeaseOwner,
		maxTaskDefinitionEditLeaseOwner) || lease.Fence <= 0 {
		return taskDefinitionEditValidation("lease identity is invalid")
	}
	return nil
}

func taskDefinitionEditOperationIsTerminal(
	status types.TaskDefinitionEditOperationStatus,
) bool {
	switch status {
	case types.TaskDefinitionEditOperationStatusCompleted,
		types.TaskDefinitionEditOperationStatusCancelled,
		types.TaskDefinitionEditOperationStatusExpired,
		types.TaskDefinitionEditOperationStatusBlocked,
		types.TaskDefinitionEditOperationStatusSuperseded:
		return true
	default:
		return false
	}
}

func taskDefinitionEditScopeMatches(
	op *types.TaskDefinitionEditOperation,
	scope types.TaskDefinitionEditScope,
) bool {
	return op != nil && op.ID == scope.ID && op.TenantID == scope.TenantID &&
		op.UserID == scope.UserID && op.TargetTenantID == scope.TargetTenantID &&
		op.TargetUserID == scope.TargetUserID && op.TaskID == scope.TaskID
}

func lockTaskDefinitionEditScheduleForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (*taskDefinitionEditScheduleRow, error) {
	var row taskDefinitionEditScheduleRow
	var rawStatus, rawMode string
	err := tx.QueryRow(ctx,
		`SELECT status, execution_mode, approved_definition_version,
		        approved_definition_digest, definition_edit_operation_id,
		        definition_edit_fence
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`, tenantID, userID, taskID,
	).Scan(&rawStatus, &rawMode, &row.Version, &row.Digest,
		&row.OperationID, &row.Fence)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("lock schedule", err)
	}
	row.Status = types.ScheduleStatus(rawStatus)
	if row.Status != types.ScheduleStatusActive && row.Status != types.ScheduleStatusPaused {
		return nil, taskDefinitionEditIntegrity()
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil || (row.Version == nil) != (row.Digest == nil) ||
		(row.OperationID == nil) != (row.Fence == nil) {
		return nil, taskDefinitionEditIntegrity()
	}
	row.Mode = mode
	return &row, nil
}

func loadTaskDefinitionEditOperationForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	var op types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`SELECT `+taskDefinitionEditOperationColumns+`
		   FROM task_definition_edit_operations
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		  FOR UPDATE`, scope.ID, scope.TenantID, scope.UserID,
		scope.TargetTenantID, scope.TargetUserID, scope.TaskID,
	), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditNotFound()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("load operation for update", err)
	}
	return &op, nil
}

func loadLeasedTaskDefinitionEditOperation(
	ctx context.Context,
	tx pgx.Tx,
	lease types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, time.Time, error) {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return nil, time.Time{}, err
	}
	op, err := loadTaskDefinitionEditOperationForUpdate(ctx, tx,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		})
	if err != nil {
		return nil, time.Time{}, err
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return nil, time.Time{}, err
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) || op.TombstonedAt != nil {
		return nil, time.Time{}, taskDefinitionEditTerminal()
	}
	if op.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence ||
		op.LeaseUntil == nil || op.TakeoverNotBefore == nil ||
		!databaseNow.Before(*op.LeaseUntil) {
		return nil, time.Time{}, taskDefinitionEditLeaseLost()
	}
	return op, databaseNow, nil
}

func taskDefinitionEditOriginalStatus(
	frozen definitioneditwire.FrozenProposal,
) (types.ScheduleStatus, error) {
	var originalStatus types.ScheduleStatus
	switch frozen.Proposal.OriginalStatus {
	case definitioneditwire.OriginalStatusActive:
		originalStatus = types.ScheduleStatusActive
	case definitioneditwire.OriginalStatusPaused:
		originalStatus = types.ScheduleStatusPaused
	default:
		return "", taskDefinitionEditValidation("proposal original status is invalid")
	}
	return originalStatus, nil
}

func validateTaskDefinitionEditCreationScope(
	ctx context.Context,
	tx pgx.Tx,
	frozen definitioneditwire.FrozenProposal,
	baseDefinition taskstate.ApprovedDefinitionV1,
	schedule *taskDefinitionEditScheduleRow,
	originalStatus types.ScheduleStatus,
) error {
	proposal := frozen.Proposal
	if schedule == nil {
		return taskDefinitionEditNotFound()
	}
	if schedule.OperationID != nil || schedule.Fence != nil {
		return taskDefinitionEditConflict("schedule already has an edit marker")
	}
	if schedule.Status != originalStatus || schedule.Mode != types.ExecutionModeCompiled ||
		schedule.Version == nil || schedule.Digest == nil ||
		*schedule.Version != proposal.BaseHead.Version ||
		subtle.ConstantTimeCompare([]byte(*schedule.Digest),
			[]byte(proposal.BaseHead.Digest)) != 1 {
		return taskDefinitionEditConflict("schedule differs from the approved base")
	}

	var membershipValid bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM tenants t
		   JOIN memberships m ON m.tenant_id=t.id
		  WHERE t.id=$1 AND m.user_id=$2 AND t.status='active'
		    AND t.deleted_at IS NULL
		  FOR SHARE OF t, m`, proposal.Actor.TenantID, proposal.Actor.UserID,
	).Scan(&membershipValid)
	if errors.Is(err, pgx.ErrNoRows) {
		return taskDefinitionEditValidation("actor membership is not active")
	}
	if err != nil {
		return taskDefinitionEditDatabaseError("validate active membership", err)
	}
	var sessionValid bool
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND status='active'
		  FOR SHARE`, proposal.SessionID, proposal.Actor.TenantID,
		proposal.Actor.UserID,
	).Scan(&sessionValid)
	if errors.Is(err, pgx.ErrNoRows) {
		return taskDefinitionEditValidation("agent session is not active or is outside scope")
	}
	if err != nil {
		return taskDefinitionEditDatabaseError("validate active session", err)
	}

	base, err := loadApprovedDefinitionVersionTx(ctx, tx,
		proposal.Target.TenantID, proposal.Target.UserID,
		proposal.Target.TaskID, proposal.BaseHead.Version)
	if err != nil {
		return err
	}
	if !bytes.Equal(base.Payload, frozen.BaseDefinitionBytes) ||
		subtle.ConstantTimeCompare([]byte(base.Digest),
			[]byte(proposal.BaseHead.Digest)) != 1 {
		return taskDefinitionEditConflict("approved base bytes differ")
	}
	if err := validateApprovedDefinitionProjectionTx(ctx, tx,
		baseDefinition, frozen.BaseDefinitionBytes); err != nil {
		return err
	}
	return validateTaskDefinitionEditCreationProvenance(ctx, tx, frozen)
}

func validateTaskDefinitionEditCreationProvenance(
	ctx context.Context,
	tx pgx.Tx,
	frozen definitioneditwire.FrozenProposal,
) error {
	creationBytes, err := definitioneditwire.CanonicalCreation(frozen.Prepared)
	if err != nil {
		return taskDefinitionEditIntegrity()
	}
	rows, err := tx.Query(ctx,
		`SELECT prepared_schedule, compiled_digest
		   FROM pending_actions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND tool_name='create_schedule' AND execution_version=$4
		    AND status=$5 AND phase=$6 AND tombstoned_at IS NOT NULL
		  ORDER BY id
		  FOR SHARE`, frozen.Proposal.Target.TenantID,
		frozen.Proposal.Target.UserID, frozen.Proposal.Target.TaskID,
		types.TaskCreationExecutionVersionV1, types.PendingActionStatusExecuted,
		types.TaskCreationPhaseCompleted,
	)
	if err != nil {
		return taskDefinitionEditDatabaseError("load creation provenance", err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var prepared []byte
		var compiledDigest string
		if err := rows.Scan(&prepared, &compiledDigest); err != nil {
			return taskDefinitionEditDatabaseError("scan creation provenance", err)
		}
		if bytes.Equal(prepared, creationBytes) &&
			subtle.ConstantTimeCompare([]byte(compiledDigest),
				[]byte(frozen.Prepared.Creation.PreparedDigest)) == 1 {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		return taskDefinitionEditDatabaseError("iterate creation provenance", err)
	}
	if !found {
		return taskDefinitionEditConflict("prepared creation provenance is unavailable")
	}
	return nil
}

// CreateTaskDefinitionEditOperation seals one current-writer-approved proposal.
// The five raw checkpoints are the only caller input; every trusted identity,
// digest and timestamp is derived from their exact canonical representation.
func (s *Store) CreateTaskDefinitionEditOperation(
	ctx context.Context,
	p types.CreateTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	frozen, err := definitioneditwire.DecodeFrozenProposal(
		p.CanonicalProposal, p.BaseDefinition, p.TargetDefinition,
		p.PreparedEdit, p.BaseSnapshot,
	)
	if err != nil {
		return nil, taskDefinitionEditValidation("proposal checkpoints are invalid")
	}
	originalStatus, err := taskDefinitionEditOriginalStatus(frozen)
	if err != nil {
		return nil, err
	}
	baseDefinition, err := taskstate.DecodeApprovedDefinitionV1(frozen.BaseDefinitionBytes)
	if err != nil {
		return nil, taskDefinitionEditValidation("base definition is invalid")
	}
	canonicalBase, err := taskstate.EncodeApprovedDefinitionV1(baseDefinition)
	if err != nil || !bytes.Equal(canonicalBase, frozen.BaseDefinitionBytes) {
		return nil, taskDefinitionEditValidation("base definition is not canonical")
	}
	targetDefinition, err := taskstate.DecodeApprovedDefinitionV1(
		frozen.TargetDefinitionBytes)
	if err != nil {
		return nil, taskDefinitionEditValidation("target definition is invalid")
	}
	canonicalTarget, err := taskstate.EncodeApprovedDefinitionV1(targetDefinition)
	if err != nil || !bytes.Equal(canonicalTarget, frozen.TargetDefinitionBytes) {
		return nil, taskDefinitionEditValidation("target definition is not canonical")
	}
	proposal := frozen.Proposal
	for _, definition := range []taskstate.ApprovedDefinitionV1{
		baseDefinition, targetDefinition,
	} {
		if definition.TenantID != proposal.Target.TenantID ||
			definition.UserID != proposal.Target.UserID ||
			definition.TaskID != proposal.Target.TaskID ||
			definition.ExecutionMode != types.ExecutionModeCompiled {
			return nil, taskDefinitionEditValidation("definition scope or mode differs")
		}
	}
	if targetDefinition.SourceScope != taskstate.SourceScopeApprovedPlan ||
		targetDefinition.Intent != targetDefinition.PlaybookContent {
		return nil, taskDefinitionEditValidation(
			"target definition is not an exact approved plan")
	}
	if err := definitioneditwire.ValidateApprovedProjectionBindings(
		frozen.Prepared,
		baseDefinition.SpecJSON, baseDefinition.ScopeJSON,
		baseDefinition.NLDescription,
		targetDefinition.SpecJSON, targetDefinition.ScopeJSON,
		targetDefinition.NLDescription,
	); err != nil {
		return nil, taskDefinitionEditValidation(
			"prepared edit differs from the approved definitions")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, proposal.Actor.TenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin operation creation", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		proposal.Target.TenantID, proposal.Target.UserID, proposal.Target.TaskID)
	if err != nil {
		return nil, err
	}
	var existing types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`SELECT `+taskDefinitionEditOperationColumns+`
		   FROM task_definition_edit_operations WHERE id=$1 FOR UPDATE`,
		proposal.OperationID,
	), &existing)
	if err == nil {
		if !taskDefinitionEditCreationReplayEqual(&existing, frozen, originalStatus) {
			return nil, taskDefinitionEditConflict("operation id already has another proposal")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, taskDefinitionEditDatabaseError("commit operation creation replay", err)
		}
		return cloneTaskDefinitionEditOperation(&existing), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditDatabaseError("load operation replay", err)
	}
	if err := validateTaskDefinitionEditCreationScope(
		ctx, tx, frozen, baseDefinition, schedule, originalStatus); err != nil {
		return nil, err
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return nil, err
	}
	expiresAt := time.UnixMicro(proposal.ExpiresAtUnixMicros)
	if !databaseNow.Before(expiresAt) {
		return nil, taskDefinitionEditValidation("proposal is already expired")
	}

	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`INSERT INTO task_definition_edit_operations (
			id, tenant_id, user_id, target_tenant_id, target_user_id,
			task_id, session_id, approval_ref, expires_at, original_status,
			base_definition_version, base_definition_digest, base_definition,
			target_definition_version, target_definition_digest, target_definition,
			canonical_proposal, proposal_digest, prepared_edit,
			prepared_edit_digest, base_snapshot, base_snapshot_digest
		 ) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17,$18,$19,$20,$21,$22
		 ) RETURNING `+taskDefinitionEditOperationColumns,
		proposal.OperationID, proposal.Actor.TenantID, proposal.Actor.UserID,
		proposal.Target.TenantID, proposal.Target.UserID, proposal.Target.TaskID,
		proposal.SessionID, proposal.ApprovalRef, expiresAt, originalStatus,
		proposal.BaseHead.Version, proposal.BaseHead.Digest,
		frozen.BaseDefinitionBytes, proposal.TargetHead.Version,
		proposal.TargetHead.Digest, frozen.TargetDefinitionBytes,
		frozen.CanonicalProposal, frozen.ProposalDigest,
		frozen.PreparedEditBytes, sha256HexTaskDefinitionEdit(frozen.PreparedEditBytes),
		frozen.BaseSnapshotBytes, sha256HexTaskDefinitionEdit(frozen.BaseSnapshotBytes),
	), &op)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, taskDefinitionEditConflict(
				"operation, approval reference, or live task edit already exists")
		}
		return nil, taskDefinitionEditDatabaseError("insert operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit operation creation", err)
	}
	return cloneTaskDefinitionEditOperation(&op), nil
}

func taskDefinitionEditCreationReplayEqual(
	op *types.TaskDefinitionEditOperation,
	frozen definitioneditwire.FrozenProposal,
	originalStatus types.ScheduleStatus,
) bool {
	proposal := frozen.Proposal
	return op != nil && op.ID == proposal.OperationID &&
		op.TenantID == proposal.Actor.TenantID && op.UserID == proposal.Actor.UserID &&
		op.TargetTenantID == proposal.Target.TenantID &&
		op.TargetUserID == proposal.Target.UserID && op.TaskID == proposal.Target.TaskID &&
		op.SessionID == proposal.SessionID && op.ApprovalRef == proposal.ApprovalRef &&
		op.ExpiresAt.Equal(time.UnixMicro(proposal.ExpiresAtUnixMicros)) &&
		op.OriginalStatus == originalStatus &&
		op.BaseDefinitionVersion == proposal.BaseHead.Version &&
		op.BaseDefinitionDigest == proposal.BaseHead.Digest &&
		op.TargetDefinitionVersion == proposal.TargetHead.Version &&
		op.TargetDefinitionDigest == proposal.TargetHead.Digest &&
		bytes.Equal(op.BaseDefinition, frozen.BaseDefinitionBytes) &&
		bytes.Equal(op.TargetDefinition, frozen.TargetDefinitionBytes) &&
		bytes.Equal(op.CanonicalProposal, frozen.CanonicalProposal) &&
		bytes.Equal(op.PreparedEdit, frozen.PreparedEditBytes) &&
		bytes.Equal(op.BaseSnapshot, frozen.BaseSnapshotBytes)
}

func sha256HexTaskDefinitionEdit(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// LoadTaskDefinitionEditOperation reads an exact durable scope. Recovery does
// not consult current memberships, so a suspended tenant can still converge.
func (s *Store) LoadTaskDefinitionEditOperation(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	if !validTaskDefinitionEditScope(scope) {
		return nil, taskDefinitionEditValidation("operation scope is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, scope.TenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin operation load", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`SELECT `+taskDefinitionEditOperationColumns+`
		   FROM task_definition_edit_operations
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6`,
		scope.ID, scope.TenantID, scope.UserID, scope.TargetTenantID,
		scope.TargetUserID, scope.TaskID,
	), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditNotFound()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("load operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit operation load", err)
	}
	return cloneTaskDefinitionEditOperation(&op), nil
}

// CancelTaskDefinitionEditOperation linearizes an explicit pre-confirmation
// cancellation against acquisition. An executing edit is never cancelled.
func (s *Store) CancelTaskDefinitionEditOperation(
	ctx context.Context,
	p types.CancelTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	return s.terminatePendingTaskDefinitionEdit(ctx, p.Scope,
		p.ReceiptProvider, p.ReceiptTarget,
		types.TaskDefinitionEditOperationStatusCancelled, false)
}

// ExpireTaskDefinitionEditOperation tombstones only a DB-clock-expired,
// unconfirmed proposal. The call cannot expire a still-valid confirmation.
func (s *Store) ExpireTaskDefinitionEditOperation(
	ctx context.Context,
	p types.ExpireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	return s.terminatePendingTaskDefinitionEdit(ctx, p.Scope,
		p.ReceiptProvider, p.ReceiptTarget,
		types.TaskDefinitionEditOperationStatusExpired, true)
}

func (s *Store) terminatePendingTaskDefinitionEdit(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
	receiptProvider, receiptTarget string,
	terminalStatus types.TaskDefinitionEditOperationStatus,
	requireExpired bool,
) (*types.TaskDefinitionEditOperation, error) {
	if !validTaskDefinitionEditScope(scope) {
		return nil, taskDefinitionEditValidation("terminal operation scope is invalid")
	}
	if err := validateTaskDefinitionEditReceiptTarget(
		receiptProvider, receiptTarget, true); err != nil {
		return nil, err
	}
	if terminalStatus != types.TaskDefinitionEditOperationStatusCancelled &&
		terminalStatus != types.TaskDefinitionEditOperationStatusExpired {
		return nil, taskDefinitionEditValidation("pending terminal status is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, scope.TenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin pending termination", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	// The schedule may already have been deleted. Lock it first when present so
	// every transaction which can later touch the marker uses the global order.
	if _, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		scope.TargetTenantID, scope.TargetUserID, scope.TaskID); err != nil {
		return nil, err
	}
	op, err := loadTaskDefinitionEditOperationForUpdate(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return nil, err
	}
	if op.Status == terminalStatus {
		if !pendingTaskDefinitionEditTerminalComplete(op, terminalStatus) ||
			op.ReceiptProvider != receiptProvider || op.ReceiptTarget != receiptTarget {
			return nil, taskDefinitionEditConflict("terminal replay differs")
		}
		if err := verifyTaskDefinitionEditReceiptForTerminal(
			ctx, tx, op.ID, op.TenantID, op.UserID); err != nil {
			return nil, err
		}
		return cloneTaskDefinitionEditOperation(op), nil
	}
	if op.Status == types.TaskDefinitionEditOperationStatusExecuting {
		return cloneTaskDefinitionEditOperation(op), taskDefinitionEditBusy()
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) {
		return cloneTaskDefinitionEditOperation(op), taskDefinitionEditTerminal()
	}
	if !pendingTaskDefinitionEditPristine(op) {
		return nil, taskDefinitionEditConflict("pending operation has durable saga state")
	}
	if requireExpired && databaseNow.Before(op.ExpiresAt) {
		return nil, taskDefinitionEditConflict("proposal has not expired")
	}
	var updated types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`UPDATE task_definition_edit_operations
		    SET status=$7, receipt_provider=$8, receipt_target=$9,
		        result=NULL, error_code='', error_message='',
		        tombstoned_at=clock_timestamp(), updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$10 AND phase=$11 AND tombstoned_at IS NULL
		    AND confirmed_at IS NULL AND lease_owner=''
		    AND lease_until IS NULL AND takeover_not_before IS NULL
		    AND fence=0 AND attempt=0
		    AND ($12::boolean = false OR expires_at <= clock_timestamp())
		  RETURNING `+taskDefinitionEditOperationColumns,
		scope.ID, scope.TenantID, scope.UserID, scope.TargetTenantID,
		scope.TargetUserID, scope.TaskID, terminalStatus,
		receiptProvider, receiptTarget,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditPhaseProposalSealed, requireExpired,
	), &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditConflict("pending operation changed concurrently")
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("write pending tombstone", err)
	}
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, updated.ID, updated.TenantID, updated.UserID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit pending termination", err)
	}
	return cloneTaskDefinitionEditOperation(&updated), nil
}

func pendingTaskDefinitionEditPristine(op *types.TaskDefinitionEditOperation) bool {
	return op != nil && op.Status == types.TaskDefinitionEditOperationStatusPending &&
		op.Phase == types.TaskDefinitionEditPhaseProposalSealed &&
		op.ConfirmedAt == nil && op.LeaseOwner == "" && op.LeaseUntil == nil &&
		op.TakeoverNotBefore == nil && op.Fence == 0 && op.Attempt == 0 &&
		op.TombstonedAt == nil && len(op.PauseSnapshot) == 0 &&
		len(op.ApplySnapshot) == 0 && len(op.RestoreSnapshot) == 0
}

func pendingTaskDefinitionEditTerminalComplete(
	op *types.TaskDefinitionEditOperation,
	status types.TaskDefinitionEditOperationStatus,
) bool {
	return op != nil && op.Status == status &&
		op.Phase == types.TaskDefinitionEditPhaseProposalSealed &&
		op.ConfirmedAt == nil && op.LeaseOwner == "" && op.LeaseUntil == nil &&
		op.TakeoverNotBefore == nil && op.Fence == 0 && op.Attempt == 0 &&
		op.TombstonedAt != nil && len(op.PauseSnapshot) == 0 &&
		len(op.ApplySnapshot) == 0 && len(op.RestoreSnapshot) == 0
}

type taskDefinitionEditScheduleAssessment int

const (
	taskDefinitionEditScheduleExact taskDefinitionEditScheduleAssessment = iota
	taskDefinitionEditScheduleMissing
	taskDefinitionEditScheduleSuperseded
	taskDefinitionEditScheduleUnsafe
)

func assessTaskDefinitionEditSchedule(
	op *types.TaskDefinitionEditOperation,
	schedule *taskDefinitionEditScheduleRow,
) taskDefinitionEditScheduleAssessment {
	if schedule == nil {
		return taskDefinitionEditScheduleMissing
	}
	if schedule.Version == nil || schedule.Digest == nil ||
		schedule.Mode != types.ExecutionModeCompiled {
		return taskDefinitionEditScheduleUnsafe
	}
	if *schedule.Version > op.TargetDefinitionVersion {
		return taskDefinitionEditScheduleSuperseded
	}
	expectedVersion := op.BaseDefinitionVersion
	expectedDigest := op.BaseDefinitionDigest
	markerRequired := op.Phase != types.TaskDefinitionEditPhaseProposalSealed
	if op.Phase == types.TaskDefinitionEditPhaseDefinitionCommitted ||
		op.Phase == types.TaskDefinitionEditPhaseTemporalTargetApplied ||
		op.Phase == types.TaskDefinitionEditPhaseTemporalTargetRestored {
		expectedVersion = op.TargetDefinitionVersion
		expectedDigest = op.TargetDefinitionDigest
	}
	if *schedule.Version != expectedVersion ||
		subtle.ConstantTimeCompare([]byte(*schedule.Digest), []byte(expectedDigest)) != 1 {
		return taskDefinitionEditScheduleUnsafe
	}
	if !markerRequired {
		if schedule.OperationID != nil || schedule.Fence != nil ||
			schedule.Status != op.OriginalStatus {
			return taskDefinitionEditScheduleUnsafe
		}
		return taskDefinitionEditScheduleExact
	}
	if schedule.Status != types.ScheduleStatusPaused ||
		schedule.OperationID == nil || *schedule.OperationID != op.ID ||
		schedule.Fence == nil || *schedule.Fence != op.Fence {
		return taskDefinitionEditScheduleUnsafe
	}
	return taskDefinitionEditScheduleExact
}

// AcquireTaskDefinitionEditOperation confirms a pending proposal or takes over
// an expired execution. Quiescing PostgreSQL is intentionally a separate CAS.
func (s *Store) AcquireTaskDefinitionEditOperation(
	ctx context.Context,
	p types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	if err := validateTaskDefinitionEditAcquire(p); err != nil {
		return nil, err
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, p.Scope.TenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin operation acquisition", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)

	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		p.Scope.TargetTenantID, p.Scope.TargetUserID, p.Scope.TaskID)
	if err != nil {
		return nil, err
	}
	op, err := loadTaskDefinitionEditOperationForUpdate(ctx, tx, p.Scope)
	if err != nil {
		return nil, err
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return nil, err
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) || op.TombstonedAt != nil {
		return cloneTaskDefinitionEditOperation(op), taskDefinitionEditTerminal()
	}

	switch op.Status {
	case types.TaskDefinitionEditOperationStatusPending:
		if !pendingTaskDefinitionEditPristine(op) {
			return nil, taskDefinitionEditConflict("pending operation has durable saga state")
		}
		if err := validateTaskDefinitionEditReceiptTarget(
			p.ReceiptProvider, p.ReceiptTarget, false); err != nil {
			return nil, err
		}
		if !databaseNow.Before(op.ExpiresAt) {
			return s.expireTaskDefinitionEditDuringAcquire(ctx, tx, op, p)
		}
		assessment := assessTaskDefinitionEditSchedule(op, schedule)
		if assessment != taskDefinitionEditScheduleExact {
			return s.confirmAndTerminateTaskDefinitionEditDuringAcquire(
				ctx, tx, op, p, assessment)
		}
		return s.acquirePendingTaskDefinitionEdit(ctx, tx, op, p)

	case types.TaskDefinitionEditOperationStatusExecuting:
		if op.ReceiptProvider != p.ReceiptProvider || op.ReceiptTarget != p.ReceiptTarget {
			return nil, taskDefinitionEditConflict("operation receipt target differs")
		}
		if op.ConfirmedAt == nil || op.LeaseOwner == "" || op.LeaseUntil == nil ||
			op.TakeoverNotBefore == nil || op.Fence <= 0 || op.Attempt <= 0 {
			return nil, taskDefinitionEditIntegrity()
		}
		if databaseNow.Before(*op.LeaseUntil) {
			if op.LeaseOwner == p.LeaseOwner {
				assessment := assessTaskDefinitionEditSchedule(op, schedule)
				if assessment != taskDefinitionEditScheduleExact {
					return s.terminateAcquiredTaskDefinitionEditAssessment(
						ctx, tx, op, assessment)
				}
				return cloneTaskDefinitionEditOperation(op), nil
			}
			return cloneTaskDefinitionEditOperation(op), taskDefinitionEditBusy()
		}
		if databaseNow.Before(*op.TakeoverNotBefore) {
			return cloneTaskDefinitionEditOperation(op), taskDefinitionEditBusy()
		}
		assessment := assessTaskDefinitionEditSchedule(op, schedule)
		if assessment != taskDefinitionEditScheduleExact {
			return s.terminateAcquiredTaskDefinitionEditAssessment(
				ctx, tx, op, assessment)
		}
		return s.takeOverTaskDefinitionEdit(ctx, tx, schedule, op, p)

	default:
		return nil, taskDefinitionEditIntegrity()
	}
}

func (s *Store) expireTaskDefinitionEditDuringAcquire(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	p types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	var updated types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`UPDATE task_definition_edit_operations
		    SET status=$7, receipt_provider=$8, receipt_target=$9,
		        tombstoned_at=clock_timestamp(), updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$10 AND phase=$11 AND tombstoned_at IS NULL
		    AND confirmed_at IS NULL AND fence=0 AND attempt=0
		    AND expires_at <= clock_timestamp()
		  RETURNING `+taskDefinitionEditOperationColumns,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, types.TaskDefinitionEditOperationStatusExpired,
		p.ReceiptProvider, p.ReceiptTarget,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditPhaseProposalSealed,
	), &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditConflict("operation changed during expiry")
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("expire acquisition target", err)
	}
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, updated.ID, updated.TenantID, updated.UserID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit acquisition expiry", err)
	}
	return cloneTaskDefinitionEditOperation(&updated), taskDefinitionEditTerminal()
}

func (s *Store) confirmAndTerminateTaskDefinitionEditDuringAcquire(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	p types.AcquireTaskDefinitionEditOperationParams,
	assessment taskDefinitionEditScheduleAssessment,
) (*types.TaskDefinitionEditOperation, error) {
	status := types.TaskDefinitionEditOperationStatusBlocked
	errorCode := string(types.TaskDefinitionEditBlockUnsafeRemoteState)
	errorMessage := "The task no longer matches the approved edit."
	if assessment == taskDefinitionEditScheduleMissing {
		errorCode = string(types.TaskDefinitionEditBlockScheduleDeleted)
		errorMessage = "The task was deleted before the edit could start."
	} else if assessment == taskDefinitionEditScheduleSuperseded {
		status = types.TaskDefinitionEditOperationStatusSuperseded
		errorCode = "definition_superseded"
		errorMessage = "A newer approved definition already exists."
	}
	var updated types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`UPDATE task_definition_edit_operations
		    SET status=$7, confirmed_at=clock_timestamp(),
		        receipt_provider=$8, receipt_target=$9,
		        fence=1, attempt=1, error_code=$10, error_message=$11,
		        lease_owner='', lease_until=NULL, takeover_not_before=NULL,
		        tombstoned_at=clock_timestamp(), updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$12 AND phase=$13 AND tombstoned_at IS NULL
		    AND confirmed_at IS NULL AND fence=0 AND attempt=0
		  RETURNING `+taskDefinitionEditOperationColumns,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, status, p.ReceiptProvider, p.ReceiptTarget,
		errorCode, errorMessage,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditPhaseProposalSealed,
	), &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditConflict("operation changed during terminal confirmation")
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("write terminal confirmation", err)
	}
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, updated.ID, updated.TenantID, updated.UserID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit terminal confirmation", err)
	}
	return cloneTaskDefinitionEditOperation(&updated), taskDefinitionEditTerminal()
}

func (s *Store) acquirePendingTaskDefinitionEdit(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	p types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	var updated types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`UPDATE task_definition_edit_operations
		    SET status=$7, confirmed_at=clock_timestamp(), lease_owner=$8,
		        lease_until=clock_timestamp()+($9*interval '1 microsecond'),
		        takeover_not_before=clock_timestamp()+($10*interval '1 microsecond'),
		        fence=1, attempt=1, receipt_provider=$11, receipt_target=$12,
		        updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$13 AND phase=$14 AND tombstoned_at IS NULL
		    AND confirmed_at IS NULL AND expires_at > clock_timestamp()
		    AND fence=0 AND attempt=0 AND lease_owner=''
		  RETURNING `+taskDefinitionEditOperationColumns,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, types.TaskDefinitionEditOperationStatusExecuting,
		p.LeaseOwner, p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskDefinitionEditTakeoverSafetyGrace).Microseconds(),
		p.ReceiptProvider, p.ReceiptTarget,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditPhaseProposalSealed,
	), &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditConflict("operation changed during acquisition")
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("acquire pending operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit operation acquisition", err)
	}
	return cloneTaskDefinitionEditOperation(&updated), nil
}

func (s *Store) terminateAcquiredTaskDefinitionEditAssessment(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	assessment taskDefinitionEditScheduleAssessment,
) (*types.TaskDefinitionEditOperation, error) {
	status := types.TaskDefinitionEditOperationStatusBlocked
	errorCode := string(types.TaskDefinitionEditBlockUnsafeRemoteState)
	errorMessage := "The durable task state is unsafe for this edit."
	if assessment == taskDefinitionEditScheduleMissing {
		errorCode = string(types.TaskDefinitionEditBlockScheduleDeleted)
		errorMessage = "The task was deleted while the edit was in progress."
	} else if assessment == taskDefinitionEditScheduleSuperseded {
		status = types.TaskDefinitionEditOperationStatusSuperseded
		errorCode = "definition_superseded"
		errorMessage = "A newer approved definition already exists."
	}
	updated, err := terminateTaskDefinitionEditTx(
		ctx, tx, op, status, errorCode, errorMessage, nil, false, nil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit unsafe acquisition", err)
	}
	return cloneTaskDefinitionEditOperation(updated), taskDefinitionEditTerminal()
}

func (s *Store) takeOverTaskDefinitionEdit(
	ctx context.Context,
	tx pgx.Tx,
	schedule *taskDefinitionEditScheduleRow,
	op *types.TaskDefinitionEditOperation,
	p types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	if op.Fence == math.MaxInt64 || op.Attempt == math.MaxInt32 {
		return nil, taskDefinitionEditConflict("operation fence or attempt is exhausted")
	}
	oldFence := op.Fence
	newFence := oldFence + 1
	var updated types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`UPDATE task_definition_edit_operations
		    SET lease_owner=$7,
		        lease_until=clock_timestamp()+($8*interval '1 microsecond'),
		        takeover_not_before=clock_timestamp()+($9*interval '1 microsecond'),
		        fence=fence+1, attempt=attempt+1, updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$10 AND tombstoned_at IS NULL AND fence=$11
		    AND takeover_not_before <= clock_timestamp()
		    AND fence < $12 AND attempt < $13
		  RETURNING `+taskDefinitionEditOperationColumns,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, p.LeaseOwner, p.LeaseDuration.Microseconds(),
		(p.LeaseDuration+taskDefinitionEditTakeoverSafetyGrace).Microseconds(),
		types.TaskDefinitionEditOperationStatusExecuting, oldFence,
		int64(math.MaxInt64), int32(math.MaxInt32),
	), &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditBusy()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("take over operation", err)
	}
	if op.Phase != types.TaskDefinitionEditPhaseProposalSealed {
		if schedule == nil || schedule.OperationID == nil ||
			*schedule.OperationID != op.ID || schedule.Fence == nil ||
			*schedule.Fence != oldFence {
			return nil, taskDefinitionEditConflict("schedule marker changed during takeover")
		}
		tag, err := tx.Exec(ctx,
			`UPDATE schedules
			    SET definition_edit_fence=$4, updated_at=clock_timestamp()
			  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
			    AND status=$5 AND definition_edit_operation_id=$6
			    AND definition_edit_fence=$7`,
			op.TargetTenantID, op.TargetUserID, op.TaskID, newFence,
			types.ScheduleStatusPaused, op.ID, oldFence)
		if err != nil {
			return nil, taskDefinitionEditDatabaseError("advance schedule marker fence", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, taskDefinitionEditConflict("schedule marker changed during takeover")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit operation takeover", err)
	}
	return cloneTaskDefinitionEditOperation(&updated), nil
}

// RenewTaskDefinitionEditLease extends only an unexpired exact lease. An
// expired owner must wait for ordinary takeover and receive a fresh fence.
func (s *Store) RenewTaskDefinitionEditLease(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	duration time.Duration,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	if duration <= 0 || duration > maxTaskDefinitionEditLease ||
		duration.Microseconds() <= 0 {
		return taskDefinitionEditValidation("lease duration is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin lease renewal", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	if assessment := assessTaskDefinitionEditSchedule(op, schedule); assessment != taskDefinitionEditScheduleExact {
		_, terminateErr := s.terminateAcquiredTaskDefinitionEditAssessment(
			ctx, tx, op, assessment)
		return terminateErr
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_definition_edit_operations
		    SET lease_until=clock_timestamp()+($7*interval '1 microsecond'),
		        takeover_not_before=clock_timestamp()+($8*interval '1 microsecond'),
		        updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$9 AND tombstoned_at IS NULL
		    AND lease_owner=$10 AND fence=$11
		    AND lease_until > clock_timestamp()`,
		lease.ID, lease.TenantID, lease.UserID, lease.TargetTenantID,
		lease.TargetUserID, lease.TaskID, duration.Microseconds(),
		(duration + taskDefinitionEditTakeoverSafetyGrace).Microseconds(),
		types.TaskDefinitionEditOperationStatusExecuting,
		lease.LeaseOwner, lease.Fence)
	if err != nil {
		return taskDefinitionEditDatabaseError("renew operation lease", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit lease renewal", err)
	}
	return nil
}

// ListStaleTaskDefinitionEditTenantIDs is the sole cross-tenant discovery
// exception. It runs in an explicit read-only owner transaction and exposes
// only tenant IDs; all row reads and every mutation are tenant-scoped below.
func (s *Store) ListStaleTaskDefinitionEditTenantIDs(
	ctx context.Context,
	before time.Time,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 || limit > 1000 {
		return nil, taskDefinitionEditValidation("stale tenant query is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin stale tenant discovery", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	rows, err := tx.Query(ctx,
		`SELECT DISTINCT tenant_id
		   FROM task_definition_edit_operations
		  WHERE status=$1 AND tombstoned_at IS NULL AND tenant_id>$3
		    AND lease_owner<>'' AND fence>0 AND attempt>0
		    AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL
		    AND lease_until <= clock_timestamp()
		    AND takeover_not_before <= LEAST($2, clock_timestamp())
		  ORDER BY tenant_id
		  LIMIT $4`, types.TaskDefinitionEditOperationStatusExecuting,
		before, afterTenantID, limit)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("list stale tenant shards", err)
	}
	tenantIDs := make([]int64, 0)
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			rows.Close()
			return nil, taskDefinitionEditDatabaseError("scan stale tenant shard", err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, taskDefinitionEditDatabaseError("iterate stale tenant shards", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit stale tenant discovery", err)
	}
	return tenantIDs, nil
}

// ListStaleTaskDefinitionEditOperations returns bounded recoverable rows for
// one tenant, including suspended tenants. Membership state is irrelevant.
func (s *Store) ListStaleTaskDefinitionEditOperations(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]types.TaskDefinitionEditOperation, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, taskDefinitionEditValidation("stale operation query is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, tenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin stale operation scan", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	rows, err := tx.Query(ctx,
		`SELECT `+taskDefinitionEditOperationColumns+`
		   FROM task_definition_edit_operations
		  WHERE tenant_id=$1 AND status=$2 AND tombstoned_at IS NULL
		    AND lease_owner<>'' AND fence>0 AND attempt>0
		    AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL
		    AND lease_until <= clock_timestamp()
		    AND takeover_not_before <= LEAST($3, clock_timestamp())
		  ORDER BY takeover_not_before, id
		  LIMIT $4`, tenantID, types.TaskDefinitionEditOperationStatusExecuting,
		before, limit)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("list stale operations", err)
	}
	operations := make([]types.TaskDefinitionEditOperation, 0)
	for rows.Next() {
		var op types.TaskDefinitionEditOperation
		if err := scanTaskDefinitionEditOperation(rows, &op); err != nil {
			rows.Close()
			return nil, taskDefinitionEditDatabaseError("scan stale operation", err)
		}
		operations = append(operations, *cloneTaskDefinitionEditOperation(&op))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, taskDefinitionEditDatabaseError("iterate stale operations", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit stale operation scan", err)
	}
	return operations, nil
}

// QuiesceTaskDefinitionEdit atomically pauses the PostgreSQL mirror, installs
// the operation/fence marker, and records db_quiesced. It performs no Temporal
// call and is deliberately separate from confirmation/acquisition.
func (s *Store) QuiesceTaskDefinitionEdit(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin database quiesce", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	if op.Phase == types.TaskDefinitionEditPhaseDBQuiesced {
		if assessTaskDefinitionEditSchedule(op, schedule) !=
			taskDefinitionEditScheduleExact {
			return taskDefinitionEditConflict("database quiesce replay differs")
		}
		return nil
	}
	if op.Phase != types.TaskDefinitionEditPhaseProposalSealed {
		return taskDefinitionEditConflict("database quiesce phase is invalid")
	}
	if assessment := assessTaskDefinitionEditSchedule(op, schedule); assessment != taskDefinitionEditScheduleExact {
		_, terminateErr := s.terminateAcquiredTaskDefinitionEditAssessment(
			ctx, tx, op, assessment)
		return terminateErr
	}
	tag, err := tx.Exec(ctx,
		`UPDATE schedules
		    SET status=$7, definition_edit_operation_id=$8,
		        definition_edit_fence=$9, updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		    AND status=$4 AND execution_mode=$5
		    AND approved_definition_version=$6
		    AND approved_definition_digest=$10
		    AND definition_edit_operation_id IS NULL
		    AND definition_edit_fence IS NULL`,
		op.TargetTenantID, op.TargetUserID, op.TaskID, op.OriginalStatus,
		types.ExecutionModeCompiled, op.BaseDefinitionVersion,
		types.ScheduleStatusPaused, op.ID, op.Fence,
		op.BaseDefinitionDigest)
	if err != nil {
		return taskDefinitionEditDatabaseError("install database quiesce marker", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditConflict("schedule changed during database quiesce")
	}
	tag, err = tx.Exec(ctx,
		`UPDATE task_definition_edit_operations
		    SET phase=$7, updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$8 AND phase=$9 AND tombstoned_at IS NULL
		    AND lease_owner=$10 AND fence=$11
		    AND lease_until > clock_timestamp()`,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditOperationStatusExecuting,
		types.TaskDefinitionEditPhaseProposalSealed,
		lease.LeaseOwner, lease.Fence)
	if err != nil {
		return taskDefinitionEditDatabaseError("checkpoint database quiesce", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit database quiesce", err)
	}
	return nil
}

// AuthorizeTaskDefinitionEditRemotePhase is the independent, short
// transaction required immediately before one raw Temporal RPC.
func (s *Store) AuthorizeTaskDefinitionEditRemotePhase(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	expectedPhase types.TaskDefinitionEditPhase,
) (*types.TaskDefinitionEditOperation, error) {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return nil, err
	}
	switch expectedPhase {
	case types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditPhaseTemporalTargetApplied:
	default:
		return nil, taskDefinitionEditValidation("remote authorization phase is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin remote authorization", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return nil, err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperation(ctx, tx, lease)
	if err != nil {
		return nil, err
	}
	if op.Phase != expectedPhase {
		return nil, taskDefinitionEditConflict("remote authorization phase changed")
	}
	if assessment := assessTaskDefinitionEditSchedule(op, schedule); assessment != taskDefinitionEditScheduleExact {
		terminated, terminateErr := s.terminateAcquiredTaskDefinitionEditAssessment(
			ctx, tx, op, assessment)
		return terminated, terminateErr
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit remote authorization", err)
	}
	return cloneTaskDefinitionEditOperation(op), nil
}

type taskDefinitionEditSnapshotCheckpoint struct {
	from   types.TaskDefinitionEditPhase
	to     types.TaskDefinitionEditPhase
	active definitioneditwire.SnapshotPhaseV1
	paused definitioneditwire.SnapshotPhaseV1
	get    func(*types.TaskDefinitionEditOperation) ([]byte, string)
	update string
}

// CheckpointTaskDefinitionEditBasePaused freezes the exact post-pause (or,
// for an originally paused task, reverified base-original) observation.
func (s *Store) CheckpointTaskDefinitionEditBasePaused(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
) error {
	return s.checkpointTaskDefinitionEditSnapshot(ctx, lease, snapshot,
		taskDefinitionEditSnapshotCheckpoint{
			from:   types.TaskDefinitionEditPhaseDBQuiesced,
			to:     types.TaskDefinitionEditPhaseTemporalBasePaused,
			active: definitioneditwire.SnapshotPhaseBasePaused,
			paused: definitioneditwire.SnapshotPhaseBaseOriginal,
			get: func(op *types.TaskDefinitionEditOperation) ([]byte, string) {
				return op.PauseSnapshot, op.PauseSnapshotDigest
			},
			update: `UPDATE task_definition_edit_operations
			             SET pause_snapshot=$7, pause_snapshot_digest=$8,
			                 phase=$9, updated_at=clock_timestamp()
			           WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			             AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
			             AND status=$10 AND phase=$11 AND tombstoned_at IS NULL
			             AND lease_owner=$12 AND fence=$13
			             AND lease_until > clock_timestamp()
			             AND pause_snapshot IS NULL AND pause_snapshot_digest=''`,
		})
}

// CheckpointTaskDefinitionEditTargetApplied freezes target-paused for an
// originally active task and target-final for an originally paused task.
func (s *Store) CheckpointTaskDefinitionEditTargetApplied(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
) error {
	return s.checkpointTaskDefinitionEditSnapshot(ctx, lease, snapshot,
		taskDefinitionEditSnapshotCheckpoint{
			from:   types.TaskDefinitionEditPhaseDefinitionCommitted,
			to:     types.TaskDefinitionEditPhaseTemporalTargetApplied,
			active: definitioneditwire.SnapshotPhaseTargetPaused,
			paused: definitioneditwire.SnapshotPhaseTargetFinal,
			get: func(op *types.TaskDefinitionEditOperation) ([]byte, string) {
				return op.ApplySnapshot, op.ApplySnapshotDigest
			},
			update: `UPDATE task_definition_edit_operations
			             SET apply_snapshot=$7, apply_snapshot_digest=$8,
			                 phase=$9, updated_at=clock_timestamp()
			           WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			             AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
			             AND status=$10 AND phase=$11 AND tombstoned_at IS NULL
			             AND lease_owner=$12 AND fence=$13
			             AND lease_until > clock_timestamp()
			             AND apply_snapshot IS NULL AND apply_snapshot_digest=''`,
		})
}

// CheckpointTaskDefinitionEditTargetRestored freezes exact target-final before
// PostgreSQL is allowed to restore the original active/paused status.
func (s *Store) CheckpointTaskDefinitionEditTargetRestored(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
) error {
	return s.checkpointTaskDefinitionEditSnapshot(ctx, lease, snapshot,
		taskDefinitionEditSnapshotCheckpoint{
			from:   types.TaskDefinitionEditPhaseTemporalTargetApplied,
			to:     types.TaskDefinitionEditPhaseTemporalTargetRestored,
			active: definitioneditwire.SnapshotPhaseTargetFinal,
			paused: definitioneditwire.SnapshotPhaseTargetFinal,
			get: func(op *types.TaskDefinitionEditOperation) ([]byte, string) {
				return op.RestoreSnapshot, op.RestoreSnapshotDigest
			},
			update: `UPDATE task_definition_edit_operations
			             SET restore_snapshot=$7, restore_snapshot_digest=$8,
			                 phase=$9, updated_at=clock_timestamp()
			           WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			             AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
			             AND status=$10 AND phase=$11 AND tombstoned_at IS NULL
			             AND lease_owner=$12 AND fence=$13
			             AND lease_until > clock_timestamp()
			             AND restore_snapshot IS NULL AND restore_snapshot_digest=''`,
		})
}

func (s *Store) checkpointTaskDefinitionEditSnapshot(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	snapshot []byte,
	cp taskDefinitionEditSnapshotCheckpoint,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	if len(snapshot) == 0 {
		return taskDefinitionEditValidation("phase snapshot is empty")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin phase checkpoint", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	existing, existingDigest := cp.get(op)
	digest := sha256HexTaskDefinitionEdit(snapshot)
	decoded, decodeErr := definitioneditwire.DecodePhaseSnapshotBytes(
		op.PreparedEdit, snapshot)
	expectedSnapshotPhase := cp.active
	if op.OriginalStatus == types.ScheduleStatusPaused {
		expectedSnapshotPhase = cp.paused
	}
	if op.Phase == cp.to {
		if !bytes.Equal(existing, snapshot) || existingDigest != digest ||
			assessTaskDefinitionEditSchedule(op, schedule) !=
				taskDefinitionEditScheduleExact {
			return taskDefinitionEditConflict("phase checkpoint replay differs")
		}
		if decodeErr != nil || decoded.Phase != expectedSnapshotPhase {
			return s.blockInvalidTaskDefinitionEditCheckpoint(ctx, tx, op, lease)
		}
		return nil
	}
	if op.Phase != cp.from || len(existing) != 0 || existingDigest != "" {
		return taskDefinitionEditConflict("phase checkpoint transition is invalid")
	}
	if assessment := assessTaskDefinitionEditSchedule(op, schedule); assessment != taskDefinitionEditScheduleExact {
		_, terminateErr := s.terminateAcquiredTaskDefinitionEditAssessment(
			ctx, tx, op, assessment)
		return terminateErr
	}
	if decodeErr != nil || decoded.Phase != expectedSnapshotPhase {
		return s.blockInvalidTaskDefinitionEditCheckpoint(ctx, tx, op, lease)
	}
	tag, err := tx.Exec(ctx, cp.update,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, snapshot, digest, cp.to,
		types.TaskDefinitionEditOperationStatusExecuting, cp.from,
		lease.LeaseOwner, lease.Fence)
	if err != nil {
		return taskDefinitionEditDatabaseError("write phase checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit phase checkpoint", err)
	}
	return nil
}

func (s *Store) blockInvalidTaskDefinitionEditCheckpoint(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	lease types.TaskDefinitionEditLease,
) error {
	message, _ := taskDefinitionEditBlockText(
		types.TaskDefinitionEditBlockCheckpointInvalid)
	if _, err := terminateTaskDefinitionEditTx(ctx, tx, op,
		types.TaskDefinitionEditOperationStatusBlocked,
		string(types.TaskDefinitionEditBlockCheckpointInvalid),
		message, nil, false, &lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit invalid checkpoint block", err)
	}
	return taskDefinitionEditTerminal()
}

func validateLoadedTaskDefinitionEditLease(
	op *types.TaskDefinitionEditOperation,
	databaseNow time.Time,
	lease types.TaskDefinitionEditLease,
) error {
	if op == nil || !taskDefinitionEditScopeMatches(op,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		}) {
		return taskDefinitionEditNotFound()
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) || op.TombstonedAt != nil {
		return taskDefinitionEditTerminal()
	}
	if op.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence ||
		op.LeaseUntil == nil || op.TakeoverNotBefore == nil ||
		!databaseNow.Before(*op.LeaseUntil) {
		return taskDefinitionEditLeaseLost()
	}
	return nil
}

func taskDefinitionEditBlockText(
	reason types.TaskDefinitionEditBlockReason,
) (string, bool) {
	switch reason {
	case types.TaskDefinitionEditBlockScheduleDeleted:
		return "The task was deleted while the edit was in progress.", true
	case types.TaskDefinitionEditBlockTemporalNotFound:
		return "The Temporal schedule no longer exists.", true
	case types.TaskDefinitionEditBlockUnsafeRemoteState:
		return "The remote schedule state is unsafe for this edit.", true
	case types.TaskDefinitionEditBlockCheckpointInvalid:
		return "A durable edit checkpoint failed validation.", true
	default:
		return "", false
	}
}

func taskDefinitionEditMarkerShapeExact(
	op *types.TaskDefinitionEditOperation,
	schedule *taskDefinitionEditScheduleRow,
) bool {
	if schedule == nil {
		return false
	}
	if op.Phase == types.TaskDefinitionEditPhaseProposalSealed {
		return schedule.Status == op.OriginalStatus &&
			schedule.OperationID == nil && schedule.Fence == nil
	}
	return schedule.Status == types.ScheduleStatusPaused &&
		schedule.OperationID != nil && *schedule.OperationID == op.ID &&
		schedule.Fence != nil && *schedule.Fence == op.Fence
}

func terminateTaskDefinitionEditTx(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
	status types.TaskDefinitionEditOperationStatus,
	errorCode, errorMessage string,
	result json.RawMessage,
	completed bool,
	lease *types.TaskDefinitionEditLease,
) (*types.TaskDefinitionEditOperation, error) {
	if op == nil || op.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		op.TombstonedAt != nil || op.Fence <= 0 || op.Attempt <= 0 {
		return nil, taskDefinitionEditConflict("operation cannot enter terminal state")
	}
	if completed {
		if status != types.TaskDefinitionEditOperationStatusCompleted ||
			op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
			len(result) == 0 || errorCode != "" || errorMessage != "" {
			return nil, taskDefinitionEditConflict("completed terminal state is invalid")
		}
	} else if (status != types.TaskDefinitionEditOperationStatusBlocked &&
		status != types.TaskDefinitionEditOperationStatusSuperseded) ||
		errorCode == "" || errorMessage == "" || len(result) != 0 {
		return nil, taskDefinitionEditConflict("failed terminal state is invalid")
	}
	leaseRequired := lease != nil
	leaseOwner := ""
	if leaseRequired {
		if err := validateTaskDefinitionEditLease(*lease); err != nil {
			return nil, err
		}
		if !taskDefinitionEditScopeMatches(op, types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		}) || op.LeaseOwner != lease.LeaseOwner || op.Fence != lease.Fence {
			return nil, taskDefinitionEditLeaseLost()
		}
		leaseOwner = lease.LeaseOwner
	}
	var updated types.TaskDefinitionEditOperation
	err := scanTaskDefinitionEditOperation(tx.QueryRow(ctx,
		`UPDATE task_definition_edit_operations
		    SET status=$7, result=$8, error_code=$9, error_message=$10,
		        lease_owner='', lease_until=NULL, takeover_not_before=NULL,
		        tombstoned_at=clock_timestamp(), updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$11 AND phase=$12 AND tombstoned_at IS NULL
		    AND fence=$13 AND attempt=$14
		    AND ($15::boolean = false OR
		         (lease_owner=$16 AND lease_until > clock_timestamp()))
		  RETURNING `+taskDefinitionEditOperationColumns,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, status, result, errorCode, errorMessage,
		types.TaskDefinitionEditOperationStatusExecuting, op.Phase,
		op.Fence, op.Attempt, leaseRequired, leaseOwner,
	), &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		if leaseRequired {
			return nil, taskDefinitionEditLeaseLost()
		}
		return nil, taskDefinitionEditConflict("operation changed during terminal transition")
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("write operation tombstone", err)
	}
	if err := insertTaskDefinitionEditReceiptForTerminal(
		ctx, tx, updated.ID, updated.TenantID, updated.UserID); err != nil {
		return nil, err
	}
	return &updated, nil
}

// BlockTaskDefinitionEditOperation records a fixed non-sensitive quarantine
// reason while retaining the last proven progress phase and database marker.
func (s *Store) BlockTaskDefinitionEditOperation(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	reason types.TaskDefinitionEditBlockReason,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	errorMessage, ok := taskDefinitionEditBlockText(reason)
	if !ok {
		return taskDefinitionEditValidation("block reason is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin operation block", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, err := loadTaskDefinitionEditOperationForUpdate(ctx, tx,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		})
	if err != nil {
		return err
	}
	if op.Status == types.TaskDefinitionEditOperationStatusBlocked {
		if op.Fence != lease.Fence || op.ErrorCode != string(reason) ||
			op.ErrorMessage != errorMessage || op.TombstonedAt == nil ||
			op.LeaseOwner != "" || op.LeaseUntil != nil ||
			op.TakeoverNotBefore != nil || len(op.Result) != 0 {
			return taskDefinitionEditConflict("blocked operation replay differs")
		}
		return verifyTaskDefinitionEditReceiptForTerminal(
			ctx, tx, op.ID, op.TenantID, op.UserID)
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) {
		return taskDefinitionEditTerminal()
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateLoadedTaskDefinitionEditLease(op, databaseNow, lease); err != nil {
		return err
	}
	if !taskDefinitionEditBlockedScheduleExact(op, schedule, reason) {
		return taskDefinitionEditConflict("database state does not support the block reason")
	}
	if _, err := terminateTaskDefinitionEditTx(ctx, tx, op,
		types.TaskDefinitionEditOperationStatusBlocked,
		string(reason), errorMessage, nil, false, &lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit operation block", err)
	}
	return nil
}

func taskDefinitionEditBlockedScheduleExact(
	op *types.TaskDefinitionEditOperation,
	schedule *taskDefinitionEditScheduleRow,
	reason types.TaskDefinitionEditBlockReason,
) bool {
	if reason == types.TaskDefinitionEditBlockScheduleDeleted {
		return schedule == nil
	}
	return assessTaskDefinitionEditSchedule(op, schedule) ==
		taskDefinitionEditScheduleExact
}

// SupersedeTaskDefinitionEditOperation is available only when the locked
// PostgreSQL head itself proves that a version beyond this operation's target
// has won. Historical Approved rows are not sufficient proof.
func (s *Store) SupersedeTaskDefinitionEditOperation(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin operation supersede", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, err := loadTaskDefinitionEditOperationForUpdate(ctx, tx,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		})
	if err != nil {
		return err
	}
	if op.Status == types.TaskDefinitionEditOperationStatusSuperseded {
		if op.Fence != lease.Fence || op.ErrorCode != "definition_superseded" ||
			op.ErrorMessage != "A newer approved definition already exists." ||
			op.TombstonedAt == nil || op.LeaseOwner != "" ||
			op.LeaseUntil != nil || op.TakeoverNotBefore != nil || len(op.Result) != 0 {
			return taskDefinitionEditConflict("superseded operation replay differs")
		}
		return verifyTaskDefinitionEditReceiptForTerminal(
			ctx, tx, op.ID, op.TenantID, op.UserID)
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) {
		return taskDefinitionEditTerminal()
	}
	if schedule == nil || schedule.Version == nil ||
		*schedule.Version <= op.TargetDefinitionVersion ||
		!taskDefinitionEditMarkerShapeExact(op, schedule) {
		return taskDefinitionEditConflict("current head does not prove supersession")
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateLoadedTaskDefinitionEditLease(op, databaseNow, lease); err != nil {
		return err
	}
	if _, err := terminateTaskDefinitionEditTx(ctx, tx, op,
		types.TaskDefinitionEditOperationStatusSuperseded,
		"definition_superseded", "A newer approved definition already exists.",
		nil, false, &lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit operation supersede", err)
	}
	return nil
}

func canonicalTaskDefinitionEditResult(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxTaskDefinitionEditResultBytes || !utf8.Valid(raw) {
		return nil, taskDefinitionEditValidation("terminal result is invalid")
	}
	var object map[string]json.RawMessage
	if err := strictjson.DecodeExact(raw, &object); err != nil || object == nil {
		return nil, taskDefinitionEditValidation("terminal result must be a strict JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil || len(canonical) == 0 || len(canonical) > maxTaskDefinitionEditResultBytes {
		return nil, taskDefinitionEditValidation("terminal result cannot be canonicalized")
	}
	return canonical, nil
}

// CompleteTaskDefinitionEditOperation restores the frozen database status,
// clears the marker, and writes the operation tombstone plus receipt atomically.
func (s *Store) CompleteTaskDefinitionEditOperation(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
	result json.RawMessage,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	canonicalResult, err := canonicalTaskDefinitionEditResult(result)
	if err != nil {
		return err
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin operation completion", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, err := loadTaskDefinitionEditOperationForUpdate(ctx, tx,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		})
	if err != nil {
		return err
	}
	if op.Status == types.TaskDefinitionEditOperationStatusCompleted {
		storedResult, storedResultErr := canonicalTaskDefinitionEditResult(op.Result)
		if op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
			op.Fence != lease.Fence || op.TombstonedAt == nil ||
			op.LeaseOwner != "" || op.LeaseUntil != nil ||
			op.TakeoverNotBefore != nil ||
			storedResultErr != nil || !bytes.Equal(storedResult, canonicalResult) {
			return taskDefinitionEditConflict("completed operation replay differs")
		}
		return verifyTaskDefinitionEditReceiptForTerminal(
			ctx, tx, op.ID, op.TenantID, op.UserID)
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) {
		return taskDefinitionEditTerminal()
	}
	if op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
		assessTaskDefinitionEditSchedule(op, schedule) != taskDefinitionEditScheduleExact {
		return taskDefinitionEditConflict("operation is not ready for completion")
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateLoadedTaskDefinitionEditLease(op, databaseNow, lease); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE schedules
		    SET status=$4, definition_edit_operation_id=NULL,
		        definition_edit_fence=NULL, updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		    AND status=$5 AND definition_edit_operation_id=$6
		    AND definition_edit_fence=$7
		    AND approved_definition_version=$8
		    AND approved_definition_digest=$9`,
		op.TargetTenantID, op.TargetUserID, op.TaskID, op.OriginalStatus,
		types.ScheduleStatusPaused, op.ID, op.Fence,
		op.TargetDefinitionVersion, op.TargetDefinitionDigest)
	if err != nil {
		return taskDefinitionEditDatabaseError("restore completed schedule status", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditConflict("schedule changed during completion")
	}
	if _, err := terminateTaskDefinitionEditTx(ctx, tx, op,
		types.TaskDefinitionEditOperationStatusCompleted,
		"", "", canonicalResult, true, &lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit operation completion", err)
	}
	return nil
}

func taskDefinitionEditCompletedScheduleExact(
	op *types.TaskDefinitionEditOperation,
	schedule *taskDefinitionEditScheduleRow,
) bool {
	return schedule != nil && schedule.Status == op.OriginalStatus &&
		schedule.Mode == types.ExecutionModeCompiled &&
		schedule.Version != nil && *schedule.Version == op.TargetDefinitionVersion &&
		schedule.Digest != nil && subtle.ConstantTimeCompare(
		[]byte(*schedule.Digest), []byte(op.TargetDefinitionDigest)) == 1 &&
		schedule.OperationID == nil && schedule.Fence == nil
}

// CommitTaskDefinitionEditDefinition appends the operation-owned target bytes,
// advances every legacy projection and checkpoints definition_committed in one
// PostgreSQL transaction. It never calls the public historical-replay writer.
func (s *Store) CommitTaskDefinitionEditDefinition(
	ctx context.Context,
	lease types.TaskDefinitionEditLease,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, lease.TenantID)
	if err != nil {
		return taskDefinitionEditDatabaseError("begin definition commit", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(ctx, tx,
		lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	if taskDefinitionEditPhaseHasCommittedDefinition(op.Phase) {
		if assessTaskDefinitionEditSchedule(op, schedule) !=
			taskDefinitionEditScheduleExact {
			return taskDefinitionEditConflict("definition commit replay differs")
		}
		if err := verifyCommittedTaskDefinitionEditTx(ctx, tx, op); err != nil {
			return err
		}
		return nil
	}
	if op.Phase != types.TaskDefinitionEditPhaseTemporalBasePaused {
		return taskDefinitionEditConflict("definition commit phase is invalid")
	}
	if assessment := assessTaskDefinitionEditSchedule(op, schedule); assessment != taskDefinitionEditScheduleExact {
		_, terminateErr := s.terminateAcquiredTaskDefinitionEditAssessment(
			ctx, tx, op, assessment)
		return terminateErr
	}
	target, err := taskstate.DecodeApprovedDefinitionV1(op.TargetDefinition)
	if err != nil {
		return taskDefinitionEditIntegrity()
	}
	canonical, err := taskstate.EncodeApprovedDefinitionV1(target)
	if err != nil || !bytes.Equal(canonical, op.TargetDefinition) ||
		target.TenantID != op.TargetTenantID || target.UserID != op.TargetUserID ||
		target.TaskID != op.TaskID || target.ExecutionMode != types.ExecutionModeCompiled ||
		sha256HexTaskDefinitionEdit(canonical) != op.TargetDefinitionDigest {
		return taskDefinitionEditIntegrity()
	}
	command := approvedDefinitionEditCommand{
		expectedHead: ApprovedDefinitionFence{
			Version: op.BaseDefinitionVersion, Digest: op.BaseDefinitionDigest,
		},
		definition:  target,
		payload:     bytes.Clone(op.TargetDefinition),
		digest:      op.TargetDefinitionDigest,
		approvalRef: op.ApprovalRef,
	}
	head := taskDefinitionHead{
		Mode:    schedule.Mode,
		Version: cloneOptionalVersion(schedule.Version),
		Digest:  cloneOptionalTaskDefinitionEditString(schedule.Digest),
	}
	record, err := appendApprovedDefinitionEditTx(ctx, tx, head, command)
	if err != nil {
		return err
	}
	if record.Version != op.TargetDefinitionVersion ||
		record.Digest != op.TargetDefinitionDigest ||
		record.ApprovalRef != op.ApprovalRef ||
		!bytes.Equal(record.Payload, op.TargetDefinition) {
		return taskDefinitionEditIntegrity()
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_definition_edit_operations
		    SET phase=$7, updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		    AND status=$8 AND phase=$9 AND tombstoned_at IS NULL
		    AND lease_owner=$10 AND fence=$11
		    AND lease_until > clock_timestamp()`,
		op.ID, op.TenantID, op.UserID, op.TargetTenantID, op.TargetUserID,
		op.TaskID, types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditOperationStatusExecuting,
		types.TaskDefinitionEditPhaseTemporalBasePaused,
		lease.LeaseOwner, lease.Fence)
	if err != nil {
		return taskDefinitionEditDatabaseError("checkpoint definition commit", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit approved definition edit", err)
	}
	return nil
}

func taskDefinitionEditPhaseHasCommittedDefinition(
	phase types.TaskDefinitionEditPhase,
) bool {
	switch phase {
	case types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditPhaseTemporalTargetApplied,
		types.TaskDefinitionEditPhaseTemporalTargetRestored:
		return true
	default:
		return false
	}
}

func verifyCommittedTaskDefinitionEditTx(
	ctx context.Context,
	tx pgx.Tx,
	op *types.TaskDefinitionEditOperation,
) error {
	record, err := loadApprovedDefinitionVersionTx(ctx, tx,
		op.TargetTenantID, op.TargetUserID, op.TaskID,
		op.TargetDefinitionVersion)
	if err != nil {
		return err
	}
	if record.Digest != op.TargetDefinitionDigest ||
		record.ApprovalRef != op.ApprovalRef ||
		!bytes.Equal(record.Payload, op.TargetDefinition) {
		return taskDefinitionEditIntegrity()
	}
	return validateApprovedDefinitionProjectionTx(ctx, tx,
		record.Definition, record.Payload)
}

func cloneOptionalTaskDefinitionEditString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
