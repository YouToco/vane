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

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/types"
)

const (
	maxActiveTasksPerUser               = 20
	taskCreationCleanupMarkerVersion    = "vane.task-creation-cleanup/v1"
	taskCreationQuarantineMarkerVersion = "vane.task-creation-quarantine/v1"
)

type taskCreationCleanupMarker struct {
	Version           string `json:"version"`
	AggregateExpected bool   `json:"aggregate_expected"`
	// AggregateGeneration is PostgreSQL's system-generated row version (xmin)
	// captured while the exact owned paused schedule is locked. Callers cannot
	// provide or recreate it through INSERT, so a delete+replacement under the
	// same TaskID cannot be mistaken for the aggregate A5 committed.
	AggregateGeneration string `json:"aggregate_generation,omitempty"`
}

type taskCreationQuarantineMarker struct {
	Version                 string `json:"version"`
	ReservationRetained     bool   `json:"reservation_retained"`
	ActiveAggregateRetained bool   `json:"active_aggregate_retained"`
	TaskIDKnown             bool   `json:"task_id_known"`
	PrimaryErrorCode        string `json:"primary_error_code,omitempty"`
	PrimaryErrorMessage     string `json:"primary_error_message,omitempty"`
}

// CommitPausedCompiledTaskDefinitionForCreation atomically writes the A2
// paused aggregate and advances the exact fenced operation. A per-membership
// UPDATE lock serializes the capacity decision for one user; reservations in
// later executing phases close the 19 -> 21 concurrent-create race.
func (s *Store) CommitPausedCompiledTaskDefinitionForCreation(
	ctx context.Context,
	p types.CommitPausedCompiledTaskDefinitionForCreationParams,
) error {
	plan, err := validatePausedCompiledTaskDefinition(p.Definition)
	if err != nil {
		return err
	}
	usesToolInvocations, err := compiledPlanUsesToolInvocations(plan)
	if err != nil {
		return err
	}
	canonicalPlan, err := json.Marshal(plan)
	if err != nil {
		return taskCreationValidation("definition fetch plan cannot be canonicalized")
	}
	p.Definition.FetchPlan = canonicalPlan
	if err := validateTaskCreationLease(p.Lease); err != nil {
		return err
	}
	if !validSHA256Digest(p.CompiledDigest) || len(p.PreparedSchedule) == 0 ||
		len(p.EnsureReceipt) == 0 {
		return taskCreationValidation("definition commit evidence is incomplete")
	}
	if p.Definition.TenantID != p.Lease.TenantID ||
		p.Definition.UserID != p.Lease.UserID {
		return taskCreationValidation("definition scope differs from operation lease")
	}

	tx, err := s.beginTaskCreationTenantTx(ctx, p.Lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin definition commit", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, p.Lease)
	if err != nil {
		return err
	}
	// The durable operation is the root for every existing v1 creation saga.
	// Purge fences the same row before schedules, receipts, sessions, and the
	// final tenant delete. Capacity locks follow it and never participate in
	// tenant purge because users may be shared across tenants.
	if err := lockValidMembershipForTaskCreation(
		ctx, tx, p.Lease.TenantID, p.Lease.UserID); err != nil {
		return err
	}
	if err := validateTaskCreationDefinitionCommitBinding(op, p); err != nil {
		return err
	}
	protocol, err := taskCreationCheckpointDefinitionProtocol(op.CompiledDefinition)
	if err != nil {
		return taskCreationConflict("compiled checkpoint protocol is invalid")
	}
	useToolModel := protocol == compiledTaskDefinitionProtocolV2
	if useToolModel && !usesToolInvocations {
		return taskCreationConflict(
			"Source-free compiled checkpoint is missing complete Tool calls")
	}

	switch op.Phase {
	case types.TaskCreationPhaseDefinitionCommitted,
		types.TaskCreationPhaseActivationStarted,
		types.TaskCreationPhaseActivated:
		expectedStatus := types.ScheduleStatusPaused
		if op.Phase == types.TaskCreationPhaseActivated {
			expectedStatus = types.ScheduleStatusActive
		}
		var matches bool
		if useToolModel {
			matches, err = pausedToolTaskDefinitionMatches(
				ctx, tx, p.Definition, expectedStatus)
		} else {
			matches, err = pausedCompiledTaskDefinitionMatches(
				ctx, tx, p.Definition, plan, expectedStatus)
		}
		if err != nil {
			return err
		}
		if !matches {
			return taskCreationConflict("committed definition aggregate differs")
		}
		if useToolModel {
			matches, err = taskCreationToolApprovedDefinitionMatchesTx(
				ctx, tx, p.Definition, plan, p.Lease.ID)
		} else {
			matches, err = taskCreationApprovedDefinitionMatchesTx(
				ctx, tx, p.Definition, plan, p.Lease.ID)
		}
		if err != nil {
			return err
		}
		if !matches {
			return taskCreationConflict("committed approved definition differs")
		}
		return nil
	case types.TaskCreationPhaseScheduleEnsured:
		// Initial transition continues below.
	default:
		return taskCreationConflict("definition commit phase is invalid")
	}

	used, err := countScopedTaskCreationCapacity(
		ctx, tx, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		return err
	}
	if used >= maxActiveTasksPerUser {
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("task creation: active task limit %d reached", maxActiveTasksPerUser),
			types.ErrTaskCreationLimit)
	}

	if useToolModel {
		if err := insertPausedToolTaskDefinitionTx(ctx, tx, p.Definition); err != nil {
			return err
		}
		approved, err := insertTaskCreationToolApprovedDefinitionTx(
			ctx, tx, p.Definition, plan, p.Lease.ID)
		if err != nil {
			return err
		}
		if _, err := insertInitialToolAdaptiveStateTx(
			ctx, tx, approved); err != nil {
			return err
		}
	} else {
		if err := insertPausedCompiledTaskDefinitionTx(
			ctx, tx, p.Definition, plan); err != nil {
			return err
		}
		if _, err := insertTaskCreationApprovedDefinitionTx(
			ctx, tx, p.Definition, plan, p.Lease.ID); err != nil {
			return err
		}
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_creation_operations
		    SET task_id = $6, phase = $7, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $8
		    AND status = $9 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $10 AND task_id = $6
		    AND compiled_digest = $11
		    AND prepared_schedule = $12 AND ensure_receipt = $13`,
		p.Lease.ID, p.Lease.TenantID, p.Lease.UserID,
		p.Lease.LeaseOwner, p.Lease.Fence, p.Definition.TaskID,
		types.TaskCreationPhaseDefinitionCommitted,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseScheduleEnsured, p.CompiledDigest,
		p.PreparedSchedule, p.EnsureReceipt,
	)
	if err != nil {
		return taskCreationDatabaseError("advance definition commit phase", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit definition and aggregate", err)
	}
	return nil
}

// BeginTaskCreationActivation authorizes at most one initial Temporal Activate
// attempt. A response-lost replay returns started=false; recovery must Describe
// the deterministic schedule before deciding whether an external call remains.
func (s *Store) BeginTaskCreationActivation(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
) (bool, error) {
	if err := validateTaskCreationLease(lease); err != nil {
		return false, err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return false, err
	}

	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return false, taskCreationDatabaseError("begin activation authorization", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return false, err
	}
	if err := lockValidMembership(ctx, tx, lease.TenantID, lease.UserID); err != nil {
		return false, err
	}
	if op.TaskID != taskID || !taskCreationBindingStateComplete(op) {
		return false, taskCreationConflict("activation binding is incomplete")
	}
	row, found, err := loadCreationScheduleForUpdate(ctx, tx, taskID)
	if err != nil {
		return false, err
	}
	if !found || row.tenantID != lease.TenantID || row.userID != lease.UserID {
		return false, taskCreationConflict("activation target ownership differs")
	}
	if row.mode != types.ExecutionModeCompiled {
		return false, taskCreationConflict("activation target is not a compiled task")
	}
	switch op.Phase {
	case types.TaskCreationPhaseActivationStarted:
		if row.status != types.ScheduleStatusPaused {
			return false, taskCreationConflict("activation-started aggregate is not paused")
		}
		return false, nil
	case types.TaskCreationPhaseActivated:
		if row.status != types.ScheduleStatusActive {
			return false, taskCreationConflict("activated aggregate is not active")
		}
		return false, nil
	case types.TaskCreationPhaseDefinitionCommitted:
		if row.status != types.ScheduleStatusPaused {
			return false, taskCreationConflict("activation target is not paused")
		}
	default:
		return false, taskCreationConflict("activation authorization phase is invalid")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE task_creation_operations
		    SET phase = $6, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $7
		    AND status = $8 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $9 AND task_id = $10`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskCreationPhaseActivationStarted,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseDefinitionCommitted, taskID,
	)
	if err != nil {
		return false, taskCreationDatabaseError("write activation authorization", err)
	}
	if tag.RowsAffected() != 1 {
		return false, taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return false, taskCreationDatabaseError("commit activation authorization", err)
	}
	return true, nil
}

// CommitTaskCreationActivation mirrors Temporal activation and advances the
// saga in one transaction. The initial path accepts only the exact owned
// paused row; replay exact-adopts only an active row paired with phase=activated.
func (s *Store) CommitTaskCreationActivation(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return err
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin activation commit", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return err
	}
	if err := lockValidMembership(ctx, tx, lease.TenantID, lease.UserID); err != nil {
		return err
	}
	if op.TaskID != taskID || !taskCreationBindingStateComplete(op) {
		return taskCreationConflict("activation binding is incomplete")
	}
	row, found, err := loadCreationScheduleForUpdate(ctx, tx, taskID)
	if err != nil {
		return err
	}
	if !found || row.tenantID != lease.TenantID || row.userID != lease.UserID {
		return taskCreationConflict("activation target ownership differs")
	}
	if row.mode != types.ExecutionModeCompiled {
		return taskCreationConflict("activation target is not a compiled task")
	}
	if op.Phase == types.TaskCreationPhaseActivated {
		if row.status != types.ScheduleStatusActive {
			return taskCreationConflict("activated operation has non-active aggregate")
		}
		return nil
	}
	if op.Phase != types.TaskCreationPhaseActivationStarted ||
		row.status != types.ScheduleStatusPaused {
		return taskCreationConflict("activation commit state is invalid")
	}

	tag, err := tx.Exec(ctx,
		`UPDATE schedules
		    SET status = $4, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3 AND status = $5
		    AND execution_mode = $6`,
		taskID, lease.TenantID, lease.UserID,
		types.ScheduleStatusActive, types.ScheduleStatusPaused,
		types.ExecutionModeCompiled,
	)
	if err != nil {
		return taskCreationDatabaseError("activate schedule mirror", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationConflict("activation target changed")
	}
	tag, err = tx.Exec(ctx,
		`UPDATE task_creation_operations
		    SET phase = $6, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $7
		    AND status = $8 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $9 AND task_id = $10`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskCreationPhaseActivated,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseActivationStarted, taskID,
	)
	if err != nil {
		return taskCreationDatabaseError("advance activated phase", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit activation mirror", err)
	}
	return nil
}

// BlockTaskCreationOperationAfterSideEffect quarantines an operation whenever
// the outcome of an external side effect can no longer be proven safe. Unlike
// cleanup it deliberately preserves every external and database artifact for
// audit/reconciliation. It also retains a capacity reservation when the exact
// TaskID cannot be recovered, or when an activated aggregate cannot be safely
// finalized.
func (s *Store) BlockTaskCreationOperationAfterSideEffect(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	errorCode string,
	errorMessage string,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if taskID != "" {
		if err := validateTaskCreationTaskID(taskID); err != nil {
			return err
		}
	}
	if err := validateTaskCreationErrorMetadata(errorCode, errorMessage); err != nil {
		return err
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin side-effect quarantine", err)
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
		marker, markerOK := decodeTaskCreationQuarantineMarker(op.Result)
		if op.Status == types.TaskOperationStatusBlocked &&
			op.Phase == types.TaskCreationPhaseBlocked &&
			op.ErrorCode == errorCode && op.ErrorMessage == errorMessage &&
			op.TombstonedAt != nil && op.LeaseUntil == nil &&
			op.TakeoverNotBefore == nil && markerOK &&
			marker.TaskIDKnown == (taskID != "") &&
			(!marker.TaskIDKnown || op.TaskID == taskID) {
			return verifyTaskCreationReceiptForTerminal(
				ctx, tx, lease.ID, lease.TenantID, lease.UserID)
		}
		return taskCreationConflict("side-effect quarantine tombstone differs")
	}
	if op.Status != types.TaskOperationStatusExecuting || op.LeaseUntil == nil ||
		op.TakeoverNotBefore == nil || !databaseNow.Before(*op.LeaseUntil) {
		return taskCreationLeaseLost()
	}
	activeAggregateRetained := false
	switch op.Phase {
	case types.TaskCreationPhaseSchedulePrepared,
		types.TaskCreationPhaseScheduleEnsured,
		types.TaskCreationPhaseDefinitionCommitted,
		types.TaskCreationPhaseActivationStarted:
		if op.ErrorCode != "" || op.ErrorMessage != "" {
			return taskCreationConflict("side-effect quarantine reason was already set")
		}
	case types.TaskCreationPhaseCleanupPending:
		if op.ErrorCode == "" {
			return taskCreationConflict("side-effect quarantine has no primary cleanup failure")
		}
	case types.TaskCreationPhaseActivated:
		row, found, err := loadCreationScheduleForUpdate(ctx, tx, op.TaskID)
		if err != nil {
			return err
		}
		if found && row.tenantID == lease.TenantID && row.userID == lease.UserID &&
			row.mode != types.ExecutionModeCompiled {
			return taskCreationConflict("activated aggregate is not a compiled task")
		}
		if found && row.tenantID == lease.TenantID && row.userID == lease.UserID &&
			row.status == types.ScheduleStatusActive {
			// The DB mirror itself already occupies the user's capacity slot. The
			// quarantine reservation uses the same known TaskID, so UNION de-duplicates
			// them while still retaining capacity if the mirror later disappears.
			activeAggregateRetained = true
		}
	default:
		return taskCreationConflict("side-effect quarantine phase is unsafe")
	}
	expectedTaskID, taskIDKnown := taskCreationQuarantineTaskID(op)
	if taskIDKnown {
		if taskID != expectedTaskID {
			return taskCreationConflict("side-effect quarantine task binding differs")
		}
	} else if taskID != "" {
		return taskCreationConflict("side-effect quarantine task id cannot be proven")
	}
	marker := taskCreationQuarantineMarker{
		Version:                 taskCreationQuarantineMarkerVersion,
		ReservationRetained:     true,
		ActiveAggregateRetained: activeAggregateRetained,
		TaskIDKnown:             taskIDKnown,
	}
	if op.Phase == types.TaskCreationPhaseCleanupPending {
		marker.PrimaryErrorCode = op.ErrorCode
		marker.PrimaryErrorMessage = op.ErrorMessage
	}
	quarantineMarker, err := json.Marshal(marker)
	if err != nil {
		return taskCreationDatabaseError("encode side-effect quarantine marker", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_creation_operations
		    SET status = $6, phase = $7, error_code = $8, error_message = $9,
		        result = $10, executed_at = NULL,
		        task_id = CASE WHEN $14 <> '' THEN $14 ELSE task_id END,
		        lease_until = NULL, takeover_not_before = NULL,
		        tombstoned_at = clock_timestamp(), updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $11
		    AND status = $12 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $13 AND task_id = $15`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskOperationStatusBlocked, types.TaskCreationPhaseBlocked,
		errorCode, errorMessage, quarantineMarker, types.TaskCreationExecutionVersionV1,
		types.TaskOperationStatusExecuting, op.Phase, taskID, op.TaskID,
	)
	if err != nil {
		return taskCreationDatabaseError("write side-effect quarantine", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := insertTaskCreationReceiptForTerminal(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit side-effect quarantine", err)
	}
	return nil
}

// BeginTaskCreationCleanup records the immutable normalized failure before an
// external Temporal delete. A prepared state may enter cleanup using its
// immutable prepared TaskID because the Ensure RPC response may have been lost;
// activated or corrupt ownership is deliberately refused.
func (s *Store) BeginTaskCreationCleanup(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	errorCode string,
	errorMessage string,
) (bool, error) {
	if err := validateTaskCreationLease(lease); err != nil {
		return false, err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return false, err
	}
	if err := validateTaskCreationErrorMetadata(errorCode, errorMessage); err != nil {
		return false, err
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return false, taskCreationDatabaseError("begin cleanup checkpoint", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	op, _, err := loadLeasedTaskCreationOperation(ctx, tx, lease)
	if err != nil {
		return false, err
	}
	if op.Phase == types.TaskCreationPhaseCleanupPending {
		if op.TaskID != taskID || !taskCreationCleanupBindingStateComplete(op, taskID) {
			return false, taskCreationConflict("cleanup binding is incomplete")
		}
		if op.ErrorCode != errorCode || op.ErrorMessage != errorMessage {
			return false, taskCreationConflict("cleanup reason differs")
		}
		marker, err := decodeTaskCreationCleanupMarker(op.Result)
		if err != nil {
			return false, taskCreationConflict("cleanup ownership marker is invalid")
		}
		row, found, err := loadCreationScheduleForUpdate(ctx, tx, taskID)
		if err != nil {
			return false, err
		}
		if err := validateCleanupAggregateState(marker, row, found, lease); err != nil {
			return false, err
		}
		return false, nil
	}
	switch op.Phase {
	case types.TaskCreationPhaseSchedulePrepared:
		if op.TaskID != "" || len(op.EnsureReceipt) != 0 ||
			!taskCreationPreparedBindingStateComplete(op, taskID) {
			return false, taskCreationConflict("pre-ensure cleanup binding is incomplete")
		}
	case types.TaskCreationPhaseScheduleEnsured,
		types.TaskCreationPhaseDefinitionCommitted,
		types.TaskCreationPhaseActivationStarted:
		if op.TaskID != taskID || !taskCreationBindingStateComplete(op) {
			return false, taskCreationConflict("cleanup binding is incomplete")
		}
	default:
		return false, taskCreationConflict("cleanup phase is unsafe")
	}
	row, found, err := loadCreationScheduleForUpdate(ctx, tx, taskID)
	if err != nil {
		return false, err
	}
	aggregateExpected := op.Phase == types.TaskCreationPhaseDefinitionCommitted ||
		op.Phase == types.TaskCreationPhaseActivationStarted
	marker := taskCreationCleanupMarker{
		Version: taskCreationCleanupMarkerVersion, AggregateExpected: aggregateExpected,
	}
	if aggregateExpected {
		marker.AggregateGeneration = row.generation
	}
	if err := validateCleanupAggregateState(marker, row, found, lease); err != nil {
		return false, err
	}
	markerJSON, err := json.Marshal(marker)
	if err != nil {
		return false, taskCreationDatabaseError("encode cleanup ownership marker", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_creation_operations
		    SET phase = $6, error_code = $7, error_message = $8,
		        result = $9, task_id = $13, updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $10
		    AND status = $11 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $12 AND (task_id = '' OR task_id = $13)
		    AND error_code = '' AND error_message = '' AND result IS NULL`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		types.TaskCreationPhaseCleanupPending, errorCode, errorMessage,
		markerJSON,
		types.TaskCreationExecutionVersionV1, types.TaskOperationStatusExecuting,
		op.Phase, taskID,
	)
	if err != nil {
		return false, taskCreationDatabaseError("write cleanup checkpoint", err)
	}
	if tag.RowsAffected() != 1 {
		return false, taskCreationLeaseLost()
	}
	if err := tx.Commit(ctx); err != nil {
		return false, taskCreationDatabaseError("commit cleanup checkpoint", err)
	}
	return true, nil
}

// FinishTaskCreationCleanup removes only the exact owned paused aggregate and
// writes the failure tombstone in the same transaction. The cleanup checkpoint
// remembers whether A5 had atomically committed its aggregate before the
// external delete. A row appearing later under the same TaskID is never adopted
// or deleted merely because its scope and paused status happen to match.
func (s *Store) FinishTaskCreationCleanup(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	terminalStatus types.TaskOperationStatus,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateTaskCreationTaskID(taskID); err != nil {
		return err
	}
	terminalPhase, err := cleanupTerminalPhase(terminalStatus)
	if err != nil {
		return err
	}
	tx, err := s.beginTaskCreationTenantTx(ctx, lease.TenantID, pgx.TxOptions{})
	if err != nil {
		return taskCreationDatabaseError("begin cleanup finish", err)
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
		if op.Status != terminalStatus || op.Phase != terminalPhase ||
			op.TaskID != taskID || op.TombstonedAt == nil ||
			op.LeaseUntil != nil || op.TakeoverNotBefore != nil ||
			op.ErrorCode == "" || len(op.Result) != 0 {
			return taskCreationConflict("cleanup tombstone differs")
		}
		_, found, err := loadCreationScheduleForUpdate(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if found {
			return taskCreationConflict("cleanup tombstone retained an aggregate")
		}
		return verifyTaskCreationReceiptForTerminal(
			ctx, tx, lease.ID, lease.TenantID, lease.UserID)
	}
	if op.Status != types.TaskOperationStatusExecuting ||
		op.Phase != types.TaskCreationPhaseCleanupPending ||
		op.TaskID != taskID || op.ErrorCode == "" ||
		op.LeaseUntil == nil || op.TakeoverNotBefore == nil ||
		!databaseNow.Before(*op.LeaseUntil) {
		return taskCreationLeaseLost()
	}
	marker, err := decodeTaskCreationCleanupMarker(op.Result)
	if err != nil {
		return taskCreationConflict("cleanup ownership marker is invalid")
	}

	row, found, err := loadCreationScheduleForUpdate(ctx, tx, taskID)
	if err != nil {
		return err
	}
	if err := validateCleanupAggregateState(marker, row, found, lease); err != nil {
		return err
	}
	if marker.AggregateExpected {
		for _, statement := range []string{
			`DELETE FROM task_fetch_targets WHERE schedule_id = $1`,
			`DELETE FROM schedule_playbooks WHERE schedule_id = $1`,
			`DELETE FROM schedules
			  WHERE id = $1 AND tenant_id = $2 AND user_id = $3 AND status = $4
			    AND execution_mode = $5`,
		} {
			args := []any{taskID}
			if strings.Contains(statement, "DELETE FROM schedules") {
				args = append(args, lease.TenantID, lease.UserID,
					types.ScheduleStatusPaused, types.ExecutionModeCompiled)
			}
			tag, err := tx.Exec(ctx, statement, args...)
			if err != nil {
				return taskCreationDatabaseError("delete cleanup aggregate", err)
			}
			if strings.Contains(statement, "DELETE FROM schedules") && tag.RowsAffected() != 1 {
				return taskCreationConflict("cleanup aggregate changed during deletion")
			}
		}
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_creation_operations
		    SET status = $6, phase = $7, result = NULL, executed_at = NULL,
		        lease_until = NULL, takeover_not_before = NULL,
		        tombstoned_at = clock_timestamp(), updated_at = clock_timestamp()
		  WHERE id = $1 AND tenant_id = $2 AND user_id = $3
		    AND tool_name = 'create_schedule' AND execution_version = $8
		    AND status = $9 AND tombstoned_at IS NULL
		    AND lease_owner = $4 AND fence = $5 AND lease_until > clock_timestamp()
		    AND phase = $10 AND task_id = $11 AND error_code <> ''`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		terminalStatus, terminalPhase, types.TaskCreationExecutionVersionV1,
		types.TaskOperationStatusExecuting, types.TaskCreationPhaseCleanupPending, taskID,
	)
	if err != nil {
		return taskCreationDatabaseError("write cleanup tombstone", err)
	}
	if tag.RowsAffected() != 1 {
		return taskCreationLeaseLost()
	}
	if err := insertTaskCreationReceiptForTerminal(
		ctx, tx, lease.ID, lease.TenantID, lease.UserID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit cleanup finish", err)
	}
	return nil
}

func validateTaskCreationDefinitionCommitBinding(
	op *types.TaskCreationOperation,
	p types.CommitPausedCompiledTaskDefinitionForCreationParams,
) error {
	if op.TaskID != p.Definition.TaskID || op.CompiledDigest != p.CompiledDigest ||
		!bytes.Equal(op.PreparedSchedule, p.PreparedSchedule) ||
		!bytes.Equal(op.EnsureReceipt, p.EnsureReceipt) {
		return taskCreationConflict("definition commit evidence differs")
	}
	if !taskCreationBindingStateComplete(op) {
		return taskCreationConflict("definition commit checkpoint is incomplete")
	}
	checkpointDigest, err := taskCreationCheckpointDefinitionDigest(op.CompiledDefinition)
	if err != nil {
		return taskCreationConflict("compiled checkpoint definition digest is invalid")
	}
	definitionDigest, err := types.DigestPausedCompiledTaskDefinition(p.Definition)
	if err != nil || definitionDigest != checkpointDigest {
		return taskCreationConflict("compiled checkpoint and aggregate definition differ")
	}
	return nil
}

func taskCreationCheckpointDefinitionDigest(checkpoint []byte) (string, error) {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(checkpoint, &fields); err != nil || fields == nil {
		return "", errors.New("compiled checkpoint is not a strict JSON object")
	}
	raw, ok := fields["definition_digest"]
	if !ok {
		return "", errors.New("compiled checkpoint definition_digest is missing")
	}
	var digest string
	if err := json.Unmarshal(raw, &digest); err != nil || !validSHA256Digest(digest) {
		return "", errors.New("compiled checkpoint definition_digest is invalid")
	}
	return digest, nil
}

func taskCreationCheckpointDefinitionProtocol(checkpoint []byte) (string, error) {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(checkpoint, &fields); err != nil || fields == nil {
		return "", errors.New("compiled checkpoint is not a strict JSON object")
	}
	raw, ok := fields["version"]
	if !ok {
		return "", errors.New("compiled checkpoint version is missing")
	}
	var version string
	if err := json.Unmarshal(raw, &version); err != nil {
		return "", errors.New("compiled checkpoint version is invalid")
	}
	switch version {
	case compiledTaskDefinitionProtocolV1, compiledTaskDefinitionProtocolV2:
		return version, nil
	default:
		return "", errors.New("compiled checkpoint version is unsupported")
	}
}

func taskCreationBindingStateComplete(op *types.TaskCreationOperation) bool {
	if op == nil || len(op.CompiledDefinition) == 0 ||
		!validSHA256Digest(op.CompiledDigest) || len(op.PreparedSchedule) == 0 ||
		len(op.EnsureReceipt) == 0 || validateTaskCreationTaskID(op.TaskID) != nil {
		return false
	}
	if !taskCreationCompiledCheckpointDigestMatches(op) {
		return false
	}
	preparedTaskID, err := taskCreationPreparedScheduleTaskID(op.PreparedSchedule)
	return err == nil && preparedTaskID == op.TaskID
}

func taskCreationPreparedBindingStateComplete(
	op *types.TaskCreationOperation,
	taskID string,
) bool {
	if op == nil || len(op.CompiledDefinition) == 0 ||
		!validSHA256Digest(op.CompiledDigest) || len(op.PreparedSchedule) == 0 ||
		validateTaskCreationTaskID(taskID) != nil ||
		!taskCreationCompiledCheckpointDigestMatches(op) {
		return false
	}
	preparedTaskID, err := taskCreationPreparedScheduleTaskID(op.PreparedSchedule)
	return err == nil && preparedTaskID == taskID
}

func taskCreationCleanupBindingStateComplete(
	op *types.TaskCreationOperation,
	taskID string,
) bool {
	if taskCreationBindingStateComplete(op) {
		return op.TaskID == taskID
	}
	return op != nil && op.TaskID == taskID && len(op.EnsureReceipt) == 0 &&
		taskCreationPreparedBindingStateComplete(op, taskID)
}

func taskCreationCompiledCheckpointDigestMatches(op *types.TaskCreationOperation) bool {
	sum := sha256.Sum256(op.CompiledDefinition)
	return hex.EncodeToString(sum[:]) == op.CompiledDigest
}

func taskCreationPreparedScheduleTaskID(raw []byte) (string, error) {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(raw, &fields); err != nil || fields == nil {
		return "", errors.New("prepared schedule is not a strict JSON object")
	}
	rawTaskID, ok := fields["task_id"]
	if !ok {
		return "", errors.New("prepared schedule task_id is missing")
	}
	var taskID string
	if err := json.Unmarshal(rawTaskID, &taskID); err != nil ||
		validateTaskCreationTaskID(taskID) != nil {
		return "", errors.New("prepared schedule task_id is invalid")
	}
	return taskID, nil
}

func lockValidMembershipForTaskCreation(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
	userID int64,
) error {
	var valid bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM memberships m
		   JOIN tenants t ON t.id = m.tenant_id
		   JOIN users u ON u.id = m.user_id
		  WHERE m.tenant_id = $1 AND m.user_id = $2
		    AND t.status = 'active' AND t.deleted_at IS NULL
		  FOR UPDATE OF u, m FOR SHARE OF t
		  /* task creation membership lock order */`,
		tenantID, userID,
	).Scan(&valid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskCreationValidation("operation membership is no longer active")
		}
		return taskCreationDatabaseError("lock task capacity membership", err)
	}
	return nil
}

func lockTaskCapacityUser(ctx context.Context, tx pgx.Tx, userID int64) error {
	var valid bool
	err := tx.QueryRow(ctx,
		`SELECT true FROM users WHERE id = $1 FOR UPDATE`, userID,
	).Scan(&valid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return taskCreationValidation("task capacity user does not exist")
		}
		return taskCreationDatabaseError("lock task capacity user", err)
	}
	return nil
}

func countTaskCreationCapacity(
	ctx context.Context,
	tx pgx.Tx,
	userID int64,
) (int, error) {
	var count int
	err := tx.QueryRow(ctx,
		`SELECT count(*)
		   FROM (
		     SELECT 0 AS reservation_kind, id AS reservation_id
		       FROM schedules
		      WHERE user_id = $1 AND status = $2
		     UNION
		     SELECT 0, task_id
		       FROM task_creation_operations
		      WHERE user_id = $1
		        AND ((tool_name = 'create_schedule' AND execution_version = $3) OR
		             (tool_name = 'manage_tasks' AND execution_version = $4))
		        AND status = $5 AND tombstoned_at IS NULL AND task_id <> ''
		        AND phase IN ($6, $7, $8)
		     UNION
		     SELECT
		       CASE WHEN result->>'task_id_known' = 'true' THEN 0 ELSE 1 END,
		       CASE WHEN result->>'task_id_known' = 'true' THEN task_id ELSE id END
		       FROM task_creation_operations
		      WHERE user_id = $1
		        AND tool_name = 'create_schedule' AND execution_version = $3
		        AND status = $9 AND phase = $10 AND tombstoned_at IS NOT NULL
		        AND result->>'version' = $11
		        AND result->>'reservation_retained' = 'true'
		     UNION
		     SELECT 0, task_id
		       FROM task_creation_operations
		      WHERE user_id = $1
		        AND tool_name = 'manage_tasks' AND execution_version = $4
		        AND status = $9 AND phase = $10 AND tombstoned_at IS NOT NULL
		        AND task_id <> ''
		   ) reserved`,
		userID, types.ScheduleStatusActive,
		types.TaskCreationExecutionVersionV1, types.TaskCreationExecutionVersionV2,
		types.TaskOperationStatusExecuting,
		types.TaskCreationPhaseDefinitionCommitted,
		types.TaskCreationPhaseActivationStarted,
		types.TaskCreationPhaseActivated,
		types.TaskOperationStatusBlocked, types.TaskCreationPhaseBlocked,
		taskCreationQuarantineMarkerVersion,
	).Scan(&count)
	if err != nil {
		return 0, taskCreationDatabaseError("count active and reserved tasks", err)
	}
	return count, nil
}

func countScopedTaskCreationCapacity(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
	userID int64,
) (int, error) {
	var count int
	if err := tx.QueryRow(ctx,
		`SELECT public.count_task_creation_capacity_v1($1,$2)`,
		tenantID, userID).Scan(&count); err != nil {
		return 0, taskCreationDatabaseError("count scoped task creation capacity", err)
	}
	return count, nil
}

func pausedCompiledTaskDefinitionMatches(
	ctx context.Context,
	tx pgx.Tx,
	def types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
	expectedStatus types.ScheduleStatus,
) (bool, error) {
	var (
		tenantID      int64
		userID        int64
		nlDescription string
		specJSON      []byte
		scopeJSON     []byte
		status        types.ScheduleStatus
		executionMode string
		strictness    *string
		playbook      string
		fetchPlan     []byte
	)
	err := tx.QueryRow(ctx,
		`SELECT s.tenant_id, s.user_id, s.nl_description, s.spec_json, s.scope_json,
		        s.status, s.execution_mode, s.push_strictness, p.content, p.fetch_plan
		   FROM schedules s
		   JOIN schedule_playbooks p ON p.schedule_id = s.id
		  WHERE s.id = $1
		  FOR UPDATE OF s, p`,
		def.TaskID,
	).Scan(&tenantID, &userID, &nlDescription, &specJSON, &scopeJSON,
		&status, &executionMode, &strictness, &playbook, &fetchPlan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, taskCreationDatabaseError("load committed aggregate", err)
	}
	wantStrictness := nullableStrictness(def.Strictness)
	if tenantID != def.TenantID || userID != def.UserID ||
		nlDescription != def.NLDescription || status != expectedStatus ||
		executionMode != string(types.ExecutionModeCompiled) ||
		playbook != def.PlaybookContent || !nullableStringsEqual(strictness, wantStrictness) ||
		!taskCreationJSONEqual(specJSON, def.SpecJSON) ||
		!taskCreationJSONEqual(scopeJSON, def.ScopeJSON) ||
		!taskCreationJSONEqual(fetchPlan, def.FetchPlan) {
		return false, nil
	}
	urls := make([]string, 0, len(plan.Targets))
	for _, source := range plan.Targets {
		urls = append(urls, source.URL)
	}
	exact, err := compiledPlanLinksExact(ctx, tx, def.TaskID, urls)
	if err != nil {
		return false, taskCreationDatabaseError("verify committed aggregate sources", err)
	}
	return exact, nil
}

type creationScheduleState struct {
	tenantID   int64
	userID     int64
	status     types.ScheduleStatus
	mode       types.ExecutionMode
	generation string
}

func loadCreationScheduleForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	taskID string,
) (creationScheduleState, bool, error) {
	var row creationScheduleState
	err := tx.QueryRow(ctx,
		`SELECT tenant_id, user_id, status, execution_mode, xmin::text
		   FROM schedules WHERE id = $1 FOR UPDATE`, taskID,
	).Scan(&row.tenantID, &row.userID, &row.status, &row.mode, &row.generation)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return creationScheduleState{}, false, nil
		}
		return creationScheduleState{}, false,
			taskCreationDatabaseError("load creation schedule", err)
	}
	return row, true, nil
}

func decodeTaskCreationCleanupMarker(raw []byte) (taskCreationCleanupMarker, error) {
	var marker taskCreationCleanupMarker
	if len(raw) == 0 {
		return marker, errors.New("cleanup marker is missing")
	}
	if err := strictjson.Decode(raw, &marker); err != nil {
		return marker, err
	}
	if marker.Version != taskCreationCleanupMarkerVersion {
		return marker, errors.New("cleanup marker version differs")
	}
	if marker.AggregateExpected != (marker.AggregateGeneration != "") {
		return marker, errors.New("cleanup marker aggregate generation is inconsistent")
	}
	return marker, nil
}

func validateCleanupAggregateState(
	marker taskCreationCleanupMarker,
	row creationScheduleState,
	found bool,
	lease types.TaskCreationLease,
) error {
	if !marker.AggregateExpected {
		if found {
			return taskCreationConflict(
				"cleanup found an aggregate that was absent at its ownership checkpoint")
		}
		return nil
	}
	if !found {
		return taskCreationConflict("cleanup aggregate disappeared after definition commit")
	}
	if row.tenantID != lease.TenantID || row.userID != lease.UserID ||
		row.status != types.ScheduleStatusPaused ||
		row.mode != types.ExecutionModeCompiled ||
		row.generation != marker.AggregateGeneration {
		return taskCreationConflict("cleanup aggregate is not the owned paused aggregate")
	}
	return nil
}

func decodeTaskCreationQuarantineMarker(
	raw []byte,
) (taskCreationQuarantineMarker, bool) {
	var marker taskCreationQuarantineMarker
	if len(raw) == 0 || strictjson.Decode(raw, &marker) != nil {
		return marker, false
	}
	return marker, marker.Version == taskCreationQuarantineMarkerVersion &&
		marker.ReservationRetained &&
		(marker.PrimaryErrorCode != "" || marker.PrimaryErrorMessage == "")
}

func taskCreationQuarantineTaskID(op *types.TaskCreationOperation) (string, bool) {
	if op == nil {
		return "", false
	}
	if op.TaskID != "" {
		if validateTaskCreationTaskID(op.TaskID) == nil {
			return op.TaskID, true
		}
		return "", false
	}
	taskID, err := taskCreationPreparedScheduleTaskID(op.PreparedSchedule)
	return taskID, err == nil
}

func validateTaskCreationTaskID(taskID string) error {
	if strings.TrimSpace(taskID) == "" || taskID != strings.TrimSpace(taskID) ||
		len(taskID) > 255 {
		return taskCreationValidation("task id is invalid")
	}
	return nil
}

func validateTaskCreationErrorMetadata(code, message string) error {
	if strings.TrimSpace(code) == "" || code != strings.TrimSpace(code) ||
		message != strings.TrimSpace(message) || len(code) > maxTaskCreationErrorCode ||
		len(message) > maxTaskCreationErrorText {
		return taskCreationValidation("cleanup error metadata is invalid")
	}
	return nil
}

func cleanupTerminalPhase(status types.TaskOperationStatus) (types.TaskCreationPhase, error) {
	switch status {
	case types.TaskOperationStatusFailed:
		return types.TaskCreationPhaseFailed, nil
	case types.TaskOperationStatusBlocked:
		return types.TaskCreationPhaseBlocked, nil
	default:
		return "", taskCreationValidation("cleanup terminal status must be failed or blocked")
	}
}

func nullableStringsEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
