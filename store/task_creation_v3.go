package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// CreateResearchTaskCreationOperationV3 persists the native V3 protocol root.
// It intentionally does not share the V1 writer's execution-version constant;
// an ID collision with any retained protocol fails closed.
func (s *Store) CreateResearchTaskCreationOperationV3(
	ctx context.Context,
	p types.CreateResearchTaskCreationOperationV3Params,
) (*types.TaskCreationOperation, error) {
	legacyShape := types.CreateTaskCreationOperationParams{
		ID: p.ID, TenantID: p.TenantID, UserID: p.UserID, SessionID: p.SessionID,
		Args: p.Args, Summary: p.Summary, ExpiresAt: p.ExpiresAt,
	}
	if err := validateCreateTaskCreationParams(legacyShape); err != nil {
		return nil, err
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(p.Args)
	if err != nil || definition.TenantID != p.TenantID || definition.UserID != p.UserID ||
		definition.TaskName != p.Summary {
		return nil, taskCreationValidation("native V3 operation definition is invalid")
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin native V3 operation creation", err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)

	existing, found, err := loadResearchTaskCreationOperationV3Tx(
		ctx, tx, p.ID, p.TenantID, p.UserID, true)
	if err != nil {
		return nil, err
	}
	if found {
		if err := validateTaskCreationOperationCreationScope(ctx, tx, legacyShape); err != nil {
			return nil, err
		}
		if !researchTaskCreationV3RequestEqual(existing, p) {
			return nil, taskCreationConflict("native V3 operation id already exists")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, taskCreationDatabaseError("commit native V3 operation replay", err)
		}
		return existing, nil
	}

	if err := lockValidMembership(ctx, tx, p.TenantID, p.UserID); err != nil {
		return nil, err
	}
	if err := validateTaskCreationOperationSession(ctx, tx, legacyShape); err != nil {
		return nil, err
	}
	databaseNow, err := taskCreationDatabaseClock(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !databaseNow.Before(p.ExpiresAt) {
		return nil, taskCreationValidation("native V3 operation expiry is not in the future")
	}

	var op types.TaskCreationOperation
	err = scanTaskCreationOperation(tx.QueryRow(ctx,
		`INSERT INTO task_creation_operations
		 (id,tenant_id,user_id,session_id,tool_name,args,summary,status,
		  expires_at,execution_version)
		 VALUES ($1,$2,$3,$4,'create_schedule',$5,$6,$7,$8,$9)
		 ON CONFLICT (id) DO NOTHING
		 RETURNING `+taskCreationOperationColumns,
		p.ID, p.TenantID, p.UserID, p.SessionID, []byte(p.Args), p.Summary,
		types.TaskOperationStatusPending, p.ExpiresAt,
		types.TaskCreationExecutionVersionV2), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskCreationConflict("native V3 operation id already exists")
	}
	if err != nil {
		return nil, taskCreationDatabaseError("insert native V3 operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskCreationDatabaseError("commit native V3 operation", err)
	}
	return &op, nil
}

// CommitPausedResearchTaskDefinitionV3ForCreation atomically publishes only a
// paused discover-at-run projection, its playbook, immutable V3 definition and
// a staged delivery authority. The SECURITY DEFINER boundary in migration 109
// performs the exact fenced write for the restricted runtime role.
func (s *Store) CommitPausedResearchTaskDefinitionV3ForCreation(
	ctx context.Context,
	p types.CommitPausedResearchTaskDefinitionV3ForCreationParams,
) error {
	if err := validateTaskCreationLease(p.Lease); err != nil {
		return err
	}
	if err := validateTaskCreationTaskID(p.TaskID); err != nil {
		return err
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(p.DefinitionPayload)
	if err != nil || definition.TenantID != p.Lease.TenantID ||
		definition.UserID != p.Lease.UserID || definition.TaskID != p.TaskID {
		return taskCreationValidation("native V3 definition scope is invalid")
	}
	digest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil || digest != p.DefinitionDigest {
		return taskCreationValidation("native V3 definition digest differs")
	}
	if len(p.PreparedSchedule) == 0 || len(p.EnsureReceipt) == 0 ||
		len(p.TargetAction) == 0 || !validSHA256Digest(p.TargetActionDigest) ||
		!validSHA256Digest(p.ActionAuthorizationDigest) {
		return taskCreationValidation("native V3 creation evidence is incomplete")
	}
	actionSum := sha256.Sum256(p.TargetAction)
	if hex.EncodeToString(actionSum[:]) != p.TargetActionDigest {
		return taskCreationValidation("native V3 target action digest differs")
	}
	if _, err := s.pool.Exec(ctx,
		`SELECT commit_native_research_task_creation_v3_v1(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		p.Lease.ID, p.Lease.TenantID, p.Lease.UserID, p.Lease.LeaseOwner,
		p.Lease.Fence, p.TaskID, p.DefinitionDigest, p.DefinitionPayload,
		p.PreparedSchedule, p.EnsureReceipt, p.TargetAction,
		p.TargetActionDigest, p.ActionAuthorizationDigest,
		types.TaskCreationExecutionVersionV2); err != nil {
		return taskCreationDatabaseError("commit native V3 paused aggregate", err)
	}
	return nil
}

// BeginResearchTaskCreationActivationV3 durably authorizes one Temporal
// activation attempt. A false result is an exact replay and requires Describe
// before any caller repeats the external mutation.
func (s *Store) BeginResearchTaskCreationActivationV3(
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
	var started bool
	if err := s.pool.QueryRow(ctx,
		`SELECT begin_native_research_task_activation_v3_v1(
		 $1,$2,$3,$4,$5,$6,$7)`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		taskID, types.TaskCreationExecutionVersionV2).Scan(&started); err != nil {
		return false, taskCreationDatabaseError("begin native V3 activation", err)
	}
	return started, nil
}

// CommitResearchTaskCreationActivationV3 makes the schedule mirror active and
// enables its exact delivery authority in one database transaction after the
// external Temporal activation has been observed.
func (s *Store) CommitResearchTaskCreationActivationV3(
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
	if _, err := s.pool.Exec(ctx,
		`SELECT commit_native_research_task_activation_v3_v1(
		 $1,$2,$3,$4,$5,$6,$7)`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		taskID, types.TaskCreationExecutionVersionV2); err != nil {
		return taskCreationDatabaseError("commit native V3 activation", err)
	}
	return nil
}

func loadResearchTaskCreationOperationV3Tx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	tenantID int64,
	userID int64,
	lock bool,
) (*types.TaskCreationOperation, bool, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR SHARE"
	}
	var op types.TaskCreationOperation
	err := scanTaskCreationOperation(tx.QueryRow(ctx,
		`SELECT `+taskCreationOperationColumns+`
		   FROM task_creation_operations
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND tool_name='create_schedule' AND execution_version=$4`+lockClause,
		id, tenantID, userID, types.TaskCreationExecutionVersionV2), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, taskCreationDatabaseError("load native V3 operation", err)
	}
	return &op, true, nil
}

func researchTaskCreationV3RequestEqual(
	op *types.TaskCreationOperation,
	p types.CreateResearchTaskCreationOperationV3Params,
) bool {
	if op == nil || op.ExecutionVersion != types.TaskCreationExecutionVersionV2 ||
		op.ID != p.ID || op.TenantID != p.TenantID || op.UserID != p.UserID ||
		op.Summary != p.Summary || !researchTaskCreationV3DefinitionsEqual(op.Args, p.Args) ||
		!op.ExpiresAt.Equal(p.ExpiresAt) {
		return false
	}
	if (op.SessionID == nil) != (p.SessionID == nil) {
		return false
	}
	return op.SessionID == nil || *op.SessionID == *p.SessionID
}

func researchTaskCreationV3DefinitionsEqual(left, right json.RawMessage) bool {
	leftDefinition, err := taskstate.DecodeApprovedDefinitionV3(left)
	if err != nil {
		return false
	}
	rightDefinition, err := taskstate.DecodeApprovedDefinitionV3(right)
	if err != nil {
		return false
	}
	leftDigest, err := taskstate.DigestApprovedDefinitionV3(leftDefinition)
	if err != nil {
		return false
	}
	rightDigest, err := taskstate.DigestApprovedDefinitionV3(rightDefinition)
	return err == nil && leftDigest == rightDigest
}
