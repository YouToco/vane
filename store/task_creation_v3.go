package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
		 VALUES ($1,$2,$3,$4,'manage_tasks',$5,$6,$7,$8,$9)
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
	tx, err := s.beginNativeResearchCreationV3Tx(ctx, p.Lease.TenantID, p.Lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	if _, err := tx.Exec(ctx,
		`SELECT commit_native_research_task_creation_v3_v1(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		p.Lease.ID, p.Lease.TenantID, p.Lease.UserID, p.Lease.LeaseOwner,
		p.Lease.Fence, p.TaskID, p.DefinitionDigest, p.DefinitionPayload,
		p.PreparedSchedule, p.EnsureReceipt, p.TargetAction,
		p.TargetActionDigest, p.ActionAuthorizationDigest,
		types.TaskCreationExecutionVersionV2); err != nil {
		return nativeResearchCreationBoundaryError(
			"commit native V3 paused aggregate", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit native V3 paused aggregate transaction", err)
	}
	return nil
}

// BeginResearchTaskCreationActivationV3 durably authorizes one Temporal
// activation attempt. A false result is an exact replay and requires Describe
// before any caller repeats the external mutation.
func (s *Store) BeginResearchTaskCreationActivationV3(
	ctx context.Context,
	lease types.TaskCreationLease,
	binding types.ResearchTaskCreationActivationBindingV3,
) (bool, error) {
	if err := validateTaskCreationLease(lease); err != nil {
		return false, err
	}
	if err := validateResearchTaskCreationActivationBindingV3(binding); err != nil {
		return false, err
	}
	var started bool
	tx, err := s.beginNativeResearchCreationV3Tx(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return false, err
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	if err := tx.QueryRow(ctx,
		`SELECT begin_native_research_task_activation_v3_v1(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		binding.TaskID, binding.DefinitionDigest, binding.TargetActionDigest,
		binding.ActionAuthorizationDigest,
		types.TaskCreationExecutionVersionV2).Scan(&started); err != nil {
		return false, nativeResearchCreationBoundaryError(
			"begin native V3 activation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, taskCreationDatabaseError("commit native V3 activation authorization", err)
	}
	return started, nil
}

// CommitResearchTaskCreationActivationV3 makes the schedule mirror active and
// enables its exact delivery authority in one database transaction after the
// external Temporal activation has been observed.
func (s *Store) CommitResearchTaskCreationActivationV3(
	ctx context.Context,
	lease types.TaskCreationLease,
	binding types.ResearchTaskCreationActivationBindingV3,
) error {
	if err := validateTaskCreationLease(lease); err != nil {
		return err
	}
	if err := validateResearchTaskCreationActivationBindingV3(binding); err != nil {
		return err
	}
	tx, err := s.beginNativeResearchCreationV3Tx(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	if _, err := tx.Exec(ctx,
		`SELECT commit_native_research_task_activation_v3_v1(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		lease.ID, lease.TenantID, lease.UserID, lease.LeaseOwner, lease.Fence,
		binding.TaskID, binding.DefinitionDigest, binding.TargetActionDigest,
		binding.ActionAuthorizationDigest,
		types.TaskCreationExecutionVersionV2); err != nil {
		return nativeResearchCreationBoundaryError(
			"commit native V3 activation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskCreationDatabaseError("commit native V3 activation transaction", err)
	}
	return nil
}

func nativeResearchCreationBoundaryError(action string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "P1091":
			return types.NewAppError(types.CodeConflict,
				"task creation: active task limit reached",
				types.ErrTaskCreationLimit)
		case "P1092":
			return types.NewAppError(types.CodeConflict,
				"task creation: owner scope is inactive",
				types.ErrTaskCreationOwnerScopeInactive)
		}
	}
	return taskCreationDatabaseError(action, err)
}

func validateResearchTaskCreationActivationBindingV3(
	binding types.ResearchTaskCreationActivationBindingV3,
) error {
	if err := validateTaskCreationTaskID(binding.TaskID); err != nil {
		return err
	}
	if !validSHA256Digest(binding.DefinitionDigest) ||
		!validSHA256Digest(binding.TargetActionDigest) ||
		!validSHA256Digest(binding.ActionAuthorizationDigest) {
		return taskCreationValidation("native V3 activation binding is invalid")
	}
	return nil
}

func (s *Store) beginNativeResearchCreationV3Tx(
	ctx context.Context, tenantID, userID int64,
) (pgx.Tx, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskCreationDatabaseError("begin native V3 coordinator transaction", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, taskCreationDatabaseError("bind native V3 coordinator scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_native_v3_creation_coordinator`); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, taskCreationDatabaseError("bind native V3 coordinator role", err)
	}
	return tx, nil
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
		    AND tool_name='manage_tasks' AND execution_version=$4`+lockClause,
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
		op.Summary != p.Summary || !researchTaskCreationV3DefinitionsEqual(op.Args, p.Args) {
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
