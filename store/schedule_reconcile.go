package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const (
	taskScheduleMutationAdvisoryLockSeed int64 = 0x56414e45
	scheduleReconcileRollbackTimeout           = 2 * time.Second
)

// AcquireScheduleReconcile turns a discovery-only active schedule into a
// fresh authorization held under the same cross-process advisory lock used by
// definition-edit quiesce. The returned release must be called after the
// bounded Temporal Describe/Update attempt; rolling back is intentional
// because this transaction owns no database mutation.
func (s *Store) AcquireScheduleReconcile(
	ctx context.Context,
	id string,
) (*types.Schedule, func(context.Context) error, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(id) != id || len(id) > 255 {
		return nil, nil, types.NewAppError(
			types.CodeValidation, "调度 reconcile 标识无效", types.ErrValidation,
		)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, types.NewAppError(
			types.CodeDatabase, fmt.Sprintf("开始调度 reconcile（id=%s）", id), err,
		)
	}
	release := func(parent context.Context) error {
		releaseCtx, cancelRelease := scheduleReconcileRollbackContext(parent)
		defer cancelRelease()
		err := tx.Rollback(releaseCtx)
		if errors.Is(err, pgx.ErrTxClosed) {
			return nil
		}
		return err
	}
	if err := lockTaskScheduleMutation(ctx, tx, id); err != nil {
		if rollbackErr := release(ctx); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf(
				"释放失败的调度 reconcile 事务（id=%s）: %w", id, rollbackErr,
			))
		}
		return nil, nil, err
	}

	var sc types.Schedule
	err = scanSchedule(tx.QueryRow(ctx,
		`SELECT `+scheduleColumns+`
		   FROM schedules s
		  WHERE s.id=$1 AND s.status=$2
		    AND s.definition_edit_operation_id IS NULL
		    AND s.definition_edit_fence IS NULL
		    AND NOT EXISTS (
		      SELECT 1
		        FROM task_definition_edit_operations o
		       WHERE o.target_tenant_id=s.tenant_id
		         AND o.target_user_id=s.user_id
		         AND o.task_id=s.id
		         AND o.status IN ($3,$4)
		         AND o.tombstoned_at IS NULL
		    )
		    AND `+matureSchedulePredicate,
		id, types.ScheduleStatusActive,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditOperationStatusExecuting,
	), &sc)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, release, nil
		}
		var databaseErr error = types.NewAppError(
			types.CodeDatabase, fmt.Sprintf("读取调度 reconcile 授权（id=%s）", id), err,
		)
		if rollbackErr := release(ctx); rollbackErr != nil {
			databaseErr = errors.Join(databaseErr, fmt.Errorf(
				"释放失败的调度 reconcile 事务（id=%s）: %w", id, rollbackErr,
			))
		}
		return nil, nil, databaseErr
	}
	return &sc, release, nil
}

func scheduleReconcileRollbackContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(parent), scheduleReconcileRollbackTimeout,
	)
}

// lockTaskScheduleMutation is the cross-process counterpart of Scheduler's
// keyed in-memory gate. Hash collisions only over-serialize unrelated task
// IDs; they cannot authorize a write. Every caller must acquire it before any
// row lock so quiesce and startup reconcile share one fixed lock order.
func lockTaskScheduleMutation(
	ctx context.Context,
	tx pgx.Tx,
	taskID string,
) error {
	if strings.TrimSpace(taskID) == "" || strings.TrimSpace(taskID) != taskID ||
		len(taskID) > 255 {
		return types.NewAppError(
			types.CodeValidation, "任务调度变更标识无效", types.ErrValidation,
		)
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`,
		taskID, taskScheduleMutationAdvisoryLockSeed,
	); err != nil {
		return types.NewAppError(
			types.CodeDatabase,
			fmt.Sprintf("获取任务调度变更锁（id=%s）", taskID),
			err,
		)
	}
	return nil
}
