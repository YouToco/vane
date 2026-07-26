package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const (
	maxScheduleCommandKeyBytes  = 128
	scheduleCommandAttemptLimit = 256
	scheduleCommandRole         = "vane_schedule_commander"
	scheduleCommandRollbackTime = 2 * time.Second
)

var scheduleCommandKeyPattern = regexp.MustCompile(
	`^[A-Za-z0-9][A-Za-z0-9._:-]*$`,
)

const scheduleCommandColumns = `
	id, tenant_id, user_id, task_id, idempotency_key, kind,
	payload_digest, remote_request_id, status, phase,
	created_at, updated_at, completed_at, error_code, error_message`

func scanScheduleCommand(row pgx.Row, command *types.ScheduleCommand) error {
	return row.Scan(
		&command.ID, &command.TenantID, &command.UserID, &command.TaskID,
		&command.IdempotencyKey, &command.Kind, &command.PayloadDigest,
		&command.RemoteRequestID, &command.Status, &command.Phase,
		&command.CreatedAt, &command.UpdatedAt, &command.CompletedAt,
		&command.ErrorCode, &command.ErrorMessage,
	)
}

func validScheduleCommandKind(kind types.ScheduleCommandKind) bool {
	switch kind {
	case types.ScheduleCommandRun, types.ScheduleCommandPause,
		types.ScheduleCommandResume, types.ScheduleCommandDelete:
		return true
	default:
		return false
	}
}

func validateScheduleCommandIdentity(
	tenantID, userID int64,
	taskID, key string,
	kind types.ScheduleCommandKind,
	payloadDigest, remoteRequestID string,
) error {
	if tenantID <= 0 || userID <= 0 ||
		taskID == "" || len(taskID) > 255 || strings.TrimSpace(taskID) != taskID ||
		!utf8.ValidString(taskID) || !validScheduleCommandKind(kind) ||
		key == "" || len(key) > maxScheduleCommandKeyBytes ||
		strings.TrimSpace(key) != key || !scheduleCommandKeyPattern.MatchString(key) ||
		!validLowerHexDigest(payloadDigest) ||
		!validLowerHexDigest(remoteRequestID) {
		return types.NewAppError(
			types.CodeValidation,
			"任务命令参数或 Idempotency-Key 无效",
			types.ErrValidation,
		)
	}
	for _, value := range []string{taskID, key} {
		for _, r := range value {
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				return types.NewAppError(
					types.CodeValidation,
					"任务命令参数或 Idempotency-Key 无效",
					types.ErrValidation,
				)
			}
		}
	}
	return nil
}

func validLowerHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func scheduleCommandMatches(
	command *types.ScheduleCommand,
	tenantID, userID int64,
	taskID, key string,
	kind types.ScheduleCommandKind,
	payloadDigest, remoteRequestID string,
) bool {
	return command != nil &&
		command.TenantID == tenantID && command.UserID == userID &&
		command.TaskID == taskID && command.IdempotencyKey == key &&
		command.Kind == kind && command.PayloadDigest == payloadDigest &&
		command.RemoteRequestID == remoteRequestID
}

func (s *Store) beginScheduleCommandTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	if tenantID <= 0 {
		return nil, types.NewAppError(
			types.CodeValidation, "任务命令租户无效", types.ErrValidation,
		)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "开始任务命令事务", err,
		)
	}
	fail := func(cause error) (pgx.Tx, error) {
		rollbackScheduleCommandTx(ctx, tx)
		return nil, cause
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10),
	); err != nil {
		return fail(types.NewAppError(
			types.CodeDatabase, "设置任务命令租户上下文", err,
		))
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+scheduleCommandRole); err != nil {
		return fail(types.NewAppError(
			types.CodeDatabase, "进入任务命令受限角色", err,
		))
	}
	return tx, nil
}

func rollbackScheduleCommandTx(parent context.Context, tx pgx.Tx) {
	if tx == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(parent), scheduleCommandRollbackTime,
	)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func scheduleCommandStoreDetachedContext(
	parent context.Context,
	maximum time.Duration,
) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(parent)
	deadline := time.Now().Add(maximum)
	if parentDeadline, ok := parent.Deadline(); ok &&
		parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(detached, deadline)
}

func loadScheduleCommandByKey(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	key string,
	forUpdate bool,
) (*types.ScheduleCommand, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	var command types.ScheduleCommand
	err := scanScheduleCommand(tx.QueryRow(ctx,
		`SELECT `+scheduleCommandColumns+`
		   FROM schedule_commands
		  WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3`+suffix,
		tenantID, userID, key,
	), &command)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "读取任务命令", err,
		)
	}
	return &command, nil
}

func loadScheduleCommandTarget(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (*types.Schedule, bool, error) {
	var (
		schedule    types.Schedule
		rawMode     string
		markerClear bool
		editClear   bool
		mature      bool
	)
	err := tx.QueryRow(ctx, `
		SELECT s.id, s.tenant_id, s.user_id, s.nl_description,
		       s.spec_json, s.scope_json, s.status, s.execution_mode,
		       s.created_at, s.updated_at,
		       s.definition_edit_operation_id IS NULL AND
		           s.definition_edit_fence IS NULL,
		       NOT EXISTS (
		           SELECT 1
		             FROM task_definition_edit_operations o
		            WHERE o.target_tenant_id=s.tenant_id
		              AND o.target_user_id=s.user_id
		              AND o.task_id=s.id
		              AND o.status IN ('pending','executing')
		              AND o.tombstoned_at IS NULL
		       ),
		       `+matureSchedulePredicate+`
		  FROM schedules s
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		 FOR UPDATE`,
		tenantID, userID, taskID,
	).Scan(
		&schedule.ID, &schedule.TenantID, &schedule.UserID,
		&schedule.NLDescription, &schedule.SpecJSON, &schedule.ScopeJSON,
		&schedule.Status, &rawMode, &schedule.CreatedAt, &schedule.UpdatedAt,
		&markerClear, &editClear, &mature,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, types.NewAppError(
			types.CodeDatabase, "读取任务命令目标", err,
		)
	}
	mode, err := types.ParseExecutionMode(rawMode)
	if err != nil {
		return nil, false, types.NewAppError(
			types.CodeInternal, "任务命令目标状态损坏", err,
		)
	}
	schedule.ExecutionMode = mode
	return &schedule, markerClear && editClear && mature, nil
}

func authorizeNewScheduleCommand(
	command *types.ScheduleCommand,
	schedule *types.Schedule,
	mutable bool,
) error {
	if schedule == nil {
		return types.NewAppError(
			types.CodeNotFound, "任务不存在", types.ErrNotFound,
		)
	}
	if !mutable {
		return types.NewAppError(
			types.CodeConflict,
			"任务正在创建或编辑，请稍后重试。",
			types.ErrConflict,
		)
	}
	switch command.Kind {
	case types.ScheduleCommandRun:
		if schedule.Status != types.ScheduleStatusActive {
			return types.NewAppError(
				types.CodeConflict,
				"任务已暂停，请先恢复后再立即运行。",
				types.ErrConflict,
			)
		}
	case types.ScheduleCommandPause, types.ScheduleCommandResume,
		types.ScheduleCommandDelete:
		if schedule.Status != types.ScheduleStatusActive &&
			schedule.Status != types.ScheduleStatusPaused {
			return types.NewAppError(
				types.CodeConflict,
				"任务当前状态不支持这项操作，请刷新后重试。",
				types.ErrConflict,
			)
		}
	default:
		return types.NewAppError(
			types.CodeValidation, "未知任务命令", types.ErrValidation,
		)
	}
	return nil
}

// CreateOrLoadScheduleCommand commits the immutable intent before any
// Temporal call. An existing key is returned only when every bound field is
// byte-for-byte equivalent; cross-kind/task/payload reuse is a conflict.
func (s *Store) CreateOrLoadScheduleCommand(
	ctx context.Context,
	tenantID, userID int64,
	taskID, key string,
	kind types.ScheduleCommandKind,
	payloadDigest, remoteRequestID string,
) (*types.ScheduleCommand, error) {
	if err := validateScheduleCommandIdentity(
		tenantID, userID, taskID, key, kind, payloadDigest, remoteRequestID,
	); err != nil {
		return nil, err
	}
	tx, err := s.beginScheduleCommandTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer rollbackScheduleCommandTx(ctx, tx)
	if err := lockTaskScheduleMutation(ctx, tx, taskID); err != nil {
		return nil, err
	}
	existing, err := loadScheduleCommandByKey(
		ctx, tx, tenantID, userID, key, true,
	)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !scheduleCommandMatches(
			existing, tenantID, userID, taskID, key, kind,
			payloadDigest, remoteRequestID,
		) {
			return nil, types.NewAppError(
				types.CodeConflict,
				"Idempotency-Key 已绑定到另一项任务操作。",
				types.ErrConflict,
			)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, types.NewAppError(
				types.CodeDatabase, "提交任务命令重放读取", err,
			)
		}
		return existing, nil
	}

	command := &types.ScheduleCommand{
		ID: uuid.NewString(), TenantID: tenantID, UserID: userID,
		TaskID: taskID, IdempotencyKey: key, Kind: kind,
		PayloadDigest: payloadDigest, RemoteRequestID: remoteRequestID,
		Status: types.ScheduleCommandPending, Phase: types.ScheduleCommandIntent,
	}
	schedule, mutable, err := loadScheduleCommandTarget(
		ctx, tx, tenantID, userID, taskID,
	)
	if err != nil {
		return nil, err
	}
	if err := authorizeNewScheduleCommand(command, schedule, mutable); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO schedule_commands (
		    id, tenant_id, user_id, task_id, idempotency_key, kind,
		    payload_digest, remote_request_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT DO NOTHING`,
		command.ID, tenantID, userID, taskID, key, kind,
		payloadDigest, remoteRequestID,
	)
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "保存任务命令意图", err,
		)
	}
	if tag.RowsAffected() != 1 {
		// The idempotency-key row cannot have appeared while the task advisory
		// lock is held. Therefore a no-op is the partial unique index: another
		// non-terminal command owns this task.
		return nil, types.NewAppError(
			types.CodeConflict,
			"任务已有一项操作正在收敛，请稍后重试。",
			types.ErrConflict,
		)
	}
	if err := tx.QueryRow(ctx, `
		SELECT created_at, updated_at
		  FROM schedule_commands
		 WHERE id=$1`,
		command.ID,
	).Scan(&command.CreatedAt, &command.UpdatedAt); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "读取已保存任务命令", err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		// A lost COMMIT response is resolved by an exact scoped read. The same
		// key cannot be rebound while its possibly-committed row exists.
		readCtx, cancel := scheduleCommandStoreDetachedContext(
			ctx, 3*time.Second,
		)
		defer cancel()
		adopted, readErr := s.LoadScheduleCommand(
			readCtx, tenantID, userID, key,
		)
		if readErr == nil && scheduleCommandMatches(
			adopted, tenantID, userID, taskID, key, kind,
			payloadDigest, remoteRequestID,
		) {
			return adopted, nil
		}
		return nil, errors.Join(
			types.NewAppError(
				types.CodeDatabase, "提交任务命令意图", err,
			),
			readErr,
		)
	}
	return command, nil
}

func (s *Store) LoadScheduleCommand(
	ctx context.Context,
	tenantID, userID int64,
	key string,
) (*types.ScheduleCommand, error) {
	if tenantID <= 0 || userID <= 0 || key == "" ||
		len(key) > maxScheduleCommandKeyBytes ||
		!scheduleCommandKeyPattern.MatchString(key) {
		return nil, types.NewAppError(
			types.CodeValidation, "任务命令读取参数无效", types.ErrValidation,
		)
	}
	tx, err := s.beginScheduleCommandTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer rollbackScheduleCommandTx(ctx, tx)
	command, err := loadScheduleCommandByKey(
		ctx, tx, tenantID, userID, key, false,
	)
	if err != nil {
		return nil, err
	}
	if command == nil {
		return nil, types.NewAppError(
			types.CodeNotFound, "任务命令不存在", types.ErrNotFound,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "提交任务命令读取", err,
		)
	}
	return command, nil
}

// BeginScheduleCommandAttempt holds the shared cross-process task mutation
// lock across one bounded Temporal call. complete commits the matching mirror
// state and terminal checkpoint in one transaction; block records a permanent
// remote integrity failure. rollback is safe after either closure commits.
func (s *Store) BeginScheduleCommandAttempt(
	ctx context.Context,
	tenantID, userID int64,
	key string,
) (
	command *types.ScheduleCommand,
	schedule *types.Schedule,
	complete func(context.Context) error,
	block func(context.Context, string, string) error,
	rollback func(context.Context) error,
	err error,
) {
	tx, err := s.beginScheduleCommandTx(ctx, tenantID)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	rollback = func(parent context.Context) error {
		releaseCtx, cancel := context.WithTimeout(
			context.WithoutCancel(parent), scheduleCommandRollbackTime,
		)
		defer cancel()
		err := tx.Rollback(releaseCtx)
		if errors.Is(err, pgx.ErrTxClosed) {
			return nil
		}
		return err
	}
	fail := func(cause error) (
		*types.ScheduleCommand,
		*types.Schedule,
		func(context.Context) error,
		func(context.Context, string, string) error,
		func(context.Context) error,
		error,
	) {
		if rollbackErr := rollback(ctx); rollbackErr != nil {
			cause = errors.Join(cause, rollbackErr)
		}
		return nil, nil, nil, nil, nil, cause
	}
	peek, err := loadScheduleCommandByKey(
		ctx, tx, tenantID, userID, key, false,
	)
	if err != nil {
		return fail(err)
	}
	if peek == nil {
		return fail(types.NewAppError(
			types.CodeNotFound, "任务命令不存在", types.ErrNotFound,
		))
	}
	if err := lockTaskScheduleMutation(ctx, tx, peek.TaskID); err != nil {
		return fail(err)
	}
	// Keep the row-lock order aligned with tenant purge:
	//
	//	task advisory lock -> schedule -> schedule command
	//
	// The unlocked peek supplies only the immutable task identity needed for
	// the advisory key. The command is reloaded and identity-checked below.
	// Locking the command first would deadlock with purge, which drains schedule
	// rows before command rows.
	schedule, mutable, err := loadScheduleCommandTarget(
		ctx, tx, tenantID, userID, peek.TaskID,
	)
	if err != nil {
		return fail(err)
	}
	command, err = loadScheduleCommandByKey(
		ctx, tx, tenantID, userID, key, true,
	)
	if err != nil {
		return fail(err)
	}
	if command == nil || command.ID != peek.ID ||
		command.TaskID != peek.TaskID {
		return fail(types.NewAppError(
			types.CodeConflict,
			"任务命令身份在加锁期间发生变化",
			types.ErrConflict,
		))
	}
	if command.Status != types.ScheduleCommandPending {
		return command, nil, nil, nil, rollback, nil
	}
	if schedule == nil && command.Kind != types.ScheduleCommandDelete {
		return fail(types.NewAppError(
			types.CodeNotFound, "任务不存在", types.ErrNotFound,
		))
	}
	if schedule != nil && !mutable {
		return fail(types.NewAppError(
			types.CodeConflict,
			"任务正在创建或编辑，请稍后重试。",
			types.ErrConflict,
		))
	}

	finish := func(
		finishCtx context.Context,
		status types.ScheduleCommandStatus,
		phase types.ScheduleCommandPhase,
		errorCode, errorMessage string,
	) error {
		if status == types.ScheduleCommandCompleted {
			switch command.Kind {
			case types.ScheduleCommandPause:
				if _, err := tx.Exec(finishCtx, `
					UPDATE schedules
					   SET status=$4, updated_at=clock_timestamp()
					 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
					tenantID, userID, command.TaskID,
					types.ScheduleStatusPaused,
				); err != nil {
					return types.NewAppError(
						types.CodeDatabase, "提交任务暂停镜像", err,
					)
				}
			case types.ScheduleCommandResume:
				if _, err := tx.Exec(finishCtx, `
					UPDATE schedules
					   SET status=$4, updated_at=clock_timestamp()
					 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
					tenantID, userID, command.TaskID,
					types.ScheduleStatusActive,
				); err != nil {
					return types.NewAppError(
						types.CodeDatabase, "提交任务恢复镜像", err,
					)
				}
			case types.ScheduleCommandDelete:
				if _, err := tx.Exec(finishCtx, `
					DELETE FROM schedules
					 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
					tenantID, userID, command.TaskID,
				); err != nil {
					return types.NewAppError(
						types.CodeDatabase, "提交任务删除镜像", err,
					)
				}
			case types.ScheduleCommandRun:
				// The run result exists only in Temporal. The task row was
				// locked and re-authorized before the remote call.
			default:
				return types.NewAppError(
					types.CodeInternal, "未知任务命令状态", nil,
				)
			}
		}
		tag, err := tx.Exec(finishCtx, `
			UPDATE schedule_commands
			   SET status=$4, phase=$5,
			       error_code=$6, error_message=$7,
			       completed_at=clock_timestamp(),
			       updated_at=clock_timestamp()
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			   AND status='pending' AND phase='intent'`,
			command.ID, tenantID, userID, status, phase,
			errorCode, errorMessage,
		)
		if err != nil {
			return types.NewAppError(
				types.CodeDatabase, "保存任务命令终态", err,
			)
		}
		if tag.RowsAffected() != 1 {
			return types.NewAppError(
				types.CodeConflict,
				"任务命令已被另一恢复器处理",
				types.ErrConflict,
			)
		}
		if err := tx.Commit(finishCtx); err != nil {
			return types.NewAppError(
				types.CodeDatabase, "提交任务命令终态", err,
			)
		}
		return nil
	}
	complete = func(finishCtx context.Context) error {
		return finish(
			finishCtx, types.ScheduleCommandCompleted,
			types.ScheduleCommandCompletedPhase, "", "",
		)
	}
	block = func(
		finishCtx context.Context,
		errorCode, errorMessage string,
	) error {
		if errorCode == "" || len(errorCode) > 64 ||
			errorMessage == "" || len(errorMessage) > 1024 {
			return types.NewAppError(
				types.CodeValidation, "任务命令阻断原因无效",
				types.ErrValidation,
			)
		}
		return finish(
			finishCtx, types.ScheduleCommandBlocked,
			types.ScheduleCommandBlockedPhase, errorCode, errorMessage,
		)
	}
	return command, schedule, complete, block, rollback, nil
}

// ListPendingScheduleCommands is recovery-only discovery. It returns immutable
// identities, not payloads or user data, and every mutation is subsequently
// re-authorized inside a tenant-scoped restricted-role transaction.
func (s *Store) ListPendingScheduleCommands(
	ctx context.Context,
	afterTenantID int64,
	afterID string,
) ([]types.ScheduleCommand, error) {
	if afterTenantID < 0 || len(afterID) > 64 {
		return nil, types.NewAppError(
			types.CodeValidation,
			"任务命令恢复游标无效",
			types.ErrValidation,
		)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+scheduleCommandColumns+`
		  FROM schedule_commands
		 WHERE status='pending'
		   AND (
		       tenant_id > $1 OR
		       (tenant_id = $1 AND id::text > $2)
		   )
		 ORDER BY tenant_id, id
		 LIMIT $3`,
		afterTenantID, afterID, scheduleCommandAttemptLimit,
	)
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "扫描待恢复任务命令", err,
		)
	}
	defer rows.Close()
	commands := make([]types.ScheduleCommand, 0)
	for rows.Next() {
		var command types.ScheduleCommand
		if err := scanScheduleCommand(rows, &command); err != nil {
			return nil, types.NewAppError(
				types.CodeDatabase, "读取待恢复任务命令", err,
			)
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "遍历待恢复任务命令", err,
		)
	}
	return commands, nil
}

// ValidateScheduleCommandRuntimeRole proves that the production connection can
// enter the NOLOGIN role and that RLS is active without granting vane_app any
// access to the durable command table.
func (s *Store) ValidateScheduleCommandRuntimeRole(ctx context.Context) error {
	var maxTenantID int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(max(id),0) FROM tenants`,
	).Scan(&maxTenantID); err != nil {
		return fmt.Errorf("load schedule command role probe tenant: %w", err)
	}
	if maxTenantID <= 0 || maxTenantID == int64(^uint64(0)>>1) {
		return errors.New("schedule command role probe tenant is unavailable")
	}
	tx, err := s.beginScheduleCommandTx(ctx, maxTenantID+1)
	if err != nil {
		return err
	}
	defer rollbackScheduleCommandTx(ctx, tx)
	var (
		currentRole, tenantContext                 string
		superuser, bypassRLS, login, inherit       bool
		ownsTable, rowSecurity                     bool
		appSelect, appInsert, appUpdate, appDelete bool
		mayUpdateTaskStatus, mayDeleteTask         bool
	)
	err = tx.QueryRow(ctx, `
		SELECT current_user,
		       current_setting('app.tenant_id', true),
		       r.rolsuper, r.rolbypassrls, r.rolcanlogin, r.rolinherit,
		       c.relowner=r.oid,
		       row_security_active('schedule_commands'::regclass),
		       has_table_privilege('vane_app','schedule_commands','SELECT'),
		       has_table_privilege('vane_app','schedule_commands','INSERT'),
		       has_table_privilege('vane_app','schedule_commands','UPDATE'),
		       has_table_privilege('vane_app','schedule_commands','DELETE'),
		       has_column_privilege(
		           current_user,'schedules','status','UPDATE'
		       ),
		       has_table_privilege(current_user,'schedules','DELETE')
		  FROM pg_roles r
		  JOIN pg_class c ON c.relname='schedule_commands'
		 WHERE r.rolname=current_user`,
	).Scan(
		&currentRole, &tenantContext, &superuser, &bypassRLS, &login,
		&inherit, &ownsTable, &rowSecurity,
		&appSelect, &appInsert, &appUpdate, &appDelete,
		&mayUpdateTaskStatus, &mayDeleteTask,
	)
	if err != nil {
		return fmt.Errorf("inspect schedule command runtime role: %w", err)
	}
	if currentRole != scheduleCommandRole ||
		tenantContext != strconv.FormatInt(maxTenantID+1, 10) ||
		superuser || bypassRLS || login || inherit || ownsTable ||
		!rowSecurity || appSelect || appInsert || appUpdate || appDelete ||
		!mayUpdateTaskStatus || !mayDeleteTask {
		return fmt.Errorf(
			"schedule command role has unsafe capabilities "+
				"(role=%s tenant=%s super=%t bypass=%t login=%t inherit=%t "+
				"owner=%t rls=%t app=%t/%t/%t/%t status=%t delete=%t)",
			currentRole, tenantContext, superuser, bypassRLS, login, inherit,
			ownsTable, rowSecurity, appSelect, appInsert, appUpdate, appDelete,
			mayUpdateTaskStatus, mayDeleteTask,
		)
	}
	return nil
}
