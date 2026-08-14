package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const scheduleCommandRecoveryWorkerKey = "scheduler"

func (s *Store) beginScheduleCommandRecoveryCursorTx(
	ctx context.Context, options pgx.TxOptions,
) (pgx.Tx, error) {
	tx, err := s.beginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	fail := func(message string, cause error) (pgx.Tx, error) {
		rollbackScheduleCommandTx(ctx, tx)
		return nil, types.NewAppError(types.CodeDatabase, message, cause)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public`); err != nil {
		return fail("固定任务命令恢复游标搜索路径", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_schedule_commander`); err != nil {
		return fail("进入任务命令恢复游标角色", err)
	}
	var currentRole, searchPath string
	var readOnly bool
	if err := tx.QueryRow(ctx, `
		SELECT current_user,current_setting('search_path'),
		       current_setting('transaction_read_only')::boolean`,
	).Scan(&currentRole, &searchPath, &readOnly); err != nil {
		return fail("校验任务命令恢复游标权限", err)
	}
	expectReadOnly := options.AccessMode == pgx.ReadOnly
	if currentRole != scheduleCommandRole || searchPath != "pg_catalog, public" ||
		readOnly != expectReadOnly {
		return fail("任务命令恢复游标权限边界无效", fmt.Errorf(
			"role=%s search_path=%s read_only=%t expected_read_only=%t",
			currentRole, searchPath, readOnly, expectReadOnly,
		))
	}
	return tx, nil
}

// LoadScheduleCommandRecoveryCursor loads the last identity durably advanced
// before a remote attempt. A missing singleton is the zero cursor.
func (s *Store) LoadScheduleCommandRecoveryCursor(
	ctx context.Context,
) (int64, string, error) {
	tx, err := s.beginScheduleCommandRecoveryCursorTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, "", err
	}
	defer rollbackScheduleCommandTx(ctx, tx)
	var tenantID int64
	var commandID string
	err = tx.QueryRow(ctx, `
		SELECT tenant_id,COALESCE(command_id::text,'')
		  FROM public.schedule_command_recovery_cursors
		 WHERE worker_key=$1`, scheduleCommandRecoveryWorkerKey,
	).Scan(&tenantID, &commandID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", types.NewAppError(
			types.CodeDatabase, "读取任务命令恢复游标", err,
		)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		tenantID, commandID = 0, ""
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, "", types.NewAppError(
			types.CodeDatabase, "提交任务命令恢复游标读取", err,
		)
	}
	return tenantID, commandID, nil
}

// SaveScheduleCommandRecoveryCursor commits progress before external I/O.
// The zero cursor is written only after the complete catalog tail is reached.
func (s *Store) SaveScheduleCommandRecoveryCursor(
	ctx context.Context, tenantID int64, commandID string,
) error {
	if tenantID < 0 || (tenantID == 0 && commandID != "") {
		return types.NewAppError(
			types.CodeValidation, "任务命令恢复游标无效", types.ErrValidation,
		)
	}
	var parsedCommandID any
	if commandID != "" {
		parsed, err := uuid.Parse(commandID)
		if err != nil || tenantID == 0 {
			return types.NewAppError(
				types.CodeValidation, "任务命令恢复游标无效", types.ErrValidation,
			)
		}
		parsedCommandID = parsed
	}
	tx, err := s.beginScheduleCommandRecoveryCursorTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer rollbackScheduleCommandTx(ctx, tx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO public.schedule_command_recovery_cursors
		       (worker_key,tenant_id,command_id,updated_at)
		VALUES ($1,$2,$3,clock_timestamp())
		ON CONFLICT (worker_key) DO UPDATE
		   SET tenant_id=EXCLUDED.tenant_id,
		       command_id=EXCLUDED.command_id,
		       updated_at=clock_timestamp()`,
		scheduleCommandRecoveryWorkerKey, tenantID, parsedCommandID,
	); err != nil {
		return types.NewAppError(
			types.CodeDatabase, "保存任务命令恢复游标", err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(
			types.CodeDatabase, "提交任务命令恢复游标", err,
		)
	}
	return nil
}
