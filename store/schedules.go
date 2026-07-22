package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// scheduleColumns 是 schedules 表全列，SELECT 与 scanSchedule 一一对应。
const scheduleColumns = `id, tenant_id, user_id, nl_description, spec_json, scope_json, status, created_at, updated_at`

// matureSchedulePredicate requires the outer schedules table to use alias s.
// A v1 aggregate is user-manageable only after its operation is both
// executed/completed; v0/legacy schedules have no matching versioned row and
// therefore remain visible.
const matureSchedulePredicate = `NOT EXISTS (
	SELECT 1
	  FROM pending_actions p
	 WHERE p.task_id = s.id
	   AND p.tenant_id = s.tenant_id AND p.user_id = s.user_id
	   AND p.tool_name = 'create_schedule' AND p.execution_version = 1
	   AND NOT (p.status = 'executed' AND p.phase = 'completed')
)`

// scanSchedule 把一行 schedules 扫进 types.Schedule（复用于单行与多行）。
func scanSchedule(row pgx.Row, sc *types.Schedule) error {
	return row.Scan(
		&sc.ID, &sc.TenantID, &sc.UserID, &sc.NLDescription, &sc.SpecJSON, &sc.ScopeJSON,
		&sc.Status, &sc.CreatedAt, &sc.UpdatedAt,
	)
}

// InsertSchedule 写入调度镜像。scheduler 在 Temporal Create 成功后调用本方法，
// 使 Postgres 侧持有一份可供 /api/schedules 列表读取与对账的副本。
// spec_json / scope_json NOT NULL DEFAULT '{}'，nil 归一为 '{}'；status 默认 active。
func (s *Store) InsertSchedule(ctx context.Context, sc *types.Schedule) error {
	if sc == nil || sc.UserID <= 0 {
		return types.NewAppError(types.CodeValidation, "调度镜像与用户归属不得为空", nil)
	}
	spec := sc.SpecJSON
	if len(spec) == 0 {
		spec = json.RawMessage("{}")
	}
	scope := sc.ScopeJSON
	if len(scope) == 0 {
		scope = json.RawMessage("{}")
	}
	status := sc.Status
	if status == "" {
		status = types.ScheduleStatusActive
	}
	if status != types.ScheduleStatusActive {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO schedules (id, tenant_id, user_id, nl_description, spec_json, scope_json, status)
			 VALUES ($1, `+tenantOfUser+`$2), $2, $3, $4, $5, $6)`,
			sc.ID, sc.UserID, sc.NLDescription, spec, scope, status)
		if err != nil {
			return types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("插入调度镜像（id=%s）", sc.ID), err)
		}
		return nil
	}

	// Legacy Scheduler.CreatePush still reaches this method while A5 is being
	// rolled out. Its preflight list cannot serialize with an A5 reservation, so
	// enforce the same user-wide limit again at the final database write. If the
	// race is lost, Scheduler's existing compensation deletes its Temporal row.
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("开始插入调度镜像事务（id=%s）", sc.ID), err)
	}
	defer rollbackTaskCreationTransaction(ctx, tx)
	if err := lockTaskCapacityUser(ctx, tx, sc.UserID); err != nil {
		return err
	}
	used, err := countTaskCreationCapacity(ctx, tx, sc.UserID)
	if err != nil {
		return err
	}
	if used >= maxActiveTasksPerUser {
		return types.NewAppError(types.CodeValidation,
			fmt.Sprintf("活跃定时任务已达上限（%d 个）", maxActiveTasksPerUser),
			types.ErrTaskCreationLimit)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO schedules (id, tenant_id, user_id, nl_description, spec_json, scope_json, status)
		 VALUES ($1, `+tenantOfUser+`$2), $2, $3, $4, $5, $6)`,
		sc.ID, sc.UserID, sc.NLDescription, spec, scope, status)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入调度镜像（id=%s）", sc.ID), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("提交调度镜像事务（id=%s）", sc.ID), err)
	}
	return nil
}

// ListSchedulesByUser 返回该用户已兑现的调度镜像，按创建时间倒序。
// A5 在 Temporal paused Ensure 与最终激活之间会短暂写入 provisioning
// aggregate；只有关联 v1 operation 同时 reached executed/completed 才向用户
// 可见。Legacy/v0 行没有该关联，普通用户主动 paused 的成熟任务也照常显示。
func (s *Store) ListSchedulesByUser(ctx context.Context, userID int64) ([]types.Schedule, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+scheduleColumns+`
		 FROM schedules s
		 WHERE s.user_id = $1
		   AND `+matureSchedulePredicate+`
		 ORDER BY s.created_at DESC`, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的调度", userID), err)
	}
	defer rows.Close()

	var out []types.Schedule
	for rows.Next() {
		var sc types.Schedule
		if err := scanSchedule(rows, &sc); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 schedule 行", err)
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 schedule 结果集", err)
	}
	return out, nil
}

// AuthorizeScheduledRun is the final database gate before a scheduled
// workflow may spend money or touch the network. It proves that the exact
// schedule is active, user-manageable (including completed A5 provisioning),
// and still belongs to an active tenant membership. Missing/revoked/paused
// rows are a normal false result; infrastructure failures are retryable errors.
func (s *Store) AuthorizeScheduledRun(
	ctx context.Context,
	scheduleID string,
	userID int64,
) (bool, error) {
	if strings.TrimSpace(scheduleID) == "" || scheduleID != strings.TrimSpace(scheduleID) ||
		len(scheduleID) > 255 || userID <= 0 {
		return false, types.NewAppError(types.CodeValidation,
			"调度运行授权参数无效", types.ErrValidation)
	}
	var authorized bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1
		      FROM schedules s
		      JOIN tenants t ON t.id = s.tenant_id
		      JOIN memberships m
		        ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		     WHERE s.id = $1 AND s.user_id = $2 AND s.status = $3
		       AND t.status = 'active' AND t.deleted_at IS NULL
		       AND `+matureSchedulePredicate+`
		)`,
		scheduleID, userID, types.ScheduleStatusActive,
	).Scan(&authorized); err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("校验调度运行授权（id=%s）", scheduleID), err)
	}
	return authorized, nil
}

// ListActiveSchedules 返回全部 active 调度镜像（跨用户），按创建时间正序。
// 无 user 谓词是刻意的：唯一调用方是启动时的 scheduler.ReconcileActions，它要把
// **所有**存量调度的 Temporal Action 入参补齐（决策 #4 的"补手册→自包含"迁移路径），
// 是系统级维护而非用户请求。单 owner MVP 下活跃调度 ≤20，无分页压力。
func (s *Store) ListActiveSchedules(ctx context.Context) ([]types.Schedule, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+scheduleColumns+`
		 FROM schedules WHERE status = $1
		 ORDER BY created_at`, types.ScheduleStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询全部 active 调度", err)
	}
	defer rows.Close()

	var out []types.Schedule
	for rows.Next() {
		var sc types.Schedule
		if err := scanSchedule(rows, &sc); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 schedule 行", err)
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 schedule 结果集", err)
	}
	return out, nil
}

// UpdateScheduleSpec 原地更新调度镜像的 spec_json（可选连带 nl_description），并推进
// updated_at。scheduler 在 Temporal Update 成功后调用，使镜像与 Temporal 保持一致。
//
// nlDesc 为 nil 表示"不改描述"（COALESCE 保留原值）；指向空串表示显式清空。
//
// 与 DeleteSchedule 的幂等语义**刻意不同**：这里 0 行受影响返回 CodeNotFound 而不是
// 静默成功。删一个不存在的调度是无害的终态；而更新一个镜像里不存在的调度意味着
// **Temporal 有、镜像没有**——调用方（scheduler）刚刚在 Temporal 侧改成功了，镜像却
// 没这行，这是必须出声的漂移，不能当成功咽掉。
func (s *Store) UpdateScheduleSpec(ctx context.Context, id string, spec json.RawMessage, nlDesc *string) error {
	if len(spec) == 0 {
		spec = json.RawMessage("{}")
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE schedules s
		    SET spec_json = $2,
		        nl_description = COALESCE($3, nl_description),
		        updated_at = now()
		  WHERE id = $1
		    AND `+matureSchedulePredicate,
		id, spec, nlDesc)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新调度镜像（id=%s）", id), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("调度 id=%s 不存在（镜像与 Temporal 漂移）", id), nil)
	}
	return nil
}

// DeleteSchedule 删除调度镜像行。scheduler 在 Temporal Delete 成功后调用；
// 幂等：删不存在的 id 不报错（无行受影响）。
// DeleteSchedule 删除调度镜像。**归属校验在 WHERE 谓词内完成**——
// 越权请求完全无副作用，而不是先查再判（那有 TOCTOU 窗口，且容易漏判）。
// 范式取自 store/agent.go 的 ClaimPendingAction，是全仓归属校验做得最好的一处。
//
// 找不到行时返回 CodeNotFound 而非「无权限」：不区分「不存在」与「不属于你」，
// 否则调用方可用它枚举他人调度 id 是否存在。
func (s *Store) DeleteSchedule(ctx context.Context, id string, userID int64) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM schedules s
		  WHERE s.id = $1 AND s.user_id = $2
		    AND `+matureSchedulePredicate, id, userID)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("删除调度镜像（id=%s）", id), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("调度 id=%s 不存在", id), nil)
	}
	return nil
}

// GetSchedule 按 id 读取单个调度镜像；不存在时返回 CodeNotFound 的 AppError，
// 调用方可用 errors.Is(err, types.ErrNotFound) 命中。
func (s *Store) GetSchedule(ctx context.Context, id string, userID int64) (*types.Schedule, error) {
	var sc types.Schedule
	err := scanSchedule(
		s.pool.QueryRow(ctx,
			`SELECT `+scheduleColumns+`
			   FROM schedules s
			  WHERE s.id = $1 AND s.user_id = $2
			    AND `+matureSchedulePredicate,
			id, userID),
		&sc)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("调度 id=%s 不存在", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询调度（id=%s）", id), err)
	}
	return &sc, nil
}

// GetScheduleStrictness 读取任务的推送门槛档位（migration 025）。
//
// 独立窄方法而非并入 scheduleColumns/scanSchedule：唯一消费方是推送管道的
// Select Activity（热路径上一次单列点查），把列并进全列扫描会迫使所有列表 API
// 一起感知这个纯推送侧的字段。NULL（未设置）返回空串，由调用方按
// types.DefaultStrictness 兜底——"没设"与"要宽松"的区分保留到最后一刻。
// 不校验归属（无 userID 谓词）：调用方是 workflow 内部路径，schedule_id 来自
// Temporal 入参而非用户输入；返回的也只是一个档位枚举，无泄露面。
// 行不存在返回空串同样走兜底：对已删调度的迟到触发，放行到兜底比报错中断推送更对。
func (s *Store) GetScheduleStrictness(ctx context.Context, scheduleID string) (types.PushStrictness, error) {
	var v *string
	err := s.pool.QueryRow(ctx,
		`SELECT push_strictness FROM schedules WHERE id = $1`, scheduleID).Scan(&v)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询调度门槛档位（id=%s）", scheduleID), err)
	}
	if v == nil {
		return "", nil
	}
	return types.PushStrictness(*v), nil
}

// SetScheduleStrictness 设置任务的推送门槛档位（agent 工具 set_task_strictness 用）。
// 归属校验在 WHERE 谓词内（同 DeleteSchedule 范式）；找不到行返回 CodeNotFound，
// 不区分"不存在"与"不属于你"。档位合法性由调用方（工具层 + DB CHECK 约束）双守，
// 这里不再重复校验——真穿透到这层，CHECK 约束会以 CodeDatabase 拒绝。
func (s *Store) SetScheduleStrictness(ctx context.Context, scheduleID string, userID int64, v types.PushStrictness) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE schedules s
		    SET push_strictness = $1, updated_at = now()
		  WHERE s.id = $2 AND s.user_id = $3
		    AND `+matureSchedulePredicate,
		string(v), scheduleID, userID)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新调度门槛档位（id=%s）", scheduleID), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("调度 id=%s 不存在", scheduleID), nil)
	}
	return nil
}
