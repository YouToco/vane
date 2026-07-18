package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// scheduleColumns 是 schedules 表全列，SELECT 与 scanSchedule 一一对应。
const scheduleColumns = `id, user_id, nl_description, spec_json, scope_json, status, created_at, updated_at`

// scanSchedule 把一行 schedules 扫进 types.Schedule（复用于单行与多行）。
func scanSchedule(row pgx.Row, sc *types.Schedule) error {
	return row.Scan(
		&sc.ID, &sc.UserID, &sc.NLDescription, &sc.SpecJSON, &sc.ScopeJSON,
		&sc.Status, &sc.CreatedAt, &sc.UpdatedAt,
	)
}

// InsertSchedule 写入调度镜像。scheduler 在 Temporal Create 成功后调用本方法，
// 使 Postgres 侧持有一份可供 /api/schedules 列表读取与对账的副本。
// spec_json / scope_json NOT NULL DEFAULT '{}'，nil 归一为 '{}'；status 默认 active。
func (s *Store) InsertSchedule(ctx context.Context, sc *types.Schedule) error {
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
	_, err := s.pool.Exec(ctx,
		`INSERT INTO schedules (id, user_id, nl_description, spec_json, scope_json, status)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		sc.ID, sc.UserID, sc.NLDescription, spec, scope, status)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入调度镜像（id=%s）", sc.ID), err)
	}
	return nil
}

// ListSchedulesByUser 返回该用户的全部调度镜像，按创建时间倒序。
func (s *Store) ListSchedulesByUser(ctx context.Context, userID int64) ([]types.Schedule, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+scheduleColumns+`
		 FROM schedules WHERE user_id = $1
		 ORDER BY created_at DESC`, userID)
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
		`UPDATE schedules
		    SET spec_json = $2,
		        nl_description = COALESCE($3, nl_description),
		        updated_at = now()
		  WHERE id = $1`,
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
		`DELETE FROM schedules WHERE id = $1 AND user_id = $2`, id, userID)
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
		s.pool.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM schedules WHERE id = $1 AND user_id = $2`, id, userID),
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
