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

// DeleteSchedule 删除调度镜像行。scheduler 在 Temporal Delete 成功后调用；
// 幂等：删不存在的 id 不报错（无行受影响）。
func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, id)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("删除调度镜像（id=%s）", id), err)
	}
	return nil
}

// GetSchedule 按 id 读取单个调度镜像；不存在时返回 CodeNotFound 的 AppError，
// 调用方可用 errors.Is(err, types.ErrNotFound) 命中。
func (s *Store) GetSchedule(ctx context.Context, id string) (*types.Schedule, error) {
	var sc types.Schedule
	err := scanSchedule(
		s.pool.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM schedules WHERE id = $1`, id),
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
