package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// UpsertSchedulePlaybook 写入/更新某定时任务的手册正文（P0 只管 content），服务三条路径：
// create_schedule 创建即初始化、edit_task_playbook 修改、老任务补手册（决策 #4）。
//
// 归属与存在性由 SQL 内的 schedules 子查询一并把关（沿用 EnableSource 先例，归属谓词进
// WHERE）：目标 schedule 不存在、或不属于 userID 时，INSERT...SELECT 的 SELECT 产 0 行 →
// 不插入 → RowsAffected==0 → 返回 ok=false（未写任何行），绝不误建/误改他人任务的手册。
// ok=true 表示确实写了一行（新建或更新）。err 只在基础设施失败时非 nil。
//
// fetch_plan 是 P1 字段，本方法**不触碰**：INSERT 路径走列 DEFAULT '{}'，ON CONFLICT
// 更新路径只改 content 与 updated_at，保留既有 fetch_plan 不被清空。
func (s *Store) UpsertSchedulePlaybook(ctx context.Context, userID int64, scheduleID, content string) (ok bool, err error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO schedule_playbooks (schedule_id, content, updated_at)
		 SELECT $1, $2, now()
		   FROM schedules
		  WHERE id = $1 AND user_id = $3
		 ON CONFLICT (schedule_id)
		 DO UPDATE SET content = EXCLUDED.content, updated_at = now()`,
		scheduleID, content, userID)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入任务手册（schedule_id=%s）", scheduleID), err)
	}
	return tag.RowsAffected() > 0, nil
}

// GetSchedulePlaybook 读某任务的手册，带 userID 做归属校验（JOIN schedules）。
// 不存在或不属于该用户 → CodeNotFound（errors.Is(err, types.ErrNotFound) 可命中，
// 不泄露他人任务存在性）。基础设施故障 → CodeDatabase。
func (s *Store) GetSchedulePlaybook(ctx context.Context, userID int64, scheduleID string) (*types.SchedulePlaybook, error) {
	var pb types.SchedulePlaybook
	err := s.pool.QueryRow(ctx,
		`SELECT p.schedule_id, p.content, p.fetch_plan, p.updated_at
		   FROM schedule_playbooks p
		   JOIN schedules s ON s.id = p.schedule_id
		  WHERE p.schedule_id = $1 AND s.user_id = $2`,
		scheduleID, userID).Scan(&pb.ScheduleID, &pb.Content, &pb.FetchPlan, &pb.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("任务手册 schedule_id=%s 不存在或不属于用户 %d", scheduleID, userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务手册（schedule_id=%s）", scheduleID), err)
	}
	return &pb, nil
}
