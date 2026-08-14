package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// UpsertSchedulePlaybook 写入/更新某定时任务的手册正文（P0 只管 content）。
// 任务创建与任务定义编辑都维护这一份手册；它是当前唯一可编辑产品真相。
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
		   FROM schedules s
		  WHERE s.id = $1 AND s.user_id = $3
		    AND `+matureSchedulePredicate+`
		 ON CONFLICT (schedule_id)
		 DO UPDATE SET content = EXCLUDED.content, updated_at = now()`,
		scheduleID, content, userID)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入任务手册（schedule_id=%s）", scheduleID), err)
	}
	return tag.RowsAffected() > 0, nil
}

// SetFetchPlan 写入某任务手册编译出的结构化抓取计划（P1 编译层）。与 UpsertSchedulePlaybook
// 只改 content 对称：本方法**只改 fetch_plan 与 updated_at，不触碰 content**——两者各管一列，
// 编译计划失败也绝不会把手册正文冲掉。
//
// 用 UPDATE … FROM schedules 而非 upsert：fetch_plan 依附于**已存在**的手册行（create 创建即
// 初始化、edit 先 Upsert 了 content），计划不该凭空建手册行。归属+存在性一并进 WHERE
// （schedules.user_id 谓词，沿用 UpsertSchedulePlaybook/EnableSource 先例）：目标任务不存在、
// 非本人、或还没有手册行 → 匹配 0 行 → ok=false（未写任何行）。err 只在基础设施失败时非 nil。
//
// plan 为空/nil 时归一化为规范零目标计划 '{"targets":[]}'（列非空约束 + 语义明确）。
func (s *Store) SetFetchPlan(ctx context.Context, userID int64, scheduleID string, plan json.RawMessage) (ok bool, err error) {
	if len(plan) == 0 {
		plan = json.RawMessage(`{"targets":[]}`)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE schedule_playbooks p
		    SET fetch_plan = $3, updated_at = now()
		   FROM schedules s
		  WHERE p.schedule_id = $1 AND s.id = $1 AND s.user_id = $2
		    AND `+matureSchedulePredicate,
		scheduleID, userID, []byte(plan))
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入任务抓取计划（schedule_id=%s）", scheduleID), err)
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
		  WHERE p.schedule_id = $1 AND s.user_id = $2
		    AND `+matureSchedulePredicate,
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
