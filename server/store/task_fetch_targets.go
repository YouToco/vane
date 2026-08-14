// Task-scoped fetch-target persistence.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// ReplaceTaskFetchTargets atomically replaces one task's complete fetch-target
// set. Re-approving a target also clears an automatic failure pause; recovery
// therefore happens through a task edit, never through source CRUD.
func (s *Store) ReplaceTaskFetchTargets(ctx context.Context, userID int64, scheduleID string, targetIDs []int64) error {
	if s.legacyAdmissionIsClosed() {
		return legacyAdmissionClosed("task fetch targets")
	}
	if targetIDs == nil {
		targetIDs = []int64{}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开始替换任务抓取目标", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`DELETE FROM task_fetch_targets
		  WHERE schedule_id = $1
		    AND fetch_target_id <> ALL($2)
		    AND EXISTS (
		        SELECT 1 FROM schedules s
		         WHERE s.id = $1 AND s.user_id = $3
		           AND `+matureSchedulePredicate+`
		    )`,
		scheduleID, targetIDs, userID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("清理任务抓取目标链接（schedule_id=%s）", scheduleID), err)
	}
	if len(targetIDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return types.NewAppError(types.CodeDatabase,
				"提交清空任务抓取目标", err)
		}
		return nil
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO task_fetch_targets (schedule_id, fetch_target_id)
		 SELECT $1, sid
		   FROM unnest($2::bigint[]) AS sid
		  WHERE EXISTS (
		      SELECT 1 FROM schedules s
		       WHERE s.id = $1 AND s.user_id = $3
		         AND `+matureSchedulePredicate+`
		  )
		 ON CONFLICT (schedule_id, fetch_target_id) DO NOTHING`,
		scheduleID, targetIDs, userID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入任务抓取目标链接（schedule_id=%s）", scheduleID), err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE fetch_targets ft
		    SET status=$4, fail_count=0, next_fetch_at=now(), updated_at=now()
		  WHERE ft.id=ANY($2)
		    AND EXISTS (
		        SELECT 1
		          FROM task_fetch_targets tft
		          JOIN schedules s ON s.id=tft.schedule_id
		         WHERE tft.schedule_id=$1
		           AND tft.fetch_target_id=ft.id
		           AND s.user_id=$3
		           AND `+matureSchedulePredicate+`
		    )`,
		scheduleID, targetIDs, userID, types.FetchTargetStatusActive); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("恢复任务抓取目标（schedule_id=%s）", scheduleID), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交替换任务抓取目标", err)
	}
	return nil
}

// ListDueFetchTargetsByTask returns this task's active targets that are due.
// due 过滤是重复计费护栏，范围只来自任务自己的 fetch target 集合。
// 无归属 userID 参数：task_fetch_targets 已把范围锚死在该任务，任务归属由调用链保证。
func (s *Store) ListDueFetchTargetsByTask(ctx context.Context, scheduleID string) ([]types.FetchTarget, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+fetchTargetColumns+`
		 FROM fetch_targets s
		 JOIN task_fetch_targets ss ON ss.fetch_target_id = s.id
		 WHERE ss.schedule_id = $1 AND s.status = $2
		   AND s.next_fetch_at <= now()
		 ORDER BY s.id`,
		scheduleID, types.FetchTargetStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务到期抓取目标（schedule_id=%s）", scheduleID), err)
	}
	defer rows.Close()

	var out []types.FetchTarget
	for rows.Next() {
		var src types.FetchTarget
		if err := scanFetchTarget(rows, &src); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 fetch_target 行", err)
		}
		out = append(out, src)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 fetch_target 结果集", err)
	}
	return out, nil
}

// TaskFetchTargetForContent 返回某内容在本任务源集里的实际出现源——content_sources ∩
// task_fetch_targets 的交集（P1b #8 卡片源归属修复）。
//
// 为什么需要：content_items.source_id 是**全局首发源**（谁先发现这条内容，插入后 ON CONFLICT
// DO NOTHING 永不更新），与「本任务通过哪个源看到它」无关。隔离任务候选走 content_sources ∩
// task_fetch_targets 选出，但构卡若用首发源标名，会给隔离任务的卡打上一个该任务根本不含的源名
// （如任务经私有 Exa 源命中，卡却标用户订阅的另一个源），违背隔离对用户呈现的一致性。构卡时改
// 用本方法取命中源。
//
// 无交集返回 (0,false)：理论上不该发生（候选正是这么选出来的），调用方回退首发源即可。
// 多个命中取 source_id 最小，保证确定性。
func (s *Store) TaskFetchTargetForContent(ctx context.Context, contentItemID int64, scheduleID string) (int64, bool, error) {
	var sid int64
	err := s.pool.QueryRow(ctx,
		`SELECT cs.source_id
		   FROM content_sources cs
		   JOIN task_fetch_targets ss ON ss.fetch_target_id = cs.source_id
		  WHERE cs.content_item_id = $1 AND ss.schedule_id = $2
		  ORDER BY cs.source_id
		  LIMIT 1`,
		contentItemID, scheduleID).Scan(&sid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询内容在任务源集的命中源（content_item=%d, schedule_id=%s）", contentItemID, scheduleID), err)
	}
	return sid, true, nil
}

// ListTaskFetchTargetIDs returns the exact internal target projection for a task.
func (s *Store) ListTaskFetchTargetIDs(ctx context.Context, userID int64, scheduleID string) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT ss.fetch_target_id
		   FROM task_fetch_targets ss
		   JOIN schedules s ON s.id = ss.schedule_id
		  WHERE ss.schedule_id = $1 AND s.user_id = $2
		    AND `+matureSchedulePredicate+`
		  ORDER BY ss.fetch_target_id`,
		scheduleID, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务抓取目标链接（schedule_id=%s）", scheduleID), err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 task_fetch_target 行", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 task_fetch_target 结果集", err)
	}
	return out, nil
}
