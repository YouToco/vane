package store

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/types"
)

// ReplaceScheduleSources 把某任务的「任务↔源」软范围链接整体替换为 sourceIDs（任务手册
// P1b b2）：删掉不再引用的、加上新引用的，最终 schedule_sources 里该任务的链接恰为 sourceIDs。
//
// 归属门禁进 SQL（沿用 UpsertSchedulePlaybook/EnableSource 先例）：两条语句都以
// `EXISTS(SELECT 1 FROM schedules WHERE id=$1 AND user_id=$3)` 把关——任务不属于该用户时
// 删/插都是 0 行，绝不动他人任务的链接。sourceIDs 为空（手册无可用源）→ 删光该任务全部链接。
//
// 不用事务（与本包全体一致：无 tx，靠幂等/自愈）：DELETE 与 INSERT 两条独立语句，中途失败
// 最坏留下"少了新链接"的部分状态；b2 阶段 schedule_sources 尚无人消费（b3 才读），且下次
// 改手册会重跑本方法自愈，故非原子可接受。source_id 的存在性由调用方（GetOrCreateSource
// 先材料化）保证，且 FK 兜底。
func (s *Store) ReplaceScheduleSources(ctx context.Context, userID int64, scheduleID string, sourceIDs []int64) error {
	if sourceIDs == nil {
		sourceIDs = []int64{}
	}
	// 删除本任务不再引用的链接。sourceIDs 为空时 `<> ALL('{}')` 恒真 → 删光。
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM schedule_sources
		  WHERE schedule_id = $1
		    AND source_id <> ALL($2)
		    AND EXISTS (SELECT 1 FROM schedules WHERE id = $1 AND user_id = $3)`,
		scheduleID, sourceIDs, userID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("清理任务源链接（schedule_id=%s）", scheduleID), err)
	}
	if len(sourceIDs) == 0 {
		return nil
	}
	// 插入新链接：WHERE EXISTS 门禁——任务非本人时零行插入。ON CONFLICT 已存在的链接跳过。
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO schedule_sources (schedule_id, source_id)
		 SELECT $1, sid
		   FROM unnest($2::bigint[]) AS sid
		  WHERE EXISTS (SELECT 1 FROM schedules WHERE id = $1 AND user_id = $3)
		 ON CONFLICT (schedule_id, source_id) DO NOTHING`,
		scheduleID, sourceIDs, userID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入任务源链接（schedule_id=%s）", scheduleID), err)
	}
	return nil
}

// ListScheduleSourceIDs 返回某任务当前绑定的源 id 列表（归属校验进 WHERE）。
// b2 供测试与 view 渲染用；b3 的 Fetch/候选隔离会据此取本任务的源。
func (s *Store) ListScheduleSourceIDs(ctx context.Context, userID int64, scheduleID string) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT ss.source_id
		   FROM schedule_sources ss
		   JOIN schedules sc ON sc.id = ss.schedule_id
		  WHERE ss.schedule_id = $1 AND sc.user_id = $2
		  ORDER BY ss.source_id`,
		scheduleID, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务源链接（schedule_id=%s）", scheduleID), err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 schedule_source 行", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 schedule_source 结果集", err)
	}
	return out, nil
}
