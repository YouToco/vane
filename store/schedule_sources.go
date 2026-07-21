package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

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
		    AND EXISTS (
		        SELECT 1 FROM schedules s
		         WHERE s.id = $1 AND s.user_id = $3
		           AND `+matureSchedulePredicate+`
		    )`,
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
		  WHERE EXISTS (
		      SELECT 1 FROM schedules s
		       WHERE s.id = $1 AND s.user_id = $3
		         AND `+matureSchedulePredicate+`
		  )
		 ON CONFLICT (schedule_id, source_id) DO NOTHING`,
		scheduleID, sourceIDs, userID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入任务源链接（schedule_id=%s）", scheduleID), err)
	}
	return nil
}

// ScheduleHasSources 判断某任务是否已绑定「任务↔源」链接（P1b b3 的分流开关）：
// 有链接=手册编译出了源的情报任务 → Fetch/候选按本任务的源隔离；无链接=老任务/空手册任务
// → 退回用户级语义（决策 #4「老任务暂保持现状」）。生产里没有带源手册的任务时本方法恒 false，
// b3 的行为切换对存量推送**休眠不生效**（只有真建了带源的情报任务才走隔离路径）。
func (s *Store) ScheduleHasSources(ctx context.Context, scheduleID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM schedule_sources WHERE schedule_id = $1)`, scheduleID).Scan(&exists); err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("判断任务是否有源链接（schedule_id=%s）", scheduleID), err)
	}
	return exists, nil
}

// ListDueSourcesBySchedule 返回本任务链接的、且信源 active、已到抓取时间的源（P1b b3）：
// 与 ListDueSourcesByUser 同款 due 过滤（重复计费护栏），只是把 JOIN subscriptions（用户订阅）
// 换成 JOIN schedule_sources（本任务的软范围源）。这就是「只按本任务手册抓」的抓取侧落地。
// 无归属 userID 参数：schedule_sources 已把范围锚死在该任务，任务归属由调用链（PushParams）保证。
func (s *Store) ListDueSourcesBySchedule(ctx context.Context, scheduleID string) ([]types.Source, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+sourceColumns+`
		 FROM sources s
		 JOIN schedule_sources ss ON ss.source_id = s.id
		 WHERE ss.schedule_id = $1 AND s.status = $2
		   AND s.next_fetch_at <= now()
		 ORDER BY s.id`,
		scheduleID, types.SourceStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务到期源（schedule_id=%s）", scheduleID), err)
	}
	defer rows.Close()

	var out []types.Source
	for rows.Next() {
		var src types.Source
		if err := scanSource(rows, &src); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 source 行", err)
		}
		out = append(out, src)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 source 结果集", err)
	}
	return out, nil
}

// ScheduleSourceForContent 返回某内容在本任务源集里的实际出现源——content_sources ∩
// schedule_sources 的交集（P1b #8 卡片源归属修复）。
//
// 为什么需要：content_items.source_id 是**全局首发源**（谁先发现这条内容，插入后 ON CONFLICT
// DO NOTHING 永不更新），与「本任务通过哪个源看到它」无关。隔离任务候选走 content_sources ∩
// schedule_sources 选出，但构卡若用首发源标名，会给隔离任务的卡打上一个该任务根本不含的源名
// （如任务经私有 Exa 源命中，卡却标用户订阅的另一个源），违背隔离对用户呈现的一致性。构卡时改
// 用本方法取命中源。
//
// 无交集返回 (0,false)：理论上不该发生（候选正是这么选出来的），调用方回退首发源即可。
// 多个命中取 source_id 最小，保证确定性。
func (s *Store) ScheduleSourceForContent(ctx context.Context, contentItemID int64, scheduleID string) (int64, bool, error) {
	var sid int64
	err := s.pool.QueryRow(ctx,
		`SELECT cs.source_id
		   FROM content_sources cs
		   JOIN schedule_sources ss ON ss.source_id = cs.source_id
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

// ListScheduleSourceIDs 返回某任务当前绑定的源 id 列表（归属校验进 WHERE）。
// b2 供测试与 view 渲染用；b3 的 Fetch/候选隔离会据此取本任务的源。
func (s *Store) ListScheduleSourceIDs(ctx context.Context, userID int64, scheduleID string) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT ss.source_id
		   FROM schedule_sources ss
		   JOIN schedules s ON s.id = ss.schedule_id
		  WHERE ss.schedule_id = $1 AND s.user_id = $2
		    AND `+matureSchedulePredicate+`
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
