package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// maxProfileTags 是 tags 的库内上限（与演化守门、update_profile 人工上限统一为 12，
// 契约 §2）；profilehint 展示截 10 是刻意分层，与本上限无关。
const maxProfileTags = 12

// profileColumns 是 profiles 表全列，SELECT 与 scanProfile 一一对应。
const profileColumns = `id, user_id, industry, occupation, tags, removed_tags, summary,
	token_budget_daily, tokens_used_today, token_reset_at,
	last_evolved_feedback_id, created_at, updated_at`

// scanProfile 把一行 profiles 扫进 types.Profile（复用于单行与 RETURNING）。
func scanProfile(row pgx.Row, p *types.Profile) error {
	return row.Scan(
		&p.ID, &p.UserID, &p.Industry, &p.Occupation, &p.Tags, &p.RemovedTags, &p.Summary,
		&p.TokenBudgetDaily, &p.TokensUsedToday, &p.TokenResetAt,
		&p.LastEvolvedFeedbackID, &p.CreatedAt, &p.UpdatedAt,
	)
}

// 写路径纪律（契约 §3.1，CAS 约定前提）：除本文件三个写方法外禁止任何代码直写 profiles。
//   - 人工（UpsertProfileFields）：无条件写 + 刷 updated_at，人工恒赢；
//   - 演化（EvolveProfile / AdvanceProfileCursor）：(updated_at, 游标) 双条件 CAS，冲突即退让。

// GetProfile 按 user_id 取画像；无行返回 CodeNotFound。
func (s *Store) GetProfile(ctx context.Context, userID int64) (*types.Profile, error) {
	var p types.Profile
	err := scanProfile(s.pool.QueryRow(ctx,
		`SELECT `+profileColumns+` FROM profiles WHERE user_id = $1`, userID), &p)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("用户 %d 无画像", userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 画像", userID), err)
	}
	return &p, nil
}

// UpsertProfileFields 人工写路径（首采 2.1 与修正 2.3 共用）：nil 字段不改，
// tags 为 nil 不改、非 nil 整体替换（截前 12）。不触碰 summary/游标/token 三件套
// （summary 归演化独有，全字段覆盖会清掉演化产物——主控裁决）。
// 必须是 INSERT ... ON CONFLICT (user_id) DO UPDATE：并发首采两张确认卡同时确认时
// 后者命中 DO UPDATE 而非报错。无条件写 + updated_at=now()：人工恒赢，
// 刷 updated_at 使并发演化的 CAS 失效退让。RETURNING 全列。
//
// removed_tags 黑名单（014，Gate ⑧）：tags 非 nil 时同语句维护——
// 新黑名单 = (旧黑名单 ∪ 旧 tags) − 新 tags（集合语义）。被删的旧标签入列、
// 人工加回的出列、仍在列上的保留；单语句维护无读-改-写窗口，与并发首采/演化
// 的原子性约定一致。演化侧凭本列硬过滤"加回人工删除标签"（evolver.dropRemovedTags）。
func (s *Store) UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error) {
	if len(tags) > maxProfileTags {
		tags = tags[:maxProfileTags]
	}
	// nil 不改的实现：pgx 把 nil 指针 / nil 切片编码为 NULL，DO UPDATE 分支
	// COALESCE(NULL, 旧值) 即保持原值；INSERT 分支 COALESCE 到列默认零值。
	// 注意谓词区分的是 nil 与非 nil——非 nil 空串/空数组是合法的"置空"。
	// $4::text[] 显式转型是必须的：COALESCE 参数不继承 INSERT 目标列类型，
	// 不写会被 PG 解析为 text 报 42804。
	// removed_tags 的 EXCEPT 子查询是集合语义（自带去重）；array_agg 加 ORDER BY
	// 使黑名单顺序确定，便于测试与人读。
	var p types.Profile
	err := scanProfile(s.pool.QueryRow(ctx,
		`INSERT INTO profiles (user_id, industry, occupation, tags, updated_at)
		 VALUES ($1, COALESCE($2, ''), COALESCE($3, ''), COALESCE($4::text[], '{}'), now())
		 ON CONFLICT (user_id) DO UPDATE SET
		     industry     = COALESCE($2, profiles.industry),
		     occupation   = COALESCE($3, profiles.occupation),
		     tags         = COALESCE($4::text[], profiles.tags),
		     removed_tags = CASE WHEN $4::text[] IS NULL THEN profiles.removed_tags ELSE
		         (SELECT COALESCE(array_agg(t ORDER BY t), '{}'::text[]) FROM (
		             SELECT unnest(profiles.removed_tags || profiles.tags) AS t
		             EXCEPT
		             SELECT unnest($4::text[])
		         ) diff(t))
		     END,
		     updated_at   = now()
		 RETURNING `+profileColumns,
		userID, industry, occupation, tags), &p)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("upsert 画像字段（user=%d）", userID), err)
	}
	return &p, nil
}

// EvolveProfile 演化写：SET 列清单只有 summary/tags/last_evolved_feedback_id +
// updated_at=now()（演化输出面硬收窄，契约 §14；标签只增不减由 evolver 守门）。
// CAS 谓词 WHERE user_id AND updated_at=$expectedAt AND last_evolved_feedback_id=$expectedCursor：
// 游标入 CAS token 是因为 AdvanceProfileCursor 不刷 updated_at——若不校验游标，
// 慢演化写回会把已推进的游标回退、反馈被二次消费（审查 F6）。
// 0 行命中返回 CodeConflict，调用方丢弃本次演化（游标不动，下轮在新画像上重新消费）。
func (s *Store) EvolveProfile(ctx context.Context, userID int64, summary string, tags []string, newCursor int64, expectedAt time.Time, expectedCursor int64) error {
	// tags 列 NOT NULL：nil 归一为空数组，避免 pgx 编码 NULL 触发约束错误。
	if tags == nil {
		tags = []string{}
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE profiles
		 SET summary = $2, tags = $3, last_evolved_feedback_id = $4, updated_at = now()
		 WHERE user_id = $1 AND updated_at = $5 AND last_evolved_feedback_id = $6`,
		userID, summary, tags, newCursor, expectedAt, expectedCursor)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("演化写画像（user=%d）", userID), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("画像演化 CAS 未命中（user=%d）：画像已被并发修改或游标已推进", userID), nil)
	}
	return nil
}

// AdvanceProfileCursor 只推进游标：不动画像内容、刻意不刷 updated_at（游标推进
// 不是"画像变更"，刷了会把并发人工修正的 CAS 语义搅浑）。CAS 谓词同 EvolveProfile
// （updated_at + 旧游标双条件），冲突返回 CodeConflict 由调用方静默跳过。
// 用途：演化"语义失败"时标记该批反馈已消费防死循环（契约 §9）。
func (s *Store) AdvanceProfileCursor(ctx context.Context, userID int64, newCursor int64, expectedAt time.Time, expectedCursor int64) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE profiles
		 SET last_evolved_feedback_id = $2
		 WHERE user_id = $1 AND updated_at = $3 AND last_evolved_feedback_id = $4`,
		userID, newCursor, expectedAt, expectedCursor)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("推进画像演化游标（user=%d）", userID), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("画像游标推进 CAS 未命中（user=%d）：画像已被并发修改或游标已推进", userID), nil)
	}
	return nil
}
