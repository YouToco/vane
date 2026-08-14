package store

import (
	"context"
	"fmt"
	"time"

	"github.com/YouToco/vane/types"
)

// ListFeedbacksForEvolution 慢通道输入：取该用户 id > afterID 的反馈（id 升序，
// limit 截断），JOIN 出演化最小上下文（当时打分 + 内容标题 + 正文前 200 字符）。
// afterID 即 profiles.last_evolved_feedback_id 游标，严格大于——游标行本身已消费。
// LEFT JOIN content_items：内容被 TTL 清理（content_item_id 置 NULL）时
// ContentTitle/ContentExcerpt 为空串，反馈行本身保留（反馈事实不随内容清理消失）。
//
// 旧卡的 👎 曾先追加 detail 为空的 not_interested，再由原因面板追加 typed
// misjudged。对画像学习而言，后一个问题原因会取代同租户、用户、投递上
// 更早的旧 UI 空白 not_interested；带 detail 的明确反馈和全部事实行仍保留。
// superseding 行的查找刻意不受 afterID/limit 限制，否则配对行跨页时旧负兴趣
// 仍会先污染画像。reason_code 非空即 typed 且合法，由 migration 040 CHECK
// 作为六种原因的单一真相源。
func (s *Store) ListFeedbacksForEvolution(ctx context.Context, userID int64, afterID int64, limit int) ([]types.FeedbackWithContent, error) {
	return s.listFeedbacksForEvolution(ctx, 0, userID, afterID, limit)
}

// ListFeedbacksForEvolutionForTenant pins both feedback and delivery rows to
// the snapshot tenant. This prevents a multi-membership user from feeding a
// compiled run with another tenant's delivery history.
func (s *Store) ListFeedbacksForEvolutionForTenant(
	ctx context.Context,
	tenantID int64,
	userID int64,
	afterID int64,
	limit int,
) ([]types.FeedbackWithContent, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, types.NewAppError(types.CodeValidation,
			"待演化反馈租户范围无效", types.ErrValidation)
	}
	return s.listFeedbacksForEvolution(ctx, tenantID, userID, afterID, limit)
}

func (s *Store) listFeedbacksForEvolution(
	ctx context.Context,
	tenantID int64,
	userID int64,
	afterID int64,
	limit int,
) ([]types.FeedbackWithContent, error) {
	query := `SELECT f.id, f.user_id, f.delivery_id, f.action,
		        COALESCE(f.reason_code, ''), f.detail, f.created_at,
		        d.score,
		        COALESCE(ci.title, ''),
		        COALESCE(left(ci.content, 200), '')
		 FROM feedbacks f
		 LEFT JOIN profile_claim_states pcs
		   ON pcs.tenant_id=f.tenant_id AND pcs.user_id=f.user_id
		 LEFT JOIN profiles p
		   ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		 JOIN deliveries d ON d.id = f.delivery_id
		 LEFT JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE f.user_id = $1 AND f.id > $2
		   AND (
		     (pcs.user_id IS NOT NULL AND pcs.active_epoch=f.profile_epoch)
		     OR
		     (pcs.user_id IS NULL AND p.user_id IS NULL AND f.profile_epoch=0)
		   )
		   AND NOT (
		       f.action = 'not_interested'
		       AND btrim(f.detail) = ''
		       AND EXISTS (
		           SELECT 1
		             FROM feedbacks superseding
		            WHERE superseding.tenant_id = f.tenant_id
		              AND superseding.user_id = f.user_id
		              AND superseding.profile_epoch = f.profile_epoch
		              AND superseding.delivery_id = f.delivery_id
		              AND superseding.id > f.id
		              AND superseding.action = 'misjudged'
		              AND superseding.reason_code IS NOT NULL
		       )
		   )
		 ORDER BY f.id ASC
		 LIMIT $3`
	args := []any{userID, afterID, limit}
	if tenantID > 0 {
		query = `SELECT f.id, f.user_id, f.delivery_id, f.action,
		        COALESCE(f.reason_code, ''), f.detail, f.created_at,
		        d.score,
		        COALESCE(ci.title, ''),
		        COALESCE(left(ci.content, 200), '')
		 FROM feedbacks f
		 LEFT JOIN profile_claim_states pcs
		   ON pcs.tenant_id=f.tenant_id AND pcs.user_id=f.user_id
		 LEFT JOIN profiles p
		   ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		 JOIN deliveries d
		   ON d.id = f.delivery_id AND d.tenant_id = $1
		 LEFT JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE f.tenant_id = $1 AND f.user_id = $2 AND f.id > $3
		   AND (
		     (pcs.user_id IS NOT NULL AND pcs.active_epoch=f.profile_epoch)
		     OR
		     (pcs.user_id IS NULL AND p.user_id IS NULL AND f.profile_epoch=0)
		   )
		   AND NOT (
		       f.action = 'not_interested'
		       AND btrim(f.detail) = ''
		       AND EXISTS (
		           SELECT 1
		             FROM feedbacks superseding
		            WHERE superseding.tenant_id = f.tenant_id
		              AND superseding.user_id = f.user_id
		              AND superseding.profile_epoch = f.profile_epoch
		              AND superseding.delivery_id = f.delivery_id
		              AND superseding.id > f.id
		              AND superseding.action = 'misjudged'
		              AND superseding.reason_code IS NOT NULL
		       )
		   )
		 ORDER BY f.id ASC
		 LIMIT $4`
		args = []any{tenantID, userID, afterID, limit}
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 待演化反馈（after=%d）", userID, afterID), err)
	}
	defer rows.Close()

	var out []types.FeedbackWithContent
	for rows.Next() {
		var fc types.FeedbackWithContent
		if err := rows.Scan(
			&fc.ID, &fc.UserID, &fc.DeliveryID, &fc.Action, &fc.ReasonCode,
			&fc.Detail, &fc.CreatedAt,
			&fc.Score, &fc.ContentTitle, &fc.ContentExcerpt,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描待演化反馈行", err)
		}
		out = append(out, fc)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历待演化反馈结果集", err)
	}
	return out, nil
}

// ListRecentNegativeFeedbackTitles 快通道：since 之后 "per-delivery 最新态度为负"
// 的内容标题，按反馈时间倒序、Go 侧保序去重后截 limit。
//
// 语义（审查 F2/F7）：对每个 delivery 取 {interested, not_interested, misjudged}
// 中最新一条（DISTINCT ON + created_at DESC, id DESC），仅当该最新行
// action ∈ {not_interested, misjudged} 才计入——改主意点回「感兴趣」后
// 不得再压制该主题 14 天；misjudged 后点 interested 同理（最新表态积极，
// 不进负面清单，misjudged 仍由慢通道演化弱化）。
// 窗口谓词放子查询内是安全的：窗口外的行必早于窗口内的行，不存在
// "窗口内旧态度遮蔽窗口外新态度"的组合。
//
// JOIN content_items 取标题（INNER）：内容已清理（content_item_id NULL）的行
// 自然丢弃——无标题即无从注入打分 prompt。空标题回退正文前 200 字符
// （X 官号类无标题内容整批 title=”，2026-07-17 实测其负反馈对打分 prompt
// 完全不可见——Gate ⑥ 盲区；200 与演化通道 ListFeedbacksForEvolution 的
// excerpt 同宽，scorer 侧 SingleLine+TruncateRunes 负责渲染收窄）；
// 标题与正文都空才在 Go 侧跳过。
// 去重按注入串保序（同一标题多条 delivery 只留最新一次）；不在 SQL 加 LIMIT，
// 去重后可能不足 limit（单用户反馈量级小，全量扫窗口可接受）。
func (s *Store) ListRecentNegativeFeedbackTitles(ctx context.Context, userID int64, since time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT COALESCE(NULLIF(ci.title, ''), left(ci.content, 200))
		 FROM (
		     SELECT DISTINCT ON (f.delivery_id)
		            f.delivery_id, f.action, f.reason_code, f.created_at
		     FROM feedbacks f
		     LEFT JOIN profile_claim_states pcs
		       ON pcs.tenant_id=f.tenant_id AND pcs.user_id=f.user_id
		     LEFT JOIN profiles p
		       ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		     WHERE f.user_id = $1 AND f.created_at >= $2
		       AND f.action IN ('interested', 'not_interested', 'misjudged')
		       AND (
		         (pcs.user_id IS NOT NULL AND pcs.active_epoch=f.profile_epoch)
		         OR
		         (pcs.user_id IS NULL AND p.user_id IS NULL AND f.profile_epoch=0)
		       )
		     ORDER BY f.delivery_id, f.created_at DESC, f.id DESC
		 ) latest
		 JOIN deliveries d ON d.id = latest.delivery_id
		 JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE latest.action = 'not_interested'
		    OR (latest.action = 'misjudged' AND latest.reason_code = 'not_relevant')
		 ORDER BY latest.created_at DESC`,
		userID, since)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 近期负面反馈标题", userID), err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	var out []string
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描负面反馈标题行", err)
		}
		if title == "" {
			continue
		}
		if _, dup := seen[title]; dup {
			continue
		}
		seen[title] = struct{}{}
		out = append(out, title)
		if len(out) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历负面反馈标题结果集", err)
	}
	return out, nil
}

// ListRecentNegativeFeedbackTitlesForTenant is the compiled-runtime variant.
// Both the feedback fact and its delivery must belong to the exact frozen
// tenant/user; a second membership of the same user cannot contribute prompt
// material to this run.
func (s *Store) ListRecentNegativeFeedbackTitlesForTenant(
	ctx context.Context,
	tenantID int64,
	userID int64,
	since time.Time,
	limit int,
) ([]string, error) {
	if tenantID <= 0 || userID <= 0 || limit <= 0 {
		return nil, types.NewAppError(types.CodeValidation,
			"近期负面反馈租户范围无效", types.ErrValidation)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT COALESCE(NULLIF(ci.title, ''), left(ci.content, 200))
		 FROM (
		     SELECT DISTINCT ON (f.delivery_id)
		            f.delivery_id, f.action, f.reason_code, f.created_at
		     FROM feedbacks f
		     LEFT JOIN profile_claim_states pcs
		       ON pcs.tenant_id=f.tenant_id AND pcs.user_id=f.user_id
		     LEFT JOIN profiles p
		       ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		     WHERE f.tenant_id = $1 AND f.user_id = $2 AND f.created_at >= $3
		       AND f.action IN ('interested', 'not_interested', 'misjudged')
		       AND (
		         (pcs.user_id IS NOT NULL AND pcs.active_epoch=f.profile_epoch)
		         OR
		         (pcs.user_id IS NULL AND p.user_id IS NULL AND f.profile_epoch=0)
		       )
		     ORDER BY f.delivery_id, f.created_at DESC, f.id DESC
		 ) latest
		 JOIN deliveries d
		   ON d.id = latest.delivery_id AND d.tenant_id = $1 AND d.user_id = $2
		 JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE latest.action = 'not_interested'
		    OR (latest.action = 'misjudged' AND latest.reason_code = 'not_relevant')
		 ORDER BY latest.created_at DESC`,
		tenantID, userID, since)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询租户 %d 用户 %d 近期负面反馈标题", tenantID, userID), err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	var out []string
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"扫描精确租户负面反馈标题行", err)
		}
		if title == "" {
			continue
		}
		if _, dup := seen[title]; dup {
			continue
		}
		seen[title] = struct{}{}
		out = append(out, title)
		if len(out) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"遍历精确租户负面反馈标题结果集", err)
	}
	return out, nil
}
