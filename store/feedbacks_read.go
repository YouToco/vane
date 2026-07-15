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
func (s *Store) ListFeedbacksForEvolution(ctx context.Context, userID int64, afterID int64, limit int) ([]types.FeedbackWithContent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT f.id, f.user_id, f.delivery_id, f.action, f.detail, f.created_at,
		        d.score,
		        COALESCE(ci.title, ''),
		        COALESCE(left(ci.content, 200), '')
		 FROM feedbacks f
		 JOIN deliveries d ON d.id = f.delivery_id
		 LEFT JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE f.user_id = $1 AND f.id > $2
		 ORDER BY f.id ASC
		 LIMIT $3`,
		userID, afterID, limit)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 待演化反馈（after=%d）", userID, afterID), err)
	}
	defer rows.Close()

	var out []types.FeedbackWithContent
	for rows.Next() {
		var fc types.FeedbackWithContent
		if err := rows.Scan(
			&fc.ID, &fc.UserID, &fc.DeliveryID, &fc.Action, &fc.Detail, &fc.CreatedAt,
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
// 自然丢弃——无标题即无从注入打分 prompt；空标题同理在 Go 侧跳过。
// 去重按标题保序（同一标题多条 delivery 只留最新一次）；不在 SQL 加 LIMIT，
// 去重后可能不足 limit（单用户反馈量级小，全量扫窗口可接受）。
func (s *Store) ListRecentNegativeFeedbackTitles(ctx context.Context, userID int64, since time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT ci.title
		 FROM (
		     SELECT DISTINCT ON (f.delivery_id)
		            f.delivery_id, f.action, f.created_at
		     FROM feedbacks f
		     WHERE f.user_id = $1 AND f.created_at >= $2
		       AND f.action IN ('interested', 'not_interested', 'misjudged')
		     ORDER BY f.delivery_id, f.created_at DESC, f.id DESC
		 ) latest
		 JOIN deliveries d ON d.id = latest.delivery_id
		 JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE latest.action IN ('not_interested', 'misjudged')
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
