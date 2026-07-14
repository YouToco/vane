package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// InsertContentItemIfNew 按 (source_id, external_id) 去重插入内容条目。
// 返回 (id, isNew, err)：首插返回新 id 且 isNew=true；命中既有条目返回其 id 且 isNew=false。
//
// 实现：ON CONFLICT DO NOTHING + RETURNING id。插入成功时 RETURNING 有行；命中冲突时
// 无行返回（pgx.ErrNoRows），再按唯一键补查既有 id。content_hash 精确指纹与 simhash
// 近似指纹由抓取方算好后随 item 带入，本方法只负责落库与去重判定。
func (s *Store) InsertContentItemIfNew(ctx context.Context, item *types.ContentItem) (id int64, isNew bool, err error) {
	err = s.pool.QueryRow(ctx,
		`INSERT INTO content_items (
			source_id, external_id, url, title, content, author,
			published_at, content_hash, simhash
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (source_id, external_id) DO NOTHING
		RETURNING id`,
		item.SourceID, item.ExternalID, item.URL, item.Title, item.Content, item.Author,
		item.PublishedAt, item.ContentHash, item.Simhash,
	).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入内容条目（source=%d, external_id=%s）", item.SourceID, item.ExternalID), err)
	}
	// 命中唯一键冲突：条目已存在，补查其 id 返回，isNew=false。
	if qerr := s.pool.QueryRow(ctx,
		`SELECT id FROM content_items WHERE source_id = $1 AND external_id = $2`,
		item.SourceID, item.ExternalID).Scan(&id); qerr != nil {
		return 0, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("回查既有内容条目（source=%d, external_id=%s）", item.SourceID, item.ExternalID), qerr)
	}
	return id, false, nil
}

// ListRecentSimhashes 取该源在 since 之后、已算出 simhash 的指纹集合，供近似去重。
// since 由调用方按 72h 窗口传入（now-72h）；查询命中 001 的
// idx_content_items_source_fetched_simhash（INCLUDE simhash 支持 index-only scan）。
func (s *Store) ListRecentSimhashes(ctx context.Context, sourceID int64, since time.Time) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT simhash FROM content_items
		 WHERE source_id = $1 AND fetched_at >= $2 AND simhash IS NOT NULL
		 ORDER BY fetched_at DESC`,
		sourceID, since)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询近期 simhash（source=%d）", sourceID), err)
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var sh int64
		if err := rows.Scan(&sh); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 simhash 行", err)
		}
		out = append(out, sh)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 simhash 结果集", err)
	}
	return out, nil
}

// ListUnpushedByUser 返回该用户 active 订阅源里、尚未向该用户投递过的内容，
// 按抓取时间倒序取最近 limit 条。NOT EXISTS 子查询排除已在 deliveries 里
// 投递过（content_item_id 相同）的条目，避免重复推送同一内容。
func (s *Store) ListUnpushedByUser(ctx context.Context, userID int64, limit int) ([]types.ContentItem, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT ci.id, ci.source_id, ci.external_id, ci.url, ci.title, ci.content, ci.author,
		        ci.published_at, ci.content_hash, ci.simhash, ci.fetched_at, ci.created_at
		 FROM content_items ci
		 JOIN subscriptions sub ON sub.source_id = ci.source_id
		 WHERE sub.user_id = $1 AND sub.status = $2
		   AND NOT EXISTS (
		       SELECT 1 FROM deliveries d
		       WHERE d.user_id = $1 AND d.content_item_id = ci.id
		   )
		 ORDER BY ci.fetched_at DESC
		 LIMIT $3`,
		userID, types.SubscriptionStatusActive, limit)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 未投递内容", userID), err)
	}
	defer rows.Close()

	var out []types.ContentItem
	for rows.Next() {
		var ci types.ContentItem
		if err := rows.Scan(
			&ci.ID, &ci.SourceID, &ci.ExternalID, &ci.URL, &ci.Title, &ci.Content, &ci.Author,
			&ci.PublishedAt, &ci.ContentHash, &ci.Simhash, &ci.FetchedAt, &ci.CreatedAt,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 content_item 行", err)
		}
		out = append(out, ci)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 content_item 结果集", err)
	}
	return out, nil
}
