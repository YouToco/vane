package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// Baseline 返回该源的 diff 基准 = 最近一个已 settled 的快照；一个都没有时返回 nil。
// settled ⟺ verdict 已定（baseline/suppressed）或 已落成 content_item。
func (s *Store) Baseline(ctx context.Context, sourceID int64) (*types.PageSnapshot, error) {
	var ps types.PageSnapshot
	err := s.pool.QueryRow(ctx,
		`SELECT ps.id, ps.source_id, ps.canonical_key, ps.content_hash, ps.extracted_text, ps.verdict, ps.first_seen_at
		 FROM page_snapshots ps
		 WHERE ps.source_id = $1
		   AND (ps.verdict IN ('baseline','suppressed')
		        OR EXISTS (SELECT 1 FROM content_items ci WHERE ci.canonical_key = ps.canonical_key))
		 ORDER BY ps.id DESC LIMIT 1`, sourceID).Scan(
		&ps.ID, &ps.SourceID, &ps.CanonicalKey, &ps.ContentHash, &ps.ExtractedText, &ps.Verdict, &ps.FirstSeenAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询 page_watch 基准（source_id=%d）", sourceID), err)
	}
	return &ps, nil
}

// PutSnapshot 追加快照，ON CONFLICT (source_id, canonical_key) DO NOTHING（重试幂等）。
func (s *Store) PutSnapshot(ctx context.Context, snap *types.PageSnapshot) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO page_snapshots (source_id, canonical_key, content_hash, extracted_text, verdict)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (source_id, canonical_key) DO NOTHING`,
		snap.SourceID, snap.CanonicalKey, snap.ContentHash, snap.ExtractedText, snap.Verdict)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入 page_watch 快照（source_id=%d, key=%s）", snap.SourceID, snap.CanonicalKey), err)
	}
	return nil
}

// SettleSnapshot 把指定快照的 verdict 置为 suppressed（LLM 门判不重要时调用）。
func (s *Store) SettleSnapshot(ctx context.Context, sourceID int64, canonicalKey string, v types.SnapshotVerdict) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE page_snapshots SET verdict = $3 WHERE source_id = $1 AND canonical_key = $2`,
		sourceID, canonicalKey, v)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("settle page_watch 快照（source_id=%d, key=%s）", sourceID, canonicalKey), err)
	}
	return nil
}
