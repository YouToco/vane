// Shared fetch-target persistence.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// fetchTargetColumns is the complete fetch_targets row shape, qualified by s.
const fetchTargetColumns = `s.id, s.platform, s.capability, s.url, s.title, s.config, s.status,
	s.fetch_interval_seconds, s.next_fetch_at, s.last_fetched_at, s.fail_count,
	s.created_at, s.updated_at`

// scanFetchTarget scans one current acquisition target.
func scanFetchTarget(row pgx.Row, target *types.FetchTarget) error {
	return row.Scan(
		&target.ID, &target.Platform, &target.Capability, &target.URL, &target.Title, &target.Config, &target.Status,
		&target.FetchIntervalSeconds, &target.NextFetchAt, &target.LastFetchedAt, &target.FailCount,
		&target.CreatedAt, &target.UpdatedAt,
	)
}

// GetOrCreateFetchTarget materializes one normalized plan target by its URL
// identity. Existing rows are immutable here: referencing a shared target must
// not rewrite another task's config or health.
//
// created=true 表示本次真的新建。实现同 UpsertContentItem：ON CONFLICT DO NOTHING（冲突不
// 返回行）+ 命中时回查一次拿既有 id。
func (s *Store) GetOrCreateFetchTarget(ctx context.Context, target *types.FetchTarget) (id int64, created bool, err error) {
	if s.legacyAdmissionIsClosed() {
		return 0, false, legacyAdmissionClosed("fetch target")
	}
	cfg := target.Config
	if len(cfg) == 0 {
		cfg = json.RawMessage("{}")
	}
	created = true
	err = s.pool.QueryRow(ctx,
		`INSERT INTO fetch_targets (platform, capability, url, title, config)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (url) DO NOTHING
		 RETURNING id`,
		target.Platform, target.Capability, target.URL, target.Title, cfg).Scan(&id)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, false, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("创建抓取目标（url=%s）", target.URL), err)
		}
		// URL is the dedup key, but acquisition semantics must still match.
		// Title is display-only and may differ between tasks.
		created = false
		var platform types.Platform
		var capability types.Capability
		var existingConfig json.RawMessage
		if qerr := s.pool.QueryRow(ctx,
			`SELECT id, platform, capability, config
			   FROM fetch_targets WHERE url = $1`,
			target.URL,
		).Scan(&id, &platform, &capability, &existingConfig); qerr != nil {
			return 0, false, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("回查既有抓取目标（url=%s）", target.URL), qerr)
		}
		if platform != target.Platform || capability != target.Capability ||
			!taskCreationJSONEqual(existingConfig, cfg) {
			return 0, false, types.NewAppError(types.CodeConflict,
				fmt.Sprintf("同一 URL 已存在不同抓取语义（url=%s）", target.URL), nil)
		}
	}
	return id, created, nil
}

// UpdateFetchTargetState advances acquisition health after one fetch attempt.
// next_fetch_at 是 001 的预计算列（调度靠它命中 (status, next_fetch_at) 索引），
// 由抓取方按 interval 算好后传入，store 层只负责落库。
func (s *Store) UpdateFetchTargetState(ctx context.Context, id int64, lastFetched, nextFetch time.Time, failCount int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE fetch_targets
		 SET last_fetched_at = $2, next_fetch_at = $3, fail_count = $4, updated_at = now()
		 WHERE id = $1`,
		id, lastFetched, nextFetch, failCount)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新抓取目标状态（id=%d）", id), err)
	}
	return nil
}

// DisableFetchTargetIfActive disables one unhealthy target idempotently.
// 返回 disabled=true 表示本次调用真的把它从 active 翻成了 disabled（rows affected>0）——
// 调用方据此判断"这一刻刚被停用"，只在真翻转时发一次停用告警。已是 disabled 的源
// 再次调用返回 false（WHERE status='active' 命不中），天然幂等。
// 停用后任务级抓取查询不再返回它，抓取自然停止。
func (s *Store) DisableFetchTargetIfActive(ctx context.Context, id int64) (disabled bool, err error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE fetch_targets SET status = $2, updated_at = now()
		 WHERE id = $1 AND status = $3`,
		id, types.FetchTargetStatusDisabled, types.FetchTargetStatusActive)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("自动停用抓取目标（id=%d）", id), err)
	}
	return tag.RowsAffected() > 0, nil
}

// GetFetchTarget reads one internal target by ID.
// 调用方可用 errors.Is(err, types.ErrNotFound) 区分"不存在"与数据库故障。
func (s *Store) GetFetchTarget(ctx context.Context, id int64) (*types.FetchTarget, error) {
	var src types.FetchTarget
	err := scanFetchTarget(
		s.pool.QueryRow(ctx, `SELECT `+fetchTargetColumns+` FROM fetch_targets s WHERE s.id = $1`, id),
		&src)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("抓取目标 id=%d 不存在", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询抓取目标（id=%d）", id), err)
	}
	return &src, nil
}
