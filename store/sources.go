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

// sourceColumns 是 sources 表的全列清单（以别名 s 限定，避免与 JOIN 的
// subscriptions 表同名列 id/status/created_at 歧义），SELECT 与 scanSource 一一对应。
const sourceColumns = `s.id, s.platform, s.capability, s.url, s.title, s.config, s.status,
	s.fetch_interval_seconds, s.next_fetch_at, s.last_fetched_at, s.fail_count,
	s.created_at, s.updated_at`

// scanSource 把一行 sources 扫进 types.Source。
// 可空列 last_fetched_at 扫进 *time.Time 字段（**time.Time 目标，NULL→nil）。
// 复用于单行（pgx.Row）与多行（pgx.Rows，其 Scan 满足 pgx.Row 接口）两种场景。
func scanSource(row pgx.Row, src *types.Source) error {
	return row.Scan(
		&src.ID, &src.Platform, &src.Capability, &src.URL, &src.Title, &src.Config, &src.Status,
		&src.FetchIntervalSeconds, &src.NextFetchAt, &src.LastFetchedAt, &src.FailCount,
		&src.CreatedAt, &src.UpdatedAt,
	)
}

// ListActiveSourcesByUser 返回该用户 active 订阅、且信源本身 active 的所有信源。
// 双重 active 过滤的原因：被连续失败自动 disabled 的信源不该再进入抓取；
// 已取消（非 active）的订阅也不该被扇出。Fetch Activity 触发时刻现查用它。
func (s *Store) ListActiveSourcesByUser(ctx context.Context, userID int64) ([]types.Source, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+sourceColumns+`
		 FROM sources s
		 JOIN subscriptions sub ON sub.source_id = s.id
		 WHERE sub.user_id = $1 AND sub.status = $2 AND s.status = $3
		 ORDER BY s.id`,
		userID, types.SubscriptionStatusActive, types.SourceStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的 active 信源", userID), err)
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

// ListDueSourcesByUser 返回该用户 active 订阅、信源 active、且已到抓取时间
// （next_fetch_at <= now()）的信源——这就是 001 注释里承诺的 ListDueForFetch 语义，
// 命中 (status, next_fetch_at) 索引。
//
// Fetch Activity 用它而非 ListActiveSourcesByUser 的原因（审查 #重复计费）：
// markFetchResult 成功后会推进 next_fetch_at，Activity 超时重试时已抓成功的源
// 自然被跳过——否则每次重试都重跑全部源，已成功的 Exa/TikHub 付费调用重复计费。
// 刚添加的源 next_fetch_at DEFAULT now()，立即 due；刚抓过的源虽被跳过，
// 但其已入库内容仍会被 ListUnpushedByUser 捞出，不影响推送结果。
func (s *Store) ListDueSourcesByUser(ctx context.Context, userID int64) ([]types.Source, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+sourceColumns+`
		 FROM sources s
		 JOIN subscriptions sub ON sub.source_id = s.id
		 WHERE sub.user_id = $1 AND sub.status = $2 AND s.status = $3
		   AND s.next_fetch_at <= now()
		 ORDER BY s.id`,
		userID, types.SubscriptionStatusActive, types.SourceStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的到期信源", userID), err)
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

// ListSubscribedSourcesByUser 返回该用户 active 订阅的所有信源，**不过滤 source.status**。
// 与 ListActiveSourcesByUser 的区别：这里保留 disabled/paused 的信源，供 API 的
// GET /api/subscriptions 把订阅列表连同状态（含被自动 disabled / 暂停的源）一并回给
// 前端，使 Sources.tsx 的状态灯可达。抓取扇出仍用双重 active 过滤的 ListActiveSourcesByUser。
func (s *Store) ListSubscribedSourcesByUser(ctx context.Context, userID int64) ([]types.Source, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+sourceColumns+`
		 FROM sources s
		 JOIN subscriptions sub ON sub.source_id = s.id
		 WHERE sub.user_id = $1 AND sub.status = $2
		 ORDER BY s.id`,
		userID, types.SubscriptionStatusActive)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的全部订阅信源", userID), err)
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

// UpsertSource 按 url 幂等地写入信源，返回其 id。
//
// 007 起靠 uq_sources_url 做真正的原子 upsert（此前是 SELECT-then-INSERT）。换掉的理由
// 是多用户：两个用户同时添加同一个源，先查后写的两个事务会双双查空、双双插入 ⇒ 重复
// 信源行 ⇒ 同一内容存两份 + 每轮重复抓取重复付费（xhs 详情 $0.01/次）。单 owner MVP
// 下这个竞态窗口撞不上，按多用户设计后它是必然事件，不是"可接受的竞态"。
//
// 沿用先前的取舍不变：
//   - 命中既有行只刷新可变元数据（type/title/config）与 updated_at，刻意不动抓取状态
//     （next_fetch_at / last_fetched_at / fail_count / status）与 created_at，
//     避免重复添加订阅时打乱调度节奏、或让一个新订阅者把源的失败计数洗掉。
//   - title 用 COALESCE(NULLIF(...))：重复添加不带 title 时保留既有展示名，而非静默
//     清成空串（代价是无法通过传空串清除 title，可接受）。
//
// 新插入时 status / fetch_interval_seconds / next_fetch_at / fail_count 走 001 的 DB
// 默认值（active / 1800 / now() / 0），调用方无需关心冷启动细节。
func (s *Store) UpsertSource(ctx context.Context, src *types.Source) (int64, error) {
	// config NOT NULL DEFAULT '{}'：nil / 空 RawMessage 归一为 '{}'，避免写入 NULL 触发约束。
	cfg := src.Config
	if len(cfg) == 0 {
		cfg = json.RawMessage("{}")
	}

	// DO UPDATE 而非 DO NOTHING：DO NOTHING 在冲突时不返回行，还得补一次 SELECT；
	// DO UPDATE 冲突时也 RETURNING id，一次往返拿到结果，且顺带完成元数据刷新。
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO sources (platform, capability, url, title, config)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (url) DO UPDATE
		 SET platform   = EXCLUDED.platform,
		     capability = EXCLUDED.capability,
		     title      = COALESCE(NULLIF(EXCLUDED.title, ''), sources.title),
		     config     = EXCLUDED.config,
		     updated_at = now()
		 RETURNING id`,
		src.Platform, src.Capability, src.URL, src.Title, cfg).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("upsert 信源（url=%s）", src.URL), err)
	}
	return id, nil
}

// UpdateSourceFetchState 抓取完成后同步写 last_fetched_at / next_fetch_at / fail_count。
// next_fetch_at 是 001 的预计算列（调度靠它命中 (status, next_fetch_at) 索引），
// 由抓取方按 interval 算好后传入，store 层只负责落库。
func (s *Store) UpdateSourceFetchState(ctx context.Context, id int64, lastFetched, nextFetch time.Time, failCount int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE sources
		 SET last_fetched_at = $2, next_fetch_at = $3, fail_count = $4, updated_at = now()
		 WHERE id = $1`,
		id, lastFetched, nextFetch, failCount)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新信源抓取状态（id=%d）", id), err)
	}
	return nil
}

// GetSource 按 id 读取单个信源；不存在时返回 CodeNotFound 的 AppError，
// 调用方可用 errors.Is(err, types.ErrNotFound) 区分"不存在"与数据库故障。
func (s *Store) GetSource(ctx context.Context, id int64) (*types.Source, error) {
	var src types.Source
	err := scanSource(
		s.pool.QueryRow(ctx, `SELECT `+sourceColumns+` FROM sources s WHERE s.id = $1`, id),
		&src)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("信源 id=%d 不存在", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询信源（id=%d）", id), err)
	}
	return &src, nil
}
