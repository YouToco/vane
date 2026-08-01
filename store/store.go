// Package store 是数据访问层：持有 pgx 连接池，按实体拆分 .go 文件提供查询方法。
package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 持有数据库连接池，是所有数据访问方法的接收者。
// 零值不可用，必须通过 New 构造。
type Store struct {
	pool                    *pgxpool.Pool
	beginTx                 func(context.Context, pgx.TxOptions) (pgx.Tx, error)
	intelligenceCursorState *intelligenceCursorState
}

type intelligenceCursorState struct {
	sync.Mutex
	keys       map[int][]byte
	activeKey  int
	keysLoaded time.Time
}

// New 解析连接串、建立连接池并做一次连通性检查。
// 池参数按单 VPS + MVP 负载设定：MaxConns=10 足够覆盖 HTTP + 后台任务。
func New(ctx context.Context, dbURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("store: 解析数据库连接串: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: 创建连接池: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: 数据库连通性检查: %w", err)
	}

	return &Store{
		pool: pool, beginTx: pool.BeginTx,
		intelligenceCursorState: &intelligenceCursorState{},
	}, nil
}

func (s *Store) ensureIntelligenceCursorKeys(ctx context.Context) error {
	state := s.intelligenceCursorState
	if state == nil {
		return fmt.Errorf("store: 情报查询游标状态未初始化")
	}
	state.Lock()
	defer state.Unlock()
	if state.activeKey != 0 && len(state.keys) > 0 &&
		time.Since(state.keysLoaded) < time.Minute {
		return nil
	}
	return s.loadIntelligenceCursorKeysLocked(ctx)
}

func (s *Store) reloadIntelligenceCursorKeys(ctx context.Context) error {
	state := s.intelligenceCursorState
	if state == nil {
		return fmt.Errorf("store: 情报查询游标状态未初始化")
	}
	state.Lock()
	defer state.Unlock()
	return s.loadIntelligenceCursorKeysLocked(ctx)
}

func (s *Store) loadIntelligenceCursorKeysLocked(ctx context.Context) error {
	rows, err := s.pool.Query(ctx,
		`SELECT key_version,key_bytes,active
		   FROM public.agent_intelligence_cursor_keys ORDER BY key_version`)
	if err != nil {
		return fmt.Errorf("store: 读取情报查询游标签名: %w", err)
	}
	defer rows.Close()
	keys := make(map[int][]byte)
	activeVersion := 0
	for rows.Next() {
		var version int
		var key []byte
		var active bool
		if err := rows.Scan(&version, &key, &active); err != nil {
			return fmt.Errorf("store: 扫描情报查询游标签名: %w", err)
		}
		if version <= 0 || len(key) < 16 || len(key) > 64 {
			return fmt.Errorf("store: 情报查询游标签名材料无效")
		}
		keys[version] = append([]byte(nil), key...)
		if active {
			if activeVersion != 0 {
				return fmt.Errorf("store: 存在多个 active 情报查询游标签名")
			}
			activeVersion = version
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: 遍历情报查询游标签名: %w", err)
	}
	if activeVersion == 0 {
		return fmt.Errorf("store: 缺少 active 情报查询游标签名")
	}
	state := s.intelligenceCursorState
	state.keys = keys
	state.activeKey = activeVersion
	state.keysLoaded = time.Now()
	return nil
}

// Ping 检查数据库连通性，供 /readyz 就绪探针使用。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close 关闭连接池，等待已借出的连接归还后释放。
func (s *Store) Close() {
	s.pool.Close()
}
