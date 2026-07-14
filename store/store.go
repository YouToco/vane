// Package store 是数据访问层：持有 pgx 连接池，按实体拆分 .go 文件提供查询方法。
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 持有数据库连接池，是所有数据访问方法的接收者。
// 零值不可用，必须通过 New 构造。
type Store struct {
	pool *pgxpool.Pool
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

	return &Store{pool: pool}, nil
}

// Ping 检查数据库连通性，供 /readyz 就绪探针使用。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close 关闭连接池，等待已借出的连接归还后释放。
func (s *Store) Close() {
	s.pool.Close()
}
