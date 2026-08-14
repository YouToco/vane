package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 database/sql 的 "pgx" 驱动，供 goose 使用
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// migrationsFS 把迁移 SQL 打进二进制，部署时无需携带 migrations 目录。
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate 对目标数据库执行所有 pending 迁移（MVP 决策：应用启动时自动执行）。
// goose 需要 database/sql 连接，这里经 pgx stdlib 适配层临时建连，
// 迁移完成即关闭，与业务用的 pgxpool 互不影响。重复执行是幂等的。
//
// ctx 贯穿建连等待与迁移执行：SIGTERM/SIGINT 可中断启动期迁移。
// VPS 开机时序上 Postgres 容器可能晚于本进程就绪，先以退避重试等待
// 数据库可达（网络类错误重试、由 ctx 决定放弃），再执行迁移——
// 这样"数据库还没起来"和"迁移 SQL 真的坏了"在日志形态上可区分。
func Migrate(ctx context.Context, dbURL string) error {
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return fmt.Errorf("store: 打开迁移连接: %w", err)
	}
	defer db.Close()

	if err := waitReachable(ctx, db); err != nil {
		return fmt.Errorf("store: 等待数据库可达: %w", err)
	}

	// embed 根目录带 migrations/ 前缀，goose provider 期望迁移文件位于 FS 根部。
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("store: 定位迁移目录: %w", err)
	}

	// go test 按包并行：store/evolver/feishu 的测试进程会对同一全新库并发
	// Migrate，后到者在先到者的迁移事务提交前读到空版本表、重复应用同一迁移
	// 而报错（TestMigrateConcurrentFreshDB 复现此形态）。用 Postgres 会话级
	// advisory lock 串行化，后进者拿锁后重读版本表即 no-op，幂等语义不变。
	// 重试间隔取最小 1s（默认 5s，对毫秒级的迁移临界区太粗），总超时 5 分钟。
	sessionLocker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300))
	if err != nil {
		return fmt.Errorf("store: 初始化迁移会话锁: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithSessionLocker(sessionLocker),
		goose.WithAllowOutofOrder(true),
	)
	if err != nil {
		return fmt.Errorf("store: 初始化 goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("store: 执行迁移: %w", err)
	}
	return nil
}

// waitReachable 以固定间隔 Ping 直到数据库可达或 ctx 取消。
// 返回的错误带上最后一次 Ping 失败原因，便于区分配错地址与容器未就绪。
func waitReachable(ctx context.Context, db *sql.DB) error {
	const interval = 2 * time.Second
	var lastErr error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w（最后一次探活失败: %v）", ctx.Err(), lastErr)
		case <-time.After(interval):
		}
	}
}
