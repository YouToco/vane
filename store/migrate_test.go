package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

// wantTables 是 Step 5 schema 的 9 张业务表 + M2 settings 表 + M3 schedules 表，
// 迁移完成后必须全部存在。
var wantTables = []string{
	"users",
	"sources",
	"subscriptions",
	"content_items",
	"push_batches",
	"deliveries",
	"feedbacks",
	"profiles",
	"llm_calls",
	"settings",
	"schedules",
}

// TestMigrate 是集成测试：依赖真实 Postgres（CI 的 test job 提供 service 与 DATABASE_URL）。
func TestMigrate(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过迁移集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}

	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	defer st.Close()

	versionAfterFirst := gooseVersion(t, st)

	// 迁移必须幂等：重复执行不报错，且不重复应用（goose 版本号不变）。
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 重复执行失败（应幂等）: %v", err)
	}
	if v := gooseVersion(t, st); v != versionAfterFirst {
		t.Errorf("重复迁移改变了 goose 版本号: %d -> %d（迁移被重复应用）", versionAfterFirst, v)
	}

	for _, table := range wantTables {
		var exists bool
		err := st.pool.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("查询 information_schema.tables 失败（表 %s）: %v", table, err)
		}
		if !exists {
			t.Errorf("迁移后缺少表: %s", table)
		}
	}
}

// TestMigrateConcurrentFreshDB 回归 go test ./... 的迁移竞态：store/evolver/feishu
// 三个测试进程各自调用 Migrate，全新库上后到者在先到者的 001 事务提交前读到
// 空 goose_db_version，跟着应用 001 报 "relation already exists"。
//
// DATABASE_URL 指向的库通常已迁移过（窗口不存在），所以另建一次性全新库；
// 每个 Migrate 各自 sql.Open 独立建连，session 语义与跨进程并发等价。
// 窗口本身极窄（先到者执行 001 的几百毫秒），靠自然时序复现会 flaky，
// 这里用 admin 事务预建 users 表且不提交来占住表锁——模拟"先到者 001 执行中"：
// 全部并发 Migrate 越过版本表检查、读到版本 0、阻塞在 001 的 CREATE TABLE users
// 上，回滚放行后同时竞争建表，无串行化保护时除赢家外必然全部报错。
func TestMigrateConcurrentFreshDB(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过迁移集成测试")
	}
	ctx := t.Context()

	admin, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("打开管理连接失败: %v", err)
	}

	freshName := fmt.Sprintf("vane_migrate_race_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+freshName); err != nil {
		admin.Close()
		t.Skipf("无 CREATE DATABASE 权限，跳过并发迁移竞态测试: %v", err)
	}
	// admin 的关闭放同一个 Cleanup（而非 defer）：defer 先于 Cleanup 执行，
	// 若在 defer 里关连接，DROP 时连接已不可用。
	t.Cleanup(func() {
		defer admin.Close()
		// WITH (FORCE) 兜底断开残留连接（PG 13+），避免临时库删不掉。
		if _, err := admin.ExecContext(context.Background(),
			"DROP DATABASE "+freshName+" WITH (FORCE)"); err != nil {
			t.Errorf("清理临时库 %s 失败: %v", freshName, err)
		}
	})

	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("解析 DATABASE_URL 失败: %v", err)
	}
	u.Path = "/" + freshName
	freshURL := u.String()

	blocker, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatalf("打开临时库占锁连接失败: %v", err)
	}
	defer blocker.Close()
	blockTx, err := blocker.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("开启占锁事务失败: %v", err)
	}
	if _, err := blockTx.ExecContext(ctx, "CREATE TABLE users (id bigint)"); err != nil {
		t.Fatalf("占锁事务预建 users 失败: %v", err)
	}

	const n = 4
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = Migrate(ctx, freshURL)
		}()
	}
	// 并发 Migrate 同时首建版本表会冲突，goose 内部按 1s 间隔重试；等一轮
	// 重试完成、全部抵达 users 表锁后再回滚放行。
	time.Sleep(3 * time.Second)
	if err := blockTx.Rollback(); err != nil {
		t.Fatalf("回滚占锁事务失败: %v", err)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("并发 Migrate #%d 失败: %v", i, err)
		}
	}

	// 并发迁移完成后库必须处于与单次迁移相同的终态。
	fresh, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatalf("打开临时库连接失败: %v", err)
	}
	defer fresh.Close()
	var version int64
	if err := fresh.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version`).Scan(&version); err != nil {
		t.Fatalf("查询 goose_db_version 失败: %v", err)
	}
	if version == 0 {
		t.Error("并发迁移后 goose_db_version 为空，迁移未生效")
	}
	for _, table := range wantTables {
		var exists bool
		err := fresh.QueryRowContext(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("查询 information_schema.tables 失败（表 %s）: %v", table, err)
		}
		if !exists {
			t.Errorf("并发迁移后缺少表: %s", table)
		}
	}
}

// gooseVersion 读取 goose 版本表的当前最大版本号。
func gooseVersion(t *testing.T, st *Store) int64 {
	t.Helper()
	var v int64
	err := st.pool.QueryRow(t.Context(),
		`SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version`).Scan(&v)
	if err != nil {
		t.Fatalf("查询 goose_db_version 失败: %v", err)
	}
	return v
}
