package store

import (
	"os"
	"testing"
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
