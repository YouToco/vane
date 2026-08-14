package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func migration030Scratch(
	t *testing.T,
) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 030 迁移集成测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	t.Cleanup(drop)
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 30); err != nil {
		t.Fatalf("迁移到 030 失败: %v", err)
	}
	return db, provider
}

func TestMigration030EmptyTableCanDowngrade(t *testing.T) {
	db, provider := migration030Scratch(t)
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("空 task_run_snapshots 应可回滚 030: %v", err)
	}
	var exists bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.task_run_snapshots') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("030 Down 后 task_run_snapshots 仍存在")
	}
}

func TestMigration030RefusesSnapshotDataDowngrade(t *testing.T) {
	db, provider := migration030Scratch(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-030-user', 'migration 030') RETURNING id
		)
		INSERT INTO task_run_snapshots (
			tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version,
			capability_catalog_digest, tool_policy_digest, prompt_policy_digest,
			model_policy_digest, quota_policy_digest, definition_digest, plan_digest,
			payload_digest, reference_digest, reference_schema_version, payload, budget
		)
		SELECT 1, id, 'migration-030-task', 'migration-030-workflow',
		       'migration-030-run', 'scheduled', 'compiled', 0,
		       repeat('0', 64), repeat('1', 64), repeat('2', 64), repeat('3', 64),
		       repeat('4', 64), repeat('5', 64), repeat('6', 64), repeat('7', 64),
		       repeat('8', 64), 'migration-030/v1', convert_to('{}', 'UTF8'), '{}'::jsonb
		  FROM created_user`); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("非空 immutable snapshot 表必须拒绝回滚: %v", err)
	}
	var version, rows int
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id), 0) FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM task_run_snapshots),
		  to_regclass('public.task_run_snapshots') IS NOT NULL`,
	).Scan(&version, &rows, &exists); err != nil {
		t.Fatal(err)
	}
	if version != 30 || rows != 1 || !exists {
		t.Fatalf("拒绝回滚必须原子保留版本/表/行: version=%d rows=%d exists=%v",
			version, rows, exists)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM task_run_snapshots`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("清空 snapshot 后应可回滚 030: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT to_regclass('public.task_run_snapshots') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("清空后 Down 应删除 task_run_snapshots")
	}
}
