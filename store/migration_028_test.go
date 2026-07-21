package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration028RefusesStatefulDowngrade(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 028 迁移集成测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()

	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatalf("打开一次性库连接失败: %v", err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("定位迁移目录失败: %v", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatalf("初始化 goose provider 失败: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("迁移到 028 失败: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-028-guard', 'guard')
			RETURNING id
		), member AS (
			INSERT INTO memberships (tenant_id, user_id, role)
			SELECT 1, id, 'owner' FROM created_user
			RETURNING user_id
		)
		INSERT INTO pending_actions (
			id, user_id, tool_name, args, expires_at, tenant_id, execution_version
		)
		SELECT 'migration-028-guard', user_id, 'create_schedule',
		       '{"intent":"guard"}'::jsonb, now() + interval '1 day', 1, 1
		FROM member`)
	if err != nil {
		t.Fatalf("插入 v1 operation 失败: %v", err)
	}

	if _, err := provider.Down(ctx); err == nil || !strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("带 v1 状态的 028 Down 应明确拒绝，实际: %v", err)
	}
	var version, versionedRows, protectedColumns int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT max(version_id) FROM goose_db_version WHERE is_applied),
			(SELECT count(*) FROM pending_actions WHERE execution_version = 1),
			(SELECT count(*) FROM information_schema.columns
			  WHERE table_schema = 'public' AND table_name = 'pending_actions'
			    AND column_name IN ('fence', 'takeover_not_before'))
	`).Scan(&version, &versionedRows, &protectedColumns); err != nil {
		t.Fatalf("核对拒绝降级后的状态失败: %v", err)
	}
	if version != 28 || versionedRows != 1 || protectedColumns != 2 {
		t.Fatalf("拒绝降级未保持原子状态: version=%d rows=%d columns=%d",
			version, versionedRows, protectedColumns)
	}

	if _, err := db.ExecContext(ctx,
		`DELETE FROM pending_actions WHERE id = 'migration-028-guard'`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("清空 v1 状态后 028 应可安全降级: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'pending_actions'
		   AND column_name IN ('execution_version', 'fence', 'takeover_not_before')
	`).Scan(&protectedColumns); err != nil {
		t.Fatal(err)
	}
	if protectedColumns != 0 {
		t.Fatalf("空状态降级后仍残留 028 列: %d", protectedColumns)
	}
}
