package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func migration035Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 035 迁移集成测试")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, dbURL)
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
	if _, err := provider.UpTo(t.Context(), 35); err != nil {
		t.Fatalf("迁移到 035 失败: %v", err)
	}
	return db, provider
}

func TestMigration035EmptyLedgerCanDowngrade(t *testing.T) {
	db, provider := migration035Scratch(t)
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("空 agent_events 应可回滚 035: %v", err)
	}
	var exists bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.agent_events') IS NOT NULL`,
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("035 Down 后 agent_events 仍存在")
	}
}

func TestMigration035RefusesEventDataDowngrade(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	payload := `{"schema_version":"vane.agent-event/v1","kind":"user_message","body":{"text":"keep"}}`
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-035-user', 'migration 035') RETURNING id
		), created_session AS (
			INSERT INTO agent_sessions (tenant_id, user_id)
			SELECT 1, id FROM created_user
			RETURNING id, user_id
		)
		INSERT INTO agent_events (
			tenant_id, user_id, session_id, sequence,
			batch_idempotency_key, batch_index, batch_size,
			kind, schema_version, payload, payload_digest, batch_digest
		)
		SELECT 1, user_id, id, 1, 'migration-035-event', 0, 1,
		       'user_message', 'vane.agent-event/v1',
		       convert_to($1, 'UTF8'),
		       encode(sha256(convert_to($1, 'UTF8')), 'hex'),
		       repeat('a', 64)
		  FROM created_session`, payload); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("非空 event ledger 必须拒绝回滚: %v", err)
	}
	var version, rows int
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id), 0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM agent_events),
		  to_regclass('public.agent_events') IS NOT NULL`,
	).Scan(&version, &rows, &exists); err != nil {
		t.Fatal(err)
	}
	if version != 35 || rows != 1 || !exists {
		t.Fatalf("拒绝回滚必须原子保留版本/表/行: version=%d rows=%d exists=%v",
			version, rows, exists)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM agent_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("清空 event ledger 后应可回滚 035: %v", err)
	}
}
