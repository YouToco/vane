package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration029BackfillsExistingTerminalV1AsLegacySuppressed(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 029 迁移集成测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()

	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 28); err != nil {
		t.Fatalf("迁移到 028 失败: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-029-user', 'migration 029') RETURNING id
		), member AS (
			INSERT INTO memberships (tenant_id, user_id, role)
			SELECT 1, id, 'owner' FROM created_user RETURNING user_id
		)
		INSERT INTO pending_actions (
			id, tenant_id, user_id, tool_name, args, summary, status, expires_at,
			execution_version, phase, tombstoned_at
		)
		SELECT 'migration-029-terminal', 1, user_id, 'create_schedule',
		       '{"intent":"legacy terminal"}'::jsonb, 'legacy terminal',
		       'cancelled', clock_timestamp() + interval '1 day', 1, 'cancelled',
		       clock_timestamp()
		FROM member`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 29); err != nil {
		t.Fatalf("迁移到 029 失败: %v", err)
	}

	var (
		status, providerMessageID, providerKey string
		sent, blocked                          bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT status, provider_message_id, provider_key::text,
		       sent_at IS NOT NULL, blocked_at IS NOT NULL
		  FROM task_creation_receipts
		 WHERE operation_id = 'migration-029-terminal'`,
	).Scan(&status, &providerMessageID, &providerKey, &sent, &blocked); err != nil {
		t.Fatal(err)
	}
	if status != "suppressed" || providerMessageID != "legacy-suppressed" ||
		providerKey != "1a6728af-5add-8966-26e2-dec0c2401a2b" || !sent || blocked {
		t.Fatalf("legacy terminal backfill mismatch: status=%q message=%q key=%q sent=%v blocked=%v",
			status, providerMessageID, providerKey, sent, blocked)
	}
	var (
		rlsEnabled bool
		policies   int
		granted    bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT c.relrowsecurity,
		       (SELECT count(*) FROM pg_policies
		         WHERE schemaname='public' AND tablename='task_creation_receipts'),
		       has_table_privilege('vane_app', 'task_creation_receipts',
		                           'SELECT,INSERT,UPDATE,DELETE')
		  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname='task_creation_receipts'`,
	).Scan(&rlsEnabled, &policies, &granted); err != nil {
		t.Fatal(err)
	}
	if !rlsEnabled || policies != 2 || !granted {
		t.Fatalf("RLS/grant incomplete: rls=%v policies=%d grant=%v",
			rlsEnabled, policies, granted)
	}
	assertReceiptRLSCount := func(tenantID string, want int) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx,
			`SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM task_creation_receipts`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("receipt RLS tenant=%s count=%d want=%d", tenantID, count, want)
		}
	}
	assertReceiptRLSCount("1", 1)
	assertReceiptRLSCount("999999", 0)

	if _, err := db.ExecContext(ctx, `
		WITH foreign_tenant AS (
			INSERT INTO tenants (status, plan)
			VALUES ('active', 'free') RETURNING id
		), foreign_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-029-foreign-user', 'foreign') RETURNING id
		), foreign_member AS (
			INSERT INTO memberships (tenant_id, user_id, role)
			SELECT t.id, u.id, 'owner' FROM foreign_tenant t CROSS JOIN foreign_user u
			RETURNING tenant_id, user_id
		)
		INSERT INTO pending_actions (
			id, tenant_id, user_id, tool_name, args, summary, status, expires_at,
			execution_version
		)
		SELECT 'migration-029-foreign-operation', tenant_id, user_id,
		       'create_schedule', '{}'::jsonb, 'foreign', 'pending',
		       clock_timestamp() + interval '1 day', 1
		  FROM foreign_member`); err != nil {
		t.Fatal(err)
	}
	var localUserID int64
	if err := db.QueryRowContext(ctx, `
		SELECT user_id FROM pending_actions
		 WHERE id = 'migration-029-terminal'`,
	).Scan(&localUserID); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id', '1', true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_creation_receipts (
			operation_id, tenant_id, user_id, provider, target, provider_key
		) VALUES (
			'migration-029-foreign-operation', 1, $1,
			'feishu_card_patch', 'om_cross_scope',
			'00000000-0000-0000-0000-000000000029'
		)`, localUserID); err == nil {
		t.Fatal("receipt RLS scope must not point at a foreign operation")
	} else if !strings.Contains(err.Error(), "fk_task_creation_receipts_operation_scope") {
		t.Fatalf("cross-scope insert failed for the wrong reason: %v", err)
	}
	if err := tx.Rollback(); err != nil && !strings.Contains(err.Error(), "already been closed") {
		t.Fatal(err)
	}

	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("纯 legacy suppressed 状态应可安全回滚 029: %v", err)
	}
	var receiptTable, receiptColumns int
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM information_schema.tables
		    WHERE table_schema='public' AND table_name='task_creation_receipts'),
		  (SELECT count(*) FROM information_schema.columns
		    WHERE table_schema='public' AND table_name='pending_actions'
		      AND column_name IN ('receipt_provider', 'receipt_target'))`,
	).Scan(&receiptTable, &receiptColumns); err != nil {
		t.Fatal(err)
	}
	if receiptTable != 0 || receiptColumns != 0 {
		t.Fatalf("029 rollback left schema behind: table=%d columns=%d",
			receiptTable, receiptColumns)
	}
}

func TestMigration029RefusesDurableReceiptStateDowngrade(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 029 迁移集成测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 29); err != nil {
		t.Fatalf("迁移到 029 失败: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		WITH created_user AS (
			INSERT INTO users (feishu_open_id, name)
			VALUES ('migration-029-guard', 'guard') RETURNING id
		), member AS (
			INSERT INTO memberships (tenant_id, user_id, role)
			SELECT 1, id, 'owner' FROM created_user RETURNING user_id
		)
		INSERT INTO pending_actions (
			id, tenant_id, user_id, tool_name, args, summary, status, expires_at,
			execution_version, receipt_provider, receipt_target
		)
		SELECT 'migration-029-bound', 1, user_id, 'create_schedule', '{}'::jsonb,
		       'bound', 'pending', clock_timestamp() + interval '1 day', 1,
		       'feishu_card_patch', 'om_bound'
		  FROM member`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("已绑定 target 但未出 outbox 时必须拒绝回滚: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE pending_actions
		   SET receipt_provider = '', receipt_target = ''
		 WHERE id = 'migration-029-bound';
		INSERT INTO task_creation_receipts (
			operation_id, tenant_id, user_id, provider, target, provider_key
		)
		SELECT id, tenant_id, user_id, 'feishu_card_patch', 'om_receipt',
		       '00000000-0000-0000-0000-000000000030'
		  FROM pending_actions WHERE id = 'migration-029-bound'`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("非 suppressed outbox 存在时必须拒绝回滚: %v", err)
	}
	var version, receiptRows, receiptColumns int
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM task_creation_receipts),
		  (SELECT count(*) FROM information_schema.columns
		    WHERE table_schema='public' AND table_name='pending_actions'
		      AND column_name IN ('receipt_provider', 'receipt_target'))`,
	).Scan(&version, &receiptRows, &receiptColumns); err != nil {
		t.Fatal(err)
	}
	if version != 29 || receiptRows != 1 || receiptColumns != 2 {
		t.Fatalf("拒绝回滚必须保持状态: version=%d receipts=%d columns=%d",
			version, receiptRows, receiptColumns)
	}
}
