package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func migration031Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
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

func TestMigration031SeparatesLegacyAndCompiledIdempotencyDomains(t *testing.T) {
	db, provider := migration031Scratch(t)
	ctx := t.Context()
	const sharedKey = "migration-031-shared-trace"
	var userID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id, name)
		 VALUES ('migration-031-user', 'migration 031') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role)
		 VALUES (1, $1, 'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO push_batches (tenant_id, user_id, idempotency_key)
		 VALUES (1, $1, $2)`, userID, sharedKey); err != nil {
		t.Fatalf("seed legacy batch at 030: %v", err)
	}
	if _, err := provider.UpTo(ctx, 31); err != nil {
		t.Fatalf("迁移到 031 失败: %v", err)
	}

	// This is the pre-031 CreatePushBatchIdempotent statement verbatim. Its
	// conflict target must remain inferable after Up so an old binary can be
	// rolled back (or overlap briefly during a forward-fix deployment).
	const rollbackKey = "migration-031-old-binary-create"
	oldCreateSQL :=
		`INSERT INTO push_batches (tenant_id, user_id, idempotency_key, schedule_id) VALUES (` + tenantOfUser + `$1), $1, $2, $3)
		 ON CONFLICT (idempotency_key) WHERE idempotency_key <> ''
		 DO UPDATE SET user_id = EXCLUDED.user_id, exit_gate = '', stage_counts = '{}'
		 RETURNING id`
	var oldFirst, oldRetry int64
	if err := db.QueryRowContext(ctx, oldCreateSQL, userID, rollbackKey, nil).Scan(&oldFirst); err != nil {
		t.Fatalf("pre-031 create SQL failed on 031 schema: %v", err)
	}
	if err := db.QueryRowContext(ctx, oldCreateSQL, userID, rollbackKey, nil).Scan(&oldRetry); err != nil {
		t.Fatalf("pre-031 create retry failed on 031 schema: %v", err)
	}
	if oldFirst != oldRetry {
		t.Fatalf("pre-031 create retry returned batches %d/%d", oldFirst, oldRetry)
	}

	const rollbackEmptyKey = "migration-031-old-binary-empty"
	oldEmptySQL :=
		`INSERT INTO push_batches (tenant_id, user_id, status, exit_gate, stage_counts, idempotency_key, schedule_id)
		 VALUES (` + tenantOfUser + `$1), $1, $2, $3, $4, $5, $6)
		 ON CONFLICT (idempotency_key) WHERE idempotency_key <> ''
		 DO UPDATE SET exit_gate = EXCLUDED.exit_gate, stage_counts = EXCLUDED.stage_counts
		 WHERE push_batches.status = $2
		 RETURNING id`
	var emptyFirst, emptyRetry int64
	oldEmptyArgs := []any{userID, "empty", "fetch", `{"fetched":0}`,
		rollbackEmptyKey, nil}
	if err := db.QueryRowContext(ctx, oldEmptySQL, oldEmptyArgs...).Scan(&emptyFirst); err != nil {
		t.Fatalf("pre-031 empty SQL failed on 031 schema: %v", err)
	}
	if err := db.QueryRowContext(ctx, oldEmptySQL, oldEmptyArgs...).Scan(&emptyRetry); err != nil {
		t.Fatalf("pre-031 empty retry failed on 031 schema: %v", err)
	}
	if emptyFirst != emptyRetry {
		t.Fatalf("pre-031 empty retry returned batches %d/%d", emptyFirst, emptyRetry)
	}

	var legacyRows int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM push_batches
		  WHERE idempotency_key = $1 AND run_snapshot_id IS NULL`, sharedKey,
	).Scan(&legacyRows); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 1 {
		t.Fatalf("legacy rows after 031 = %d, want 1", legacyRows)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO push_batches (tenant_id, user_id, idempotency_key)
		 VALUES (1, $1, $2)`, userID, sharedKey); err == nil {
		t.Fatal("duplicate legacy trace unexpectedly bypassed global legacy domain")
	}

	var firstSnapshot, resetSnapshot int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO task_run_snapshots (
			tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version,
			capability_catalog_digest, tool_policy_digest, prompt_policy_digest,
			model_policy_digest, quota_policy_digest, definition_digest, plan_digest,
			payload_digest, reference_digest, reference_schema_version, payload, budget
		) VALUES (
			1, $1, 'migration-031-task', 'migration-031-workflow', $2,
			'scheduled', 'compiled', 0,
			repeat('0', 64), repeat('1', 64), repeat('2', 64), repeat('3', 64),
			repeat('4', 64), repeat('5', 64), repeat('6', 64), repeat('7', 64),
			repeat('8', 64), 'migration-031/v1', convert_to('{}', 'UTF8'), '{}'::jsonb
		) RETURNING id`, userID, "migration-031-run-original",
	).Scan(&firstSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO task_run_snapshots (
			tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version,
			capability_catalog_digest, tool_policy_digest, prompt_policy_digest,
			model_policy_digest, quota_policy_digest, definition_digest, plan_digest,
			payload_digest, reference_digest, reference_schema_version, payload, budget
		) VALUES (
			1, $1, 'migration-031-task', 'migration-031-workflow', $2,
			'scheduled', 'compiled', 0,
			repeat('0', 64), repeat('1', 64), repeat('2', 64), repeat('3', 64),
			repeat('4', 64), repeat('5', 64), repeat('6', 64), repeat('7', 64),
			repeat('8', 64), 'migration-031/v1', convert_to('{}', 'UTF8'), '{}'::jsonb
		) RETURNING id`, userID, "migration-031-run-reset",
	).Scan(&resetSnapshot); err != nil {
		t.Fatal(err)
	}
	firstPhysicalKey := compiledPushBatchPhysicalKeyV1(firstSnapshot, sharedKey)
	resetPhysicalKey := compiledPushBatchPhysicalKeyV1(resetSnapshot, sharedKey)
	if firstPhysicalKey == resetPhysicalKey || firstPhysicalKey == sharedKey ||
		resetPhysicalKey == sharedKey {
		t.Fatalf("compiled physical keys are not snapshot scoped: first=%q reset=%q",
			firstPhysicalKey, resetPhysicalKey)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO push_batches
		    (tenant_id, user_id, idempotency_key, run_snapshot_id)
		 VALUES (1, $1, $2, $4), (1, $1, $3, $5)`,
		userID, firstPhysicalKey, resetPhysicalKey, firstSnapshot, resetSnapshot,
	); err != nil {
		t.Fatalf("same trace in two immutable runs must coexist: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO push_batches
		    (tenant_id, user_id, idempotency_key, run_snapshot_id)
		 VALUES (1, $1, $2, $3)`, userID, firstPhysicalKey, firstSnapshot,
	); err == nil {
		t.Fatal("duplicate trace inside one snapshot unexpectedly bypassed idempotency")
	}
	var compiledRows, distinctSnapshots int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*), count(DISTINCT run_snapshot_id)
		   FROM push_batches
		  WHERE (run_snapshot_id = $1 AND idempotency_key = $2)
		     OR (run_snapshot_id = $3 AND idempotency_key = $4)`,
		firstSnapshot, firstPhysicalKey, resetSnapshot, resetPhysicalKey,
	).Scan(&compiledRows, &distinctSnapshots); err != nil {
		t.Fatal(err)
	}
	if compiledRows != 2 || distinctSnapshots != 2 {
		t.Fatalf("compiled rows/snapshots = %d/%d, want 2/2",
			compiledRows, distinctSnapshots)
	}

	if _, err := provider.Down(ctx); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("compiled batches must block 031 downgrade: %v", err)
	}
	var version int
	var columnExists bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id), 0) FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM push_batches WHERE run_snapshot_id IS NOT NULL),
		  EXISTS (SELECT 1 FROM information_schema.columns
		           WHERE table_schema = 'public' AND table_name = 'push_batches'
		             AND column_name = 'run_snapshot_id')`,
	).Scan(&version, &compiledRows, &columnExists); err != nil {
		t.Fatal(err)
	}
	if version != 31 || compiledRows != 2 || !columnExists {
		t.Fatalf("failed downgrade drifted state: version=%d rows=%d column=%v",
			version, compiledRows, columnExists)
	}

	if _, err := db.ExecContext(ctx,
		`DELETE FROM push_batches WHERE run_snapshot_id IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("031 should downgrade after compiled batches are removed: %v", err)
	}
	var oldIndexExists bool
	if err := db.QueryRowContext(ctx, `
		SELECT
		  NOT EXISTS (SELECT 1 FROM information_schema.columns
		              WHERE table_schema = 'public' AND table_name = 'push_batches'
		                AND column_name = 'run_snapshot_id'),
		  to_regclass('public.uq_push_batches_idem') IS NOT NULL`,
	).Scan(&columnExists, &oldIndexExists); err != nil {
		t.Fatal(err)
	}
	if !columnExists || !oldIndexExists {
		t.Fatalf("031 downgrade did not restore 030 shape: no-column=%v old-index=%v",
			columnExists, oldIndexExists)
	}
}
