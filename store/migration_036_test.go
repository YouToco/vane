package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

func migration036Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is not set; skipping migration 036 test")
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
	if _, err := provider.UpTo(t.Context(), 36); err != nil {
		t.Fatalf("migrate to 036: %v", err)
	}
	return db, provider
}

func TestMigration036DownSerializesParentThenChild(t *testing.T) {
	db, provider := migration036Scratch(t)
	ctx := t.Context()
	var userID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id, name)
		 VALUES ('migration-036-user','migration 036') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	writer, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	writerDone := false
	defer func() {
		if !writerDone {
			_ = writer.Rollback()
		}
	}()
	if _, err := writer.ExecContext(ctx, `SET LOCAL statement_timeout='3s'`); err != nil {
		t.Fatal(err)
	}
	var snapshotID int64
	if err := writer.QueryRowContext(ctx, `
		INSERT INTO task_run_snapshots (
			tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
			run_kind,execution_mode,adaptive_version,
			capability_catalog_digest,tool_policy_digest,prompt_policy_digest,
			model_policy_digest,quota_policy_digest,definition_digest,plan_digest,
			payload_digest,reference_digest,reference_schema_version,payload,budget
		) VALUES (
			1,$1,'migration-036-task','wf-migration-036-task','migration-036-run',
			'scheduled','compiled',0,
			repeat('1',64),repeat('2',64),repeat('3',64),repeat('4',64),
			repeat('5',64),repeat('6',64),repeat('7',64),
			encode(sha256(convert_to('{}','UTF8')),'hex'),repeat('8',64),
			'vane.run-snapshot-ref/v1',convert_to('{}','UTF8'),'{}'
		) RETURNING id`, userID).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}

	downDone := make(chan error, 1)
	go func() {
		_, err := provider.Down(ctx)
		downDone <- err
	}()
	time.Sleep(200 * time.Millisecond)

	if _, err := writer.ExecContext(ctx, `
		WITH body AS (
			SELECT jsonb_build_object(
				'schema_version','vane.task-run-snapshot-shadow/v2',
				'status','headless',
				'identity',jsonb_build_object(
					'tenant_id',1,'user_id',$2::bigint,'task_id','migration-036-task',
					'temporal_workflow_id','wf-migration-036-task',
					'temporal_run_id','migration-036-run'),
				'legacy',jsonb_build_object('snapshot_id',$1::bigint),
				'approved',NULL,'adaptive',NULL
			) AS value
		)
		INSERT INTO task_run_snapshot_v2_shadows (
			run_snapshot_id,tenant_id,user_id,task_id,
			temporal_workflow_id,temporal_run_id,status,
			approved_definition_version,approved_definition_digest,
			adaptive_version,adaptive_digest,payload,payload_digest
		)
		SELECT $1::bigint,1,$2::bigint,'migration-036-task','wf-migration-036-task',
		       'migration-036-run','headless',NULL,NULL,0,NULL,
		       convert_to(value::text,'UTF8'),
		       encode(sha256(convert_to(value::text,'UTF8')),'hex')
		  FROM body`, snapshotID, userID); err != nil {
		t.Fatalf("child insert deadlocked behind reverse-order Down lock: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writerDone = true

	select {
	case err := <-downDone:
		if err == nil || !strings.Contains(err.Error(), "refusing downgrade") {
			t.Fatalf("Down accepted concurrent sidecar: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("036 Down did not converge")
	}
	var version, rows int
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM task_run_snapshot_v2_shadows)`,
	).Scan(&version, &rows); err != nil {
		t.Fatal(err)
	}
	if version != 36 || rows != 1 {
		t.Fatalf("Down fence retained version/rows = %d/%d", version, rows)
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM task_run_snapshot_v2_shadows`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM task_run_snapshots WHERE id=$1`, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(ctx); err != nil {
		t.Fatalf("empty 036 should downgrade: %v", err)
	}
}
