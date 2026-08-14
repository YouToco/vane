package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func migration037Scratch(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
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
	if _, err := provider.UpTo(t.Context(), 37); err != nil {
		t.Fatalf("migrate to 037: %v", err)
	}
	return db, provider
}

func TestMigration037EmptyFoundationCanDowngrade(t *testing.T) {
	db, provider := migration037Scratch(t)
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("empty 037 downgrade: %v", err)
	}
	var version int
	var eventTable, scheduleColumn, snapshotColumn *string
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  to_regclass('task_run_snapshot_v2_cutover_events')::text,
		  (SELECT column_name FROM information_schema.columns
		    WHERE table_name='schedules'
		      AND column_name='run_snapshot_cutover_event_id'),
		  (SELECT column_name FROM information_schema.columns
		    WHERE table_name='task_run_snapshots'
		      AND column_name='v2_cutover_event_id')`,
	).Scan(&version, &eventTable, &scheduleColumn, &snapshotColumn); err != nil {
		t.Fatal(err)
	}
	if version != 36 || eventTable != nil ||
		scheduleColumn != nil || snapshotColumn != nil {
		t.Fatalf("downgraded version/table/schedule/snapshot = %d/%v/%v/%v",
			version, eventTable, scheduleColumn, snapshotColumn)
	}
}

func TestMigration037RefusesDurableEventDowngrade(t *testing.T) {
	db, provider := migration037Scratch(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(),
		`SELECT set_config('app.tenant_id','1',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO task_run_snapshot_v2_cutover_events (
		    tenant_id,user_id,task_id,generation,action,
		    approved_definition_version,approved_definition_digest,
		    snapshot_high_watermark,audit_from_snapshot_id,
		    audit_count,audit_through_id
		) VALUES (
		    1,1,'migration-037-retained',1,'activate',
		    1,repeat('a',64),1,1,1,1
		)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = provider.Down(t.Context())
	if err == nil || !strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("037 downgrade accepted durable event: %v", err)
	}
	var version, events int
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM task_run_snapshot_v2_cutover_events)`,
	).Scan(&version, &events); err != nil {
		t.Fatal(err)
	}
	if version != 37 || events != 1 {
		t.Fatalf("failed downgrade version/events = %d/%d", version, events)
	}
}
