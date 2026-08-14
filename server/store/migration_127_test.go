package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration127DurableRecoveryCursorBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/127_schedule_command_recovery_cursor.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sql := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE schedule_command_recovery_cursors",
		"worker_key = 'scheduler'",
		"REVOKE ALL ON schedule_command_recovery_cursors FROM PUBLIC,vane_app",
		"GRANT SELECT,INSERT,UPDATE ON schedule_command_recovery_cursors",
		"TO vane_schedule_commander",
		"CREATE FUNCTION count_task_creation_capacity_v1(",
		"SECURITY DEFINER",
		"requested_tenant_id IS DISTINCT FROM",
		"current_setting('app.tenant_id',true)",
		"GRANT EXECUTE ON FUNCTION count_task_creation_capacity_v1(BIGINT,BIGINT)",
		"TO vane_app",
		"DROP FUNCTION IF EXISTS count_task_creation_capacity_v1(BIGINT,BIGINT)",
		"DROP TABLE IF EXISTS schedule_command_recovery_cursors",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("migration 127 lost boundary %q", fragment)
		}
	}
	if got := strings.Count(sql, "-- +goose StatementBegin"); got != 1 {
		t.Fatalf("migration 127 must keep its PL/pgSQL body in one goose statement, begin markers=%d", got)
	}
	if got := strings.Count(sql, "-- +goose StatementEnd"); got != 1 {
		t.Fatalf("migration 127 must keep its PL/pgSQL body in one goose statement, end markers=%d", got)
	}
}

func TestMigration127EmptyDownKeepsCurrentPurgeCompatible(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 127); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 126); err != nil {
		t.Fatal(err)
	}
	var clean bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.schedule_command_recovery_cursors') IS NULL
		   AND to_regprocedure(
		       'public.count_task_creation_capacity_v1(bigint,bigint)') IS NULL`,
	).Scan(&clean); err != nil {
		t.Fatal(err)
	}
	if !clean {
		t.Fatal("migration 127 Down retained recovery cursor authority")
	}
	rolledBackStore, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rolledBackStore.Close)
	var tenantID int64
	if err := rolledBackStore.pool.QueryRow(t.Context(), `
		INSERT INTO tenants (status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := rolledBackStore.PurgeTenant(t.Context(), tenantID, false); err != nil {
		t.Fatalf("current binary cannot purge after 127 rollback: %v", err)
	}
}
