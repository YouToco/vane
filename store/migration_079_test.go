package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestManualRunSnapshotAdmissionMigrationIsExactAndIrreversible(
	t *testing.T,
) {
	raw, err := fs.ReadFile(
		migrationsFS,
		"migrations/079_manual_run_snapshot_admission.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, required := range []string{
		"NEW.temporal_workflow_id = 'wf-manual-' || c.id::text",
		"c.tenant_id = NEW.tenant_id",
		"c.user_id = NEW.user_id",
		"c.task_id = NEW.task_id",
		"c.kind = 'run'",
		"c.status IN ('pending', 'completed')",
		"FOR SHARE",
		"schedule_status = 'paused' AND exact_manual_run",
		"079: manual run snapshot admission is irreversible",
	} {
		if !strings.Contains(sqlText, required) {
			t.Errorf("migration 079 missing %q", required)
		}
	}
}

func TestManualRunSnapshotAdmissionMigrationDownRefuses(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 079 integration tests")
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
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 79); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 78); err == nil ||
		!strings.Contains(err.Error(), "manual run snapshot admission is irreversible") {
		t.Fatalf("DownTo(78) err=%v", err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT COALESCE(MAX(version_id), 0)
		   FROM goose_db_version
		  WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 79 {
		t.Fatalf("migration version after refused Down=%d, want 79", version)
	}
}
