package store

import (
	"database/sql"
	"io/fs"
	"os"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration115DownRevokesProjectionAndIdentityFencesPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 115); err != nil {
		t.Fatal(err)
	}
	assertState := func(wantGrant, wantPolicy bool) {
		t.Helper()
		var grant bool
		if err := database.QueryRowContext(t.Context(), `SELECT
			has_column_privilege('vane_intelligence_reader',
			 'task_run_snapshots','reference_schema_version','SELECT')`).Scan(&grant); err != nil {
			t.Fatal(err)
		}
		var policyCount int
		if err := database.QueryRowContext(t.Context(), `
			SELECT count(*) FROM pg_policies
			 WHERE policyname='intelligence_reader_identity'
			   AND tablename IN ('schedules','schedule_playbooks','task_run_snapshots',
			       'task_run_outcomes','task_run_content_provenance','brief_snapshots','tool_calls')`,
		).Scan(&policyCount); err != nil {
			t.Fatal(err)
		}
		if grant != wantGrant || (policyCount == 7) != wantPolicy {
			t.Fatalf("migration115 grant=%v policies=%d", grant, policyCount)
		}
	}
	assertState(true, true)
	if _, err := provider.DownTo(t.Context(), 114); err != nil {
		t.Fatal(err)
	}
	assertState(false, false)
}
