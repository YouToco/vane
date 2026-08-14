package store

import (
	"database/sql"
	"io/fs"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration065DownRemovesDatabaseLocalReaderCapability(t *testing.T) {
	freshURL := freshMigrationDatabase(t, "vane_brief_reader_down")
	db, err := sql.Open("pgx", freshURL)
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
	if _, err := provider.UpTo(t.Context(), 65); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 64); err != nil {
		t.Fatal(err)
	}
	var (
		briefRead, outcomeRead, feedbackRead bool
		readerPolicy                         bool
		version                              int64
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_brief_reader','brief_snapshots','payload','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','task_run_outcomes','result','SELECT'),
		       has_column_privilege(
		           'vane_brief_reader','feedbacks','action','SELECT'),
		       COALESCE(max(version_id),0)
		  FROM goose_db_version
		 WHERE is_applied
		 GROUP BY 1,2,3`,
	).Scan(
		&briefRead, &outcomeRead, &feedbackRead, &version,
	); err != nil {
		t.Fatal(err)
	}
	if briefRead || outcomeRead || feedbackRead || version != 64 {
		t.Fatalf(
			"Down retained local capability brief=%t outcome=%t feedback=%t version=%d",
			briefRead, outcomeRead, feedbackRead, version,
		)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (
		    SELECT 1
		      FROM pg_policy p
		      JOIN pg_class c ON c.oid=p.polrelid
		      JOIN pg_namespace n ON n.oid=c.relnamespace
		     WHERE n.nspname='public' AND c.relname='feedbacks'
		       AND p.polname='brief_reader_identity'
		)`,
	).Scan(&readerPolicy); err != nil {
		t.Fatal(err)
	}
	if readerPolicy {
		t.Fatal("Down retained brief_reader_identity policy")
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("065 re-up failed: %v", err)
	}
	if err := db.QueryRowContext(t.Context(),
		`SELECT has_column_privilege(
		     'vane_brief_reader','brief_snapshots','payload','SELECT')`,
	).Scan(&briefRead); err != nil {
		t.Fatal(err)
	}
	if !briefRead {
		t.Fatal("065 re-up did not restore Brief read")
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (
		    SELECT 1
		      FROM pg_policy p
		      JOIN pg_class c ON c.oid=p.polrelid
		     WHERE c.relname='feedbacks'
		       AND p.polname='brief_reader_identity'
		)`,
	).Scan(&readerPolicy); err != nil {
		t.Fatal(err)
	}
	if !readerPolicy {
		t.Fatal("065 re-up did not restore feedback reader policy")
	}
}
