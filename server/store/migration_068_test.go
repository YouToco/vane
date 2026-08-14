package store

import (
	"database/sql"
	"io/fs"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration068EventEvidenceReaderIsLeastPrivilege(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	var (
		publicExecute, appExecute, writerExecute bool
		writerObserved, writerContent            bool
		writerSources, writerSnapshot            bool
	)
	if err := f.base.st.pool.QueryRow(t.Context(), `
		SELECT has_function_privilege(
		           'public',
		           'read_canonical_brief_event_evidence_v1(bigint,bigint,bigint,bigint)',
		           'EXECUTE'),
		       has_function_privilege(
		           'vane_app',
		           'read_canonical_brief_event_evidence_v1(bigint,bigint,bigint,bigint)',
		           'EXECUTE'),
		       has_function_privilege(
		           'vane_brief_writer',
		           'read_canonical_brief_event_evidence_v1(bigint,bigint,bigint,bigint)',
		           'EXECUTE'),
		       has_any_column_privilege(
		           'vane_brief_writer','task_observed_events','SELECT'),
		       has_any_column_privilege(
		           'vane_brief_writer','content_items','SELECT'),
		       has_any_column_privilege(
		           'vane_brief_writer','content_sources','SELECT'),
		       has_any_column_privilege(
		           'vane_brief_writer','task_run_snapshots','SELECT')`,
	).Scan(
		&publicExecute, &appExecute, &writerExecute,
		&writerObserved, &writerContent, &writerSources, &writerSnapshot,
	); err != nil {
		t.Fatal(err)
	}
	if publicExecute || appExecute || !writerExecute ||
		writerObserved || writerContent || writerSources || writerSnapshot {
		t.Fatalf(
			"unsafe 068 ACL public=%t app=%t writer=%t tables=%t/%t/%t/%t",
			publicExecute, appExecute, writerExecute,
			writerObserved, writerContent, writerSources, writerSnapshot)
	}
}

func TestMigration068DownRemovesEventEvidenceCapability(t *testing.T) {
	freshURL := freshMigrationDatabase(t, "brief_event_evidence_down")
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
	if _, err := provider.UpTo(t.Context(), 68); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 67); err != nil {
		t.Fatal(err)
	}
	var (
		functionExists bool
		version        int64
	)
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regprocedure(
		           'read_canonical_brief_event_evidence_v1(bigint,bigint,bigint,bigint)'
		       ) IS NOT NULL,
		       COALESCE(max(version_id),0)
		  FROM goose_db_version
		 WHERE is_applied
		 GROUP BY 1`,
	).Scan(&functionExists, &version); err != nil {
		t.Fatal(err)
	}
	if functionExists || version != 67 {
		t.Fatalf("Down retained function=%t version=%d",
			functionExists, version)
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("068 re-up failed: %v", err)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regprocedure(
		    'read_canonical_brief_event_evidence_v1(bigint,bigint,bigint,bigint)'
		) IS NOT NULL`,
	).Scan(&functionExists); err != nil {
		t.Fatal(err)
	}
	if !functionExists {
		t.Fatal("068 re-up did not restore event evidence reader")
	}
}
