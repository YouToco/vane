package store

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/types"
)

func TestMigration064StageRoleIsLeastPrivilege(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	var (
		appRead, appWrite, writerDelete bool
		writerSelect, writerInsert      bool
		writerUpdatePayload             bool
		publicStageTrigger              bool
		publicBriefTrigger              bool
	)
	if err := f.base.st.pool.QueryRow(t.Context(), `
		SELECT
		    has_table_privilege('vane_app','canonical_brief_stages','SELECT'),
		    has_table_privilege(
		        'vane_app','canonical_brief_stages','INSERT,UPDATE,DELETE'),
		    has_table_privilege(
		        'vane_brief_writer','canonical_brief_stages','DELETE'),
		    has_any_column_privilege(
		        'vane_brief_writer','canonical_brief_stages','SELECT'),
		    has_any_column_privilege(
		        'vane_brief_writer','canonical_brief_stages','INSERT'),
		    has_column_privilege(
		        'vane_brief_writer','canonical_brief_stages','payload','UPDATE'),
		    has_function_privilege(
		        'public','enforce_canonical_brief_stage_authority_v1()','EXECUTE'),
		    has_function_privilege(
		        'public','enforce_brief_snapshot_admission_v1()','EXECUTE')`,
	).Scan(
		&appRead, &appWrite, &writerDelete,
		&writerSelect, &writerInsert, &writerUpdatePayload,
		&publicStageTrigger, &publicBriefTrigger,
	); err != nil {
		t.Fatal(err)
	}
	if appRead || appWrite || writerDelete ||
		!writerSelect || !writerInsert || writerUpdatePayload ||
		publicStageTrigger || publicBriefTrigger {
		t.Fatalf(
			"unsafe 064 ACL app=%t/%t writer=%t/%t/%t/%t public=%t/%t",
			appRead, appWrite, writerDelete, writerSelect, writerInsert,
			writerUpdatePayload, publicStageTrigger, publicBriefTrigger)
	}
}

func TestMigration064DownRefusesStageEvidence(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
		time.Date(2026, 7, 27, 7, 8, 9, 0, time.UTC),
		f.deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
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
	if _, err := provider.DownTo(t.Context(), 63); err == nil ||
		!strings.Contains(err.Error(),
			"refusing to drop canonical Brief stage evidence") {
		t.Fatalf("064 Down accepted staged evidence: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id)
		   FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 64 {
		t.Fatalf("failed 064 Down changed version to %d", version)
	}
}

func TestMigration064UsesCanonicalWriterFence(t *testing.T) {
	raw, err := fs.ReadFile(
		migrationsFS, "migrations/064_canonical_brief_stage.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw),
		"SELECT pg_advisory_xact_lock(6215335020355474248)") != 2 {
		t.Fatal("064 Up/Down do not both use the canonical writer fence")
	}
	if !strings.Contains(string(raw),
		"payload           BYTEA") {
		t.Fatal("064 stage payload is not byte-preserving BYTEA")
	}
}

func TestP1BFinalizerRemainsValidAfterSafe064Down(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
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
	if _, err := provider.DownTo(t.Context(), 63); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, upErr := provider.UpTo(context.Background(), 64); upErr != nil {
			t.Errorf("restore latest migration: %v", upErr)
		}
	}()
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultQuiet,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
	}
	if _, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim); err != nil {
		t.Fatalf("P1-B finalizer depended on migration 064: %v", err)
	}
}
