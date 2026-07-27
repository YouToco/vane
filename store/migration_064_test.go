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
		publicRecoveryGate              bool
		coordinatorRecoveryGate         bool
		writerEmptyTerminal             bool
		receiptEmptyCompletion          bool
		writerEmptyColumns              bool
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
		        'public','enforce_brief_snapshot_admission_v1()','EXECUTE'),
		    has_function_privilege(
		        'public',
		        'canonical_brief_push_recovery_admitted_v1(bigint,bigint,text,bigint,bigint)',
		        'EXECUTE'),
		    has_function_privilege(
		        'vane_push_effect_coordinator',
		        'canonical_brief_push_recovery_admitted_v1(bigint,bigint,text,bigint,bigint)',
		        'EXECUTE'),
		    has_function_privilege(
		        'vane_brief_writer',
		        'canonical_brief_empty_terminal_v1(bigint,bigint,text,bigint)',
		        'EXECUTE'),
		    has_function_privilege(
		        'vane_push_effect_receipt',
		        'complete_canonical_empty_push_batch_v1(bigint,bigint,bigint,bigint)',
		        'EXECUTE'),
		    has_column_privilege(
		        'vane_brief_writer','push_batches','delivery_authority','SELECT')
		    AND has_column_privilege(
		        'vane_brief_writer','push_batches','idempotency_key','SELECT')
		    AND has_column_privilege(
		        'vane_brief_writer','push_batches','status','SELECT')`,
	).Scan(
		&appRead, &appWrite, &writerDelete,
		&writerSelect, &writerInsert, &writerUpdatePayload,
		&publicStageTrigger, &publicBriefTrigger,
		&publicRecoveryGate, &coordinatorRecoveryGate,
		&writerEmptyTerminal, &receiptEmptyCompletion,
		&writerEmptyColumns,
	); err != nil {
		t.Fatal(err)
	}
	if appRead || appWrite || writerDelete ||
		!writerSelect || !writerInsert || writerUpdatePayload ||
		publicStageTrigger || publicBriefTrigger || publicRecoveryGate ||
		!coordinatorRecoveryGate || !writerEmptyTerminal ||
		!receiptEmptyCompletion || !writerEmptyColumns {
		t.Fatalf(
			"unsafe 064 ACL app=%t/%t writer=%t/%t/%t/%t public=%t/%t recovery=%t/%t empty=%t/%t/%t",
			appRead, appWrite, writerDelete, writerSelect, writerInsert,
			writerUpdatePayload, publicStageTrigger, publicBriefTrigger,
			publicRecoveryGate, coordinatorRecoveryGate,
			writerEmptyTerminal, receiptEmptyCompletion,
			writerEmptyColumns)
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

func TestMigration064DownRefusesUnsettledSealedEmptyEvidence(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect authority=%q err=%v", winner, err)
	}
	if err := f.base.st.SealEmptyBriefBatchV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
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
		!strings.Contains(
			err.Error(),
			"refusing to drop unsettled canonical Brief empty evidence",
		) {
		t.Fatalf("064 Down accepted unsettled empty evidence: %v", err)
	}
}

func TestMigration064DownAllowsFinalizedSealedEmptyReceipt(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil || winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect authority=%q err=%v", winner, err)
	}
	if err := f.base.st.SealEmptyBriefBatchV1(
		t.Context(), f.identity, f.ref, marker, f.batchID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.CompleteEmptyPushEffectBatch(
		t.Context(),
		types.PushBatchScope{
			TenantID: f.identity.TenantID,
			UserID:   f.identity.UserID,
			BatchID:  f.batchID,
		},
		f.ref.SnapshotID,
	); err != nil {
		t.Fatal(err)
	}
	claim := types.RunOutcomeClaimV1{
		RunOutcomeMarkerV1: marker,
		Result:             types.RunResultQuiet,
		SourceCoverage:     types.RunCompletenessComplete,
		Processing:         types.RunCompletenessComplete,
	}
	if _, err := f.base.st.FinalizeRunOutcomeClaimV1(
		t.Context(), f.identity, f.ref, claim); err != nil {
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
		t.Fatalf("064 Down rejected settled empty evidence: %v", err)
	}
	defer func() {
		if _, upErr := provider.UpTo(context.Background(), 64); upErr != nil {
			t.Errorf("restore latest migration: %v", upErr)
		}
	}()
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
	if strings.Count(string(raw),
		"encode(sha256(payload),'hex')") != 1 {
		t.Fatal("064 stage request digest is not bound to exact draft bytes")
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

func TestTenantPurgeRemainsValidAfterSafe064Down(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
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
	report, err := f.base.st.PurgeTenant(
		t.Context(), f.identity.TenantID, false)
	if err != nil {
		t.Fatalf("current binary purge after 064 Down: %v", err)
	}
	if report.Rows["canonical_brief_stages"] != 0 ||
		report.Rows["tenants"] != 1 {
		t.Fatalf("safe-down purge report = %+v", report)
	}
}
