package store

import (
	"io/fs"
	"strings"
	"testing"
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
