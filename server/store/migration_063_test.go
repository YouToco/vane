package store

import (
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"
)

func TestMigration063RecoveryRoleIsReadOnlyAndKeysetBounded(t *testing.T) {
	first := newCanonicalBriefFixture(t, 0)
	second := newCanonicalBriefFixture(t, 0)
	oldest := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Microsecond)
	for index, fixture := range []*canonicalBriefFixture{first, second} {
		tx, err := fixture.base.st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(),
			`SELECT set_config('app.tenant_id',$1,true),
			        set_config('app.user_id',$2,true)`,
			fmt.Sprint(fixture.identity.TenantID),
			fmt.Sprint(fixture.identity.UserID)); err != nil {
			_ = tx.Rollback(t.Context())
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(),
			`INSERT INTO task_run_outcomes (
			    tenant_id,user_id,task_id,run_snapshot_id,
			    schema_version,created_at
			 ) VALUES ($1,$2,$3,$4,'vane.run-outcome/v1',$5)`,
			fixture.identity.TenantID, fixture.identity.UserID,
			fixture.identity.TaskID, fixture.ref.SnapshotID,
			oldest.Add(time.Duration(index)*time.Second)); err != nil {
			_ = tx.Rollback(t.Context())
			t.Fatal(err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	page, err := first.base.st.ListStaleRunOutcomeCandidatesV1(
		t.Context(), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Marker.RunSnapshotID != first.ref.SnapshotID ||
		page[0].Identity.TemporalWorkflowID !=
			first.identity.TemporalWorkflowID ||
		page[0].Identity.TemporalRunID != first.identity.TemporalRunID {
		t.Fatalf("first recovery page = %+v", page)
	}
	next, err := first.base.st.ListStaleRunOutcomeCandidatesV1(
		t.Context(), &RunOutcomeRecoveryCursorV1{
			CreatedAt: page[0].CreatedAt, ID: page[0].Marker.ID,
		}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 ||
		next[0].Marker.RunSnapshotID != second.ref.SnapshotID {
		t.Fatalf("second recovery page = %+v", next)
	}

	var (
		noLogin, noInherit, noBypass, noSuper bool
		ownerCanSet, appCanSet                bool
		tableRead, tableWrite, functionExec   bool
	)
	if err := first.base.st.pool.QueryRow(t.Context(), `
		SELECT NOT rolcanlogin,NOT rolinherit,NOT rolbypassrls,NOT rolsuper,
		       pg_has_role(current_user,oid,'SET'),
		       pg_has_role('vane_app',oid,'SET'),
		       has_table_privilege(
		           'vane_run_outcome_recovery',
		           'task_run_outcomes','SELECT'),
		       has_table_privilege(
		           'vane_run_outcome_recovery',
		           'task_run_outcomes','INSERT,UPDATE,DELETE'),
		       has_function_privilege(
		           'vane_run_outcome_recovery',
		           'read_stale_run_outcomes_v1(timestamptz,bigint,integer)',
		           'EXECUTE')
		  FROM pg_roles
		 WHERE rolname='vane_run_outcome_recovery'`,
	).Scan(
		&noLogin, &noInherit, &noBypass, &noSuper,
		&ownerCanSet, &appCanSet, &tableRead, &tableWrite, &functionExec,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !noSuper ||
		!ownerCanSet || appCanSet || tableRead || tableWrite || !functionExec {
		t.Fatalf(
			"unsafe recovery ACL attrs=%t/%t/%t/%t set=%t/%t table=%t/%t function=%t",
			noLogin, noInherit, noBypass, noSuper, ownerCanSet, appCanSet,
			tableRead, tableWrite, functionExec)
	}

	tx, err := first.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SET LOCAL ROLE vane_run_outcome_recovery`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`SELECT count(*) FROM task_run_outcomes`); err == nil {
		t.Fatal("recovery role obtained direct outcome table read")
	}
}

func TestMigration063ReaderDoesNotExposeFreshPendingRows(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.base.st.ListStaleRunOutcomeCandidatesV1(
		t.Context(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range page {
		if candidate.Marker.ID == marker.ID {
			t.Fatalf("fresh marker %d escaped stale grace", marker.ID)
		}
	}
	if _, err := f.base.st.ListStaleRunOutcomeCandidatesV1(
		t.Context(), nil, 101); err == nil {
		t.Fatal("reader admitted a page above its hard limit")
	}
}

func TestMigration063DownUsesCanonicalWriterFence(t *testing.T) {
	raw, err := fs.ReadFile(
		migrationsFS, "migrations/063_run_outcome_recovery.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw),
		"SELECT pg_advisory_xact_lock(6215335020355474248)") {
		t.Fatal("063 Down lacks the canonical writer admission fence")
	}
	if strings.Contains(string(raw),
		"REVOKE vane_run_outcome_recovery FROM CURRENT_USER") {
		t.Fatal("063 Down must not remove preexisting cluster role membership")
	}
}
