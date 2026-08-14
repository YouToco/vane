package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

func TestMigration105AdmissionACLAndIrreversibleDowngradePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
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
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 105); err != nil {
		t.Fatal(err)
	}

	var publicExecute, executorExecute, securityDefiner, safeConfig, shadowFence bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_function_privilege('public',p.oid,'EXECUTE'),
		       has_function_privilege('vane_research_v3_executor',p.oid,'EXECUTE'),
		       p.prosecdef,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[],
		       position('research-v3-shadow-[0-9a-f]{64}' IN pg_get_functiondef(p.oid))>0
		  FROM pg_proc p
		 WHERE p.oid='admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure`,
	).Scan(&publicExecute, &executorExecute, &securityDefiner, &safeConfig, &shadowFence); err != nil {
		t.Fatal(err)
	}
	if publicExecute || !executorExecute || !securityDefiner || !safeConfig || !shadowFence {
		t.Fatalf("unsafe 105 function public=%v executor=%v definer=%v config=%v shadow=%v",
			publicExecute, executorExecute, securityDefiner, safeConfig, shadowFence)
	}

	if _, err := provider.DownTo(t.Context(), 104); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade after shadow Tool admission authority") {
		t.Fatalf("105 downgrade unexpectedly succeeded or lost guard: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 105 {
		t.Fatalf("failed 105 downgrade changed version to %d", version)
	}
}

func TestResearchShadowToolAdmissionUsesExactPreparedSidecarPostgres(t *testing.T) {
	t.Run("active exact prepared shadow admits and replays once", func(t *testing.T) {
		f := newResearchShadowRunSpendFixtureV3(t, 1_000_000)
		first, err := f.begin(t, 0)
		if err != nil || !first.FirstWriter {
			t.Fatalf("prepared shadow first admission=%+v err=%v", first, err)
		}

		// Once the external effect has a durable first-writer receipt, replay is
		// recovery rather than new authorization.  Roll back the sidecar and
		// prove replay returns the same claim without another quota debit.
		if _, err := f.store.pool.Exec(t.Context(),
			`DELETE FROM research_v3_prepared_definition_heads
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			f.tenantID, f.userID, f.identity.TaskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE research_v3_definition_prepare_operations
			    SET phase='rolled_back'
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			f.tenantID, f.userID, f.identity.TaskID); err != nil {
			t.Fatal(err)
		}
		replay, err := f.begin(t, 0)
		if err != nil || replay.FirstWriter || replay.StepID != first.StepID ||
			replay.SpendReservationID != first.SpendReservationID {
			t.Fatalf("prepared shadow replay=%+v first=%+v err=%v", replay, first, err)
		}
		starts, reservations := f.spendCounts(t)
		if starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
			t.Fatalf("prepared shadow replay mutated spend starts=%d reservations=%d quota=%v",
				starts, reservations, f.quotaTokens(t))
		}
		if next, err := f.begin(t, 1); err == nil || next.StepID != 0 {
			t.Fatalf("rolled-back shadow admitted new ordinal=%+v err=%v", next, err)
		}
		starts, reservations = f.spendCounts(t)
		if starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
			t.Fatalf("rejected new ordinal mutated spend starts=%d reservations=%d quota=%v",
				starts, reservations, f.quotaTokens(t))
		}
	})

	for name, mutate := range map[string]func(*testing.T, researchRunSpendFixtureV3){
		"prepared head removed": func(t *testing.T, f researchRunSpendFixtureV3) {
			_, err := f.store.pool.Exec(t.Context(),
				`DELETE FROM research_v3_prepared_definition_heads
				  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
				f.tenantID, f.userID, f.identity.TaskID)
			if err != nil {
				t.Fatal(err)
			}
		},
		"schedule paused": func(t *testing.T, f researchRunSpendFixtureV3) {
			if _, err := f.store.pool.Exec(t.Context(),
				`UPDATE schedules SET status='paused' WHERE id=$1`,
				f.identity.TaskID); err != nil {
				t.Fatal(err)
			}
		},
		"owner membership removed": func(t *testing.T, f researchRunSpendFixtureV3) {
			if _, err := f.store.pool.Exec(t.Context(),
				`UPDATE memberships SET role='member'
				  WHERE tenant_id=$1 AND user_id=$2`, f.tenantID, f.userID); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name+" fails before spend", func(t *testing.T) {
			f := newResearchShadowRunSpendFixtureV3(t, 1_000_000)
			mutate(t, f)
			if execution, err := f.begin(t, 0); err == nil || execution.StepID != 0 {
				t.Fatalf("unauthorized shadow admission=%+v err=%v", execution, err)
			}
			starts, reservations := f.spendCounts(t)
			if starts != 0 || reservations != 0 || f.quotaTokens(t) != 10 {
				t.Fatalf("unauthorized shadow spent starts=%d reservations=%d quota=%v",
					starts, reservations, f.quotaTokens(t))
			}
		})
	}
}

func TestResearchShadowToolAdmissionSerializesRevocationWithoutDeadlockPostgres(t *testing.T) {
	type mutation struct {
		name      string
		lockSQL   string
		revokeSQL string
		args      func(researchRunSpendFixtureV3) []any
	}
	mutations := []mutation{
		{"membership", `SELECT 1 FROM memberships WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`,
			`UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`,
			func(f researchRunSpendFixtureV3) []any { return []any{f.tenantID, f.userID} }},
		// status/deleted_at do not change a tenant key. Match PostgreSQL's
		// real UPDATE lock strength so the test does not invent a conflict
		// with child-row foreign-key KEY SHARE locks.
		{"tenant", `SELECT 1 FROM tenants WHERE id=$1 FOR NO KEY UPDATE`,
			`UPDATE tenants SET status='suspended' WHERE id=$1`,
			func(f researchRunSpendFixtureV3) []any { return []any{f.tenantID} }},
		{"schedule", `SELECT 1 FROM schedules WHERE id=$1 FOR UPDATE`,
			`UPDATE schedules SET status='paused' WHERE id=$1`,
			func(f researchRunSpendFixtureV3) []any { return []any{f.identity.TaskID} }},
		{"prepared head", `SELECT 1 FROM research_v3_prepared_definition_heads
			WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 FOR UPDATE`,
			`DELETE FROM research_v3_prepared_definition_heads
			WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			func(f researchRunSpendFixtureV3) []any {
				return []any{f.tenantID, f.userID, f.identity.TaskID}
			}},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			f := newResearchShadowRunSpendFixtureV3(t, 1_000_000)
			ctx := t.Context()
			gate, err := f.store.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Rollback(ctx)
			budgetKey := "research-spend/v3:" + f.identity.TemporalRunID + ":budget"
			if _, err := gate.Exec(ctx,
				`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, budgetKey); err != nil {
				t.Fatal(err)
			}
			var gatePID int
			if err := gate.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&gatePID); err != nil {
				t.Fatal(err)
			}

			revoke, err := f.store.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer revoke.Rollback(ctx)
			var revokePID int
			if err := revoke.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&revokePID); err != nil {
				t.Fatal(err)
			}
			args := test.args(f)
			var one int
			if err := revoke.QueryRow(ctx, test.lockSQL, args...).Scan(&one); err != nil {
				t.Fatal(err)
			}

			type beginResult struct {
				execution ResearchRunStepExecutionV3
				err       error
			}
			begun := make(chan beginResult, 1)
			go func() {
				execution, beginErr := f.store.BeginResearchRunStepV3(
					ctx, f.identity, f.snapshotID, f.planRef, 0)
				begun <- beginResult{execution: execution, err: beginErr}
			}()
			waitForResearchAdmissionLockV3(
				t, f.store, f.identity.TemporalRunID, gatePID)

			revoked := make(chan error, 1)
			go func() {
				_, revokeErr := revoke.Exec(ctx, test.revokeSQL, args...)
				revoked <- revokeErr
			}()
			// Let the revocation reach migration 102's exact-task exclusive
			// fence while admission owns the shared fence, then release the
			// budget gate. A row-locking authorization SELECT deadlocks here.
			waitForBackendAdvisoryLockV3(t, f.store, revokePID,
				test.name+" revocation")
			if err := gate.Rollback(ctx); err != nil {
				t.Fatal(err)
			}

			select {
			case got := <-begun:
				if got.err != nil || !got.execution.FirstWriter {
					t.Fatalf("shadow admission during %s revocation=%+v err=%v",
						test.name, got.execution, got.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("shadow admission deadlocked with %s revocation", test.name)
			}
			select {
			case err := <-revoked:
				if err != nil {
					t.Fatalf("%s revocation deadlocked: %v", test.name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s revocation did not serialize", test.name)
			}
			if err := revoke.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if next, err := f.begin(t, 1); err == nil || next.StepID != 0 {
				t.Fatalf("%s revocation admitted new ordinal=%+v err=%v",
					test.name, next, err)
			}
			starts, reservations := f.spendCounts(t)
			if starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
				t.Fatalf("%s revocation spend starts=%d reservations=%d quota=%v",
					test.name, starts, reservations, f.quotaTokens(t))
			}
		})
	}
}

func waitForResearchAdmissionLockV3(
	t *testing.T, st *Store, temporalRunID string, blockerPID int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			 WHERE datname=current_database()
			   AND query LIKE '%admit_research_run_tool_step_cap_v1%'
			   AND query NOT LIKE '%pg_stat_activity%'
			   AND wait_event_type='Lock' AND wait_event='advisory'
			   AND $1=ANY(pg_blocking_pids(pid))
		)`, blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("research admission for %s did not reach the budget lock", temporalRunID)
}

func waitForBackendAdvisoryLockV3(t *testing.T, st *Store, pid int, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			 WHERE datname=current_database() AND pid=$1
			   AND wait_event_type='Lock' AND wait_event='advisory'
		)`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not reach the exact-task advisory lock", label)
}

func TestMigration105SQLKeepsFormalAndShadowAuthorizationOrthogonal(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/105_research_shadow_tool_admission.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"schedule.execution_mode='discover_at_run'",
		"authorize_research_manual_task_run_cap_v1",
		"^research-v3-shadow-[0-9a-f]{64}$",
		"research_v3_prepared_definition_heads",
		"research_v3_definition_prepare_operations",
		"operation.phase='prepared'",
		"membership.role='owner'",
		"schedule.status='active'",
		"head.definition_digest=snapshot_row.definition_digest",
		"refusing downgrade after shadow Tool admission authority",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("105 migration omitted %q", required)
		}
	}
	if strings.Contains(sqlText,
		"FOR SHARE OF schedule,tenant,membership,head,operation,definition") {
		t.Fatal("shadow authorization reintroduced advisory-to-row lock inversion")
	}
}
