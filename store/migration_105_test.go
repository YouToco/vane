package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration105AdmissionACLAndIrreversibleDowngradePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
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
}
