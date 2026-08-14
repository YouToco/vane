package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration090SpendBoundaryAndEmptyDown(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 090 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 90); err != nil {
		t.Fatal(err)
	}

	var reservations, settlements, binding, bindingUnique bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.research_run_step_spend_reservations') IS NOT NULL,
		       to_regclass('public.research_run_step_spend_settlements') IS NOT NULL,
		       EXISTS (
		           SELECT 1 FROM information_schema.columns
		            WHERE table_schema='public' AND table_name='tool_calls'
		              AND column_name='research_run_step_spend_reservation_id'
		       ),
		       to_regclass('public.uq_tool_calls_research_step_spend_reservation') IS NOT NULL`,
	).Scan(&reservations, &settlements, &binding, &bindingUnique); err != nil {
		t.Fatal(err)
	}
	if !reservations || !settlements || !binding || !bindingUnique {
		t.Fatalf("090 objects reservation=%v settlement=%v binding=%v unique=%v",
			reservations, settlements, binding, bindingUnique)
	}

	for _, table := range []string{
		"research_run_step_spend_reservations",
		"research_run_step_spend_settlements",
	} {
		var rls, insert, update, deletePrivilege, truncate bool
		if err := database.QueryRowContext(t.Context(), `
			SELECT c.relrowsecurity,
			       has_table_privilege('vane_research_v3_executor',$1,'INSERT'),
			       has_table_privilege('vane_research_v3_executor',$1,'UPDATE'),
			       has_table_privilege('vane_research_v3_executor',$1,'DELETE'),
			       has_table_privilege('vane_research_v3_executor',$1,'TRUNCATE')
			  FROM pg_class c WHERE c.oid=$1::regclass`, table,
		).Scan(&rls, &insert, &update, &deletePrivilege, &truncate); err != nil {
			t.Fatal(err)
		}
		// A column-level INSERT grant intentionally makes the table-level query
		// false. Store writers receive only the explicit immutable columns.
		if !rls || insert || update || deletePrivilege || truncate {
			t.Fatalf("%s privileges rls=%v insert=%v update=%v delete=%v truncate=%v",
				table, rls, insert, update, deletePrivilege, truncate)
		}
	}
	var reservationColumnInsert, settlementColumnInsert bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_research_v3_executor','research_run_step_spend_reservations',
		           'reserved_cost_micro_usd','INSERT'),
		       has_column_privilege(
		           'vane_research_v3_executor','research_run_step_spend_settlements',
		           'actual_cost_micro_usd','INSERT')`,
	).Scan(&reservationColumnInsert, &settlementColumnInsert); err != nil {
		t.Fatal(err)
	}
	if !reservationColumnInsert || !settlementColumnInsert {
		t.Fatalf("narrow inserts reservation=%v settlement=%v",
			reservationColumnInsert, settlementColumnInsert)
	}

	rows, err := database.QueryContext(t.Context(), `
		SELECT p.proname,has_function_privilege('public',p.oid,'EXECUTE'),
		       p.proowner::regrole::text,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.proname IN (
		     'enforce_research_run_step_spend_reservation_v1',
		     'require_research_run_step_spend_reservation_v1',
		     'protect_bound_research_tool_call_v1',
		     'enforce_research_run_step_spend_settlement_v1',
		     'require_research_run_step_spend_settlement_v1',
		     'reserve_research_run_quota_v3'
		 ) ORDER BY p.proname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name, owner string
		var publicExecute, safeConfig bool
		if err := rows.Scan(&name, &publicExecute, &owner, &safeConfig); err != nil {
			t.Fatal(err)
		}
		if publicExecute || owner == "vane_app" || !safeConfig {
			t.Fatalf("unsafe 090 function %s public=%v owner=%q config=%v",
				name, publicExecute, owner, safeConfig)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 6 {
		t.Fatalf("090 function count=%d err=%v", count, err)
	}
	var quotaExecute, quotaUpdate, finiteConstraint bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_function_privilege(
		           'vane_research_v3_executor',
		           'reserve_research_run_quota_v3(bigint,text,double precision)',
		           'EXECUTE'),
		       has_table_privilege('vane_research_v3_executor','tenant_quota','UPDATE'),
		       EXISTS (
		           SELECT 1 FROM pg_constraint
		            WHERE conrelid='tenant_quota'::regclass
		              AND conname='ck_tenant_quota_finite_v3'
		       )`,
	).Scan(&quotaExecute, &quotaUpdate, &finiteConstraint); err != nil {
		t.Fatal(err)
	}
	if !quotaExecute || quotaUpdate || !finiteConstraint {
		t.Fatalf("quota boundary execute=%v direct_update=%v finite=%v",
			quotaExecute, quotaUpdate, finiteConstraint)
	}

	if _, err := provider.DownTo(t.Context(), 89); err != nil {
		t.Fatalf("empty 090 Down: %v", err)
	}
	if err := database.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.research_run_step_spend_reservations') IS NULL
		   AND to_regclass('public.research_run_step_spend_settlements') IS NULL
		   AND NOT EXISTS (
		       SELECT 1 FROM information_schema.columns
		        WHERE table_schema='public' AND table_name='tool_calls'
		          AND column_name='research_run_step_spend_reservation_id'
		   )`,
	).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if !reservations {
		t.Fatal("090 Down retained spend schema")
	}
}

func TestMigration090RefusesExistingStartedSteps(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 090 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 89); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO research_run_steps (
		    tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
		    step_ordinal,phase,invocation_id,tool_name,request_digest,
		    result_digest,cost_micro_usd,error_code,schema_version
		) VALUES (900001,900001,'m090-dark',900001,'run-m090-dark',$1,
		          0,'started','inv-m090','web_search',$1,NULL,0,NULL,
		          'vane.research-run-step/v3')`, sha); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 90); err == nil ||
		!strings.Contains(err.Error(), "pre-ledger V3 steps exist") {
		t.Fatalf("090 Up with historical start err=%v", err)
	}
}

func TestMigration090DownRefusesSpendEvidence(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 090 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 90); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("b", 64)
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO research_run_step_spend_reservations (
		    tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
		    temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
		    request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
		    reserved_cost_micro_usd,schema_version
		) VALUES (900002,900002,'m090-down',900002,900002,900002,
		          'run-m090-down',$1,0,'inv-m090','web_search',$1,$1,
		          'exa_calls',1,10000,
		          'vane.research-run-step-spend-reservation/v1')`, sha); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 89); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade while V3 spend authority") {
		t.Fatalf("090 Down with spend authority err=%v", err)
	}
}

func TestMigration090SQLContainsAtomicFences(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/090_research_run_step_spend.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"DEFERRABLE INITIALLY DEFERRED",
		"snapshot_json #> '{research_tools,allowed_tools}'",
		"reserved_cost_micro_usd",
		"research_run_step_spend_reservation_id",
		"V3-bound tool call is immutable",
		"SECURITY INVOKER",
		"pg_trigger_depth()<=1",
		"fk_tool_calls_tenant",
		"ON DELETE CASCADE",
		"refund_unattempted_research_quota_v3",
		"vane_research_runtime",
		"NOBYPASSRLS",
		"refusing downgrade while V3 spend authority or evidence exists",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("090 migration omitted %q", required)
		}
	}
}
