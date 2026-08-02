package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration104ResearchPlanReceiptProjectionPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 104); err != nil {
		t.Fatal(err)
	}

	plan := []byte(`{"schema_version":"vane.research-execution-plan/v3","definition_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","capability_catalog_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","tool_policy_digest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","steps":[{"invocation_id":"1","tool_name":"web_contents","arguments":{"page_url":"https://www.kimi.com/membership/pricing"}}]}`)
	formattedCompletion := `{
      "steps": [{"tool_name":"web_contents","arguments":{"page_url":"https://www.kimi.com/membership/pricing"},"invocation_id":"1"}],
      "schema_version": "vane.research-planner-output/v3"
    }`
	var matches bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT research_plan_matches_planner_completion_v1($1,$2)`,
		plan, formattedCompletion).Scan(&matches); err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Fatal("representation-only planner formatting did not match durable Plan steps")
	}

	mutations := []string{
		strings.Replace(formattedCompletion, "web_contents", "read_page", 1),
		strings.Replace(formattedCompletion, `"invocation_id":"1"`, `"invocation_id":"2"`, 1),
		strings.Replace(formattedCompletion, `"schema_version": "vane.research-planner-output/v3"`, `"schema_version": "vane.research-planner-output/v3", "extra": true`, 1),
		strings.Replace(string(plan), `"schema_version":"vane.research-execution-plan/v3"`, `"schema_version":"vane.research-execution-plan/v3","extra":true`, 1),
	}
	for index, mutation := range mutations {
		candidatePlan, candidateCompletion := plan, mutation
		if index == len(mutations)-1 {
			candidatePlan, candidateCompletion = []byte(mutation), formattedCompletion
		}
		if err := db.QueryRowContext(t.Context(),
			`SELECT research_plan_matches_planner_completion_v1($1,$2)`,
			candidatePlan, candidateCompletion).Scan(&matches); err != nil {
			t.Fatalf("mutation %d query: %v", index, err)
		}
		if matches {
			t.Fatalf("mutation %d crossed the planner receipt projection fence", index)
		}
	}

	var publicExecute, safeConfig bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_function_privilege(
		           'public',p.oid,'EXECUTE'),
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.proname='research_plan_matches_planner_completion_v1'`,
	).Scan(&publicExecute, &safeConfig); err != nil {
		t.Fatal(err)
	}
	if publicExecute || !safeConfig {
		t.Fatalf("unsafe projection helper public=%v config=%v", publicExecute, safeConfig)
	}

	if _, err := provider.DownTo(t.Context(), 103); err != nil {
		t.Fatal(err)
	}
	var helperRemoved, legacyRestored bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regprocedure(
		           'research_plan_matches_planner_completion_v1(bytea,text)') IS NULL,
		       position(
		           'convert_from(NEW.plan_payload,''UTF8'')::jsonb=call.completion::jsonb'
		           IN pg_get_functiondef(
		               'enforce_research_run_plan_llm_receipt_v1()'::regprocedure))>0`,
	).Scan(&helperRemoved, &legacyRestored); err != nil {
		t.Fatal(err)
	}
	if !helperRemoved || !legacyRestored {
		t.Fatalf("104 Down helper_removed=%v legacy_restored=%v",
			helperRemoved, legacyRestored)
	}
}

func TestMigration104SQLKeepsReceiptAndSnapshotBoundaries(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/104_research_plan_receipt_projection.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"planner_llm_spend_reservation_id",
		"settlement.outcome='completed'",
		"settlement.usage_known",
		"call.research_run_llm_spend_reservation_id=reservation.id",
		"research_plan_matches_planner_completion_v1",
		"vane.research-planner-output/v3",
		"vane.research-execution-plan/v3",
		"convert_from(plan_payload,'UTF8')::jsonb->'steps'",
		"REVOKE ALL ON FUNCTION research_plan_matches_planner_completion_v1",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("104 migration omitted %q", required)
		}
	}
}
