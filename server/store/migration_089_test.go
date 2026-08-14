package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration089InstallsExactToolPolicyPlanFence(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
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
	var definition string
	var publicExecute bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT pg_get_functiondef(p.oid),
		        has_function_privilege('public',p.oid,'EXECUTE')
		   FROM pg_proc p WHERE p.proname='enforce_research_run_plan_v3'`,
	).Scan(&definition, &publicExecute); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"tool_policy_digest", "snapshot.tool_policy_digest",
		"vane.research-execution-plan/v3",
	} {
		if !strings.Contains(definition, required) {
			t.Fatalf("089 plan fence omitted %q", required)
		}
	}
	if publicExecute {
		t.Fatal("089 plan fence is executable by PUBLIC")
	}
	if _, err := provider.DownTo(t.Context(), 88); err != nil {
		t.Fatalf("empty 089 Down: %v", err)
	}
	if err := database.QueryRowContext(t.Context(),
		`SELECT pg_get_functiondef(p.oid)
		   FROM pg_proc p WHERE p.proname='enforce_research_run_plan_v3'`,
	).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(definition, "tool_policy_digest") {
		t.Fatal("089 Down did not restore the retained 086 plan fence")
	}
}

func TestMigration089SQLGuardsDowngradeWithRetainedPlans(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/089_research_plan_tool_policy_fence.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"IF EXISTS (SELECT 1 FROM research_run_plans)",
		"refusing downgrade while Tool-policy-bound plans exist",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("089 migration omitted %q", required)
		}
	}
}
