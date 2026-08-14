package store

import (
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration122KeepsGroundingAuthorityNarrowAndFailClosed(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration test path")
	}
	payload, err := os.ReadFile(filepath.Join(filepath.Dir(file), "migrations",
		"122_research_brief_grounding_verifier.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(payload)
	for _, required := range []string{
		"research_brief_grounding_verifications",
		"research-synthesis.render/v3.3",
		"grounding_verifier",
		"grounding.verifier_prompt=convert_to(requested_user_prompt,'UTF8')",
		"grounding.verifier_prompt_digest=",
		"round_ordinal IN (0,1)",
		"CREATE POLICY research_v3_scope",
		"CREATE POLICY research_v3_capability_scope",
		"admit_research_run_llm_spend_cap_v4",
		"REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v3",
		"grounding verification history exists",
		"LOCK TABLE research_brief_grounding_verifications",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 122 lost guard %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT DELETE ON research_brief_grounding_verifications",
		"GRANT TRUNCATE ON research_brief_grounding_verifications",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 122 widened destructive authority with %q", forbidden)
		}
	}
}

func TestMigration122GroundingPrivilegesAndExactEmptyDownPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 122 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 122); err != nil {
		t.Fatal(err)
	}
	var rls, tableInsert, tableUpdate, deletePrivilege, truncatePrivilege bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT relrowsecurity,
		       has_table_privilege('vane_research_v3_executor',$1,'INSERT'),
		       has_table_privilege('vane_research_v3_executor',$1,'UPDATE'),
		       has_table_privilege('vane_research_v3_executor',$1,'DELETE'),
		       has_table_privilege('vane_research_v3_executor',$1,'TRUNCATE')
		  FROM pg_class WHERE oid=$1::regclass`,
		"research_brief_grounding_verifications").Scan(
		&rls, &tableInsert, &tableUpdate, &deletePrivilege, &truncatePrivilege); err != nil {
		t.Fatal(err)
	}
	if !rls || tableInsert || tableUpdate || deletePrivilege || truncatePrivilege {
		t.Fatalf("grounding privilege boundary rls=%v insert=%v update=%v delete=%v truncate=%v",
			rls, tableInsert, tableUpdate, deletePrivilege, truncatePrivilege)
	}
	var candidateInsert, statusUpdate bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		has_column_privilege('vane_research_v3_executor',
		 'research_brief_grounding_verifications','candidate_digest','INSERT'),
		has_column_privilege('vane_research_v3_executor',
		 'research_brief_grounding_verifications','status','UPDATE')`,
	).Scan(&candidateInsert, &statusUpdate); err != nil {
		t.Fatal(err)
	}
	if !candidateInsert || !statusUpdate {
		t.Fatalf("grounding narrow writes insert=%v update=%v", candidateInsert, statusUpdate)
	}
	if _, err := provider.DownTo(t.Context(), 121); err != nil {
		t.Fatal(err)
	}
	var tableExists bool
	var triggerBody string
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regclass('public.research_brief_grounding_verifications') IS NOT NULL,
		pg_get_functiondef('enforce_research_run_llm_spend_reservation_v1()'::regprocedure)`,
	).Scan(&tableExists, &triggerBody); err != nil {
		t.Fatal(err)
	}
	if tableExists || strings.Contains(triggerBody, "research_brief_grounding_verifications") ||
		!strings.Contains(triggerBody, "091: synthesis reservation subject differs") {
		t.Fatalf("migration 122 down table=%v trigger=%s", tableExists, triggerBody)
	}
}
