package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration108ResearchEffectCapabilityACLPostgres(t *testing.T) {
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
	migration, err := fs.ReadFile(dir,
		"108_research_shadow_synthesis_admission.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(migration),
		"SELECT pg_advisory_xact_lock(6215335020355474248);") != 2 {
		t.Fatal("migration 108 Up/Down do not both hold the exclusive schema fence")
	}
	if _, err := provider.UpTo(t.Context(), 108); err != nil {
		t.Fatal(err)
	}

	var publicExecute, executorExecute, securityDefiner, safePath bool
	var owner, definition string
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_function_privilege('public',p.oid,'EXECUTE'),
		       has_function_privilege('vane_research_v3_executor',p.oid,'EXECUTE'),
		       p.prosecdef,p.proowner::regrole::text,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.oid='authorize_research_run_effect_cap_v1(bigint)'::regprocedure`,
	).Scan(&publicExecute, &executorExecute, &securityDefiner, &owner, &safePath); err != nil {
		t.Fatal(err)
	}
	if publicExecute || !executorExecute || !securityDefiner || owner == "vane_app" || !safePath {
		t.Fatalf("unsafe 108 capability public=%v executor=%v definer=%v owner=%q path=%v",
			publicExecute, executorExecute, securityDefiner, owner, safePath)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'authorize_research_run_effect_cap_v1(bigint)'::regprocedure)`,
	).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if strings.Count(definition,
		`^research-v3-shadow-[0-9a-f]{64}$`) != 1 {
		t.Fatalf("migration 108 lost exact shadow classifier: %s", definition)
	}

	if _, err := provider.DownTo(t.Context(), 107); err != nil {
		t.Fatal(err)
	}
	var functionExists bool
	if err := db.QueryRowContext(t.Context(), `SELECT to_regprocedure(
		'authorize_research_run_effect_cap_v1(bigint)') IS NOT NULL`,
	).Scan(&functionExists); err != nil {
		t.Fatal(err)
	}
	if functionExists {
		t.Fatal("migration 108 downgrade retained research effect capability")
	}
}
