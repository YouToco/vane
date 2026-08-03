package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration109NativeResearchCreationBoundaryPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 109); err != nil {
		t.Fatal(err)
	}

	for _, signature := range []string{
		"commit_native_research_task_creation_v3_v1(text,bigint,bigint,text,bigint,text,text,bytea,bytea,bytea,bytea,text,text,smallint)",
		"begin_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,smallint)",
		"commit_native_research_task_activation_v3_v1(text,bigint,bigint,text,bigint,text,smallint)",
	} {
		var publicExecute, appExecute, securityDefiner, safePath bool
		var owner string
		if err := db.QueryRowContext(t.Context(), `
			SELECT has_function_privilege('public',p.oid,'EXECUTE'),
			       has_function_privilege('vane_app',p.oid,'EXECUTE'),
			       p.prosecdef,p.proowner::regrole::text,
			       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
			  FROM pg_proc p WHERE p.oid=$1::regprocedure`, signature).Scan(
			&publicExecute, &appExecute, &securityDefiner, &owner, &safePath); err != nil {
			t.Fatal(err)
		}
		if publicExecute || !appExecute || !securityDefiner || owner == "vane_app" || !safePath {
			t.Fatalf("unsafe native V3 function %s public=%v app=%v definer=%v owner=%q path=%v",
				signature, publicExecute, appExecute, securityDefiner, owner, safePath)
		}
	}

	var constraintDefinition string
	if err := db.QueryRowContext(t.Context(), `
		SELECT pg_get_constraintdef(oid)
		  FROM pg_constraint
		 WHERE conname='task_creation_operations_execution_version_current'`,
	).Scan(&constraintDefinition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(constraintDefinition, "ANY (ARRAY[1, 2])") {
		t.Fatalf("migration 109 did not retain only V1/V2 creation protocols: %s",
			constraintDefinition)
	}

	migration, err := fs.ReadFile(dir, "109_native_research_task_creation.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := strings.Split(string(migration), "-- +goose Down")[0]
	for _, forbidden := range []string{"task_fetch_targets", "fetch_targets", "tool_calls"} {
		if strings.Contains(strings.ToLower(up), forbidden) {
			t.Fatalf("native V3 creation migration references retired execution state %q", forbidden)
		}
	}

	if _, err := provider.DownTo(t.Context(), 108); err != nil {
		t.Fatal(err)
	}
}
