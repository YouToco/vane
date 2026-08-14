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

const (
	researchAdmissionCapV4Signature = "admit_research_run_llm_spend_cap_v4(bigint,bigint,text,bigint,text,integer,bigint,text,text,text,text)"
	researchAdmissionCapV5Signature = "admit_research_run_llm_spend_cap_v5(bigint,bigint,text,bigint,text,integer,bigint,text,text,text,text)"
)

func TestMigration123VersionsV34GroundingAdmissionWithoutMutatingV33(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration test path")
	}
	payload, err := os.ReadFile(filepath.Join(filepath.Dir(file), "migrations",
		"123_research_v34_grounding_admission.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"enforce_research_run_llm_spend_reservation_v2",
		"EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v2()",
		"EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v1()",
		"admit_research_run_llm_spend_v5",
		"admit_research_run_llm_spend_cap_v5",
		"'research-synthesis.render/v3.3'",
		"'research-synthesis.render/v3.4'",
		"grounding.verifier_prompt=convert_to(requested_user_prompt,'UTF8')",
		"grounding.verifier_prompt_digest=",
		"current_research_run_capability_v1",
		"v3.4 snapshot history exists",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 123 lost guard %q", required)
		}
	}
	for _, forbidden := range []string{
		"CREATE OR REPLACE FUNCTION enforce_research_run_llm_spend_reservation_v1",
		"CREATE OR REPLACE FUNCTION admit_research_run_llm_spend_v4",
		"DROP FUNCTION admit_research_run_llm_spend_cap_v4",
		"REVOKE ALL ON FUNCTION admit_research_run_llm_spend_cap_v4",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("migration 123 mutated retained v3.3 admission with %q", forbidden)
		}
	}
}

func TestMigration123GroundingAdmissionPrivilegesAndEmptyDownPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 123 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 123); err != nil {
		t.Fatal(err)
	}
	var v4Exists, v5Exists, executorV4, executorV5, publicV5 bool
	var triggerFunction string
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regprocedure($1) IS NOT NULL,
		to_regprocedure($2) IS NOT NULL,
		has_function_privilege('vane_research_v3_executor',$1,'EXECUTE'),
		has_function_privilege('vane_research_v3_executor',$2,'EXECUTE'),
		has_function_privilege('public',$2,'EXECUTE')`,
		researchAdmissionCapV4Signature, researchAdmissionCapV5Signature,
	).Scan(&v4Exists, &v5Exists, &executorV4, &executorV5, &publicV5); err != nil {
		t.Fatal(err)
	}
	if !v4Exists || !v5Exists || !executorV4 || !executorV5 || publicV5 {
		t.Fatalf("admission versions v4=%v v5=%v executor_v4=%v executor_v5=%v public_v5=%v",
			v4Exists, v5Exists, executorV4, executorV5, publicV5)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT procedure.proname
		FROM pg_trigger trigger
		JOIN pg_proc procedure ON procedure.oid=trigger.tgfoid
		WHERE trigger.tgrelid='research_run_llm_spend_reservations'::regclass
		  AND trigger.tgname='research_run_llm_spend_reservation_v1'
		  AND NOT trigger.tgisinternal`,
	).Scan(&triggerFunction); err != nil {
		t.Fatal(err)
	}
	if triggerFunction != "enforce_research_run_llm_spend_reservation_v2" {
		t.Fatalf("migration 123 trigger function=%q", triggerFunction)
	}
	if _, err := provider.DownTo(t.Context(), 122); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regprocedure($1) IS NOT NULL,
		to_regprocedure($2) IS NOT NULL`,
		researchAdmissionCapV4Signature, researchAdmissionCapV5Signature,
	).Scan(&v4Exists, &v5Exists); err != nil {
		t.Fatal(err)
	}
	if !v4Exists || v5Exists {
		t.Fatalf("migration 123 down v4=%v v5=%v", v4Exists, v5Exists)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT procedure.proname
		FROM pg_trigger trigger
		JOIN pg_proc procedure ON procedure.oid=trigger.tgfoid
		WHERE trigger.tgrelid='research_run_llm_spend_reservations'::regclass
		  AND trigger.tgname='research_run_llm_spend_reservation_v1'
		  AND NOT trigger.tgisinternal`,
	).Scan(&triggerFunction); err != nil {
		t.Fatal(err)
	}
	if triggerFunction != "enforce_research_run_llm_spend_reservation_v1" {
		t.Fatalf("migration 123 down trigger function=%q", triggerFunction)
	}
}
