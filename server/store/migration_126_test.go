package store

import (
	"bytes"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/server/runcontext"
)

func TestMigration126FreezesPlannerToolSearchAuthority(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/126_research_planner_tool_search.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"research_planner_tool_search_receipts",
		"vane.research-planner-tool-search-receipt/v1",
		"research-planner.render/v3.3",
		"vane.research-planner-output/v3.3",
		"research_planner_search_canonical_v126",
		"enforce_research_planner_search_receipt_v126",
		"protect_research_planner_search_receipt_v126",
		"research_plan_matches_planner_completion_v126",
		"enforce_research_run_plan_llm_receipt_v126",
		"planner_llm_spend_reservation_id",
		"settlement.outcome='completed'",
		"call.research_run_llm_spend_reservation_id=reservation.id",
		"receipt.round_ordinal<final_round",
		"match->>'name'=step->>'tool_name'",
		"research_run_capability_allows_v1",
		"126: v3.3 planner history exists",
		"DROP TABLE research_planner_tool_search_receipts",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 126 lost authority guard %q", required)
		}
	}
	storeSource, err := os.ReadFile(filepath.Join("store.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"research_planner_tool_search_receipts",
		"enforce_research_planner_search_receipt_v126",
		"research_plan_matches_planner_completion_v126",
		"enforce_research_run_plan_llm_receipt_v126",
		"trigger.tgenabled='O'",
	} {
		if !bytes.Contains(storeSource, []byte(required)) {
			t.Fatalf("research runtime probe lost v126 authority %q", required)
		}
	}
}

func TestMigration126CanonicalReceiptParityPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 126); err != nil {
		t.Fatal(err)
	}
	receipt, err := runcontext.BuildResearchPlannerToolSearchReceiptV1(
		1, strings.Repeat("a", 64), "official <release> \u2028 & status", 2,
		[]runcontext.ResearchPlannerToolSearchMatchV1{{
			Name: "web_search", SchemaDigest: strings.Repeat("b", 64),
			Score: "1.250000000",
		}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := runcontext.EncodeResearchPlannerToolSearchReceiptV1(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var rebuilt []byte
	if err := db.QueryRowContext(t.Context(),
		`SELECT research_planner_search_canonical_v126($1)`, payload,
	).Scan(&rebuilt); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, rebuilt) {
		t.Fatalf("Go/SQL canonical bytes differ\nGo:  %s\nSQL: %s", payload, rebuilt)
	}
	for _, mutation := range [][]byte{
		bytes.Replace(payload, []byte(`"score":"1.250000000"`),
			[]byte(`"score":"1.25"`), 1),
		bytes.Replace(payload, []byte(`"limit":2`), []byte(`"limit":"2"`), 1),
		bytes.Replace(payload, []byte(`"query":`), []byte(`"query":"bad","query":`), 1),
	} {
		var valid bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT research_planner_search_canonical_v126($1) IS NOT NULL`, mutation,
		).Scan(&valid); err != nil {
			t.Fatal(err)
		}
		if valid {
			t.Fatalf("invalid receipt crossed SQL canonicalizer: %s", mutation)
		}
	}
}

func TestMigration126EmptyDownRestoresV125Postgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 126); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 125); err != nil {
		t.Fatal(err)
	}
	var clean, retained, triggerRestored bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT to_regclass('public.research_planner_tool_search_receipts') IS NULL
		   AND to_regprocedure(
		       'public.research_planner_search_canonical_v126(bytea)') IS NULL
		   AND to_regprocedure(
		       'public.research_plan_matches_planner_completion_v126(bytea,text)') IS NULL
		   AND to_regprocedure(
		       'public.enforce_research_run_plan_llm_receipt_v126()') IS NULL,
		       to_regprocedure(
		       'public.research_plan_matches_planner_completion_v1(bytea,text)') IS NOT NULL,
		       EXISTS (
		         SELECT 1 FROM pg_catalog.pg_trigger trigger
		         JOIN pg_catalog.pg_proc function ON function.oid=trigger.tgfoid
		          WHERE trigger.tgrelid='public.research_run_plans'::regclass
		            AND trigger.tgname='research_run_plan_llm_receipt_v1'
		            AND function.proname='enforce_research_run_plan_llm_receipt_v1'
		            AND trigger.tgenabled='O')
	`).Scan(&clean, &retained, &triggerRestored); err != nil {
		t.Fatal(err)
	}
	if !clean || !retained || !triggerRestored {
		t.Fatalf("migration 126 Down clean=%v retained_v104=%v trigger_restored=%v",
			clean, retained, triggerRestored)
	}
	rolledBackStore, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rolledBackStore.Close)
	var tenantID int64
	if err := rolledBackStore.pool.QueryRow(t.Context(), `
		INSERT INTO tenants (status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := rolledBackStore.PurgeTenant(t.Context(), tenantID, false); err != nil {
		t.Fatalf("current binary cannot purge after 126 rollback: %v", err)
	}
}
