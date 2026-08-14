package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration092ArtifactReceiptBoundaryAndEmptyDown(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 092 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 92); err != nil {
		t.Fatal(err)
	}

	var planColumn, briefColumn, planUnique, briefUnique bool
	var planInsert, briefUpdate bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT EXISTS (
		           SELECT 1 FROM information_schema.columns
		            WHERE table_schema='public' AND table_name='research_run_plans'
		              AND column_name='planner_llm_spend_reservation_id'
		              AND is_nullable='YES'),
		       EXISTS (
		           SELECT 1 FROM information_schema.columns
		            WHERE table_schema='public' AND table_name='research_brief_syntheses'
		              AND column_name='synthesis_llm_spend_reservation_id'
		              AND is_nullable='YES'),
		       to_regclass('public.uq_research_run_plan_planner_llm_reservation') IS NOT NULL,
		       to_regclass('public.uq_research_brief_synthesis_llm_reservation') IS NOT NULL,
		       has_column_privilege(
		           'vane_research_v3_executor','research_run_plans',
		           'planner_llm_spend_reservation_id','INSERT'),
		       has_column_privilege(
		           'vane_research_v3_executor','research_brief_syntheses',
		           'synthesis_llm_spend_reservation_id','UPDATE')`,
	).Scan(&planColumn, &briefColumn, &planUnique, &briefUnique,
		&planInsert, &briefUpdate); err != nil {
		t.Fatal(err)
	}
	if !planColumn || !briefColumn || !planUnique || !briefUnique ||
		!planInsert || !briefUpdate {
		t.Fatalf("092 boundary plan=%v/%v/%v brief=%v/%v/%v",
			planColumn, planUnique, planInsert, briefColumn, briefUnique, briefUpdate)
	}

	rows, err := database.QueryContext(t.Context(), `
		SELECT p.proname,has_function_privilege('public',p.oid,'EXECUTE'),
		       p.proowner::regrole::text,
		       p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		  FROM pg_proc p
		 WHERE p.proname IN (
		     'enforce_research_run_plan_llm_receipt_v1',
		     'enforce_research_brief_llm_receipt_v1'
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
			t.Fatalf("unsafe 092 function %s public=%v owner=%q config=%v",
				name, publicExecute, owner, safeConfig)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 2 {
		t.Fatalf("092 function count=%d err=%v", count, err)
	}

	if _, err := provider.DownTo(t.Context(), 91); err != nil {
		t.Fatalf("empty 092 Down: %v", err)
	}
	var removed, spendRetained bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT NOT EXISTS (
		           SELECT 1 FROM information_schema.columns
		            WHERE table_schema='public'
		              AND column_name IN (
		                  'planner_llm_spend_reservation_id',
		                  'synthesis_llm_spend_reservation_id')),
		       to_regclass('public.research_run_llm_spend_reservations') IS NOT NULL
		       AND to_regclass('public.research_run_llm_spend_settlements') IS NOT NULL`,
	).Scan(&removed, &spendRetained); err != nil {
		t.Fatal(err)
	}
	if !removed || !spendRetained {
		t.Fatalf("092 Down removed=%v retained_091=%v", removed, spendRetained)
	}
}

func TestMigration092SQLContainsReceiptFences(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/092_research_artifact_llm_receipts.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"planner_llm_spend_reservation_id",
		"synthesis_llm_spend_reservation_id",
		"DEFERRABLE INITIALLY DEFERRED",
		"reservation.stage='planner'",
		"reservation.subject_id=0",
		"reservation.stage='synthesis'",
		"reservation.subject_id=NEW.id",
		"settlement.outcome='completed'",
		"settlement.usage_known",
		"convert_from(NEW.plan_payload,'UTF8')::jsonb=call.completion::jsonb",
		"convert_from(NEW.brief_payload,'UTF8')::jsonb=call.completion::jsonb",
		"refusing downgrade while receipt-bound artifacts exist",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("092 migration omitted %q", required)
		}
	}
}
