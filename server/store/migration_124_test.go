package store

import (
	"database/sql"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/pressly/goose/v3"
)

func TestMigration124ScopesV35ProjectionAndPreservesV34Admission(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("migrations", "124_research_scope_window.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(payload)
	for _, required := range []string{
		"'research-synthesis.render/v3.5'",
		"'vane.research-synthesis-context/v3.3'",
		"#>> '{definition,research_scope,mode}' IS DISTINCT FROM 'event_window'",
		"lookback_seconds}')::bigint IS DISTINCT FROM 604800",
		"'(start,end]'",
		"regexp_match(value",
		"evidence.truncated",
		"convert_from(evidence.result_bytes,'UTF8')::jsonb",
		"research_scope_timestamp_ns_v124",
		"published_ns>window_start_ns AND published_ns<=window_end_ns",
		"project_research_evidence_context_v118",
		"context_json->'current_evidence' IS DISTINCT FROM expected_evidence_context",
		"enforce_research_brief_synthesis_admission_v33",
		"NULLIF(current_setting('app.tenant_id',true),'')::bigint",
		"118: research Brief parent scope differs",
		"118: research Brief history is not exact same-owner history",
		"118: research Brief payload is not grounded in frozen Evidence",
		"actual_ids IS DISTINCT FROM expected_ids",
		"expected_ids='[]'::jsonb",
		"DROP TRIGGER research_scope_window_v33",
		"scoped definition, v3.5 snapshot, or v3.3 synthesis history exists",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 124 lost guard %q", required)
		}
	}
	if strings.Count(sql, "'research-synthesis.render/v3.3'") < 2 ||
		strings.Count(sql, "'research-synthesis.render/v3.4'") < 2 {
		t.Fatal("migration 124 did not retain v3.3/v3.4 admission on Up and Down")
	}
}

func TestMigration124SQLProjectionBoundaryAndDateParityPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), databaseURL); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	end := time.Date(2026, 8, 9, 8, 45, 50, 329463000, time.UTC)
	start := end.Add(-7 * 24 * time.Hour)
	documents := []researchWindowDocumentV33{
		{Title: "start", URL: "https://e/start", PublishedAt: start.Format(time.RFC3339Nano), Text: "excluded"},
		{Title: "start+1ns", URL: "https://e/after", PublishedAt: start.Add(time.Nanosecond).Format(time.RFC3339Nano), Text: "included"},
		{Title: "offset", URL: "https://e/offset", PublishedAt: end.Add(-time.Hour).In(time.FixedZone("plus8", 8*3600)).Format(time.RFC3339Nano), Text: "included"},
		{Title: "end", URL: "https://e/end", PublishedAt: end.Format(time.RFC3339Nano), Text: "included"},
		{Title: "end+1ns", URL: "https://e/future", PublishedAt: end.Add(time.Nanosecond).Format(time.RFC3339Nano), Text: "excluded"},
		{Title: "missing", URL: "https://e/missing", Text: "excluded"},
		{Title: "invalid", URL: "https://e/invalid", PublishedAt: "2026-08-09 08:00:00", Text: "excluded"},
	}
	full, err := json.Marshal(documents)
	if err != nil {
		t.Fatal(err)
	}
	var filtered string
	if err := database.QueryRowContext(t.Context(),
		`SELECT filter_research_scope_evidence_v124(
			$1::bytea,research_scope_timestamp_ns_v124($2),research_scope_timestamp_ns_v124($3))`,
		full, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano),
	).Scan(&filtered); err != nil {
		t.Fatal(err)
	}
	var got []researchWindowDocumentV33
	if err := json.Unmarshal([]byte(filtered), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Title != "start+1ns" || got[1].Title != "offset" || got[2].Title != "end" {
		t.Fatalf("SQL filtered=%s", filtered)
	}
}

func TestMigration124EmptyDownPostgres(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 124); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 123); err != nil {
		t.Fatal(err)
	}
	var v33Admission, scopeFunction, nsHelper, jsonHelper, filterHelper bool
	var v33Trigger, scopeTrigger bool
	var restoredReservation string
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regprocedure('enforce_research_brief_synthesis_admission_v33()') IS NOT NULL,
		to_regprocedure('enforce_research_scope_window_v33()') IS NOT NULL,
		to_regprocedure('research_scope_timestamp_ns_v124(text)') IS NOT NULL,
		to_regprocedure('research_scope_json_string_v124(text)') IS NOT NULL,
		to_regprocedure('filter_research_scope_evidence_v124(bytea,numeric,numeric)') IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='research_brief_synthesis_admission_v33'),
		EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='research_scope_window_v33'),
		pg_get_functiondef('enforce_research_run_llm_spend_reservation_v2()'::regprocedure)`,
	).Scan(&v33Admission, &scopeFunction, &nsHelper, &jsonHelper, &filterHelper,
		&v33Trigger, &scopeTrigger, &restoredReservation); err != nil {
		t.Fatal(err)
	}
	if v33Admission || scopeFunction || nsHelper || jsonHelper || filterHelper ||
		v33Trigger || scopeTrigger {
		t.Fatalf("124 objects survived Down: %v %v %v %v %v %v %v",
			v33Admission, scopeFunction, nsHelper, jsonHelper, filterHelper, v33Trigger, scopeTrigger)
	}
	if !strings.Contains(restoredReservation, "research-synthesis.render/v3.3") ||
		!strings.Contains(restoredReservation, "research-synthesis.render/v3.4") ||
		strings.Contains(restoredReservation, "research-synthesis.render/v3.5") {
		t.Fatal("Down did not restore exact v123 verifier admission")
	}
}

func TestMigration125RetainedV36RefusesDownPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	t.Cleanup(func() {
		restoreCurrentRetainedFixtureSchema(t, databaseURL)
	})
	now := time.Now().UTC()
	fixture := scopedResearchBriefFixtureV35(t, []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{Title: "inside", URL: "https://e/inside",
			PublishedAt: now.Add(-time.Hour).Format(time.RFC3339Nano), Text: "eligible"},
	)))
	if fixture.snapshotRef.SnapshotID == 0 {
		t.Fatal("scoped snapshot was not created")
	}
	database, err := sql.Open("pgx", databaseURL)
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
	if _, err := provider.DownTo(t.Context(), 124); err == nil ||
		!strings.Contains(err.Error(), "v3.6 correction history exists") {
		t.Fatalf("retained v3.6 downgrade err=%v", err)
	}
}

func TestMigration124PreparedScopedDefinitionRefusesDownPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	t.Cleanup(func() {
		restoreCurrentRetainedFixtureSchema(t, databaseURL)
	})
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	policy := researchV3PreparePolicyForTest()
	policy.TenantID, policy.UserID, policy.TaskID = tenantID, userID, taskID
	policy.IdempotencyKey = "migration-124-scope-down"
	policy.ResearchScope = &taskstate.ResearchScopeV3{
		Mode:            taskstate.ResearchScopeEventWindowV3,
		LookbackSeconds: taskstate.ResearchScopeWeekSecondsV3,
	}
	if _, err := st.PrepareResearchV3Definition(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("pgx", databaseURL)
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
	if _, err := provider.DownTo(t.Context(), 123); err == nil ||
		!strings.Contains(err.Error(), "scoped definition, v3.5 snapshot, or v3.3 synthesis history exists") {
		t.Fatalf("prepared scoped definition downgrade err=%v", err)
	}
}
