package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration088ResearchBriefBoundaryAndFailClosedDown(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 088 integration tests")
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
	if _, err := provider.UpTo(t.Context(), 88); err != nil {
		t.Fatal(err)
	}

	var updateIdentity, updateStatus, deletePrivilege, truncate bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT has_column_privilege('vane_app','research_brief_syntheses','task_id','UPDATE'),
		        has_column_privilege('vane_app','research_brief_syntheses','status','UPDATE'),
		        has_table_privilege('vane_app','research_brief_syntheses','DELETE'),
		        has_table_privilege('vane_app','research_brief_syntheses','TRUNCATE')`,
	).Scan(&updateIdentity, &updateStatus, &deletePrivilege, &truncate); err != nil {
		t.Fatal(err)
	}
	if updateIdentity || !updateStatus || deletePrivilege || truncate {
		t.Fatalf("unsafe synthesis privileges identity=%v status=%v delete=%v truncate=%v",
			updateIdentity, updateStatus, deletePrivilege, truncate)
	}
	rows, err := database.QueryContext(t.Context(),
		`SELECT p.proname,has_function_privilege('public',p.oid,'EXECUTE'),
		        p.proowner::regrole::text,
		        p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		   FROM pg_proc p
		  WHERE p.proname IN (
		      'enforce_research_brief_synthesis_admission_v3',
		      'enforce_research_brief_synthesis_transition_v3',
		      'read_research_history_v3',
		      'read_research_history_content_v3'
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
			t.Fatalf("unsafe function %s public=%v owner=%q config=%v",
				name, publicExecute, owner, safeConfig)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 4 {
		t.Fatalf("synthesis functions count=%d err=%v", count, err)
	}
	var rlsEnabled bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT relrowsecurity FROM pg_class WHERE oid='research_brief_syntheses'::regclass`,
	).Scan(&rlsEnabled); err != nil || !rlsEnabled {
		t.Fatalf("RLS enabled=%v err=%v", rlsEnabled, err)
	}

	if _, err := provider.DownTo(t.Context(), 87); err != nil {
		t.Fatalf("empty 088 Down: %v", err)
	}
	var exists bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.research_brief_syntheses') IS NOT NULL`,
	).Scan(&exists); err != nil || exists {
		t.Fatalf("empty Down table exists=%v err=%v", exists, err)
	}
	if _, err := provider.UpTo(t.Context(), 88); err != nil {
		t.Fatal(err)
	}

	// Test-only corruption injection creates a durable row without inventing a
	// fake V3 parent graph. Downgrade must refuse instead of deleting evidence.
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	contextPayload := []byte(`{"manual":"test"}`)
	evidencePayload := []byte(`{"schema_version":"vane.research-evidence-manifest/v3","items":[]}`)
	historyPayload := []byte(`{"schema_version":"vane.research-history-manifest/v3","history_through_utc":"2026-08-01T00:00:00Z","items":[]}`)
	_, err = tx.ExecContext(t.Context(),
		`INSERT INTO research_brief_syntheses (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		     notification_threshold,request_digest,context_payload,context_digest,
		     evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
		 ) VALUES (987654321,987654322,'task-down',987654323,987654324,
		           'workflow-down','run-down',$1,$1,'major_updates_only',$1,
		           $2,encode(sha256($2),'hex'),$3,encode(sha256($3),'hex'),
		           $4,encode(sha256($4),'hex'),'vane.research-brief-synthesis/v3')`,
		digest, contextPayload, evidencePayload, historyPayload)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 87); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade while research Brief synthesis evidence exists") {
		t.Fatalf("088 Down with durable synthesis err=%v", err)
	}
}
