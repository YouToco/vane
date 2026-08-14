package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration087EvidenceBoundaryAndEmptyDown(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 87); err != nil {
		t.Fatal(err)
	}

	var update, deletePrivilege, truncate, readerSelect bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT has_table_privilege('vane_app','research_run_evidence','UPDATE'),
		        has_table_privilege('vane_app','research_run_evidence','DELETE'),
		        has_table_privilege('vane_app','research_run_evidence','TRUNCATE'),
		        has_column_privilege('vane_intelligence_reader',
		                             'research_run_evidence','result_bytes','SELECT')`,
	).Scan(&update, &deletePrivilege, &truncate, &readerSelect); err != nil {
		t.Fatal(err)
	}
	if update || deletePrivilege || truncate || !readerSelect {
		t.Fatalf("evidence privileges update=%v delete=%v truncate=%v reader=%v",
			update, deletePrivilege, truncate, readerSelect)
	}
	rows, err := database.QueryContext(t.Context(),
		`SELECT p.proname,has_function_privilege('public',p.oid,'EXECUTE'),
		        p.proowner::regrole::text,
		        p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		   FROM pg_proc p
		  WHERE p.proname IN (
		      'enforce_research_run_evidence_v3',
		      'enforce_research_run_evidence_terminal_v3'
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
	if err := rows.Err(); err != nil || count != 2 {
		t.Fatalf("evidence functions count=%d err=%v", count, err)
	}

	if _, err := provider.DownTo(t.Context(), 86); err != nil {
		t.Fatalf("empty 087 Down: %v", err)
	}
	var tableExists bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.research_run_evidence') IS NOT NULL`,
	).Scan(&tableExists); err != nil {
		t.Fatal(err)
	}
	if tableExists {
		t.Fatal("087 Down retained evidence table")
	}
	var terminalFence string
	if err := database.QueryRowContext(t.Context(),
		`SELECT pg_get_functiondef('enforce_research_run_step_v3()'::regprocedure)`,
	).Scan(&terminalFence); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(terminalFence, "research_run_evidence") {
		t.Fatal("087 Down did not restore the 086 terminal fence")
	}

	if _, err := provider.UpTo(t.Context(), 87); err != nil {
		t.Fatal(err)
	}
	// Simulate an already durable completed fact without guessing historical
	// model-visible bytes. The downgrade must refuse rather than erase the 087
	// contract. session_replication_role is test-only corruption injection.
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var tenantID, userID, snapshotID, planID int64
	if err := tx.QueryRowContext(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(),
		`INSERT INTO users(feishu_open_id,name)
		 VALUES('m087-down-guard','m087') RETURNING id`,
	).Scan(&userID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	if err := tx.QueryRowContext(t.Context(),
		`INSERT INTO task_run_snapshots (
		     tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		     run_kind,execution_mode,adaptive_version,capability_catalog_digest,
		     tool_policy_digest,prompt_policy_digest,model_policy_digest,
		     quota_policy_digest,definition_digest,plan_digest,payload_digest,
		     reference_digest,reference_schema_version,payload,budget
		 ) VALUES ($1,$2,'task-m087','wf-m087','run-m087','scheduled',
		           'discover_at_run',0,$3,$3,$3,$3,$3,$3,'',$3,$3,
		           'vane.research-run-snapshot-ref/v3','{}','{}')
		 RETURNING id`, tenantID, userID, sha).Scan(&snapshotID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(t.Context(),
		`INSERT INTO research_run_plans (
		     tenant_id,user_id,task_id,run_snapshot_id,temporal_workflow_id,
		     temporal_run_id,definition_digest,capability_catalog_digest,
		     plan_digest,plan_payload,schema_version
		 ) VALUES ($1,$2,'task-m087',$3,'wf-m087','run-m087',$4,$4,$4,
		           '{}','vane.research-run-plan/v3') RETURNING id`,
		tenantID, userID, snapshotID, sha).Scan(&planID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	for _, phase := range []string{"started", "completed"} {
		var result any
		if phase == "completed" {
			result = sha
		}
		if _, err := tx.ExecContext(t.Context(),
			`INSERT INTO research_run_steps (
			     tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
			     step_ordinal,phase,invocation_id,tool_name,request_digest,
			     result_digest,cost_micro_usd,error_code,schema_version
			 ) VALUES ($1,$2,'task-m087',$3,'run-m087',$4,0,$5,
			           'inv-m087','web_search',$4,$6,0,NULL,
			           'vane.research-run-step/v3')`,
			tenantID, userID, planID, sha, phase, result); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 86); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade while exact research evidence exists") {
		t.Fatalf("087 Down with completed fact err=%v", err)
	}
}
