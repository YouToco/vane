package store

import (
	"database/sql"
	"strings"
	"testing"
)

func TestMigration055MinimumPrivilegesRLSAndDowngradeFence(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 55); err != nil {
		t.Fatalf("migrate to 055: %v", err)
	}
	var (
		rls, restrictive, appRead, candidateInsert bool
		idInsert, createdInsert, update, delete    bool
		truncate, sequenceUsage, sequenceSelect    bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  (SELECT relrowsecurity FROM pg_class
		    WHERE oid='agent_turn_context_snapshots'::regclass),
		  EXISTS (
		    SELECT 1 FROM pg_policies
		     WHERE tablename='agent_turn_context_snapshots'
		       AND policyname='tenant_isolation'
		       AND permissive='RESTRICTIVE'
		  ),
		  has_table_privilege(
		    'vane_app','agent_turn_context_snapshots','SELECT'),
		  has_column_privilege(
		    'vane_app','agent_turn_context_snapshots',
		    'candidate_snapshot','INSERT'),
		  has_column_privilege(
		    'vane_app','agent_turn_context_snapshots','id','INSERT'),
		  has_column_privilege(
		    'vane_app','agent_turn_context_snapshots','created_at','INSERT'),
		  has_table_privilege(
		    'vane_app','agent_turn_context_snapshots','UPDATE'),
		  has_table_privilege(
		    'vane_app','agent_turn_context_snapshots','DELETE'),
		  has_table_privilege(
		    'vane_app','agent_turn_context_snapshots','TRUNCATE'),
		  has_sequence_privilege(
		    'vane_app','agent_turn_context_snapshots_id_seq','USAGE'),
		  has_sequence_privilege(
		    'vane_app','agent_turn_context_snapshots_id_seq','SELECT')`,
	).Scan(
		&rls, &restrictive, &appRead, &candidateInsert,
		&idInsert, &createdInsert, &update, &delete,
		&truncate, &sequenceUsage, &sequenceSelect,
	); err != nil {
		t.Fatal(err)
	}
	if !rls || !restrictive || !appRead || !candidateInsert ||
		idInsert || createdInsert || update || delete || truncate ||
		!sequenceUsage || sequenceSelect {
		t.Fatalf(
			"055 privilege drift rls=%v/%v read=%v insert=%v/%v/%v "+
				"mutation=%v/%v/%v sequence=%v/%v",
			rls, restrictive, appRead, candidateInsert, idInsert,
			createdInsert, update, delete, truncate,
			sequenceUsage, sequenceSelect,
		)
	}

	seedMigration055Snapshot(t, db)
	if _, err := provider.DownTo(ctx, 54); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("055 downgrade accepted non-empty snapshots: %v", err)
	}
}

func seedMigration055Snapshot(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := t.Context()
	var tenantID, userID, sessionID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id,name)
		VALUES ('migration-055-user','migration 055')
		RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES ($1,$2,'owner')`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (tenant_id,user_id)
		VALUES ($1,$2) RETURNING id`, tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	candidate := `{
	  "schema_version":"vane.agent-turn-context-snapshot/v1",
	  "compiler_version":"vane.agent-context/v1"
	}`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turn_context_snapshots (
		  tenant_id,user_id,session_id,turn_id,model_step,
		  schema_version,compiler_version,candidate_digest,
		  candidate_snapshot,replayable,authority_generation,
		  ledger_head_sequence,ledger_head_event_id,
		  ledger_projection_digest,snapshot_digest
		) VALUES (
		  $1,$2,$3,'turn',1,
		  'vane.agent-turn-context-snapshot/v1','vane.agent-context/v1',
		  $4,$5::jsonb,true,1,1,1,$4,$4
		)`, tenantID, userID, sessionID, digest, candidate); err != nil {
		t.Fatal(err)
	}
}
