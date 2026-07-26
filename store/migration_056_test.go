package store

import (
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
	"github.com/pressly/goose/v3"
)

func TestMigration056RLSAndMinimumRuntimePrivileges(t *testing.T) {
	f := newAgentSessionFactFixture(t)
	ctx := t.Context()
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	_ = feedbackID

	var rls, forceRLS bool
	var appCanSelect, appCanDelete, appCanTruncate, appCanInsert bool
	var appCanUpdateStatus, projectorCanUpdateStatus bool
	var projectorCanUpdatePayload, appSequenceUsage, appSequenceSelect bool
	var projectorLogin, projectorInherit, projectorBypassRLS bool
	if err := f.store.pool.QueryRow(ctx,
		`SELECT c.relrowsecurity,c.relforcerowsecurity,
		        has_table_privilege(
		            'vane_app','agent_session_fact_outbox','SELECT'),
		        has_table_privilege(
		            'vane_app','agent_session_fact_outbox','DELETE'),
		        has_table_privilege(
		            'vane_app','agent_session_fact_outbox','TRUNCATE'),
		        has_column_privilege(
		            'vane_app','agent_session_fact_outbox',
		            'fact_id','INSERT'),
		        has_column_privilege(
		            'vane_app','agent_session_fact_outbox',
		            'status','UPDATE'),
		        has_column_privilege(
		            'vane_agent_session_fact_projector',
		            'agent_session_fact_outbox','status','UPDATE'),
		        has_column_privilege(
		            'vane_agent_session_fact_projector',
		            'agent_session_fact_outbox',
		            'session_messages','UPDATE'),
		        has_sequence_privilege(
		            'vane_app','agent_session_fact_outbox_id_seq','USAGE'),
		        has_sequence_privilege(
		            'vane_app','agent_session_fact_outbox_id_seq','SELECT'),
		        r.rolcanlogin,r.rolinherit,r.rolbypassrls
		   FROM pg_class c
		   CROSS JOIN pg_roles r
		  WHERE c.oid='agent_session_fact_outbox'::regclass
		    AND r.rolname='vane_agent_session_fact_projector'`,
	).Scan(
		&rls, &forceRLS, &appCanSelect, &appCanDelete,
		&appCanTruncate, &appCanInsert, &appCanUpdateStatus,
		&projectorCanUpdateStatus, &projectorCanUpdatePayload,
		&appSequenceUsage, &appSequenceSelect, &projectorLogin,
		&projectorInherit, &projectorBypassRLS,
	); err != nil {
		t.Fatal(err)
	}
	if !rls || forceRLS || appCanSelect || appCanDelete ||
		appCanTruncate || !appCanInsert || appCanUpdateStatus ||
		!projectorCanUpdateStatus || projectorCanUpdatePayload ||
		!appSequenceUsage || appSequenceSelect || projectorLogin ||
		projectorInherit || projectorBypassRLS {
		t.Fatalf(
			"rls=%v force=%v app_select=%v delete=%v truncate=%v "+
				"insert=%v app_update=%v projector_update=%v "+
				"projector_payload=%v seq_usage=%v seq_select=%v "+
				"login=%v inherit=%v bypassrls=%v",
			rls, forceRLS, appCanSelect, appCanDelete,
			appCanTruncate, appCanInsert, appCanUpdateStatus,
			projectorCanUpdateStatus, projectorCanUpdatePayload,
			appSequenceUsage, appSequenceSelect, projectorLogin,
			projectorInherit, projectorBypassRLS,
		)
	}

	tx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", f.tenantB),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_agent_session_fact_projector`,
	); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM agent_session_fact_outbox
		  WHERE tenant_id=$1`,
		f.tenantA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("cross-tenant visible rows=%d", count)
	}
}

func TestMigration056DownRefusesDurableFacts(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 056 Down fence 真库测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()

	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatal(err)
	}
	var tenantID, userID, sessionID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ('ou_m056_down','m056') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		tenantID, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agent_session_fact_outbox (
		     tenant_id,user_id,fact_type,fact_id,source_identity,
		     session_id,session_messages,payload_digest,status
		 )
		 VALUES ($1,$2,'feedback',1,'feedback-click:1',$3,
		         '[{"role":"user","content":"x"}]'::bytea,
		         repeat('a',64),'pending')`,
		tenantID, userID, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	_, err = provider.DownTo(ctx, 55)
	if err == nil || !strings.Contains(
		err.Error(),
		"refusing downgrade while Agent continuation facts exist",
	) {
		t.Fatalf("DownTo(55) err=%v", err)
	}
}
