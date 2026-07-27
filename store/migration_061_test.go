package store

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

func TestMigration061RoleRLSAndLeastPrivilege(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	otherUser := testUser(t, f.base.st)
	if _, err := f.base.st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'member')`,
		f.identity.TenantID, otherUser); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			f.identity.TenantID, otherUser)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM users WHERE id=$1`, otherUser)
	})

	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID), fmt.Sprint(otherUser)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := tx.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_outcomes`).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("same-tenant other user saw %d run outcomes", visible)
	}
	if _, err := tx.Exec(t.Context(),
		`INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		 ) VALUES ($1,$2,$3,$4,'vane.run-outcome/v1')`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
		f.ref.SnapshotID+999999,
	); err == nil {
		t.Fatal("same-tenant other user wrote an outcome for the owner")
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	appTx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = appTx.Rollback(t.Context()) }()
	if _, err := appTx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprint(f.identity.TenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := appTx.Exec(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := appTx.Exec(t.Context(),
		`UPDATE push_batches SET brief_state='sealed' WHERE id=$1`,
		f.batchID); err == nil {
		t.Fatal("ambient vane_app sealed a canonical Brief batch")
	}
	if err := appTx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	var (
		noLogin, noInherit, noBypass, noSuper bool
		ownerCanSet, appCanSet                bool
		appOutcomeAccess, appBriefAccess      bool
		writerDelete, writerPayloadUpdate     bool
		writerSealUpdate, writerDeliveryRead  bool
		writerEvidenceExec, writerUsersRead   bool
		rolePath                              bool
	)
	if err := f.base.st.pool.QueryRow(t.Context(), `
		SELECT
		  NOT rolcanlogin,NOT rolinherit,NOT rolbypassrls,NOT rolsuper,
		  pg_has_role(current_user,oid,'SET'),
		  pg_has_role('vane_app',oid,'SET'),
		  has_table_privilege(
		      'vane_app','task_run_outcomes','SELECT,INSERT,UPDATE,DELETE'),
		  has_table_privilege(
		      'vane_app','brief_snapshots','SELECT,INSERT,UPDATE,DELETE'),
		  has_table_privilege(
		      'vane_brief_writer','brief_snapshots','DELETE'),
		  has_column_privilege(
		      'vane_brief_writer','brief_snapshots','payload','UPDATE'),
		  has_column_privilege(
		      'vane_brief_writer','push_batches','brief_state','UPDATE'),
			  has_column_privilege(
			      'vane_brief_writer','deliveries','id','SELECT'),
			  has_function_privilege(
			      'vane_brief_writer',
			      'read_canonical_brief_delivery_evidence_v1(bigint,bigint)',
			      'EXECUTE'),
			  has_table_privilege(
			      'vane_brief_writer','users','SELECT'),
			  cardinality(COALESCE(rolconfig,ARRAY[]::text[]))=1
			    AND rolconfig[1]='search_path=pg_catalog, public, pg_temp'
		  FROM pg_roles WHERE rolname='vane_brief_writer'`,
	).Scan(
		&noLogin, &noInherit, &noBypass, &noSuper,
		&ownerCanSet, &appCanSet, &appOutcomeAccess, &appBriefAccess,
		&writerDelete, &writerPayloadUpdate, &writerSealUpdate,
		&writerDeliveryRead, &writerEvidenceExec, &writerUsersRead,
		&rolePath,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !noSuper ||
		!ownerCanSet || appCanSet || appOutcomeAccess || appBriefAccess ||
		writerDelete || writerPayloadUpdate || !writerSealUpdate ||
		writerDeliveryRead || !writerEvidenceExec || writerUsersRead ||
		!rolePath {
		t.Fatalf(
			"unsafe brief role attrs=%t/%t/%t/%t set=%t/%t app=%t/%t "+
				"writer=%t/%t/%t/%t/%t/%t path=%t marker=%d",
			noLogin, noInherit, noBypass, noSuper, ownerCanSet, appCanSet,
			appOutcomeAccess, appBriefAccess, writerDelete,
			writerPayloadUpdate, writerSealUpdate, writerDeliveryRead,
			writerEvidenceExec, writerUsersRead, rolePath, marker.ID,
		)
	}
}

func TestMigration061RejectsDeliveryScopeDrift(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	sameTenantUser := testUser(t, f.base.st)
	if _, err := f.base.st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'member')`,
		f.identity.TenantID, sameTenantUser); err != nil {
		t.Fatal(err)
	}
	var otherTenant, otherTenantUser int64
	if err := f.base.st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&otherTenant); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.pool.QueryRow(t.Context(),
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ($1,'brief scope other') RETURNING id`,
		fmt.Sprintf("ou_brief_scope_%d", otherTenant),
	).Scan(&otherTenantUser); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'owner')`, otherTenant, otherTenantUser); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM memberships WHERE user_id IN ($1,$2)`,
			sameTenantUser, otherTenantUser)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM users WHERE id IN ($1,$2)`,
			sameTenantUser, otherTenantUser)
		cleanupExec(ctx, t, f.base.st,
			`DELETE FROM tenants WHERE id=$1`, otherTenant)
	})

	cases := []struct {
		name     string
		tenantID int64
		userID   int64
	}{
		{"same tenant other user", f.identity.TenantID, sameTenantUser},
		{"other tenant", otherTenant, otherTenantUser},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.base.st.pool.Exec(t.Context(),
				`INSERT INTO deliveries (
				    tenant_id,batch_id,user_id,body_md
				 ) VALUES ($1,$2,$3,'scope poison')`,
				tc.tenantID, f.batchID, tc.userID,
			); err == nil {
				t.Fatal("delivery scope drift was admitted")
			}
		})
	}
}

func TestMigration061RejectsPreexistingDeliveryScopeDrift(t *testing.T) {
	db, provider := migration035Scratch(t)
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}
	var ownerID, otherID, batchID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users (feishu_open_id,name)
		VALUES ('ou_brief_scope_owner','brief scope owner')
		RETURNING id`,
	).Scan(&ownerID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users (feishu_open_id,name)
		VALUES ('ou_brief_scope_other','brief scope other')
		RETURNING id`,
	).Scan(&otherID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES (1,$1,'owner'),(1,$2,'member')`, ownerID, otherID,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO push_batches (tenant_id,user_id)
		VALUES (1,$1) RETURNING id`, ownerID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO deliveries (tenant_id,batch_id,user_id,body_md)
		VALUES (1,$1,$2,'preexisting scope poison')`, batchID, otherID,
	); err != nil {
		t.Fatalf("v060 unexpectedly rejected fixture poison: %v", err)
	}
	if _, err := provider.UpTo(t.Context(), 61); err == nil ||
		!strings.Contains(
			err.Error(), "delivery scope differs from its push batch") {
		t.Fatalf("migration admitted preexisting scope poison: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 60 {
		t.Fatalf("failed migration left version %d, want 60", version)
	}
}

func TestMigration061FinalizedOutcomeCannotReturnPending(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	outcome := f.finalizedContentOutcome(t)
	tx, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		fmt.Sprint(f.identity.TenantID),
		fmt.Sprint(f.identity.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`SET LOCAL ROLE vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE task_run_outcomes
		    SET status='pending',result=NULL,source_coverage=NULL,
		        processing=NULL,failure_code='',failure_message='',
		        finalized_at=NULL,outcome_digest=NULL
		  WHERE id=$1`, outcome.ID,
	); err == nil || !strings.Contains(
		err.Error(), "run outcome transition authority denied") {
		t.Fatalf("finalized outcome returned to pending: %v", err)
	}
}

func TestMigration061ScrubsPreexistingWriterACL(t *testing.T) {
	db, provider := migration035Scratch(t)
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `
		DO $$
		BEGIN
		    IF NOT EXISTS (
		        SELECT 1 FROM pg_roles WHERE rolname='vane_brief_writer'
		    ) THEN
		        CREATE ROLE vane_brief_writer
		            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
		            NOLOGIN NOINHERIT NOBYPASSRLS;
		    END IF;
		END $$;
		GRANT SELECT ON users TO vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 61); err != nil {
		t.Fatalf("migrate to 061 with preexisting ACL: %v", err)
	}
	var canReadUsers, canExecuteEvidence bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_table_privilege('vane_brief_writer','users','SELECT'),
		  has_function_privilege(
		    'vane_brief_writer',
		    'read_canonical_brief_delivery_evidence_v1(bigint,bigint)',
		    'EXECUTE')`,
	).Scan(&canReadUsers, &canExecuteEvidence); err != nil {
		t.Fatal(err)
	}
	if canReadUsers || !canExecuteEvidence {
		t.Fatalf("writer ACL scrub/whitelist drift: users=%t evidence=%t",
			canReadUsers, canExecuteEvidence)
	}
}

func TestMigration061HasSafeDownFenceAndRoleRaceGuard(t *testing.T) {
	raw, err := fs.ReadFile(
		migrationsFS, "migrations/061_canonical_brief_dark_store.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, required := range []string{
		"SELECT pg_advisory_xact_lock(6215335020355474248)",
		"LOCK TABLE task_run_outcomes,deliveries,push_batches,brief_snapshots",
		"refusing downgrade while canonical outcome/brief evidence exists",
		"WHEN duplicate_object OR unique_violation THEN NULL",
		"delivery scope differs from its push batch",
		"fk_deliveries_brief_batch_scope",
		"deliveries_require_open_brief_batch_v1",
		"task_run_outcomes_one_way_finalization_v1",
		"read_canonical_brief_delivery_evidence_v1",
		"DROP OWNED BY vane_brief_writer",
		"push_batches_brief_state_authority_v1",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 061 missing %q", required)
		}
	}
}

func TestMigration061DownDoesNotDeadlockWithDeliveryInsert(t *testing.T) {
	db, provider := migration035Scratch(t)
	if _, err := provider.UpTo(t.Context(), 61); err != nil {
		t.Fatalf("migrate to 061: %v", err)
	}
	var userID, batchID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users (feishu_open_id,name)
		VALUES ('ou_brief_down_race','brief down race')
		RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES (1,$1,'owner')`, userID,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO push_batches (tenant_id,user_id)
		VALUES (1,$1) RETURNING id`, userID,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	writer, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	writerDone := false
	defer func() {
		if !writerDone {
			_ = writer.Rollback()
		}
	}()
	if _, err := writer.ExecContext(ctx, `
		SET LOCAL deadlock_timeout='100ms';
		SET LOCAL statement_timeout='5s';
		LOCK TABLE deliveries IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	downDone := make(chan error, 1)
	go func() {
		_, downErr := provider.Down(ctx)
		downDone <- downErr
	}()
	time.Sleep(200 * time.Millisecond)

	if _, err := writer.ExecContext(ctx, `
		INSERT INTO deliveries (tenant_id,batch_id,user_id,body_md)
		VALUES (1,$1,$2,'concurrent down delivery')`, batchID, userID,
	); err != nil {
		t.Fatalf("delivery insert deadlocked with 061 Down: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writerDone = true

	select {
	case err := <-downDone:
		if err != nil {
			t.Fatalf("061 Down did not converge after delivery commit: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("061 Down did not converge")
	}
	var version, deliveries int
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  (SELECT max(version_id)
		     FROM goose_db_version WHERE is_applied),
		  (SELECT count(*) FROM deliveries WHERE batch_id=$1)`,
		batchID,
	).Scan(&version, &deliveries); err != nil {
		t.Fatal(err)
	}
	if version != 60 || deliveries != 1 {
		t.Fatalf("Down result version/deliveries=%d/%d want 60/1",
			version, deliveries)
	}
}

func TestMigration061DownRefusesDurableOutcomeEvidence(t *testing.T) {
	freshURL := freshMigrationDatabase(t, "vane_brief_down")
	if err := Migrate(t.Context(), freshURL); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var userID int64
	if err := db.QueryRowContext(t.Context(),
		`INSERT INTO users (feishu_open_id,name)
		 VALUES ('ou_brief_down','brief down') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES (1,$1,'owner')`, userID); err != nil {
		t.Fatal(err)
	}
	var snapshotID int64
	if _, err := db.ExecContext(t.Context(),
		`SET session_replication_role='replica'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO task_run_snapshots (
		    tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		    run_kind,execution_mode,adaptive_version,
		    capability_catalog_digest,tool_policy_digest,
		    prompt_policy_digest,model_policy_digest,quota_policy_digest,
		    definition_digest,plan_digest,payload_digest,reference_digest,
		    reference_schema_version,payload,budget
		) VALUES (
		    1,$1,'brief-down-task','wf-brief-down-task','brief-down-run',
		    'scheduled','compiled',0,
		    repeat('a',64),repeat('b',64),repeat('c',64),repeat('d',64),
		    repeat('e',64),repeat('f',64),repeat('1',64),repeat('2',64),
		    repeat('3',64),'vane.run-snapshot-ref/v1',
		    convert_to('{}','UTF8'),'{}'::jsonb
		) RETURNING id`, userID,
	).Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`SET session_replication_role='origin'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		 ) VALUES (1,$1,'brief-down-task',$2,'vane.run-outcome/v1')`,
		userID, snapshotID); err != nil {
		t.Fatal(err)
	}
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(context.Background()); err == nil ||
		!strings.Contains(err.Error(),
			"refusing downgrade while canonical outcome/brief evidence exists") {
		t.Fatalf("061 Down accepted durable evidence: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT COALESCE(max(version_id),0)
		   FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 61 {
		t.Fatalf("failed 061 Down changed version to %d", version)
	}
}
