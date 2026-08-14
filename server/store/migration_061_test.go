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
		noLogin, noInherit, noBypass, noSuper  bool
		ownerCanSet, appCanSet                 bool
		appOutcomeAccess, appBriefAccess       bool
		writerDelete, writerPayloadUpdate      bool
		writerSealUpdate, writerDeliveryRead   bool
		writerEvidenceExec, writerUsersRead    bool
		writerSnapshotRead, writerIdentityExec bool
		rolePath                               bool
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
			  has_table_privilege(
			      'vane_brief_writer','task_run_snapshots','SELECT'),
			  has_function_privilege(
			      'vane_brief_writer',
			      'read_canonical_brief_run_identity_v1(bigint)',
			      'EXECUTE'),
			  cardinality(COALESCE(rolconfig,ARRAY[]::text[]))=1
			    AND rolconfig[1]='search_path=pg_catalog, public, pg_temp'
		  FROM pg_roles WHERE rolname='vane_brief_writer'`,
	).Scan(
		&noLogin, &noInherit, &noBypass, &noSuper,
		&ownerCanSet, &appCanSet, &appOutcomeAccess, &appBriefAccess,
		&writerDelete, &writerPayloadUpdate, &writerSealUpdate,
		&writerDeliveryRead, &writerEvidenceExec, &writerUsersRead,
		&writerSnapshotRead, &writerIdentityExec, &rolePath,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !noSuper ||
		!ownerCanSet || appCanSet || appOutcomeAccess || appBriefAccess ||
		writerDelete || writerPayloadUpdate || !writerSealUpdate ||
		writerDeliveryRead || !writerEvidenceExec || writerUsersRead ||
		writerSnapshotRead || !writerIdentityExec || !rolePath {
		t.Fatalf(
			"unsafe brief role attrs=%t/%t/%t/%t set=%t/%t app=%t/%t "+
				"writer=%t/%t/%t/%t/%t/%t/%t/%t path=%t marker=%d",
			noLogin, noInherit, noBypass, noSuper, ownerCanSet, appCanSet,
			appOutcomeAccess, appBriefAccess, writerDelete,
			writerPayloadUpdate, writerSealUpdate, writerDeliveryRead,
			writerEvidenceExec, writerUsersRead, writerSnapshotRead,
			writerIdentityExec, rolePath, marker.ID,
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

func TestMigration061RejectsRunOutcomeSnapshotScopeDrift(t *testing.T) {
	f := newCanonicalBriefFixture(t, 0)
	otherUser := testUser(t, f.base.st)
	if _, err := f.base.st.pool.Exec(t.Context(), `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES ($1,$2,'member')`,
		f.identity.TenantID, otherUser,
	); err != nil {
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
	if _, err := f.base.st.pool.Exec(t.Context(), `
		INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		) VALUES ($1,$2,$3,$4,'vane.run-outcome/v1')`,
		f.identity.TenantID, otherUser, f.identity.TaskID, f.ref.SnapshotID,
	); err == nil || !strings.Contains(
		err.Error(), "run outcome snapshot scope differs") {
		t.Fatalf("cross-user run outcome scope was admitted: %v", err)
	}
}

func TestMigration061RejectsPreexistingWriterACL(t *testing.T) {
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
	if _, err := provider.UpTo(t.Context(), 61); err == nil ||
		!strings.Contains(
			err.Error(), "preexisting ACL in this database") {
		t.Fatalf("migration admitted preexisting writer ACL: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 60 {
		t.Fatalf("failed ACL migration left version %d, want 60", version)
	}
}

func TestMigration061RejectsPreexistingWriterDatabaseACL(t *testing.T) {
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
		END $$`); err != nil {
		t.Fatal(err)
	}
	var databaseName string
	if err := db.QueryRowContext(t.Context(),
		`SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf(
		`GRANT CREATE ON DATABASE %q TO vane_brief_writer`, databaseName,
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 61); err == nil ||
		!strings.Contains(
			err.Error(), "preexisting ACL in this database") {
		t.Fatalf("migration admitted writer database ACL: %v", err)
	}
}

func TestMigration061RejectsWriterParameterACL(t *testing.T) {
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
		GRANT SET ON PARAMETER session_replication_role
		    TO vane_brief_writer`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if _, err := db.ExecContext(ctx, `
			REVOKE SET ON PARAMETER session_replication_role
			    FROM vane_brief_writer`); err != nil {
			t.Errorf("cleanup writer parameter ACL: %v", err)
		}
	})
	if _, err := provider.UpTo(t.Context(), 61); err == nil ||
		!strings.Contains(
			err.Error(), "unsafe cluster parameter ACL") {
		t.Fatalf("migration admitted writer parameter ACL: %v", err)
	}
	var canDisableTriggers bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_parameter_privilege(
		    'vane_brief_writer','session_replication_role','SET')`,
	).Scan(&canDisableTriggers); err != nil {
		t.Fatal(err)
	}
	if !canDisableTriggers {
		t.Fatal("test fixture did not retain the parameter ACL after fail-closed")
	}
}

func TestMigration061RejectsPublicParameterACL(t *testing.T) {
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
		GRANT SET ON PARAMETER session_replication_role TO PUBLIC`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		if _, err := db.ExecContext(ctx, `
			REVOKE SET ON PARAMETER session_replication_role
			    FROM PUBLIC`); err != nil {
			t.Errorf("cleanup PUBLIC parameter ACL: %v", err)
		}
	})
	if _, err := provider.UpTo(t.Context(), 61); err == nil ||
		!strings.Contains(
			err.Error(), "unsafe cluster parameter ACL") {
		t.Fatalf("migration admitted PUBLIC parameter ACL: %v", err)
	}
	var inherited bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_parameter_privilege(
		    'vane_brief_writer','session_replication_role','SET')`,
	).Scan(&inherited); err != nil {
		t.Fatal(err)
	}
	if !inherited {
		t.Fatal("test fixture did not retain PUBLIC parameter ACL")
	}
}

func TestMigration061PreservesWriterACLInOtherDatabase(t *testing.T) {
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
		END $$`); err != nil {
		t.Fatal(err)
	}
	otherURL := freshMigrationDatabase(t, "vane_brief_acl_other")
	otherDB, err := sql.Open("pgx", otherURL)
	if err != nil {
		t.Fatal(err)
	}
	defer otherDB.Close()
	var otherName string
	if err := otherDB.QueryRowContext(t.Context(),
		`SELECT current_database()`).Scan(&otherName); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf(`
		REVOKE CONNECT ON DATABASE %q FROM PUBLIC;
		GRANT CONNECT ON DATABASE %q TO vane_brief_writer`,
		otherName, otherName,
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 61); err != nil {
		t.Fatalf("migrate to 061 with other-database ACL: %v", err)
	}
	var retained bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT has_database_privilege(
		    'vane_brief_writer',$1,'CONNECT')`, otherName,
	).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if !retained {
		t.Fatal("migration revoked writer ACL in another database")
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
		"LOCK TABLE deliveries,push_batches",
		"LOCK TABLE task_run_outcomes,deliveries,push_batches,brief_snapshots",
		"refusing downgrade while canonical outcome/brief evidence exists",
		"WHEN duplicate_object OR unique_violation THEN NULL",
		"delivery scope differs from its push batch",
		"fk_deliveries_brief_batch_scope",
		"deliveries_require_open_brief_batch_v1",
		"task_run_outcomes_snapshot_scope_v1",
		"task_run_outcomes_one_way_finalization_v1",
		"read_canonical_brief_run_identity_v1",
		"read_canonical_brief_delivery_evidence_v1",
		"brief writer has preexisting ACL in this database",
		"brief writer has unsafe cluster parameter ACL",
		"pg_parameter_acl",
		"push_batches_brief_state_authority_v1",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 061 missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"ALTER TABLE task_run_snapshots",
		"POLICY brief_writer_identity ON task_run_snapshots",
		"FOREIGN KEY (run_snapshot_id) REFERENCES task_run_snapshots",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("migration 061 must not lock task_run_snapshots via %q",
				forbidden)
		}
	}
}

func TestMigration061UpDoesNotDeadlockWithDeliveryInsert(t *testing.T) {
	db, provider := migration035Scratch(t)
	if _, err := provider.UpTo(t.Context(), 60); err != nil {
		t.Fatalf("migrate to 060: %v", err)
	}
	var userID, batchID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users (feishu_open_id,name)
		VALUES ('ou_brief_up_race','brief up race')
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

	upDone := make(chan error, 1)
	go func() {
		_, upErr := provider.UpTo(ctx, 61)
		upDone <- upErr
	}()
	time.Sleep(200 * time.Millisecond)

	if _, err := writer.ExecContext(ctx, `
		INSERT INTO deliveries (tenant_id,batch_id,user_id,body_md)
		VALUES (1,$1,$2,'concurrent up delivery')`, batchID, userID,
	); err != nil {
		t.Fatalf("delivery insert deadlocked with 061 Up: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writerDone = true

	select {
	case err := <-upDone:
		if err != nil {
			t.Fatalf("061 Up did not converge after delivery commit: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("061 Up did not converge")
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
	if version != 61 || deliveries != 1 {
		t.Fatalf("Up result version/deliveries=%d/%d want 61/1",
			version, deliveries)
	}
}

func TestMigration061UpDoesNotDeadlockTaskSnapshotFence(t *testing.T) {
	testMigration061TaskSnapshotFence(t, true)
}

func TestMigration061DownDoesNotDeadlockTaskSnapshotFence(t *testing.T) {
	testMigration061TaskSnapshotFence(t, false)
}

func testMigration061TaskSnapshotFence(t *testing.T, up bool) {
	t.Helper()
	db, provider := migration035Scratch(t)
	target := int64(60)
	if !up {
		target = 61
	}
	if _, err := provider.UpTo(t.Context(), target); err != nil {
		t.Fatalf("migrate to %03d: %v", target, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	snapshotTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDone := false
	defer func() {
		if !snapshotDone {
			_ = snapshotTx.Rollback()
		}
	}()
	var snapshots int
	if err := snapshotTx.QueryRowContext(ctx,
		`SELECT count(*) FROM task_run_snapshots`,
	).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}

	blockerTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	blockerDone := false
	defer func() {
		if !blockerDone {
			_ = blockerTx.Rollback()
		}
	}()
	if _, err := blockerTx.ExecContext(ctx,
		`LOCK TABLE deliveries IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	migrationDone := make(chan error, 1)
	go func() {
		if up {
			_, err := provider.UpTo(ctx, 61)
			migrationDone <- err
			return
		}
		_, err := provider.Down(ctx)
		migrationDone <- err
	}()
	// Do not guess that the migration acquired its exclusive schema fence from
	// elapsed wall time. On a loaded race runner the goroutine may not have
	// started within 200ms; letting the snapshot writer win the shared fence
	// then creates a deadlock in the test itself. Observe the actual PostgreSQL
	// lock before introducing the competing writer.
	fenceDeadline := time.Now().Add(5 * time.Second)
	for {
		var migrationHoldsFence bool
		if err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
			  SELECT 1
			    FROM pg_locks
			   WHERE locktype='advisory'
			     AND mode='ExclusiveLock'
			     AND granted
			     AND database=(
			       SELECT oid FROM pg_database
			        WHERE datname=current_database()
			     )
			)`).Scan(&migrationHoldsFence); err != nil {
			t.Fatal(err)
		}
		if migrationHoldsFence {
			break
		}
		if time.Now().After(fenceDeadline) {
			t.Fatal("061 migration did not acquire its exclusive schema fence")
		}
		time.Sleep(10 * time.Millisecond)
	}

	fenceDone := make(chan error, 1)
	go func() {
		_, err := snapshotTx.ExecContext(ctx,
			`SELECT pg_advisory_xact_lock_shared(6215335020355474248)`)
		fenceDone <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := blockerTx.Commit(); err != nil {
		t.Fatal(err)
	}
	blockerDone = true

	select {
	case err := <-migrationDone:
		if err != nil {
			t.Fatalf("061 migration deadlocked with snapshot-first writer: %v",
				err)
		}
	case <-ctx.Done():
		t.Fatal("061 migration did not converge with snapshot-first writer")
	}
	select {
	case err := <-fenceDone:
		if err != nil {
			t.Fatalf("snapshot-first writer did not enter shared fence: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("snapshot-first writer did not converge")
	}
	if err := snapshotTx.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshotDone = true
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

func TestMigration063BlocksDowngradeWithPendingOutcome(t *testing.T) {
	freshURL := freshMigrationDatabase(t, "vane_brief_down")
	db, err := sql.Open("pgx", freshURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bootstrapProvider := migration062Provider(t, db)
	if _, err := bootstrapProvider.UpTo(t.Context(), 61); err != nil {
		t.Fatal(err)
	}
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
	if _, err := db.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id','1',false),
		       set_config('app.user_id',$1,false)`,
		fmt.Sprint(userID),
	); err != nil {
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
	if _, err := provider.UpTo(t.Context(), 63); err != nil {
		t.Fatalf("migrate fresh downgrade fixture to 063: %v", err)
	}
	var beforeDown int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT COALESCE(max(version_id),0)
		   FROM goose_db_version WHERE is_applied`).Scan(&beforeDown); err != nil {
		t.Fatal(err)
	}
	if beforeDown != 63 {
		t.Fatalf("migration version before 063 Down = %d", beforeDown)
	}
	if _, err := provider.DownTo(context.Background(), 62); err == nil ||
		!strings.Contains(err.Error(),
			"refusing downgrade while pending run outcomes exist") {
		t.Fatalf("063 Down accepted pending outcome evidence: %v", err)
	}
	var version int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT COALESCE(max(version_id),0)
		   FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 63 {
		t.Fatalf("failed 063 Down changed version to %d", version)
	}
}
