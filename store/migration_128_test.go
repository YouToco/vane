package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

func TestMigration128RetiresOnlySessionFactContinuation(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/128_agent_first_runtime_physical_freeze.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"LOCK TABLE agent_session_fact_outbox IN ACCESS EXCLUSIVE MODE",
		"status='pending' OR lease_owner IS NOT NULL",
		"CREATE TRIGGER agent_session_fact_outbox_retired_v128",
		"CREATE FUNCTION provision_vane_server_runtime_v128()",
		"CREATE FUNCTION deprovision_vane_server_runtime_v128()",
		"REVOKE vane_agent_session_fact_projector FROM vane_server_runtime",
		"REVOKE USAGE ON SEQUENCE agent_session_fact_outbox_id_seq FROM vane_app",
		"FROM pg_catalog.pg_shdepend dependency",
		"direct_grants<>0",
		"GRANT USAGE ON SEQUENCE agent_session_fact_outbox_id_seq TO vane_app",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 128 lost retirement boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"task_creation_operations_retired_v1_v128",
		"task_definition_edit_operations_retired_v1_v128",
		"live V1 task creation",
		"live protocol1 definition edit",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 128 improperly freezes Temporal-coupled lane %q", forbidden)
		}
	}
	if got := strings.Count(sqlText, "-- +goose StatementBegin"); got != 7 {
		t.Fatalf("migration 128 statement begin markers=%d, want 7", got)
	}
	if got := strings.Count(sqlText, "-- +goose StatementEnd"); got != 7 {
		t.Fatalf("migration 128 statement end markers=%d, want 7", got)
	}
}

func TestMigration128RejectsLiveOutboxAndFreezesTerminalHistoryPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 127); err != nil {
		t.Fatal(err)
	}
	userID, tenantID := migration128Identity(t, database)
	var sessionID int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO agent_sessions(tenant_id,user_id) VALUES($1,$2) RETURNING id`,
		tenantID, userID).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO agent_session_fact_outbox(
		 tenant_id,user_id,fact_type,fact_id,source_identity,session_id,
		 session_messages,payload_digest,status)
		VALUES($1,$2,'feedback',1,'feedback-click:1',$3,
		       '[{"role":"user","content":"x"}]'::bytea,repeat('a',64),'pending')`,
		tenantID, userID, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 128); err == nil ||
		!strings.Contains(err.Error(), "live Agent session fact") {
		t.Fatalf("migration 128 accepted live outbox: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE agent_session_fact_outbox
		   SET status='completed',session_recorded_at=clock_timestamp(),
		       updated_at=clock_timestamp() WHERE fact_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE agent_session_fact_outbox SET updated_at=clock_timestamp()
		 WHERE fact_id=1`); err == nil || !postgresCodeIs(err, "23514") {
		t.Fatalf("terminal outbox history remained mutable: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		DELETE FROM agent_session_fact_outbox WHERE fact_id=1`); err != nil {
		t.Fatalf("tenant purge/delete path was frozen: %v", err)
	}
	// Temporal-coupled retained lanes remain writable until a later,
	// deployment-attested migration can safely freeze them.
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations
		 (id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
		VALUES('migration-128-v1',$1,$2,'create_schedule','{}','retained','pending',$3,1)`,
		tenantID, userID, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("retained V1 creation was prematurely frozen: %v", err)
	}
	var appInsert, appSequence, projectorMember bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		has_column_privilege('vane_app','agent_session_fact_outbox','tenant_id','INSERT'),
		has_sequence_privilege('vane_app','agent_session_fact_outbox_id_seq','USAGE'),
		EXISTS(SELECT 1 FROM pg_auth_members edge
		 JOIN pg_roles granted ON granted.oid=edge.roleid
		 JOIN pg_roles member ON member.oid=edge.member
		 WHERE granted.rolname='vane_agent_session_fact_projector'
		   AND member.rolname=current_user)`,
	).Scan(&appInsert, &appSequence, &projectorMember); err != nil {
		t.Fatal(err)
	}
	if appInsert || appSequence || !projectorMember {
		t.Fatalf("retired authority remains app=(%v,%v) owner_member=%v",
			appInsert, appSequence, projectorMember)
	}
}

func TestMigration128EmptyDownRestores127AuthorityPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 127); err != nil {
		t.Fatal(err)
	}
	var clean, appInsert, appSequence, projectorSelect, projectorUpdate, ownerMember bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regprocedure('public.reject_retired_session_fact_v128()') IS NULL
		AND to_regprocedure('public.provision_vane_server_runtime_v128()') IS NULL
		AND to_regprocedure('public.deprovision_vane_server_runtime_v128()') IS NULL,
		has_column_privilege('vane_app','agent_session_fact_outbox','tenant_id','INSERT'),
		has_sequence_privilege('vane_app','agent_session_fact_outbox_id_seq','USAGE'),
		has_table_privilege('vane_agent_session_fact_projector','agent_session_fact_outbox','SELECT'),
		has_column_privilege('vane_agent_session_fact_projector','agent_session_fact_outbox','status','UPDATE'),
		EXISTS(SELECT 1 FROM pg_auth_members edge
		 JOIN pg_roles granted ON granted.oid=edge.roleid
		 JOIN pg_roles member ON member.oid=edge.member
		 WHERE granted.rolname='vane_agent_session_fact_projector'
		   AND member.rolname=current_user)`,
	).Scan(&clean, &appInsert, &appSequence, &projectorSelect, &projectorUpdate,
		&ownerMember); err != nil {
		t.Fatal(err)
	}
	if !clean || !appInsert || !appSequence || !projectorSelect || !projectorUpdate ||
		!ownerMember {
		t.Fatalf("Down did not restore 127 clean=%v app=(%v,%v) projector=(%v,%v) member=%v",
			clean, appInsert, appSequence, projectorSelect, projectorUpdate, ownerMember)
	}
}

func TestMigration128DownRejectsRetiredProjectorDriftPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`ALTER ROLE vane_agent_session_fact_projector LOGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 127); err == nil ||
		!strings.Contains(err.Error(), "retired projector role is absent or unsafe") {
		t.Fatalf("migration 128 Down accepted unsafe retired role: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		ALTER ROLE vane_agent_session_fact_projector NOLOGIN NOINHERIT NOBYPASSRLS
		 NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`); err != nil {
		t.Fatal(err)
	}
	assertDownRejectsAuthority := func(label string) {
		t.Helper()
		if _, err := provider.DownTo(t.Context(), 127); err == nil ||
			!strings.Contains(err.Error(), "retired projector retains") {
			t.Fatalf("migration 128 Down accepted %s authority drift: %v", label, err)
		}
	}

	// Database ACLs are shared-catalog objects.  Checking only the current
	// database's relation/function catalogs misses both this database and a
	// different database in the same PostgreSQL cluster.
	var currentDatabase string
	if err := database.QueryRowContext(t.Context(), `SELECT current_database()`).Scan(
		&currentDatabase,
	); err != nil {
		t.Fatal(err)
	}
	grantCurrentDatabase := `GRANT CONNECT ON DATABASE ` + pgQuoteIdent(currentDatabase) +
		` TO vane_agent_session_fact_projector`
	revokeCurrentDatabase := `REVOKE CONNECT ON DATABASE ` + pgQuoteIdent(currentDatabase) +
		` FROM vane_agent_session_fact_projector`
	if _, err := database.ExecContext(t.Context(), grantCurrentDatabase); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = database.ExecContext(ctx, revokeCurrentDatabase)
	})
	assertDownRejectsAuthority("current database CONNECT")
	if _, err := database.ExecContext(t.Context(), revokeCurrentDatabase); err != nil {
		t.Fatal(err)
	}

	if _, err := database.ExecContext(t.Context(),
		`GRANT CONNECT ON DATABASE postgres TO vane_agent_session_fact_projector`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = database.ExecContext(ctx,
			`REVOKE CONNECT ON DATABASE postgres FROM vane_agent_session_fact_projector`)
	})
	assertDownRejectsAuthority("cross-database CONNECT")
	if _, err := database.ExecContext(t.Context(),
		`REVOKE CONNECT ON DATABASE postgres FROM vane_agent_session_fact_projector`); err != nil {
		t.Fatal(err)
	}

	if _, err := database.ExecContext(t.Context(), `
		CREATE TYPE migration_128_projector_acl AS ENUM ('retired');
		GRANT USAGE ON TYPE migration_128_projector_acl
		  TO vane_agent_session_fact_projector`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = database.ExecContext(ctx, `
			REVOKE USAGE ON TYPE migration_128_projector_acl
			  FROM vane_agent_session_fact_projector;
			DROP TYPE IF EXISTS migration_128_projector_acl`)
	})
	assertDownRejectsAuthority("type ACL")
	if _, err := database.ExecContext(t.Context(), `
		REVOKE USAGE ON TYPE migration_128_projector_acl
		  FROM vane_agent_session_fact_projector;
		DROP TYPE migration_128_projector_acl`); err != nil {
		t.Fatal(err)
	}

	if _, err := database.ExecContext(t.Context(), `
		CREATE SCHEMA migration_128_projector_owned
		  AUTHORIZATION vane_agent_session_fact_projector`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = database.ExecContext(ctx, `
			ALTER SCHEMA migration_128_projector_owned OWNER TO CURRENT_USER;
			DROP SCHEMA IF EXISTS migration_128_projector_owned CASCADE`)
	})
	assertDownRejectsAuthority("owned schema")
	if _, err := database.ExecContext(t.Context(), `
		ALTER SCHEMA migration_128_projector_owned OWNER TO CURRENT_USER;
		DROP SCHEMA migration_128_projector_owned`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 127); err != nil {
		t.Fatalf("migration 128 Down after drift repair: %v", err)
	}
}

func TestMigration128SchemaUpDoesNotChangeClusterRuntimeMembershipPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 127); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.provision_vane_server_runtime_v1()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = database.ExecContext(ctx,
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
		if err := DeprovisionServerRuntime(ctx, scratchURL); err != nil {
			t.Errorf("deprovision cross-database runtime fixture: %v", err)
		}
	})
	assertRuntimeProjectorMembership := func(want bool) {
		t.Helper()
		var got bool
		if err := database.QueryRowContext(t.Context(), `SELECT EXISTS(
			SELECT 1 FROM pg_auth_members edge
			JOIN pg_roles granted ON granted.oid=edge.roleid
			JOIN pg_roles member ON member.oid=edge.member
			WHERE granted.rolname='vane_agent_session_fact_projector'
			  AND member.rolname='vane_server_runtime')`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("runtime projector membership=%v, want %v", got, want)
		}
	}
	assertRuntimeProjectorMembership(true)
	if _, err := provider.UpTo(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	// Ordinary per-database schema migration must not alter a cluster-global
	// identity shared by other databases in the PostgreSQL cluster.
	assertRuntimeProjectorMembership(true)
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	assertRuntimeProjectorMembership(false)
	if err := DeprovisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
}

func migration128Scratch(
	t *testing.T, databaseURL string,
) (*sql.DB, *goose.Provider, string, func()) {
	t.Helper()
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		drop()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		drop()
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		drop()
		t.Fatal(err)
	}
	return database, provider, scratchURL, drop
}

func migration128Identity(t *testing.T, database *sql.DB) (userID, tenantID int64) {
	t.Helper()
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-128-owner','owner') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(
		&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	return userID, tenantID
}
