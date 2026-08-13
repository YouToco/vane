package store

import (
	"database/sql"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMigration129LedgerBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/129_long_term_memory.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE memory_authorizations",
		"CREATE TABLE memory_records",
		"CREATE TABLE memory_events",
		"CREATE TABLE memory_receipts",
		"evidence_source_type='owner_explicit_agent_turn'",
		"CREATE UNIQUE INDEX uq_memory_event_target_once",
		"CREATE FUNCTION assert_vane_memory_editor_v129()",
		"ALTER TABLE memory_records FORCE ROW LEVEL SECURITY",
		"CREATE POLICY memory_records_exact_user",
		"GRANT SELECT,INSERT ON memory_records,memory_events,memory_receipts",
		"refusing downgrade while retained memory history exists",
		"REVOKE vane_memory_editor FROM vane_server_runtime",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 129 lost boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"UPDATE ON memory_records", "DELETE ON memory_records",
		"REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE",
		"ALTER ROLE vane_memory_editor RESET ALL",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 129 introduced forbidden authority %q", forbidden)
		}
	}
}

func TestMigration129RLSRetentionAndDownGuardPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 129); err != nil {
		t.Fatal(err)
	}
	var canUpdate, canDelete bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_memory_editor','memory_records','UPDATE'),
		       has_table_privilege('vane_memory_editor','memory_records','DELETE')`,
	).Scan(&canUpdate, &canDelete); err != nil {
		t.Fatal(err)
	}
	if canUpdate || canDelete {
		t.Fatalf("memory editor mutation privilege update=%t delete=%t", canUpdate, canDelete)
	}
	userA, tenantA := migration129Identity(t, database, "a")
	userB, tenantB := migration129Identity(t, database, "b")
	var sessionA int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO agent_sessions(tenant_id,user_id) VALUES($1,$2) RETURNING id`,
		tenantA, userA).Scan(&sessionA); err != nil {
		t.Fatal(err)
	}
	authorizationA := uuid.NewString()
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO memory_authorizations(
		 id,tenant_id,user_id,session_id,trace_id,owner_request,
		 authorization_digest,request_digest)
		VALUES($1,$2,$3,$4,$5,'请记住 retained memory',repeat('d',64),repeat('c',64))`,
		authorizationA, tenantA, userA, sessionA, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO memory_records(
		 tenant_id,user_id,memory_text,evidence_source_type,evidence_source_id,
		 authorization_id,owner_request,authorization_digest)
		VALUES($1,$2,'inferred','model_inferred',$3,$4,'请记住 inferred',repeat('d',64))`,
		tenantA, userA, uuid.NewString(), authorizationA); err == nil {
		t.Fatal("database accepted model-inferred memory authority")
	}
	var recordA int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO memory_records(
		 tenant_id,user_id,memory_text,evidence_source_type,evidence_source_id,
		 authorization_id,owner_request,authorization_digest)
		VALUES($1,$2,'retained memory','owner_explicit_agent_turn',$3,$4,
		       '请记住 retained memory',repeat('d',64))
		RETURNING id`, tenantA, userA, uuid.NewString(), authorizationA).Scan(&recordA); err != nil {
		t.Fatal(err)
	}
	var eventA int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO memory_events(
		 tenant_id,user_id,actor_user_id,event_kind,result_memory_id,
		 evidence_source_type,evidence_source_id,authorization_id,
		 owner_request,authorization_digest)
		VALUES($1,$2,$2,'remember',$3,'owner_explicit_agent_turn',$4,$5,
		       '请记住 retained memory',repeat('d',64))
		RETURNING id`, tenantA, userA, recordA, uuid.NewString(), authorizationA).Scan(&eventA); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO memory_receipts(
		 tenant_id,user_id,idempotency_key,request_digest,event_id,response_payload)
		VALUES($1,$2,repeat('a',64),repeat('b',64),$3,'{}')`,
		tenantA, userA, eventA); err != nil {
		t.Fatal(err)
	}
	// Revoking membership removes authorization without deleting the ledger.
	if _, err := database.ExecContext(t.Context(), `
		DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenantA, userA); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := database.QueryRowContext(t.Context(), `
		SELECT (SELECT count(*) FROM memory_records WHERE id=$1)+
		       (SELECT count(*) FROM memory_events WHERE id=$2)+
		       (SELECT count(*) FROM memory_receipts WHERE event_id=$2)`,
		recordA, eventA).Scan(&retained); err != nil || retained != 3 {
		t.Fatalf("membership revoke retained=%d err=%v", retained, err)
	}

	// The restricted role sees only the exact configured user and cannot mutate
	// history. A deliberately mismatched scope sees zero rows.
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true)`, strconv.FormatInt(tenantB, 10),
		strconv.FormatInt(userB, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_memory_editor`); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := tx.QueryRowContext(t.Context(),
		`SELECT count(*) FROM memory_records`).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("cross-tenant/user RLS exposed %d records", visible)
	}
	if _, err := tx.ExecContext(t.Context(),
		`UPDATE memory_records SET memory_text='changed' WHERE id=$1`, recordA); err == nil {
		t.Fatal("memory editor unexpectedly has UPDATE authority")
	}
	_ = tx.Rollback()

	if _, err := provider.DownTo(t.Context(), 128); err == nil ||
		!strings.Contains(err.Error(), "retained memory history") {
		t.Fatalf("migration 129 Down destroyed retained history: %v", err)
	}
}

func TestMigration129PlainUpDoesNotGrantClusterRuntimePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.provision_vane_server_runtime_v128()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = database.ExecContext(ctx, `ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
		_ = DeprovisionServerRuntime(ctx, scratchURL)
	})
	if _, err := provider.UpTo(t.Context(), 129); err != nil {
		t.Fatal(err)
	}
	assertMembershipColumns := func(want bool) {
		t.Helper()
		var tenantColumn, userColumn bool
		if err := database.QueryRowContext(t.Context(), `
			SELECT has_column_privilege(
			         'vane_memory_editor','memberships','tenant_id','SELECT'),
			       has_column_privilege(
			         'vane_memory_editor','memberships','user_id','SELECT')`,
		).Scan(&tenantColumn, &userColumn); err != nil {
			t.Fatal(err)
		}
		if tenantColumn != want || userColumn != want {
			t.Fatalf("membership column privileges=(%t,%t), want %t",
				tenantColumn, userColumn, want)
		}
	}
	assertMembershipColumns(false)
	assertRuntimeMemoryMembership := func(want bool) {
		t.Helper()
		var got bool
		if err := database.QueryRowContext(t.Context(), `
			SELECT pg_has_role(
			 'vane_server_runtime','vane_memory_editor','MEMBER')`,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("runtime memory membership=%t, want %t", got, want)
		}
	}
	assertRuntimeMemoryMembership(false)
	// Every required authority is part of the startup contract. Removing any
	// single privilege must fail the same provision operation that deploys and
	// reconciles the server runtime, rather than surfacing on the first owner
	// memory request.
	requiredACLMutations := []struct {
		name    string
		revoke  string
		restore string
	}{
		{"schema usage", `REVOKE USAGE ON SCHEMA public FROM vane_memory_editor`, `GRANT USAGE ON SCHEMA public TO vane_memory_editor`},
		{"authorizations select", `REVOKE SELECT ON memory_authorizations FROM vane_memory_editor`, `GRANT SELECT ON memory_authorizations TO vane_memory_editor`},
		{"authorizations insert", `REVOKE INSERT ON memory_authorizations FROM vane_memory_editor`, `GRANT INSERT ON memory_authorizations TO vane_memory_editor`},
		{"records select", `REVOKE SELECT ON memory_records FROM vane_memory_editor`, `GRANT SELECT ON memory_records TO vane_memory_editor`},
		{"records insert", `REVOKE INSERT ON memory_records FROM vane_memory_editor`, `GRANT INSERT ON memory_records TO vane_memory_editor`},
		{"events select", `REVOKE SELECT ON memory_events FROM vane_memory_editor`, `GRANT SELECT ON memory_events TO vane_memory_editor`},
		{"events insert", `REVOKE INSERT ON memory_events FROM vane_memory_editor`, `GRANT INSERT ON memory_events TO vane_memory_editor`},
		{"receipts select", `REVOKE SELECT ON memory_receipts FROM vane_memory_editor`, `GRANT SELECT ON memory_receipts TO vane_memory_editor`},
		{"receipts insert", `REVOKE INSERT ON memory_receipts FROM vane_memory_editor`, `GRANT INSERT ON memory_receipts TO vane_memory_editor`},
		{"records sequence usage", `REVOKE USAGE ON SEQUENCE memory_records_id_seq FROM vane_memory_editor`, `GRANT USAGE ON SEQUENCE memory_records_id_seq TO vane_memory_editor`},
		{"records sequence select", `REVOKE SELECT ON SEQUENCE memory_records_id_seq FROM vane_memory_editor`, `GRANT SELECT ON SEQUENCE memory_records_id_seq TO vane_memory_editor`},
		{"events sequence usage", `REVOKE USAGE ON SEQUENCE memory_events_id_seq FROM vane_memory_editor`, `GRANT USAGE ON SEQUENCE memory_events_id_seq TO vane_memory_editor`},
		{"events sequence select", `REVOKE SELECT ON SEQUENCE memory_events_id_seq FROM vane_memory_editor`, `GRANT SELECT ON SEQUENCE memory_events_id_seq TO vane_memory_editor`},
		{"authorization consume", `REVOKE UPDATE (consumed_event_id) ON memory_authorizations FROM vane_memory_editor`, `GRANT UPDATE (consumed_event_id) ON memory_authorizations TO vane_memory_editor`},
	}
	for _, mutation := range requiredACLMutations {
		if _, err := database.ExecContext(t.Context(), mutation.revoke); err != nil {
			t.Fatalf("revoke %s: %v", mutation.name, err)
		}
		if err := ProvisionServerRuntime(t.Context(), scratchURL); err == nil ||
			!strings.Contains(err.Error(), "required authorities") {
			t.Fatalf("provision accepted missing %s: %v", mutation.name, err)
		}
		if _, err := database.ExecContext(t.Context(), mutation.restore); err != nil {
			t.Fatalf("restore %s: %v", mutation.name, err)
		}
	}
	// Required privileges must not be delegable by the restricted role.
	if _, err := database.ExecContext(t.Context(),
		`GRANT SELECT ON memory_records TO vane_memory_editor WITH GRANT OPTION`); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err == nil ||
		!strings.Contains(err.Error(), "unexpected authorities") {
		t.Fatalf("provision accepted grant-option drift: %v", err)
	}
	if _, err := database.ExecContext(t.Context(),
		`REVOKE GRANT OPTION FOR SELECT ON memory_records FROM vane_memory_editor`); err != nil {
		t.Fatal(err)
	}
	// Database CONNECT is a shared-object ACL and is therefore not visible in
	// relation/schema catalogs. pg_shdepend must nevertheless reject it.
	if _, err := database.ExecContext(t.Context(), `DO $body$
	BEGIN
	  EXECUTE format('GRANT CONNECT ON DATABASE %I TO vane_memory_editor',
	                 current_database());
	END $body$`); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err == nil ||
		!strings.Contains(err.Error(), "unexpected authorities") {
		t.Fatalf("provision accepted database CONNECT authority: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `DO $body$
	BEGIN
	  EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM vane_memory_editor',
	                 current_database());
	END $body$`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`GRANT SELECT ON agent_sessions TO vane_memory_editor`); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err == nil ||
		!strings.Contains(err.Error(), "unexpected authorities") {
		t.Fatalf("provision accepted extra relation ACL: %v", err)
	}
	if _, err := database.ExecContext(t.Context(),
		`REVOKE SELECT ON agent_sessions FROM vane_memory_editor`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`GRANT vane_memory_editor TO vane_app WITH SET TRUE, INHERIT FALSE`); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err == nil ||
		!strings.Contains(err.Error(), "membership drift") {
		t.Fatalf("provision accepted extra memory member: %v", err)
	}
	if _, err := database.ExecContext(t.Context(),
		`REVOKE vane_memory_editor FROM vane_app`); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	assertRuntimeMemoryMembership(true)
	// Provisioning is a deploy/restart reconciliation operation, not a one-shot
	// migration. The v129 wrapper must remain idempotent even though the frozen
	// v098 validator only knows the historical role set.
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatalf("second exact v129 provision: %v", err)
	}
	assertRuntimeMemoryMembership(true)
	var admin, inherit, setOption bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT edge.admin_option,edge.inherit_option,edge.set_option
		  FROM pg_catalog.pg_auth_members edge
		  JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
		  JOIN pg_catalog.pg_roles member ON member.oid=edge.member
		 WHERE granted.rolname='vane_memory_editor'
		   AND member.rolname='vane_server_runtime'`,
	).Scan(&admin, &inherit, &setOption); err != nil {
		t.Fatal(err)
	}
	if admin || inherit || !setOption {
		t.Fatalf("memory runtime edge admin=%t inherit=%t set=%t",
			admin, inherit, setOption)
	}
	if _, err := provider.DownTo(t.Context(), 128); err == nil ||
		!strings.Contains(err.Error(), "deprovision vane_server_runtime") {
		t.Fatalf("migration 129 Down accepted retained runtime: %v", err)
	}
	if err := DeprovisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 128); err != nil {
		t.Fatal(err)
	}
	assertMembershipColumns(false)
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err == nil ||
		!strings.Contains(err.Error(), "provision_vane_server_runtime_v129") {
		t.Fatalf("current provisioner silently fell back after Down: %v", err)
	}
}

func migration129Identity(
	t *testing.T, database *sql.DB, suffix string,
) (userID, tenantID int64) {
	t.Helper()
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name) VALUES($1,$1) RETURNING id`,
		"migration-129-"+suffix+"-"+uuid.NewString()).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	return userID, tenantID
}
