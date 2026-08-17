package store

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/internal/testgate"
)

func TestMigration152CapabilityInvocationLedgerContract(t *testing.T) {
	payload, err := os.ReadFile("migrations/152_capability_invocation_ledger.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, required := range []string{
		"CREATE TABLE capability_invocations",
		"CREATE TABLE capability_invocation_receipts",
		"FORCE ROW LEVEL SECURITY",
		"vane_capability_invocation_coordinator",
		"member.rolname='vane_server_runtime'",
		"enforce_capability_invocation_receipt_v1",
		"enforce_capability_invocation_checkpoint_v1",
		"status='unknown_effect'",
		"NEW.status='ambiguous'",
		"effective_grants_safe",
		"catalog_grants_safe",
		"schema_grants_safe",
		"pg_get_triggerdef",
		"pg_get_functiondef",
		"trigger_functions_safe",
		"refusing downgrade while retained capability invocation evidence exists",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("migration 152 omitted %q", required)
		}
	}
	if strings.Contains(source, "GRANT vane_capability_invocation_coordinator TO vane_server_runtime") {
		t.Error("dark migration granted the production runtime invocation authority")
	}
	schemaLock := strings.LastIndex(source, "LOCK TABLE tenants IN SHARE ROW EXCLUSIVE MODE")
	admissionLock := strings.LastIndex(source, "vane/tenant-admission/v1/")
	ledgerLock := strings.LastIndex(source,
		"LOCK TABLE capability_invocation_receipts,capability_invocations IN ACCESS EXCLUSIVE MODE")
	if schemaLock < 0 || admissionLock < 0 || ledgerLock < 0 ||
		schemaLock >= admissionLock || admissionLock >= ledgerLock {
		t.Fatal("migration 152 Down does not follow schema -> admission -> ledger lock order")
	}
	if !strings.Contains(source, "pg_try_advisory_xact_lock") ||
		!strings.Contains(source, "admission is busy; rollback and retry downgrade") {
		t.Fatal("migration 152 Down does not fail closed on busy tenant admission")
	}
}

func TestMigration152CapabilityInvocationLedgerPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 152); err != nil {
		t.Fatal(err)
	}
	admissionUser, admittedTenant := migration128Identity(t, database)
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if _, err := database.ExecContext(t.Context(),
		`SELECT public.assert_vane_capability_invocation_coordinator_v152()`); err != nil {
		t.Fatal(err)
	}

	var forced, runtimeMember bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		bool_and(c.relrowsecurity AND c.relforcerowsecurity),
		EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
		 JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
		 JOIN pg_catalog.pg_roles member ON member.oid=edge.member
		 WHERE granted.rolname='vane_capability_invocation_coordinator'
		   AND member.rolname='vane_server_runtime')
		FROM pg_catalog.pg_class c WHERE c.oid IN(
		 'public.capability_invocations'::regclass,
		 'public.capability_invocation_receipts'::regclass)`).Scan(&forced, &runtimeMember); err != nil {
		t.Fatal(err)
	}
	if !forced || runtimeMember {
		t.Fatalf("dark ledger authority forced=%v runtime_member=%v", forced, runtimeMember)
	}
	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{"tenantless RLS", `ALTER POLICY capability_invocation_select
			ON capability_invocations USING(user_id=NULLIF(current_setting('app.user_id',true),'')::bigint)`},
		{"OR true RLS bypass", `ALTER POLICY capability_invocation_select
			ON capability_invocations USING(
			 (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
			  user_id=NULLIF(current_setting('app.user_id',true),'')::bigint) OR true)`},
		{"missing RLS policy", `DROP POLICY capability_invocation_select
			ON capability_invocations`},
		{"decoy schema column ACL", `DO $mutation$ BEGIN
			EXECUTE 'CREATE SCHEMA capability_acl_decoy';
			EXECUTE 'CREATE TABLE capability_acl_decoy.capability_invocations(lease_until timestamptz)';
			EXECUTE 'REVOKE UPDATE(lease_until) ON public.capability_invocations FROM vane_capability_invocation_coordinator';
			EXECUTE 'GRANT UPDATE(lease_until) ON capability_acl_decoy.capability_invocations TO vane_capability_invocation_coordinator';
			END $mutation$`},
		{"PUBLIC schema CREATE", `GRANT CREATE ON SCHEMA public TO PUBLIC`},
		{"PUBLIC table DELETE", `GRANT DELETE ON public.capability_invocations TO PUBLIC`},
		{"PUBLIC table SELECT", `GRANT SELECT ON public.capability_invocations TO PUBLIC`},
		{"other role receipt INSERT", `GRANT INSERT ON public.capability_invocation_receipts TO vane_app`},
		{"PUBLIC column UPDATE", `GRANT UPDATE(principal_role) ON public.capability_invocations TO PUBLIC`},
		{"allowed column UPDATE for other role", `GRANT UPDATE(status) ON public.capability_invocations TO vane_app`},
		{"PUBLIC sequence UPDATE", `GRANT UPDATE ON SEQUENCE public.capability_invocation_receipts_id_seq TO PUBLIC`},
		{"PUBLIC sequence SELECT", `GRANT SELECT ON SEQUENCE public.capability_invocation_receipts_id_seq TO PUBLIC`},
		{"other role sequence USAGE", `GRANT USAGE ON SEQUENCE public.capability_invocation_receipts_id_seq TO vane_app`},
		{"receipt delete authority", `GRANT DELETE ON capability_invocation_receipts
			TO vane_capability_invocation_coordinator`},
		{"delegable role edge", `GRANT vane_capability_invocation_coordinator TO CURRENT_USER
			WITH ADMIN TRUE,SET TRUE,INHERIT FALSE`},
		{"disabled checkpoint trigger", `ALTER TABLE public.capability_invocations
			DISABLE TRIGGER capability_invocation_checkpoint_v1`},
		{"dropped receipt trigger", `DROP TRIGGER capability_invocation_receipt_v1
			ON public.capability_invocation_receipts`},
		{"rebound receipt trigger", `DO $mutation$ BEGIN
			EXECUTE 'DROP TRIGGER capability_invocation_receipt_v1 ON public.capability_invocation_receipts';
			EXECUTE 'CREATE TRIGGER capability_invocation_receipt_v1 BEFORE INSERT ON public.capability_invocation_receipts FOR EACH ROW EXECUTE FUNCTION public.enforce_capability_invocation_checkpoint_v1()';
			END $mutation$`},
		{"trigger relation owner", `ALTER TABLE public.capability_invocations OWNER TO vane_app`},
		{"replaced trigger function body", `CREATE OR REPLACE FUNCTION
			public.enforce_capability_invocation_receipt_v1() RETURNS trigger
			LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public,pg_temp
			AS $replacement$ BEGIN RETURN NEW; END $replacement$`},
		{"trigger function owner", `ALTER FUNCTION
			public.enforce_capability_invocation_receipt_v1() OWNER TO vane_app`},
		{"trigger function security", `ALTER FUNCTION
			public.enforce_capability_invocation_receipt_v1() SECURITY DEFINER`},
		{"trigger function search path", `ALTER FUNCTION
			public.enforce_capability_invocation_receipt_v1() SET search_path=public`},
	} {
		t.Run("assert rejects "+mutation.name, func(t *testing.T) {
			tx, err := database.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(t.Context(), mutation.sql); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(t.Context(),
				`SELECT public.assert_vane_capability_invocation_coordinator_v152()`); err == nil {
				t.Fatal("unsafe capability ledger catalog passed assertion")
			}
			if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
				t.Fatal(err)
			}
		})
	}

	// Block the real production PurgeTenant after it has acquired exclusive tenant
	// admission. The real goose Down must try (not wait for) that admission while
	// holding its schema freeze, reject explicitly, and roll back without 40P01.
	blockerTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blockerTx.Rollback() }()
	if _, err := blockerTx.ExecContext(t.Context(),
		`LOCK TABLE push_batches IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	purgeContext, cancelPurge := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelPurge()
	purgeDone := make(chan error, 1)
	go func() {
		_, purgeErr := st.PurgeTenant(purgeContext, admittedTenant, false)
		purgeDone <- purgeErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var purgeBlockedAfterAdmission bool
		if err := database.QueryRowContext(t.Context(), `SELECT EXISTS(
			SELECT 1 FROM pg_catalog.pg_stat_activity
			WHERE datname=current_database() AND pid<>pg_backend_pid()
			  AND wait_event_type='Lock'
			  AND query LIKE '%tenant purge push batch lock order%')`).
			Scan(&purgeBlockedAfterAdmission); err != nil {
			t.Fatal(err)
		}
		if purgeBlockedAfterAdmission {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PurgeTenant did not reach its post-admission production lock path")
		}
		time.Sleep(10 * time.Millisecond)
	}
	downStarted := time.Now()
	if _, err := provider.DownTo(t.Context(), 151); err == nil ||
		!strings.Contains(err.Error(), "admission is busy") ||
		strings.Contains(err.Error(), "40P01") || strings.Contains(strings.ToLower(err.Error()), "deadlock") {
		t.Fatalf("busy PurgeTenant downgrade err=%v, want explicit non-deadlock retry rejection", err)
	} else if time.Since(downStarted) > 3*time.Second {
		t.Fatalf("busy PurgeTenant downgrade waited instead of failing closed: %v", time.Since(downStarted))
	}
	var ledgerRetained bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regclass('public.capability_invocations') IS NOT NULL AND
		to_regclass('public.capability_invocation_receipts') IS NOT NULL`).Scan(&ledgerRetained); err != nil {
		t.Fatal(err)
	}
	if !ledgerRetained {
		t.Fatal("busy admission downgrade silently removed ledger schema")
	}
	if err := blockerTx.Rollback(); err != nil && err != sql.ErrTxDone {
		t.Fatal(err)
	}
	select {
	case err := <-purgeDone:
		if err != nil {
			t.Fatalf("PurgeTenant after rejected Down err=%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PurgeTenant did not finish after test barrier released")
	}
	var membershipGone bool
	if err := database.QueryRowContext(t.Context(), `SELECT NOT EXISTS(
		SELECT 1 FROM memberships WHERE tenant_id=$1 AND user_id=$2)`,
		admittedTenant, admissionUser).Scan(&membershipGone); err != nil {
		t.Fatal(err)
	}
	if !membershipGone {
		t.Fatal("production PurgeTenant path did not remove the target membership")
	}
	if _, err := provider.DownTo(t.Context(), 151); err != nil {
		t.Fatal(err)
	}
	var tablesGone bool
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regclass('public.capability_invocations') IS NULL AND
		to_regclass('public.capability_invocation_receipts') IS NULL`).Scan(&tablesGone); err != nil {
		t.Fatal(err)
	}
	if !tablesGone {
		t.Fatal("migration 152 empty downgrade retained ledger tables")
	}
}
