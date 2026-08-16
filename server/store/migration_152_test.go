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
}

func TestMigration152CapabilityInvocationLedgerPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 152); err != nil {
		t.Fatal(err)
	}
	admissionUser, admittedTenant := migration128Identity(t, database)
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
		{"receipt delete authority", `GRANT DELETE ON capability_invocation_receipts
			TO vane_capability_invocation_coordinator`},
		{"delegable role edge", `GRANT vane_capability_invocation_coordinator TO CURRENT_USER
			WITH ADMIN TRUE,SET TRUE,INHERIT FALSE`},
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

	// Hold the same shared advisory admission lock that every runtime settlement
	// acquires, then start the real goose Down on a second connection. Down must
	// wait at tenant admission instead of taking the ledger first; the runtime
	// transaction can therefore acquire its ledger lock and finish without a
	// lock cycle.
	admissionTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admissionTx.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock_shared(
		hashtextextended('vane/tenant-admission/v1/'||($1::bigint)::text,1447120453))`, admittedTenant); err != nil {
		_ = admissionTx.Rollback()
		t.Fatal(err)
	}
	downDone := make(chan error, 1)
	go func() {
		_, downErr := provider.DownTo(context.Background(), 151)
		downDone <- downErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waitingAtAdmission bool
		if err := database.QueryRowContext(t.Context(), `SELECT EXISTS(
			SELECT 1 FROM pg_catalog.pg_stat_activity
			WHERE datname=current_database() AND pid<>pg_backend_pid()
			  AND wait_event_type='Lock' AND wait_event='advisory'
			  AND query LIKE '%vane/tenant-admission/v1/%')`).
			Scan(&waitingAtAdmission); err != nil {
			_ = admissionTx.Rollback()
			t.Fatal(err)
		}
		if waitingAtAdmission {
			break
		}
		if time.Now().After(deadline) {
			_ = admissionTx.Rollback()
			t.Fatal("migration 152 Down did not block at tenant admission lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := admissionTx.ExecContext(t.Context(), `SET LOCAL lock_timeout='2s'`); err != nil {
		_ = admissionTx.Rollback()
		t.Fatal(err)
	}
	var admittedUser int64
	if err := admissionTx.QueryRowContext(t.Context(), `SELECT membership.user_id
		FROM memberships membership JOIN tenants tenant ON tenant.id=membership.tenant_id
		WHERE tenant.id=$1 AND membership.user_id=$2 FOR SHARE OF membership,tenant`,
		admittedTenant, admissionUser).Scan(&admittedUser); err != nil {
		_ = admissionTx.Rollback()
		t.Fatalf("runtime authority locks deadlocked with Down schema freeze: %v", err)
	}
	if _, err := admissionTx.ExecContext(t.Context(),
		`LOCK TABLE capability_invocations IN ROW EXCLUSIVE MODE`); err != nil {
		_ = admissionTx.Rollback()
		t.Fatalf("runtime admission->ledger lock order deadlocked with Down: %v", err)
	}
	if err := admissionTx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-downDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("migration 152 Down did not finish after admission transaction committed")
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
