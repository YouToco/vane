package store

import (
	"database/sql"
	"os"
	"strings"
	"testing"

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
