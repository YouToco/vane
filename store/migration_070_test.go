package store

import (
	"strings"
	"testing"
)

func TestMigration070SynthesisRolesAreLeastPrivilegeAndEmptyDown(
	t *testing.T,
) {
	_, db, provider := openMigration066Database(
		t, "vane_executive_brief_070_acl")
	if _, err := provider.UpTo(t.Context(), 70); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{
		"vane_brief_synthesis_writer",
		"vane_brief_synthesis_recovery",
	} {
		var (
			canLogin, inherit, bypassRLS, superuser bool
		)
		if err := db.QueryRowContext(t.Context(), `
			SELECT rolcanlogin,rolinherit,rolbypassrls,rolsuper
			  FROM pg_roles WHERE rolname=$1`, role,
		).Scan(&canLogin, &inherit, &bypassRLS, &superuser); err != nil {
			t.Fatal(err)
		}
		if canLogin || inherit || bypassRLS || superuser {
			t.Fatalf("unsafe synthesis role %s: %t/%t/%t/%t",
				role, canLogin, inherit, bypassRLS, superuser)
		}
	}
	checkPrivilege := func(role, object, privilege string, want bool) {
		t.Helper()
		var got bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT has_table_privilege($1,$2,$3)`,
			role, object, privilege).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s %s on %s = %t, want %t",
				role, privilege, object, got, want)
		}
	}
	checkPrivilege("vane_app",
		"executive_brief_synthesis_receipts", "SELECT", false)
	checkPrivilege("vane_app",
		"executive_brief_artifacts", "INSERT", false)
	checkPrivilege("vane_brief_synthesis_writer",
		"executive_brief_synthesis_receipts", "INSERT", true)
	checkPrivilege("vane_brief_synthesis_writer",
		"executive_brief_synthesis_receipts", "DELETE", false)
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_synthesis_receipts", "SELECT", true)
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_synthesis_receipts", "INSERT", false)
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_artifacts", "SELECT", false)

	if _, err := provider.DownTo(t.Context(), 69); err != nil {
		t.Fatalf("empty 070 Down failed: %v", err)
	}
	for _, object := range []string{
		"executive_brief_synthesis_receipts",
		"executive_brief_artifacts",
	} {
		var exists bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT to_regclass($1) IS NOT NULL`,
			"public."+object).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("070 Down left %s", object)
		}
	}
	var roleCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_roles
		 WHERE rolname = ANY($1)`,
		[]string{
			"vane_brief_synthesis_writer",
			"vane_brief_synthesis_recovery",
		},
	).Scan(&roleCount); err != nil {
		t.Fatal(err)
	}
	if roleCount != 0 {
		t.Fatalf("070 Down left synthesis roles: %d", roleCount)
	}
}

func TestMigration070DownRefusalTextIsExplicit(t *testing.T) {
	body, err := migrationsFS.ReadFile(
		"migrations/070_executive_brief_artifacts.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"NOLOGIN NOINHERIT",
		"NOBYPASSRLS",
		"converge to deterministic fallback",
		"refusing Down while executive Brief state exists",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("migration 070 lacks %q", want)
		}
	}
}
