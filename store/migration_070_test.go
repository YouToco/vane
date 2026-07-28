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
	var recoveryCanReadAny bool
	if err := db.QueryRowContext(t.Context(),
		`SELECT has_any_column_privilege(
		    'vane_brief_synthesis_recovery',
		    'executive_brief_synthesis_receipts','SELECT')`,
	).Scan(&recoveryCanReadAny); err != nil {
		t.Fatal(err)
	}
	if !recoveryCanReadAny {
		t.Fatal("recovery role cannot read receipt columns")
	}
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_synthesis_receipts", "INSERT", false)
	var insertIdentity, insertStatus bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_synthesis_receipts',
		           'run_outcome_id','INSERT'),
		       has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_synthesis_receipts',
		           'status','INSERT')`,
	).Scan(&insertIdentity, &insertStatus); err != nil {
		t.Fatal(err)
	}
	if !insertIdentity || insertStatus {
		t.Fatalf("recovery receipt INSERT boundary = %t/%t",
			insertIdentity, insertStatus)
	}
	checkPrivilege("vane_brief_synthesis_recovery",
		"executive_brief_artifacts", "SELECT", false)
	var recoveryArtifactRead, recoveryArtifactInsert,
		recoveryArtifactDelete bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_artifacts','payload','SELECT'),
		       has_column_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_artifacts','id','INSERT'),
		       has_table_privilege(
		           'vane_brief_synthesis_recovery',
		           'executive_brief_artifacts','DELETE')`,
	).Scan(
		&recoveryArtifactRead, &recoveryArtifactInsert,
		&recoveryArtifactDelete); err != nil {
		t.Fatal(err)
	}
	if !recoveryArtifactRead || !recoveryArtifactInsert ||
		recoveryArtifactDelete {
		t.Fatalf("recovery artifact boundary = %t/%t/%t",
			recoveryArtifactRead, recoveryArtifactInsert,
			recoveryArtifactDelete)
	}

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
	var roleCount, privilegedRoleCount int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*),
		       count(*) FILTER (
		           WHERE has_any_column_privilege(
		                     rolname,'brief_snapshots','SELECT')
		       )
		  FROM pg_roles
		 WHERE rolname = ANY($1)`,
		[]string{
			"vane_brief_synthesis_writer",
			"vane_brief_synthesis_recovery",
		},
	).Scan(&roleCount, &privilegedRoleCount); err != nil {
		t.Fatal(err)
	}
	if roleCount != 2 || privilegedRoleCount != 0 {
		t.Fatalf("070 Down role boundary roles=%d privileged=%d",
			roleCount, privilegedRoleCount)
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
