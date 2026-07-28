package store

import "testing"

func TestMigration071PeriodicRolesRLSAndEmptyDown(t *testing.T) {
	_, db, provider := openMigration066Database(
		t, "vane_periodic_brief_071_acl")
	if _, err := provider.UpTo(t.Context(), 71); err != nil {
		t.Fatal(err)
	}
	var canLogin, inherit, bypassRLS, superuser bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT rolcanlogin,rolinherit,rolbypassrls,rolsuper
		  FROM pg_roles WHERE rolname='vane_periodic_brief_writer'
	`).Scan(&canLogin, &inherit, &bypassRLS, &superuser); err != nil {
		t.Fatal(err)
	}
	if canLogin || inherit || bypassRLS || superuser {
		t.Fatalf("unsafe periodic writer role: %t/%t/%t/%t",
			canLogin, inherit, bypassRLS, superuser)
	}
	for _, table := range []string{
		"brief_report_settings", "periodic_brief_intents",
		"periodic_synthesis_receipts", "periodic_brief_reports",
	} {
		var rls bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT relrowsecurity FROM pg_class
			  WHERE oid=$1::regclass`, table).Scan(&rls); err != nil {
			t.Fatal(err)
		}
		if !rls {
			t.Fatalf("%s has RLS disabled", table)
		}
	}
	var appCanRead, writerCanDelete, readerCanRead bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT
		  has_table_privilege(
		    'vane_app','periodic_brief_reports','SELECT'),
		  has_table_privilege(
		    'vane_periodic_brief_writer',
		    'periodic_synthesis_receipts','DELETE'),
		  has_table_privilege(
		    'vane_brief_reader','periodic_brief_reports','SELECT')
	`).Scan(&appCanRead, &writerCanDelete, &readerCanRead); err != nil {
		t.Fatal(err)
	}
	if appCanRead || writerCanDelete || !readerCanRead {
		t.Fatalf("periodic ACL drifted: app_read=%v writer_delete=%v reader_read=%v",
			appCanRead, writerCanDelete, readerCanRead)
	}
	if _, err := provider.DownTo(t.Context(), 70); err != nil {
		t.Fatalf("empty 071 Down failed: %v", err)
	}
}
