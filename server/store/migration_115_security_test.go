package store

import (
	"context"
	"testing"
)

func TestMigration115RevalidatesRuntimeAndNormalizesNewACLPostgres(t *testing.T) {
	_, db, provider := openMigration066Database(
		t, "vane_intelligence_v3_security_115",
	)
	if _, err := provider.UpTo(t.Context(), 111); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(),
		`SELECT provision_vane_server_runtime_v2()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(),
			`ALTER ROLE vane_server_runtime NOLOGIN NOBYPASSRLS NOREPLICATION`)
		_, _ = db.ExecContext(context.Background(),
			`SELECT deprovision_vane_server_runtime_v2()`)
	})
	if _, err := provider.UpTo(t.Context(), 114); err != nil {
		t.Fatal(err)
	}

	// Role attributes are mutable cluster state. Migration 115 must not expand
	// reader capability after the intentional runtime member becomes capable of
	// bypassing the row-security boundary validated by migration 114.
	if _, err := db.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime BYPASSRLS`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 115); err == nil {
		t.Fatal("migration 115 accepted a BYPASSRLS server runtime")
	}
	if _, err := db.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime NOBYPASSRLS`); err != nil {
		t.Fatal(err)
	}

	// Simulate an out-of-band broad grant on each new catalog surface. The
	// migration must remove it before applying the fixed semantic projection.
	if _, err := db.ExecContext(t.Context(), `
		GRANT SELECT ON research_run_plans,research_brief_syntheses,
		                research_brief_deliveries
		TO vane_intelligence_reader`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 115); err != nil {
		t.Fatal(err)
	}

	for _, check := range []struct {
		table, allowed, forbidden string
	}{
		{"research_run_plans", "plan_digest", "plan_payload"},
		{"research_brief_syntheses", "brief_payload", "context_payload"},
		{"research_brief_deliveries", "status", "provider_message_id"},
	} {
		var allowed, forbidden, tableWide bool
		if err := db.QueryRowContext(t.Context(), `SELECT
			has_column_privilege('vane_intelligence_reader',$1,$2,'SELECT'),
			has_column_privilege('vane_intelligence_reader',$1,$3,'SELECT'),
			has_table_privilege('vane_intelligence_reader',$1,'SELECT')`,
			check.table, check.allowed, check.forbidden,
		).Scan(&allowed, &forbidden, &tableWide); err != nil {
			t.Fatal(err)
		}
		if !allowed || forbidden || tableWide {
			t.Fatalf("migration 115 ACL %s allowed=%v forbidden=%v table=%v",
				check.table, allowed, forbidden, tableWide)
		}
	}
}
