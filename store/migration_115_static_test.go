package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration115IntelligenceProjectionIsColumnScoped(t *testing.T) {
	raw, err := os.ReadFile("migrations/115_intelligence_v3_artifact_projection.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(raw), "\r\n", "\n")
	for _, required := range []string{
		"member_role.rolname NOT IN (CURRENT_USER,'vane_server_runtime')",
		"runtime_role.rolname='vane_server_runtime'",
		"runtime_role.rolsuper OR runtime_role.rolbypassrls",
		"REVOKE ALL ON research_run_plans,research_brief_syntheses,",
		"id,tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id,plan_digest",
		"GRANT SELECT (reference_schema_version)",
		"GRANT SELECT (task_id)",
		"GRANT SELECT (payload_digest)",
		"brief_payload,brief_digest",
		"tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id,status,sent_at",
		"ON research_run_plans TO vane_intelligence_reader",
		"ON research_brief_syntheses TO vane_intelligence_reader",
		"ON research_brief_deliveries TO vane_intelligence_reader",
		"CREATE POLICY intelligence_reader_identity ON task_run_snapshots AS RESTRICTIVE",
		"CREATE POLICY intelligence_reader_identity ON task_run_content_provenance AS RESTRICTIVE",
		"CREATE POLICY intelligence_reader_identity ON schedule_playbooks AS RESTRICTIVE",
		"DROP POLICY IF EXISTS intelligence_reader_identity ON task_run_snapshots",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 115 is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON research_run_plans",
		"GRANT SELECT ON research_brief_syntheses",
		"GRANT SELECT ON research_brief_deliveries",
		"DISABLE ROW LEVEL SECURITY", "SECURITY DEFINER",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 115 contains broad capability %q", forbidden)
		}
	}
}
