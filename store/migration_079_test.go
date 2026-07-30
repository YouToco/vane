package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration079KeepsManualAuthorityNarrowAndFailClosed(t *testing.T) {
	raw, err := os.ReadFile("migrations/079_manual_task_run_authority.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	required := []string{
		"CREATE FUNCTION authorize_manual_task_run_v1(",
		"SECURITY DEFINER",
		"expected_workflow_id = 'wf-manual-' || c.id::text",
		"c.tenant_id = expected_tenant_id",
		"c.user_id = expected_user_id",
		"c.task_id = expected_task_id",
		"c.kind = 'run'",
		"c.status IN ('pending', 'completed')",
		"REVOKE ALL ON FUNCTION authorize_manual_task_run_v1(",
		"TO vane_app, vane_push_effect_coordinator",
		"CREATE OR REPLACE FUNCTION task_run_snapshot_v2_admission_fence()",
		"schedule_status = 'paused'",
		"public.authorize_manual_task_run_v1(",
		"079: manual task run authority migration is irreversible",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration 079 omitted %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON schedule_commands TO vane_app",
		"GRANT SELECT ON schedule_commands TO vane_push_effect_coordinator",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 079 widened command-table access with %q",
				forbidden)
		}
	}
}
