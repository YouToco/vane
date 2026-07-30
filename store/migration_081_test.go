package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration081BindsManualNominalTriggerExactly(t *testing.T) {
	raw, err := os.ReadFile(
		"migrations/081_manual_task_run_nominal_trigger.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	required := []string{
		"CREATE OR REPLACE FUNCTION authorize_manual_task_run_v1(",
		"'wf-manual-' || c.id::text",
		"c.created_at AT TIME ZONE 'UTC'",
		`'YYYY-MM-DD"T"HH24:MI:SS"Z"'`,
		"c.tenant_id = expected_tenant_id",
		"c.user_id = expected_user_id",
		"c.task_id = expected_task_id",
		"c.kind = 'run'",
		"c.status IN ('pending', 'completed')",
		"081: timestamped manual task run authority migration is irreversible",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration 081 omitted %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON schedule_commands TO vane_app",
		"GRANT SELECT ON schedule_commands TO vane_push_effect_coordinator",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 081 widened command-table access with %q",
				forbidden)
		}
	}
}
