package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration075SeparatesToolRuntimeFromLegacyShadowFence(t *testing.T) {
	raw, err := os.ReadFile("migrations/075_tool_run_snapshot_admission.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"'vane.run-snapshot-ref/v1'",
		"'vane.run-snapshot-ref/v2'",
		"'vane.task-approved-definition/v2'",
		"SECURITY DEFINER",
		"NEW.v2_cutover_event_id IS NOT NULL",
		"NEW.execution_mode IS DISTINCT FROM schedule_execution_mode",
		"adaptive_basis_version",
		"adaptive_schema_version IS DISTINCT FROM",
		"adaptive_last_known_good_definition_version IS NOT NULL",
		"FOR SHARE OF d, a",
	} {
		if !strings.Contains(sql, required) {
			t.Errorf("migration 075 missing %q", required)
		}
	}
}
