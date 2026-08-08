package store

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMigration121KeepsDormantPinNarrowAndFailClosed(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration test path")
	}
	payload, err := os.ReadFile(filepath.Join(
		filepath.Dir(file), "migrations", "121_research_v3_dormant_snapshot_v2_pin.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(payload)
	for _, required := range []string{
		"schedule_mode = 'discover_at_run'",
		"'vane.task-approved-definition/v3'",
		"'vane.task-approved-definition/v1'",
		"pointer_definition_mode IS DISTINCT FROM 'compiled'",
		"task run snapshot v2 dormant definition pin is invalid",
		"rollback Research V3 tasks with dormant V2 pins before downgrade",
		"LOCK TABLE schedules IN SHARE ROW EXCLUSIVE MODE",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 121 lost guard %q", required)
		}
	}
}
