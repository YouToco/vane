package store

import (
	"strings"
	"testing"
)

func TestMigration103DurableRecoveryCursorBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/103_schedule_command_recovery_cursor.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sql := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE schedule_command_recovery_cursors",
		"worker_key = 'scheduler'",
		"REVOKE ALL ON schedule_command_recovery_cursors FROM PUBLIC,vane_app",
		"GRANT SELECT,INSERT,UPDATE ON schedule_command_recovery_cursors",
		"TO vane_schedule_commander",
		"DROP TABLE IF EXISTS schedule_command_recovery_cursors",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("migration 103 lost boundary %q", fragment)
		}
	}
}
