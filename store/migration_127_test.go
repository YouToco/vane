package store

import (
	"strings"
	"testing"
)

func TestMigration127DurableRecoveryCursorBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/127_schedule_command_recovery_cursor.sql",
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
		"CREATE FUNCTION count_task_creation_capacity_v1(",
		"SECURITY DEFINER",
		"requested_tenant_id IS DISTINCT FROM",
		"current_setting('app.tenant_id',true)",
		"GRANT EXECUTE ON FUNCTION count_task_creation_capacity_v1(BIGINT,BIGINT)",
		"TO vane_app",
		"DROP FUNCTION IF EXISTS count_task_creation_capacity_v1(BIGINT,BIGINT)",
		"DROP TABLE IF EXISTS schedule_command_recovery_cursors",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("migration 127 lost boundary %q", fragment)
		}
	}
}
