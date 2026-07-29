package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration076FencesSourceFreeEvidenceAndSafeDowngrade(
	t *testing.T,
) {
	raw, err := os.ReadFile("migrations/076_task_run_content_provenance.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"ALTER COLUMN source_id DROP NOT NULL",
		"PRIMARY KEY (run_snapshot_id, invocation_digest)",
		"vane.run-snapshot-ref/v2",
		"vane.task-run-content-observation-set/v1",
		"SECURITY DEFINER",
		"FOR SHARE OF s",
		"encode(sha256(NEW.observation_payload), 'hex')",
		"WITH ORDINALITY",
		"IS DISTINCT FROM content.content_id::text",
		"GRANT SELECT, INSERT",
		"ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY tenant_isolation",
		"LOCK TABLE task_run_content_provenance, content_items",
		"IN ACCESS EXCLUSIVE MODE",
		"refusing downgrade while Source-free content evidence exists",
		"ALTER COLUMN source_id SET NOT NULL",
	} {
		if !strings.Contains(sql, required) {
			t.Errorf("migration 076 missing %q", required)
		}
	}
	if strings.Contains(sql, "GRANT UPDATE") ||
		strings.Contains(sql, "GRANT DELETE") {
		t.Fatal("migration 076 granted mutable evidence privileges")
	}
}
