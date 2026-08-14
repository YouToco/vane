package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration077FencesToolDeliveryProvenanceAndDowngrade(
	t *testing.T,
) {
	raw, err := os.ReadFile("migrations/077_tool_delivery_provenance.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"ADD COLUMN invocation_digest TEXT",
		"vane.run-snapshot-ref/v2",
		"task_run_content_provenance",
		"NEW.content_item_id =",
		"ANY(p.content_item_ids)",
		"legacy delivery cannot carry Tool invocation provenance",
		"OLD.content_item_id IS NOT NULL",
		"NEW.content_item_id IS NULL",
		"NEW.invocation_digest IS NOT DISTINCT FROM",
		"SECURITY DEFINER",
		"FOR SHARE OF b",
		"FOR SHARE OF s",
		"BEFORE INSERT OR UPDATE OF",
		"LOCK TABLE deliveries",
		"IN ACCESS EXCLUSIVE MODE",
		"refusing downgrade while Tool delivery evidence exists",
	} {
		if !strings.Contains(sql, required) {
			t.Errorf("migration 077 missing %q", required)
		}
	}
}
