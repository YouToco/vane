package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration078PersistsBoundedToolEvidence(t *testing.T) {
	raw, err := os.ReadFile("migrations/078_tool_delivery_evidence.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"ADD COLUMN tool_evidence_required BOOLEAN NOT NULL DEFAULT FALSE",
		"ADD COLUMN tool_evidence JSONB",
		"NOT tool_evidence_required OR tool_evidence IS NOT NULL",
		"jsonb_array_length(tool_evidence) BETWEEN 1 AND 8",
		"task_run_content_provenance",
		"NEW.content_item_id IS NOT NULL",
		"first_evidence ->> 'invocation_digest'",
		"IS DISTINCT FROM 'object'",
		"Tool delivery evidence is immutable",
		"legacy delivery cannot carry Tool evidence",
		"refusing downgrade while Tool delivery evidence exists",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 078 omitted %q", required)
		}
	}
}
