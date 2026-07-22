package store

import (
	"encoding/json"
	"testing"
)

// This is a hand-pinned v1 wire artifact, not an expected value produced by
// the current writer. Changing a field/tag/order without bumping and retaining
// the old schema reader must fail here before persisted BYTEA rows are shipped.
func TestTaskRunSnapshotPayloadV1Golden(t *testing.T) {
	const golden = `{"schema_version":"vane.task-run-snapshot-payload/v1","tenant_id":7,"user_id":11,"task_id":"golden-task","run_kind":"scheduled","mode":"compiled","adaptive_version":0,"policies":{"capability_catalog":{"capabilities":["web/search"]},"tool_policy":{"allow":["fetch"]},"prompt_policy":{"score":"v1"},"model_policy":{"model":"m1","provider":"test"},"quota_policy":{"bucket":"fetch","limit":7}},"budget":{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0},"definition":{"task_id":"golden-task","tenant_id":7,"user_id":11,"nl_description":"monitor status","spec_json":{"cron":"0 8 * * *","tz":"UTC"},"scope_json":{"max_items":3},"playbook_content":"trusted only","strictness":"normal","source_scope":"approved_plan","fetch_plan":{"sources":[{"platform":"web","capability":"search","title":"Official","url":"https://example.test/status","config":{"query":"status"}}]},"sources":[{"source_id":42,"platform":"web","capability":"search","title":"Official","url":"https://example.test/status","config":{"query":"status"}}]},"reference_schema_version":"vane.run-snapshot-ref/v1"}`
	const goldenSHA256 = "6d19f3dd9212b73c0cf26f724bddd41adfd927de4e21deffe76631f9435c4e31"

	var payload taskRunSnapshotPayload
	if err := json.Unmarshal([]byte(golden), &payload); err != nil {
		t.Fatalf("decode pinned v1 payload: %v", err)
	}
	canonical, definitionDigest, planDigest, err := canonicalizeTaskRunPayload(&payload)
	if err != nil {
		t.Fatalf("current reader must retain v1 compatibility: %v", err)
	}
	if string(canonical) != golden {
		t.Fatalf("v1 payload bytes drifted without a schema bump:\n got %s\nwant %s",
			canonical, golden)
	}
	if got := sha256Hex(canonical); got != goldenSHA256 {
		t.Fatalf("v1 payload SHA drifted: got %s want %s", got, goldenSHA256)
	}
	if !validSHA256Digest(definitionDigest) || !validSHA256Digest(planDigest) {
		t.Fatal("pinned v1 definition and plan must remain digestible")
	}
}
