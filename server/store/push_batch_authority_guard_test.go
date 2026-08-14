package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigration047RemainsAuthorityOnly(t *testing.T) {
	path := filepath.Join("migrations", "047_push_batch_delivery_authority.sql")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sql := string(body)
	for _, forbidden := range []string{
		"canonical_payload",
		"task_observed_events",
		"delivered_at",
		"lock_push_effect_aggregate_v1",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("authority-only migration contains %q", forbidden)
		}
	}
	for _, required := range []string{
		"pg_advisory_xact_lock(6215335020355474248)",
		"lock_push_effect_batch_v1",
		"vane_push_batch_authority",
		"refusing downgrade while durable batch authority exists",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("authority-only migration is missing %q", required)
		}
	}
}

func TestPushBatchAuthorityRunbookPinsIrreversibleBoundary(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "runbooks", "push-batch-authority-rollout.md")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"Stop every pre-047 worker",
		"Apply migration 047",
		"never roll a worker back to a pre-fence binary",
		"Production push-effect and recovery call points remain dark",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("authority rollout runbook is missing %q", required)
		}
	}
}
