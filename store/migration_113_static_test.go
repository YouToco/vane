package store

import (
	"os"
	"strings"
	"testing"
)

func TestMigration113FeedbackCatalogCapabilityIsColumnScoped(t *testing.T) {
	raw, err := os.ReadFile("migrations/113_feedback_intelligence_catalog_v2.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"'feedbacks','invalid'",
		"member_role.rolname NOT IN (CURRENT_USER,'vane_server_runtime')",
		"runtime_role.rolname='vane_server_runtime'",
		"runtime_role.rolsuper OR runtime_role.rolbypassrls",
		"GRANT SELECT (\n    id,tenant_id,user_id,delivery_id,action,reason_code,detail,",
		"GRANT SELECT (id,tenant_id,user_id,batch_id)",
		"GRANT SELECT (id,tenant_id,user_id,schedule_id,run_snapshot_id)",
		"GRANT SELECT (tenant_id,user_id,active_epoch)",
		"CREATE POLICY intelligence_feedback_identity ON feedbacks AS RESTRICTIVE",
		"CREATE POLICY intelligence_feedback_identity ON deliveries AS RESTRICTIVE",
		"CREATE POLICY intelligence_feedback_identity ON push_batches AS RESTRICTIVE",
		"CREATE POLICY intelligence_feedback_identity ON profiles AS RESTRICTIVE",
		"CREATE POLICY intelligence_feedback_identity ON profile_claim_states AS RESTRICTIVE",
		"WHERE dataset='feedbacks'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 113 is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON feedbacks", "GRANT SELECT ON deliveries",
		"GRANT SELECT ON push_batches", "DISABLE ROW LEVEL SECURITY",
		"DELETE FROM feedbacks", "DROP TABLE feedbacks",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 113 contains broad/destructive capability %q", forbidden)
		}
	}
}
