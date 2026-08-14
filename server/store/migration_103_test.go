package store

import (
	"strings"
	"testing"
)

func TestMigration103ExposesOnlyScopedQuotaPolicyProjection(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/103_research_control_quota_projection.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(payload), "\r\n", "\n")
	for _, required := range []string{
		"CREATE FUNCTION resolve_research_quota_rule_v1(",
		"RETURNS TABLE(out_rate DOUBLE PRECISION,out_burst DOUBLE PRECISION)",
		"requested_tenant_id IS DISTINCT FROM",
		"current_setting('app.tenant_id',true)",
		"requested_user_id IS DISTINCT FROM",
		"schedule.id=requested_task_id",
		"schedule.status='active'",
		"tenant.status='active' AND tenant.deleted_at IS NULL",
		"membership.role='owner'",
		"GRANT EXECUTE ON FUNCTION resolve_research_quota_rule_v1(",
		"REVOKE ALL ON tenant_quota FROM vane_app",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 103 missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON tenant_quota",
		"GRANT SELECT (rate,burst) ON tenant_quota",
		"SELECT quota.tokens",
		"TO vane_research_v3_executor",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 103 exposes forbidden capability %q", forbidden)
		}
	}
}
