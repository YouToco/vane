package store

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMigration117KeepsPausedQuotaReadBoundToPreparedShadow(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/117_research_paused_shadow_quota.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := strings.ReplaceAll(string(payload), "\r\n", "\n")
	for _, required := range []string{
		"schedule.status='active' OR",
		"schedule.status='paused' AND EXISTS",
		"head.prepared_schedule_status='paused'",
		"operation.original_schedule_status='paused'",
		"operation.original_schedule_status=head.prepared_schedule_status",
		"operation.phase='prepared'",
		"operation.target_definition_version=head.definition_version",
		"operation.source_baseline_digest=head.source_baseline_digest",
		"operation.original_execution_mode=head.base_execution_mode",
		"definition.schema_version='vane.task-approved-definition/v3'",
		"schedule.execution_mode=head.base_execution_mode",
		"schedule.execution_mode='discover_at_run'",
		"tenant.status='active' AND tenant.deleted_at IS NULL",
		"membership.role='owner'",
		"REVOKE ALL ON FUNCTION resolve_research_quota_rule_v1",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("migration 117 is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"schedule.status IN ('active','paused')",
		"GRANT SELECT ON tenant_quota",
		"quota.tokens",
		"research_v3_delivery_authorities",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("migration 117 widens paused quota authority with %q", forbidden)
		}
	}
}

func TestMigration117PausedPreparedQuotaProjectionPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, scratchURL := newMigration116ScratchProvider(t, databaseURL)
	if _, err := provider.UpTo(t.Context(), 116); err != nil {
		t.Fatal(err)
	}
	owner, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	user, err := owner.UpsertUserByOpenID(t.Context(),
		"ou-m117-"+uuid.NewString(), "migration 117 owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := database.QueryRowContext(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}
	if err := owner.SeedTenantQuota(t.Context(), tenantID); err != nil {
		t.Fatal(err)
	}
	taskID := "m117-paused-" + uuid.NewString()
	if _, err := database.ExecContext(t.Context(), `INSERT INTO schedules
		(id,tenant_id,user_id,nl_description,spec_json,scope_json,status,push_strictness)
		VALUES($1,$2,$3,'paused shadow quota','{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}',
		'{}','paused','strict')`, taskID, tenantID, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO schedule_playbooks
		(schedule_id,content,fetch_plan) VALUES($1,'paused shadow quota manual','{}')`,
		taskID); err != nil {
		t.Fatal(err)
	}
	p := researchV3PreparePolicyForTest()
	p.TenantID, p.UserID, p.TaskID, p.IdempotencyKey =
		tenantID, user.ID, taskID, "m117-prepare"
	if _, err := owner.PrepareResearchV3Definition(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	countQuotaRows := func() int {
		t.Helper()
		tx, err := database.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(t.Context(),
			`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
			fmt.Sprint(tenantID), fmt.Sprint(user.ID)); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := tx.QueryRowContext(t.Context(), `SELECT count(*) FROM
			resolve_research_quota_rule_v1($1,$2,$3,'llm_tokens')`,
			tenantID, user.ID, taskID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if count := countQuotaRows(); count != 0 {
		t.Fatalf("schema 116 exposed paused quota rows=%d", count)
	}
	if _, err := provider.UpTo(t.Context(), 117); err != nil {
		t.Fatal(err)
	}
	if count := countQuotaRows(); count != 1 {
		t.Fatalf("schema 117 paused prepared quota rows=%d, want 1", count)
	}
	if _, err := provider.DownTo(t.Context(), 116); err != nil {
		t.Fatal(err)
	}
	if count := countQuotaRows(); count != 0 {
		t.Fatalf("schema 117 Down retained paused quota rows=%d", count)
	}
	if _, err := provider.UpTo(t.Context(), 117); err != nil {
		t.Fatal(err)
	}
	if count := countQuotaRows(); count != 1 {
		t.Fatalf("schema 117 reapply paused prepared quota rows=%d, want 1", count)
	}
	if _, err := database.ExecContext(t.Context(),
		`DELETE FROM research_v3_prepared_definition_heads
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`, tenantID, user.ID, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE research_v3_definition_prepare_operations SET phase='rolled_back'
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`, tenantID, user.ID, taskID); err != nil {
		t.Fatal(err)
	}
	if count := countQuotaRows(); count != 0 {
		t.Fatalf("rolled-back preparation exposed quota rows=%d", count)
	}
}
