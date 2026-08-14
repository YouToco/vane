package store

import (
	"database/sql"
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestMigration086DownRestoresLegacyTriggerAndRefusesV3Evidence(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 086 integration tests")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 86); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 85); err != nil {
		t.Fatalf("empty 086 Down: %v", err)
	}
	var legacyTrigger string
	if err := database.QueryRowContext(t.Context(),
		`SELECT pg_get_triggerdef(oid)
		   FROM pg_trigger
		  WHERE tgrelid='task_run_snapshots'::regclass
		    AND tgname='task_run_snapshot_v2_admission_fence'
		    AND NOT tgisinternal`,
	).Scan(&legacyTrigger); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(legacyTrigger, " WHEN ") {
		t.Fatalf("legacy trigger remained filtered after 086 Down: %s", legacyTrigger)
	}
	if _, err := provider.UpTo(t.Context(), 86); err != nil {
		t.Fatal(err)
	}

	var tenantID, userID int64
	if err := database.QueryRowContext(t.Context(),
		`INSERT INTO tenants (status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(),
		`INSERT INTO users (feishu_open_id,name) VALUES ($1,'m086') RETURNING id`,
		"m086-"+uuid.NewString()).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	taskID := "m086-" + uuid.NewString()
	spec := json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`)
	if _, err := database.ExecContext(t.Context(),
		`INSERT INTO schedules (id,tenant_id,user_id,nl_description,spec_json,
		     scope_json,status,push_strictness)
		 VALUES ($1,$2,$3,'m086',$4,'{}','active','strict')`,
		taskID, tenantID, userID, spec); err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID, TaskName: "m086",
		TaskManual: "检查官方信息并与历史比较，没有重大更新不推送。",
		SpecJSON:   spec, ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3, SuppressEmpty: true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language: taskstate.OutputLanguageZhCNV3,
			Format:   taskstate.OutputFormatExecutiveBriefV3, IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16,
			MaxTokens: 32768, MaxCostMicroUSD: 1_000_000, DurationMs: 300_000},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := taskstate.EncodeApprovedDefinitionV3(definition)
	digest, _ := taskstate.DigestApprovedDefinitionV3(definition)
	if _, err := database.ExecContext(t.Context(),
		`INSERT INTO task_approved_definition_versions (
		     tenant_id,user_id,task_id,version,schema_version,execution_mode,
		     definition_digest,payload,operation_ref)
		 VALUES ($1,$2,$3,1,$4,'discover_at_run',$5,$6,$7)`,
		tenantID, userID, taskID, taskstate.ApprovedDefinitionSchemaVersionV3,
		digest, payload, "m086:"+uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE schedules SET execution_mode='discover_at_run',
		     approved_definition_version=1,approved_definition_digest=$2 WHERE id=$1`,
		taskID, digest); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	if _, err := database.ExecContext(t.Context(),
		`INSERT INTO task_run_snapshots (
		     tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
		     run_kind,execution_mode,adaptive_version,capability_catalog_digest,
		     tool_policy_digest,prompt_policy_digest,model_policy_digest,
		     quota_policy_digest,definition_digest,plan_digest,payload_digest,
		     reference_digest,reference_schema_version,payload,budget)
		 VALUES ($1,$2,$3,$4,$5,'scheduled','discover_at_run',0,$6,$6,$6,$6,$6,
		         $7,'',$6,$6,$8,$9,'{}')`,
		tenantID, userID, taskID, "wf-"+taskID, "run-"+uuid.NewString(), sha,
		digest, types.ResearchRunSnapshotRefSchemaV3, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 85); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade while research run evidence exists") {
		t.Fatalf("086 Down with V3 evidence err=%v", err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT COALESCE(MAX(version_id),0) FROM goose_db_version WHERE is_applied`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 86 {
		t.Fatalf("version after refused 086 Down=%d", version)
	}
}
