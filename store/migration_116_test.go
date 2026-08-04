package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestMigration116KeepsRecoveryDiscoveryNarrowAndShadowOnly(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/116_research_v3_paused_cutover_recovery.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := strings.ReplaceAll(string(payload), "\r\n", "\n")
	for _, required := range []string{
		"RETURNS TABLE(task_id TEXT)",
		"SECURITY DEFINER",
		"current_setting('role',true)<>'vane_app'",
		"pg_has_role(session_user,'vane_app','MEMBER')",
		"schedule.status IN ('active','paused')",
		"cannot downgrade with multiple enabled V3 authorities",
		"paused shadow admission patch is ambiguous",
		"paused shadow snapshot patch is ambiguous",
		"prepared status binding patch is ambiguous",
		"head.prepared_schedule_status=schedule.status",
		"tenant.status='active' AND tenant.deleted_at IS NULL",
		"membership.role='owner'",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("migration 116 is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON research_v3_delivery_authorities TO vane_app",
		"RETURNS TABLE(tenant_id",
		"RETURNS TABLE(user_id",
		"GRANT vane_research_v3_cutover_operator TO vane_server_runtime",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("migration 116 exposes forbidden authority %q", forbidden)
		}
	}
}

func TestMigration116PausedScopeAndRecoverySelectorPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	f := newResearchBriefFixtureV3(t,
		taskstate.NotificationThresholdMajorV3, true)
	digest := f.snapshotRef.DefinitionDigest
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedules SET status='paused' WHERE id=$1`, f.taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(), `
		INSERT INTO research_v3_delivery_authorities (
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,
		 status,enabled_at
		) VALUES ($2,$3,$1,1,$4,$5,$6,$7,'enabled',clock_timestamp())`,
		f.taskID, f.tenantID, f.userID, f.snapshotRef.DefinitionVersion,
		digest, strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = f.st.pool.Exec(ctx, `DELETE FROM research_v3_delivery_authorities
			WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`, f.tenantID, f.userID, f.taskID)
	})

	scope, err := f.st.ResolveResearchV3OperatorScope(t.Context(), f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	if scope.Status != types.ScheduleStatusPaused || scope.TenantID != f.tenantID ||
		scope.UserID != f.userID || scope.TaskID != f.taskID {
		t.Fatalf("resolved scope=%+v", scope)
	}
	taskIDs, err := f.st.ListEnabledResearchV3RecoveryTaskIDs(t.Context(), "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, taskID := range taskIDs {
		found = found || taskID == f.taskID
	}
	if !found {
		t.Fatalf("paused enabled task missing from recovery selector: %v", taskIDs)
	}
	var publicExecute, appExecute, appTableSelect bool
	var functionResult string
	if err := f.st.pool.QueryRow(t.Context(), `SELECT
		has_function_privilege('public',
		 'list_enabled_research_v3_recovery_tasks_v1(text,integer)','EXECUTE'),
		has_function_privilege('vane_app',
		 'list_enabled_research_v3_recovery_tasks_v1(text,integer)','EXECUTE'),
		has_table_privilege('vane_app','research_v3_delivery_authorities','SELECT'),
		pg_get_function_result(
		 'list_enabled_research_v3_recovery_tasks_v1(text,integer)'::regprocedure)`,
	).Scan(&publicExecute, &appExecute, &appTableSelect, &functionResult); err != nil {
		t.Fatal(err)
	}
	if publicExecute || !appExecute || appTableSelect || functionResult != "TABLE(task_id text)" {
		t.Fatalf("selector ACL public=%t app=%t table=%t result=%q",
			publicExecute, appExecute, appTableSelect, functionResult)
	}
}

func TestMigration116PausedTaskPreparedWhilePausedCanShadowPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required")
	}
	st, tenantID, userID, taskID := researchV3PrepareFixture(t)
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET status='paused' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	policy := researchV3PreparePolicyForTest()
	policy.TenantID, policy.UserID, policy.TaskID = tenantID, userID, taskID
	policy.IdempotencyKey = "prepare-while-paused"
	prepared, err := st.PrepareResearchV3Definition(t.Context(), policy)
	if err != nil || prepared.OriginalScheduleStatus != types.ScheduleStatusPaused {
		t.Fatalf("paused prepare=%+v err=%v", prepared, err)
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: "research-v3-shadow-" + strings.Repeat("c", 64),
		TemporalRunID:      "run-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID,
		UserID:             userID,
		TaskID:             taskID,
	}
	if _, err := st.CreateOrGetResearchRunSnapshotWithAuthorityV3(
		t.Context(), identity, testCompiledRunPolicyV1(t),
		testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t), "",
	); err != nil {
		t.Fatalf("paused exact shadow snapshot: %v", err)
	}
}

func TestMigration116DownRejectsMultipleEnabledAuthoritiesPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
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
	if _, err := provider.UpTo(t.Context(), 116); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	for index, taskID := range []string{"task-one", "task-two"} {
		if _, err := database.ExecContext(t.Context(), `
			INSERT INTO research_v3_delivery_authorities (
			 tenant_id,user_id,task_id,generation,definition_version,
			 definition_digest,target_action_digest,action_authorization_digest,
			 status,enabled_at
			) VALUES (1,1,$1,$2,1,$3,$3,$3,'enabled',clock_timestamp())`,
			taskID, index+1, digest); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := provider.DownTo(t.Context(), 113); err == nil ||
		!strings.Contains(err.Error(), "cannot downgrade with multiple enabled V3 authorities") {
		t.Fatalf("unsafe migration 116 downgrade result: %v", err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 116 {
		t.Fatalf("failed downgrade changed schema version to %d", version)
	}
}
