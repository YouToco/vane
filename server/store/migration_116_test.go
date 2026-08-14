package store

import (
	"database/sql"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
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
		"GRANT SELECT (prepared_schedule_status)",
		"REVOKE SELECT (prepared_schedule_status)",
		"length(definition)-length(replace(definition,needle,''))<>",
		"current_setting('role',true)<>'vane_app'",
		"pg_has_role(session_user,'vane_app','MEMBER')",
		"schedule.status IN ('active','paused')",
		"cannot downgrade with enabled V3 authority",
		"cannot downgrade with V3 cutover audit",
		"cannot downgrade with paused V3 preparation audit",
		"paused shadow admission patch is ambiguous",
		"paused shadow snapshot patch is ambiguous",
		"prepared status binding patch is ambiguous",
		"head.prepared_schedule_status=schedule.status",
		"tenant.status='active' AND tenant.deleted_at IS NULL",
		"membership.role='owner'",
		"DISABLE TRIGGER protect_research_v3_cutover_operation",
		"ENABLE TRIGGER protect_research_v3_cutover_operation",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("migration 116 is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT SELECT ON research_v3_delivery_authorities TO vane_app",
		"strpos(replace(definition,needle,''),needle)>0",
		"RETURNS TABLE(tenant_id",
		"RETURNS TABLE(user_id",
		"GRANT vane_research_v3_cutover_operator TO vane_server_runtime",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("migration 116 exposes forbidden authority %q", forbidden)
		}
	}
}

func TestMigration116BackfillsHistoricalCutoverJournalPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _ := newMigration116ScratchProvider(t, databaseURL)
	if _, err := provider.UpTo(t.Context(), 115); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO research_v3_delivery_authorities (
		 tenant_id,user_id,task_id,generation,definition_version,
		 definition_digest,target_action_digest,action_authorization_digest,status
		) VALUES (1,1,'historical-task',1,1,$1,$1,$1,'staged')`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO research_v3_cutover_operations (
		 tenant_id,user_id,task_id,idempotency_key,generation,
		 definition_version,definition_digest,frozen_schedule,
		 frozen_schedule_digest,frozen_conflict_token,conflict_token_digest,
		 target_action,target_action_digest,action_authorization_digest,
		 original_paused,phase,original_execution_mode,source_baseline_digest
		) VALUES (
		 1,1,'historical-task','historical-terminal',1,1,$1,decode('01','hex'),$1,
		 decode('02','hex'),$1,decode('03','hex'),$1,$1,false,'prepared','compiled',$1
		)`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 116); err != nil {
		t.Fatalf("upgrade schema 115 with historical journal: %v", err)
	}
	var phase, originalStatus, preflightDigest string
	if err := database.QueryRowContext(t.Context(), `
		SELECT phase,original_schedule_status,preflight_digest
		  FROM research_v3_cutover_operations
		 WHERE task_id='historical-task'`,
	).Scan(&phase, &originalStatus, &preflightDigest); err != nil {
		t.Fatal(err)
	}
	if phase != "prepared" || originalStatus != "active" ||
		len(preflightDigest) != 64 {
		t.Fatalf("backfill phase=%q original_status=%q digest=%q",
			phase, originalStatus, preflightDigest)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE research_v3_cutover_operations
		   SET preflight_digest=$1,phase='pause_requested'
		 WHERE task_id='historical-task'`, strings.Repeat("b", 64)); err == nil ||
		!strings.Contains(err.Error(), "immutable V3 cutover preflight changed") {
		t.Fatalf("migration 116 immutability trigger was not restored: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE research_v3_cutover_operations
		   SET phase='prepared'
		 WHERE task_id='historical-task'`); err == nil ||
		!strings.Contains(err.Error(), "illegal V3 cutover phase transition") {
		t.Fatalf("migration 102 transition trigger was not restored: %v", err)
	}
}

func TestMigration116VaneAppPreparedStatusPrivilegeIsSymmetricPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _ := newMigration116ScratchProvider(t, databaseURL)
	if _, err := provider.UpTo(t.Context(), 116); err != nil {
		t.Fatal(err)
	}
	var granted bool
	if err := database.QueryRowContext(t.Context(), `SELECT has_column_privilege(
		'vane_app','public.research_v3_prepared_definition_heads',
		'prepared_schedule_status','SELECT')`).Scan(&granted); err != nil {
		t.Fatal(err)
	}
	if !granted {
		t.Fatal("vane_app cannot read migration 116 prepared status")
	}
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(t.Context(), `
		SELECT prepared_schedule_status
		  FROM research_v3_prepared_definition_heads
		 LIMIT 0`)
	if err != nil {
		t.Fatalf("vane_app SELECT prepared status: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.DownTo(t.Context(), 115); err != nil {
		t.Fatal(err)
	}
	var residualPrivileges int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*)
		FROM information_schema.column_privileges
		WHERE table_schema='public'
		  AND table_name='research_v3_prepared_definition_heads'
		  AND column_name='prepared_schedule_status'
		  AND grantee='vane_app'`).Scan(&residualPrivileges); err != nil {
		t.Fatal(err)
	}
	if residualPrivileges != 0 {
		t.Fatalf("migration 116 Down retained %d prepared-status grants",
			residualPrivileges)
	}
}

func TestMigration116RejectsDuplicateDynamicFunctionNeedlePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _ := newMigration116ScratchProvider(t, databaseURL)
	if _, err := provider.UpTo(t.Context(), 115); err != nil {
		t.Fatal(err)
	}
	const needle = "AND schedule.status='active'\n           AND head.execution_mode='discover_at_run'"
	var definition string
	if err := database.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'public.admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure)`,
	).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if strings.Count(definition, needle) != 1 {
		t.Fatalf("migration 115 admission needle count=%d",
			strings.Count(definition, needle))
	}
	tampered := strings.Replace(definition, needle,
		needle+"\n/*"+needle+"*/", 1)
	if _, err := database.ExecContext(t.Context(), tampered); err != nil {
		t.Fatalf("install duplicated admission needle: %v", err)
	}
	if _, err := provider.UpTo(t.Context(), 116); err == nil ||
		!strings.Contains(err.Error(), "paused shadow admission patch is ambiguous") {
		t.Fatalf("ambiguous dynamic function migration result: %v", err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 115 {
		t.Fatalf("ambiguous migration changed schema version to %d", version)
	}
}

func TestMigration116DownRejectsDuplicateDynamicFunctionNeedlePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _ := newMigration116ScratchProvider(t, databaseURL)
	if _, err := provider.UpTo(t.Context(), 116); err != nil {
		t.Fatal(err)
	}
	const needle = "AND schedule.status IN ('active','paused')\n           AND head.prepared_schedule_status=schedule.status\n           AND head.execution_mode='discover_at_run'"
	var definition string
	if err := database.QueryRowContext(t.Context(), `SELECT pg_get_functiondef(
		'public.admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure)`,
	).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if strings.Count(definition, needle) != 1 {
		t.Fatalf("migration 116 admission needle count=%d",
			strings.Count(definition, needle))
	}
	tampered := strings.Replace(definition, needle,
		needle+"\n/*"+needle+"*/", 1)
	if _, err := database.ExecContext(t.Context(), tampered); err != nil {
		t.Fatalf("install duplicated rollback needle: %v", err)
	}
	if _, err := provider.DownTo(t.Context(), 115); err == nil ||
		!strings.Contains(err.Error(), "paused shadow admission rollback is ambiguous") {
		t.Fatalf("ambiguous dynamic function rollback result: %v", err)
	}
	var version int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 116 {
		t.Fatalf("ambiguous rollback changed schema version to %d", version)
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

func TestMigration116PausedShadowRunsPlannerToolsEffectAndSynthesisDarkPostgres(t *testing.T) {
	// This fixture creates the paused schedule and paused prepared head before
	// the shadow snapshot. It then persists a receipt-backed Plan, executes all
	// Tool steps, authorizes synthesis through the run-effect capability, and
	// claims the no-Tools synthesis spend. Every admission therefore traverses
	// the real migration-116 PostgreSQL predicates rather than a Go-only fake.
	spend := newPausedResearchShadowRunSpendFixtureV3(t, 1_000_000)
	fixture, synthesis, reservation := preparedPartialResearchBriefV3(t, spend)
	claim, err := fixture.st.ClaimResearchBriefSynthesisV3(
		t.Context(), researchShadowBriefClaimV3(fixture, synthesis, reservation))
	if err != nil || !claim.Claimed ||
		claim.Synthesis.Status != ResearchBriefSynthesisSpendingV3 ||
		claim.ReceiptState != ResearchBriefLLMReceiptPendingV3 {
		t.Fatalf("paused shadow full-chain claim=%+v err=%v", claim, err)
	}
	var status types.ScheduleStatus
	var plans, terminalSteps, briefs, deliveries int
	if err := fixture.st.pool.QueryRow(t.Context(), `SELECT
		(SELECT status FROM schedules
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3),
		(SELECT count(*) FROM research_run_plans
		  WHERE tenant_id=$2 AND user_id=$3 AND task_id=$1),
		(SELECT count(*) FROM research_run_steps
		  WHERE tenant_id=$2 AND user_id=$3 AND task_id=$1 AND phase<>'started'),
		(SELECT count(*) FROM research_brief_syntheses
		  WHERE tenant_id=$2 AND user_id=$3 AND task_id=$1),
		(SELECT count(*) FROM research_brief_deliveries
		  WHERE tenant_id=$2 AND user_id=$3 AND task_id=$1)`,
		fixture.taskID, fixture.tenantID, fixture.userID,
	).Scan(&status, &plans, &terminalSteps, &briefs, &deliveries); err != nil {
		t.Fatal(err)
	}
	if status != types.ScheduleStatusPaused || plans != 1 ||
		terminalSteps != spend.planRef.StepCount || briefs != 1 || deliveries != 0 {
		t.Fatalf("paused shadow chain status=%s plans=%d steps=%d/%d briefs=%d deliveries=%d",
			status, plans, terminalSteps, spend.planRef.StepCount, briefs, deliveries)
	}
}

func TestMigration116DownRejectsEnabledAuthorityAndCutoverAuditPostgres(t *testing.T) {
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
	if _, err := database.ExecContext(t.Context(), `
			INSERT INTO research_v3_delivery_authorities (
			 tenant_id,user_id,task_id,generation,definition_version,
			 definition_digest,target_action_digest,action_authorization_digest,
			 status,enabled_at
			) VALUES (1,1,'task-one',1,1,$1,$1,$1,'enabled',clock_timestamp())`,
		digest); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 113); err == nil ||
		!strings.Contains(err.Error(), "cannot downgrade with enabled V3 authority") {
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
	if _, err := database.ExecContext(t.Context(), `
		UPDATE research_v3_delivery_authorities
		   SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=1 AND user_id=1 AND task_id='task-one'`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO research_v3_cutover_operations (
		 tenant_id,user_id,task_id,idempotency_key,generation,
		 definition_version,definition_digest,frozen_schedule,
		 frozen_schedule_digest,frozen_conflict_token,conflict_token_digest,
		 target_action,target_action_digest,action_authorization_digest,
		 original_paused,original_schedule_status,preflight_digest,phase,
		 original_execution_mode,source_baseline_digest
		) VALUES (
		 1,1,'task-one','cutover-audit',1,1,$1,decode('01','hex'),$1,
		 decode('02','hex'),$1,decode('03','hex'),$1,$1,true,'paused',$1,
		 'rolled_back','compiled',$1
		)`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 113); err == nil ||
		!strings.Contains(err.Error(), "cannot downgrade with V3 cutover audit") {
		t.Fatalf("audit-destroying migration 116 downgrade result: %v", err)
	}
	if err := database.QueryRowContext(t.Context(),
		`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 116 {
		t.Fatalf("audit downgrade changed schema version to %d", version)
	}
}

func newMigration116ScratchProvider(
	t *testing.T, databaseURL string,
) (*sql.DB, *goose.Provider, string) {
	t.Helper()
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
	return database, provider, scratchURL
}
