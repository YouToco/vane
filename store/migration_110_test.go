package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestMigration110ProtocolIsolationAndACLPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 110); err != nil {
		t.Fatal(err)
	}
	var defaultValue, nullable, definition string
	if err := db.QueryRowContext(t.Context(), `
		SELECT column_default,is_nullable FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='task_definition_edit_operations'
		   AND column_name='operation_protocol'`).Scan(&defaultValue, &nullable); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(defaultValue, "1") || nullable != "NO" {
		t.Fatalf("unsafe protocol column default=%q nullable=%q", defaultValue, nullable)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		 WHERE conname='task_definition_edit_operation_protocol_valid'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(definition, "ANY (ARRAY[1, 3])") {
		t.Fatalf("protocol constraint does not retain only legacy/V3: %s", definition)
	}
	var appInsert, appUpdate, legacyInsert, legacyUpdate, nativeInsert, nativeUpdate bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app','research_v3_delivery_authorities','INSERT'),
		       has_table_privilege('vane_app','research_v3_delivery_authorities','UPDATE'),
		       has_column_privilege('vane_edit_coordinator','research_v3_delivery_authorities','generation','INSERT'),
		       has_column_privilege('vane_edit_coordinator','research_v3_delivery_authorities','status','UPDATE'),
		       has_column_privilege('vane_native_v3_edit_coordinator','research_v3_delivery_authorities','generation','INSERT'),
		       has_column_privilege('vane_native_v3_edit_coordinator','research_v3_delivery_authorities','status','UPDATE')`).Scan(
		&appInsert, &appUpdate, &legacyInsert, &legacyUpdate,
		&nativeInsert, &nativeUpdate); err != nil {
		t.Fatal(err)
	}
	if appInsert || appUpdate || legacyInsert || legacyUpdate ||
		!nativeInsert || !nativeUpdate {
		t.Fatalf("native V3 edit ACL differs app=(%v,%v) legacy=(%v,%v) native=(%v,%v)",
			appInsert, appUpdate, legacyInsert, legacyUpdate, nativeInsert, nativeUpdate)
	}
	var canLogin, inherits, bypasses, runtimeMember, appExec bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT rolcanlogin,rolinherit,rolbypassrls,
		       EXISTS (SELECT 1 FROM pg_auth_members edge
		         JOIN pg_roles granted ON granted.oid=edge.roleid
		         JOIN pg_roles member ON member.oid=edge.member
		        WHERE granted.rolname='vane_native_v3_edit_coordinator'
		          AND member.rolname='vane_server_runtime'),
		       has_function_privilege('vane_app',
		         'load_native_research_v3_edit_operation_v1(text,bigint,bigint,text)',
		         'EXECUTE')
		  FROM pg_roles WHERE rolname='vane_native_v3_edit_coordinator'`).Scan(
		&canLogin, &inherits, &bypasses, &runtimeMember, &appExec); err != nil {
		t.Fatal(err)
	}
	if canLogin || inherits || bypasses || runtimeMember || !appExec {
		t.Fatalf("unsafe native role login=%v inherit=%v bypass=%v runtime_member=%v app_exec=%v",
			canLogin, inherits, bypasses, runtimeMember, appExec)
	}
	if _, err := provider.DownTo(t.Context(), 109); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		 WHERE table_schema='public' AND table_name='task_definition_edit_operations'
		   AND column_name='operation_protocol')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("migration 110 downgrade retained protocol discriminator")
	}
}

func TestMigration110ContainsNoRetiredExecutionState(t *testing.T) {
	raw, err := fs.ReadFile(migrationsFS,
		"migrations/110_native_research_task_definition_edit.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(raw))
	for _, required := range []string{
		"operation_protocol", "check (operation_protocol in (1,3))",
		"refusing downgrade while native v3 edits exist",
		"key in ('tool_calls','sources','fetch_targets','source_catalog')",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 110 is missing %q", required)
		}
	}
}

func TestNativeResearchV3EditServerRuntimeLifecycleAndACLPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	owner, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	user, err := owner.UpsertUserByOpenID(t.Context(), "v3-edit-runtime-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := owner.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`, tenantID, user.ID); err != nil {
		t.Fatal(err)
	}
	session, err := owner.CreateAgentSession(t.Context(), user.ID)
	if err != nil {
		t.Fatal(err)
	}

	taskID := "v3-edit-runtime-" + uuid.NewString()
	base := nativeV3EditDefinition110(t, tenantID, user.ID, taskID, "Kimi 套餐", "检查官方购买入口")
	target := nativeV3EditDefinition110(t, tenantID, user.ID, taskID, "Kimi 套餐状态", "检查官方购买入口并对比历史")
	basePayload, _ := taskstate.EncodeApprovedDefinitionV3(base)
	targetPayload, _ := taskstate.EncodeApprovedDefinitionV3(target)
	baseDigest, _ := taskstate.DigestApprovedDefinitionV3(base)
	targetDigest, _ := taskstate.DigestApprovedDefinitionV3(target)
	creationID := "v3-create-" + uuid.NewString()
	token := strings.Repeat("a", 64)
	basePrepared := nativeV3PreparedSchedule110(t, tenantID, user.ID, taskID,
		creationID, baseDigest, token, []byte("base-action"))
	targetPrepared := nativeV3PreparedSchedule110(t, tenantID, user.ID, taskID,
		creationID, targetDigest, token, []byte("target-action"))
	var baseWire, targetWire preparedResearchTaskScheduleV3StoreView
	if json.Unmarshal(basePrepared, &baseWire) != nil || json.Unmarshal(targetPrepared, &targetWire) != nil {
		t.Fatal("decode prepared fixture")
	}
	setupTx, err := owner.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = setupTx.Rollback(context.Background()) }()
	if _, err := setupTx.Exec(t.Context(), `SET CONSTRAINTS ALL DEFERRED`); err != nil {
		t.Fatal(err)
	}
	if _, err := setupTx.Exec(t.Context(), `
		INSERT INTO schedules(id,tenant_id,user_id,nl_description,spec_json,scope_json,
		 status,push_strictness,execution_mode,approved_definition_version,
		 approved_definition_digest)
		VALUES($1,$2,$3,$4,$5::jsonb,'{}','active','strict','compiled',NULL,NULL)`,
		taskID, tenantID, user.ID, base.TaskName, string(base.SpecJSON)); err != nil {
		t.Fatal(err)
	}
	if _, err := setupTx.Exec(t.Context(),
		`INSERT INTO schedule_playbooks(schedule_id,content,fetch_plan) VALUES($1,$2,'{}')`,
		taskID, base.TaskManual); err != nil {
		t.Fatal(err)
	}
	if _, err := setupTx.Exec(t.Context(), `
		INSERT INTO task_approved_definition_versions
		 (tenant_id,user_id,task_id,version,schema_version,execution_mode,
		  definition_digest,payload,operation_ref)
		VALUES($1,$2,$3,1,$4,'discover_at_run',$5,$6,$7)`, tenantID, user.ID,
		taskID, taskstate.ApprovedDefinitionSchemaVersionV3, baseDigest,
		basePayload, "creation-v3/"+creationID); err != nil {
		t.Fatal(err)
	}
	if _, err := setupTx.Exec(t.Context(), `UPDATE schedules SET execution_mode='discover_at_run',
		approved_definition_version=1,approved_definition_digest=$2 WHERE id=$1`,
		taskID, baseDigest); err != nil {
		t.Fatal(err)
	}
	if err := setupTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.pool.Exec(t.Context(), `
		INSERT INTO research_v3_delivery_authorities
		 (tenant_id,user_id,task_id,generation,definition_version,definition_digest,
		  target_action_digest,action_authorization_digest,status,enabled_at)
		VALUES($1,$2,$3,1,1,$4,$5,$6,'enabled',clock_timestamp())`, tenantID,
		user.ID, taskID, baseDigest, baseWire.TargetActionDigest,
		baseWire.ActionAuthorizationDigest); err != nil {
		t.Fatal(err)
	}
	createOp, err := owner.CreateResearchTaskCreationOperationV3(t.Context(),
		types.CreateResearchTaskCreationOperationV3Params{ID: creationID,
			TenantID: tenantID, UserID: user.ID, Args: basePayload,
			Summary: base.TaskName, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.pool.Exec(t.Context(), `
		UPDATE task_creation_operations SET status='executed',phase='completed',
		 compiled_definition=$2,compiled_digest=$3,prepared_schedule=$4,
		 task_id=$5,executed_at=clock_timestamp(),tombstoned_at=clock_timestamp()
		WHERE id=$1`, createOp.ID, basePayload, baseDigest, basePrepared, taskID); err != nil {
		t.Fatal(err)
	}

	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = DeprovisionServerRuntime(ctx, scratchURL)
	})
	const password = "native-v3-edit-runtime-password"
	if _, err := owner.pool.Exec(t.Context(),
		`ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = owner.pool.Exec(context.Background(),
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtime, err := NewServerRuntime(t.Context(), serverRuntimeTestURL(t, scratchURL, password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)

	editID := "v3-edit-" + uuid.NewString()
	prepared := preparedResearchTaskEditV3StoreView{WireVersion: preparedResearchTaskEditV3StoreWire,
		OperationID: editID, OriginalState: "active", BaseDefinitionVersion: 1,
		BaseDefinitionDigest: baseDigest, TargetDefinitionVersion: 2,
		TargetDefinitionDigest: targetDigest, Base: basePrepared, Target: targetPrepared}
	withoutDigest, _ := json.Marshal(prepared)
	prepared.RequestDigest = nativeV3Hex110(withoutDigest)
	preparedBytes, _ := json.Marshal(prepared)
	baseSnapshot, _ := json.Marshal(researchTaskEditSnapshotV3StoreView{
		WireVersion: researchTaskEditSnapshotV3StoreWire, Phase: "base_original",
		TaskID: taskID, DefinitionDigest: baseDigest, Revision: "base-r1", Paused: false})
	params := types.CreateResearchTaskDefinitionEditOperationV3Params{ID: editID,
		TenantID: tenantID, UserID: user.ID, TaskID: taskID, SessionID: session.ID,
		ExpiresAt: time.Now().Add(time.Hour), BaseVersion: 1, BaseDefinition: basePayload,
		TargetVersion: 2, TargetDefinition: targetPayload, PreparedEdit: preparedBytes,
		BaseSnapshot: baseSnapshot}
	probeTx, err := runtime.beginResearchTaskDefinitionEditTxV3(t.Context(), tenantID, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = probeTx.Rollback(context.Background()) }()
	var probeCount int
	if err := probeTx.QueryRow(t.Context(),
		`SELECT count(*) FROM load_native_research_v3_edit_basis_v1($1,$2,$3)`,
		tenantID, user.ID, taskID).Scan(&probeCount); err != nil {
		t.Fatalf("raw basis probe: %#v", err)
	}
	if probeCount != 1 {
		t.Fatalf("native V3 basis rows=%d, want 1", probeCount)
	}
	_ = probeTx.Rollback(t.Context())
	op, err := runtime.CreateResearchTaskDefinitionEditOperationV3(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	leaseOwner := "runtime-lifecycle"
	op, err = runtime.AcquireResearchTaskDefinitionEditOperationV3(t.Context(),
		types.AcquireTaskDefinitionEditOperationParams{Scope: op.Scope(), LeaseOwner: leaseOwner,
			LeaseDuration: time.Hour, ReceiptProvider: "feishu", ReceiptTarget: "chat-1"})
	if err != nil {
		t.Fatal(err)
	}
	lease := op.Lease()
	if err := runtime.QuiesceResearchTaskDefinitionEditV3(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AuthorizeResearchTaskDefinitionEditRemotePhaseV3(t.Context(), lease,
		types.TaskDefinitionEditPhaseDBQuiesced); err != nil {
		t.Fatal(err)
	}
	basePaused := nativeV3Snapshot110(t, "base_paused", taskID, baseDigest, true)
	if err := runtime.CheckpointResearchTaskDefinitionEditBasePausedV3(t.Context(), lease, basePaused); err != nil {
		t.Fatal(err)
	}
	if err := runtime.CommitResearchTaskDefinitionEditDefinitionV3(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AuthorizeResearchTaskDefinitionEditRemotePhaseV3(t.Context(), lease,
		types.TaskDefinitionEditPhaseDefinitionCommitted); err != nil {
		t.Fatal(err)
	}
	targetPaused := nativeV3Snapshot110(t, "target_paused", taskID, targetDigest, true)
	if err := runtime.CheckpointResearchTaskDefinitionEditTargetAppliedV3(t.Context(), lease, targetPaused); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AuthorizeResearchTaskDefinitionEditRemotePhaseV3(t.Context(), lease,
		types.TaskDefinitionEditPhaseTemporalTargetApplied); err != nil {
		t.Fatal(err)
	}
	targetFinal := nativeV3Snapshot110(t, "target_final", taskID, targetDigest, false)
	if err := runtime.CheckpointResearchTaskDefinitionEditTargetRestoredV3(t.Context(), lease, targetFinal); err != nil {
		t.Fatal(err)
	}
	if err := runtime.CompleteResearchTaskDefinitionEditOperationV3(t.Context(), lease,
		json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	completed, err := runtime.LoadResearchTaskDefinitionEditOperationV3(t.Context(), op.Scope())
	if err != nil || completed.Status != types.TaskDefinitionEditOperationStatusCompleted {
		t.Fatalf("completed=%+v err=%v", completed, err)
	}

	conn, err := runtime.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(t.Context(), `RESET ROLE; SET ROLE vane_native_v3_edit_coordinator`); err == nil {
		t.Fatal("server runtime entered native V3 coordinator role")
	}
	if _, err := conn.Exec(t.Context(), `RESET ROLE; SET ROLE vane_edit_coordinator`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `UPDATE research_v3_delivery_authorities SET status='revoked' WHERE task_id=$1`, taskID); err == nil {
		t.Fatal("legacy coordinator mutated native V3 authority")
	}
	if _, err := conn.Exec(t.Context(), `INSERT INTO research_v3_delivery_authorities
		(tenant_id,user_id,task_id,generation,definition_version,definition_digest,
		 target_action_digest,action_authorization_digest,status)
		VALUES($1,$2,'forged-task',99,99,$3,$3,$3,'staged')`,
		tenantID, user.ID, strings.Repeat("f", 64)); err == nil {
		t.Fatal("legacy coordinator inserted a forged native V3 authority")
	}
	var visible int
	if err := conn.QueryRow(t.Context(), `SELECT count(*) FROM task_definition_edit_operations WHERE operation_protocol=3`).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("legacy coordinator observed %d protocol-3 rows", visible)
	}
}

func nativeV3EditDefinition110(t *testing.T, tenantID, userID int64, taskID, name, manual string) taskstate.ApprovedDefinitionV3 {
	t.Helper()
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID, TaskName: name,
		TaskManual: manual, SpecJSON: json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 9 * * 1"}`),
		ExecutionMode:  types.ExecutionModeDiscoverAtRun,
		Notification:   taskstate.NotificationPolicyV3{MinimumSignificance: taskstate.NotificationThresholdMajorV3, SuppressEmpty: true},
		Output:         taskstate.OutputPreferenceV3{Language: taskstate.OutputLanguageZhCNV3, Format: taskstate.OutputFormatExecutiveBriefV3, IncludeEvidenceLinks: true},
		PlannerBudget:  types.PlannerBudget{MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768, MaxCostMicroUSD: 1000000, DurationMs: 300000},
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu, TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func nativeV3PreparedSchedule110(t *testing.T, tenantID, userID int64, taskID, operationID, digest, token string, action []byte) []byte {
	t.Helper()
	schedule, _ := json.Marshal(preparedResearchTaskScheduleIdentityV3StoreView{TaskID: taskID,
		TenantID: tenantID, UserID: userID, OperationID: operationID, PreparedDigest: digest})
	var wire preparedResearchTaskScheduleV3StoreView
	wire.WireVersion, wire.Schedule = preparedResearchTaskScheduleV3StoreWire, schedule
	wire.Input.TenantID, wire.Input.UserID, wire.Input.TaskID = tenantID, userID, taskID
	wire.Input.ActionAuthorizationToken = token
	wire.TargetAction = action
	wire.TargetActionDigest = nativeV3Hex110(action)
	wire.ActionAuthorizationDigest = nativeV3Hex110([]byte(token))
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func nativeV3Snapshot110(t *testing.T, phase, taskID, digest string, paused bool) []byte {
	t.Helper()
	raw, err := json.Marshal(researchTaskEditSnapshotV3StoreView{WireVersion: researchTaskEditSnapshotV3StoreWire,
		Phase: phase, TaskID: taskID, DefinitionDigest: digest, Revision: phase + "-r1", Paused: paused})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func nativeV3Hex110(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
