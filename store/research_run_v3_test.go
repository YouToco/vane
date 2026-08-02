package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func researchProviderCallV3ForTest(traceID string, costMicroUSD int64) ResearchProviderCallV3 {
	status := 200
	return ResearchProviderCallV3{
		TraceID: traceID, Provider: "exa", UsageQuantity: 10,
		QuotaUnits: researchRunQuotaUnitsV3, HTTPStatus: &status, DurationMS: 25,
		Attempted: true, CostKnown: true, CostMicroUSD: costMicroUSD,
		PricingStatus: "provider_reported", CostCurrency: "USD",
	}
}

func researchExecutionTraceV3ForTest(
	t *testing.T,
	identity types.RunIdentity,
	snapshotID int64,
	planRef types.ResearchRunPlanRefV3,
	ordinal int,
	invocationID string,
) string {
	t.Helper()
	traceID, err := runcontext.ResearchExecutionTraceV3(
		identity, snapshotID, planRef.PlanDigest, ordinal, invocationID)
	if err != nil {
		t.Fatal(err)
	}
	return traceID
}

// V3 production code requires a separately authenticated non-owner pool. These
// ledger integration tests intentionally inject the schema-owner transaction
// factory because they exercise row/trigger semantics, while constructor and
// authority validation are covered independently in research_runtime_pool_test.
func useOwnerResearchRuntimeForTest(st *Store) {
	st.beginResearchTx = st.pool.BeginTx
	st.beginGatewayTx = st.pool.BeginTx
}

func TestResearchRunV3PlanAndStepLedgerPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required for V3 research ledger integration test")
	}
	st := tenantTestStore(t)
	useOwnerResearchRuntimeForTest(st)
	ctx := t.Context()
	userID := testUser(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants (status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if err := st.SeedTenantQuota(ctx, tenantID); err != nil {
		t.Fatal(err)
	}
	taskID := "research-v3-" + uuid.NewString()
	workflowID, runID := "workflow-"+taskID, "run-"+uuid.NewString()
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules (
		     id,tenant_id,user_id,nl_description,spec_json,scope_json,status,
		     push_strictness
		 ) VALUES ($1,$2,$3,'Kimi pricing','{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}',
		           '{}','active','strict')`,
		taskID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		TaskName: "Kimi pricing", TaskManual: "检查 Kimi 官方套餐并交叉核验；没有重大更新不推送。",
		SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3, SuppressEmpty: true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language: taskstate.OutputLanguageZhCNV3, Format: taskstate.OutputFormatExecutiveBriefV3,
			IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768,
			MaxCostMicroUSD: 1_000_000, DurationMs: 300_000,
		},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	definitionPayload, _ := taskstate.EncodeApprovedDefinitionV3(definition)
	digestA, _ := taskstate.DigestApprovedDefinitionV3(definition)
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO task_approved_definition_versions (
		     tenant_id,user_id,task_id,version,schema_version,execution_mode,
		     definition_digest,payload,operation_ref
		 ) VALUES ($1,$2,$3,1,$4,'discover_at_run',$5,$6,$7)`,
		tenantID, userID, taskID, taskstate.ApprovedDefinitionSchemaVersionV3,
		digestA, definitionPayload, "test-v3:"+uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET execution_mode='discover_at_run',
		     approved_definition_version=1,approved_definition_digest=$2
		 WHERE id=$1`, taskID, digestA); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		if _, err := st.PurgeTenant(cleanupCtx, tenantID, false); err != nil {
			t.Errorf("purge V3 research fixture tenant: %v", err)
			return
		}
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id=$1`, userID)
	})

	identity := types.RunIdentity{
		TemporalWorkflowID: workflowID, TemporalRunID: runID,
		RunKind:  types.RunSnapshotKindScheduled,
		TenantID: tenantID, UserID: userID, TaskID: taskID,
	}
	snapshotRef, err := st.CreateOrGetResearchRunSnapshotV3(
		ctx, identity, testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		testResearchModelPolicyStoreV3(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshotRef.ValidateFor(identity); err != nil ||
		snapshotRef.DefinitionDigest != digestA ||
		snapshotRef.PlannerBudget != definition.PlannerBudget {
		t.Fatalf("snapshot ref=%+v err=%v", snapshotRef, err)
	}
	loadedSnapshot, err := st.LoadResearchRunSnapshotV3(ctx, identity, snapshotRef)
	if err != nil || loadedSnapshot.Payload.Definition.TaskManual != definition.TaskManual ||
		loadedSnapshot.Payload.Definition.TaskName != definition.TaskName ||
		loadedSnapshot.Payload.DefinitionDigest != digestA {
		t.Fatalf("loaded V3 snapshot=%+v err=%v", loadedSnapshot.Payload, err)
	}
	tamperedSnapshotRef := snapshotRef
	tamperedSnapshotRef.PayloadDigest = strings.Repeat("b", 64)
	if _, err := st.LoadResearchRunSnapshotV3(ctx, identity, tamperedSnapshotRef); err == nil {
		t.Fatal("tampered V3 snapshot reference loaded")
	}
	mismatchIdentity := identity
	mismatchIdentity.TemporalRunID = "run-" + uuid.NewString()
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET spec_json='{"cron":"30 9 * * 1","tz":"Asia/Shanghai"}'
		  WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateOrGetResearchRunSnapshotV3(
		ctx, mismatchIdentity, testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		testResearchModelPolicyStoreV3(t)); err == nil {
		t.Fatal("definition schedule drift created a V3 snapshot")
	}
	if _, err := st.pool.Exec(ctx, `UPDATE schedules SET spec_json=$2 WHERE id=$1`,
		taskID, definition.SpecJSON); err != nil {
		t.Fatal(err)
	}
	// Response-loss recovery reads the committed snapshot before consulting a
	// changed/invalid worker policy.
	replayedSnapshot, err := st.CreateOrGetResearchRunSnapshotV3(
		ctx, identity, runtimepolicy.BundleV1{}, runtimepolicy.ResearchToolPolicyV3{},
		runtimepolicy.ResearchModelPolicyV3{})
	if err != nil || replayedSnapshot != snapshotRef {
		t.Fatalf("snapshot first-writer replay=%+v err=%v", replayedSnapshot, err)
	}
	snapshotID := snapshotRef.SnapshotID
	plan := researchRunPlanFixtureV3(t, digestA,
		snapshotRef.CapabilityCatalogDigest, snapshotRef.ToolPolicyDigest, "Kimi pricing")
	if recovered, found, err := st.LoadResearchRunPlanRefV3(ctx, identity, snapshotRef); err != nil || found || recovered != (types.ResearchRunPlanRefV3{}) {
		t.Fatalf("empty plan recovery=%+v found=%v err=%v", recovered, found, err)
	}
	// Store and trigger both reject a plan bound to a non-V3 snapshot schema.
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_run_snapshots SET reference_schema_version='vane.run-snapshot-ref/v2'
		  WHERE id=$1`, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateOrGetResearchRunPlanV3(ctx, CreateOrGetResearchRunPlanV3Params{
		Identity: identity, RunSnapshotID: snapshotID, Plan: plan,
	}); err == nil {
		t.Fatal("non-V3 snapshot created a V3 plan")
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_run_snapshots SET reference_schema_version=$2 WHERE id=$1`,
		snapshotID, types.ResearchRunSnapshotRefSchemaV3); err != nil {
		t.Fatal(err)
	}
	driftedPlan := plan
	driftedPlan.ToolPolicyDigest = strings.Repeat("c", 64)
	driftedPayload, err := runcontext.EncodeResearchExecutionPlanV3(driftedPlan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO research_run_plans (
		     tenant_id,user_id,task_id,run_snapshot_id,temporal_workflow_id,
		     temporal_run_id,definition_digest,capability_catalog_digest,
		     plan_digest,plan_payload,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		tenantID, userID, taskID, snapshotID, workflowID, runID,
		digestA, snapshotRef.CapabilityCatalogDigest,
		researchRunSHA256(driftedPayload), driftedPayload, researchRunPlanSchemaV3,
	); err == nil {
		t.Fatal("database admitted a plan bound to a different Tool policy")
	}
	ref, _ := createResearchPlanFromReceiptV3(t, st, identity, snapshotRef, plan)
	if recovered, found, err := st.LoadResearchRunPlanRefV3(ctx, identity, snapshotRef); err != nil || !found || recovered != ref {
		t.Fatalf("sealed plan recovery=%+v found=%v err=%v", recovered, found, err)
	}
	if err := ref.ValidateFor(identity, snapshotID); err != nil {
		t.Fatal(err)
	}
	otherUserID := testUser(t, st)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id=$1`, otherUserID)
	})
	rlsTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rlsTx.Rollback(ctx)
	if _, err := rlsTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(otherUserID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := rlsTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var foreignVisible int
	if err := rlsTx.QueryRow(ctx,
		`SELECT count(*) FROM research_run_plans WHERE tenant_id=$1`, tenantID,
	).Scan(&foreignVisible); err != nil {
		t.Fatal(err)
	}
	if foreignVisible != 0 {
		t.Fatalf("cross-user RLS exposed %d plans", foreignVisible)
	}
	if _, err := rlsTx.Exec(ctx,
		`INSERT INTO research_run_steps (
		     tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
		     step_ordinal,phase,invocation_id,tool_name,request_digest,
		     result_digest,cost_micro_usd,error_code,schema_version)
		 VALUES ($1,$2,$3,$4,$5,$6,1,'started','cross-user','web_search',$7,
		         NULL,0,NULL,'vane.research-run-step/v3')`,
		tenantID, userID, taskID, ref.PlanID, identity.TemporalRunID,
		ref.PlanDigest, strings.Repeat("f", 64)); err == nil {
		t.Fatal("cross-user RLS admitted a step INSERT")
	}
	if err := rlsTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	crossTenantTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer crossTenantTx.Rollback(ctx)
	if _, err := crossTenantTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID+1, 10), strconv.FormatInt(userID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := crossTenantTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if err := crossTenantTx.QueryRow(ctx,
		`SELECT count(*) FROM research_run_plans WHERE id=$1`, ref.PlanID,
	).Scan(&foreignVisible); err != nil {
		t.Fatal(err)
	}
	if foreignVisible != 0 {
		t.Fatalf("cross-tenant RLS exposed %d plans", foreignVisible)
	}
	if err := crossTenantTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var planUpdate, planDelete, planTruncate, stepUpdate, stepDelete, stepTruncate bool
	var evidenceUpdate, evidenceDelete, evidenceTruncate bool
	if err := st.pool.QueryRow(ctx,
		`SELECT has_table_privilege('vane_app','research_run_plans','UPDATE'),
		        has_table_privilege('vane_app','research_run_plans','DELETE'),
		        has_table_privilege('vane_app','research_run_plans','TRUNCATE'),
		        has_table_privilege('vane_app','research_run_steps','UPDATE'),
		        has_table_privilege('vane_app','research_run_steps','DELETE'),
		        has_table_privilege('vane_app','research_run_steps','TRUNCATE'),
		        has_table_privilege('vane_app','research_run_evidence','UPDATE'),
		        has_table_privilege('vane_app','research_run_evidence','DELETE'),
		        has_table_privilege('vane_app','research_run_evidence','TRUNCATE')`,
	).Scan(&planUpdate, &planDelete, &planTruncate, &stepUpdate, &stepDelete, &stepTruncate,
		&evidenceUpdate, &evidenceDelete, &evidenceTruncate); err != nil {
		t.Fatal(err)
	}
	if planUpdate || planDelete || planTruncate || stepUpdate || stepDelete || stepTruncate ||
		evidenceUpdate || evidenceDelete || evidenceTruncate {
		t.Fatalf("append-only grants drifted: plan=%v/%v/%v step=%v/%v/%v evidence=%v/%v/%v",
			planUpdate, planDelete, planTruncate, stepUpdate, stepDelete, stepTruncate,
			evidenceUpdate, evidenceDelete, evidenceTruncate)
	}
	var publicExecute bool
	var functionOwner string
	var functionConfig []string
	if err := st.pool.QueryRow(ctx,
		`SELECT has_function_privilege('public',p.oid,'EXECUTE'),
		        p.proowner::regrole::text,p.proconfig
		   FROM pg_proc p
		  WHERE p.oid='task_run_snapshot_v3_admission_fence()'::regprocedure`,
	).Scan(&publicExecute, &functionOwner, &functionConfig); err != nil {
		t.Fatal(err)
	}
	if publicExecute || functionOwner == "vane_app" ||
		len(functionConfig) != 1 || functionConfig[0] != "search_path=pg_catalog, public, pg_temp" {
		t.Fatalf("V3 admission function security drifted: public=%v owner=%q config=%v",
			publicExecute, functionOwner, functionConfig)
	}
	// Response-lost retry returns the first writer before consulting a missing
	// or no-longer-valid current planner candidate.
	replayed, err := st.CreateOrGetResearchRunPlanV3(ctx, CreateOrGetResearchRunPlanV3Params{
		Identity: identity, RunSnapshotID: snapshotID,
	})
	if err != nil || replayed != ref {
		t.Fatalf("first writer replay ref=%+v err=%v want=%+v", replayed, err, ref)
	}

	started, err := st.BeginResearchRunStepV3(ctx, identity, snapshotID, ref, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !started.FirstWriter || started.ToolName != "web_search" ||
		string(started.Arguments) != `{"query":"Kimi pricing"}` ||
		!validSHA256Digest(started.RequestDigest) {
		t.Fatalf("started=%+v", started)
	}
	replayedStart, err := st.BeginResearchRunStepV3(ctx, identity, snapshotID, ref, 0)
	if err != nil || replayedStart.StepID != started.StepID || replayedStart.FirstWriter ||
		len(replayedStart.Arguments) != 0 {
		t.Fatalf("start replay=%+v err=%v", replayedStart, err)
	}
	startedResolution, err := st.LoadResearchRunStepResolutionV3(
		ctx, identity, snapshotID, ref, 0)
	if err != nil || startedResolution.Phase != ResearchRunStepStartedV3 ||
		startedResolution.Evidence != nil {
		t.Fatalf("started resolution=%+v err=%v", startedResolution, err)
	}
	stepRLSTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stepRLSTx.Rollback(ctx)
	if _, err := stepRLSTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(otherUserID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := stepRLSTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if err := stepRLSTx.QueryRow(ctx,
		`SELECT count(*) FROM research_run_steps WHERE id=$1`, started.StepID,
	).Scan(&foreignVisible); err != nil {
		t.Fatal(err)
	}
	if foreignVisible != 0 {
		t.Fatalf("cross-user RLS exposed %d steps", foreignVisible)
	}
	if err := stepRLSTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitResearchRunStepEvidenceV3(ctx, CommitResearchRunStepEvidenceV3Params{
		Identity: identity, RunSnapshotID: snapshotID, PlanRef: ref, Ordinal: 1,
		Result: []byte("result without start"), OriginalSize: 20, TrustType: "external",
	}); err == nil {
		t.Fatal("evidence without an immutable start passed")
	}
	if _, err := st.CommitResearchRunStepEvidenceV3(ctx, CommitResearchRunStepEvidenceV3Params{
		Identity: identity, RunSnapshotID: snapshotID, PlanRef: ref, Ordinal: 0,
		Result: []byte{0xff}, OriginalSize: 1, TrustType: "external",
	}); err == nil {
		t.Fatal("invalid UTF-8 evidence passed")
	}
	// The deferred inverse fence prevents an evidence-only transaction.
	orphanResult := []byte(`{"orphan":true}`)
	orphanTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = orphanTx.Exec(ctx,
		`INSERT INTO research_run_evidence (
		     tenant_id,user_id,task_id,plan_id,started_step_id,temporal_run_id,
		     plan_digest,step_ordinal,invocation_id,tool_name,request_digest,
		     result_bytes,result_digest,original_size,truncated,trust_type,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,0,$8,$9,$10,$11,$12,$13,false,'external',$14)`,
		tenantID, userID, taskID, ref.PlanID, started.StepID,
		identity.TemporalRunID, ref.PlanDigest, started.InvocationID,
		started.ToolName, started.RequestDigest, orphanResult,
		researchRunSHA256(orphanResult), len(orphanResult), researchRunEvidenceSchemaV3)
	if err != nil {
		_ = orphanTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := orphanTx.Commit(ctx); err == nil {
		t.Fatal("evidence-only transaction committed")
	}
	// Even an owner-side raw completion cannot bypass the exact-evidence fence.
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO research_run_steps (
		     tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
		     step_ordinal,phase,invocation_id,tool_name,request_digest,
		     result_digest,cost_micro_usd,error_code,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,0,'completed',$7,$8,$9,$10,21,NULL,$11)`,
		tenantID, userID, taskID, ref.PlanID, identity.TemporalRunID, ref.PlanDigest,
		started.InvocationID, started.ToolName, started.RequestDigest,
		strings.Repeat("c", 64), researchRunStepSchemaV3); err == nil {
		t.Fatal("raw completed step without exact evidence passed")
	}
	visibleResult := []byte(`{"status":"available","source":"official"}`)
	providerTrace := researchExecutionTraceV3ForTest(
		t, identity, snapshotID, ref, 0, started.InvocationID)
	receipt, err := st.CommitResearchRunStepEvidenceV3(ctx, CommitResearchRunStepEvidenceV3Params{
		Identity: identity, RunSnapshotID: snapshotID, PlanRef: ref, Ordinal: 0,
		Result: visibleResult, OriginalSize: len(visibleResult) + 99,
		TrustType: "external", CostMicroUSD: 21,
		ProviderCall: researchProviderCallV3ForTest(providerTrace, 21),
	})
	if err != nil {
		t.Fatal(err)
	}
	replayedReceipt, err := st.CommitResearchRunStepEvidenceV3(ctx, CommitResearchRunStepEvidenceV3Params{
		Identity: identity, RunSnapshotID: snapshotID, PlanRef: ref, Ordinal: 0,
		Result: visibleResult, OriginalSize: len(visibleResult) + 99,
		TrustType: "external", CostMicroUSD: 21,
		ProviderCall: researchProviderCallV3ForTest(providerTrace, 21),
	})
	if err != nil || replayedReceipt != receipt {
		t.Fatalf("receipt replay=%+v err=%v want=%+v", replayedReceipt, err, receipt)
	}
	if _, err := st.CommitResearchRunStepEvidenceV3(ctx, CommitResearchRunStepEvidenceV3Params{
		Identity: identity, RunSnapshotID: snapshotID, PlanRef: ref, Ordinal: 0,
		Result: []byte(`{"status":"different"}`), OriginalSize: 22,
		TrustType: "external", CostMicroUSD: 21,
		ProviderCall: researchProviderCallV3ForTest(providerTrace, 21),
	}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("conflicting receipt err=%v", err)
	}
	if receipt.EvidenceID <= 0 || !receipt.Truncated ||
		receipt.OriginalSize != len(visibleResult)+99 || receipt.TrustType != "external" {
		t.Fatalf("evidence receipt=%+v", receipt)
	}
	completedResolution, err := st.LoadResearchRunStepResolutionV3(
		ctx, identity, snapshotID, ref, 0)
	if err != nil || completedResolution.Phase != ResearchRunStepCompletedV3 ||
		completedResolution.Evidence == nil ||
		!bytes.Equal(completedResolution.Evidence.Result, visibleResult) ||
		completedResolution.Evidence.EvidenceID != receipt.EvidenceID {
		t.Fatalf("completed resolution=%+v err=%v", completedResolution, err)
	}
	evidenceRLSTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer evidenceRLSTx.Rollback(ctx)
	if _, err := evidenceRLSTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(otherUserID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := evidenceRLSTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if err := evidenceRLSTx.QueryRow(ctx,
		`SELECT count(*) FROM research_run_evidence WHERE id=$1`, receipt.EvidenceID,
	).Scan(&foreignVisible); err != nil {
		t.Fatal(err)
	}
	if foreignVisible != 0 {
		t.Fatalf("cross-user RLS exposed %d evidence rows", foreignVisible)
	}
	if err := evidenceRLSTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	foreignIdentity := identity
	foreignIdentity.UserID++
	if _, err := st.BeginResearchRunStepV3(ctx, foreignIdentity, snapshotID, ref, 1); err == nil {
		t.Fatal("cross-user plan reference passed")
	}
	if _, err := st.pool.Exec(ctx, `UPDATE schedules SET status='paused' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	pausedRecovery, err := st.BeginResearchRunStepV3(ctx, identity, snapshotID, ref, 0)
	if err != nil || pausedRecovery.FirstWriter || pausedRecovery.StepID != started.StepID ||
		len(pausedRecovery.Arguments) != 0 {
		t.Fatalf("paused response-loss recovery=%+v err=%v", pausedRecovery, err)
	}
	if _, err := st.BeginResearchRunStepV3(ctx, identity, snapshotID, ref, 1); err == nil {
		t.Fatal("paused task admitted a new external step")
	}
	command, err := st.CreateOrLoadScheduleCommand(
		ctx, tenantID, userID, taskID, "research-v3-manual-"+uuid.NewString(),
		types.ScheduleCommandRun, strings.Repeat("d", 64), strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedule_commands SET status=$2,phase=$3,completed_at=clock_timestamp()
		  WHERE id=$1`, command.ID, types.ScheduleCommandCompleted,
		types.ScheduleCommandCompletedPhase); err != nil {
		t.Fatal(err)
	}
	manualIdentity := types.RunIdentity{
		TemporalWorkflowID: types.ManualTaskWorkflowPrefix + command.ID,
		TemporalRunID:      "run-" + uuid.NewString(), RunKind: types.RunSnapshotKindScheduled,
		TenantID: tenantID, UserID: userID, TaskID: taskID,
	}
	manualSnapshot, err := st.CreateOrGetResearchRunSnapshotV3(
		ctx, manualIdentity, testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		testResearchModelPolicyStoreV3(t))
	if err != nil {
		t.Fatalf("paused owner manual snapshot: %v", err)
	}
	manualPlan := researchRunPlanFixtureV3(t, digestA,
		manualSnapshot.CapabilityCatalogDigest, manualSnapshot.ToolPolicyDigest, "manual Kimi pricing")
	manualRef, _ := createResearchPlanFromReceiptV3(
		t, st, manualIdentity, manualSnapshot, manualPlan)
	if manualStart, err := st.BeginResearchRunStepV3(
		ctx, manualIdentity, manualSnapshot.SnapshotID, manualRef, 0,
	); err != nil || !manualStart.FirstWriter {
		t.Fatalf("paused owner manual start=%+v err=%v", manualStart, err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedule_commands SET status=$2,phase=$3,error_code='blocked_test',
		        error_message='blocked by test',completed_at=clock_timestamp()
		  WHERE id=$1`, command.ID, types.ScheduleCommandBlocked,
		types.ScheduleCommandBlockedPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := st.BeginResearchRunStepV3(
		ctx, manualIdentity, manualSnapshot.SnapshotID, manualRef, 1,
	); err == nil {
		t.Fatal("blocked manual command admitted another external step")
	}
	var starts, terminals, evidenceRows int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE phase='started'),
		        count(*) FILTER (WHERE phase<>'started')
		   FROM research_run_steps WHERE plan_id=$1`, ref.PlanID).Scan(&starts, &terminals); err != nil {
		t.Fatal(err)
	}
	if starts != 1 || terminals != 1 {
		t.Fatalf("step rows started=%d terminal=%d", starts, terminals)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM research_run_evidence WHERE plan_id=$1`, ref.PlanID,
	).Scan(&evidenceRows); err != nil {
		t.Fatal(err)
	}
	if evidenceRows != 1 {
		t.Fatalf("evidence rows=%d", evidenceRows)
	}
}

func researchRunPlanFixtureV3(
	t *testing.T, definitionDigest, catalogDigest, toolPolicyDigest, query string,
) runcontext.ResearchExecutionPlanV3 {
	t.Helper()
	arguments, _ := json.Marshal(map[string]any{"query": query})
	plan, err := runcontext.BuildResearchExecutionPlanV3(
		definitionDigest, catalogDigest, toolPolicyDigest,
		[]runcontext.ResearchPlanStepV3{
			{InvocationID: "search-official", ToolName: "web_search", Arguments: arguments},
			{InvocationID: "read-official", ToolName: "web_contents", Arguments: json.RawMessage(`{"page_url":"https://www.kimi.com/membership/pricing"}`)},
		},
		func(_ string, raw json.RawMessage) (json.RawMessage, error) { return raw, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testResearchToolPolicyStoreV3(t *testing.T) runtimepolicy.ResearchToolPolicyV3 {
	t.Helper()
	tools := make([]runtimepolicy.ResearchToolDefinitionV3, 0, 2)
	for _, item := range []struct {
		name           string
		implementation runtimepolicy.ResearchToolImplementationV3
	}{
		{"web_search", runtimepolicy.ResearchToolExaSearchV3},
		{"web_contents", runtimepolicy.ResearchToolExaContentsV3},
	} {
		tools = append(tools, runtimepolicy.ResearchToolDefinitionV3{
			Name: item.name, Description: "Test scheduled public read",
			Parameters:     json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}`),
			Implementation: item.implementation, ImplementationGeneration: 1,
			Provider: "exa", Effects: []runtimepolicy.ResearchToolEffectV3{
				runtimepolicy.ResearchToolEffectBillableV3,
				runtimepolicy.ResearchToolEffectNetworkReadV3,
				runtimepolicy.ResearchToolEffectTrustTaintV3,
			}, ResultTrust: runtimepolicy.ResearchToolTrustExternalV3,
			BudgetBucket: "exa_calls", CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
			}, MaxCostMicroUSD: 10_000,
		})
	}
	policy, err := runtimepolicy.BuildResearchToolPolicyV3(tools)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func testResearchModelPolicyStoreV3(t *testing.T) runtimepolicy.ResearchModelPolicyV3 {
	t.Helper()
	policy, err := runtimepolicy.BuildResearchModelPolicyV3(runtimepolicy.ResearchModelPolicyV3{
		Provider: runtimepolicy.ModelProviderDeepSeekV1,
		Endpoint: runtimepolicy.EndpointRefV1{
			ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 1,
		},
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 1,
		},
		Planner: runtimepolicy.ResearchModelStageV3{
			Stage: runtimepolicy.ResearchModelStagePlannerV3, Model: "strong-model",
			MaxTokens: 4096, SystemPrompt: "Plan from the trusted task manual.",
			RendererVersion: "research-planner.render/v3",
		},
		Synthesis: runtimepolicy.ResearchModelStageV3{
			Stage: runtimepolicy.ResearchModelStageSynthesisV3, Model: "strong-model",
			MaxTokens: 8192, SystemPrompt: "Synthesize without Tools.",
			RendererVersion: "research-synthesis.render/v3",
		},
		QuotaBucket: "llm_tokens",
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func testProductionResearchModelPolicyStoreV3(t *testing.T) runtimepolicy.ResearchModelPolicyV3 {
	t.Helper()
	policy := testResearchModelPolicyStoreV3(t)
	policy.Planner.Model = "deepseek-v4-pro"
	policy.Planner.DisableThinking = true
	policy.Synthesis.Model = "deepseek-v4-pro"
	policy.Synthesis.DisableThinking = true
	policy, err := runtimepolicy.BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
