package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type researchBriefFixtureV3 struct {
	st          *Store
	tenantID    int64
	userID      int64
	taskID      string
	identity    types.RunIdentity
	snapshotRef types.ResearchRunSnapshotRefV3
	planRef     types.ResearchRunPlanRefV3
}

func newResearchBriefFixtureV3(
	t *testing.T, threshold taskstate.NotificationThresholdV3, completeEvidence bool,
) researchBriefFixtureV3 {
	return newResearchBriefFixtureWithResultV3(t, threshold, completeEvidence, nil)
}

func newResearchBriefFixtureWithResultV3(
	t *testing.T, threshold taskstate.NotificationThresholdV3, completeEvidence bool,
	evidenceResult []byte,
) researchBriefFixtureV3 {
	t.Helper()
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
	taskID := "research-brief-v3-" + uuid.NewString()
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules (
		     id,tenant_id,user_id,nl_description,spec_json,scope_json,status,push_strictness
		 ) VALUES ($1,$2,$3,'Kimi pricing',
		           '{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}','{}','active','strict')`,
		taskID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		TaskName: "Kimi pricing", TaskManual: "检查 Kimi 官方套餐；没有达到门槛不推送。",
		SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: threshold, SuppressEmpty: true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language: taskstate.OutputLanguageZhCNV3,
			Format:   taskstate.OutputFormatExecutiveBriefV3, IncludeEvidenceLinks: true,
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
	definitionDigest, _ := taskstate.DigestApprovedDefinitionV3(definition)
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO task_approved_definition_versions (
		     tenant_id,user_id,task_id,version,schema_version,execution_mode,
		     definition_digest,payload,operation_ref
		 ) VALUES ($1,$2,$3,1,$4,'discover_at_run',$5,$6,$7)`,
		tenantID, userID, taskID, taskstate.ApprovedDefinitionSchemaVersionV3,
		definitionDigest, definitionPayload, "test-brief-v3:"+uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET execution_mode='discover_at_run',
		     approved_definition_version=1,approved_definition_digest=$2 WHERE id=$1`,
		taskID, definitionDigest); err != nil {
		t.Fatal(err)
	}

	identity := types.RunIdentity{
		TemporalWorkflowID: "workflow-" + taskID, TemporalRunID: "run-" + uuid.NewString(),
		RunKind: types.RunSnapshotKindScheduled, TenantID: tenantID, UserID: userID, TaskID: taskID,
	}
	researchTools := researchBriefToolPolicyV3(t)
	snapshotRef, err := st.CreateOrGetResearchRunSnapshotV3(
		ctx, identity, testCompiledRunPolicyV1(t), researchTools,
		testResearchModelPolicyStoreV3(t))
	if err != nil {
		t.Fatal(err)
	}
	arguments := json.RawMessage(`{"query":"Kimi membership pricing"}`)
	plan, err := runcontext.BuildResearchExecutionPlanV3(
		definitionDigest, snapshotRef.CapabilityCatalogDigest, snapshotRef.ToolPolicyDigest,
		[]runcontext.ResearchPlanStepV3{{
			InvocationID: "search-official", ToolName: "web_search", Arguments: arguments,
		}}, func(_ string, raw json.RawMessage) (json.RawMessage, error) { return raw, nil })
	if err != nil {
		t.Fatal(err)
	}
	planRef, err := st.CreateOrGetResearchRunPlanV3(ctx, CreateOrGetResearchRunPlanV3Params{
		Identity: identity, RunSnapshotID: snapshotRef.SnapshotID, Plan: plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginResearchRunStepV3(ctx, identity, snapshotRef.SnapshotID, planRef, 0)
	if err != nil {
		t.Fatal(err)
	}
	if completeEvidence {
		result := evidenceResult
		if result == nil {
			result = []byte(`{"url":"https://www.kimi.com/membership/pricing","state":"reservation_only"}`)
		}
		if _, err := st.CommitResearchRunStepEvidenceV3(ctx, CommitResearchRunStepEvidenceV3Params{
			Identity: identity, RunSnapshotID: snapshotRef.SnapshotID, PlanRef: planRef,
			Ordinal: 0, Result: result, OriginalSize: len(result), TrustType: "external",
			CostMicroUSD: 100,
			ProviderCall: researchProviderCallV3ForTest(
				researchExecutionTraceV3ForTest(t, identity, snapshotRef.SnapshotID,
					planRef, 0, started.InvocationID), 100),
		}); err != nil {
			t.Fatal(err)
		}
	} else if started.StepID <= 0 {
		t.Fatal("research step did not start")
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		if _, err := st.PurgeTenant(cleanupCtx, tenantID, false); err != nil {
			t.Errorf("purge V3 research Brief fixture tenant: %v", err)
			return
		}
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id=$1`, userID)
	})
	return researchBriefFixtureV3{
		st: st, tenantID: tenantID, userID: userID, taskID: taskID,
		identity: identity, snapshotRef: snapshotRef, planRef: planRef,
	}
}

func researchBriefToolPolicyV3(t *testing.T) runtimepolicy.ResearchToolPolicyV3 {
	t.Helper()
	parameters := json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}`)
	sum := sha256.Sum256(parameters)
	policy, err := runtimepolicy.BuildResearchToolPolicyV3([]runtimepolicy.ResearchToolDefinitionV3{{
		Name: "web_search", Description: "Search public web evidence",
		Parameters: parameters, SchemaDigest: hex.EncodeToString(sum[:]),
		Implementation: runtimepolicy.ResearchToolExaSearchV3, ImplementationGeneration: 1,
		Provider: "exa", Effects: []runtimepolicy.ResearchToolEffectV3{
			runtimepolicy.ResearchToolEffectBillableV3,
			runtimepolicy.ResearchToolEffectNetworkReadV3,
			runtimepolicy.ResearchToolEffectTrustTaintV3,
		},
		ResultTrust: runtimepolicy.ResearchToolTrustExternalV3, BudgetBucket: "exa_calls",
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
		},
		MaxCostMicroUSD: 10_000,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func researchBriefPrepareParamsV3(f researchBriefFixtureV3) PrepareResearchBriefSynthesisV3Params {
	return PrepareResearchBriefSynthesisV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
	}
}

func researchBriefPayloadV3(
	t *testing.T, synthesis ResearchBriefSynthesisV3,
	significance types.ResearchBriefSignificanceV3, summary string,
) []byte {
	t.Helper()
	var evidence researchEvidenceManifestV3
	if err := json.Unmarshal(synthesis.EvidenceManifest, &evidence); err != nil || len(evidence.Items) == 0 {
		t.Fatalf("decode Evidence manifest: items=%d err=%v", len(evidence.Items), err)
	}
	payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV3,
		Headline:      "Kimi 套餐状态",
		Summary:       summary,
		Significance:  significance,
		Citations: []types.ResearchBriefCitationV3{{
			Kind: types.ResearchBriefCitationCurrentEvidenceV3,
			Ref:  strconv.FormatInt(evidence.Items[0].EvidenceID, 10),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func finalizeResearchBriefFixtureV3(
	t *testing.T, f researchBriefFixtureV3,
	significance types.ResearchBriefSignificanceV3,
) (types.ResearchBriefRefV3, ResearchBriefSynthesisV3) {
	t.Helper()
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
	}
	if claim, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle); err != nil || !claim.Claimed {
		t.Fatalf("fixture claim=%+v err=%v", claim, err)
	}
	ref, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload: researchBriefPayloadV3(t, prepared.Synthesis, significance,
				"foreign owner history"),
		})
	if err != nil {
		t.Fatal(err)
	}
	state, err := f.st.LoadResearchBriefSynthesisV3(t.Context(),
		f.identity, f.snapshotRef, f.planRef)
	if err != nil || state.FinalizedAt == nil {
		t.Fatalf("fixture state=%+v err=%v", state, err)
	}
	return ref, state
}

func assertForgedForeignHistoryRejectedV3(
	t *testing.T, f researchBriefFixtureV3,
	foreignRef types.ResearchBriefRefV3, foreignState ResearchBriefSynthesisV3,
) {
	t.Helper()
	ctx := t.Context()
	var item researchEvidenceManifestItemV3
	if err := f.st.pool.QueryRow(ctx,
		`SELECT id,step_ordinal,invocation_id,tool_name,request_digest,
		        result_digest,original_size,truncated,trust_type
		   FROM research_run_evidence WHERE plan_id=$1`, f.planRef.PlanID,
	).Scan(&item.EvidenceID, &item.Ordinal, &item.InvocationID, &item.ToolName,
		&item.RequestDigest, &item.ResultDigest, &item.OriginalSize,
		&item.Truncated, &item.TrustType); err != nil {
		t.Fatal(err)
	}
	evidence, _ := json.Marshal(researchEvidenceManifestV3{
		SchemaVersion: researchEvidenceManifestSchemaV3,
		Items:         []researchEvidenceManifestItemV3{item},
	})
	history, _ := json.Marshal(researchHistoryManifestV3{
		SchemaVersion:     researchHistoryManifestSchemaV3,
		HistoryThroughUTC: f.snapshotRef.HistoryThroughUTC,
		CandidateCount:    1,
		ReturnedCount:     1,
		Items: []researchHistoryManifestItemV3{{
			Kind: "v3_brief", RecordID: strconv.FormatInt(foreignRef.BriefID, 10),
			RunSnapshotID: foreignRef.RunSnapshotID,
			GeneratedAt: foreignState.FinalizedAt.UTC().Truncate(time.Microsecond).
				Format("2006-01-02T15:04:05.000000Z"),
			Digest: foreignRef.BriefDigest, Coverage: "exact",
		}},
	})
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(ctx,
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	contextPayload := prepared.Synthesis.ContextPayload
	exactHistory := prepared.Synthesis.HistoryManifest
	if _, err := f.st.pool.Exec(ctx,
		`DELETE FROM research_brief_syntheses WHERE id=$1`, prepared.Synthesis.ID); err != nil {
		t.Fatal(err)
	}
	tx, err := f.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(f.userID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	requestDigest := digestResearchBriefRequestV3(researchBriefRequestDigestV3{
		SchemaVersion: researchBriefSynthesisSchemaV3,
		RunSnapshotID: f.snapshotRef.SnapshotID, PlanID: f.planRef.PlanID,
		DefinitionDigest: f.snapshotRef.DefinitionDigest, PlanDigest: f.planRef.PlanDigest,
		NotificationThreshold: string(taskstate.NotificationThresholdMajorV3),
		ContextDigest:         researchRunSHA256(contextPayload),
		EvidenceDigest:        researchRunSHA256(evidence), HistoryDigest: researchRunSHA256(history),
	})
	const insertSynthesis = `INSERT INTO research_brief_syntheses (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		     notification_threshold,request_digest,context_payload,context_digest,
		     evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'major_updates_only',$10,
		           $11,encode(sha256($11),'hex'),$12,encode(sha256($12),'hex'),
		           $13,encode(sha256($13),'hex'),$14)`
	if _, err := tx.Exec(ctx, `SAVEPOINT wrong_request_digest`); err != nil {
		t.Fatal(err)
	}
	_, wrongRequestErr := tx.Exec(ctx, insertSynthesis,
		f.tenantID, f.userID, f.taskID, f.snapshotRef.SnapshotID, f.planRef.PlanID,
		f.identity.TemporalWorkflowID, f.identity.TemporalRunID,
		f.snapshotRef.DefinitionDigest, f.planRef.PlanDigest, strings.Repeat("f", 64),
		contextPayload, evidence, exactHistory, researchBriefSynthesisSchemaV3)
	if wrongRequestErr == nil {
		t.Fatal("database accepted a forged request digest")
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT wrong_request_digest`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, insertSynthesis,
		f.tenantID, f.userID, f.taskID, f.snapshotRef.SnapshotID, f.planRef.PlanID,
		f.identity.TemporalWorkflowID, f.identity.TemporalRunID,
		f.snapshotRef.DefinitionDigest, f.planRef.PlanDigest, requestDigest,
		contextPayload, evidence, history, researchBriefSynthesisSchemaV3)
	if err == nil || !strings.Contains(err.Error(), "history is not exact same-owner history") {
		t.Fatalf("foreign forged history insert err=%v", err)
	}
}

func TestResearchBriefSynthesisV3PostgresLifecycleAndIsolation(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	foreign := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	foreignRef, foreignState := finalizeResearchBriefFixtureV3(
		t, foreign, types.ResearchBriefSignificanceMajorV3)
	assertForgedForeignHistoryRejectedV3(t, f, foreignRef, foreignState)
	ctx := t.Context()
	params := researchBriefPrepareParamsV3(f)
	const workers = 4
	results := make([]PrepareResearchBriefSynthesisV3Result, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for index := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index], errs[index] = f.st.PrepareOrGetResearchBriefSynthesisV3(ctx, params)
		}(index)
	}
	wg.Wait()
	firstWriters := 0
	for index, err := range errs {
		if err != nil {
			t.Fatalf("prepare %d: %v", index, err)
		}
		if results[index].FirstWriter {
			firstWriters++
		}
	}
	if firstWriters != 1 {
		t.Fatalf("first writers=%d", firstWriters)
	}
	prepared := results[0].Synthesis
	if prepared.ID <= 0 || prepared.Status != ResearchBriefSynthesisPreparedV3 ||
		prepared.RequestDigest == "" {
		t.Fatalf("prepared=%+v", prepared)
	}
	replay, err := f.st.PrepareOrGetResearchBriefSynthesisV3(ctx, params)
	if err != nil || replay.FirstWriter || replay.Synthesis.ID != prepared.ID {
		t.Fatalf("canonical replay=%+v err=%v", replay, err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		SynthesisID: prepared.ID, RequestDigest: prepared.RequestDigest,
	}
	claim, err := f.st.ClaimResearchBriefSynthesisV3(ctx, handle)
	if err != nil || !claim.Claimed || claim.Synthesis.Status != ResearchBriefSynthesisSpendingV3 ||
		claim.Synthesis.SpendingStartedAt == nil {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	payload := researchBriefPayloadV3(t, prepared,
		types.ResearchBriefSignificanceQualifiedV3,
		"仍需预约，未达到重大更新门槛。")
	badDecisionTx, err := f.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := badDecisionTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(f.userID)); err != nil {
		_ = badDecisionTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := badDecisionTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		_ = badDecisionTx.Rollback(ctx)
		t.Fatal(err)
	}
	badDigest := sha256.Sum256(payload)
	if _, err := badDecisionTx.Exec(ctx,
		`UPDATE research_brief_syntheses
		    SET status='finalized',significance='major',decision='deliver',
		        delivery_required=true,brief_payload=$2,brief_digest=$3
		  WHERE id=$1`, prepared.ID, payload, hex.EncodeToString(badDigest[:])); err == nil {
		_ = badDecisionTx.Rollback(ctx)
		t.Fatal("database accepted significance forged outside the Brief payload")
	}
	_ = badDecisionTx.Rollback(ctx)
	ref, err := f.st.FinalizeResearchBriefSynthesisV3(ctx,
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        payload,
		})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Decision != types.ResearchBriefDecisionQuietV3 || ref.DeliveryRequired ||
		ref.NotificationThreshold != string(taskstate.NotificationThresholdMajorV3) {
		t.Fatalf("Store decision=%+v", ref)
	}
	if err := ref.ValidateFor(f.identity, f.snapshotRef.SnapshotID, f.planRef.PlanID); err != nil {
		t.Fatal(err)
	}
	replayedRef, err := f.st.FinalizeResearchBriefSynthesisV3(ctx,
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        payload,
		})
	if err != nil || replayedRef != ref {
		t.Fatalf("finalize replay=%+v err=%v", replayedRef, err)
	}
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(ctx,
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload: researchBriefPayloadV3(t, prepared,
				types.ResearchBriefSignificanceMajorV3, "conflicting replay"),
		}); err == nil {
		t.Fatal("conflicting terminal synthesis replayed")
	}
	brief, err := f.st.LoadResearchBriefV3(ctx, f.identity, ref)
	if err != nil || !json.Valid(brief.Payload) || brief.Ref != ref {
		t.Fatalf("loaded Brief=%+v err=%v", brief, err)
	}

	// Even with the exact RLS scope and update privilege, terminal rows reject
	// mutation and cross-user readers see no row.
	tx, err := f.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(f.userID)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE research_brief_syntheses SET status='failed',
		     delivery_required=false,failure_code='tamper' WHERE id=$1`, ref.BriefID); err == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("terminal synthesis mutated")
	}
	_ = tx.Rollback(ctx)

	otherUserID := testUser(t, f.st)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.st, `DELETE FROM users WHERE id=$1`, otherUserID)
	})
	rlsTx, err := f.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rlsTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(otherUserID)); err != nil {
		_ = rlsTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := rlsTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		_ = rlsTx.Rollback(ctx)
		t.Fatal(err)
	}
	var visible int
	if err := rlsTx.QueryRow(ctx,
		`SELECT count(*) FROM research_brief_syntheses WHERE id=$1`, ref.BriefID,
	).Scan(&visible); err != nil {
		_ = rlsTx.Rollback(ctx)
		t.Fatal(err)
	}
	_ = rlsTx.Rollback(ctx)
	if visible != 0 {
		t.Fatalf("cross-user RLS exposed %d rows", visible)
	}
}

func TestResearchBriefSynthesisV3StoreNotificationMatrix(t *testing.T) {
	tests := []struct {
		name         string
		threshold    taskstate.NotificationThresholdV3
		significance types.ResearchBriefSignificanceV3
		decision     types.ResearchBriefDecisionV3
		deliver      bool
	}{
		{"major-none", taskstate.NotificationThresholdMajorV3, types.ResearchBriefSignificanceNoneV3, types.ResearchBriefDecisionQuietV3, false},
		{"major-qualified", taskstate.NotificationThresholdMajorV3, types.ResearchBriefSignificanceQualifiedV3, types.ResearchBriefDecisionQuietV3, false},
		{"major-major", taskstate.NotificationThresholdMajorV3, types.ResearchBriefSignificanceMajorV3, types.ResearchBriefDecisionDeliverV3, true},
		{"qualified-none", taskstate.NotificationThresholdQualifiedV3, types.ResearchBriefSignificanceNoneV3, types.ResearchBriefDecisionQuietV3, false},
		{"qualified-qualified", taskstate.NotificationThresholdQualifiedV3, types.ResearchBriefSignificanceQualifiedV3, types.ResearchBriefDecisionDeliverV3, true},
		{"qualified-major", taskstate.NotificationThresholdQualifiedV3, types.ResearchBriefSignificanceMajorV3, types.ResearchBriefDecisionDeliverV3, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newResearchBriefFixtureV3(t, test.threshold, true)
			prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
				researchBriefPrepareParamsV3(f))
			if err != nil {
				t.Fatal(err)
			}
			handle := ClaimResearchBriefSynthesisV3Params{
				Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
				SynthesisID:   prepared.Synthesis.ID,
				RequestDigest: prepared.Synthesis.RequestDigest,
			}
			if claim, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle); err != nil || !claim.Claimed {
				t.Fatalf("claim=%+v err=%v", claim, err)
			}
			ref, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
				FinalizeResearchBriefSynthesisV3Params{
					ClaimResearchBriefSynthesisV3Params: handle,
					BriefPayload: researchBriefPayloadV3(t, prepared.Synthesis,
						test.significance, test.name),
				})
			if err != nil {
				t.Fatal(err)
			}
			if ref.Decision != test.decision || ref.DeliveryRequired != test.deliver {
				t.Fatalf("decision=%s delivery=%v", ref.Decision, ref.DeliveryRequired)
			}
		})
	}
}

func TestResearchBriefSynthesisV3RequiresCompleteEvidenceAndFailsIdempotently(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, false)
	if _, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f)); err == nil {
		t.Fatal("incomplete Evidence admitted synthesis")
	}

	complete := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := complete.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(complete))
	if err != nil {
		t.Fatal(err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: complete.identity, SnapshotRef: complete.snapshotRef, PlanRef: complete.planRef,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
	}
	failed, err := complete.st.FailResearchBriefSynthesisV3(t.Context(),
		FailResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			Status:                              ResearchBriefSynthesisFailedV3, FailureCode: "prompt_unavailable",
		})
	if err != nil || failed.Status != ResearchBriefSynthesisFailedV3 || failed.FinalizedAt == nil {
		t.Fatalf("failed=%+v err=%v", failed, err)
	}
	replay, err := complete.st.FailResearchBriefSynthesisV3(t.Context(),
		FailResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			Status:                              ResearchBriefSynthesisFailedV3, FailureCode: "prompt_unavailable",
		})
	if err != nil || replay.ID != failed.ID {
		t.Fatalf("failure replay=%+v err=%v", replay, err)
	}
	if _, err := complete.st.ClaimResearchBriefSynthesisV3(t.Context(), handle); err != nil {
		t.Fatalf("terminal claim read should be idempotent: %v", err)
	}
}

func TestResearchBriefSynthesisV3SpendingRecoveryIsAmbiguousAndNeverRetries(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
	}
	claim, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	recovery, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle)
	ambiguous := recovery.Synthesis
	if err != nil || ambiguous.Status != ResearchBriefSynthesisAmbiguousV3 ||
		recovery.Claimed ||
		ambiguous.FinalizedAt == nil || ambiguous.DeliveryRequired == nil ||
		*ambiguous.DeliveryRequired {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
	replay, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle)
	if err != nil || replay.Claimed || replay.Synthesis.Status != ResearchBriefSynthesisAmbiguousV3 {
		t.Fatalf("recovery claim=%+v err=%v", replay, err)
	}
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload: researchBriefPayloadV3(t, prepared.Synthesis,
				types.ResearchBriefSignificanceMajorV3, "must not finalize"),
		}); err == nil {
		t.Fatal("ambiguous paid synthesis was retried/finalized")
	}
}

func TestResearchBriefSynthesisV3ReportsHistoryTruncation(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	cutoff, err := time.Parse(time.RFC3339Nano, f.snapshotRef.HistoryThroughUTC)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	emptyJSON := []byte(`{}`)
	emptyDigest := researchRunSHA256(emptyJSON)
	for index := 0; index < 21; index++ {
		briefPayload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
			SchemaVersion: types.ResearchBriefPayloadSchemaV3,
			Headline:      "prior " + strconv.Itoa(index),
			Summary:       "retained prior Brief",
			Significance:  types.ResearchBriefSignificanceNoneV3,
			Citations: []types.ResearchBriefCitationV3{{
				Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "1",
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		briefDigest := researchRunSHA256(briefPayload)
		generated := cutoff.Add(-time.Duration(index+1) * time.Second)
		if _, err := tx.Exec(t.Context(),
			`INSERT INTO research_brief_syntheses (
			     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
			     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
			     notification_threshold,request_digest,context_payload,context_digest,
			     evidence_manifest,evidence_digest,history_manifest,history_digest,
			     schema_version,status,significance,decision,delivery_required,
			     brief_payload,brief_digest,spending_started_at,finalized_at
			 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,'major_updates_only',$8,
			           $9,$10,$9,$10,$9,$10,$11,'finalized','none','quiet',false,
			           $12,$13,$14,$14)`,
			f.tenantID, f.userID, f.taskID, int64(7_000_000_000+index),
			int64(8_000_000_000+index), "history-workflow-"+strconv.Itoa(index),
			"history-run-"+strconv.Itoa(index), digest, emptyJSON, emptyDigest,
			researchBriefSynthesisSchemaV3, briefPayload, briefDigest, generated); err != nil {
			t.Fatal(err)
		}
	}
	var legacyBriefID int64
	if err := tx.QueryRow(t.Context(),
		`INSERT INTO brief_snapshots (
		     tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,push_batch_id,
		     schema_version,request_digest,payload_digest,payload,insight_count,generated_at
		 ) VALUES ($1,$2,$3,9000000001,9000000002,9000000003,'vane.brief/v1',$4,
		           encode(sha256(convert_to(repeat('x',33554432),'UTF8')),'hex'),
		           convert_to(repeat('x',33554432),'UTF8'),1,$5)
		 RETURNING id`,
		f.tenantID, f.userID, f.taskID, digest, cutoff.Add(-500*time.Millisecond)).Scan(&legacyBriefID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`INSERT INTO task_run_content_provenance (
		     tenant_id,user_id,task_id,run_snapshot_id,invocation_digest,
		     content_item_ids,observation_payload,observation_digest,created_at
		 ) VALUES ($1,$2,$3,9000000004,$4,'{}'::bigint[],
		           convert_to(repeat('y',8388608),'UTF8'),
		           encode(sha256(convert_to(repeat('y',8388608),'UTF8')),'hex'),$5)`,
		f.tenantID, f.userID, f.taskID, digest, cutoff.Add(-400*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if _, err := tx.Exec(t.Context(),
			`INSERT INTO task_run_outcomes (
			     id,tenant_id,user_id,task_id,run_snapshot_id,schema_version,status,
			     result,source_coverage,processing,failure_code,failure_message,
			     finalized_at,outcome_digest
			 ) VALUES ($1,$2,$3,$4,$5,'vane.run-outcome/v1','finalized',
			           'quiet','complete','complete','','',$6,$7)`,
			int64(9_000_000_010+index), f.tenantID, f.userID, f.taskID,
			int64(9_000_000_020+index), cutoff, digest); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var history researchHistoryManifestV3
	if err := json.Unmarshal(prepared.Synthesis.HistoryManifest, &history); err != nil {
		t.Fatal(err)
	}
	if history.CandidateCount != 25 || history.ReturnedCount != 20 ||
		!history.Truncated || history.Continuation == nil || len(history.Items) != 20 {
		t.Fatalf("history coverage=%+v items=%d", history, len(history.Items))
	}
	var synthesisContext researchSynthesisContextV3
	if err := json.Unmarshal(prepared.Synthesis.ContextPayload, &synthesisContext); err != nil {
		t.Fatal(err)
	}
	if synthesisContext.History.CandidateCount != history.CandidateCount ||
		synthesisContext.History.ReturnedCount != history.ReturnedCount ||
		!synthesisContext.History.Truncated || synthesisContext.History.Continuation == nil {
		t.Fatalf("model-visible history coverage=%+v", synthesisContext.History)
	}
	if len(synthesisContext.History.Items) < 4 ||
		synthesisContext.History.Items[0].RecordID != "run:9000000011" ||
		synthesisContext.History.Items[1].RecordID != "run:9000000010" {
		t.Fatalf("equal-cutoff history order=%+v", synthesisContext.History.Items[:2])
	}
	found := map[string]researchHistoryContextItemV3{}
	for _, item := range synthesisContext.History.Items {
		found[item.Kind] = item
	}
	if brief := found["legacy_v1_brief"]; !brief.ContextTruncated || brief.ContextStoredSize != 32<<20 ||
		brief.ContextVisibleSize != 4096 || len(brief.PayloadText) != researchHistoryContextCharsV3 {
		t.Fatalf("large legacy Brief preview=%+v payload=%d", brief, len(brief.PayloadText))
	}
	if observation := found["legacy_v1_observation"]; !observation.ContextTruncated ||
		observation.ContextStoredSize != 8<<20 || observation.ContextVisibleSize != 4096 {
		t.Fatalf("large legacy Observation preview=%+v", observation)
	}
	if gap := found["legacy_run_gap"]; gap.GapReason != "legacy_evidence_unavailable" ||
		gap.PayloadText != "" || gap.ContextStoredSize != 0 || gap.ContextTruncated {
		t.Fatalf("legacy gap=%+v", gap)
	}
	chunk, err := f.st.LoadResearchHistoryChunkV3(t.Context(), LoadResearchHistoryChunkV3Params{
		ClaimResearchBriefSynthesisV3Params: ClaimResearchBriefSynthesisV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
			SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
		},
		RecordID:    "brief:" + strconv.FormatInt(legacyBriefID, 10),
		OffsetChars: researchHistoryContextCharsV3, LimitChars: researchHistoryContextCharsV3,
	})
	if err != nil || chunk.OffsetChars != 4096 || chunk.NextOffsetChars != 8192 ||
		chunk.TotalChars != 32<<20 || chunk.TotalBytes != 32<<20 || chunk.Complete ||
		chunk.Text != strings.Repeat("x", researchHistoryContextCharsV3) {
		t.Fatalf("history continuation=%+v err=%v", chunk, err)
	}
}

func TestResearchBriefSynthesisV3LateHistoryCommitCannotChangeFrozenTop20(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	cutoff, err := time.Parse(time.RFC3339Nano, f.snapshotRef.HistoryThroughUTC)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("b", 64)
	emptyJSON := []byte(`{}`)
	emptyDigest := researchRunSHA256(emptyJSON)
	baseID := time.Now().UnixNano()
	const insertHistoricalBrief = `INSERT INTO research_brief_syntheses (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		     notification_threshold,request_digest,context_payload,context_digest,
		     evidence_manifest,evidence_digest,history_manifest,history_digest,
		     schema_version,status,significance,decision,delivery_required,
		     brief_payload,brief_digest,spending_started_at,finalized_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,'major_updates_only',$8,
		           $9,$10,$9,$10,$9,$10,$11,'finalized','none','quiet',false,
		           $12,$13,$14,$14)
		 RETURNING id`

	visibleTx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = visibleTx.Rollback(t.Context()) }()
	if _, err := visibleTx.Exec(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 20; index++ {
		payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
			SchemaVersion: types.ResearchBriefPayloadSchemaV3,
			Headline:      "frozen prior " + strconv.Itoa(index),
			Summary:       strings.Repeat("h", 5000) + strconv.Itoa(index),
			Significance:  types.ResearchBriefSignificanceNoneV3,
			Citations: []types.ResearchBriefCitationV3{{
				Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "1",
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		generated := cutoff.Add(-time.Duration(index+1) * time.Second)
		var ignoredID int64
		if err := visibleTx.QueryRow(t.Context(), insertHistoricalBrief,
			f.tenantID, f.userID, f.taskID, baseID+int64(index),
			baseID+1000+int64(index), "visible-workflow-"+strconv.Itoa(index),
			"visible-run-"+strconv.Itoa(index), digest, emptyJSON, emptyDigest,
			researchBriefSynthesisSchemaV3, payload, researchRunSHA256(payload), generated,
		).Scan(&ignoredID); err != nil {
			t.Fatal(err)
		}
	}
	if err := visibleTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	lateTx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lateTx.Rollback(t.Context()) }()
	if _, err := lateTx.Exec(t.Context(), `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	latePayload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV3,
		Headline:      "late cutoff-eligible prior",
		Summary:       strings.Repeat("l", 5000),
		Significance:  types.ResearchBriefSignificanceNoneV3,
		Citations: []types.ResearchBriefCitationV3{{
			Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var lateID int64
	if err := lateTx.QueryRow(t.Context(), insertHistoricalBrief,
		f.tenantID, f.userID, f.taskID, baseID+100, baseID+1100,
		"late-workflow", "late-run", digest, emptyJSON, emptyDigest,
		researchBriefSynthesisSchemaV3, latePayload, researchRunSHA256(latePayload),
		cutoff.Add(-500*time.Millisecond),
	).Scan(&lateID); err != nil {
		t.Fatal(err)
	}

	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var frozen researchHistoryManifestV3
	if err := json.Unmarshal(prepared.Synthesis.HistoryManifest, &frozen); err != nil {
		t.Fatal(err)
	}
	if frozen.CandidateCount != 20 || frozen.ReturnedCount != 20 || len(frozen.Items) != 20 {
		t.Fatalf("frozen history=%+v", frozen)
	}
	frozenRank20 := frozen.Items[19].RecordID
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
	}
	if claim, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle); err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	if err := lateTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	checkTx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = checkTx.Rollback(t.Context()) }()
	if err := setResearchRunScopeV3(t.Context(), checkTx, f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	var lateVisible, rank20StillDynamic bool
	if err := checkTx.QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM read_research_history_v3($1,$2,$3,$4,$5)
		                       WHERE record_id=$6),
		        EXISTS (SELECT 1 FROM read_research_history_v3($1,$2,$3,$4,$5)
		                       WHERE record_id=$7)`,
		f.tenantID, f.userID, f.taskID, f.snapshotRef.SnapshotID, f.planRef.PlanID,
		strconv.FormatInt(lateID, 10), frozenRank20,
	).Scan(&lateVisible, &rank20StillDynamic); err != nil {
		t.Fatal(err)
	}
	if !lateVisible || rank20StillDynamic {
		t.Fatalf("late history did not displace frozen rank20: late=%v rank20=%v",
			lateVisible, rank20StillDynamic)
	}
	if err := checkTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	replay, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil || replay.FirstWriter || replay.Synthesis.ID != prepared.Synthesis.ID ||
		replay.Synthesis.HistoryDigest != prepared.Synthesis.HistoryDigest {
		t.Fatalf("frozen prepare replay=%+v err=%v", replay, err)
	}

	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload: researchBriefPayloadV3(t, prepared.Synthesis,
				types.ResearchBriefSignificanceMajorV3, "late commit cannot alter frozen history"),
		}); err != nil {
		t.Fatalf("finalize after late commit: %v", err)
	}
	chunk, err := f.st.LoadResearchHistoryChunkV3(t.Context(), LoadResearchHistoryChunkV3Params{
		ClaimResearchBriefSynthesisV3Params: handle,
		RecordID:                            frozenRank20, OffsetChars: researchHistoryContextCharsV3, LimitChars: 128,
	})
	if err != nil || chunk.RecordID != frozenRank20 || chunk.OffsetChars != 4096 ||
		chunk.NextOffsetChars <= chunk.OffsetChars || chunk.Text == "" {
		t.Fatalf("frozen rank20 continuation=%+v err=%v", chunk, err)
	}
}

func TestResearchBriefSynthesisV3DatabaseRejectsMalformedBriefContract(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, PlanRef: f.planRef,
		SynthesisID: prepared.Synthesis.ID, RequestDigest: prepared.Synthesis.RequestDigest,
	}
	if claim, err := f.st.ClaimResearchBriefSynthesisV3(t.Context(), handle); err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	var evidence researchEvidenceManifestV3
	if err := json.Unmarshal(prepared.Synthesis.EvidenceManifest, &evidence); err != nil || len(evidence.Items) == 0 {
		t.Fatalf("Evidence=%+v err=%v", evidence, err)
	}
	malformed, err := json.Marshal(map[string]any{
		"schema_version": types.ResearchBriefPayloadSchemaV3,
		"significance":   "major",
		"citations": []map[string]string{{
			"kind": string(types.ResearchBriefCitationCurrentEvidenceV3),
			"ref":  strconv.FormatInt(evidence.Items[0].EvidenceID, 10),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := researchRunSHA256(malformed)
	tx, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmtInt64V3(f.tenantID), fmtInt64V3(f.userID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(),
		`UPDATE research_brief_syntheses
		    SET status='finalized',significance='major',decision='deliver',
		        delivery_required=true,brief_payload=$2,brief_digest=$3
		  WHERE id=$1`, prepared.Synthesis.ID, malformed, digest); err == nil {
		t.Fatal("database accepted a Brief without headline and summary")
	}
}

func TestResearchSynthesisContextV3WorstCaseEscapingFitsBudget(t *testing.T) {
	digest := strings.Repeat("a", 64)
	evidence := make([]researchEvidenceContextItemV3, 16)
	for index := range evidence {
		visible := strings.Repeat("\x01", 256<<10)
		evidence[index] = researchEvidenceContextItemV3{
			researchEvidenceManifestItemV3: researchEvidenceManifestItemV3{
				EvidenceID: int64(index + 1), Ordinal: index,
				InvocationID: "invocation", ToolName: "web_search",
				RequestDigest: digest, ResultDigest: digest,
				OriginalSize: 256 << 10, Truncated: true, TrustType: "external",
			},
			SynthesisVisibleText: visible,
			ContextStoredSize:    len(visible),
			ContextVisibleSize:   len(visible),
			ContextVisibleDigest: researchRunSHA256([]byte(visible)),
			ContextTruncated:     false,
		}
	}
	historyItems := make([]researchHistoryContextItemV3, 20)
	for index := range historyItems {
		visible := strings.Repeat("\x01", researchHistoryContextCharsV3)
		historyItems[index] = researchHistoryContextItemV3{
			researchHistoryManifestItemV3: researchHistoryManifestItemV3{
				Kind: "legacy_v1_brief", RecordID: "brief:" + strconv.Itoa(index+1),
				RunSnapshotID: int64(index + 1),
				GeneratedAt:   "2026-08-01T00:00:00.000000Z",
				Digest:        digest, Coverage: "legacy",
			},
			PayloadText: visible, ContextStoredSize: 32 << 20,
			ContextVisibleSize:   len(visible),
			ContextVisibleDigest: researchRunSHA256([]byte(visible)), ContextTruncated: true,
		}
	}
	payload, err := json.Marshal(researchSynthesisContextV3{
		SchemaVersion: researchSynthesisContextSchemaV3,
		Definition: researchSynthesisDefinitionContextV3{
			TaskName:   strings.Repeat("\x01", 16<<10),
			TaskManual: strings.Repeat("\x01", 256<<10),
			Output: taskstate.OutputPreferenceV3{
				Instructions: strings.Repeat("\x01", 16<<10),
			},
		},
		CurrentEvidence: evidence,
		History: researchHistoryContextV3{
			HistoryThroughUTC: "2026-08-01T00:00:00Z",
			CandidateCount:    21, ReturnedCount: 20, Truncated: true,
			Continuation: &researchHistoryContinuationV3{
				GeneratedAt: "2026-07-01T00:00:00.000000Z", Kind: "legacy_v1_brief", RecordID: "brief:20",
			},
			Items: historyItems,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > researchSynthesisContextMaxV3 {
		t.Fatalf("worst-case escaped context=%d exceeds budget=%d",
			len(payload), researchSynthesisContextMaxV3)
	}
}

func TestResearchBriefSynthesisV3KeepsMaxEvidenceExact(t *testing.T) {
	result := []byte(strings.Repeat("\x01", 256<<10))
	f := newResearchBriefFixtureWithResultV3(t,
		taskstate.NotificationThresholdMajorV3, true, result)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var synthesisContext researchSynthesisContextV3
	if err := json.Unmarshal(prepared.Synthesis.ContextPayload, &synthesisContext); err != nil {
		t.Fatal(err)
	}
	if len(synthesisContext.CurrentEvidence) != 1 {
		t.Fatalf("current Evidence=%d", len(synthesisContext.CurrentEvidence))
	}
	visible := synthesisContext.CurrentEvidence[0]
	if visible.ContextTruncated || visible.ContextStoredSize != len(result) ||
		visible.ContextVisibleSize != len(result) ||
		visible.SynthesisVisibleText != string(result) ||
		visible.ContextVisibleDigest != researchRunSHA256(result) {
		t.Fatalf("max Evidence was not exact: %+v", visible)
	}
}

func TestResearchBriefSynthesisV3WaitsForTenantPurgeAdmission(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	blocker, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(t.Context()) }()
	if exists, err := lockTenantAdmissionRoot(t.Context(), blocker, f.tenantID); err != nil || !exists {
		t.Fatalf("exclusive tenant admission exists=%v err=%v", exists, err)
	}
	type result struct {
		prepared PrepareResearchBriefSynthesisV3Result
		err      error
	}
	done := make(chan result, 1)
	go func() {
		prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
			t.Context(), researchBriefPrepareParamsV3(f))
		done <- result{prepared: prepared, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("Prepare bypassed tenant purge admission: %+v err=%v", got.prepared, got.err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || !got.prepared.FirstWriter {
			t.Fatalf("Prepare after purge admission=%+v err=%v", got.prepared, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare remained blocked after tenant purge admission released")
	}
}

func fmtInt64V3(value int64) string {
	return strconv.FormatInt(value, 10)
}
