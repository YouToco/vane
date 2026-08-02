package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type researchRunSpendFixtureV3 struct {
	store            *Store
	tenantID         int64
	userID           int64
	identity         types.RunIdentity
	snapshotID       int64
	snapshotRef      types.ResearchRunSnapshotRefV3
	definitionDigest string
	planRef          types.ResearchRunPlanRefV3
}

func newResearchRunSpendFixtureV3(t *testing.T, maxRunCostMicroUSD int64) researchRunSpendFixtureV3 {
	t.Helper()
	return newResearchRunSpendFixtureWithToolBudgetV3(t, maxRunCostMicroUSD, 16, true)
}

func newResearchRunSpendFixtureWithToolBudgetV3(
	t *testing.T, maxRunCostMicroUSD int64, maxToolCalls int, createPlan bool,
) researchRunSpendFixtureV3 {
	t.Helper()
	return newResearchRunSpendFixtureWithModelPolicyV3(t, maxRunCostMicroUSD,
		maxToolCalls, createPlan, testResearchModelPolicyStoreV3(t))
}

func newResearchRunSpendFixtureWithModelPolicyV3(
	t *testing.T, maxRunCostMicroUSD int64, maxToolCalls int, createPlan bool,
	modelPolicy runtimepolicy.ResearchModelPolicyV3,
) researchRunSpendFixtureV3 {
	t.Helper()
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is required for V3 research spend integration tests")
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
	t.Cleanup(func() { cleanupResearchRunSpendFixtureV3(t, st, tenantID, userID) })
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if err := st.SeedTenantQuota(ctx, tenantID); err != nil {
		t.Fatal(err)
	}
	// Eliminate token-bucket refill from exact debit assertions.
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenant_quota
		    SET tokens=10,rate=0,burst=10,updated_at=now()
		  WHERE tenant_id=$1 AND bucket='exa_calls'`, tenantID); err != nil {
		t.Fatal(err)
	}

	taskID := "research-spend-v3-" + uuid.NewString()
	identity := types.RunIdentity{
		TemporalWorkflowID: "workflow-" + taskID,
		TemporalRunID:      "run-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID,
		UserID:             userID,
		TaskID:             taskID,
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO schedules (
		     id,tenant_id,user_id,nl_description,spec_json,scope_json,status,
		     push_strictness
		 ) VALUES ($1,$2,$3,'Kimi pricing spend test',
		           '{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}',
		           '{}','active','strict')`,
		taskID, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		TaskName:      "Kimi pricing spend test",
		TaskManual:    "检查 Kimi 官方套餐并交叉核验；没有重大更新不推送。",
		SpecJSON:      json.RawMessage(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3,
			SuppressEmpty:       true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language:             taskstate.OutputLanguageZhCNV3,
			Format:               taskstate.OutputFormatExecutiveBriefV3,
			IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: 8, MaxToolCalls: maxToolCalls, MaxTokens: 32768,
			MaxCostMicroUSD: maxRunCostMicroUSD, DurationMs: 300_000,
		},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO task_approved_definition_versions (
		     tenant_id,user_id,task_id,version,schema_version,execution_mode,
		     definition_digest,payload,operation_ref
		 ) VALUES ($1,$2,$3,1,$4,'discover_at_run',$5,$6,$7)`,
		tenantID, userID, taskID, taskstate.ApprovedDefinitionSchemaVersionV3,
		digest, payload, "test-v3-spend:"+uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules
		    SET execution_mode='discover_at_run',approved_definition_version=1,
		        approved_definition_digest=$2
		  WHERE id=$1`, taskID, digest); err != nil {
		t.Fatal(err)
	}
	snapshotRef, err := st.CreateOrGetResearchRunSnapshotV3(
		ctx, identity, testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		modelPolicy)
	if err != nil {
		t.Fatal(err)
	}
	var planRef types.ResearchRunPlanRefV3
	if createPlan {
		plan := researchRunPlanFixtureV3(t, digest, snapshotRef.CapabilityCatalogDigest,
			snapshotRef.ToolPolicyDigest, "Kimi pricing")
		planRef, _ = createResearchPlanFromReceiptV3(t, st, identity, snapshotRef, plan)
	}
	return researchRunSpendFixtureV3{
		store: st, tenantID: tenantID, userID: userID, identity: identity,
		snapshotID: snapshotRef.SnapshotID, snapshotRef: snapshotRef,
		definitionDigest: digest, planRef: planRef,
	}
}

func cleanupResearchRunSpendFixtureV3(t *testing.T, st *Store, tenantID, userID int64) {
	t.Helper()
	ctx, cancel := cleanupContext()
	defer cancel()
	if _, err := st.PurgeTenant(ctx, tenantID, false); err != nil {
		t.Errorf("purge V3 spend fixture tenant: %v", err)
		return
	}
	if _, err := st.pool.Exec(ctx, `DELETE FROM users WHERE id=$1`, userID); err != nil {
		t.Errorf("delete V3 spend fixture user: %v", err)
	}
}

func (f researchRunSpendFixtureV3) begin(t *testing.T, ordinal int) (ResearchRunStepExecutionV3, error) {
	t.Helper()
	return f.store.BeginResearchRunStepV3(
		t.Context(), f.identity, f.snapshotID, f.planRef, ordinal)
}

func (f researchRunSpendFixtureV3) trace(
	t *testing.T, ordinal int, invocationID string,
) string {
	t.Helper()
	return researchExecutionTraceV3ForTest(
		t, f.identity, f.snapshotID, f.planRef, ordinal, invocationID)
}

func (f researchRunSpendFixtureV3) quotaTokens(t *testing.T) float64 {
	t.Helper()
	var tokens float64
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT tokens FROM tenant_quota WHERE tenant_id=$1 AND bucket='exa_calls'`,
		f.tenantID).Scan(&tokens); err != nil {
		t.Fatal(err)
	}
	return tokens
}

func (f researchRunSpendFixtureV3) spendCounts(t *testing.T) (int, int) {
	t.Helper()
	var starts, reservations int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		   (SELECT count(*) FROM research_run_steps
		     WHERE tenant_id=$1 AND temporal_run_id=$2 AND phase='started'),
		   (SELECT count(*) FROM research_run_step_spend_reservations
		     WHERE tenant_id=$1 AND temporal_run_id=$2)`,
		f.tenantID, f.identity.TemporalRunID).Scan(&starts, &reservations); err != nil {
		t.Fatal(err)
	}
	return starts, reservations
}

func TestResearchRunSpendConcurrentBeginAndResponseLossPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	type result struct {
		execution ResearchRunStepExecutionV3
		err       error
	}
	ready := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			execution, err := f.store.BeginResearchRunStepV3(
				t.Context(), f.identity, f.snapshotID, f.planRef, 0)
			results <- result{execution: execution, err: err}
		}()
	}
	close(ready)
	wg.Wait()
	close(results)

	var first, replay ResearchRunStepExecutionV3
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent BeginResearchRunStepV3: %v", got.err)
		}
		if got.execution.FirstWriter {
			if first.StepID != 0 {
				t.Fatalf("multiple first writers: first=%+v second=%+v", first, got.execution)
			}
			first = got.execution
		} else {
			replay = got.execution
		}
	}
	if first.StepID <= 0 || first.SpendReservationID <= 0 || len(first.Arguments) == 0 {
		t.Fatalf("first writer did not receive sealed execution authority: %+v", first)
	}
	if replay.StepID != first.StepID || replay.SpendReservationID != first.SpendReservationID ||
		len(replay.Arguments) != 0 {
		t.Fatalf("response-loss contender exposed authority: first=%+v replay=%+v", first, replay)
	}
	starts, reservations := f.spendCounts(t)
	if starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
		t.Fatalf("concurrent admission drifted: starts=%d reservations=%d tokens=%v",
			starts, reservations, f.quotaTokens(t))
	}

	// A later response-loss retry must recover the immutable reservation without
	// returning Tool arguments or consuming another provider invocation token.
	later, err := f.begin(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	if later.FirstWriter || later.StepID != first.StepID ||
		later.SpendReservationID != first.SpendReservationID || len(later.Arguments) != 0 {
		t.Fatalf("response-loss replay=%+v first=%+v", later, first)
	}
	if tokens := f.quotaTokens(t); tokens != 9 {
		t.Fatalf("response-loss replay consumed quota again: tokens=%v", tokens)
	}
}

func TestResearchRunSpendQuotaFailsClosedPostgres(t *testing.T) {
	t.Run("missing bucket", func(t *testing.T) {
		f := newResearchRunSpendFixtureV3(t, 1_000_000)
		if _, err := f.store.pool.Exec(t.Context(),
			`DELETE FROM tenant_quota WHERE tenant_id=$1 AND bucket='exa_calls'`,
			f.tenantID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.begin(t, 0); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("missing quota error=%v, want ErrQuotaExceeded", err)
		}
		starts, reservations := f.spendCounts(t)
		if starts != 0 || reservations != 0 {
			t.Fatalf("missing quota left spend authority: starts=%d reservations=%d",
				starts, reservations)
		}
	})

	t.Run("insufficient tokens", func(t *testing.T) {
		f := newResearchRunSpendFixtureV3(t, 1_000_000)
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE tenant_quota SET tokens=0,rate=0,updated_at=now()
			  WHERE tenant_id=$1 AND bucket='exa_calls'`, f.tenantID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.begin(t, 0); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("insufficient quota error=%v, want ErrQuotaExceeded", err)
		}
		starts, reservations := f.spendCounts(t)
		if starts != 0 || reservations != 0 || f.quotaTokens(t) != 0 {
			t.Fatalf("insufficient quota was not atomic: starts=%d reservations=%d tokens=%v",
				starts, reservations, f.quotaTokens(t))
		}
	})
}

func TestResearchRunSpendConcurrentOrdinalsRespectRunBudgetPostgres(t *testing.T) {
	// The receipt-backed fixture spends three micro-USD on the completed planner
	// call. Both frozen Tool grants reserve another 10,000, so a run budget of
	// exactly 10,003 admits one ordinal and serializes the other behind the run
	// lock without pretending planning was free.
	f := newResearchRunSpendFixtureV3(t, 10_003)
	type result struct {
		ordinal   int
		execution ResearchRunStepExecutionV3
		err       error
	}
	ready := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for ordinal := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			execution, err := f.store.BeginResearchRunStepV3(
				t.Context(), f.identity, f.snapshotID, f.planRef, ordinal)
			results <- result{ordinal: ordinal, execution: execution, err: err}
		}()
	}
	close(ready)
	wg.Wait()
	close(results)

	successes, exhausted := 0, 0
	for got := range results {
		switch {
		case got.err == nil:
			successes++
			if !got.execution.FirstWriter || got.execution.Ordinal != got.ordinal ||
				got.execution.ReservedCostMicroUSD != 10_000 {
				t.Fatalf("invalid admitted ordinal: %+v", got)
			}
		case errors.Is(got.err, ErrQuotaExceeded):
			exhausted++
		default:
			t.Fatalf("ordinal %d returned unexpected error: %v", got.ordinal, got.err)
		}
	}
	starts, reservations := f.spendCounts(t)
	if successes != 1 || exhausted != 1 || starts != 1 || reservations != 1 ||
		f.quotaTokens(t) != 9 {
		t.Fatalf("run budget race drifted: success=%d exhausted=%d starts=%d reservations=%d tokens=%v",
			successes, exhausted, starts, reservations, f.quotaTokens(t))
	}
}

func TestResearchRunSpendSettlementControlsLaterBudgetPostgres(t *testing.T) {
	t.Run("actual below reservation retains admitted cost floor", func(t *testing.T) {
		// Tool admission is a permanent quota and cost floor. A provider reporting
		// 4,000 after a 10,000 admission must not release authority for another
		// provider call that the run budget could not originally admit.
		f := newResearchRunSpendFixtureV3(t, 15_000)
		first, err := f.begin(t, 0)
		if err != nil || !first.FirstWriter {
			t.Fatalf("begin first ordinal=%+v err=%v", first, err)
		}
		if _, err := f.begin(t, 1); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("open reservation did not hold full budget: %v", err)
		}
		result := []byte(`{"status":"available","source":"official"}`)
		if _, err := f.store.CommitResearchRunStepEvidenceV3(t.Context(),
			CommitResearchRunStepEvidenceV3Params{
				Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
				Ordinal: 0, Result: result, OriginalSize: len(result),
				TrustType: "external", CostMicroUSD: 4_000,
				ProviderCall: researchProviderCallV3ForTest(
					f.trace(t, 0, first.InvocationID), 4_000),
			}); err != nil {
			t.Fatal(err)
		}
		second, err := f.begin(t, 1)
		if !errors.Is(err, ErrQuotaExceeded) || second.StepID != 0 {
			t.Fatalf("settlement released the permanent Tool floor: second=%+v err=%v", second, err)
		}
		starts, reservations := f.spendCounts(t)
		if starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
			t.Fatalf("Tool cost-floor accounting drifted: starts=%d reservations=%d tokens=%v",
				starts, reservations, f.quotaTokens(t))
		}
	})

	t.Run("actual above reservation persists overrun", func(t *testing.T) {
		// With two open reservations the 20,000 budget would be exactly full.
		// Settling the first at 12,000 must persist that overrun and reject the
		// next 10,000 reservation rather than silently capping it at 10,000.
		f := newResearchRunSpendFixtureV3(t, 20_000)
		first, err := f.begin(t, 0)
		if err != nil || !first.FirstWriter {
			t.Fatalf("begin first ordinal=%+v err=%v", first, err)
		}
		status := 200
		result := []byte(`{"status":"available","source":"official"}`)
		if _, err := f.store.CommitResearchRunStepEvidenceV3(t.Context(),
			CommitResearchRunStepEvidenceV3Params{
				Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
				Ordinal: 0, Result: result, OriginalSize: len(result),
				TrustType: "external", CostMicroUSD: 12_000,
				ProviderCall: ResearchProviderCallV3{
					TraceID:  f.trace(t, 0, first.InvocationID),
					Provider: "exa", UsageQuantity: 10,
					QuotaUnits: researchRunQuotaUnitsV3, HTTPStatus: &status,
					DurationMS: 25, Attempted: true, CostKnown: true,
					CostMicroUSD: 12_000, PricingStatus: "provider_reported",
					CostCurrency: "USD",
				},
			}); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("completed over-cap receipt was accepted: %v", err)
		}
		if _, err := f.store.CommitResearchRunStepV3(t.Context(),
			CommitResearchRunStepV3Params{
				Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
				Ordinal: 0, Phase: ResearchRunStepIndeterminateV3,
				CostMicroUSD: 12_000, ErrorCode: "provider_cost_exceeded",
				ProviderCall: ResearchProviderCallV3{
					TraceID:  f.trace(t, 0, first.InvocationID),
					Provider: "exa", UsageQuantity: 10,
					QuotaUnits: researchRunQuotaUnitsV3, HTTPStatus: &status,
					DurationMS: 25, Attempted: true, CostKnown: true,
					CostMicroUSD: 12_000, PricingStatus: "provider_reported",
					CostCurrency: "USD",
				},
			}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.begin(t, 1); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("actual overrun did not block later ordinal: %v", err)
		}
		var actual int64
		if err := f.store.pool.QueryRow(t.Context(),
			`SELECT actual_cost_micro_usd
			   FROM research_run_step_spend_settlements
			  WHERE tenant_id=$1 AND temporal_run_id=$2 AND step_ordinal=0`,
			f.tenantID, f.identity.TemporalRunID).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		starts, reservations := f.spendCounts(t)
		if actual != 12_000 || starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
			t.Fatalf("overrun was not durable: actual=%d starts=%d reservations=%d tokens=%v",
				actual, starts, reservations, f.quotaTokens(t))
		}
	})

	t.Run("indeterminate keeps conservative reservation", func(t *testing.T) {
		f := newResearchRunSpendFixtureV3(t, 15_000)
		first, err := f.begin(t, 0)
		if err != nil || !first.FirstWriter {
			t.Fatalf("begin first ordinal=%+v err=%v", first, err)
		}
		if _, err := f.store.CommitResearchRunStepV3(t.Context(),
			CommitResearchRunStepV3Params{
				Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
				Ordinal: 0, Phase: ResearchRunStepIndeterminateV3,
				CostMicroUSD: first.ReservedCostMicroUSD,
				ErrorCode:    "provider_outcome_unknown",
				ProviderCall: ResearchProviderCallV3{
					TraceID:  f.trace(t, 0, first.InvocationID),
					Provider: "exa", QuotaUnits: researchRunQuotaUnitsV3,
					DurationMS: 25, Attempted: true, CostKnown: false,
					PricingStatus: "unpriced",
				},
			}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.begin(t, 1); !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("indeterminate outcome released conservative budget: %v", err)
		}
		var outcome string
		var actual int64
		if err := f.store.pool.QueryRow(t.Context(),
			`SELECT outcome,actual_cost_micro_usd
			   FROM research_run_step_spend_settlements
			  WHERE tenant_id=$1 AND temporal_run_id=$2 AND step_ordinal=0`,
			f.tenantID, f.identity.TemporalRunID).Scan(&outcome, &actual); err != nil {
			t.Fatal(err)
		}
		starts, reservations := f.spendCounts(t)
		if outcome != string(ResearchRunStepIndeterminateV3) || actual != 10_000 ||
			starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
			t.Fatalf("indeterminate settlement drifted: outcome=%q actual=%d starts=%d reservations=%d tokens=%v",
				outcome, actual, starts, reservations, f.quotaTokens(t))
		}
	})
}

func TestResearchRunSpendUnattemptedFailureReplayPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	started, err := f.begin(t, 0)
	if err != nil || !started.FirstWriter {
		t.Fatalf("begin unattempted failure=%+v err=%v", started, err)
	}
	params := CommitResearchRunStepV3Params{
		Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
		Ordinal: 0, Phase: ResearchRunStepFailedV3,
		ErrorCode: "route_unavailable",
	}
	first, err := f.store.CommitResearchRunStepV3(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.store.CommitResearchRunStepV3(t.Context(), params)
	if err != nil {
		t.Fatalf("unattempted response-loss replay failed: %v", err)
	}
	if replay != first || replay.CostMicroUSD != 0 ||
		replay.Phase != ResearchRunStepFailedV3 {
		t.Fatalf("unattempted replay=%+v first=%+v", replay, first)
	}
	var toolCalls, settlements int
	var actualQuota float64
	var quotaFloorPolicy int
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM tool_calls
		    WHERE tenant_id=$1 AND research_run_step_spend_reservation_id IS NOT NULL),
		  (SELECT count(*) FROM research_run_step_spend_settlements
		    WHERE tenant_id=$1 AND temporal_run_id=$2),
		  (SELECT actual_quota_units::double precision
		     FROM research_run_step_spend_settlements
		    WHERE tenant_id=$1 AND temporal_run_id=$2),
		  (SELECT quota_floor_policy_version
		     FROM research_run_step_spend_settlements
		    WHERE tenant_id=$1 AND temporal_run_id=$2)`,
		f.tenantID, f.identity.TemporalRunID).Scan(
		&toolCalls, &settlements, &actualQuota, &quotaFloorPolicy); err != nil {
		t.Fatal(err)
	}
	if toolCalls != 0 || settlements != 1 || actualQuota != 0 ||
		quotaFloorPolicy != 1 || f.quotaTokens(t) != 9 {
		t.Fatalf("unattempted replay accounting: calls=%d settlements=%d actual=%v policy=%d tokens=%v",
			toolCalls, settlements, actualQuota, quotaFloorPolicy, f.quotaTokens(t))
	}
}

func TestResearchRunSpendAttemptedFailureExactReplayPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	started, err := f.begin(t, 0)
	if err != nil || !started.FirstWriter {
		t.Fatalf("begin attempted failure=%+v err=%v", started, err)
	}
	status := 400
	params := CommitResearchRunStepV3Params{
		Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
		Ordinal: 0, Phase: ResearchRunStepFailedV3,
		CostMicroUSD: 300, ErrorCode: "provider_rejected",
		ProviderCall: ResearchProviderCallV3{
			TraceID:  f.trace(t, 0, started.InvocationID),
			Provider: "exa", UsageQuantity: 10,
			QuotaUnits: researchRunQuotaUnitsV3, HTTPStatus: &status,
			DurationMS: 25, Attempted: true, CostKnown: true,
			CostMicroUSD: 300, PricingStatus: "provider_reported",
			CostCurrency: "USD",
		},
	}
	first, err := f.store.CommitResearchRunStepV3(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.store.CommitResearchRunStepV3(t.Context(), params)
	if err != nil || replay != first {
		t.Fatalf("attempted failure replay=%+v first=%+v err=%v", replay, first, err)
	}
	for name, mutate := range map[string]func(*CommitResearchRunStepV3Params){
		"trace": func(p *CommitResearchRunStepV3Params) {
			p.ProviderCall.TraceID = "research-spend-rejected-tampered"
		},
		"usage": func(p *CommitResearchRunStepV3Params) { p.ProviderCall.UsageQuantity = 1 },
		"quota": func(p *CommitResearchRunStepV3Params) { p.ProviderCall.QuotaUnits = 2 },
		"cost":  func(p *CommitResearchRunStepV3Params) { p.ProviderCall.CostMicroUSD = 301 },
	} {
		t.Run("tampered "+name, func(t *testing.T) {
			tampered := params
			mutate(&tampered)
			if _, err := f.store.CommitResearchRunStepV3(t.Context(), tampered); err == nil {
				t.Fatalf("tampered %s replay was accepted", name)
			}
		})
	}
	var calls, settlements int
	var actualQuota float64
	var actualCost int64
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM tool_calls
		    WHERE tenant_id=$1 AND research_run_step_spend_reservation_id IS NOT NULL),
		  (SELECT count(*) FROM research_run_step_spend_settlements
		    WHERE tenant_id=$1 AND temporal_run_id=$2),
		  actual_quota_units::double precision,actual_cost_micro_usd
		 FROM research_run_step_spend_settlements
		 WHERE tenant_id=$1 AND temporal_run_id=$2`,
		f.tenantID, f.identity.TemporalRunID).Scan(
		&calls, &settlements, &actualQuota, &actualCost); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || settlements != 1 || actualQuota != 1 || actualCost != 300 ||
		f.quotaTokens(t) != 9 {
		t.Fatalf("attempted failure accounting: calls=%d settlements=%d quota=%v cost=%d tokens=%v",
			calls, settlements, actualQuota, actualCost, f.quotaTokens(t))
	}
}

func TestResearchRunSpendRejectsTraceRelabeledToAnotherOrdinalPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	first, err := f.begin(t, 0)
	if err != nil || !first.FirstWriter {
		t.Fatalf("begin source ordinal=%+v err=%v", first, err)
	}
	second, err := f.begin(t, 1)
	if err != nil || !second.FirstWriter {
		t.Fatalf("begin target ordinal=%+v err=%v", second, err)
	}

	// Simulate a coordinator that changes only its execution ordinal while
	// retaining the provider receipt produced for the original invocation.
	// Store must derive the expected claim from persisted plan step 1 rather
	// than trusting either coordinator field.
	params := CommitResearchRunStepV3Params{
		Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
		Ordinal: 1, Phase: ResearchRunStepIndeterminateV3,
		CostMicroUSD: 300, ErrorCode: "provider_rejected",
		ProviderCall: researchProviderCallV3ForTest(
			f.trace(t, 0, first.InvocationID), 300),
	}
	if _, err := f.store.CommitResearchRunStepV3(t.Context(), params); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("receipt relabeled from ordinal 0 to 1 was accepted: %v", err)
	}
	result := []byte(`{"state":"available"}`)
	if _, err := f.store.CommitResearchRunStepEvidenceV3(t.Context(),
		CommitResearchRunStepEvidenceV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
			Ordinal: 1, Result: result, OriginalSize: len(result),
			TrustType: "external", CostMicroUSD: 300,
			ProviderCall: researchProviderCallV3ForTest(
				f.trace(t, 0, first.InvocationID), 300),
		}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("evidence receipt relabeled from ordinal 0 to 1 was accepted: %v", err)
	}

	var terminalCount int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_run_steps
		  WHERE tenant_id=$1 AND temporal_run_id=$2 AND step_ordinal=1
		    AND phase<>'started'`,
		f.tenantID, f.identity.TemporalRunID).Scan(&terminalCount); err != nil {
		t.Fatal(err)
	}
	if terminalCount != 0 {
		t.Fatalf("rejected relabel persisted %d terminal rows", terminalCount)
	}
}

func waitForResearchSpendAdvisoryWaitV3(t *testing.T, st *Store, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(),
			`SELECT EXISTS (
			   SELECT 1 FROM pg_locks
			    WHERE pid=$1 AND locktype='advisory' AND NOT granted
			 )`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("backend %d did not wait on the run spend advisory lock", pid)
}

func researchSpendStoreWithPIDV3(
	t *testing.T, st *Store, pid chan<- int,
) *Store {
	t.Helper()
	clone := *st
	clone.beginResearchTx = func(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
		tx, err := st.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		var backendPID int
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			return nil, err
		}
		pid <- backendPID
		return tx, nil
	}
	return &clone
}

func TestResearchRunSpendConcurrentSettlementPrecedesBeginPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 20_000)
	first, err := f.begin(t, 0)
	if err != nil || !first.FirstWriter {
		t.Fatalf("begin first ordinal=%+v err=%v", first, err)
	}

	// Hold the exact run ledger lock, queue settlement first, then queue Begin.
	// This makes the lock ordering observable and deterministic: after release,
	// the 12,000 actual settlement must become visible before ordinal 1 performs
	// its 10,000 admission check against the 20,000 run ceiling.
	blocker, err := f.store.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = blocker.Rollback(context.Background())
		}
	})
	if _, err := blocker.Exec(t.Context(),
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		"research-spend/v3:"+f.identity.TemporalRunID+":budget"); err != nil {
		t.Fatal(err)
	}

	type settlementResult struct{ err error }
	settlementPID := make(chan int, 1)
	settlementDone := make(chan settlementResult, 1)
	settlementStore := researchSpendStoreWithPIDV3(t, f.store, settlementPID)
	status := 200
	go func() {
		_, commitErr := settlementStore.CommitResearchRunStepV3(t.Context(),
			CommitResearchRunStepV3Params{
				Identity: f.identity, RunSnapshotID: f.snapshotID, PlanRef: f.planRef,
				Ordinal: 0, Phase: ResearchRunStepIndeterminateV3,
				CostMicroUSD: 12_000, ErrorCode: "provider_cost_exceeded",
				ProviderCall: ResearchProviderCallV3{
					TraceID:  f.trace(t, 0, first.InvocationID),
					Provider: "exa", UsageQuantity: 10,
					QuotaUnits: researchRunQuotaUnitsV3, HTTPStatus: &status,
					DurationMS: 25, Attempted: true, CostKnown: true,
					CostMicroUSD: 12_000, PricingStatus: "provider_reported",
					CostCurrency: "USD",
				},
			})
		settlementDone <- settlementResult{err: commitErr}
	}()
	settlementBackend := <-settlementPID
	waitForResearchSpendAdvisoryWaitV3(t, f.store, settlementBackend)

	type beginResult struct {
		execution ResearchRunStepExecutionV3
		err       error
	}
	beginPID := make(chan int, 1)
	beginDone := make(chan beginResult, 1)
	beginStore := researchSpendStoreWithPIDV3(t, f.store, beginPID)
	go func() {
		execution, beginErr := beginStore.BeginResearchRunStepV3(
			t.Context(), f.identity, f.snapshotID, f.planRef, 1)
		beginDone <- beginResult{execution: execution, err: beginErr}
	}()
	beginBackend := <-beginPID
	waitForResearchSpendAdvisoryWaitV3(t, f.store, beginBackend)

	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	released = true
	if settled := <-settlementDone; settled.err != nil {
		t.Fatalf("ordered settlement failed: %v", settled.err)
	}
	begin := <-beginDone
	if !errors.Is(begin.err, ErrQuotaExceeded) || begin.execution.StepID != 0 {
		t.Fatalf("Begin did not observe settled overrun: execution=%+v err=%v",
			begin.execution, begin.err)
	}

	var ledgerCost int64
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT COALESCE(sum(CASE WHEN settlement.id IS NULL
		                         THEN reservation.reserved_cost_micro_usd
		                         ELSE settlement.actual_cost_micro_usd END),0)::bigint
		   FROM research_run_step_spend_reservations reservation
		   LEFT JOIN research_run_step_spend_settlements settlement
		     ON settlement.reservation_id=reservation.id
		  WHERE reservation.tenant_id=$1 AND reservation.temporal_run_id=$2`,
		f.tenantID, f.identity.TemporalRunID).Scan(&ledgerCost); err != nil {
		t.Fatal(err)
	}
	starts, reservations := f.spendCounts(t)
	if ledgerCost != 12_000 || starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
		t.Fatalf("ordered overrun ledger drifted: cost=%d starts=%d reservations=%d tokens=%v",
			ledgerCost, starts, reservations, f.quotaTokens(t))
	}
}

func TestResearchRunSpendPlanExceedingMaxToolCallsFailsClosedPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 20_000, 1, false)
	plan := researchRunPlanFixtureV3(t, f.definitionDigest,
		f.snapshotRef.CapabilityCatalogDigest, f.snapshotRef.ToolPolicyDigest,
		"Kimi pricing")
	if len(plan.Steps) != 2 {
		t.Fatalf("test requires a two-step plan, got %d", len(plan.Steps))
	}
	if _, err := f.store.CreateOrGetResearchRunPlanV3(t.Context(),
		CreateOrGetResearchRunPlanV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotID, Plan: plan,
		}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("two-step plan under MaxToolCalls=1 did not fail validation: %v", err)
	}
	var plans, starts, reservations int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		   (SELECT count(*) FROM research_run_plans
		     WHERE tenant_id=$1 AND temporal_run_id=$2),
		   (SELECT count(*) FROM research_run_steps
		     WHERE tenant_id=$1 AND temporal_run_id=$2),
		   (SELECT count(*) FROM research_run_step_spend_reservations
		     WHERE tenant_id=$1 AND temporal_run_id=$2)`,
		f.tenantID, f.identity.TemporalRunID).Scan(&plans, &starts, &reservations); err != nil {
		t.Fatal(err)
	}
	if plans != 0 || starts != 0 || reservations != 0 || f.quotaTokens(t) != 10 {
		t.Fatalf("over-budget plan left authority: plans=%d starts=%d reservations=%d tokens=%v",
			plans, starts, reservations, f.quotaTokens(t))
	}
}

func researchSpendVisibleAsExecutorV3(
	t *testing.T, st *Store, bearerHex string,
	scopeTenantID, scopeUserID, reservationID, settlementID int64,
) (int, int) {
	t.Helper()
	tx, err := st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.research_run_capability_v1',$1,true),
		        set_config('app.tenant_id',$2,true),set_config('app.user_id',$3,true)`,
		bearerHex, strconv.FormatInt(scopeTenantID, 10),
		strconv.FormatInt(scopeUserID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE `+researchRuntimeCapabilityRole); err != nil {
		t.Fatal(err)
	}
	var reservations, settlements int
	if err := tx.QueryRow(t.Context(),
		`SELECT
		   (SELECT count(*) FROM research_run_step_spend_reservations WHERE id=$1),
		   (SELECT count(*) FROM research_run_step_spend_settlements WHERE id=$2)`,
		reservationID, settlementID).Scan(&reservations, &settlements); err != nil {
		t.Fatal(err)
	}
	return reservations, settlements
}

func assertResearchSpendCloneInsertDeniedV3(
	t *testing.T, st *Store, scopeTenantID, scopeUserID int64,
	table string, canonicalRow []byte,
) {
	t.Helper()
	tx, err := st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		strconv.FormatInt(scopeTenantID, 10), strconv.FormatInt(scopeUserID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE `+researchRuntimeCapabilityRole); err != nil {
		t.Fatal(err)
	}
	var insertSQL string
	switch table {
	case "reservation":
		insertSQL = `
			INSERT INTO research_run_step_spend_reservations (
			  tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
			  temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
			  request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
			  reserved_cost_micro_usd,schema_version
			)
			SELECT tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
			       temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
			       request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
			       reserved_cost_micro_usd,schema_version
			  FROM jsonb_populate_record(
			         NULL::research_run_step_spend_reservations,$1::jsonb)`
	case "settlement":
		insertSQL = `
			INSERT INTO research_run_step_spend_settlements (
			  tenant_id,user_id,task_id,run_snapshot_id,plan_id,reservation_id,
			  terminal_step_id,tool_call_id,temporal_run_id,plan_digest,step_ordinal,
			  invocation_id,tool_name,request_digest,outcome,actual_quota_units,
			  actual_cost_micro_usd,pricing_status,cost_currency,schema_version
			)
			SELECT tenant_id,user_id,task_id,run_snapshot_id,plan_id,reservation_id,
			       terminal_step_id,tool_call_id,temporal_run_id,plan_digest,step_ordinal,
			       invocation_id,tool_name,request_digest,outcome,actual_quota_units,
			       actual_cost_micro_usd,pricing_status,cost_currency,schema_version
			  FROM jsonb_populate_record(
			         NULL::research_run_step_spend_settlements,$1::jsonb)`
	default:
		t.Fatalf("unknown spend ledger table %q", table)
	}
	_, err = tx.Exec(t.Context(), insertSQL, canonicalRow)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("forged %s INSERT error=%v, want RLS insufficient_privilege", table, err)
	}
}

func TestResearchRunSpendLedgersEnforceTenantAndUserRLSPostgres(t *testing.T) {
	owner := newResearchRunSpendFixtureV3(t, 20_000)
	foreign := newResearchRunSpendFixtureV3(t, 20_000)
	started, err := owner.begin(t, 0)
	if err != nil || !started.FirstWriter {
		t.Fatalf("begin owner spend=%+v err=%v", started, err)
	}
	result := []byte(`{"status":"available","source":"official"}`)
	if _, err := owner.store.CommitResearchRunStepEvidenceV3(t.Context(),
		CommitResearchRunStepEvidenceV3Params{
			Identity: owner.identity, RunSnapshotID: owner.snapshotID, PlanRef: owner.planRef,
			Ordinal: 0, Result: result, OriginalSize: len(result),
			TrustType: "external", CostMicroUSD: 4_000,
			ProviderCall: researchProviderCallV3ForTest(
				owner.trace(t, 0, started.InvocationID), 4_000),
		}); err != nil {
		t.Fatal(err)
	}

	var reservationID, settlementID, toolCallID int64
	var reservationRow, settlementRow []byte
	if err := owner.store.pool.QueryRow(t.Context(),
		`SELECT reservation.id,settlement.id,settlement.tool_call_id,
		        to_jsonb(reservation),to_jsonb(settlement)
		   FROM research_run_step_spend_reservations reservation
		   JOIN research_run_step_spend_settlements settlement
		     ON settlement.reservation_id=reservation.id
		  WHERE reservation.tenant_id=$1 AND reservation.temporal_run_id=$2`,
		owner.tenantID, owner.identity.TemporalRunID).Scan(
		&reservationID, &settlementID, &toolCallID,
		&reservationRow, &settlementRow); err != nil {
		t.Fatal(err)
	}
	capability, err := owner.store.resolveResearchRunCapabilityV1(
		t.Context(), owner.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	bearerHex := hex.EncodeToString(capability.raw[:])

	if reservations, settlements := researchSpendVisibleAsExecutorV3(t, owner.store, bearerHex,
		owner.tenantID, owner.userID, reservationID, settlementID); reservations != 1 || settlements != 1 {
		t.Fatalf("owner scope cannot read its spend ledger: reservations=%d settlements=%d",
			reservations, settlements)
	}
	for _, scope := range []struct {
		name     string
		tenantID int64
		userID   int64
	}{
		{name: "cross tenant", tenantID: foreign.tenantID, userID: foreign.userID},
		{name: "same tenant cross user", tenantID: owner.tenantID, userID: foreign.userID},
	} {
		t.Run(scope.name, func(t *testing.T) {
			if reservations, settlements := researchSpendVisibleAsExecutorV3(t, owner.store, bearerHex,
				scope.tenantID, scope.userID, reservationID, settlementID); reservations != 0 || settlements != 0 {
				t.Fatalf("forged scope exposed spend ledger: reservations=%d settlements=%d",
					reservations, settlements)
			}
			assertResearchSpendCloneInsertDeniedV3(t, owner.store,
				scope.tenantID, scope.userID, "reservation", reservationRow)
			assertResearchSpendCloneInsertDeniedV3(t, owner.store,
				scope.tenantID, scope.userID, "settlement", settlementRow)
		})
	}
	t.Run("reset role and forged purge flag cannot delete immutable Tool evidence", func(t *testing.T) {
		tx, err := owner.store.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		if _, err := tx.Exec(t.Context(), `
			SELECT set_config('app.tenant_id',$1,true),
			       set_config('app.user_id',$2,true),
			       set_config('app.tenant_purge','on',true)`,
			strconv.FormatInt(owner.tenantID, 10),
			strconv.FormatInt(owner.userID, 10)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
			t.Fatal(err)
		}
		// The pool authenticates as the schema owner. Prove that recovering that
		// identity is still not a selective evidence-deletion capability.
		if _, err := tx.Exec(t.Context(), `RESET ROLE`); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(t.Context(), `DELETE FROM tool_calls WHERE id=$1`, toolCallID)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("forged purge flag delete error=%v, want 42501", err)
		}
	})
}
