package store

import (
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func ensureResearchLLMPriceV3(t *testing.T, st *Store) {
	t.Helper()
	var exists bool
	if err := st.pool.QueryRow(t.Context(),
		`SELECT EXISTS (
		   SELECT 1 FROM provider_price_rules
		    WHERE provider='deepseek' AND resource='strong-model'
		      AND meter='llm_tokens' AND effective_to IS NULL
		 )`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		return
	}
	// Keep the synthetic rule active even when a virtualized CI clock steps
	// backwards between fixture setup and the admission transaction. Production
	// prices remain versioned by their real effective timestamps.
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO provider_price_rules (
		     provider,resource,meter,currency,input_cache_hit_per_million,
		     input_cache_miss_per_million,output_per_million,effective_from,
		     source_url,note,created_by,change_id,request_hash
		 ) VALUES ('deepseek','strong-model','llm_tokens','USD',0.1,1.0,2.0,
		           TIMESTAMPTZ '2000-01-01 00:00:00+00','https://example.test/pricing',
		           'V3 research model spend test',NULL,$1,$2)`,
		"research-llm-v3-test-"+uuid.NewString(),
		"research-llm-v3-test-"+uuid.NewString()); err != nil {
		t.Fatal(err)
	}
}

func researchPlannerBeginV3(f researchRunSpendFixtureV3, round int, userPrompt string) BeginResearchRunLLMSpendV3Params {
	return BeginResearchRunLLMSpendV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef,
		Stage: ResearchRunLLMStagePlannerV3, RoundOrdinal: round,
		SystemPrompt: "Plan from the trusted task manual.", UserPrompt: userPrompt,
	}
}

func researchLLMCallForTestV3(
	identity types.RunIdentity, snapshot types.ResearchRunSnapshotRefV3,
	reservation ResearchRunLLMSpendReservationV3, systemPrompt, userPrompt string,
) types.LLMCall {
	tenantID, userID, snapshotID := identity.TenantID, identity.UserID, snapshot.SnapshotID
	temperature, maxTokens := reservation.Temperature, reservation.MaxTokens
	return types.LLMCall{
		RunSnapshotID: &snapshotID, TenantID: &tenantID, UserID: &userID,
		TraceID: reservation.TraceID, SpanName: researchRunLLMSpanV3(reservation.Stage),
		RefType: types.RefType(researchRunLLMRefTypeV3), RefID: &snapshotID,
		Provider: reservation.Provider, Model: reservation.Model,
		SystemPrompt: systemPrompt, UserPrompt: userPrompt,
		Temperature: &temperature, MaxTokens: &maxTokens,
	}
}

func TestResearchRunLLMSpendV3ConcurrentBeginAndExactSettlementPostgres(t *testing.T) {
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE tenant_quota SET tokens=2000000,rate=0,burst=2000000,updated_at=now()
		  WHERE tenant_id=$1 AND bucket='llm_tokens'`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	results := make([]ResearchRunLLMSpendReservationV3, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errs[index] = f.store.BeginResearchRunLLMSpendV3(
				t.Context(), researchPlannerBeginV3(f, 1, "plan Kimi official checks"))
		}(i)
	}
	close(start)
	wg.Wait()
	firstWriters := 0
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("begin[%d]: %v", i, errs[i])
		}
		if results[i].ReservationID != results[0].ReservationID ||
			results[i].TraceID != results[0].TraceID {
			t.Fatalf("concurrent reservation drift: %+v vs %+v", results[i], results[0])
		}
		if results[i].FirstWriter {
			firstWriters++
		}
	}
	if firstWriters != 1 {
		t.Fatalf("first writers=%d, want 1", firstWriters)
	}
	reservation := results[0]
	call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
		"Plan from the trusted task manual.", "plan Kimi official checks")
	call.Model = "provider-reported-model-alias"
	call.Completion = `{"steps":[]}`
	call.PromptTokens, call.CompletionTokens, call.LatencyMs = 100, 25, 50
	receipt, signedParams, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: reservation.ReservationID, Call: call,
			DisableThinking: reservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		})
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Settled || receipt.LLMCallID <= 0 || receipt.ActualPromptTokens != 100 ||
		receipt.ActualCompletionTokens != 25 || receipt.ActualCostMicroUSD != 150 {
		t.Fatalf("receipt=%+v", receipt)
	}
	exactReplay, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store, signedParams)
	if err != nil || exactReplay.LLMCallID != receipt.LLMCallID {
		t.Fatalf("exact settlement replay=%+v err=%v", exactReplay, err)
	}
	mutatedReplay := call
	mutatedReplay.LatencyMs++
	mutatedParams := signedParams
	mutatedParams.Call = mutatedReplay
	if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store, mutatedParams); err == nil {
		t.Fatal("settlement replay accepted mutated latency metadata")
	}
	loaded, found, err := f.store.LoadResearchRunLLMReceiptV3(t.Context(), f.identity,
		f.snapshotRef, ResearchRunLLMStagePlannerV3, 1)
	if err != nil || !found || !loaded.Settled || loaded.Call.Completion != call.Completion ||
		loaded.Call.Model != call.Model || loaded.PricingStatus != "estimated" ||
		loaded.Reservation.ReservationID != reservation.ReservationID {
		t.Fatalf("loaded=%+v found=%v err=%v", loaded, found, err)
	}
	var reservations, settlements, calls int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		   (SELECT count(*) FROM research_run_llm_spend_reservations
		     WHERE run_snapshot_id=$1 AND stage='planner' AND round_ordinal=1),
		   (SELECT count(*) FROM research_run_llm_spend_settlements
		     WHERE run_snapshot_id=$1 AND stage='planner' AND round_ordinal=1),
		   (SELECT count(*) FROM llm_calls WHERE research_run_llm_spend_reservation_id=$2)`,
		f.snapshotID, reservation.ReservationID).Scan(&reservations, &settlements, &calls); err != nil {
		t.Fatal(err)
	}
	if reservations != 1 || settlements != 1 || calls != 1 {
		t.Fatalf("ledger counts=%d/%d/%d", reservations, settlements, calls)
	}
}

func TestResearchRunLLMSpendV3UnknownAndUnattemptedUsagePostgres(t *testing.T) {
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE tenant_quota SET tokens=2000000,rate=0,burst=2000000,updated_at=now()
		  WHERE tenant_id=$1 AND bucket='llm_tokens'`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	unknownPrompt := "planner timeout after send"
	unknown, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, unknownPrompt))
	if err != nil {
		t.Fatal(err)
	}
	unknownCall := researchLLMCallForTestV3(f.identity, f.snapshotRef, unknown,
		"Plan from the trusted task manual.", unknownPrompt)
	unknownCall.Error = "provider response lost"
	unknownReceipt, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: unknown.ReservationID, Call: unknownCall,
			DisableThinking: unknown.DisableThinking,
			Attempted:       true, Outcome: ResearchRunLLMIndeterminateV3,
			ErrorCode: string(types.CodeLLMUnavailable),
		})
	if err != nil {
		t.Fatal(err)
	}
	if unknownReceipt.UsageKnown || unknownReceipt.ActualPromptTokens != 0 ||
		unknownReceipt.ActualCompletionTokens != 0 ||
		unknownReceipt.ActualCostMicroUSD != unknown.ReservedCostMicroUSD {
		t.Fatalf("unknown receipt=%+v", unknownReceipt)
	}
	unattemptedPrompt := "planner rejected before send"
	unattempted, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 2, unattemptedPrompt))
	if err != nil {
		t.Fatal(err)
	}
	unattemptedCall := researchLLMCallForTestV3(f.identity, f.snapshotRef, unattempted,
		"Plan from the trusted task manual.", unattemptedPrompt)
	unattemptedCall.Error = "local gate rejected"
	unattemptedReceipt, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: unattempted.ReservationID, Call: unattemptedCall,
			DisableThinking:     unattempted.DisableThinking,
			DefinitelyZeroUsage: true, Outcome: ResearchRunLLMFailedV3,
			ErrorCode: string(types.CodeQuotaExceeded),
		})
	if err != nil {
		t.Fatal(err)
	}
	if unattemptedReceipt.Attempted || unattemptedReceipt.LLMCallID != 0 ||
		unattemptedReceipt.ActualCostMicroUSD != 0 {
		t.Fatalf("unattempted receipt=%+v", unattemptedReceipt)
	}
	zeroPrompt := "provider rejected request before generation"
	zero, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 3, zeroPrompt))
	if err != nil {
		t.Fatal(err)
	}
	zeroCall := researchLLMCallForTestV3(f.identity, f.snapshotRef, zero,
		"Plan from the trusted task manual.", zeroPrompt)
	zeroCall.Error = "provider returned 429"
	zeroReceipt, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: zero.ReservationID, Call: zeroCall,
			DisableThinking: zero.DisableThinking,
			Attempted:       true, DefinitelyZeroUsage: true,
			Outcome:   ResearchRunLLMFailedV3,
			ErrorCode: string(types.CodeLLMRateLimit),
		})
	if err != nil {
		t.Fatal(err)
	}
	if !zeroReceipt.Attempted || !zeroReceipt.DefinitelyZeroUsage ||
		zeroReceipt.LLMCallID <= 0 || zeroReceipt.ActualCostMicroUSD != 0 {
		t.Fatalf("definite-zero receipt=%+v", zeroReceipt)
	}
}

func TestResearchRunLLMSpendV3PersistsKnownOverReservationAndBlocksNextRoundPostgres(t *testing.T) {
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE tenant_quota SET tokens=2000000,rate=0,burst=2000000,updated_at=now()
		  WHERE tenant_id=$1 AND bucket='llm_tokens'`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	prompt := "provider tokenizer exceeded the conservative rune estimate"
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, prompt))
	if err != nil {
		t.Fatal(err)
	}
	call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
		"Plan from the trusted task manual.", prompt)
	call.Completion = `{"steps":[]}`
	call.PromptTokens, call.CompletionTokens = 995_000, 4_096
	receipt, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: reservation.ReservationID, Call: call,
			DisableThinking: reservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		})
	if err != nil {
		t.Fatalf("known over-reservation evidence was not persisted: %v", err)
	}
	if receipt.ActualCostMicroUSD <= reservation.ReservedCostMicroUSD ||
		receipt.ActualCostMicroUSD <= 1_000_000 {
		t.Fatalf("receipt did not exceed reservation/run budget: %+v vs %+v",
			receipt, reservation)
	}
	if _, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 2, "must be blocked after actual overspend")); err == nil {
		t.Fatal("next planner round passed after exact known spend exceeded budget")
	}
}

func TestResearchRunLLMSpendV3SynthesisDoesNotConsumePlannerBudgetPostgres(t *testing.T) {
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	var plannerReservationsBefore int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_run_llm_spend_reservations
		  WHERE run_snapshot_id=$1 AND counts_against_planner_budget`,
		f.snapshotRef.SnapshotID).Scan(&plannerReservationsBefore); err != nil {
		t.Fatal(err)
	}
	reservation, err := f.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, SubjectID: prepared.Synthesis.ID,
			SystemPrompt: "Synthesize without Tools.",
			UserPrompt:   string(prepared.Synthesis.ContextPayload),
		})
	if err != nil {
		t.Fatal(err)
	}
	if reservation.CountsAgainstPlannerBudget || reservation.ReservedPlannerTokens != 0 {
		t.Fatalf("synthesis reservation consumed planner budget: %+v", reservation)
	}
	var plannerReservations int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_run_llm_spend_reservations
		  WHERE run_snapshot_id=$1 AND counts_against_planner_budget`,
		f.snapshotRef.SnapshotID).Scan(&plannerReservations); err != nil {
		t.Fatal(err)
	}
	if plannerReservations != plannerReservationsBefore {
		t.Fatalf("planner reservations changed by synthesis: before=%d after=%d",
			plannerReservationsBefore, plannerReservations)
	}
}
