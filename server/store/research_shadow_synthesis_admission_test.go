package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/types"
)

func TestResearchShadowBriefSynthesisAdmissionPostgres(t *testing.T) {
	t.Run("exact prepared partial shadow enters synthesis spending", func(t *testing.T) {
		fixture, synthesis, reservation := preparedPartialResearchShadowBriefV3(t)
		assertScheduleExecutionModeV3(t, fixture, types.ExecutionModeCompiled)

		handle := researchShadowBriefClaimV3(fixture, synthesis, reservation)
		claim, err := fixture.st.ClaimResearchBriefSynthesisV3(t.Context(),
			handle)
		if err != nil || !claim.Claimed ||
			claim.Synthesis.Status != ResearchBriefSynthesisSpendingV3 ||
			claim.ReceiptState != ResearchBriefLLMReceiptPendingV3 {
			t.Fatalf("exact prepared shadow claim=%+v err=%v", claim, err)
		}
		assertScheduleExecutionModeV3(t, fixture, types.ExecutionModeCompiled)

		if _, err := fixture.st.pool.Exec(t.Context(),
			`DELETE FROM research_v3_prepared_definition_heads
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			fixture.tenantID, fixture.userID, fixture.taskID); err != nil {
			t.Fatal(err)
		}
		replay, err := fixture.st.ClaimResearchBriefSynthesisV3(t.Context(), handle)
		if err != nil || replay.Claimed ||
			replay.Synthesis.Status != ResearchBriefSynthesisSpendingV3 ||
			replay.ReceiptState != ResearchBriefLLMReceiptPendingV3 {
			t.Fatalf("response-loss replay after sidecar removal=%+v err=%v", replay, err)
		}
		var reservations int
		if err := fixture.st.pool.QueryRow(t.Context(),
			`SELECT count(*) FROM research_run_llm_spend_reservations
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
			reservation.ReservationID, fixture.tenantID, fixture.userID,
		).Scan(&reservations); err != nil {
			t.Fatal(err)
		}
		if reservations != 1 {
			t.Fatalf("response-loss replay changed LLM reservations: %d", reservations)
		}
	})

	t.Run("prepared binding drift fails closed before spending", func(t *testing.T) {
		fixture, synthesis, reservation := preparedPartialResearchShadowBriefV3(t)
		if _, err := fixture.st.pool.Exec(t.Context(),
			`DELETE FROM research_v3_prepared_definition_heads
			  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
			fixture.tenantID, fixture.userID, fixture.taskID); err != nil {
			t.Fatal(err)
		}

		claim, err := fixture.st.ClaimResearchBriefSynthesisV3(t.Context(),
			researchShadowBriefClaimV3(fixture, synthesis, reservation))
		if err == nil || claim.Claimed {
			t.Fatalf("drifted prepared binding admitted claim=%+v err=%v", claim, err)
		}
		assertResearchBriefSynthesisStatusV3(t, fixture, synthesis.ID,
			ResearchBriefSynthesisPreparedV3)
	})

	t.Run("non exact shadow cannot freeze a compiled schedule", func(t *testing.T) {
		fixture, _, _ := preparedPartialResearchShadowBriefV3(t)
		identity := fixture.identity
		identity.TemporalWorkflowID = "research-v3-shadow-" + strings.Repeat("a", 63)
		identity.TemporalRunID = "run-" + uuid.NewString()
		if ref, err := fixture.st.CreateOrGetResearchRunSnapshotV3(
			t.Context(), identity, testCompiledRunPolicyV1(t),
			testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t),
		); err == nil || ref.SnapshotID != 0 {
			t.Fatalf("non-exact shadow froze compiled schedule ref=%+v err=%v", ref, err)
		}
	})

	t.Run("cross tenant claim fails closed", func(t *testing.T) {
		fixture, synthesis, reservation := preparedPartialResearchShadowBriefV3(t)
		handle := researchShadowBriefClaimV3(fixture, synthesis, reservation)
		handle.Identity.TenantID++
		claim, err := fixture.st.ClaimResearchBriefSynthesisV3(t.Context(), handle)
		if err == nil || claim.Claimed {
			t.Fatalf("cross-tenant shadow admitted claim=%+v err=%v", claim, err)
		}
		assertResearchBriefSynthesisStatusV3(t, fixture, synthesis.ID,
			ResearchBriefSynthesisPreparedV3)
	})

	t.Run("scoped capability rejects a foreign snapshot id", func(t *testing.T) {
		fixture, _, _ := preparedPartialResearchShadowBriefV3(t)
		foreignIdentity := fixture.identity
		foreignIdentity.TemporalRunID = "run-foreign-" + uuid.NewString()
		foreignRef, err := fixture.st.CreateOrGetResearchRunSnapshotV3(
			t.Context(), foreignIdentity, testCompiledRunPolicyV1(t),
			testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t))
		if err != nil || foreignRef.SnapshotID <= 0 ||
			foreignRef.SnapshotID == fixture.snapshotRef.SnapshotID {
			t.Fatalf("create foreign run snapshot=%+v err=%v", foreignRef, err)
		}
		tx, _, err := fixture.st.beginScopedResearchRunTransactionV3(
			t.Context(), pgx.TxOptions{}, fixture.identity,
			fixture.snapshotRef.SnapshotID)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		var admitted bool
		err = tx.QueryRow(t.Context(),
			`SELECT authorize_research_run_effect_cap_v1($1)`,
			foreignRef.SnapshotID).Scan(&admitted)
		var pgErr *pgconn.PgError
		if err == nil || admitted || !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("foreign snapshot admitted=%v err=%v", admitted, err)
		}
	})

	t.Run("formal snapshot cannot synthesize after schedule returns to compiled", func(t *testing.T) {
		spend := newResearchRunSpendFixtureV3(t, 1_000_000)
		fixture, synthesis, reservation := preparedPartialResearchBriefV3(t, spend)
		if _, err := fixture.st.pool.Exec(t.Context(),
			`UPDATE schedules SET execution_mode='compiled',
			 approved_definition_version=NULL,approved_definition_digest=NULL
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
			fixture.taskID, fixture.tenantID, fixture.userID); err != nil {
			t.Fatal(err)
		}
		claim, err := fixture.st.ClaimResearchBriefSynthesisV3(t.Context(),
			researchShadowBriefClaimV3(fixture, synthesis, reservation))
		if err == nil || claim.Claimed {
			t.Fatalf("formal compiled schedule admitted claim=%+v err=%v", claim, err)
		}
		assertResearchBriefSynthesisStatusV3(t, fixture, synthesis.ID,
			ResearchBriefSynthesisPreparedV3)
	})

	t.Run("claim takes schema fence before brief relations", func(t *testing.T) {
		fixture, synthesis, reservation := preparedPartialResearchShadowBriefV3(t)
		blocker, err := fixture.st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		released := false
		defer func() {
			if !released {
				_ = blocker.Rollback(context.Background())
			}
		}()
		if _, err := blocker.Exec(t.Context(),
			`SELECT pg_advisory_xact_lock($1)`, pushEffectSchemaFenceKey); err != nil {
			t.Fatal(err)
		}

		pid := make(chan int, 1)
		claimStore := *fixture.st
		beginResearchTx := fixture.st.beginResearchTx
		claimStore.beginResearchTx = func(
			ctx context.Context, options pgx.TxOptions,
		) (pgx.Tx, error) {
			tx, err := beginResearchTx(ctx, options)
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
		type claimResult struct {
			claim ClaimResearchBriefSynthesisV3Result
			err   error
		}
		done := make(chan claimResult, 1)
		go func() {
			claim, claimErr := claimStore.ClaimResearchBriefSynthesisV3(
				t.Context(), researchShadowBriefClaimV3(fixture, synthesis, reservation))
			done <- claimResult{claim: claim, err: claimErr}
		}()
		backendPID := <-pid
		waitForResearchSpendAdvisoryWaitV3(t, fixture.st, backendPID)
		var briefLocks int
		if err := fixture.st.pool.QueryRow(t.Context(), `
			SELECT count(*) FROM pg_locks lock
			JOIN pg_class relation ON relation.oid=lock.relation
			WHERE lock.pid=$1 AND lock.granted
			  AND relation.relname='research_brief_syntheses'`,
			backendPID).Scan(&briefLocks); err != nil {
			t.Fatal(err)
		}
		if briefLocks != 0 {
			t.Fatalf("claim held %d Brief relation locks while waiting for schema fence",
				briefLocks)
		}
		if _, err := blocker.Exec(t.Context(),
			`SET LOCAL lock_timeout='5s';
			 LOCK TABLE research_brief_syntheses IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatalf("claim took brief relation before schema fence: %v", err)
		}
		if err := blocker.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		released = true
		select {
		case got := <-done:
			if got.err != nil || !got.claim.Claimed {
				t.Fatalf("claim after schema migration fence=%+v err=%v", got.claim, got.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("claim did not resume after schema migration fence")
		}
	})

	t.Run("formal workflow cannot freeze a compiled schedule", func(t *testing.T) {
		fixture, _, _ := preparedPartialResearchShadowBriefV3(t)
		identity := fixture.identity
		identity.TemporalWorkflowID = "workflow-formal-" + uuid.NewString()
		identity.TemporalRunID = "run-" + uuid.NewString()
		if ref, err := fixture.st.CreateOrGetResearchRunSnapshotV3(
			t.Context(), identity, testCompiledRunPolicyV1(t),
			testResearchToolPolicyStoreV3(t), testResearchModelPolicyStoreV3(t),
		); err == nil || ref.SnapshotID != 0 {
			t.Fatalf("formal workflow froze compiled schedule ref=%+v err=%v", ref, err)
		}
	})
}

func preparedPartialResearchShadowBriefV3(
	t *testing.T,
) (researchBriefFixtureV3, ResearchBriefSynthesisV3, ResearchRunLLMSpendReservationV3) {
	t.Helper()
	return preparedPartialResearchBriefV3(t,
		newResearchShadowRunSpendFixtureV3(t, 1_000_000))
}

func preparedPartialResearchBriefV3(
	t *testing.T, spend researchRunSpendFixtureV3,
) (researchBriefFixtureV3, ResearchBriefSynthesisV3, ResearchRunLLMSpendReservationV3) {
	t.Helper()
	for ordinal := 0; ordinal < spend.planRef.StepCount; ordinal++ {
		started, err := spend.begin(t, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		if ordinal == 0 {
			result := []byte(`{"url":"https://www.kimi.com/membership/pricing","state":"reservation_only"}`)
			if _, err := spend.store.CommitResearchRunStepEvidenceV3(t.Context(),
				CommitResearchRunStepEvidenceV3Params{
					Identity: spend.identity, RunSnapshotID: spend.snapshotID,
					PlanRef: spend.planRef, Ordinal: ordinal,
					Result: result, OriginalSize: len(result), TrustType: "external",
					CostMicroUSD: 100,
					ProviderCall: researchProviderCallV3ForTest(
						spend.trace(t, ordinal, started.InvocationID), 100),
				}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := spend.store.CommitResearchRunStepV3(t.Context(),
			CommitResearchRunStepV3Params{
				Identity: spend.identity, RunSnapshotID: spend.snapshotID,
				PlanRef: spend.planRef, Ordinal: ordinal,
				Phase:     ResearchRunStepIndeterminateV3,
				ErrorCode: "provider_outcome_uncertain", CostMicroUSD: 10_000,
				ProviderCall: ResearchProviderCallV3{
					TraceID:  spend.trace(t, ordinal, started.InvocationID),
					Provider: "exa", QuotaUnits: researchRunQuotaUnitsV3,
					Attempted: true, PricingStatus: "unpriced",
				},
			}); err != nil {
			t.Fatal(err)
		}
	}

	fixture := researchBriefFixtureV3{
		st: spend.store, tenantID: spend.tenantID, userID: spend.userID,
		taskID: spend.identity.TaskID, identity: spend.identity,
		snapshotRef: spend.snapshotRef, planRef: spend.planRef,
	}
	prepared, err := fixture.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.FirstWriter || !prepared.PartialCoverage ||
		prepared.Synthesis.Status != ResearchBriefSynthesisPreparedV3 {
		t.Fatalf("partial shadow synthesis prepare=%+v", prepared)
	}

	ensureResearchLLMPriceV3(t, fixture.st)
	reservation, err := fixture.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: fixture.identity, SnapshotRef: fixture.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, SubjectID: prepared.Synthesis.ID,
			SystemPrompt: "Synthesize without Tools.",
			UserPrompt:   string(prepared.Synthesis.ContextPayload),
		})
	if err != nil || !reservation.FirstWriter || reservation.ReservationID <= 0 {
		t.Fatalf("partial shadow synthesis reservation=%+v err=%v", reservation, err)
	}
	return fixture, prepared.Synthesis, reservation
}

func researchShadowBriefClaimV3(
	fixture researchBriefFixtureV3, synthesis ResearchBriefSynthesisV3,
	reservation ResearchRunLLMSpendReservationV3,
) ClaimResearchBriefSynthesisV3Params {
	return ClaimResearchBriefSynthesisV3Params{
		Identity: fixture.identity, SnapshotRef: fixture.snapshotRef,
		PlanRef: fixture.planRef, SynthesisID: synthesis.ID,
		RequestDigest:             synthesis.RequestDigest,
		SynthesisLLMReservationID: reservation.ReservationID,
	}
}

func assertScheduleExecutionModeV3(
	t *testing.T, fixture researchBriefFixtureV3, want types.ExecutionMode,
) {
	t.Helper()
	var got types.ExecutionMode
	if err := fixture.st.pool.QueryRow(t.Context(),
		`SELECT execution_mode FROM schedules
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		fixture.taskID, fixture.tenantID, fixture.userID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("schedule execution mode=%q want=%q", got, want)
	}
}

func assertResearchBriefSynthesisStatusV3(
	t *testing.T, fixture researchBriefFixtureV3, synthesisID int64,
	want ResearchBriefSynthesisStatusV3,
) {
	t.Helper()
	var got ResearchBriefSynthesisStatusV3
	if err := fixture.st.pool.QueryRow(t.Context(),
		`SELECT status FROM research_brief_syntheses
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		synthesisID, fixture.tenantID, fixture.userID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("research Brief synthesis status=%q want=%q", got, want)
	}
}
