package store

import (
	"context"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
	"github.com/jackc/pgx/v5/pgconn"
)

func gatewayCommitParamsV3(f researchRunSpendFixtureV3,
	reservation ResearchRunLLMSpendReservationV3,
	receipt types.ResearchLLMGatewayReceiptV3) CommitResearchRunLLMReceiptV3Params {
	return CommitResearchRunLLMReceiptV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef, ReservationID: reservation.ReservationID,
		Call: receipt.Call, DisableThinking: receipt.DisableThinking,
		Attempted: receipt.Attempted, UsageKnown: receipt.UsageKnown,
		DefinitelyZeroUsage: receipt.DefinitelyZeroUsage,
		Outcome:             ResearchRunLLMOutcomeV3(receipt.Outcome), ErrorCode: receipt.ErrorCode,
		GatewayReceipt: receipt,
	}
}

func requireLegacy094WritableGatewayV3(t *testing.T, st *Store) {
	t.Helper()
	var processGateway bool
	if err := st.pool.QueryRow(t.Context(), `SELECT to_regprocedure(
		'claim_research_llm_gateway_request_v2(bigint,text,text)') IS NOT NULL`).Scan(&processGateway); err != nil {
		t.Fatal(err)
	}
	if processGateway {
		t.Skip("094 writable signer path is historical-only; 097 process gateway revokes it")
	}
}

func TestResearchLLMProcessGatewayV2AtomicClaimPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithModelPolicyV3(t, 1_000_000, 16, false,
		testProductionResearchModelPolicyStoreV3(t))
	useOwnerResearchRuntimeForTest(f.store)
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, "process gateway atomic claim"))
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Model != "deepseek-v4-pro" || !reservation.DisableThinking {
		t.Fatalf("reservation model=%q disable_thinking=%v",
			reservation.Model, reservation.DisableThinking)
	}
	capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	bearer := hex.EncodeToString(capability.raw[:])
	start := make(chan struct{})
	type claimResult struct {
		first           bool
		model           string
		disableThinking bool
	}
	results := make(chan claimResult, 16)
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tx, claimErr := f.store.pool.Begin(t.Context())
			if claimErr != nil {
				errs <- claimErr
				return
			}
			defer func() { _ = tx.Rollback(t.Context()) }()
			if _, claimErr = tx.Exec(t.Context(), `SET LOCAL ROLE vane_research_llm_gateway`); claimErr != nil {
				errs <- claimErr
				return
			}
			var result claimResult
			claimErr = tx.QueryRow(t.Context(), `SELECT out_first_writer,out_model,out_disable_thinking
				FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
				reservation.ReservationID, reservation.RequestDigest, bearer).
				Scan(&result.first, &result.model, &result.disableThinking)
			if claimErr == nil {
				claimErr = tx.Commit(t.Context())
			}
			results <- result
			errs <- claimErr
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for claimErr := range errs {
		if claimErr != nil {
			t.Fatal(claimErr)
		}
	}
	winners := 0
	for result := range results {
		if result.model != "deepseek-v4-pro" || !result.disableThinking {
			t.Fatalf("gateway claim model=%q disable_thinking=%v", result.model, result.disableThinking)
		}
		if result.first {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("first writers=%d, want exactly 1", winners)
	}
	var attempts, reservations, settlements, calls int
	if err := f.store.pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM research_llm_gateway_attempts WHERE reservation_id=$1),
		(SELECT count(*) FROM research_run_llm_spend_reservations WHERE id=$1),
		(SELECT count(*) FROM research_run_llm_spend_settlements WHERE reservation_id=$1),
		(SELECT count(*) FROM llm_calls WHERE research_run_llm_spend_reservation_id=$1)`,
		reservation.ReservationID).Scan(&attempts, &reservations, &settlements, &calls); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || reservations != 1 || settlements != 0 || calls != 0 {
		t.Fatalf("attempts=%d reservations=%d settlements=%d calls=%d",
			attempts, reservations, settlements, calls)
	}
}

func TestResearchLLMGatewayOverrunSettlementPrecedesToolAdmissionPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, true)
	useOwnerResearchRuntimeForTest(f.store)
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE tenant_quota SET tokens=2000000,rate=0,burst=2000000,updated_at=now()
		  WHERE tenant_id=$1 AND bucket='llm_tokens'`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, "gateway overrun must fence Tool admission"))
	if err != nil {
		t.Fatal(err)
	}
	capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	bearer := hex.EncodeToString(capability.raw[:])
	var first bool
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT out_first_writer FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
		reservation.ReservationID, reservation.RequestDigest, bearer).Scan(&first); err != nil || !first {
		t.Fatalf("claim first=%v err=%v", first, err)
	}

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

	settlementPID := make(chan int, 1)
	settlementDone := make(chan error, 1)
	go func() {
		tx, settleErr := f.store.pool.Begin(t.Context())
		if settleErr != nil {
			settlementPID <- 0
			settlementDone <- settleErr
			return
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
		var pid int
		if settleErr = tx.QueryRow(t.Context(), `SELECT pg_backend_pid()`).Scan(&pid); settleErr != nil {
			settlementPID <- 0
			settlementDone <- settleErr
			return
		}
		settlementPID <- pid
		if _, settleErr = tx.Exec(t.Context(), `SET LOCAL ROLE vane_research_llm_gateway`); settleErr == nil {
			_, settleErr = tx.Exec(t.Context(), `SELECT * FROM settle_research_llm_gateway_request_v2(
				$1,$2,$3,$4,$5,$6,$7,NULL,NULL,NULL,1,NULL,'',true,true,false,'completed','')`,
				reservation.ReservationID, reservation.RequestDigest, bearer,
				`{"steps":[]}`, reservation.Model, 995_000, 4_096)
		}
		if settleErr == nil {
			settleErr = tx.Commit(t.Context())
		}
		settlementDone <- settleErr
	}()
	var settlementBackend int
	select {
	case settlementBackend = <-settlementPID:
	case settleErr := <-settlementDone:
		t.Fatalf("settlement exited before reaching the budget lock: %v", settleErr)
	case <-time.After(5 * time.Second):
		t.Fatal("settlement did not reach the budget lock")
	}
	if settlementBackend <= 0 {
		t.Fatalf("settlement backend did not start: %v", <-settlementDone)
	}
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
			t.Context(), f.identity, f.snapshotID, f.planRef, 0)
		beginDone <- beginResult{execution: execution, err: beginErr}
	}()
	var beginBackend int
	select {
	case beginBackend = <-beginPID:
	case begin := <-beginDone:
		t.Fatalf("Tool admission exited before reaching the budget lock: execution=%+v err=%v",
			begin.execution, begin.err)
	case <-time.After(5 * time.Second):
		t.Fatal("Tool admission did not reach the budget lock")
	}
	waitForResearchSpendAdvisoryWaitV3(t, f.store, beginBackend)

	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	released = true
	if err := <-settlementDone; err != nil {
		t.Fatalf("gateway overrun settlement failed: %v", err)
	}
	begin := <-beginDone
	if !errors.Is(begin.err, ErrQuotaExceeded) || begin.execution.StepID != 0 {
		t.Fatalf("Tool admission ignored preceding gateway overrun: execution=%+v err=%v",
			begin.execution, begin.err)
	}
	var settlements, toolReservations int
	if err := f.store.pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM research_run_llm_spend_settlements WHERE reservation_id=$1),
		(SELECT count(*) FROM research_run_step_spend_reservations
		  WHERE run_snapshot_id=$2 AND step_ordinal=0)`,
		reservation.ReservationID, f.snapshotID).Scan(&settlements, &toolReservations); err != nil {
		t.Fatal(err)
	}
	if settlements != 1 || toolReservations != 0 {
		t.Fatalf("settlements=%d tool reservations=%d", settlements, toolReservations)
	}
}

func TestResearchLLMProcessGatewayV2TerminalReplayAndRevocationPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	for _, test := range []struct {
		name           string
		revoke         bool
		ageReservation bool
		attempted      bool
		usageKnown     bool
		zero           bool
		outcome        string
	}{
		{name: "pre-send rejection response loss", attempted: false, zero: true, outcome: "failed"},
		{name: "claimed effect terminalizes after revocation", revoke: true, attempted: true,
			usageKnown: true, outcome: "completed"},
		{name: "late gateway availability binds receipt to provider attempt",
			ageReservation: true, attempted: true, usageKnown: true, outcome: "completed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			seed := tenantTestStore(t)
			ensureResearchLLMPriceV3(t, seed)
			f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
			useOwnerResearchRuntimeForTest(f.store)
			reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
				researchPlannerBeginV3(f, 1, "terminal replay"))
			if err != nil {
				t.Fatal(err)
			}
			capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
			if err != nil {
				t.Fatal(err)
			}
			bearer := hex.EncodeToString(capability.raw[:])
			if test.ageReservation {
				tx, err := f.store.pool.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(t.Context(), `SET LOCAL session_replication_role=replica`); err == nil {
					_, err = tx.Exec(t.Context(), `UPDATE research_run_llm_spend_reservations
						SET created_at=now()-interval '11 minutes' WHERE id=$1`,
						reservation.ReservationID)
				}
				if err == nil {
					err = tx.Commit(t.Context())
				} else {
					_ = tx.Rollback(t.Context())
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			var first, settled bool
			var endpointID, credentialID string
			var endpointGeneration, credentialGeneration int64
			if err := f.store.pool.QueryRow(t.Context(), `SELECT out_first_writer,out_settled,
				out_endpoint_id,out_endpoint_generation,out_credential_id,out_credential_generation
				FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
				reservation.ReservationID, reservation.RequestDigest, bearer,
			).Scan(&first, &settled, &endpointID, &endpointGeneration,
				&credentialID, &credentialGeneration); err != nil || !first || settled {
				t.Fatalf("initial claim first=%v settled=%v err=%v", first, settled, err)
			}
			if endpointID != "deepseek-compatible-primary" || endpointGeneration != 1 ||
				credentialID != "llm-primary" || credentialGeneration != 1 {
				t.Fatalf("claim route=%s/%d %s/%d", endpointID, endpointGeneration,
					credentialID, credentialGeneration)
			}
			if test.revoke {
				if _, err := f.store.pool.Exec(t.Context(), `UPDATE research_run_capabilities
					SET revoked_at=GREATEST(clock_timestamp(),issued_at)
				  WHERE run_snapshot_id=$1`, f.snapshotID); err != nil {
					t.Fatal(err)
				}
			}
			completion, errorText, errorCode := `{"steps":[]}`, "", ""
			promptTokens, completionTokens := 1, 1
			if !test.attempted {
				completion, errorText, errorCode = "", "local pre-send rejection", "LLM_UNAVAILABLE"
				promptTokens, completionTokens = 0, 0
			}
			if _, err := f.store.pool.Exec(t.Context(), `SELECT *
				FROM settle_research_llm_gateway_request_v2(
				 $1,$2,$3,$4,$5,$6,$7,NULL,NULL,NULL,1,NULL,$8,$9,$10,$11,$12,$13)`,
				reservation.ReservationID, reservation.RequestDigest, bearer, completion,
				reservation.Model, promptTokens, completionTokens, errorText,
				test.attempted, test.usageKnown, test.zero, test.outcome, errorCode); err != nil {
				t.Fatal(err)
			}
			if err := f.store.pool.QueryRow(t.Context(), `SELECT out_first_writer,out_settled
				FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
				reservation.ReservationID, reservation.RequestDigest, bearer,
			).Scan(&first, &settled); err != nil || first || !settled {
				t.Fatalf("terminal replay first=%v settled=%v err=%v", first, settled, err)
			}
		})
	}
}

func TestResearchLLMProcessGatewayV2RecoversRevokedClaimPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	useOwnerResearchRuntimeForTest(f.store)
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, "recover revoked claim"))
	if err != nil {
		t.Fatal(err)
	}
	capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	bearer := hex.EncodeToString(capability.raw[:])
	var first bool
	if err := f.store.pool.QueryRow(t.Context(), `SELECT out_first_writer
		FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
		reservation.ReservationID, reservation.RequestDigest, bearer).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Fatal("initial recovery claim was not first writer")
	}
	if _, err := f.store.pool.Exec(t.Context(), `UPDATE research_llm_gateway_attempts
		SET send_started_at=now()-interval '11 minutes' WHERE reservation_id=$1`,
		reservation.ReservationID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(), `UPDATE research_run_capabilities
		SET revoked_at=GREATEST(clock_timestamp(),issued_at)
	  WHERE run_snapshot_id=$1`, f.snapshotID); err != nil {
		t.Fatal(err)
	}
	var settled bool
	if err := f.store.pool.QueryRow(t.Context(), `SELECT out_first_writer,out_settled
		FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
		reservation.ReservationID, reservation.RequestDigest, bearer,
	).Scan(&first, &settled); err != nil || first || settled {
		t.Fatalf("revoked in-flight replay first=%v settled=%v err=%v", first, settled, err)
	}
	if _, err := f.store.pool.Exec(t.Context(),
		`SELECT recover_research_llm_gateway_request_v2($1,$2,$3)`,
		reservation.ReservationID, reservation.RequestDigest, bearer); err != nil {
		t.Fatal(err)
	}
	var outcome, provenance string
	var processMarkers int
	if err := f.store.pool.QueryRow(t.Context(), `SELECT settlement.outcome,
		settlement.receipt_provenance,
		(SELECT count(*) FROM research_llm_process_gateway_settlements marker
		  WHERE marker.reservation_id=settlement.reservation_id)
		FROM research_run_llm_spend_settlements settlement
		WHERE settlement.reservation_id=$1`, reservation.ReservationID,
	).Scan(&outcome, &provenance, &processMarkers); err != nil {
		t.Fatal(err)
	}
	if outcome != "indeterminate" || provenance != "verified_gateway" || processMarkers != 1 {
		t.Fatalf("recovery outcome=%s provenance=%s markers=%d",
			outcome, provenance, processMarkers)
	}
	if _, err := f.store.pool.Exec(t.Context(),
		`SELECT recover_research_llm_gateway_request_v2($1,$2,$3)`,
		reservation.ReservationID, reservation.RequestDigest, strings.Repeat("ab", 32)); err == nil {
		t.Fatal("wrong capability recovered another run effect")
	}
}

func TestResearchLLMGatewayRecoveryAndNormalSettlementShareBudgetLockPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	for _, recoveryFirst := range []bool{false, true} {
		name := "normal settlement precedes recovery"
		if recoveryFirst {
			name = "recovery precedes late normal settlement"
		}
		t.Run(name, func(t *testing.T) {
			seed := tenantTestStore(t)
			ensureResearchLLMPriceV3(t, seed)
			f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
			useOwnerResearchRuntimeForTest(f.store)
			reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
				researchPlannerBeginV3(f, 1, "recovery and late receipt serialize"))
			if err != nil {
				t.Fatal(err)
			}
			capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
			if err != nil {
				t.Fatal(err)
			}
			bearer := hex.EncodeToString(capability.raw[:])
			var first bool
			if err := f.store.pool.QueryRow(t.Context(), `SELECT out_first_writer
				FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
				reservation.ReservationID, reservation.RequestDigest, bearer).Scan(&first); err != nil || !first {
				t.Fatalf("claim first=%v err=%v", first, err)
			}
			if _, err := f.store.pool.Exec(t.Context(), `UPDATE research_llm_gateway_attempts
				SET send_started_at=now()-interval '11 minutes' WHERE reservation_id=$1`,
				reservation.ReservationID); err != nil {
				t.Fatal(err)
			}

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

			type gatewayOperation struct {
				pid  chan int
				done chan error
			}
			launch := func(recoverEffect bool) gatewayOperation {
				op := gatewayOperation{pid: make(chan int, 1), done: make(chan error, 1)}
				go func() {
					tx, opErr := f.store.pool.Begin(t.Context())
					if opErr != nil {
						op.pid <- 0
						op.done <- opErr
						return
					}
					defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
					var pid int
					if opErr = tx.QueryRow(t.Context(), `SELECT pg_backend_pid()`).Scan(&pid); opErr != nil {
						op.pid <- 0
						op.done <- opErr
						return
					}
					op.pid <- pid
					if _, opErr = tx.Exec(t.Context(), `SET LOCAL ROLE vane_research_llm_gateway`); opErr == nil {
						if recoverEffect {
							_, opErr = tx.Exec(t.Context(),
								`SELECT recover_research_llm_gateway_request_v2($1,$2,$3)`,
								reservation.ReservationID, reservation.RequestDigest, bearer)
						} else {
							_, opErr = tx.Exec(t.Context(), `SELECT * FROM settle_research_llm_gateway_request_v2(
								$1,$2,$3,$4,$5,1,1,NULL,NULL,NULL,1,NULL,'',true,true,false,'completed','')`,
								reservation.ReservationID, reservation.RequestDigest, bearer,
								`{"steps":[]}`, reservation.Model)
						}
					}
					if opErr == nil {
						opErr = tx.Commit(t.Context())
					}
					op.done <- opErr
				}()
				return op
			}
			awaitBudgetWait := func(op gatewayOperation) {
				t.Helper()
				select {
				case pid := <-op.pid:
					if pid <= 0 {
						t.Fatalf("gateway operation did not start: %v", <-op.done)
					}
					waitForResearchSpendAdvisoryWaitV3(t, f.store, pid)
				case opErr := <-op.done:
					t.Fatalf("gateway operation bypassed budget serialization: %v", opErr)
				case <-time.After(5 * time.Second):
					t.Fatal("gateway operation did not reach budget lock")
				}
			}

			firstOp := launch(recoveryFirst)
			awaitBudgetWait(firstOp)
			secondOp := launch(!recoveryFirst)
			awaitBudgetWait(secondOp)
			if err := blocker.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}
			released = true
			waitDone := func(op gatewayOperation) error {
				select {
				case opErr := <-op.done:
					return opErr
				case <-time.After(10 * time.Second):
					t.Fatal("serialized gateway operation did not finish")
					return nil
				}
			}
			firstErr, secondErr := waitDone(firstOp), waitDone(secondOp)
			for _, opErr := range []error{firstErr, secondErr} {
				var pgErr *pgconn.PgError
				if errors.As(opErr, &pgErr) && pgErr.Code == "40P01" {
					t.Fatalf("gateway settlement lock order deadlocked: %v", opErr)
				}
			}
			recoveryErr, normalErr := secondErr, firstErr
			if recoveryFirst {
				recoveryErr, normalErr = firstErr, secondErr
			}
			if recoveryErr != nil {
				t.Fatalf("serialized recovery failed: %v", recoveryErr)
			}
			if normalErr == nil {
				t.Fatal("late normal settlement bypassed the Provider attempt window")
			}
			var outcome string
			var count int
			if err := f.store.pool.QueryRow(t.Context(), `SELECT max(outcome),count(*)
				FROM research_run_llm_spend_settlements WHERE reservation_id=$1`,
				reservation.ReservationID).Scan(&outcome, &count); err != nil {
				t.Fatal(err)
			}
			wantOutcome := "indeterminate"
			if count != 1 || outcome != wantOutcome {
				t.Fatalf("terminal settlement count=%d outcome=%q want %q", count, outcome, wantOutcome)
			}
		})
	}
}

func TestResearchLLMGatewayAtomicSendClaimPostgres(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	seed := tenantTestStore(t)
	requireLegacy094WritableGatewayV3(t, seed)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	f.store.beginGatewayTx = f.store.pool.BeginTx
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, "atomic gateway claim"))
	if err != nil {
		t.Fatal(err)
	}
	binding := types.ResearchLLMGatewayCallBindingV3{ReservationID: reservation.ReservationID,
		RequestDigest: reservation.RequestDigest, Identity: f.identity, SnapshotRef: f.snapshotRef}
	intent := types.ResearchLLMGatewaySendIntentV3{Binding: binding,
		Call: researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
			"Plan from the trusted task manual.", "atomic gateway claim"),
		DisableThinking: reservation.DisableThinking}
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			first, claimErr := f.store.MarkResearchLLMGatewaySendStartedV3(t.Context(), intent)
			results <- first
			errs <- claimErr
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for claimErr := range errs {
		if claimErr != nil {
			t.Fatal(claimErr)
		}
	}
	firstWriters := 0
	for first := range results {
		if first {
			firstWriters++
		}
	}
	if firstWriters != 1 {
		t.Fatalf("first writers=%d, want exactly one", firstWriters)
	}
}

func TestResearchLLMGatewayRestrictedPoolProbePostgres(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL required")
	}
	admin := tenantTestStore(t)
	const password = "vane_gateway_test_only_20260801"
	if _, err := admin.pool.Exec(t.Context(), `ALTER ROLE vane_research_llm_gateway_runtime
		LOGIN PASSWORD '`+password+`' NOSUPERUSER NOCREATEDB NOCREATEROLE
		NOINHERIT NOREPLICATION NOBYPASSRLS`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = admin.pool.Exec(ctx, `ALTER ROLE vane_research_llm_gateway_runtime NOLOGIN`)
	})
	parsed, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(researchGatewayLoginRole, password)
	pool, err := newStorePool(t.Context(), parsed.String(), validateResearchGatewayConnection)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	if _, err := admin.pool.Exec(t.Context(), `GRANT UPDATE ON provider_price_rules TO vane_research_llm_gateway`); err != nil {
		t.Fatal(err)
	}
	if unsafePool, err := newStorePool(t.Context(), parsed.String(), validateResearchGatewayConnection); err == nil {
		unsafePool.Close()
		t.Fatal("gateway startup accepted direct provider-price UPDATE grant")
	}
	if _, err := admin.pool.Exec(t.Context(), `REVOKE UPDATE ON provider_price_rules FROM vane_research_llm_gateway`); err != nil {
		t.Fatal(err)
	}
}

func TestResearchLLMGatewaySignedSettlementAndAttacksPostgres(t *testing.T) {
	seed := tenantTestStore(t)
	requireLegacy094WritableGatewayV3(t, seed)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	f.store.beginGatewayTx = f.store.pool.BeginTx
	if _, err := f.store.pool.Exec(t.Context(), `UPDATE tenant_quota SET tokens=2000000,
		rate=0,burst=2000000,updated_at=now() WHERE tenant_id=$1 AND bucket='llm_tokens'`,
		f.tenantID); err != nil {
		t.Fatal(err)
	}
	prompt := "gateway exact prompt"
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(), researchPlannerBeginV3(f, 1, prompt))
	if err != nil {
		t.Fatal(err)
	}
	binding := types.ResearchLLMGatewayCallBindingV3{ReservationID: reservation.ReservationID,
		RequestDigest: reservation.RequestDigest, Identity: f.identity, SnapshotRef: f.snapshotRef}
	call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
		"Plan from the trusted task manual.", prompt)
	intent := types.ResearchLLMGatewaySendIntentV3{Binding: binding, Call: call,
		DisableThinking: reservation.DisableThinking}
	if err := f.store.PrepareResearchLLMGatewayReceiptV3(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	if first, err := f.store.MarkResearchLLMGatewaySendStartedV3(t.Context(), intent); err != nil || !first {
		t.Fatal(err)
	}
	started, err := f.store.ResearchLLMGatewayAttemptStartedV3(t.Context(), binding)
	if err != nil || started != types.ResearchLLMGatewayAttemptSendStartedV3 {
		t.Fatalf("started=%v err=%v", started, err)
	}
	call.Completion = `{"steps":[]}`
	call.PromptTokens = 10
	call.CompletionTokens = 2
	unsigned := types.ResearchLLMGatewayReceiptV3{SchemaVersion: types.ResearchLLMGatewayReceiptSchemaV3,
		SignedAtUnixMillis: time.Now().UTC().UnixMilli(), ReservationID: reservation.ReservationID,
		RequestDigest: reservation.RequestDigest, Call: call, DisableThinking: reservation.DisableThinking,
		Attempted: true, UsageKnown: true, Outcome: "completed"}
	signed, err := f.store.FinalizeMeasuredResearchLLMGatewayReceiptV3(t.Context(), binding, unsigned)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := f.store.CommitResearchRunLLMReceiptV3(t.Context(), gatewayCommitParamsV3(f, reservation, signed))
	if err != nil || !settled.Settled {
		t.Fatalf("settled=%+v err=%v", settled, err)
	}
	var provenance string
	if err := f.store.pool.QueryRow(t.Context(), `SELECT receipt_provenance FROM
		research_run_llm_spend_settlements WHERE reservation_id=$1`, reservation.ReservationID).Scan(&provenance); err != nil {
		t.Fatal(err)
	}
	if provenance != "verified_gateway" {
		t.Fatalf("provenance=%q", provenance)
	}
	if replay, err := f.store.CommitResearchRunLLMReceiptV3(t.Context(), gatewayCommitParamsV3(f, reservation, signed)); err != nil || replay.LLMCallID != settled.LLMCallID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}

	// Signature covers completion/usage/provider model; any mutation is rejected.
	mutated := signed
	mutated.Call.Completion = `{"steps":[{"forged":true}]}`
	params := gatewayCommitParamsV3(f, reservation, mutated)
	if _, err := f.store.CommitResearchRunLLMReceiptV3(t.Context(), params); err == nil {
		t.Fatal("mutated signed completion accepted")
	}
	// A privilege assertion is safer than invoking the large legacy function.
	var can bool
	if err := f.store.pool.QueryRow(t.Context(), `SELECT has_function_privilege(
		'vane_research_v3_executor','settle_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text)','EXECUTE')`).Scan(&can); err != nil || can {
		t.Fatalf("executor legacy settle privilege=%v err=%v", can, err)
	}

	rejectedPrompt := "local rejected request"
	rejectedReservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 2, rejectedPrompt))
	if err != nil {
		t.Fatal(err)
	}
	rejectedBinding := types.ResearchLLMGatewayCallBindingV3{ReservationID: rejectedReservation.ReservationID,
		RequestDigest: rejectedReservation.RequestDigest, Identity: f.identity, SnapshotRef: f.snapshotRef}
	rejectedCall := researchLLMCallForTestV3(f.identity, f.snapshotRef, rejectedReservation,
		"Plan from the trusted task manual.", rejectedPrompt)
	rejectedIntent := types.ResearchLLMGatewaySendIntentV3{Binding: rejectedBinding,
		Call: rejectedCall, DisableThinking: rejectedReservation.DisableThinking}
	if first, err := f.store.MarkResearchLLMGatewayPreSendRejectedV3(t.Context(), rejectedIntent); err != nil || !first {
		t.Fatalf("pre-reject claim first=%v err=%v", first, err)
	}
	if _, err := f.store.MarkResearchLLMGatewaySendStartedV3(t.Context(), rejectedIntent); err == nil {
		t.Fatal("pre-send rejection transitioned to send_started")
	}
	state, err := f.store.ResearchLLMGatewayAttemptStartedV3(t.Context(), rejectedBinding)
	if err != nil || state != types.ResearchLLMGatewayAttemptPreSendRejectedV3 {
		t.Fatalf("pre-reject state=%q err=%v", state, err)
	}
	zero, err := f.store.SignConfirmedZeroResearchLLMGatewayRecoveryV3(t.Context(), rejectedBinding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CommitResearchRunLLMReceiptV3(t.Context(), gatewayCommitParamsV3(f, rejectedReservation, zero)); err != nil {
		t.Fatal(err)
	}

	wrong := binding
	wrong.Identity.TaskID = "other-task"
	if _, err := f.store.ResearchLLMGatewayAttemptStartedV3(t.Context(), wrong); err == nil {
		t.Fatal("cross-scope marker query accepted")
	}
}

func TestResearchLLMGatewayBackdatingAndRecoveryPostgres(t *testing.T) {
	seed := tenantTestStore(t)
	requireLegacy094WritableGatewayV3(t, seed)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	f.store.beginGatewayTx = f.store.pool.BeginTx
	prompt := "recovery prompt"
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(), researchPlannerBeginV3(f, 2, prompt))
	if err != nil {
		t.Fatal(err)
	}
	binding := types.ResearchLLMGatewayCallBindingV3{ReservationID: reservation.ReservationID,
		RequestDigest: reservation.RequestDigest, Identity: f.identity, SnapshotRef: f.snapshotRef}
	call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
		"Plan from the trusted task manual.", prompt)
	if first, err := f.store.MarkResearchLLMGatewaySendStartedV3(t.Context(), types.ResearchLLMGatewaySendIntentV3{
		Binding: binding, Call: call, DisableThinking: reservation.DisableThinking}); err != nil {
		t.Fatal(err)
	} else if !first {
		t.Fatal("gateway send claim unexpectedly replayed")
	}

	backdated := types.ResearchLLMGatewayReceiptV3{SchemaVersion: types.ResearchLLMGatewayReceiptSchemaV3,
		SignedAtUnixMillis: time.Now().Add(-time.Hour).UnixMilli(), ReservationID: reservation.ReservationID,
		RequestDigest: reservation.RequestDigest, Call: call, DisableThinking: reservation.DisableThinking,
		Attempted: true, UsageKnown: false, Outcome: "indeterminate", ErrorCode: string(types.CodeLLMUnavailable)}
	backdated.Call.Error = "timeout"
	backdated, err = f.store.FinalizeMeasuredResearchLLMGatewayReceiptV3(t.Context(), binding, backdated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CommitResearchRunLLMReceiptV3(t.Context(), gatewayCommitParamsV3(f, reservation, backdated)); err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("backdated receipt err=%v", err)
	}

	if _, err := f.store.SignConservativeResearchLLMGatewayRecoveryV3(t.Context(), binding); err == nil {
		t.Fatal("in-flight send was recovered before grace window")
	}
	if _, err := f.store.pool.Exec(t.Context(), `UPDATE research_llm_gateway_attempts
		SET send_started_at=now()-interval '11 minutes' WHERE reservation_id=$1`,
		reservation.ReservationID); err != nil {
		t.Fatal(err)
	}
	recovery, err := f.store.SignConservativeResearchLLMGatewayRecoveryV3(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := f.store.CommitResearchRunLLMReceiptV3(t.Context(), gatewayCommitParamsV3(f, reservation, recovery))
	if err != nil || settled.Outcome != ResearchRunLLMIndeterminateV3 || settled.ActualCostMicroUSD != reservation.ReservedCostMicroUSD {
		t.Fatalf("recovery settled=%+v err=%v", settled, err)
	}
}
