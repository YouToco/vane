package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func commitResearchRunLLMReceiptForTestV3(
	t *testing.T, st *Store, params CommitResearchRunLLMReceiptV3Params,
) (ResearchRunLLMReceiptV3, CommitResearchRunLLMReceiptV3Params, error) {
	t.Helper()
	var requestDigest, stage string
	var round int
	if err := st.pool.QueryRow(t.Context(), `SELECT request_digest,stage,round_ordinal
		FROM research_run_llm_spend_reservations WHERE id=$1`, params.ReservationID).
		Scan(&requestDigest, &stage, &round); err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	capability, err := st.resolveResearchRunCapabilityV1(t.Context(), params.SnapshotRef)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	tx, err := st.pool.Begin(t.Context())
	if err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_research_llm_gateway`); err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	bearer := hex.EncodeToString(capability.raw[:])
	if _, err := tx.Exec(t.Context(), `SELECT * FROM claim_research_llm_gateway_request_v2($1,$2,$3)`,
		params.ReservationID, requestDigest, bearer); err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	call := params.Call
	if _, err := tx.Exec(t.Context(), `SELECT * FROM settle_research_llm_gateway_request_v2(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		params.ReservationID, requestDigest, bearer, call.Completion, call.Model,
		call.PromptTokens, call.CompletionTokens, call.PromptCacheHitTokens,
		call.PromptCacheMissTokens, call.ReasoningTokens, call.LatencyMs,
		call.PrefixCacheHit, call.Error, params.Attempted, params.UsageKnown,
		params.DefinitelyZeroUsage, string(params.Outcome), params.ErrorCode); err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	if err := tx.Commit(t.Context()); err != nil {
		return ResearchRunLLMReceiptV3{}, params, err
	}
	receipt, _, err := st.LoadResearchRunLLMReceiptV3(t.Context(), params.Identity,
		params.SnapshotRef, stage, round)
	return receipt, params, err
}

// createResearchPlanFromReceiptV3 gives Store integration fixtures the same
// receipt-first ordering required from production: reserve, persist the exact
// model-visible JSON response and only then admit the typed Plan artifact.
func researchPlannerCompletionFromPlanV3(
	t *testing.T, plan runcontext.ResearchExecutionPlanV3,
) []byte {
	t.Helper()
	payload, err := json.Marshal(struct {
		SchemaVersion string                          `json:"schema_version"`
		Steps         []runcontext.ResearchPlanStepV3 `json:"steps"`
	}{
		SchemaVersion: "vane.research-planner-output/v3",
		Steps:         plan.Steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func createResearchPlanFromReceiptV3(
	t *testing.T, st *Store, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan runcontext.ResearchExecutionPlanV3,
) (types.ResearchRunPlanRefV3, ResearchRunLLMSpendReservationV3) {
	t.Helper()
	ensureResearchLLMPriceV3(t, st)
	payload := researchPlannerCompletionFromPlanV3(t, plan)
	const userPrompt = "Return the frozen research Plan as JSON."
	reservation, err := st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: identity, SnapshotRef: snapshot,
			Stage: ResearchRunLLMStagePlannerV3, RoundOrdinal: 0,
			SystemPrompt: "Plan from the trusted task manual.", UserPrompt: userPrompt,
		})
	if err != nil {
		t.Fatal(err)
	}
	call := researchLLMCallForTestV3(identity, snapshot, reservation,
		"Plan from the trusted task manual.", userPrompt)
	call.Completion = string(payload)
	call.PromptTokens, call.CompletionTokens = 1, 1
	if _, _, err := commitResearchRunLLMReceiptForTestV3(t, st,
		CommitResearchRunLLMReceiptV3Params{
			Identity: identity, SnapshotRef: snapshot,
			ReservationID: reservation.ReservationID, Call: call,
			DisableThinking: reservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		}); err != nil {
		t.Fatal(err)
	}
	ref, err := st.CreateOrGetResearchRunPlanV3(t.Context(),
		CreateOrGetResearchRunPlanV3Params{
			Identity: identity, RunSnapshotID: snapshot.SnapshotID,
			PlannerLLMReservationID: reservation.ReservationID, Plan: plan,
		})
	if err != nil {
		t.Fatal(err)
	}
	return ref, reservation
}

func claimResearchBriefWithPendingReceiptV3(
	t *testing.T, fixture researchBriefFixtureV3, synthesis ResearchBriefSynthesisV3,
) (ClaimResearchBriefSynthesisV3Params, ResearchRunLLMSpendReservationV3) {
	t.Helper()
	ensureResearchLLMPriceV3(t, fixture.st)
	seal, err := fixture.st.LoadResearchRunSnapshotV3(
		t.Context(), fixture.identity, fixture.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := fixture.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: fixture.identity, SnapshotRef: fixture.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, SubjectID: synthesis.ID,
			SystemPrompt: seal.ResearchModel.Synthesis.SystemPrompt,
			UserPrompt:   string(synthesis.ContextPayload),
		})
	if err != nil {
		t.Fatal(err)
	}
	handle := ClaimResearchBriefSynthesisV3Params{
		Identity: fixture.identity, SnapshotRef: fixture.snapshotRef,
		PlanRef: fixture.planRef, SynthesisID: synthesis.ID,
		RequestDigest:             synthesis.RequestDigest,
		SynthesisLLMReservationID: reservation.ReservationID,
	}
	claim, err := fixture.st.ClaimResearchBriefSynthesisV3(t.Context(), handle)
	if err != nil || !claim.Claimed ||
		claim.ReceiptState != ResearchBriefLLMReceiptPendingV3 {
		t.Fatalf("claim pending research Brief receipt=%+v err=%v", claim, err)
	}
	return handle, reservation
}

func settleResearchBriefReceiptV3(
	t *testing.T, fixture researchBriefFixtureV3,
	reservation ResearchRunLLMSpendReservationV3, synthesis ResearchBriefSynthesisV3,
	briefPayload []byte,
) ResearchRunLLMReceiptV3 {
	t.Helper()
	seal, err := fixture.st.LoadResearchRunSnapshotV3(
		t.Context(), fixture.identity, fixture.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	call := researchLLMCallForTestV3(fixture.identity, fixture.snapshotRef,
		reservation, seal.ResearchModel.Synthesis.SystemPrompt, string(synthesis.ContextPayload))
	call.Completion = string(briefPayload)
	call.PromptTokens, call.CompletionTokens = 1, 1
	receipt, _, err := commitResearchRunLLMReceiptForTestV3(t, fixture.st,
		CommitResearchRunLLMReceiptV3Params{
			Identity: fixture.identity, SnapshotRef: fixture.snapshotRef,
			ReservationID: reservation.ReservationID, Call: call,
			DisableThinking: reservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestResearchRunPlanV3RequiresSemanticallyEqualCompletedReceiptPostgres(t *testing.T) {
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	ensureResearchLLMPriceV3(t, f.store)
	plan := researchRunPlanFixtureV3(t, f.definitionDigest,
		f.snapshotRef.CapabilityCatalogDigest, f.snapshotRef.ToolPolicyDigest,
		"Kimi pricing")

	reserveAndSettle := func(round int, completion string) ResearchRunLLMSpendReservationV3 {
		t.Helper()
		prompt := "Return planner round " + string(rune('0'+round)) + " as JSON."
		reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
			researchPlannerBeginV3(f, round, prompt))
		if err != nil {
			t.Fatal(err)
		}
		call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
			"Plan from the trusted task manual.", prompt)
		call.Completion = completion
		call.PromptTokens, call.CompletionTokens = 1, 1
		if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.store,
			CommitResearchRunLLMReceiptV3Params{
				Identity: f.identity, SnapshotRef: f.snapshotRef,
				ReservationID: reservation.ReservationID, Call: call,
				DisableThinking: reservation.DisableThinking,
				Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
			}); err != nil {
			t.Fatal(err)
		}
		return reservation
	}

	mismatched := reserveAndSettle(0, `{"schema_version":"not-the-plan"}`)
	if _, err := f.store.CreateOrGetResearchRunPlanV3(t.Context(),
		CreateOrGetResearchRunPlanV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotRef.SnapshotID,
			PlannerLLMReservationID: mismatched.ReservationID, Plan: plan,
		}); err == nil {
		t.Fatal("database admitted a Plan different from the completed receipt")
	}

	payload := researchPlannerCompletionFromPlanV3(t, plan)
	validCompletion := string(payload)
	duplicateCompletions := []string{
		strings.Replace(validCompletion,
			`{"schema_version":`, `{"schema_version":"wrong","schema_version":`, 1),
		strings.Replace(validCompletion,
			`{"invocation_id":`, `{"invocation_id":"wrong","invocation_id":`, 1),
		strings.Replace(validCompletion,
			`"tool_name":"web_search"`,
			`"tool_name":"wrong","tool_name":"web_search"`, 1),
		strings.Replace(validCompletion,
			`{"query":"Kimi pricing"}`,
			`{"query":"attacker-visible-first","query":"Kimi pricing"}`, 1),
	}
	for index, completion := range duplicateCompletions {
		reservation := reserveAndSettle(index+1, completion)
		if _, err := f.store.CreateOrGetResearchRunPlanV3(t.Context(),
			CreateOrGetResearchRunPlanV3Params{
				Identity: f.identity, RunSnapshotID: f.snapshotRef.SnapshotID,
				PlannerLLMReservationID: reservation.ReservationID, Plan: plan,
			}); err == nil {
			t.Fatalf("database admitted duplicate-key planner receipt mutation %d", index)
		}
	}

	var semantic any
	if err := json.Unmarshal(payload, &semantic); err != nil {
		t.Fatal(err)
	}
	pretty, err := json.MarshalIndent(semantic, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	equivalent := reserveAndSettle(len(duplicateCompletions)+1, string(pretty))
	if _, err := f.store.CreateOrGetResearchRunPlanV3(t.Context(),
		CreateOrGetResearchRunPlanV3Params{
			Identity: f.identity, RunSnapshotID: f.snapshotRef.SnapshotID,
			PlannerLLMReservationID: equivalent.ReservationID, Plan: plan,
		}); err != nil {
		t.Fatalf("semantically equal Plan receipt was rejected: %v", err)
	}
}

func TestResearchBriefV3RequiresSemanticallyEqualCompletedReceiptPostgres(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(t.Context(),
		researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(t, f, prepared.Synthesis)
	receiptPayload := researchBriefPayloadV3(t, prepared.Synthesis,
		types.ResearchBriefSignificanceQualifiedV3, "receipt conclusion")
	settleResearchBriefReceiptV3(t, f, reservation, prepared.Synthesis, receiptPayload)

	differentPayload := researchBriefPayloadV3(t, prepared.Synthesis,
		types.ResearchBriefSignificanceQualifiedV3, "different conclusion")
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        differentPayload,
		}); err == nil {
		t.Fatal("database admitted a Brief different from the completed receipt")
	} else if errors.Is(err, types.ErrConflict) {
		// The exact mismatch is intentionally enforced by the database trigger,
		// not a caller-supplied digest comparison in the Store.
		t.Fatalf("Brief mismatch was rejected before the database receipt fence: %v", err)
	}
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        receiptPayload,
		}); err != nil {
		t.Fatalf("receipt-backed Brief was rejected after mismatch rollback: %v", err)
	}
}
