package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func testResearchGroundingModelPolicyV1(t *testing.T) runtimepolicy.ResearchModelPolicyV3 {
	t.Helper()
	policy := testResearchModelPolicyStoreV3(t)
	policy.Synthesis.RendererVersion = runtimepolicy.ResearchSynthesisRendererVersionV34
	policy.GroundingVerifier = &runtimepolicy.ResearchModelStageV3{
		Stage: runtimepolicy.ResearchModelStageGroundingVerifierV3,
		Model: "strong-model", MaxTokens: 4096, DisableThinking: true,
		SystemPrompt:    "Verify the candidate against only its cited evidence.",
		RendererVersion: runtimepolicy.ResearchGroundingVerifierRendererVersionV1,
	}
	policy, err := runtimepolicy.BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func newResearchGroundingFixtureV1(t *testing.T) researchBriefFixtureV3 {
	t.Helper()
	st := tenantTestStore(t)
	return newResearchBriefFixtureWithStoreWorkflowAndModelV3(
		t, st, taskstate.NotificationThresholdMajorV3, true,
		[]byte(`{"title":"OpenAI release","text":"OpenAI released GPT-5.6.","url":"https://openai.com/index/gpt-5-6/"}`),
		"", "", testResearchGroundingModelPolicyV1(t))
}

func newResearchGroundingFixtureV11(t *testing.T) researchBriefFixtureV3 {
	t.Helper()
	policy := testResearchGroundingModelPolicyV1(t)
	policy.GroundingVerifier.RendererVersion =
		runtimepolicy.ResearchGroundingVerifierRendererVersionV11
	policy, err := runtimepolicy.BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	st := tenantTestStore(t)
	return newResearchBriefFixtureWithStoreWorkflowAndModelV3(
		t, st, taskstate.NotificationThresholdMajorV3, true,
		[]byte(`{"title":"OpenAI release","text":"OpenAI released GPT-5.6.","url":"https://openai.com/index/gpt-5-6/"}`),
		"", "", policy)
}

func newResearchGroundingFixtureV12(t *testing.T) researchBriefFixtureV3 {
	t.Helper()
	policy := testResearchGroundingModelPolicyV1(t)
	policy.GroundingVerifier.RendererVersion =
		runtimepolicy.ResearchGroundingVerifierRendererVersionV12
	policy, err := runtimepolicy.BuildResearchModelPolicyV3(policy)
	if err != nil {
		t.Fatal(err)
	}
	st := tenantTestStore(t)
	return newResearchBriefFixtureWithStoreWorkflowAndModelV3(
		t, st, taskstate.NotificationThresholdMajorV3, true,
		[]byte(`{"title":"OpenAI release","text":"OpenAI released GPT-5.6.","url":"https://openai.com/index/gpt-5-6/"}`),
		"", "", policy)
}

func prepareGroundingCandidateV1(t *testing.T, f researchBriefFixtureV3) (
	ResearchBriefSynthesisV3, ClaimResearchBriefSynthesisV3Params, []byte,
	ResearchBriefGroundingV1,
) {
	t.Helper()
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(
		t, f, prepared.Synthesis)
	candidate := researchBriefPayloadV3(t, prepared.Synthesis,
		types.ResearchBriefSignificanceMajorV3,
		"OpenAI released GPT-5.6 according to the cited official page.")
	settleResearchBriefReceiptV3(t, f, reservation, prepared.Synthesis, candidate)
	grounding, err := f.st.PrepareOrGetResearchBriefGroundingV1(t.Context(),
		PrepareResearchBriefGroundingV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			CandidateBriefPayload:               candidate,
		})
	if err != nil {
		t.Fatal(err)
	}
	if !grounding.FirstWriter || grounding.Grounding.Status !=
		ResearchBriefGroundingPreparedV1 {
		t.Fatalf("grounding=%+v", grounding)
	}
	return prepared.Synthesis, handle, candidate, grounding.Grounding
}

func settleGroundingVerifierV1(
	t *testing.T, f researchBriefFixtureV3, synthesis ResearchBriefSynthesisV3,
	handle ClaimResearchBriefSynthesisV3Params, grounding ResearchBriefGroundingV1,
	verdict types.ResearchGroundingVerdictV1,
) ResearchBriefGroundingV1 {
	t.Helper()
	ensureResearchLLMPriceV3(t, f.st)
	beginParams := BeginResearchRunLLMSpendV3Params{
		Identity: f.identity, SnapshotRef: f.snapshotRef,
		Stage: ResearchRunLLMStageSynthesisV3, RoundOrdinal: 1,
		SubjectID:    synthesis.ID,
		SystemPrompt: "Verify the candidate against only its cited evidence.",
		UserPrompt:   string(grounding.VerifierPrompt),
	}
	reservation, err := f.st.BeginResearchRunLLMSpendV3(t.Context(), beginParams)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a process crash immediately after quota admission. Retrying the
	// exact request must recover the one reservation without a second debit.
	replay, err := f.st.BeginResearchRunLLMSpendV3(t.Context(), beginParams)
	if err != nil || replay.ReservationID != reservation.ReservationID ||
		replay.RequestDigest != reservation.RequestDigest || replay.FirstWriter {
		t.Fatalf("verifier admission replay=%+v reservation=%+v err=%v",
			replay, reservation, err)
	}
	var verifierReservations int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_run_llm_spend_reservations
		  WHERE run_snapshot_id=$1 AND stage='synthesis' AND round_ordinal=1`,
		f.snapshotRef.SnapshotID).Scan(&verifierReservations); err != nil ||
		verifierReservations != 1 {
		t.Fatalf("verifier reservation replay count=%d err=%v",
			verifierReservations, err)
	}
	issues := []types.ResearchGroundingIssueV1{}
	if verdict == types.ResearchGroundingUnsupportedV1 {
		candidate, _, decodeErr := types.DecodeResearchBriefPayloadV3(
			grounding.CandidateBriefPayload)
		if decodeErr != nil || len(candidate.Citations) == 0 {
			t.Fatalf("decode grounding candidate: %+v err=%v", candidate, decodeErr)
		}
		issues = []types.ResearchGroundingIssueV1{{
			Field: "summary", Claim: "Anthropic released Claude Opus 5.",
			Refs:   []types.ResearchBriefCitationV3{candidate.Citations[0]},
			Reason: "The cited evidence is about OpenAI, not Anthropic.",
		}}
	}
	verdictPayload, err := json.Marshal(types.ResearchGroundingVerdictPayloadV1{
		SchemaVersion:   types.ResearchGroundingVerdictSchemaV1,
		CandidateDigest: grounding.CandidateDigest, Verdict: verdict, Issues: issues,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerCompletion, err := json.MarshalIndent(types.ResearchGroundingVerdictPayloadV1{
		SchemaVersion:   types.ResearchGroundingVerdictSchemaV1,
		CandidateDigest: grounding.CandidateDigest, Verdict: verdict, Issues: issues,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	call := researchLLMCallForTestV3(f.identity, f.snapshotRef, reservation,
		"Verify the candidate against only its cited evidence.",
		string(grounding.VerifierPrompt))
	// Provider bytes are retained exactly; only the separately persisted
	// application verdict is canonicalized for deterministic comparison.
	call.Completion = string(providerCompletion)
	call.PromptTokens, call.CompletionTokens = 1, 1
	if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.st,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: reservation.ReservationID, Call: call,
			DisableThinking: reservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		}); err != nil {
		t.Fatal(err)
	}
	settled, err := f.st.SettleResearchBriefGroundingV1(t.Context(),
		SettleResearchBriefGroundingV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			GroundingID:                         grounding.ID,
			VerifierLLMReservationID:            reservation.ReservationID,
			VerdictPayload:                      verdictPayload,
		})
	if err != nil {
		t.Fatal(err)
	}
	return settled
}

func TestResearchBriefGroundingV1PostgresGatesFinalization(t *testing.T) {
	f := newResearchGroundingFixtureV1(t)
	synthesis, handle, candidate, grounding := prepareGroundingCandidateV1(t, f)
	if !strings.Contains(string(grounding.VerifierPrompt),
		`"synthesis_visible_text":"{\"title\":\"OpenAI release\"`) ||
		!strings.Contains(string(grounding.VerifierPrompt), grounding.CandidateDigest) {
		t.Fatalf("verifier prompt is not candidate/evidence bound: %s", grounding.VerifierPrompt)
	}
	if strings.Contains(string(grounding.VerifierPrompt), `"issue_refs_item_fields"`) {
		t.Fatalf("frozen v1 verifier prompt changed: %s", grounding.VerifierPrompt)
	}
	if strings.Contains(string(grounding.VerifierPrompt), `"history_through_utc"`) {
		t.Fatalf("frozen v1 verifier prompt changed: %s", grounding.VerifierPrompt)
	}
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        candidate,
		}); err == nil {
		t.Fatal("V3.3 Brief finalized without independent grounding")
	}
	settled := settleGroundingVerifierV1(t, f, synthesis, handle, grounding,
		types.ResearchGroundingGroundedV1)
	if settled.VerifierLLMReservationID == nil {
		t.Fatal("grounded verification lost its LLM reservation")
	}
	replay, err := f.st.SettleResearchBriefGroundingV1(t.Context(),
		SettleResearchBriefGroundingV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			GroundingID:                         settled.ID,
			VerifierLLMReservationID:            *settled.VerifierLLMReservationID,
			VerdictPayload:                      settled.VerdictPayload,
		})
	if err != nil || replay.ID != settled.ID || replay.VerdictDigest != settled.VerdictDigest {
		t.Fatalf("grounding settlement replay=%+v err=%v", replay, err)
	}
	var providerCompletion string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT call.completion
		   FROM research_run_llm_spend_settlements settlement
		   JOIN llm_calls call ON call.id=settlement.llm_call_id
		  WHERE settlement.reservation_id=$1`,
		*settled.VerifierLLMReservationID).Scan(&providerCompletion); err != nil ||
		!strings.Contains(providerCompletion, "\n") ||
		providerCompletion == string(settled.VerdictPayload) {
		t.Fatalf("provider completion was not retained exactly: %q err=%v",
			providerCompletion, err)
	}
	ref, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        candidate, GroundingVerificationID: settled.ID,
		})
	if err != nil || !ref.DeliveryRequired {
		t.Fatalf("grounded finalization ref=%+v err=%v", ref, err)
	}
	if replay, err := f.st.PrepareOrGetResearchBriefGroundingV1(t.Context(),
		PrepareResearchBriefGroundingV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			CandidateBriefPayload:               candidate,
		}); err == nil || replay.Grounding.ID != 0 {
		// The synthesis is terminal; preparing a new verification must not be a
		// side door even though the immutable record remains queryable.
		t.Fatal("terminal Brief admitted a new grounding preparation")
	}
}

func TestResearchBriefGroundingV11ExplainsCitationObjectContract(t *testing.T) {
	f := newResearchGroundingFixtureV11(t)
	_, _, _, grounding := prepareGroundingCandidateV1(t, f)
	prompt := string(grounding.VerifierPrompt)
	for _, want := range []string{
		`"issue_refs_item_fields":["kind","ref"]`,
		`"issue_refs_kind_values":["current_evidence","history"]`,
		`never a bare string`,
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("v1.1 verifier prompt missing %q: %s", want, prompt)
		}
	}
	if strings.Contains(prompt, `"history_through_utc"`) {
		t.Fatalf("frozen v1.1 verifier prompt changed: %s", prompt)
	}
}

func TestResearchBriefGroundingV12BindsFrozenRunClock(t *testing.T) {
	f := newResearchGroundingFixtureV12(t)
	synthesis, _, _, grounding := prepareGroundingCandidateV1(t, f)
	prompt := string(grounding.VerifierPrompt)
	for _, want := range []string{
		`"schema_version":"vane.research-grounding-check-input/v1.1"`,
		`"history_through_utc":"` + f.snapshotRef.HistoryThroughUTC + `"`,
		`"issue_refs_item_fields":["kind","ref"]`,
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("v1.2 verifier prompt missing %q: %s", want, prompt)
		}
	}

	var context researchSynthesisContextV3
	if err := json.Unmarshal(synthesis.ContextPayload, &context); err != nil {
		t.Fatal(err)
	}
	context.History.HistoryThroughUTC = "not-a-frozen-clock"
	malformedContext, err := json.Marshal(context)
	if err != nil {
		t.Fatal(err)
	}
	brief, canonical, err := types.DecodeResearchBriefPayloadV3(
		grounding.CandidateBriefPayload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildResearchGroundingPromptV1(
		brief, canonical, grounding.CandidateDigest, malformedContext,
		runtimepolicy.ResearchGroundingVerifierRendererVersionV12,
	); err == nil {
		t.Fatal("v1.2 verifier accepted an invalid frozen run clock")
	}
}

func TestResearchBriefGroundingV1PostgresRejectsSemanticMismatchAndTampering(t *testing.T) {
	f := newResearchGroundingFixtureV1(t)
	synthesis, handle, candidate, grounding := prepareGroundingCandidateV1(t, f)
	ensureResearchLLMPriceV3(t, f.st)
	if _, err := f.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, RoundOrdinal: 1,
			SubjectID:    synthesis.ID,
			SystemPrompt: "Verify the candidate against only its cited evidence.",
			UserPrompt:   string(grounding.VerifierPrompt) + " ",
		}); err == nil {
		t.Fatal("tampered verifier prompt consumed quota")
	}
	var verifierReservations int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_run_llm_spend_reservations
		  WHERE run_snapshot_id=$1 AND stage='synthesis' AND round_ordinal=1`,
		f.snapshotRef.SnapshotID).Scan(&verifierReservations); err != nil ||
		verifierReservations != 0 {
		t.Fatalf("tampered prompt reservations=%d err=%v", verifierReservations, err)
	}
	settled := settleGroundingVerifierV1(t, f, synthesis, handle, grounding,
		types.ResearchGroundingUnsupportedV1)
	if settled.Status != ResearchBriefGroundingRejectedV1 {
		t.Fatalf("settled=%+v", settled)
	}
	state, err := f.st.LoadResearchBriefSynthesisV3(t.Context(),
		f.identity, f.snapshotRef, f.planRef)
	if err != nil || state.Status != ResearchBriefSynthesisFailedV3 ||
		state.FailureCode != "citation_grounding_failed" {
		t.Fatalf("synthesis=%+v err=%v", state, err)
	}
	if _, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        candidate, GroundingVerificationID: settled.ID,
		}); err == nil {
		t.Fatal("rejected grounding finalized a Brief")
	}
}

func TestResearchBriefGroundingV1PostgresRejectsRetainedRenderer(t *testing.T) {
	f := newResearchBriefFixtureV3(t, taskstate.NotificationThresholdMajorV3, true)
	prepared, err := f.st.PrepareOrGetResearchBriefSynthesisV3(
		t.Context(), researchBriefPrepareParamsV3(f))
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(
		t, f, prepared.Synthesis)
	candidate := researchBriefPayloadV3(t, prepared.Synthesis,
		types.ResearchBriefSignificanceMajorV3,
		"OpenAI released GPT-5.6 according to the cited official page.")
	settleResearchBriefReceiptV3(t, f, reservation, prepared.Synthesis, candidate)
	if _, err := f.st.PrepareOrGetResearchBriefGroundingV1(t.Context(),
		PrepareResearchBriefGroundingV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			CandidateBriefPayload:               candidate,
		}); err == nil {
		t.Fatal("retained synthesis renderer admitted a V3.3 grounding record")
	}
	var rows int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_brief_grounding_verifications
		  WHERE run_snapshot_id=$1`, f.snapshotRef.SnapshotID).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("retained renderer grounding rows=%d err=%v", rows, err)
	}
}
