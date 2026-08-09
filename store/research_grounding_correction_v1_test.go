package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestResearchGroundingCorrectionV1PostgresAllowsExactlyOneRepairAndReverification(
	t *testing.T,
) {
	f := scopedResearchBriefFixtureV35(t, []byte(canonicalWindowDocumentsV33(t,
		researchWindowDocumentV33{
			Title: "OpenAI release", URL: "https://openai.com/release",
			PublishedAt: fNowForCorrectionV1().Format("2006-01-02T15:04:05Z"),
			Text:        "OpenAI released GPT-5.6.",
		},
	)))
	synthesis, handle, _, grounding := prepareGroundingCandidateV1(t, f)
	grounding = settleGroundingVerifierV1(t, f, synthesis, handle, grounding,
		types.ResearchGroundingUnsupportedV1)
	state, err := f.st.LoadResearchBriefSynthesisV3(t.Context(),
		f.identity, f.snapshotRef, f.planRef)
	if err != nil || state.Status != ResearchBriefSynthesisSpendingV3 {
		t.Fatalf("v3.6 first rejection must remain correctable: state=%+v err=%v", state, err)
	}
	prepared, err := f.st.PrepareOrGetResearchBriefGroundingCorrectionV1(t.Context(),
		PrepareResearchBriefGroundingCorrectionV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			GroundingVerificationID:             grounding.ID,
		})
	if err != nil || !prepared.FirstWriter ||
		prepared.Correction.Status != ResearchBriefGroundingCorrectionPreparedV1 {
		t.Fatalf("prepared=%+v err=%v", prepared, err)
	}
	for _, want := range []string{
		`"schema_version":"vane.research-grounding-correction-input/v1"`,
		`"initial_grounding_input"`, `"initial_verdict"`,
		`citations must be a subset`, grounding.CandidateDigest,
	} {
		if !strings.Contains(string(prepared.Correction.CorrectionPrompt), want) {
			t.Fatalf("correction prompt missing %q: %s", want,
				prepared.Correction.CorrectionPrompt)
		}
	}
	replay, err := f.st.PrepareOrGetResearchBriefGroundingCorrectionV1(t.Context(),
		PrepareResearchBriefGroundingCorrectionV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			GroundingVerificationID:             grounding.ID,
		})
	if err != nil || replay.FirstWriter || replay.Correction.ID != prepared.Correction.ID {
		t.Fatalf("correction replay=%+v err=%v", replay, err)
	}

	seal, err := f.st.LoadResearchRunSnapshotV3(t.Context(), f.identity, f.snapshotRef)
	if err != nil || seal.ResearchModel.GroundingCorrector == nil ||
		seal.ResearchModel.GroundingVerifier == nil {
		t.Fatalf("seal=%+v err=%v", seal.ResearchModel, err)
	}
	ensureResearchLLMPriceV3(t, f.st)
	corrected := researchBriefPayloadV3(t, synthesis,
		types.ResearchBriefSignificanceMajorV3,
		"The cited official page reports that OpenAI released GPT-5.6.")
	corrector := *seal.ResearchModel.GroundingCorrector
	correctorReservation, err := f.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, RoundOrdinal: 2,
			SubjectID: synthesis.ID, SystemPrompt: corrector.SystemPrompt,
			UserPrompt: string(prepared.Correction.CorrectionPrompt),
		})
	if err != nil {
		t.Fatal(err)
	}
	correctedPayload, _, err := types.DecodeResearchBriefPayloadV3(corrected)
	if err != nil {
		t.Fatal(err)
	}
	providerCorrected, err := json.MarshalIndent(correctedPayload, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	correctorCall := researchLLMCallForTestV3(f.identity, f.snapshotRef,
		correctorReservation, corrector.SystemPrompt,
		string(prepared.Correction.CorrectionPrompt))
	correctorCall.Completion = string(providerCorrected)
	correctorCall.PromptTokens, correctorCall.CompletionTokens = 1, 1
	if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.st,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: correctorReservation.ReservationID, Call: correctorCall,
			DisableThinking: correctorReservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		}); err != nil {
		t.Fatal(err)
	}
	correction, err := f.st.SettleResearchBriefGroundingCorrectionCandidateV1(
		t.Context(), SettleResearchBriefGroundingCorrectionCandidateV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			CorrectionID:                        prepared.Correction.ID,
			CorrectorLLMReservationID:           correctorReservation.ReservationID,
			CorrectedBriefPayload:               corrected,
		})
	if err != nil || correction.Status != ResearchBriefGroundingCorrectionCorrectedV1 {
		t.Fatalf("correction=%+v err=%v", correction, err)
	}

	verifier := *seal.ResearchModel.GroundingVerifier
	verifierReservation, err := f.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, RoundOrdinal: 3,
			SubjectID: synthesis.ID, SystemPrompt: verifier.SystemPrompt,
			UserPrompt: string(correction.VerifierPrompt),
		})
	if err != nil {
		t.Fatal(err)
	}
	verdictPayload, err := json.Marshal(types.ResearchGroundingVerdictPayloadV1{
		SchemaVersion:   types.ResearchGroundingVerdictSchemaV1,
		CandidateDigest: correction.CorrectedBriefDigest,
		Verdict:         types.ResearchGroundingGroundedV1,
		Issues:          []types.ResearchGroundingIssueV1{},
	})
	if err != nil {
		t.Fatal(err)
	}
	verifierCall := researchLLMCallForTestV3(f.identity, f.snapshotRef,
		verifierReservation, verifier.SystemPrompt, string(correction.VerifierPrompt))
	verifierCall.Completion = string(verdictPayload)
	verifierCall.PromptTokens, verifierCall.CompletionTokens = 1, 1
	if _, _, err := commitResearchRunLLMReceiptForTestV3(t, f.st,
		CommitResearchRunLLMReceiptV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			ReservationID: verifierReservation.ReservationID, Call: verifierCall,
			DisableThinking: verifierReservation.DisableThinking,
			Attempted:       true, UsageKnown: true, Outcome: ResearchRunLLMCompletedV3,
		}); err != nil {
		t.Fatal(err)
	}
	correction, err = f.st.SettleResearchBriefGroundingCorrectionVerificationV1(
		t.Context(), SettleResearchBriefGroundingCorrectionVerificationV1Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			CorrectionID:                        correction.ID,
			VerifierLLMReservationID:            verifierReservation.ReservationID,
			VerdictPayload:                      verdictPayload,
		})
	if err != nil || correction.Status != ResearchBriefGroundingCorrectionGroundedV1 {
		t.Fatalf("verified correction=%+v err=%v", correction, err)
	}
	ref, err := f.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        corrected, GroundingVerificationID: grounding.ID,
			GroundingCorrectionID: correction.ID,
		})
	if err != nil || !ref.DeliveryRequired {
		t.Fatalf("corrected finalization ref=%+v err=%v", ref, err)
	}
	var rounds int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM research_run_llm_spend_reservations
		  WHERE run_snapshot_id=$1 AND stage='synthesis' AND round_ordinal IN (2,3)`,
		f.snapshotRef.SnapshotID).Scan(&rounds); err != nil || rounds != 2 {
		t.Fatalf("bounded correction rounds=%d err=%v", rounds, err)
	}
	if _, err := f.st.BeginResearchRunLLMSpendV3(t.Context(),
		BeginResearchRunLLMSpendV3Params{
			Identity: f.identity, SnapshotRef: f.snapshotRef,
			Stage: ResearchRunLLMStageSynthesisV3, RoundOrdinal: 4,
			SubjectID: synthesis.ID, SystemPrompt: verifier.SystemPrompt,
			UserPrompt: string(correction.VerifierPrompt),
		}); err == nil {
		t.Fatal("a third grounding pass was admitted")
	}
}

func fNowForCorrectionV1() time.Time {
	return time.Now().UTC().Add(-time.Hour)
}
