package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

const (
	researchGroundingCorrectionInputSchemaV1  = "vane.research-grounding-correction-input/v1"
	researchGroundingCorrectionRecordSchemaV1 = "vane.research-grounding-correction/v1"
)

type ResearchBriefGroundingCorrectionStatusV1 string

const (
	ResearchBriefGroundingCorrectionPreparedV1  ResearchBriefGroundingCorrectionStatusV1 = "prepared"
	ResearchBriefGroundingCorrectionCorrectedV1 ResearchBriefGroundingCorrectionStatusV1 = "corrected"
	ResearchBriefGroundingCorrectionGroundedV1  ResearchBriefGroundingCorrectionStatusV1 = "grounded"
	ResearchBriefGroundingCorrectionRejectedV1  ResearchBriefGroundingCorrectionStatusV1 = "rejected"
)

type ResearchBriefGroundingCorrectionV1 struct {
	ID                        int64
	TenantID                  int64
	UserID                    int64
	TaskID                    string
	RunSnapshotID             int64
	PlanID                    int64
	SynthesisID               int64
	GroundingVerificationID   int64
	CorrectionPrompt          []byte
	CorrectionPromptDigest    string
	CorrectorLLMReservationID *int64
	CorrectedBriefPayload     []byte
	CorrectedBriefDigest      string
	VerifierPrompt            []byte
	VerifierPromptDigest      string
	VerifierLLMReservationID  *int64
	Status                    ResearchBriefGroundingCorrectionStatusV1
	VerdictPayload            []byte
	VerdictDigest             string
}

type PrepareResearchBriefGroundingCorrectionV1Params struct {
	ClaimResearchBriefSynthesisV3Params
	GroundingVerificationID int64
}

type PrepareResearchBriefGroundingCorrectionV1Result struct {
	Correction  ResearchBriefGroundingCorrectionV1
	FirstWriter bool
}

type SettleResearchBriefGroundingCorrectionCandidateV1Params struct {
	ClaimResearchBriefSynthesisV3Params
	CorrectionID              int64
	CorrectorLLMReservationID int64
	CorrectedBriefPayload     []byte
}

type SettleResearchBriefGroundingCorrectionVerificationV1Params struct {
	ClaimResearchBriefSynthesisV3Params
	CorrectionID             int64
	VerifierLLMReservationID int64
	VerdictPayload           []byte
}

type researchGroundingCorrectionResponseContractV1 struct {
	OutputSchema      string `json:"output_schema"`
	CanonicalJSONOnly bool   `json:"canonical_json_only"`
	CitationRule      string `json:"citation_rule"`
	CorrectionRule    string `json:"correction_rule"`
}

type researchGroundingCorrectionInputV1 struct {
	SchemaVersion           string                                        `json:"schema_version"`
	OriginalCandidateDigest string                                        `json:"original_candidate_digest"`
	InitialGroundingInput   json.RawMessage                               `json:"initial_grounding_input"`
	InitialVerdict          json.RawMessage                               `json:"initial_verdict"`
	ResponseContract        researchGroundingCorrectionResponseContractV1 `json:"response_contract"`
}

func (s *Store) PrepareOrGetResearchBriefGroundingCorrectionV1(
	ctx context.Context, params PrepareResearchBriefGroundingCorrectionV1Params,
) (PrepareResearchBriefGroundingCorrectionV1Result, error) {
	if err := validateResearchBriefSynthesisHandleV3(
		params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		params.GroundingVerificationID <= 0 {
		return PrepareResearchBriefGroundingCorrectionV1Result{},
			researchRunValidationError("research grounding correction is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{},
			researchRunDatabaseError("begin research grounding correction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, err
	}
	synthesis, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, err
	}
	if synthesis.Status != ResearchBriefSynthesisSpendingV3 {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, researchRunConflictError()
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx,
		params.Identity, params.SnapshotRef)
	if err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, err
	}
	if seal.ResearchModel.Synthesis.RendererVersion !=
		runtimepolicy.ResearchSynthesisRendererVersionV36 ||
		seal.ResearchModel.GroundingCorrector == nil ||
		seal.ResearchModel.GroundingVerifier == nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, researchRunConflictError()
	}
	grounding, found, err := loadResearchBriefGroundingV1(ctx, tx, params.Identity,
		params.SnapshotRef.SnapshotID, params.SynthesisID)
	if err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, err
	}
	if !found || grounding.ID != params.GroundingVerificationID ||
		grounding.PlanID != params.PlanRef.PlanID ||
		grounding.Status != ResearchBriefGroundingRejectedV1 {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, researchRunConflictError()
	}
	prompt, err := buildResearchGroundingCorrectionPromptV1(grounding)
	if err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, err
	}
	if existing, found, err := loadResearchBriefGroundingCorrectionV1(
		ctx, tx, params.Identity, params.SnapshotRef.SnapshotID,
		params.SynthesisID); err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{}, err
	} else if found {
		if existing.GroundingVerificationID != grounding.ID ||
			!bytes.Equal(existing.CorrectionPrompt, prompt) {
			return PrepareResearchBriefGroundingCorrectionV1Result{}, researchRunConflictError()
		}
		if err := tx.Commit(ctx); err != nil {
			return PrepareResearchBriefGroundingCorrectionV1Result{},
				researchRunDatabaseError("commit research grounding correction replay", err)
		}
		return PrepareResearchBriefGroundingCorrectionV1Result{Correction: existing}, nil
	}
	row, err := scanResearchBriefGroundingCorrectionV1(tx.QueryRow(ctx,
		`INSERT INTO research_brief_grounding_corrections (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,synthesis_id,
		     grounding_verification_id,correction_prompt,
		     correction_prompt_digest,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		 RETURNING `+researchBriefGroundingCorrectionColumnsV1,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, params.PlanRef.PlanID, params.SynthesisID,
		grounding.ID, prompt, researchRunSHA256(prompt),
		researchGroundingCorrectionRecordSchemaV1))
	if err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{},
			researchRunDatabaseError("insert research grounding correction", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PrepareResearchBriefGroundingCorrectionV1Result{},
			researchRunDatabaseError("commit research grounding correction", err)
	}
	return PrepareResearchBriefGroundingCorrectionV1Result{
		Correction: row, FirstWriter: true,
	}, nil
}

func (s *Store) SettleResearchBriefGroundingCorrectionCandidateV1(
	ctx context.Context, params SettleResearchBriefGroundingCorrectionCandidateV1Params,
) (ResearchBriefGroundingCorrectionV1, error) {
	if err := validateResearchBriefSynthesisHandleV3(
		params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		params.CorrectionID <= 0 || params.CorrectorLLMReservationID <= 0 {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunValidationError("research grounding correction candidate is invalid")
	}
	corrected, canonical, err := types.DecodeResearchBriefPayloadV3(
		params.CorrectedBriefPayload)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunValidationError("corrected research Brief is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunDatabaseError("begin corrected research Brief settlement", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	row, found, err := loadResearchBriefGroundingCorrectionV1(ctx, tx,
		params.Identity, params.SnapshotRef.SnapshotID, params.SynthesisID)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	if !found || row.ID != params.CorrectionID || row.PlanID != params.PlanRef.PlanID {
		return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
	}
	correctedDigest := researchRunSHA256(canonical)
	if row.Status != ResearchBriefGroundingCorrectionPreparedV1 {
		if row.CorrectorLLMReservationID == nil ||
			*row.CorrectorLLMReservationID != params.CorrectorLLMReservationID ||
			row.CorrectedBriefDigest != correctedDigest ||
			!bytes.Equal(row.CorrectedBriefPayload, canonical) {
			return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchBriefGroundingCorrectionV1{},
				researchRunDatabaseError("commit corrected research Brief replay", err)
		}
		return row, nil
	}
	synthesis, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil || synthesis.Status != ResearchBriefSynthesisSpendingV3 {
		if err != nil {
			return ResearchBriefGroundingCorrectionV1{}, err
		}
		return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
	}
	grounding, found, err := loadResearchBriefGroundingV1(ctx, tx, params.Identity,
		params.SnapshotRef.SnapshotID, params.SynthesisID)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	if !found || grounding.ID != row.GroundingVerificationID ||
		grounding.Status != ResearchBriefGroundingRejectedV1 {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	original, _, err := types.DecodeResearchBriefPayloadV3(
		grounding.CandidateBriefPayload)
	if err != nil || !researchGroundingCorrectionCitationsSubsetV1(original, corrected) {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	if err := validateResearchBriefCoverageV31(corrected,
		synthesis.EvidenceManifest); err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	if err := validateResearchBriefCitationsV3(corrected, synthesis.ContextPayload,
		synthesis.EvidenceManifest, synthesis.HistoryManifest); err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx,
		params.Identity, params.SnapshotRef)
	if err != nil || seal.ResearchModel.Synthesis.RendererVersion !=
		runtimepolicy.ResearchSynthesisRendererVersionV36 ||
		seal.ResearchModel.GroundingCorrector == nil ||
		seal.ResearchModel.GroundingVerifier == nil {
		if err != nil {
			return ResearchBriefGroundingCorrectionV1{}, err
		}
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	correctorPolicy := *seal.ResearchModel.GroundingCorrector
	reservation, err := loadResearchRunLLMReservationByIDV3(ctx, tx,
		params.Identity, params.SnapshotRef, params.CorrectorLLMReservationID, false)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	if !researchGroundingCorrectionReservationMatchesV1(reservation, params.SynthesisID,
		2, row.CorrectionPromptDigest, correctorPolicy) {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	receipt, settled, err := loadResearchRunLLMSettlementV3(ctx, tx, reservation)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	_, receiptCanonical, normalizeErr := normalizeResearchGroundingCorrectionCompletionV1(
		[]byte(receipt.Call.Completion))
	if !settled || receipt.Outcome != ResearchRunLLMCompletedV3 ||
		!receipt.Attempted || !receipt.UsageKnown || receipt.LLMCallID <= 0 ||
		receipt.Call.Error != "" || receipt.Call.UserPrompt != string(row.CorrectionPrompt) ||
		normalizeErr != nil || !bytes.Equal(receiptCanonical, canonical) {
		return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
	}
	verifierPrompt, err := buildResearchGroundingPromptV1(corrected, canonical,
		correctedDigest, synthesis.ContextPayload,
		seal.ResearchModel.GroundingVerifier.RendererVersion)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	row, err = scanResearchBriefGroundingCorrectionV1(tx.QueryRow(ctx,
		`UPDATE research_brief_grounding_corrections
		    SET status='corrected',corrector_llm_spend_reservation_id=$2,
		        corrected_brief_payload=$3,corrected_brief_digest=$4,
		        verifier_prompt=$5,verifier_prompt_digest=$6
		  WHERE id=$1 AND status='prepared'
		  RETURNING `+researchBriefGroundingCorrectionColumnsV1,
		params.CorrectionID, params.CorrectorLLMReservationID, canonical,
		correctedDigest, verifierPrompt, researchRunSHA256(verifierPrompt)))
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunDatabaseError("settle corrected research Brief", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunDatabaseError("commit corrected research Brief", err)
	}
	return row, nil
}

func (s *Store) SettleResearchBriefGroundingCorrectionVerificationV1(
	ctx context.Context,
	params SettleResearchBriefGroundingCorrectionVerificationV1Params,
) (ResearchBriefGroundingCorrectionV1, error) {
	if err := validateResearchBriefSynthesisHandleV3(
		params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		params.CorrectionID <= 0 || params.VerifierLLMReservationID <= 0 {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunValidationError("corrected research grounding verdict is invalid")
	}
	verdict, canonical, err := types.DecodeResearchGroundingVerdictV1(
		params.VerdictPayload)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunValidationError("corrected research grounding verdict is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunDatabaseError("begin corrected grounding settlement", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	row, found, err := loadResearchBriefGroundingCorrectionV1(ctx, tx,
		params.Identity, params.SnapshotRef.SnapshotID, params.SynthesisID)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	wantStatus := ResearchBriefGroundingCorrectionGroundedV1
	if verdict.Verdict == types.ResearchGroundingUnsupportedV1 {
		wantStatus = ResearchBriefGroundingCorrectionRejectedV1
	}
	verdictDigest := researchRunSHA256(canonical)
	if !found || row.ID != params.CorrectionID || row.PlanID != params.PlanRef.PlanID ||
		verdict.CandidateDigest != row.CorrectedBriefDigest {
		return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
	}
	if row.Status != ResearchBriefGroundingCorrectionCorrectedV1 {
		if row.Status != wantStatus || row.VerifierLLMReservationID == nil ||
			*row.VerifierLLMReservationID != params.VerifierLLMReservationID ||
			row.VerdictDigest != verdictDigest || !bytes.Equal(row.VerdictPayload, canonical) {
			return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchBriefGroundingCorrectionV1{},
				researchRunDatabaseError("commit corrected grounding replay", err)
		}
		return row, nil
	}
	corrected, _, err := types.DecodeResearchBriefPayloadV3(row.CorrectedBriefPayload)
	if err != nil || !groundingVerdictRefsBoundToCandidateV1(verdict, corrected) {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	synthesis, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil || synthesis.Status != ResearchBriefSynthesisSpendingV3 {
		if err != nil {
			return ResearchBriefGroundingCorrectionV1{}, err
		}
		return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx,
		params.Identity, params.SnapshotRef)
	if err != nil || seal.ResearchModel.Synthesis.RendererVersion !=
		runtimepolicy.ResearchSynthesisRendererVersionV36 ||
		seal.ResearchModel.GroundingVerifier == nil {
		if err != nil {
			return ResearchBriefGroundingCorrectionV1{}, err
		}
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	verifierPolicy := *seal.ResearchModel.GroundingVerifier
	reservation, err := loadResearchRunLLMReservationByIDV3(ctx, tx,
		params.Identity, params.SnapshotRef, params.VerifierLLMReservationID, false)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	if !researchGroundingCorrectionReservationMatchesV1(reservation, params.SynthesisID,
		3, row.VerifierPromptDigest, verifierPolicy) {
		return ResearchBriefGroundingCorrectionV1{}, researchRunIntegrityError()
	}
	receipt, settled, err := loadResearchRunLLMSettlementV3(ctx, tx, reservation)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	_, receiptCanonical, normalizeErr := types.NormalizeResearchGroundingVerdictV1(
		[]byte(receipt.Call.Completion))
	if !settled || receipt.Outcome != ResearchRunLLMCompletedV3 ||
		!receipt.Attempted || !receipt.UsageKnown || receipt.LLMCallID <= 0 ||
		receipt.Call.Error != "" || receipt.Call.UserPrompt != string(row.VerifierPrompt) ||
		normalizeErr != nil || !bytes.Equal(receiptCanonical, canonical) {
		return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
	}
	row, err = scanResearchBriefGroundingCorrectionV1(tx.QueryRow(ctx,
		`UPDATE research_brief_grounding_corrections
		    SET status=$2,verifier_llm_spend_reservation_id=$3,
		        verdict_payload=$4,verdict_digest=$5
		  WHERE id=$1 AND status='corrected'
		  RETURNING `+researchBriefGroundingCorrectionColumnsV1,
		params.CorrectionID, string(wantStatus), params.VerifierLLMReservationID,
		canonical, verdictDigest))
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunDatabaseError("settle corrected grounding verification", err)
	}
	if wantStatus == ResearchBriefGroundingCorrectionRejectedV1 {
		result, err := tx.Exec(ctx,
			`UPDATE research_brief_syntheses
			    SET status='failed',delivery_required=false,
			        failure_code='citation_grounding_failed'
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND status='spending'`,
			params.SynthesisID, params.Identity.TenantID, params.Identity.UserID)
		if err != nil {
			return ResearchBriefGroundingCorrectionV1{},
				researchRunDatabaseError("reject corrected research Brief", err)
		}
		if result.RowsAffected() != 1 {
			return ResearchBriefGroundingCorrectionV1{}, researchRunConflictError()
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefGroundingCorrectionV1{},
			researchRunDatabaseError("commit corrected grounding verification", err)
	}
	return row, nil
}

func buildResearchGroundingCorrectionPromptV1(
	grounding ResearchBriefGroundingV1,
) ([]byte, error) {
	if grounding.Status != ResearchBriefGroundingRejectedV1 ||
		grounding.CandidateDigest == "" || len(grounding.VerifierPrompt) < 2 ||
		len(grounding.VerdictPayload) < 2 {
		return nil, researchRunIntegrityError()
	}
	var verifierInput json.RawMessage
	var verdict json.RawMessage
	if json.Unmarshal(grounding.VerifierPrompt, &verifierInput) != nil ||
		json.Unmarshal(grounding.VerdictPayload, &verdict) != nil {
		return nil, researchRunIntegrityError()
	}
	prompt, err := json.Marshal(researchGroundingCorrectionInputV1{
		SchemaVersion:           researchGroundingCorrectionInputSchemaV1,
		OriginalCandidateDigest: grounding.CandidateDigest,
		InitialGroundingInput:   append(json.RawMessage(nil), grounding.VerifierPrompt...),
		InitialVerdict:          append(json.RawMessage(nil), grounding.VerdictPayload...),
		ResponseContract: researchGroundingCorrectionResponseContractV1{
			OutputSchema:      "one canonical vane.research-brief/v3, v3.1, or v3.2 JSON object as permitted by the frozen evidence coverage",
			CanonicalJSONOnly: true,
			CitationRule:      "citations must be a subset of the original candidate citations; never add a ref",
			CorrectionRule:    "resolve every initial_verdict issue by deleting or narrowing unsupported claims; never introduce a new factual claim",
		},
	})
	if err != nil || len(prompt) < 2 || len(prompt) > researchRunLLMMaxPromptBytesV3 {
		return nil, researchRunValidationError("research grounding correction prompt exceeds its budget")
	}
	return prompt, nil
}

func researchGroundingCorrectionCitationsSubsetV1(
	original, corrected types.ResearchBriefPayloadV3,
) bool {
	allowed := make(map[string]struct{}, len(original.Citations))
	for _, citation := range original.Citations {
		allowed[string(citation.Kind)+"\x00"+citation.Ref] = struct{}{}
	}
	for _, citation := range corrected.Citations {
		if _, ok := allowed[string(citation.Kind)+"\x00"+citation.Ref]; !ok {
			return false
		}
	}
	return true
}

func researchGroundingCorrectionReservationMatchesV1(
	reservation researchRunLLMReservationRowV3, synthesisID int64,
	round int, userPromptDigest string, policy runtimepolicy.ResearchModelStageV3,
) bool {
	return reservation.Stage == ResearchRunLLMStageSynthesisV3 &&
		reservation.RoundOrdinal == round && reservation.SubjectID == synthesisID &&
		reservation.UserPromptDigest == userPromptDigest &&
		reservation.SystemPromptDigest == researchRunSHA256([]byte(policy.SystemPrompt)) &&
		reservation.Model == policy.Model &&
		reservation.Temperature == float32(policy.Temperature) &&
		reservation.MaxTokens == policy.MaxTokens &&
		reservation.DisableThinking == policy.DisableThinking &&
		!reservation.CountsAgainstPlannerBudget
}

type researchGroundingCorrectionBriefWireV1 struct {
	SchemaVersion string                                      `json:"schema_version"`
	Assessment    types.ResearchBriefAssessmentV31            `json:"assessment,omitempty"`
	Headline      string                                      `json:"headline"`
	Summary       string                                      `json:"summary"`
	Significance  types.ResearchBriefSignificanceV3           `json:"significance"`
	Citations     []researchGroundingCorrectionCitationWireV1 `json:"citations"`
}

type researchGroundingCorrectionCitationWireV1 struct {
	Kind types.ResearchBriefCitationKindV3 `json:"kind"`
	Ref  json.RawMessage                   `json:"ref"`
}

// normalizeResearchGroundingCorrectionCompletionV1 mirrors the frozen v3.6
// representation repair used by the coordinator, so Store binds the exact
// provider receipt to the same canonical candidate before advancing state.
func normalizeResearchGroundingCorrectionCompletionV1(raw []byte) (
	types.ResearchBriefPayloadV3, []byte, error,
) {
	normalized := bytes.TrimSpace(raw)
	if len(normalized) < 2 || len(normalized) > 256<<10 {
		return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
	}
	if bytes.HasPrefix(normalized, []byte("```")) {
		const open = "```json"
		if !bytes.HasPrefix(normalized, []byte(open)) ||
			!bytes.HasSuffix(normalized, []byte("```")) {
			return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
		}
		remainder := normalized[len(open):]
		if bytes.HasPrefix(remainder, []byte("\r\n")) {
			remainder = remainder[2:]
		} else if bytes.HasPrefix(remainder, []byte("\n")) {
			remainder = remainder[1:]
		} else {
			return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
		}
		remainder = remainder[:len(remainder)-3]
		if bytes.HasSuffix(remainder, []byte("\r\n")) {
			remainder = remainder[:len(remainder)-2]
		} else if bytes.HasSuffix(remainder, []byte("\n")) {
			remainder = remainder[:len(remainder)-1]
		} else {
			return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
		}
		normalized = bytes.TrimSpace(remainder)
		if len(normalized) < 2 || bytes.Contains(normalized, []byte("```")) {
			return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
		}
	}
	var wire researchGroundingCorrectionBriefWireV1
	if err := strictjson.DecodeExact(normalized, &wire); err != nil {
		return types.ResearchBriefPayloadV3{}, nil, err
	}
	citations := make([]types.ResearchBriefCitationV3, len(wire.Citations))
	for index, citation := range wire.Citations {
		ref := bytes.TrimSpace(citation.Ref)
		var value string
		if len(ref) > 0 && ref[0] >= '1' && ref[0] <= '9' &&
			citation.Kind == types.ResearchBriefCitationCurrentEvidenceV3 {
			for _, digit := range ref {
				if digit < '0' || digit > '9' {
					return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
				}
			}
			value = string(ref)
		} else if json.Unmarshal(ref, &value) != nil {
			return types.ResearchBriefPayloadV3{}, nil, types.ErrValidation
		}
		citations[index] = types.ResearchBriefCitationV3{Kind: citation.Kind, Ref: value}
	}
	payload := types.ResearchBriefPayloadV3{
		SchemaVersion: wire.SchemaVersion, Assessment: wire.Assessment,
		Headline: wire.Headline, Summary: wire.Summary,
		Significance: wire.Significance, Citations: citations,
	}
	if err := payload.Validate(); err != nil {
		return types.ResearchBriefPayloadV3{}, nil, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return types.ResearchBriefPayloadV3{}, nil, err
	}
	return payload, canonical, nil
}

const researchBriefGroundingCorrectionColumnsV1 = `
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,synthesis_id,
    grounding_verification_id,correction_prompt,correction_prompt_digest,
    corrector_llm_spend_reservation_id,corrected_brief_payload,
    corrected_brief_digest,verifier_prompt,verifier_prompt_digest,
    verifier_llm_spend_reservation_id,status,verdict_payload,verdict_digest`

type researchBriefGroundingCorrectionScannerV1 interface{ Scan(...any) error }

func scanResearchBriefGroundingCorrectionV1(
	scanner researchBriefGroundingCorrectionScannerV1,
) (ResearchBriefGroundingCorrectionV1, error) {
	var row ResearchBriefGroundingCorrectionV1
	var status string
	var correctedDigest, verifierPromptDigest, verdictDigest *string
	err := scanner.Scan(&row.ID, &row.TenantID, &row.UserID, &row.TaskID,
		&row.RunSnapshotID, &row.PlanID, &row.SynthesisID,
		&row.GroundingVerificationID, &row.CorrectionPrompt,
		&row.CorrectionPromptDigest, &row.CorrectorLLMReservationID,
		&row.CorrectedBriefPayload, &correctedDigest, &row.VerifierPrompt,
		&verifierPromptDigest, &row.VerifierLLMReservationID, &status,
		&row.VerdictPayload, &verdictDigest)
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, err
	}
	row.Status = ResearchBriefGroundingCorrectionStatusV1(status)
	if correctedDigest != nil {
		row.CorrectedBriefDigest = *correctedDigest
	}
	if verifierPromptDigest != nil {
		row.VerifierPromptDigest = *verifierPromptDigest
	}
	if verdictDigest != nil {
		row.VerdictDigest = *verdictDigest
	}
	row.CorrectionPrompt = append([]byte(nil), row.CorrectionPrompt...)
	row.CorrectedBriefPayload = append([]byte(nil), row.CorrectedBriefPayload...)
	row.VerifierPrompt = append([]byte(nil), row.VerifierPrompt...)
	row.VerdictPayload = append([]byte(nil), row.VerdictPayload...)
	return row, nil
}

func loadResearchBriefGroundingCorrectionV1(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	snapshotID, synthesisID int64,
) (ResearchBriefGroundingCorrectionV1, bool, error) {
	row, err := scanResearchBriefGroundingCorrectionV1(tx.QueryRow(ctx,
		`SELECT `+researchBriefGroundingCorrectionColumnsV1+`
		   FROM research_brief_grounding_corrections
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND run_snapshot_id=$4 AND synthesis_id=$5`,
		identity.TenantID, identity.UserID, identity.TaskID, snapshotID, synthesisID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchBriefGroundingCorrectionV1{}, false, nil
	}
	if err != nil {
		return ResearchBriefGroundingCorrectionV1{}, false,
			researchRunDatabaseError("load research grounding correction", err)
	}
	return row, true, nil
}
