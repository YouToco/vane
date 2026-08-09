package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

const (
	researchGroundingInputSchemaV1  = "vane.research-grounding-check-input/v1"
	researchGroundingRecordSchemaV1 = "vane.research-grounding-verification/v1"
)

type ResearchBriefGroundingStatusV1 string

const (
	ResearchBriefGroundingPreparedV1 ResearchBriefGroundingStatusV1 = "prepared"
	ResearchBriefGroundingGroundedV1 ResearchBriefGroundingStatusV1 = "grounded"
	ResearchBriefGroundingRejectedV1 ResearchBriefGroundingStatusV1 = "rejected"
)

type ResearchBriefGroundingV1 struct {
	ID                       int64
	TenantID                 int64
	UserID                   int64
	TaskID                   string
	RunSnapshotID            int64
	PlanID                   int64
	SynthesisID              int64
	CandidateBriefPayload    []byte
	CandidateDigest          string
	VerifierPrompt           []byte
	VerifierPromptDigest     string
	VerifierLLMReservationID *int64
	Status                   ResearchBriefGroundingStatusV1
	VerdictPayload           []byte
	VerdictDigest            string
}

type PrepareResearchBriefGroundingV1Params struct {
	ClaimResearchBriefSynthesisV3Params
	CandidateBriefPayload []byte
}

type PrepareResearchBriefGroundingV1Result struct {
	Grounding   ResearchBriefGroundingV1
	FirstWriter bool
}

type SettleResearchBriefGroundingV1Params struct {
	ClaimResearchBriefSynthesisV3Params
	GroundingID              int64
	VerifierLLMReservationID int64
	VerdictPayload           []byte
}

type researchGroundingEvidenceInputV1 struct {
	Kind                 types.ResearchBriefCitationKindV3 `json:"kind"`
	Ref                  string                            `json:"ref"`
	ToolName             string                            `json:"tool_name,omitempty"`
	TrustType            string                            `json:"trust_type,omitempty"`
	GeneratedAt          string                            `json:"generated_at,omitempty"`
	Coverage             string                            `json:"coverage,omitempty"`
	SynthesisVisibleText string                            `json:"synthesis_visible_text,omitempty"`
	PayloadText          string                            `json:"payload_text,omitempty"`
	ContextTruncated     bool                              `json:"context_truncated"`
}

type researchGroundingResponseContractV1 struct {
	SchemaVersionLiteral   string   `json:"schema_version_literal"`
	RequiredTopLevelFields []string `json:"required_top_level_fields"`
	VerdictValues          []string `json:"verdict_values"`
	GroundedIssuesRule     string   `json:"grounded_issues_rule"`
	UnsupportedIssuesRule  string   `json:"unsupported_issues_rule"`
	IssueFields            []string `json:"issue_fields"`
	IssueRefsItemFields    []string `json:"issue_refs_item_fields,omitempty"`
	IssueRefsKindValues    []string `json:"issue_refs_kind_values,omitempty"`
	IssueRefsRule          string   `json:"issue_refs_rule,omitempty"`
	SingleCanonicalJSON    bool     `json:"single_canonical_json"`
}

type researchGroundingInputV1 struct {
	SchemaVersion    string                              `json:"schema_version"`
	CandidateDigest  string                              `json:"candidate_digest"`
	TaskManual       string                              `json:"task_manual"`
	CandidateBrief   json.RawMessage                     `json:"candidate_brief"`
	CitedEvidence    []researchGroundingEvidenceInputV1  `json:"cited_evidence"`
	ToolFailures     []researchToolFailureContextV31     `json:"tool_failures"`
	ResponseContract researchGroundingResponseContractV1 `json:"response_contract"`
}

func (s *Store) PrepareOrGetResearchBriefGroundingV1(
	ctx context.Context, params PrepareResearchBriefGroundingV1Params,
) (PrepareResearchBriefGroundingV1Result, error) {
	if err := validateResearchBriefSynthesisHandleV3(
		params.ClaimResearchBriefSynthesisV3Params); err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	brief, canonical, err := types.DecodeResearchBriefPayloadV3(params.CandidateBriefPayload)
	if err != nil {
		return PrepareResearchBriefGroundingV1Result{},
			researchRunValidationError("research grounding candidate is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return PrepareResearchBriefGroundingV1Result{},
			researchRunDatabaseError("begin research grounding preparation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return PrepareResearchBriefGroundingV1Result{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	synthesis, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	if synthesis.Status != ResearchBriefSynthesisSpendingV3 {
		return PrepareResearchBriefGroundingV1Result{}, researchRunConflictError()
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx,
		params.Identity, params.SnapshotRef)
	if err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	if seal.ResearchModel.Synthesis.RendererVersion !=
		runtimepolicy.ResearchSynthesisRendererVersionV33 ||
		seal.ResearchModel.GroundingVerifier == nil {
		return PrepareResearchBriefGroundingV1Result{}, researchRunConflictError()
	}
	receiptState, _, err := loadResearchBriefLLMReceiptStateV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params, synthesis)
	if err != nil || receiptState != ResearchBriefLLMReceiptCompletedV3 {
		if err != nil {
			return PrepareResearchBriefGroundingV1Result{}, err
		}
		return PrepareResearchBriefGroundingV1Result{}, researchRunConflictError()
	}
	if err := validateResearchBriefCoverageV31(brief, synthesis.EvidenceManifest); err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	if err := validateResearchBriefCitationsV3(
		brief, synthesis.EvidenceManifest, synthesis.HistoryManifest); err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	candidateDigest := researchRunSHA256(canonical)
	prompt, err := buildResearchGroundingPromptV1(
		brief, canonical, candidateDigest, synthesis.ContextPayload,
		seal.ResearchModel.GroundingVerifier.RendererVersion)
	if err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	}
	if existing, found, err := loadResearchBriefGroundingV1(
		ctx, tx, params.Identity, params.SnapshotRef.SnapshotID,
		params.SynthesisID); err != nil {
		return PrepareResearchBriefGroundingV1Result{}, err
	} else if found {
		if existing.PlanID != params.PlanRef.PlanID ||
			existing.CandidateDigest != candidateDigest ||
			!bytes.Equal(existing.CandidateBriefPayload, canonical) ||
			!bytes.Equal(existing.VerifierPrompt, prompt) {
			return PrepareResearchBriefGroundingV1Result{}, researchRunConflictError()
		}
		if err := tx.Commit(ctx); err != nil {
			return PrepareResearchBriefGroundingV1Result{},
				researchRunDatabaseError("commit research grounding replay", err)
		}
		return PrepareResearchBriefGroundingV1Result{Grounding: existing}, nil
	}
	row, err := scanResearchBriefGroundingV1(tx.QueryRow(ctx,
		`INSERT INTO research_brief_grounding_verifications (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,synthesis_id,
		     candidate_brief_payload,candidate_digest,verifier_prompt,
		     verifier_prompt_digest,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 RETURNING `+researchBriefGroundingColumnsV1,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, params.PlanRef.PlanID, params.SynthesisID,
		canonical, candidateDigest, prompt, researchRunSHA256(prompt),
		researchGroundingRecordSchemaV1))
	if err != nil {
		return PrepareResearchBriefGroundingV1Result{},
			researchRunDatabaseError("insert research grounding verification", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PrepareResearchBriefGroundingV1Result{},
			researchRunDatabaseError("commit research grounding preparation", err)
	}
	return PrepareResearchBriefGroundingV1Result{Grounding: row, FirstWriter: true}, nil
}

func (s *Store) SettleResearchBriefGroundingV1(
	ctx context.Context, params SettleResearchBriefGroundingV1Params,
) (ResearchBriefGroundingV1, error) {
	if err := validateResearchBriefSynthesisHandleV3(
		params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		params.GroundingID <= 0 || params.VerifierLLMReservationID <= 0 {
		return ResearchBriefGroundingV1{},
			researchRunValidationError("research grounding settlement is invalid")
	}
	verdict, canonical, err := types.DecodeResearchGroundingVerdictV1(params.VerdictPayload)
	if err != nil {
		return ResearchBriefGroundingV1{},
			researchRunValidationError("research grounding verdict is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchBriefGroundingV1{},
			researchRunDatabaseError("begin research grounding settlement", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchBriefGroundingV1{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	row, found, err := loadResearchBriefGroundingV1(ctx, tx, params.Identity,
		params.SnapshotRef.SnapshotID, params.SynthesisID)
	if err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	if !found || row.ID != params.GroundingID ||
		row.PlanID != params.PlanRef.PlanID || verdict.CandidateDigest != row.CandidateDigest {
		return ResearchBriefGroundingV1{}, researchRunConflictError()
	}
	candidate, _, err := types.DecodeResearchBriefPayloadV3(row.CandidateBriefPayload)
	if err != nil || !groundingVerdictRefsBoundToCandidateV1(verdict, candidate) {
		return ResearchBriefGroundingV1{}, researchRunIntegrityError()
	}
	wantStatus := ResearchBriefGroundingGroundedV1
	if verdict.Verdict == types.ResearchGroundingUnsupportedV1 {
		wantStatus = ResearchBriefGroundingRejectedV1
	}
	verdictDigest := researchRunSHA256(canonical)
	if row.Status != ResearchBriefGroundingPreparedV1 {
		if row.Status != wantStatus || row.VerifierLLMReservationID == nil ||
			*row.VerifierLLMReservationID != params.VerifierLLMReservationID ||
			row.VerdictDigest != verdictDigest || !bytes.Equal(row.VerdictPayload, canonical) {
			return ResearchBriefGroundingV1{}, researchRunConflictError()
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchBriefGroundingV1{},
				researchRunDatabaseError("commit research grounding settlement replay", err)
		}
		return row, nil
	}
	synthesis, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	if synthesis.Status != ResearchBriefSynthesisSpendingV3 {
		return ResearchBriefGroundingV1{}, researchRunConflictError()
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx,
		params.Identity, params.SnapshotRef)
	if err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	if seal.ResearchModel.Synthesis.RendererVersion !=
		runtimepolicy.ResearchSynthesisRendererVersionV33 ||
		seal.ResearchModel.GroundingVerifier == nil {
		return ResearchBriefGroundingV1{}, researchRunIntegrityError()
	}
	verifierPolicy := *seal.ResearchModel.GroundingVerifier
	reservation, err := loadResearchRunLLMReservationByIDV3(ctx, tx,
		params.Identity, params.SnapshotRef, params.VerifierLLMReservationID, false)
	if err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	if reservation.Stage != ResearchRunLLMStageSynthesisV3 ||
		reservation.RoundOrdinal != 1 || reservation.SubjectID != params.SynthesisID ||
		reservation.UserPromptDigest != row.VerifierPromptDigest ||
		reservation.SystemPromptDigest != researchRunSHA256(
			[]byte(verifierPolicy.SystemPrompt)) ||
		reservation.Model != verifierPolicy.Model ||
		reservation.Temperature != float32(verifierPolicy.Temperature) ||
		reservation.MaxTokens != verifierPolicy.MaxTokens ||
		reservation.DisableThinking != verifierPolicy.DisableThinking ||
		reservation.CountsAgainstPlannerBudget {
		return ResearchBriefGroundingV1{}, researchRunIntegrityError()
	}
	receipt, settled, err := loadResearchRunLLMSettlementV3(ctx, tx, reservation)
	if err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	_, receiptCanonical, normalizeErr := types.NormalizeResearchGroundingVerdictV1(
		[]byte(receipt.Call.Completion))
	if !settled || receipt.Outcome != ResearchRunLLMCompletedV3 ||
		!receipt.Attempted || !receipt.UsageKnown || receipt.LLMCallID <= 0 ||
		receipt.Call.Error != "" || receipt.Call.UserPrompt != string(row.VerifierPrompt) ||
		normalizeErr != nil || !bytes.Equal(receiptCanonical, canonical) {
		return ResearchBriefGroundingV1{}, researchRunConflictError()
	}
	row, err = scanResearchBriefGroundingV1(tx.QueryRow(ctx,
		`UPDATE research_brief_grounding_verifications
		    SET status=$2,verifier_llm_spend_reservation_id=$3,
		        verdict_payload=$4,verdict_digest=$5
		  WHERE id=$1 AND status='prepared'
		  RETURNING `+researchBriefGroundingColumnsV1,
		params.GroundingID, string(wantStatus), params.VerifierLLMReservationID,
		canonical, verdictDigest))
	if err != nil {
		return ResearchBriefGroundingV1{},
			researchRunDatabaseError("settle research grounding verification", err)
	}
	if wantStatus == ResearchBriefGroundingRejectedV1 {
		result, err := tx.Exec(ctx,
			`UPDATE research_brief_syntheses
			    SET status='failed',delivery_required=false,
			        failure_code='citation_grounding_failed'
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND status='spending'`,
			params.SynthesisID, params.Identity.TenantID, params.Identity.UserID)
		if err != nil {
			return ResearchBriefGroundingV1{},
				researchRunDatabaseError("reject ungrounded research Brief", err)
		}
		if result.RowsAffected() != 1 {
			return ResearchBriefGroundingV1{}, researchRunConflictError()
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefGroundingV1{},
			researchRunDatabaseError("commit research grounding settlement", err)
	}
	return row, nil
}

func groundingVerdictRefsBoundToCandidateV1(
	verdict types.ResearchGroundingVerdictPayloadV1,
	candidate types.ResearchBriefPayloadV3,
) bool {
	allowed := make(map[string]struct{}, len(candidate.Citations))
	for _, citation := range candidate.Citations {
		allowed[string(citation.Kind)+"\x00"+citation.Ref] = struct{}{}
	}
	for _, issue := range verdict.Issues {
		for _, citation := range issue.Refs {
			if _, ok := allowed[string(citation.Kind)+"\x00"+citation.Ref]; !ok {
				return false
			}
		}
	}
	return true
}

func buildResearchGroundingPromptV1(
	brief types.ResearchBriefPayloadV3, canonical []byte, candidateDigest string,
	contextPayload []byte, rendererVersion string,
) ([]byte, error) {
	if rendererVersion != runtimepolicy.ResearchGroundingVerifierRendererVersionV1 &&
		rendererVersion != runtimepolicy.ResearchGroundingVerifierRendererVersionV11 {
		return nil, researchRunIntegrityError()
	}
	var synthesis researchSynthesisContextV3
	if json.Unmarshal(contextPayload, &synthesis) != nil ||
		!validResearchSynthesisContextVersionV3(synthesis.SchemaVersion, len(synthesis.ToolFailures)) {
		return nil, researchRunIntegrityError()
	}
	current := make(map[string]researchEvidenceContextItemV3, len(synthesis.CurrentEvidence))
	for _, item := range synthesis.CurrentEvidence {
		current[strconv.FormatInt(item.EvidenceID, 10)] = item
	}
	history := make(map[string]researchHistoryContextItemV3, len(synthesis.History.Items))
	for _, item := range synthesis.History.Items {
		history[item.RecordID] = item
	}
	items := make([]researchGroundingEvidenceInputV1, 0, len(brief.Citations))
	for _, citation := range brief.Citations {
		switch citation.Kind {
		case types.ResearchBriefCitationCurrentEvidenceV3:
			item, ok := current[citation.Ref]
			if !ok {
				return nil, researchRunIntegrityError()
			}
			items = append(items, researchGroundingEvidenceInputV1{
				Kind: citation.Kind, Ref: citation.Ref, ToolName: item.ToolName,
				TrustType: item.TrustType, SynthesisVisibleText: item.SynthesisVisibleText,
				ContextTruncated: item.ContextTruncated,
			})
		case types.ResearchBriefCitationHistoryV3:
			item, ok := history[citation.Ref]
			if !ok {
				return nil, researchRunIntegrityError()
			}
			items = append(items, researchGroundingEvidenceInputV1{
				Kind: citation.Kind, Ref: citation.Ref, GeneratedAt: item.GeneratedAt,
				Coverage: item.Coverage, PayloadText: item.PayloadText,
				ContextTruncated: item.ContextTruncated,
			})
		default:
			return nil, researchRunIntegrityError()
		}
	}
	responseContract := researchGroundingResponseContractV1{
		SchemaVersionLiteral:   types.ResearchGroundingVerdictSchemaV1,
		RequiredTopLevelFields: []string{"schema_version", "candidate_digest", "verdict", "issues"},
		VerdictValues:          []string{"grounded", "unsupported"},
		GroundedIssuesRule:     "issues must be []",
		UnsupportedIssuesRule:  "issues must contain every unsupported claim",
		IssueFields:            []string{"field", "claim", "refs", "reason"},
		SingleCanonicalJSON:    true,
	}
	if rendererVersion == runtimepolicy.ResearchGroundingVerifierRendererVersionV11 {
		responseContract.IssueRefsItemFields = []string{"kind", "ref"}
		responseContract.IssueRefsKindValues = []string{
			string(types.ResearchBriefCitationCurrentEvidenceV3),
			string(types.ResearchBriefCitationHistoryV3),
		}
		responseContract.IssueRefsRule = "refs must be a JSON array of citation objects copied exactly from candidate_brief.citations; each object must contain only kind and ref, never a bare string; use [] when no candidate citation supports the claim"
	}
	prompt, err := json.Marshal(researchGroundingInputV1{
		SchemaVersion: researchGroundingInputSchemaV1, CandidateDigest: candidateDigest,
		TaskManual:     synthesis.Definition.TaskManual,
		CandidateBrief: append(json.RawMessage(nil), canonical...), CitedEvidence: items,
		ToolFailures: append([]researchToolFailureContextV31(nil),
			synthesis.ToolFailures...),
		ResponseContract: responseContract,
	})
	if err != nil || len(prompt) < 2 || len(prompt) > researchRunLLMMaxPromptBytesV3 {
		return nil, researchRunValidationError("research grounding prompt exceeds its budget")
	}
	return prompt, nil
}

const researchBriefGroundingColumnsV1 = `
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,synthesis_id,
    candidate_brief_payload,candidate_digest,verifier_prompt,
    verifier_prompt_digest,verifier_llm_spend_reservation_id,status,
    verdict_payload,verdict_digest`

type researchBriefGroundingScannerV1 interface{ Scan(...any) error }

func scanResearchBriefGroundingV1(scanner researchBriefGroundingScannerV1) (
	ResearchBriefGroundingV1, error,
) {
	var row ResearchBriefGroundingV1
	var status string
	var verdictDigest *string
	err := scanner.Scan(&row.ID, &row.TenantID, &row.UserID, &row.TaskID,
		&row.RunSnapshotID, &row.PlanID, &row.SynthesisID,
		&row.CandidateBriefPayload, &row.CandidateDigest, &row.VerifierPrompt,
		&row.VerifierPromptDigest, &row.VerifierLLMReservationID, &status,
		&row.VerdictPayload, &verdictDigest)
	if err != nil {
		return ResearchBriefGroundingV1{}, err
	}
	row.Status = ResearchBriefGroundingStatusV1(status)
	if verdictDigest != nil {
		row.VerdictDigest = *verdictDigest
	}
	row.CandidateBriefPayload = append([]byte(nil), row.CandidateBriefPayload...)
	row.VerifierPrompt = append([]byte(nil), row.VerifierPrompt...)
	row.VerdictPayload = append([]byte(nil), row.VerdictPayload...)
	return row, nil
}

func loadResearchBriefGroundingV1(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	snapshotID, synthesisID int64,
) (ResearchBriefGroundingV1, bool, error) {
	row, err := scanResearchBriefGroundingV1(tx.QueryRow(ctx,
		`SELECT `+researchBriefGroundingColumnsV1+`
		   FROM research_brief_grounding_verifications
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND run_snapshot_id=$4 AND synthesis_id=$5`,
		identity.TenantID, identity.UserID, identity.TaskID, snapshotID, synthesisID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchBriefGroundingV1{}, false, nil
	}
	if err != nil {
		return ResearchBriefGroundingV1{}, false,
			researchRunDatabaseError("load research grounding verification", err)
	}
	return row, true, nil
}
