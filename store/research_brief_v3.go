package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const (
	researchBriefSynthesisSchemaV3    = "vane.research-brief-synthesis/v3"
	researchEvidenceManifestSchemaV3  = "vane.research-evidence-manifest/v3"
	researchEvidenceManifestSchemaV31 = "vane.research-evidence-manifest/v3.1"
	researchHistoryManifestSchemaV3   = "vane.research-history-manifest/v3"
	researchSynthesisContextSchemaV3  = "vane.research-synthesis-context/v3"
	researchSynthesisContextSchemaV31 = "vane.research-synthesis-context/v3.1"
	researchSynthesisContextMaxV3     = 32 << 20
	researchHistoryContextCharsV3     = 4096
)

type ResearchBriefSynthesisStatusV3 string

type ResearchBriefLLMReceiptStateV3 string

const (
	ResearchBriefSynthesisPreparedV3  ResearchBriefSynthesisStatusV3 = "prepared"
	ResearchBriefSynthesisSpendingV3  ResearchBriefSynthesisStatusV3 = "spending"
	ResearchBriefSynthesisFinalizedV3 ResearchBriefSynthesisStatusV3 = "finalized"
	ResearchBriefSynthesisAmbiguousV3 ResearchBriefSynthesisStatusV3 = "ambiguous"
	ResearchBriefSynthesisFailedV3    ResearchBriefSynthesisStatusV3 = "failed"
)

const (
	ResearchBriefLLMReceiptPendingV3       ResearchBriefLLMReceiptStateV3 = "pending"
	ResearchBriefLLMReceiptCompletedV3     ResearchBriefLLMReceiptStateV3 = "completed"
	ResearchBriefLLMReceiptFailedV3        ResearchBriefLLMReceiptStateV3 = "failed"
	ResearchBriefLLMReceiptIndeterminateV3 ResearchBriefLLMReceiptStateV3 = "indeterminate"
)

type PrepareResearchBriefSynthesisV3Params struct {
	Identity    types.RunIdentity
	SnapshotRef types.ResearchRunSnapshotRefV3
	PlanRef     types.ResearchRunPlanRefV3
}

type ResearchBriefSynthesisV3 struct {
	ID                        int64
	TenantID                  int64
	UserID                    int64
	TaskID                    string
	RunSnapshotID             int64
	PlanID                    int64
	TemporalWorkflowID        string
	TemporalRunID             string
	DefinitionDigest          string
	PlanDigest                string
	NotificationThreshold     string
	RequestDigest             string
	ContextPayload            []byte
	ContextDigest             string
	EvidenceManifest          []byte
	EvidenceDigest            string
	HistoryManifest           []byte
	HistoryDigest             string
	SynthesisLLMReservationID *int64
	Status                    ResearchBriefSynthesisStatusV3
	Significance              types.ResearchBriefSignificanceV3
	Decision                  types.ResearchBriefDecisionV3
	DeliveryRequired          *bool
	BriefPayload              []byte
	BriefDigest               string
	FailureCode               string
	SpendingStartedAt         *time.Time
	FinalizedAt               *time.Time
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

type PrepareResearchBriefSynthesisV3Result struct {
	Synthesis       ResearchBriefSynthesisV3
	FirstWriter     bool
	PartialCoverage bool
}

type ClaimResearchBriefSynthesisV3Params struct {
	Identity                  types.RunIdentity
	SnapshotRef               types.ResearchRunSnapshotRefV3
	PlanRef                   types.ResearchRunPlanRefV3
	SynthesisID               int64
	RequestDigest             string
	SynthesisLLMReservationID int64
}

type ClaimResearchBriefSynthesisV3Result struct {
	Synthesis    ResearchBriefSynthesisV3
	Claimed      bool
	ReceiptState ResearchBriefLLMReceiptStateV3
	LLMCallID    int64
}

type FinalizeResearchBriefSynthesisV3Params struct {
	ClaimResearchBriefSynthesisV3Params
	BriefPayload []byte
}

type FailResearchBriefSynthesisV3Params struct {
	ClaimResearchBriefSynthesisV3Params
	Status      ResearchBriefSynthesisStatusV3
	FailureCode string
}

type ResearchBriefV3 struct {
	Ref     types.ResearchBriefRefV3
	Payload []byte
}

type LoadResearchHistoryChunkV3Params struct {
	ClaimResearchBriefSynthesisV3Params
	RecordID    string
	OffsetChars int
	LimitChars  int
}

type ResearchHistoryChunkV3 struct {
	RecordID        string
	OffsetChars     int
	NextOffsetChars int
	TotalChars      int
	TotalBytes      int
	Text            string
	ChunkDigest     string
	FullDigest      string
	Complete        bool
}

type researchEvidenceManifestItemV3 struct {
	EvidenceID    int64  `json:"evidence_id"`
	Ordinal       int    `json:"ordinal"`
	InvocationID  string `json:"invocation_id"`
	ToolName      string `json:"tool_name"`
	RequestDigest string `json:"request_digest"`
	ResultDigest  string `json:"result_digest"`
	OriginalSize  int    `json:"original_size"`
	Truncated     bool   `json:"truncated"`
	TrustType     string `json:"trust_type"`
}

type researchEvidenceManifestV3 struct {
	SchemaVersion string                           `json:"schema_version"`
	Items         []researchEvidenceManifestItemV3 `json:"items"`
	ToolFailures  []researchToolFailureContextV31  `json:"tool_failures,omitempty"`
}

type researchEvidenceContextItemV3 struct {
	researchEvidenceManifestItemV3
	SynthesisVisibleText string `json:"synthesis_visible_text"`
	ContextStoredSize    int    `json:"context_stored_size"`
	ContextVisibleSize   int    `json:"context_visible_size"`
	ContextVisibleDigest string `json:"context_visible_digest"`
	ContextTruncated     bool   `json:"context_truncated"`
}

// researchToolFailureContextV31 is a sanitized, immutable coverage fact. It
// deliberately carries no provider prose or result bytes and therefore can
// inform a quiet/unknown synthesis without being cited as current Evidence.
type researchToolFailureContextV31 struct {
	Ordinal       int    `json:"ordinal"`
	InvocationID  string `json:"invocation_id"`
	ToolName      string `json:"tool_name"`
	RequestDigest string `json:"request_digest"`
	Phase         string `json:"phase"`
	ErrorCode     string `json:"error_code"`
	CostMicroUSD  int64  `json:"cost_micro_usd"`
}

type researchHistoryManifestItemV3 struct {
	Kind          string `json:"kind"`
	RecordID      string `json:"record_id"`
	RunSnapshotID int64  `json:"run_snapshot_id"`
	GeneratedAt   string `json:"generated_at"`
	Digest        string `json:"digest,omitempty"`
	Coverage      string `json:"coverage"`
}

type researchHistoryContextItemV3 struct {
	researchHistoryManifestItemV3
	PayloadText          string `json:"payload_text,omitempty"`
	GapReason            string `json:"gap_reason,omitempty"`
	ContextStoredSize    int    `json:"context_stored_size"`
	ContextVisibleSize   int    `json:"context_visible_size"`
	ContextVisibleDigest string `json:"context_visible_digest"`
	ContextTruncated     bool   `json:"context_truncated"`
}

type researchHistoryManifestV3 struct {
	SchemaVersion     string                          `json:"schema_version"`
	HistoryThroughUTC string                          `json:"history_through_utc"`
	CandidateCount    int                             `json:"candidate_count"`
	ReturnedCount     int                             `json:"returned_count"`
	Truncated         bool                            `json:"truncated"`
	Continuation      *researchHistoryContinuationV3  `json:"continuation,omitempty"`
	Items             []researchHistoryManifestItemV3 `json:"items"`
}

type researchHistoryContinuationV3 struct {
	GeneratedAt string `json:"generated_at"`
	Kind        string `json:"kind"`
	RecordID    string `json:"record_id"`
}

type researchHistoryContextV3 struct {
	HistoryThroughUTC string                         `json:"history_through_utc"`
	CandidateCount    int                            `json:"candidate_count"`
	ReturnedCount     int                            `json:"returned_count"`
	Truncated         bool                           `json:"truncated"`
	Continuation      *researchHistoryContinuationV3 `json:"continuation,omitempty"`
	Items             []researchHistoryContextItemV3 `json:"items"`
}

type researchSynthesisDefinitionContextV3 struct {
	TaskName     string                         `json:"task_name"`
	TaskManual   string                         `json:"task_manual"`
	Output       taskstate.OutputPreferenceV3   `json:"output"`
	Notification taskstate.NotificationPolicyV3 `json:"notification"`
}

type researchSynthesisContextV3 struct {
	SchemaVersion   string                               `json:"schema_version"`
	Definition      researchSynthesisDefinitionContextV3 `json:"definition"`
	CurrentEvidence []researchEvidenceContextItemV3      `json:"current_evidence"`
	ToolFailures    []researchToolFailureContextV31      `json:"tool_failures,omitempty"`
	History         researchHistoryContextV3             `json:"history"`
}

type researchBriefRequestDigestV3 struct {
	SchemaVersion         string `json:"schema_version"`
	RunSnapshotID         int64  `json:"run_snapshot_id"`
	PlanID                int64  `json:"plan_id"`
	DefinitionDigest      string `json:"definition_digest"`
	PlanDigest            string `json:"plan_digest"`
	NotificationThreshold string `json:"notification_threshold"`
	ContextDigest         string `json:"context_digest"`
	EvidenceDigest        string `json:"evidence_digest"`
	HistoryDigest         string `json:"history_digest"`
}

func (s *Store) PrepareOrGetResearchBriefSynthesisV3(
	ctx context.Context, params PrepareResearchBriefSynthesisV3Params,
) (PrepareResearchBriefSynthesisV3Result, error) {
	if err := validateResearchBriefSynthesisScopeV3(params.Identity,
		params.SnapshotRef, params.PlanRef); err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunDatabaseError("begin research Brief synthesis transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunIntegrityError()
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunDatabaseError("lock research Brief schema admission", err)
	}
	tenantExists, err := lockTenantAdmissionRootShared(ctx, tx, params.Identity.TenantID)
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunDatabaseError(
			"lock research Brief tenant admission", err)
	}
	if !tenantExists {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunValidationError(
			"research Brief tenant is unavailable")
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx, params.Identity.TemporalRunID); err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	}
	snapshotSeal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx, params.Identity, params.SnapshotRef)
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	}
	_, planRow, err := loadAndValidateResearchRunPlanV3(ctx, tx, params.Identity,
		params.SnapshotRef.SnapshotID, params.PlanRef)
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	}
	threshold := string(snapshotSeal.Payload.Definition.Notification.MinimumSignificance)
	if existing, found, err := loadResearchBriefSynthesisByPlanV3(ctx, tx,
		params.Identity, params.SnapshotRef.SnapshotID, planRow.ID); err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	} else if found {
		// CreateOrGet recovery is bound to the bytes frozen by the first writer.
		// Rebuilding mutable history here would turn a cutoff-eligible late commit
		// into a false conflict after the model may already have been paid.
		if !researchBriefSynthesisScopeEqualV3(existing, params, threshold) {
			return PrepareResearchBriefSynthesisV3Result{}, researchRunConflictError()
		}
		if !researchBriefSynthesisFrozenPayloadsValidV3(existing) {
			return PrepareResearchBriefSynthesisV3Result{}, researchRunIntegrityError()
		}
		if err := tx.Commit(ctx); err != nil {
			return PrepareResearchBriefSynthesisV3Result{}, researchRunDatabaseError("commit research Brief synthesis replay", err)
		}
		return PrepareResearchBriefSynthesisV3Result{
			Synthesis: existing, PartialCoverage: researchBriefSynthesisPartialCoverageV31(existing),
		}, nil
	}
	evidencePayload, evidenceContext, toolFailures, err :=
		buildResearchEvidenceManifestV3(ctx, tx, params.Identity, params.PlanRef)
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	}
	historyPayload, historyContext, err := buildResearchHistoryManifestV3(ctx, tx, params.Identity,
		params.SnapshotRef, params.PlanRef.PlanID)
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, err
	}
	contextSchema := researchSynthesisContextSchemaV3
	if len(toolFailures) > 0 {
		contextSchema = researchSynthesisContextSchemaV31
	}
	contextPayload, err := json.Marshal(researchSynthesisContextV3{
		SchemaVersion: contextSchema,
		Definition: researchSynthesisDefinitionContextV3{
			TaskName:     snapshotSeal.Payload.Definition.TaskName,
			TaskManual:   snapshotSeal.Payload.Definition.TaskManual,
			Output:       snapshotSeal.Payload.Definition.Output,
			Notification: snapshotSeal.Payload.Definition.Notification,
		},
		CurrentEvidence: evidenceContext, ToolFailures: toolFailures,
		History: historyContext,
	})
	if err != nil || len(contextPayload) < 2 || len(contextPayload) > researchSynthesisContextMaxV3 {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunValidationError("research synthesis exact context exceeds its budget")
	}
	contextDigest := researchRunSHA256(contextPayload)
	evidenceDigest := researchRunSHA256(evidencePayload)
	historyDigest := researchRunSHA256(historyPayload)
	requestDigest := digestResearchBriefRequestV3(researchBriefRequestDigestV3{
		SchemaVersion:         researchBriefSynthesisSchemaV3,
		RunSnapshotID:         params.SnapshotRef.SnapshotID,
		PlanID:                params.PlanRef.PlanID,
		DefinitionDigest:      params.SnapshotRef.DefinitionDigest,
		PlanDigest:            params.PlanRef.PlanDigest,
		NotificationThreshold: threshold, ContextDigest: contextDigest,
		EvidenceDigest: evidenceDigest, HistoryDigest: historyDigest,
	})
	row, err := scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`INSERT INTO research_brief_syntheses (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,
		     temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
		     notification_threshold,request_digest,context_payload,context_digest,
		     evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		 RETURNING `+researchBriefSynthesisColumnsV3,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, planRow.ID, params.Identity.TemporalWorkflowID,
		params.Identity.TemporalRunID, params.SnapshotRef.DefinitionDigest,
		params.PlanRef.PlanDigest, threshold, requestDigest, contextPayload, contextDigest,
		evidencePayload, evidenceDigest, historyPayload, historyDigest,
		researchBriefSynthesisSchemaV3))
	if err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunDatabaseError("insert research Brief synthesis", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PrepareResearchBriefSynthesisV3Result{}, researchRunDatabaseError("commit research Brief synthesis", err)
	}
	return PrepareResearchBriefSynthesisV3Result{
		Synthesis: row, FirstWriter: true, PartialCoverage: len(toolFailures) > 0,
	}, nil
}

func (s *Store) ClaimResearchBriefSynthesisV3(
	ctx context.Context, params ClaimResearchBriefSynthesisV3Params,
) (ClaimResearchBriefSynthesisV3Result, error) {
	if err := validateResearchBriefSynthesisHandleV3(params); err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, err
	}
	if params.SynthesisLLMReservationID <= 0 {
		return ClaimResearchBriefSynthesisV3Result{},
			researchRunValidationError("research Brief model reservation is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, researchRunDatabaseError("begin research Brief claim", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ClaimResearchBriefSynthesisV3Result{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx, params.Identity.TemporalRunID); err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, err
	}
	row, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx, params)
	if err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, err
	}
	if row.Status == ResearchBriefSynthesisSpendingV3 {
		if row.SynthesisLLMReservationID == nil ||
			*row.SynthesisLLMReservationID != params.SynthesisLLMReservationID {
			return ClaimResearchBriefSynthesisV3Result{}, researchRunConflictError()
		}
		state, callID, err := loadResearchBriefLLMReceiptStateV3(ctx, tx, params, row)
		if err != nil {
			return ClaimResearchBriefSynthesisV3Result{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ClaimResearchBriefSynthesisV3Result{}, researchRunDatabaseError(
				"commit research Brief receipt recovery", err)
		}
		return ClaimResearchBriefSynthesisV3Result{
			Synthesis: row, ReceiptState: state, LLMCallID: callID,
		}, nil
	}
	if row.Status != ResearchBriefSynthesisPreparedV3 {
		if row.SynthesisLLMReservationID == nil ||
			*row.SynthesisLLMReservationID != params.SynthesisLLMReservationID {
			return ClaimResearchBriefSynthesisV3Result{}, researchRunConflictError()
		}
		state, callID, err := loadResearchBriefLLMReceiptStateV3(ctx, tx, params, row)
		if err != nil {
			return ClaimResearchBriefSynthesisV3Result{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ClaimResearchBriefSynthesisV3Result{}, researchRunDatabaseError("commit research Brief claim replay", err)
		}
		return ClaimResearchBriefSynthesisV3Result{
			Synthesis: row, ReceiptState: state, LLMCallID: callID,
		}, nil
	}
	if err := authorizeResearchRunEffectV3(ctx, tx, params.Identity); err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, err
	}
	row, err = scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`UPDATE research_brief_syntheses
		    SET status='spending',synthesis_llm_spend_reservation_id=$4
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND status='prepared'
		  RETURNING `+researchBriefSynthesisColumnsV3,
		params.SynthesisID, params.Identity.TenantID, params.Identity.UserID,
		params.SynthesisLLMReservationID))
	if err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, researchRunDatabaseError("claim research Brief synthesis spend", err)
	}
	state, callID, err := loadResearchBriefLLMReceiptStateV3(ctx, tx, params, row)
	if err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimResearchBriefSynthesisV3Result{}, researchRunDatabaseError("commit research Brief synthesis claim", err)
	}
	return ClaimResearchBriefSynthesisV3Result{
		Synthesis: row, Claimed: state == ResearchBriefLLMReceiptPendingV3,
		ReceiptState: state, LLMCallID: callID,
	}, nil
}

func (s *Store) FinalizeResearchBriefSynthesisV3(
	ctx context.Context, params FinalizeResearchBriefSynthesisV3Params,
) (types.ResearchBriefRefV3, error) {
	if err := validateResearchBriefSynthesisHandleV3(params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		params.SynthesisLLMReservationID <= 0 {
		return types.ResearchBriefRefV3{}, researchRunValidationError("research Brief finalization is invalid")
	}
	brief, briefPayload, err := types.DecodeResearchBriefPayloadV3(params.BriefPayload)
	if err != nil {
		return types.ResearchBriefRefV3{}, researchRunValidationError("research Brief payload is invalid")
	}
	briefDigest := researchRunSHA256(briefPayload)
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return types.ResearchBriefRefV3{}, researchRunDatabaseError("begin research Brief finalization", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return types.ResearchBriefRefV3{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx, params.Identity.TemporalRunID); err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	row, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	if row.SynthesisLLMReservationID == nil ||
		*row.SynthesisLLMReservationID != params.SynthesisLLMReservationID {
		return types.ResearchBriefRefV3{}, researchRunConflictError()
	}
	receiptState, _, err := loadResearchBriefLLMReceiptStateV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params, row)
	if err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	if receiptState != ResearchBriefLLMReceiptCompletedV3 {
		return types.ResearchBriefRefV3{}, researchRunConflictError()
	}
	if row.Status == ResearchBriefSynthesisFinalizedV3 {
		if row.Significance != brief.Significance || row.BriefDigest != briefDigest ||
			!bytes.Equal(row.BriefPayload, briefPayload) {
			return types.ResearchBriefRefV3{}, researchRunConflictError()
		}
		ref, err := researchBriefRefFromSynthesisV3(row)
		if err != nil {
			return types.ResearchBriefRefV3{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return types.ResearchBriefRefV3{}, researchRunDatabaseError("commit research Brief finalization replay", err)
		}
		return ref, nil
	}
	if row.Status != ResearchBriefSynthesisSpendingV3 {
		return types.ResearchBriefRefV3{}, researchRunConflictError()
	}
	if err := validateResearchBriefCoverageV31(
		brief, row.EvidenceManifest); err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	if err := validateResearchBriefCitationsV3(brief, row.EvidenceManifest, row.HistoryManifest); err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	decision, deliveryRequired, err := researchBriefNotificationDecisionV3(
		row.NotificationThreshold, brief.Significance)
	if err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	row, err = scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`UPDATE research_brief_syntheses
		    SET status='finalized',significance=$2,decision=$3,
		        delivery_required=$4,brief_payload=$5,brief_digest=$6
		  WHERE id=$1 AND status='spending'
		  RETURNING `+researchBriefSynthesisColumnsV3,
		params.SynthesisID, string(brief.Significance), string(decision),
		deliveryRequired, briefPayload, briefDigest))
	if err != nil {
		return types.ResearchBriefRefV3{}, researchRunDatabaseError("finalize research Brief synthesis", err)
	}
	ref, err := researchBriefRefFromSynthesisV3(row)
	if err != nil {
		return types.ResearchBriefRefV3{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchBriefRefV3{}, researchRunDatabaseError("commit research Brief finalization", err)
	}
	return ref, nil
}

func (s *Store) FailResearchBriefSynthesisV3(
	ctx context.Context, params FailResearchBriefSynthesisV3Params,
) (ResearchBriefSynthesisV3, error) {
	if err := validateResearchBriefSynthesisHandleV3(params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		(params.Status != ResearchBriefSynthesisAmbiguousV3 && params.Status != ResearchBriefSynthesisFailedV3) ||
		!validResearchRunErrorCode(params.FailureCode) {
		return ResearchBriefSynthesisV3{}, researchRunValidationError("research Brief failure is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchBriefSynthesisV3{}, researchRunDatabaseError("begin research Brief failure", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchBriefSynthesisV3{}, researchRunIntegrityError()
	}
	if err := lockResearchBriefSynthesisV3(ctx, tx, params.Identity.TemporalRunID); err != nil {
		return ResearchBriefSynthesisV3{}, err
	}
	row, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil {
		return ResearchBriefSynthesisV3{}, err
	}
	if (row.SynthesisLLMReservationID == nil && params.SynthesisLLMReservationID > 0) ||
		(row.SynthesisLLMReservationID != nil &&
			(params.SynthesisLLMReservationID <= 0 ||
				*row.SynthesisLLMReservationID != params.SynthesisLLMReservationID)) {
		return ResearchBriefSynthesisV3{}, researchRunConflictError()
	}
	if row.Status == params.Status && row.FailureCode == params.FailureCode {
		if err := tx.Commit(ctx); err != nil {
			return ResearchBriefSynthesisV3{}, researchRunDatabaseError("commit research Brief failure replay", err)
		}
		return row, nil
	}
	if (row.Status == ResearchBriefSynthesisPreparedV3 && params.Status != ResearchBriefSynthesisFailedV3) ||
		(row.Status != ResearchBriefSynthesisPreparedV3 && row.Status != ResearchBriefSynthesisSpendingV3) {
		return ResearchBriefSynthesisV3{}, researchRunConflictError()
	}
	row, err = scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`UPDATE research_brief_syntheses
		    SET status=$2,delivery_required=false,failure_code=$3
		  WHERE id=$1 AND status=$4
		  RETURNING `+researchBriefSynthesisColumnsV3,
		params.SynthesisID, string(params.Status), params.FailureCode, string(row.Status)))
	if err != nil {
		return ResearchBriefSynthesisV3{}, researchRunDatabaseError("finalize research Brief failure", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefSynthesisV3{}, researchRunDatabaseError("commit research Brief failure", err)
	}
	return row, nil
}

func (s *Store) LoadResearchBriefSynthesisV3(
	ctx context.Context, identity types.RunIdentity,
	snapshotRef types.ResearchRunSnapshotRefV3, planRef types.ResearchRunPlanRefV3,
) (ResearchBriefSynthesisV3, error) {
	if err := validateResearchBriefSynthesisScopeV3(identity, snapshotRef, planRef); err != nil {
		return ResearchBriefSynthesisV3{}, err
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, identity, snapshotRef.SnapshotID)
	if err != nil {
		return ResearchBriefSynthesisV3{}, researchRunDatabaseError("begin research Brief read", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != snapshotRef {
		return ResearchBriefSynthesisV3{}, researchRunIntegrityError()
	}
	row, found, err := loadResearchBriefSynthesisByPlanV3(ctx, tx, identity,
		snapshotRef.SnapshotID, planRef.PlanID)
	if err != nil {
		return ResearchBriefSynthesisV3{}, err
	}
	if !found || row.DefinitionDigest != snapshotRef.DefinitionDigest ||
		row.PlanDigest != planRef.PlanDigest {
		return ResearchBriefSynthesisV3{}, researchRunValidationError("research Brief synthesis is unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefSynthesisV3{}, researchRunDatabaseError("commit research Brief read", err)
	}
	return row, nil
}

func (s *Store) LoadResearchBriefV3(
	ctx context.Context, identity types.RunIdentity, ref types.ResearchBriefRefV3,
) (ResearchBriefV3, error) {
	if err := ref.ValidateFor(identity, ref.RunSnapshotID, ref.PlanID); err != nil {
		return ResearchBriefV3{}, researchRunValidationError("research Brief reference is invalid")
	}
	tx, _, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, identity, ref.RunSnapshotID)
	if err != nil {
		return ResearchBriefV3{}, researchRunDatabaseError("begin research Brief artifact read", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	row, err := scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`SELECT `+researchBriefSynthesisColumnsV3+`
		   FROM research_brief_syntheses
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		    AND run_snapshot_id=$5 AND plan_id=$6 AND status='finalized'`,
		ref.BriefID, identity.TenantID, identity.UserID, identity.TaskID,
		ref.RunSnapshotID, ref.PlanID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchBriefV3{}, researchRunValidationError("research Brief is unavailable")
	}
	if err != nil {
		return ResearchBriefV3{}, researchRunDatabaseError("load research Brief artifact", err)
	}
	storedRef, err := researchBriefRefFromSynthesisV3(row)
	if err != nil || storedRef != ref {
		return ResearchBriefV3{}, researchRunIntegrityError()
	}
	_, canonicalBrief, err := types.DecodeResearchBriefPayloadV3(row.BriefPayload)
	if err != nil || !bytes.Equal(canonicalBrief, row.BriefPayload) {
		return ResearchBriefV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefV3{}, researchRunDatabaseError("commit research Brief artifact read", err)
	}
	return ResearchBriefV3{Ref: storedRef, Payload: append([]byte(nil), row.BriefPayload...)}, nil
}

// LoadResearchHistoryChunkV3 is the only continuation path for a truncated
// model-visible history item. It remains bound to the frozen top-20 manifest,
// same-owner scope and full artifact digest; callers cannot turn it into an
// arbitrary legacy-table reader.
func (s *Store) LoadResearchHistoryChunkV3(
	ctx context.Context, params LoadResearchHistoryChunkV3Params,
) (ResearchHistoryChunkV3, error) {
	if err := validateResearchBriefSynthesisHandleV3(params.ClaimResearchBriefSynthesisV3Params); err != nil ||
		params.RecordID == "" || len(params.RecordID) > 255 || params.OffsetChars < 0 ||
		params.LimitChars < 1 || params.LimitChars > researchHistoryContextCharsV3 {
		return ResearchHistoryChunkV3{}, researchRunValidationError("research history chunk cursor is invalid")
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(
		ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, params.Identity,
		params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchHistoryChunkV3{}, researchRunDatabaseError("begin research history chunk read", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchHistoryChunkV3{}, researchRunIntegrityError()
	}
	row, err := loadAndValidateResearchBriefSynthesisHandleV3(ctx, tx,
		params.ClaimResearchBriefSynthesisV3Params)
	if err != nil {
		return ResearchHistoryChunkV3{}, err
	}
	var manifest researchHistoryManifestV3
	if err := json.Unmarshal(row.HistoryManifest, &manifest); err != nil ||
		manifest.SchemaVersion != researchHistoryManifestSchemaV3 {
		return ResearchHistoryChunkV3{}, researchRunIntegrityError()
	}
	var expectedDigest string
	for _, item := range manifest.Items {
		if item.RecordID == params.RecordID && item.Coverage != "unavailable" {
			expectedDigest = item.Digest
			break
		}
	}
	if expectedDigest == "" {
		return ResearchHistoryChunkV3{}, researchRunValidationError("research history chunk is outside the frozen manifest")
	}
	var chunk ResearchHistoryChunkV3
	err = tx.QueryRow(ctx,
		`SELECT record_id,chunk_offset_chars,next_offset_chars,total_chars,total_bytes,
		        chunk_text,chunk_digest,full_digest,complete
		   FROM read_research_history_content_cap_v3($1,$2,$3,$4,$5,$6,$7,$8)`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SynthesisID, params.RequestDigest, params.RecordID,
		params.OffsetChars, params.LimitChars).Scan(
		&chunk.RecordID, &chunk.OffsetChars, &chunk.NextOffsetChars,
		&chunk.TotalChars, &chunk.TotalBytes, &chunk.Text,
		&chunk.ChunkDigest, &chunk.FullDigest, &chunk.Complete)
	if err != nil {
		return ResearchHistoryChunkV3{}, researchRunDatabaseError("read research history chunk", err)
	}
	if chunk.RecordID != params.RecordID || chunk.OffsetChars != params.OffsetChars ||
		chunk.NextOffsetChars < chunk.OffsetChars || chunk.NextOffsetChars > chunk.TotalChars ||
		chunk.TotalBytes < len(chunk.Text) || chunk.FullDigest != expectedDigest ||
		!validResearchRunDigest(chunk.ChunkDigest) ||
		researchRunSHA256([]byte(chunk.Text)) != chunk.ChunkDigest ||
		chunk.Complete != (chunk.NextOffsetChars == chunk.TotalChars) {
		return ResearchHistoryChunkV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchHistoryChunkV3{}, researchRunDatabaseError("commit research history chunk read", err)
	}
	return chunk, nil
}

func validateResearchBriefSynthesisScopeV3(
	identity types.RunIdentity, snapshotRef types.ResearchRunSnapshotRefV3,
	planRef types.ResearchRunPlanRefV3,
) error {
	if snapshotRef.ValidateFor(identity) != nil ||
		planRef.ValidateFor(identity, snapshotRef.SnapshotID) != nil ||
		snapshotRef.DefinitionDigest != planRef.DefinitionDigest {
		return researchRunValidationError("research Brief synthesis scope is invalid")
	}
	return nil
}

func validateResearchBriefSynthesisHandleV3(params ClaimResearchBriefSynthesisV3Params) error {
	if err := validateResearchBriefSynthesisScopeV3(params.Identity,
		params.SnapshotRef, params.PlanRef); err != nil || params.SynthesisID <= 0 ||
		!validResearchRunDigest(params.RequestDigest) {
		return researchRunValidationError("research Brief synthesis handle is invalid")
	}
	return nil
}

func loadAndValidateResearchBriefSnapshotV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	ref types.ResearchRunSnapshotRefV3,
) (runcontext.ResearchSnapshotSealV3, error) {
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		TemporalWorkflowID: identity.TemporalWorkflowID, TemporalRunID: identity.TemporalRunID,
	}
	row, found, err := loadResearchRunSnapshotRowV3(ctx, tx, lookup)
	if err != nil || !found {
		if err != nil {
			return runcontext.ResearchSnapshotSealV3{}, err
		}
		return runcontext.ResearchSnapshotSealV3{}, researchRunValidationError("research Brief snapshot is unavailable")
	}
	storedRef, err := validateStoredResearchRunSnapshotV3(identity, row)
	if err != nil || storedRef != ref {
		return runcontext.ResearchSnapshotSealV3{}, researchRunIntegrityError()
	}
	seal, err := runcontext.DecodeResearchSnapshotPayloadV3(row.Payload)
	if err != nil {
		return runcontext.ResearchSnapshotSealV3{}, researchRunIntegrityError()
	}
	return seal, nil
}

func buildResearchEvidenceManifestV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	planRef types.ResearchRunPlanRefV3,
) ([]byte, []researchEvidenceContextItemV3, []researchToolFailureContextV31, error) {
	rows, err := tx.Query(ctx,
		`SELECT evidence.id,evidence.step_ordinal,evidence.invocation_id,
		        evidence.tool_name,evidence.request_digest,evidence.result_digest,
		        evidence.original_size,evidence.truncated,evidence.trust_type,
		        convert_from(evidence.result_bytes,'UTF8'),octet_length(evidence.result_bytes),
		        octet_length(evidence.result_bytes),evidence.result_digest,false
		   FROM research_run_evidence evidence
		   JOIN research_run_steps terminal
		     ON terminal.tenant_id=evidence.tenant_id AND terminal.user_id=evidence.user_id
		    AND terminal.task_id=evidence.task_id AND terminal.plan_id=evidence.plan_id
		    AND terminal.temporal_run_id=evidence.temporal_run_id
		    AND terminal.plan_digest=evidence.plan_digest
		    AND terminal.step_ordinal=evidence.step_ordinal AND terminal.phase='completed'
		    AND terminal.invocation_id=evidence.invocation_id
		    AND terminal.tool_name=evidence.tool_name
		    AND terminal.request_digest=evidence.request_digest
		    AND terminal.result_digest=evidence.result_digest
		  WHERE evidence.tenant_id=$1 AND evidence.user_id=$2 AND evidence.task_id=$3
		    AND evidence.plan_id=$4 AND evidence.temporal_run_id=$5
		    AND evidence.plan_digest=$6
		  ORDER BY evidence.step_ordinal`,
		identity.TenantID, identity.UserID, identity.TaskID, planRef.PlanID,
		identity.TemporalRunID, planRef.PlanDigest)
	if err != nil {
		return nil, nil, nil, researchRunDatabaseError("load research Brief Evidence", err)
	}
	defer rows.Close()
	items := make([]researchEvidenceManifestItemV3, 0, planRef.StepCount)
	contextItems := make([]researchEvidenceContextItemV3, 0, planRef.StepCount)
	for rows.Next() {
		var item researchEvidenceManifestItemV3
		var contextItem researchEvidenceContextItemV3
		if err := rows.Scan(&item.EvidenceID, &item.Ordinal, &item.InvocationID,
			&item.ToolName, &item.RequestDigest, &item.ResultDigest,
			&item.OriginalSize, &item.Truncated, &item.TrustType,
			&contextItem.SynthesisVisibleText, &contextItem.ContextStoredSize,
			&contextItem.ContextVisibleSize, &contextItem.ContextVisibleDigest,
			&contextItem.ContextTruncated); err != nil {
			return nil, nil, nil, researchRunDatabaseError("scan research Brief Evidence", err)
		}
		items = append(items, item)
		contextItem.researchEvidenceManifestItemV3 = item
		if contextItem.ContextStoredSize < contextItem.ContextVisibleSize ||
			!validResearchRunDigest(contextItem.ContextVisibleDigest) ||
			researchRunSHA256([]byte(contextItem.SynthesisVisibleText)) != contextItem.ContextVisibleDigest ||
			contextItem.ContextTruncated != (contextItem.ContextStoredSize > contextItem.ContextVisibleSize) {
			return nil, nil, nil, researchRunIntegrityError()
		}
		contextItems = append(contextItems, contextItem)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, researchRunDatabaseError("iterate research Brief Evidence", err)
	}
	for _, item := range items {
		if item.EvidenceID <= 0 || item.Ordinal < 0 || item.Ordinal >= planRef.StepCount ||
			!validResearchRunDigest(item.RequestDigest) || !validResearchRunDigest(item.ResultDigest) ||
			(item.TrustType != "local" && item.TrustType != "external") {
			return nil, nil, nil, researchRunIntegrityError()
		}
	}
	failureRows, err := tx.Query(ctx,
		`SELECT step_ordinal,invocation_id,tool_name,request_digest,phase,
		        error_code,cost_micro_usd
		   FROM research_run_steps
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND temporal_run_id=$5 AND plan_digest=$6
		    AND phase IN ('failed','indeterminate')
		  ORDER BY step_ordinal`,
		identity.TenantID, identity.UserID, identity.TaskID, planRef.PlanID,
		identity.TemporalRunID, planRef.PlanDigest)
	if err != nil {
		return nil, nil, nil, researchRunDatabaseError(
			"load research Brief Tool failures", err)
	}
	defer failureRows.Close()
	failures := make([]researchToolFailureContextV31, 0, planRef.StepCount-len(items))
	for failureRows.Next() {
		var failure researchToolFailureContextV31
		if err := failureRows.Scan(&failure.Ordinal, &failure.InvocationID,
			&failure.ToolName, &failure.RequestDigest, &failure.Phase,
			&failure.ErrorCode, &failure.CostMicroUSD); err != nil {
			return nil, nil, nil, researchRunDatabaseError(
				"scan research Brief Tool failure", err)
		}
		if failure.Ordinal < 0 || failure.Ordinal >= planRef.StepCount ||
			!validResearchRunDigest(failure.RequestDigest) ||
			(failure.Phase != string(ResearchRunStepFailedV3) &&
				failure.Phase != string(ResearchRunStepIndeterminateV3)) ||
			failure.ErrorCode == "" || strings.TrimSpace(failure.ErrorCode) != failure.ErrorCode ||
			len(failure.ErrorCode) > 128 || failure.CostMicroUSD < 0 {
			return nil, nil, nil, researchRunIntegrityError()
		}
		failures = append(failures, failure)
	}
	if err := failureRows.Err(); err != nil {
		return nil, nil, nil, researchRunDatabaseError(
			"iterate research Brief Tool failures", err)
	}
	covered := make([]bool, planRef.StepCount)
	for _, item := range items {
		if covered[item.Ordinal] {
			return nil, nil, nil, researchRunIntegrityError()
		}
		covered[item.Ordinal] = true
	}
	for _, failure := range failures {
		if covered[failure.Ordinal] {
			return nil, nil, nil, researchRunIntegrityError()
		}
		covered[failure.Ordinal] = true
	}
	for _, terminal := range covered {
		if !terminal {
			return nil, nil, nil, researchRunValidationError(
				"research Brief requires every Tool step to be terminal")
		}
	}
	manifestSchema := researchEvidenceManifestSchemaV3
	if len(failures) > 0 {
		manifestSchema = researchEvidenceManifestSchemaV31
	}
	payload, err := json.Marshal(researchEvidenceManifestV3{
		SchemaVersion: manifestSchema, Items: items, ToolFailures: failures,
	})
	if err != nil || len(payload) > 64<<10 {
		return nil, nil, nil, researchRunIntegrityError()
	}
	return payload, contextItems, failures, nil
}

func buildResearchHistoryManifestV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	snapshotRef types.ResearchRunSnapshotRefV3, currentPlanID int64,
) ([]byte, researchHistoryContextV3, error) {
	cutoff, err := time.Parse(time.RFC3339Nano, snapshotRef.HistoryThroughUTC)
	if err != nil {
		return nil, researchHistoryContextV3{}, researchRunValidationError("research Brief history cutoff is invalid")
	}
	rows, err := tx.Query(ctx,
		`SELECT kind,record_id,run_snapshot_id,generated_at,digest,coverage,
		        payload_text,gap_reason,context_stored_size,context_visible_size,
		        context_visible_digest,context_truncated,candidate_count
		   FROM read_research_history_cap_v3($1,$2,$3,$4,$5)`,
		identity.TenantID, identity.UserID, identity.TaskID,
		snapshotRef.SnapshotID, currentPlanID)
	if err != nil {
		return nil, researchHistoryContextV3{}, researchRunDatabaseError("load research Brief history", err)
	}
	defer rows.Close()
	items := make([]researchHistoryManifestItemV3, 0, 20)
	contextItems := make([]researchHistoryContextItemV3, 0, 20)
	candidateCount := 0
	for rows.Next() {
		var item researchHistoryManifestItemV3
		var generated time.Time
		var payloadText *string
		var gapReason *string
		var rowCandidateCount int
		contextItem := researchHistoryContextItemV3{}
		if err := rows.Scan(&item.Kind, &item.RecordID, &item.RunSnapshotID,
			&generated, &item.Digest, &item.Coverage, &payloadText, &gapReason,
			&contextItem.ContextStoredSize, &contextItem.ContextVisibleSize,
			&contextItem.ContextVisibleDigest, &contextItem.ContextTruncated,
			&rowCandidateCount); err != nil {
			return nil, researchHistoryContextV3{}, researchRunDatabaseError("scan research Brief history", err)
		}
		if rowCandidateCount < 1 || (candidateCount != 0 && candidateCount != rowCandidateCount) {
			return nil, researchHistoryContextV3{}, researchRunIntegrityError()
		}
		candidateCount = rowCandidateCount
		generated = generated.Round(0).UTC().Truncate(time.Microsecond)
		if item.RecordID == "" || item.RunSnapshotID <= 0 || generated.After(cutoff) ||
			!validResearchRunDigest(item.Digest) ||
			(item.Coverage != "exact" && item.Coverage != "legacy" && item.Coverage != "unavailable") {
			return nil, researchHistoryContextV3{}, researchRunIntegrityError()
		}
		if (item.Coverage == "unavailable") != (payloadText == nil) ||
			(item.Coverage == "unavailable") != (gapReason != nil) {
			return nil, researchHistoryContextV3{}, researchRunIntegrityError()
		}
		item.GeneratedAt = generated.Format("2006-01-02T15:04:05.000000Z")
		items = append(items, item)
		contextItem.researchHistoryManifestItemV3 = item
		if payloadText != nil {
			contextItem.PayloadText = *payloadText
			if contextItem.ContextStoredSize < contextItem.ContextVisibleSize ||
				!validResearchRunDigest(contextItem.ContextVisibleDigest) ||
				researchRunSHA256([]byte(*payloadText)) != contextItem.ContextVisibleDigest ||
				contextItem.ContextTruncated != (contextItem.ContextStoredSize > contextItem.ContextVisibleSize) ||
				(!contextItem.ContextTruncated && contextItem.ContextVisibleDigest != item.Digest) {
				return nil, researchHistoryContextV3{}, researchRunIntegrityError()
			}
		} else if contextItem.ContextStoredSize != 0 || contextItem.ContextVisibleSize != 0 ||
			contextItem.ContextVisibleDigest != "" || contextItem.ContextTruncated {
			return nil, researchHistoryContextV3{}, researchRunIntegrityError()
		}
		if gapReason != nil {
			contextItem.GapReason = *gapReason
		}
		contextItems = append(contextItems, contextItem)
	}
	if err := rows.Err(); err != nil {
		return nil, researchHistoryContextV3{}, researchRunDatabaseError("iterate research Brief history", err)
	}
	if candidateCount < len(items) || len(items) > 20 {
		return nil, researchHistoryContextV3{}, researchRunIntegrityError()
	}
	truncated := candidateCount > len(items)
	var continuation *researchHistoryContinuationV3
	if truncated {
		last := items[len(items)-1]
		continuation = &researchHistoryContinuationV3{
			GeneratedAt: last.GeneratedAt, Kind: last.Kind, RecordID: last.RecordID,
		}
	}
	manifest := researchHistoryManifestV3{
		SchemaVersion:     researchHistoryManifestSchemaV3,
		HistoryThroughUTC: snapshotRef.HistoryThroughUTC,
		CandidateCount:    candidateCount, ReturnedCount: len(items),
		Truncated: truncated, Continuation: continuation, Items: items,
	}
	payload, err := json.Marshal(researchHistoryManifestV3{
		SchemaVersion: manifest.SchemaVersion, HistoryThroughUTC: manifest.HistoryThroughUTC,
		CandidateCount: manifest.CandidateCount, ReturnedCount: manifest.ReturnedCount,
		Truncated: manifest.Truncated, Continuation: manifest.Continuation, Items: manifest.Items,
	})
	if err != nil || len(payload) > 64<<10 {
		return nil, researchHistoryContextV3{}, researchRunValidationError("research Brief history manifest is invalid")
	}
	return payload, researchHistoryContextV3{
		HistoryThroughUTC: snapshotRef.HistoryThroughUTC,
		CandidateCount:    candidateCount, ReturnedCount: len(contextItems),
		Truncated: truncated, Continuation: continuation, Items: contextItems,
	}, nil
}

func researchBriefNotificationDecisionV3(
	threshold string, significance types.ResearchBriefSignificanceV3,
) (types.ResearchBriefDecisionV3, bool, error) {
	deliver := false
	switch threshold {
	case "major_updates_only":
		deliver = significance == types.ResearchBriefSignificanceMajorV3
	case "all_qualified_updates":
		deliver = significance == types.ResearchBriefSignificanceQualifiedV3 ||
			significance == types.ResearchBriefSignificanceMajorV3
	default:
		return "", false, researchRunIntegrityError()
	}
	if deliver {
		return types.ResearchBriefDecisionDeliverV3, true, nil
	}
	return types.ResearchBriefDecisionQuietV3, false, nil
}

func validateResearchBriefCitationsV3(
	brief types.ResearchBriefPayloadV3, evidencePayload, historyPayload []byte,
) error {
	var evidence researchEvidenceManifestV3
	var history researchHistoryManifestV3
	if json.Unmarshal(evidencePayload, &evidence) != nil ||
		json.Unmarshal(historyPayload, &history) != nil ||
		!validResearchEvidenceManifestVersionV3(
			evidence.SchemaVersion, len(evidence.ToolFailures)) ||
		history.SchemaVersion != researchHistoryManifestSchemaV3 {
		return researchRunIntegrityError()
	}
	evidenceRefs := make(map[string]struct{}, len(evidence.Items))
	for _, item := range evidence.Items {
		evidenceRefs[strconv.FormatInt(item.EvidenceID, 10)] = struct{}{}
	}
	historyRefs := make(map[string]struct{}, len(history.Items))
	for _, item := range history.Items {
		historyRefs[item.RecordID] = struct{}{}
	}
	for _, citation := range brief.Citations {
		switch citation.Kind {
		case types.ResearchBriefCitationCurrentEvidenceV3:
			if _, ok := evidenceRefs[citation.Ref]; !ok {
				return researchRunValidationError("research Brief cites Evidence outside the frozen manifest")
			}
		case types.ResearchBriefCitationHistoryV3:
			if _, ok := historyRefs[citation.Ref]; !ok {
				return researchRunValidationError("research Brief cites history outside the frozen manifest")
			}
		default:
			return researchRunValidationError("research Brief citation kind is invalid")
		}
	}
	return nil
}

func validateResearchBriefCoverageV31(
	brief types.ResearchBriefPayloadV3, evidencePayload []byte,
) error {
	var evidence researchEvidenceManifestV3
	if json.Unmarshal(evidencePayload, &evidence) != nil ||
		!validResearchEvidenceManifestVersionV3(
			evidence.SchemaVersion, len(evidence.ToolFailures)) {
		return researchRunIntegrityError()
	}
	if len(evidence.ToolFailures) == 0 {
		if brief.SchemaVersion != types.ResearchBriefPayloadSchemaV3 ||
			brief.Assessment != "" {
			return researchRunValidationError(
				"complete-coverage research Brief must use the retained payload")
		}
		return nil
	}
	if brief.SchemaVersion != types.ResearchBriefPayloadSchemaV31 ||
		brief.Assessment != types.ResearchBriefAssessmentUnknownV31 ||
		brief.Significance != types.ResearchBriefSignificanceNoneV3 {
		return researchRunValidationError(
			"partial-coverage research Brief must be unknown and quiet")
	}
	return nil
}

func digestResearchBriefRequestV3(value researchBriefRequestDigestV3) string {
	payload := []byte(strings.Join([]string{
		value.SchemaVersion,
		strconv.FormatInt(value.RunSnapshotID, 10),
		strconv.FormatInt(value.PlanID, 10),
		value.DefinitionDigest,
		value.PlanDigest,
		value.NotificationThreshold,
		value.ContextDigest,
		value.EvidenceDigest,
		value.HistoryDigest,
	}, "\n"))
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func lockResearchBriefSynthesisV3(ctx context.Context, tx pgx.Tx, temporalRunID string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		"research-brief-synthesis/v3:"+temporalRunID)
	if err != nil {
		return researchRunDatabaseError("lock research Brief synthesis", err)
	}
	return nil
}

const researchBriefSynthesisColumnsV3 = `
    id,tenant_id,user_id,task_id,run_snapshot_id,plan_id,
    temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
    notification_threshold,request_digest,context_payload,context_digest,
    evidence_manifest,evidence_digest,history_manifest,history_digest,
    synthesis_llm_spend_reservation_id,status,
    significance,decision,delivery_required,brief_payload,brief_digest,failure_code,
    spending_started_at,finalized_at,created_at,updated_at`

type researchBriefSynthesisScannerV3 interface{ Scan(...any) error }

func scanResearchBriefSynthesisV3(scanner researchBriefSynthesisScannerV3) (
	ResearchBriefSynthesisV3, error,
) {
	var row ResearchBriefSynthesisV3
	var significance, decision, briefDigest *string
	var status string
	err := scanner.Scan(&row.ID, &row.TenantID, &row.UserID, &row.TaskID,
		&row.RunSnapshotID, &row.PlanID, &row.TemporalWorkflowID, &row.TemporalRunID,
		&row.DefinitionDigest, &row.PlanDigest, &row.NotificationThreshold,
		&row.RequestDigest, &row.ContextPayload, &row.ContextDigest,
		&row.EvidenceManifest, &row.EvidenceDigest, &row.HistoryManifest,
		&row.HistoryDigest, &row.SynthesisLLMReservationID, &status,
		&significance, &decision, &row.DeliveryRequired,
		&row.BriefPayload, &briefDigest, &row.FailureCode, &row.SpendingStartedAt,
		&row.FinalizedAt, &row.CreatedAt, &row.UpdatedAt)
	if err != nil {
		return ResearchBriefSynthesisV3{}, err
	}
	row.Status = ResearchBriefSynthesisStatusV3(status)
	if significance != nil {
		row.Significance = types.ResearchBriefSignificanceV3(*significance)
	}
	if decision != nil {
		row.Decision = types.ResearchBriefDecisionV3(*decision)
	}
	if briefDigest != nil {
		row.BriefDigest = *briefDigest
	}
	row.ContextPayload = append([]byte(nil), row.ContextPayload...)
	row.EvidenceManifest = append([]byte(nil), row.EvidenceManifest...)
	row.HistoryManifest = append([]byte(nil), row.HistoryManifest...)
	row.BriefPayload = append([]byte(nil), row.BriefPayload...)
	return row, nil
}

func loadResearchBriefLLMReceiptStateV3(
	ctx context.Context, tx pgx.Tx, params ClaimResearchBriefSynthesisV3Params,
	row ResearchBriefSynthesisV3,
) (ResearchBriefLLMReceiptStateV3, int64, error) {
	if row.SynthesisLLMReservationID == nil {
		// NULL is reserved for rows created before migration 092. Those rows are
		// readable, but can never authorize a new provider call or finalization.
		return "", 0, nil
	}
	if *row.SynthesisLLMReservationID != params.SynthesisLLMReservationID {
		return "", 0, researchRunConflictError()
	}
	reservation, err := loadResearchRunLLMReservationByIDV3(ctx, tx,
		params.Identity, params.SnapshotRef, params.SynthesisLLMReservationID, false)
	if err != nil {
		return "", 0, err
	}
	if reservation.Stage != ResearchRunLLMStageSynthesisV3 ||
		reservation.RoundOrdinal != 0 || reservation.SubjectID != row.ID {
		return "", 0, researchRunIntegrityError()
	}
	receipt, settled, err := loadResearchRunLLMSettlementV3(ctx, tx, reservation)
	if err != nil {
		return "", 0, err
	}
	if !settled {
		return ResearchBriefLLMReceiptPendingV3, 0, nil
	}
	switch receipt.Outcome {
	case ResearchRunLLMCompletedV3:
		if !receipt.Attempted || !receipt.UsageKnown || receipt.DefinitelyZeroUsage ||
			receipt.LLMCallID <= 0 || receipt.ErrorCode != "" {
			return "", 0, researchRunIntegrityError()
		}
		return ResearchBriefLLMReceiptCompletedV3, receipt.LLMCallID, nil
	case ResearchRunLLMFailedV3:
		return ResearchBriefLLMReceiptFailedV3, receipt.LLMCallID, nil
	case ResearchRunLLMIndeterminateV3:
		return ResearchBriefLLMReceiptIndeterminateV3, receipt.LLMCallID, nil
	default:
		return "", 0, researchRunIntegrityError()
	}
}

func loadResearchBriefSynthesisByPlanV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	snapshotID, planID int64,
) (ResearchBriefSynthesisV3, bool, error) {
	row, err := scanResearchBriefSynthesisV3(tx.QueryRow(ctx,
		`SELECT `+researchBriefSynthesisColumnsV3+`
		   FROM research_brief_syntheses
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		    AND run_snapshot_id=$4 AND plan_id=$5
		    AND temporal_workflow_id=$6 AND temporal_run_id=$7`,
		identity.TenantID, identity.UserID, identity.TaskID, snapshotID, planID,
		identity.TemporalWorkflowID, identity.TemporalRunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchBriefSynthesisV3{}, false, nil
	}
	if err != nil {
		return ResearchBriefSynthesisV3{}, false, researchRunDatabaseError("load research Brief synthesis", err)
	}
	return row, true, nil
}

func loadAndValidateResearchBriefSynthesisHandleV3(
	ctx context.Context, tx pgx.Tx, params ClaimResearchBriefSynthesisV3Params,
) (ResearchBriefSynthesisV3, error) {
	row, found, err := loadResearchBriefSynthesisByPlanV3(ctx, tx, params.Identity,
		params.SnapshotRef.SnapshotID, params.PlanRef.PlanID)
	if err != nil {
		return ResearchBriefSynthesisV3{}, err
	}
	if !found || row.ID != params.SynthesisID || row.RequestDigest != params.RequestDigest ||
		row.DefinitionDigest != params.SnapshotRef.DefinitionDigest ||
		row.PlanDigest != params.PlanRef.PlanDigest {
		return ResearchBriefSynthesisV3{}, researchRunValidationError("research Brief synthesis handle is unavailable")
	}
	return row, nil
}

func researchBriefSynthesisScopeEqualV3(
	row ResearchBriefSynthesisV3, params PrepareResearchBriefSynthesisV3Params,
	threshold string,
) bool {
	return row.TenantID == params.Identity.TenantID &&
		row.UserID == params.Identity.UserID && row.TaskID == params.Identity.TaskID &&
		row.TemporalWorkflowID == params.Identity.TemporalWorkflowID &&
		row.TemporalRunID == params.Identity.TemporalRunID &&
		row.RunSnapshotID == params.SnapshotRef.SnapshotID &&
		row.PlanID == params.PlanRef.PlanID &&
		row.DefinitionDigest == params.SnapshotRef.DefinitionDigest &&
		row.PlanDigest == params.PlanRef.PlanDigest &&
		row.NotificationThreshold == threshold
}

func researchBriefSynthesisFrozenPayloadsValidV3(row ResearchBriefSynthesisV3) bool {
	if !validResearchRunDigest(row.RequestDigest) ||
		!validResearchRunDigest(row.ContextDigest) ||
		!validResearchRunDigest(row.EvidenceDigest) ||
		!validResearchRunDigest(row.HistoryDigest) ||
		researchRunSHA256(row.ContextPayload) != row.ContextDigest ||
		researchRunSHA256(row.EvidenceManifest) != row.EvidenceDigest ||
		researchRunSHA256(row.HistoryManifest) != row.HistoryDigest {
		return false
	}
	expectedRequest := digestResearchBriefRequestV3(researchBriefRequestDigestV3{
		SchemaVersion:         researchBriefSynthesisSchemaV3,
		RunSnapshotID:         row.RunSnapshotID,
		PlanID:                row.PlanID,
		DefinitionDigest:      row.DefinitionDigest,
		PlanDigest:            row.PlanDigest,
		NotificationThreshold: row.NotificationThreshold,
		ContextDigest:         row.ContextDigest,
		EvidenceDigest:        row.EvidenceDigest,
		HistoryDigest:         row.HistoryDigest,
	})
	if row.RequestDigest != expectedRequest {
		return false
	}
	var contextPayload researchSynthesisContextV3
	var evidence researchEvidenceManifestV3
	var history researchHistoryManifestV3
	if json.Unmarshal(row.ContextPayload, &contextPayload) != nil ||
		!validResearchSynthesisContextVersionV3(
			contextPayload.SchemaVersion, len(contextPayload.ToolFailures)) ||
		json.Unmarshal(row.EvidenceManifest, &evidence) != nil ||
		!validResearchEvidenceManifestVersionV3(
			evidence.SchemaVersion, len(evidence.ToolFailures)) ||
		json.Unmarshal(row.HistoryManifest, &history) != nil ||
		history.SchemaVersion != researchHistoryManifestSchemaV3 ||
		!equalResearchToolFailuresV31(
			contextPayload.ToolFailures, evidence.ToolFailures) {
		return false
	}
	canonicalContext, contextErr := json.Marshal(contextPayload)
	canonicalEvidence, evidenceErr := json.Marshal(evidence)
	canonicalHistory, historyErr := json.Marshal(history)
	return contextErr == nil && evidenceErr == nil && historyErr == nil &&
		bytes.Equal(canonicalContext, row.ContextPayload) &&
		bytes.Equal(canonicalEvidence, row.EvidenceManifest) &&
		bytes.Equal(canonicalHistory, row.HistoryManifest)
}

func validResearchSynthesisContextVersionV3(version string, failures int) bool {
	return (version == researchSynthesisContextSchemaV3 && failures == 0) ||
		(version == researchSynthesisContextSchemaV31 && failures > 0)
}

func researchBriefSynthesisPartialCoverageV31(
	row ResearchBriefSynthesisV3,
) bool {
	var contextPayload struct {
		SchemaVersion string `json:"schema_version"`
	}
	return json.Unmarshal(row.ContextPayload, &contextPayload) == nil &&
		contextPayload.SchemaVersion == researchSynthesisContextSchemaV31
}

func validResearchEvidenceManifestVersionV3(version string, failures int) bool {
	return (version == researchEvidenceManifestSchemaV3 && failures == 0) ||
		(version == researchEvidenceManifestSchemaV31 && failures > 0)
}

func equalResearchToolFailuresV31(
	left, right []researchToolFailureContextV31,
) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func researchBriefRefFromSynthesisV3(row ResearchBriefSynthesisV3) (
	types.ResearchBriefRefV3, error,
) {
	if row.Status != ResearchBriefSynthesisFinalizedV3 || row.DeliveryRequired == nil ||
		row.FinalizedAt == nil || !row.Significance.Valid() || !row.Decision.Valid() ||
		!validResearchRunDigest(row.BriefDigest) || !validResearchRunDigest(row.RequestDigest) ||
		!validResearchRunDigest(row.EvidenceDigest) || !validResearchRunDigest(row.HistoryDigest) {
		return types.ResearchBriefRefV3{}, researchRunIntegrityError()
	}
	ref, err := types.SealResearchBriefRefV3(types.ResearchBriefRefV3{
		BriefID: row.ID, RunSnapshotID: row.RunSnapshotID, PlanID: row.PlanID,
		TemporalWorkflowID: row.TemporalWorkflowID, TemporalRunID: row.TemporalRunID,
		TenantID: row.TenantID, UserID: row.UserID, TaskID: row.TaskID,
		DefinitionDigest: row.DefinitionDigest, PlanDigest: row.PlanDigest,
		RequestDigest: row.RequestDigest, BriefDigest: row.BriefDigest,
		EvidenceDigest: row.EvidenceDigest, HistoryDigest: row.HistoryDigest,
		NotificationThreshold: row.NotificationThreshold,
		Significance:          row.Significance, Decision: row.Decision,
		DeliveryRequired: *row.DeliveryRequired,
	})
	if err != nil {
		return types.ResearchBriefRefV3{}, researchRunIntegrityError()
	}
	return ref, nil
}
