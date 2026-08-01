package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

const (
	ResearchRunLLMStagePlannerV3   = "planner"
	ResearchRunLLMStageSynthesisV3 = "synthesis"

	researchRunLLMReservationSchemaV3  = "vane.research-run-llm-spend-reservation/v1"
	researchRunLLMSettlementSchemaV3   = "vane.research-run-llm-spend-settlement/v1"
	researchRunLLMRequestSchemaV3      = "vane.research-run-llm-request/v1"
	researchRunLLMMinQuotaTokensV3     = 64
	researchRunLLMMaxPromptBytesV3     = 2 << 20
	researchRunLLMMaxCompletionBytesV3 = 8 << 10
	researchRunLLMMaxErrorBytesV3      = 4 << 10
	researchRunLLMMaxModelBytesV3      = 255
	researchRunLLMRefTypeV3            = "research_run_snapshot"
)

type BeginResearchRunLLMSpendV3Params struct {
	Identity     types.RunIdentity
	SnapshotRef  types.ResearchRunSnapshotRefV3
	Stage        string
	RoundOrdinal int
	SubjectID    int64
	SystemPrompt string
	UserPrompt   string
}

type ResearchRunLLMSpendReservationV3 struct {
	ReservationID              int64
	FirstWriter                bool
	Stage                      string
	RoundOrdinal               int
	SubjectID                  int64
	RequestDigest              string
	TraceID                    string
	Provider                   string
	Model                      string
	Temperature                float32
	MaxTokens                  int
	DisableThinking            bool
	ReservedQuotaTokens        int
	ReservedCompletionTokens   int
	ReservedPlannerTokens      int
	ReservedCostMicroUSD       int64
	PricingRuleID              int64
	Currency                   string
	CountsAgainstPlannerBudget bool
}

type ResearchRunLLMOutcomeV3 string

const (
	ResearchRunLLMCompletedV3     ResearchRunLLMOutcomeV3 = "completed"
	ResearchRunLLMFailedV3        ResearchRunLLMOutcomeV3 = "failed"
	ResearchRunLLMIndeterminateV3 ResearchRunLLMOutcomeV3 = "indeterminate"
)

// CommitResearchRunLLMReceiptV3Params carries the exact model-visible request
// and provider response. The Store, not an observability Recorder, owns the
// only transaction that may bind these bytes to a V3 spend reservation.
type CommitResearchRunLLMReceiptV3Params struct {
	Identity        types.RunIdentity
	SnapshotRef     types.ResearchRunSnapshotRefV3
	ReservationID   int64
	Call            types.LLMCall
	DisableThinking bool
	Attempted       bool
	UsageKnown      bool
	// DefinitelyZeroUsage is true only for a local pre-send rejection or an
	// explicit provider 4xx/429 rejection. It prevents those calls from being
	// charged as an indeterminate response while keeping attempted=true in the
	// immutable evidence.
	DefinitelyZeroUsage bool
	Outcome             ResearchRunLLMOutcomeV3
	ErrorCode           string
	// GatewayReceipt is the trusted provider-bound attestation. The general
	// V3 executor cannot settle bare Call fields after migration 094.
	GatewayReceipt types.ResearchLLMGatewayReceiptV3
}

type ResearchRunLLMReceiptV3 struct {
	Reservation            ResearchRunLLMSpendReservationV3
	Settled                bool
	LLMCallID              int64
	Call                   types.LLMCall
	Attempted              bool
	UsageKnown             bool
	DefinitelyZeroUsage    bool
	Outcome                ResearchRunLLMOutcomeV3
	ErrorCode              string
	ActualPromptTokens     int
	ActualCompletionTokens int
	ActualCostMicroUSD     int64
	PricingStatus          string
}

type researchRunLLMReservationRowV3 struct {
	ResearchRunLLMSpendReservationV3
	TenantID           int64
	UserID             int64
	TaskID             string
	RunSnapshotID      int64
	AttemptKey         string
	ModelPolicyDigest  string
	SystemPromptDigest string
	UserPromptDigest   string
}

type researchRunLLMRequestV3 struct {
	SchemaVersion   string  `json:"schema_version"`
	RunSnapshotID   int64   `json:"run_snapshot_id"`
	Stage           string  `json:"stage"`
	RoundOrdinal    int     `json:"round_ordinal"`
	SubjectID       int64   `json:"subject_id"`
	Provider        string  `json:"provider"`
	Model           string  `json:"model"`
	Temperature     float32 `json:"temperature"`
	MaxTokens       int     `json:"max_tokens"`
	DisableThinking bool    `json:"disable_thinking"`
	SystemPrompt    string  `json:"system_prompt"`
	UserPrompt      string  `json:"user_prompt"`
}

func researchRunLLMRequestDigestV3(request researchRunLLMRequestV3) (string, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func researchRunLLMTraceV3(identity types.RunIdentity, snapshotID int64,
	stage string, round int, subjectID int64, requestDigest string) (string, error) {
	payload, err := json.Marshal(struct {
		SchemaVersion string            `json:"schema_version"`
		Identity      types.RunIdentity `json:"identity"`
		SnapshotID    int64             `json:"snapshot_id"`
		Stage         string            `json:"stage"`
		Round         int               `json:"round"`
		SubjectID     int64             `json:"subject_id"`
		RequestDigest string            `json:"request_digest"`
	}{"vane.research-run-llm-trace/v1", identity, snapshotID, stage, round,
		subjectID, requestDigest})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return "research-llm-v3-" + hex.EncodeToString(sum[:]), nil
}

func researchRunLLMStagePolicyV3(seal runcontext.ResearchSnapshotSealV3,
	stage string) (runtimepolicy.ResearchModelStageV3, bool, error) {
	switch stage {
	case ResearchRunLLMStagePlannerV3:
		return seal.ResearchModel.Planner, true, nil
	case ResearchRunLLMStageSynthesisV3:
		return seal.ResearchModel.Synthesis, false, nil
	default:
		return runtimepolicy.ResearchModelStageV3{}, false,
			researchRunValidationError("research model stage is invalid")
	}
}

func validateResearchRunLLMBeginV3(params BeginResearchRunLLMSpendV3Params) error {
	if err := params.SnapshotRef.ValidateFor(params.Identity); err != nil ||
		params.RoundOrdinal < 0 || params.RoundOrdinal >= 8 ||
		len(params.SystemPrompt) == 0 || len(params.SystemPrompt) > researchRunLLMMaxPromptBytesV3 ||
		len(params.UserPrompt) == 0 || len(params.UserPrompt) > researchRunLLMMaxPromptBytesV3 ||
		!utf8.ValidString(params.SystemPrompt) || !utf8.ValidString(params.UserPrompt) ||
		strings.ContainsRune(params.SystemPrompt, 0) || strings.ContainsRune(params.UserPrompt, 0) {
		return researchRunValidationError("research model request is invalid")
	}
	if params.Stage == ResearchRunLLMStagePlannerV3 {
		if params.SubjectID != 0 {
			return researchRunValidationError("research planner subject is invalid")
		}
	} else if params.Stage == ResearchRunLLMStageSynthesisV3 {
		if params.SubjectID <= 0 || params.RoundOrdinal != 0 {
			return researchRunValidationError("research synthesis subject is invalid")
		}
	} else {
		return researchRunValidationError("research model stage is invalid")
	}
	return nil
}

func (s *Store) BeginResearchRunLLMSpendV3(
	ctx context.Context, params BeginResearchRunLLMSpendV3Params,
) (ResearchRunLLMSpendReservationV3, error) {
	if err := validateResearchRunLLMBeginV3(params); err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	}
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(ctx,
		pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunDatabaseError("begin research model spend", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchRunLLMSpendReservationV3{}, researchRunIntegrityError()
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunDatabaseError("lock research model schema admission", err)
	}
	if exists, err := lockTenantAdmissionRootShared(ctx, tx,
		params.Identity.TenantID); err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunDatabaseError("lock research model tenant admission", err)
	} else if !exists {
		return ResearchRunLLMSpendReservationV3{},
			researchRunValidationError("research model tenant is unavailable")
	}
	if err := lockResearchRunSpendBudgetV3(ctx, tx,
		params.Identity.TemporalRunID); err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, tx, params.Identity,
		params.SnapshotRef)
	if err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	}
	stagePolicy, plannerStage, err := researchRunLLMStagePolicyV3(seal, params.Stage)
	if err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	}
	if stagePolicy.SystemPrompt != params.SystemPrompt ||
		(plannerStage && params.RoundOrdinal >= seal.Payload.PlannerBudget.MaxPlannerRounds) {
		return ResearchRunLLMSpendReservationV3{},
			researchRunValidationError("research model request differs from frozen policy")
	}
	temperature := float32(stagePolicy.Temperature)
	requestDigest, err := researchRunLLMRequestDigestV3(researchRunLLMRequestV3{
		SchemaVersion: researchRunLLMRequestSchemaV3,
		RunSnapshotID: params.SnapshotRef.SnapshotID,
		Stage:         params.Stage, RoundOrdinal: params.RoundOrdinal,
		SubjectID: params.SubjectID, Provider: string(seal.ResearchModel.Provider),
		Model: stagePolicy.Model, Temperature: temperature,
		MaxTokens: stagePolicy.MaxTokens, DisableThinking: stagePolicy.DisableThinking,
		SystemPrompt: params.SystemPrompt, UserPrompt: params.UserPrompt,
	})
	if err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunValidationError("research model request cannot be sealed")
	}
	traceID, err := researchRunLLMTraceV3(params.Identity,
		params.SnapshotRef.SnapshotID, params.Stage, params.RoundOrdinal,
		params.SubjectID, requestDigest)
	if err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunValidationError("research model trace cannot be sealed")
	}
	attemptSum := sha256.Sum256([]byte(traceID))
	attemptKey := hex.EncodeToString(attemptSum[:])

	if replay, found, err := loadResearchRunLLMReservationV3(ctx, tx, params,
		requestDigest, traceID, stagePolicy, plannerStage); err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	} else if found {
		if err := freezeResearchLLMGatewayRequestV2(ctx, tx, replay.ReservationID,
			requestDigest, params.SystemPrompt, params.UserPrompt); err != nil {
			return ResearchRunLLMSpendReservationV3{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunLLMSpendReservationV3{},
				researchRunDatabaseError("commit research model spend replay", err)
		}
		return replay, nil
	}

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`,
		providerPricingLedgerLock); err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunDatabaseError("lock research model pricing", err)
	}
	var reservationID int64
	var firstWriter bool
	err = tx.QueryRow(ctx,
		`SELECT out_reservation_id,out_first_writer
		   FROM admit_research_run_llm_spend_cap_v3(
		        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, params.Stage, params.RoundOrdinal,
		researchRunLLMSubjectV3(params.Stage, params.SubjectID), attemptKey,
		requestDigest, traceID, params.UserPrompt,
	).Scan(&reservationID, &firstWriter)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "P0001" {
			return ResearchRunLLMSpendReservationV3{}, types.NewAppError(
				types.CodeQuotaExceeded, "research model quota is exhausted", ErrQuotaExceeded)
		}
		return ResearchRunLLMSpendReservationV3{},
			researchRunDatabaseError("admit research model spend reservation", err)
	}
	reservation, found, err := loadResearchRunLLMReservationV3(ctx, tx, params,
		requestDigest, traceID, stagePolicy, plannerStage)
	if err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	}
	if !found || reservation.ReservationID != reservationID {
		return ResearchRunLLMSpendReservationV3{}, researchRunIntegrityError()
	}
	if err := freezeResearchLLMGatewayRequestV2(ctx, tx, reservation.ReservationID,
		requestDigest, params.SystemPrompt, params.UserPrompt); err != nil {
		return ResearchRunLLMSpendReservationV3{}, err
	}
	reservation.FirstWriter = firstWriter
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunLLMSpendReservationV3{},
			researchRunDatabaseError("commit research model spend reservation", err)
	}
	return reservation, nil
}

func freezeResearchLLMGatewayRequestV2(ctx context.Context, tx pgx.Tx,
	reservationID int64, requestDigest, systemPrompt, userPrompt string) error {
	if _, err := tx.Exec(ctx, `SELECT freeze_research_llm_gateway_request_v2($1,$2,$3,$4)`,
		reservationID, requestDigest, systemPrompt, userPrompt); err != nil {
		return researchRunDatabaseError("freeze research gateway request", err)
	}
	return nil
}

func researchRunLLMSubjectV3(stage string, subjectID int64) any {
	if stage == ResearchRunLLMStagePlannerV3 {
		return int64(0)
	}
	return subjectID
}

func loadResearchRunLLMReservationV3(
	ctx context.Context, tx pgx.Tx, params BeginResearchRunLLMSpendV3Params,
	requestDigest, traceID string, stage runtimepolicy.ResearchModelStageV3,
	plannerStage bool,
) (ResearchRunLLMSpendReservationV3, bool, error) {
	var out ResearchRunLLMSpendReservationV3
	var storedSubject *int64
	var systemPromptDigest, userPromptDigest string
	err := tx.QueryRow(ctx,
		`SELECT reservation.id,reservation.subject_id,reservation.request_digest,
		        reservation.trace_id,pricing.provider,reservation.model,
		        reservation.system_prompt_digest,reservation.user_prompt_digest,
		        reservation.temperature,reservation.disable_thinking,
		        reserved_quota_tokens,reserved_completion_tokens,reserved_planner_tokens,
		        reserved_cost_micro_usd,
		        pricing_rule_id,cost_currency,counts_against_planner_budget
		   FROM research_run_llm_spend_reservations reservation
		   JOIN provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
		  WHERE reservation.tenant_id=$1 AND reservation.user_id=$2
		    AND reservation.task_id=$3 AND reservation.run_snapshot_id=$4
		    AND reservation.stage=$5 AND reservation.round_ordinal=$6`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, params.Stage, params.RoundOrdinal,
	).Scan(&out.ReservationID, &storedSubject, &out.RequestDigest, &out.TraceID,
		&out.Provider, &out.Model, &systemPromptDigest, &userPromptDigest,
		&out.Temperature, &out.DisableThinking,
		&out.ReservedQuotaTokens, &out.ReservedCompletionTokens,
		&out.ReservedPlannerTokens,
		&out.ReservedCostMicroUSD, &out.PricingRuleID, &out.Currency,
		&out.CountsAgainstPlannerBudget)
	if err == pgx.ErrNoRows {
		return ResearchRunLLMSpendReservationV3{}, false, nil
	}
	if err != nil {
		return ResearchRunLLMSpendReservationV3{}, false,
			researchRunDatabaseError("load research model spend reservation", err)
	}
	storedSubjectID := int64(0)
	if storedSubject != nil {
		storedSubjectID = *storedSubject
	}
	if out.ReservationID <= 0 || storedSubjectID != params.SubjectID ||
		out.RequestDigest != requestDigest || out.TraceID != traceID ||
		out.Provider != string(runtimepolicy.ModelProviderDeepSeekV1) ||
		out.Model != stage.Model || out.Currency != "USD" ||
		systemPromptDigest != researchRunSHA256([]byte(params.SystemPrompt)) ||
		userPromptDigest != researchRunSHA256([]byte(params.UserPrompt)) ||
		out.Temperature != float32(stage.Temperature) ||
		out.DisableThinking != stage.DisableThinking ||
		out.CountsAgainstPlannerBudget != plannerStage ||
		out.ReservedQuotaTokens <= 0 || out.ReservedCostMicroUSD <= 0 ||
		out.ReservedCompletionTokens != stage.MaxTokens ||
		(plannerStage && out.ReservedPlannerTokens != out.ReservedQuotaTokens) ||
		(!plannerStage && out.ReservedPlannerTokens != 0) {
		return ResearchRunLLMSpendReservationV3{}, false, researchRunConflictError()
	}
	out.FirstWriter = false
	out.Stage, out.RoundOrdinal, out.SubjectID = params.Stage, params.RoundOrdinal, params.SubjectID
	out.MaxTokens = stage.MaxTokens
	return out, true, nil
}

func (s *Store) CommitResearchRunLLMReceiptV3(
	ctx context.Context, params CommitResearchRunLLMReceiptV3Params,
) (ResearchRunLLMReceiptV3, error) {
	if err := validateResearchRunLLMCommitV3(params); err != nil {
		return ResearchRunLLMReceiptV3{}, err
	}
	// Owner/control-plane preflight performs rich integrity checks without
	// widening gateway SELECT privileges. It does not write a provider effect.
	controlTx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ResearchRunLLMReceiptV3{},
			researchRunDatabaseError("begin research model settlement preflight", err)
	}
	defer func() { _ = controlTx.Rollback(context.WithoutCancel(ctx)) }()
	reservation, err := loadResearchRunLLMReservationByIDV3(ctx, controlTx, params.Identity,
		params.SnapshotRef, params.ReservationID, false)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, err
	}
	if existing, found, err := loadResearchRunLLMSettlementV3(ctx, controlTx, reservation); err != nil {
		return ResearchRunLLMReceiptV3{}, err
	} else if found {
		if err := validateResearchRunLLMSettlementReplayV3(existing, params); err != nil {
			return ResearchRunLLMReceiptV3{}, err
		}
		if err := controlTx.Commit(ctx); err != nil {
			return ResearchRunLLMReceiptV3{},
				researchRunDatabaseError("commit research model settlement replay", err)
		}
		return existing, nil
	}
	seal, err := loadAndValidateResearchBriefSnapshotV3(ctx, controlTx, params.Identity,
		params.SnapshotRef)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, err
	}
	stagePolicy, _, err := researchRunLLMStagePolicyV3(seal, reservation.Stage)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, err
	}
	if err := validateResearchRunLLMCallV3(params, reservation, seal, stagePolicy); err != nil {
		return ResearchRunLLMReceiptV3{}, err
	}
	if params.GatewayReceipt.ReservationID != reservation.ReservationID ||
		params.GatewayReceipt.RequestDigest != reservation.RequestDigest {
		return ResearchRunLLMReceiptV3{},
			researchRunValidationError("research model gateway receipt differs from reservation")
	}
	if err := controlTx.Commit(ctx); err != nil {
		return ResearchRunLLMReceiptV3{},
			researchRunDatabaseError("commit research model settlement preflight", err)
	}

	call := params.Call
	tx, scopedRef, err := s.beginScopedResearchGatewayTransactionV3(ctx,
		pgx.TxOptions{}, params.Identity, params.SnapshotRef.SnapshotID)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, researchRunDatabaseError("begin signed model settlement", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != params.SnapshotRef {
		return ResearchRunLLMReceiptV3{}, researchRunIntegrityError()
	}
	var callID *int64
	var settlementID int64
	err = tx.QueryRow(ctx,
		`SELECT out_llm_call_id,out_settlement_id
		   FROM settle_signed_research_run_llm_spend_v3(
		        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
		        $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, reservation.ReservationID, reservation.RequestDigest,
		call.SystemPrompt, call.UserPrompt, call.Completion, call.Provider, call.Model,
		call.PromptTokens, call.CompletionTokens, call.PromptCacheHitTokens,
		call.PromptCacheMissTokens, call.ReasoningTokens, call.LatencyMs,
		call.PrefixCacheHit, *call.Temperature, *call.MaxTokens,
		params.DisableThinking, call.Error, params.Attempted, params.UsageKnown,
		params.DefinitelyZeroUsage, string(params.Outcome), params.ErrorCode,
		params.GatewayReceipt.KeyID, params.GatewayReceipt.SignedAtUnixMillis,
		params.GatewayReceipt.Signature,
	).Scan(&callID, &settlementID)
	if err != nil {
		return ResearchRunLLMReceiptV3{},
			researchRunDatabaseError("settle exact research model spend", err)
	}
	if settlementID <= 0 || (params.Attempted && callID == nil) ||
		(!params.Attempted && callID != nil) {
		return ResearchRunLLMReceiptV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunLLMReceiptV3{},
			researchRunDatabaseError("commit research model settlement", err)
	}
	settled, found, err := s.LoadResearchRunLLMReceiptV3(ctx, params.Identity,
		params.SnapshotRef, reservation.Stage, reservation.RoundOrdinal)
	if err != nil || !found || !settled.Settled {
		if err != nil {
			return ResearchRunLLMReceiptV3{}, err
		}
		return ResearchRunLLMReceiptV3{}, researchRunIntegrityError()
	}
	if err := validateResearchRunLLMSettlementReplayV3(settled, params); err != nil {
		return ResearchRunLLMReceiptV3{}, err
	}
	return settled, nil
}

func validateResearchRunLLMCommitV3(params CommitResearchRunLLMReceiptV3Params) error {
	if err := params.SnapshotRef.ValidateFor(params.Identity); err != nil ||
		params.ReservationID <= 0 ||
		(params.Outcome != ResearchRunLLMCompletedV3 &&
			params.Outcome != ResearchRunLLMFailedV3 &&
			params.Outcome != ResearchRunLLMIndeterminateV3) ||
		params.UsageKnown && !params.Attempted ||
		params.DefinitelyZeroUsage && params.UsageKnown ||
		!params.Attempted && !params.DefinitelyZeroUsage ||
		params.GatewayReceipt.SchemaVersion != types.ResearchLLMGatewayReceiptSchemaV3 ||
		params.GatewayReceipt.ReservationID != params.ReservationID ||
		params.GatewayReceipt.SignedAtUnixMillis <= 0 ||
		len(params.GatewayReceipt.Signature) != sha256.Size ||
		params.GatewayReceipt.Call != params.Call {
		return researchRunValidationError("research model settlement is invalid")
	}
	if params.GatewayReceipt.DisableThinking != params.DisableThinking ||
		params.GatewayReceipt.Attempted != params.Attempted ||
		params.GatewayReceipt.UsageKnown != params.UsageKnown ||
		params.GatewayReceipt.DefinitelyZeroUsage != params.DefinitelyZeroUsage ||
		params.GatewayReceipt.Outcome != string(params.Outcome) ||
		params.GatewayReceipt.ErrorCode != params.ErrorCode {
		return researchRunValidationError("research model settlement differs from gateway receipt")
	}
	if len(params.Call.Error) > researchRunLLMMaxErrorBytesV3 ||
		!utf8.ValidString(params.Call.Error) || strings.ContainsRune(params.Call.Error, 0) {
		return researchRunValidationError("research model settlement error is invalid")
	}
	if params.Outcome == ResearchRunLLMCompletedV3 {
		if !params.Attempted || !params.UsageKnown || params.DefinitelyZeroUsage ||
			params.ErrorCode != "" || params.Call.Error != "" {
			return researchRunValidationError("completed research model settlement is invalid")
		}
	} else if !validResearchRunErrorCode(params.ErrorCode) || params.Call.Error == "" {
		return researchRunValidationError("failed research model settlement is invalid")
	}
	if params.Outcome == ResearchRunLLMIndeterminateV3 &&
		(!params.Attempted || params.UsageKnown || params.DefinitelyZeroUsage) {
		return researchRunValidationError("indeterminate research model settlement is invalid")
	}
	if params.Outcome == ResearchRunLLMFailedV3 && params.Attempted &&
		!params.UsageKnown && !params.DefinitelyZeroUsage {
		return researchRunValidationError("unknown research model usage must be indeterminate")
	}
	return nil
}

func validateResearchRunLLMCallV3(
	params CommitResearchRunLLMReceiptV3Params,
	reservation researchRunLLMReservationRowV3,
	seal runcontext.ResearchSnapshotSealV3,
	stage runtimepolicy.ResearchModelStageV3,
) error {
	call := params.Call
	if call.TraceID != reservation.TraceID ||
		call.SpanName != researchRunLLMSpanV3(reservation.Stage) ||
		call.Provider != string(seal.ResearchModel.Provider) ||
		strings.TrimSpace(call.Model) == "" || len(call.Model) > researchRunLLMMaxModelBytesV3 ||
		call.SystemPrompt != stage.SystemPrompt || call.UserPrompt == "" ||
		len(call.Completion) > researchRunLLMMaxCompletionBytesV3 ||
		!utf8.ValidString(call.Completion) || strings.ContainsRune(call.Completion, 0) ||
		call.RunSnapshotID == nil || *call.RunSnapshotID != params.SnapshotRef.SnapshotID ||
		call.TenantID == nil || *call.TenantID != params.Identity.TenantID ||
		call.UserID == nil || *call.UserID != params.Identity.UserID ||
		call.RefType != types.RefType(researchRunLLMRefTypeV3) || call.RefID == nil ||
		*call.RefID != params.SnapshotRef.SnapshotID || call.MaxTokens == nil ||
		*call.MaxTokens != stage.MaxTokens || call.Temperature == nil ||
		*call.Temperature != float32(stage.Temperature) ||
		params.DisableThinking != stage.DisableThinking || call.CostUSD != 0 {
		return researchRunValidationError("research model call differs from its reservation")
	}
	digest, err := researchRunLLMRequestDigestV3(researchRunLLMRequestV3{
		SchemaVersion: researchRunLLMRequestSchemaV3,
		RunSnapshotID: params.SnapshotRef.SnapshotID,
		Stage:         reservation.Stage, RoundOrdinal: reservation.RoundOrdinal,
		SubjectID: reservation.SubjectID, Provider: string(seal.ResearchModel.Provider),
		Model: stage.Model, Temperature: *call.Temperature,
		MaxTokens: *call.MaxTokens, DisableThinking: stage.DisableThinking,
		SystemPrompt: call.SystemPrompt, UserPrompt: call.UserPrompt,
	})
	if err != nil || digest != reservation.RequestDigest {
		return researchRunValidationError("research model call request digest differs")
	}
	if params.UsageKnown {
		if call.PromptTokens < 0 || call.CompletionTokens < 0 ||
			call.PromptTokens+call.CompletionTokens <= 0 {
			return researchRunValidationError("research model usage is invalid")
		}
		if call.ReasoningTokens != nil &&
			(*call.ReasoningTokens < 0 || *call.ReasoningTokens > call.CompletionTokens) {
			return researchRunValidationError("research model reasoning usage is invalid")
		}
		if (call.PromptCacheHitTokens == nil) != (call.PromptCacheMissTokens == nil) {
			return researchRunValidationError("research model cache usage is incomplete")
		}
		if call.PromptCacheHitTokens == nil {
			if call.PrefixCacheHit != nil {
				return researchRunValidationError("research model cache signal was fabricated")
			}
		} else if *call.PromptCacheHitTokens < 0 || *call.PromptCacheMissTokens < 0 ||
			*call.PromptCacheHitTokens+*call.PromptCacheMissTokens != call.PromptTokens ||
			call.PrefixCacheHit == nil ||
			*call.PrefixCacheHit != (*call.PromptCacheHitTokens > 0) {
			return researchRunValidationError("research model cache usage is invalid")
		}
	} else if call.PromptTokens != 0 || call.CompletionTokens != 0 ||
		call.PromptCacheHitTokens != nil || call.PromptCacheMissTokens != nil ||
		call.ReasoningTokens != nil || call.PrefixCacheHit != nil {
		return researchRunValidationError("unknown research model usage was fabricated")
	}
	return nil
}

func researchRunLLMSpanV3(stage string) string {
	if stage == ResearchRunLLMStagePlannerV3 {
		return runtimepolicy.ResearchModelStagePlannerV3
	}
	return runtimepolicy.ResearchModelStageSynthesisV3
}

func loadResearchRunLLMReservationByIDV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, reservationID int64, lock bool,
) (researchRunLLMReservationRowV3, error) {
	if err := snapshot.ValidateFor(identity); err != nil || reservationID <= 0 {
		return researchRunLLMReservationRowV3{},
			researchRunValidationError("research model reservation scope is invalid")
	}
	if lock {
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
			"research-llm-settlement/v3:"+identity.TemporalRunID+":"+
				researchRunSHA256([]byte(identity.TaskID))+":"+
				fmt.Sprint(reservationID)); err != nil {
			return researchRunLLMReservationRowV3{},
				researchRunDatabaseError("lock research model reservation", err)
		}
	}
	var row researchRunLLMReservationRowV3
	var subject *int64
	err := tx.QueryRow(ctx,
		`SELECT reservation.id,reservation.stage,reservation.round_ordinal,
		        reservation.subject_id,reservation.attempt_key,reservation.request_digest,
		        reservation.trace_id,pricing.provider,reservation.model,
		        reservation.system_prompt_digest,reservation.user_prompt_digest,
		        reservation.temperature,reservation.disable_thinking,
		        reservation.model_policy_digest,reservation.reserved_quota_tokens,
		        reservation.reserved_completion_tokens,
		        reserved_planner_tokens,
		        reserved_cost_micro_usd,pricing_rule_id,cost_currency,
		        counts_against_planner_budget
		   FROM research_run_llm_spend_reservations reservation
		   JOIN provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
		  WHERE reservation.id=$1 AND reservation.tenant_id=$2
		    AND reservation.user_id=$3 AND reservation.task_id=$4
		    AND reservation.run_snapshot_id=$5`,
		reservationID, identity.TenantID, identity.UserID, identity.TaskID,
		snapshot.SnapshotID,
	).Scan(&row.ReservationID, &row.Stage, &row.RoundOrdinal, &subject,
		&row.AttemptKey, &row.RequestDigest, &row.TraceID, &row.Provider, &row.Model,
		&row.SystemPromptDigest, &row.UserPromptDigest, &row.Temperature,
		&row.DisableThinking, &row.ModelPolicyDigest, &row.ReservedQuotaTokens,
		&row.ReservedCompletionTokens,
		&row.ReservedPlannerTokens,
		&row.ReservedCostMicroUSD, &row.PricingRuleID, &row.Currency,
		&row.CountsAgainstPlannerBudget)
	if err == pgx.ErrNoRows {
		return researchRunLLMReservationRowV3{},
			researchRunValidationError("research model reservation is unavailable")
	}
	if err != nil {
		return researchRunLLMReservationRowV3{},
			researchRunDatabaseError("load research model reservation", err)
	}
	if subject != nil {
		row.SubjectID = *subject
	}
	row.TenantID, row.UserID, row.TaskID, row.RunSnapshotID = identity.TenantID,
		identity.UserID, identity.TaskID, snapshot.SnapshotID
	row.MaxTokens = row.ReservedCompletionTokens
	if row.ModelPolicyDigest != snapshot.ModelPolicyDigest ||
		row.ReservationID <= 0 || row.RequestDigest == "" || row.TraceID == "" ||
		row.Provider != string(runtimepolicy.ModelProviderDeepSeekV1) ||
		len(row.SystemPromptDigest) != sha256.Size*2 || len(row.UserPromptDigest) != sha256.Size*2 ||
		row.Model == "" || row.MaxTokens <= 0 ||
		row.ReservedQuotaTokens <= 0 || row.ReservedCostMicroUSD <= 0 ||
		row.PricingRuleID <= 0 || row.Currency != "USD" {
		return researchRunLLMReservationRowV3{}, researchRunIntegrityError()
	}
	return row, nil
}

func (s *Store) LoadResearchRunLLMReceiptV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, stage string, roundOrdinal int,
) (ResearchRunLLMReceiptV3, bool, error) {
	if err := snapshot.ValidateFor(identity); err != nil ||
		(stage != ResearchRunLLMStagePlannerV3 && stage != ResearchRunLLMStageSynthesisV3) ||
		roundOrdinal < 0 || roundOrdinal >= 16 {
		return ResearchRunLLMReceiptV3{}, false,
			researchRunValidationError("research model receipt scope is invalid")
	}
	// 094 removes direct llm_calls visibility from the executor. The receipt read
	// therefore enters the exact run capability and crosses only the narrow
	// load_research_run_bound_llm_call_v1 SECURITY DEFINER boundary.
	tx, scopedRef, err := s.beginScopedResearchRunTransactionV3(ctx,
		pgx.TxOptions{AccessMode: pgx.ReadOnly}, identity, snapshot.SnapshotID)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, false,
			researchRunDatabaseError("begin research model receipt read", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if scopedRef != snapshot {
		return ResearchRunLLMReceiptV3{}, false, researchRunIntegrityError()
	}
	var reservationID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM research_run_llm_spend_reservations
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND run_snapshot_id=$4
		    AND stage=$5 AND round_ordinal=$6`,
		identity.TenantID, identity.UserID, identity.TaskID, snapshot.SnapshotID,
		stage, roundOrdinal).Scan(&reservationID)
	if err == pgx.ErrNoRows {
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunLLMReceiptV3{}, false,
				researchRunDatabaseError("commit empty research model receipt read", err)
		}
		return ResearchRunLLMReceiptV3{}, false, nil
	}
	if err != nil {
		return ResearchRunLLMReceiptV3{}, false,
			researchRunDatabaseError("load research model receipt reservation", err)
	}
	reservation, err := loadResearchRunLLMReservationByIDV3(ctx, tx, identity,
		snapshot, reservationID, false)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, false, err
	}
	receipt, settled, err := loadResearchRunLLMSettlementV3(ctx, tx, reservation)
	if err != nil {
		return ResearchRunLLMReceiptV3{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunLLMReceiptV3{}, false,
			researchRunDatabaseError("commit research model receipt read", err)
	}
	if !settled {
		return ResearchRunLLMReceiptV3{
			Reservation: reservation.ResearchRunLLMSpendReservationV3,
		}, true, nil
	}
	return receipt, true, nil
}

func loadResearchRunLLMSettlementV3(
	ctx context.Context, tx pgx.Tx, reservation researchRunLLMReservationRowV3,
) (ResearchRunLLMReceiptV3, bool, error) {
	var receipt ResearchRunLLMReceiptV3
	receipt.Reservation = reservation.ResearchRunLLMSpendReservationV3
	var llmCallID *int64
	var outcome string
	err := tx.QueryRow(ctx,
		`SELECT llm_call_id,attempted,usage_known,definitely_zero_usage,actual_prompt_tokens,
		        actual_completion_tokens,actual_cost_micro_usd,pricing_status,
		        outcome,error_code
		   FROM research_run_llm_spend_settlements
		  WHERE reservation_id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		    AND run_snapshot_id=$5`, reservation.ReservationID, reservation.TenantID,
		reservation.UserID, reservation.TaskID, reservation.RunSnapshotID,
	).Scan(&llmCallID, &receipt.Attempted, &receipt.UsageKnown,
		&receipt.DefinitelyZeroUsage,
		&receipt.ActualPromptTokens, &receipt.ActualCompletionTokens,
		&receipt.ActualCostMicroUSD, &receipt.PricingStatus, &outcome,
		&receipt.ErrorCode)
	if err == pgx.ErrNoRows {
		return ResearchRunLLMReceiptV3{}, false, nil
	}
	if err != nil {
		return ResearchRunLLMReceiptV3{}, false,
			researchRunDatabaseError("load research model settlement", err)
	}
	receipt.Settled, receipt.Outcome = true, ResearchRunLLMOutcomeV3(outcome)
	if receipt.Attempted {
		if llmCallID == nil || *llmCallID <= 0 {
			return ResearchRunLLMReceiptV3{}, false, researchRunIntegrityError()
		}
		receipt.LLMCallID = *llmCallID
		call, err := loadResearchRunBoundLLMCallV3(ctx, tx, reservation, *llmCallID)
		if err != nil {
			return ResearchRunLLMReceiptV3{}, false, err
		}
		receipt.Call = call
	} else if llmCallID != nil {
		return ResearchRunLLMReceiptV3{}, false, researchRunIntegrityError()
	}
	return receipt, true, nil
}

func loadResearchRunBoundLLMCallV3(
	ctx context.Context, tx pgx.Tx, reservation researchRunLLMReservationRowV3,
	callID int64,
) (types.LLMCall, error) {
	var call types.LLMCall
	var tenantID *int64
	err := tx.QueryRow(ctx,
		`SELECT id,run_snapshot_id,tenant_id,trace_id,span_name,user_id,ref_type,ref_id,
		        provider,model,system_prompt,user_prompt,completion,prompt_tokens,
		        completion_tokens,prompt_cache_hit_tokens,prompt_cache_miss_tokens,
		        reasoning_tokens,latency_ms,cost_usd::double precision,prefix_cache_hit,
		        temperature,max_tokens,error,created_at
		   FROM load_research_run_bound_llm_call_v1($1,$2)`,
		callID, reservation.ReservationID,
	).Scan(&call.ID, &call.RunSnapshotID, &tenantID, &call.TraceID, &call.SpanName,
		&call.UserID, &call.RefType, &call.RefID, &call.Provider, &call.Model,
		&call.SystemPrompt, &call.UserPrompt, &call.Completion, &call.PromptTokens,
		&call.CompletionTokens, &call.PromptCacheHitTokens,
		&call.PromptCacheMissTokens, &call.ReasoningTokens, &call.LatencyMs,
		&call.CostUSD, &call.PrefixCacheHit, &call.Temperature, &call.MaxTokens,
		&call.Error, &call.CreatedAt)
	if err == pgx.ErrNoRows {
		return types.LLMCall{}, researchRunIntegrityError()
	}
	if err != nil {
		return types.LLMCall{}, researchRunDatabaseError("load bound research model call", err)
	}
	call.TenantID = tenantID
	if call.ID != callID || call.TraceID != reservation.TraceID ||
		call.Provider != reservation.Provider || strings.TrimSpace(call.Model) == "" {
		return types.LLMCall{}, researchRunIntegrityError()
	}
	return call, nil
}

func validateResearchRunLLMSettlementReplayV3(
	existing ResearchRunLLMReceiptV3, params CommitResearchRunLLMReceiptV3Params,
) error {
	if !existing.Settled || existing.Attempted != params.Attempted ||
		existing.UsageKnown != params.UsageKnown || existing.Outcome != params.Outcome ||
		existing.DefinitelyZeroUsage != params.DefinitelyZeroUsage ||
		existing.ErrorCode != params.ErrorCode ||
		existing.Reservation.DisableThinking != params.DisableThinking {
		return researchRunConflictError()
	}
	if params.Attempted {
		candidate := params.Call
		stored := existing.Call
		if !equalInt64PointerV3(candidate.RunSnapshotID, stored.RunSnapshotID) ||
			!equalInt64PointerV3(candidate.TenantID, stored.TenantID) ||
			candidate.TraceID != stored.TraceID || candidate.SpanName != stored.SpanName ||
			!equalInt64PointerV3(candidate.UserID, stored.UserID) ||
			candidate.RefType != stored.RefType ||
			!equalInt64PointerV3(candidate.RefID, stored.RefID) ||
			candidate.Provider != stored.Provider || candidate.Model != stored.Model ||
			candidate.SystemPrompt != stored.SystemPrompt || candidate.UserPrompt != stored.UserPrompt ||
			candidate.Completion != stored.Completion || candidate.PromptTokens != stored.PromptTokens ||
			candidate.CompletionTokens != stored.CompletionTokens ||
			!equalIntPointerV3(candidate.PromptCacheHitTokens, stored.PromptCacheHitTokens) ||
			!equalIntPointerV3(candidate.PromptCacheMissTokens, stored.PromptCacheMissTokens) ||
			!equalIntPointerV3(candidate.ReasoningTokens, stored.ReasoningTokens) ||
			candidate.LatencyMs != stored.LatencyMs ||
			!equalBoolPointerV3(candidate.PrefixCacheHit, stored.PrefixCacheHit) ||
			!equalFloat32PointerV3(candidate.Temperature, stored.Temperature) ||
			!equalIntPointerV3(candidate.MaxTokens, stored.MaxTokens) ||
			candidate.Error != stored.Error {
			return researchRunConflictError()
		}
	}
	return nil
}

func equalInt64PointerV3(left, right *int64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func equalIntPointerV3(left, right *int) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func equalBoolPointerV3(left, right *bool) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func equalFloat32PointerV3(left, right *float32) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
