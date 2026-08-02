package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

const researchBriefDeliverySchemaV3 = "vane.research-brief-delivery/v3"

var researchBriefDeliveryUUIDNamespaceV3 = uuid.MustParse("2048809c-258f-4e52-9b64-37486675410f")

type ResearchBriefDeliveryV3 struct {
	ID                   int64
	Identity             types.RunIdentity
	RunSnapshotID        int64
	PlanID               int64
	BriefID              int64
	BriefReferenceDigest string
	BriefDigest          string
	CardDigest           string
	BatchID              int64
	DeliveryID           int64
	EffectID             string
	Status               string
	ProviderMessageID    string
	ReceiptDigest        string
	SentAt               *time.Time
}

type PrepareResearchBriefDeliveryV3Params struct {
	Identity       types.RunIdentity
	SnapshotRef    types.ResearchRunSnapshotRefV3
	PlanRef        types.ResearchRunPlanRefV3
	BriefRef       types.ResearchBriefRefV3
	Provider       string
	AppIdentity    string
	ProviderChatID string
	Target         string
	Card           []byte
}

func (s *Store) LoadResearchBriefPayloadForDeliveryV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	brief types.ResearchBriefRefV3,
) (types.ResearchBriefPayloadV3, error) {
	if err := validateResearchBriefDeliveryRefsV3(identity, snapshot, plan, brief); err != nil {
		return types.ResearchBriefPayloadV3{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return types.ResearchBriefPayloadV3{}, researchRunDatabaseError("begin research Brief delivery read", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setResearchBriefDeliveryAppScopeV3(ctx, tx, identity); err != nil {
		return types.ResearchBriefPayloadV3{}, err
	}
	raw, err := loadExactResearchBriefDeliveryPayloadV3(ctx, tx, identity, snapshot, plan, brief)
	if err != nil {
		return types.ResearchBriefPayloadV3{}, err
	}
	payload, canonical, err := types.DecodeResearchBriefPayloadV3(raw)
	if err != nil || string(canonical) != string(raw) {
		return types.ResearchBriefPayloadV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchBriefPayloadV3{}, researchRunDatabaseError("commit research Brief delivery read", err)
	}
	return payload, nil
}

// PrepareOrGetResearchBriefDeliveryV3 atomically freezes the finalized Brief,
// projection rows, exact provider request, and durable effect before any send
// claim can be acquired. Exact response-lost replay returns the same anchor.
func (s *Store) PrepareOrGetResearchBriefDeliveryV3(
	ctx context.Context, params PrepareResearchBriefDeliveryV3Params,
) (ResearchBriefDeliveryV3, *pusheffect.Effect, error) {
	if err := validateResearchBriefDeliveryPrepareV3(params); err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	canonicalCard := json.RawMessage(params.Card)
	if !json.Valid(canonicalCard) {
		return ResearchBriefDeliveryV3{}, nil, researchRunValidationError("research Brief delivery card is invalid")
	}
	cardDigestBytes := sha256.Sum256(params.Card)
	cardDigest := hex.EncodeToString(cardDigestBytes[:])
	effectID := uuid.NewSHA1(researchBriefDeliveryUUIDNamespaceV3,
		[]byte(params.BriefRef.ReferenceDigest)).String()

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("begin research Brief delivery preparation", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setResearchBriefDeliveryAppScopeV3(ctx, tx, params.Identity); err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,100))`,
		params.BriefRef.ReferenceDigest); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError(
			"lock research Brief delivery preparation", err)
	}
	if existing, found, err := loadResearchBriefDeliveryV3(ctx, tx, params.Identity, params.BriefRef.BriefID, false); err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	} else if found {
		if err := validateResearchBriefDeliveryReplayV3(existing, params, effectID, cardDigest); err != nil {
			return ResearchBriefDeliveryV3{}, nil, err
		}
		if _, err := loadExactResearchBriefDeliveryPayloadV3(ctx, tx, params.Identity,
			params.SnapshotRef, params.PlanRef, params.BriefRef); err != nil {
			return ResearchBriefDeliveryV3{}, nil, err
		}
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_push_effect_coordinator`); err != nil {
			return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("enter research delivery effect role", err)
		}
		effect, err := loadPushEffectForUpdate(ctx, tx, pusheffect.Scope{
			ID: existing.EffectID, TenantID: params.Identity.TenantID, UserID: params.Identity.UserID,
		})
		if err != nil || effect.PayloadDigest == "" || effect.CardDigest != cardDigest {
			return ResearchBriefDeliveryV3{}, nil, researchRunIntegrityError()
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("commit research Brief delivery replay", err)
		}
		return existing, effect, nil
	}

	briefPayload, err := loadExactResearchBriefDeliveryPayloadV3(ctx, tx, params.Identity,
		params.SnapshotRef, params.PlanRef, params.BriefRef)
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	decodedBrief, _, err := types.DecodeResearchBriefPayloadV3(briefPayload)
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunIntegrityError()
	}
	if err := authorizeResearchBriefDeliveryPrepareV3(ctx, tx, params.Identity); err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("lock research delivery schema admission", err)
	}
	physicalKey := "research-v3/" + params.BriefRef.ReferenceDigest
	var batchID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO push_batches
		    (tenant_id,user_id,idempotency_key,schedule_id,run_snapshot_id)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (idempotency_key) WHERE idempotency_key<>''
		DO UPDATE SET user_id=EXCLUDED.user_id
		 WHERE push_batches.tenant_id=EXCLUDED.tenant_id
		   AND push_batches.user_id=EXCLUDED.user_id
		   AND push_batches.schedule_id=EXCLUDED.schedule_id
		   AND push_batches.run_snapshot_id=EXCLUDED.run_snapshot_id
		RETURNING id`, params.Identity.TenantID, params.Identity.UserID, physicalKey,
		params.Identity.TaskID, params.SnapshotRef.SnapshotID).Scan(&batchID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchBriefDeliveryV3{}, nil, researchRunConflictError()
	}
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("create research Brief push batch", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_push_batch_authority`); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("enter research delivery batch authority", err)
	}
	var authority types.PushBatchDeliveryAuthority
	if err := tx.QueryRow(ctx, `
		UPDATE push_batches SET delivery_authority=COALESCE(delivery_authority,$4)
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		 RETURNING delivery_authority`, batchID, params.Identity.TenantID,
		params.Identity.UserID, types.PushBatchDeliveryAuthorityEffect).Scan(&authority); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("claim research delivery batch authority", err)
	}
	if authority != types.PushBatchDeliveryAuthorityEffect {
		return ResearchBriefDeliveryV3{}, nil, researchRunConflictError()
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("restore research delivery app role", err)
	}
	var deliveryID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO deliveries
		    (tenant_id,batch_id,user_id,score,body_md,card_json,feishu_message_id,status)
		VALUES ($1,$2,$3,0,$4,'{}'::jsonb,'',$5)
		RETURNING id`, params.Identity.TenantID, batchID, params.Identity.UserID,
		decodedBrief.Summary, types.DeliveryStatusPending).Scan(&deliveryID)
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("create research Brief delivery projection", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("read research delivery clock", err)
	}
	prepared := pusheffect.Prepared{
		ID: effectID, TenantID: params.Identity.TenantID, UserID: params.Identity.UserID,
		TaskID: params.Identity.TaskID, RunSnapshotID: params.SnapshotRef.SnapshotID,
		RunID: params.Identity.TemporalRunID, StepID: "research-brief-delivery/v3",
		ChunkIndex: 0, ChunkCount: 1, BatchID: batchID, DeliveryIDs: []int64{deliveryID},
		Provider: params.Provider, AppIdentity: params.AppIdentity,
		ProviderChatID: params.ProviderChatID, Target: params.Target,
		Card: append([]byte(nil), params.Card...), ProviderUUID: effectID,
		IdempotencyExpiresAt: databaseNow.UTC().Add(55 * time.Minute).Truncate(time.Microsecond),
	}
	canonicalEffect, err := pusheffect.Canonicalize(prepared)
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunValidationError("research Brief effect is invalid")
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_push_effect_coordinator`); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("enter research delivery effect role", err)
	}
	if _, err := lockPushEffectBatchAdmission(ctx, tx, types.PushBatchScope{
		TenantID: params.Identity.TenantID, UserID: params.Identity.UserID, BatchID: batchID,
	}, params.SnapshotRef.SnapshotID, types.PushBatchDeliveryAuthorityEffect); err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	if err := verifyPushEffectAggregate(ctx, tx, prepared); err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO push_effects (
		 id,tenant_id,user_id,task_id,run_snapshot_id,run_id,step_id,
		 chunk_index,chunk_count,batch_id,delivery_ids,provider,app_identity,
		 provider_chat_id,target,card_payload,card_digest,provider_uuid,
		 idempotency_expires_at,schema_version,canonical_payload,payload_digest
		) VALUES ($1,$2,$3,$4,$5,$6,$7,0,1,$8,$9,$10,$11,$12,$13,$14,$15,$16::uuid,$17,$18,$19,$20)`,
		prepared.ID, prepared.TenantID, prepared.UserID, prepared.TaskID,
		prepared.RunSnapshotID, prepared.RunID, prepared.StepID, prepared.BatchID,
		prepared.DeliveryIDs, prepared.Provider, prepared.AppIdentity,
		prepared.ProviderChatID, prepared.Target, prepared.Card, canonicalEffect.CardDigest(),
		prepared.ProviderUUID, prepared.IdempotencyExpiresAt, pusheffect.SchemaVersion,
		canonicalEffect.Payload(), canonicalEffect.Digest())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ResearchBriefDeliveryV3{}, nil, researchRunConflictError()
		}
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("freeze research Brief push effect", err)
	}
	effect, err := loadPushEffectForUpdate(ctx, tx, prepared.Scope())
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, err
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("restore research delivery writer", err)
	}
	var anchor ResearchBriefDeliveryV3
	err = tx.QueryRow(ctx, `
		INSERT INTO research_brief_deliveries (
		 tenant_id,user_id,task_id,run_snapshot_id,plan_id,brief_id,
		 temporal_workflow_id,temporal_run_id,brief_reference_digest,brief_digest,
		 card_digest,batch_id,delivery_id,effect_id,schema_version
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		RETURNING id,status,provider_message_id,receipt_digest,sent_at`,
		params.Identity.TenantID, params.Identity.UserID, params.Identity.TaskID,
		params.SnapshotRef.SnapshotID, params.PlanRef.PlanID, params.BriefRef.BriefID,
		params.Identity.TemporalWorkflowID, params.Identity.TemporalRunID,
		params.BriefRef.ReferenceDigest, params.BriefRef.BriefDigest, cardDigest,
		batchID, deliveryID, effectID, researchBriefDeliverySchemaV3).Scan(
		&anchor.ID, &anchor.Status, &anchor.ProviderMessageID, &anchor.ReceiptDigest, &anchor.SentAt)
	if err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("freeze research Brief delivery anchor", err)
	}
	fillResearchBriefDeliveryV3(&anchor, params, batchID, deliveryID, effectID, cardDigest)
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefDeliveryV3{}, nil, researchRunDatabaseError("commit research Brief delivery preparation", err)
	}
	return anchor, effect, nil
}

func (s *Store) ClaimResearchBriefDeliveryV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	brief types.ResearchBriefRefV3, leaseOwner string, leaseDuration time.Duration,
) (*pusheffect.Effect, error) {
	if err := validateResearchBriefDeliveryRefsV3(identity, snapshot, plan, brief); err != nil ||
		strings.TrimSpace(leaseOwner) != leaseOwner || leaseOwner == "" || leaseDuration <= 0 {
		return nil, researchRunValidationError("research Brief delivery claim is invalid")
	}
	anchor, err := s.LoadResearchBriefDeliveryV3(ctx, identity, snapshot, plan, brief)
	if err != nil {
		return nil, err
	}
	claimed, decision, err := s.claimAuthorizedPushEffectWithGate(ctx, pusheffect.AuthorizedClaimParams{
		ClaimParams: pusheffect.ClaimParams{
			Scope:      pusheffect.Scope{ID: anchor.EffectID, TenantID: identity.TenantID, UserID: identity.UserID},
			LeaseOwner: leaseOwner, LeaseDuration: leaseDuration,
		},
		ExpectedTaskID: identity.TaskID, DenialRetryAfter: 30 * time.Second,
	}, false, func(_ context.Context, _ pgx.Tx, effect *pusheffect.Effect) error {
		if effect == nil || effect.ID != anchor.EffectID ||
			effect.RunSnapshotID != snapshot.SnapshotID ||
			effect.RunID != identity.TemporalRunID {
			return researchRunIntegrityError()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if decision == pusheffect.AuthorizedClaimDenied {
		return nil, researchDeliveryNotFoundV3()
	}
	if decision != pusheffect.AuthorizedClaimed || claimed == nil ||
		claimed.ID != anchor.EffectID || claimed.RunSnapshotID != snapshot.SnapshotID ||
		claimed.RunID != identity.TemporalRunID {
		return nil, researchRunConflictError()
	}
	return claimed, nil
}

func (s *Store) LoadResearchBriefDeliveryV3(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	brief types.ResearchBriefRefV3,
) (ResearchBriefDeliveryV3, error) {
	if err := validateResearchBriefDeliveryRefsV3(identity, snapshot, plan, brief); err != nil {
		return ResearchBriefDeliveryV3{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return ResearchBriefDeliveryV3{}, researchRunDatabaseError("begin research Brief delivery load", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := setResearchBriefDeliveryAppScopeV3(ctx, tx, identity); err != nil {
		return ResearchBriefDeliveryV3{}, err
	}
	row, found, err := loadResearchBriefDeliveryV3(ctx, tx, identity, brief.BriefID, false)
	if err != nil {
		return ResearchBriefDeliveryV3{}, err
	}
	if !found || row.RunSnapshotID != snapshot.SnapshotID || row.PlanID != plan.PlanID ||
		row.BriefReferenceDigest != brief.ReferenceDigest || row.BriefDigest != brief.BriefDigest {
		return ResearchBriefDeliveryV3{}, researchDeliveryNotFoundV3()
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchBriefDeliveryV3{}, researchRunDatabaseError("commit research Brief delivery load", err)
	}
	return row, nil
}

func setResearchBriefDeliveryAppScopeV3(ctx context.Context, tx pgx.Tx, identity types.RunIdentity) error {
	if tx == nil || identity.Validate() != nil {
		return researchRunValidationError("research Brief delivery scope is invalid")
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
		fmt.Sprint(identity.TenantID), fmt.Sprint(identity.UserID)); err != nil {
		return researchRunDatabaseError("install research Brief delivery scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return researchRunDatabaseError("enter research Brief delivery role", err)
	}
	return nil
}

func loadExactResearchBriefDeliveryPayloadV3(ctx context.Context, tx pgx.Tx,
	identity types.RunIdentity, snapshot types.ResearchRunSnapshotRefV3,
	plan types.ResearchRunPlanRefV3, brief types.ResearchBriefRefV3,
) ([]byte, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT synthesis.brief_payload
		  FROM research_brief_syntheses synthesis
		  JOIN research_run_plans plan ON plan.id=synthesis.plan_id
		  JOIN task_run_snapshots snapshot ON snapshot.id=synthesis.run_snapshot_id
		 WHERE synthesis.id=$1 AND synthesis.tenant_id=$2 AND synthesis.user_id=$3
		   AND synthesis.task_id=$4 AND synthesis.run_snapshot_id=$5 AND synthesis.plan_id=$6
		   AND synthesis.temporal_workflow_id=$7 AND synthesis.temporal_run_id=$8
		   AND synthesis.definition_digest=$9 AND synthesis.plan_digest=$10
		   AND synthesis.request_digest=$11 AND synthesis.brief_digest=$12
		   AND synthesis.evidence_digest=$13 AND synthesis.history_digest=$14
		   AND synthesis.notification_threshold=$15
		   AND synthesis.significance=$16 AND synthesis.decision=$17
		   AND synthesis.delivery_required=true AND synthesis.status='finalized'
		   AND plan.plan_digest=$10 AND plan.definition_digest=$9
		   AND snapshot.reference_schema_version=$18
		   AND snapshot.reference_digest=$19`,
		brief.BriefID, identity.TenantID, identity.UserID, identity.TaskID,
		snapshot.SnapshotID, plan.PlanID, identity.TemporalWorkflowID, identity.TemporalRunID,
		brief.DefinitionDigest, brief.PlanDigest, brief.RequestDigest, brief.BriefDigest,
		brief.EvidenceDigest, brief.HistoryDigest, brief.NotificationThreshold,
		brief.Significance, brief.Decision, types.ResearchRunSnapshotRefSchemaV3,
		snapshot.ReferenceDigest).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, researchDeliveryNotFoundV3()
	}
	if err != nil {
		return nil, researchRunDatabaseError("load finalized research Brief for delivery", err)
	}
	return raw, nil
}

func loadResearchBriefDeliveryV3(ctx context.Context, tx pgx.Tx,
	identity types.RunIdentity, briefID int64, lock bool,
) (ResearchBriefDeliveryV3, bool, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	row := ResearchBriefDeliveryV3{Identity: identity}
	err := tx.QueryRow(ctx, `
		SELECT id,run_snapshot_id,plan_id,brief_id,brief_reference_digest,
		       brief_digest,card_digest,batch_id,delivery_id,effect_id,status,
		       provider_message_id,receipt_digest,sent_at
		  FROM research_brief_deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND brief_id=$4`+suffix,
		identity.TenantID, identity.UserID, identity.TaskID, briefID).Scan(
		&row.ID, &row.RunSnapshotID, &row.PlanID, &row.BriefID,
		&row.BriefReferenceDigest, &row.BriefDigest, &row.CardDigest, &row.BatchID,
		&row.DeliveryID, &row.EffectID, &row.Status, &row.ProviderMessageID,
		&row.ReceiptDigest, &row.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchBriefDeliveryV3{}, false, nil
	}
	if err != nil {
		return ResearchBriefDeliveryV3{}, false, researchRunDatabaseError("load research Brief delivery", err)
	}
	if row.ID <= 0 || row.BriefID != briefID || row.BatchID <= 0 || row.DeliveryID <= 0 ||
		row.EffectID == "" || !validResearchRunDigest(row.BriefReferenceDigest) ||
		!validResearchRunDigest(row.BriefDigest) || !validResearchRunDigest(row.CardDigest) ||
		(row.Status == "prepared" && (row.ProviderMessageID != "" || row.ReceiptDigest != "" || row.SentAt != nil)) ||
		(row.Status == "sent" && (row.ProviderMessageID == "" || !validResearchRunDigest(row.ReceiptDigest) || row.SentAt == nil)) ||
		(row.Status != "prepared" && row.Status != "sent") {
		return ResearchBriefDeliveryV3{}, false, researchRunIntegrityError()
	}
	return row, true, nil
}

func validateResearchBriefDeliveryPrepareV3(params PrepareResearchBriefDeliveryV3Params) error {
	if err := validateResearchBriefDeliveryRefsV3(params.Identity, params.SnapshotRef,
		params.PlanRef, params.BriefRef); err != nil {
		return err
	}
	if !params.BriefRef.DeliveryRequired || len(params.Card) == 0 || len(params.Card) > 2<<20 ||
		!validResearchDeliveryTextV3(params.Provider, 512) ||
		!validResearchDeliveryTextV3(params.AppIdentity, 512) ||
		!validResearchDeliveryTextV3(params.ProviderChatID, 512) ||
		!validResearchDeliveryTextV3(params.Target, 1024) {
		return researchRunValidationError("research Brief delivery preparation is invalid")
	}
	return nil
}

func validateResearchBriefDeliveryRefsV3(identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, plan types.ResearchRunPlanRefV3,
	brief types.ResearchBriefRefV3,
) error {
	if identity.Validate() != nil || identity.RunKind != types.RunSnapshotKindScheduled ||
		snapshot.ValidateFor(identity) != nil || plan.ValidateFor(identity, snapshot.SnapshotID) != nil ||
		brief.ValidateFor(identity, snapshot.SnapshotID, plan.PlanID) != nil {
		return researchRunValidationError("research Brief delivery references are invalid")
	}
	return nil
}

func validateResearchBriefDeliveryReplayV3(row ResearchBriefDeliveryV3,
	params PrepareResearchBriefDeliveryV3Params, effectID, cardDigest string,
) error {
	if row.Identity != params.Identity || row.RunSnapshotID != params.SnapshotRef.SnapshotID ||
		row.PlanID != params.PlanRef.PlanID || row.BriefID != params.BriefRef.BriefID ||
		row.BriefReferenceDigest != params.BriefRef.ReferenceDigest ||
		row.BriefDigest != params.BriefRef.BriefDigest || row.CardDigest != cardDigest ||
		row.EffectID != effectID {
		return researchRunConflictError()
	}
	return nil
}

func fillResearchBriefDeliveryV3(row *ResearchBriefDeliveryV3,
	params PrepareResearchBriefDeliveryV3Params, batchID, deliveryID int64,
	effectID, cardDigest string,
) {
	row.Identity = params.Identity
	row.RunSnapshotID = params.SnapshotRef.SnapshotID
	row.PlanID = params.PlanRef.PlanID
	row.BriefID = params.BriefRef.BriefID
	row.BriefReferenceDigest = params.BriefRef.ReferenceDigest
	row.BriefDigest = params.BriefRef.BriefDigest
	row.CardDigest = cardDigest
	row.BatchID = batchID
	row.DeliveryID = deliveryID
	row.EffectID = effectID
}

func validResearchDeliveryTextV3(value string, max int) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= max
}

func authorizeResearchBriefDeliveryPrepareV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
) error {
	var authorized int
	err := tx.QueryRow(ctx, `
		SELECT 1
		  FROM schedules schedule
		  JOIN tenants tenant ON tenant.id=schedule.tenant_id
		   AND tenant.status='active' AND tenant.deleted_at IS NULL
		  JOIN memberships membership
		    ON membership.tenant_id=schedule.tenant_id
		   AND membership.user_id=schedule.user_id
		 WHERE schedule.id=$1 AND schedule.tenant_id=$2 AND schedule.user_id=$3
		   AND schedule.status='active' AND schedule.execution_mode='discover_at_run'
		 FOR SHARE OF schedule,tenant,membership`, identity.TaskID,
		identity.TenantID, identity.UserID).Scan(&authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return researchDeliveryNotFoundV3()
	}
	if err != nil {
		return researchRunDatabaseError("authorize research Brief delivery preparation", err)
	}
	return nil
}

func researchDeliveryNotFoundV3() error {
	return types.NewAppError(types.CodeNotFound,
		"research Brief delivery is unavailable", types.ErrNotFound)
}
