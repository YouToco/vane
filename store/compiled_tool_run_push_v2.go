package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

const compiledToolPushBatchKeyNamespaceV2 = "compiled-tool/v2/"

func compiledToolPushBatchPhysicalKeyV2(
	snapshotID int64,
	logicalKey string,
) string {
	return compiledToolPushBatchKeyNamespaceV2 +
		strconv.FormatInt(snapshotID, 10) + "/" + logicalKey
}

func validToolInvocationDigestV2(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32 &&
		digest == strings.ToLower(digest)
}

func (s *Store) CreatePushBatchForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	idempotencyKey string,
) (int64, error) {
	if idempotencyKey == "" {
		return 0, taskRunValidationError(
			"compiled Tool push batch idempotency key is empty")
	}
	physicalKey := compiledToolPushBatchPhysicalKeyV2(
		ref.SnapshotID, idempotencyKey)
	tx, _, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return 0, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return 0, taskRunDatabaseError(
			"lock compiled Tool push batch schema admission", err)
	}
	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO push_batches
		    (tenant_id,user_id,idempotency_key,schedule_id,run_snapshot_id)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (idempotency_key)
		     WHERE idempotency_key <> ''
		 DO UPDATE SET user_id=EXCLUDED.user_id,
		               exit_gate='',stage_counts='{}'
		 WHERE push_batches.tenant_id=EXCLUDED.tenant_id
		   AND push_batches.user_id=EXCLUDED.user_id
		   AND push_batches.schedule_id IS NOT DISTINCT FROM
		       EXCLUDED.schedule_id
		   AND push_batches.run_snapshot_id=EXCLUDED.run_snapshot_id
		 RETURNING id`,
		expected.TenantID, expected.UserID, physicalKey,
		expected.TaskID, ref.SnapshotID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, types.NewAppError(types.CodeConflict,
			"compiled Tool push batch key belongs to another scope", nil)
	}
	if err != nil {
		return 0, taskRunDatabaseError(
			"create compiled Tool push batch", err)
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit compiled Tool push batch"); err != nil {
		return 0, err
	}
	return id, nil
}

// CreateOrRecoverPushBatchForTaskRunV2 allows revoked recovery only after all
// exact deliveries are already sent. It cannot authorize a new delivery or
// provider attempt.
func (s *Store) CreateOrRecoverPushBatchForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	idempotencyKey string,
) (id int64, recoveryOnly bool, err error) {
	id, err = s.CreatePushBatchForTaskRunV2(
		ctx, expected, ref, idempotencyKey)
	if err == nil {
		return id, false, nil
	}
	if !errors.Is(err, types.ErrNotFound) {
		return 0, false, err
	}
	physicalKey := compiledToolPushBatchPhysicalKeyV2(
		ref.SnapshotID, idempotencyKey)
	tx, _, err := s.beginCompiledToolRunReadV2(ctx, expected, ref)
	if err != nil {
		return 0, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	err = tx.QueryRow(ctx,
		`SELECT b.id
		   FROM push_batches b
		  WHERE b.idempotency_key=$1
		    AND b.tenant_id=$2 AND b.user_id=$3
		    AND b.schedule_id=$4 AND b.run_snapshot_id=$5
		    AND b.delivery_authority=$6
		    AND b.status IN ($7,$8)
		    AND EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.batch_id=b.id
		           AND d.tenant_id=b.tenant_id
		           AND d.user_id=b.user_id
		           AND d.status=$9
		           AND d.invocation_digest IS NOT NULL
		    )
		    AND NOT EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.batch_id=b.id
		           AND (d.tenant_id IS DISTINCT FROM b.tenant_id
		             OR d.user_id IS DISTINCT FROM b.user_id
		             OR d.status IS DISTINCT FROM $9
		             OR d.invocation_digest IS NULL)
		    )
		  FOR SHARE OF b`,
		physicalKey, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
		types.BatchStatusPending, types.BatchStatusDone,
		types.DeliveryStatusSent,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, taskRunNotFound()
	}
	if err != nil {
		return 0, false, taskRunDatabaseError(
			"recover compiled Tool push batch receipt", err)
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit compiled Tool batch receipt recovery"); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (s *Store) RecordEmptyPushBatchForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	idempotencyKey string,
	gate types.BatchExitGate,
	counts types.PipelineCounts,
) (id int64, skipped bool, err error) {
	if idempotencyKey == "" || gate == "" {
		return 0, false, taskRunValidationError(
			"compiled Tool empty batch input is invalid")
	}
	countsJSON, err := json.Marshal(counts)
	if err != nil {
		return 0, false, types.NewAppError(
			types.CodeInternal,
			"serialize compiled Tool empty batch counts", err)
	}
	physicalKey := compiledToolPushBatchPhysicalKeyV2(
		ref.SnapshotID, idempotencyKey)
	tx, _, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return 0, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return 0, false, taskRunDatabaseError(
			"lock compiled Tool empty batch schema admission", err)
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO push_batches
		    (tenant_id,user_id,status,exit_gate,stage_counts,
		     idempotency_key,schedule_id,run_snapshot_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (idempotency_key)
		     WHERE idempotency_key <> ''
		 DO UPDATE SET exit_gate=EXCLUDED.exit_gate,
		               stage_counts=EXCLUDED.stage_counts
		 WHERE push_batches.status=EXCLUDED.status
		   AND push_batches.tenant_id=EXCLUDED.tenant_id
		   AND push_batches.user_id=EXCLUDED.user_id
		   AND push_batches.schedule_id IS NOT DISTINCT FROM
		       EXCLUDED.schedule_id
		   AND push_batches.run_snapshot_id=EXCLUDED.run_snapshot_id
		 RETURNING id`,
		expected.TenantID, expected.UserID, types.BatchStatusEmpty,
		gate, countsJSON, physicalKey, expected.TaskID, ref.SnapshotID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingStatus types.BatchStatus
		lookupErr := tx.QueryRow(ctx,
			`SELECT status FROM push_batches
			  WHERE tenant_id=$1 AND user_id=$2
			    AND schedule_id=$3 AND run_snapshot_id=$4
			    AND idempotency_key=$5`,
			expected.TenantID, expected.UserID, expected.TaskID,
			ref.SnapshotID, physicalKey,
		).Scan(&existingStatus)
		if lookupErr != nil {
			return 0, false, taskRunDatabaseError(
				"inspect compiled Tool empty batch conflict", lookupErr)
		}
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit compiled Tool empty batch guard"); err != nil {
			return 0, false, err
		}
		return 0, existingStatus != types.BatchStatusEmpty, nil
	}
	if err != nil {
		return 0, false, taskRunDatabaseError(
			"record compiled Tool empty batch", err)
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit compiled Tool empty batch"); err != nil {
		return 0, false, err
	}
	return id, false, nil
}

func (s *Store) InsertDeliveryForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	idempotencyKey string,
	invocationDigest string,
	d *types.Delivery,
) (id int64, existed bool, sentAlready bool, err error) {
	if d == nil || d.BatchID <= 0 ||
		d.UserID != expected.UserID ||
		d.ContentItemID == nil || *d.ContentItemID <= 0 ||
		idempotencyKey == "" ||
		!validToolInvocationDigestV2(invocationDigest) ||
		d.InvocationDigest != "" &&
			d.InvocationDigest != invocationDigest {
		return 0, false, false, taskRunValidationError(
			"compiled Tool delivery input is invalid")
	}
	physicalKey := compiledToolPushBatchPhysicalKeyV2(
		ref.SnapshotID, idempotencyKey)
	card := d.CardJSON
	if len(card) == 0 {
		card = json.RawMessage("{}")
	}
	status := d.Status
	if status == "" {
		status = types.DeliveryStatusPending
	}
	tx, snapshot, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return 0, false, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, found := frozenToolBindingByDigestV2(
		snapshot, invocationDigest); !found {
		return 0, false, false, taskRunValidationError(
			"compiled Tool delivery invocation is outside the frozen run")
	}
	if _, err := lockPushEffectBatchAdmission(
		ctx, tx,
		types.PushBatchScope{
			TenantID: expected.TenantID,
			UserID:   expected.UserID,
			BatchID:  d.BatchID,
		},
		ref.SnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	); err != nil {
		if errors.Is(err, types.ErrConflict) {
			return 0, false, false, taskRunNotFound()
		}
		return 0, false, false, err
	}
	effectBatches, err := lockObservedEventPushEffects(
		ctx, tx, expected.TenantID, expected.UserID, []int64{d.BatchID})
	if err != nil {
		return 0, false, false, err
	}
	if effectBatches[d.BatchID] {
		var storedStatus types.DeliveryStatus
		lookupErr := tx.QueryRow(ctx,
			`SELECT d.id,d.status
			   FROM deliveries d
			   JOIN push_batches b ON b.id=d.batch_id
			  WHERE d.batch_id=$1 AND d.content_item_id=$2
			    AND d.tenant_id=$3 AND d.user_id=$4
			    AND d.invocation_digest=$5
			    AND d.score IS NOT DISTINCT FROM $6
			    AND d.body_md=$7
			    AND b.idempotency_key=$8
			    AND b.schedule_id=$9
			    AND b.run_snapshot_id=$10
			  FOR UPDATE OF d`,
			d.BatchID, *d.ContentItemID,
			expected.TenantID, expected.UserID, invocationDigest,
			d.Score, d.BodyMD, physicalKey, expected.TaskID,
			ref.SnapshotID,
		).Scan(&id, &storedStatus)
		if lookupErr == nil {
			if err := commitCompiledRunWriteV1(
				ctx, tx, "commit frozen compiled Tool delivery replay",
			); err != nil {
				return 0, false, false, err
			}
			return id, true,
				storedStatus == types.DeliveryStatusSent, nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return 0, false, false, taskRunDatabaseError(
				"load frozen compiled Tool delivery", lookupErr)
		}
		return 0, false, false, types.NewAppError(
			types.CodeConflict,
			"compiled Tool effect delivery aggregate is frozen", nil)
	}

	err = tx.QueryRow(ctx,
		`INSERT INTO deliveries (
		    tenant_id,batch_id,user_id,content_item_id,invocation_digest,
		    score,body_md,card_json,feishu_message_id,status
		 )
		 SELECT $1,b.id,$2,$5,$6,$7,$8,$9,$10,$11
		   FROM push_batches b
		  WHERE b.id=$3 AND b.idempotency_key=$4
		    AND b.tenant_id=$1 AND b.user_id=$2
		    AND b.schedule_id=$12 AND b.run_snapshot_id=$13
		 ON CONFLICT (batch_id,content_item_id)
		     WHERE content_item_id IS NOT NULL DO NOTHING
		 RETURNING id`,
		expected.TenantID, expected.UserID, d.BatchID, physicalKey,
		d.ContentItemID, invocationDigest, d.Score, d.BodyMD,
		card, d.FeishuMessageID, status, expected.TaskID,
		ref.SnapshotID,
	).Scan(&id)
	if err == nil {
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit compiled Tool delivery"); err != nil {
			return 0, false, false, err
		}
		return id, false, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false, taskRunDatabaseError(
			"insert compiled Tool delivery", err)
	}
	var storedStatus types.DeliveryStatus
	lookupErr := tx.QueryRow(ctx,
		`SELECT d.id,d.status
		   FROM deliveries d
		   JOIN push_batches b ON b.id=d.batch_id
		  WHERE d.batch_id=$1 AND d.content_item_id=$2
		    AND d.tenant_id=$3 AND d.user_id=$4
		    AND d.invocation_digest=$5
		    AND d.score IS NOT DISTINCT FROM $6
		    AND d.body_md=$7
		    AND b.idempotency_key=$8
		    AND b.schedule_id=$9 AND b.run_snapshot_id=$10`,
		d.BatchID, *d.ContentItemID,
		expected.TenantID, expected.UserID, invocationDigest,
		d.Score, d.BodyMD, physicalKey, expected.TaskID,
		ref.SnapshotID,
	).Scan(&id, &storedStatus)
	if lookupErr == nil {
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit compiled Tool delivery replay"); err != nil {
			return 0, false, false, err
		}
		return id, true,
			storedStatus == types.DeliveryStatusSent, nil
	}
	if !errors.Is(lookupErr, pgx.ErrNoRows) {
		return 0, false, false, taskRunDatabaseError(
			"load compiled Tool delivery replay", lookupErr)
	}
	return 0, false, false, taskRunNotFound()
}

func frozenToolBindingByDigestV2(
	snapshot runcontext.CompiledSnapshotV2,
	digest string,
) (runcontext.ToolBindingV1, bool) {
	for _, binding := range snapshot.ToolBindings {
		if binding.InvocationDigest == digest {
			return binding, true
		}
	}
	return runcontext.ToolBindingV1{}, false
}

// ClaimPushBatchDeliveryAuthorityForTaskRunV2 combines the exact live V2 run
// gate with the one-way durable authority election in one transaction.
func (s *Store) ClaimPushBatchDeliveryAuthorityForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	batchID int64,
) (types.PushBatchDeliveryAuthority, error) {
	if batchID <= 0 {
		return "", taskRunValidationError(
			"compiled Tool push authority input is invalid")
	}
	tx, _, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return "", err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return "", taskRunDatabaseError(
			"lock compiled Tool push authority schema", err)
	}
	var lockedBatchID int64
	if err := tx.QueryRow(ctx,
		`SELECT id FROM push_batches
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND schedule_id=$4 AND run_snapshot_id=$5
		  FOR UPDATE`,
		batchID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	).Scan(&lockedBatchID); errors.Is(err, pgx.ErrNoRows) {
		return "", taskRunNotFound()
	} else if err != nil {
		return "", taskRunDatabaseError(
			"lock compiled Tool push authority batch", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_push_batch_authority`); err != nil {
		return "", taskRunDatabaseError(
			"enter compiled Tool push authority role", err)
	}
	var winner types.PushBatchDeliveryAuthority
	err = tx.QueryRow(ctx,
		`UPDATE push_batches
		    SET delivery_authority=COALESCE(delivery_authority,$4)
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  RETURNING delivery_authority`,
		batchID, expected.TenantID, expected.UserID,
		types.PushBatchDeliveryAuthorityEffect,
	).Scan(&winner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", taskRunNotFound()
	}
	if err != nil {
		return "", taskRunDatabaseError(
			"claim compiled Tool push authority", err)
	}
	if !winner.Valid() {
		return "", taskRunIntegrityError()
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit compiled Tool push authority"); err != nil {
		return "", err
	}
	return winner, nil
}

// CreatePushEffectForTaskRunV2 freezes a provider request in the same
// transaction that share-locks the live task/tenant/member authorization.
func (s *Store) CreatePushEffectForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	prepared pusheffect.Prepared,
) (*pusheffect.Effect, error) {
	if prepared.TenantID != expected.TenantID ||
		prepared.UserID != expected.UserID ||
		prepared.TaskID != expected.TaskID ||
		prepared.RunSnapshotID != ref.SnapshotID ||
		prepared.RunID != expected.TemporalRunID {
		return nil, taskRunValidationError(
			"compiled Tool push effect scope is invalid")
	}
	canonical, err := pusheffect.Canonicalize(prepared)
	if err != nil {
		return nil, taskRunValidationError(
			"compiled Tool push effect payload is invalid")
	}
	tx, _, err := s.beginAuthorizedCompiledToolRunWriteV2(
		ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_push_effect_coordinator`); err != nil {
		return nil, taskRunDatabaseError(
			"enter compiled Tool push effect coordinator role", err)
	}
	batchStatus, err := lockPushEffectBatchAdmission(
		ctx, tx,
		types.PushBatchScope{
			TenantID: prepared.TenantID,
			UserID:   prepared.UserID,
			BatchID:  prepared.BatchID,
		},
		prepared.RunSnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil {
		return nil, err
	}
	existing, err := loadPushEffectForUpdate(
		ctx, tx, prepared.Scope())
	if err == nil {
		if err := validatePushEffectReplay(existing, canonical); err != nil {
			return nil, err
		}
		if err := validateStoredPushEffect(existing); err != nil {
			return nil, err
		}
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit compiled Tool push effect replay"); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if !errors.Is(err, types.ErrNotFound) {
		return nil, err
	}
	if batchStatus != types.BatchStatusPending {
		return nil, types.NewAppError(types.CodeConflict,
			"compiled Tool push effect batch is not pending", nil)
	}
	if err := verifyPushEffectAggregate(ctx, tx, prepared); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO push_effects (
			id,tenant_id,user_id,task_id,run_snapshot_id,run_id,step_id,
			chunk_index,chunk_count,batch_id,delivery_ids,provider,
			app_identity,provider_chat_id,target,card_payload,card_digest,
			provider_uuid,idempotency_expires_at,schema_version,
			canonical_payload,payload_digest
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17,$18::uuid,$19,$20,$21,$22
		) ON CONFLICT (id) DO NOTHING`,
		prepared.ID, prepared.TenantID, prepared.UserID, prepared.TaskID,
		prepared.RunSnapshotID, prepared.RunID, prepared.StepID,
		prepared.ChunkIndex, prepared.ChunkCount, prepared.BatchID,
		prepared.DeliveryIDs, prepared.Provider, prepared.AppIdentity,
		prepared.ProviderChatID, prepared.Target, prepared.Card,
		canonical.CardDigest(), prepared.ProviderUUID,
		prepared.IdempotencyExpiresAt, pusheffect.SchemaVersion,
		canonical.Payload(), canonical.Digest(),
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, types.NewAppError(types.CodeConflict,
				"compiled Tool push effect identity is already bound", nil)
		}
		return nil, taskRunDatabaseError(
			"insert compiled Tool push effect", err)
	}
	effect, err := loadPushEffectForUpdate(
		ctx, tx, prepared.Scope())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		if err := validatePushEffectReplay(effect, canonical); err != nil {
			return nil, err
		}
	}
	if err := validateStoredPushEffect(effect); err != nil {
		return nil, err
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit compiled Tool push effect"); err != nil {
		return nil, err
	}
	return effect, nil
}

// ClaimPushEffectForTaskRunV2 uses the coordinator's existing atomic
// authorization+lease transition after pinning the immutable effect to ref.
func (s *Store) ClaimPushEffectForTaskRunV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRefV2,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	if ref.ValidateFor(expected) != nil {
		return nil, taskRunValidationError(
			"compiled Tool push effect reference is invalid")
	}
	effect, err := s.LoadPushEffect(ctx, params.Scope)
	if err != nil {
		return nil, err
	}
	if effect.RunSnapshotID != ref.SnapshotID ||
		effect.TenantID != expected.TenantID ||
		effect.UserID != expected.UserID ||
		effect.TaskID != expected.TaskID ||
		effect.RunID != expected.TemporalRunID {
		return nil, taskRunValidationError(
			"compiled Tool push effect differs from the exact run")
	}
	claimed, decision, err := s.claimAuthorizedPushEffect(
		ctx, pusheffect.AuthorizedClaimParams{
			ClaimParams:      params,
			ExpectedTaskID:   expected.TaskID,
			DenialRetryAfter: 30 * time.Second,
		}, false)
	if err != nil {
		return nil, err
	}
	switch decision {
	case pusheffect.AuthorizedClaimed:
		return claimed, nil
	case pusheffect.AuthorizedClaimDenied:
		return nil, taskRunNotFound()
	default:
		return nil, types.NewAppError(
			types.CodeConflict,
			"compiled Tool push effect is not claimable", nil)
	}
}
