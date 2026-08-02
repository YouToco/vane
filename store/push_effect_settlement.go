package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

// ListPushEffectsForBatch loads the immutable effect plan for one exact batch.
// It is a normal Activity retry primitive, not a cross-tenant recovery scan.
func (s *Store) ListPushEffectsForBatch(
	ctx context.Context,
	scope types.PushBatchScope,
	runSnapshotID int64,
) ([]*pusheffect.Effect, error) {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.BatchID <= 0 ||
		runSnapshotID <= 0 {
		return nil, pushEffectValidation(
			"push effect batch plan scope is invalid")
	}
	tx, err := s.beginPushEffectCoordinatorTx(ctx, scope.TenantID)
	if err != nil {
		return nil, pushEffectDatabaseError(
			"begin push effect batch plan load", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	if _, err := lockPushEffectBatchAdmission(
		ctx,
		tx,
		scope,
		runSnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT `+pushEffectColumns+`
		  FROM push_effects
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		 ORDER BY chunk_index,id
		 FOR UPDATE`,
		scope.TenantID, scope.UserID, scope.BatchID,
	)
	if err != nil {
		return nil, pushEffectDatabaseError(
			"load push effect batch plan", err)
	}
	defer rows.Close()
	effects := make([]*pusheffect.Effect, 0)
	for rows.Next() {
		effect, err := scanPushEffect(rows)
		if err != nil {
			return nil, pushEffectScanError(
				"scan push effect batch plan", err)
		}
		if err := validateStoredPushEffect(effect); err != nil {
			return nil, err
		}
		if effect.BatchID != scope.BatchID ||
			effect.RunSnapshotID != runSnapshotID {
			return nil, pushEffectIntegrity()
		}
		effects = append(effects, effect)
	}
	if err := rows.Err(); err != nil {
		return nil, pushEffectDatabaseError(
			"iterate push effect batch plan", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, pushEffectDatabaseError(
			"commit push effect batch plan load", err)
	}
	return effects, nil
}

// CompleteEmptyPushEffectBatch closes an effect-authority batch only when no
// delivery or effect was created. It is the duplicate-only observation exit.
func (s *Store) CompleteEmptyPushEffectBatch(
	ctx context.Context,
	scope types.PushBatchScope,
	runSnapshotID int64,
) error {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.BatchID <= 0 ||
		runSnapshotID <= 0 {
		return pushEffectValidation(
			"empty push effect batch scope is invalid")
	}
	tx, err := s.beginPushEffectReceiptTx(ctx, scope.TenantID)
	if err != nil {
		return pushEffectDatabaseError(
			"begin empty push effect batch receipt", err)
	}
	defer rollbackPushEffectTx(ctx, tx)
	var canonicalCapability bool
	if err := tx.QueryRow(ctx, `
		SELECT to_regprocedure(
		    'public.complete_canonical_empty_push_batch_v1(bigint,bigint,bigint,bigint)'
		) IS NOT NULL`,
	).Scan(&canonicalCapability); err != nil {
		return pushEffectDatabaseError(
			"check canonical empty receipt capability", err)
	}
	if canonicalCapability {
		var decision string
		if err := tx.QueryRow(ctx, `
			SELECT public.complete_canonical_empty_push_batch_v1(
			    $1,$2,$3,$4
			)`,
			scope.TenantID, scope.UserID, scope.BatchID, runSnapshotID,
		).Scan(&decision); err != nil {
			return pushEffectDatabaseError(
				"complete canonical empty push batch", err)
		}
		switch decision {
		case "done":
			if err := tx.Commit(ctx); err != nil {
				return pushEffectDatabaseError(
					"commit canonical empty push batch", err)
			}
			return nil
		case "legacy":
			// Continue through the original open-batch settlement.
		case "denied":
			return pushEffectConflict(
				"canonical empty push batch receipt differs")
		default:
			return pushEffectIntegrity()
		}
	}
	status, err := lockPushEffectBatchAdmission(
		ctx,
		tx,
		scope,
		runSnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil {
		return err
	}
	var effectCount, deliveryCount int
	if err := tx.QueryRow(ctx, `
		SELECT
		    (SELECT count(id) FROM push_effects
		      WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3),
		    (SELECT count(id) FROM deliveries
		      WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3)`,
		scope.TenantID,
		scope.UserID,
		scope.BatchID,
	).Scan(&effectCount, &deliveryCount); err != nil {
		return pushEffectDatabaseError(
			"inspect empty push effect batch", err)
	}
	if effectCount != 0 || deliveryCount != 0 {
		return pushEffectConflict(
			"empty push effect batch aggregate differs")
	}
	if status != types.BatchStatusPending &&
		status != types.BatchStatusDone {
		return pushEffectConflict(
			"empty push effect batch status differs")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE push_batches
		   SET status=$5
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND run_snapshot_id=$4
		   AND delivery_authority=$6
		   AND brief_state='open'
		   AND status IN ($5,$7)`,
		scope.BatchID,
		scope.TenantID,
		scope.UserID,
		runSnapshotID,
		types.BatchStatusDone,
		types.PushBatchDeliveryAuthorityEffect,
		types.BatchStatusPending,
	)
	if err != nil {
		return pushEffectDatabaseError(
			"complete empty push effect batch", err)
	}
	if tag.RowsAffected() != 1 {
		return pushEffectConflict(
			"empty push effect batch completion differs")
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError(
			"commit empty push effect batch receipt", err)
	}
	return nil
}

// RecordPushEffectSentWithDeliveries atomically closes one provider effect and
// every delivery and observed event frozen into it. The same transaction
// closes the batch only after the complete immutable aggregate is terminal.
func (s *Store) RecordPushEffectSentWithDeliveries(
	ctx context.Context,
	receipt pusheffect.SentReceipt,
) error {
	if err := validatePushEffectSentReceipt(receipt); err != nil {
		return err
	}
	tx, err := s.beginPushEffectReceiptTx(ctx, receipt.TenantID)
	if err != nil {
		return pushEffectDatabaseError(
			"begin sent delivery receipt transaction", err)
	}
	defer rollbackPushEffectTx(ctx, tx)

	var (
		admissionBatchID       int64
		admissionPayload       []byte
		admissionPayloadDigest string
	)
	err = tx.QueryRow(ctx, `
		SELECT batch_id,canonical_payload,payload_digest
		  FROM push_effects
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		receipt.ID,
		receipt.TenantID,
		receipt.UserID,
	).Scan(
		&admissionBatchID,
		&admissionPayload,
		&admissionPayloadDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectNotFound()
	}
	if err != nil {
		return pushEffectDatabaseError(
			"load sent receipt batch admission", err)
	}
	admissionCanonical, err := pusheffect.Decode(
		admissionPayload,
		admissionPayloadDigest,
	)
	if err != nil {
		return pushEffectIntegrity()
	}
	admissionPrepared := admissionCanonical.Prepared()
	if admissionPrepared.ID != receipt.ID ||
		admissionPrepared.TenantID != receipt.TenantID ||
		admissionPrepared.UserID != receipt.UserID ||
		admissionPrepared.BatchID != admissionBatchID {
		return pushEffectIntegrity()
	}
	batchStatus, err := lockPushEffectBatchAdmission(
		ctx,
		tx,
		types.PushBatchScope{
			TenantID: receipt.TenantID,
			UserID:   receipt.UserID,
			BatchID:  admissionBatchID,
		},
		admissionPrepared.RunSnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil {
		return err
	}
	if err := lockPushEffectBatchRows(
		ctx,
		tx,
		receipt.TenantID,
		receipt.UserID,
		admissionBatchID,
	); err != nil {
		return err
	}

	var (
		status                        pusheffect.Status
		leaseOwner, providerMessageID string
		fence, batchID                int64
		deliveryIDs                   []int64
		cardPayload, canonicalPayload []byte
		payloadDigest                 string
		sentAt                        *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT status, lease_owner, fence, provider_message_id, sent_at,
		       batch_id, delivery_ids, card_payload,
		       canonical_payload, payload_digest
		  FROM push_effects
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		 FOR UPDATE`,
		receipt.ID, receipt.TenantID, receipt.UserID,
	).Scan(
		&status, &leaseOwner, &fence, &providerMessageID, &sentAt,
		&batchID, &deliveryIDs, &cardPayload,
		&canonicalPayload, &payloadDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectNotFound()
	}
	if err != nil {
		return pushEffectDatabaseError(
			"load sent delivery receipt target", err)
	}
	if len(deliveryIDs) == 0 || len(cardPayload) == 0 {
		return pushEffectConflict(
			"sent delivery receipt checkpoint is incomplete")
	}
	canonical, err := pusheffect.Decode(canonicalPayload, payloadDigest)
	if err != nil {
		return pushEffectIntegrity()
	}
	prepared := canonical.Prepared()
	if prepared.ID != receipt.ID ||
		prepared.TenantID != receipt.TenantID ||
		prepared.UserID != receipt.UserID ||
		prepared.BatchID != batchID ||
		!slices.Equal(prepared.DeliveryIDs, deliveryIDs) ||
		!bytes.Equal(prepared.Card, cardPayload) {
		return pushEffectIntegrity()
	}
	if !slices.Equal(
		receipt.ObservationEventKeys,
		prepared.ObservationEventKeys,
	) {
		return pushEffectConflict("sent observation receipt differs")
	}

	deliveriesMatch, err := lockAndValidatePushEffectDeliveries(
		ctx, tx, receipt, batchID, deliveryIDs, cardPayload,
	)
	if err != nil {
		return err
	}
	if _, err := lockAndValidatePushEffectObservedEvents(
		ctx, tx, prepared,
	); err != nil {
		return err
	}
	if status == pusheffect.StatusSent {
		if fence != receipt.ExpectedFence ||
			providerMessageID != receipt.ProviderMessageID ||
			sentAt == nil || !deliveriesMatch {
			return pushEffectConflict("sent delivery receipt differs")
		}
		if err := markPushEffectObservedEventsDelivered(
			ctx, tx, prepared,
		); err != nil {
			return err
		}
		if err := settlePushEffectBatchReceipt(
			ctx, tx, prepared, batchStatus, false,
		); err != nil {
			return err
		}
		if err := settleResearchBriefDeliveryReceiptV3(
			ctx, tx, prepared, receipt.ProviderMessageID,
		); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return pushEffectDatabaseError(
				"commit sent delivery receipt replay", err)
		}
		return nil
	}
	if fence != receipt.ExpectedFence ||
		(status == pusheffect.StatusSending &&
			leaseOwner != receipt.LeaseOwner) ||
		(status == pusheffect.StatusAmbiguous &&
			receipt.LeaseOwner != "") ||
		(status != pusheffect.StatusSending &&
			status != pusheffect.StatusAmbiguous) {
		return pushEffectLeaseLost()
	}

	tag, err := tx.Exec(ctx, `
		UPDATE deliveries
		   SET feishu_message_id=$5, card_json=$6::jsonb,
		       status=$7, sent_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		   AND id=ANY($4)
		   AND (
		       status=$8 OR (
		           status=$7 AND feishu_message_id=$5
		           AND card_json=$6::jsonb AND sent_at IS NOT NULL
		       )
		   )`,
		receipt.TenantID, receipt.UserID, batchID, deliveryIDs,
		receipt.ProviderMessageID, cardPayload,
		types.DeliveryStatusSent, types.DeliveryStatusPending,
	)
	if err != nil {
		return pushEffectDatabaseError("write delivery receipts", err)
	}
	if tag.RowsAffected() != int64(len(deliveryIDs)) {
		return pushEffectConflict("delivery receipt aggregate differs")
	}
	if err := markPushEffectObservedEventsDelivered(
		ctx, tx, prepared,
	); err != nil {
		return err
	}

	tag, err = tx.Exec(ctx, `
		UPDATE push_effects
		   SET status='sent', lease_owner='', lease_until=NULL,
		       takeover_not_before=NULL, provider_message_id=$5,
		       failure_class='', ambiguous_since=NULL,
		       sent_at=clock_timestamp(), updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND fence=$4
		   AND status IN ('sending','ambiguous')`,
		receipt.ID, receipt.TenantID, receipt.UserID,
		receipt.ExpectedFence, receipt.ProviderMessageID,
	)
	if err != nil {
		return pushEffectDatabaseError("write sent effect receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return pushEffectLeaseLost()
	}
	if err := settlePushEffectBatchReceipt(
		ctx, tx, prepared, batchStatus, false,
	); err != nil {
		return err
	}
	if err := settleResearchBriefDeliveryReceiptV3(
		ctx, tx, prepared, receipt.ProviderMessageID,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError(
			"commit sent delivery receipt transaction", err)
	}
	return nil
}

// settleResearchBriefDeliveryReceiptV3 is an optional extension of the
// existing effect settlement transaction. Legacy/compiled effects have no V3
// anchor and are left byte-for-byte unchanged; a V3 effect seals its receipt
// in the same commit as effect, delivery, and batch terminal state.
func settleResearchBriefDeliveryReceiptV3(
	ctx context.Context, tx pgx.Tx, prepared pusheffect.Prepared,
	providerMessageID string,
) error {
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.user_id',$1,true)`,
		fmt.Sprint(prepared.UserID)); err != nil {
		return pushEffectDatabaseError(
			"set research delivery receipt user scope", err)
	}
	var anchorTableAvailable bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass('public.research_brief_deliveries') IS NOT NULL`,
	).Scan(&anchorTableAvailable); err != nil {
		return pushEffectDatabaseError(
			"check research delivery receipt schema", err)
	}
	if !anchorTableAvailable {
		if prepared.StepID == "research-brief-delivery/v3" {
			return pushEffectIntegrity()
		}
		return nil
	}
	var (
		id, batchID, deliveryID        int64
		status, effectID, cardDigest   string
		storedMessageID, receiptDigest string
		sentAt                         *time.Time
	)
	err := tx.QueryRow(ctx, `
		SELECT id,batch_id,delivery_id,status,effect_id,card_digest,
		       provider_message_id,receipt_digest,sent_at
		  FROM research_brief_deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND effect_id=$3
		 FOR UPDATE`, prepared.TenantID, prepared.UserID, prepared.ID).Scan(
		&id, &batchID, &deliveryID, &status, &effectID, &cardDigest,
		&storedMessageID, &receiptDigest, &sentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if prepared.StepID == "research-brief-delivery/v3" {
			return pushEffectIntegrity()
		}
		return nil
	}
	if err != nil {
		return pushEffectDatabaseError("lock research delivery receipt", err)
	}
	cardSum := sha256.Sum256(prepared.Card)
	expectedCardDigest := hex.EncodeToString(cardSum[:])
	if id <= 0 || batchID != prepared.BatchID ||
		len(prepared.DeliveryIDs) != 1 || deliveryID != prepared.DeliveryIDs[0] ||
		effectID != prepared.ID || cardDigest != expectedCardDigest {
		return pushEffectConflict("research delivery receipt aggregate differs")
	}
	if status == "sent" {
		if storedMessageID != providerMessageID || sentAt == nil ||
			!validResearchRunDigest(receiptDigest) {
			return pushEffectConflict("research delivery sent receipt differs")
		}
		return nil
	}
	if status != "prepared" || storedMessageID != "" || receiptDigest != "" || sentAt != nil {
		return pushEffectConflict("research delivery receipt state differs")
	}
	var sealedDigest string
	err = tx.QueryRow(ctx, `
		UPDATE research_brief_deliveries
		   SET status='sent',provider_message_id=$4::text,
		       receipt_digest=encode(sha256(convert_to(concat_ws(E'\n',
		           'vane.research-brief-delivery-receipt/v3',id::text,
		           brief_id::text,effect_id,brief_reference_digest,brief_digest,
		           card_digest,$4::text,'sent'
		       ),'UTF8')),'hex')
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND status='prepared'
		 RETURNING receipt_digest`, id, prepared.TenantID, prepared.UserID,
		providerMessageID).Scan(&sealedDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectConflict("research delivery receipt lost its fence")
	}
	if err != nil {
		return pushEffectDatabaseError("seal research delivery receipt", err)
	}
	if !validResearchRunDigest(sealedDigest) {
		return pushEffectIntegrity()
	}
	return nil
}

func lockPushEffectBatchRows(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID, batchID int64,
) error {
	rows, err := tx.Query(ctx, `
		SELECT id
		  FROM push_effects
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		 ORDER BY chunk_index,id
		 FOR UPDATE`,
		tenantID, userID, batchID,
	)
	if err != nil {
		return pushEffectDatabaseError("lock push effect batch rows", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return pushEffectDatabaseError(
				"scan push effect batch row", err)
		}
	}
	if err := rows.Err(); err != nil {
		return pushEffectDatabaseError(
			"iterate push effect batch rows", err)
	}
	return nil
}

func lockAndValidatePushEffectDeliveries(
	ctx context.Context,
	tx pgx.Tx,
	receipt pusheffect.SentReceipt,
	batchID int64,
	deliveryIDs []int64,
	cardPayload []byte,
) (bool, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, status, feishu_message_id,
		       card_json=$5::jsonb, sent_at
		  FROM deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3 AND id=ANY($4)
		 ORDER BY id
		 FOR UPDATE`,
		receipt.TenantID, receipt.UserID, batchID, deliveryIDs, cardPayload,
	)
	if err != nil {
		return false, pushEffectDatabaseError(
			"lock delivery receipt aggregate", err)
	}
	defer rows.Close()

	seen := make([]int64, 0, len(deliveryIDs))
	allSentAndMatching := true
	for rows.Next() {
		var (
			statusMessageID string
			deliveryID      int64
			status          types.DeliveryStatus
			cardMatches     bool
			deliverySentAt  *time.Time
		)
		if err := rows.Scan(
			&deliveryID, &status, &statusMessageID,
			&cardMatches, &deliverySentAt,
		); err != nil {
			return false, pushEffectDatabaseError(
				"scan delivery receipt aggregate", err)
		}
		seen = append(seen, deliveryID)
		if status == types.DeliveryStatusPending {
			allSentAndMatching = false
			continue
		}
		if status != types.DeliveryStatusSent ||
			statusMessageID != receipt.ProviderMessageID ||
			deliverySentAt == nil || !cardMatches {
			return false, pushEffectConflict(
				"delivery receipt aggregate differs")
		}
	}
	if err := rows.Err(); err != nil {
		return false, pushEffectDatabaseError(
			"iterate delivery receipt aggregate", err)
	}
	if !slices.Equal(seen, deliveryIDs) {
		return false, pushEffectConflict(
			"delivery receipt aggregate is incomplete")
	}
	return allSentAndMatching, nil
}

func lockAndValidatePushEffectObservedEvents(
	ctx context.Context,
	tx pgx.Tx,
	prepared pusheffect.Prepared,
) (bool, error) {
	rows, err := tx.Query(ctx, `
		SELECT tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id,
		       delivery_id,event_key,status,delivered_at
		  FROM task_observed_events
		 WHERE tenant_id=$1 AND user_id=$2
		   AND delivery_id=ANY($3::bigint[])
		 ORDER BY delivery_id,event_key
		 FOR UPDATE`,
		prepared.TenantID,
		prepared.UserID,
		prepared.DeliveryIDs,
	)
	if err != nil {
		return false, pushEffectDatabaseError(
			"lock observed event receipt aggregate", err)
	}
	defer rows.Close()

	expected := make(map[int64]string, len(prepared.ObservationEventKeys))
	for i, eventKey := range prepared.ObservationEventKeys {
		if eventKey != "" {
			expected[prepared.DeliveryIDs[i]] = eventKey
		}
	}
	seen := make(map[int64]struct{}, len(expected))
	allDelivered := true
	for rows.Next() {
		var (
			tenantID, userID, snapshotID, deliveryID int64
			taskID, runID, eventKey, status          string
			deliveredAt                              *time.Time
		)
		if err := rows.Scan(
			&tenantID,
			&userID,
			&taskID,
			&snapshotID,
			&runID,
			&deliveryID,
			&eventKey,
			&status,
			&deliveredAt,
		); err != nil {
			return false, pushEffectDatabaseError(
				"scan observed event receipt aggregate", err)
		}
		expectedKey, ok := expected[deliveryID]
		_, duplicate := seen[deliveryID]
		if !ok || duplicate ||
			tenantID != prepared.TenantID ||
			userID != prepared.UserID ||
			taskID != prepared.TaskID ||
			snapshotID != prepared.RunSnapshotID ||
			runID != prepared.RunID ||
			eventKey != expectedKey ||
			(status != "qualified" && status != "delivered") ||
			(status == "qualified" && deliveredAt != nil) ||
			(status == "delivered" && deliveredAt == nil) {
			return false, pushEffectConflict(
				"observed event receipt aggregate differs")
		}
		allDelivered = allDelivered && status == "delivered"
		seen[deliveryID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return false, pushEffectDatabaseError(
			"iterate observed event receipt aggregate", err)
	}
	if len(seen) != len(expected) {
		return false, pushEffectConflict(
			"observed event receipt aggregate is incomplete")
	}
	return allDelivered, nil
}

func markPushEffectObservedEventsDelivered(
	ctx context.Context,
	tx pgx.Tx,
	prepared pusheffect.Prepared,
) error {
	for i, eventKey := range prepared.ObservationEventKeys {
		if eventKey == "" {
			continue
		}
		deliveryID := prepared.DeliveryIDs[i]
		tag, err := tx.Exec(ctx, `
			UPDATE task_observed_events e
			   SET status='delivered',
			       delivered_at=COALESCE(e.delivered_at,d.sent_at)
			  FROM deliveries d
			 WHERE e.tenant_id=$1 AND e.user_id=$2 AND e.task_id=$3
			   AND e.run_snapshot_id=$4 AND e.temporal_run_id=$5
			   AND e.delivery_id=$6 AND e.event_key=$7
			   AND e.status IN ('qualified','delivered')
			   AND d.id=e.delivery_id
			   AND d.tenant_id=e.tenant_id
			   AND d.user_id=e.user_id
			   AND d.status=$8
			   AND d.sent_at IS NOT NULL`,
			prepared.TenantID,
			prepared.UserID,
			prepared.TaskID,
			prepared.RunSnapshotID,
			prepared.RunID,
			deliveryID,
			eventKey,
			types.DeliveryStatusSent,
		)
		if err != nil {
			return pushEffectDatabaseError(
				"write observed event receipt", err)
		}
		if tag.RowsAffected() != 1 {
			return pushEffectConflict(
				"observed event delivery receipt differs")
		}
	}
	return nil
}

func settlePushEffectBatchReceipt(
	ctx context.Context,
	tx pgx.Tx,
	current pusheffect.Prepared,
	batchStatus types.BatchStatus,
	requireComplete bool,
) error {
	rows, err := tx.Query(ctx, `
		SELECT id,status,chunk_index,chunk_count,delivery_ids,
		       canonical_payload,payload_digest
		  FROM push_effects
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		 ORDER BY chunk_index,id
		 FOR UPDATE`,
		current.TenantID,
		current.UserID,
		current.BatchID,
	)
	if err != nil {
		return pushEffectDatabaseError(
			"lock push effect batch aggregate", err)
	}
	defer rows.Close()

	type batchEffect struct {
		prepared pusheffect.Prepared
		status   pusheffect.Status
	}
	effects := make([]batchEffect, 0, current.ChunkCount)
	deliveryIDs := make([]int64, 0)
	terminal := true
	for rows.Next() {
		var (
			id, payloadDigest string
			status            pusheffect.Status
			chunkIndex        int
			chunkCount        int
			storedDeliveryIDs []int64
			canonicalPayload  []byte
		)
		if err := rows.Scan(
			&id,
			&status,
			&chunkIndex,
			&chunkCount,
			&storedDeliveryIDs,
			&canonicalPayload,
			&payloadDigest,
		); err != nil {
			return pushEffectDatabaseError(
				"scan push effect batch aggregate", err)
		}
		canonical, err := pusheffect.Decode(
			canonicalPayload,
			payloadDigest,
		)
		if err != nil {
			return pushEffectIntegrity()
		}
		prepared := canonical.Prepared()
		if prepared.ID != id ||
			prepared.TenantID != current.TenantID ||
			prepared.UserID != current.UserID ||
			prepared.BatchID != current.BatchID ||
			prepared.RunSnapshotID != current.RunSnapshotID ||
			prepared.RunID != current.RunID ||
			prepared.ChunkIndex != chunkIndex ||
			prepared.ChunkCount != chunkCount ||
			!slices.Equal(
				prepared.DeliveryIDs,
				storedDeliveryIDs,
			) {
			return pushEffectIntegrity()
		}
		effects = append(effects, batchEffect{
			prepared: prepared,
			status:   status,
		})
		deliveryIDs = append(deliveryIDs, prepared.DeliveryIDs...)
	}
	if err := rows.Err(); err != nil {
		return pushEffectDatabaseError(
			"iterate push effect batch aggregate", err)
	}
	rows.Close()
	if len(effects) != current.ChunkCount {
		return pushEffectConflict(
			"push effect batch plan is incomplete")
	}
	for index, effect := range effects {
		if effect.prepared.ChunkCount != current.ChunkCount ||
			effect.prepared.ChunkIndex != index {
			return pushEffectConflict(
				"push effect batch plan differs")
		}
		if effect.status != pusheffect.StatusSent {
			terminal = false
		}
	}
	slices.Sort(deliveryIDs)
	for index := 1; index < len(deliveryIDs); index++ {
		if deliveryIDs[index] == deliveryIDs[index-1] {
			return pushEffectConflict(
				"push effect batch delivery plan overlaps")
		}
	}

	deliveryRows, err := tx.Query(ctx, `
		SELECT id,status
		  FROM deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		 ORDER BY id
		 FOR UPDATE`,
		current.TenantID,
		current.UserID,
		current.BatchID,
	)
	if err != nil {
		return pushEffectDatabaseError(
			"lock push effect batch deliveries", err)
	}
	defer deliveryRows.Close()
	storedDeliveryIDs := make([]int64, 0, len(deliveryIDs))
	for deliveryRows.Next() {
		var id int64
		var status types.DeliveryStatus
		if err := deliveryRows.Scan(&id, &status); err != nil {
			return pushEffectDatabaseError(
				"scan push effect batch delivery", err)
		}
		storedDeliveryIDs = append(storedDeliveryIDs, id)
		switch status {
		case types.DeliveryStatusSent:
		case types.DeliveryStatusPending:
			terminal = false
		default:
			return pushEffectConflict(
				"push effect batch delivery status differs")
		}
	}
	if err := deliveryRows.Err(); err != nil {
		return pushEffectDatabaseError(
			"iterate push effect batch deliveries", err)
	}
	deliveryRows.Close()
	if !slices.Equal(deliveryIDs, storedDeliveryIDs) {
		return pushEffectConflict(
			"push effect batch delivery aggregate differs")
	}
	for _, effect := range effects {
		allDelivered, err := lockAndValidatePushEffectObservedEvents(
			ctx, tx, effect.prepared)
		if err != nil {
			return err
		}
		if !allDelivered {
			terminal = false
		}
	}
	if !terminal {
		if batchStatus == types.BatchStatusDone {
			return pushEffectConflict(
				"completed push effect batch aggregate differs")
		}
		if batchStatus == types.BatchStatusFailed {
			tag, err := tx.Exec(ctx, `
				UPDATE push_batches
				   SET status=$5
				 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
				   AND run_snapshot_id=$4
				   AND delivery_authority=$6
				   AND status=$7`,
				current.BatchID,
				current.TenantID,
				current.UserID,
				current.RunSnapshotID,
				types.BatchStatusPending,
				types.PushBatchDeliveryAuthorityEffect,
				types.BatchStatusFailed,
			)
			if err != nil {
				return pushEffectDatabaseError(
					"reopen incomplete push effect batch", err)
			}
			if tag.RowsAffected() != 1 {
				return pushEffectConflict(
					"incomplete push effect batch status differs")
			}
		}
		if requireComplete {
			return pushEffectConflict(
				"push effect batch aggregate is incomplete")
		}
		return nil
	}

	tag, err := tx.Exec(ctx, `
		UPDATE push_batches
		   SET status=$5
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND run_snapshot_id=$4
		   AND delivery_authority=$6
		   AND status IN ($5,$7,$8)`,
		current.BatchID,
		current.TenantID,
		current.UserID,
		current.RunSnapshotID,
		types.BatchStatusDone,
		types.PushBatchDeliveryAuthorityEffect,
		types.BatchStatusPending,
		types.BatchStatusFailed,
	)
	if err != nil {
		return pushEffectDatabaseError(
			"complete push effect batch", err)
	}
	if tag.RowsAffected() != 1 {
		return pushEffectConflict(
			"push effect batch completion differs")
	}
	return nil
}

// SettlePushEffectBatchReceipt replays the exact effect-authority terminal
// projection. It never sends and requires the complete immutable aggregate.
func (s *Store) SettlePushEffectBatchReceipt(
	ctx context.Context,
	scope types.PushBatchScope,
	runSnapshotID int64,
) error {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.BatchID <= 0 ||
		runSnapshotID <= 0 {
		return pushEffectValidation(
			"push effect batch receipt scope is invalid")
	}
	tx, err := s.beginPushEffectReceiptTx(ctx, scope.TenantID)
	if err != nil {
		return pushEffectDatabaseError(
			"begin push effect batch receipt", err)
	}
	defer rollbackPushEffectTx(ctx, tx)

	batchStatus, err := lockPushEffectBatchAdmission(
		ctx,
		tx,
		scope,
		runSnapshotID,
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil {
		return err
	}
	var canonicalPayload []byte
	var payloadDigest string
	err = tx.QueryRow(ctx, `
		SELECT canonical_payload,payload_digest
		  FROM push_effects
		 WHERE tenant_id=$1 AND user_id=$2 AND batch_id=$3
		 ORDER BY chunk_index,id
		 LIMIT 1`,
		scope.TenantID,
		scope.UserID,
		scope.BatchID,
	).Scan(&canonicalPayload, &payloadDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushEffectConflict(
			"push effect batch aggregate is missing")
	}
	if err != nil {
		return pushEffectDatabaseError(
			"load push effect batch receipt anchor", err)
	}
	canonical, err := pusheffect.Decode(canonicalPayload, payloadDigest)
	if err != nil {
		return pushEffectIntegrity()
	}
	prepared := canonical.Prepared()
	if prepared.TenantID != scope.TenantID ||
		prepared.UserID != scope.UserID ||
		prepared.BatchID != scope.BatchID ||
		prepared.RunSnapshotID != runSnapshotID {
		return pushEffectIntegrity()
	}
	if err := settlePushEffectBatchReceipt(
		ctx, tx, prepared, batchStatus, true,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return pushEffectDatabaseError(
			"commit push effect batch receipt", err)
	}
	return nil
}
