package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const compiledPushBatchKeyNamespaceV1 = "compiled-run/v1/"

// compiledPushBatchPhysicalKeyV1 maps the workflow's logical trace into the
// global physical idempotency domain retained for rollback compatibility. The
// mapping is injective for (snapshotID, logicalKey): Temporal retries in one
// immutable run reuse the same row, while a Reset with a new snapshot cannot
// conflict with or adopt the old run's batch.
func compiledPushBatchPhysicalKeyV1(snapshotID int64, logicalKey string) string {
	return compiledPushBatchKeyNamespaceV1 +
		strconv.FormatInt(snapshotID, 10) + "/" + logicalKey
}

// compiledPushBatchLogicalKeyV1 restores the public trace/idempotency value
// from the rollback-compatible physical key. run_snapshot_id is supplied from
// the same row, so an arbitrary look-alike prefix cannot be decoded under a
// different run.
func compiledPushBatchLogicalKeyV1(snapshotID int64, physicalKey string) (string, bool) {
	if snapshotID <= 0 {
		return "", false
	}
	prefix := compiledPushBatchPhysicalKeyV1(snapshotID, "")
	if len(physicalKey) <= len(prefix) || physicalKey[:len(prefix)] != prefix {
		return "", false
	}
	return physicalKey[len(prefix):], true
}

// beginAuthorizedCompiledRunWriteV1 opens the only transaction in which a
// compiled run may write tenant-owned pipeline state. The immutable snapshot
// is re-read through the pinned V1 reader (including payload/digest checks),
// then the exact task, tenant and membership rows are share-locked before the
// caller writes. A concurrent revoke therefore either wins before this gate or
// waits until the authorized write commits; it can never redirect the write to
// another membership of the same user.
func (s *Store) beginAuthorizedCompiledRunWriteV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (pgx.Tx, error) {
	tx, err := s.beginCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return nil, err
	}
	if err := lockLiveCompiledRunWriteV1(ctx, tx, expected); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, err
	}
	return tx, nil
}

// beginCompiledRunReceiptV1 deliberately omits the live-state lock. It is only
// for durable receipts after an external send has already succeeded: refusing
// that receipt because membership was revoked in the meantime would leave a
// sent card pending and make a retry duplicate it. The exact stored snapshot,
// tenant, user, task, batch and idempotency key are still checked by each
// receipt writer, so this exception cannot redirect data into another tenant.
func (s *Store) beginCompiledRunReceiptV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (pgx.Tx, error) {
	return s.beginCompiledRunWriteV1(ctx, expected, ref)
}

func (s *Store) beginCompiledRunWriteV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (pgx.Tx, error) {
	callerReference, err := validateTaskRunSnapshotReferenceForExpectedV1(ref, expected)
	if err != nil {
		return nil, taskRunValidationError("task run snapshot reference is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskRunDatabaseError("begin compiled task run write", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", expected.TenantID)); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, taskRunDatabaseError("set compiled task run tenant context", err)
	}
	// Production connects as the schema owner, which bypasses ordinary RLS.
	// Every compiled write explicitly enters the same restricted application
	// role exercised by RLS integration tests before reading any tenant row.
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, taskRunDatabaseError("enter compiled task run role", err)
	}

	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: expected.TenantID, UserID: expected.UserID, TaskID: expected.TaskID,
		TemporalWorkflowID: expected.TemporalWorkflowID,
		TemporalRunID:      expected.TemporalRunID,
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, tx, lookup)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, err
	}
	if !found {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, taskRunNotFound()
	}
	storedRef, err := snapshot.safeRef()
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, taskRunIntegrityError()
	}
	storedReference, err := validateTaskRunSnapshotReferenceForExpectedV1(storedRef, expected)
	if err != nil || storedReference != callerReference {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, taskRunIntegrityError()
	}
	return tx, nil
}

func lockLiveCompiledRunWriteV1(
	ctx context.Context,
	tx pgx.Tx,
	identity types.RunIdentity,
) error {
	var live bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM schedules s
		   JOIN tenants t ON t.id = s.tenant_id
		   JOIN memberships m
		     ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id
		  WHERE s.id = $1
		    AND s.tenant_id = $2
		    AND s.user_id = $3
		    AND s.status = $4
		    AND t.status = $5 AND t.deleted_at IS NULL
		    AND `+matureSchedulePredicate+`
		  FOR SHARE OF s, t, m`,
		identity.TaskID, identity.TenantID, identity.UserID,
		types.ScheduleStatusActive, types.TenantStatusActive,
	).Scan(&live)
	if errors.Is(err, pgx.ErrNoRows) || !live {
		return taskRunNotFound()
	}
	if err != nil {
		return taskRunDatabaseError("lock live compiled task run write", err)
	}
	return nil
}

func commitCompiledRunWriteV1(ctx context.Context, tx pgx.Tx, action string) error {
	if err := tx.Commit(ctx); err != nil {
		return taskRunDatabaseError(action, err)
	}
	return nil
}

// CreatePushBatchForTaskRunV1 creates or recovers the exact batch owned by one
// immutable run snapshot. Temporal Reset may replay the same trace in a new
// RunID; the snapshot-namespaced physical key prevents a global-index conflict
// and run_snapshot_id remains the independently checked ownership proof.
func (s *Store) CreatePushBatchForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
) (int64, error) {
	if idempotencyKey == "" {
		return 0, taskRunValidationError("compiled push batch idempotency key is empty")
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return 0, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return 0, taskRunDatabaseError(
			"lock compiled push batch schema admission", err)
	}

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO push_batches
		    (tenant_id, user_id, idempotency_key, schedule_id, run_snapshot_id)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (idempotency_key)
		     WHERE idempotency_key <> ''
		 DO UPDATE SET user_id = EXCLUDED.user_id, exit_gate = '', stage_counts = '{}'
		 WHERE push_batches.tenant_id = EXCLUDED.tenant_id
		   AND push_batches.user_id = EXCLUDED.user_id
		   AND push_batches.schedule_id IS NOT DISTINCT FROM EXCLUDED.schedule_id
		   AND push_batches.run_snapshot_id = EXCLUDED.run_snapshot_id
		 RETURNING id`,
		expected.TenantID, expected.UserID, physicalKey, expected.TaskID,
		ref.SnapshotID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, types.NewAppError(types.CodeConflict,
			"compiled push batch idempotency key belongs to another scope", nil)
	}
	if err != nil {
		return 0, taskRunDatabaseError("create compiled push batch", err)
	}
	if err := commitCompiledRunWriteV1(ctx, tx, "commit compiled push batch"); err != nil {
		return 0, err
	}
	return id, nil
}

// CreateOrRecoverPushBatchForTaskRunV1 is the compiled Activity entry point.
// A live run follows the normal authorized create/reuse path. If live access
// has already been revoked, the only permitted recovery is an exact existing
// batch whose deliveries are all durably sent. That state proves the external
// effect already happened and only the terminal batch receipt remains; no new
// delivery or external send is authorized by the recovery result.
func (s *Store) CreateOrRecoverPushBatchForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
) (id int64, recoveryOnly bool, err error) {
	id, err = s.CreatePushBatchForTaskRunV1(ctx, expected, ref, idempotencyKey)
	if err == nil {
		return id, false, nil
	}
	if !errors.Is(err, types.ErrNotFound) {
		return 0, false, err
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)

	tx, err := s.beginCompiledRunReceiptV1(ctx, expected, ref)
	if err != nil {
		return 0, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	err = tx.QueryRow(ctx,
		`SELECT b.id
		   FROM push_batches b
		  WHERE b.idempotency_key = $1
		    AND b.tenant_id = $2
		    AND b.user_id = $3
		    AND b.schedule_id = $4
		    AND b.run_snapshot_id = $5
		    AND b.status IN ($6, $7)
		    AND EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.batch_id = b.id
		           AND d.tenant_id = b.tenant_id
		           AND d.user_id = b.user_id
		           AND d.status = $8
		    )
		    AND NOT EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.batch_id = b.id
		           AND (d.tenant_id IS DISTINCT FROM b.tenant_id
		             OR d.user_id IS DISTINCT FROM b.user_id
		             OR d.status IS DISTINCT FROM $8)
		    )
		  FOR SHARE OF b`,
		physicalKey, expected.TenantID, expected.UserID, expected.TaskID,
		ref.SnapshotID, types.BatchStatusPending, types.BatchStatusDone,
		types.DeliveryStatusSent,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, taskRunNotFound()
	}
	if err != nil {
		return 0, false, taskRunDatabaseError("recover compiled push batch receipt", err)
	}
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit compiled push batch receipt recovery"); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// RecordEmptyPushBatchForTaskRunV1 records a terminal empty batch while the
// exact compiled task is live. A real batch in the same immutable run is the
// existing replay guard (skipped=true); another run may safely reuse the raw
// trace because its physical key is namespaced to run_snapshot_id.
func (s *Store) RecordEmptyPushBatchForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
	gate types.BatchExitGate,
	counts types.PipelineCounts,
) (id int64, skipped bool, err error) {
	if idempotencyKey == "" || gate == "" {
		return 0, false, taskRunValidationError(
			"compiled empty push batch requires an idempotency key and exit gate")
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)
	countsJSON, err := json.Marshal(counts)
	if err != nil {
		return 0, false, types.NewAppError(types.CodeInternal,
			"serialize compiled push batch stage counts", err)
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return 0, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return 0, false, taskRunDatabaseError(
			"lock compiled empty batch schema admission", err)
	}

	err = tx.QueryRow(ctx,
		`INSERT INTO push_batches
		    (tenant_id, user_id, status, exit_gate, stage_counts,
		     idempotency_key, schedule_id, run_snapshot_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (idempotency_key)
		     WHERE idempotency_key <> ''
		 DO UPDATE SET exit_gate = EXCLUDED.exit_gate,
		               stage_counts = EXCLUDED.stage_counts
		 WHERE push_batches.status = EXCLUDED.status
		   AND push_batches.tenant_id = EXCLUDED.tenant_id
		   AND push_batches.user_id = EXCLUDED.user_id
		   AND push_batches.schedule_id IS NOT DISTINCT FROM EXCLUDED.schedule_id
		   AND push_batches.run_snapshot_id = EXCLUDED.run_snapshot_id
		 RETURNING id`,
		expected.TenantID, expected.UserID, types.BatchStatusEmpty, gate,
		countsJSON, physicalKey, expected.TaskID, ref.SnapshotID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var tenantID, userID int64
		var scheduleID *string
		lookupErr := tx.QueryRow(ctx,
			`SELECT tenant_id, user_id, schedule_id
			   FROM push_batches
			  WHERE run_snapshot_id = $1 AND idempotency_key = $2`,
			ref.SnapshotID, physicalKey,
		).Scan(&tenantID, &userID, &scheduleID)
		if lookupErr != nil {
			return 0, false, taskRunDatabaseError(
				"inspect compiled empty push batch conflict", lookupErr)
		}
		if tenantID != expected.TenantID || userID != expected.UserID ||
			scheduleID == nil || *scheduleID != expected.TaskID {
			return 0, false, types.NewAppError(types.CodeConflict,
				"compiled empty push batch idempotency key belongs to another scope", nil)
		}
		if err := commitCompiledRunWriteV1(ctx, tx,
			"commit compiled empty push batch replay guard"); err != nil {
			return 0, false, err
		}
		return 0, true, nil
	}
	if err != nil {
		return 0, false, taskRunDatabaseError("record compiled empty push batch", err)
	}
	if err := commitCompiledRunWriteV1(ctx, tx, "commit compiled empty push batch"); err != nil {
		return 0, false, err
	}
	return id, false, nil
}

// InsertDeliveryForTaskRunV1 inserts or recovers a delivery only inside the
// exact compiled run batch. It never derives tenant scope from memberships.
func (s *Store) InsertDeliveryForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
	d *types.Delivery,
) (id int64, existed bool, sentAlready bool, err error) {
	if d == nil || d.BatchID <= 0 || d.UserID != expected.UserID || idempotencyKey == "" {
		return 0, false, false,
			taskRunValidationError("compiled delivery input is invalid")
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)
	card := d.CardJSON
	if len(card) == 0 {
		card = json.RawMessage("{}")
	}
	status := d.Status
	if status == "" {
		status = types.DeliveryStatusPending
	}
	tx, _, err := s.beginObservedEventAdmissionV1(
		ctx,
		expected,
		ref,
		[]types.PushBatchScope{{
			TenantID: expected.TenantID,
			UserID:   expected.UserID,
			BatchID:  d.BatchID,
		}},
		map[int64]int64{d.BatchID: ref.SnapshotID},
		false,
	)
	if err != nil {
		if errors.Is(err, types.ErrConflict) {
			return 0, false, false, taskRunNotFound()
		}
		return 0, false, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	effectBatches, err := lockObservedEventPushEffects(
		ctx,
		tx,
		expected.TenantID,
		expected.UserID,
		[]int64{d.BatchID},
	)
	if err != nil {
		return 0, false, false, err
	}
	if effectBatches[d.BatchID] {
		if d.ContentItemID == nil {
			return 0, false, false, observationConflict(
				"push effect delivery aggregate is frozen")
		}
		var storedStatus types.DeliveryStatus
		lookupErr := tx.QueryRow(ctx, `
			SELECT d.id,d.status
			  FROM deliveries d
			  JOIN push_batches b ON b.id=d.batch_id
			 WHERE d.batch_id=$1 AND d.content_item_id=$2
			   AND d.tenant_id=$3 AND d.user_id=$4
			   AND b.tenant_id=$3 AND b.user_id=$4
			   AND b.idempotency_key=$5 AND b.schedule_id=$6
			   AND b.run_snapshot_id=$7
			   AND d.score IS NOT DISTINCT FROM $8
			   AND d.body_md=$9
			 FOR UPDATE OF d`,
			d.BatchID,
			*d.ContentItemID,
			expected.TenantID,
			expected.UserID,
			physicalKey,
			expected.TaskID,
			ref.SnapshotID,
			d.Score,
			d.BodyMD,
		).Scan(&id, &storedStatus)
		if lookupErr == nil {
			if err := commitCompiledRunWriteV1(
				ctx, tx, "commit frozen compiled delivery replay",
			); err != nil {
				return 0, false, false, err
			}
			return id, true,
				storedStatus == types.DeliveryStatusSent, nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return 0, false, false, taskRunDatabaseError(
				"load frozen compiled delivery replay", lookupErr)
		}
		return 0, false, false, observationConflict(
			"push effect delivery aggregate is frozen")
	}

	err = tx.QueryRow(ctx,
		`INSERT INTO deliveries (
		    tenant_id, batch_id, user_id, content_item_id, score, body_md,
		    card_json, feishu_message_id, status
		 )
		 SELECT $1, b.id, $2, $5, $6, $7, $8, $9, $10
		   FROM push_batches b
		  WHERE b.id = $3
		    AND b.idempotency_key = $4
		    AND b.tenant_id = $1
		    AND b.user_id = $2
		    AND b.schedule_id = $11
		    AND b.run_snapshot_id = $12
		 ON CONFLICT (batch_id, content_item_id)
		     WHERE content_item_id IS NOT NULL DO NOTHING
		 RETURNING id`,
		expected.TenantID, expected.UserID, d.BatchID, physicalKey,
		d.ContentItemID, d.Score, d.BodyMD, card, d.FeishuMessageID, status,
		expected.TaskID, ref.SnapshotID,
	).Scan(&id)
	if err == nil {
		if err := commitCompiledRunWriteV1(ctx, tx, "commit compiled delivery"); err != nil {
			return 0, false, false, err
		}
		return id, false, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false, taskRunDatabaseError("insert compiled delivery", err)
	}

	if d.ContentItemID != nil {
		var status types.DeliveryStatus
		lookupErr := tx.QueryRow(ctx,
			`SELECT d.id, d.status
			   FROM deliveries d
			   JOIN push_batches b ON b.id = d.batch_id
			  WHERE d.batch_id = $1 AND d.content_item_id = $2
			    AND d.tenant_id = $3 AND d.user_id = $4
			    AND b.tenant_id = $3 AND b.user_id = $4
			    AND b.idempotency_key = $5 AND b.schedule_id = $6
			    AND b.run_snapshot_id = $7`,
			d.BatchID, *d.ContentItemID, expected.TenantID, expected.UserID,
			physicalKey, expected.TaskID, ref.SnapshotID,
		).Scan(&id, &status)
		if lookupErr == nil {
			if err := commitCompiledRunWriteV1(ctx, tx,
				"commit compiled delivery replay"); err != nil {
				return 0, false, false, err
			}
			return id, true, status == types.DeliveryStatusSent, nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return 0, false, false,
				taskRunDatabaseError("load compiled delivery replay", lookupErr)
		}
	}
	return 0, false, false, taskRunNotFound()
}

// UpdatePushBatchStatusForTaskRunV1 is the live-authorized status path used
// when no successful external send needs to be receipted.
func (s *Store) UpdatePushBatchStatusForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
	batchID int64,
	status types.BatchStatus,
) error {
	if idempotencyKey == "" || batchID <= 0 ||
		(status != types.BatchStatusDone && status != types.BatchStatusFailed) {
		return taskRunValidationError("compiled push batch status input is invalid")
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCompiledLegacyPushBatchV1(
		ctx, tx, expected, ref.SnapshotID, batchID,
		types.PushBatchDeliveryAuthorityLegacy,
	); err != nil {
		return err
	}
	if err := updateCompiledPushBatchStatusV1(
		ctx, tx, expected, ref.SnapshotID, physicalKey, batchID, status,
	); err != nil {
		return err
	}
	return commitCompiledRunWriteV1(ctx, tx, "commit compiled push batch status")
}

// MarkPushBatchDoneReceiptV1 records the terminal batch receipt after at least
// one external send succeeded. It remains possible after revocation, but is
// pinned to the original snapshot and exact batch scope.
func (s *Store) MarkPushBatchDoneReceiptV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
	batchID int64,
) error {
	if idempotencyKey == "" || batchID <= 0 {
		return taskRunValidationError("compiled push batch receipt input is invalid")
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)
	tx, err := s.beginCompiledRunReceiptV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCompiledLegacyPushBatchV1(
		ctx, tx, expected, ref.SnapshotID, batchID,
		types.PushBatchDeliveryAuthorityLegacy,
	); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE push_batches b
		    SET status = $7
		  WHERE b.id = $1
		    AND b.tenant_id = $2
		    AND b.user_id = $3
		    AND b.schedule_id = $4
		    AND b.idempotency_key = $5
		    AND b.run_snapshot_id = $6
		    AND b.status IN ($7, $9)
		    AND EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.batch_id = b.id
		           AND d.tenant_id = b.tenant_id
		           AND d.user_id = b.user_id
		           AND d.status = $8
		    )
		    AND NOT EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.batch_id = b.id
		           AND (d.tenant_id IS DISTINCT FROM b.tenant_id
		             OR d.user_id IS DISTINCT FROM b.user_id
		             OR d.status IS DISTINCT FROM $8)
		    )`,
		batchID, expected.TenantID, expected.UserID, expected.TaskID,
		physicalKey, ref.SnapshotID, types.BatchStatusDone, types.DeliveryStatusSent,
		types.BatchStatusPending,
	)
	if err != nil {
		return taskRunDatabaseError("record compiled push batch receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return taskRunNotFound()
	}
	return commitCompiledRunWriteV1(ctx, tx, "commit compiled push batch receipt")
}

func updateCompiledPushBatchStatusV1(
	ctx context.Context,
	tx pgx.Tx,
	expected types.RunIdentity,
	snapshotID int64,
	physicalIdempotencyKey string,
	batchID int64,
	status types.BatchStatus,
) error {
	tag, err := tx.Exec(ctx,
		`UPDATE push_batches
		    SET status = $7
		  WHERE id = $1
		    AND tenant_id = $2
		    AND user_id = $3
		    AND schedule_id = $4
		    AND run_snapshot_id = $5
		    AND idempotency_key = $6`,
		batchID, expected.TenantID, expected.UserID, expected.TaskID,
		snapshotID, physicalIdempotencyKey, status,
	)
	if err != nil {
		return taskRunDatabaseError("update compiled push batch status", err)
	}
	if tag.RowsAffected() != 1 {
		return taskRunNotFound()
	}
	return nil
}

// MarkDeliverySentForTaskRunV1 is the post-send durable receipt. Revocation is
// intentionally not rechecked, while every immutable/original scope field is.
func (s *Store) MarkDeliverySentForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
	batchID int64,
	deliveryID int64,
	feishuMessageID string,
	cardJSON json.RawMessage,
	sentAt time.Time,
) error {
	if idempotencyKey == "" || batchID <= 0 || deliveryID <= 0 || sentAt.IsZero() {
		return taskRunValidationError("compiled delivery receipt input is invalid")
	}
	physicalKey := compiledPushBatchPhysicalKeyV1(ref.SnapshotID, idempotencyKey)
	if len(cardJSON) == 0 {
		cardJSON = json.RawMessage("{}")
	}
	tx, err := s.beginCompiledRunReceiptV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCompiledLegacyPushBatchV1(
		ctx, tx, expected, ref.SnapshotID, batchID,
		types.PushBatchDeliveryAuthorityLegacy,
	); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE deliveries d
		    SET feishu_message_id = $8,
		        card_json = $9,
		        status = $10,
		        sent_at = $11
		   FROM push_batches b
		  WHERE d.id = $1
		    AND d.batch_id = $2
		    AND d.tenant_id = $3
		    AND d.user_id = $4
		    AND b.id = d.batch_id
		    AND b.tenant_id = $3
		    AND b.user_id = $4
		    AND b.schedule_id = $5
		    AND b.idempotency_key = $6
		    AND b.run_snapshot_id = $7`,
		deliveryID, batchID, expected.TenantID, expected.UserID,
		expected.TaskID, physicalKey, ref.SnapshotID, feishuMessageID, cardJSON,
		types.DeliveryStatusSent, sentAt,
	)
	if err != nil {
		return taskRunDatabaseError("record compiled delivery receipt", err)
	}
	if tag.RowsAffected() != 1 {
		return taskRunNotFound()
	}
	return commitCompiledRunWriteV1(ctx, tx, "commit compiled delivery receipt")
}

func lockCompiledLegacyPushBatchV1(
	ctx context.Context,
	tx pgx.Tx,
	expected types.RunIdentity,
	snapshotID, batchID int64,
	requiredAuthority types.PushBatchDeliveryAuthority,
) error {
	_, err := lockPushEffectBatchAdmission(
		ctx,
		tx,
		types.PushBatchScope{
			TenantID: expected.TenantID,
			UserID:   expected.UserID,
			BatchID:  batchID,
		},
		snapshotID,
		requiredAuthority,
	)
	if errors.Is(err, types.ErrConflict) {
		return taskRunNotFound()
	}
	return err
}

// EvolveProfileForTaskRunV1 is the exact-tenant compiled evolution CAS. The
// caller should still use the legacy method for non-compiled activity paths.
func (s *Store) EvolveProfileForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	summary string,
	tags []string,
	newCursor int64,
	expectedAt time.Time,
	expectedCursor int64,
) error {
	if tags == nil {
		tags = []string{}
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	tag, err := tx.Exec(ctx,
		`UPDATE profiles
		    SET summary = $3, tags = $4,
		        last_evolved_feedback_id = $5, updated_at = now()
		  WHERE tenant_id = $1 AND user_id = $2
		    AND updated_at = $6 AND last_evolved_feedback_id = $7`,
		expected.TenantID, expected.UserID, summary, tags, newCursor,
		expectedAt, expectedCursor,
	)
	if err != nil {
		return taskRunDatabaseError("evolve compiled task profile", err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeConflict,
			"compiled task profile evolution CAS did not match", nil)
	}
	return commitCompiledRunWriteV1(ctx, tx, "commit compiled task profile evolution")
}

// AdvanceProfileCursorForTaskRunV1 is the exact-tenant semantic-failure CAS.
func (s *Store) AdvanceProfileCursorForTaskRunV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	newCursor int64,
	expectedAt time.Time,
	expectedCursor int64,
) error {
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	tag, err := tx.Exec(ctx,
		`UPDATE profiles
		    SET last_evolved_feedback_id = $3
		  WHERE tenant_id = $1 AND user_id = $2
		    AND updated_at = $4 AND last_evolved_feedback_id = $5`,
		expected.TenantID, expected.UserID, newCursor, expectedAt, expectedCursor,
	)
	if err != nil {
		return taskRunDatabaseError("advance compiled task profile cursor", err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeConflict,
			"compiled task profile cursor CAS did not match", nil)
	}
	return commitCompiledRunWriteV1(ctx, tx, "commit compiled task profile cursor")
}
