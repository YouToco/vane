package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

const (
	ObservationStepPrepared  = "prepared"
	ObservationStepSending   = "sending"
	ObservationStepCompleted = "completed"
	ObservationStepUncertain = "uncertain"
)

// PrepareObservationQualificationStep establishes the durable pre-effect
// checkpoint. A prior sending state is ambiguous after retry and is atomically
// quarantined instead of authorizing a duplicate paid request.
func (s *Store) PrepareObservationQualificationStep(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	stepID, requestDigest string,
) (status string, response json.RawMessage, err error) {
	if stepID == "" || !validObservationDigest(requestDigest) {
		return "", nil, taskRunValidationError("observation qualification step is invalid")
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return "", nil, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	var inserted string
	err = tx.QueryRow(ctx,
		`INSERT INTO task_event_qualification_steps (
		     tenant_id,user_id,task_id,run_snapshot_id,temporal_run_id,
		     step_id,request_digest,status
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,'prepared')
		 ON CONFLICT (tenant_id,task_id,run_snapshot_id,temporal_run_id,step_id)
		     DO NOTHING
		 RETURNING status`,
		expected.TenantID, expected.UserID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID, requestDigest,
	).Scan(&inserted)
	if err == nil {
		if err := commitCompiledRunWriteV1(ctx, tx,
			"commit observation qualification preparation"); err != nil {
			return "", nil, err
		}
		return inserted, nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", nil, taskRunDatabaseError(
			"prepare observation qualification step", err)
	}
	var storedDigest string
	err = tx.QueryRow(ctx,
		`SELECT request_digest,status,response_json
		   FROM task_event_qualification_steps
		  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
		    AND temporal_run_id=$4 AND step_id=$5
		  FOR UPDATE`,
		expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID,
	).Scan(&storedDigest, &status, &response)
	if err != nil {
		return "", nil, taskRunDatabaseError(
			"load observation qualification checkpoint", err)
	}
	if storedDigest != requestDigest {
		return "", nil, observationConflict("observation qualification request differs")
	}
	if status == ObservationStepSending {
		if _, err := tx.Exec(ctx,
			`UPDATE task_event_qualification_steps
			    SET status='uncertain',updated_at=clock_timestamp()
			  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
			    AND temporal_run_id=$4 AND step_id=$5 AND status='sending'`,
			expected.TenantID, expected.TaskID, ref.SnapshotID,
			expected.TemporalRunID, stepID,
		); err != nil {
			return "", nil, taskRunDatabaseError(
				"quarantine ambiguous observation qualification", err)
		}
		status = ObservationStepUncertain
	}
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit observation qualification replay"); err != nil {
		return "", nil, err
	}
	return status, response, nil
}

func (s *Store) MarkObservationQualificationSending(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	stepID, requestDigest string,
) error {
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	tag, err := tx.Exec(ctx,
		`UPDATE task_event_qualification_steps
		    SET status='sending',updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
		    AND temporal_run_id=$4 AND step_id=$5
		    AND request_digest=$6 AND status='prepared'`,
		expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID, requestDigest)
	if err != nil {
		return taskRunDatabaseError("mark observation qualification sending", err)
	}
	if tag.RowsAffected() != 1 {
		return observationConflict("observation qualification is not prepared")
	}
	return commitCompiledRunWriteV1(ctx, tx,
		"commit observation qualification sending")
}

func (s *Store) CompleteObservationQualificationStep(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	stepID, requestDigest string,
	response json.RawMessage,
) error {
	if len(response) == 0 || !json.Valid(response) {
		return taskRunValidationError("observation qualification response is invalid")
	}
	tx, err := s.beginCompiledRunReceiptV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	tag, err := tx.Exec(ctx,
		`UPDATE task_event_qualification_steps
		    SET status='completed',response_json=$7,updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
		    AND temporal_run_id=$4 AND step_id=$5
		    AND request_digest=$6 AND status='sending'`,
		expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID, requestDigest, response)
	if err != nil {
		return taskRunDatabaseError("complete observation qualification", err)
	}
	if tag.RowsAffected() == 1 {
		return commitCompiledRunWriteV1(ctx, tx,
			"commit observation qualification result")
	}
	var status string
	var stored json.RawMessage
	if err := tx.QueryRow(ctx,
		`SELECT status,response_json
		   FROM task_event_qualification_steps
		  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
		    AND temporal_run_id=$4 AND step_id=$5 AND request_digest=$6`,
		expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID, requestDigest,
	).Scan(&status, &stored); err != nil {
		return taskRunDatabaseError("load completed observation qualification", err)
	}
	if status != ObservationStepCompleted || !bytes.Equal(stored, response) {
		return observationConflict("observation qualification completion differs")
	}
	return commitCompiledRunWriteV1(ctx, tx,
		"commit observation qualification completion replay")
}

func (s *Store) MarkObservationQualificationUncertain(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	stepID, requestDigest string,
) error {
	tx, err := s.beginCompiledRunReceiptV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, err := tx.Exec(ctx,
		`UPDATE task_event_qualification_steps
		    SET status='uncertain',response_json=NULL,updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
		    AND temporal_run_id=$4 AND step_id=$5 AND request_digest=$6
		    AND status IN ('prepared','sending','uncertain')`,
		expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID, requestDigest,
	); err != nil {
		return taskRunDatabaseError("mark observation qualification uncertain", err)
	}
	return commitCompiledRunWriteV1(ctx, tx,
		"commit observation qualification uncertainty")
}

func (s *Store) ReserveObservedEventV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	batchID int64,
	event observation.QualifiedEvent,
) (bool, error) {
	if !validObservationDigest(event.PolicyDigest) ||
		!validObservationDigest(event.EventKey) || event.EventType == "" ||
		event.Subject == "" || event.OccurredAt.IsZero() ||
		len(event.EvidenceJSON) == 0 || !json.Valid(event.EvidenceJSON) {
		return false, taskRunValidationError("qualified event is invalid")
	}
	for attempt := 0; attempt < 3; attempt++ {
		accepted, err := s.reserveObservedEventAttemptV1(
			ctx, expected, ref, batchID, event,
		)
		if !errors.Is(err, errObservedEventAdmissionDrift) {
			return accepted, err
		}
	}
	return false, observationConflict("observed event admission kept changing")
}

var errObservedEventAdmissionDrift = errors.New(
	"observed event admission candidate changed",
)

type observedEventAdmissionCandidate struct {
	found                  bool
	id, userID, snapshotID int64
	runID, status          string
	createdAt              time.Time
	deliveryID, batchID    *int64
	batchSnapshotID        *int64
}

func (s *Store) loadObservedEventAdmissionCandidate(
	ctx context.Context,
	expected types.RunIdentity,
	event observation.QualifiedEvent,
) (observedEventAdmissionCandidate, error) {
	var candidate observedEventAdmissionCandidate
	err := s.pool.QueryRow(ctx, `
		SELECT e.id,e.user_id,e.run_snapshot_id,e.temporal_run_id,e.status,
		       e.created_at,e.delivery_id,d.batch_id,b.run_snapshot_id
		  FROM task_observed_events e
		  LEFT JOIN deliveries d
		    ON d.id=e.delivery_id AND d.tenant_id=e.tenant_id
		   AND d.user_id=e.user_id
		  LEFT JOIN push_batches b
		    ON b.id=d.batch_id AND b.tenant_id=d.tenant_id
		   AND b.user_id=d.user_id
		 WHERE e.tenant_id=$1 AND e.task_id=$2
		   AND e.policy_digest=$3 AND e.event_key=$4`,
		expected.TenantID,
		expected.TaskID,
		event.PolicyDigest,
		event.EventKey,
	).Scan(
		&candidate.id,
		&candidate.userID,
		&candidate.snapshotID,
		&candidate.runID,
		&candidate.status,
		&candidate.createdAt,
		&candidate.deliveryID,
		&candidate.batchID,
		&candidate.batchSnapshotID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return candidate, nil
	}
	if err != nil {
		return candidate, taskRunDatabaseError(
			"inspect observed event admission candidate",
			err,
		)
	}
	candidate.found = true
	return candidate, nil
}

func (s *Store) reserveObservedEventAttemptV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	batchID int64,
	event observation.QualifiedEvent,
) (bool, error) {
	candidate, err := s.loadObservedEventAdmissionCandidate(
		ctx, expected, event,
	)
	if err != nil {
		return false, err
	}
	scopes := []types.PushBatchScope{{
		TenantID: expected.TenantID,
		UserID:   expected.UserID,
		BatchID:  batchID,
	}}
	snapshots := map[int64]int64{batchID: ref.SnapshotID}
	if candidate.found && candidate.deliveryID != nil {
		if candidate.userID != expected.UserID ||
			candidate.batchID == nil ||
			candidate.batchSnapshotID == nil {
			return false, observationConflict(
				"observed event delivery batch is unavailable",
			)
		}
		scopes = append(scopes, types.PushBatchScope{
			TenantID: expected.TenantID,
			UserID:   expected.UserID,
			BatchID:  *candidate.batchID,
		})
		snapshots[*candidate.batchID] = *candidate.batchSnapshotID
	}
	tx, batchStatuses, err := s.beginObservedEventAdmissionV1(
		ctx, expected, ref, scopes, snapshots, true,
	)
	if err != nil {
		return false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	batchIDs := make([]int64, 0, len(scopes))
	for _, scope := range scopes {
		batchIDs = append(batchIDs, scope.BatchID)
	}
	effectBatch, err := lockObservedEventPushEffects(
		ctx, tx, expected.TenantID, expected.UserID, batchIDs,
	)
	if err != nil {
		return false, err
	}

	if !candidate.found {
		var id int64
		err = tx.QueryRow(ctx,
			`INSERT INTO task_observed_events (
		     tenant_id,user_id,task_id,policy_digest,event_key,event_type,
		     subject,occurred_at,evidence_json,run_snapshot_id,temporal_run_id
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (tenant_id,task_id,policy_digest,event_key) DO NOTHING
		 RETURNING id`,
			expected.TenantID, expected.UserID, expected.TaskID,
			event.PolicyDigest, event.EventKey, event.EventType, event.Subject,
			event.OccurredAt, event.EvidenceJSON, ref.SnapshotID,
			expected.TemporalRunID,
		).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, errObservedEventAdmissionDrift
		}
		if err != nil {
			return false, taskRunDatabaseError("reserve qualified event", err)
		}
		if err := commitCompiledRunWriteV1(ctx, tx,
			"commit qualified event reservation"); err != nil {
			return false, err
		}
		return true, nil
	}

	var deliveryStatus types.DeliveryStatus
	if candidate.deliveryID != nil {
		var lockedBatchID int64
		if err := tx.QueryRow(ctx, `
			SELECT batch_id,status
			  FROM deliveries
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			 FOR UPDATE`,
			*candidate.deliveryID,
			expected.TenantID,
			expected.UserID,
		).Scan(&lockedBatchID, &deliveryStatus); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, errObservedEventAdmissionDrift
			}
			return false, taskRunDatabaseError(
				"lock observed event delivery candidate", err)
		}
		if candidate.batchID == nil || lockedBatchID != *candidate.batchID {
			return false, errObservedEventAdmissionDrift
		}
	}

	var (
		locked observedEventAdmissionCandidate
		stale  bool
	)
	err = tx.QueryRow(ctx, `
		SELECT id,user_id,run_snapshot_id,temporal_run_id,status,
		       created_at,delivery_id,
		       created_at <= clock_timestamp()-interval '10 minutes'
		  FROM task_observed_events
		 WHERE tenant_id=$1 AND task_id=$2
		   AND policy_digest=$3 AND event_key=$4
		 FOR UPDATE`,
		expected.TenantID,
		expected.TaskID,
		event.PolicyDigest,
		event.EventKey,
	).Scan(
		&locked.id,
		&locked.userID,
		&locked.snapshotID,
		&locked.runID,
		&locked.status,
		&locked.createdAt,
		&locked.deliveryID,
		&stale,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, errObservedEventAdmissionDrift
	}
	if err != nil {
		return false, taskRunDatabaseError("lock qualified event conflict", err)
	}
	if locked.id != candidate.id ||
		locked.userID != candidate.userID ||
		locked.snapshotID != candidate.snapshotID ||
		locked.runID != candidate.runID ||
		locked.status != candidate.status ||
		!locked.createdAt.Equal(candidate.createdAt) ||
		!equalOptionalInt64(locked.deliveryID, candidate.deliveryID) {
		return false, errObservedEventAdmissionDrift
	}

	accepted := locked.snapshotID == ref.SnapshotID &&
		locked.runID == expected.TemporalRunID &&
		locked.status == "qualified"
	reclaimable := locked.status == "qualified" && stale
	if candidate.deliveryID != nil {
		previousBatchID := *candidate.batchID
		reclaimable = reclaimable &&
			deliveryStatus == types.DeliveryStatusPending &&
			(batchStatuses[previousBatchID] == types.BatchStatusFailed ||
				batchStatuses[previousBatchID] == types.BatchStatusPending) &&
			!effectBatch[previousBatchID]
	}
	if !accepted && reclaimable && candidate.deliveryID != nil {
		tag, updateErr := tx.Exec(ctx,
			`UPDATE deliveries
			    SET status='failed'
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			    AND batch_id=$4 AND status='pending'`,
			*candidate.deliveryID,
			expected.TenantID,
			expected.UserID,
			*candidate.batchID,
		)
		if updateErr != nil {
			return false, taskRunDatabaseError(
				"retire stale observed event delivery", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return false, errObservedEventAdmissionDrift
		}
	}
	if !accepted && reclaimable {
		tag, updateErr := tx.Exec(ctx,
			`UPDATE task_observed_events
			    SET event_type=$5,subject=$6,occurred_at=$7,evidence_json=$8,
			        run_snapshot_id=$9,temporal_run_id=$10,delivery_id=NULL,
			        created_at=clock_timestamp()
			  WHERE tenant_id=$1 AND task_id=$2
			    AND policy_digest=$3 AND event_key=$4
			    AND status='qualified'`,
			expected.TenantID, expected.TaskID, event.PolicyDigest, event.EventKey,
			event.EventType, event.Subject, event.OccurredAt, event.EvidenceJSON,
			ref.SnapshotID, expected.TemporalRunID)
		if updateErr != nil {
			return false, taskRunDatabaseError("reclaim qualified event", updateErr)
		}
		accepted = tag.RowsAffected() == 1
	}
	if err := commitCompiledRunWriteV1(ctx, tx,
		"commit qualified event replay"); err != nil {
		return false, err
	}
	return accepted, nil
}

func equalOptionalInt64(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func lockObservedEventPushEffects(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	batchIDs []int64,
) (map[int64]bool, error) {
	slices.Sort(batchIDs)
	batchIDs = slices.Compact(batchIDs)
	var effectBatchIDs []int64
	err := tx.QueryRow(ctx, `
		SELECT lock_observed_event_push_effects_v1($1,$2,$3)`,
		tenantID, userID, batchIDs,
	).Scan(&effectBatchIDs)
	if err != nil {
		return nil, taskRunDatabaseError(
			"lock observed event push effects", err)
	}
	found := make(map[int64]bool, len(batchIDs))
	for _, effectBatchID := range effectBatchIDs {
		found[effectBatchID] = true
	}
	return found, nil
}

func (s *Store) BindObservedEventDeliveryV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	policyDigest, eventKey string,
	batchID int64,
	deliveryID int64,
) error {
	if !validObservationDigest(policyDigest) || !validObservationDigest(eventKey) ||
		batchID <= 0 || deliveryID <= 0 {
		return taskRunValidationError("observed event delivery binding is invalid")
	}
	tx, _, err := s.beginObservedEventAdmissionV1(
		ctx, expected, ref,
		[]types.PushBatchScope{{
			TenantID: expected.TenantID,
			UserID:   expected.UserID,
			BatchID:  batchID,
		}},
		map[int64]int64{batchID: ref.SnapshotID},
		true,
	)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if _, err := lockObservedEventPushEffects(
		ctx, tx, expected.TenantID, expected.UserID, []int64{batchID},
	); err != nil {
		return err
	}
	var lockedDeliveryID int64
	if err := tx.QueryRow(ctx, `
		SELECT id
		  FROM deliveries
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND batch_id=$4
		 FOR UPDATE`,
		deliveryID,
		expected.TenantID,
		expected.UserID,
		batchID,
	).Scan(&lockedDeliveryID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return observationConflict("observed event delivery is unavailable")
		}
		return taskRunDatabaseError("lock observed event delivery", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE task_observed_events e
		    SET delivery_id=$6
		  WHERE e.tenant_id=$1 AND e.task_id=$2
		    AND e.policy_digest=$3 AND e.event_key=$4
		    AND e.run_snapshot_id=$5 AND e.temporal_run_id=$7
		    AND e.status='qualified'
		    AND (e.delivery_id IS NULL OR e.delivery_id=$6)`,
		expected.TenantID, expected.TaskID, policyDigest, eventKey,
		ref.SnapshotID, deliveryID, expected.TemporalRunID)
	if err != nil {
		return taskRunDatabaseError("bind observed event delivery", err)
	}
	if tag.RowsAffected() != 1 {
		return observationConflict("observed event delivery cannot be bound")
	}
	return commitCompiledRunWriteV1(ctx, tx,
		"commit observed event delivery binding")
}

func (s *Store) MarkObservedEventDeliveredV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	deliveryID int64,
) error {
	tx, err := s.beginCompiledRunReceiptV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	tag, err := tx.Exec(ctx,
		`UPDATE task_observed_events e
		    SET status='delivered',delivered_at=COALESCE(d.sent_at,clock_timestamp())
		   FROM deliveries d
		  WHERE e.delivery_id=$1 AND d.id=e.delivery_id
		    AND e.tenant_id=$2 AND e.task_id=$3
		    AND e.run_snapshot_id=$4 AND e.temporal_run_id=$5
		    AND d.tenant_id=$2 AND d.user_id=$6 AND d.status='sent'
		    AND e.status IN ('qualified','delivered')`,
		deliveryID, expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, expected.UserID)
	if err != nil {
		return taskRunDatabaseError("mark observed event delivered", err)
	}
	if tag.RowsAffected() < 1 {
		return observationConflict("observed event delivery receipt is unavailable")
	}
	return commitCompiledRunWriteV1(ctx, tx,
		"commit observed event delivered receipt")
}

func validObservationDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}

func observationConflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, types.ErrConflict)
}
