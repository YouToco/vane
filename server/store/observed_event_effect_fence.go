package store

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

func (s *Store) ReserveObservedEventV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	batchID int64,
	event observation.QualifiedEvent,
) (bool, error) {
	if err := validateObservedEventReservationV1(batchID, event); err != nil {
		return false, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		_, accepted, err := s.reserveObservedEventAttemptV1(
			ctx, expected, ref, batchID, event, false)
		if !errors.Is(err, errObservedEventAdmissionDrift) {
			return accepted, err
		}
	}
	return false, observationConflict(
		"observed event admission kept changing")
}

// ReserveObservedEventProvenanceV1 is the Phase 2-B0 Store primitive wired
// only by Phase 2-B1's versioned Activity path. It preserves
// ReserveObservedEventV1's exact admission/replay/CAS behavior while returning
// the immutable observed-event row identity and evidence digest needed by the
// optional event-evidence Brief extension.
func (s *Store) ReserveObservedEventProvenanceV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	batchID int64,
	event observation.QualifiedEvent,
) (types.ObservedEventProvenanceV1, bool, error) {
	if len(event.EvidenceJSON) >
		types.ObservedEventProvenanceMaxEvidenceBytesV1 {
		return types.ObservedEventProvenanceV1{}, false,
			taskRunValidationError(
				"qualified event evidence is too large")
	}
	if err := validateObservedEventReservationV1(batchID, event); err != nil {
		return types.ObservedEventProvenanceV1{}, false, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		provenance, accepted, err := s.reserveObservedEventAttemptV1(
			ctx, expected, ref, batchID, event, true)
		if errors.Is(err, errObservedEventAdmissionDrift) {
			continue
		}
		if err != nil || !accepted {
			return types.ObservedEventProvenanceV1{}, accepted, err
		}
		return provenance, true, nil
	}
	return types.ObservedEventProvenanceV1{}, false, observationConflict(
		"observed event admission kept changing")
}

func validateObservedEventReservationV1(
	batchID int64,
	event observation.QualifiedEvent,
) error {
	if batchID <= 0 ||
		!validObservationDigest(event.PolicyDigest) ||
		!validObservationDigest(event.EventKey) ||
		event.EventType == "" ||
		event.Subject == "" ||
		event.OccurredAt.IsZero() ||
		len(event.EvidenceJSON) == 0 ||
		!json.Valid(event.EvidenceJSON) {
		return taskRunValidationError("qualified event is invalid")
	}
	return nil
}

var errObservedEventAdmissionDrift = errors.New(
	"observed event admission candidate changed")

type observedEventAdmissionCandidate struct {
	found                  bool
	id, userID, snapshotID int64
	runID, status          string
	createdAt              time.Time
	deliveryID, batchID    *int64
	batchSnapshotID        *int64
}

type observedEventReservationV1 struct {
	id           int64
	policyDigest string
	eventKey     string
	eventType    string
	subject      string
	occurredAt   time.Time
	evidenceJSON json.RawMessage
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
			"inspect observed event admission candidate", err)
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
	requireProvenance bool,
) (types.ObservedEventProvenanceV1, bool, error) {
	candidate, err := s.loadObservedEventAdmissionCandidate(
		ctx, expected, event)
	if err != nil {
		return types.ObservedEventProvenanceV1{}, false, err
	}
	if candidate.found && candidate.userID != expected.UserID {
		return types.ObservedEventProvenanceV1{}, false,
			observationConflict(
				"observed event user scope does not match")
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
			return types.ObservedEventProvenanceV1{}, false, observationConflict(
				"observed event delivery batch is unavailable")
		}
		scopes = append(scopes, types.PushBatchScope{
			TenantID: expected.TenantID,
			UserID:   expected.UserID,
			BatchID:  *candidate.batchID,
		})
		snapshots[*candidate.batchID] = *candidate.batchSnapshotID
	}
	tx, batchStatuses, err := s.beginObservedEventAdmissionV1(
		ctx, expected, ref, scopes, snapshots, true)
	if err != nil {
		return types.ObservedEventProvenanceV1{}, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	batchIDs := make([]int64, 0, len(scopes))
	for _, scope := range scopes {
		batchIDs = append(batchIDs, scope.BatchID)
	}
	effectBatch, err := lockObservedEventPushEffects(
		ctx, tx, expected.TenantID, expected.UserID, batchIDs)
	if err != nil {
		return types.ObservedEventProvenanceV1{}, false, err
	}

	if !candidate.found {
		if effectBatch[batchID] {
			return types.ObservedEventProvenanceV1{}, false, observationConflict(
				"push effect observed event aggregate is frozen")
		}
		var reserved observedEventReservationV1
		err = tx.QueryRow(ctx, `
			INSERT INTO task_observed_events (
			     tenant_id,user_id,task_id,policy_digest,event_key,event_type,
			     subject,occurred_at,evidence_json,run_snapshot_id,
			     temporal_run_id
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (
			    tenant_id,task_id,policy_digest,event_key
			) DO NOTHING
			RETURNING id,policy_digest,event_key,event_type,subject,
			          occurred_at,evidence_json`,
			expected.TenantID,
			expected.UserID,
			expected.TaskID,
			event.PolicyDigest,
			event.EventKey,
			event.EventType,
			event.Subject,
			event.OccurredAt,
			event.EvidenceJSON,
			ref.SnapshotID,
			expected.TemporalRunID,
		).Scan(
			&reserved.id,
			&reserved.policyDigest,
			&reserved.eventKey,
			&reserved.eventType,
			&reserved.subject,
			&reserved.occurredAt,
			&reserved.evidenceJSON,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return types.ObservedEventProvenanceV1{}, false,
				errObservedEventAdmissionDrift
		}
		if err != nil {
			return types.ObservedEventProvenanceV1{}, false, taskRunDatabaseError(
				"reserve qualified event", err)
		}
		provenance, err := sealObservedEventReservationV1(
			reserved, requireProvenance)
		if err != nil {
			return types.ObservedEventProvenanceV1{}, false, err
		}
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit qualified event reservation",
		); err != nil {
			return types.ObservedEventProvenanceV1{}, false, err
		}
		return provenance, true, nil
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
				return types.ObservedEventProvenanceV1{}, false,
					errObservedEventAdmissionDrift
			}
			return types.ObservedEventProvenanceV1{}, false, taskRunDatabaseError(
				"lock observed event delivery candidate", err)
		}
		if candidate.batchID == nil ||
			lockedBatchID != *candidate.batchID {
			return types.ObservedEventProvenanceV1{}, false,
				errObservedEventAdmissionDrift
		}
	}

	var (
		locked   observedEventAdmissionCandidate
		reserved observedEventReservationV1
		stale    bool
	)
	err = tx.QueryRow(ctx, `
		SELECT id,user_id,run_snapshot_id,temporal_run_id,status,
		       created_at,delivery_id,policy_digest,event_key,event_type,
		       subject,occurred_at,evidence_json,
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
		&reserved.policyDigest,
		&reserved.eventKey,
		&reserved.eventType,
		&reserved.subject,
		&reserved.occurredAt,
		&reserved.evidenceJSON,
		&stale,
	)
	reserved.id = locked.id
	if errors.Is(err, pgx.ErrNoRows) {
		return types.ObservedEventProvenanceV1{}, false,
			errObservedEventAdmissionDrift
	}
	if err != nil {
		return types.ObservedEventProvenanceV1{}, false, taskRunDatabaseError(
			"lock qualified event conflict", err)
	}
	if locked.id != candidate.id ||
		locked.userID != expected.UserID ||
		locked.userID != candidate.userID ||
		locked.snapshotID != candidate.snapshotID ||
		locked.runID != candidate.runID ||
		locked.status != candidate.status ||
		!locked.createdAt.Equal(candidate.createdAt) ||
		!equalOptionalInt64(locked.deliveryID, candidate.deliveryID) {
		return types.ObservedEventProvenanceV1{}, false,
			errObservedEventAdmissionDrift
	}

	if effectBatch[batchID] {
		if locked.userID != expected.UserID ||
			locked.snapshotID != ref.SnapshotID ||
			locked.runID != expected.TemporalRunID ||
			locked.status != "qualified" ||
			locked.deliveryID == nil ||
			candidate.batchID == nil ||
			*candidate.batchID != batchID ||
			deliveryStatus != types.DeliveryStatusPending {
			return types.ObservedEventProvenanceV1{}, false, observationConflict(
				"push effect observed event reservation is frozen")
		}
		provenance, err := sealObservedEventReservationV1(
			reserved, requireProvenance)
		if err != nil {
			return types.ObservedEventProvenanceV1{}, false, err
		}
		if err := commitCompiledRunWriteV1(
			ctx, tx, "commit frozen observed event reservation replay",
		); err != nil {
			return types.ObservedEventProvenanceV1{}, false, err
		}
		return provenance, true, nil
	}

	accepted := locked.userID == expected.UserID &&
		locked.snapshotID == ref.SnapshotID &&
		locked.runID == expected.TemporalRunID &&
		locked.status == "qualified"
	reclaimable := locked.userID == expected.UserID &&
		locked.status == "qualified" && stale
	if candidate.deliveryID != nil {
		previousBatchID := *candidate.batchID
		reclaimable = reclaimable &&
			deliveryStatus == types.DeliveryStatusPending &&
			(batchStatuses[previousBatchID] == types.BatchStatusFailed ||
				batchStatuses[previousBatchID] == types.BatchStatusPending) &&
			!effectBatch[previousBatchID]
	}
	if !accepted && reclaimable && candidate.deliveryID != nil {
		tag, updateErr := tx.Exec(ctx, `
			UPDATE deliveries
			   SET status='failed'
			 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			   AND batch_id=$4 AND status='pending'`,
			*candidate.deliveryID,
			expected.TenantID,
			expected.UserID,
			*candidate.batchID,
		)
		if updateErr != nil {
			return types.ObservedEventProvenanceV1{}, false, taskRunDatabaseError(
				"retire stale observed event delivery", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return types.ObservedEventProvenanceV1{}, false,
				errObservedEventAdmissionDrift
		}
	}
	if !accepted && reclaimable {
		updateErr := tx.QueryRow(ctx, `
			UPDATE task_observed_events
			   SET event_type=$5,subject=$6,occurred_at=$7,
			       evidence_json=$8,run_snapshot_id=$9,
			       temporal_run_id=$10,delivery_id=NULL,
			       created_at=clock_timestamp()
			 WHERE tenant_id=$1 AND task_id=$2
			   AND policy_digest=$3 AND event_key=$4
			   AND status='qualified'
			RETURNING id,policy_digest,event_key,event_type,subject,
			          occurred_at,evidence_json`,
			expected.TenantID,
			expected.TaskID,
			event.PolicyDigest,
			event.EventKey,
			event.EventType,
			event.Subject,
			event.OccurredAt,
			event.EvidenceJSON,
			ref.SnapshotID,
			expected.TemporalRunID,
		).Scan(
			&reserved.id,
			&reserved.policyDigest,
			&reserved.eventKey,
			&reserved.eventType,
			&reserved.subject,
			&reserved.occurredAt,
			&reserved.evidenceJSON,
		)
		if errors.Is(updateErr, pgx.ErrNoRows) {
			return types.ObservedEventProvenanceV1{}, false,
				errObservedEventAdmissionDrift
		}
		if updateErr != nil {
			return types.ObservedEventProvenanceV1{}, false, taskRunDatabaseError(
				"reclaim qualified event", updateErr)
		}
		accepted = true
	}
	provenance, err := sealObservedEventReservationV1(
		reserved, requireProvenance && accepted)
	if err != nil {
		return types.ObservedEventProvenanceV1{}, false, err
	}
	if err := commitCompiledRunWriteV1(
		ctx, tx, "commit qualified event replay",
	); err != nil {
		return types.ObservedEventProvenanceV1{}, false, err
	}
	return provenance, accepted, nil
}

func sealObservedEventReservationV1(
	reserved observedEventReservationV1,
	required bool,
) (types.ObservedEventProvenanceV1, error) {
	if !required {
		return types.ObservedEventProvenanceV1{}, nil
	}
	provenance, err := types.SealObservedEventProvenanceV1(
		reserved.id,
		reserved.policyDigest,
		reserved.eventKey,
		reserved.eventType,
		reserved.subject,
		reserved.occurredAt,
		reserved.evidenceJSON,
	)
	if err != nil {
		return types.ObservedEventProvenanceV1{},
			taskRunValidationError(
				"observed event provenance cannot be sealed")
	}
	return provenance, nil
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
	if !validObservationDigest(policyDigest) ||
		!validObservationDigest(eventKey) ||
		batchID <= 0 ||
		deliveryID <= 0 {
		return taskRunValidationError(
			"observed event delivery binding is invalid")
	}
	tx, _, err := s.beginObservedEventAdmissionV1(
		ctx,
		expected,
		ref,
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
	effectBatch, err := lockObservedEventPushEffects(
		ctx,
		tx,
		expected.TenantID,
		expected.UserID,
		[]int64{batchID},
	)
	if err != nil {
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
			return observationConflict(
				"observed event delivery is unavailable")
		}
		return taskRunDatabaseError(
			"lock observed event delivery", err)
	}
	if effectBatch[batchID] {
		var boundDeliveryID *int64
		err := tx.QueryRow(ctx, `
			SELECT delivery_id
			  FROM task_observed_events
			 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
			   AND policy_digest=$4 AND event_key=$5
			   AND run_snapshot_id=$6 AND temporal_run_id=$7
			   AND status IN ('qualified','delivered')
			 FOR UPDATE`,
			expected.TenantID,
			expected.UserID,
			expected.TaskID,
			policyDigest,
			eventKey,
			ref.SnapshotID,
			expected.TemporalRunID,
		).Scan(&boundDeliveryID)
		if errors.Is(err, pgx.ErrNoRows) {
			return observationConflict(
				"frozen observed event binding is unavailable")
		}
		if err != nil {
			return taskRunDatabaseError(
				"load frozen observed event binding", err)
		}
		if boundDeliveryID == nil || *boundDeliveryID != deliveryID {
			return observationConflict(
				"push effect observed event binding is frozen")
		}
		return commitCompiledRunWriteV1(
			ctx, tx, "commit frozen observed event binding replay")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE task_observed_events e
		   SET delivery_id=$6
		 WHERE e.tenant_id=$1 AND e.task_id=$2
		   AND e.policy_digest=$3 AND e.event_key=$4
		   AND e.run_snapshot_id=$5 AND e.temporal_run_id=$7
		   AND e.status='qualified'
		   AND (e.delivery_id IS NULL OR e.delivery_id=$6)`,
		expected.TenantID,
		expected.TaskID,
		policyDigest,
		eventKey,
		ref.SnapshotID,
		deliveryID,
		expected.TemporalRunID,
	)
	if err != nil {
		return taskRunDatabaseError(
			"bind observed event delivery", err)
	}
	if tag.RowsAffected() != 1 {
		return observationConflict(
			"observed event delivery cannot be bound")
	}
	return commitCompiledRunWriteV1(
		ctx, tx, "commit observed event delivery binding")
}
