package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runtimepolicy"
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

// AuthorizeObservationQualificationSpendV1 is the final external-effect fence
// for event qualification. Both modes first prove the immutable per-run
// rollout decision and move prepared -> sending exactly once. Authority also
// reserves the production compiled LLM bucket in the same transaction; shadow
// is operator-funded and deliberately never reads or changes that bucket.
func (s *Store) AuthorizeObservationQualificationSpendV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	stepID, requestDigest string,
	rollout observation.RolloutMode,
	rule *runtimepolicy.QuotaBucketV1,
	amount float64,
) error {
	if stepID == "" || !validObservationDigest(requestDigest) ||
		(rollout != observation.RolloutShadow &&
			rollout != observation.RolloutAuthority) ||
		amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return taskRunValidationError(
			"observation qualification spend is invalid")
	}
	if rollout == observation.RolloutAuthority {
		if rule == nil || rule.Name != string(QuotaLLMTokens) ||
			!rule.Financial ||
			rule.EnforcementVersion !=
				runtimepolicy.QuotaEnforcementLLMPrechargeV1 {
			return taskRunValidationError(
				"observation authority quota request is invalid")
		}
	} else if rule != nil {
		return taskRunValidationError(
			"observation shadow must not use production quota")
	}

	// This transaction intentionally stays in the schema-owner role. vane_app
	// has no UPDATE privilege on tenant_quota, while the quota reservation and
	// prepared->sending transition must commit atomically. The same exact
	// sealed-reference validation and live aggregate share locks used by
	// beginAuthorizedCompiledRunWriteV1 are performed explicitly before either
	// write; every following predicate repeats the full tenant/run scope.
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskRunDatabaseError(
			"begin observation qualification spend", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	_, frozen, err := loadCompiledTaskRunSnapshotV1(
		ctx, tx, expected, ref)
	if err != nil {
		return err
	}
	if frozen.ObservationRollout != rollout {
		return observationConflict(
			"observation qualification rollout differs")
	}
	if err := lockLiveCompiledRunWriteV1(ctx, tx, expected); err != nil {
		return err
	}

	if rollout == observation.RolloutAuthority {
		tag, err := tx.Exec(ctx,
			`UPDATE tenant_quota
			    SET tokens = LEAST(
			            burst,
			            tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))
			        ) - $3,
			        updated_at = now()
			  WHERE tenant_id=$1 AND bucket=$2
			    AND LEAST(
			            burst,
			            tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))
			        ) >= $3`,
			expected.TenantID, string(QuotaLLMTokens), amount)
		if err != nil {
			return classifyQuotaErr(err,
				"reserve observation authority llm quota")
		}
		if tag.RowsAffected() != 1 {
			return ErrQuotaExceeded
		}
	}

	tag, err := tx.Exec(ctx,
		`UPDATE task_event_qualification_steps
		    SET status='sending',updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND task_id=$2 AND run_snapshot_id=$3
		    AND temporal_run_id=$4 AND step_id=$5
		    AND request_digest=$6 AND status='prepared'`,
		expected.TenantID, expected.TaskID, ref.SnapshotID,
		expected.TemporalRunID, stepID, requestDigest)
	if err != nil {
		return taskRunDatabaseError(
			"authorize observation qualification spend", err)
	}
	if tag.RowsAffected() != 1 {
		return observationConflict(
			"observation qualification is not prepared")
	}
	return commitCompiledRunWriteV1(ctx, tx,
		"commit observation qualification spend authorization")
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
	event observation.QualifiedEvent,
) (bool, error) {
	if !validObservationDigest(event.PolicyDigest) ||
		!validObservationDigest(event.EventKey) || event.EventType == "" ||
		event.Subject == "" || event.OccurredAt.IsZero() ||
		len(event.EvidenceJSON) == 0 || !json.Valid(event.EvidenceJSON) {
		return false, taskRunValidationError("qualified event is invalid")
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
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
	if err == nil {
		if err := commitCompiledRunWriteV1(ctx, tx,
			"commit qualified event reservation"); err != nil {
			return false, err
		}
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, taskRunDatabaseError("reserve qualified event", err)
	}
	var snapshotID int64
	var runID, status string
	var deliveryID *int64
	var reclaimable bool
	if err := tx.QueryRow(ctx,
		`SELECT e.run_snapshot_id,e.temporal_run_id,e.status,e.delivery_id,
		        COALESCE((
		            e.status='qualified'
		            AND e.created_at <= clock_timestamp() - interval '10 minutes'
		            AND (
		                e.delivery_id IS NULL
		                OR (
		                    d.status='pending'
		                    AND b.status IN ('failed','pending')
		                )
		            )
		        ),false) AS reclaimable
		   FROM task_observed_events e
		   LEFT JOIN deliveries d ON d.id=e.delivery_id
		   LEFT JOIN push_batches b ON b.id=d.batch_id
		  WHERE e.tenant_id=$1 AND e.task_id=$2
		    AND e.policy_digest=$3 AND e.event_key=$4
		  FOR UPDATE OF e`,
		expected.TenantID, expected.TaskID, event.PolicyDigest, event.EventKey,
	).Scan(&snapshotID, &runID, &status, &deliveryID, &reclaimable); err != nil {
		return false, taskRunDatabaseError("load qualified event conflict", err)
	}
	accepted := snapshotID == ref.SnapshotID &&
		runID == expected.TemporalRunID && status == "qualified"
	if !accepted && reclaimable {
		if deliveryID != nil {
			if _, updateErr := tx.Exec(ctx,
				`UPDATE deliveries
				    SET status='failed'
				  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
				    AND status='pending'`,
				*deliveryID, expected.TenantID, expected.UserID,
			); updateErr != nil {
				return false, taskRunDatabaseError(
					"retire stale observed event delivery", updateErr)
			}
		}
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

func (s *Store) BindObservedEventDeliveryV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	policyDigest, eventKey string,
	deliveryID int64,
) error {
	if !validObservationDigest(policyDigest) || !validObservationDigest(eventKey) ||
		deliveryID <= 0 {
		return taskRunValidationError("observed event delivery binding is invalid")
	}
	tx, err := s.beginAuthorizedCompiledRunWriteV1(ctx, expected, ref)
	if err != nil {
		return err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	tag, err := tx.Exec(ctx,
		`UPDATE task_observed_events e
		    SET delivery_id=$6
		  WHERE e.tenant_id=$1 AND e.task_id=$2
		    AND e.policy_digest=$3 AND e.event_key=$4
		    AND e.run_snapshot_id=$5 AND e.temporal_run_id=$7
		    AND e.status='qualified'
		    AND (e.delivery_id IS NULL OR e.delivery_id=$6)
		    AND EXISTS (
		        SELECT 1 FROM deliveries d
		         WHERE d.id=$6 AND d.tenant_id=$1 AND d.user_id=$8
		    )`,
		expected.TenantID, expected.TaskID, policyDigest, eventKey,
		ref.SnapshotID, deliveryID, expected.TemporalRunID, expected.UserID)
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
