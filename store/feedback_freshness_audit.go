package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

// AuditOutdatedFeedback deterministically checks whether an outdated report
// crossed the immutable policy used by the delivery's run. It records only a
// diagnostic outcome: task definitions are never changed on this path.
func (s *Store) AuditOutdatedFeedback(
	ctx context.Context,
	userID, feedbackID int64,
) (types.FreshnessFeedbackAuditOutcome, error) {
	var (
		tenantID    int64
		deliveryID  int64
		publishedAt *time.Time
		payload     []byte
		workflowID  string
		batchTaskID string
	)
	err := s.pool.QueryRow(ctx,
		`SELECT f.tenant_id,f.delivery_id,c.published_at,
		        COALESCE(r.payload,''::bytea),
		        COALESCE(r.temporal_workflow_id,''),
		        COALESCE(b.schedule_id,'')
		   FROM feedbacks f
		   JOIN deliveries d ON d.id=f.delivery_id
		   LEFT JOIN content_items c ON c.id=d.content_item_id
		   JOIN push_batches b ON b.id=d.batch_id
		   LEFT JOIN task_run_snapshots r ON r.id=b.run_snapshot_id
		  WHERE f.id=$1 AND f.user_id=$2
		    AND f.action='misjudged'
		    AND f.reason_code='outdated_or_out_of_window'`,
		feedbackID, userID,
	).Scan(
		&tenantID, &deliveryID, &publishedAt, &payload,
		&workflowID, &batchTaskID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(
			types.CodeNotFound, "过时反馈不存在或不属于该用户", nil)
	}
	if err != nil {
		return "", taskRunDatabaseError("load outdated feedback audit input", err)
	}

	outcome := types.FreshnessAuditUnverifiable
	var taskID *string
	audit := map[string]any{
		"schema":      "vane.feedback-freshness-audit/v1",
		"feedback_id": feedbackID,
		"delivery_id": deliveryID,
	}
	if batchTaskID != "" {
		taskID = &batchTaskID
		audit["task_id"] = batchTaskID
	}
	if batchTaskID == "" {
		audit["reason"] = "delivery batch task identity is unavailable"
	} else if len(payload) == 0 {
		outcome = types.FreshnessAuditTaskPolicySuggestion
		audit["reason"] = "delivery has no immutable compiled run snapshot"
	} else if decoded, decodeErr := readTaskRunSnapshotPayload(payload); decodeErr != nil {
		audit["reason"] = "immutable run snapshot could not be verified"
	} else {
		definition := decoded.Payload.Definition
		hasObservation, scopeErr :=
			decodeHistoricalObservationPresence(definition.ScopeJSON)
		if definition.TaskID != batchTaskID {
			audit["reason"] =
				"immutable run snapshot task differs from delivery batch"
		} else if scopeErr != nil {
			audit["reason"] =
				"immutable run snapshot observation policy is invalid"
		} else if !hasObservation {
			outcome = types.FreshnessAuditTaskPolicySuggestion
			audit["reason"] = "task has no approved observation policy"
		} else if publishedAt == nil {
			audit["reason"] = "content occurrence time is unavailable"
		} else {
			policy, err := decodeHistoricalObservationPolicy(
				definition.ScopeJSON)
			if err != nil || policy == nil {
				return "", taskStateIntegrity()
			}
			nominal, nominalErr := observation.NominalTrigger(
				definition.TaskID, workflowID)
			var spec struct {
				Cron         string `json:"cron"`
				EverySeconds int    `json:"every_seconds"`
				AnchorAt     string `json:"anchor_at"`
				TZ           string `json:"tz"`
			}
			specErr := json.Unmarshal(definition.SpecJSON, &spec)
			window, windowErr := observation.WindowForNominal(
				*policy,
				observation.Schedule{
					Cron: spec.Cron, EverySeconds: spec.EverySeconds,
					AnchorAt: spec.AnchorAt, TimeZone: spec.TZ,
				},
				nominal,
			)
			if nominalErr != nil || specErr != nil || windowErr != nil {
				audit["reason"] = "approved observation window could not be reconstructed"
			} else {
				start := window.Start
				if policy.LatePolicy == observation.LateBounded {
					start = start.Add(
						-time.Duration(policy.AllowedLatenessSecs) *
							time.Second)
				}
				admission := observation.Window{Start: start, End: window.End}
				audit["window_start"] = admission.Start
				audit["window_end"] = admission.End
				audit["published_at"] = publishedAt.UTC()
				if admission.Contains(publishedAt.UTC()) {
					outcome = types.FreshnessAuditPolicySatisfied
				} else {
					outcome = types.FreshnessAuditSystemDefect
				}
			}
		}
	}
	audit["outcome"] = outcome
	auditJSON, marshalErr := json.Marshal(audit)
	if marshalErr != nil {
		return "", types.NewAppError(
			types.CodeInternal, "编码过时反馈审计失败", marshalErr)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", taskRunDatabaseError("begin outdated feedback audit", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return "", taskRunDatabaseError("set outdated feedback tenant context", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return "", taskRunDatabaseError("enter outdated feedback audit role", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET task_id=$5,outcome=$6,audit_json=$7,status='classified',
		        attempts=attempts+1,updated_at=clock_timestamp(),
		        notification_status=CASE
		            WHEN $6='task_policy_suggestion' THEN 'pending'
		            ELSE 'not_required'
		        END,
		        notified_at=NULL
		  WHERE tenant_id=$1 AND user_id=$2 AND feedback_id=$3
		    AND delivery_id=$4
		    AND reason_code='outdated_or_out_of_window'`,
		tenantID, userID, feedbackID, deliveryID, taskID, outcome, auditJSON)
	if err != nil {
		return "", taskRunDatabaseError("record outdated feedback audit", err)
	}
	if tag.RowsAffected() != 1 {
		return "", types.NewAppError(
			types.CodeConflict, "过时反馈待审计记录缺失", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", taskRunDatabaseError("commit outdated feedback audit", err)
	}
	return outcome, nil
}

// AuditPendingOutdatedFeedbacks retries durable diagnostics before the next
// profile/push cycle. A transient failure in the synchronous callback path
// therefore leaves a visible pending row rather than losing the report.
func (s *Store) AuditPendingOutdatedFeedbacks(
	ctx context.Context,
	tenantID, userID int64,
	limit int,
) ([]types.FreshnessFeedbackAuditOutcome, error) {
	if userID <= 0 || limit <= 0 {
		return nil, types.NewAppError(
			types.CodeValidation, "待审计过时反馈范围无效", nil)
	}
	query := `SELECT feedback_id
	            FROM feedback_freshness_triage
	           WHERE user_id=$1 AND status='pending'
	             AND reason_code='outdated_or_out_of_window'`
	args := []any{userID}
	if tenantID > 0 {
		query += ` AND tenant_id=$2`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at,id LIMIT $` +
		fmt.Sprintf("%d", len(args)+1)
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, taskRunDatabaseError("list pending outdated feedback audits", err)
	}
	var feedbackIDs []int64
	for rows.Next() {
		var feedbackID int64
		if err := rows.Scan(&feedbackID); err != nil {
			rows.Close()
			return nil, taskRunDatabaseError(
				"scan pending outdated feedback audit", err)
		}
		feedbackIDs = append(feedbackIDs, feedbackID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, taskRunDatabaseError(
			"iterate pending outdated feedback audits", err)
	}
	rows.Close()
	outcomes := make([]types.FreshnessFeedbackAuditOutcome, 0, len(feedbackIDs))
	for _, feedbackID := range feedbackIDs {
		outcome, auditErr := s.AuditOutdatedFeedback(ctx, userID, feedbackID)
		if auditErr != nil {
			return outcomes, auditErr
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes, nil
}

const taskPolicySuggestionLease = 5 * time.Minute

// ClaimTaskPolicySuggestion atomically grants one sender a fenced lease.
// Claiming is still pre-dispatch; BeginTaskPolicySuggestionDispatch records the
// irreversible boundary immediately before the Feishu API call.
func (s *Store) ClaimTaskPolicySuggestion(
	ctx context.Context,
	tenantID, userID int64,
) (types.TaskPolicySuggestion, error) {
	if tenantID <= 0 || userID <= 0 {
		return types.TaskPolicySuggestion{}, types.NewAppError(
			types.CodeValidation, "任务策略建议范围无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.TaskPolicySuggestion{},
			taskRunDatabaseError("begin task policy suggestion claim", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return types.TaskPolicySuggestion{},
			taskRunDatabaseError("set task suggestion claim tenant", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return types.TaskPolicySuggestion{},
			taskRunDatabaseError("enter task suggestion claim role", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_status=CASE
		            WHEN notification_status='claimed'
		            THEN 'pending'
		            ELSE 'uncertain'
		        END,
		        notification_claim_token=NULL,
		        notification_lease_until=NULL,
		        notification_last_error=CASE
		            WHEN notification_status='claimed'
		            THEN '发送前租约过期；已安全返回待重试'
		            ELSE '发送租约过期；无法确定飞书是否已接收'
		        END,
		        updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2
		    AND outcome='task_policy_suggestion'
		    AND notification_status IN ('claimed','sending')
		    AND notification_lease_until <= clock_timestamp()`,
		tenantID, userID); err != nil {
		return types.TaskPolicySuggestion{},
			taskRunDatabaseError("reconcile expired task suggestion claim", err)
	}
	claimToken := uuid.NewString()
	var suggestion types.TaskPolicySuggestion
	err = tx.QueryRow(ctx,
		`WITH candidate AS (
		     SELECT id
		       FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND user_id=$2
		        AND outcome='task_policy_suggestion'
		        AND notification_status='pending'
		      ORDER BY updated_at,id
		      FOR UPDATE SKIP LOCKED
		      LIMIT 1
		 )
		 UPDATE feedback_freshness_triage t
		    SET notification_status='claimed',
		        notification_claim_token=$3,
		        notification_lease_until=clock_timestamp()+$4::interval,
		        notification_attempts=notification_attempts+1,
		        notification_last_error=NULL,
		        updated_at=clock_timestamp()
		   FROM candidate
		  WHERE t.id=candidate.id
		 RETURNING t.feedback_id,t.delivery_id,COALESCE(t.task_id,''),
		           t.created_at,t.notification_claim_token`,
		tenantID, userID, claimToken,
		taskPolicySuggestionLease.String(),
	).Scan(
		&suggestion.FeedbackID, &suggestion.DeliveryID,
		&suggestion.TaskID, &suggestion.CreatedAt, &suggestion.ClaimToken,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return types.TaskPolicySuggestion{},
				taskRunDatabaseError(
					"commit task policy suggestion reconciliation", commitErr)
		}
		return types.TaskPolicySuggestion{}, types.ErrNotFound
	}
	if err != nil {
		return types.TaskPolicySuggestion{},
			taskRunDatabaseError("claim task policy suggestion", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.TaskPolicySuggestion{},
			taskRunDatabaseError("commit task policy suggestion claim", err)
	}
	return suggestion, nil
}

type currentObservationPolicyState uint8

const (
	currentObservationPolicyUnverifiable currentObservationPolicyState = iota
	currentObservationPolicyAbsent
	currentObservationPolicyPresent
	currentObservationPolicyTaskMissing
)

func currentApprovedObservationPolicyTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
) (currentObservationPolicyState, error) {
	if taskID == "" {
		return currentObservationPolicyUnverifiable, nil
	}
	var version *int64
	var digest *string
	var rawMode string
	var rawStatus string
	var editOperationID *string
	var editFence *int64
	var tenantActive, membershipExists bool
	err := tx.QueryRow(ctx,
		`SELECT s.approved_definition_version,s.approved_definition_digest,
		        s.execution_mode,s.definition_edit_operation_id,
		        s.definition_edit_fence,s.status,
		        EXISTS (
		            SELECT 1
		              FROM tenants t
		             WHERE t.id=s.tenant_id
		               AND t.status='active' AND t.deleted_at IS NULL
		        ),
		        EXISTS (
		            SELECT 1
		              FROM memberships m
		             WHERE m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		        )
		   FROM schedules s
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		  FOR KEY SHARE OF s`,
		tenantID, userID, taskID).Scan(
		&version, &digest, &rawMode, &editOperationID, &editFence,
		&rawStatus, &tenantActive, &membershipExists)
	if errors.Is(err, pgx.ErrNoRows) {
		return currentObservationPolicyTaskMissing, nil
	}
	if err != nil {
		return currentObservationPolicyUnverifiable,
			taskRunDatabaseError("lock current task policy head", err)
	}
	if !tenantActive || !membershipExists ||
		(rawStatus != string(types.ScheduleStatusActive) &&
			rawStatus != string(types.ScheduleStatusPaused)) {
		return currentObservationPolicyUnverifiable, nil
	}
	mode, modeErr := types.ParseExecutionMode(rawMode)
	if version == nil || digest == nil ||
		editOperationID != nil || editFence != nil ||
		modeErr != nil || mode != types.ExecutionModeCompiled {
		return currentObservationPolicyUnverifiable, nil
	}
	record, err := scanApprovedDefinitionVersion(tx.QueryRow(ctx,
		`SELECT d.version,d.schema_version,d.execution_mode,
		        d.definition_digest,d.payload,d.approval_ref,d.created_at
		   FROM task_approved_definition_versions d
		  WHERE d.tenant_id=$1 AND d.user_id=$2 AND d.task_id=$3
		    AND d.version=$4 AND d.definition_digest=$5`,
		tenantID, userID, taskID, *version, *digest),
		tenantID, userID, taskID)
	if errors.Is(err, types.ErrNotFound) {
		return currentObservationPolicyUnverifiable, nil
	}
	if err != nil {
		return currentObservationPolicyUnverifiable, err
	}
	if record.Definition.ExecutionMode != mode {
		return currentObservationPolicyUnverifiable, taskStateIntegrity()
	}
	hasObservation, err := decodeObservationPresence(
		record.Definition.ScopeJSON)
	if err != nil {
		return currentObservationPolicyUnverifiable, taskStateIntegrity()
	}
	if hasObservation {
		return currentObservationPolicyPresent, nil
	}
	return currentObservationPolicyAbsent, nil
}

func decodeHistoricalObservationPresence(raw json.RawMessage) (bool, error) {
	policy, err := decodeHistoricalObservationPolicy(raw)
	return policy != nil, err
}

func decodeHistoricalObservationPolicy(
	raw json.RawMessage,
) (*observation.PolicyV1, error) {
	var scope map[string]json.RawMessage
	if err := strictjson.Decode(raw, &scope); err != nil || scope == nil {
		return nil, err
	}
	rawObservation, ok := scope["observation"]
	if !ok || len(rawObservation) == 0 ||
		bytes.Equal(bytes.TrimSpace(rawObservation), []byte("null")) {
		return nil, nil
	}
	policy, err := observation.DecodePolicyV1Exact(rawObservation)
	if err != nil {
		return nil, err
	}
	return &policy, nil
}

func decodeObservationPresence(raw json.RawMessage) (bool, error) {
	var scope struct {
		Observation json.RawMessage `json:"observation,omitempty"`
		SourceIDs   []int64         `json:"source_ids,omitempty"`
		TopN        int             `json:"top_n,omitempty"`
	}
	if err := strictjson.DecodeExact(raw, &scope); err != nil {
		return false, err
	}
	if len(scope.Observation) == 0 ||
		bytes.Equal(bytes.TrimSpace(scope.Observation), []byte("null")) {
		return false, nil
	}
	if _, err := observation.DecodePolicyV1Exact(scope.Observation); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) BeginTaskPolicySuggestionDispatch(
	ctx context.Context,
	tenantID, userID int64,
	claimToken string,
) (bool, error) {
	if tenantID <= 0 || userID <= 0 || claimToken == "" {
		return false, types.NewAppError(
			types.CodeValidation, "任务策略建议发送围栏无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, taskRunDatabaseError(
			"begin task suggestion dispatch", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return false, taskRunDatabaseError(
			"set task suggestion dispatch tenant", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return false, taskRunDatabaseError(
			"enter task suggestion dispatch role", err)
	}
	var triageTaskID, batchTaskID string
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(t.task_id,''),COALESCE(b.schedule_id,'')
		   FROM feedback_freshness_triage t
		   JOIN deliveries d
		     ON d.tenant_id=t.tenant_id AND d.user_id=t.user_id
		    AND d.id=t.delivery_id
		   JOIN push_batches b
		     ON b.tenant_id=d.tenant_id AND b.user_id=d.user_id
		    AND b.id=d.batch_id
		  WHERE t.tenant_id=$1 AND t.user_id=$2
		    AND t.outcome='task_policy_suggestion'
		    AND t.notification_status='claimed'
		    AND t.notification_claim_token=$3`,
		tenantID, userID, claimToken).Scan(&triageTaskID, &batchTaskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, types.NewAppError(
			types.CodeConflict, "任务策略建议发送租约已变化", nil)
	}
	if err != nil {
		return false, taskRunDatabaseError(
			"read task suggestion dispatch identity", err)
	}
	identityVerified := batchTaskID != "" &&
		(triageTaskID == "" || triageTaskID == batchTaskID)
	if !identityVerified {
		tag, updateErr := tx.Exec(ctx,
			`UPDATE feedback_freshness_triage
			    SET notification_status='uncertain',
			        notification_claim_token=NULL,
			        notification_lease_until=NULL,
			        notification_last_error=
			            'task identity could not be verified before dispatch',
			        updated_at=clock_timestamp()
			  WHERE tenant_id=$1 AND user_id=$2
			    AND outcome='task_policy_suggestion'
			    AND notification_status='claimed'
			    AND notification_claim_token=$3`,
			tenantID, userID, claimToken)
		if updateErr != nil {
			return false, taskRunDatabaseError(
				"quarantine task suggestion identity", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return false, types.NewAppError(
				types.CodeConflict, "任务策略建议发送租约已变化", nil)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, taskRunDatabaseError(
				"commit unverified task suggestion identity", err)
		}
		return false, nil
	}
	policyState, err := currentApprovedObservationPolicyTx(
		ctx, tx, tenantID, userID, batchTaskID)
	if err != nil {
		return false, err
	}
	if policyState != currentObservationPolicyAbsent {
		status := "not_required"
		cause := "current approved observation policy already exists"
		switch policyState {
		case currentObservationPolicyTaskMissing:
			cause = "source task is missing or deleted"
		case currentObservationPolicyUnverifiable:
			status = "uncertain"
			cause = "current approved task policy could not be verified"
		}
		tag, updateErr := tx.Exec(ctx,
			`UPDATE feedback_freshness_triage
			    SET notification_status=$4,
			        task_id=COALESCE(task_id,$6),
			        notification_claim_token=NULL,
			        notification_lease_until=NULL,
			        notification_last_error=$5,
			        updated_at=clock_timestamp()
			  WHERE tenant_id=$1 AND user_id=$2
			    AND outcome='task_policy_suggestion'
			    AND notification_status='claimed'
			    AND notification_claim_token=$3
			    AND (task_id IS NULL OR task_id=$6)
			    AND EXISTS (
			        SELECT 1
			          FROM deliveries d
			          JOIN push_batches b
			            ON b.tenant_id=d.tenant_id AND b.user_id=d.user_id
			           AND b.id=d.batch_id
			         WHERE d.tenant_id=feedback_freshness_triage.tenant_id
			           AND d.user_id=feedback_freshness_triage.user_id
			           AND d.id=feedback_freshness_triage.delivery_id
			           AND b.schedule_id=$6
			    )`,
			tenantID, userID, claimToken, status, cause, batchTaskID)
		if updateErr != nil {
			return false, taskRunDatabaseError(
				"resolve task suggestion before dispatch", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return false, types.NewAppError(
				types.CodeConflict, "任务策略建议发送租约已变化", nil)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, taskRunDatabaseError(
				"commit resolved task suggestion", err)
		}
		return false, nil
	}
	tag, err := tx.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_status='sending',
		        task_id=COALESCE(task_id,$5),
		        notification_lease_until=clock_timestamp()+$4::interval,
		        updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2
		    AND outcome='task_policy_suggestion'
		    AND notification_status='claimed'
		    AND notification_claim_token=$3
		    AND (task_id IS NULL OR task_id=$5)
		    AND EXISTS (
		        SELECT 1
		          FROM deliveries d
		          JOIN push_batches b
		            ON b.tenant_id=d.tenant_id AND b.user_id=d.user_id
		           AND b.id=d.batch_id
		         WHERE d.tenant_id=feedback_freshness_triage.tenant_id
		           AND d.user_id=feedback_freshness_triage.user_id
		           AND d.id=feedback_freshness_triage.delivery_id
		           AND b.schedule_id=$5
		    )`,
		tenantID, userID, claimToken, taskPolicySuggestionLease.String(),
		batchTaskID)
	if err != nil {
		return false, taskRunDatabaseError(
			"fence task suggestion dispatch", err)
	}
	if tag.RowsAffected() != 1 {
		return false, types.NewAppError(
			types.CodeConflict, "任务策略建议发送租约已变化", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, taskRunDatabaseError(
			"commit task suggestion dispatch", err)
	}
	return true, nil
}

func (s *Store) CompleteTaskPolicySuggestion(
	ctx context.Context,
	tenantID, userID int64,
	claimToken, messageID string,
) error {
	return s.finishTaskPolicySuggestion(
		ctx, tenantID, userID, claimToken, "sent", messageID, "")
}

func (s *Store) MarkTaskPolicySuggestionUncertain(
	ctx context.Context,
	tenantID, userID int64,
	claimToken, cause string,
) error {
	return s.finishTaskPolicySuggestion(
		ctx, tenantID, userID, claimToken, "uncertain", "", cause)
}

func (s *Store) ReleaseTaskPolicySuggestion(
	ctx context.Context,
	tenantID, userID int64,
	claimToken, cause string,
) error {
	return s.finishTaskPolicySuggestion(
		ctx, tenantID, userID, claimToken, "pending", "", cause)
}

func (s *Store) finishTaskPolicySuggestion(
	ctx context.Context,
	tenantID, userID int64,
	claimToken, status, messageID, cause string,
) error {
	if tenantID <= 0 || userID <= 0 || claimToken == "" {
		return types.NewAppError(
			types.CodeValidation, "任务策略建议回执范围无效", nil)
	}
	if status != "pending" && status != "sent" && status != "uncertain" {
		return types.NewAppError(
			types.CodeValidation, "任务策略建议回执状态无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskRunDatabaseError("begin task policy suggestion receipt", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return taskRunDatabaseError("set task suggestion tenant context", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return taskRunDatabaseError("enter task suggestion receipt role", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_status=$4,
		        notification_claim_token=NULL,
		        notification_lease_until=NULL,
		        notification_message_id=NULLIF($5,''),
		        notification_last_error=NULLIF($6,''),
		        notified_at=CASE WHEN $4='sent'
		            THEN clock_timestamp() ELSE NULL END,
		        updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2
		    AND outcome='task_policy_suggestion'
		    AND (
		        ($4='pending' AND notification_status IN ('claimed','sending')) OR
		        ($4<>'pending' AND notification_status='sending')
		    )
		    AND notification_claim_token=$3`,
		tenantID, userID, claimToken, status, messageID, cause)
	if err != nil {
		return taskRunDatabaseError("mark task policy suggestion notified", err)
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(
			types.CodeConflict, "任务策略建议通知状态已变化", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskRunDatabaseError("commit task policy suggestion receipt", err)
	}
	return nil
}
