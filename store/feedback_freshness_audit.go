package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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
	)
	err := s.pool.QueryRow(ctx,
		`SELECT f.tenant_id,f.delivery_id,c.published_at,
		        COALESCE(r.payload,''::bytea),
		        COALESCE(r.temporal_workflow_id,'')
		   FROM feedbacks f
		   JOIN deliveries d ON d.id=f.delivery_id
		   LEFT JOIN content_items c ON c.id=d.content_item_id
		   JOIN push_batches b ON b.id=d.batch_id
		   LEFT JOIN task_run_snapshots r ON r.id=b.run_snapshot_id
		  WHERE f.id=$1 AND f.user_id=$2
		    AND f.action='misjudged'
		    AND f.reason_code='outdated_or_out_of_window'`,
		feedbackID, userID,
	).Scan(&tenantID, &deliveryID, &publishedAt, &payload, &workflowID)
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
	if len(payload) == 0 {
		outcome = types.FreshnessAuditTaskPolicySuggestion
		audit["reason"] = "delivery has no immutable compiled run snapshot"
	} else if decoded, decodeErr := readTaskRunSnapshotPayload(payload); decodeErr != nil {
		audit["reason"] = "immutable run snapshot could not be verified"
	} else {
		definition := decoded.Payload.Definition
		taskValue := definition.TaskID
		taskID = &taskValue
		var scope struct {
			Observation *observation.PolicyV1 `json:"observation,omitempty"`
		}
		if json.Unmarshal(definition.ScopeJSON, &scope) != nil ||
			scope.Observation == nil {
			outcome = types.FreshnessAuditTaskPolicySuggestion
			audit["reason"] = "task has no approved observation policy"
		} else if publishedAt == nil {
			audit["reason"] = "content occurrence time is unavailable"
		} else {
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
				*scope.Observation,
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
				if scope.Observation.LatePolicy == observation.LateBounded {
					start = start.Add(
						-time.Duration(scope.Observation.AllowedLatenessSecs) *
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

func (s *Store) BeginTaskPolicySuggestionDispatch(
	ctx context.Context,
	tenantID, userID int64,
	claimToken string,
) error {
	if tenantID <= 0 || userID <= 0 || claimToken == "" {
		return types.NewAppError(
			types.CodeValidation, "任务策略建议发送围栏无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return taskRunDatabaseError("begin task suggestion dispatch", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return taskRunDatabaseError("set task suggestion dispatch tenant", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return taskRunDatabaseError("enter task suggestion dispatch role", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_status='sending',
		        notification_lease_until=clock_timestamp()+$4::interval,
		        updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2
		    AND outcome='task_policy_suggestion'
		    AND notification_status='claimed'
		    AND notification_claim_token=$3`,
		tenantID, userID, claimToken, taskPolicySuggestionLease.String())
	if err != nil {
		return taskRunDatabaseError("fence task suggestion dispatch", err)
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(
			types.CodeConflict, "任务策略建议发送租约已变化", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskRunDatabaseError("commit task suggestion dispatch", err)
	}
	return nil
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
