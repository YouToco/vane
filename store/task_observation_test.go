package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/types"
)

func TestObservationQualificationCheckpointAndEventLedger(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM task_observed_events WHERE tenant_id = $1`, f.base.tenantID)
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM task_event_qualification_steps WHERE tenant_id = $1`, f.base.tenantID)
	})

	requestDigest := strings.Repeat("a", 64)
	status, _, err := f.base.st.PrepareObservationQualificationStep(
		ctx, f.idA, f.refA, "qualify-events-v1", requestDigest)
	if err != nil || status != ObservationStepPrepared {
		t.Fatalf("prepare status=%q err=%v", status, err)
	}
	if err := f.base.st.MarkObservationQualificationSending(
		ctx, f.idA, f.refA, "qualify-events-v1", requestDigest); err != nil {
		t.Fatal(err)
	}
	status, response, err := f.base.st.PrepareObservationQualificationStep(
		ctx, f.idA, f.refA, "qualify-events-v1", requestDigest)
	if err != nil || status != ObservationStepUncertain || response != nil {
		t.Fatalf("ambiguous retry status=%q response=%s err=%v", status, response, err)
	}

	completedDigest := strings.Repeat("b", 64)
	if status, _, err := f.base.st.PrepareObservationQualificationStep(
		ctx, f.idA, f.refA, "qualify-events-completed", completedDigest,
	); err != nil || status != ObservationStepPrepared {
		t.Fatalf("second prepare status=%q err=%v", status, err)
	}
	if err := f.base.st.MarkObservationQualificationSending(
		ctx, f.idA, f.refA, "qualify-events-completed", completedDigest); err != nil {
		t.Fatal(err)
	}
	canonical := json.RawMessage(`{"outcome":"no_match","events":[]}`)
	if err := f.base.st.CompleteObservationQualificationStep(
		ctx, f.idA, f.refA, "qualify-events-completed", completedDigest, canonical,
	); err != nil {
		t.Fatal(err)
	}
	status, response, err = f.base.st.PrepareObservationQualificationStep(
		ctx, f.idA, f.refA, "qualify-events-completed", completedDigest)
	if err != nil || status != ObservationStepCompleted ||
		!jsonEqual(response, canonical) {
		t.Fatalf("completed replay status=%q response=%s err=%v", status, response, err)
	}

	event := observation.QualifiedEvent{
		PolicyDigest: strings.Repeat("c", 64),
		EventKey:     strings.Repeat("d", 64),
		EventType:    "model_release",
		Subject:      "OpenAI models",
		OccurredAt:   time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC),
		EvidenceJSON: json.RawMessage(`{"content_ids":[1]}`),
	}
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := f.base.st.ReserveObservedEventV1(ctx, f.idA, f.refA, event)
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			if ok {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 8 {
		t.Fatalf("same-run exact replay accepted=%d want=8", accepted.Load())
	}
	var rows int
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_observed_events
		  WHERE tenant_id=$1 AND task_id=$2 AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID, f.idA.TaskID, event.PolicyDigest, event.EventKey).
		Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("ledger rows=%d err=%v", rows, err)
	}

	otherIdentity := scheduledRunIdentity(
		f.taskA, f.base.tenantID, f.base.userID, "run-observe-"+uuid.NewString())
	otherRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		ctx, CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: otherIdentity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, otherIdentity, otherRef, event); err != nil || ok {
		t.Fatalf("cross-run duplicate accepted=%v err=%v", ok, err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE task_observed_events
		    SET created_at=clock_timestamp() - interval '11 minutes'
		  WHERE tenant_id=$1 AND task_id=$2
		    AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID, f.idA.TaskID, event.PolicyDigest, event.EventKey,
	); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, otherIdentity, otherRef, event); err != nil || !ok {
		t.Fatalf("stale unbound event takeover accepted=%v err=%v", ok, err)
	}
}

func TestAuditOutdatedFeedbackWithoutPolicyCreatesSuggestion(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	batchKey := "freshness-audit-" + uuid.NewString()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, batchKey)
	if err != nil {
		t.Fatal(err)
	}
	contentID := f.createContent(t, f.sourceA, "freshness-audit")
	deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, batchKey, &types.Delivery{
			BatchID: batchID, UserID: f.idA.UserID,
			ContentItemID: &contentID, BodyMD: "old",
		})
	if err != nil {
		t.Fatal(err)
	}
	feedbackID, err := f.base.st.InsertFeedback(ctx, &types.Feedback{
		UserID: f.idA.UserID, DeliveryID: deliveryID,
		Action:     types.FeedbackActionMisjudged,
		ReasonCode: types.FeedbackReasonOutdated,
		Detail:     "too old",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedback_freshness_triage WHERE feedback_id=$1`, feedbackID)
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=$1`, feedbackID)
	})
	var pendingStatus string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT status FROM feedback_freshness_triage WHERE feedback_id=$1`,
		feedbackID).Scan(&pendingStatus); err != nil || pendingStatus != "pending" {
		t.Fatalf("pending status=%q err=%v", pendingStatus, err)
	}
	outcomes, err := f.base.st.AuditPendingOutdatedFeedbacks(
		ctx, f.idA.TenantID, f.idA.UserID, 10)
	if err != nil || len(outcomes) != 1 ||
		outcomes[0] != types.FreshnessAuditTaskPolicySuggestion {
		t.Fatalf("outcomes=%v err=%v", outcomes, err)
	}
	var stored string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT outcome FROM feedback_freshness_triage WHERE feedback_id=$1`,
		feedbackID).Scan(&stored); err != nil ||
		stored != string(types.FreshnessAuditTaskPolicySuggestion) {
		t.Fatalf("stored outcome=%q err=%v", stored, err)
	}
	const claimers = 8
	var (
		claimWG sync.WaitGroup
		mu      sync.Mutex
		claims  []types.TaskPolicySuggestion
	)
	claimWG.Add(claimers)
	for range claimers {
		go func() {
			defer claimWG.Done()
			suggestion, claimErr := f.base.st.ClaimTaskPolicySuggestion(
				ctx, f.idA.TenantID, f.idA.UserID)
			if errors.Is(claimErr, types.ErrNotFound) {
				return
			}
			if claimErr != nil {
				t.Errorf("concurrent claim: %v", claimErr)
				return
			}
			mu.Lock()
			claims = append(claims, suggestion)
			mu.Unlock()
		}()
	}
	claimWG.Wait()
	if len(claims) != 1 || claims[0].FeedbackID != feedbackID ||
		claims[0].ClaimToken == "" {
		t.Fatalf("claims=%+v, want exactly one fenced claim", claims)
	}
	if err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, claims[0].ClaimToken,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.CompleteTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID,
		claims[0].ClaimToken, "om_policy_suggestion",
	); err != nil {
		t.Fatal(err)
	}
	var notificationStatus, messageID string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT notification_status,COALESCE(notification_message_id,'')
		   FROM feedback_freshness_triage WHERE feedback_id=$1`,
		feedbackID).Scan(&notificationStatus, &messageID); err != nil ||
		notificationStatus != "sent" || messageID != "om_policy_suggestion" {
		t.Fatalf("notification status=%q message=%q err=%v",
			notificationStatus, messageID, err)
	}

	// Simulate SendCard success followed by a lost DB receipt: the record
	// remains sending. Once its lease expires it must become uncertain and
	// never return to the send queue.
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_status='pending',notification_message_id=NULL,
		        notified_at=NULL
		  WHERE feedback_id=$1`,
		feedbackID); err != nil {
		t.Fatal(err)
	}
	preDispatchClaim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_lease_until=clock_timestamp()-interval '1 second'
		  WHERE feedback_id=$1`,
		feedbackID); err != nil {
		t.Fatal(err)
	}
	lostReceiptClaim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil || lostReceiptClaim.ClaimToken == preDispatchClaim.ClaimToken {
		t.Fatalf("expired pre-dispatch claim not safely reclaimed: %+v err=%v",
			lostReceiptClaim, err)
	}
	if err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, lostReceiptClaim.ClaimToken,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_lease_until=clock_timestamp()-interval '1 second'
		  WHERE feedback_id=$1`,
		feedbackID); err != nil {
		t.Fatal(err)
	}
	_, err = f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("expired ambiguous send was reclaimed: %v", err)
	}
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT notification_status
		   FROM feedback_freshness_triage WHERE feedback_id=$1`,
		feedbackID).Scan(&notificationStatus); err != nil ||
		notificationStatus != "uncertain" {
		t.Fatalf("lost receipt status=%q err=%v claim=%s",
			notificationStatus, err, lostReceiptClaim.ClaimToken)
	}

	// A definite Feishu rejection is different: releasing its sending claim
	// makes it available to a later run, without retrying inside this run.
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET notification_status='pending',notification_last_error=NULL
		  WHERE feedback_id=$1`,
		feedbackID); err != nil {
		t.Fatal(err)
	}
	retryableClaim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, retryableClaim.ClaimToken,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.ReleaseTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID,
		retryableClaim.ClaimToken, "channel not connected",
	); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil || reclaimed.FeedbackID != feedbackID {
		t.Fatalf("definite failure not retryable: %+v err=%v", reclaimed, err)
	}
	if err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, reclaimed.ClaimToken,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.MarkTaskPolicySuggestionUncertain(
		ctx, f.idA.TenantID, f.idA.UserID,
		reclaimed.ClaimToken, "test cleanup",
	); err != nil {
		t.Fatal(err)
	}
}

func TestStalePendingObservedEventReentersCandidatesAndTransfers(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM task_observed_events WHERE tenant_id=$1`,
			f.idA.TenantID)
	})
	event := observation.QualifiedEvent{
		PolicyDigest: strings.Repeat("e", 64),
		EventKey:     strings.Repeat("f", 64),
		EventType:    "model_release",
		Subject:      "OpenAI models",
		OccurredAt:   time.Now().UTC().Truncate(time.Second),
		EvidenceJSON: json.RawMessage(`{"content_ids":[1]}`),
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, f.idA, f.refA, event); err != nil || !ok {
		t.Fatalf("initial reserve accepted=%v err=%v", ok, err)
	}
	key := "partial-failure-" + uuid.NewString()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, key)
	if err != nil {
		t.Fatal(err)
	}
	contentID := f.createContent(t, f.sourceA, "partial-failure")
	deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, key, &types.Delivery{
			BatchID: batchID, UserID: f.idA.UserID,
			ContentItemID: &contentID, BodyMD: "pending block",
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.BindObservedEventDeliveryV1(
		ctx, f.idA, f.refA, event.PolicyDigest, event.EventKey, deliveryID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE task_observed_events
		    SET created_at=clock_timestamp() - interval '11 minutes'
		  WHERE tenant_id=$1 AND task_id=$2
		    AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID, f.idA.TaskID, event.PolicyDigest, event.EventKey,
	); err != nil {
		t.Fatal(err)
	}

	nextIdentity := scheduledRunIdentity(
		f.taskA, f.idA.TenantID, f.idA.UserID, "run-next-"+uuid.NewString())
	nextRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		ctx, CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: nextIdentity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := f.base.st.ListUnpushedForTaskRunV1(
		ctx, nextIdentity, nextRef, []int64{f.sourceA}, 10, 10)
	if err != nil || len(candidates) != 1 || candidates[0].ID != contentID {
		t.Fatalf("recovery candidates=%+v err=%v", candidates, err)
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, nextIdentity, nextRef, event); err != nil || !ok {
		t.Fatalf("next-run takeover accepted=%v err=%v", ok, err)
	}
	var runID string
	var bound *int64
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT temporal_run_id,delivery_id
		   FROM task_observed_events
		  WHERE tenant_id=$1 AND task_id=$2
		    AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID, f.idA.TaskID, event.PolicyDigest, event.EventKey,
	).Scan(&runID, &bound); err != nil ||
		runID != nextIdentity.TemporalRunID || bound != nil {
		t.Fatalf("takeover run=%q delivery=%v err=%v", runID, bound, err)
	}
	var firstStatus string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT status FROM deliveries WHERE id=$1`,
		deliveryID).Scan(&firstStatus); err != nil || firstStatus != "failed" {
		t.Fatalf("first stale delivery status=%q err=%v", firstStatus, err)
	}

	replacementKey := "partial-failure-replacement-" + uuid.NewString()
	replacementBatch, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, nextIdentity, nextRef, replacementKey)
	if err != nil {
		t.Fatal(err)
	}
	replacementDelivery, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, nextIdentity, nextRef, replacementKey, &types.Delivery{
			BatchID: replacementBatch, UserID: nextIdentity.UserID,
			ContentItemID: &contentID, BodyMD: "replacement pending block",
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.BindObservedEventDeliveryV1(
		ctx, nextIdentity, nextRef,
		event.PolicyDigest, event.EventKey, replacementDelivery,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE task_observed_events
		    SET created_at=clock_timestamp() - interval '11 minutes'
		  WHERE tenant_id=$1 AND task_id=$2
		    AND policy_digest=$3 AND event_key=$4`,
		f.idA.TenantID, f.idA.TaskID, event.PolicyDigest, event.EventKey,
	); err != nil {
		t.Fatal(err)
	}
	thirdIdentity := scheduledRunIdentity(
		f.taskA, f.idA.TenantID, f.idA.UserID, "run-third-"+uuid.NewString())
	thirdRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		ctx, CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: thirdIdentity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err = f.base.st.ListUnpushedForTaskRunV1(
		ctx, thirdIdentity, thirdRef, []int64{f.sourceA}, 10, 10)
	if err != nil || len(candidates) != 1 || candidates[0].ID != contentID {
		t.Fatalf("second recovery candidates=%+v err=%v", candidates, err)
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, thirdIdentity, thirdRef, event); err != nil || !ok {
		t.Fatalf("second takeover accepted=%v err=%v", ok, err)
	}
	var replacementStatus string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT status FROM deliveries WHERE id=$1`,
		replacementDelivery).Scan(&replacementStatus); err != nil ||
		replacementStatus != "failed" {
		t.Fatalf("replacement stale status=%q err=%v", replacementStatus, err)
	}
}

func TestAllProblemReasonsEnterDurableTriage(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	key := "problem-triage-" + uuid.NewString()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, key)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		reason  types.FeedbackReason
		status  string
		outcome *string
	}{
		{types.FeedbackReasonOutdated, "pending", nil},
		{types.FeedbackReasonNotRelevant, "routed", stringPointer("interest_signal")},
		{types.FeedbackReasonDuplicate, "routed", stringPointer("duplicate_diagnostic")},
		{types.FeedbackReasonFactWrong, "routed", stringPointer("factual_diagnostic")},
		{types.FeedbackReasonPoorSource, "routed", stringPointer("evidence_diagnostic")},
		{types.FeedbackReasonOther, "routed", stringPointer("manual_review")},
	}
	var feedbackIDs []int64
	for index, tc := range cases {
		contentID := f.createContent(t, f.sourceA, fmt.Sprintf("triage-%d", index))
		deliveryID, _, _, insertErr := f.base.st.InsertDeliveryForTaskRunV1(
			ctx, f.idA, f.refA, key, &types.Delivery{
				BatchID: batchID, UserID: f.idA.UserID,
				ContentItemID: &contentID, BodyMD: "problem",
			})
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		feedbackID, insertErr := f.base.st.InsertFeedback(ctx, &types.Feedback{
			UserID: f.idA.UserID, DeliveryID: deliveryID,
			Action:     types.FeedbackActionMisjudged,
			ReasonCode: tc.reason, Detail: "detail-" + string(tc.reason),
		})
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		feedbackIDs = append(feedbackIDs, feedbackID)
		var status string
		var outcome *string
		var reason, detail string
		if err := f.base.st.pool.QueryRow(ctx,
			`SELECT status,outcome,reason_code,detail
			   FROM feedback_freshness_triage WHERE feedback_id=$1`,
			feedbackID).Scan(&status, &outcome, &reason, &detail); err != nil ||
			status != tc.status || !reflect.DeepEqual(outcome, tc.outcome) ||
			reason != string(tc.reason) ||
			detail != "detail-"+string(tc.reason) {
			t.Fatalf("reason=%s status=%q outcome=%v detail=%q err=%v",
				tc.reason, status, outcome, detail, err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=ANY($1)`, feedbackIDs)
	})
}

func stringPointer(value string) *string {
	return &value
}

func jsonEqual(left, right []byte) bool {
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil &&
		json.Unmarshal(right, &rightValue) == nil &&
		reflect.DeepEqual(leftValue, rightValue)
}
