package store

import (
	"context"
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
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestAuthorizeObservationQualificationSpendV1_IsolatesShadowAndAtomicallyFencesAuthority(
	t *testing.T,
) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM task_event_qualification_steps WHERE tenant_id=$1`,
			f.base.tenantID)
	})
	setBucket(t, f.base.st, f.base.tenantID,
		QuotaLLMTokens, 100, 0.000001, 100)
	rule := runtimepolicy.QuotaBucketV1{
		Name: string(QuotaLLMTokens), Financial: true,
		EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
	}

	createRun := func(mode observation.RolloutMode) (
		types.RunIdentity, types.RunSnapshotRef,
	) {
		t.Helper()
		identity := scheduledRunIdentity(
			f.taskA, f.base.tenantID, f.base.userID,
			"run-observation-spend-"+string(mode)+"-"+uuid.NewString())
		ref, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
			ctx, CreateOrGetCompiledTaskRunSnapshotV1Params{
				Identity: identity, Policy: testCompiledRunPolicyV1(t),
				ObservationRollout: mode,
			})
		if err != nil {
			t.Fatal(err)
		}
		return identity, ref
	}
	prepare := func(identity types.RunIdentity, ref types.RunSnapshotRef, digest string) {
		t.Helper()
		status, _, err := f.base.st.PrepareObservationQualificationStep(
			ctx, identity, ref, "qualify-events-v1", digest)
		if err != nil || status != ObservationStepPrepared {
			t.Fatalf("prepare status=%q err=%v", status, err)
		}
	}

	shadowID, shadowRef := createRun(observation.RolloutShadow)
	shadowDigest := strings.Repeat("1", 64)
	prepare(shadowID, shadowRef, shadowDigest)
	beforeShadow := runtimeQuotaTokens(
		t, f.base.st, f.base.tenantID, QuotaLLMTokens)
	if err := f.base.st.AuthorizeObservationQualificationSpendV1(
		ctx, shadowID, shadowRef, "qualify-events-v1", shadowDigest,
		observation.RolloutShadow, nil, 20,
	); err != nil {
		t.Fatal(err)
	}
	afterShadow := runtimeQuotaTokens(
		t, f.base.st, f.base.tenantID, QuotaLLMTokens)
	if afterShadow != beforeShadow {
		t.Fatalf("shadow changed production quota: before=%f after=%f",
			beforeShadow, afterShadow)
	}
	if err := f.base.st.AuthorizeObservationQualificationSpendV1(
		ctx, shadowID, shadowRef, "qualify-events-v1", shadowDigest,
		observation.RolloutShadow, nil, 20,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("shadow replay spend = %v, want conflict", err)
	}

	authorityID, authorityRef := createRun(observation.RolloutAuthority)
	authorityDigest := strings.Repeat("2", 64)
	prepare(authorityID, authorityRef, authorityDigest)
	beforeAuthority := runtimeQuotaTokens(
		t, f.base.st, f.base.tenantID, QuotaLLMTokens)
	if err := f.base.st.AuthorizeObservationQualificationSpendV1(
		ctx, authorityID, authorityRef, "qualify-events-v1", authorityDigest,
		observation.RolloutAuthority, &rule, 20,
	); err != nil {
		t.Fatal(err)
	}
	afterAuthority := runtimeQuotaTokens(
		t, f.base.st, f.base.tenantID, QuotaLLMTokens)
	if delta := beforeAuthority - afterAuthority; delta < 19.9 || delta > 20.1 {
		t.Fatalf("authority quota delta=%f, want about 20", delta)
	}
	if err := f.base.st.AuthorizeObservationQualificationSpendV1(
		ctx, authorityID, authorityRef, "qualify-events-v1", authorityDigest,
		observation.RolloutAuthority, &rule, 20,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("authority replay spend = %v, want conflict", err)
	}
	afterReplay := runtimeQuotaTokens(
		t, f.base.st, f.base.tenantID, QuotaLLMTokens)
	if delta := afterAuthority - afterReplay; delta < -0.01 || delta > 0.01 {
		t.Fatalf("failed authority replay leaked quota: delta=%f", delta)
	}
}

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
	batchA := createObservationBatch(
		t, f, f.idA, f.refA, "observe-a-"+uuid.NewString())
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := f.base.st.ReserveObservedEventV1(
				ctx, f.idA, f.refA, batchA, event)
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
	provenance, ok, err := f.base.st.ReserveObservedEventProvenanceV1(
		ctx, f.idA, f.refA, batchA, observation.QualifiedEvent{
			PolicyDigest: event.PolicyDigest,
			EventKey:     event.EventKey,
			EventType:    "caller-replay-must-not-win",
			Subject:      "caller replay must not win",
			OccurredAt:   event.OccurredAt.Add(time.Hour),
			EvidenceJSON: json.RawMessage(`{"content_ids":[999]}`),
		},
	)
	if err != nil || !ok {
		t.Fatalf("provenance replay accepted=%v err=%v", ok, err)
	}
	if provenance.ID <= 0 ||
		provenance.EventType != event.EventType ||
		provenance.Subject != event.Subject ||
		!provenance.OccurredAt.Equal(event.OccurredAt) ||
		!provenance.MatchesEvidenceJSON(event.EvidenceJSON) ||
		provenance.MatchesEvidenceJSON(
			json.RawMessage(`{"content_ids":[999]}`),
		) {
		t.Fatalf("provenance did not bind stored first writer: %+v", provenance)
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
	otherBatch := createObservationBatch(
		t, f, otherIdentity, otherRef,
		"observe-other-"+uuid.NewString())
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, otherIdentity, otherRef, otherBatch, event); err != nil || ok {
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
		ctx, otherIdentity, otherRef, otherBatch, event); err != nil || !ok {
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
	installCurrentApprovedTaskPolicy(t, f, false)
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
	if dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, claims[0].ClaimToken,
	); err != nil || !dispatch {
		t.Fatalf("begin dispatch=%v err=%v", dispatch, err)
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
	if dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, lostReceiptClaim.ClaimToken,
	); err != nil || !dispatch {
		t.Fatalf("begin lost-receipt dispatch=%v err=%v", dispatch, err)
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
	if dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, retryableClaim.ClaimToken,
	); err != nil || !dispatch {
		t.Fatalf("begin retryable dispatch=%v err=%v", dispatch, err)
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
	if dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, reclaimed.ClaimToken,
	); err != nil || !dispatch {
		t.Fatalf("begin reclaimed dispatch=%v err=%v", dispatch, err)
	}
	if err := f.base.st.MarkTaskPolicySuggestionUncertain(
		ctx, f.idA.TenantID, f.idA.UserID,
		reclaimed.ClaimToken, "test cleanup",
	); err != nil {
		t.Fatal(err)
	}
}

func TestTaskPolicySuggestionSuppressedByCurrentApprovedObservation(
	t *testing.T,
) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()

	firstFeedbackID, _ := createOutdatedSuggestion(t, f, "before-policy")
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE feedback_freshness_triage
		    SET task_id=NULL
		  WHERE feedback_id=$1`,
		firstFeedbackID); err != nil {
		t.Fatal(err)
	}
	installCurrentApprovedTaskPolicy(t, f, false)
	firstClaim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil || firstClaim.FeedbackID != firstFeedbackID {
		t.Fatalf("claim before policy=%+v err=%v", firstClaim, err)
	}

	advanceCurrentApprovedObservationPolicy(t, f)
	dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, firstClaim.ClaimToken)
	if err != nil || dispatch {
		t.Fatalf("dispatch after policy commit=%v err=%v, want suppressed",
			dispatch, err)
	}
	assertTaskSuggestionNotificationStatus(
		t, f, firstFeedbackID, "not_required")

	secondFeedbackID, _ := createOutdatedSuggestion(t, f, "after-policy")
	secondClaim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err = f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, secondClaim.ClaimToken)
	if err != nil || dispatch {
		t.Fatalf("current policy backlog dispatch=%v err=%v, want suppressed",
			dispatch, err)
	}
	assertTaskSuggestionNotificationStatus(
		t, f, secondFeedbackID, "not_required")
}

func TestTaskPolicySuggestionUnverifiableCurrentStateNeverClaims(
	t *testing.T,
) {
	t.Run("approved head missing", func(t *testing.T) {
		f := newCompiledRunWriteFixture(t)
		feedbackID, _ := createOutdatedSuggestion(t, f, "missing-head")
		claim, err := f.base.st.ClaimTaskPolicySuggestion(
			t.Context(), f.idA.TenantID, f.idA.UserID)
		if err != nil {
			t.Fatal(err)
		}
		dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
			t.Context(), f.idA.TenantID, f.idA.UserID, claim.ClaimToken)
		if err != nil || dispatch {
			t.Fatalf("missing head dispatch=%v err=%v", dispatch, err)
		}
		assertTaskSuggestionNotificationStatus(
			t, f, feedbackID, "uncertain")
	})

	t.Run("task identity missing", func(t *testing.T) {
		f := newCompiledRunWriteFixture(t)
		feedbackID, deliveryID :=
			createOutdatedSuggestion(t, f, "missing-identity")
		if _, err := f.base.st.pool.Exec(t.Context(),
			`UPDATE feedback_freshness_triage
			    SET task_id=NULL
			  WHERE feedback_id=$1`,
			feedbackID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.base.st.pool.Exec(t.Context(),
			`UPDATE push_batches
			    SET schedule_id=NULL
			  WHERE id=(SELECT batch_id FROM deliveries WHERE id=$1)`,
			deliveryID); err != nil {
			t.Fatal(err)
		}
		claim, err := f.base.st.ClaimTaskPolicySuggestion(
			t.Context(), f.idA.TenantID, f.idA.UserID)
		if err != nil {
			t.Fatal(err)
		}
		dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
			t.Context(), f.idA.TenantID, f.idA.UserID, claim.ClaimToken)
		if err != nil || dispatch {
			t.Fatalf("missing identity dispatch=%v err=%v", dispatch, err)
		}
		assertTaskSuggestionNotificationStatus(
			t, f, feedbackID, "uncertain")
	})

	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *compiledRunWriteFixture)
	}{
		{
			name: "tenant inactive",
			mutate: func(t *testing.T, f *compiledRunWriteFixture) {
				t.Helper()
				if _, err := f.base.st.pool.Exec(t.Context(),
					`UPDATE tenants SET status='suspended'
					  WHERE id=$1`,
					f.idA.TenantID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "membership missing",
			mutate: func(t *testing.T, f *compiledRunWriteFixture) {
				t.Helper()
				if _, err := f.base.st.pool.Exec(t.Context(),
					`DELETE FROM memberships
					  WHERE tenant_id=$1 AND user_id=$2`,
					f.idA.TenantID, f.idA.UserID); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCompiledRunWriteFixture(t)
			feedbackID, _ := createOutdatedSuggestion(
				t, f, strings.ReplaceAll(tc.name, " ", "-"))
			installCurrentApprovedTaskPolicy(t, f, false)
			claim, err := f.base.st.ClaimTaskPolicySuggestion(
				t.Context(), f.idA.TenantID, f.idA.UserID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, f)
			dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
				t.Context(), f.idA.TenantID, f.idA.UserID,
				claim.ClaimToken)
			if err != nil || dispatch {
				t.Fatalf("unverifiable dispatch=%v err=%v",
					dispatch, err)
			}
			assertTaskSuggestionNotificationStatus(
				t, f, feedbackID, "uncertain")
		})
	}
}

func TestTaskPolicySuggestionDeletedTaskIsNotRequired(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	feedbackID, _ := createOutdatedSuggestion(t, f, "deleted-task")
	claim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`DELETE FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.idA.TenantID, f.idA.UserID, f.taskA); err != nil {
		t.Fatal(err)
	}

	dispatch, err := f.base.st.BeginTaskPolicySuggestionDispatch(
		ctx, f.idA.TenantID, f.idA.UserID, claim.ClaimToken)
	if err != nil || dispatch {
		t.Fatalf("deleted task dispatch=%v err=%v, want suppressed",
			dispatch, err)
	}
	assertTaskSuggestionNotificationStatus(
		t, f, feedbackID, "not_required")
	var lastError string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT COALESCE(notification_last_error,'')
		   FROM feedback_freshness_triage
		  WHERE tenant_id=$1 AND user_id=$2 AND feedback_id=$3`,
		f.idA.TenantID, f.idA.UserID, feedbackID,
	).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError != "source task is missing or deleted" {
		t.Fatalf("deleted task last error=%q", lastError)
	}
}

func TestBeginTaskPolicySuggestionDispatchLocksScheduleBeforeTriage(
	t *testing.T,
) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	feedbackID, _ := createOutdatedSuggestion(t, f, "schedule-first")
	installCurrentApprovedTaskPolicy(t, f, false)
	claim, err := f.base.st.ClaimTaskPolicySuggestion(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}

	blocker, err := f.base.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx,
		`SELECT id FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		  FOR UPDATE`,
		f.idA.TenantID, f.idA.UserID, f.taskA); err != nil {
		t.Fatal(err)
	}

	type dispatchResult struct {
		dispatch bool
		err      error
	}
	done := make(chan dispatchResult, 1)
	go func() {
		dispatch, beginErr := f.base.st.BeginTaskPolicySuggestionDispatch(
			context.Background(), f.idA.TenantID, f.idA.UserID,
			claim.ClaimToken)
		done <- dispatchResult{dispatch: dispatch, err: beginErr}
	}()
	time.Sleep(200 * time.Millisecond)

	if _, err := blocker.Exec(ctx, `SET LOCAL statement_timeout='1s'`); err != nil {
		t.Fatal(err)
	}
	var lockedFeedbackID int64
	if err := blocker.QueryRow(ctx,
		`SELECT feedback_id
		   FROM feedback_freshness_triage
		  WHERE feedback_id=$1
		  FOR UPDATE`,
		feedbackID).Scan(&lockedFeedbackID); err != nil {
		t.Fatalf("dispatch held triage while waiting for schedule: %v", err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-done:
		if result.err != nil || !result.dispatch {
			t.Fatalf("dispatch after schedule release=%v err=%v",
				result.dispatch, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not finish after schedule lock was released")
	}
}

func TestCurrentApprovedObservationPolicyRejectsInFlightEdit(
	t *testing.T,
) {
	f := newTaskDefinitionEditEntrypointFixture(t, true)
	f.acquireAndQuiesce(t, "observation-policy-in-flight:"+uuid.NewString())

	ctx := t.Context()
	tx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", f.base.TenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	state, err := currentApprovedObservationPolicyTx(
		ctx, tx, f.base.TenantID, f.base.UserID, f.base.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if state != currentObservationPolicyUnverifiable {
		t.Fatalf("in-flight edit policy state=%v, want unverifiable", state)
	}
}

func createOutdatedSuggestion(
	t *testing.T,
	f *compiledRunWriteFixture,
	suffix string,
) (int64, int64) {
	t.Helper()
	ctx := t.Context()
	batchKey := "freshness-current-policy-" + suffix + "-" + uuid.NewString()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, batchKey)
	if err != nil {
		t.Fatal(err)
	}
	contentID := f.createContent(t, f.sourceA, suffix)
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
			`DELETE FROM feedback_freshness_triage WHERE feedback_id=$1`,
			feedbackID)
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=$1`, feedbackID)
	})
	outcome, err := f.base.st.AuditOutdatedFeedback(
		ctx, f.idA.UserID, feedbackID)
	if err != nil ||
		outcome != types.FreshnessAuditTaskPolicySuggestion {
		t.Fatalf("audit outcome=%q err=%v", outcome, err)
	}
	return feedbackID, deliveryID
}

func installCurrentApprovedTaskPolicy(
	t *testing.T,
	f *compiledRunWriteFixture,
	withObservation bool,
) {
	t.Helper()
	ctx := t.Context()
	scopeJSON := currentTaskScopeJSON(t, f, withObservation)
	if _, err := f.base.st.pool.Exec(ctx,
		`UPDATE schedules SET scope_json=$2 WHERE id=$1`,
		f.taskA, scopeJSON); err != nil {
		t.Fatal(err)
	}
	result, err := f.base.st.reconcileTaskDefinitionBaseline(
		ctx, TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.idA.TenantID,
			UserID:   f.idA.UserID,
			TaskID:   f.taskA,
		})
	if err != nil || result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("install approved observation result=%+v err=%v", result, err)
	}
}

func advanceCurrentApprovedObservationPolicy(
	t *testing.T,
	f *compiledRunWriteFixture,
) {
	t.Helper()
	ctx := t.Context()
	current, err := f.base.st.GetCurrentApprovedDefinition(
		ctx, f.idA.TenantID, f.idA.UserID, f.taskA)
	if err != nil {
		t.Fatal(err)
	}
	definition := current.Definition
	definition.ScopeJSON = currentTaskScopeJSON(t, f, true)
	payload, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}
	digest := digestTaskStatePayload(payload)
	tx, err := f.base.st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`INSERT INTO task_approved_definition_versions (
			tenant_id,user_id,task_id,version,schema_version,
			execution_mode,definition_digest,payload,approval_ref
		 ) VALUES ($1,$2,$3,2,$4,$5,$6,$7,$8)`,
		f.idA.TenantID, f.idA.UserID, f.taskA,
		definition.SchemaVersion, definition.ExecutionMode,
		digest, payload, "observation-suppression-v2:"+f.taskA,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE schedules
		    SET scope_json=$4,approved_definition_version=2,
		        approved_definition_digest=$5
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.idA.TenantID, f.idA.UserID, f.taskA,
		definition.ScopeJSON, digest); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func currentTaskScopeJSON(
	t *testing.T,
	f *compiledRunWriteFixture,
	withObservation bool,
) json.RawMessage {
	t.Helper()
	var policy *observation.PolicyV1
	if withObservation {
		compiled, err := observation.Compile(observation.PolicySpecV1{
			Schema: observation.SchemaV1,
			Mode:   observation.ModeEvent,
			Window: observation.WindowSpecV1{
				Kind: observation.WindowScheduleInterval,
			},
			LatePolicy: observation.LateStrict,
			Evidence: observation.EvidencePolicyV1{
				Requirement:     observation.EvidenceOfficialRequired,
				OfficialDomains: []string{"openai.com"},
			},
			UnknownTime: observation.UnknownTimeReject,
			Event: &observation.EventPolicyV1{
				Subject:       "OpenAI models",
				EventKind:     "model release",
				Qualification: observation.QualificationGeneralAvailability,
			},
			QualifierPrompt: observation.QualifierPromptV1,
		}, time.Now().UTC().Add(-time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		policy = &compiled
	}
	scopeJSON, err := json.Marshal(struct {
		SourceIDs   []int64               `json:"source_ids,omitempty"`
		TopN        int                   `json:"top_n,omitempty"`
		Observation *observation.PolicyV1 `json:"observation,omitempty"`
	}{
		SourceIDs: []int64{f.sourceA}, TopN: 5, Observation: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	return scopeJSON
}

func assertTaskSuggestionNotificationStatus(
	t *testing.T,
	f *compiledRunWriteFixture,
	feedbackID int64,
	want string,
) {
	t.Helper()
	var (
		status     string
		claimToken *string
		leaseUntil *time.Time
	)
	if err := f.base.st.pool.QueryRow(t.Context(),
		`SELECT notification_status,notification_claim_token,
		        notification_lease_until
		   FROM feedback_freshness_triage WHERE feedback_id=$1`,
		feedbackID,
	).Scan(&status, &claimToken, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if status != want || claimToken != nil || leaseUntil != nil {
		t.Fatalf("notification status=%q token=%v lease=%v, want %q/NULL/NULL",
			status, claimToken, leaseUntil, want)
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
	key := "partial-failure-" + uuid.NewString()
	batchID := createObservationBatch(t, f, f.idA, f.refA, key)
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, f.idA, f.refA, batchID, event); err != nil || !ok {
		t.Fatalf("initial reserve accepted=%v err=%v", ok, err)
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
		ctx, f.idA, f.refA, event.PolicyDigest, event.EventKey,
		batchID, deliveryID,
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
	replacementKey := "partial-failure-replacement-" + uuid.NewString()
	replacementBatch := createObservationBatch(
		t, f, nextIdentity, nextRef, replacementKey)
	candidates, err := f.base.st.ListUnpushedForTaskRunV1(
		ctx, nextIdentity, nextRef, []int64{f.sourceA}, 10, 10)
	if err != nil || len(candidates) != 1 || candidates[0].ID != contentID {
		t.Fatalf("recovery candidates=%+v err=%v", candidates, err)
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, nextIdentity, nextRef, replacementBatch, event); err != nil || !ok {
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
		event.PolicyDigest, event.EventKey,
		replacementBatch, replacementDelivery,
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
	thirdBatch := createObservationBatch(
		t, f, thirdIdentity, thirdRef,
		"partial-failure-third-"+uuid.NewString())
	candidates, err = f.base.st.ListUnpushedForTaskRunV1(
		ctx, thirdIdentity, thirdRef, []int64{f.sourceA}, 10, 10)
	if err != nil || len(candidates) != 1 || candidates[0].ID != contentID {
		t.Fatalf("second recovery candidates=%+v err=%v", candidates, err)
	}
	if ok, err := f.base.st.ReserveObservedEventV1(
		ctx, thirdIdentity, thirdRef, thirdBatch, event); err != nil || !ok {
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

func createObservationBatch(
	t *testing.T,
	f *compiledRunWriteFixture,
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	key string,
) int64 {
	t.Helper()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		t.Context(), identity, ref, key)
	if err != nil {
		t.Fatal(err)
	}
	winner, err := f.base.st.ClaimPushBatchDeliveryAuthority(
		t.Context(),
		types.PushBatchScope{
			TenantID: identity.TenantID,
			UserID:   identity.UserID,
			BatchID:  batchID,
		},
		types.PushBatchDeliveryAuthorityEffect,
	)
	if err != nil {
		t.Fatal(err)
	}
	if winner != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("observation batch authority=%q", winner)
	}
	return batchID
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
