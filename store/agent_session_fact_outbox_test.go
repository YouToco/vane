package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
	"github.com/jackc/pgx/v5"
)

type agentSessionFactFixture struct {
	agentEventFixture
	batchID    int64
	deliveryID int64
}

func newAgentSessionFactFixture(t *testing.T) agentSessionFactFixture {
	t.Helper()
	base := newAgentEventFixture(t)
	ctx := t.Context()
	var batchID, deliveryID int64
	if err := base.store.pool.QueryRow(ctx,
		`INSERT INTO push_batches (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		base.tenantA, base.userA,
	).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	if err := base.store.pool.QueryRow(ctx,
		`INSERT INTO deliveries (tenant_id,batch_id,user_id)
		 VALUES ($1,$2,$3) RETURNING id`,
		base.tenantA, batchID, base.userA,
	).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, statement := range []string{
			`DELETE FROM agent_session_fact_outbox WHERE tenant_id=$1`,
			`DELETE FROM feedback_freshness_triage WHERE tenant_id=$1`,
			`DELETE FROM feedbacks WHERE tenant_id=$1`,
			`DELETE FROM deliveries WHERE tenant_id=$1`,
			`DELETE FROM push_batches WHERE tenant_id=$1`,
			`DELETE FROM agent_session_projection_authority_events
			  WHERE tenant_id=$1`,
			`DELETE FROM agent_events WHERE tenant_id=$1`,
		} {
			cleanupExec(ctx, t, base.store, statement, base.tenantA)
		}
	})
	return agentSessionFactFixture{
		agentEventFixture: base,
		batchID:           batchID,
		deliveryID:        deliveryID,
	}
}

func (f agentSessionFactFixture) insertFeedback(
	t *testing.T,
	action types.FeedbackAction,
) int64 {
	t.Helper()
	id, err := f.store.InsertFeedback(t.Context(), &types.Feedback{
		UserID: f.userA, DeliveryID: f.deliveryID, Action: action,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (f agentSessionFactFixture) loadFact(
	t *testing.T,
	feedbackID int64,
) AgentSessionFact {
	t.Helper()
	fact, err := scanAgentSessionFact(f.store.pool.QueryRow(t.Context(),
		`SELECT `+agentSessionFactColumns+`
		   FROM agent_session_fact_outbox
		  WHERE tenant_id=$1 AND fact_id=$2`,
		f.tenantA, feedbackID,
	))
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func acquireFact(
	t *testing.T,
	f agentSessionFactFixture,
	fact AgentSessionFact,
	owner string,
) AgentSessionFactLease {
	t.Helper()
	acquired, err := f.store.AcquireAgentSessionFact(
		t.Context(), AcquireAgentSessionFactParams{
			ID: fact.ID, TenantID: fact.TenantID, UserID: fact.UserID,
			LeaseOwner: owner, LeaseDuration: time.Minute,
		})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := acquired.Lease()
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func TestAgentSessionFactFeedbackFreezesExactSessionAndReplays(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	ctx := t.Context()
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	fact := f.loadFact(t, feedbackID)
	if fact.SessionID == nil || *fact.SessionID != f.sessionA ||
		fact.Status != AgentSessionFactStatusPending ||
		fact.SourceIdentity != fmt.Sprintf("feedback-click:%d", feedbackID) {
		t.Fatalf("frozen fact=%+v", fact)
	}

	// A later active session must never receive the already-frozen fact.
	var laterSessionID int64
	if err := f.store.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		f.tenantA, f.userA,
	).Scan(&laterSessionID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExec(context.Background(), t, f.store,
			`DELETE FROM agent_sessions WHERE id=$1`, laterSessionID)
	})

	lease := acquireFact(t, f, fact, "fact-exact-session")
	if err := f.store.ProjectAgentSessionFact(ctx, lease); err != nil {
		t.Fatal(err)
	}
	// Simulate a committed response whose acknowledgement was lost.
	if err := f.store.ProjectAgentSessionFact(ctx, lease); err != nil {
		t.Fatalf("exact response-loss replay: %v", err)
	}
	fact = f.loadFact(t, feedbackID)
	if fact.Status != AgentSessionFactStatusCompleted ||
		fact.SessionRecordedAt == nil {
		t.Fatalf("completed fact=%+v", fact)
	}
	assertSessionContainsFeedbackFact(
		t, f.store, f.sessionA, feedbackID, true)
	assertSessionContainsFeedbackFact(
		t, f.store, laterSessionID, feedbackID, false)
}

func TestAgentSessionFactCompletedReplayNeverReconstructsMissingBatch(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	fact := f.loadFact(t, feedbackID)
	lease := acquireFact(t, f, fact, "fact-completed-missing")
	if err := f.store.ProjectAgentSessionFact(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	key, _, err := agentSessionAppendIdentity(
		lease.Source, json.RawMessage(lease.Messages))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(),
		`DELETE FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key=$4`,
		lease.TenantID, lease.UserID, lease.SessionID, key,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentSessionFact(
		t.Context(), lease,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("missing completed replay evidence err=%v", err)
	}
	var events int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key=$4`,
		lease.TenantID, lease.UserID, lease.SessionID, key,
	).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("completed replay reconstructed %d events", events)
	}
	if got := f.loadFact(t, feedbackID).Status; got !=
		AgentSessionFactStatusCompleted {
		t.Fatalf("completed checkpoint changed to %q", got)
	}
}

func TestAgentSessionFactCompletedReplayRejectsDamagedBatch(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	fact := f.loadFact(t, feedbackID)
	lease := acquireFact(t, f, fact, "fact-completed-damaged")
	if err := f.store.ProjectAgentSessionFact(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	key, _, err := agentSessionAppendIdentity(
		lease.Source, json.RawMessage(lease.Messages))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE agent_events SET payload_digest=repeat('a',64)
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key=$4 AND batch_index=0`,
		lease.TenantID, lease.UserID, lease.SessionID, key,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentSessionFact(
		t.Context(), lease,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("damaged completed replay evidence err=%v", err)
	}
	var digest string
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT payload_digest FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key=$4 AND batch_index=0`,
		lease.TenantID, lease.UserID, lease.SessionID, key,
	).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest != strings.Repeat("a", 64) {
		t.Fatalf("completed replay rewrote damaged digest=%q", digest)
	}
}

func TestAgentSessionFactOutboxFailureRollsBackFeedback(t *testing.T) {
	f := newAgentSessionFactFixture(t)
	faultStore := *f.store
	faultStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		realTx, err := f.store.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &compiledTaskFaultTx{
			Tx:           realTx,
			failContains: "INSERT INTO agent_session_fact_outbox",
		}, nil
	}
	_, err := faultStore.InsertFeedbackWithSessionCutoff(
		t.Context(), &types.Feedback{
			UserID: f.userA, DeliveryID: f.deliveryID,
			Action: types.FeedbackActionInterested,
		}, time.Now().Add(-time.Hour))
	if !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("outbox fault err=%v", err)
	}
	var feedbacks, facts int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM feedbacks
		      WHERE tenant_id=$1 AND delivery_id=$2),
		    (SELECT count(*) FROM agent_session_fact_outbox
		      WHERE tenant_id=$1)`,
		f.tenantA, f.deliveryID,
	).Scan(&feedbacks, &facts); err != nil {
		t.Fatal(err)
	}
	if feedbacks != 0 || facts != 0 {
		t.Fatalf("atomic rollback feedbacks=%d facts=%d", feedbacks, facts)
	}
}

func TestAgentSessionFactAppendFailureRollsBackSavepointBeforeBlock(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	ctx := t.Context()
	for _, statement := range []string{
		`CREATE FUNCTION m056_corrupt_second_event()
		   RETURNS trigger LANGUAGE plpgsql AS $$
		   BEGIN
		     IF NEW.batch_index = 1 AND
		        NEW.batch_idempotency_key LIKE 'side.%' THEN
		       NEW.payload_digest := repeat('a',64);
		     END IF;
		     RETURN NEW;
		   END $$`,
		`CREATE TRIGGER m056_corrupt_second_event
		   BEFORE INSERT ON agent_events
		   FOR EACH ROW EXECUTE FUNCTION m056_corrupt_second_event()`,
	} {
		if _, err := f.store.pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanupExec(context.Background(), t, f.store,
			`DROP TRIGGER IF EXISTS m056_corrupt_second_event
			   ON agent_events`)
		cleanupExec(context.Background(), t, f.store,
			`DROP FUNCTION IF EXISTS m056_corrupt_second_event()`)
	})

	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	fact := f.loadFact(t, feedbackID)
	lease := acquireFact(t, f, fact, "fact-savepoint-rollback")
	key, _, err := agentSessionAppendIdentity(
		lease.Source, json.RawMessage(lease.Messages))
	if err != nil {
		t.Fatal(err)
	}
	var beforeMessages []byte
	if err := f.store.pool.QueryRow(ctx,
		`SELECT messages FROM agent_sessions WHERE id=$1`,
		lease.SessionID,
	).Scan(&beforeMessages); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentSessionFact(ctx, lease); err != nil {
		t.Fatalf("post-write integrity failure must checkpoint blocked: %v", err)
	}
	blocked := f.loadFact(t, feedbackID)
	if blocked.Status != AgentSessionFactStatusBlocked ||
		blocked.BlockedReason == nil ||
		*blocked.BlockedReason != "projection_integrity" {
		t.Fatalf("blocked fact=%+v", blocked)
	}
	var events int
	var afterMessages []byte
	if err := f.store.pool.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM agent_events
		      WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		        AND batch_idempotency_key=$4),
		    (SELECT messages FROM agent_sessions WHERE id=$3)`,
		lease.TenantID, lease.UserID, lease.SessionID, key,
	).Scan(&events, &afterMessages); err != nil {
		t.Fatal(err)
	}
	if events != 0 || string(afterMessages) != string(beforeMessages) {
		t.Fatalf(
			"savepoint leaked helper writes events=%d before=%s after=%s",
			events, beforeMessages, afterMessages)
	}
}

func TestAgentSessionFactQuestionAndDeepDiveStayOutsideContinuation(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	for _, action := range []types.FeedbackAction{
		types.FeedbackActionQuestion,
		types.FeedbackActionDeepDive,
	} {
		feedbackID, err := f.store.InsertFeedback(
			t.Context(), &types.Feedback{
				UserID: f.userA, DeliveryID: f.deliveryID,
				Action: action, Detail: "already handled by its own flow",
			})
		if err != nil {
			t.Fatal(err)
		}
		var facts int
		if err := f.store.pool.QueryRow(t.Context(),
			`SELECT count(*) FROM agent_session_fact_outbox
			  WHERE tenant_id=$1 AND fact_id=$2`,
			f.tenantA, feedbackID,
		).Scan(&facts); err != nil {
			t.Fatal(err)
		}
		if facts != 0 {
			t.Fatalf("action=%s continuation facts=%d", action, facts)
		}
	}
}

func TestAgentSessionFactLedgerRouteAndProjectionDriftBlock(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	ctx := t.Context()
	firstID := f.insertFeedback(t, types.FeedbackActionInterested)
	first := f.loadFact(t, firstID)
	firstLease := acquireFact(t, f, first, "fact-ledger-seed")
	if err := f.store.ProjectAgentSessionFact(ctx, firstLease); err != nil {
		t.Fatal(err)
	}
	status, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, f.tenantA, f.userA, f.sessionA,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil || status.Route != AgentSessionProjectionRouteLedger {
		t.Fatalf("activate ledger: status=%+v err=%v", status, err)
	}

	secondID := f.insertFeedback(t, types.FeedbackActionNotInterested)
	second := f.loadFact(t, secondID)
	secondLease := acquireFact(t, f, second, "fact-ledger-route")
	if err := f.store.ProjectAgentSessionFact(ctx, secondLease); err != nil {
		t.Fatal(err)
	}
	assertSessionContainsFeedbackFact(
		t, f.store, f.sessionA, secondID, true)

	thirdID := f.insertFeedback(t, types.FeedbackActionInterested)
	third := f.loadFact(t, thirdID)
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_sessions
		    SET messages=messages ||
		        '[{"role":"user","content":"owner drift"}]'::jsonb
		  WHERE id=$1`,
		f.sessionA,
	); err != nil {
		t.Fatal(err)
	}
	thirdLease := acquireFact(t, f, third, "fact-drift")
	if err := f.store.ProjectAgentSessionFact(ctx, thirdLease); err != nil {
		t.Fatalf("integrity drift must converge to blocked checkpoint: %v", err)
	}
	third = f.loadFact(t, thirdID)
	if third.Status != AgentSessionFactStatusBlocked ||
		third.BlockedReason == nil ||
		*third.BlockedReason != "projection_integrity" {
		t.Fatalf("drifted fact=%+v", third)
	}
}

func TestAgentSessionFactNoActiveSessionSuppressedAndCorruptBlocked(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	ctx := t.Context()
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_sessions
		    SET status='active',
		        updated_at=clock_timestamp()-interval '2 hours'
		  WHERE id=$1`,
		f.sessionA,
	); err != nil {
		t.Fatal(err)
	}
	suppressedID, err := f.store.InsertFeedbackWithSessionCutoff(
		ctx, &types.Feedback{
			UserID: f.userA, DeliveryID: f.deliveryID,
			Action: types.FeedbackActionInterested,
		}, time.Now().Add(-30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	suppressed := f.loadFact(t, suppressedID)
	if suppressed.Status != AgentSessionFactStatusSuppressed ||
		suppressed.SessionID != nil ||
		suppressed.SuppressionReason == nil ||
		*suppressed.SuppressionReason != agentSessionFactSuppressedNoActive {
		t.Fatalf("suppressed fact=%+v", suppressed)
	}
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_sessions
		    SET status='active',updated_at=clock_timestamp()
		  WHERE id=$1`,
		f.sessionA,
	); err != nil {
		t.Fatal(err)
	}
	corruptID := f.insertFeedback(
		t, types.FeedbackActionNotInterested)
	corrupt := f.loadFact(t, corruptID)
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_session_fact_outbox
		    SET payload_digest=repeat('a',64)
		  WHERE id=$1`,
		corrupt.ID,
	); err != nil {
		t.Fatal(err)
	}
	corrupt = f.loadFact(t, corruptID)
	lease := acquireFact(t, f, corrupt, "fact-corrupt")
	if err := f.store.ProjectAgentSessionFact(ctx, lease); err != nil {
		t.Fatalf("corruption must checkpoint blocked: %v", err)
	}
	corrupt = f.loadFact(t, corruptID)
	if corrupt.Status != AgentSessionFactStatusBlocked ||
		corrupt.BlockedReason == nil ||
		*corrupt.BlockedReason != "payload_integrity" {
		t.Fatalf("corrupt fact=%+v", corrupt)
	}
	assertSessionContainsFeedbackFact(
		t, f.store, f.sessionA, corruptID, false)
}

func TestAgentSessionFactConcurrentAcquireHasOneWinner(t *testing.T) {
	f := newAgentSessionFactFixture(t)
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	fact := f.loadFact(t, feedbackID)

	const workers = 8
	start := make(chan struct{})
	var wait sync.WaitGroup
	var mutex sync.Mutex
	winners := 0
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, err := f.store.AcquireAgentSessionFact(
				context.Background(), AcquireAgentSessionFactParams{
					ID: fact.ID, TenantID: fact.TenantID,
					UserID:        fact.UserID,
					LeaseOwner:    fmt.Sprintf("fact-worker-%d", index),
					LeaseDuration: time.Minute,
				})
			if err == nil {
				mutex.Lock()
				winners++
				mutex.Unlock()
				return
			}
			if !errors.Is(err, ErrAgentSessionFactBusy) {
				t.Errorf("worker %d: %v", index, err)
			}
		}(i)
	}
	close(start)
	wait.Wait()
	if winners != 1 {
		t.Fatalf("acquire winners=%d want=1", winners)
	}
}

func TestAgentSessionFactLegacyMisjudgedReplayDoesNotCreateOutbox(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	firstID, err := f.store.InsertFeedback(
		t.Context(), &types.Feedback{
			UserID: f.userA, DeliveryID: f.deliveryID,
			Action:     types.FeedbackActionMisjudged,
			ReasonCode: types.FeedbackReasonOther,
			Detail:     "first",
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(),
		`DELETE FROM feedback_freshness_triage WHERE feedback_id=$1`,
		firstID,
	); err != nil {
		t.Fatal(err)
	}
	replayedID, err := f.store.InsertFeedback(
		t.Context(), &types.Feedback{
			UserID: f.userA, DeliveryID: f.deliveryID,
			Action:     types.FeedbackActionMisjudged,
			ReasonCode: types.FeedbackReasonDuplicate,
		})
	if err != nil {
		t.Fatal(err)
	}
	if replayedID != firstID {
		t.Fatalf("misjudged replay id=%d want=%d", replayedID, firstID)
	}
	var feedbacks, facts, triage int
	var triageReason, triageDetail, triageOutcome string
	var auditReason, auditDetail string
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM feedbacks
		      WHERE tenant_id=$1 AND delivery_id=$2
		        AND action='misjudged'),
		    (SELECT count(*) FROM agent_session_fact_outbox
		      WHERE tenant_id=$1 AND fact_type='feedback'
		        AND fact_id=$3),
		    (SELECT count(*) FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND feedback_id=$3),
		    (SELECT reason_code FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND feedback_id=$3),
		    (SELECT detail FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND feedback_id=$3),
		    (SELECT outcome FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND feedback_id=$3),
		    (SELECT audit_json->>'reason_code'
		       FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND feedback_id=$3),
		    (SELECT audit_json->>'detail'
		       FROM feedback_freshness_triage
		      WHERE tenant_id=$1 AND feedback_id=$3)`,
		f.tenantA, f.deliveryID, firstID,
	).Scan(
		&feedbacks, &facts, &triage,
		&triageReason, &triageDetail, &triageOutcome,
		&auditReason, &auditDetail,
	); err != nil {
		t.Fatal(err)
	}
	if feedbacks != 1 || facts != 1 || triage != 1 ||
		triageReason != string(types.FeedbackReasonOther) ||
		triageDetail != "first" || triageOutcome != "manual_review" ||
		auditReason != triageReason || auditDetail != triageDetail {
		t.Fatalf(
			"feedbacks=%d facts=%d triage=%d reason=%q detail=%q "+
				"outcome=%q audit_reason=%q audit_detail=%q",
			feedbacks, facts, triage,
			triageReason, triageDetail, triageOutcome,
			auditReason, auditDetail)
	}
}

func TestAgentSessionFactProjectionAndTenantPurgeConverge(
	t *testing.T,
) {
	f := newAgentSessionFactFixture(t)
	feedbackID := f.insertFeedback(t, types.FeedbackActionInterested)
	fact := f.loadFact(t, feedbackID)
	lease := acquireFact(t, f, fact, "fact-purge-race")

	type purgeResult struct {
		report *PurgeReport
		err    error
	}
	start := make(chan struct{})
	projectDone := make(chan error, 1)
	purgeDone := make(chan purgeResult, 1)
	go func() {
		<-start
		projectDone <- f.store.ProjectAgentSessionFact(
			context.Background(), lease)
	}()
	go func() {
		<-start
		report, err := f.store.PurgeTenant(
			context.Background(), f.tenantA, false)
		purgeDone <- purgeResult{report: report, err: err}
	}()
	close(start)
	select {
	case err := <-projectDone:
		if err != nil && !errors.Is(err, types.ErrNotFound) &&
			!errors.Is(err, ErrAgentSessionFactBusy) {
			t.Fatalf("project race: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("continuation projection deadlocked with tenant purge")
	}
	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatalf("purge race: %v", result.err)
		}
		if result.report == nil ||
			result.report.Rows["agent_session_fact_outbox"] != 1 ||
			result.report.Rows["tenants"] != 1 {
			t.Fatalf("purge report=%+v", result.report)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant purge deadlocked with continuation projection")
	}
	var facts, sessions, tenants int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM agent_session_fact_outbox
		      WHERE tenant_id=$1),
		    (SELECT count(*) FROM agent_sessions WHERE tenant_id=$1),
		    (SELECT count(*) FROM tenants WHERE id=$1)`,
		f.tenantA,
	).Scan(&facts, &sessions, &tenants); err != nil {
		t.Fatal(err)
	}
	if facts != 0 || sessions != 0 || tenants != 0 {
		t.Fatalf("purge residue facts=%d sessions=%d tenants=%d",
			facts, sessions, tenants)
	}
}

func assertSessionContainsFeedbackFact(
	t *testing.T,
	st *Store,
	sessionID int64,
	feedbackID int64,
	want bool,
) {
	t.Helper()
	var raw []byte
	if err := st.pool.QueryRow(t.Context(),
		`SELECT messages FROM agent_sessions WHERE id=$1`,
		sessionID,
	).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	needle := fmt.Sprintf("feedback-click:%d", feedbackID)
	// The stable identity is in the ledger; the user-facing message contains
	// the delivery/action only. Inspect the exact batch identity separately.
	sum := sha256.Sum256([]byte(
		"vane.agent-side-writer/v1\x00" + needle))
	key := "side." + fmt.Sprintf("%x", sum[:])
	var count int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_events
		  WHERE session_id=$1
		    AND batch_idempotency_key=$2`,
		sessionID, key,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	got := count > 0
	if got != want {
		t.Fatalf("session %d feedback %d present=%v want=%v messages=%s",
			sessionID, feedbackID, got, want, raw)
	}
}
