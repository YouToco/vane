package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type agentActionProjectionFaultFixture struct {
	agentEventFixture
	sourceID int64
	actionID string
	lease    AgentActionContinuationLease
	eventKey string
}

func newAgentActionProjectionFaultFixture(
	t *testing.T,
	tag string,
) agentActionProjectionFaultFixture {
	t.Helper()
	f := newAgentEventFixture(t)
	ctx := t.Context()
	sourceID, _, err := f.store.UpsertSource(ctx, &types.Source{
		Platform: types.PlatformWeb, Capability: types.CapFeed,
		URL: "https://example.com/action-fault-" + tag + "-" +
			uuid.NewString(),
		Title: "action fault " + tag,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.AddSubscription(ctx, f.userA, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE sources SET status='disabled',fail_count=9
		  WHERE id=$1`,
		sourceID,
	); err != nil {
		t.Fatal(err)
	}
	actionID := uuid.NewString()
	if err := f.store.CreatePendingAction(
		ctx,
		&types.PendingAction{
			ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
			ToolName: "enable_source",
			Args: []byte(fmt.Sprintf(
				`{"source_id":%d}`, sourceID,
			)),
			Summary:   "fault projection",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ActivateAgentActionContinuation(
		ctx, f.tenantA, f.userA, actionID, "fault projection",
	); err != nil {
		t.Fatal(err)
	}
	if outcome, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, actionID,
	); err != nil || !outcome.Accepted {
		t.Fatalf("confirm outcome=%+v err=%v", outcome, err)
	}
	acquired, err := f.store.AcquireAgentActionContinuation(
		ctx, actionID, f.tenantA, f.userA,
		"fault-"+tag+"-"+actionID, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := acquired.Lease()
	if err != nil {
		t.Fatal(err)
	}
	var messages []byte
	if err := f.store.pool.QueryRow(ctx,
		`SELECT success_messages FROM agent_action_continuations
		  WHERE action_id=$1`,
		actionID,
	).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	eventKey, _, err := agentSessionAppendIdentity(
		"agent-action:enable-source:"+actionID,
		json.RawMessage(messages),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=$1`, actionID,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM subscriptions
			  WHERE tenant_id=$1 AND user_id=$2 AND source_id=$3`,
			f.tenantA, f.userA, sourceID,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM sources WHERE id=$1`, sourceID,
		)
	})
	return agentActionProjectionFaultFixture{
		agentEventFixture: f, sourceID: sourceID,
		actionID: actionID, lease: lease, eventKey: eventKey,
	}
}

func (f agentActionProjectionFaultFixture) assertState(
	t *testing.T,
	wantSource, wantAction string,
	wantEvents int,
) {
	t.Helper()
	var sourceStatus, actionStatus string
	var events int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT status FROM sources WHERE id=$1),
		    (SELECT status FROM agent_action_continuations
		      WHERE action_id=$2),
		    (SELECT count(*) FROM agent_events
		      WHERE tenant_id=$3 AND session_id=$4
		        AND batch_idempotency_key=$5)`,
		f.sourceID, f.actionID, f.tenantA, f.sessionA, f.eventKey,
	).Scan(&sourceStatus, &actionStatus, &events); err != nil {
		t.Fatal(err)
	}
	if sourceStatus != wantSource || actionStatus != wantAction ||
		events != wantEvents {
		t.Fatalf(
			"source/action/events=%s/%s/%d want=%s/%s/%d",
			sourceStatus, actionStatus, events,
			wantSource, wantAction, wantEvents,
		)
	}
}

func requireRetryableDatabaseError(t *testing.T, err error) {
	t.Helper()
	var appErr *types.AppError
	if !errors.As(err, &appErr) ||
		appErr.Code != types.CodeDatabase || !appErr.Retryable {
		t.Fatalf("error=%v, want retryable CodeDatabase", err)
	}
}

func TestAgentActionProjectionScanFailureRollsBackAndRetries(t *testing.T) {
	f := newAgentActionProjectionFaultFixture(t, "infinity")
	ctx := t.Context()
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET next_attempt_at='infinity'::timestamptz
		  WHERE action_id=$1`,
		f.actionID,
	); err != nil {
		t.Fatal(err)
	}
	err := f.store.ProjectAgentActionContinuation(ctx, f.lease)
	requireRetryableDatabaseError(t, err)
	f.assertState(
		t, string(types.SourceStatusDisabled),
		AgentActionStatusConfirmed, 0,
	)
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET next_attempt_at=clock_timestamp()
		  WHERE action_id=$1`,
		f.actionID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(
		ctx, f.lease,
	); err != nil {
		t.Fatalf("projection after scan repair: %v", err)
	}
	f.assertState(
		t, string(types.SourceStatusActive),
		AgentActionStatusCompleted, 3,
	)
}

func TestAgentActionProjectionBackendTerminationIsAtomic(t *testing.T) {
	f := newAgentActionProjectionFaultFixture(t, "terminate")
	ctx := t.Context()
	blocker, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.WithoutCancel(ctx)) }()
	var sourceID int64
	if err := blocker.QueryRow(ctx,
		`SELECT id FROM sources WHERE id=$1 FOR UPDATE`,
		f.sourceID,
	).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}

	projectDone := make(chan error, 1)
	go func() {
		projectDone <- f.store.ProjectAgentActionContinuation(ctx, f.lease)
	}()
	pid := waitForBlockedAgentActionSourceUpdate(t, f.store)
	var terminated bool
	if err := f.store.pool.QueryRow(ctx,
		`SELECT pg_terminate_backend($1)`, pid,
	).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatalf("backend %d was not terminated", pid)
	}
	select {
	case err := <-projectDone:
		requireRetryableDatabaseError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("terminated projection did not return")
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	f.assertState(
		t, string(types.SourceStatusDisabled),
		AgentActionStatusConfirmed, 0,
	)

	if err := f.store.ReleaseAgentActionContinuation(
		ctx, f.lease, 5*time.Millisecond,
	); err != nil {
		t.Fatalf("release after disconnect: %v", err)
	}
	if _, err := f.store.pool.Exec(ctx, `SELECT pg_sleep(0.02)`); err != nil {
		t.Fatal(err)
	}
	acquired, err := f.store.AcquireAgentActionContinuation(
		ctx, f.actionID, f.tenantA, f.userA,
		"fault-retry-"+f.actionID, time.Minute,
	)
	if err != nil {
		t.Fatalf("reacquire after backoff: %v", err)
	}
	retryLease, err := acquired.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if retryLease.Fence != f.lease.Fence+1 {
		t.Fatalf(
			"retry fence=%d want=%d",
			retryLease.Fence, f.lease.Fence+1,
		)
	}
	if err := f.store.ProjectAgentActionContinuation(
		ctx, retryLease,
	); err != nil {
		t.Fatalf("retry projection: %v", err)
	}
	f.assertState(
		t, string(types.SourceStatusActive),
		AgentActionStatusCompleted, 3,
	)
	if err := f.store.ProjectAgentActionContinuation(
		ctx, retryLease,
	); err != nil {
		t.Fatalf("completed replay: %v", err)
	}
	f.assertState(
		t, string(types.SourceStatusActive),
		AgentActionStatusCompleted, 3,
	)
}

func waitForBlockedAgentActionSourceUpdate(
	t *testing.T,
	st *Store,
) int32 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var pid int32
		err := st.pool.QueryRow(t.Context(), `
			SELECT pid
			  FROM pg_stat_activity
			 WHERE datname=current_database()
			   AND pid<>pg_backend_pid()
			   AND wait_event_type='Lock'
			   AND query LIKE '%UPDATE sources%'
			   AND query LIKE
			       '%SET status=$4,fail_count=0,next_fetch_at=clock_timestamp(),%'
			 ORDER BY pid
			 LIMIT 1`).Scan(&pid)
		if err == nil {
			return pid
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("projection did not block on source update")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
