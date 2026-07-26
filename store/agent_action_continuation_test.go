package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestAgentActionContinuationDefaultOffPromoteAndRollback(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionIDs := make([]string, 0, 4)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`,
			actionIDs,
		)
	})
	create := func(sourceID int64) string {
		t.Helper()
		actionID := uuid.NewString()
		actionIDs = append(actionIDs, actionID)
		if err := f.store.CreatePendingAction(ctx, &types.PendingAction{
			ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
			ToolName:  "enable_source",
			Args:      []byte(fmt.Sprintf(`{"source_id":%d}`, sourceID)),
			Summary:   "重新启用信源",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		return actionID
	}

	t.Run("default off remains legacy v0", func(t *testing.T) {
		actionID := create(900001)
		claimed, err := f.store.ClaimPendingAction(
			ctx, actionID, f.userA,
		)
		if err != nil {
			t.Fatalf("legacy ClaimPendingAction: %v", err)
		}
		if claimed.ToolName != "enable_source" {
			t.Fatalf("legacy claimed tool=%q", claimed.ToolName)
		}
		var continuations int
		if err := f.store.pool.QueryRow(ctx,
			`SELECT count(*) FROM agent_action_continuations
			  WHERE action_id=$1`,
			actionID,
		).Scan(&continuations); err != nil {
			t.Fatal(err)
		}
		if continuations != 0 {
			t.Fatalf("default-off continuation rows=%d", continuations)
		}
	})

	t.Run("exact activation promotes then confirms", func(t *testing.T) {
		actionID := create(900002)
		generation, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "test exact canary",
		)
		if err != nil || generation != 1 {
			t.Fatalf("activate generation=%d err=%v", generation, err)
		}
		if _, err := f.store.ClaimPendingAction(
			ctx, actionID, f.userA,
		); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("legacy Claim reached promoted action: %v", err)
		}
		outcome, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, actionID,
		)
		if err != nil || !outcome.Handled || !outcome.Accepted ||
			outcome.Status != AgentActionStatusConfirmed {
			t.Fatalf("confirm outcome=%+v err=%v", outcome, err)
		}
		if _, err := f.store.RollbackAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "too late",
		); !errors.Is(err, ErrAgentActionTerminal) {
			t.Fatalf("confirmed rollback err=%v", err)
		}
	})

	t.Run("pristine rollback demotes once", func(t *testing.T) {
		actionID := create(900003)
		if _, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "test rollback canary",
		); err != nil {
			t.Fatal(err)
		}
		generation, err := f.store.RollbackAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "test rollback",
		)
		if err != nil || generation != 2 {
			t.Fatalf("rollback generation=%d err=%v", generation, err)
		}
		replay, err := f.store.RollbackAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "test rollback",
		)
		if err != nil || replay != generation {
			t.Fatalf("rollback replay generation=%d err=%v", replay, err)
		}
		if _, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "forbidden reactivate",
		); !errors.Is(err, ErrAgentActionTerminal) {
			t.Fatalf("reactivate rolled-back action err=%v", err)
		}
		if _, err := f.store.ClaimPendingAction(
			ctx, actionID, f.userA,
		); err != nil {
			t.Fatalf("rolled-back v0 Claim: %v", err)
		}
	})

	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{
			name: "tool drift",
			sql:  `UPDATE pending_actions SET tool_name='list_sources' WHERE id=$1`,
		},
		{
			name: "arguments drift",
			sql:  `UPDATE pending_actions SET args='{"source_id":999999}'::jsonb WHERE id=$1`,
		},
	} {
		t.Run("rollback rejects "+mutation.name, func(t *testing.T) {
			actionID := create(900004)
			if _, err := f.store.ActivateAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID, "drift canary",
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.pool.Exec(
				ctx, mutation.sql, actionID,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.RollbackAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID, "must reject drift",
			); err == nil {
				t.Fatal("rollback accepted a drifted authority root")
			}
			var executionVersion, authorityEvents int
			var continuationStatus string
			if err := f.store.pool.QueryRow(ctx,
				`SELECT p.execution_version,c.status,
				        (SELECT count(*)
				           FROM agent_action_continuation_authority_events e
				          WHERE e.action_id=p.id)
				   FROM pending_actions p
				   JOIN agent_action_continuations c ON c.action_id=p.id
				  WHERE p.id=$1`,
				actionID,
			).Scan(
				&executionVersion, &continuationStatus, &authorityEvents,
			); err != nil {
				t.Fatal(err)
			}
			if executionVersion != AgentActionExecutionVersion ||
				continuationStatus != AgentActionStatusPending ||
				authorityEvents != 1 {
				t.Fatalf(
					"drift rollback mutated version/status/history=%d/%s/%d",
					executionVersion, continuationStatus, authorityEvents,
				)
			}
		})
	}
}

func TestAgentActionContinuationEffectAndSessionCheckpointAreAtomic(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	sourceIDs := make([]int64, 0, 2)
	actionIDs := make([]string, 0, 2)
	var laterSessionID int64
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`, actionIDs,
		)
		if laterSessionID > 0 {
			cleanupExec(
				cleanupCtx, t, f.store,
				`DELETE FROM agent_sessions WHERE id=$1`, laterSessionID,
			)
		}
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM subscriptions WHERE source_id=ANY($1)`, sourceIDs,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM sources WHERE id=ANY($1)`, sourceIDs,
		)
	})
	createSource := func(tag string) int64 {
		t.Helper()
		sourceID, _, err := f.store.UpsertSource(ctx, &types.Source{
			Platform: types.PlatformWeb, Capability: types.CapFeed,
			URL: "https://example.com/action-continuation-" +
				tag + "-" + uuid.NewString(),
			Title: "action continuation " + tag,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		if err := f.store.AddSubscription(ctx, f.userA, sourceID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.pool.Exec(ctx,
			`UPDATE sources
			    SET status='disabled',fail_count=9
			  WHERE id=$1`,
			sourceID,
		); err != nil {
			t.Fatal(err)
		}
		return sourceID
	}
	createConfirmed := func(sourceID int64) (
		string,
		AgentActionContinuationLease,
	) {
		t.Helper()
		actionID := uuid.NewString()
		actionIDs = append(actionIDs, actionID)
		if err := f.store.CreatePendingAction(ctx, &types.PendingAction{
			ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
			ToolName:  "enable_source",
			Args:      []byte(fmt.Sprintf(`{"source_id":%d}`, sourceID)),
			Summary:   "重新启用信源",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, "effect test",
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
			"effect-test-"+actionID, time.Minute,
		)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := acquired.Lease()
		if err != nil {
			t.Fatal(err)
		}
		return actionID, lease
	}

	sourceID := createSource("success")
	actionID, lease := createConfirmed(sourceID)
	if err := f.store.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		f.tenantA, f.userA,
	).Scan(&laterSessionID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
		t.Fatal(err)
	}
	var status string
	var failCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT status,fail_count FROM sources WHERE id=$1`,
		sourceID,
	).Scan(&status, &failCount); err != nil {
		t.Fatal(err)
	}
	if status != string(types.SourceStatusActive) || failCount != 0 {
		t.Fatalf("source status/fail_count=%s/%d", status, failCount)
	}
	var actionStatus, terminalCode string
	if err := f.store.pool.QueryRow(ctx,
		`SELECT status,terminal_code FROM agent_action_continuations
		  WHERE action_id=$1`,
		actionID,
	).Scan(&actionStatus, &terminalCode); err != nil {
		t.Fatal(err)
	}
	if actionStatus != AgentActionStatusCompleted ||
		terminalCode != agentActionTerminalEnabled {
		t.Fatalf("action status/code=%s/%s", actionStatus, terminalCode)
	}
	identity := "agent-action:enable-source:" + actionID
	var frozenMessages []byte
	if err := f.store.pool.QueryRow(ctx,
		`SELECT success_messages FROM agent_action_continuations
		  WHERE action_id=$1`,
		actionID,
	).Scan(&frozenMessages); err != nil {
		t.Fatal(err)
	}
	idempotencyKey, _, err := agentSessionAppendIdentity(
		identity, json.RawMessage(frozenMessages),
	)
	if err != nil {
		t.Fatal(err)
	}
	var originalEvents, laterEvents int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2
		    AND batch_idempotency_key=$3`,
		f.tenantA, f.sessionA, idempotencyKey,
	).Scan(&originalEvents); err != nil {
		t.Fatal(err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2
		    AND batch_idempotency_key=$3`,
		f.tenantA, laterSessionID, idempotencyKey,
	).Scan(&laterEvents); err != nil {
		t.Fatal(err)
	}
	if originalEvents == 0 || laterEvents != 0 {
		t.Fatalf(
			"frozen/later session events=%d/%d",
			originalEvents, laterEvents,
		)
	}
	if err := f.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
		t.Fatalf("completed exact replay: %v", err)
	}
	authority, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, f.tenantA, f.userA, f.sessionA,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil ||
		authority.Route != AgentSessionProjectionRouteLedger {
		t.Fatalf("activate ledger route=%+v err=%v", authority, err)
	}
	ledgerSourceID := createSource("ledger-route")
	ledgerActionID, ledgerLease := createConfirmed(ledgerSourceID)
	if err := f.store.ProjectAgentActionContinuation(
		ctx, ledgerLease,
	); err != nil {
		t.Fatalf("ledger-authoritative projection: %v", err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT s.status,a.status
		   FROM sources s
		   CROSS JOIN agent_action_continuations a
		  WHERE s.id=$1 AND a.action_id=$2`,
		ledgerSourceID, ledgerActionID,
	).Scan(&status, &actionStatus); err != nil {
		t.Fatal(err)
	}
	if status != string(types.SourceStatusActive) ||
		actionStatus != AgentActionStatusCompleted {
		t.Fatalf(
			"ledger route source/action status=%s/%s",
			status, actionStatus,
		)
	}
	var ledgerMessages []byte
	if err := f.store.pool.QueryRow(ctx,
		`SELECT success_messages FROM agent_action_continuations
		  WHERE action_id=$1`,
		ledgerActionID,
	).Scan(&ledgerMessages); err != nil {
		t.Fatal(err)
	}
	ledgerKey, _, err := agentSessionAppendIdentity(
		"agent-action:enable-source:"+ledgerActionID,
		json.RawMessage(ledgerMessages),
	)
	if err != nil {
		t.Fatal(err)
	}
	var ledgerEventsBefore, ledgerEventsAfter int
	var sourceUpdatedBefore, sourceUpdatedAfter time.Time
	if err := f.store.pool.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM agent_events
		      WHERE tenant_id=$1 AND session_id=$2
		        AND batch_idempotency_key=$3),
		    (SELECT updated_at FROM sources WHERE id=$4)`,
		f.tenantA, f.sessionA, ledgerKey, ledgerSourceID,
	).Scan(&ledgerEventsBefore, &sourceUpdatedBefore); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(
		ctx, ledgerLease,
	); err != nil {
		t.Fatalf("ledger response-loss replay: %v", err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM agent_events
		      WHERE tenant_id=$1 AND session_id=$2
		        AND batch_idempotency_key=$3),
		    (SELECT updated_at FROM sources WHERE id=$4)`,
		f.tenantA, f.sessionA, ledgerKey, ledgerSourceID,
	).Scan(&ledgerEventsAfter, &sourceUpdatedAfter); err != nil {
		t.Fatal(err)
	}
	if ledgerEventsBefore == 0 ||
		ledgerEventsAfter != ledgerEventsBefore ||
		!sourceUpdatedAfter.Equal(sourceUpdatedBefore) {
		t.Fatalf(
			"ledger replay events/source time=%d/%d %s/%s",
			ledgerEventsBefore, ledgerEventsAfter,
			sourceUpdatedBefore, sourceUpdatedAfter,
		)
	}

	conflictSourceID := createSource("conflict")
	conflictActionID, conflictLease := createConfirmed(conflictSourceID)
	conflictIdentity := "agent-action:enable-source:" + conflictActionID
	if _, err := f.store.CommitAgentSessionAppend(
		ctx, f.userA, f.sessionA, conflictIdentity,
		json.RawMessage(
			`[{"role":"user","content":"conflicting frozen bytes"}]`,
		),
	); err != nil {
		t.Fatal(err)
	}
	var conflictEventsBefore int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2`,
		f.tenantA, f.sessionA,
	).Scan(&conflictEventsBefore); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(
		ctx, conflictLease,
	); err != nil {
		t.Fatalf("conflict should checkpoint blocked: %v", err)
	}
	var blockReason string
	if err := f.store.pool.QueryRow(ctx,
		`SELECT s.status,a.status,a.blocked_reason
		   FROM sources s
		   CROSS JOIN agent_action_continuations a
		  WHERE s.id=$1 AND a.action_id=$2`,
		conflictSourceID, conflictActionID,
	).Scan(&status, &actionStatus, &blockReason); err != nil {
		t.Fatal(err)
	}
	if status != string(types.SourceStatusDisabled) ||
		actionStatus != AgentActionStatusBlocked ||
		blockReason != "projection_integrity" {
		t.Fatalf(
			"conflicting source/action status/reason=%s/%s/%s",
			status, actionStatus, blockReason,
		)
	}
	var conflictEventsAfter int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2`,
		f.tenantA, f.sessionA,
	).Scan(&conflictEventsAfter); err != nil {
		t.Fatal(err)
	}
	if conflictEventsAfter != conflictEventsBefore {
		t.Fatalf(
			"projection-integrity block changed session events=%d/%d",
			conflictEventsBefore, conflictEventsAfter,
		)
	}
	blockedReplay, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, conflictActionID,
	)
	if err != nil || !blockedReplay.Replayed ||
		blockedReplay.Status != AgentActionStatusBlocked {
		t.Fatalf(
			"projection-integrity replay=%+v err=%v",
			blockedReplay, err,
		)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2`,
		f.tenantA, f.sessionA,
	).Scan(&conflictEventsAfter); err != nil {
		t.Fatal(err)
	}
	if conflictEventsAfter != conflictEventsBefore {
		t.Fatalf(
			"projection-integrity replay changed events=%d/%d",
			conflictEventsBefore, conflictEventsAfter,
		)
	}

	corruptSourceID := createSource("corrupt")
	corruptActionID, corruptLease := createConfirmed(corruptSourceID)
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET success_messages=
		      '[{"role":"user","content":"mutated"}]'::bytea
		  WHERE action_id=$1`,
		corruptActionID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(
		ctx, corruptLease,
	); err != nil {
		t.Fatalf("corrupt payload should checkpoint blocked: %v", err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT s.status,a.status,a.blocked_reason
		   FROM sources s
		   CROSS JOIN agent_action_continuations a
		  WHERE s.id=$1 AND a.action_id=$2`,
		corruptSourceID, corruptActionID,
	).Scan(&status, &actionStatus, &blockReason); err != nil {
		t.Fatal(err)
	}
	if status != string(types.SourceStatusDisabled) ||
		actionStatus != AgentActionStatusBlocked ||
		blockReason != "payload_integrity" {
		t.Fatalf(
			"corrupt source/action status/reason=%s/%s/%s",
			status, actionStatus, blockReason,
		)
	}
	blockedKey, _, err := agentSessionAppendIdentity(
		"agent-action:enable-source:"+corruptActionID,
		json.RawMessage(agentActionBlockedSessionMessages),
	)
	if err != nil {
		t.Fatal(err)
	}
	var blockedEvents int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2
		    AND batch_idempotency_key=$3`,
		f.tenantA, f.sessionA, blockedKey,
	).Scan(&blockedEvents); err != nil {
		t.Fatal(err)
	}
	if blockedEvents == 0 {
		t.Fatal("payload-integrity block terminal fact is absent")
	}
	if replay, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, corruptActionID,
	); err == nil || replay.Handled {
		t.Fatalf(
			"payload-integrity block bypassed frozen validation: %+v err=%v",
			replay, err,
		)
	}
}

func TestAgentActionTerminalMessageLiteralGolden(t *testing.T) {
	if agentActionAdapterVersion != "vane.enable-source/postgres/v1" {
		t.Fatalf(
			"adapter version=%q want vane.enable-source/postgres/v1",
			agentActionAdapterVersion,
		)
	}
	for name, test := range map[string]struct {
		got  string
		want string
	}{
		"cancelled": {
			got:  agentActionCancelledSessionMessages,
			want: `[{"role":"user","content":"[卡片回调] 用户已取消重新启用信源；未产生变更。"}]`,
		},
		"expired": {
			got:  agentActionExpiredSessionMessages,
			want: `[{"role":"user","content":"[卡片回调] 重新启用信源确认卡已过期；未产生变更。"}]`,
		},
		"blocked": {
			got:  agentActionBlockedSessionMessages,
			want: `[{"role":"user","content":"[卡片回调] 重新启用信源操作已因安全检查停止；未产生额外变更。"}]`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("terminal message=%q want=%q", test.got, test.want)
			}
			var messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal([]byte(test.got), &messages); err != nil {
				t.Fatalf("terminal message is not valid JSON: %v", err)
			}
			if len(messages) != 1 || messages[0].Role != "user" ||
				messages[0].Content == "" {
				t.Fatalf("terminal message shape=%+v", messages)
			}
		})
	}
}

func TestAgentActionContinuationTerminalPathsAndFences(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	var actionIDs []string
	var sourceIDs []int64
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`, actionIDs,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM subscriptions WHERE source_id=ANY($1)`, sourceIDs,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM sources WHERE id=ANY($1)`, sourceIDs,
		)
	})
	createSource := func(tag string, subscriber int64) int64 {
		t.Helper()
		sourceID, _, err := f.store.UpsertSource(ctx, &types.Source{
			Platform: types.PlatformWeb, Capability: types.CapFeed,
			URL: "https://example.com/action-terminal-" +
				tag + "-" + uuid.NewString(),
			Title: "action terminal " + tag,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		if subscriber > 0 {
			if err := f.store.AddSubscription(
				ctx, subscriber, sourceID,
			); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.store.pool.Exec(ctx,
			`UPDATE sources SET status='disabled',fail_count=7
			  WHERE id=$1`,
			sourceID,
		); err != nil {
			t.Fatal(err)
		}
		return sourceID
	}
	create := func(sourceID int64, evidence string) string {
		t.Helper()
		actionID := uuid.NewString()
		actionIDs = append(actionIDs, actionID)
		if err := f.store.CreatePendingAction(ctx, &types.PendingAction{
			ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
			ToolName: "enable_source",
			Args: []byte(fmt.Sprintf(
				`{"source_id":%d}`, sourceID,
			)),
			Summary:   "重新启用信源",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, actionID, evidence,
		); err != nil {
			t.Fatal(err)
		}
		return actionID
	}
	confirm := func(actionID string) {
		t.Helper()
		got, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, actionID,
		)
		if err != nil || !got.Accepted {
			t.Fatalf("confirm=%+v err=%v", got, err)
		}
	}
	acquire := func(
		actionID, owner string,
		duration time.Duration,
	) AgentActionContinuationLease {
		t.Helper()
		got, err := f.store.AcquireAgentActionContinuation(
			ctx, actionID, f.tenantA, f.userA, owner, duration,
		)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := got.Lease()
		if err != nil {
			t.Fatal(err)
		}
		return lease
	}
	sessionTerminalEvents := func(
		actionID, messages string,
	) int {
		t.Helper()
		key, _, err := agentSessionAppendIdentity(
			"agent-action:enable-source:"+actionID,
			json.RawMessage(messages),
		)
		if err != nil {
			t.Fatal(err)
		}
		var events int
		if err := f.store.pool.QueryRow(ctx,
			`SELECT count(*) FROM agent_events
			  WHERE tenant_id=$1 AND session_id=$2
			    AND batch_idempotency_key=$3`,
			f.tenantA, f.sessionA, key,
		).Scan(&events); err != nil {
			t.Fatal(err)
		}
		return events
	}

	t.Run("cancel and expiry activation replay", func(t *testing.T) {
		cancelID := create(910001, "cancel replay")
		cancelled, err := f.store.CancelAgentActionContinuation(
			ctx, f.userA, cancelID,
		)
		if err != nil || cancelled.Status != AgentActionStatusCancelled {
			t.Fatalf("cancel=%+v err=%v", cancelled, err)
		}
		cancelEvents := sessionTerminalEvents(
			cancelID, agentActionCancelledSessionMessages,
		)
		if cancelEvents == 0 {
			t.Fatal("cancelled terminal session fact is absent")
		}
		cancelReplay, err := f.store.CancelAgentActionContinuation(
			ctx, f.userA, cancelID,
		)
		if err != nil || !cancelReplay.Replayed ||
			cancelReplay.Status != AgentActionStatusCancelled {
			t.Fatalf("cancel replay=%+v err=%v", cancelReplay, err)
		}
		if replayEvents := sessionTerminalEvents(
			cancelID, agentActionCancelledSessionMessages,
		); replayEvents != cancelEvents {
			t.Fatalf(
				"cancel replay duplicated events=%d/%d",
				cancelEvents, replayEvents,
			)
		}
		if generation, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, cancelID, "cancel replay",
		); err != nil || generation != 1 {
			t.Fatalf("cancel activation replay=%d err=%v", generation, err)
		}

		expiredID := create(910002, "expiry replay")
		if _, err := f.store.pool.Exec(ctx,
			`UPDATE pending_actions
			    SET expires_at=clock_timestamp()-interval '1 second'
			  WHERE id=$1`,
			expiredID,
		); err != nil {
			t.Fatal(err)
		}
		expired, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, expiredID,
		)
		if err != nil || expired.Status != AgentActionStatusExpired {
			t.Fatalf("expire=%+v err=%v", expired, err)
		}
		expiredEvents := sessionTerminalEvents(
			expiredID, agentActionExpiredSessionMessages,
		)
		if expiredEvents == 0 {
			t.Fatal("expired terminal session fact is absent")
		}
		expiredReplay, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, expiredID,
		)
		if err != nil || !expiredReplay.Replayed ||
			expiredReplay.Status != AgentActionStatusExpired {
			t.Fatalf("expiry replay=%+v err=%v", expiredReplay, err)
		}
		if replayEvents := sessionTerminalEvents(
			expiredID, agentActionExpiredSessionMessages,
		); replayEvents != expiredEvents {
			t.Fatalf(
				"expiry replay duplicated events=%d/%d",
				expiredEvents, replayEvents,
			)
		}
		if generation, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, expiredID, "expiry replay",
		); err != nil || generation != 1 {
			t.Fatalf("expiry activation replay=%d err=%v", generation, err)
		}

		driftID := create(910003, "activation drift")
		if _, err := f.store.pool.Exec(ctx,
			`UPDATE pending_actions SET args='{"source_id":910004}'::jsonb
			  WHERE id=$1`,
			driftID,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.ActivateAgentActionContinuation(
			ctx, f.tenantA, f.userA, driftID, "activation drift",
		); err == nil {
			t.Fatal("activation replay accepted root drift")
		}

		confirmDriftID := create(910005, "confirm drift")
		if _, err := f.store.pool.Exec(ctx,
			`UPDATE pending_actions SET tool_name='list_sources'
			  WHERE id=$1`,
			confirmDriftID,
		); err != nil {
			t.Fatal(err)
		}
		if got, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, confirmDriftID,
		); err == nil || got.Accepted {
			t.Fatalf("confirm accepted drift: %+v err=%v", got, err)
		}
		var rootStatus, continuationStatus string
		if err := f.store.pool.QueryRow(ctx,
			`SELECT p.status,c.status
			   FROM pending_actions p
			   JOIN agent_action_continuations c ON c.action_id=p.id
			  WHERE p.id=$1`,
			confirmDriftID,
		).Scan(&rootStatus, &continuationStatus); err != nil {
			t.Fatal(err)
		}
		if rootStatus != string(types.PendingActionStatusPending) ||
			continuationStatus != AgentActionStatusPending {
			t.Fatalf(
				"confirm drift mutated root/continuation=%s/%s",
				rootStatus, continuationStatus,
			)
		}
	})

	t.Run("terminal session conflict rolls back decision", func(t *testing.T) {
		tests := []struct {
			name     string
			sourceID int64
			expire   bool
		}{
			{name: "cancel", sourceID: 910006},
			{name: "expire", sourceID: 910007, expire: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actionID := create(tt.sourceID, tt.name+" conflict")
				if tt.expire {
					if _, err := f.store.pool.Exec(ctx,
						`UPDATE pending_actions
						    SET expires_at=clock_timestamp()-interval '1 second'
						  WHERE id=$1`,
						actionID,
					); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := f.store.CommitAgentSessionAppend(
					ctx, f.userA, f.sessionA,
					"agent-action:enable-source:"+actionID,
					json.RawMessage(
						`[{"role":"user","content":"conflicting bytes"}]`,
					),
				); err != nil {
					t.Fatal(err)
				}
				var err error
				if tt.expire {
					_, err = f.store.ConfirmAgentActionContinuation(
						ctx, f.userA, actionID,
					)
				} else {
					_, err = f.store.CancelAgentActionContinuation(
						ctx, f.userA, actionID,
					)
				}
				if err == nil {
					t.Fatal("terminal conflict unexpectedly committed")
				}
				var rootStatus, continuationStatus string
				if err := f.store.pool.QueryRow(ctx,
					`SELECT p.status,c.status
					   FROM pending_actions p
					   JOIN agent_action_continuations c
					     ON c.action_id=p.id
					  WHERE p.id=$1`,
					actionID,
				).Scan(
					&rootStatus, &continuationStatus,
				); err != nil {
					t.Fatal(err)
				}
				if rootStatus !=
					string(types.PendingActionStatusPending) ||
					continuationStatus != AgentActionStatusPending {
					t.Fatalf(
						"terminal conflict committed=%s/%s",
						rootStatus, continuationStatus,
					)
				}
			})
		}
	})

	t.Run("stale lease takeover fences old owner", func(t *testing.T) {
		sourceID := createSource("takeover", f.userA)
		actionID := create(sourceID, "takeover")
		confirm(actionID)
		oldLease := acquire(actionID, "takeover-old", time.Millisecond)
		if _, err := f.store.pool.Exec(
			ctx, `SELECT pg_sleep(0.01)`,
		); err != nil {
			t.Fatal(err)
		}
		newLease := acquire(actionID, "takeover-new", time.Minute)
		if err := f.store.ProjectAgentActionContinuation(
			ctx, oldLease,
		); !errors.Is(err, ErrAgentActionBusy) {
			t.Fatalf("stale fence projection err=%v", err)
		}
		var status string
		if err := f.store.pool.QueryRow(ctx,
			`SELECT status FROM sources WHERE id=$1`, sourceID,
		).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != string(types.SourceStatusDisabled) {
			t.Fatalf("stale owner changed source to %s", status)
		}
		if err := f.store.ProjectAgentActionContinuation(
			ctx, newLease,
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("authority damage terminal blocks", func(t *testing.T) {
		sourceID := createSource("authority", f.userA)
		actionID := create(sourceID, "authority")
		confirm(actionID)
		if _, err := f.store.pool.Exec(ctx,
			`DELETE FROM agent_action_continuation_authority_events
			  WHERE action_id=$1`,
			actionID,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.AcquireAgentActionContinuation(
			ctx, actionID, f.tenantA, f.userA,
			"authority-worker", time.Minute,
		); !errors.Is(err, ErrAgentActionTerminal) {
			t.Fatalf("damaged authority acquire err=%v", err)
		}
		var status, reason string
		if err := f.store.pool.QueryRow(ctx,
			`SELECT status,blocked_reason
			   FROM agent_action_continuations WHERE action_id=$1`,
			actionID,
		).Scan(&status, &reason); err != nil {
			t.Fatal(err)
		}
		if status != AgentActionStatusBlocked ||
			reason != "authority_integrity" {
			t.Fatalf("authority block=%s/%s", status, reason)
		}
		if events := sessionTerminalEvents(
			actionID, agentActionBlockedSessionMessages,
		); events == 0 {
			t.Fatal("authority block terminal session fact is absent")
		}
		if replay, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, actionID,
		); err == nil || replay.Handled {
			t.Fatalf(
				"authority-integrity block bypassed authority validation: %+v err=%v",
				replay, err,
			)
		}
	})

	t.Run("authority damage session conflict blocks without half write", func(t *testing.T) {
		sourceID := createSource("authority-conflict", f.userA)
		actionID := create(sourceID, "authority conflict")
		confirm(actionID)
		if _, err := f.store.CommitAgentSessionAppend(
			ctx, f.userA, f.sessionA,
			"agent-action:enable-source:"+actionID,
			json.RawMessage(
				`[{"role":"user","content":"conflicting authority bytes"}]`,
			),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.pool.Exec(ctx,
			`DELETE FROM agent_action_continuation_authority_events
			  WHERE action_id=$1`,
			actionID,
		); err != nil {
			t.Fatal(err)
		}

		type snapshot struct {
			sessionMessages string
			sessionEvents   int
			sourceStatus    string
			sourceFailCount int
			sourceUpdatedAt time.Time
		}
		readSnapshot := func() snapshot {
			t.Helper()
			var got snapshot
			if err := f.store.pool.QueryRow(ctx,
				`SELECT s.messages,
				        (SELECT count(*) FROM agent_events e
				          WHERE e.tenant_id=$1 AND e.session_id=$2),
				        src.status,src.fail_count,src.updated_at
				   FROM agent_sessions s
				   CROSS JOIN sources src
				  WHERE s.id=$2 AND s.tenant_id=$1 AND s.user_id=$3
				    AND src.id=$4`,
				f.tenantA, f.sessionA, f.userA, sourceID,
			).Scan(
				&got.sessionMessages, &got.sessionEvents,
				&got.sourceStatus, &got.sourceFailCount,
				&got.sourceUpdatedAt,
			); err != nil {
				t.Fatal(err)
			}
			return got
		}
		before := readSnapshot()

		if _, err := f.store.AcquireAgentActionContinuation(
			ctx, actionID, f.tenantA, f.userA,
			"authority-conflict-worker", time.Minute,
		); !errors.Is(err, ErrAgentActionTerminal) {
			t.Fatalf("authority conflict acquire err=%v", err)
		}
		var actionStatus, blockReason, rootStatus string
		if err := f.store.pool.QueryRow(ctx,
			`SELECT c.status,c.blocked_reason,p.status
			   FROM agent_action_continuations c
			   JOIN pending_actions p ON p.id=c.action_id
			  WHERE c.action_id=$1`,
			actionID,
		).Scan(
			&actionStatus, &blockReason, &rootStatus,
		); err != nil {
			t.Fatal(err)
		}
		if actionStatus != AgentActionStatusBlocked ||
			blockReason != "projection_integrity" ||
			rootStatus != string(types.PendingActionStatusExecuted) {
			t.Fatalf(
				"authority conflict action/reason/root=%s/%s/%s",
				actionStatus, blockReason, rootStatus,
			)
		}
		afterBlock := readSnapshot()
		if afterBlock != before {
			t.Fatalf(
				"authority conflict half-wrote session/source\nbefore=%+v\nafter=%+v",
				before, afterBlock,
			)
		}

		replay, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, actionID,
		)
		if err != nil || !replay.Handled || !replay.Replayed ||
			replay.Status != AgentActionStatusBlocked {
			t.Fatalf(
				"projection-integrity callback replay=%+v err=%v",
				replay, err,
			)
		}
		afterReplay := readSnapshot()
		if afterReplay != before {
			t.Fatalf(
				"projection-integrity replay wrote session/source\nbefore=%+v\nafter=%+v",
				before, afterReplay,
			)
		}
		if _, err := f.store.pool.Exec(ctx,
			`UPDATE pending_actions
			    SET args=jsonb_build_object('source_id',$2::bigint)
			  WHERE id=$1`,
			actionID, sourceID+1_000_000,
		); err != nil {
			t.Fatal(err)
		}
		if replay, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, actionID,
		); err == nil || replay.Handled {
			t.Fatalf(
				"projection-integrity replay bypassed root args binding: %+v err=%v",
				replay, err,
			)
		}
		afterRootDrift := readSnapshot()
		if afterRootDrift != before {
			t.Fatalf(
				"rejected projection-integrity root drift wrote session/source\nbefore=%+v\nafter=%+v",
				before, afterRootDrift,
			)
		}
	})

	t.Run("cross tenant unowned source is not found", func(t *testing.T) {
		sourceID := createSource("not-found", f.userB)
		actionID := create(sourceID, "not found")
		confirm(actionID)
		lease := acquire(actionID, "not-found-worker", time.Minute)
		if _, err := f.store.AcquireAgentActionContinuation(
			ctx, actionID, f.tenantB, f.userA,
			"wrong-tenant", time.Minute,
		); err == nil {
			t.Fatal("cross-tenant acquisition unexpectedly succeeded")
		}
		if err := f.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
			t.Fatal(err)
		}
		var sourceStatus, actionStatus, terminalCode string
		var frozenMessages []byte
		if err := f.store.pool.QueryRow(ctx,
			`SELECT s.status,c.status,c.terminal_code,c.not_found_messages
			   FROM sources s
			   JOIN agent_action_continuations c ON c.source_id=s.id
			  WHERE c.action_id=$1`,
			actionID,
		).Scan(
			&sourceStatus, &actionStatus, &terminalCode, &frozenMessages,
		); err != nil {
			t.Fatal(err)
		}
		if sourceStatus != string(types.SourceStatusDisabled) ||
			actionStatus != AgentActionStatusCompleted ||
			terminalCode != agentActionTerminalNotFound {
			t.Fatalf(
				"not-found source/action/code=%s/%s/%s",
				sourceStatus, actionStatus, terminalCode,
			)
		}
		key, _, err := agentSessionAppendIdentity(
			"agent-action:enable-source:"+actionID,
			json.RawMessage(frozenMessages),
		)
		if err != nil {
			t.Fatal(err)
		}
		var before, after int
		if err := f.store.pool.QueryRow(ctx,
			`SELECT count(*) FROM agent_events
			  WHERE tenant_id=$1 AND session_id=$2
			    AND batch_idempotency_key=$3`,
			f.tenantA, f.sessionA, key,
		).Scan(&before); err != nil {
			t.Fatal(err)
		}
		if before == 0 {
			t.Fatal("not-found frozen session batch is absent")
		}
		if err := f.store.ProjectAgentActionContinuation(
			ctx, lease,
		); err != nil {
			t.Fatalf("not-found response replay: %v", err)
		}
		if err := f.store.pool.QueryRow(ctx,
			`SELECT count(*) FROM agent_events
			  WHERE tenant_id=$1 AND session_id=$2
			    AND batch_idempotency_key=$3`,
			f.tenantA, f.sessionA, key,
		).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Fatalf("not-found replay events=%d want=%d", after, before)
		}
	})
}

func TestAgentActionRuntimeRoleDriftFailsClosed(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionID := uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		_, _ = f.store.pool.Exec(
			cleanupCtx,
			`ALTER ROLE vane_agent_action_operator NOLOGIN;
			 REVOKE SELECT (status) ON sources
			   FROM vane_agent_action_continuator`,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=$1`, actionID,
		)
	})
	if err := f.store.CreatePendingAction(ctx, &types.PendingAction{
		ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
		ToolName: "enable_source", Args: []byte(`{"source_id":920001}`),
		Summary: "role drift", Status: types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(
		ctx, `ALTER ROLE vane_agent_action_operator LOGIN`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ActivateAgentActionContinuation(
		ctx, f.tenantA, f.userA, actionID, "role drift",
	); err == nil {
		t.Fatal("operator role drift was accepted")
	}
	if _, err := f.store.pool.Exec(
		ctx, `ALTER ROLE vane_agent_action_operator NOLOGIN`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ActivateAgentActionContinuation(
		ctx, f.tenantA, f.userA, actionID, "role drift",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`GRANT SELECT (status) ON sources
		   TO vane_agent_action_continuator`,
	); err != nil {
		t.Fatal(err)
	}
	if got, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, actionID,
	); err == nil || got.Accepted {
		t.Fatalf("continuator role drift accepted: %+v err=%v", got, err)
	}
	var rootStatus, continuationStatus string
	if err := f.store.pool.QueryRow(ctx,
		`SELECT p.status,c.status
		   FROM pending_actions p
		   JOIN agent_action_continuations c ON c.action_id=p.id
		  WHERE p.id=$1`,
		actionID,
	).Scan(&rootStatus, &continuationStatus); err != nil {
		t.Fatal(err)
	}
	if rootStatus != string(types.PendingActionStatusPending) ||
		continuationStatus != AgentActionStatusPending {
		t.Fatalf(
			"role drift mutated root/continuation=%s/%s",
			rootStatus, continuationStatus,
		)
	}
}
