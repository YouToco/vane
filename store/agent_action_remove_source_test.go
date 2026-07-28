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

func TestRemoveSourceDurableContinuationAtomicBatchAndReplay(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	var actionIDs []string
	var sourceIDs []int64
	var sameTenantUserID int64
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
		if sameTenantUserID != 0 {
			cleanupExec(
				cleanupCtx, t, f.store,
				`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
				f.tenantA, sameTenantUserID,
			)
			cleanupExec(
				cleanupCtx, t, f.store,
				`DELETE FROM users WHERE id=$1`, sameTenantUserID,
			)
		}
	})

	createSource := func(label string) int64 {
		t.Helper()
		sourceID, _, err := f.store.UpsertSource(ctx, &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapFeed,
			URL: "https://example.com/remove-source-" +
				label + "-" + uuid.NewString(),
			Title: "remove source " + label,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		return sourceID
	}
	sourceA := createSource("a")
	sourceB := createSource("b")
	for _, sourceID := range []int64{sourceA, sourceB} {
		if err := f.store.AddSubscription(
			ctx, f.userA, sourceID,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.AddSubscription(ctx, f.userB, sourceA); err != nil {
		t.Fatal(err)
	}
	sameTenantUser, err := f.store.UpsertUserByOpenID(
		ctx,
		"remove-source-same-tenant-"+uuid.NewString(),
		"remove source same tenant",
	)
	if err != nil {
		t.Fatal(err)
	}
	sameTenantUserID = sameTenantUser.ID
	if _, err := f.store.pool.Exec(ctx, `
		INSERT INTO memberships (tenant_id,user_id,role)
		VALUES ($1,$2,'member')`,
		f.tenantA, sameTenantUserID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.AddSubscription(
		ctx, sameTenantUserID, sourceA,
	); err != nil {
		t.Fatal(err)
	}

	propose := func(ids []int64) string {
		t.Helper()
		raw, err := json.Marshal(struct {
			SourceIDs []int64 `json:"source_ids"`
		}{SourceIDs: ids})
		if err != nil {
			t.Fatal(err)
		}
		actionID := uuid.NewString()
		actionIDs = append(actionIDs, actionID)
		if err := f.store.ProposeAgentActionContinuation(
			ctx,
			&types.PendingAction{
				ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
				ToolName: agentActionRemoveSourceToolName,
				Args:     raw, Summary: "取消订阅信源",
				Status:    types.PendingActionStatusPending,
				ExpiresAt: time.Now().Add(time.Hour),
			},
		); err != nil {
			t.Fatal(err)
		}
		return actionID
	}
	confirmAndAcquire := func(actionID, owner string) AgentActionContinuationLease {
		t.Helper()
		outcome, err := f.store.ConfirmAgentActionContinuation(
			ctx, f.userA, actionID,
		)
		if err != nil || !outcome.Accepted ||
			outcome.Status != AgentActionStatusConfirmed {
			t.Fatalf("confirm=%+v err=%v", outcome, err)
		}
		action, err := f.store.AcquireAgentActionContinuation(
			ctx, actionID, f.tenantA, f.userA, owner, time.Minute,
		)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := action.Lease()
		if err != nil {
			t.Fatal(err)
		}
		return lease
	}

	actionID := propose([]int64{sourceA, sourceB, sourceA})
	status, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, actionID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if status.ToolName != agentActionRemoveSourceToolName ||
		!equalAgentActionSourceIDs(
			status.SourceIDs, []int64{sourceA, sourceB},
		) ||
		status.SourceID != sourceA ||
		status.Generation != 1 ||
		status.Route != AgentActionAuthorityDurable {
		t.Fatalf("remove_source proposal status=%+v", status)
	}
	var rootArgs string
	var adapter string
	if err := f.store.pool.QueryRow(ctx, `
		SELECT p.args::text,c.adapter_version
		  FROM pending_actions p
		  JOIN agent_action_continuations c ON c.action_id=p.id
		 WHERE p.id=$1`,
		actionID,
	).Scan(&rootArgs, &adapter); err != nil {
		t.Fatal(err)
	}
	wantArgs := fmt.Sprintf(
		`{"source_ids": [%d, %d]}`, sourceA, sourceB,
	)
	if rootArgs != wantArgs ||
		adapter != agentActionRemoveSourceAdapterVersion {
		t.Fatalf("canonical root/adapter=%s/%s", rootArgs, adapter)
	}
	if _, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userB, actionID,
	); !errors.Is(err, ErrAgentActionNotRouted) {
		t.Fatalf("foreign user routed remove_source action: %v", err)
	}

	lease := confirmAndAcquire(actionID, "remove-source-worker")
	if err := f.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
		t.Fatal(err)
	}
	var (
		ownerRows, foreignRows, sameTenantRows, sourceRows int
		terminalCode                                       string
	)
	if err := f.store.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM subscriptions
		    WHERE tenant_id=$1 AND user_id=$2
		      AND source_id=ANY($4)),
		  (SELECT count(*) FROM subscriptions
		    WHERE tenant_id=$3 AND user_id=$5 AND source_id=$6),
		  (SELECT count(*) FROM subscriptions
		    WHERE tenant_id=$1 AND user_id=$8 AND source_id=$6),
		  (SELECT count(*) FROM sources WHERE id=ANY($4)),
		  (SELECT terminal_code FROM agent_action_continuations
		    WHERE action_id=$7)`,
		f.tenantA, f.userA, f.tenantB,
		[]int64{sourceA, sourceB}, f.userB, sourceA, actionID,
		sameTenantUserID,
	).Scan(
		&ownerRows, &foreignRows, &sameTenantRows,
		&sourceRows, &terminalCode,
	); err != nil {
		t.Fatal(err)
	}
	if ownerRows != 0 || foreignRows != 1 || sameTenantRows != 1 ||
		sourceRows != 2 ||
		terminalCode != agentActionTerminalRemoved {
		t.Fatalf(
			"owner/foreign/same-tenant/sources/terminal=%d/%d/%d/%d/%s",
			ownerRows, foreignRows, sameTenantRows,
			sourceRows, terminalCode,
		)
	}

	frozen, err := freezeRemoveSourceAction([]int64{sourceA, sourceB})
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := agentSessionAppendIdentity(
		"agent-action:remove-source:"+actionID,
		frozen.successMessages,
	)
	if err != nil {
		t.Fatal(err)
	}
	var eventsBefore int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2
		    AND batch_idempotency_key=$3`,
		f.tenantA, f.sessionA, key,
	).Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	if eventsBefore == 0 {
		t.Fatal("remove_source terminal session fact is absent")
	}
	if err := f.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
		t.Fatalf("completed projection replay: %v", err)
	}
	replay, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, actionID,
	)
	if err != nil || !replay.Replayed ||
		replay.Status != AgentActionStatusCompleted {
		t.Fatalf("completed confirm replay=%+v err=%v", replay, err)
	}
	var eventsAfter int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND session_id=$2
		    AND batch_idempotency_key=$3`,
		f.tenantA, f.sessionA, key,
	).Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf(
			"remove_source replay duplicated events=%d/%d",
			eventsBefore, eventsAfter,
		)
	}

	noOpID := propose([]int64{sourceA, sourceB})
	noOpLease := confirmAndAcquire(noOpID, "remove-source-noop")
	if err := f.store.ProjectAgentActionContinuation(
		ctx, noOpLease,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT terminal_code FROM agent_action_continuations
		  WHERE action_id=$1`,
		noOpID,
	).Scan(&terminalCode); err != nil {
		t.Fatal(err)
	}
	if terminalCode != agentActionTerminalNotSubscribed {
		t.Fatalf("no-op terminal=%s", terminalCode)
	}
}

func TestRemoveSourceDurableContinuationProjectionConflictRollsBackBatch(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	var actionID string
	var sourceIDs []int64
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=$1`, actionID,
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
	for _, label := range []string{"atomic-a", "atomic-b"} {
		sourceID, _, err := f.store.UpsertSource(ctx, &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapFeed,
			URL:        "https://example.com/" + label + "-" + uuid.NewString(),
			Title:      label,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		if err := f.store.AddSubscription(
			ctx, f.userA, sourceID,
		); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(struct {
		SourceIDs []int64 `json:"source_ids"`
	}{SourceIDs: sourceIDs})
	if err != nil {
		t.Fatal(err)
	}
	actionID = uuid.NewString()
	if err := f.store.ProposeAgentActionContinuation(
		ctx,
		&types.PendingAction{
			ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
			ToolName: agentActionRemoveSourceToolName,
			Args:     raw, Summary: "取消订阅信源",
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, actionID,
	); err != nil {
		t.Fatal(err)
	}
	action, err := f.store.AcquireAgentActionContinuation(
		ctx, actionID, f.tenantA, f.userA,
		"remove-source-conflict", time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := action.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CommitAgentSessionAppend(
		ctx, f.userA, f.sessionA,
		"agent-action:remove-source:"+actionID,
		json.RawMessage(
			`[{"role":"user","content":"conflicting remove fact"}]`,
		),
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(
		ctx, lease,
	); err != nil {
		t.Fatal(err)
	}
	var subscriptions int
	var status, blockedReason string
	if err := f.store.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM subscriptions
		    WHERE tenant_id=$1 AND user_id=$2 AND source_id=ANY($3)),
		  status,blocked_reason
		  FROM agent_action_continuations WHERE action_id=$4`,
		f.tenantA, f.userA, sourceIDs, actionID,
	).Scan(&subscriptions, &status, &blockedReason); err != nil {
		t.Fatal(err)
	}
	if subscriptions != len(sourceIDs) ||
		status != AgentActionStatusBlocked ||
		blockedReason != "projection_integrity" {
		t.Fatalf(
			"subscriptions/status/reason=%d/%s/%s",
			subscriptions, status, blockedReason,
		)
	}
}

func TestRemoveSourceDurableDecisionFactsAndProtocolBytes(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	var actionIDs []string
	var sourceID int64
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`, actionIDs,
		)
		if sourceID != 0 {
			cleanupExec(
				cleanupCtx, t, f.store,
				`DELETE FROM subscriptions WHERE source_id=$1`, sourceID,
			)
			cleanupExec(
				cleanupCtx, t, f.store,
				`DELETE FROM sources WHERE id=$1`, sourceID,
			)
		}
	})

	const protocolActionID = "018f86f0-4fd4-7de5-b30d-02a35f2f4662"
	if got := agentActionOperationIdentity(AgentActionContinuation{
		ActionID: protocolActionID,
		ToolName: agentActionRemoveSourceToolName,
	}); got != "agent-action:remove-source:"+protocolActionID {
		t.Fatalf("remove_source operation identity=%q", got)
	}

	var err error
	sourceID, _, err = f.store.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL: "https://example.com/remove-source-decisions-" +
			uuid.NewString(),
		Title: "remove source decisions",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.AddSubscription(ctx, f.userA, sourceID); err != nil {
		t.Fatal(err)
	}

	propose := func() string {
		t.Helper()
		actionID := uuid.NewString()
		actionIDs = append(actionIDs, actionID)
		if err := f.store.ProposeAgentActionContinuation(
			ctx,
			&types.PendingAction{
				ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
				ToolName: agentActionRemoveSourceToolName,
				Args: []byte(fmt.Sprintf(
					`{"source_ids":[%d]}`, sourceID,
				)),
				Summary:   "取消订阅信源",
				Status:    types.PendingActionStatusPending,
				ExpiresAt: time.Now().Add(time.Hour),
			},
		); err != nil {
			t.Fatal(err)
		}
		return actionID
	}
	terminalEvents := func(actionID, messages string) int {
		t.Helper()
		key, _, err := agentSessionAppendIdentity(
			"agent-action:remove-source:"+actionID,
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

	const cancelledMessages = `[{"role":"user","content":"[卡片回调] 用户已取消退订信源；未产生变更。"}]`
	cancelID := propose()
	cancelled, err := f.store.CancelAgentActionContinuation(
		ctx, f.userA, cancelID,
	)
	if err != nil || cancelled.Status != AgentActionStatusCancelled {
		t.Fatalf("cancel=%+v err=%v", cancelled, err)
	}
	cancelEvents := terminalEvents(cancelID, cancelledMessages)
	if cancelEvents == 0 {
		t.Fatal("cancelled terminal fact is absent")
	}
	cancelReplay, err := f.store.CancelAgentActionContinuation(
		ctx, f.userA, cancelID,
	)
	if err != nil || !cancelReplay.Replayed ||
		cancelReplay.Status != AgentActionStatusCancelled ||
		terminalEvents(cancelID, cancelledMessages) != cancelEvents {
		t.Fatalf("cancel replay=%+v err=%v", cancelReplay, err)
	}

	const expiredMessages = `[{"role":"user","content":"[卡片回调] 取消订阅信源确认卡已过期；未产生变更。"}]`
	expiredID := propose()
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
	expiredEvents := terminalEvents(expiredID, expiredMessages)
	if expiredEvents == 0 {
		t.Fatal("expired terminal fact is absent")
	}
	expiredReplay, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, expiredID,
	)
	if err != nil || !expiredReplay.Replayed ||
		expiredReplay.Status != AgentActionStatusExpired ||
		terminalEvents(expiredID, expiredMessages) != expiredEvents {
		t.Fatalf("expiry replay=%+v err=%v", expiredReplay, err)
	}

	const blockedMessages = `[{"role":"user","content":"[卡片回调] 取消订阅信源操作已因安全检查停止；未产生额外变更。"}]`
	blockedID := propose()
	if _, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, blockedID,
	); err != nil {
		t.Fatal(err)
	}
	action, err := f.store.AcquireAgentActionContinuation(
		ctx, blockedID, f.tenantA, f.userA,
		"remove-source-payload-block", time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := action.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET success_messages=
		      '[{"role":"user","content":"mutated"}]'::bytea
		  WHERE action_id=$1`,
		blockedID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProjectAgentActionContinuation(ctx, lease); err != nil {
		t.Fatalf("payload damage should checkpoint blocked: %v", err)
	}
	var subscriptions int
	var status, reason string
	if err := f.store.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM subscriptions
		    WHERE tenant_id=$1 AND user_id=$2 AND source_id=$3),
		  status,blocked_reason
		  FROM agent_action_continuations WHERE action_id=$4`,
		f.tenantA, f.userA, sourceID, blockedID,
	).Scan(&subscriptions, &status, &reason); err != nil {
		t.Fatal(err)
	}
	if subscriptions != 1 ||
		status != AgentActionStatusBlocked ||
		reason != "payload_integrity" {
		t.Fatalf(
			"subscriptions/status/reason=%d/%s/%s",
			subscriptions, status, reason,
		)
	}
	if events := terminalEvents(blockedID, blockedMessages); events == 0 {
		t.Fatal("blocked terminal fact is absent")
	}
}

func TestCanonicalRemoveSourceArgsStrictAndDeduplicated(t *testing.T) {
	sourceIDs, canonical, err := canonicalRemoveSourceArgs(
		[]byte(`{"source_ids":[7,8,7]}`),
	)
	if err != nil ||
		!equalAgentActionSourceIDs(sourceIDs, []int64{7, 8}) ||
		string(canonical) != `{"source_ids":[7,8]}` {
		t.Fatalf("canonical=%s ids=%v err=%v", canonical, sourceIDs, err)
	}
	for _, raw := range []string{
		`{"source_id":7}`,
		`{"source_ids":[]}`,
		`{"source_ids":[0]}`,
		`{"source_ids":[7],"unknown":true}`,
		`{"source_ids":[7],"source_ids":[8]}`,
		`{"source_ids":null}`,
	} {
		if _, _, err := canonicalRemoveSourceArgs(
			[]byte(raw),
		); err == nil {
			t.Fatalf("accepted invalid remove_source args %s", raw)
		}
	}
}
