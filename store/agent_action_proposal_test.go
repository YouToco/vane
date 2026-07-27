package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

func TestProposeAgentActionContinuationIsAtomicAndReplayExact(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionIDs := []string{
		uuid.NewString(), uuid.NewString(), uuid.NewString(),
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`,
			actionIDs,
		)
	})

	expiresAt := time.Now().Add(time.Hour)
	action := &types.PendingAction{
		ID: actionIDs[0], UserID: f.userA, SessionID: &f.sessionA,
		ToolName:  "enable_source",
		Args:      []byte(`{"source_id":900001}`),
		Summary:   "重新启用信源（id=900001）",
		Status:    types.PendingActionStatusPending,
		ExpiresAt: expiresAt,
	}
	if err := f.store.ProposeAgentActionContinuation(ctx, action); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ClaimPendingAction(
		ctx, action.ID, f.userA,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("legacy Claim reached v2 proposal: %v", err)
	}
	if err := f.store.ProposeAgentActionContinuation(ctx, action); err != nil {
		t.Fatalf("exact response-loss replay: %v", err)
	}
	status, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, action.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if status.ActivationEligible || status.RollbackEligible ||
		status.Generation != 1 ||
		status.Route != AgentActionAuthorityDurable {
		t.Fatalf("automatic proposal operator status=%+v", status)
	}
	if _, err := f.store.RollbackAgentActionContinuation(
		ctx, f.tenantA, f.userA, action.ID, "must stay durable",
	); !errors.Is(err, ErrAgentActionTerminal) {
		t.Fatalf("automatic proposal rollback error=%v", err)
	}

	var (
		version, continuationRows, authorityRows int
		rootStatus, continuationStatus, evidence string
	)
	if err := f.store.pool.QueryRow(ctx, `
		SELECT p.execution_version,p.status,c.status,e.evidence,
		       (SELECT count(*) FROM agent_action_continuations
		         WHERE action_id=p.id),
		       (SELECT count(*)
		          FROM agent_action_continuation_authority_events
		         WHERE action_id=p.id)
		  FROM pending_actions p
		  JOIN agent_action_continuations c ON c.action_id=p.id
		  JOIN agent_action_continuation_authority_events e
		    ON e.action_id=p.id
		 WHERE p.id=$1`,
		action.ID,
	).Scan(
		&version, &rootStatus, &continuationStatus, &evidence,
		&continuationRows, &authorityRows,
	); err != nil {
		t.Fatal(err)
	}
	if version != AgentActionExecutionVersion ||
		rootStatus != string(types.PendingActionStatusPending) ||
		continuationStatus != AgentActionStatusPending ||
		evidence != agentActionProposalEvidence ||
		continuationRows != 1 || authorityRows != 1 {
		t.Fatalf(
			"root/continuation/evidence/counts=%d/%s/%s/%s/%d/%d",
			version, rootStatus, continuationStatus, evidence,
			continuationRows, authorityRows,
		)
	}

	drifted := *action
	drifted.Summary = "different summary"
	if err := f.store.ProposeAgentActionContinuation(
		ctx, &drifted,
	); err == nil {
		t.Fatal("proposal replay accepted different visible summary")
	}

	// A session from another tenant can satisfy the legacy root FK, but not the
	// exact continuation scope. The continuation failure must roll the newly
	// inserted root back as part of the same transaction.
	crossTenant := &types.PendingAction{
		ID: actionIDs[1], UserID: f.userA, SessionID: &f.sessionB,
		ToolName:  "enable_source",
		Args:      []byte(fmt.Sprintf(`{"source_id":%d}`, 900002)),
		Summary:   "重新启用信源（id=900002）",
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	if err := f.store.ProposeAgentActionContinuation(
		ctx, crossTenant,
	); err == nil {
		t.Fatal("proposal accepted a cross-tenant session")
	}
	var roots int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_actions WHERE id=$1`,
		crossTenant.ID,
	).Scan(&roots); err != nil {
		t.Fatal(err)
	}
	if roots != 0 {
		t.Fatalf("failed proposal leaked %d pending root(s)", roots)
	}

	lostResponse := &types.PendingAction{
		ID: actionIDs[2], UserID: f.userA, SessionID: &f.sessionA,
		ToolName:  "enable_source",
		Args:      []byte(`{"source_id":900003}`),
		Summary:   "重新启用信源（id=900003）",
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	responseLostStore := *f.store
	responseLostStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := f.store.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &agentActionProposalLostCommitTx{Tx: tx}, nil
	}
	if err := responseLostStore.ProposeAgentActionContinuation(
		ctx, lostResponse,
	); err == nil {
		t.Fatal("lost commit response was not surfaced")
	}
	if err := f.store.ProposeAgentActionContinuation(
		ctx, lostResponse,
	); err != nil {
		t.Fatalf("lost response exact replay: %v", err)
	}
	var rootsAfterReplay, continuationsAfterReplay, authorityAfterReplay int
	if err := f.store.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM pending_actions WHERE id=$1),
		  (SELECT count(*) FROM agent_action_continuations
		    WHERE action_id=$1),
		  (SELECT count(*)
		     FROM agent_action_continuation_authority_events
		    WHERE action_id=$1)`,
		lostResponse.ID,
	).Scan(
		&rootsAfterReplay, &continuationsAfterReplay,
		&authorityAfterReplay,
	); err != nil {
		t.Fatal(err)
	}
	if rootsAfterReplay != 1 || continuationsAfterReplay != 1 ||
		authorityAfterReplay != 1 {
		t.Fatalf(
			"lost response replay duplicated root/continuation/authority=%d/%d/%d",
			rootsAfterReplay, continuationsAfterReplay,
			authorityAfterReplay,
		)
	}
}

type agentActionProposalLostCommitTx struct {
	pgx.Tx
}

func (tx *agentActionProposalLostCommitTx) Commit(
	ctx context.Context,
) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	return errors.New("injected Agent action proposal commit response loss")
}

func TestProposeAgentActionContinuationRejectsRoleDriftBeforeWrite(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionID := uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		_, _ = f.store.pool.Exec(
			cleanupCtx,
			`REVOKE SELECT (id) ON sources
			   FROM vane_agent_action_proposer`,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=$1`, actionID,
		)
	})
	if _, err := f.store.pool.Exec(ctx,
		`GRANT SELECT (id) ON sources TO vane_agent_action_proposer`,
	); err != nil {
		t.Fatal(err)
	}
	action := &types.PendingAction{
		ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
		ToolName: "enable_source", Args: []byte(`{"source_id":920001}`),
		Summary:   "重新启用信源（id=920001）",
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	if err := f.store.ProposeAgentActionContinuation(
		ctx, action,
	); err == nil {
		t.Fatal("proposer role drift was accepted")
	}
	var roots int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_actions WHERE id=$1`,
		actionID,
	).Scan(&roots); err != nil {
		t.Fatal(err)
	}
	if roots != 0 {
		t.Fatalf("role drift leaked %d pending root(s)", roots)
	}
}

func TestProposeAgentActionContinuationBindsExactLiveSessionTenant(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionID := uuid.NewString()
	if _, err := f.store.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'member')`,
		f.tenantB, f.userA,
	); err != nil {
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
			`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			f.tenantB, f.userA,
		)
	})
	action := &types.PendingAction{
		ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
		ToolName: "enable_source", Args: []byte(`{"source_id":930001}`),
		Summary:   "重新启用信源（id=930001）",
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	if err := f.store.ProposeAgentActionContinuation(ctx, action); err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := f.store.pool.QueryRow(ctx,
		`SELECT tenant_id FROM pending_actions WHERE id=$1`,
		actionID,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if tenantID != f.tenantA {
		t.Fatalf("proposal tenant=%d, want session tenant=%d",
			tenantID, f.tenantA)
	}
}

func TestAgentActionProposalAndMembershipRevocationLinearize(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionID := uuid.NewString()
	revokedActionID := uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		_, _ = f.store.pool.Exec(cleanupCtx,
			`INSERT INTO memberships (tenant_id,user_id,role)
			 VALUES ($1,$2,'owner') ON CONFLICT DO NOTHING`,
			f.tenantA, f.userA,
		)
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`,
			[]string{actionID, revokedActionID},
		)
	})

	blocker, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx,
		`LOCK TABLE pending_actions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	action := &types.PendingAction{
		ID: actionID, UserID: f.userA, SessionID: &f.sessionA,
		ToolName: "enable_source", Args: []byte(`{"source_id":930002}`),
		Summary:   "重新启用信源（id=930002）",
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	proposalDone := make(chan error, 1)
	go func() {
		proposalDone <- f.store.ProposeAgentActionContinuation(
			context.Background(), action,
		)
	}()
	waitForAgentActionProposalLock(
		t, f.store, "FROM pending_actions",
	)

	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := f.store.pool.Exec(
			context.Background(),
			`DELETE FROM memberships
			  WHERE tenant_id=$1 AND user_id=$2`,
			f.tenantA, f.userA,
		)
		revokeDone <- revokeErr
	}()
	waitForAgentActionProposalLock(
		t, f.store, "DELETE FROM memberships",
	)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-proposalDone; err != nil {
		t.Fatalf("proposal failed before revocation: %v", err)
	}
	if err := <-revokeDone; err != nil {
		t.Fatalf("revoke membership: %v", err)
	}
	if _, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, actionID,
	); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("revoked membership confirmed durable action: %v", err)
	}
	var rootStatus, continuationStatus string
	if err := f.store.pool.QueryRow(ctx, `
		SELECT p.status,c.status
		  FROM pending_actions p
		  JOIN agent_action_continuations c ON c.action_id=p.id
		 WHERE p.id=$1`,
		actionID,
	).Scan(&rootStatus, &continuationStatus); err != nil {
		t.Fatal(err)
	}
	if rootStatus != string(types.PendingActionStatusPending) ||
		continuationStatus != AgentActionStatusPending {
		t.Fatalf("revoked confirmation mutated root/continuation=%s/%s",
			rootStatus, continuationStatus)
	}
	revokedProposal := *action
	revokedProposal.ID = revokedActionID
	if err := f.store.ProposeAgentActionContinuation(
		ctx, &revokedProposal,
	); types.CodeOf(err) != types.CodeNotFound {
		t.Fatalf("revoked membership created new proposal: %v", err)
	}
}

func TestReplayAgentActionProposalRejectsPartialOrDriftedEvidence(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionIDs := []string{uuid.NewString(), uuid.NewString()}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`, actionIDs,
		)
	})
	newAction := func(id string, sourceID int64) *types.PendingAction {
		return &types.PendingAction{
			ID: id, UserID: f.userA, SessionID: &f.sessionA,
			ToolName: "enable_source",
			Args: []byte(fmt.Sprintf(
				`{"source_id":%d}`, sourceID,
			)),
			Summary: fmt.Sprintf(
				"重新启用信源（id=%d）", sourceID,
			),
			Status:    types.PendingActionStatusPending,
			ExpiresAt: time.Now().Add(time.Hour),
		}
	}

	partial := newAction(actionIDs[0], 930003)
	if err := f.store.ProposeAgentActionContinuation(
		ctx, partial,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`DELETE FROM agent_action_continuations WHERE action_id=$1`,
		partial.ID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProposeAgentActionContinuation(
		ctx, partial,
	); types.CodeOf(err) != types.CodeInternal {
		t.Fatalf("partial evidence replay error=%v", err)
	}

	drifted := newAction(actionIDs[1], 930004)
	if err := f.store.ProposeAgentActionContinuation(
		ctx, drifted,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET attempt_count=1,lease_fence=7,
		        next_attempt_at=next_attempt_at+interval '1 hour'
		  WHERE action_id=$1`,
		drifted.ID,
	); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ProposeAgentActionContinuation(
		ctx, drifted,
	); types.CodeOf(err) != types.CodeInternal {
		t.Fatalf("drifted evidence replay error=%v", err)
	}
}

func waitForAgentActionProposalLock(
	t *testing.T,
	st *Store,
	queryFragment string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
			  SELECT 1
			    FROM pg_stat_activity
			   WHERE datname=current_database()
			     AND pid<>pg_backend_pid()
			     AND state='active'
			     AND wait_event_type='Lock'
			     AND position($1 in query)>0
			)`,
			queryFragment,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("query did not block: %s", queryFragment)
}
