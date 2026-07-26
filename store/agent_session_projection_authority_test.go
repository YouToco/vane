package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/agentledger"
	"github.com/YouToco/vane/types"
)

func TestAgentSessionProjectionAuthorityLifecycleAndExactReplay(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	scope := f.scopeA()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	first := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]`,
		),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`["endpoint-a"]`),
	}
	firstBatch := projectionSnapshotBatch(
		t, scope, "authority-turn-1", base, first, "",
	)
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, first, firstBatch,
	); err != nil {
		t.Fatal(err)
	}

	initial, err := f.store.GetAgentSessionProjectionAuthorityStatus(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Route != AgentSessionProjectionRouteLegacy ||
		initial.Generation != 0 || initial.EventID != 0 {
		t.Fatalf("initial status=%+v", initial)
	}
	preflight, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := loadAgentSessionProjection(ctx, preflight, scope, false)
	if err == nil {
		var events []agentledger.Event
		events, err = loadCompleteAgentEventLedger(ctx, preflight, scope)
		if err == nil {
			_, err = verifiedLedgerSessionProjection(legacy, events)
		}
	}
	if err != nil {
		_ = preflight.Rollback(ctx)
		t.Fatalf("authority projection preflight: %v", err)
	}
	if err = validateAgentSessionProjectionOperator(ctx, preflight); err != nil {
		_ = preflight.Rollback(ctx)
		t.Fatalf("authority operator preflight: %v", err)
	}
	_ = preflight.Rollback(ctx)

	activated, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if activated.Route != AgentSessionProjectionRouteLedger ||
		activated.Generation != 1 || activated.EventID <= 0 ||
		activated.LedgerHeadSequence != int64(len(firstBatch.Events)) {
		t.Fatalf("activated=%+v", activated)
	}
	replayed, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != activated {
		t.Fatalf("activation exact replay=%+v, want %+v", replayed, activated)
	}

	second := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"},` +
				`{"role":"user","content":"again"},{"role":"assistant","content":"done"}]`,
		),
		TurnCount:      2,
		ActivatedTools: json.RawMessage(`["endpoint-a"]`),
	}
	secondBatch := projectionSnapshotBatch(
		t, scope, "authority-turn-2", first, second, "",
	)
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, second, secondBatch,
	); err != nil {
		t.Fatal(err)
	}
	// A lost activation response retried after a later dual-write must still
	// return the exact durable transition, not append another generation.
	replayedAfterWrite, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayedAfterWrite != activated {
		t.Fatalf("late activation replay=%+v, want %+v",
			replayedAfterWrite, activated)
	}

	rolledBack, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityRollback,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Route != AgentSessionProjectionRouteLegacy ||
		rolledBack.Generation != 2 ||
		rolledBack.LedgerHeadSequence !=
			int64(len(firstBatch.Events)+len(secondBatch.Events)) {
		t.Fatalf("rolled back=%+v", rolledBack)
	}
	rollbackReplay, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityRollback,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rollbackReplay != rolledBack {
		t.Fatalf("rollback exact replay=%+v, want %+v",
			rollbackReplay, rolledBack)
	}

	reactivated, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reactivated.Route != AgentSessionProjectionRouteLedger ||
		reactivated.Generation != 3 ||
		reactivated.EventID == activated.EventID {
		t.Fatalf("reactivated=%+v", reactivated)
	}
	status, err := f.store.GetAgentSessionProjectionAuthorityStatus(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if status != reactivated {
		t.Fatalf("status=%+v, want %+v", status, reactivated)
	}
}

func TestAgentSessionProjectionAuthorityConcurrentActivationConverges(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	scope := f.scopeA()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	next := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"hello"}]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, next,
		projectionSnapshotBatch(
			t, scope, "authority-concurrent", base, next, "",
		),
	); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	results := make(chan AgentSessionProjectionAuthorityStatus, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := f.store.ControlAgentSessionProjectionAuthority(
				ctx, scope.TenantID, scope.UserID, scope.SessionID,
				AgentSessionProjectionAuthorityActivate,
			)
			results <- result
			errs <- err
		}()
	}
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent activation: %v", err)
		}
	}
	var expected AgentSessionProjectionAuthorityStatus
	for result := range results {
		if expected.EventID == 0 {
			expected = result
			continue
		}
		if result != expected {
			t.Fatalf("concurrent result=%+v, want %+v", result, expected)
		}
	}
	var count int
	if err := f.store.pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM agent_session_projection_authority_events
		 WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		scope.TenantID, scope.UserID, scope.SessionID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("authority rows=%d, want 1", count)
	}
}

func TestAgentSessionProjectionAuthorityPublicAppendUsesLedgerBaseAndReplays(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	scope := f.scopeA()
	initializeEmptyAgentSessionLedgerAuthority(
		t, f.store, scope, "authority-public-append-base",
	)
	messages := json.RawMessage(
		`[{"role":"user","content":"durable side writer"}]`,
	)
	lostStore := storeWithCommitResponseLost(f.store)
	if _, err := lostStore.CommitAgentSessionAppend(
		ctx, scope.UserID, scope.SessionID,
		"authority-public-append", messages,
	); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("append response loss error=%v, want database", err)
	}
	audit, err := f.store.CommitAgentSessionAppend(
		ctx, scope.UserID, scope.SessionID,
		"authority-public-append", messages,
	)
	if err != nil {
		t.Fatalf("append exact replay: %v", err)
	}
	if !audit.Match || audit.PriorState != "match" {
		t.Fatalf("append replay audit=%+v", audit)
	}
	if _, err := f.store.CommitAgentSessionAppend(
		ctx, scope.UserID, scope.SessionID,
		"authority-public-append",
		json.RawMessage(`[{"role":"user","content":"changed"}]`),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed append replay error=%v, want conflict", err)
	}
	session, err := f.store.GetActiveAgentSession(
		ctx, scope.UserID, time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	var projected []map[string]any
	if err := json.Unmarshal(session.Messages, &projected); err != nil {
		t.Fatal(err)
	}
	if len(projected) != 1 ||
		projected[0]["content"] != "durable side writer" {
		t.Fatalf("ledger-authoritative append projection=%+v", projected)
	}
}

func TestAgentSessionProjectionAuthorityActivationRacesSideWriter(
	t *testing.T,
) {
	for iteration := 0; iteration < 8; iteration++ {
		t.Run(fmt.Sprintf("iteration-%d", iteration), func(t *testing.T) {
			f := newAgentEventFixture(t)
			ctx := t.Context()
			scope := f.scopeA()
			empty := agentledger.SessionProjection{
				Messages:       json.RawMessage(`[]`),
				ActivatedTools: json.RawMessage(`[]`),
			}
			if _, err := f.store.CommitAgentSessionTurn(
				ctx, empty,
				projectionSnapshotBatch(
					t, scope,
					fmt.Sprintf("authority-race-base-%d", iteration),
					empty, empty, "",
				),
			); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			errs := make(chan error, 2)
			go func() {
				<-start
				_, err := f.store.ControlAgentSessionProjectionAuthority(
					ctx, scope.TenantID, scope.UserID, scope.SessionID,
					AgentSessionProjectionAuthorityActivate,
				)
				errs <- err
			}()
			go func() {
				<-start
				_, err := f.store.CommitAgentSessionAppend(
					ctx, scope.UserID, scope.SessionID,
					fmt.Sprintf("authority-race-append-%d", iteration),
					json.RawMessage(
						`[{"role":"user","content":"raced append"}]`,
					),
				)
				errs <- err
			}()
			close(start)
			for range 2 {
				if err := <-errs; err != nil {
					t.Fatalf("cutover/append race: %v", err)
				}
			}
			status, err := f.store.GetAgentSessionProjectionAuthorityStatus(
				ctx, scope.TenantID, scope.UserID, scope.SessionID,
			)
			if err != nil {
				t.Fatal(err)
			}
			if status.Route != AgentSessionProjectionRouteLedger ||
				status.Generation != 1 {
				t.Fatalf("race status=%+v", status)
			}
			session, err := f.store.GetActiveAgentSession(
				ctx, scope.UserID, time.Now().Add(-time.Hour),
			)
			if err != nil {
				t.Fatal(err)
			}
			var messages []map[string]any
			if err := json.Unmarshal(session.Messages, &messages); err != nil {
				t.Fatal(err)
			}
			if len(messages) != 1 ||
				messages[0]["content"] != "raced append" {
				t.Fatalf("race projection=%+v", messages)
			}
		})
	}
}

func TestAgentSessionProjectionAuthorityActivationFailsClosed(
	t *testing.T,
) {
	t.Run("empty ledger", func(t *testing.T) {
		f := newAgentEventFixture(t)
		scope := f.scopeA()
		_, err := f.store.ControlAgentSessionProjectionAuthority(
			t.Context(), scope.TenantID, scope.UserID, scope.SessionID,
			AgentSessionProjectionAuthorityActivate,
		)
		if !errors.Is(err, types.ErrInternal) {
			t.Fatalf("empty ledger error=%v, want internal", err)
		}
	})

	t.Run("legacy drift", func(t *testing.T) {
		f := newAgentEventFixture(t)
		ctx := t.Context()
		scope := f.scopeA()
		base := agentledger.SessionProjection{
			Messages:       json.RawMessage(`[]`),
			ActivatedTools: json.RawMessage(`[]`),
		}
		next := agentledger.SessionProjection{
			Messages:       json.RawMessage(`[{"role":"user","content":"safe"}]`),
			TurnCount:      1,
			ActivatedTools: json.RawMessage(`[]`),
		}
		if _, err := f.store.CommitAgentSessionTurn(
			ctx, next,
			projectionSnapshotBatch(
				t, scope, "authority-drift", base, next, "",
			),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.pool.Exec(ctx, `
			UPDATE agent_sessions
			   SET messages='[{"role":"user","content":"drift"}]'::jsonb
			 WHERE id=$1`, scope.SessionID,
		); err != nil {
			t.Fatal(err)
		}
		_, err := f.store.ControlAgentSessionProjectionAuthority(
			ctx, scope.TenantID, scope.UserID, scope.SessionID,
			AgentSessionProjectionAuthorityActivate,
		)
		if !errors.Is(err, types.ErrInternal) {
			t.Fatalf("drift error=%v, want internal", err)
		}
	})

	t.Run("corrupt ledger", func(t *testing.T) {
		f := newAgentEventFixture(t)
		ctx := t.Context()
		scope := f.scopeA()
		base := agentledger.SessionProjection{
			Messages:       json.RawMessage(`[]`),
			ActivatedTools: json.RawMessage(`[]`),
		}
		next := agentledger.SessionProjection{
			Messages:       json.RawMessage(`[{"role":"user","content":"safe"}]`),
			TurnCount:      1,
			ActivatedTools: json.RawMessage(`[]`),
		}
		if _, err := f.store.CommitAgentSessionTurn(
			ctx, next,
			projectionSnapshotBatch(
				t, scope, "authority-corrupt", base, next, "",
			),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.pool.Exec(ctx, `
			UPDATE agent_events SET payload_digest=repeat('f',64)
			 WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
			   AND sequence=2`,
			scope.TenantID, scope.UserID, scope.SessionID,
		); err != nil {
			t.Fatal(err)
		}
		_, err := f.store.ControlAgentSessionProjectionAuthority(
			ctx, scope.TenantID, scope.UserID, scope.SessionID,
			AgentSessionProjectionAuthorityActivate,
		)
		if !errors.Is(err, types.ErrInternal) {
			t.Fatalf("corrupt ledger error=%v, want internal", err)
		}
	})
}

func TestAgentSessionProjectionAuthorityRejectsMismatchedExactScope(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	scope := f.scopeA()
	empty := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, empty,
		projectionSnapshotBatch(
			t, scope, "authority-exact-scope", empty, empty, "",
		),
	); err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []agentledger.Scope{
		{TenantID: f.tenantB, UserID: f.userA, SessionID: f.sessionA},
		{TenantID: f.tenantA, UserID: f.userB, SessionID: f.sessionA},
		{TenantID: f.tenantA, UserID: f.userA, SessionID: f.sessionB},
	} {
		if _, err := f.store.ControlAgentSessionProjectionAuthority(
			ctx, wrong.TenantID, wrong.UserID, wrong.SessionID,
			AgentSessionProjectionAuthorityActivate,
		); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("wrong scope=%+v error=%v, want not found", wrong, err)
		}
	}
	var count int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_session_projection_authority_events
		  WHERE session_id=ANY($1)`,
		[]int64{f.sessionA, f.sessionB},
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("mismatched exact scope appended %d authority rows", count)
	}
}

func TestAgentSessionProjectionAuthorityLoaderRejectsDamagedGeneration(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	scope := f.scopeA()
	if _, err := f.store.pool.Exec(ctx, `
		INSERT INTO agent_session_projection_authority_events (
		    tenant_id,user_id,session_id,generation,action,
		    ledger_head_sequence,legacy_digest,ledger_digest
		) VALUES ($1,$2,$3,2,'activate',1,repeat('a',64),repeat('a',64))`,
		scope.TenantID, scope.UserID, scope.SessionID,
	); err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setAgentEventTenantContext(ctx, tx, scope.TenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := agentSessionProjectionLedgerAuthoritative(
		ctx, tx, scope,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("damaged generation error=%v, want internal", err)
	}
}

func TestAgentSessionProjectionAuthorityActiveRouteRejectsLaterDrift(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	scope := f.scopeA()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	next := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"safe"}]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, next,
		projectionSnapshotBatch(
			t, scope, "authority-active-drift", base, next, "",
		),
	); err != nil {
		t.Fatal(err)
	}
	activated, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx, `
		UPDATE agent_sessions
		   SET messages='[{"role":"user","content":"unsafe legacy drift"}]'::jsonb
		 WHERE id=$1`, scope.SessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetAgentSessionProjectionAuthorityStatus(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("active status drift error=%v, want internal", err)
	}
	if _, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityRollback,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("unsafe rollback error=%v, want internal", err)
	}
	var count int
	if err := f.store.pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM agent_session_projection_authority_events
		 WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		scope.TenantID, scope.UserID, scope.SessionID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 || activated.Generation != 1 {
		t.Fatalf("failed rollback mutated authority rows=%d activated=%+v",
			count, activated)
	}
}

func TestAgentSessionProjectionAuthorityEvidenceTamperFailsClosed(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate string
	}{
		{
			name: `matching but false digests`,
			mutate: `UPDATE agent_session_projection_authority_events
				SET legacy_digest=repeat('f',64),
				    ledger_digest=repeat('f',64)
				WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		},
		{
			name: `head inside batch`,
			mutate: `UPDATE agent_session_projection_authority_events
				SET ledger_head_sequence=2
				WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newAgentEventFixture(t)
			ctx := t.Context()
			scope := f.scopeA()
			base := agentledger.SessionProjection{
				Messages:       json.RawMessage(`[]`),
				ActivatedTools: json.RawMessage(`[]`),
			}
			next := agentledger.SessionProjection{
				Messages: json.RawMessage(
					`[{"role":"user","content":"safe"}]`,
				),
				TurnCount:      1,
				ActivatedTools: json.RawMessage(`[]`),
			}
			batch := projectionSnapshotBatch(
				t, scope, "authority-evidence-tamper", base, next, "",
			)
			if len(batch.Events) < 3 {
				t.Fatalf("fixture batch too short: %d", len(batch.Events))
			}
			if _, err := f.store.CommitAgentSessionTurn(
				ctx, next, batch,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.ControlAgentSessionProjectionAuthority(
				ctx, scope.TenantID, scope.UserID, scope.SessionID,
				AgentSessionProjectionAuthorityActivate,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.pool.Exec(
				ctx, test.mutate,
				scope.TenantID, scope.UserID, scope.SessionID,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.GetAgentSessionProjectionAuthorityStatus(
				ctx, scope.TenantID, scope.UserID, scope.SessionID,
			); !errors.Is(err, types.ErrInternal) {
				t.Fatalf("tampered status error=%v, want internal", err)
			}
			tx, err := f.store.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			if err := setAgentEventTenantContext(
				ctx, tx, scope.TenantID,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := agentSessionProjectionLedgerAuthoritative(
				ctx, tx, scope,
			); !errors.Is(err, types.ErrInternal) {
				t.Fatalf("tampered runtime route error=%v, want internal", err)
			}
		})
	}
}

func TestAgentSessionProjectionAuthorityOperatorDriftFailsClosed(
	t *testing.T,
) {
	tests := []struct {
		name    string
		mutate  string
		restore string
	}{
		{
			name: "session update",
			mutate: `GRANT UPDATE (messages) ON agent_sessions
				TO vane_agent_session_projection_operator`,
			restore: `REVOKE UPDATE (messages) ON agent_sessions
				FROM vane_agent_session_projection_operator`,
		},
		{
			name: "server owned authority insert",
			mutate: `GRANT INSERT (created_at)
				ON agent_session_projection_authority_events
				TO vane_agent_session_projection_operator`,
			restore: `REVOKE INSERT (created_at)
				ON agent_session_projection_authority_events
				FROM vane_agent_session_projection_operator`,
		},
		{
			name: "unrelated table read",
			mutate: `GRANT SELECT ON users
				TO vane_agent_session_projection_operator`,
			restore: `REVOKE SELECT ON users
				FROM vane_agent_session_projection_operator`,
		},
		{
			name: "unrelated column read",
			mutate: `GRANT SELECT (name) ON users
				TO vane_agent_session_projection_operator`,
			restore: `REVOKE SELECT (name) ON users
				FROM vane_agent_session_projection_operator`,
		},
		{
			name: "unrelated column insert",
			mutate: `GRANT INSERT (name) ON users
				TO vane_agent_session_projection_operator`,
			restore: `REVOKE INSERT (name) ON users
				FROM vane_agent_session_projection_operator`,
		},
		{
			name: "schema create",
			mutate: `GRANT CREATE ON SCHEMA public
				TO vane_agent_session_projection_operator`,
			restore: `REVOKE CREATE ON SCHEMA public
				FROM vane_agent_session_projection_operator`,
		},
		{
			name: "operator named schema hijack",
			mutate: `CREATE SCHEMA vane_agent_session_projection_operator
				AUTHORIZATION vane_agent_session_projection_operator;
				CREATE TABLE
				  vane_agent_session_projection_operator.
				    agent_session_projection_authority_events (
				      id BIGSERIAL PRIMARY KEY
				    )`,
			restore: `DROP SCHEMA
				vane_agent_session_projection_operator CASCADE`,
		},
		{
			name: "security definer execute",
			mutate: `CREATE FUNCTION public.agent_projection_test_escalation()
				RETURNS void LANGUAGE sql SECURITY DEFINER
				SET search_path='pg_catalog, public'
				AS 'SELECT 1'`,
			restore: `DROP FUNCTION public.agent_projection_test_escalation()`,
		},
		{
			name: "role config",
			mutate: `ALTER ROLE vane_agent_session_projection_operator
				SET statement_timeout='10s'`,
			restore: `ALTER ROLE vane_agent_session_projection_operator
				RESET statement_timeout`,
		},
		{
			name: "extra membership",
			mutate: `DO $$
				BEGIN
				  IF NOT EXISTS (
				    SELECT 1 FROM pg_roles
				     WHERE rolname='vane_agent_projection_test_member'
				  ) THEN
				    EXECUTE 'CREATE ROLE vane_agent_projection_test_member NOLOGIN';
				  END IF;
				  EXECUTE 'GRANT vane_agent_session_projection_operator
				           TO vane_agent_projection_test_member';
				END $$`,
			restore: `DO $$
				BEGIN
				  EXECUTE 'REVOKE vane_agent_session_projection_operator
				           FROM vane_agent_projection_test_member';
				  EXECUTE 'DROP ROLE vane_agent_projection_test_member';
				END $$`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newAgentEventFixture(t)
			ctx := t.Context()
			scope := f.scopeA()
			base := agentledger.SessionProjection{
				Messages:       json.RawMessage(`[]`),
				ActivatedTools: json.RawMessage(`[]`),
			}
			next := agentledger.SessionProjection{
				Messages: json.RawMessage(
					`[{"role":"user","content":"safe"}]`,
				),
				TurnCount:      1,
				ActivatedTools: json.RawMessage(`[]`),
			}
			if _, err := f.store.CommitAgentSessionTurn(
				ctx, next,
				projectionSnapshotBatch(
					t, scope, "authority-role-drift", base, next, "",
				),
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.pool.Exec(ctx, test.mutate); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := f.store.pool.Exec(
					context.WithoutCancel(ctx), test.restore,
				); err != nil {
					t.Errorf("restore operator role: %v", err)
				}
			})
			if _, err := f.store.ControlAgentSessionProjectionAuthority(
				ctx, scope.TenantID, scope.UserID, scope.SessionID,
				AgentSessionProjectionAuthorityActivate,
			); !errors.Is(err, types.ErrInternal) {
				t.Fatalf("operator drift error=%v, want internal", err)
			}
			var count int
			if err := f.store.pool.QueryRow(ctx, `
				SELECT count(*)
				  FROM agent_session_projection_authority_events
				 WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
				scope.TenantID, scope.UserID, scope.SessionID,
			).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("operator drift appended %d rows", count)
			}
		})
	}
}
