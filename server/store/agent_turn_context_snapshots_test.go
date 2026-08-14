package store

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/YouToco/vane/server/agentcontext"
	"github.com/YouToco/vane/server/agentledger"
	"github.com/YouToco/vane/server/types"
	"github.com/jackc/pgx/v5"
)

func TestSealAgentTurnContextSnapshotLegacySkipsWithoutWrite(t *testing.T) {
	f := newAgentEventFixture(t)
	candidate := testAgentTurnCandidate(t, f, "legacy-turn", 1, false)
	seal, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !seal.Skipped || seal.Sealed {
		t.Fatalf("legacy seal=%+v", seal)
	}
	var count int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_turn_context_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("legacy route wrote %d snapshots", count)
	}
}

func TestSealAgentTurnContextSnapshotLedgerExactReplayAndConflict(t *testing.T) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-ledger-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())

	candidate := testAgentTurnCandidate(t, f, "ledger-turn", 1, false)
	first, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Sealed || first.Skipped ||
		first.Snapshot.SealLedgerHeadSequence <= 0 ||
		first.Snapshot.SealAuthorityGeneration != 1 {
		t.Fatalf("first seal=%+v", first)
	}
	replayed, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Snapshot.SnapshotDigest !=
		first.Snapshot.SnapshotDigest {
		t.Fatalf("replay=%+v first=%+v", replayed, first)
	}

	changed := candidate
	changed.Model = "changed"
	changed.Digest = ""
	rebuilt, err := agentcontext.Build(agentcontext.BuildInput{
		Scope: changed.Scope, TurnID: changed.TurnID,
		ContextStep: changed.ContextStep, Model: changed.Model,
		SystemPrompt: "system", Current: agentcontext.AtomicGroup{
			Trust: agentcontext.TrustTrusted,
			Messages: []agentcontext.Message{{
				Role: "user", Content: "question",
			}},
		},
		ContextWindowTokens: 4096, MaxOutputTokens: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, rebuilt.Candidate,
	)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed replay error=%v, want conflict", err)
	}
}

func TestSealAgentTurnContextSnapshotConcurrentReplayConverges(t *testing.T) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-concurrent-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	candidate := testAgentTurnCandidate(t, f, "same-turn", 1, false)

	const workers = 8
	results := make(chan agentcontext.SealResult, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			result, err := f.store.SealAgentTurnContextSnapshot(
				t.Context(), candidate.Scope, candidate,
			)
			results <- result
			errs <- err
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var digest string
	for result := range results {
		if !result.Sealed {
			t.Fatalf("result=%+v", result)
		}
		if digest == "" {
			digest = result.Snapshot.SnapshotDigest
		}
		if result.Snapshot.SnapshotDigest != digest {
			t.Fatalf("snapshot digests diverged")
		}
	}
	var count int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_turn_context_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND turn_id='same-turn' AND context_step=1`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent replay wrote %d rows", count)
	}
}

func TestSealAgentTurnContextSnapshotRejectsRollbackGenerationTamper(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-generation-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	candidate := testAgentTurnCandidate(t, f, "generation-turn", 1, false)
	first, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	)
	if err != nil {
		t.Fatal(err)
	}
	status, err := f.store.ControlAgentSessionProjectionAuthority(
		t.Context(), f.tenantA, f.userA, f.sessionA,
		AgentSessionProjectionAuthorityRollback,
	)
	if err != nil || status.Generation != 2 {
		t.Fatalf("rollback status=%+v err=%v", status, err)
	}
	tampered := first.Snapshot
	tampered.SealAuthorityGeneration = status.Generation
	tampered.SnapshotDigest, err = agentcontext.TurnSnapshotDigest(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE agent_turn_context_snapshots
		    SET seal_authority_generation=$1,snapshot_digest=$2
		  WHERE tenant_id=$3 AND user_id=$4 AND session_id=$5
		    AND turn_id=$6 AND context_step=$7`,
		tampered.SealAuthorityGeneration, tampered.SnapshotDigest,
		f.tenantA, f.userA, f.sessionA, candidate.TurnID,
		candidate.ContextStep,
	); err != nil {
		t.Fatal(err)
	}
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	if _, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	); err == nil {
		t.Fatal("snapshot bound to rollback generation unexpectedly replayed")
	}
}

func TestSealAgentTurnContextSnapshotRejectsDuplicateColumnDrift(t *testing.T) {
	mutations := map[string]string{
		"candidate digest": `UPDATE agent_turn_context_snapshots
		    SET candidate_digest=repeat('f',64)
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		"replayable": `UPDATE agent_turn_context_snapshots
		    SET replayable=NOT replayable
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
	}
	for name, statement := range mutations {
		t.Run(name, func(t *testing.T) {
			f := newAgentEventFixture(t)
			initializeAgentSessionProjectionFixture(
				t, f, "context-duplicate-column-"+
					strings.ReplaceAll(name, " ", "-"),
			)
			activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
			candidate := testAgentTurnCandidate(
				t, f, "duplicate-column-turn", 1, false,
			)
			if _, err := f.store.SealAgentTurnContextSnapshot(
				t.Context(), candidate.Scope, candidate,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.pool.Exec(
				t.Context(), statement, f.tenantA, f.userA, f.sessionA,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.SealAgentTurnContextSnapshot(
				t.Context(), candidate.Scope, candidate,
			); err == nil {
				t.Fatal("relational/JSON duplicate column drift replayed")
			}
		})
	}
}

func TestSealAgentTurnContextSnapshotUntrustedRawNeverReachesDatabase(t *testing.T) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-untrusted-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	const attack = "EXTERNAL-RAW-DO-NOT-PERSIST"
	candidate := testAgentTurnCandidate(t, f, "untrusted-turn", 1, true)
	if strings.Contains(string(mustJSON(t, candidate)), attack) {
		t.Fatal("test candidate unexpectedly contains raw external content")
	}
	if _, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT candidate_snapshot::text
		   FROM agent_turn_context_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND turn_id='untrusted-turn' AND context_step=1`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), attack) {
		t.Fatal("raw external content reached database snapshot")
	}
}

func TestSealAgentTurnContextSnapshotLedgerCorruptionFailsWithoutWrite(t *testing.T) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-corrupt-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE agent_events SET payload_digest=repeat('f',64)
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND sequence=1`,
		f.tenantA, f.userA, f.sessionA,
	); err != nil {
		t.Fatal(err)
	}
	candidate := testAgentTurnCandidate(t, f, "corrupt-turn", 1, false)
	if _, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	); err == nil {
		t.Fatal("corrupt ledger unexpectedly sealed")
	}
	var count int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_turn_context_snapshots
		  WHERE tenant_id=$1`, f.tenantA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("corrupt ledger wrote %d snapshots", count)
	}
}

func TestAgentTurnContextSnapshotRLSAndMutationDenial(t *testing.T) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-rls-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	candidate := testAgentTurnCandidate(t, f, "rls-turn", 1, false)
	if _, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	); err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.pool.BeginTx(
		t.Context(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if err := setAgentEventRuntimeContext(
		t.Context(), tx, f.tenantB,
	); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := tx.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_turn_context_snapshots`,
	).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("tenant B sees %d tenant A snapshots", visible)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	mutations := []string{
		`UPDATE agent_turn_context_snapshots SET replayable=false`,
		`DELETE FROM agent_turn_context_snapshots`,
		`TRUNCATE agent_turn_context_snapshots`,
	}
	for _, statement := range mutations {
		mutationTx, err := f.store.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := setAgentEventRuntimeContext(
			t.Context(), mutationTx, f.tenantA,
		); err != nil {
			_ = mutationTx.Rollback(t.Context())
			t.Fatal(err)
		}
		_, mutationErr := mutationTx.Exec(t.Context(), statement)
		_ = mutationTx.Rollback(t.Context())
		if mutationErr == nil {
			t.Fatalf("vane_app mutation succeeded: %s", statement)
		}
	}
}

func TestPurgeTenantRemovesAgentTurnContextSnapshots(t *testing.T) {
	f := newAgentEventFixture(t)
	initializeAgentSessionProjectionFixture(t, f, "context-purge-base")
	activateAgentSessionProjectionFixture(t, f.store, f.scopeA())
	candidate := testAgentTurnCandidate(t, f, "purge-turn", 1, false)
	if _, err := f.store.SealAgentTurnContextSnapshot(
		t.Context(), candidate.Scope, candidate,
	); err != nil {
		t.Fatal(err)
	}
	report, err := f.store.PurgeTenant(t.Context(), f.tenantA, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows["agent_turn_context_snapshots"] != 1 {
		t.Fatalf("purge report=%+v", report)
	}
	var count int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM agent_turn_context_snapshots
		  WHERE tenant_id=$1`, f.tenantA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("purge left %d snapshots", count)
	}
}

func testAgentTurnCandidate(
	t *testing.T,
	f agentEventFixture,
	turnID string,
	step int,
	untrusted bool,
) agentcontext.CandidateSnapshot {
	t.Helper()
	trust := agentcontext.TrustTrusted
	content := "question"
	messages := []agentcontext.Message{{
		Role: "user", Content: content,
	}}
	var firstOrdinal, lastOrdinal int64
	if untrusted {
		trust = agentcontext.TrustUntrustedCurrent
		firstOrdinal, lastOrdinal = 1, 3
		messages = []agentcontext.Message{
			{Role: "user", Content: "question"},
			{Role: "assistant", ToolCalls: []agentcontext.ToolCall{{
				ID: "external-1", Name: "read", Arguments: `{}`,
			}}},
			{
				Role: "tool", ToolCallID: "external-1",
				Content: "EXTERNAL-RAW-DO-NOT-PERSIST",
			},
		}
	}
	result, err := agentcontext.Build(agentcontext.BuildInput{
		Scope: agentcontext.Scope{
			TenantID: f.tenantA, UserID: f.userA,
			SessionID: f.sessionA,
		},
		TurnID: turnID, ContextStep: step, Model: "model",
		SystemPrompt: "system",
		Current: agentcontext.AtomicGroup{
			FirstMessageOrdinal: firstOrdinal,
			LastMessageOrdinal:  lastOrdinal,
			Trust:               trust, Messages: messages,
		},
		ContextWindowTokens: 4096, MaxOutputTokens: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Candidate
}

func activateAgentSessionProjectionFixture(
	t *testing.T,
	st *Store,
	scope agentledger.Scope,
) {
	t.Helper()
	status, err := st.ControlAgentSessionProjectionAuthority(
		t.Context(), scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatal(err)
	}
	if status.Route != AgentSessionProjectionRouteLedger {
		t.Fatalf("authority=%+v", status)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
