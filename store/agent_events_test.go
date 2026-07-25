package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/agentledger"
	"github.com/YouToco/vane/types"
)

type agentEventFixture struct {
	store *Store

	tenantA  int64
	userA    int64
	sessionA int64

	tenantB  int64
	userB    int64
	sessionB int64
}

func newAgentEventFixture(t *testing.T) agentEventFixture {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 Agent event ledger 真库测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)

	create := func(tag string) (int64, int64, int64) {
		var tenantID int64
		if err := st.pool.QueryRow(t.Context(),
			`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
		).Scan(&tenantID); err != nil {
			t.Fatal(err)
		}
		user, err := st.UpsertUserByOpenID(
			t.Context(), "agent_event_"+tag+"_"+uuid.NewString(), "ledger "+tag,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(t.Context(),
			`INSERT INTO memberships (tenant_id, user_id, role)
			 VALUES ($1, $2, 'owner')`,
			tenantID, user.ID,
		); err != nil {
			t.Fatal(err)
		}
		var sessionID int64
		if err := st.pool.QueryRow(t.Context(),
			`INSERT INTO agent_sessions (tenant_id, user_id)
			 VALUES ($1, $2) RETURNING id`,
			tenantID, user.ID,
		).Scan(&sessionID); err != nil {
			t.Fatal(err)
		}
		return tenantID, user.ID, sessionID
	}

	tenantA, userA, sessionA := create("a")
	tenantB, userB, sessionB := create("b")
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, tenantID := range []int64{tenantA, tenantB} {
			cleanupExec(ctx, t, st,
				`DELETE FROM agent_events WHERE tenant_id=$1`, tenantID)
			cleanupExec(ctx, t, st,
				`DELETE FROM agent_sessions WHERE tenant_id=$1`, tenantID)
			cleanupExec(ctx, t, st,
				`DELETE FROM memberships WHERE tenant_id=$1`, tenantID)
			cleanupExec(ctx, t, st,
				`DELETE FROM tenants WHERE id=$1`, tenantID)
		}
		cleanupExec(ctx, t, st,
			`DELETE FROM users WHERE id=ANY($1)`, []int64{userA, userB})
	})
	return agentEventFixture{
		store:   st,
		tenantA: tenantA, userA: userA, sessionA: sessionA,
		tenantB: tenantB, userB: userB, sessionB: sessionB,
	}
}

func (f agentEventFixture) scopeA() agentledger.Scope {
	return agentledger.Scope{
		TenantID: f.tenantA, UserID: f.userA, SessionID: f.sessionA,
	}
}

func TestAgentEventsAppendListReplayAndExactReplay(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	batch := agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "turn-1-input",
		Events: []agentledger.Input{
			{Kind: agentledger.KindTurnStarted, Body: []byte(`{"turn_id":"turn-1"}`)},
			{Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"hello","trust":"trusted"}`)},
		},
	}
	first, err := f.store.AppendAgentEvents(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Sequence != 1 || first[1].Sequence != 2 {
		t.Fatalf("first append=%+v", first)
	}

	// Whitespace and object key order canonicalize to the exact same bytes.
	retry := batch
	retry.Events = []agentledger.Input{
		{Kind: agentledger.KindTurnStarted, Body: []byte(`{ "turn_id" : "turn-1" }`)},
		{Kind: agentledger.KindUserMessage, Body: []byte(`{"trust":"trusted","text":"hello"}`)},
	}
	replayed, err := f.store.AppendAgentEvents(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != len(first) {
		t.Fatalf("replay size=%d, want %d", len(replayed), len(first))
	}
	for i := range first {
		if replayed[i].ID != first[i].ID ||
			replayed[i].Sequence != first[i].Sequence ||
			string(replayed[i].Payload) != string(first[i].Payload) {
			t.Fatalf("exact replay[%d]=%+v, want %+v", i, replayed[i], first[i])
		}
	}

	page, err := f.store.ListAgentEvents(ctx, f.scopeA(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Sequence != 1 {
		t.Fatalf("first page=%+v", page)
	}
	page, err = f.store.ListAgentEvents(ctx, f.scopeA(), page[0].Sequence, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Sequence != 2 {
		t.Fatalf("second page=%+v", page)
	}
	all, err := f.store.ReplayAgentEvents(ctx, f.scopeA())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].ID != first[0].ID || all[1].ID != first[1].ID {
		t.Fatalf("ReplayAgentEvents()=%+v", all)
	}
}

func TestCommitAgentSessionTurnAtomicProjectionAndShadowResync(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	firstProjection := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]`,
		),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	emptyProjection := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	firstBatch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-shadow-1", emptyProjection, firstProjection, "",
	)
	firstAudit, err := f.store.CommitAgentSessionTurn(
		ctx, firstProjection, firstBatch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !firstAudit.Match || firstAudit.PriorState != "uninitialized" ||
		firstAudit.Reason != "initialized" {
		t.Fatalf("first audit=%+v", firstAudit)
	}
	assertStoredAgentSessionProjection(t, f, firstProjection)

	callback := json.RawMessage(
		`[{"role":"user","content":"[卡片回调] resolved"}]`,
	)
	if err := f.store.AppendAgentSessionMessages(
		ctx, f.sessionA, callback,
	); err != nil {
		t.Fatal(err)
	}
	callbackProjection := agentledger.SessionProjection{
		Messages: json.RawMessage(`[
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"hi"},
			{"role":"user","content":"[卡片回调] resolved"}
		]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	secondProjection := agentledger.SessionProjection{
		Messages: json.RawMessage(`[
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"hi"},
			{"role":"user","content":"[卡片回调] resolved"},
			{"role":"user","content":"continue"},
			{"role":"assistant","content":"done"}
		]`),
		TurnCount:      2,
		ActivatedTools: json.RawMessage(`["endpoint_a"]`),
	}
	secondBatch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-shadow-2", callbackProjection, secondProjection, "",
	)
	secondAudit, err := f.store.CommitAgentSessionTurn(
		ctx, secondProjection, secondBatch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !secondAudit.Match ||
		secondAudit.PriorState != "unsupported_writer_or_projection_drift" ||
		secondAudit.Reason !=
			"resynced_after_unsupported_writer_or_projection_drift" {
		t.Fatalf("second audit=%+v", secondAudit)
	}
	assertStoredAgentSessionProjection(t, f, secondProjection)

	replayed, err := f.store.ReplayAgentEvents(ctx, f.scopeA())
	if err != nil {
		t.Fatal(err)
	}
	projected, err := agentledger.ProjectLatestSessionSnapshot(replayed)
	if err != nil {
		t.Fatal(err)
	}
	gotDigest, err := agentledger.ProjectionDigest(projected)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := agentledger.ProjectionDigest(secondProjection)
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("event projection digest=%s want=%s", gotDigest, wantDigest)
	}
}

func TestCommitAgentSessionTurnExactReplayAndConflict(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	projection := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"hello"}]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	batch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-exact-replay",
		agentledger.SessionProjection{
			Messages:       json.RawMessage(`[]`),
			ActivatedTools: json.RawMessage(`[]`),
		},
		projection, "",
	)
	if _, err := f.store.CommitAgentSessionTurn(ctx, projection, batch); err != nil {
		t.Fatal(err)
	}
	replayAudit, err := f.store.CommitAgentSessionTurn(ctx, projection, batch)
	if err != nil {
		t.Fatal(err)
	}
	if !replayAudit.Match {
		t.Fatalf("replay audit=%+v", replayAudit)
	}

	changed := batch
	changed.Events = append([]agentledger.Input(nil), batch.Events...)
	changed.Events[1] = agentledger.Input{
		Kind: agentledger.KindUserMessage,
		Body: json.RawMessage(
			`{"schema_version":"vane.agent-session-projection/v1",` +
				`"turn_id":"turn-exact-replay",` +
				`"message":{"role":"user","content":"changed"}}`,
		),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, projection, changed,
	); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("changed replay error=%v, want CodeConflict", err)
	}
}

func TestCommitAgentSessionTurnRejectsStaleBaseProjection(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	desired := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]`,
		),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	batch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-stale-base", base, desired, "",
	)
	callback := json.RawMessage(
		`[{"role":"user","content":"[卡片回调] concurrent"}]`,
	)
	if err := f.store.AppendAgentSessionMessages(
		ctx, f.sessionA, callback,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, desired, batch,
	); types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("stale base error=%v, want CodeConflict", err)
	}
	var eventCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("stale commit appended %d events", eventCount)
	}
	var messages json.RawMessage
	if err := f.store.pool.QueryRow(ctx,
		`SELECT messages FROM agent_sessions WHERE id=$1`,
		f.sessionA,
	).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	var decoded []json.RawMessage
	if err := json.Unmarshal(messages, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("concurrent callback was overwritten: %s", messages)
	}
}

func TestCommitAgentSessionTurnConcurrentWritersUseBaseFence(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	projections := []agentledger.SessionProjection{
		{
			Messages:       json.RawMessage(`[{"role":"user","content":"a"}]`),
			TurnCount:      1,
			ActivatedTools: json.RawMessage(`[]`),
		},
		{
			Messages:       json.RawMessage(`[{"role":"user","content":"b"}]`),
			TurnCount:      1,
			ActivatedTools: json.RawMessage(`[]`),
		},
	}
	batches := []agentledger.AppendBatch{
		projectionSnapshotBatch(
			t, f.scopeA(), "turn-concurrent-a", base, projections[0], "",
		),
		projectionSnapshotBatch(
			t, f.scopeA(), "turn-concurrent-b", base, projections[1], "",
		),
	}
	errs := make(chan error, len(batches))
	var wg sync.WaitGroup
	for i := range batches {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := f.store.CommitAgentSessionTurn(
				ctx, projections[index], batches[index],
			)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes, conflicts := 0, 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		switch types.CodeOf(err) {
		case types.CodeConflict:
			conflicts++
		default:
			t.Fatalf("concurrent commit error=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	var batchCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(DISTINCT batch_idempotency_key)
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&batchCount); err != nil {
		t.Fatal(err)
	}
	if batchCount != 1 {
		t.Fatalf("committed event batches=%d want=1", batchCount)
	}
}

func TestCommitAgentSessionTurnRollsBackEventsWhenProjectionUpdateFails(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	tooLargeForPostgresInt := int64(1 << 31)
	if int64(int(tooLargeForPostgresInt)) != tooLargeForPostgresInt {
		t.Skip("requires an int wider than PostgreSQL int4")
	}
	projection := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"hello"}]`),
		TurnCount:      int(tooLargeForPostgresInt),
		ActivatedTools: json.RawMessage(`[]`),
	}
	batch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-rollback",
		agentledger.SessionProjection{
			Messages:       json.RawMessage(`[]`),
			ActivatedTools: json.RawMessage(`[]`),
		},
		projection, "",
	)
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, projection, batch,
	); err == nil {
		t.Fatal("out-of-range projection update unexpectedly succeeded")
	}
	var eventCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("rolled-back transaction left %d events", eventCount)
	}
	var turnCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT turn_count FROM agent_sessions WHERE id=$1`,
		f.sessionA,
	).Scan(&turnCount); err != nil {
		t.Fatal(err)
	}
	if turnCount != 0 {
		t.Fatalf("rolled-back transaction changed turn_count to %d", turnCount)
	}
}

func TestCommitAgentSessionTurnRejectsProjectionBatchMismatchAtomically(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	desired := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"desired"}]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	other := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"other"}]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	batch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-projection-mismatch", base, other, "",
	)

	if _, err := f.store.CommitAgentSessionTurn(
		ctx, desired, batch,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("projection/batch mismatch error=%v, want validation", err)
	}
	assertAgentTurnCommitLeftNoMutation(t, f)
}

func TestCommitAgentSessionTurnRejectsIllegalSnapshotGenerationAtomically(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	desired := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[{"role":"user","content":"desired"}]`),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	batch := projectionSnapshotBatch(
		t, f.scopeA(), "turn-illegal-generation", base, desired, "",
	)
	batch.Events[len(batch.Events)-1].Body = json.RawMessage(
		`{"schema_version":"vane.agent-session-projection/v1",` +
			`"generation":"delta","turn_id":"turn-illegal-generation",` +
			`"turn_count":1,"activated_tools":[],"outcome":"reply"}`,
	)

	if _, err := f.store.CommitAgentSessionTurn(
		ctx, desired, batch,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("illegal latest generation error=%v, want validation", err)
	}
	assertAgentTurnCommitLeftNoMutation(t, f)
}

func assertAgentTurnCommitLeftNoMutation(t *testing.T, f agentEventFixture) {
	t.Helper()
	var eventCount, turnCount int
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT
			(SELECT count(*) FROM agent_events
			  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3),
			(SELECT turn_count FROM agent_sessions
			  WHERE id=$3 AND tenant_id=$1 AND user_id=$2)`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&eventCount, &turnCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 || turnCount != 0 {
		t.Fatalf("rejected commit left events=%d turn_count=%d",
			eventCount, turnCount)
	}
}

func projectionSnapshotBatch(
	t *testing.T,
	scope agentledger.Scope,
	turnID string,
	base agentledger.SessionProjection,
	projection agentledger.SessionProjection,
	confirmationAction string,
) agentledger.AppendBatch {
	t.Helper()
	baseDigest, err := agentledger.ProjectionDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := agentledger.BuildProjectionSnapshotBatch(
		agentledger.ProjectionSnapshotInput{
			Scope:                scope,
			TurnID:               turnID,
			BaseProjectionDigest: baseDigest,
			Messages:             projection.Messages,
			TurnCount:            projection.TurnCount,
			ActivatedTools:       projection.ActivatedTools,
			ConfirmationAction:   confirmationAction,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func assertStoredAgentSessionProjection(
	t *testing.T,
	f agentEventFixture,
	want agentledger.SessionProjection,
) {
	t.Helper()
	var got agentledger.SessionProjection
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT messages, turn_count, activated_tools
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		f.sessionA, f.tenantA, f.userA,
	).Scan(&got.Messages, &got.TurnCount, &got.ActivatedTools); err != nil {
		t.Fatal(err)
	}
	gotDigest, err := agentledger.ProjectionDigest(got)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := agentledger.ProjectionDigest(want)
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("stored projection digest=%s want=%s", gotDigest, wantDigest)
	}
}

func TestAgentEventsIdempotencyConflicts(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "same-key",
		Events: []agentledger.Input{
			{Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"hello"}`)},
			{Kind: agentledger.KindTurnCompleted, Body: []byte(`{"outcome":"reply"}`)},
		},
	}
	if _, err := f.store.AppendAgentEvents(ctx, base); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		events []agentledger.Input
	}{
		{
			name: "changed body",
			events: []agentledger.Input{
				{Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"changed"}`)},
				base.Events[1],
			},
		},
		{
			name: "changed kind",
			events: []agentledger.Input{
				{Kind: agentledger.KindAssistantMessage, Body: []byte(`{"text":"hello"}`)},
				base.Events[1],
			},
		},
		{name: "changed order", events: []agentledger.Input{base.Events[1], base.Events[0]}},
		{name: "changed size", events: []agentledger.Input{base.Events[0]}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
				Scope: f.scopeA(), IdempotencyKey: base.IdempotencyKey,
				Events: tt.events,
			})
			if !errors.Is(err, types.ErrConflict) {
				t.Fatalf("AppendAgentEvents() error=%v, want conflict", err)
			}
		})
	}
	var count int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(base.Events) {
		t.Fatalf("conflicting replays changed row count=%d", count)
	}
}

func TestAgentEventsConcurrentSequenceAndReplay(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	const writers = 16
	results := make([]agentledger.Event, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			events, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
				Scope: f.scopeA(), IdempotencyKey: fmt.Sprintf("writer-%02d", i),
				Events: []agentledger.Input{{
					Kind: agentledger.KindUserMessage,
					Body: []byte(fmt.Sprintf(`{"writer":%d}`, i)),
				}},
			})
			errs[i] = err
			if len(events) == 1 {
				results[i] = events[0]
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	sequences := make([]int64, writers)
	for i := range results {
		sequences[i] = results[i].Sequence
	}
	slices.Sort(sequences)
	for i, sequence := range sequences {
		if sequence != int64(i+1) {
			t.Fatalf("sequences=%v", sequences)
		}
	}

	// Concurrent response-lost retries all resolve to one physical batch.
	const retries = 12
	ids := make([]int64, retries)
	retryErrs := make([]error, retries)
	for i := range retries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			events, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
				Scope: f.scopeA(), IdempotencyKey: "shared-response-lost",
				Events: []agentledger.Input{{
					Kind: agentledger.KindTurnCompleted,
					Body: []byte(`{"outcome":"reply"}`),
				}},
			})
			retryErrs[i] = err
			if len(events) == 1 {
				ids[i] = events[0].ID
			}
		}()
	}
	wg.Wait()
	for i, err := range retryErrs {
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("retry ids=%v", ids)
		}
	}
	var sharedCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key='shared-response-lost'`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&sharedCount); err != nil {
		t.Fatal(err)
	}
	if sharedCount != 1 {
		t.Fatalf("concurrent exact replay rows=%d, want 1", sharedCount)
	}
}

func TestAgentEventsScopeAndAtomicValidation(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	valid := agentledger.Input{
		Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"safe"}`),
	}
	secretInvalid := "duplicate-secret-must-not-leak"
	_, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "atomic-invalid",
		Events: []agentledger.Input{
			valid,
			{
				Kind: agentledger.KindToolCall,
				Body: []byte(
					`{"x":"` + secretInvalid + `","x":"changed"}`,
				),
			},
		},
	})
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid atomic batch error=%v", err)
	}
	if strings.Contains(err.Error(), secretInvalid) {
		t.Fatalf("validation error leaked event payload: %v", err)
	}
	var count int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid batch partially appended %d rows", count)
	}

	wrongScopes := []agentledger.Scope{
		{TenantID: f.tenantB, UserID: f.userA, SessionID: f.sessionA},
		{TenantID: f.tenantA, UserID: f.userB, SessionID: f.sessionA},
		{TenantID: f.tenantA, UserID: f.userA, SessionID: f.sessionB},
	}
	for i, scope := range wrongScopes {
		if _, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
			Scope: scope, IdempotencyKey: fmt.Sprintf("wrong-%d", i),
			Events: []agentledger.Input{valid},
		}); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("wrong scope %d error=%v, want not found", i, err)
		}
		if events, err := f.store.ListAgentEvents(ctx, scope, 0, 10); err != nil ||
			len(events) != 0 {
			t.Fatalf("wrong scope list %d events=%+v err=%v", i, events, err)
		}
		if _, err := f.store.ReplayAgentEvents(ctx, scope); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("wrong scope replay %d error=%v", i, err)
		}
	}

	tooMany := make([]agentledger.Input, maxAgentEventBatchSize+1)
	for i := range tooMany {
		tooMany[i] = valid
	}
	bounds := []agentledger.AppendBatch{
		{Scope: f.scopeA(), IdempotencyKey: "", Events: []agentledger.Input{valid}},
		{Scope: f.scopeA(), IdempotencyKey: strings.Repeat("x", maxAgentEventKeyBytes+1), Events: []agentledger.Input{valid}},
		{Scope: f.scopeA(), IdempotencyKey: "empty", Events: nil},
		{Scope: f.scopeA(), IdempotencyKey: "too-many", Events: tooMany},
	}
	for i, batch := range bounds {
		if _, err := f.store.AppendAgentEvents(ctx, batch); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("bound %d error=%v", i, err)
		}
	}
}

func TestAgentEventsBatchRollsBackAfterPartialDatabaseFailure(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	testStore := *f.store
	testStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := f.store.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &failSecondAgentEventInsertTx{Tx: tx}, nil
	}
	secret := "must-not-appear-in-errors"
	_, err := testStore.AppendAgentEvents(ctx, agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "partial-db-failure",
		Events: []agentledger.Input{
			{Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"first"}`)},
			{
				Kind: agentledger.KindAssistantMessage,
				Body: []byte(`{"text":"` + secret + `"}`),
			},
		},
	})
	if !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("partial insert error=%v, want database error", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("database error leaked event payload: %v", err)
	}
	var count int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed batch committed %d partial rows", count)
	}
}

type failSecondAgentEventInsertTx struct {
	pgx.Tx
	inserts int
}

func (tx *failSecondAgentEventInsertTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	if strings.Contains(sql, "INSERT INTO agent_events") {
		tx.inserts++
		if tx.inserts == 2 {
			return agentEventErrorRow{
				err: errors.New("synthetic database failure"),
			}
		}
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

type agentEventErrorRow struct {
	err error
}

func (row agentEventErrorRow) Scan(...any) error {
	return row.err
}

func TestAgentEventsListAndReplayFailClosedOnCorruption(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	events, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "corruption",
		Events: []agentledger.Input{
			{Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"first"}`)},
			{Kind: agentledger.KindAssistantMessage, Body: []byte(`{"text":"second"}`)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalDigest := events[1].PayloadDigest
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_events SET payload_digest=repeat('f',64) WHERE id=$1`,
		events[1].ID,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.store.pool.Exec(
			context.WithoutCancel(ctx),
			`UPDATE agent_events SET payload_digest=$2 WHERE id=$1`,
			events[1].ID, originalDigest,
		)
	})
	if listed, err := f.store.ListAgentEvents(ctx, f.scopeA(), 0, 1); !errors.Is(err, types.ErrInternal) || listed != nil {
		t.Fatalf("corrupt second row crossed first-page boundary: events=%+v err=%v", listed, err)
	}
	if listed, err := f.store.ListAgentEvents(ctx, f.scopeA(), 0, 10); !errors.Is(err, types.ErrInternal) || listed != nil {
		t.Fatalf("corrupt List events=%+v err=%v", listed, err)
	}
	if replayed, err := f.store.ReplayAgentEvents(ctx, f.scopeA()); !errors.Is(err, types.ErrInternal) || replayed != nil {
		t.Fatalf("corrupt Replay events=%+v err=%v", replayed, err)
	}
	if _, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "corruption",
		Events: []agentledger.Input{
			{Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"first"}`)},
			{Kind: agentledger.KindAssistantMessage, Body: []byte(`{"text":"second"}`)},
		},
	}); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("corrupt exact replay error=%v", err)
	}
}

func TestAgentEventsRLSAndAppendOnlyRole(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	events, err := f.store.AppendAgentEvents(ctx, agentledger.AppendBatch{
		Scope: f.scopeA(), IdempotencyKey: "rls-source",
		Events: []agentledger.Input{{
			Kind: agentledger.KindUserMessage, Body: []byte(`{"text":"secret tenant text"}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeTx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtimeTx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentEventRuntimeContext(
		ctx, runtimeTx, f.tenantA,
	); err != nil {
		t.Fatal(err)
	}
	var currentRole, tenantContext, eventsOwner, sessionsOwner string
	var eventRLSActive, sessionRLSActive bool
	if err := runtimeTx.QueryRow(ctx, `
		SELECT current_role,
		       current_setting('app.tenant_id', true),
		       row_security_active('agent_events'),
		       row_security_active('agent_sessions'),
		       (SELECT relowner::regrole::text FROM pg_class
		         WHERE oid='agent_events'::regclass),
		       (SELECT relowner::regrole::text FROM pg_class
		         WHERE oid='agent_sessions'::regclass)`,
	).Scan(
		&currentRole, &tenantContext,
		&eventRLSActive, &sessionRLSActive,
		&eventsOwner, &sessionsOwner,
	); err != nil {
		t.Fatal(err)
	}
	if currentRole != "vane_app" ||
		tenantContext != fmt.Sprint(f.tenantA) ||
		!eventRLSActive || !sessionRLSActive {
		t.Fatalf(
			"runtime boundary role=%q tenant=%q event_rls=%t session_rls=%t",
			currentRole, tenantContext, eventRLSActive, sessionRLSActive,
		)
	}
	if eventsOwner == "vane_app" || sessionsOwner == "vane_app" {
		t.Fatalf("RLS fixture accidentally owns table: events=%q sessions=%q",
			eventsOwner, sessionsOwner)
	}
	if err := runtimeTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	userSameTenant, err := f.store.UpsertUserByOpenID(
		ctx, "agent_event_same_tenant_"+uuid.NewString(), "same tenant",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'member')`,
		f.tenantA, userSameTenant.ID,
	); err != nil {
		t.Fatal(err)
	}
	var sessionSameTenant int64
	if err := f.store.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id)
		 VALUES ($1,$2) RETURNING id`,
		f.tenantA, userSameTenant.ID,
	).Scan(&sessionSameTenant); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.store,
			`DELETE FROM agent_sessions WHERE id=$1`, sessionSameTenant)
		cleanupExec(cleanupCtx, t, f.store,
			`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			f.tenantA, userSameTenant.ID)
		cleanupExec(cleanupCtx, t, f.store,
			`DELETE FROM users WHERE id=$1`, userSameTenant.ID)
	})

	var visible int
	asTenant(t, f.store, f.tenantA, func(tx pgx.Tx) {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM agent_events`).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 1 {
			t.Fatalf("tenant A visible=%d, want 1", visible)
		}
	})
	asTenant(t, f.store, f.tenantB, func(tx pgx.Tx) {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM agent_events`).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 0 {
			t.Fatalf("tenant B leaked %d rows", visible)
		}
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM agent_sessions WHERE tenant_id=$1`,
			f.tenantA,
		).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 0 {
			t.Fatalf("tenant B leaked %d tenant A agent sessions", visible)
		}
	})
	asTenant(t, f.store, 0, func(tx pgx.Tx) {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM agent_events`).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 0 {
			t.Fatalf("no tenant context leaked %d rows", visible)
		}
		// Migration 022's older agent_sessions policy may reject an empty
		// custom GUC while migration 035's ledger policy returns zero rows.
		// Both are fail-closed; neither may expose a row.
		sessionErr := tx.QueryRow(
			ctx, `SELECT count(*) FROM agent_sessions`,
		).Scan(&visible)
		if sessionErr == nil && visible != 0 {
			t.Fatalf("no tenant context leaked %d agent sessions", visible)
		}
	})

	attempts := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "cross tenant insert",
			sql: `INSERT INTO agent_events (
					tenant_id,user_id,session_id,sequence,batch_idempotency_key,
					batch_index,batch_size,kind,schema_version,payload,
					payload_digest,batch_digest
				) VALUES ($1,$2,$3,2,'spoof-tenant',0,1,'user_message',
					'vane.agent-event/v1',$4,$5,$6)`,
			args: []any{
				f.tenantB, f.userB, f.sessionB, events[0].Payload,
				events[0].PayloadDigest, events[0].BatchDigest,
			},
		},
		{
			name: "user session spoof",
			sql: `INSERT INTO agent_events (
					tenant_id,user_id,session_id,sequence,batch_idempotency_key,
					batch_index,batch_size,kind,schema_version,payload,
					payload_digest,batch_digest
				) VALUES ($1,$2,$3,2,'spoof-user',0,1,'user_message',
					'vane.agent-event/v1',$4,$5,$6)`,
			args: []any{
				f.tenantA, userSameTenant.ID, f.sessionA, events[0].Payload,
				events[0].PayloadDigest, events[0].BatchDigest,
			},
		},
		{name: "update", sql: `UPDATE agent_events SET kind='tool_call'`},
		{name: "delete", sql: `DELETE FROM agent_events`},
		{name: "truncate", sql: `TRUNCATE agent_events`},
	}
	for _, attempt := range attempts {
		asTenant(t, f.store, f.tenantA, func(tx pgx.Tx) {
			if _, err := tx.Exec(ctx, attempt.sql, attempt.args...); err == nil {
				t.Errorf("%s unexpectedly succeeded", attempt.name)
			}
		})
	}
}

func TestAgentEventLedgerProductionCallBoundary(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Agent event ledger guard")
	}
	storeDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(storeDir))
	provider := filepath.Join(storeDir, "agent_events.go")
	agentLoop := filepath.Join(repoRoot, "agent", "loop.go")
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			base := entry.Name()
			if path != repoRoot &&
				(base == "vendor" || base == "third_party" || base == "testdata" ||
					strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		var allowed map[token.Pos]struct{}
		if filepath.Clean(path) == provider {
			allowed, err = agentEventLedgerProviderDeclarations(file)
			if err != nil {
				return err
			}
		} else if filepath.Clean(path) == agentLoop {
			allowed, err = agentEventLedgerAgentLoopReferences(file)
			if err != nil {
				return err
			}
		}
		violations = append(
			violations,
			agentEventLedgerForbiddenReferences(fset, file, allowed)...,
		)
		for _, imported := range file.Imports {
			if strings.Trim(imported.Path.Value, `"`) !=
				"github.com/YouToco/vane/agentledger" {
				continue
			}
			clean := filepath.Clean(path)
			if clean != provider && clean != agentLoop {
				violations = append(violations,
					fmt.Sprintf("%s: forbidden Agent event ledger import",
						fset.Position(imported.Pos())))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("7.7-B permits only normal Agent turn dual-write:\n%s",
			strings.Join(violations, "\n"))
	}
}

func TestAgentEventLedgerGuardCatchesIndirectReferences(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "escape.go", `package escape
type ledger interface { AppendAgentEvents() }
func renamedWrapper(s interface{ ReplayAgentEvents() }) {
	call := s.ReplayAgentEvents
	call()
}
func sameNameWrapper() { AppendAgentEvents() }
func escapedCommit(s interface{ CommitAgentSessionTurn() }) {
	s.CommitAgentSessionTurn()
}
var byName = "ListAgentEvents"
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations := agentEventLedgerForbiddenReferences(fset, file, nil)
	if len(violations) < 6 {
		t.Fatalf("method value/interface/wrapper escaped guard: %v", violations)
	}
}

func TestAgentEventLedgerGuardCatchesProviderWrapper(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "agent_events.go", `package store
type Store struct{}
func (s *Store) AppendAgentEvents() {}
func (s *Store) ListAgentEvents() {}
func (s *Store) ReplayAgentEvents() {}
func (s *Store) CommitAgentSessionTurn() {}
func (s *Store) hiddenAppendWrapper() {
	s.AppendAgentEvents()
}
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := agentEventLedgerProviderDeclarations(file)
	if err != nil {
		t.Fatal(err)
	}
	violations := agentEventLedgerForbiddenReferences(fset, file, allowed)
	if !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(violation, "AppendAgentEvents")
	}) {
		t.Fatalf("provider-local wrapper escaped zero-call guard: %v", violations)
	}
}

func TestAgentEventLedgerAgentLoopGuardRejectsMethodValueAndWrapper(
	t *testing.T,
) {
	t.Parallel()
	mutations := map[string]string{
		"method value alias": `package agent
type Store interface { CommitAgentSessionTurn() error }
type Loop struct { store Store }
func (l *Loop) saveSession() {
	commit := l.store.CommitAgentSessionTurn
	_ = commit()
}`,
		"wrapper": `package agent
type Store interface { CommitAgentSessionTurn() error }
type Loop struct { store Store }
func (l *Loop) commitTurn() error {
	return l.store.CommitAgentSessionTurn()
}
func (l *Loop) saveSession() {
	_ = l.commitTurn()
}`,
	}
	for name, source := range mutations {
		t.Run(name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "loop.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := agentEventLedgerAgentLoopReferences(file); err == nil {
				t.Fatal("mutated Agent loop unexpectedly passed direct-call guard")
			}
		})
	}
}

func agentEventLedgerForbiddenReferences(
	fset *token.FileSet,
	file *ast.File,
	allowed map[token.Pos]struct{},
) []string {
	sensitive := map[string]struct{}{
		"AppendAgentEvents":      {},
		"ListAgentEvents":        {},
		"ReplayAgentEvents":      {},
		"CommitAgentSessionTurn": {},
	}
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.Ident:
			if _, forbidden := sensitive[typed.Name]; forbidden {
				if _, ok := allowed[typed.Pos()]; ok {
					return true
				}
				violations = append(violations,
					fmt.Sprintf("%s: forbidden Agent event ledger reference %s",
						fset.Position(typed.Pos()), typed.Name))
			}
		case *ast.BasicLit:
			value := strings.Trim(typed.Value, "`\"")
			if _, forbidden := sensitive[value]; forbidden {
				violations = append(violations,
					fmt.Sprintf("%s: forbidden dynamic Agent event ledger reference %s",
						fset.Position(typed.Pos()), value))
			}
		}
		return true
	})
	return violations
}

func agentEventLedgerProviderDeclarations(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	want := map[string]int{
		"AppendAgentEvents":      0,
		"ListAgentEvents":        0,
		"ReplayAgentEvents":      0,
		"CommitAgentSessionTurn": 0,
	}
	allowed := make(map[token.Pos]struct{}, len(want))
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil {
			continue
		}
		if _, sensitive := want[function.Name.Name]; !sensitive {
			continue
		}
		if len(function.Recv.List) != 1 ||
			!agentEventLedgerStoreReceiver(function.Recv.List[0].Type) {
			return nil, fmt.Errorf(
				"Agent event ledger provider %s has a non-Store receiver",
				function.Name.Name,
			)
		}
		want[function.Name.Name]++
		allowed[function.Name.Pos()] = struct{}{}
	}
	for name, count := range want {
		if count != 1 {
			return nil, fmt.Errorf(
				"Agent event ledger provider must declare %s exactly once, got %d",
				name, count,
			)
		}
	}
	return allowed, nil
}

func agentEventLedgerAgentLoopReferences(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	const name = "CommitAgentSessionTurn"
	allowed := make(map[token.Pos]struct{}, 2)

	interfaceFields := 0
	var saveSession *ast.FuncDecl
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.GenDecl:
			if typed.Tok != token.TYPE {
				continue
			}
			for _, spec := range typed.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || typeSpec.Name.Name != "Store" {
					continue
				}
				interfaceType, ok := typeSpec.Type.(*ast.InterfaceType)
				if !ok {
					return nil, errors.New(
						"Agent loop Store must remain an interface",
					)
				}
				for _, field := range interfaceType.Methods.List {
					if len(field.Names) != 1 ||
						field.Names[0].Name != name {
						continue
					}
					if _, ok := field.Type.(*ast.FuncType); !ok {
						return nil, errors.New(
							"Agent loop ledger boundary must be a method field",
						)
					}
					interfaceFields++
					allowed[field.Names[0].Pos()] = struct{}{}
				}
			}
		case *ast.FuncDecl:
			if typed.Name.Name == "saveSession" {
				if saveSession != nil {
					return nil, errors.New(
						"Agent loop must declare saveSession exactly once",
					)
				}
				saveSession = typed
			}
		}
	}
	if interfaceFields != 1 {
		return nil, fmt.Errorf(
			"Agent loop Store must declare %s exactly once, got %d",
			name, interfaceFields,
		)
	}
	if saveSession == nil || saveSession.Body == nil {
		return nil, errors.New("Agent loop saveSession is unavailable")
	}
	if saveSession.Recv == nil || len(saveSession.Recv.List) != 1 ||
		len(saveSession.Recv.List[0].Names) != 1 ||
		saveSession.Recv.List[0].Names[0].Name != "l" ||
		!agentEventLedgerLoopReceiver(saveSession.Recv.List[0].Type) {
		return nil, errors.New(
			"Agent loop saveSession must remain a method on receiver l *Loop",
		)
	}

	directCalls := 0
	ast.Inspect(saveSession.Body, func(node ast.Node) bool {
		if _, nestedFunction := node.(*ast.FuncLit); nestedFunction {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != name {
			return true
		}
		store, ok := method.X.(*ast.SelectorExpr)
		if !ok || store.Sel.Name != "store" {
			return true
		}
		receiver, ok := store.X.(*ast.Ident)
		if !ok || receiver.Name != "l" {
			return true
		}
		directCalls++
		allowed[method.Sel.Pos()] = struct{}{}
		return true
	})
	if directCalls != 1 {
		return nil, fmt.Errorf(
			"Agent loop saveSession must directly call l.store.%s exactly once, got %d",
			name, directCalls,
		)
	}
	return allowed, nil
}

func agentEventLedgerLoopReceiver(expression ast.Expr) bool {
	star, ok := expression.(*ast.StarExpr)
	if !ok {
		return false
	}
	identifier, ok := star.X.(*ast.Ident)
	return ok && identifier.Name == "Loop"
}

func agentEventLedgerStoreReceiver(expression ast.Expr) bool {
	if star, ok := expression.(*ast.StarExpr); ok {
		expression = star.X
	}
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "Store"
}
