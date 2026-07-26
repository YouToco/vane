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
	"time"

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
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_sessions
		    SET messages=messages || $2::jsonb
		  WHERE id=$1`,
		f.sessionA, callback,
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

func TestCommitAgentSessionAppendExactReplayTruncationAndScope(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	messages := make([]map[string]string, 60)
	for i := range messages {
		role := "assistant"
		if i%3 == 0 {
			role = "user"
		}
		messages[i] = map[string]string{
			"role": role, "content": fmt.Sprintf("message-%d", i),
		}
	}
	current, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(ctx, `
		UPDATE agent_sessions
		   SET messages=$2,turn_count=9,
		       activated_tools='["endpoint-a"]'::jsonb
		 WHERE id=$1 /* controlled truncation fixture */`,
		f.sessionA, current,
	); err != nil {
		t.Fatal(err)
	}
	appended := json.RawMessage(
		`[{"role":"user","content":"[卡片回调] exact"}]`,
	)
	first, err := f.store.CommitAgentSessionAppend(
		ctx, f.userA, f.sessionA, "card-callback:execute:action-1", appended,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Match || first.PriorState != "uninitialized" {
		t.Fatalf("first side-writer audit=%+v", first)
	}
	var stored []map[string]any
	var turnCount int
	var activated []string
	if err := f.store.pool.QueryRow(ctx,
		`SELECT messages,turn_count,activated_tools
		   FROM agent_sessions WHERE id=$1`,
		f.sessionA,
	).Scan(&stored, &turnCount, &activated); err != nil {
		t.Fatal(err)
	}
	if len(stored) > 41 ||
		stored[0]["content"] != "message-0" ||
		stored[len(stored)-1]["content"] != "[卡片回调] exact" {
		t.Fatalf("side-writer truncation=%+v", stored)
	}
	if turnCount != 9 || !slices.Equal(activated, []string{"endpoint-a"}) {
		t.Fatalf("side writer changed retained state: turn=%d tools=%v",
			turnCount, activated)
	}
	var firstEventCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&firstEventCount); err != nil {
		t.Fatal(err)
	}
	replay, err := f.store.CommitAgentSessionAppend(
		ctx, f.userA, f.sessionA, "card-callback:execute:action-1", appended,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Match {
		t.Fatalf("replay audit=%+v", replay)
	}
	var replayEventCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&replayEventCount); err != nil {
		t.Fatal(err)
	}
	if replayEventCount != firstEventCount {
		t.Fatalf("replay appended events: before=%d after=%d",
			firstEventCount, replayEventCount)
	}
	later := json.RawMessage(
		`[{"role":"user","content":"later generation"}]`,
	)
	if _, err := f.store.CommitAgentSessionAppend(
		ctx, f.userA, f.sessionA, "feedback-click:2", later,
	); err != nil {
		t.Fatal(err)
	}
	var afterLaterEventCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&afterLaterEventCount); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CommitAgentSessionAppend(
		ctx, f.userA, f.sessionA, "card-callback:execute:action-1", appended,
	); err != nil {
		t.Fatalf("older exact replay after later generation: %v", err)
	}
	var afterOlderReplay []map[string]any
	var afterOlderReplayEventCount int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT messages FROM agent_sessions WHERE id=$1`,
		f.sessionA,
	).Scan(&afterOlderReplay); err != nil {
		t.Fatal(err)
	}
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		f.tenantA, f.userA, f.sessionA,
	).Scan(&afterOlderReplayEventCount); err != nil {
		t.Fatal(err)
	}
	if afterOlderReplay[len(afterOlderReplay)-1]["content"] !=
		"later generation" {
		t.Fatalf("older replay overwrote later projection: %+v",
			afterOlderReplay)
	}
	if afterOlderReplayEventCount != afterLaterEventCount {
		t.Fatalf("older replay appended events: before=%d after=%d",
			afterLaterEventCount, afterOlderReplayEventCount)
	}
	secret := "changed-body-must-not-leak"
	_, err = f.store.CommitAgentSessionAppend(
		ctx,
		f.userA,
		f.sessionA,
		"card-callback:execute:action-1",
		json.RawMessage(
			`[{"role":"user","content":"`+secret+`"}]`,
		),
	)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed replay error=%v, want conflict", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("changed replay leaked message content: %v", err)
	}
	if _, err := f.store.CommitAgentSessionAppend(
		ctx, f.userB, f.sessionA, "feedback-click:1", appended,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant/user append error=%v, want not found", err)
	}
}

func TestCommitAgentSessionAppendAtomicResponseLossAndPreCommitFailure(
	t *testing.T,
) {
	t.Run("commit response loss retains both projections exactly once",
		func(t *testing.T) {
			f := newAgentEventFixture(t)
			ctx := t.Context()
			messages := json.RawMessage(
				`[{"role":"user","content":"response lost"}]`,
			)
			lost := storeWithCommitResponseLost(f.store)
			if _, err := lost.CommitAgentSessionAppend(
				ctx,
				f.userA,
				f.sessionA,
				"feedback-click:response-loss",
				messages,
			); !errors.Is(err, types.ErrDatabase) {
				t.Fatalf("commit response loss error=%v, want database", err)
			}
			assertAgentSessionAppendState(
				t, f, "[{\"content\":\"response lost\",\"role\":\"user\"}]", 3,
			)
			if _, err := f.store.CommitAgentSessionAppend(
				ctx,
				f.userA,
				f.sessionA,
				"feedback-click:response-loss",
				messages,
			); err != nil {
				t.Fatalf("response-loss exact replay: %v", err)
			}
			assertAgentSessionAppendState(
				t, f, "[{\"content\":\"response lost\",\"role\":\"user\"}]", 3,
			)
		},
	)
	t.Run("pre-commit event failure rolls back both projections",
		func(t *testing.T) {
			f := newAgentEventFixture(t)
			ctx := t.Context()
			faultStore := *f.store
			faultStore.beginTx = func(
				ctx context.Context,
				options pgx.TxOptions,
			) (pgx.Tx, error) {
				tx, err := f.store.pool.BeginTx(ctx, options)
				if err != nil {
					return nil, err
				}
				return &failSecondAgentEventInsertTx{Tx: tx}, nil
			}
			if _, err := faultStore.CommitAgentSessionAppend(
				ctx,
				f.userA,
				f.sessionA,
				"feedback-click:pre-commit-failure",
				json.RawMessage(
					`[{"role":"user","content":"must roll back"}]`,
				),
			); !errors.Is(err, types.ErrDatabase) {
				t.Fatalf("pre-commit failure error=%v, want database", err)
			}
			assertAgentSessionAppendState(t, f, "[]", 0)
		},
	)
}

func assertAgentSessionAppendState(
	t *testing.T,
	f agentEventFixture,
	wantMessages string,
	wantEvents int,
) {
	t.Helper()
	var messages json.RawMessage
	var eventCount int
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT messages,
		       (SELECT count(*) FROM agent_events
		         WHERE tenant_id=$2 AND user_id=$3 AND session_id=$1)
		  FROM agent_sessions
		 WHERE id=$1`,
		f.sessionA, f.tenantA, f.userA,
	).Scan(&messages, &eventCount); err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal(messages, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(wantMessages), &want); err != nil {
		t.Fatal(err)
	}
	gotWire, _ := json.Marshal(got)
	wantWire, _ := json.Marshal(want)
	if string(gotWire) != string(wantWire) || eventCount != wantEvents {
		t.Fatalf("append state messages=%s events=%d want=%s/%d",
			gotWire, eventCount, wantWire, wantEvents)
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
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_sessions
		    SET messages=messages || $2::jsonb
		  WHERE id=$1`,
		f.sessionA, callback,
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

func TestCommitAgentSessionTurnPostWriteAuditMismatchRollsBackAndKeepsBaseReusable(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	desired := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"correct"}]`,
		),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	const failedTurnID = "turn-post-write-audit-mismatch"
	failedBatch := projectionSnapshotBatch(
		t, f.scopeA(), failedTurnID, base, desired, "",
	)
	poisonProjection := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"storage-mismatch"}]`,
		),
		TurnCount:      1,
		ActivatedTools: json.RawMessage(`[]`),
	}
	poisonBatch := projectionSnapshotBatch(
		t, f.scopeA(), failedTurnID, base, poisonProjection, "",
	)
	poisonEvent, err := agentledger.Canonicalize(poisonBatch.Events[1])
	if err != nil {
		t.Fatal(err)
	}

	mismatchStore := *f.store
	mismatchStore.beginTx = func(
		ctx context.Context,
		options pgx.TxOptions,
	) (pgx.Tx, error) {
		tx, err := f.store.pool.BeginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &corruptAgentEventInsertTx{
			Tx: tx, targetInsert: 2,
			payload: poisonEvent.Payload(), payloadDigest: poisonEvent.Digest(),
		}, nil
	}
	if _, err := mismatchStore.CommitAgentSessionTurn(
		ctx, desired, failedBatch,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("post-write projection mismatch error=%v, want internal", err)
	}
	assertAgentTurnCommitLeftNoMutation(t, f)
	var failedKeyRows int
	if err := f.store.pool.QueryRow(ctx, `
		SELECT count(*) FROM agent_events
		 WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		   AND batch_idempotency_key=$4`,
		f.tenantA, f.userA, f.sessionA, failedBatch.IdempotencyKey,
	).Scan(&failedKeyRows); err != nil {
		t.Fatal(err)
	}
	if failedKeyRows != 0 {
		t.Fatalf("rolled-back mismatch retained %d rows for key %q",
			failedKeyRows, failedBatch.IdempotencyKey)
	}

	const successTurnID = "turn-after-post-write-audit-mismatch"
	successBatch := projectionSnapshotBatch(
		t, f.scopeA(), successTurnID, base, desired, "",
	)
	audit, err := f.store.CommitAgentSessionTurn(ctx, desired, successBatch)
	if err != nil {
		t.Fatalf("same base was contaminated by rolled-back mismatch: %v", err)
	}
	if !audit.Match {
		t.Fatalf("correct retry audit=%+v", audit)
	}
	events, err := f.store.ReplayAgentEvents(ctx, f.scopeA())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(successBatch.Events) {
		t.Fatalf("replayed events=%d want=%d", len(events), len(successBatch.Events))
	}
	for i := range events {
		if events[i].Sequence != int64(i+1) ||
			events[i].IdempotencyKey != successBatch.IdempotencyKey {
			t.Fatalf("event[%d] sequence/key=%d/%q want=%d/%q",
				i, events[i].Sequence, events[i].IdempotencyKey,
				i+1, successBatch.IdempotencyKey)
		}
	}
	assertStoredAgentSessionProjection(t, f, desired)
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

func TestGetActiveAgentSessionLedgerAuthorityFailsClosedAndRollbackRestoresLegacy(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	base := agentledger.SessionProjection{
		Messages: json.RawMessage(`[]`), ActivatedTools: json.RawMessage(`[]`),
	}
	first := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"first"},{"role":"assistant","content":"reply"}]`,
		),
		TurnCount: 1, ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, first,
		projectionSnapshotBatch(
			t, f.scopeA(), "authority-read-1", base, first, "",
		),
	); err != nil {
		t.Fatalf("initialize dual-write projection: %v", err)
	}
	if status, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, f.tenantA, f.userA, f.sessionA,
		AgentSessionProjectionAuthorityActivate,
	); err != nil || status.Route != AgentSessionProjectionRouteLedger {
		t.Fatalf("activate ledger authority: status=%+v err=%v", status, err)
	}

	got, err := f.store.GetActiveAgentSession(
		ctx, f.userA, time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("ledger-authoritative read: %v", err)
	}
	assertAgentSessionProjectionDigest(t, got, first)

	if _, err := f.store.pool.Exec(ctx,
		`UPDATE agent_sessions
		    SET messages='[{"role":"user","content":"drift"}]'::jsonb
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		f.sessionA, f.tenantA, f.userA,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetActiveAgentSession(
		ctx, f.userA, time.Now().Add(-time.Hour),
	); err == nil {
		t.Fatal("ledger-authoritative read accepted a mismatched legacy replica")
	}

	second := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"first"},{"role":"assistant","content":"reply"},` +
				`{"role":"user","content":"second"},{"role":"assistant","content":"reply two"}]`,
		),
		TurnCount: 2, ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		ctx, second,
		projectionSnapshotBatch(
			t, f.scopeA(), "authority-read-2", first, second, "",
		),
	); err == nil {
		t.Fatal("ledger-authoritative writer accepted a mismatched legacy replica")
	}

	updateAgentSessionProjectionFixture(t, f, first)
	if status, err := f.store.ControlAgentSessionProjectionAuthority(
		ctx, f.tenantA, f.userA, f.sessionA,
		AgentSessionProjectionAuthorityRollback,
	); err != nil || status.Route != AgentSessionProjectionRouteLegacy {
		t.Fatalf("rollback legacy authority: status=%+v err=%v", status, err)
	}
	legacyAfterRollback := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"legacy after rollback"}]`,
		),
		TurnCount: 3, ActivatedTools: json.RawMessage(`[]`),
	}
	updateAgentSessionProjectionFixture(t, f, legacyAfterRollback)
	got, err = f.store.GetActiveAgentSession(
		ctx, f.userA, time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("legacy read after rollback: %v", err)
	}
	assertAgentSessionProjectionDigest(t, got, legacyAfterRollback)

	afterRollbackWrite := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"legacy after rollback"},` +
				`{"role":"assistant","content":"resynchronized"}]`,
		),
		TurnCount: 4, ActivatedTools: json.RawMessage(`[]`),
	}
	audit, err := f.store.CommitAgentSessionTurn(
		ctx, afterRollbackWrite,
		projectionSnapshotBatch(
			t, f.scopeA(), "authority-read-3",
			legacyAfterRollback, afterRollbackWrite, "",
		),
	)
	if err != nil {
		t.Fatalf("legacy writer after rollback: %v", err)
	}
	if !audit.Match ||
		audit.PriorState != "unsupported_writer_or_projection_drift" {
		t.Fatalf("rollback resync audit=%+v", audit)
	}
}

func TestGetActiveAgentSessionLedgerAuthorityRejectsUnavailableLedger(
	t *testing.T,
) {
	t.Run("zero events", func(t *testing.T) {
		f := newAgentEventFixture(t)
		insertInvalidAgentSessionProjectionAuthorityFixture(t, f)
		if _, err := f.store.GetActiveAgentSession(
			t.Context(), f.userA, time.Now().Add(-time.Hour),
		); err == nil {
			t.Fatal("ledger-authoritative read accepted an empty ledger")
		}
	})

	t.Run("incomplete batch", func(t *testing.T) {
		f := newAgentEventFixture(t)
		initializeAgentSessionProjectionFixture(t, f, "missing-batch")
		if _, err := f.store.ControlAgentSessionProjectionAuthority(
			t.Context(), f.tenantA, f.userA, f.sessionA,
			AgentSessionProjectionAuthorityActivate,
		); err != nil {
			t.Fatalf("activate ledger authority: %v", err)
		}
		if _, err := f.store.pool.Exec(t.Context(),
			`DELETE FROM agent_events
			  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
			    AND batch_index=1`,
			f.tenantA, f.userA, f.sessionA,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetActiveAgentSession(
			t.Context(), f.userA, time.Now().Add(-time.Hour),
		); err == nil {
			t.Fatal("ledger-authoritative read accepted an incomplete batch")
		}
	})

	t.Run("corrupt payload", func(t *testing.T) {
		f := newAgentEventFixture(t)
		initializeAgentSessionProjectionFixture(t, f, "corrupt-payload")
		if _, err := f.store.ControlAgentSessionProjectionAuthority(
			t.Context(), f.tenantA, f.userA, f.sessionA,
			AgentSessionProjectionAuthorityActivate,
		); err != nil {
			t.Fatalf("activate ledger authority: %v", err)
		}
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE agent_events
			    SET payload=decode('00', 'hex')
			  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
			    AND batch_index=1`,
			f.tenantA, f.userA, f.sessionA,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetActiveAgentSession(
			t.Context(), f.userA, time.Now().Add(-time.Hour),
		); err == nil {
			t.Fatal("ledger-authoritative read accepted a corrupt event payload")
		}
	})
}

func TestActiveAgentSessionReadSnapshotDoesNotTearAcrossCutover(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	projection := initializeAgentSessionProjectionFixture(
		t, f, "read-cutover-snapshot",
	)
	tx, err := f.store.beginTx(
		t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(t.Context()))
	}()

	// Establish the read transaction snapshot before the control transaction
	// activates ledger authority.
	var session types.AgentSession
	if err := scanAgentSession(tx.QueryRow(t.Context(),
		`SELECT `+agentSessionColumns+`
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		f.sessionA, f.tenantA, f.userA,
	), &session); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ControlAgentSessionProjectionAuthority(
		t.Context(), f.tenantA, f.userA, f.sessionA,
		AgentSessionProjectionAuthorityActivate,
	); err != nil {
		t.Fatalf("activate ledger authority: %v", err)
	}

	// The in-flight read must finish wholly on its earlier legacy route. A
	// fresh read below observes the committed ledger route.
	if err := loadAuthoritativeActiveAgentSessionProjection(
		t.Context(), tx, &session,
	); err != nil {
		t.Fatalf("in-flight legacy snapshot: %v", err)
	}
	assertAgentSessionProjectionDigest(t, &session, projection)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	fresh, err := f.store.GetActiveAgentSession(
		t.Context(), f.userA, time.Now().Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("fresh ledger-authoritative read: %v", err)
	}
	assertAgentSessionProjectionDigest(t, fresh, projection)
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

func initializeAgentSessionProjectionFixture(
	t *testing.T,
	f agentEventFixture,
	turnID string,
) agentledger.SessionProjection {
	t.Helper()
	base := agentledger.SessionProjection{
		Messages: json.RawMessage(`[]`), ActivatedTools: json.RawMessage(`[]`),
	}
	projection := agentledger.SessionProjection{
		Messages: json.RawMessage(
			`[{"role":"user","content":"fixture"},{"role":"assistant","content":"reply"}]`,
		),
		TurnCount: 1, ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := f.store.CommitAgentSessionTurn(
		t.Context(), projection,
		projectionSnapshotBatch(
			t, f.scopeA(), turnID, base, projection, "",
		),
	); err != nil {
		t.Fatalf("initialize projection fixture: %v", err)
	}
	return projection
}

func initializeEmptyAgentSessionLedgerAuthority(
	t *testing.T,
	st *Store,
	scope agentledger.Scope,
	turnID string,
) {
	t.Helper()
	empty := agentledger.SessionProjection{
		Messages:       json.RawMessage(`[]`),
		ActivatedTools: json.RawMessage(`[]`),
	}
	if _, err := st.CommitAgentSessionTurn(
		t.Context(), empty,
		projectionSnapshotBatch(t, scope, turnID, empty, empty, ""),
	); err != nil {
		t.Fatalf("initialize empty Agent ledger: %v", err)
	}
	status, err := st.ControlAgentSessionProjectionAuthority(
		t.Context(), scope.TenantID, scope.UserID, scope.SessionID,
		AgentSessionProjectionAuthorityActivate,
	)
	if err != nil {
		t.Fatalf("activate Agent ledger authority: %v", err)
	}
	if status.Route != AgentSessionProjectionRouteLedger {
		t.Fatalf("Agent ledger authority status=%+v", status)
	}
}

func insertInvalidAgentSessionProjectionAuthorityFixture(
	t *testing.T,
	f agentEventFixture,
) {
	t.Helper()
	var projection agentledger.SessionProjection
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT messages, turn_count, activated_tools
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		f.sessionA, f.tenantA, f.userA,
	).Scan(
		&projection.Messages,
		&projection.TurnCount,
		&projection.ActivatedTools,
	); err != nil {
		t.Fatal(err)
	}
	digest, err := agentledger.ProjectionDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(), `
		INSERT INTO agent_session_projection_authority_events (
			tenant_id, user_id, session_id, generation, action,
			ledger_head_sequence, legacy_digest, ledger_digest
		) VALUES ($1,$2,$3,1,'activate',1,$4,$4)`,
		f.tenantA, f.userA, f.sessionA, digest,
	); err != nil {
		t.Fatalf("insert invalid zero-ledger authority fixture: %v", err)
	}
}

func updateAgentSessionProjectionFixture(
	t *testing.T,
	f agentEventFixture,
	projection agentledger.SessionProjection,
) {
	t.Helper()
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE agent_sessions
		    SET messages=$4, turn_count=$5, activated_tools=$6
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		f.sessionA, f.tenantA, f.userA,
		projection.Messages, projection.TurnCount, projection.ActivatedTools,
	); err != nil {
		t.Fatal(err)
	}
}

func assertAgentSessionProjectionDigest(
	t *testing.T,
	session *types.AgentSession,
	want agentledger.SessionProjection,
) {
	t.Helper()
	if session == nil {
		t.Fatal("nil AgentSession")
	}
	gotDigest, err := agentledger.ProjectionDigest(
		agentledger.SessionProjection{
			Messages:       session.Messages,
			TurnCount:      session.TurnCount,
			ActivatedTools: session.ActivatedTools,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := agentledger.ProjectionDigest(want)
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("AgentSession projection digest=%s want=%s",
			gotDigest, wantDigest)
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

type corruptAgentEventInsertTx struct {
	pgx.Tx
	inserts       int
	targetInsert  int
	payload       []byte
	payloadDigest string
}

func (tx *corruptAgentEventInsertTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	if !strings.Contains(sql, "INSERT INTO agent_events") {
		return tx.Tx.QueryRow(ctx, sql, args...)
	}
	tx.inserts++
	if tx.inserts != tx.targetInsert {
		return tx.Tx.QueryRow(ctx, sql, args...)
	}
	corruptedArgs := append([]any(nil), args...)
	corruptedArgs[9] = tx.payload
	corruptedArgs[10] = tx.payloadDigest
	return tx.Tx.QueryRow(ctx, sql, corruptedArgs...)
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
	authority := filepath.Join(
		storeDir, "agent_session_projection_authority.go",
	)
	runtimeAdmin := filepath.Join(repoRoot, "cmd", "runtimeadmin", "main.go")
	agentLoop := filepath.Join(repoRoot, "agent", "loop.go")
	creationReceipts := filepath.Join(
		storeDir, "task_creation_receipts.go",
	)
	definitionReceipts := filepath.Join(
		storeDir, "task_definition_edit_receipts.go",
	)
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
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var allowed map[token.Pos]struct{}
		if filepath.Clean(path) == provider {
			allowed, err = agentEventLedgerProviderDeclarations(file)
			if err != nil {
				return err
			}
		} else if filepath.Clean(path) == authority {
			allowed, err = agentEventLedgerAuthorityDeclarations(file)
			if err != nil {
				return err
			}
		} else if filepath.Clean(path) == runtimeAdmin {
			allowed, err = agentEventLedgerRuntimeAdminAuthorityReferences(file)
			if err != nil {
				return err
			}
		} else if filepath.Clean(path) == agentLoop {
			allowed, err = agentEventLedgerAgentLoopReferences(file)
			if err != nil {
				return err
			}
		} else if filepath.Clean(path) == creationReceipts {
			allowed, err = agentEventLedgerReceiptHelperReferences(
				file,
				"RecordTaskCreationReceiptSessionMessages",
				2,
			)
			if err != nil {
				return err
			}
		} else if filepath.Clean(path) == definitionReceipts {
			allowed, err = agentEventLedgerReceiptHelperReferences(
				file,
				"RecordTaskDefinitionEditReceiptSessionMessages",
				2,
			)
			if err != nil {
				return err
			}
		}
		violations = append(
			violations,
			agentEventLedgerForbiddenReferences(fset, file, allowed)...,
		)
		clean := filepath.Clean(path)
		if clean != authority &&
			strings.Contains(
				string(raw),
				"SET LOCAL ROLE vane_agent_session_projection_operator",
			) {
			violations = append(violations,
				fmt.Sprintf("%s: Agent projection operator role entry escaped controller",
					clean))
		}
		if clean != authority &&
			strings.Contains(
				string(raw),
				"INSERT INTO agent_session_projection_authority_events",
			) {
			violations = append(violations,
				fmt.Sprintf("%s: raw Agent projection authority append escaped controller",
					clean))
		}
		for _, imported := range file.Imports {
			if strings.Trim(imported.Path.Value, `"`) !=
				"github.com/YouToco/vane/agentledger" {
				continue
			}
			if clean != provider && clean != authority && clean != agentLoop {
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
		t.Fatalf("7.7-B2 permits only the exact Agent session write boundaries:\n%s",
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
func escapedSideWriter(s interface{ CommitAgentSessionAppend() }) {
	s.CommitAgentSessionAppend()
}
func resurrectLegacyAppend(s interface{ AppendAgentSessionMessages() }) {
	s.AppendAgentSessionMessages()
}
func resurrectProjectionOverwrite(s interface{ UpdateAgentSession() }) {
	s.UpdateAgentSession()
}
func escapedPrivateHelper() { commitAgentSessionAppendTx() }
func escapedRouteHelper() { agentSessionProjectionLedgerAuthoritative() }
func escapedAuthorityStatus(s interface{ GetAgentSessionProjectionAuthorityStatus() }) {
	s.GetAgentSessionProjectionAuthorityStatus()
}
func escapedAuthorityControl(s interface{ ControlAgentSessionProjectionAuthority() }) {
	control := s.ControlAgentSessionProjectionAuthority
	control()
}
var helperByName = "commitAgentSessionAppendTx"
var overwriteByName = "UpdateAgentSession"
var byName = "ListAgentEvents"
var routeByName = "agentSessionProjectionLedgerAuthoritative"
var statusByName = "GetAgentSessionProjectionAuthorityStatus"
var controlByName = "ControlAgentSessionProjectionAuthority"
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations := agentEventLedgerForbiddenReferences(fset, file, nil)
	if len(violations) < 15 {
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
func (s *Store) CommitAgentSessionTurn() {
	_ = "UPDATE agent_sessions SET messages=$1,turn_count=$2,activated_tools=$3"
}
func (s *Store) CommitAgentSessionAppend() {
	commitAgentSessionAppendTx()
}
func commitAgentSessionAppendTx() {
	_ = "UPDATE agent_sessions SET messages=$1"
}
func (s *Store) hiddenAppendWrapper() {
	s.AppendAgentEvents()
}
func hiddenSideWriterWrapper() {
	commitAgentSessionAppendTx()
}
func hiddenProjectionSQL() {
	_ = "UPDATE agent_sessions SET messages='[]'"
}
func loadAuthoritativeActiveAgentSessionProjection() {
	agentSessionProjectionLedgerAuthoritative()
}
func loadAuthoritativeAgentSessionProjectionForUpdate() {
	agentSessionProjectionLedgerAuthoritative()
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
	}) || !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(violation, "commitAgentSessionAppendTx")
	}) || !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(
			violation, "Agent session projection SQL write",
		)
	}) {
		t.Fatalf("provider-local wrapper escaped zero-call guard: %v", violations)
	}
}

func TestAgentEventLedgerGuardCatchesReceiptHelperEscape(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset,
		"task_creation_receipts.go",
		`package store
type Store struct{}
func (s *Store) RecordTaskCreationReceiptSessionMessages() {
	commitAgentSessionAppendTx()
	commitAgentSessionAppendTx()
	alias := commitAgentSessionAppendTx
	_ = alias
}
func receiptHelperWrapper() {
	commitAgentSessionAppendTx()
}
var helperName = "commitAgentSessionAppendTx"
`,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := agentEventLedgerReceiptHelperReferences(
		file,
		"RecordTaskCreationReceiptSessionMessages",
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	violations := agentEventLedgerForbiddenReferences(
		fset, file, allowed,
	)
	if len(violations) < 3 {
		t.Fatalf(
			"receipt helper method value/wrapper/dynamic string escaped guard: %v",
			violations,
		)
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

func TestAgentEventLedgerAuthorityGuardRejectsWrappers(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "agent_session_projection_authority.go", `package store
type Store struct{}
func agentSessionProjectionLedgerAuthoritative() {}
func (s *Store) GetAgentSessionProjectionAuthorityStatus() {}
func (s *Store) ControlAgentSessionProjectionAuthority() {}
func hiddenAuthorityWrapper() {
	agentSessionProjectionLedgerAuthoritative()
}
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := agentEventLedgerAuthorityDeclarations(file)
	if err != nil {
		t.Fatal(err)
	}
	violations := agentEventLedgerForbiddenReferences(fset, file, allowed)
	if !slices.ContainsFunc(violations, func(violation string) bool {
		return strings.Contains(
			violation, "agentSessionProjectionLedgerAuthoritative",
		)
	}) {
		t.Fatalf("authority wrapper escaped exact declaration guard: %v", violations)
	}
}

func TestAgentEventLedgerRuntimeAdminAuthorityGuardRejectsIndirection(
	t *testing.T,
) {
	t.Parallel()
	mutations := map[string]string{
		"method value alias": `package main
type agentSessionCutoverStore interface {
	GetAgentSessionProjectionAuthorityStatus()
	ControlAgentSessionProjectionAuthority()
}
func executeAgentSessionCutover(st agentSessionCutoverStore) {
	status := st.GetAgentSessionProjectionAuthorityStatus
	status()
	st.ControlAgentSessionProjectionAuthority()
}`,
		"wrapper": `package main
type agentSessionCutoverStore interface {
	GetAgentSessionProjectionAuthorityStatus()
	ControlAgentSessionProjectionAuthority()
}
func getStatus(st agentSessionCutoverStore) {
	st.GetAgentSessionProjectionAuthorityStatus()
}
func executeAgentSessionCutover(st agentSessionCutoverStore) {
	getStatus(st)
	st.ControlAgentSessionProjectionAuthority()
}`,
	}
	for name, source := range mutations {
		t.Run(name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "main.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := agentEventLedgerRuntimeAdminAuthorityReferences(
				file,
			); err == nil {
				t.Fatal(
					"mutated runtimeadmin unexpectedly passed direct-call guard",
				)
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
		"AppendAgentEvents":                         {},
		"ListAgentEvents":                           {},
		"ReplayAgentEvents":                         {},
		"CommitAgentSessionTurn":                    {},
		"CommitAgentSessionAppend":                  {},
		"AppendAgentSessionMessages":                {},
		"commitAgentSessionAppendTx":                {},
		"UpdateAgentSession":                        {},
		"agentSessionProjectionLedgerAuthoritative": {},
		"GetAgentSessionProjectionAuthorityStatus":  {},
		"ControlAgentSessionProjectionAuthority":    {},
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
			if agentSessionProjectionSQLWrite(value) {
				if _, ok := allowed[typed.Pos()]; !ok {
					violations = append(violations,
						fmt.Sprintf(
							"%s: forbidden direct Agent session projection SQL write",
							fset.Position(typed.Pos()),
						))
				}
			}
		}
		return true
	})
	return violations
}

func agentSessionProjectionSQLWrite(value string) bool {
	normalized := strings.ToLower(
		strings.Join(strings.Fields(value), " "),
	)
	if !strings.Contains(normalized, "update agent_sessions") {
		return false
	}
	return strings.Contains(normalized, "messages") ||
		strings.Contains(normalized, "turn_count") ||
		strings.Contains(normalized, "activated_tools")
}

func agentEventLedgerProviderDeclarations(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	want := map[string]int{
		"AppendAgentEvents":        0,
		"ListAgentEvents":          0,
		"ReplayAgentEvents":        0,
		"CommitAgentSessionTurn":   0,
		"CommitAgentSessionAppend": 0,
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
	helperDeclarations := 0
	providerCalls := 0
	projectionSQLWrites := 0
	routeBoundaries := map[string]int{
		"loadAuthoritativeActiveAgentSessionProjection":    0,
		"loadAuthoritativeAgentSessionProjectionForUpdate": 0,
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if function.Name.Name == "commitAgentSessionAppendTx" {
			if function.Recv != nil {
				return nil, errors.New(
					"Agent session append helper must remain package-private",
				)
			}
			helperDeclarations++
			allowed[function.Name.Pos()] = struct{}{}
		}
		if (function.Name.Name == "CommitAgentSessionTurn" ||
			function.Name.Name == "commitAgentSessionAppendTx") &&
			function.Body != nil {
			ast.Inspect(function.Body, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok {
					return true
				}
				value := strings.Trim(literal.Value, "`\"")
				if !agentSessionProjectionSQLWrite(value) {
					return true
				}
				projectionSQLWrites++
				allowed[literal.Pos()] = struct{}{}
				return true
			})
		}
		if function.Name.Name != "CommitAgentSessionAppend" ||
			function.Body == nil {
			if _, guarded := routeBoundaries[function.Name.Name]; !guarded ||
				function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				helper, ok := call.Fun.(*ast.Ident)
				if !ok ||
					helper.Name !=
						"agentSessionProjectionLedgerAuthoritative" {
					return true
				}
				routeBoundaries[function.Name.Name]++
				allowed[helper.Pos()] = struct{}{}
				return true
			})
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			helper, ok := call.Fun.(*ast.Ident)
			if !ok || helper.Name != "commitAgentSessionAppendTx" {
				return true
			}
			providerCalls++
			allowed[helper.Pos()] = struct{}{}
			return true
		})
	}
	if helperDeclarations != 1 || providerCalls != 1 ||
		projectionSQLWrites != 2 {
		return nil, fmt.Errorf(
			"Agent session provider helper declarations/calls/SQL writes=%d/%d/%d, want 1/1/2",
			helperDeclarations, providerCalls, projectionSQLWrites,
		)
	}
	for functionName, count := range routeBoundaries {
		if count != 1 {
			return nil, fmt.Errorf(
				"Agent session provider %s must directly call projection authority exactly once, got %d",
				functionName, count,
			)
		}
	}
	return allowed, nil
}

func agentEventLedgerAuthorityDeclarations(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	methods := map[string]int{
		"GetAgentSessionProjectionAuthorityStatus": 0,
		"ControlAgentSessionProjectionAuthority":   0,
	}
	helperDeclarations := 0
	allowed := make(map[token.Pos]struct{}, len(methods)+1)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if function.Name.Name ==
			"agentSessionProjectionLedgerAuthoritative" {
			if function.Recv != nil {
				return nil, errors.New(
					"Agent session projection route helper must remain package-private",
				)
			}
			helperDeclarations++
			allowed[function.Name.Pos()] = struct{}{}
			continue
		}
		if _, guarded := methods[function.Name.Name]; !guarded {
			continue
		}
		if function.Recv == nil || len(function.Recv.List) != 1 ||
			!agentEventLedgerStoreReceiver(function.Recv.List[0].Type) {
			return nil, fmt.Errorf(
				"Agent session projection authority %s must remain a Store method",
				function.Name.Name,
			)
		}
		methods[function.Name.Name]++
		allowed[function.Name.Pos()] = struct{}{}
	}
	if helperDeclarations != 1 {
		return nil, fmt.Errorf(
			"Agent session projection route helper declarations=%d, want 1",
			helperDeclarations,
		)
	}
	for name, count := range methods {
		if count != 1 {
			return nil, fmt.Errorf(
				"Agent session projection authority must declare %s exactly once, got %d",
				name, count,
			)
		}
	}
	return allowed, nil
}

func agentEventLedgerRuntimeAdminAuthorityReferences(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	methods := map[string]int{
		"GetAgentSessionProjectionAuthorityStatus": 0,
		"ControlAgentSessionProjectionAuthority":   0,
	}
	allowed := make(map[token.Pos]struct{}, len(methods)*2)
	var execute *ast.FuncDecl
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.GenDecl:
			if typed.Tok != token.TYPE {
				continue
			}
			for _, spec := range typed.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok ||
					typeSpec.Name.Name != "agentSessionCutoverStore" {
					continue
				}
				interfaceType, ok := typeSpec.Type.(*ast.InterfaceType)
				if !ok {
					return nil, errors.New(
						"runtimeadmin Agent session cutover store must remain an interface",
					)
				}
				for _, field := range interfaceType.Methods.List {
					if len(field.Names) != 1 {
						continue
					}
					name := field.Names[0].Name
					if _, guarded := methods[name]; !guarded {
						continue
					}
					if _, ok := field.Type.(*ast.FuncType); !ok {
						return nil, fmt.Errorf(
							"runtimeadmin authority boundary %s must remain a method field",
							name,
						)
					}
					methods[name]++
					allowed[field.Names[0].Pos()] = struct{}{}
				}
			}
		case *ast.FuncDecl:
			if typed.Name.Name != "executeAgentSessionCutover" {
				continue
			}
			if execute != nil {
				return nil, errors.New(
					"runtimeadmin Agent session cutover executor is duplicated",
				)
			}
			execute = typed
		}
	}
	for name, count := range methods {
		if count != 1 {
			return nil, fmt.Errorf(
				"runtimeadmin cutover store must declare %s exactly once, got %d",
				name, count,
			)
		}
	}
	if execute == nil || execute.Body == nil {
		return nil, errors.New(
			"runtimeadmin Agent session cutover executor is unavailable",
		)
	}
	directCalls := map[string]int{
		"GetAgentSessionProjectionAuthorityStatus": 0,
		"ControlAgentSessionProjectionAuthority":   0,
	}
	ast.Inspect(execute.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, guarded := directCalls[method.Sel.Name]; !guarded {
			return true
		}
		receiver, ok := method.X.(*ast.Ident)
		if !ok || receiver.Name != "st" {
			return true
		}
		directCalls[method.Sel.Name]++
		allowed[method.Sel.Pos()] = struct{}{}
		return true
	})
	for name, count := range directCalls {
		if count != 1 {
			return nil, fmt.Errorf(
				"runtimeadmin cutover executor must directly call st.%s exactly once, got %d",
				name, count,
			)
		}
	}
	return allowed, nil
}

func agentEventLedgerReceiptHelperReferences(
	file *ast.File,
	functionName string,
	expectedCalls int,
) (map[token.Pos]struct{}, error) {
	allowed := make(map[token.Pos]struct{}, expectedCalls)
	var target *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != functionName {
			continue
		}
		if target != nil {
			return nil, fmt.Errorf(
				"receipt helper boundary %s is duplicated", functionName,
			)
		}
		target = function
	}
	if target == nil || target.Body == nil ||
		target.Recv == nil || len(target.Recv.List) != 1 ||
		!agentEventLedgerStoreReceiver(target.Recv.List[0].Type) {
		return nil, fmt.Errorf(
			"receipt helper boundary %s must remain a Store method",
			functionName,
		)
	}
	directCalls := 0
	ast.Inspect(target.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		helper, ok := call.Fun.(*ast.Ident)
		if !ok || helper.Name != "commitAgentSessionAppendTx" {
			return true
		}
		directCalls++
		allowed[helper.Pos()] = struct{}{}
		return true
	})
	if directCalls != expectedCalls {
		return nil, fmt.Errorf(
			"receipt boundary %s must directly call commitAgentSessionAppendTx %d times, got %d",
			functionName, expectedCalls, directCalls,
		)
	}
	return allowed, nil
}

func agentEventLedgerAgentLoopReferences(
	file *ast.File,
) (map[token.Pos]struct{}, error) {
	boundaries := map[string]string{
		"CommitAgentSessionTurn":   "saveSession",
		"CommitAgentSessionAppend": "appendCardCallback",
	}
	allowed := make(map[token.Pos]struct{}, 6)

	interfaceFields := make(map[string]int, len(boundaries))
	functions := make(map[string]*ast.FuncDecl, len(boundaries)+1)
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
					if len(field.Names) != 1 {
						continue
					}
					name := field.Names[0].Name
					if _, guarded := boundaries[name]; !guarded {
						continue
					}
					if _, ok := field.Type.(*ast.FuncType); !ok {
						return nil, errors.New(
							"Agent loop ledger boundary must be a method field",
						)
					}
					interfaceFields[name]++
					allowed[field.Names[0].Pos()] = struct{}{}
				}
			}
		case *ast.FuncDecl:
			if typed.Name.Name == "saveSession" ||
				typed.Name.Name == "appendCardCallback" ||
				typed.Name.Name == "NotifyEvent" {
				if functions[typed.Name.Name] != nil {
					return nil, errors.New(
						"Agent loop guarded function is duplicated",
					)
				}
				functions[typed.Name.Name] = typed
			}
		}
	}
	for name, functionName := range boundaries {
		if interfaceFields[name] != 1 {
			return nil, fmt.Errorf(
				"Agent loop Store must declare %s exactly once, got %d",
				name, interfaceFields[name],
			)
		}
		function := functions[functionName]
		if function == nil || function.Body == nil {
			return nil, fmt.Errorf(
				"Agent loop %s is unavailable", functionName,
			)
		}
		if function.Recv == nil || len(function.Recv.List) != 1 ||
			len(function.Recv.List[0].Names) != 1 ||
			function.Recv.List[0].Names[0].Name != "l" ||
			!agentEventLedgerLoopReceiver(function.Recv.List[0].Type) {
			return nil, fmt.Errorf(
				"Agent loop %s must remain a method on receiver l *Loop",
				functionName,
			)
		}
		directCalls := 0
		ast.Inspect(function.Body, func(node ast.Node) bool {
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
				"Agent loop %s must directly call l.store.%s exactly once, got %d",
				functionName, name, directCalls,
			)
		}
	}
	// NotifyEvent shares the same exact append boundary and must contain one
	// direct call; allow only that second selector occurrence.
	notify := functions["NotifyEvent"]
	if notify == nil || notify.Body == nil {
		return nil, errors.New("Agent loop NotifyEvent is unavailable")
	}
	notifyCalls := 0
	ast.Inspect(notify.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "CommitAgentSessionAppend" {
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
		notifyCalls++
		allowed[method.Sel.Pos()] = struct{}{}
		return true
	})
	if notifyCalls != 1 {
		return nil, fmt.Errorf(
			"Agent loop NotifyEvent must directly call l.store.CommitAgentSessionAppend exactly once, got %d",
			notifyCalls,
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
