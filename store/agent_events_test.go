package store

import (
	"context"
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
	})
	asTenant(t, f.store, 0, func(tx pgx.Tx) {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM agent_events`).Scan(&visible); err != nil {
			t.Fatal(err)
		}
		if visible != 0 {
			t.Fatalf("no tenant context leaked %d rows", visible)
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

func TestAgentEventLedgerHasZeroProductionCallPoints(t *testing.T) {
	t.Parallel()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Agent event ledger guard")
	}
	storeDir := filepath.Clean(filepath.Dir(testFile))
	repoRoot := filepath.Clean(filepath.Dir(storeDir))
	provider := filepath.Join(storeDir, "agent_events.go")
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
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") ||
			filepath.Clean(path) == provider {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		violations = append(
			violations,
			agentEventLedgerForbiddenReferences(fset, file)...,
		)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("7.7-A must keep zero production call points:\n%s",
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
var byName = "ListAgentEvents"
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations := agentEventLedgerForbiddenReferences(fset, file)
	if len(violations) < 4 {
		t.Fatalf("method value/interface/wrapper escaped guard: %v", violations)
	}
}

func agentEventLedgerForbiddenReferences(
	fset *token.FileSet,
	file *ast.File,
) []string {
	sensitive := map[string]struct{}{
		"AppendAgentEvents": {},
		"ListAgentEvents":   {},
		"ReplayAgentEvents": {},
	}
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.Ident:
			if _, forbidden := sensitive[typed.Name]; forbidden {
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
