package store

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func memoryTestEvidence() types.MemoryEvidence {
	return types.MemoryEvidence{
		SourceType:          types.MemoryEvidenceOwnerExplicitAgentTurn,
		SourceID:            uuid.NewString(),
		OwnerRequest:        "请记住这条测试经验",
		AuthorizationDigest: strings.Repeat("d", 64),
	}
}

func authorizeMemoryAction(
	t *testing.T, st *Store, tenantID, userID int64, action types.MemoryAction,
) types.MemoryAction {
	t.Helper()
	session, err := st.CreateAgentSession(t.Context(), userID)
	if err != nil {
		t.Fatal(err)
	}
	id, err := st.PrepareMemoryAuthorization(
		t.Context(), tenantID, userID, session.ID, action,
	)
	if err != nil {
		t.Fatal(err)
	}
	action.Evidence.AuthorizationID = id
	return action
}

func TestRankMemoryRecordsBilingualAndStable(t *testing.T) {
	t.Parallel()
	records := []types.MemoryRecord{
		{ID: 30, Text: "User prefers dark mode and concise English reports", Active: true},
		{ID: 20, Text: "用户喜欢深色模式，中文报告要简洁", Active: true},
		{ID: 10, Text: "每周一发送市场情报 weekly market brief", Active: true},
	}
	chinese, err := rankMemoryRecords(records, "深色模式 简洁报告", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(chinese) < 1 || chinese[0].Memory.ID != 20 || chinese[0].Score <= 0 {
		t.Fatalf("Chinese BM25=%+v, want memory 20 first", chinese)
	}
	english, err := rankMemoryRecords(records, "dark mode concise reports", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(english) < 1 || english[0].Memory.ID != 30 || english[0].Score <= 0 {
		t.Fatalf("English BM25=%+v, want memory 30 first", english)
	}
	first, err := rankMemoryRecords(records, "weekly market brief", 3)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		got, err := rankMemoryRecords(records, "weekly market brief", 3)
		if err != nil || !reflect.DeepEqual(got, first) {
			t.Fatalf("ranking drift got=%+v err=%v want=%+v", got, err, first)
		}
	}
}

func TestValidateMemoryBoundsAndExplicitAuthority(t *testing.T) {
	t.Parallel()
	base := types.MemoryAction{
		Action:   types.MemoryActionRemember,
		Text:     "记住这个 owner preference",
		Evidence: memoryTestEvidence(),
	}
	validKey := strings.Repeat("a", 64)
	if _, _, err := validateMemoryAction(validKey, base); err != nil {
		t.Fatalf("valid action: %v", err)
	}
	for name, mutate := range map[string]func(*types.MemoryAction) string{
		"implicit source": func(a *types.MemoryAction) string {
			a.Evidence.SourceType = "model_inferred"
			return validKey
		},
		"non canonical UUID": func(a *types.MemoryAction) string {
			a.Evidence.SourceID = strings.ToUpper(a.Evidence.SourceID)
			return validKey
		},
		"oversize text": func(a *types.MemoryAction) string {
			a.Text = strings.Repeat("界", maxMemoryTextBytes/3+1)
			return validKey
		},
		"unsearchable": func(a *types.MemoryAction) string {
			a.Text = "___ !!!"
			return validKey
		},
		"credential": func(a *types.MemoryAction) string {
			a.Text = "password: correct-horse-battery"
			return validKey
		},
		"bad key": func(_ *types.MemoryAction) string { return "A" + validKey[1:] },
	} {
		t.Run(name, func(t *testing.T) {
			action := base
			key := mutate(&action)
			if _, _, err := validateMemoryAction(key, action); err == nil {
				t.Fatal("invalid action accepted")
			}
		})
	}
}

func TestRecallMemoryQueryBoundsBeforeDatabase(t *testing.T) {
	t.Parallel()
	for name, query := range map[string]types.MemoryRecallQuery{
		"empty":        {Query: "", Limit: 1},
		"invalid UTF8": {Query: string([]byte{0xff}), Limit: 1},
		"too many bytes": {
			Query: strings.Repeat("界", maxMemoryRecallQueryBytes/3+1), Limit: 1,
		},
		"zero limit":  {Query: "memory", Limit: 0},
		"large limit": {Query: "memory", Limit: maxMemoryRecallResults + 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (&Store{}).RecallMemories(
				t.Context(), 1, 1, query); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("RecallMemories(%+v) err=%v", query, err)
			}
		})
	}
}

func TestMemoryLedgerRecallIsolationReplayAndLifecyclePostgres(t *testing.T) {
	st := memoryTestStore(t)
	ctx := t.Context()
	userA := memoryTestUser(t, st, "memory_a")
	userB := memoryTestUser(t, st, "memory_b")
	t.Cleanup(func() { cleanupMemoryUsers(t, st, userA, userB) })

	remember := types.MemoryAction{
		Action:   types.MemoryActionRemember,
		Text:     "用户喜欢中文周报，偏好 concise weekly reports",
		Evidence: memoryTestEvidence(),
	}
	if _, err := st.ApplyMemoryAction(
		ctx, 1, userA, strings.Repeat("0", 64), remember,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("bare trace created active memory without durable authorization: %v", err)
	}
	remember = authorizeMemoryAction(t, st, 1, userA, remember)
	key := strings.Repeat("1", 64)
	type outcome struct {
		result *types.MemoryActionResult
		err    error
	}
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			result, err := st.ApplyMemoryAction(ctx, 1, userA, key, remember)
			results <- outcome{result, err}
		}()
	}
	var remembered *types.MemoryActionResult
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatalf("concurrent replay: %v", got.err)
		}
		if remembered == nil {
			remembered = got.result
		} else if !memoryActionResultsEquivalent(remembered, got.result) {
			t.Fatalf("exact replay diverged: %+v / %+v", remembered, got.result)
		}
	}
	if _, err := st.ApplyMemoryAction(ctx, 1, userA, key,
		types.MemoryAction{
			Action: types.MemoryActionRemember, Text: "different",
			Evidence: remember.Evidence,
		}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("same key different request=%v, want conflict", err)
	}
	if _, err := st.GetMemory(ctx, 1, userB, remembered.Memory.ID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("cross-user GetMemory leaked target: %v", err)
	}
	otherRecall, err := st.RecallMemories(ctx, 1, userB,
		types.MemoryRecallQuery{Query: "中文周报", Limit: 8})
	if err != nil || len(otherRecall.Memories) != 0 {
		t.Fatalf("cross-user recall=%+v err=%v", otherRecall, err)
	}

	correctAction := authorizeMemoryAction(t, st, 1, userA, types.MemoryAction{
		Action: types.MemoryActionCorrect, MemoryID: remembered.Memory.ID,
		Text:     "用户喜欢中文日报，偏好 concise daily reports",
		Evidence: memoryTestEvidence(),
	})
	corrected, err := st.ApplyMemoryAction(ctx, 1, userA,
		strings.Repeat("2", 64), correctAction)
	if err != nil {
		t.Fatal(err)
	}
	if corrected.Memory.SupersedesMemoryID != remembered.Memory.ID {
		t.Fatalf("correction=%+v", corrected)
	}
	if _, err := st.GetMemory(ctx, 1, userA, remembered.Memory.ID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("superseded record remained active: %v", err)
	}
	recall, err := st.RecallMemories(ctx, 1, userA,
		types.MemoryRecallQuery{Query: "中文 daily", Limit: 8})
	if err != nil || len(recall.Memories) != 1 ||
		recall.Memories[0].Memory.ID != corrected.Memory.ID {
		t.Fatalf("corrected recall=%+v err=%v", recall, err)
	}
	forgetAction := authorizeMemoryAction(t, st, 1, userA, types.MemoryAction{
		Action: types.MemoryActionForget, MemoryID: corrected.Memory.ID,
		Evidence: memoryTestEvidence(),
	})
	forgotten, err := st.ApplyMemoryAction(ctx, 1, userA,
		strings.Repeat("3", 64), forgetAction)
	if err != nil || forgotten.Memory.Active {
		t.Fatalf("forget=%+v err=%v", forgotten, err)
	}
	recall, err = st.RecallMemories(ctx, 1, userA,
		types.MemoryRecallQuery{Query: "中文 daily", Limit: 8})
	if err != nil || len(recall.Memories) != 0 {
		t.Fatalf("forgotten memory recalled=%+v err=%v", recall, err)
	}
}

// PostgreSQL stores timestamptz at microsecond precision; receipt JSON
// preserves the same instant but Go's decoded time.Location is UTC rather than
// the pgx-scanned local pointer. Equal compares instants and keeps the replay
// oracle about payload semantics instead of time.Location identity.
func memoryActionResultsEquivalent(a, b *types.MemoryActionResult) bool {
	if a == nil || b == nil {
		return a == b
	}
	aCopy, bCopy := *a, *b
	aMemory, bMemory := a.Memory, b.Memory
	aEvent, bEvent := a.Event, b.Event
	if !aMemory.CreatedAt.Equal(bMemory.CreatedAt) ||
		!aEvent.CreatedAt.Equal(bEvent.CreatedAt) {
		return false
	}
	aMemory.CreatedAt, bMemory.CreatedAt = time.Time{}, time.Time{}
	aEvent.CreatedAt, bEvent.CreatedAt = time.Time{}, time.Time{}
	aCopy.Memory, bCopy.Memory = aMemory, bMemory
	aCopy.Event, bCopy.Event = aEvent, bEvent
	return reflect.DeepEqual(aCopy, bCopy)
}

func TestMemoryConcurrentDifferentCorrectionsOnlyOneWinsPostgres(t *testing.T) {
	st := memoryTestStore(t)
	userID := memoryTestUser(t, st, "memory_race")
	t.Cleanup(func() { cleanupMemoryUsers(t, st, userID) })
	rememberAction := authorizeMemoryAction(t, st, 1, userID, types.MemoryAction{
		Action: types.MemoryActionRemember, Text: "prefers blue",
		Evidence: memoryTestEvidence(),
	})
	remembered, err := st.ApplyMemoryAction(t.Context(), 1, userID,
		strings.Repeat("4", 64), rememberAction)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, text := range []string{"prefers green", "prefers red"} {
		action := authorizeMemoryAction(t, st, 1, userID, types.MemoryAction{
			Action: types.MemoryActionCorrect, MemoryID: remembered.Memory.ID,
			Text: text, Evidence: memoryTestEvidence(),
		})
		wg.Add(1)
		go func(i int, action types.MemoryAction) {
			defer wg.Done()
			<-start
			_, err := st.ApplyMemoryAction(t.Context(), 1, userID,
				strings.Repeat(string(rune('5'+i)), 64), action)
			errs <- err
		}(i, action)
	}
	close(start)
	wg.Wait()
	close(errs)
	successes, conflicts := 0, 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, types.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected race error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestPurgeTenantRemovesLongTermMemoryHistoryPostgres(t *testing.T) {
	st := memoryTestStore(t)
	tenantID := seedPurgeTenant(t, st)
	var userID int64
	if err := st.pool.QueryRow(t.Context(), `
		SELECT user_id FROM memberships WHERE tenant_id=$1 AND role='owner'`,
		tenantID).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	purgeAction := authorizeMemoryAction(t, st, tenantID, userID, types.MemoryAction{
		Action: types.MemoryActionRemember, Text: "purge retained memory",
		Evidence: memoryTestEvidence(),
	})
	remembered, err := st.ApplyMemoryAction(t.Context(), tenantID, userID,
		strings.Repeat("7", 64), purgeAction)
	if err != nil {
		t.Fatal(err)
	}
	dryRun, err := st.PurgeTenant(t.Context(), tenantID, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"memory_receipts", "memory_events", "memory_records", "memory_authorizations"} {
		if dryRun.Rows[table] != 1 {
			t.Fatalf("dry-run %s=%d, report=%+v", table, dryRun.Rows[table], dryRun)
		}
	}
	if _, err := st.GetMemory(t.Context(), tenantID, userID,
		remembered.Memory.ID); err != nil {
		t.Fatalf("dry-run changed memory: %v", err)
	}
	report, err := st.PurgeTenant(t.Context(), tenantID, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"memory_receipts", "memory_events", "memory_records", "memory_authorizations"} {
		if report.Rows[table] != 1 {
			t.Fatalf("purge %s=%d, report=%+v", table, report.Rows[table], report)
		}
	}
}

func TestMemoryActiveCorpusAndCountBoundsPostgres(t *testing.T) {
	st := memoryTestStore(t)
	corpusUser := memoryTestUser(t, st, "memory_corpus_bound")
	countUser := memoryTestUser(t, st, "memory_count_bound")
	damagedUser := memoryTestUser(t, st, "memory_damaged_bound")
	t.Cleanup(func() { cleanupMemoryUsers(t, st, corpusUser, countUser, damagedUser) })
	seed := func(userID int64, count, textBytes int) {
		t.Helper()
		session, err := st.CreateAgentSession(t.Context(), userID)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		if _, err := tx.Exec(t.Context(), `
			INSERT INTO memory_authorizations(
			 id,tenant_id,user_id,session_id,trace_id,owner_request,
			 authorization_digest,request_digest)
			SELECT gen_random_uuid(),1,$1,$2,gen_random_uuid(),
			       '请记住边界测试',repeat('d',64),repeat('c',64)
			  FROM generate_series(1,$3)`, userID, session.ID, count); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), `
			INSERT INTO memory_records(
			 tenant_id,user_id,memory_text,evidence_source_type,evidence_source_id,
			 authorization_id,owner_request,authorization_digest)
			SELECT 1,$1,repeat('x',$2),'owner_explicit_agent_turn',gen_random_uuid(),
			       id,owner_request,authorization_digest
			  FROM memory_authorizations WHERE tenant_id=1 AND user_id=$1`,
			userID, textBytes); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(t.Context(), `
			INSERT INTO memory_events(
			 tenant_id,user_id,actor_user_id,event_kind,result_memory_id,
			 evidence_source_type,evidence_source_id,authorization_id,
			 owner_request,authorization_digest)
			SELECT 1,$1,$1,'remember',record.id,'owner_explicit_agent_turn',
			       gen_random_uuid(),record.authorization_id,record.owner_request,
			       record.authorization_digest
			  FROM memory_records record
			 WHERE tenant_id=1 AND user_id=$1`, userID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	seed(corpusUser, maxActiveMemoryCorpusBytes/maxMemoryTextBytes, maxMemoryTextBytes)
	seed(countUser, maxActiveMemories, 1)
	seed(damagedUser, maxActiveMemories+1, 1)
	if _, err := st.RecallMemories(t.Context(), 1, damagedUser,
		types.MemoryRecallQuery{Query: "x", Limit: 8},
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("damaged oversized corpus reached BM25: %v", err)
	}
	for label, userID := range map[string]int64{
		"corpus": corpusUser, "count": countUser,
	} {
		action := authorizeMemoryAction(t, st, 1, userID, types.MemoryAction{
			Action: types.MemoryActionRemember, Text: "one more",
			Evidence: memoryTestEvidence(),
		})
		_, err := st.ApplyMemoryAction(t.Context(), 1, userID,
			strings.Repeat("8", 64), action)
		if !errors.Is(err, types.ErrValidation) {
			t.Fatalf("%s bound err=%v, want validation", label, err)
		}
	}
}

func memoryTestStore(t *testing.T) *Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is required for long-term memory PostgreSQL tests")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	return st
}

func memoryTestUser(t *testing.T, st *Store, prefix string) int64 {
	t.Helper()
	user, err := st.UpsertUserByOpenID(t.Context(), prefix+"_"+uuid.NewString(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, user.ID)
	return user.ID
}

func cleanupMemoryUsers(t *testing.T, st *Store, userIDs ...int64) {
	ctx, cancel := cleanupContext()
	defer cancel()
	for _, table := range []string{"memory_receipts", "memory_events", "memory_records", "memory_authorizations"} {
		cleanupExec(ctx, t, st, "DELETE FROM "+table+" WHERE user_id=ANY($1)", userIDs)
	}
	cleanupExec(ctx, t, st, "DELETE FROM agent_sessions WHERE user_id=ANY($1)", userIDs)
	cleanupExec(ctx, t, st, "DELETE FROM memberships WHERE user_id=ANY($1)", userIDs)
	cleanupExec(ctx, t, st, "DELETE FROM users WHERE id=ANY($1)", userIDs)
}
