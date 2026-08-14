package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestTaskCreationCheckpointBoundsRejectBeforeDatabase(t *testing.T) {
	var st Store
	lease := types.TaskCreationLease{
		ID: "size-limit", TenantID: 1, UserID: 1, LeaseOwner: "test", Fence: 1,
	}
	cases := []struct {
		name string
		call func() error
	}{
		{"normalized command", func() error {
			return st.SealTaskCreationCommand(
				t.Context(), lease, make([]byte, maxTaskCreationCommandBytes+1))
		}},
		{"compiled definition", func() error {
			return st.CheckpointTaskCreationDefinition(
				t.Context(), lease, make([]byte, maxTaskCreationDefinitionBytes+1),
				strings.Repeat("0", 64))
		}},
		{"prepared schedule", func() error {
			return st.CheckpointTaskCreationSchedule(
				t.Context(), lease, make([]byte, maxTaskCreationPreparedBytes+1))
		}},
		{"ensure receipt", func() error {
			return st.CheckpointTaskCreationEnsureReceipt(
				t.Context(), lease, make([]byte, maxTaskCreationReceiptBytes+1), "task")
		}},
		{"terminal result", func() error {
			result := json.RawMessage(`{"x":"` +
				strings.Repeat("x", maxTaskCreationResultBytes) + `"}`)
			return st.CompleteTaskCreationOperation(t.Context(), lease, "task", result)
		}},
		{"terminal result duplicate key", func() error {
			return st.CompleteTaskCreationOperation(
				t.Context(), lease, "task", json.RawMessage(`{"x":1,"x":2}`))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("oversized/ambiguous checkpoint 必须在 DB 前拒绝: %v", err)
			}
		})
	}
}

// TestTaskCreationOperationStore uses two independent pgx pools against real
// PostgreSQL. The lease/fence protocol exists specifically for process races;
// a single in-memory fake or one mocked transaction cannot prove its claims.
func TestTaskCreationOperationStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 task creation operation 真库测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}

	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 第一连接池失败: %v", err)
	}
	registerStoreClose(t, st)
	st2, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 第二连接池失败: %v", err)
	}
	registerStoreClose(t, st2)

	userID := testUserWithTenant(t, st, "creation-saga-owner")
	otherUserID := testUserWithTenant(t, st, "creation-saga-other")
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_receipts WHERE user_id IN ($1, $2)`, userID, otherUserID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM schedules WHERE user_id IN ($1, $2)`, userID, otherUserID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_creation_operations WHERE user_id IN ($1, $2)`, userID, otherUserID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM agent_sessions WHERE user_id IN ($1, $2)`, userID, otherUserID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM memberships WHERE user_id IN ($1, $2)`, userID, otherUserID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM users WHERE id IN ($1, $2)`, userID, otherUserID)
	})

	t.Run("双连接互斥与响应丢失同owner重放", func(t *testing.T) {
		const requiredTakeoverSafetyGrace = 30 * time.Second
		const renewedLeaseDuration = 10 * time.Minute

		id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		type result struct {
			op  *types.TaskCreationOperation
			err error
		}
		start := make(chan struct{})
		results := make(chan result, 2)
		for _, candidate := range []struct {
			store *Store
			owner string
		}{{st, "racing-worker-a"}, {st2, "racing-worker-b"}} {
			go func(candidate struct {
				store *Store
				owner string
			}) {
				<-start
				op, err := candidate.store.AcquireTaskCreationOperation(
					ctx, acquireParams(id, userID, candidate.owner))
				results <- result{op: op, err: err}
			}(candidate)
		}
		close(start)
		first, second := <-results, <-results
		outcomes := []result{first, second}
		var winner *types.TaskCreationOperation
		busy := 0
		for _, outcome := range outcomes {
			switch {
			case outcome.err == nil:
				winner = outcome.op
			case errors.Is(outcome.err, types.ErrTaskCreationBusy):
				busy++
			default:
				t.Fatalf("并发 Acquire 非预期结果: op=%+v err=%v", outcome.op, outcome.err)
			}
		}
		if winner == nil || busy != 1 {
			t.Fatalf("应恰有一个 winner/一个 busy: winner=%+v busy=%d", winner, busy)
		}
		if winner.Fence != 1 || winner.Attempt != 1 ||
			winner.Phase != types.TaskCreationPhaseClaimed || winner.LeaseUntil == nil ||
			winner.TakeoverNotBefore == nil {
			t.Fatalf("首次领取状态错误: %+v", winner)
		}
		initialGrace := winner.TakeoverNotBefore.Sub(*winner.LeaseUntil)
		if initialGrace < requiredTakeoverSafetyGrace ||
			initialGrace > requiredTakeoverSafetyGrace+time.Second {
			t.Fatalf("首次领取安全窗口错误: grace=%v operation=%+v", initialGrace, winner)
		}

		// 模拟 COMMIT 已成功但响应丢失：同一 owner token 从另一进程重放。
		replay, err := st2.AcquireTaskCreationOperation(ctx, acquireParams(id, userID, winner.LeaseOwner))
		if err != nil {
			t.Fatalf("same-owner replay 失败: %v", err)
		}
		if replay.Fence != winner.Fence || replay.Attempt != winner.Attempt ||
			replay.LeaseUntil == nil || !replay.LeaseUntil.Equal(*winner.LeaseUntil) ||
			replay.TakeoverNotBefore == nil ||
			!replay.TakeoverNotBefore.Equal(*winner.TakeoverNotBefore) {
			t.Fatalf("replay 不得改 fence/attempt/lease: first=%+v replay=%+v", winner, replay)
		}

		var databaseBeforeRenew time.Time
		if err := st2.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseBeforeRenew); err != nil {
			t.Fatal(err)
		}
		if err := st2.RenewTaskCreationLease(ctx, replay.Lease(), renewedLeaseDuration); err != nil {
			t.Fatalf("renew 失败: %v", err)
		}
		var databaseAfterRenew time.Time
		if err := st2.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseAfterRenew); err != nil {
			t.Fatal(err)
		}
		renewed, err := st2.LoadTaskCreationOperation(ctx, id, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if renewed.LeaseUntil == nil || renewed.TakeoverNotBefore == nil {
			t.Fatalf("renew 未持久化 lease 边界: %+v", renewed)
		}
		renewedGrace := renewed.TakeoverNotBefore.Sub(*renewed.LeaseUntil)
		if !renewed.LeaseUntil.After(*replay.LeaseUntil) ||
			renewed.LeaseUntil.Before(databaseBeforeRenew.Add(renewedLeaseDuration)) ||
			renewed.LeaseUntil.After(databaseAfterRenew.Add(renewedLeaseDuration)) ||
			renewedGrace < requiredTakeoverSafetyGrace ||
			renewedGrace > requiredTakeoverSafetyGrace+time.Second {
			t.Fatalf("renew 未持久化独立安全窗口: %+v", renewed)
		}
	})

	t.Run("takeoverGrace与单调fence隔离旧worker所有写", func(t *testing.T) {
		id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		first, err := st.AcquireTaskCreationOperation(ctx, acquireParams(id, userID, "old-worker"))
		if err != nil {
			t.Fatal(err)
		}
		oldLease := first.Lease()
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET lease_until=clock_timestamp()-interval '2 seconds',
			        takeover_not_before=clock_timestamp()+interval '1 minute'
			  WHERE id=$1`, id,
		); err != nil {
			t.Fatal(err)
		}

		grace := acquireParams(id, userID, "new-worker")
		if _, err := st2.AcquireTaskCreationOperation(ctx, grace); !errors.Is(err, types.ErrTaskCreationBusy) {
			t.Fatalf("grace 内接管应 Busy，实际 %v", err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET takeover_not_before=clock_timestamp()-interval '1 second'
			  WHERE id=$1`, id,
		); err != nil {
			t.Fatal(err)
		}
		second, err := st2.AcquireTaskCreationOperation(ctx, grace)
		if err != nil {
			t.Fatalf("过 grace 接管失败: %v", err)
		}
		if second.Fence != oldLease.Fence+1 || second.Attempt != 2 || second.LeaseOwner != "new-worker" {
			t.Fatalf("接管未单调推进 fence/attempt: %+v", second)
		}

		staleDefinition := []byte(`{"sources":[1]}`)
		validDigest := digestOf(staleDefinition)
		staleWrites := []struct {
			name string
			call func() error
		}{
			{"renew", func() error { return st.RenewTaskCreationLease(ctx, oldLease, time.Minute) }},
			{"seal", func() error { return st.SealTaskCreationCommand(ctx, oldLease, []byte(`{"x":1}`)) }},
			{"definition", func() error {
				return st.CheckpointTaskCreationDefinition(ctx, oldLease, staleDefinition, validDigest)
			}},
			{"schedule", func() error {
				return st.CheckpointTaskCreationSchedule(ctx, oldLease, []byte(`{"schedule":1}`))
			}},
			{"ensure", func() error {
				return st.CheckpointTaskCreationEnsureReceipt(ctx, oldLease, []byte(`{"ok":true}`), "task-x")
			}},
			{"block", func() error { return st.BlockTaskCreationOperation(ctx, oldLease, "AMBIGUOUS", "blocked") }},
			{"fail", func() error { return st.FailTaskCreationOperation(ctx, oldLease, "INVALID", "failed") }},
			{"complete", func() error {
				return st.CompleteTaskCreationOperation(ctx, oldLease, "task-x", json.RawMessage(`{"ok":true}`))
			}},
		}
		for _, write := range staleWrites {
			t.Run(write.name, func(t *testing.T) {
				if err := write.call(); !errors.Is(err, types.ErrTaskCreationLeaseLost) {
					t.Fatalf("旧 fence 写应 LeaseLost，实际 %v", err)
				}
			})
		}
		if started, err := st.BeginTaskCreationTranslation(ctx, oldLease); started ||
			!errors.Is(err, types.ErrTaskCreationLeaseLost) {
			t.Fatalf("旧 fence BeginTranslation 应 (false, LeaseLost)，实际 (%v,%v)", started, err)
		}

		after, err := st.LoadTaskCreationOperation(ctx, id, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if after.Fence != second.Fence || after.LeaseOwner != second.LeaseOwner ||
			after.Phase != types.TaskCreationPhaseClaimed || len(after.NormalizedCommand) != 0 ||
			after.TombstonedAt != nil {
			t.Fatalf("旧 fence 产生了副作用: %+v", after)
		}

		// owner token 不是 fencing token 的替代品：同一进程过期后以同 owner
		// 接管也必须 fence+1，旧 lease 仅凭 owner 相同仍然写不进去。
		sameOwnerID := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		sameFirst, err := st.AcquireTaskCreationOperation(ctx,
			acquireParams(sameOwnerID, userID, "stable-process-owner"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET lease_until=clock_timestamp()-interval '1 minute',
			        takeover_not_before=clock_timestamp()-interval '1 second'
			  WHERE id=$1`,
			sameOwnerID,
		); err != nil {
			t.Fatal(err)
		}
		sameTakeover := acquireParams(sameOwnerID, userID, "stable-process-owner")
		sameSecond, err := st.AcquireTaskCreationOperation(ctx, sameTakeover)
		if err != nil {
			t.Fatal(err)
		}
		if sameSecond.Fence != sameFirst.Fence+1 || sameSecond.LeaseOwner != sameFirst.LeaseOwner {
			t.Fatalf("same-owner takeover 未推进 fence: first=%+v second=%+v", sameFirst, sameSecond)
		}
		if err := st.SealTaskCreationCommand(ctx, sameFirst.Lease(), []byte(`{"old":true}`)); !errors.Is(err, types.ErrTaskCreationLeaseLost) {
			t.Fatalf("same owner 的 stale fence 应 LeaseLost，实际 %v", err)
		}
	})

	t.Run("持锁跨过expiry后使用真实时钟接管且新lease不缩短", func(t *testing.T) {
		id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		initial := acquireParams(id, userID, "expiring-worker")
		initial.LeaseDuration = 400 * time.Millisecond
		first, err := st.AcquireTaskCreationOperation(ctx, initial)
		if err != nil {
			t.Fatal(err)
		}
		if first.LeaseUntil == nil {
			t.Fatal("首次 lease_until 为空")
		}
		// Keep this clock test short by moving only its persisted safety
		// boundary to lease expiry. Other tests prove production acquisition
		// always writes the fixed, non-caller-controlled grace.
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations SET takeover_not_before=lease_until WHERE id=$1`, id,
		); err != nil {
			t.Fatal(err)
		}

		// 在第一条 lease 仍有效时锁住行。第二个连接先开启 Acquire，随后
		// 被 FOR UPDATE 挡住，直到旧 lease 已过期才放行。如果实现使用事务
		// 固定的 now()，它仍会看到等锁前的时间并错误返回 Busy，或把新 lease
		// 从旧时间起算而白白缩短。
		locker, err := st.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackTaskCreationTransaction(ctx, locker)
		var lockedID string
		if err := locker.QueryRow(ctx,
			`SELECT id FROM task_creation_operations WHERE id=$1 FOR UPDATE`, id,
		).Scan(&lockedID); err != nil {
			t.Fatal(err)
		}

		outcome := make(chan struct {
			op  *types.TaskCreationOperation
			err error
		}, 1)
		takeover := acquireParams(id, userID, "post-expiry-worker")
		takeover.LeaseDuration = 2 * time.Second
		go func() {
			op, err := st2.AcquireTaskCreationOperation(ctx, takeover)
			outcome <- struct {
				op  *types.TaskCreationOperation
				err error
			}{op: op, err: err}
		}()

		// The lease is owned by PostgreSQL's clock, which can drift briefly
		// from the host clock used by the Go test under a loaded VM. Wait for
		// the authoritative clock to cross the persisted boundary instead of
		// assuming a host-side sleep proves database expiry.
		databaseExpired := false
		var releasedAt time.Time
		waitDeadline := time.Now().Add(5 * time.Second)
		for !databaseExpired {
			if err := st.pool.QueryRow(ctx,
				`SELECT clock_timestamp() >= $1, clock_timestamp()`,
				first.LeaseUntil,
			).Scan(&databaseExpired, &releasedAt); err != nil {
				t.Fatal(err)
			}
			if !databaseExpired {
				if time.Now().After(waitDeadline) {
					t.Fatal("database clock did not cross lease expiry")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		if err := locker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got := <-outcome
		if got.err != nil {
			t.Fatalf("锁等待跨 expiry 后应接管成功，实际 %v", got.err)
		}
		if got.op.Fence != first.Fence+1 || got.op.LeaseOwner != takeover.LeaseOwner ||
			got.op.LeaseUntil == nil {
			t.Fatalf("跨 expiry 接管状态错误: %+v", got.op)
		}
		if !got.op.LeaseUntil.After(releasedAt.Add(1500 * time.Millisecond)) {
			t.Fatalf("新 lease 从等锁前固定时间起算而被缩短: release=%v lease_until=%v",
				releasedAt, got.op.LeaseUntil)
		}
	})

	t.Run("持锁跨过expiry后renew和terminal均不能复活旧lease", func(t *testing.T) {
		for _, testCase := range []struct {
			name string
			call func(*Store, types.TaskCreationLease) error
		}{
			{
				name: "renew",
				call: func(store *Store, lease types.TaskCreationLease) error {
					return store.RenewTaskCreationLease(ctx, lease, 2*time.Second)
				},
			},
			{
				name: "terminal",
				call: func(store *Store, lease types.TaskCreationLease) error {
					return store.BlockTaskCreationOperation(ctx, lease, "EXPIRED", "lease expired")
				},
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
				params := acquireParams(id, userID, "expiry-write-worker-"+testCase.name)
				params.LeaseDuration = 350 * time.Millisecond
				op, err := st.AcquireTaskCreationOperation(ctx, params)
				if err != nil {
					t.Fatal(err)
				}
				locker, err := st.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer rollbackTaskCreationTransaction(ctx, locker)
				var locked string
				if err := locker.QueryRow(ctx,
					`SELECT id FROM task_creation_operations WHERE id=$1 FOR UPDATE`, id,
				).Scan(&locked); err != nil {
					t.Fatal(err)
				}
				outcome := make(chan error, 1)
				go func() { outcome <- testCase.call(st2, op.Lease()) }()
				time.Sleep(600 * time.Millisecond)
				if err := locker.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				if err := <-outcome; !errors.Is(err, types.ErrTaskCreationLeaseLost) {
					t.Fatalf("锁等待跨 expiry 后写应 LeaseLost，实际 %v", err)
				}
				loaded, err := st.LoadTaskCreationOperation(ctx, id, 1, userID)
				if err != nil {
					t.Fatal(err)
				}
				if loaded.Status != types.TaskOperationStatusExecuting || loaded.TombstonedAt != nil {
					t.Fatalf("过期写产生副作用: %+v", loaded)
				}
			})
		}
	})

	t.Run("scope工具版本到期和终态均failClosed", func(t *testing.T) {
		id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		paddedOwner := acquireParams(id, userID, " padded-owner ")
		if _, err := st.AcquireTaskCreationOperation(ctx, paddedOwner); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("带首尾空格 owner 应 Validation，实际 %v", err)
		}
		wrongTenant := acquireParams(id, userID, "scope-worker")
		wrongTenant.TenantID = 999_999_999
		if _, err := st.AcquireTaskCreationOperation(ctx, wrongTenant); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("跨 tenant 应 NotFound，实际 %v", err)
		}
		wrongUser := acquireParams(id, otherUserID, "scope-worker")
		if _, err := st.AcquireTaskCreationOperation(ctx, wrongUser); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("跨 user 应 NotFound，实际 %v", err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations SET expires_at=now()-interval '1 second' WHERE id=$1`, id,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := st.AcquireTaskCreationOperation(ctx,
			acquireParams(id, userID, "scope-worker")); !errors.Is(err, types.ErrTaskCreationTerminal) {
			t.Fatalf("已到期 v1 应 Terminal，实际 %v", err)
		}
		var (
			status     types.TaskOperationStatus
			phase      types.TaskCreationPhase
			tombstoned bool
		)
		if err := st.pool.QueryRow(ctx,
			`SELECT status, phase, tombstoned_at IS NOT NULL
			   FROM task_creation_operations WHERE id=$1`, id,
		).Scan(&status, &phase, &tombstoned); err != nil {
			t.Fatal(err)
		}
		if status != types.TaskOperationStatusExpired ||
			phase != types.TaskCreationPhaseExpired || !tombstoned {
			t.Fatalf("expiry 必须耐久 tombstone，status=%q phase=%q tombstone=%v",
				status, phase, tombstoned)
		}
	})

	t.Run("不可变检查点同字节幂等异字节冲突", func(t *testing.T) {
		id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		op, err := st.AcquireTaskCreationOperation(ctx, acquireParams(id, userID, "checkpoint-worker"))
		if err != nil {
			t.Fatal(err)
		}
		lease := op.Lease()
		if err := st.CheckpointTaskCreationEnsureReceipt(ctx, lease, []byte(`{"ok":true}`), " padded "); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("带首尾空格 taskID 应 Validation，实际 %v", err)
		}

		command := []byte("{\n  \"query\": \"AI news\"\n}")
		if err := st.SealTaskCreationCommand(ctx, lease, command); err != nil {
			t.Fatal(err)
		}
		if err := st.SealTaskCreationCommand(ctx, lease, command); err != nil {
			t.Fatalf("command 同字节重放失败: %v", err)
		}
		if err := st.SealTaskCreationCommand(ctx, lease, []byte(`{"query":"different"}`)); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("command 异字节应 Conflict，实际 %v", err)
		}

		started, err := st.BeginTaskCreationTranslation(ctx, lease)
		if err != nil || !started {
			t.Fatalf("首次 BeginTranslation 应授权: started=%v err=%v", started, err)
		}
		started, err = st.BeginTaskCreationTranslation(ctx, lease)
		if err != nil || started {
			t.Fatalf("响应丢失重放不得二次授权: started=%v err=%v", started, err)
		}

		definition := []byte("{\n  \"fetch_plan\": {\"sources\":[{\"url\":\"https://example.com\"}]}\n}")
		digest := digestOf(definition)
		if err := st.CheckpointTaskCreationDefinition(ctx, lease, definition, digestOf([]byte("other"))); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("首次 definition digest 与 bytes 不符应 Validation，实际 %v", err)
		}
		if err := st.CheckpointTaskCreationDefinition(ctx, lease, definition, digest); err != nil {
			t.Fatal(err)
		}
		if err := st.CheckpointTaskCreationDefinition(ctx, lease, definition, digest); err != nil {
			t.Fatalf("definition 同字节重放失败: %v", err)
		}
		if err := st.CheckpointTaskCreationDefinition(ctx, lease, append([]byte(nil), definition...), digestOf([]byte("other"))); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("definition digest 自身不匹配应 Validation，实际 %v", err)
		}
		differentDefinition := []byte(`{"different":true}`)
		if err := st.CheckpointTaskCreationDefinition(ctx, lease, differentDefinition, digestOf(differentDefinition)); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("definition 异 bytes 应 Conflict，实际 %v", err)
		}

		prepared := []byte("{\n  \"request_id\": \"raw-token\"\n}")
		if err := st.CheckpointTaskCreationSchedule(ctx, lease, prepared); err != nil {
			t.Fatal(err)
		}
		if err := st.CheckpointTaskCreationSchedule(ctx, lease, prepared); err != nil {
			t.Fatalf("schedule 同字节重放失败: %v", err)
		}
		if err := st.CheckpointTaskCreationSchedule(ctx, lease, []byte(`{"request_id":"other"}`)); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("schedule 异字节应 Conflict，实际 %v", err)
		}

		receipt := []byte(`{"schedule_id":"vane-task-test","paused":true}`)
		if err := st.CheckpointTaskCreationEnsureReceipt(ctx, lease, receipt, "vane-task-test"); err != nil {
			t.Fatal(err)
		}
		if err := st.CheckpointTaskCreationEnsureReceipt(ctx, lease, receipt, "vane-task-test"); err != nil {
			t.Fatalf("receipt 同字节重放失败: %v", err)
		}
		if err := st.CheckpointTaskCreationEnsureReceipt(ctx, lease, receipt, "vane-task-other"); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("receipt 异 taskID 应 Conflict，实际 %v", err)
		}

		if err := st.RenewTaskCreationLease(ctx, lease, 3*time.Minute); err != nil {
			t.Fatalf("活 lease renew 失败: %v", err)
		}
		loaded, err := st.LoadTaskCreationOperation(ctx, id, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Phase != types.TaskCreationPhaseScheduleEnsured ||
			!bytesEqual(loaded.NormalizedCommand, command) ||
			!bytesEqual(loaded.CompiledDefinition, definition) ||
			loaded.CompiledDigest != digest ||
			!bytesEqual(loaded.PreparedSchedule, prepared) ||
			!bytesEqual(loaded.EnsureReceipt, receipt) || loaded.TaskID != "vane-task-test" {
			t.Fatalf("checkpoint 回读不一致: %+v", loaded)
		}
	})

	t.Run("终态tombstone不可复活且不进入stale扫描", func(t *testing.T) {
		completeID := checkpointedOperation(t, st, userID, "complete-worker")
		complete, err := st.LoadTaskCreationOperation(ctx, completeID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		lease := complete.Lease()
		result := json.RawMessage(`{"message":"ok","count":9007199254740992}`)
		if err := st.CompleteTaskCreationOperation(ctx, lease, "task-prepared", result); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("schedule_ensured 不得越过 DB/Activate 直接 Complete，实际 %v", err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations SET phase=$2 WHERE id=$1`,
			completeID, types.TaskCreationPhaseActivated,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO schedules
			    (id, tenant_id, user_id, spec_json, scope_json, status)
			 VALUES ('task-prepared', 1, $1, '{}', '{}', 'active')`,
			userID); err != nil {
			t.Fatal(err)
		}
		if err := st.CompleteTaskCreationOperation(ctx, lease, "task-prepared", result); err != nil {
			t.Fatal(err)
		}
		// 模拟终态 COMMIT 成功、响应丢失：同 task/result（JSON 空白/键序
		// 可不同）exact-adopt；任何不同结果或 taskID 都是 immutable conflict。
		if err := st.CompleteTaskCreationOperation(ctx, lease, "task-prepared",
			json.RawMessage(`{ "count": 9007199254740992, "message": "ok" }`)); err != nil {
			t.Fatalf("Complete 同结果重放应 exact-adopt: %v", err)
		}
		if err := st.CompleteTaskCreationOperation(ctx, lease, "task-prepared",
			json.RawMessage(`{"message":"ok","count":9007199254740993}`)); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("Complete 相邻大整数异结果应 Conflict，实际 %v", err)
		}
		if err := st.CompleteTaskCreationOperation(ctx, lease, "different-task", result); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("Complete 异 taskID 应 Conflict，实际 %v", err)
		}
		completed, err := st.LoadTaskCreationOperation(ctx, completeID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Status != types.TaskOperationStatusExecuted ||
			completed.Phase != types.TaskCreationPhaseCompleted || completed.TombstonedAt == nil ||
			completed.ExecutedAt == nil || completed.LeaseUntil != nil ||
			completed.TakeoverNotBefore != nil || completed.TaskID != "task-prepared" {
			t.Fatalf("完成 tombstone 不完整: %+v", completed)
		}
		assertTaskCreationReceiptExactlyOne(t, st, completeID)
		if _, err := st2.AcquireTaskCreationOperation(ctx,
			acquireParams(completeID, userID, "resurrect-worker")); !errors.Is(err, types.ErrTaskCreationTerminal) {
			t.Fatalf("completed 不得重领，实际 %v", err)
		}
		if err := st.RenewTaskCreationLease(ctx, lease, time.Minute); !errors.Is(err, types.ErrTaskCreationTerminal) {
			t.Fatalf("terminal renew 应 Terminal，实际 %v", err)
		}

		blockedID := preparedOperation(t, st, userID, "block-worker")
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations SET task_id='reserved-before-side-effect' WHERE id=$1`, blockedID,
		); err != nil {
			t.Fatal(err)
		}
		blocked, err := st.LoadTaskCreationOperation(ctx, blockedID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, blocked.Lease(), "reserved-before-side-effect",
			"TRANSLATION_AMBIGUOUS", "manual retry required"); err != nil {
			t.Fatal(err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, blocked.Lease(), "reserved-before-side-effect",
			"TRANSLATION_AMBIGUOUS", "manual retry required"); err != nil {
			t.Fatalf("Block 同结果重放应 exact-adopt: %v", err)
		}
		if err := st.BlockTaskCreationOperationAfterSideEffect(
			ctx, blocked.Lease(), "reserved-before-side-effect",
			"OTHER", "manual retry required"); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("Block 异结果应 Conflict，实际 %v", err)
		}
		blockedFinal, err := st.LoadTaskCreationOperation(ctx, blockedID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if blockedFinal.TaskID != "reserved-before-side-effect" {
			t.Fatalf("Block 覆盖了 ensure 的 immutable taskID: %+v", blockedFinal)
		}
		assertTaskCreationReceiptExactlyOne(t, st, blockedID)
		if _, err := st.AcquireTaskCreationOperation(ctx,
			acquireParams(blockedID, userID, "resurrect-worker")); !errors.Is(err, types.ErrTaskCreationTerminal) {
			t.Fatalf("blocked 不得重领，实际 %v", err)
		}

		failedID := preparedOperation(t, st, userID, "fail-worker")
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET task_id='reserved-before-side-effect', phase='definition_compiled',
			        prepared_schedule=NULL
			  WHERE id=$1`, failedID,
		); err != nil {
			t.Fatal(err)
		}
		failed, err := st.LoadTaskCreationOperation(ctx, failedID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.FailTaskCreationOperation(ctx, failed.Lease(), "INVALID_DEFINITION", "definition rejected"); err != nil {
			t.Fatal(err)
		}
		failedFinal, err := st.LoadTaskCreationOperation(ctx, failedID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if failedFinal.TaskID != "reserved-before-side-effect" {
			t.Fatalf("Fail 覆盖了 ensure 的 immutable taskID: %+v", failedFinal)
		}
		assertTaskCreationReceiptExactlyOne(t, st, failedID)

		sideEffectID := checkpointedOperation(t, st, userID, "cleanup-required-worker")
		sideEffect, err := st.LoadTaskCreationOperation(ctx, sideEffectID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.BlockTaskCreationOperation(ctx, sideEffect.Lease(), "LATE_FAILURE", "cleanup required"); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("schedule_ensured 后 Block 必须拒绝并保留恢复入口，实际 %v", err)
		}
		sideEffectAfter, err := st.LoadTaskCreationOperation(ctx, sideEffectID, 1, userID)
		if err != nil {
			t.Fatal(err)
		}
		if sideEffectAfter.Status != types.TaskOperationStatusExecuting || sideEffectAfter.TombstonedAt != nil {
			t.Fatalf("有副作用 operation 被错误 tombstone: %+v", sideEffectAfter)
		}

		incompleteID := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		incomplete, err := st.AcquireTaskCreationOperation(ctx, acquireParams(incompleteID, userID, "incomplete-terminal-worker"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET status='executed', phase='completed', tombstoned_at=clock_timestamp(),
			        lease_until=NULL, takeover_not_before=NULL, executed_at=NULL,
			        task_id='task-incomplete', result='{"ok":true}'::jsonb,
			        error_code='', error_message=''
			  WHERE id=$1`, incompleteID,
		); err != nil {
			t.Fatal(err)
		}
		if err := st.CompleteTaskCreationOperation(ctx, incomplete.Lease(), "task-incomplete", json.RawMessage(`{"ok":true}`)); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("executed_at 为空的残缺终态不得 exact-adopt，实际 %v", err)
		}

		stale, err := st.ListStaleTaskCreationOperations(ctx, 1, time.Now().Add(24*time.Hour), 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{completeID, blockedID, failedID} {
			assertOperationAbsent(t, stale, id)
		}
	})

	t.Run("stale扫描只返回同租户可恢复v1 executing", func(t *testing.T) {
		staleID := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		staleOp, err := st.AcquireTaskCreationOperation(ctx, acquireParams(staleID, userID, "stale-worker"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET lease_until=clock_timestamp()-interval '2 minutes',
			        takeover_not_before=clock_timestamp()-interval '1 minute'
			  WHERE id=$1`, staleID,
		); err != nil {
			t.Fatal(err)
		}
		activeID := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		if _, err := st.AcquireTaskCreationOperation(ctx, acquireParams(activeID, userID, "active-worker")); err != nil {
			t.Fatal(err)
		}
		graceID := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		if _, err := st.AcquireTaskCreationOperation(ctx, acquireParams(graceID, userID, "grace-worker")); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET lease_until=clock_timestamp()-interval '1 minute',
			        takeover_not_before=clock_timestamp()+interval '1 minute'
			  WHERE id=$1`, graceID,
		); err != nil {
			t.Fatal(err)
		}
		cleanupID := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
		if _, err := st.AcquireTaskCreationOperation(ctx, acquireParams(cleanupID, userID, "cleanup-worker")); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`UPDATE task_creation_operations
			    SET phase=$2,
			        lease_until=clock_timestamp()-interval '2 minutes',
			        takeover_not_before=clock_timestamp()-interval '1 minute'
			  WHERE id=$1`, cleanupID, types.TaskCreationPhaseCleanupPending,
		); err != nil {
			t.Fatal(err)
		}
		// 恶意/错误 caller 即使把 before 传到未来，也不能越过数据库当前时间
		// 和持久化的 takeover safety boundary。
		operations, err := st2.ListStaleTaskCreationOperations(ctx, 1, time.Now().Add(24*time.Hour), 1000)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(operations))
		for _, op := range operations {
			ids = append(ids, op.ID)
		}
		sort.Strings(ids)
		if !containsString(ids, staleID) {
			t.Fatalf("stale v1 executing 未被扫描: %v", ids)
		}
		if !containsString(ids, cleanupID) {
			t.Fatalf("cleanup_pending 必须保留恢复入口: %v", ids)
		}
		for _, forbidden := range []string{activeID, graceID} {
			if containsString(ids, forbidden) {
				t.Fatalf("扫描包含禁入行 %s: %v", forbidden, ids)
			}
		}
		if staleOp.Fence != 1 {
			t.Fatalf("fixture 未真实经历领取，fence=%d", staleOp.Fence)
		}
	})
}

func insertTaskCreationTestAction(
	t *testing.T,
	st *Store,
	userID int64,
	toolName string,
	executionVersion int16,
) string {
	t.Helper()
	id := uuid.NewString()
	if executionVersion != types.TaskCreationExecutionVersionV1 {
		t.Fatalf("current fixture only supports task creation v1, got %d", executionVersion)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO task_creation_operations
			(id, tenant_id, user_id, tool_name, args, summary, status,
			 expires_at, execution_version)
		 SELECT $1, tenant_id, user_id, $2, $3, '测试任务', 'pending',
		        clock_timestamp()+interval '24 hours', $4
		   FROM memberships
		  WHERE user_id=$5
		  ORDER BY tenant_id
		  LIMIT 1`,
		id, toolName,
		json.RawMessage(`{"spec":{"cron":"0 8 * * *","tz":"Asia/Shanghai"}}`),
		executionVersion, userID,
	); err != nil {
		t.Fatalf("insert current task creation operation: %v", err)
	}
	return id
}

func acquireParams(id string, userID int64, owner string) types.AcquireTaskCreationOperationParams {
	return types.AcquireTaskCreationOperationParams{
		ID:              id,
		TenantID:        1,
		UserID:          userID,
		LeaseOwner:      owner,
		LeaseDuration:   5 * time.Minute,
		ReceiptProvider: "feishu_message_patch",
		ReceiptTarget:   "om-test-" + id,
	}
}

func checkpointedOperation(t *testing.T, st *Store, userID int64, owner string) string {
	t.Helper()
	id := preparedOperation(t, st, userID, owner)
	op, err := st.LoadTaskCreationOperation(t.Context(), id, 1, userID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CheckpointTaskCreationEnsureReceipt(t.Context(), op.Lease(), []byte(`{"ensured":true}`), "task-prepared"); err != nil {
		t.Fatal(err)
	}
	return id
}

func preparedOperation(t *testing.T, st *Store, userID int64, owner string) string {
	t.Helper()
	id := insertTaskCreationTestAction(t, st, userID, "create_schedule", 1)
	op, err := st.AcquireTaskCreationOperation(t.Context(), acquireParams(id, userID, owner))
	if err != nil {
		t.Fatal(err)
	}
	lease := op.Lease()
	if err := st.SealTaskCreationCommand(t.Context(), lease, []byte(`{"query":"test"}`)); err != nil {
		t.Fatal(err)
	}
	started, err := st.BeginTaskCreationTranslation(t.Context(), lease)
	if err != nil || !started {
		t.Fatalf("BeginTranslation: started=%v err=%v", started, err)
	}
	definition := []byte(`{"fetch_plan":{"sources":[{"url":"https://example.com"}]}}`)
	if err := st.CheckpointTaskCreationDefinition(t.Context(), lease, definition, digestOf(definition)); err != nil {
		t.Fatal(err)
	}
	if err := st.CheckpointTaskCreationSchedule(t.Context(), lease, []byte(`{"prepared":true}`)); err != nil {
		t.Fatal(err)
	}
	return id
}

func digestOf(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func assertOperationAbsent(t *testing.T, operations []types.TaskCreationOperation, id string) {
	t.Helper()
	for _, op := range operations {
		if op.ID == id {
			t.Fatalf("operation %s 不应出现在 stale 扫描", id)
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
