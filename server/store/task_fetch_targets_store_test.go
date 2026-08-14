package store

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

// TestScheduleSourcesStore 是 DATABASE_URL 门控的集成测试（P1b b2）：GetOrCreateFetchTarget 的
// 建新/命中不覆写、ReplaceTaskFetchTargets 的增删/归属门禁/清空、删任务 CASCADE 带走链接。
func TestTaskFetchTargetsStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	registerStoreClose(t, st)

	u, err := st.UpsertUserByOpenID(ctx, "test_schedsrc_"+uuid.NewString(), "schedsrc-owner")
	if err != nil {
		t.Fatalf("建 owner 失败: %v", err)
	}
	attachTenant(t, st, u.ID) // #76 后 schedules.tenant_id NOT NULL：InsertSchedule 需用户有租户归属
	u2, err := st.UpsertUserByOpenID(ctx, "test_schedsrc_stranger_"+uuid.NewString(), "schedsrc-stranger")
	if err != nil {
		t.Fatalf("建 stranger 失败: %v", err)
	}
	attachTenant(t, st, u2.ID)
	schedID := "push-schedsrc-" + uuid.NewString()
	if err := st.InsertSchedule(ctx, &types.Schedule{
		ID: schedID, UserID: u.ID,
		SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
		Status: types.ScheduleStatusActive,
	}); err != nil {
		t.Fatalf("InsertSchedule 失败: %v", err)
	}

	// 三个源（url 唯一）：分别用于链接增删。
	mkSource := func(t *testing.T, cfg string) int64 {
		t.Helper()
		id, _, err := st.GetOrCreateFetchTarget(ctx, &types.FetchTarget{
			Platform: types.PlatformWeb, Capability: types.CapSearch,
			URL: "vane://web/search?q=" + uuid.NewString(), Config: json.RawMessage(cfg),
		})
		if err != nil {
			t.Fatalf("GetOrCreateFetchTarget 失败: %v", err)
		}
		return id
	}
	s1 := mkSource(t, `{"query":"a"}`)
	s2 := mkSource(t, `{"query":"b"}`)
	s3 := mkSource(t, `{"query":"c"}`)

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM task_fetch_targets WHERE schedule_id = $1`, schedID)
		cleanupExec(ctx, t, st, `DELETE FROM schedules WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM fetch_targets WHERE id = ANY($1)`, []int64{s1, s2, s3})
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = ANY($1)`, []int64{u.ID, u2.ID})
	})

	t.Run("GetOrCreateFetchTarget 拒绝同 URL 不同抓取语义", func(t *testing.T) {
		url := "vane://web/search?q=" + uuid.NewString()
		id1, created1, err := st.GetOrCreateFetchTarget(ctx, &types.FetchTarget{
			Platform: types.PlatformWeb, Capability: types.CapSearch, URL: url, Config: json.RawMessage(`{"query":"orig"}`),
		})
		if err != nil || !created1 {
			t.Fatalf("首次应建新: created=%v err=%v", created1, err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM fetch_targets WHERE id = $1`, id1)
		// 同 URL 再来但 config 不同：必须在进入任务计划前拒绝。
		id2, created2, err := st.GetOrCreateFetchTarget(ctx, &types.FetchTarget{
			Platform: types.PlatformWeb, Capability: types.CapSearch, URL: url, Config: json.RawMessage(`{"query":"CLOBBERED"}`),
		})
		if !errors.Is(err, types.ErrConflict) || created2 || id2 != 0 {
			t.Fatalf("语义冲突应 fail closed: created=%v id=%d err=%v", created2, id2, err)
		}
		var cfg string
		if err := st.pool.QueryRow(ctx, `SELECT config::text FROM fetch_targets WHERE id = $1`, id1).Scan(&cfg); err != nil {
			t.Fatalf("回查 config 失败: %v", err)
		}
		if cfg != `{"query": "orig"}` && cfg != `{"query":"orig"}` {
			t.Errorf("既有目标 config 不该被覆写，应仍为 orig，实得 %s", cfg)
		}
	})

	ids := func(t *testing.T) []int64 {
		t.Helper()
		got, err := st.ListTaskFetchTargetIDs(ctx, u.ID, schedID)
		if err != nil {
			t.Fatalf("ListTaskFetchTargetIDs 失败: %v", err)
		}
		return got
	}

	t.Run("链接 2 源 → 增到 3 → 减到 1", func(t *testing.T) {
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{s1, s2}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if got := ids(t); len(got) != 2 {
			t.Fatalf("应链接 2 源, 实得 %v", got)
		}
		// 换成 s1,s2,s3：新增 s3。
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{s1, s2, s3}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if got := ids(t); len(got) != 3 {
			t.Fatalf("应链接 3 源, 实得 %v", got)
		}
		// 换成只剩 s3：删 s1/s2。
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{s3}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if got := ids(t); len(got) != 1 || got[0] != s3 {
			t.Fatalf("应只剩 s3, 实得 %v", got)
		}
	})

	t.Run("空 sourceIDs 清空全部链接", func(t *testing.T) {
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{s1, s2}); err != nil {
			t.Fatalf("预置失败: %v", err)
		}
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, nil); err != nil {
			t.Fatalf("清空失败: %v", err)
		}
		if got := ids(t); len(got) != 0 {
			t.Fatalf("空 sourceIDs 应清光链接, 实得 %v", got)
		}
	})

	t.Run("归属门禁：非属主 Replace 不动链接", func(t *testing.T) {
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{s1, s2}); err != nil {
			t.Fatalf("预置失败: %v", err)
		}
		// stranger 尝试改本任务链接：SQL EXISTS 门禁应让删/插都是 0 行。
		if err := st.ReplaceTaskFetchTargets(ctx, u2.ID, schedID, []int64{s3}); err != nil {
			t.Fatalf("非属主不应报错: %v", err)
		}
		if got := ids(t); len(got) != 2 {
			t.Fatalf("非属主不得改写属主链接，应仍为 2, 实得 %v", got)
		}
	})

	t.Run("cascade：删任务带走链接", func(t *testing.T) {
		schedID2 := "push-schedsrc-cascade-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedID2, UserID: u.ID,
			SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID2, []int64{s1}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, schedID2); err != nil {
			t.Fatalf("删 schedule 失败: %v", err)
		}
		var exists bool
		if err := st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM task_fetch_targets WHERE schedule_id = $1)`, schedID2).Scan(&exists); err != nil {
			t.Fatalf("查 EXISTS 失败: %v", err)
		}
		if exists {
			t.Error("删 schedule 后 ON DELETE CASCADE 应带走其源链接")
		}
	})

	t.Run("cascade：删源带走链接（迁移 020 承诺的源侧级联）", func(t *testing.T) {
		sTmp := mkSource(t, `{"query":"tmp"}`)
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{sTmp}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `DELETE FROM fetch_targets WHERE id = $1`, sTmp); err != nil {
			t.Fatalf("删 source 失败: %v", err)
		}
		var exists bool
		if err := st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM task_fetch_targets WHERE fetch_target_id = $1)`, sTmp).Scan(&exists); err != nil {
			t.Fatalf("查 EXISTS 失败: %v", err)
		}
		if exists {
			t.Error("删 source 后 ON DELETE CASCADE 应带走引用它的链接（不留悬挂引用）")
		}
	})

	t.Run("b3 隔离：取材按任务隔离（只见本任务源的内容）+ 用户级去重", func(t *testing.T) {
		// schedID 绑 sA；另建 schedID2 绑 sB。内容 c 只经 sA 出现。
		sA := mkSource(t, `{"query":"A"}`)
		sB := mkSource(t, `{"query":"B"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM fetch_targets WHERE id = ANY($1)`, []int64{sA, sB})
		schedID2 := "push-schedsrc-iso-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedID2, UserID: u.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedID2)
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{sA}); err != nil {
			t.Fatalf("Replace A 失败: %v", err)
		}
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID2, []int64{sB}); err != nil {
			t.Fatalf("Replace B 失败: %v", err)
		}
		// 内容 c 经 sA 入库（登记 content_sources: c↔sA）。
		ck := "https://example.com/iso-" + uuid.NewString()
		cID, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID: sA, ExternalID: "ext-" + uuid.NewString(), CanonicalKey: ck,
			URL: ck, Title: "隔离测试内容", ContentHash: "h-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("UpsertContentItem 失败: %v", err)
		}
		defer func() {
			c, cancel := cleanupContext()
			defer cancel()
			cleanupExec(c, t, st, `DELETE FROM content_sources WHERE content_item_id = $1`, cID)
			cleanupExec(c, t, st, `DELETE FROM content_items WHERE id = $1`, cID)
		}()

		has := func(list []types.ContentItem, id int64) bool {
			for _, ci := range list {
				if ci.ID == id {
					return true
				}
			}
			return false
		}
		// 任务 A 看得到 c（它的源 sA 见过）。
		aList, err := st.ListUnpushedBySchedule(ctx, schedID, 50, 50)
		if err != nil {
			t.Fatalf("ListUnpushedBySchedule(A) 失败: %v", err)
		}
		if !has(aList, cID) {
			t.Errorf("任务 A 应看到自己源的内容 %d", cID)
		}
		// **取材隔离**：任务 B（绑 sB，没绑 sA）看不到 c。
		bList, err := st.ListUnpushedBySchedule(ctx, schedID2, 50, 50)
		if err != nil {
			t.Fatalf("ListUnpushedBySchedule(B) 失败: %v", err)
		}
		if has(bList, cID) {
			t.Errorf("任务 B 不该看到任务 A 源的内容 %d（取材隔离破了）", cID)
		}
		// 用户级去重（决策 A）：A 把 c 投了 → A 再也看不到 c（用户已读，任何路径都不再重推）。
		batchID, err := st.CreatePushBatchIdempotent(ctx, u.ID, "tr-iso-"+uuid.NewString(), schedID)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent 失败: %v", err)
		}
		if _, _, _, err := st.InsertDeliveryIdempotent(ctx, &types.Delivery{
			BatchID: batchID, UserID: u.ID, ContentItemID: &cID, Score: 80, BodyMD: "x",
		}); err != nil {
			t.Fatalf("InsertDeliveryIdempotent 失败: %v", err)
		}
		aList2, err := st.ListUnpushedBySchedule(ctx, schedID, 50, 50)
		if err != nil {
			t.Fatalf("ListUnpushedBySchedule(A 投递后) 失败: %v", err)
		}
		if has(aList2, cID) {
			t.Errorf("任务 A 投过 c 后不该再看到它（用户级去重：用户已读不重推）")
		}
		// ListDueFetchTargetsByTask 只返回本任务链接的、active+due 的源。
		due, err := st.ListDueFetchTargetsByTask(ctx, schedID)
		if err != nil {
			t.Fatalf("ListDueFetchTargetsByTask 失败: %v", err)
		}
		if len(due) != 1 || due[0].ID != sA {
			t.Errorf("任务 A 的到期源应恰为 sA=%d，实得 %+v", sA, due)
		}
	})

	t.Run("b3 去重用户级（决策 A）：共享源下 A 投过 B 也看不到——同一条永不重复轰炸用户", func(t *testing.T) {
		// 一个源 sShared 同时绑给 A(schedID) 和 B(schedID2)，内容 c 经它入库。
		sShared := mkSource(t, `{"query":"shared"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM fetch_targets WHERE id = $1`, sShared)
		schedB := "push-schedsrc-dedup-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedB, UserID: u.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedB)
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{sShared}); err != nil {
			t.Fatalf("Replace A: %v", err)
		}
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedB, []int64{sShared}); err != nil {
			t.Fatalf("Replace B: %v", err)
		}
		ck := "https://example.com/dedup-" + uuid.NewString()
		cID, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID: sShared, ExternalID: "ext-" + uuid.NewString(), CanonicalKey: ck, URL: ck,
			Title: "共享内容", ContentHash: "h-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("UpsertContentItem: %v", err)
		}
		defer func() {
			c, cancel := cleanupContext()
			defer cancel()
			cleanupExec(c, t, st, `DELETE FROM content_sources WHERE content_item_id = $1`, cID)
			cleanupExec(c, t, st, `DELETE FROM content_items WHERE id = $1`, cID)
		}()
		has := func(l []types.ContentItem, id int64) bool {
			for _, ci := range l {
				if ci.ID == id {
					return true
				}
			}
			return false
		}
		// 初始：A、B 都看得到 c（共享源，取材面都覆盖）。
		if a, _ := st.ListUnpushedBySchedule(ctx, schedID, 50, 50); !has(a, cID) {
			t.Fatal("初始 A 应看到共享内容")
		}
		if b, _ := st.ListUnpushedBySchedule(ctx, schedB, 50, 50); !has(b, cID) {
			t.Fatal("初始 B 应看到共享内容")
		}
		// A 投递 c（A 的 batch）。
		batchA, err := st.CreatePushBatchIdempotent(ctx, u.ID, "tr-dedupA-"+uuid.NewString(), schedID)
		if err != nil {
			t.Fatalf("建 A 批次: %v", err)
		}
		if _, _, _, err := st.InsertDeliveryIdempotent(ctx, &types.Delivery{
			BatchID: batchA, UserID: u.ID, ContentItemID: &cID, Score: 80, BodyMD: "x",
		}); err != nil {
			t.Fatalf("A 投递: %v", err)
		}
		// **用户级去重的关键（决策 A）**：A 投过 → 用户已读 → A 看不到，**B 也看不到**。
		// 代价（已知、接受）：共享源的两个任务不再各推一遍同一条。
		if a, _ := st.ListUnpushedBySchedule(ctx, schedID, 50, 50); has(a, cID) {
			t.Error("A 投过 c 后不该再看到（用户级去重）")
		}
		if b, _ := st.ListUnpushedBySchedule(ctx, schedB, 50, 50); has(b, cID) {
			t.Error("A 投过的 c 用户已读，B 也不该再看到（决策 A：同一条永不重复轰炸用户）")
		}

		// 多用户隔离：u 的投递只按属主去重，不抑制**别的用户**的任务候选
		//（user_id 反查谓词的定向用例——谓词写错成固定用户或丢失时这里立刻红）。
		schedU2 := "push-schedsrc-dedup-u2-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedU2, UserID: u2.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule u2 失败: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedU2)
		if err := st.ReplaceTaskFetchTargets(ctx, u2.ID, schedU2, []int64{sShared}); err != nil {
			t.Fatalf("Replace u2: %v", err)
		}
		if l, _ := st.ListUnpushedBySchedule(ctx, schedU2, 50, 50); !has(l, cID) {
			t.Error("u 投过 c 不该影响 u2 的任务候选（去重按属主，不是全局）")
		}

		// 决策 A 的直接动机：全局推送（push_now，NULL schedule_id 批次）投过的内容，
		// 隔离任务不得重推——转隔离首日 47 条候选里 40 条是用户已读的事故形状。
		batchNull, err := st.CreatePushBatchIdempotent(ctx, u.ID, "tr-null-"+uuid.NewString(), "") // schedule_id=NULL
		if err != nil {
			t.Fatalf("建 NULL 批次: %v", err)
		}
		var isNull bool
		if err := st.pool.QueryRow(ctx, `SELECT schedule_id IS NULL FROM push_batches WHERE id = $1`, batchNull).Scan(&isNull); err != nil || !isNull {
			t.Fatalf("push_now 批次 schedule_id 应为 NULL, isNull=%v err=%v", isNull, err)
		}
		// 造第二条内容 c2 经共享源，用 NULL 批次投它。
		ck2 := "https://example.com/dedup2-" + uuid.NewString()
		c2ID, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID: sShared, ExternalID: "ext-" + uuid.NewString(), CanonicalKey: ck2, URL: ck2,
			Title: "共享内容2", ContentHash: "h2-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("UpsertContentItem c2: %v", err)
		}
		defer func() {
			c, cancel := cleanupContext()
			defer cancel()
			cleanupExec(c, t, st, `DELETE FROM content_sources WHERE content_item_id = $1`, c2ID)
			cleanupExec(c, t, st, `DELETE FROM content_items WHERE id = $1`, c2ID)
		}()
		if _, _, _, err := st.InsertDeliveryIdempotent(ctx, &types.Delivery{
			BatchID: batchNull, UserID: u.ID, ContentItemID: &c2ID, Score: 70, BodyMD: "y",
		}); err != nil {
			t.Fatalf("NULL 批次投递: %v", err)
		}
		// push_now 全局推送投过 c2 = 用户已读 → B（schedule 任务）不得再把 c2 当候选。
		if b, _ := st.ListUnpushedBySchedule(ctx, schedB, 50, 50); has(b, c2ID) {
			t.Error("push_now 全局推送投过的 c2 用户已读，B 不该再看到（决策 A 的核心场景）")
		}
	})

	t.Run("ListDueFetchTargetsByTask 排除 disabled 与未到期源", func(t *testing.T) {
		sDis := mkSource(t, `{"q":"dis"}`)
		sFuture := mkSource(t, `{"q":"future"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM fetch_targets WHERE id = ANY($1)`, []int64{sDis, sFuture})
		schedC := "push-schedsrc-due-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedC, UserID: u.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedC)
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedC, []int64{sDis, sFuture}); err != nil {
			t.Fatalf("Replace: %v", err)
		}
		// sDis 停用、sFuture 未到期。
		if _, err := st.pool.Exec(ctx, `UPDATE fetch_targets SET status = $2 WHERE id = $1`, sDis, types.FetchTargetStatusDisabled); err != nil {
			t.Fatalf("置 disabled: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `UPDATE fetch_targets SET next_fetch_at = now() + interval '1 hour' WHERE id = $1`, sFuture); err != nil {
			t.Fatalf("置未到期: %v", err)
		}
		due, err := st.ListDueFetchTargetsByTask(ctx, schedC)
		if err != nil {
			t.Fatalf("ListDueFetchTargetsByTask: %v", err)
		}
		if len(due) != 0 {
			t.Errorf("disabled 与未到期源都应被排除（重复计费护栏），实得 %+v", due)
		}
	})

	t.Run("重新批准任务目标会恢复自动暂停", func(t *testing.T) {
		targetID := mkSource(t, `{"q":"recover"}`)
		defer cleanupExec(
			t.Context(), t, st,
			`DELETE FROM fetch_targets WHERE id = $1`, targetID,
		)
		if _, err := st.pool.Exec(ctx,
			`UPDATE fetch_targets
			    SET status=$2,fail_count=12,next_fetch_at=now()+interval '1 day'
			  WHERE id=$1`,
			targetID, types.FetchTargetStatusDisabled,
		); err != nil {
			t.Fatal(err)
		}
		if err := st.ReplaceTaskFetchTargets(
			ctx, u.ID, schedID, []int64{targetID},
		); err != nil {
			t.Fatalf("ReplaceTaskFetchTargets: %v", err)
		}
		var status types.FetchTargetStatus
		var failCount int
		if err := st.pool.QueryRow(ctx,
			`SELECT status,fail_count FROM fetch_targets WHERE id=$1`,
			targetID,
		).Scan(&status, &failCount); err != nil {
			t.Fatal(err)
		}
		if status != types.FetchTargetStatusActive || failCount != 0 {
			t.Fatalf("status/fail_count=%s/%d, want active/0",
				status, failCount)
		}
	})

	t.Run("TaskFetchTargetForContent 取任务命中源而非全局首发源（#8）", func(t *testing.T) {
		// 本任务只绑 s1（任务源）；s2、s3 是非任务源。
		if err := st.ReplaceTaskFetchTargets(ctx, u.ID, schedID, []int64{s1}); err != nil {
			t.Fatalf("Replace [s1] 失败: %v", err)
		}
		// 内容 c 先经 s2 入库（首发源=s2，非任务），再经 s1 登记 → content_sources: c↔{s2,s1}。
		ck := "https://example.com/attr-" + uuid.NewString()
		cID, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID: s2, ExternalID: "ext-" + uuid.NewString(), CanonicalKey: ck,
			URL: ck, Title: "归属测试", ContentHash: "h-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("Upsert(c via s2) 失败: %v", err)
		}
		if _, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID: s1, ExternalID: "ext2-" + uuid.NewString(), CanonicalKey: ck,
			URL: ck, Title: "归属测试", ContentHash: "h2-" + uuid.NewString(),
		}); err != nil {
			t.Fatalf("Upsert(c via s1) 失败: %v", err)
		}
		// 另一内容 c2 只经 s3（非任务源）入库——与本任务源无交集。
		ck2 := "https://example.com/attr-nomatch-" + uuid.NewString()
		c2ID, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID: s3, ExternalID: "ext3-" + uuid.NewString(), CanonicalKey: ck2,
			URL: ck2, Title: "无交集", ContentHash: "h3-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("Upsert(c2 via s3) 失败: %v", err)
		}
		defer func() {
			c, cancel := cleanupContext()
			defer cancel()
			cleanupExec(c, t, st, `DELETE FROM content_sources WHERE content_item_id = ANY($1)`, []int64{cID, c2ID})
			cleanupExec(c, t, st, `DELETE FROM content_items WHERE id = ANY($1)`, []int64{cID, c2ID})
		}()

		// 命中：c 经任务源 s1 出现 → 返回 s1，而非全局首发源 s2。
		sid, ok, err := st.TaskFetchTargetForContent(ctx, cID, schedID)
		if err != nil {
			t.Fatalf("TaskFetchTargetForContent(c) 失败: %v", err)
		}
		if !ok || sid != s1 {
			t.Errorf("应返回任务命中源 s1=%d（非首发源 s2=%d），实得 ok=%v sid=%d", s1, s2, ok, sid)
		}
		// 无交集：c2 只在非任务源 s3 下 → (0,false)，调用方回退首发源。
		if sid, ok, err := st.TaskFetchTargetForContent(ctx, c2ID, schedID); err != nil || ok || sid != 0 {
			t.Errorf("无交集应返回 (0,false)，实得 sid=%d ok=%v err=%v", sid, ok, err)
		}
	})
}
