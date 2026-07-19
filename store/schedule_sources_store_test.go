package store

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestScheduleSourcesStore 是 DATABASE_URL 门控的集成测试（P1b b2）：GetOrCreateSource 的
// 建新/命中不覆写、ReplaceScheduleSources 的增删/归属门禁/清空、删任务 CASCADE 带走链接。
func TestScheduleSourcesStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 schedule_sources 集成测试")
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
		id, _, err := st.GetOrCreateSource(ctx, &types.Source{
			Platform: types.PlatformWeb, Capability: types.CapSearch,
			URL: "vane://web/search?q=" + uuid.NewString(), Config: json.RawMessage(cfg),
		})
		if err != nil {
			t.Fatalf("GetOrCreateSource 失败: %v", err)
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
		cleanupExec(ctx, t, st, `DELETE FROM schedule_sources WHERE schedule_id = $1`, schedID)
		cleanupExec(ctx, t, st, `DELETE FROM schedules WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = ANY($1)`, []int64{s1, s2, s3})
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = ANY($1)`, []int64{u.ID, u2.ID})
	})

	t.Run("GetOrCreateSource 命中既有不覆写 config", func(t *testing.T) {
		url := "vane://web/search?q=" + uuid.NewString()
		id1, created1, err := st.GetOrCreateSource(ctx, &types.Source{
			Platform: types.PlatformWeb, Capability: types.CapSearch, URL: url, Config: json.RawMessage(`{"query":"orig"}`),
		})
		if err != nil || !created1 {
			t.Fatalf("首次应建新: created=%v err=%v", created1, err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM sources WHERE id = $1`, id1)
		// 同 url 再来，config 不同：应返回同 id、created=false，且**既有 config 不被改**。
		id2, created2, err := st.GetOrCreateSource(ctx, &types.Source{
			Platform: types.PlatformWeb, Capability: types.CapSearch, URL: url, Config: json.RawMessage(`{"query":"CLOBBERED"}`),
		})
		if err != nil {
			t.Fatalf("二次失败: %v", err)
		}
		if created2 || id2 != id1 {
			t.Fatalf("命中既有应 created=false 且同 id: created=%v id1=%d id2=%d", created2, id1, id2)
		}
		var cfg string
		if err := st.pool.QueryRow(ctx, `SELECT config::text FROM sources WHERE id = $1`, id1).Scan(&cfg); err != nil {
			t.Fatalf("回查 config 失败: %v", err)
		}
		if cfg != `{"query": "orig"}` && cfg != `{"query":"orig"}` {
			t.Errorf("既有源 config 不该被覆写，应仍为 orig，实得 %s", cfg)
		}
	})

	ids := func(t *testing.T) []int64 {
		t.Helper()
		got, err := st.ListScheduleSourceIDs(ctx, u.ID, schedID)
		if err != nil {
			t.Fatalf("ListScheduleSourceIDs 失败: %v", err)
		}
		return got
	}

	t.Run("链接 2 源 → 增到 3 → 减到 1", func(t *testing.T) {
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{s1, s2}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if got := ids(t); len(got) != 2 {
			t.Fatalf("应链接 2 源, 实得 %v", got)
		}
		// 换成 s1,s2,s3：新增 s3。
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{s1, s2, s3}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if got := ids(t); len(got) != 3 {
			t.Fatalf("应链接 3 源, 实得 %v", got)
		}
		// 换成只剩 s3：删 s1/s2。
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{s3}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if got := ids(t); len(got) != 1 || got[0] != s3 {
			t.Fatalf("应只剩 s3, 实得 %v", got)
		}
	})

	t.Run("空 sourceIDs 清空全部链接", func(t *testing.T) {
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{s1, s2}); err != nil {
			t.Fatalf("预置失败: %v", err)
		}
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, nil); err != nil {
			t.Fatalf("清空失败: %v", err)
		}
		if got := ids(t); len(got) != 0 {
			t.Fatalf("空 sourceIDs 应清光链接, 实得 %v", got)
		}
	})

	t.Run("归属门禁：非属主 Replace 不动链接", func(t *testing.T) {
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{s1, s2}); err != nil {
			t.Fatalf("预置失败: %v", err)
		}
		// stranger 尝试改本任务链接：SQL EXISTS 门禁应让删/插都是 0 行。
		if err := st.ReplaceScheduleSources(ctx, u2.ID, schedID, []int64{s3}); err != nil {
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
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID2, []int64{s1}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, schedID2); err != nil {
			t.Fatalf("删 schedule 失败: %v", err)
		}
		var exists bool
		if err := st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schedule_sources WHERE schedule_id = $1)`, schedID2).Scan(&exists); err != nil {
			t.Fatalf("查 EXISTS 失败: %v", err)
		}
		if exists {
			t.Error("删 schedule 后 ON DELETE CASCADE 应带走其源链接")
		}
	})

	t.Run("cascade：删源带走链接（迁移 020 承诺的源侧级联）", func(t *testing.T) {
		sTmp := mkSource(t, `{"query":"tmp"}`)
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{sTmp}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `DELETE FROM sources WHERE id = $1`, sTmp); err != nil {
			t.Fatalf("删 source 失败: %v", err)
		}
		var exists bool
		if err := st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schedule_sources WHERE source_id = $1)`, sTmp).Scan(&exists); err != nil {
			t.Fatalf("查 EXISTS 失败: %v", err)
		}
		if exists {
			t.Error("删 source 后 ON DELETE CASCADE 应带走引用它的链接（不留悬挂引用）")
		}
	})

	t.Run("b3 隔离：只见本任务源的内容 + 本任务独立投递账本 + 任务间互不干扰", func(t *testing.T) {
		// schedID 绑 sA；另建 schedID2 绑 sB。内容 c 只经 sA 出现。
		sA := mkSource(t, `{"query":"A"}`)
		sB := mkSource(t, `{"query":"B"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM sources WHERE id = ANY($1)`, []int64{sA, sB})
		schedID2 := "push-schedsrc-iso-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedID2, UserID: u.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedID2)
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{sA}); err != nil {
			t.Fatalf("Replace A 失败: %v", err)
		}
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID2, []int64{sB}); err != nil {
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
		// **互不干扰**：任务 B（绑 sB，没绑 sA）看不到 c。
		bList, err := st.ListUnpushedBySchedule(ctx, schedID2, 50, 50)
		if err != nil {
			t.Fatalf("ListUnpushedBySchedule(B) 失败: %v", err)
		}
		if has(bList, cID) {
			t.Errorf("任务 B 不该看到任务 A 源的内容 %d（互不干扰破了）", cID)
		}
		// 本任务独立投递账本：A 把 c 投了 → A 再也看不到 c，但 B 的可见性本就不含 c（不受影响）。
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
			t.Errorf("任务 A 投过 c 后不该再看到它（本任务投递账本）")
		}
		// ScheduleHasSources 分流开关。
		if ok, _ := st.ScheduleHasSources(ctx, schedID); !ok {
			t.Error("有链接的任务 ScheduleHasSources 应为 true")
		}
		if ok, _ := st.ScheduleHasSources(ctx, "push-nonexistent"); ok {
			t.Error("无链接任务 ScheduleHasSources 应为 false")
		}
		// ListDueSourcesBySchedule 只返回本任务链接的、active+due 的源。
		due, err := st.ListDueSourcesBySchedule(ctx, schedID)
		if err != nil {
			t.Fatalf("ListDueSourcesBySchedule 失败: %v", err)
		}
		if len(due) != 1 || due[0].ID != sA {
			t.Errorf("任务 A 的到期源应恰为 sA=%d，实得 %+v", sA, due)
		}
	})

	t.Run("b3 投递账本按 schedule 隔离：共享源下 A 投过 B 仍能看到（反证不是源隔离在起作用）", func(t *testing.T) {
		// 一个源 sShared 同时绑给 A(schedID) 和 B(schedID2)，内容 c 经它入库。
		sShared := mkSource(t, `{"query":"shared"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM sources WHERE id = $1`, sShared)
		schedB := "push-schedsrc-ledger-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedB, UserID: u.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedB)
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{sShared}); err != nil {
			t.Fatalf("Replace A: %v", err)
		}
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedB, []int64{sShared}); err != nil {
			t.Fatalf("Replace B: %v", err)
		}
		ck := "https://example.com/ledger-" + uuid.NewString()
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
		// 初始：A、B 都看得到 c（共享源）。
		if a, _ := st.ListUnpushedBySchedule(ctx, schedID, 50, 50); !has(a, cID) {
			t.Fatal("初始 A 应看到共享内容")
		}
		if b, _ := st.ListUnpushedBySchedule(ctx, schedB, 50, 50); !has(b, cID) {
			t.Fatal("初始 B 应看到共享内容")
		}
		// A 投递 c（A 的 batch）。
		batchA, err := st.CreatePushBatchIdempotent(ctx, u.ID, "tr-ledgerA-"+uuid.NewString(), schedID)
		if err != nil {
			t.Fatalf("建 A 批次: %v", err)
		}
		if _, _, _, err := st.InsertDeliveryIdempotent(ctx, &types.Delivery{
			BatchID: batchA, UserID: u.ID, ContentItemID: &cID, Score: 80, BodyMD: "x",
		}); err != nil {
			t.Fatalf("A 投递: %v", err)
		}
		// **账本隔离的关键**：A 投过 → A 看不到；但 **B 仍能看到**（B 有自己的账本，A 的投递与它无关）。
		if a, _ := st.ListUnpushedBySchedule(ctx, schedID, 50, 50); has(a, cID) {
			t.Error("A 投过 c 后不该再看到（本任务账本）")
		}
		if b, _ := st.ListUnpushedBySchedule(ctx, schedB, 50, 50); !has(b, cID) {
			t.Error("A 投的不该影响 B——B 仍应看到 c（证明账本按 schedule 而非 user 隔离）")
		}

		// b3 NULL schedule_id 历史批次不误命中：push_now（NULL 批次）投过 c，不该让 B 看不到 c。
		batchNull, err := st.CreatePushBatchIdempotent(ctx, u.ID, "tr-null-"+uuid.NewString(), "") // schedule_id=NULL
		if err != nil {
			t.Fatalf("建 NULL 批次: %v", err)
		}
		var isNull bool
		if err := st.pool.QueryRow(ctx, `SELECT schedule_id IS NULL FROM push_batches WHERE id = $1`, batchNull).Scan(&isNull); err != nil || !isNull {
			t.Fatalf("push_now 批次 schedule_id 应为 NULL, isNull=%v err=%v", isNull, err)
		}
		// 造第二条内容 c2 经共享源，用 NULL 批次投它。
		ck2 := "https://example.com/ledger2-" + uuid.NewString()
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
		// B 是 schedule 级任务：NULL 批次（push_now）投过 c2 不该让 B 看不到 c2（账本按 schedule_id 匹配，NULL≠B）。
		if b, _ := st.ListUnpushedBySchedule(ctx, schedB, 50, 50); !has(b, c2ID) {
			t.Error("push_now 的 NULL 批次投递不该命中 schedule 任务的账本，B 仍应看到 c2")
		}
	})

	t.Run("ListDueSourcesBySchedule 排除 disabled 与未到期源", func(t *testing.T) {
		sDis := mkSource(t, `{"q":"dis"}`)
		sFuture := mkSource(t, `{"q":"future"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM sources WHERE id = ANY($1)`, []int64{sDis, sFuture})
		schedC := "push-schedsrc-due-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedC, UserID: u.ID, SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule: %v", err)
		}
		defer cleanupExec(t.Context(), t, st, `DELETE FROM schedules WHERE id = $1`, schedC)
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedC, []int64{sDis, sFuture}); err != nil {
			t.Fatalf("Replace: %v", err)
		}
		// sDis 停用、sFuture 未到期。
		if _, err := st.pool.Exec(ctx, `UPDATE sources SET status = $2 WHERE id = $1`, sDis, types.SourceStatusDisabled); err != nil {
			t.Fatalf("置 disabled: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `UPDATE sources SET next_fetch_at = now() + interval '1 hour' WHERE id = $1`, sFuture); err != nil {
			t.Fatalf("置未到期: %v", err)
		}
		due, err := st.ListDueSourcesBySchedule(ctx, schedC)
		if err != nil {
			t.Fatalf("ListDueSourcesBySchedule: %v", err)
		}
		if len(due) != 0 {
			t.Errorf("disabled 与未到期源都应被排除（重复计费护栏），实得 %+v", due)
		}
	})

	t.Run("EnableSource 认 schedule_sources 归属：plan 源（无订阅）可被属主重启用", func(t *testing.T) {
		// 纯 plan 源：只经 schedule_sources 绑定，无 subscription。
		sPlan := mkSource(t, `{"q":"plan"}`)
		defer cleanupExec(t.Context(), t, st, `DELETE FROM sources WHERE id = $1`, sPlan)
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{sPlan}); err != nil {
			t.Fatalf("Replace: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `UPDATE sources SET status = $2, fail_count = 12 WHERE id = $1`, sPlan, types.SourceStatusDisabled); err != nil {
			t.Fatalf("置 disabled: %v", err)
		}
		// 属主经其任务的 schedule_sources 链接应能重启用（放宽前这里恒 false → plan 源永久失活）。
		enabled, err := st.EnableSource(ctx, u.ID, sPlan)
		if err != nil {
			t.Fatalf("EnableSource: %v", err)
		}
		if !enabled {
			t.Error("plan 源的属主经 schedule_sources 归属应能重启用（enabled=true）")
		}
		// 非属主不能。
		if en2, _ := st.EnableSource(ctx, u2.ID, sPlan); en2 {
			t.Error("非属主不该能重启用他人任务的 plan 源")
		}
	})

	t.Run("零行为变化守卫：材料化源无订阅、不进抓取扇出", func(t *testing.T) {
		// b2 的头号不变量：材料化出的源只进 sources + schedule_sources，**绝不建 subscription**，
		// 故既有抓取扇出（ListDueSourcesByUser，JOIN subscriptions）看不到它、也不出现在用户订阅里。
		sGuard := mkSource(t, `{"query":"guard"}`)
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{sGuard}); err != nil {
			t.Fatalf("Replace 失败: %v", err)
		}
		// (a) 该用户没有对应 subscription。
		var subCnt int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM subscriptions WHERE user_id = $1 AND source_id = $2`, u.ID, sGuard).Scan(&subCnt); err != nil {
			t.Fatalf("查订阅数失败: %v", err)
		}
		if subCnt != 0 {
			t.Errorf("材料化源不该建订阅（任务私有），实得 %d 条", subCnt)
		}
		// (b) 抓取扇出看不到它。
		due, err := st.ListDueSourcesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListDueSourcesByUser 失败: %v", err)
		}
		for _, s := range due {
			if s.ID == sGuard {
				t.Errorf("材料化的无订阅源不该进抓取扇出，实得含 %d", sGuard)
			}
		}
	})

	t.Run("ScheduleSourceForContent 取任务命中源而非全局首发源（#8）", func(t *testing.T) {
		// 本任务只绑 s1（任务源）；s2、s3 是非任务源。
		if err := st.ReplaceScheduleSources(ctx, u.ID, schedID, []int64{s1}); err != nil {
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
		sid, ok, err := st.ScheduleSourceForContent(ctx, cID, schedID)
		if err != nil {
			t.Fatalf("ScheduleSourceForContent(c) 失败: %v", err)
		}
		if !ok || sid != s1 {
			t.Errorf("应返回任务命中源 s1=%d（非首发源 s2=%d），实得 ok=%v sid=%d", s1, s2, ok, sid)
		}
		// 无交集：c2 只在非任务源 s3 下 → (0,false)，调用方回退首发源。
		if sid, ok, err := st.ScheduleSourceForContent(ctx, c2ID, schedID); err != nil || ok || sid != 0 {
			t.Errorf("无交集应返回 (0,false)，实得 sid=%d ok=%v err=%v", sid, ok, err)
		}
	})
}
