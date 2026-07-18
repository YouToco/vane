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
}
