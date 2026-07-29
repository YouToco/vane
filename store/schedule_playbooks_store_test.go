package store

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestSchedulePlaybookStore 是任务手册 P0 存取层的 DATABASE_URL 门控集成测试：
// UpsertSchedulePlaybook 的新建/更新（ON CONFLICT 不清 fetch_plan）/归属拦截，
// GetSchedulePlaybook 的往返/NotFound/归属拦截，以及删 schedule 的 CASCADE。
func TestSchedulePlaybookStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 schedule_playbooks 集成测试")
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

	u, err := st.UpsertUserByOpenID(ctx, "test_playbook_"+uuid.NewString(), "playbook-owner")
	if err != nil {
		t.Fatalf("建 owner 失败: %v", err)
	}
	attachTenant(t, st, u.ID)
	u2, err := st.UpsertUserByOpenID(ctx, "test_playbook_stranger_"+uuid.NewString(), "playbook-stranger")
	if err != nil {
		t.Fatalf("建 stranger 失败: %v", err)
	}
	attachTenant(t, st, u2.ID)
	schedID := "push-test-" + uuid.NewString()
	if err := st.InsertSchedule(ctx, &types.Schedule{
		ID: schedID, UserID: u.ID,
		SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
		Status: types.ScheduleStatusActive,
	}); err != nil {
		t.Fatalf("InsertSchedule 失败: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序：playbook（虽 CASCADE 会带，显式删更稳）→ schedules → users。
		cleanupExec(ctx, t, st, `DELETE FROM schedule_playbooks WHERE schedule_id = $1`, schedID)
		cleanupExec(ctx, t, st, `DELETE FROM schedules WHERE user_id = ANY($1)`, []int64{u.ID, u2.ID})
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = ANY($1)`, []int64{u.ID, u2.ID})
	})

	t.Run("upsert 首次插入 + 往返", func(t *testing.T) {
		ok, err := st.UpsertSchedulePlaybook(ctx, u.ID, schedID, "每天早8点推 AI 大模型动态")
		if err != nil {
			t.Fatalf("UpsertSchedulePlaybook 失败: %v", err)
		}
		if !ok {
			t.Fatal("首次写入自己的任务应 ok=true")
		}
		pb, err := st.GetSchedulePlaybook(ctx, u.ID, schedID)
		if err != nil {
			t.Fatalf("GetSchedulePlaybook 失败: %v", err)
		}
		if pb.Content != "每天早8点推 AI 大模型动态" {
			t.Errorf("content 不符: %q", pb.Content)
		}
		var m map[string]any
		if err := json.Unmarshal(pb.FetchPlan, &m); err != nil || len(m) != 0 {
			t.Errorf("fetch_plan 应为合法空对象, 实得 %s err=%v", pb.FetchPlan, err)
		}
		if pb.UpdatedAt.IsZero() {
			t.Error("updated_at 应非零")
		}
	})

	t.Run("upsert 更新不清 fetch_plan", func(t *testing.T) {
		// 模拟 P1 已写入 fetch_plan，然后 P0 的 upsert（只改 content）必须保留它。
		if _, err := st.pool.Exec(ctx,
			`UPDATE schedule_playbooks SET fetch_plan = '{"p1":true}' WHERE schedule_id = $1`, schedID); err != nil {
			t.Fatalf("预置 fetch_plan 失败: %v", err)
		}
		if _, err := st.UpsertSchedulePlaybook(ctx, u.ID, schedID, "改成每天九点半"); err != nil {
			t.Fatalf("Upsert 失败: %v", err)
		}
		pb, err := st.GetSchedulePlaybook(ctx, u.ID, schedID)
		if err != nil {
			t.Fatalf("Get 失败: %v", err)
		}
		if pb.Content != "改成每天九点半" {
			t.Errorf("content 未更新: %q", pb.Content)
		}
		var m map[string]any
		_ = json.Unmarshal(pb.FetchPlan, &m)
		if v, _ := m["p1"].(bool); !v {
			t.Errorf("P0 的 upsert 不应清空 P1 的 fetch_plan, 实得 %s", pb.FetchPlan)
		}
	})

	t.Run("get 不存在返回 NotFound", func(t *testing.T) {
		if _, err := st.GetSchedulePlaybook(ctx, u.ID, "push-does-not-exist"); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("应返回 NotFound, 实得 %v", err)
		}
	})

	// —— P1 编译层：SetFetchPlan（只改 fetch_plan、归属进 SQL、依附已存在手册行）——
	targetCount := func(t *testing.T, raw json.RawMessage) int {
		t.Helper()
		var p struct {
			Targets []map[string]any `json:"targets"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("fetch_plan 不是合法计划 JSON: %v (%s)", err, raw)
		}
		return len(p.Targets)
	}

	t.Run("SetFetchPlan 只改计划不动正文", func(t *testing.T) {
		plan := json.RawMessage(`{"targets":[{"platform":"web","capability":"search","url":"vane://web/search?q=ai"}]}`)
		ok, err := st.SetFetchPlan(ctx, u.ID, schedID, plan)
		if err != nil {
			t.Fatalf("SetFetchPlan 失败: %v", err)
		}
		if !ok {
			t.Fatal("属主写自己任务的计划应 ok=true")
		}
		pb, err := st.GetSchedulePlaybook(ctx, u.ID, schedID)
		if err != nil {
			t.Fatalf("Get 失败: %v", err)
		}
		if pb.Content != "改成每天九点半" {
			t.Errorf("SetFetchPlan 不应改动 content, 实得 %q", pb.Content)
		}
		if n := targetCount(t, pb.FetchPlan); n != 1 {
			t.Errorf("fetch_plan 未写入预期计划（应 1 个目标）, 实得 %d: %s", n, pb.FetchPlan)
		}
	})

	t.Run("SetFetchPlan 空计划归一化为零目标对象", func(t *testing.T) {
		ok, err := st.SetFetchPlan(ctx, u.ID, schedID, nil)
		if err != nil || !ok {
			t.Fatalf("空计划应 ok=true 无错: ok=%v err=%v", ok, err)
		}
		pb, _ := st.GetSchedulePlaybook(ctx, u.ID, schedID)
		if n := targetCount(t, pb.FetchPlan); n != 0 {
			t.Errorf("nil 应归一化为 {\"targets\":[]}, 实得 %d 个目标: %s", n, pb.FetchPlan)
		}
	})

	t.Run("SetFetchPlan 归属校验：拒绝非属主且不覆盖", func(t *testing.T) {
		ok, err := st.SetFetchPlan(ctx, u2.ID, schedID, json.RawMessage(`{"targets":[{"platform":"web","capability":"feed","url":"https://x.com/rss"}]}`))
		if err != nil {
			t.Fatalf("非属主不应报基础设施错误: %v", err)
		}
		if ok {
			t.Fatal("非属主写他人计划应 ok=false")
		}
		pb, _ := st.GetSchedulePlaybook(ctx, u.ID, schedID)
		if n := targetCount(t, pb.FetchPlan); n != 0 {
			t.Errorf("非属主写入不得覆盖属主计划（应仍为零目标）, 实得 %d 个目标: %s", n, pb.FetchPlan)
		}
	})

	t.Run("SetFetchPlan 到不存在的任务 ok=false", func(t *testing.T) {
		ok, err := st.SetFetchPlan(ctx, u.ID, "push-no-such-sched", json.RawMessage(`{"targets":[]}`))
		if err != nil {
			t.Fatalf("不存在任务不应报错: %v", err)
		}
		if ok {
			t.Error("不存在的任务应 ok=false")
		}
	})

	t.Run("SetFetchPlan 无手册行时 ok=false（不建孤儿计划行）", func(t *testing.T) {
		// 任务存在但从未 Upsert 过手册正文（无 schedule_playbooks 行）→ UPDATE 匹配 0 行。
		schedNoPB := "push-test-nopb-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedNoPB, UserID: u.ID,
			SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		defer cleanupExec(ctx, t, st, `DELETE FROM schedules WHERE id = $1`, schedNoPB)
		ok, err := st.SetFetchPlan(ctx, u.ID, schedNoPB, json.RawMessage(`{"targets":[]}`))
		if err != nil {
			t.Fatalf("无手册行不应报错: %v", err)
		}
		if ok {
			t.Error("无手册行应 ok=false（计划依附不上，不该建只有计划没正文的孤儿行）")
		}
	})

	t.Run("归属校验：upsert 拒绝非属主且不覆盖", func(t *testing.T) {
		ok, err := st.UpsertSchedulePlaybook(ctx, u2.ID, schedID, "越权写")
		if err != nil {
			t.Fatalf("非属主 upsert 不应报基础设施错误: %v", err)
		}
		if ok {
			t.Fatal("非属主写他人任务的手册应 ok=false（SELECT 产 0 行）")
		}
		pb, _ := st.GetSchedulePlaybook(ctx, u.ID, schedID)
		if pb.Content != "改成每天九点半" {
			t.Errorf("非属主写入不得覆盖属主内容, 实得 %q", pb.Content)
		}
	})

	t.Run("归属校验：get 拒绝非属主", func(t *testing.T) {
		if _, err := st.GetSchedulePlaybook(ctx, u2.ID, schedID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("非属主读应 NotFound, 实得 %v", err)
		}
	})

	t.Run("upsert 到不存在的 schedule 返回 ok=false", func(t *testing.T) {
		ok, err := st.UpsertSchedulePlaybook(ctx, u.ID, "push-no-such-sched", "x")
		if err != nil {
			t.Fatalf("不应撞 FK 报 DATABASE, 存在性由 SELECT 兜住: %v", err)
		}
		if ok {
			t.Error("不存在的 schedule 应 ok=false")
		}
	})

	t.Run("cascade：删 schedule 带走 playbook", func(t *testing.T) {
		// 独立 schedID2，测完自然随 owner 清理，不污染上面的主 schedID。
		schedID2 := "push-test-cascade-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: schedID2, UserID: u.ID,
			SpecJSON: json.RawMessage("{}"), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		if _, err := st.UpsertSchedulePlaybook(ctx, u.ID, schedID2, "级联测试"); err != nil {
			t.Fatalf("Upsert 失败: %v", err)
		}
		if _, err := st.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, schedID2); err != nil {
			t.Fatalf("删 schedule 失败: %v", err)
		}
		var exists bool
		if err := st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schedule_playbooks WHERE schedule_id = $1)`, schedID2).Scan(&exists); err != nil {
			t.Fatalf("查 EXISTS 失败: %v", err)
		}
		if exists {
			t.Error("删 schedule 后 ON DELETE CASCADE 应带走其手册")
		}
	})
}
