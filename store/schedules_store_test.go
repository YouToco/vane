package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestUpdateScheduleSpecStore 是 DATABASE_URL 门控的集成测试，钉死 UpdateScheduleSpec
// 的三条语义：spec_json 真的被替换、nlDesc 的 nil/非 nil 指针语义、以及**改不存在的 id
// 必须返回 NotFound 而不是静默成功**（那意味着 Temporal 有、镜像没有的漂移，不能咽掉）。
func TestUpdateScheduleSpecStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过调度镜像 store 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	t.Cleanup(st.Close)

	u, err := st.UpsertUserByOpenID(ctx, "test_schedupd_"+uuid.NewString(), "sched-update-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}
	schedID := "push-test-" + uuid.NewString()
	t.Cleanup(func() {
		// 同 push_batches 测试：t.Context() 在 Cleanup 前已取消，必须另起 context，
		// 且不吞错——静默失败的清理会让脏数据一轮轮堆积而测试照样全绿。
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := st.pool.Exec(cleanCtx, `DELETE FROM schedules WHERE id = $1`, schedID); err != nil {
			t.Errorf("清理 schedules 失败: %v", err)
		}
		if _, err := st.pool.Exec(cleanCtx, `DELETE FROM users WHERE id = $1`, u.ID); err != nil {
			t.Errorf("清理 users 失败: %v", err)
		}
	})

	orig := &types.Schedule{
		ID:            schedID,
		UserID:        u.ID,
		NLDescription: "原始描述",
		SpecJSON:      json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
		ScopeJSON:     json.RawMessage(`{}`),
		Status:        types.ScheduleStatusActive,
	}
	if err := st.InsertSchedule(ctx, orig); err != nil {
		t.Fatalf("InsertSchedule() 失败: %v", err)
	}

	t.Run("替换 spec 且 nil 描述不改", func(t *testing.T) {
		newSpec := json.RawMessage(`{"cron":"30 9 * * *","tz":"Asia/Shanghai"}`)
		if err := st.UpdateScheduleSpec(ctx, schedID, newSpec, nil); err != nil {
			t.Fatalf("UpdateScheduleSpec() 失败: %v", err)
		}
		got, err := st.GetSchedule(ctx, schedID, u.ID)
		if err != nil {
			t.Fatalf("GetSchedule() 失败: %v", err)
		}
		var spec map[string]any
		if err := json.Unmarshal(got.SpecJSON, &spec); err != nil {
			t.Fatalf("spec_json 不是合法 JSON: %v", err)
		}
		if spec["cron"] != "30 9 * * *" {
			t.Errorf("spec_json 应更新为新 cron，实得 %s", got.SpecJSON)
		}
		if got.NLDescription != "原始描述" {
			t.Errorf("nlDesc 传 nil 时描述必须保持不变，实得 %q", got.NLDescription)
		}
		if !got.UpdatedAt.After(got.CreatedAt) && got.UpdatedAt.Equal(got.CreatedAt) {
			t.Error("updated_at 应被推进（表无触发器，靠 SQL 里的 now()）")
		}
	})

	t.Run("非 nil 描述被更新", func(t *testing.T) {
		desc := "改成每天九点半"
		if err := st.UpdateScheduleSpec(ctx, schedID,
			json.RawMessage(`{"cron":"30 9 * * *"}`), &desc); err != nil {
			t.Fatalf("UpdateScheduleSpec() 失败: %v", err)
		}
		got, err := st.GetSchedule(ctx, schedID, u.ID)
		if err != nil {
			t.Fatalf("GetSchedule() 失败: %v", err)
		}
		if got.NLDescription != desc {
			t.Errorf("描述应更新为 %q，实得 %q", desc, got.NLDescription)
		}
	})

	t.Run("不存在的 id 返回 NotFound", func(t *testing.T) {
		err := st.UpdateScheduleSpec(ctx, "push-does-not-exist",
			json.RawMessage(`{"cron":"0 8 * * *"}`), nil)
		if err == nil {
			t.Fatal("改不存在的调度必须报错（镜像与 Temporal 漂移），不能静默成功")
		}
		if !errors.Is(err, types.ErrNotFound) {
			t.Errorf("应是 CodeNotFound，实得 %v", err)
		}
	})
}
