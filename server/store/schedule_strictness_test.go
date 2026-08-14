package store

import (
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

// TestScheduleStrictness 是 DATABASE_URL 门控的集成测试（无则跳过），覆盖
// 任务门槛档位（migration 025）的关键往返：NULL=未设置返回空串、Set→Get、
// 归属校验（WHERE 谓词内，越权 NotFound 且零副作用）、不存在行的兜底语义。
func TestScheduleStrictness(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 schedule strictness 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	owner, err := st.UpsertUserByOpenID(ctx, "test_strict_"+uuid.NewString(), "strictness-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}
	attachTenant(t, st, owner.ID)
	other, err := st.UpsertUserByOpenID(ctx, "test_strict_other_"+uuid.NewString(), "strictness-other")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID(other) 失败: %v", err)
	}
	attachTenant(t, st, other.ID)

	schedID := "push-test-strict-" + uuid.NewString()
	if err := st.InsertSchedule(ctx, &types.Schedule{ID: schedID, UserID: owner.ID}); err != nil {
		t.Fatalf("InsertSchedule() 失败: %v", err)
	}
	t.Cleanup(func() {
		_ = st.DeleteSchedule(ctx, schedID, owner.ID) //nolint:errcheck // 清理尽力而为
	})

	// NULL（migration 刚加列、从未设置）→ 空串："没设" ≠ "要宽松"。
	got, err := st.GetScheduleStrictness(ctx, schedID)
	if err != nil {
		t.Fatalf("GetScheduleStrictness(未设置) 失败: %v", err)
	}
	if got != "" {
		t.Fatalf("未设置的档位应为空串（调用方兜底），实得 %q", got)
	}

	// 越权写：归属校验在 WHERE 谓词内，须 NotFound 且不产生任何写入。
	err = st.SetScheduleStrictness(ctx, schedID, other.ID, types.StrictnessStrict)
	if err == nil || !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("越权设置应 NotFound，实得: %v", err)
	}
	if got, _ = st.GetScheduleStrictness(ctx, schedID); got != "" {
		t.Fatalf("越权尝试后档位应仍未设置，实得 %q", got)
	}

	// 正常 Set→Get 往返 + 覆盖更新。
	if err := st.SetScheduleStrictness(ctx, schedID, owner.ID, types.StrictnessStrict); err != nil {
		t.Fatalf("SetScheduleStrictness(strict) 失败: %v", err)
	}
	if got, _ = st.GetScheduleStrictness(ctx, schedID); got != types.StrictnessStrict {
		t.Fatalf("Set 后应读回 strict，实得 %q", got)
	}
	if err := st.SetScheduleStrictness(ctx, schedID, owner.ID, types.StrictnessNormal); err != nil {
		t.Fatalf("SetScheduleStrictness(normal 覆盖) 失败: %v", err)
	}
	if got, _ = st.GetScheduleStrictness(ctx, schedID); got != types.StrictnessNormal {
		t.Fatalf("覆盖后应读回 normal，实得 %q", got)
	}

	// 行不存在 → 空串 + nil（对已删调度的迟到触发放行到兜底，不中断推送）。
	got, err = st.GetScheduleStrictness(ctx, "push-test-strict-missing-"+uuid.NewString())
	if err != nil {
		t.Fatalf("不存在的调度应零错误走兜底，实得: %v", err)
	}
	if got != "" {
		t.Fatalf("不存在的调度档位应为空串，实得 %q", got)
	}
}
