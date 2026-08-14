package store

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// 契约 §2.8「已知越权洞」点名了两处：api/schedules.go 的 handleDeleteSchedule 与
// agent/tools.go 的 removeScheduleTool，注释都写着「单 owner：所有调度同属一人，
// 故不再逐条校验归属」。
//
// 2026-07-19 排查：**两处都已修好**——都改成把 userID 传给 scheduler.DeletePush /
// UpdatePush，而那两个方法用 GetSchedule(id, userID) 做归属校验先行。
//
// 但**没有任何回归测试锁住这件事**。同期的 ClaimPendingAction、EnableSource、
// 卡片回调 delivery 都有越权用例，唯独调度这条没有——正是契约点名的那两个。
//
// 修好而无守卫等于「这一版是对的」，不等于「以后也对」。这一组把它变成后者：
// 归属谓词在 SQL 内，越权请求**零副作用**地失败（契约称之为范式的那种做法）。

func ownershipStore(t *testing.T) *Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过调度归属集成测试")
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
	return st
}

// seedOwnedSchedule 建一个属于新用户的调度，返回 (scheduleID, ownerUserID)。
func seedOwnedSchedule(t *testing.T, st *Store) (string, int64) {
	t.Helper()
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, "sched_own_"+uuid.NewString(), "归属测试")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	attachTenant(t, st, u.ID)

	schedID := "push-own-" + uuid.NewString()
	sc := &types.Schedule{
		ID: schedID, UserID: u.ID, Status: types.ScheduleStatusActive,
		NLDescription: "归属测试任务",
		SpecJSON:      json.RawMessage(`{"cron":"30 8 * * *","tz":"Asia/Shanghai"}`),
		ScopeJSON:     json.RawMessage(`{}`),
	}
	if err := st.InsertSchedule(ctx, sc); err != nil {
		t.Fatalf("建调度失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM schedules WHERE id = $1`, schedID)
	})
	return schedID, u.ID
}

// TestGetSchedule_RejectsOtherUsersSchedule 锁住归属谓词本身。
//
// GetSchedule 是 scheduler.DeletePush / UpdatePush 的**唯一**归属校验点——
// 它一旦丢掉 user_id 谓词，那两条路径同时失守：任何人只要猜到（或从别处看到）
// 一个 schedule id，就能删掉或改掉别人的定时任务。
//
// 「不存在」与「不属于你」必须归一为 NotFound：分开报会让攻击者能枚举出哪些 id 真实存在。
func TestGetSchedule_RejectsOtherUsersSchedule(t *testing.T) {
	st := ownershipStore(t)
	ctx := t.Context()
	schedID, ownerID := seedOwnedSchedule(t, st)

	other, err := st.UpsertUserByOpenID(ctx, "sched_other_"+uuid.NewString(), "他人")
	if err != nil {
		t.Fatalf("建他人用户失败: %v", err)
	}
	attachTenant(t, st, other.ID)

	// 归属者本人：拿得到。
	if _, err := st.GetSchedule(ctx, schedID, ownerID); err != nil {
		t.Fatalf("归属者应能读到自己的调度: %v", err)
	}

	// 他人：必须拿不到，且错误码与「不存在」一致。
	_, err = st.GetSchedule(ctx, schedID, other.ID)
	if err == nil {
		t.Fatal("他人不得读到该调度 —— DeletePush / UpdatePush 的归属校验全靠这一条，" +
			"它失守意味着任何人猜到 id 就能删改别人的定时任务")
	}
	assertAppCode(t, err, types.CodeNotFound)

	// 不存在的 id：错误码必须与「他人的」完全一致，否则可被用来枚举 id 是否存在。
	_, err2 := st.GetSchedule(ctx, "push-nonexistent-"+uuid.NewString(), other.ID)
	assertAppCode(t, err2, types.CodeNotFound)
}

// TestDeleteSchedule_RejectsOtherUserWithoutSideEffect：越权删除必须**零副作用**。
//
// 契约把 store/agent.go 的 ClaimPendingAction 称为范式——归属校验写在 WHERE 谓词内，
// 越权请求打不到任何行。这条用例要求 DeleteSchedule 达到同样标准：
// 不是「先查再删」（那有 TOCTOU 窗口），而是删不动。
func TestDeleteSchedule_RejectsOtherUserWithoutSideEffect(t *testing.T) {
	st := ownershipStore(t)
	ctx := t.Context()
	schedID, ownerID := seedOwnedSchedule(t, st)

	other, err := st.UpsertUserByOpenID(ctx, "sched_del_other_"+uuid.NewString(), "他人")
	if err != nil {
		t.Fatalf("建他人用户失败: %v", err)
	}
	attachTenant(t, st, other.ID)

	// 越权删除：报错，且调度必须还在。
	if err := st.DeleteSchedule(ctx, schedID, other.ID); err == nil {
		t.Error("他人不得删除该调度")
	}
	if _, err := st.GetSchedule(ctx, schedID, ownerID); err != nil {
		t.Fatalf("越权删除必须零副作用，调度却已消失: %v", err)
	}

	// 归属者本人：删得掉。
	if err := st.DeleteSchedule(ctx, schedID, ownerID); err != nil {
		t.Fatalf("归属者应能删除自己的调度: %v", err)
	}
	if _, err := st.GetSchedule(ctx, schedID, ownerID); err == nil {
		t.Error("删除后不该还能读到")
	}
}
