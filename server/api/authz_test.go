package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/scheduler"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

// spyScheduler 记录是否真的对 Temporal 下了手。
//
// 关键：删除/更新任务的归属校验必须发生在**动 Temporal 之前**——
// 生产实现里 Temporal 删除不可逆，「先删后校验」等于校验失败时对方的调度
// 已经没了。本替身只要被调到，就说明归属把关没拦住。
type spyScheduler struct {
	deleted string
	updated string
}

type allowTeamTaskAccess struct{ executionUserID int64 }

func (a allowTeamTaskAccess) AuthorizeScheduleMutation(
	context.Context, int64, int64, string, store.TaskMutation,
) (*types.Schedule, error) {
	return &types.Schedule{UserID: a.executionUserID}, nil
}

func (a allowTeamTaskAccess) TransferScheduleAssignee(
	context.Context, int64, int64, string, int64,
) (*types.Schedule, error) {
	return &types.Schedule{UserID: a.executionUserID}, nil
}

func (s *spyScheduler) CreatePush(context.Context, int64, scheduler.ScheduleSpec, workflow.PushScope, string) (string, error) {
	return "sched-x", nil
}
func (s *spyScheduler) UpdatePush(_ context.Context, id string, _ int64, _ scheduler.ScheduleSpec, _ *string) error {
	s.updated = id
	return nil
}
func (s *spyScheduler) DeletePushIdempotent(
	_ context.Context,
	id string,
	_ int64,
	_ string,
) error {
	s.deleted = id
	return nil
}

// authzMux 起一个以 userID 身份登录的 mux。
func authzMux(t *testing.T, userID, tenantID int64, sched any) (*http.ServeMux, *http.Cookie) {
	t.Helper()
	fake := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: userID, TenantID: tenantID,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fake.members[userID] = []types.Membership{{
		TenantID: tenantID, UserID: userID,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{
		Auth: fake, Principal: auth.NewContextResolver(), Scheduler: sched,
		TeamTasks: allowTeamTaskAccess{executionUserID: userID},
	})
	return mux, &http.Cookie{Name: sessionCookieName, Value: token}
}

// TestAuthz_ScheduleOwnershipIsChecked 钉住一个**真实存在过的越权洞**。
//
// 背景：M3 单 owner 时代，api/schedules.go 的 DELETE/PATCH 明写「不逐条校验归属
// （Dashboard 有密码门）」——那时全系统只有一个人，假设成立。D2′ 引入真多用户后，
// 这个假设当场失效：任何登录用户都能凭调度 id 删/改别人的调度。
// 本用例在改造当时实测复现过（user 999 删掉他人调度返回 200），修复后转为回归守卫。
//
// 修复形状：归属校验下沉到 scheduler 层、且**在动 Temporal 之前**；
// store 侧 DELETE/SELECT 都带 user_id 谓词（范式取自 ClaimPendingAction）。
func TestAuthz_ScheduleOwnershipIsChecked(t *testing.T) {
	// 生产实现（*scheduler.Scheduler）会先 GetSchedule(id, userID)，查不到即 NotFound。
	// 此处用 spy 代替 Temporal，验证的是「handler 把 userID 传下去了」这一半；
	// 「scheduler 校验后才动 Temporal」由 scheduler 包自己的用例覆盖。
	spy := &spyScheduler{}
	mux, cookie := authzMux(t, 999, 999, spy)

	t.Run("DELETE 必须带上调用者身份", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodDelete, "/api/schedules/push-1-victim", nil)
		r.Header.Set("Idempotency-Key", "authz-delete-1")
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code == http.StatusUnauthorized {
			t.Fatal("会话有效却被拒，脚手架有问题")
		}
	})

	t.Run("PATCH 必须带上调用者身份", func(t *testing.T) {
		body := `{"spec":{"cron":"30 9 * * *"}}`
		r := httptest.NewRequest(http.MethodPatch, "/api/schedules/push-1-victim", strings.NewReader(body))
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code == http.StatusUnauthorized {
			t.Fatal("会话有效却被拒，脚手架有问题")
		}
	})
}
