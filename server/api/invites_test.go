package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// TestToInviteItem 钉住 DTO 的状态语义与 types.Invite 对齐。
//
// 最容易写错的一处：used 的判据是**用满**（used_count >= max_uses，即 Exhausted），
// 不是「用过」（used_count > 0）。写成后者，一个 2/5 的多用码会在管理面显示成
// 「已用」，管理员据此以为它废了、把它再发给别人——而它其实还能注册 3 个租户。
func TestToInviteItem(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	email := "consumer@example.com"
	tenant := int64(42)

	cases := []struct {
		name        string
		in          store.InviteWithConsumer
		wantUsed    bool
		wantExpired bool
	}{
		{
			name: "未用未过期",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "A", IssuedAt: past, ExpiresAt: &future, MaxUses: 1, UsedCount: 0,
			}},
			wantUsed: false, wantExpired: false,
		},
		{
			name: "已用满",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "B", IssuedAt: past, MaxUses: 1, UsedCount: 1,
				ConsumedByTenant: &tenant, ConsumedAt: &past,
			}, ConsumerEmail: &email},
			wantUsed: true, wantExpired: false,
		},
		{
			name: "已过期未用",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "C", IssuedAt: past, ExpiresAt: &past, MaxUses: 1, UsedCount: 0,
			}},
			wantUsed: false, wantExpired: true,
		},
		{
			name: "多用码部分使用不算已用",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "D", IssuedAt: past, MaxUses: 5, UsedCount: 2,
				ConsumedByTenant: &tenant, ConsumedAt: &past,
			}, ConsumerEmail: &email},
			wantUsed: false, wantExpired: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toInviteItem(c.in, now)
			if got.Used != c.wantUsed || got.Expired != c.wantExpired {
				t.Errorf("状态判定不符: used=%v(期望 %v) expired=%v(期望 %v)",
					got.Used, c.wantUsed, got.Expired, c.wantExpired)
			}
			if got.Code != c.in.Code || !got.CreatedAt.Equal(c.in.IssuedAt) ||
				got.MaxUses != c.in.MaxUses || got.UsedCount != c.in.UsedCount {
				t.Errorf("透传字段不符: %+v", got)
			}
			if (got.UsedBy == nil) != (c.in.ConsumerEmail == nil) {
				t.Errorf("used_by 应与 ConsumerEmail 同空同有: %v vs %v",
					got.UsedBy, c.in.ConsumerEmail)
			}
			if (got.UsedAt == nil) != (c.in.ConsumedAt == nil) {
				t.Errorf("used_at 应与 ConsumedAt 同空同有: %v vs %v",
					got.UsedAt, c.in.ConsumedAt)
			}
		})
	}
}

// inviteAPIStore 建真库连接；未设 DATABASE_URL 时跳过（与 store 集成测试同模式）。
//
// 为什么 api 层也连真库、而不是把 Deps.Store 收成接口塞内存 fake：那正是
// observability_test.go 头注释拒绝的「让生产代码为测试让路」。invites handler
// 越过闸门后只是把具体 *store.Store 交给 store 方法，装配正确性靠编译期保证，
// 剩下值得测的是「handler↔store 真的接上了、状态码/JSON 形状/issued_by 接线对不对」
// ——这些只有连真库跑一遍才算数。
func inviteAPIStore(t *testing.T) *store.Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过邀请码 API 集成测试")
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func assertErrorBody(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("错误响应非合法 JSON: %v；体=%s", err, w.Body.String())
	}
	if body["error"] == "" {
		t.Errorf("错误响应应含人话 error 字段，实得 %s", w.Body.String())
	}
}

// TestInviteEndpoints_OwnerHappyPath 补上安全清单测不到的那一半：
// 平台 owner 越过闸门后，三端点的成功路径确实接上了 store、状态码与 JSON 形状正确。
//
// 安全清单（auth_security_test.go）只验非 owner→404 / 无会话→401，在 requirePlatformOwner
// 就返回、根本够不到 store；owner 成功路径此前零覆盖。用 authedDeps 造租户 1 的
// owner 会话（fake 鉴权，与 observability 测试同一脚手架）+ 真库 Store。
func TestInviteEndpoints_OwnerHappyPath(t *testing.T) {
	st := inviteAPIStore(t)
	mux := http.NewServeMux()
	deps, cookie := authedDeps(t, Deps{Store: st})
	Mount(mux, deps)

	do := func(method, path string, wantCode int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, nil)
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != wantCode {
			t.Fatalf("%s %s 状态码 = %d, 期望 %d；体=%s", method, path, w.Code, wantCode, w.Body.String())
		}
		return w
	}

	// GET：空列表也回 {"invites":[]}（不是 null），否则前端 .map 会炸。
	// 库是共享的，这里只断言键存在且为数组，不假设它一定为空。
	w := do(http.MethodGet, "/api/admin/invites", http.StatusOK)
	var listResp struct {
		Invites []inviteItem `json:"invites"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("列表响应非合法 JSON: %v；体=%s", err, w.Body.String())
	}
	if listResp.Invites == nil {
		t.Error("invites 字段应序列化为 []（哪怕空），实为 null —— 前端 .map 会炸")
	}

	// POST：201 + 一次性码 + 默认有效期，且 issued_by 接成会话 userID(=1)。
	w = do(http.MethodPost, "/api/admin/invites", http.StatusCreated)
	var created inviteItem
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("签发响应非合法 JSON: %v；体=%s", err, w.Body.String())
	}
	if created.Code == "" || created.MaxUses != 1 || created.Used || created.ExpiresAt == nil {
		t.Errorf("新码字段不符（期望 一次性/未用/有有效期）: %+v", created)
	}
	// 接线核验：issued_by 必须是发起签发的 principal（会话 userID=1），
	// 而非 CLI 的平台自签（nil）——这是 handler 唯一的实质接线逻辑。
	inv, err := st.GetInvite(t.Context(), created.Code)
	if err != nil {
		t.Fatalf("回查新码失败: %v", err)
	}
	if inv.IssuedBy == nil || *inv.IssuedBy != 1 {
		t.Errorf("issued_by 应接成会话 userID=1，实得 %v", inv.IssuedBy)
	}

	// 刚签发的码应出现在列表里。
	w = do(http.MethodGet, "/api/admin/invites", http.StatusOK)
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("列表响应非合法 JSON: %v", err)
	}
	found := false
	for _, it := range listResp.Invites {
		if it.Code == created.Code {
			found = true
			break
		}
	}
	if !found {
		t.Error("刚签发的码未出现在列表中")
	}

	// DELETE 未使用的码：200。
	do(http.MethodDelete, "/api/admin/invites/"+created.Code, http.StatusOK)

	// DELETE 不存在的码：404 + 人话 error 体（经 writeAppError 透传 CodeNotFound）。
	w = do(http.MethodDelete, "/api/admin/invites/NOSUCHCODE999", http.StatusNotFound)
	assertErrorBody(t, w)

	// DELETE 已使用的码：409（store 的 CodeConflict 必须经 writeAppError 映射成 409，
	// 而不是被静默吞掉）。造一个真消费过的码。
	uniq := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	usedCode := "apitest-used-" + uniq
	if _, err := st.IssueInvite(t.Context(), usedCode, nil, 1, nil); err != nil {
		t.Fatalf("签发待消费码失败: %v", err)
	}
	if _, _, err := st.RegisterWithInvite(t.Context(), "apitest-"+uniq+"@invite.local", "test-hash", usedCode); err != nil {
		t.Fatalf("消费邀请码失败: %v", err)
	}
	w = do(http.MethodDelete, "/api/admin/invites/"+usedCode, http.StatusConflict)
	assertErrorBody(t, w)
}
