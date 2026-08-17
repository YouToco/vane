package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

// ============================================================
// 内存假 AuthStore：让鉴权用例不依赖数据库（见 AuthStore 接口注释）
// ============================================================

type fakeAuthStore struct {
	mu       sync.Mutex
	users    map[string]*types.User // key = 归一化邮箱
	sessions map[string]*types.Session
	members  map[int64][]types.Membership
	nextUID  int64

	registerErr error
	createErr   error

	inviteChecked bool
	onInviteCheck func()
}

func newFakeAuthStore() *fakeAuthStore {
	return &fakeAuthStore{
		users:    map[string]*types.User{},
		sessions: map[string]*types.Session{},
		members:  map[int64][]types.Membership{},
		nextUID:  100,
	}
}

func norm(e string) string { return strings.ToLower(strings.TrimSpace(e)) }

// addUser 造一个可登录的用户（邮箱+密码+租户归属）。
func (f *fakeAuthStore) addUser(t *testing.T, email, password string, tenantID int64) *types.User {
	t.Helper()
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextUID++
	e := norm(email)
	u := &types.User{ID: f.nextUID, Email: &e, PasswordHash: &hash}
	f.users[e] = u
	f.members[u.ID] = []types.Membership{{TenantID: tenantID, UserID: u.ID, Role: types.MembershipRoleOwner}}
	return u
}

func (f *fakeAuthStore) RegisterWithInvite(_ context.Context, email, passwordHash, code string) (*types.User, *types.Tenant, error) {
	if f.registerErr != nil {
		return nil, nil, f.registerErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e := norm(email)
	if _, dup := f.users[e]; dup {
		return nil, nil, types.NewAppError(types.CodeConflict, "该邮箱已注册", nil)
	}
	if code == "" || strings.HasPrefix(code, "bad") {
		return nil, nil, types.NewAppError(types.CodeValidation, "邀请码无效或已失效", nil)
	}
	f.nextUID++
	u := &types.User{ID: f.nextUID, Email: &e, PasswordHash: &passwordHash}
	f.users[e] = u
	tn := &types.Tenant{ID: f.nextUID * 10}
	f.members[u.ID] = []types.Membership{{TenantID: tn.ID, UserID: u.ID, Role: types.MembershipRoleOwner}}
	return u, tn, nil
}

func (f *fakeAuthStore) InviteUsable(_ context.Context, code string) (bool, error) {
	f.mu.Lock()
	f.inviteChecked = true
	cb := f.onInviteCheck
	f.mu.Unlock()
	if cb != nil {
		cb()
	}
	return code != "" && !strings.HasPrefix(code, "bad"), nil
}

func (f *fakeAuthStore) GetUserByEmail(_ context.Context, email string) (*types.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.users[norm(email)]
	if !ok {
		return nil, types.NewAppError(types.CodeNotFound, "用户不存在", nil)
	}
	return u, nil
}

func (f *fakeAuthStore) GetUserEmailByID(_ context.Context, userID int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.users {
		if u.ID == userID {
			if u.Email != nil {
				return *u.Email, nil
			}
			return "", nil
		}
	}
	return "", types.NewAppError(types.CodeNotFound, "用户不存在", nil)
}

func (f *fakeAuthStore) UpdatePasswordHash(_ context.Context, userID int64, h string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.users {
		if u.ID == userID {
			u.PasswordHash = &h
			return nil
		}
	}
	return types.NewAppError(types.CodeNotFound, "用户不存在", nil)
}

func (f *fakeAuthStore) ListMembershipsByUser(_ context.Context, userID int64) ([]types.Membership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.members[userID], nil
}

func (f *fakeAuthStore) CreateSession(_ context.Context, hash []byte, userID, tenantID int64, exp time.Time) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	role := types.MembershipRoleOwner
	for _, membership := range f.members[userID] {
		if membership.TenantID == tenantID {
			role = membership.Role
			break
		}
	}
	f.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: userID, TenantID: tenantID, Role: role,
		ActorType: types.ActorTypeUser, ExpiresAt: exp,
	}
	return nil
}

func (f *fakeAuthStore) LookupSession(_ context.Context, hash []byte) (*types.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sessions[string(hash)]
	// 过期与不存在归一为同一结果（与 store 层同语义）。
	hasExactMembership := false
	if ok {
		for _, membership := range f.members[s.UserID] {
			if membership.TenantID == s.TenantID {
				hasExactMembership = true
				break
			}
		}
	}
	if !ok || !s.ExpiresAt.After(time.Now()) || !hasExactMembership {
		return nil, types.NewAppError(types.CodeNotFound, "会话不存在或已过期", nil)
	}
	return s, nil
}

func (f *fakeAuthStore) DeleteSession(_ context.Context, hash []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sessions, string(hash))
	return nil
}

func (f *fakeAuthStore) sessionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sessions)
}

// ============================================================
// 测试脚手架
// ============================================================

func newAuthTestServer(t *testing.T) (*http.ServeMux, *fakeAuthStore) {
	t.Helper()
	fake := newFakeAuthStore()
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: fake, Principal: auth.NewContextResolver()})
	return mux, fake
}

func postJSON(t *testing.T, mux *http.ServeMux, path string, body any, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func sessionCookieFrom(t *testing.T, rec *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName && c.Value != "" {
			return c
		}
	}
	t.Fatalf("响应未下发会话 cookie（状态 %d，体 %s）", rec.Code, rec.Body.String())
	return nil
}

func errMsgOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var m map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &m)
	return m["error"]
}

// ============================================================
// 安全用例
// ============================================================

// TestSec_NoAccountEnumeration 是本轮最重要的安全用例：
// 登录接口**不得**泄漏某邮箱是否注册过。一旦「用户不存在」与「密码错误」
// 在状态码或文案上可区分，攻击者就能批量试邮箱来确认哪些注册过本站——
// 拿去撞库、钓鱼，或仅仅是泄漏用户名单本身。
func TestSec_NoAccountEnumeration(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "real@example.com", "correct-password-123", 1)

	existing := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "real@example.com", "password": "wrong-password-999"}, nil)
	missing := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "nobody@example.com", "password": "wrong-password-999"}, nil)

	if existing.Code != missing.Code {
		t.Errorf("状态码泄漏账号存在性: 已注册=%d 未注册=%d", existing.Code, missing.Code)
	}
	if existing.Code != http.StatusUnauthorized {
		t.Errorf("应为 401，实得 %d", existing.Code)
	}
	if a, b := errMsgOf(t, existing), errMsgOf(t, missing); a != b {
		t.Errorf("文案泄漏账号存在性: 已注册=%q 未注册=%q", a, b)
	}
	if got := errMsgOf(t, existing); got != authFailMsg {
		t.Errorf("应使用统一失败文案，实得 %q", got)
	}
}

// TestSec_NoPasswordlessUserBypass：纯飞书身份的用户没有密码
// （存量 owner 接管前即是此态）。此时任何密码——**包括空密码**——都不得登录成功。
// 若代码里写成「哈希为空就跳过校验」，那就是一个人人可登的后门。
func TestSec_NoPasswordlessUserBypass(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	openID := "ou_legacy_owner"
	e := "legacy@example.com"
	fake.mu.Lock()
	fake.users[e] = &types.User{ID: 7, FeishuOpenID: &openID, Email: &e} // 无 PasswordHash
	fake.members[7] = []types.Membership{{TenantID: 1, UserID: 7, Role: types.MembershipRoleOwner}}
	fake.mu.Unlock()

	for _, pw := range []string{"", "anything", "null"} {
		rec := postJSON(t, mux, "/api/auth/login",
			map[string]string{"email": e, "password": pw}, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("无密码用户用 %q 登录应 401，实得 %d", pw, rec.Code)
		}
	}
	if fake.sessionCount() != 0 {
		t.Error("无密码用户不得产生任何会话")
	}
}

// TestSec_SessionFixation：登录**必须**签发全新 token。
// 若沿用请求里带来的 token，攻击者可预先把已知 token 塞进受害者浏览器
// （如通过子域名写 cookie），受害者登录后该 token 即成为已认证会话，攻击者直接接管。
func TestSec_SessionFixation(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "victim@example.com", "victim-password-123", 1)

	attacker := &http.Cookie{Name: sessionCookieName, Value: "attacker-planted-token-value"}
	rec := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "victim@example.com", "password": "victim-password-123"}, attacker)
	if rec.Code != http.StatusOK {
		t.Fatalf("登录应成功，实得 %d: %s", rec.Code, rec.Body.String())
	}
	issued := sessionCookieFrom(t, rec)
	if issued.Value == attacker.Value {
		t.Fatal("登录沿用了请求带来的 token —— 会话固定攻击成立")
	}
	// 攻击者预置的 token 必须始终无效。
	probe := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	probe.AddCookie(attacker)
	prec := httptest.NewRecorder()
	mux.ServeHTTP(prec, probe)
	if prec.Code != http.StatusUnauthorized {
		t.Errorf("预置 token 必须无效，实得 %d", prec.Code)
	}
}

// TestSec_LogoutInvalidatesServerSide：登出必须**服务端失效**，而不只是清 cookie。
// 只清 cookie 的话，已被窃取的 token 在到期前依然有效——
// 而「token 可能已泄漏」恰恰是用户点登出的主要动机。
func TestSec_LogoutInvalidatesServerSide(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "user@example.com", "user-password-123", 1)
	login := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "user@example.com", "password": "user-password-123"}, nil)
	cookie := sessionCookieFrom(t, login)

	if rec := postJSON(t, mux, "/api/auth/logout", nil, cookie); rec.Code != http.StatusOK {
		t.Fatalf("登出应 200，实得 %d", rec.Code)
	}
	// 拿着同一个 cookie 再访问（模拟持有被窃 token 的攻击者）。
	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("登出后原 token 必须失效，实得 %d —— 仅清 cookie 挡不住已泄漏的 token", rec.Code)
	}
	if fake.sessionCount() != 0 {
		t.Error("登出应删除服务端会话记录")
	}
}

func TestSec_MembershipRevocationImmediatelyInvalidatesOldSession(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	user := fake.addUser(t, "revoked@example.com", "user-password-123", 1)
	login := postJSON(t, mux, "/api/auth/login",
		map[string]string{
			"email": "revoked@example.com", "password": "user-password-123",
		}, nil)
	cookie := sessionCookieFrom(t, login)
	fake.mu.Lock()
	delete(fake.members, user.ID)
	fake.mu.Unlock()

	get := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	get.AddCookie(cookie)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session GET status=%d", getRec.Code)
	}
	if rec := postJSON(
		t, mux, "/api/auth/logout", nil, cookie,
	); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session POST status=%d", rec.Code)
	}
}

// TestSec_CookieAttributes：会话 cookie 必须 HttpOnly（挡 XSS 窃取）、
// Secure（不走明文）、SameSite（挡跨站携带）。
func TestSec_CookieAttributes(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "cookie@example.com", "cookie-password-123", 1)
	rec := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "cookie@example.com", "password": "cookie-password-123"}, nil)
	c := sessionCookieFrom(t, rec)

	if !c.HttpOnly {
		t.Error("会话 cookie 必须 HttpOnly，否则 XSS 可直接读走")
	}
	if !c.Secure {
		t.Error("会话 cookie 必须 Secure，否则明文链路会泄漏")
	}
	if c.SameSite == http.SameSiteNoneMode || c.SameSite == 0 {
		t.Errorf("会话 cookie 必须设 SameSite，实得 %v", c.SameSite)
	}
}

// TestSec_ProtectedEndpointsRequireSession：除登录/注册外，
// **所有** /api/* 端点都必须要求会话。漏挂一个就是一个免鉴权入口。
func TestSec_ProtectedEndpointsRequireSession(t *testing.T) {
	mux, _ := newAuthTestServer(t)
	protected := []struct{ method, path string }{
		{http.MethodGet, "/api/auth/me"},
		{http.MethodPost, "/api/auth/logout"},
		{http.MethodGet, "/api/schedules"},
		{http.MethodPost, "/api/schedules"},
		{http.MethodDelete, "/api/schedules/abc"},
		{http.MethodGet, "/api/deliveries"},
		{http.MethodGet, "/api/profile"},
		{http.MethodGet, "/api/channels/delivery-preference"},
		{http.MethodPatch, "/api/channels/delivery-preference"},
		{http.MethodGet, "/api/admin/observability"},
		{http.MethodGet, "/api/admin/runstats"},
		{http.MethodGet, "/api/admin/cost-calls"},
		{http.MethodGet, "/api/admin/invites"},
		{http.MethodPost, "/api/admin/invites"},
		{http.MethodDelete, "/api/admin/invites/SOMECODE"},
		{http.MethodGet, "/api/admin/provider-prices"},
		{http.MethodPost, "/api/admin/provider-prices"},
		{http.MethodGet, "/api/admin/fetch/credentials"},
		{http.MethodPut, "/api/admin/fetch/credentials"},
		{http.MethodDelete, "/api/admin/fetch/credentials"},
		{http.MethodGet, "/api/admin/llm/credentials"},
		{http.MethodPut, "/api/admin/llm/credentials"},
		{http.MethodDelete, "/api/admin/llm/credentials"},
		{http.MethodGet, "/api/admin/execution-traces/users"},
		{http.MethodGet, "/api/admin/execution-traces/tenants/1/users/1/tasks"},
		{http.MethodGet, "/api/admin/execution-traces/tenants/1/users/1/tasks/task/runs"},
		{http.MethodGet, "/api/admin/execution-traces/tenants/1/users/1/tasks/task/runs/1"},
		{http.MethodGet, "/api/feishu/status"},
		{http.MethodPost, "/api/feishu/config"},
	}
	for _, p := range protected {
		t.Run(p.method+" "+p.path, func(t *testing.T) {
			req := httptest.NewRequest(p.method, p.path, strings.NewReader("{}"))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("无会话访问应 401，实得 %d —— 该端点疑似未受鉴权保护", rec.Code)
			}
		})
	}
}

// TestSec_RetiredScheduleDeliveryEndpointIsNotMounted locks the professional
// task surface to canonical runs, Briefs and reports. Item-level delivery rows
// remain an internal delivery/audit projection and the global history endpoint,
// but must not reappear as a second task-content API.
func TestSec_RetiredScheduleDeliveryEndpointIsNotMounted(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "retired-deliveries@example.com", "user-password-123", 1)
	login := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email": "retired-deliveries@example.com", "password": "user-password-123",
	}, nil)
	cookie := sessionCookieFrom(t, login)

	req := httptest.NewRequest(http.MethodGet,
		"/api/schedules/task-retired/deliveries", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("retired schedule deliveries status=%d want=404", rec.Code)
	}
}

// TestSec_ExpiredSessionRejected：过期会话不得放行。
func TestSec_ExpiredSessionRejected(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: 1, TenantID: 1, ExpiresAt: time.Now().Add(-time.Minute),
	}
	fake.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("过期会话应 401，实得 %d", rec.Code)
	}
}

// TestSec_ForgedTokenRejected：伪造/篡改的 token 一律无效。
func TestSec_ForgedTokenRejected(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "u@example.com", "u-password-1234", 1)
	login := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "u@example.com", "password": "u-password-1234"}, nil)
	valid := sessionCookieFrom(t, login)

	forged := []string{
		"",
		"random-garbage",
		valid.Value + "x",                // 尾部篡改
		valid.Value[:len(valid.Value)-1], // 截断
		strings.ToUpper(valid.Value),     // 变形
	}
	for _, v := range forged {
		req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: v})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("伪造 token %q 应 401，实得 %d", v, rec.Code)
		}
	}
}

// TestSec_PrincipalIsolation：会话解析出的 principal 必须是**该会话自己的**用户与租户。
// 串号即越权——A 的 cookie 拿到 B 的数据。
func TestSec_PrincipalIsolation(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "alice@example.com", "alice-password-123", 11)
	fake.addUser(t, "bob@example.com", "bob-password-12345", 22)

	loginAs := func(email, pw string) *http.Cookie {
		rec := postJSON(t, mux, "/api/auth/login",
			map[string]string{"email": email, "password": pw}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s 登录失败 %d: %s", email, rec.Code, rec.Body.String())
		}
		return sessionCookieFrom(t, rec)
	}
	meOf := func(c *http.Cookie) map[string]any {
		req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
		req.AddCookie(c)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var m map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
			t.Fatalf("解析 /me 响应失败: %s", rec.Body.String())
		}
		return m
	}

	a, b := meOf(loginAs("alice@example.com", "alice-password-123")),
		meOf(loginAs("bob@example.com", "bob-password-12345"))

	if a["tenant_id"] == b["tenant_id"] {
		t.Errorf("两个用户解析出同一租户，租户隔离失效: %v / %v", a, b)
	}
	if a["user_id"] == b["user_id"] {
		t.Errorf("两个用户解析出同一 user_id: %v / %v", a, b)
	}
	if got := fmt.Sprint(a["tenant_id"]); got != "11" {
		t.Errorf("alice 的租户应为 11，实得 %v", a["tenant_id"])
	}
	if got := fmt.Sprint(b["tenant_id"]); got != "22" {
		t.Errorf("bob 的租户应为 22，实得 %v", b["tenant_id"])
	}
}

// TestSec_RegisterRequiresValidInvite：I-A2 在 HTTP 层同样成立——
// 无有效邀请码不得注册。这是 D3「平台全垫付」的财务闸门。
func TestSec_RegisterRequiresValidInvite(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	for _, code := range []string{"", "bad-code"} {
		rec := postJSON(t, mux, "/api/auth/register", map[string]string{
			"email": "new@example.com", "password": "new-password-123", "invite_code": code,
		}, nil)
		if rec.Code == http.StatusOK {
			t.Errorf("邀请码 %q 不应注册成功", code)
		}
	}
	if fake.sessionCount() != 0 {
		t.Error("注册失败不得签发会话")
	}
}

// TestSec_WeakPasswordRejected：弱密码在**哈希之前**被拒（既给反馈，也避免
// 为不合规输入白跑一次 argon2）。
func TestSec_WeakPasswordRejected(t *testing.T) {
	mux, _ := newAuthTestServer(t)
	for _, pw := range []string{"", "short", "1234567"} {
		rec := postJSON(t, mux, "/api/auth/register", map[string]string{
			"email": "weak@example.com", "password": pw, "invite_code": "good-code",
		}, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("弱密码 %q 应 400，实得 %d", pw, rec.Code)
		}
	}
}

// TestSec_OversizedBodyRejected：认证端点无需鉴权，必须有请求体上限，
// 否则是免费的内存 DoS 面。
func TestSec_OversizedBodyRejected(t *testing.T) {
	mux, _ := newAuthTestServer(t)
	huge := `{"email":"a@b.com","password":"` + strings.Repeat("x", 64<<10) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(huge))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Error("超大请求体不应被接受")
	}
}

// TestSec_NoPasswordHashInResponses：任何响应体都不得出现密码哈希或会话 token。
// User 结构体上的 PasswordHash 带 json:"-"，本用例把它钉死。
func TestSec_NoPasswordHashInResponses(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	u := fake.addUser(t, "leak@example.com", "leak-password-123", 1)

	login := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "leak@example.com", "password": "leak-password-123"}, nil)
	cookie := sessionCookieFrom(t, login)
	me := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	me.AddCookie(cookie)
	merec := httptest.NewRecorder()
	mux.ServeHTTP(merec, me)

	for _, body := range []string{login.Body.String(), merec.Body.String()} {
		if strings.Contains(body, "argon2") || strings.Contains(body, *u.PasswordHash) {
			t.Errorf("响应体泄漏密码哈希: %s", body)
		}
		if strings.Contains(body, cookie.Value) {
			t.Errorf("响应体泄漏会话 token: %s", body)
		}
	}
}

// TestSec_LoginRateLimited：登录失败必须计入限流，挡在线爆破。
func TestSec_LoginRateLimited(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "brute@example.com", "brute-password-123", 1)

	var got429 bool
	for i := 0; i < 40; i++ {
		rec := postJSON(t, mux, "/api/auth/login",
			map[string]string{"email": "brute@example.com", "password": "wrong"}, nil)
		if rec.Code == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}
	if !got429 {
		t.Error("连续失败登录未触发限流 —— 在线爆破无阻拦")
	}
}

// ============================================================
// 对抗式安全审查后补的用例（审查员实测发现的攻击面）
// ============================================================

// TestSec_SuccessfulLoginIsRateLimited 钉住 CRITICAL 发现的核心：
// **登录成功也必须计入限流**。
//
// 原实现只在失败时记账，于是持一份有效凭据即可无限次登录，
// 每次跑一遍 19MiB 的 argon2——限流器全程放行，一个 429 都不会发。
// 这不是猜密码攻击，是把认证端点当成内存 DoS 放大器。
func TestSec_SuccessfulLoginIsRateLimited(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.addUser(t, "flood@example.com", "flood-password-123", 1)

	var got429 bool
	for i := 0; i < 60; i++ {
		rec := postJSON(t, mux, "/api/auth/login",
			map[string]string{"email": "flood@example.com", "password": "flood-password-123"}, nil)
		if rec.Code == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}
	if !got429 {
		t.Error("连续成功登录未触发限流 —— 有效凭据即可无限触发 argon2，构成内存 DoS 放大器")
	}
}

// TestSec_RateLimitNotBypassedByIPv6Rotation：限流键按 IPv6 /64 归一。
// 一个普通 VPS 用户即持有 2^64 个源地址，按完整地址计数等于人人自带无限额度。
func TestSec_RateLimitNotBypassedByIPv6Rotation(t *testing.T) {
	l := newAuthLimiter()
	now := time.Now()
	// 同一 /64 内换 200 个源地址。
	var allowed int
	for i := 0; i < 200; i++ {
		r := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
		r.RemoteAddr = fmt.Sprintf("[2001:db8:1:1::%x]:443", i+1)
		if l.allowAndRecord(ipLimitKey(r), now) {
			allowed++
		}
	}
	if allowed > l.max {
		t.Errorf("同一 /64 内轮换源地址放行了 %d 次（上限应为 %d）—— 限流可被 IPv6 轮换绕过", allowed, l.max)
	}
	// 不同 /64 之间必须仍然独立计数（不能误伤正常用户）。
	r := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
	r.RemoteAddr = "[2001:db8:2:2::1]:443"
	if !l.allowAndRecord(ipLimitKey(r), now) {
		t.Error("不同 /64 应独立计数，不应被别人的额度连累")
	}
}

// TestSec_PerAccountRateLimit：分布式来源对**单个账号**的猜测同样要有上限。
func TestSec_PerAccountRateLimit(t *testing.T) {
	l := newAuthLimiter()
	now := time.Now()
	var allowed int
	for i := 0; i < 100; i++ {
		r := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
		r.RemoteAddr = fmt.Sprintf("10.%d.%d.%d:443", i/256, i%256, i%251+1) // 每次换 IP
		if l.allowAndRecord(accountLimitKey("victim@example.com"), now) {
			allowed++
		}
	}
	if allowed > l.max {
		t.Errorf("换 IP 对同一账号猜了 %d 次（上限 %d）—— 只按 IP 限流挡不住分布式猜测", allowed, l.max)
	}
}

// TestSec_LimiterSweepsStaleKeys：限流器必须能全局回收过期键。
// 原实现只在「该键被再次触碰」时清理，而攻击者每次换新地址，旧键永久驻留。
func TestSec_LimiterSweepsStaleKeys(t *testing.T) {
	l := newAuthLimiter()
	base := time.Now()
	for i := 0; i < 500; i++ {
		r := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
		r.RemoteAddr = fmt.Sprintf("10.1.%d.%d:443", i/256, i%256)
		l.allowAndRecord(ipLimitKey(r), base)
	}
	if len(l.attempts) < 400 {
		t.Fatalf("脚手架异常：应积累约 500 个键，实得 %d", len(l.attempts))
	}
	// 窗口之后来一个无关请求，触发全局清扫。
	r := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
	r.RemoteAddr = "10.99.99.99:443"
	l.allowAndRecord(ipLimitKey(r), base.Add(2*time.Minute))
	if len(l.attempts) > 10 {
		t.Errorf("过期键未被回收，仍驻留 %d 条 —— map 会随攻击者轮换地址无限膨胀", len(l.attempts))
	}
}

// TestSec_RegisterValidatesInviteBeforeHashing 钉住 CRITICAL 的匿名可达面：
// 注册必须**先验邀请码再算 argon2**，否则匿名攻击者用伪造码即可反复触发
// 19MiB 的昂贵计算，把未鉴权端点变成内存 DoS 放大器。
func TestSec_RegisterValidatesInviteBeforeHashing(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	var hashedBefore int
	fake.onInviteCheck = func() { hashedBefore = 0 } // 预检发生时哈希还没做

	rec := postJSON(t, mux, "/api/auth/register", map[string]string{
		"email": "x@example.com", "password": "valid-password-123", "invite_code": "bad-code",
	}, nil)
	if rec.Code == http.StatusOK {
		t.Fatal("无效邀请码不应注册成功")
	}
	if hashedBefore != 0 {
		t.Error("邀请码预检应先于密码哈希")
	}
	if !fake.inviteChecked {
		t.Error("未调用邀请码预检 —— 无效码会白跑一次 19MiB argon2")
	}
}

// TestSec_PlatformEndpointsGatedToOwner 钉住 CRITICAL：
// 平台级端点（全局飞书凭证、全库统计）不得对普通租户开放。
//
// 原状态下，任一受邀用户即可 POST /api/feishu/config 覆写全局飞书凭证，
// 把机器人指向自己的应用 —— **劫持所有租户的推送通道**，跨租户完全沦陷。
func TestSec_PlatformEndpointsGatedToOwner(t *testing.T) {
	platformOnly := []struct{ method, path string }{
		{http.MethodGet, "/api/feishu/status"},
		{http.MethodPost, "/api/feishu/config"},
		{http.MethodPost, "/api/feishu/verify"},
		{http.MethodPost, "/api/feishu/test"},
		{http.MethodGet, "/api/admin/observability"},
		{http.MethodGet, "/api/admin/runstats"},
		{http.MethodGet, "/api/admin/cost-calls"},
		// 邀请码管理：能发码 = 能凭空创造「注册并消耗平台预算的资格」（I-A2），
		// 比读统计的危害更直接，绝不能漏出平台 owner 闸门。
		{http.MethodGet, "/api/admin/invites"},
		{http.MethodPost, "/api/admin/invites"},
		{http.MethodDelete, "/api/admin/invites/SOMECODE"},
		{http.MethodGet, "/api/admin/provider-prices"},
		{http.MethodPost, "/api/admin/provider-prices"},
		{http.MethodGet, "/api/admin/execution-traces/users"},
		{http.MethodGet, "/api/admin/execution-traces/tenants/1/users/1/tasks"},
		{http.MethodGet, "/api/admin/execution-traces/tenants/1/users/1/tasks/task/runs"},
		{http.MethodGet, "/api/admin/execution-traces/tenants/1/users/1/tasks/task/runs/1"},
	}
	// 普通租户（非平台 owner）：一律不可见。
	mux, cookie := authzMux(t, 555, 555, nil)
	for _, p := range platformOnly {
		t.Run("普通租户 "+p.path, func(t *testing.T) {
			r := httptest.NewRequest(p.method, p.path, strings.NewReader("{}"))
			r.AddCookie(cookie)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusNotFound {
				t.Errorf("普通租户访问平台端点应 404，实得 %d —— 可劫持全局配置或读走全平台数据", w.Code)
			}
		})
	}
}

func TestSec_PlatformTenantMemberIsNotSuperAdministrator(t *testing.T) {
	fake := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: 77, TenantID: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	fake.members[77] = []types.Membership{{
		TenantID: 1, UserID: 77, Role: types.MembershipRoleMember,
	}}
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: fake, Principal: auth.NewContextResolver()})
	request := httptest.NewRequest(http.MethodGet, "/api/admin/llm/credentials", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("platform tenant member got %d, want hidden 404", response.Code)
	}
}

// TestSec_MultiTenantLoginFailsLoudly：用户属于多个租户时，登录必须**报错**而非
// 静默登进第一个。
//
// 原实现 `return ms[0].TenantID` 会让这样的人进入「碰巧排在前面」的租户——
// 他看到一份陌生的信源与推送历史，却没有任何提示说明发生了什么。
// 与写入侧（store 的推导子查询报「多行」）和 a2a/gate（ownerResolver）三处对齐。
func TestSec_MultiTenantLoginFailsLoudly(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	u := fake.addUser(t, "multi@example.com", "multi-password-123", 11)
	// 再给同一个人加一条别的租户成员关系。
	fake.mu.Lock()
	fake.members[u.ID] = append(fake.members[u.ID],
		types.Membership{TenantID: 22, UserID: u.ID, Role: types.MembershipRoleOwner})
	fake.mu.Unlock()

	rec := postJSON(t, mux, "/api/auth/login",
		map[string]string{"email": "multi@example.com", "password": "multi-password-123"}, nil)
	if rec.Code == http.StatusOK {
		t.Fatal("属于多个租户时不应登录成功 —— 会静默进入某一个租户")
	}
	if fake.sessionCount() != 0 {
		t.Error("失败路径不得签发会话")
	}
	if !strings.Contains(errMsgOf(t, rec), "多个租户") {
		t.Errorf("错误文案应点明多租户，实得 %q", errMsgOf(t, rec))
	}
}

func TestSessionPersistenceFailureDoesNotWriteSuccess(t *testing.T) {
	mux, fake := newAuthTestServer(t)
	fake.createErr = errors.New("session store unavailable")
	register := postJSON(t, mux, "/api/auth/register", map[string]string{
		"email":       "register-session-fail@example.com",
		"password":    "valid-password-123",
		"invite_code": "valid-invite",
	}, nil)
	if register.Code != http.StatusInternalServerError ||
		strings.Contains(register.Body.String(), `"ok":true`) {
		t.Fatalf("register status=%d body=%s", register.Code, register.Body.String())
	}

	fake.addUser(t, "login-session-fail@example.com", "valid-password-123", 1)
	login := postJSON(t, mux, "/api/auth/login", map[string]string{
		"email":    "login-session-fail@example.com",
		"password": "valid-password-123",
	}, nil)
	if login.Code != http.StatusInternalServerError ||
		strings.Contains(login.Body.String(), `"ok":true`) {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
}
