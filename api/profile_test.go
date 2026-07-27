package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const profilePath = "/api/profile"

// newProfileMux 起一个挂好路由的 mux 与一张有效会话 cookie。
// Deps.Store 刻意留 nil（同 newObsMux）：本文件只走碰不到 Store 的路径——
// handleProfile 在 requireSession 放行后才碰 Store，故只测未授权被挡住的分支，
// 带真 owner+画像的 200/404 路径需真 Postgres，按本包既定纪律不 mock（见
// observability_test.go 头注释：Deps.Store 是具体类型，不为测试改成接口）。
func newProfileMux(t *testing.T) (*http.ServeMux, *http.Cookie) {
	t.Helper()
	mux := http.NewServeMux()
	deps, cookie := authedDeps(t, Deps{})
	Mount(mux, deps)
	return mux, cookie
}

func TestProfileWritesRejectMalformedRequestsBeforeStore(t *testing.T) {
	mux, cookie := newProfileMux(t)
	tests := []struct {
		name, path, body, key string
	}{
		{"missing idempotency", profilePath, `{"expected_updated_at":null,"industry":"AI"}`, ""},
		{"unknown summary", profilePath, `{"expected_updated_at":null,"summary":"forbidden"}`, "profile-1"},
		{"removed tags read only", profilePath, `{"expected_updated_at":null,"removed_tags":[]}`, "profile-ro"},
		{"industry null", profilePath, `{"expected_updated_at":null,"industry":null,"occupation":"x"}`, "profile-null-1"},
		{"tags null", profilePath, `{"expected_updated_at":null,"tags":null,"occupation":"x"}`, "profile-null-2"},
		{"duplicate key", profilePath, `{"expected_updated_at":null,"industry":"A","industry":"B"}`, "profile-dup"},
		{"exact case", profilePath, `{"expected_updated_at":null,"Industry":"A"}`, "profile-case"},
		{"missing expected", profilePath, `{"industry":"AI"}`, "profile-2"},
		{"empty patch", profilePath, `{"expected_updated_at":null}`, "profile-3"},
		{"trailing JSON", profilePath, `{"expected_updated_at":null,"industry":"AI"} {}`, "profile-4"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPatch, tc.path, bytes.NewBufferString(tc.body))
			r.AddCookie(cookie)
			if tc.key != "" {
				r.Header.Set("Idempotency-Key", tc.key)
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if strings.Contains(tc.name, "null") &&
				!strings.Contains(w.Body.String(), "不能为 null") {
				t.Fatalf("null must be rejected specifically: %s", w.Body.String())
			}
		})
	}
}

func TestProfileHistoryRejectsUnknownQueryAndInvalidUndoID(t *testing.T) {
	mux, cookie := newProfileMux(t)
	r := httptest.NewRequest(http.MethodGet, profilePath+"/edits?cursor=secret", nil)
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown query status=%d", w.Code)
	}

	r = httptest.NewRequest(
		http.MethodPost, profilePath+"/edits/not-an-id/undo",
		bytes.NewBufferString(`{"expected_updated_at":"2026-07-27T00:00:00Z"}`))
	r.AddCookie(cookie)
	r.Header.Set("Idempotency-Key", "undo-invalid")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid id status=%d", w.Code)
	}
}

func TestProfileClaimActionsRejectMalformedRequestsBeforeStore(t *testing.T) {
	mux, cookie := newProfileMux(t)
	path := profilePath + "/claims/actions"
	tests := []struct {
		name, body, key string
	}{
		{"missing idempotency", `{"expected_version":0,"action":"pin","claim_id":"1"}`, ""},
		{"missing version", `{"action":"pin","claim_id":"1"}`, "claim-1"},
		{"unknown action", `{"expected_version":0,"action":"delete","claim_id":"1"}`, "claim-2"},
		{"summary smuggling", `{"expected_version":0,"action":"pin","claim_id":"1","summary":"x"}`, "claim-3"},
		{"pin value", `{"expected_version":0,"action":"pin","claim_id":"1","value":"x"}`, "claim-4"},
		{"correct empty", `{"expected_version":0,"action":"correct","claim_id":"1","value":" "}`, "claim-5"},
		{"revoke claim", `{"expected_version":0,"action":"revoke","claim_id":"1","event_id":"2"}`, "claim-6"},
		{"duplicate action", `{"expected_version":0,"action":"pin","action":"suppress","claim_id":"1"}`, "claim-7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(tc.body))
			r.AddCookie(cookie)
			if tc.key != "" {
				r.Header.Set("Idempotency-Key", tc.key)
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestProfileClaimsPaginationRejectsInvalidQueryBeforeStore(t *testing.T) {
	mux, cookie := newProfileMux(t)
	for _, rawQuery := range []string{
		"unknown=1",
		"event_limit=0",
		"event_limit=51",
		"event_limit=abc",
		"event_limit=20&event_limit=21",
		"event_cursor=",
		"event_cursor=a&event_cursor=b",
	} {
		r := httptest.NewRequest(
			http.MethodGet, profilePath+"/claims?"+rawQuery, nil)
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("query=%q status=%d body=%s",
				rawQuery, w.Code, w.Body.String())
		}
	}
}

func TestProfileClaimEventPageOptionsDefaultWithoutQuery(t *testing.T) {
	options, validationMessage := parseProfileClaimEventPageOptions(nil)
	if validationMessage != "" {
		t.Fatalf("parameterless GET rejected: %s", validationMessage)
	}
	if options.Limit != 20 || options.Cursor != "" {
		t.Fatalf("parameterless GET options=%+v", options)
	}

	options, validationMessage = parseProfileClaimEventPageOptions(
		map[string][]string{
			"event_limit":  {"37"},
			"event_cursor": {"opaque/current"},
		},
	)
	if validationMessage != "" {
		t.Fatalf("explicit pagination rejected: %s", validationMessage)
	}
	if options.Limit != 37 || options.Cursor != "opaque/current" {
		t.Fatalf("explicit pagination options=%+v", options)
	}
}

func TestCanonicalizeProfilePatch(t *testing.T) {
	industry := "  AI 应用  "
	occupation := " 独立开发者 "
	tags := []string{" Go ", "Go", "AI"}
	got, err := canonicalizeProfilePatch(patchProfileRequest{
		Industry:   mustProfileRaw(t, industry),
		Occupation: mustProfileRaw(t, occupation),
		Tags:       mustProfileRaw(t, tags),
	})
	if err != nil {
		t.Fatal(err)
	}
	if *got.Industry != "AI 应用" || *got.Occupation != "独立开发者" {
		t.Fatalf("trim failed: %+v", got)
	}
	if len(*got.Tags) != 2 || (*got.Tags)[0] != "Go" || (*got.Tags)[1] != "AI" {
		t.Fatalf("tags=%v", *got.Tags)
	}
}

func TestCanonicalizeProfilePatchRejectsInvalidTags(t *testing.T) {
	for _, tags := range [][]string{
		{" "}, {"line\nbreak"}, {"tab\tinside"}, {"control\u0001"},
		{"line\u2028separator"}, {"paragraph\u2029separator"},
		{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"},
	} {
		if _, err := canonicalizeProfilePatch(
			patchProfileRequest{Tags: mustProfileRaw(t, tags)}); err == nil {
			t.Fatalf("tags=%q should fail", tags)
		}
	}
	duplicates := []string{
		"A", " A ", "A", "A", "A", "A", "A",
		"A", "A", "A", "A", "A", "A",
	}
	got, err := canonicalizeProfilePatch(
		patchProfileRequest{Tags: mustProfileRaw(t, duplicates)})
	if err != nil || len(*got.Tags) != 1 || (*got.Tags)[0] != "A" {
		t.Fatalf("dedupe after normalize=%v err=%v", got.Tags, err)
	}
}

func mustProfileRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestProfileRequiresSession 验证 GET /api/profile 受会话中间件保护，
// 并顺带证明路由确实挂上了（没挂 ServeMux 会回 404 而非 401）。
// 画像页是"零新增鉴权面"，前提就是它老实待在 /api/ 前缀下继承会话中间件。
func TestProfileRequiresSession(t *testing.T) {
	mux, _ := newProfileMux(t)

	// 无 cookie：requireSession 在碰 Store 前就回 401。
	r := httptest.NewRequest(http.MethodGet, profilePath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("未登录状态码 = %d, 期望 401", w.Code)
	}

	// 伪造 cookie 同样挡住（签名过不了）。
	r = httptest.NewRequest(http.MethodGet, profilePath, nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "forged.token"})
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("伪造 token 状态码 = %d, 期望 401", w.Code)
	}
}

// TestProfileRouteMounted 用一张有效 cookie 证明路由已挂载：请求能穿过会话中间件
// 到达 handler（不再是 401）。Store 为 nil，handler 走到 ownerUserID 会 panic，
// 故用 recover 捕获——能 panic 恰恰说明已过中间件、进了 handleProfile；
// 若路由没挂，ServeMux 会回 404 且不 panic，测试据此失败。
func TestProfileRouteMounted(t *testing.T) {
	mux, cookie := newProfileMux(t)

	defer func() {
		if recover() == nil {
			t.Fatal("期望 handler 因 Store=nil 而 panic（证明已过中间件进入 handleProfile），却未 panic——路由可能未挂载")
		}
	}()

	r := httptest.NewRequest(http.MethodGet, profilePath, nil)
	r.AddCookie(cookie)
	mux.ServeHTTP(httptest.NewRecorder(), r)
}
