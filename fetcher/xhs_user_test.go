package fetcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// sampleXHSUserResponse 是 get_user_posted_notes 的成功响应样本（按 2026-07-17 实测结构裁剪）：
// 两条同一用户的笔记，note 直接在 data.data.notes[]，create_time 为 Unix 秒，无 xsec_token。
const sampleXHSUserResponse = `{
  "code": 200,
  "data": {
    "code": 0,
    "success": true,
    "msg": "success",
    "data": {
      "notes": [
        {
          "id": "6a5a501d000000000f03235e",
          "title": "AI编程，先别急着付费",
          "display_title": "AI编程，先别急着付费",
          "desc": "建议初学者先使用免费AI编程工具，积累经验后再考虑付费工具。",
          "create_time": 1784303645,
          "type": "normal",
          "user": {"userid": "6a5578b3000000000e03cc00", "nickname": "青木"}
        },
        {
          "id": "6a5a23680000000011006ef3",
          "title": "",
          "display_title": "零基础AI编程的三个核心步骤",
          "desc": "第一步理解需求，第二步拆解模块，第三步交给AI实现。",
          "create_time": 1784292200,
          "type": "video",
          "user": {"userid": "6a5578b3000000000e03cc00", "nickname": "青木"}
        }
      ],
      "tags": [],
      "has_more": false
    }
  }
}`

// newTestXHSUser 构造一个指向 httptest 服务端的 XHSUserFetcher。
func newTestXHSUser(baseURL string) *XHSUserFetcher {
	f := NewXHSUser(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, TikhubAPIKey: "k"})
	f.baseURL = baseURL
	return f
}

func xhsUserSource(userID string) types.Source {
	cfg, _ := json.Marshal(map[string]string{"user_id": userID})
	return types.Source{ID: 7, Platform: types.PlatformXHS, Capability: types.CapUserPosts, Config: cfg}
}

func TestXHSUser_Fetch_MapsNotes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 断言参数正确传递。
		if got := r.URL.Query().Get("user_id"); got != "6a5578b3000000000e03cc00" {
			t.Errorf("user_id 未透传，实际 %q", got)
		}
		if _, ok := r.URL.Query()["cursor"]; !ok {
			t.Error("缺 cursor 参数")
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer k" {
			t.Errorf("鉴权头错误：%q", auth)
		}
		_, _ = w.Write([]byte(sampleXHSUserResponse))
	}))
	defer srv.Close()

	f := newTestXHSUser(srv.URL)
	items, err := f.Fetch(context.Background(), xhsUserSource("6a5578b3000000000e03cc00"))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("期望 2 条，实际 %d", len(items))
	}

	// 第 1 条：title 非空、URL、身份、时间、种类。
	it := items[0]
	if it.ExternalID != "6a5a501d000000000f03235e" {
		t.Errorf("ExternalID=%q", it.ExternalID)
	}
	if it.CanonicalKey != "6a5a501d000000000f03235e" {
		t.Errorf("CanonicalKey 应为裸 note_id（与 search 同源以便跨源去重），实际 %q", it.CanonicalKey)
	}
	if it.URL != "https://www.xiaohongshu.com/explore/6a5a501d000000000f03235e" {
		t.Errorf("URL=%q", it.URL)
	}
	if strings.Contains(it.URL, "xsec_token") {
		t.Error("user_posts 不应拼 xsec_token（拿不到）")
	}
	if it.Title != "AI编程，先别急着付费" {
		t.Errorf("Title=%q", it.Title)
	}
	if it.Author != "青木" {
		t.Errorf("Author=%q", it.Author)
	}
	if it.Kind != types.KindArticle {
		t.Errorf("Kind 应为 article，实际 %q", it.Kind)
	}
	if it.PublishedAt == nil || it.PublishedAt.Unix() != 1784303645 {
		t.Errorf("PublishedAt 应为 Unix 秒 1784303645，实际 %v", it.PublishedAt)
	}
	if it.Simhash == nil || *it.Simhash == 0 {
		t.Error("finalize 应已算好 simhash")
	}

	// 第 2 条：title 空时回退 display_title。
	if items[1].Title != "零基础AI编程的三个核心步骤" {
		t.Errorf("title 空应回退 display_title，实际 %q", items[1].Title)
	}
}

func TestXHSUser_Fetch_MissingUserID(t *testing.T) {
	f := newTestXHSUser("http://unused.invalid")
	src := types.Source{ID: 7, Platform: types.PlatformXHS, Capability: types.CapUserPosts, Config: json.RawMessage(`{}`)}
	_, err := f.Fetch(context.Background(), src)
	if types.CodeOf(err) != types.CodeValidation {
		t.Errorf("缺 user_id 应判 CodeValidation，实际 %v", err)
	}
}

func TestXHSUser_Fetch_MissingKey(t *testing.T) {
	f := NewXHSUser(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}) // 无 key
	f.baseURL = "http://unused.invalid"
	_, err := f.Fetch(context.Background(), xhsUserSource("u1"))
	if types.CodeOf(err) != types.CodeValidation {
		t.Errorf("缺 key 应判 CodeValidation，实际 %v", err)
	}
}

func TestXHSUser_Fetch_BusinessFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":{"success":false,"msg":"用户不存在","data":{"notes":[]}}}`))
	}))
	defer srv.Close()
	f := newTestXHSUser(srv.URL)
	_, err := f.Fetch(context.Background(), xhsUserSource("u1"))
	if err == nil {
		t.Fatal("success=false 应返回错误")
	}
	if types.IsRetryable(err) {
		t.Error("业务失败应按确定性不可重试")
	}
}

func TestXHSUser_Fetch_RateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	f := newTestXHSUser(srv.URL)
	_, err := f.Fetch(context.Background(), xhsUserSource("u1"))
	if types.CodeOf(err) != types.CodeFetchRateLimit {
		t.Errorf("429 应判 CodeFetchRateLimit，实际 %v", err)
	}
	if !types.IsRetryable(err) {
		t.Error("429 应可重试")
	}
}

func TestXHSUser_Fetch_AuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	f := newTestXHSUser(srv.URL)
	_, err := f.Fetch(context.Background(), xhsUserSource("u1"))
	if types.CodeOf(err) != types.CodeValidation {
		t.Errorf("401 应判 CodeValidation（配置问题，不可重试），实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("401 不应可重试")
	}
}

// TestXHSUser_Fetch_5xxRetryable：非鉴权/限流的 5xx 视为瞬态可重试。
func TestXHSUser_Fetch_5xxRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	f := newTestXHSUser(srv.URL)
	_, err := f.Fetch(context.Background(), xhsUserSource("u1"))
	if !types.IsRetryable(err) {
		t.Errorf("5xx 应可重试，实际 %v", err)
	}
}

// TestXHSUser_SkipsMismatchedAuthor：作者 userid 与所订 user_id 不符的笔记被丢弃（防串号）。
func TestXHSUser_SkipsMismatchedAuthor(t *testing.T) {
	body := `{"code":200,"data":{"success":true,"data":{"notes":[
      {"id":"aaaa000000000000000000aa","title":"本人笔记","desc":"x","create_time":1784303645,"user":{"userid":"me","nickname":"我"}},
      {"id":"bbbb000000000000000000bb","title":"别人笔记","desc":"y","create_time":1784303645,"user":{"userid":"someone_else","nickname":"他"}}
    ]}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	f := newTestXHSUser(srv.URL)
	items, err := f.Fetch(context.Background(), xhsUserSource("me"))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 1 || items[0].ExternalID != "aaaa000000000000000000aa" {
		t.Fatalf("应只保留所订用户的 1 条，实际 %d 条: %+v", len(items), items)
	}
}

// TestXHSUser_SkipsEmptyNoteID：无 note_id 的笔记（无身份）被丢弃。
func TestXHSUser_SkipsEmptyNoteID(t *testing.T) {
	body := `{"code":200,"data":{"success":true,"data":{"notes":[
      {"id":"","title":"无身份","desc":"x","create_time":1784303645,"user":{"userid":"me"}},
      {"id":"cccc000000000000000000cc","title":"正常","desc":"y","create_time":1784303645,"user":{"userid":"me"}}
    ]}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	f := newTestXHSUser(srv.URL)
	items, err := f.Fetch(context.Background(), xhsUserSource("me"))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 1 || items[0].ExternalID != "cccc000000000000000000cc" {
		t.Fatalf("应丢弃无 note_id 的条目，只留 1 条，实际 %d", len(items))
	}
}
