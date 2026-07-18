package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// newTestExaContents 构造指向 httptest.Server 的 ExaContentsFetcher。
func newTestExaContents(srvURL string) *ExaContentsFetcher {
	e := NewExaContents(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, ExaAPIKey: "test-key"})
	e.contentURL = srvURL
	return e
}

func contentsSource(cfg string) types.Source {
	return types.Source{
		ID: 11, Platform: types.PlatformWeb, Capability: types.CapContents,
		Config: json.RawMessage(cfg),
	}
}

// contentsServer 返回一个吐固定 body 的 httptest.Server，并记录收到的请求体。
func contentsServer(t *testing.T, status int, body string, gotReq *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotReq != nil {
			b := make([]byte, r.ContentLength)
			r.Body.Read(b)
			*gotReq = string(b)
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
}

// TestExaContents_产出与身份 正常抓取产出一条，canonical_key = contents://url#hash。
func TestExaContents_产出与身份(t *testing.T) {
	body := `{"requestId":"r1","results":[{"id":"ex1","url":"https://x.com/pricing","title":"Pricing","text":"gpt-5 | $5 | $30"}],"statuses":[{"status":"success","source":"crawled"}]}`
	var gotReq string
	srv := contentsServer(t, 200, body, &gotReq)
	defer srv.Close()

	e := newTestExaContents(srv.URL)
	items, err := e.Fetch(context.Background(), contentsSource(`{"url":"https://x.com/pricing"}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("应产出 1 条，实得 %d", len(items))
	}
	it := items[0]
	if !strings.HasPrefix(it.CanonicalKey, "contents://https://x.com/pricing#") {
		t.Errorf("canonical_key 应为 contents://<url>#<hash>，实得 %q", it.CanonicalKey)
	}
	if it.Title != "Pricing" || it.Kind != types.KindPageContent || it.URL != "https://x.com/pricing" {
		t.Errorf("字段映射不符（Kind 必须是 page_content，否则 Dedup 会吞掉变化）: %+v", it)
	}
	// 请求体：maxAgeHours:0 强制活抓（漏了它监控会静默失效）。
	if !strings.Contains(gotReq, `"maxAgeHours":0`) {
		t.Errorf("请求体必须含 maxAgeHours:0，实得 %s", gotReq)
	}
}

// TestExaContents_变化才换身份 同内容→同 canonical_key（去重），内容变→新 key（产出）。
func TestExaContents_变化才换身份(t *testing.T) {
	mk := func(text string) string {
		return `{"results":[{"id":"i","url":"https://x.com/p","title":"P","text":"` + text + `"}],"statuses":[{"status":"success","source":"crawled"}]}`
	}
	key := func(t *testing.T, text string) string {
		srv := contentsServer(t, 200, mk(text), nil)
		defer srv.Close()
		items, err := newTestExaContents(srv.URL).Fetch(context.Background(), contentsSource(`{"url":"https://x.com/p"}`))
		if err != nil || len(items) != 1 {
			t.Fatalf("Fetch 异常: %v len=%d", err, len(items))
		}
		return items[0].CanonicalKey
	}
	k1 := key(t, "price 30")
	k1again := key(t, "price 30")
	k2 := key(t, "price 24")
	if k1 != k1again {
		t.Errorf("相同内容应算出相同 canonical_key（去重靠它）：%q vs %q", k1, k1again)
	}
	if k1 == k2 {
		t.Error("内容变化应算出不同 canonical_key（变化才产出新内容）")
	}
}

// TestExaContents_statuses_error 抓取失败是 HTTP 200 + status=error → 报错而非静默 0 条（审计 D6）。
func TestExaContents_statuses_error(t *testing.T) {
	body := `{"results":[],"statuses":[{"status":"error","source":"crawled"}]}`
	srv := contentsServer(t, 200, body, nil)
	defer srv.Close()

	_, err := newTestExaContents(srv.URL).Fetch(context.Background(), contentsSource(`{"url":"https://x.com/p"}`))
	if err == nil {
		t.Fatal("statuses.status=error 必须报错，不能当成内容为空静默返回")
	}
	var ae *types.AppError
	if !errors.As(err, &ae) || !ae.Retryable {
		t.Errorf("抓取失败应是可重试的瞬态错误，实得 %v", err)
	}
}

// TestExaContents_空正文 results 有但 text 空 / results 空且无 error → 空结果（不报错，下轮再抓）。
func TestExaContents_空正文(t *testing.T) {
	for _, body := range []string{
		`{"results":[{"id":"i","url":"https://x.com/p","title":"P","text":""}],"statuses":[{"status":"success","source":"crawled"}]}`,
		`{"results":[],"statuses":[{"status":"success","source":"crawled"}]}`,
	} {
		srv := contentsServer(t, 200, body, nil)
		items, err := newTestExaContents(srv.URL).Fetch(context.Background(), contentsSource(`{"url":"https://x.com/p"}`))
		srv.Close()
		if err != nil {
			t.Errorf("空正文不应报错，实得 %v", err)
		}
		if len(items) != 0 {
			t.Errorf("空正文应产出 0 条，实得 %d", len(items))
		}
	}
}

// TestExaContents_config校验 缺 key / 缺 url → CodeValidation。
func TestExaContents_config校验(t *testing.T) {
	// 缺 key
	e := NewExaContents(config.FetchConfig{}) // ExaAPIKey 空
	if _, err := e.Fetch(context.Background(), contentsSource(`{"url":"https://x.com/p"}`)); err == nil {
		t.Error("缺 API key 应报错")
	}
	// 缺 url
	srv := contentsServer(t, 200, `{}`, nil)
	defer srv.Close()
	if _, err := newTestExaContents(srv.URL).Fetch(context.Background(), contentsSource(`{}`)); err == nil {
		t.Error("缺 url 应报错")
	}
}

// TestExaContents_title覆盖 config.title 覆盖 Exa 返回的标题。
func TestExaContents_title覆盖(t *testing.T) {
	body := `{"results":[{"id":"i","url":"https://x.com/p","title":"Exa Title","text":"body"}],"statuses":[{"status":"success","source":"crawled"}]}`
	srv := contentsServer(t, 200, body, nil)
	defer srv.Close()
	items, err := newTestExaContents(srv.URL).Fetch(context.Background(),
		contentsSource(`{"url":"https://x.com/p","title":"我的定价监控"}`))
	if err != nil || len(items) != 1 {
		t.Fatalf("Fetch 异常: %v", err)
	}
	if items[0].Title != "我的定价监控" {
		t.Errorf("config.title 应覆盖 Exa 标题，实得 %q", items[0].Title)
	}
}

// TestExaContents_HTML净化不被拒 正文含 "<字母"（比较符/代码）不该被 finalize 的
// htmlTagRe 静默拒收（对抗审查发现 3）——净化成 "< 字母" 后正常产出。
func TestExaContents_HTML净化不被拒(t *testing.T) {
	// "<div"、"</table" 会命中 htmlTagRe（< 紧跟字母/斜杠）→ 未净化则 finalize 静默拒收。
	// "<10ms"（< 紧跟数字）本就不命中，不需净化、也不该被拒。
	body := `{"results":[{"id":"i","url":"https://x.com/p","title":"P","text":"延迟 <10ms，代码 <div>x</table> 示例，价格 30"}],"statuses":[{"status":"success","source":"crawled"}]}`
	srv := contentsServer(t, 200, body, nil)
	defer srv.Close()
	items, err := newTestExaContents(srv.URL).Fetch(context.Background(), contentsSource(`{"url":"https://x.com/p"}`))
	if err != nil {
		t.Fatalf("含 < 的正文不应报错: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("含 <字母 的正文应净化后产出 1 条（不被 htmlTagRe 静默拒），实得 %d", len(items))
	}
	if strings.Contains(items[0].Content, "<div") || strings.Contains(items[0].Content, "</table") {
		t.Errorf("< 紧跟字母/斜杠应被净化成 < X，实得 %q", items[0].Content)
	}
}

// TestExaContents_身份只随监控区变 截断区（前 N 字节）之外的尾部噪音变化不翻转
// canonical_key（对抗审查发现 2）——hash 用截断+净化后的同一份文本，只随监控区变。
func TestExaContents_身份只随监控区变(t *testing.T) {
	head := strings.Repeat("价格表 gpt-5 30 60 缓存 3。", 300) // 远超 4000 字节，构成监控区主体
	keyFor := func(tail string) string {
		text := head + " 页脚：" + tail // 尾部噪音在截断区之外
		body := `{"results":[{"id":"i","url":"https://x.com/p","title":"P","text":` +
			mustJSON(text) + `}],"statuses":[{"status":"success","source":"crawled"}]}`
		srv := contentsServer(t, 200, body, nil)
		defer srv.Close()
		items, err := newTestExaContents(srv.URL).Fetch(context.Background(), contentsSource(`{"url":"https://x.com/p"}`))
		if err != nil || len(items) != 1 {
			t.Fatalf("Fetch 异常: %v len=%d", err, len(items))
		}
		return items[0].CanonicalKey
	}
	if keyFor("最后更新 10:00") != keyFor("最后更新 20:00") {
		t.Error("截断区之外的尾部噪音变化不应翻转 canonical_key（否则页脚每变都推送）")
	}
}

func mustJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
