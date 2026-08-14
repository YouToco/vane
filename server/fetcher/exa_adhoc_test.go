package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/types"
)

// ============================================================
// ad-hoc 方法（agent web_search/read_page 工具的 fetcher 层）测试。
// 复用 exa_test.go / exa_contents_test.go 的 httptest 基建。
// ============================================================

func TestExaSearch_Adhoc(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sampleExaResponse))
	}))
	defer srv.Close()

	e := newTestExa(srv.URL)
	results, err := e.Search(context.Background(), "kimi 会员价格", 0, []string{"kimi.com"})
	if err != nil {
		t.Fatalf("Search 失败: %v", err)
	}
	// 第三条无 url 无 title 应被跳过（与 mapExaResults 同判据）。
	if len(results) != 2 {
		t.Fatalf("期望 2 条，实得 %d", len(results))
	}
	if results[0].Title != "AI Weekly Digest" || results[0].URL != "https://example.com/ai-news-1" {
		t.Errorf("首条结果映射错: %+v", results[0])
	}
	// numResults=0 应回落默认值；includeDomains 应透传；type 固定 auto。
	var req exaRequest
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("解析请求体失败: %v", err)
	}
	if req.NumResults != exaDefaultNumResults {
		t.Errorf("numResults 默认应为 %d，实得 %d", exaDefaultNumResults, req.NumResults)
	}
	if len(req.IncludeDomains) != 1 || req.IncludeDomains[0] != "kimi.com" {
		t.Errorf("includeDomains 应透传，实得 %v", req.IncludeDomains)
	}
	if !req.Contents.Text {
		t.Error("contents.text 必须为 true（否则没有正文摘要）")
	}
}

func TestExaSearch_Adhoc钳制与校验(t *testing.T) {
	e := newTestExa("http://unused")
	// 空 query → CodeValidation，不打上游。
	if _, err := e.Search(context.Background(), "  ", 5, nil); !errors.Is(err, types.ErrValidation) {
		t.Errorf("空 query 应 CodeValidation，实得 %v", err)
	}
	// 缺 key → CodeValidation。
	noKey := NewExa(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}, nil)
	if _, err := noKey.Search(context.Background(), "x", 5, nil); !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 key 应 CodeValidation，实得 %v", err)
	}
}

func TestExaReadPage_Adhoc(t *testing.T) {
	var gotReq string
	srv := contentsServer(t, 200, `{
	  "results": [{"id":"https://www.kimi.com/membership/pricing","url":"https://www.kimi.com/membership/pricing","title":"Kimi 会员定价","text":"套餐 A ¥99/月"}],
	  "statuses": [{"status":"success","source":"crawled"}]
	}`, &gotReq)
	defer srv.Close()

	e := newTestExaContents(srv.URL)
	title, text, cached, err := e.ReadPage(context.Background(), " https://www.kimi.com/membership/pricing ")
	if err != nil {
		t.Fatalf("ReadPage 失败: %v", err)
	}
	if title != "Kimi 会员定价" || !strings.Contains(text, "套餐 A ¥99/月") {
		t.Errorf("标题/正文映射错: title=%q text=%q", title, text)
	}
	if cached {
		t.Error("crawled 不该标 cached")
	}
	// maxAgeHours 必须为 0（强制活抓——ad-hoc 读页吃缓存等于读到旧价格）。
	var req exaContentsRequest
	if err := json.Unmarshal([]byte(gotReq), &req); err != nil {
		t.Fatalf("解析请求体失败: %v", err)
	}
	if req.MaxAgeHours != 0 {
		t.Errorf("maxAgeHours 必须为 0（活抓），实得 %d", req.MaxAgeHours)
	}
	if len(req.URLs) != 1 || req.URLs[0] != "https://www.kimi.com/membership/pricing" {
		t.Errorf("url 应 TrimSpace 后透传，实得 %v", req.URLs)
	}
}

func TestExaReadPage_Adhoc错误语义(t *testing.T) {
	// statuses error → ErrPageUnreachable 哨兵必须穿透（工具层据此给「检查 URL」话术）。
	srv := contentsServer(t, 200, `{"results":[],"statuses":[{"status":"error"}]}`, new(string))
	defer srv.Close()
	e := newTestExaContents(srv.URL)
	if _, _, _, err := e.ReadPage(context.Background(), "https://a.b"); !errors.Is(err, ErrPageUnreachable) {
		t.Errorf("statuses error 应带 ErrPageUnreachable，实得 %v", err)
	}

	// 成功但无正文 → CodeFetchTimeout 不可重试（空页重试无意义）。
	srv2 := contentsServer(t, 200, `{"results":[{"id":"x","url":"https://a.b","title":"","text":"  "}],"statuses":[{"status":"success","source":"crawled"}]}`, new(string))
	defer srv2.Close()
	e2 := newTestExaContents(srv2.URL)
	_, _, _, err := e2.ReadPage(context.Background(), "https://a.b")
	var ae *types.AppError
	if !errors.As(err, &ae) || ae.Code != types.CodeFetchTimeout || ae.Retryable {
		t.Errorf("无正文应 CodeFetchTimeout 不可重试，实得 %v", err)
	}

	// 空 url → CodeValidation，不打上游。
	if _, _, _, err := e.ReadPage(context.Background(), " "); !errors.Is(err, types.ErrValidation) {
		t.Errorf("空 url 应 CodeValidation，实得 %v", err)
	}
}
