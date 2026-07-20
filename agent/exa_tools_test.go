package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/types"
)

// ============================================================
// web_search / read_page（Exa ad-hoc 工具对）单测。
// fetcher 层用内存假实现：参数钳制、错误翻译、输出形态全在此钉死；
// 真实 HTTP 与记账由 fetcher 包自己的测试覆盖（exa_test.go / exa_cost_test.go）。
// ============================================================

type fakeWebSearcher struct {
	calls      int
	gotQuery   string
	gotNum     int
	gotDomains []string
	results    []fetcher.SearchResult
	err        error
}

func (f *fakeWebSearcher) Search(_ context.Context, q string, n int, d []string) ([]fetcher.SearchResult, error) {
	f.calls++
	f.gotQuery, f.gotNum, f.gotDomains = q, n, d
	return f.results, f.err
}

type fakePageReader struct {
	calls  int
	gotURL string
	title  string
	text   string
	cached bool
	err    error
}

func (f *fakePageReader) ReadPage(_ context.Context, u string) (string, string, bool, error) {
	f.calls++
	f.gotURL = u
	return f.title, f.text, f.cached, f.err
}

func TestWebSearch_HappyPath(t *testing.T) {
	fs := &fakeWebSearcher{results: []fetcher.SearchResult{
		{Title: "Kimi 会员定价", URL: "https://www.kimi.com/membership/pricing", PublishedDate: "2026-07-01", Author: "Kimi", Text: "会员套餐价格正文"},
		{Title: "", URL: "https://example.com/b", Text: "无标题结果回退 URL"},
	}}
	tl := &webSearchTool{searcher: fs}
	out, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"kimi 会员价格"}`))
	if err != nil {
		t.Fatalf("Execute 失败: %v", err)
	}
	if fs.calls != 1 || fs.gotQuery != "kimi 会员价格" || fs.gotNum != 5 {
		t.Errorf("上游应被调 1 次、默认 5 条，实得 calls=%d q=%q n=%d", fs.calls, fs.gotQuery, fs.gotNum)
	}
	for _, want := range []string{"搜到 2 条结果", "[1] Kimi 会员定价", "https://www.kimi.com/membership/pricing", "发布: 2026-07-01", "会员套餐价格正文", "[2] https://example.com/b"} {
		if !strings.Contains(out, want) {
			t.Errorf("输出应含 %q，实得:\n%s", want, out)
		}
	}
}

func TestWebSearch_参数校验不打上游不计费(t *testing.T) {
	fs := &fakeWebSearcher{}
	tl := &webSearchTool{searcher: fs}
	for name, args := range map[string]string{
		"空 query":  `{"query":"  "}`,
		"缺 query":  `{}`,
		"坏 JSON": `{`,
	} {
		out, err := tl.Execute(context.Background(), 1, json.RawMessage(args))
		if err != nil {
			t.Errorf("%s：确定性失败应走文案通道（nil error），实得 %v", name, err)
		}
		if out == "" {
			t.Errorf("%s：应有可自纠文案", name)
		}
	}
	if fs.calls != 0 {
		t.Errorf("参数校验失败绝不能打上游（计费），实得 %d 次", fs.calls)
	}
}

func TestWebSearch_条数钳制(t *testing.T) {
	fs := &fakeWebSearcher{}
	tl := &webSearchTool{searcher: fs}
	if _, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x","num_results":100}`)); err != nil {
		t.Fatal(err)
	}
	if fs.gotNum != webSearchMaxResults {
		t.Errorf("num_results 应钳到 %d，实得 %d", webSearchMaxResults, fs.gotNum)
	}
	if _, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x","num_results":3,"include_domains":["openai.com"]}`)); err != nil {
		t.Fatal(err)
	}
	if fs.gotNum != 3 || len(fs.gotDomains) != 1 || fs.gotDomains[0] != "openai.com" {
		t.Errorf("num_results/include_domains 应透传，实得 n=%d d=%v", fs.gotNum, fs.gotDomains)
	}
}

func TestWebSearch_正文截断(t *testing.T) {
	long := strings.Repeat("字", webSearchTextMaxRunes+500)
	fs := &fakeWebSearcher{results: []fetcher.SearchResult{{Title: "t", URL: "https://a.b", Text: long}}}
	tl := &webSearchTool{searcher: fs}
	out, _ := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`))
	if strings.Contains(out, long) {
		t.Error("正文应按对话预算截断，不应整段输出")
	}
	if !strings.Contains(out, "…") {
		t.Error("截断应带省略号标记")
	}
}

func TestWebSearch_错误翻译(t *testing.T) {
	// AppError → Message 人话回模型（nil error）。
	fs := &fakeWebSearcher{err: types.NewAppError(types.CodeFetchRateLimit, "Exa 搜索被限流(429)", nil)}
	tl := &webSearchTool{searcher: fs}
	out, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`))
	if err != nil || !strings.Contains(out, "限流") {
		t.Errorf("AppError 应翻译成文案通道，实得 out=%q err=%v", out, err)
	}
	// 非 AppError（真基础设施失败）→ 向上抛。
	fs.err = errors.New("dial tcp: connection refused")
	if _, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`)); err == nil {
		t.Error("非 AppError 必须上抛，不能吞成文案")
	}
	// 空结果 → 引导文案。
	fs.err = nil
	fs.results = nil
	out, _ = tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`))
	if !strings.Contains(out, "没有搜到") {
		t.Errorf("空结果应有下一步引导，实得 %q", out)
	}
}

func TestReadPage_HappyPath(t *testing.T) {
	fr := &fakePageReader{title: "Kimi 会员定价", text: "套餐 A ¥99/月\n套餐 B ¥199/月"}
	tl := &readPageTool{reader: fr}
	out, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://www.kimi.com/membership/pricing"}`))
	if err != nil {
		t.Fatalf("Execute 失败: %v", err)
	}
	if fr.calls != 1 || fr.gotURL != "https://www.kimi.com/membership/pricing" {
		t.Errorf("上游应被调 1 次且 URL 透传，实得 calls=%d url=%q", fr.calls, fr.gotURL)
	}
	if !strings.Contains(out, "Kimi 会员定价") || !strings.Contains(out, "套餐 A ¥99/月") {
		t.Errorf("输出应含标题与正文，实得:\n%s", out)
	}
	if strings.Contains(out, "缓存副本") {
		t.Error("活抓成功不该提示缓存")
	}
}

func TestReadPage_缓存提示(t *testing.T) {
	fr := &fakePageReader{title: "t", text: "正文", cached: true}
	tl := &readPageTool{reader: fr}
	out, _ := tl.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://a.b"}`))
	if !strings.Contains(out, "缓存副本") {
		t.Error("cached=true 时必须如实告知「可能不是最新」（诚实口径同 enrich 路径）")
	}
}

func TestReadPage_参数校验不打上游不计费(t *testing.T) {
	fr := &fakePageReader{}
	tl := &readPageTool{reader: fr}
	for name, args := range map[string]string{
		"空 url":   `{"url":" "}`,
		"非 http": `{"url":"ftp://a.b/c"}`,
		"缺协议":   `{"url":"www.kimi.com/pricing"}`,
	} {
		out, err := tl.Execute(context.Background(), 1, json.RawMessage(args))
		if err != nil {
			t.Errorf("%s：确定性失败应走文案通道，实得 %v", name, err)
		}
		if out == "" {
			t.Errorf("%s：应有可自纠文案", name)
		}
	}
	if fr.calls != 0 {
		t.Errorf("参数校验失败绝不能打上游（计费），实得 %d 次", fr.calls)
	}
}

func TestReadPage_错误翻译(t *testing.T) {
	// 页面抓不到 → 「检查 URL」话术（对齐 probe 准入的翻译，不只说稍后再试）。
	fr := &fakePageReader{err: types.NewAppError(types.CodeFetchTimeout, "Exa /contents 抓取失败", fetcher.ErrPageUnreachable)}
	tl := &readPageTool{reader: fr}
	out, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://a.b"}`))
	if err != nil || !strings.Contains(out, "检查 URL") {
		t.Errorf("ErrPageUnreachable 应翻译成检查 URL 话术，实得 out=%q err=%v", out, err)
	}
	// AppError → 文案；非 AppError → 上抛。
	fr.err = types.NewAppError(types.CodeValidation, "读取页面需要配置 VANE_FETCH_EXA_API_KEY，当前为空", nil)
	out, err = tl.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://a.b"}`))
	if err != nil || !strings.Contains(out, "EXA_API_KEY") {
		t.Errorf("AppError 应走文案通道，实得 out=%q err=%v", out, err)
	}
	fr.err = errors.New("dial tcp: connection refused")
	if _, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://a.b"}`)); err == nil {
		t.Error("非 AppError 必须上抛")
	}
}

// TestBuildTools_Exa装配 钉住装配语义：exa 非 nil 时两工具进白名单、nil 时工具面与
// 上线前一致（key 未配置不广告用不了的工具，同 endpoints 语义）。
func TestBuildTools_Exa装配(t *testing.T) {
	names := func(tools []Tool) map[string]bool {
		m := make(map[string]bool, len(tools))
		for _, tl := range tools {
			m[tl.Name()] = true
		}
		return m
	}
	with := names(BuildTools(nil, nil, nil, nil, nil, nil, NewExaTools(&fakeWebSearcher{}, &fakePageReader{})))
	if !with["web_search"] || !with["read_page"] {
		t.Errorf("exa 非 nil 时 web_search/read_page 必须在白名单，实得 %v", with)
	}
	without := names(BuildTools(nil, nil, nil, nil, nil, nil, nil))
	if without["web_search"] || without["read_page"] {
		t.Errorf("exa=nil 时两工具不得出现（缺 key 不广告），实得 %v", without)
	}
	// 两工具都是只读（免确认、不进 pending_actions）。
	for _, tl := range BuildTools(nil, nil, nil, nil, nil, nil, NewExaTools(nil, nil)) {
		if (tl.Name() == "web_search" || tl.Name() == "read_page") && tl.Mutating() {
			t.Errorf("%s 必须是只读工具", tl.Name())
		}
	}
}
