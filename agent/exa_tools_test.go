package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/types"
)

// ============================================================
// web_search / read_page（Exa ad-hoc 工具对）单测。
// fetcher 层用内存假实现：参数钳制、错误翻译、输出形态、双重限额全在此钉死；
// 真实 HTTP 与记账由 fetcher 包自己的测试覆盖（exa_adhoc_test.go 等）。
// ============================================================

type fakeWebSearcher struct {
	calls          int
	gotQuery       string
	gotNum         int
	gotDomains     []string
	gotTrace       string
	gotUserID      int64
	gotAttribution bool
	results        []fetcher.SearchResult
	err            error
}

func (f *fakeWebSearcher) Search(ctx context.Context, q string, n int, d []string) ([]fetcher.SearchResult, error) {
	f.calls++
	f.gotQuery, f.gotNum, f.gotDomains = q, n, d
	f.gotTrace, f.gotUserID, f.gotAttribution = fetcher.BindingAttributionFromContext(ctx)
	return f.results, f.err
}

type fakePageReader struct {
	calls          int
	gotURL         string
	gotTrace       string
	gotUserID      int64
	gotAttribution bool
	title          string
	text           string
	cached         bool
	err            error
}

func (f *fakePageReader) ReadPage(ctx context.Context, u string) (string, string, bool, error) {
	f.calls++
	f.gotURL = u
	f.gotTrace, f.gotUserID, f.gotAttribution = fetcher.BindingAttributionFromContext(ctx)
	return f.title, f.text, f.cached, f.err
}

type fakeExaCounter struct {
	n   int
	err error
}

func (f *fakeExaCounter) CountExaAdHocCallsSince(_ context.Context, _ time.Time) (int, error) {
	return f.n, f.err
}

// newTestExaTools 造无限额（caps=0）的工具对：功能用例不碰预算逻辑。
func newTestExaTools(s webSearcher, r pageReader) *ExaTools {
	return &ExaTools{searcher: s, reader: r}
}

// ctxWithRun 给 ctx 装上 per-message 运行状态与记账记录（限额/标记用例）。
func ctxWithRun(state *toolRunState, rec *types.ToolCall) context.Context {
	ctx := context.Background()
	if state != nil {
		ctx = context.WithValue(ctx, toolRunKey{}, state)
	}
	if rec != nil {
		ctx = context.WithValue(ctx, toolCallRecKey{}, rec)
	}
	return ctx
}

func TestWebSearch_HappyPath(t *testing.T) {
	fs := &fakeWebSearcher{results: []fetcher.SearchResult{
		{Title: "Kimi 会员定价", URL: "https://www.kimi.com/membership/pricing", PublishedDate: "2026-07-01", Author: "Kimi", Text: "会员套餐价格正文"},
		{Title: "", URL: "https://example.com/b", Text: "无标题结果回退 URL"},
	}}
	tl := &webSearchTool{et: newTestExaTools(fs, nil)}
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
	tl := &webSearchTool{et: newTestExaTools(fs, nil)}
	for name, args := range map[string]string{
		"空 query":            `{"query":"  "}`,
		"缺 query":            `{}`,
		"坏 JSON":             `{`,
		"query 超长":           `{"query":"` + strings.Repeat("字", exaQueryMaxRunes+1) + `"}`,
		"include_domains 超数": `{"query":"x","include_domains":[` + strings.Repeat(`"a.com",`, exaMaxIncludeDomains) + `"b.com"]}`,
	} {
		rec := &types.ToolCall{}
		out, err := tl.Execute(ctxWithRun(nil, rec), 1, json.RawMessage(args))
		if err != nil {
			t.Errorf("%s：确定性失败应走文案通道（nil error），实得 %v", name, err)
		}
		if out == "" {
			t.Errorf("%s：应有可自纠文案", name)
		}
		if rec.ErrorType != types.ToolErrInvalidArgs {
			t.Errorf("%s：应记 invalid_args（日限额 COUNT 排除），实得 %q", name, rec.ErrorType)
		}
	}
	if fs.calls != 0 {
		t.Errorf("参数校验失败绝不能打上游（计费），实得 %d 次", fs.calls)
	}
}

func TestWebSearch_条数钳制(t *testing.T) {
	fs := &fakeWebSearcher{}
	tl := &webSearchTool{et: newTestExaTools(fs, nil)}
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
	tl := &webSearchTool{et: newTestExaTools(fs, nil)}
	out, _ := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`))
	if strings.Contains(out, long) {
		t.Error("正文应按对话预算截断，不应整段输出")
	}
	if !strings.Contains(out, "…") {
		t.Error("截断应带省略号标记")
	}
}

// TestWebSearch_上游可控字段截断 钉住对抗审查 F3：标题/作者/URL 是上游可控文本，
// 不截断会被恶意页面构造的超长 <title> 撑爆对话上下文。
func TestWebSearch_上游可控字段截断(t *testing.T) {
	longTitle := strings.Repeat("题", exaOutMetaMaxRunes+300)
	longAuthor := strings.Repeat("者", exaOutMetaMaxRunes+300)
	longURL := "https://a.b/" + strings.Repeat("p", exaOutURLMaxRunes+300)
	fs := &fakeWebSearcher{results: []fetcher.SearchResult{{Title: longTitle, URL: longURL, Author: longAuthor, Text: "x"}}}
	tl := &webSearchTool{et: newTestExaTools(fs, nil)}
	out, _ := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`))
	for name, raw := range map[string]string{"标题": longTitle, "作者": longAuthor, "URL": longURL} {
		if strings.Contains(out, raw) {
			t.Errorf("%s 未截断——上游可控文本必须截断后才能进模型上下文", name)
		}
	}
}

func TestWebSearch_错误翻译(t *testing.T) {
	// Retryable AppError is surfaced so the unified loop can retry it.
	fs := &fakeWebSearcher{err: types.NewAppError(types.CodeFetchRateLimit, "Exa 搜索被限流(429)", nil)}
	tl := &webSearchTool{et: newTestExaTools(fs, nil)}
	out, err := tl.Execute(context.Background(), 1, json.RawMessage(`{"query":"x"}`))
	if err == nil || out != "" || !types.IsRetryable(err) {
		t.Errorf("retryable error should reach harness: out=%q err=%v", out, err)
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
	tl := &readPageTool{et: newTestExaTools(nil, fr)}
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

// TestExaTools_UpstreamAttribution 钉住 H-1 的跨包接缝：Agent 层已有的
// trace/user 必须注入 fetcher 上游调用；只测 fetcher 自己“收到就会写”会让调用方
// 漏注入时仍假绿。tenant_id 由 store 按该 userID 的 membership 推导。
func TestExaTools_UpstreamAttribution(t *testing.T) {
	const traceID = "h1-trace-sentinel"
	ctx := context.WithValue(context.Background(), chatMetaKey{}, chatMeta{traceID: traceID, userID: 7})

	fs := &fakeWebSearcher{}
	search := &webSearchTool{et: newTestExaTools(fs, nil)}
	if _, err := search.Execute(ctx, 7, json.RawMessage(`{"query":"Vane H-1"}`)); err != nil {
		t.Fatalf("web_search: %v", err)
	}
	if !fs.gotAttribution || fs.gotTrace != traceID || fs.gotUserID != 7 {
		t.Fatalf("web_search 上游归属丢失: trace=%q user=%d hasUser=%v",
			fs.gotTrace, fs.gotUserID, fs.gotAttribution)
	}

	fr := &fakePageReader{text: "正文"}
	read := &readPageTool{et: newTestExaTools(nil, fr)}
	if _, err := read.Execute(ctx, 7, json.RawMessage(`{"url":"https://example.com/h1"}`)); err != nil {
		t.Fatalf("read_page: %v", err)
	}
	if !fr.gotAttribution || fr.gotTrace != traceID || fr.gotUserID != 7 {
		t.Fatalf("read_page 上游归属丢失: trace=%q user=%d hasUser=%v",
			fr.gotTrace, fr.gotUserID, fr.gotAttribution)
	}
}

func TestReadPage_缓存提示(t *testing.T) {
	fr := &fakePageReader{title: "t", text: "正文", cached: true}
	tl := &readPageTool{et: newTestExaTools(nil, fr)}
	out, _ := tl.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://a.b"}`))
	if !strings.Contains(out, "缓存副本") {
		t.Error("cached=true 时必须如实告知「可能不是最新」（诚实口径同 enrich 路径）")
	}
}

func TestReadPage_参数校验不打上游不计费(t *testing.T) {
	fr := &fakePageReader{}
	tl := &readPageTool{et: newTestExaTools(nil, fr)}
	for name, args := range map[string]string{
		"空 url":  `{"url":" "}`,
		"非 http": `{"url":"ftp://a.b/c"}`,
		"缺协议":    `{"url":"www.kimi.com/pricing"}`,
		"url 超长": `{"url":"https://a.b/` + strings.Repeat("p", exaURLMaxRunes) + `"}`,
	} {
		rec := &types.ToolCall{}
		out, err := tl.Execute(ctxWithRun(nil, rec), 1, json.RawMessage(args))
		if err != nil {
			t.Errorf("%s：确定性失败应走文案通道，实得 %v", name, err)
		}
		if out == "" {
			t.Errorf("%s：应有可自纠文案", name)
		}
		if rec.ErrorType != types.ToolErrInvalidArgs {
			t.Errorf("%s：应记 invalid_args，实得 %q", name, rec.ErrorType)
		}
	}
	if fr.calls != 0 {
		t.Errorf("参数校验失败绝不能打上游（计费），实得 %d 次", fr.calls)
	}
}

func TestReadPage_错误翻译(t *testing.T) {
	// 页面抓不到 → 「检查 URL」话术（对齐 probe 准入的翻译，不只说稍后再试）。
	fr := &fakePageReader{err: types.NewAppError(types.CodeFetchTimeout, "Exa /contents 抓取失败", fetcher.ErrPageUnreachable)}
	tl := &readPageTool{et: newTestExaTools(nil, fr)}
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

// ============================================================
// 双重限额（对抗审查 HIGH）：按次计费 + 免确认就必须有频率护栏。
// ============================================================

// TestExaTools_每日限额与failclosed 钉住第二重：24h 窗口达 dailyCap 拒绝；
// 计数查询失败时 fail-closed 拒绝（护栏失效即放开计费面，宁可少查）。
func TestExaTools_每日限额与failclosed(t *testing.T) {
	// 达日限额 → 拒绝、不打上游、记 budget_exceeded。
	fr := &fakePageReader{}
	et := &ExaTools{reader: fr, counter: &fakeExaCounter{n: 100}, dailyCap: 100}
	tl := &readPageTool{et: et}
	rec := &types.ToolCall{}
	out, err := tl.Execute(ctxWithRun(nil, rec), 1, json.RawMessage(`{"url":"https://a.b"}`))
	if err != nil {
		t.Fatalf("限额拒绝应走文案通道，实得 %v", err)
	}
	if !strings.Contains(out, "过去 24 小时网页查询已达上限") {
		t.Errorf("应有日限额文案，实得 %q", out)
	}
	if fr.calls != 0 || rec.ErrorType != types.ToolErrBudgetExceeded {
		t.Errorf("日限额拒绝：不打上游 + budget_exceeded，实得 calls=%d type=%q", fr.calls, rec.ErrorType)
	}

	// 计数查询失败 → fail-closed 拒绝（固定文案不泄错误链）。
	et2 := &ExaTools{reader: fr, counter: &fakeExaCounter{err: errors.New("db down")}, dailyCap: 100}
	tl2 := &readPageTool{et: et2}
	rec2 := &types.ToolCall{}
	out, err = tl2.Execute(ctxWithRun(nil, rec2), 1, json.RawMessage(`{"url":"https://a.b"}`))
	if err != nil {
		t.Fatalf("fail-closed 应走文案通道，实得 %v", err)
	}
	if !strings.Contains(out, "限额检查暂时不可用") || strings.Contains(out, "db down") {
		t.Errorf("fail-closed 应回固定文案（不泄错误链），实得 %q", out)
	}
	if fr.calls != 0 || rec2.ErrorType != types.ToolErrBudgetExceeded {
		t.Errorf("fail-closed：不打上游 + budget_exceeded，实得 calls=%d type=%q", fr.calls, rec2.ErrorType)
	}

	// 未达日限额 → 放行。
	et3 := &ExaTools{reader: fr, counter: &fakeExaCounter{n: 99}, dailyCap: 100}
	tl3 := &readPageTool{et: et3}
	if _, err := tl3.Execute(context.Background(), 1, json.RawMessage(`{"url":"https://a.b"}`)); err != nil {
		t.Fatalf("未达限额应放行，实得 %v", err)
	}
	if fr.calls != 1 {
		t.Errorf("未达限额应打上游，实得 %d 次", fr.calls)
	}
}

// TestBuildPublicResearchTools_Exa装配 钉住装配语义：exa 非 nil 时两工具进白名单。
// 上线前一致（key 未配置不广告用不了的工具，同 endpoints 语义）。
func TestBuildPublicResearchTools_Exa装配(t *testing.T) {
	names := func(tools []ToolSpec) map[string]bool {
		m := make(map[string]bool, len(tools))
		for _, tl := range tools {
			m[tl.Name()] = true
		}
		return m
	}
	with := names(BuildPublicResearchTools(nil, NewExaTools(&fakeWebSearcher{}, &fakePageReader{}, nil, 0)))
	if !with["web_search"] || !with["read_page"] {
		t.Errorf("exa 非 nil 时 web_search/read_page 必须在白名单，实得 %v", with)
	}
	without := names(BuildPublicResearchTools(nil, nil))
	if without["web_search"] || without["read_page"] {
		t.Errorf("exa=nil 时两工具不得出现（缺 key 不广告），实得 %v", without)
	}
	// 两工具成本由工具现有 cap 管理。
	for _, tl := range BuildPublicResearchTools(nil, NewExaTools(nil, nil, nil, 0)) {
		if (tl.Name() == "web_search" || tl.Name() == "read_page") &&
			tl.Policy.Budget != BudgetToolManaged {
			t.Errorf("%s 策略不符: %+v", tl.Name(), tl.Policy)
		}
	}
}

// TestSystemPrompt_ExaNote条件注入 钉住对抗审查 MEDIUM：分流引导行只在 web_search
// 真装配时出现——缺 key 环境 prompt 不得广告白名单里不存在的工具（否则模型按
// prompt 调用被白名单拒绝，浪费一轮还向用户食言）。
func TestSystemPrompt_ExaNote条件注入(t *testing.T) {
	const marker = "一次性需求"
	with := New(Deps{Tools: []ToolSpec{newToolSpec(
		&webSearchTool{et: newTestExaTools(nil, nil)},
		ownerPolicy(Effects(EffectNetworkRead, EffectBillable, EffectTrustTaint),
			BudgetToolManaged),
	)}})
	if !strings.Contains(with.sys, marker) {
		t.Error("web_search 在场时分流引导行必须注入")
	}
	without := New(Deps{})
	if strings.Contains(without.sys, marker) {
		t.Error("web_search 不在场时不得广告该工具（会制造白名单拒绝循环）")
	}
	// A2A 轨（自定义 prompt + 只读白名单，无 web_search）同样不得出现。
	a2a := New(Deps{SystemPrompt: "A2A 语境"})
	if strings.Contains(a2a.sys, marker) {
		t.Error("A2A 轨不得注入飞书轨的分流引导")
	}
}
