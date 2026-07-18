package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
)

// ============================================================
// 假实现：invoker / 每日计数 / 记账 inserter
// ============================================================

// fakeInvoker 记录调用并按脚本返回，覆盖 endpointInvoker 窄接口。
type fakeInvoker struct {
	calls  []fakeInvokeRecord
	status int
	body   string
	err    error
}

type fakeInvokeRecord struct {
	entry  tikhubcatalog.Entry
	params map[string]any
}

func (f *fakeInvoker) Invoke(_ context.Context, entry tikhubcatalog.Entry, params map[string]any) (*tikhubinvoke.Result, error) {
	f.calls = append(f.calls, fakeInvokeRecord{entry: entry, params: params})
	if f.err != nil {
		return nil, f.err
	}
	status := f.status
	if status == 0 {
		status = 200
	}
	return &tikhubinvoke.Result{Status: status, Body: []byte(f.body), DurationMs: 1}, nil
}

// fakeCounter 覆盖 endpointCallCounter。
type fakeCounter struct {
	n   int
	err error
}

func (f *fakeCounter) CountTikHubEndpointCallsSince(context.Context, time.Time) (int, error) {
	return f.n, f.err
}

// fakeToolCallInserter 收集记账行，供断言记录内容。
type fakeToolCallInserter struct {
	calls []*types.ToolCall
}

func (f *fakeToolCallInserter) InsertToolCall(_ context.Context, c *types.ToolCall) (int64, error) {
	cp := *c
	f.calls = append(f.calls, &cp)
	return int64(len(f.calls)), nil
}

// testEndpoint 取注册表里一个已知端点（小红书搜索，参数含必填 keyword），
// golden 依赖它存在（catalog_test.go 已单独锁住）。
func testEndpoint(t *testing.T) tikhubcatalog.Entry {
	t.Helper()
	e, ok := tikhubcatalog.Lookup("xiaohongshu_app_v2_search_notes")
	if !ok {
		t.Fatal("注册表缺少 xiaohongshu_app_v2_search_notes")
	}
	return e
}

// ctxWithRunState 构造带运行状态与记账记录的 ctx（模拟 execRecorded 的注入）。
func ctxWithRunState(state *toolRunState, rec *types.ToolCall) context.Context {
	ctx := context.Background()
	if state != nil {
		ctx = context.WithValue(ctx, toolRunKey{}, state)
	}
	if rec != nil {
		ctx = context.WithValue(ctx, toolCallRecKey{}, rec)
	}
	return ctx
}

// ============================================================
// activationState
// ============================================================

func TestActivationState_ActivateOrderDedupEvict(t *testing.T) {
	a := &activationState{}
	for i := 0; i < maxActivatedEndpoints; i++ {
		if ev := a.activate(fmt.Sprintf("ep_%02d", i)); ev != "" {
			t.Fatalf("未满员不应逐出，实得 %q", ev)
		}
	}
	// 重复激活：不动位置、不逐出。
	if ev := a.activate("ep_03"); ev != "" {
		t.Fatalf("重复激活不应逐出，实得 %q", ev)
	}
	if a.names[3] != "ep_03" {
		t.Fatalf("重复激活不应改变位置，实际 names[3]=%q", a.names[3])
	}
	// 满员：FIFO 逐出最早的 ep_00。
	if ev := a.activate("ep_new"); ev != "ep_00" {
		t.Fatalf("满员应逐出 ep_00，实得 %q", ev)
	}
	if len(a.names) != maxActivatedEndpoints {
		t.Fatalf("激活集应保持上限 %d，实际 %d", maxActivatedEndpoints, len(a.names))
	}
	if !a.contains("ep_new") || a.contains("ep_00") {
		t.Fatal("逐出/加入结果不符")
	}
}

func TestActivationState_EncodeDecodeRoundTrip(t *testing.T) {
	a := &activationState{}
	a.activate("ep_b")
	a.activate("ep_a")
	got := decodeActivation(a.encode())
	if len(got.names) != 2 || got.names[0] != "ep_b" || got.names[1] != "ep_a" {
		t.Fatalf("round-trip 应保序，实际 %v", got.names)
	}
	// 空与损坏数据自愈为空集（与 decodeMessages 同原则）。
	if got := decodeActivation(nil); len(got.names) != 0 {
		t.Fatalf("nil 应解为空集，实际 %v", got.names)
	}
	if got := decodeActivation(json.RawMessage(`{broken`)); len(got.names) != 0 {
		t.Fatalf("损坏 JSON 应自愈为空集，实际 %v", got.names)
	}
}

// ============================================================
// search_endpoints
// ============================================================

func newTestEndpointTools(inv endpointInvoker, counter endpointCallCounter, msgCap, dailyCap int) *EndpointTools {
	return NewEndpointTools(inv, counter, msgCap, dailyCap)
}

func TestSearchEndpointsTool_ActivatesAndRecords(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	tool := ep.SearchTool()

	state := &toolRunState{activation: &activationState{}}
	rec := &types.ToolCall{}
	out, err := tool.Execute(ctxWithRunState(state, rec), 1,
		json.RawMessage(`{"query":"小红书 搜索 笔记","platform":"xiaohongshu"}`))
	if err != nil {
		t.Fatalf("Execute 意外报错: %v", err)
	}
	if len(state.activation.names) == 0 {
		t.Fatal("检索命中后应激活端点")
	}
	// 激活的端点必须逐个出现在结果文本里（模型靠文本知道有什么可调）。
	for _, name := range state.activation.names {
		if !strings.Contains(out, name) {
			t.Errorf("结果文本缺少已激活端点 %s", name)
		}
	}
	// 检索留痕（契约 §6）：query 与候选进记账记录。
	if !strings.Contains(rec.RetrievalQuery, "小红书 搜索 笔记") || !strings.Contains(rec.RetrievalQuery, "platform=xiaohongshu") {
		t.Errorf("RetrievalQuery 留痕不符: %q", rec.RetrievalQuery)
	}
	if len(rec.CandidateTools) != len(state.activation.names) {
		t.Errorf("候选留痕 %d 与激活数 %d 不符", len(rec.CandidateTools), len(state.activation.names))
	}
	// 平台过滤是硬约束。
	for _, name := range state.activation.names {
		e, _ := tikhubcatalog.Lookup(name)
		if e.Platform != "xiaohongshu" {
			t.Errorf("platform 过滤失效：激活了 %s（%s）", name, e.Platform)
		}
	}
}

func TestSearchEndpointsTool_SelfCorrectMessages(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	tool := ep.SearchTool()
	state := &toolRunState{activation: &activationState{}}

	if out, _ := tool.Execute(ctxWithRunState(state, nil), 1, json.RawMessage(`{"query":"  "}`)); !strings.Contains(out, "query 不能为空") {
		t.Errorf("空 query 应回自纠文案，实得 %q", out)
	}
	if out, _ := tool.Execute(ctxWithRunState(state, nil), 1, json.RawMessage(`{broken`)); !strings.Contains(out, "合法 JSON") {
		t.Errorf("坏 JSON 应回自纠文案，实得 %q", out)
	}
	// 未知平台：给平台清单而不是干瘪的零命中。
	out, _ := tool.Execute(ctxWithRunState(state, nil), 1, json.RawMessage(`{"query":"视频","platform":"myspace"}`))
	if !strings.Contains(out, "不在目录中") {
		t.Errorf("未知平台应提示平台清单，实得 %q", out)
	}
}

// TestSearchEndpointsTool_NoRunStateStillSearches：ctx 无运行状态（防御路径）时
// 检索照常返回，只是无法激活——不 panic 是底线。
func TestSearchEndpointsTool_NoRunStateStillSearches(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	out, err := ep.SearchTool().Execute(context.Background(), 1, json.RawMessage(`{"query":"抖音 热榜"}`))
	if err != nil || out == "" {
		t.Fatalf("无运行状态也应正常检索: err=%v out=%q", err, out)
	}
}

// ============================================================
// endpointTool
// ============================================================

func TestEndpointTool_InvokeSuccessAndRecord(t *testing.T) {
	inv := &fakeInvoker{body: `{"code":200,"data":{"items":[]}}`}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	tool, ok := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	if !ok {
		t.Fatal("已激活端点应可解析")
	}

	state := &toolRunState{activation: &activationState{}}
	rec := &types.ToolCall{}
	out, err := tool.Execute(ctxWithRunState(state, rec), 1, json.RawMessage(`{"keyword":"AI 编程","page":1}`))
	if err != nil {
		t.Fatalf("Execute 意外报错: %v", err)
	}
	if !strings.Contains(out, `"code":200`) {
		t.Errorf("应返回上游响应原文，实得 %q", out)
	}
	if len(inv.calls) != 1 {
		t.Fatalf("应恰好调用上游 1 次，实得 %d", len(inv.calls))
	}
	if inv.calls[0].params["keyword"] != "AI 编程" {
		t.Errorf("参数透传不符: %+v", inv.calls[0].params)
	}
	if state.endpointCalls != 1 {
		t.Errorf("消息内计数应为 1，实际 %d", state.endpointCalls)
	}
	if rec.EndpointPath != entry.Path || rec.HTTPStatus == nil || *rec.HTTPStatus != 200 {
		t.Errorf("记账回填不符: path=%q status=%v", rec.EndpointPath, rec.HTTPStatus)
	}
	if rec.ResultSize != len(inv.body) {
		t.Errorf("ResultSize 应为上游体量 %d，实际 %d", len(inv.body), rec.ResultSize)
	}
}

func TestEndpointTool_ArgValidation(t *testing.T) {
	inv := &fakeInvoker{}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	tool, _ := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	state := &toolRunState{activation: &activationState{}}

	// 必填缺失（keyword 必填）。
	out, err := tool.Execute(ctxWithRunState(state, nil), 1, json.RawMessage(`{}`))
	if err != nil || !strings.Contains(out, "keyword") {
		t.Errorf("缺必填应回含参数名的自纠文案: err=%v out=%q", err, out)
	}
	// 未知参数。
	out, _ = tool.Execute(ctxWithRunState(state, nil), 1, json.RawMessage(`{"keyword":"x","nonsense":1}`))
	if !strings.Contains(out, "未知参数") || !strings.Contains(out, "nonsense") {
		t.Errorf("未知参数应点名拒绝，实得 %q", out)
	}
	// 校验失败不得打上游、不得吃消息限额。
	if len(inv.calls) != 0 || state.endpointCalls != 0 {
		t.Errorf("校验失败不应触发上游调用（calls=%d, count=%d）", len(inv.calls), state.endpointCalls)
	}
}

func TestEndpointTool_MsgCapEnforced(t *testing.T) {
	inv := &fakeInvoker{body: `{}`}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 1, 200)
	entry := testEndpoint(t)
	tool, _ := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	state := &toolRunState{activation: &activationState{}}
	args := json.RawMessage(`{"keyword":"x"}`)

	if _, err := tool.Execute(ctxWithRunState(state, nil), 1, args); err != nil {
		t.Fatalf("第 1 次应成功: %v", err)
	}
	rec := &types.ToolCall{}
	out, err := tool.Execute(ctxWithRunState(state, rec), 1, args)
	if err != nil || !strings.Contains(out, "上限") {
		t.Fatalf("第 2 次应被消息限额拦截: err=%v out=%q", err, out)
	}
	if len(inv.calls) != 1 {
		t.Fatalf("被拦截的调用不应打上游，实得 %d 次", len(inv.calls))
	}
	if rec.ErrorType != types.ToolErrBudgetExceeded {
		t.Errorf("记账 error_type 应为 budget_exceeded，实际 %q", rec.ErrorType)
	}
}

func TestEndpointTool_DailyCapEnforcedAndFailClosed(t *testing.T) {
	entry := testEndpoint(t)
	args := json.RawMessage(`{"keyword":"x"}`)

	// 达到每日上限：拒绝并注明。
	inv := &fakeInvoker{body: `{}`}
	ep := newTestEndpointTools(inv, &fakeCounter{n: 200}, 10, 200)
	tool, _ := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	out, err := tool.Execute(ctxWithRunState(&toolRunState{activation: &activationState{}}, nil), 1, args)
	if err != nil || !strings.Contains(out, "24 小时") {
		t.Fatalf("应被每日限额拦截: err=%v out=%q", err, out)
	}
	if len(inv.calls) != 0 {
		t.Fatal("每日限额拦截不应打上游")
	}

	// 限额判定不可用：fail-closed，拒绝调用（护栏失效即放开计费面）。三条纪律：
	// ① 回自纠文案而非裸 err（错误链不进模型上下文）；② rec 记 budget_exceeded
	// 而非 internal（没打上游、零计费，不进限额 COUNT）；③ 不打上游。
	ep2 := newTestEndpointTools(inv, &fakeCounter{err: fmt.Errorf("db down: host=secret user=admin")}, 10, 200)
	tool2, _ := ep2.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	rec := &types.ToolCall{}
	out, err = tool2.Execute(ctxWithRunState(&toolRunState{activation: &activationState{}}, rec), 1, args)
	if err != nil {
		t.Fatalf("fail-closed 应回文案而非上抛错误（错误链不进模型上下文），实得 err=%v", err)
	}
	if !strings.Contains(out, "限额检查暂时不可用") {
		t.Errorf("fail-closed 应回固定自纠文案，实得 %q", out)
	}
	if strings.Contains(out, "host=secret") || strings.Contains(out, "db down") {
		t.Errorf("fail-closed 文案不得泄漏 DB 错误链，实得 %q", out)
	}
	if rec.ErrorType != types.ToolErrBudgetExceeded {
		t.Errorf("fail-closed 记账应为 budget_exceeded（不进限额 COUNT），实得 %q", rec.ErrorType)
	}
	if len(inv.calls) != 0 {
		t.Fatal("fail-closed 不应打上游")
	}
}

func TestEndpointTool_UpstreamErrorAndHTTPError(t *testing.T) {
	entry := testEndpoint(t)
	args := json.RawMessage(`{"keyword":"x"}`)
	state := func() context.Context {
		return ctxWithRunState(&toolRunState{activation: &activationState{}}, nil)
	}

	// 传输层失败：文案回给模型（AppError.Message，不带错误链），nil error。
	epErr := newTestEndpointTools(&fakeInvoker{err: types.NewAppError(types.CodeFetchTimeout, "TikHub 端点 x 调用超时", fmt.Errorf("内部细节"))}, &fakeCounter{}, 10, 200)
	tool, _ := epErr.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	out, err := tool.Execute(state(), 1, args)
	if err != nil || !strings.Contains(out, "调用超时") || strings.Contains(out, "内部细节") {
		t.Errorf("上游失败应回 Message 且不带错误链: err=%v out=%q", err, out)
	}

	// 非 2xx：状态码 + 原文摘录回给模型自纠。
	ep4xx := newTestEndpointTools(&fakeInvoker{status: 422, body: `{"detail":"keyword required"}`}, &fakeCounter{}, 10, 200)
	tool2, _ := ep4xx.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	out, err = tool2.Execute(state(), 1, args)
	if err != nil || !strings.Contains(out, "422") || !strings.Contains(out, "keyword required") {
		t.Errorf("非 2xx 应带状态码与原文: err=%v out=%q", err, out)
	}
}

func TestEndpointTool_ResultTruncation(t *testing.T) {
	big := strings.Repeat("数", endpointResultMaxRunes+100)
	ep := newTestEndpointTools(&fakeInvoker{body: big}, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	tool, _ := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	out, err := tool.Execute(ctxWithRunState(&toolRunState{activation: &activationState{}}, nil), 1, json.RawMessage(`{"keyword":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "已截断") {
		t.Error("超限结果应标注截断")
	}
	if got := len([]rune(out)); got > endpointResultMaxRunes+100 {
		t.Errorf("截断后仍有 %d rune", got)
	}
}

// ============================================================
// Resolve / Defs 白名单语义
// ============================================================

func TestResolveAndDefs_WhitelistSemantics(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)

	// 未激活：注册表里存在也不解析（跳过检索直呼端点名是旁门）。
	if _, ok := ep.Resolve(entry.Name, &activationState{}); ok {
		t.Fatal("未激活端点不应解析")
	}
	if _, ok := ep.Resolve(entry.Name, nil); ok {
		t.Fatal("nil 激活集不应解析")
	}
	// 已激活但注册表没有（re-gen 下线）：不解析、Defs 同步跳过。
	ghost := &activationState{names: []string{"ghost_endpoint", entry.Name}}
	if _, ok := ep.Resolve("ghost_endpoint", ghost); ok {
		t.Fatal("注册表已下线的端点不应解析")
	}
	defs := ep.Defs(ghost)
	if len(defs) != 1 || defs[0].Name != entry.Name {
		t.Fatalf("Defs 应只含在册端点，实得 %+v", defs)
	}
	// 声明形态：参数 schema 合法 JSON 且必填齐全。
	var schema struct {
		Type       string                    `json:"type"`
		Properties map[string]map[string]any `json:"properties"`
		Required   []string                  `json:"required"`
	}
	if err := json.Unmarshal(defs[0].Parameters, &schema); err != nil {
		t.Fatalf("参数 schema 非法: %v", err)
	}
	if schema.Type != "object" || schema.Properties["keyword"] == nil {
		t.Errorf("schema 形态不符: %+v", schema)
	}
	hasKeyword := false
	for _, r := range schema.Required {
		if r == "keyword" {
			hasKeyword = true
		}
	}
	if !hasKeyword {
		t.Error("keyword 必填应进 required")
	}
}

// ============================================================
// loop 集成：检索 → 注入 → 调用 → 持久化 → 记账 全链路
// ============================================================

func TestHandleMessage_EndpointSearchInjectInvoke(t *testing.T) {
	fs := newFakeStore()
	inv := &fakeInvoker{body: `{"code":200,"data":"ok"}`}
	inserter := &fakeToolCallInserter{}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)

	chat := &scriptedChat{responses: []*llm.ChatResponse{
		// 轮 1：模型检索端点。
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "search_endpoints",
			Arguments: `{"query":"小红书 搜索 笔记","platform":"xiaohongshu"}`}}, FinishReason: "tool_calls"},
		// 轮 2：模型调用已激活端点。
		{ToolCalls: []llm.ToolCall{{ID: "c2", Name: entry.Name,
			Arguments: `{"keyword":"AI"}`}}, FinishReason: "tool_calls"},
		// 轮 3：收敛。
		{Content: "查到了", FinishReason: "stop"},
	}}

	l := New(Deps{
		Store: fs, Profiles: fs,
		Tools:      BuildTools2Static(ep),
		Model:      "deepseek-v4-pro",
		MaxTurns:   5,
		SessionTTL: 30 * time.Minute,
		Endpoints:  ep,
		ToolCalls:  NewToolCallRecorder(inserter),
	})
	l.chatFn = chat.fn

	out, err := l.HandleMessage(context.Background(), 1, "帮我看看小红书上 AI 的笔记")
	if err != nil {
		t.Fatalf("HandleMessage 报错: %v", err)
	}
	if out.Reply != "查到了" {
		t.Fatalf("Reply = %q", out.Reply)
	}

	// 轮 1 请求：只有静态工具（尚未激活）。轮 2 请求：动态端点声明已注入。
	if len(chat.requests) != 3 {
		t.Fatalf("应 3 次模型调用，实得 %d", len(chat.requests))
	}
	if n := len(chat.requests[0].Tools); n != 1 {
		t.Fatalf("轮 1 应只带静态 search_endpoints 声明，实得 %d 个", n)
	}
	r2names := map[string]bool{}
	for _, d := range chat.requests[1].Tools {
		r2names[d.Name] = true
	}
	if !r2names[entry.Name] {
		t.Fatalf("轮 2 应含已激活端点 %s 的声明，实得 %v", entry.Name, r2names)
	}
	// 静态声明必须仍在最前（前缀稳定纪律）。
	if chat.requests[1].Tools[0].Name != "search_endpoints" {
		t.Errorf("静态声明应在动态声明之前，实际首位 %s", chat.requests[1].Tools[0].Name)
	}

	// 上游恰好被调 1 次，参数透传。
	if len(inv.calls) != 1 || inv.calls[0].params["keyword"] != "AI" {
		t.Fatalf("上游调用不符: %+v", inv.calls)
	}

	// 会话持久化：激活集写回。
	sess := fs.sessions[1]
	var activated []string
	if err := json.Unmarshal(sess.ActivatedTools, &activated); err != nil {
		t.Fatalf("activated_tools 非法: %v（%s）", err, sess.ActivatedTools)
	}
	found := false
	for _, n := range activated {
		if n == entry.Name {
			found = true
		}
	}
	if !found {
		t.Fatalf("激活集应含 %s，实际 %v", entry.Name, activated)
	}

	// 记账：search + endpoint 各一条，kind/留痕正确（契约 §6）。
	if len(inserter.calls) != 2 {
		t.Fatalf("应记账 2 条，实得 %d", len(inserter.calls))
	}
	search, endpoint := inserter.calls[0], inserter.calls[1]
	if search.ToolKind != types.ToolCallKindTikHubSearch || search.RetrievalQuery == "" || len(search.CandidateTools) == 0 {
		t.Errorf("search 记账不符: %+v", search)
	}
	if endpoint.ToolKind != types.ToolCallKindTikHubEndpoint || endpoint.EndpointPath != entry.Path ||
		endpoint.HTTPStatus == nil || *endpoint.HTTPStatus != 200 {
		t.Errorf("endpoint 记账不符: %+v", endpoint)
	}
	if search.TraceID == "" || search.TraceID != endpoint.TraceID {
		t.Errorf("两条记账应共享同一 trace_id: %q vs %q", search.TraceID, endpoint.TraceID)
	}
}

// BuildTools2Static 测试装配：只装 search_endpoints（其余静态工具与本特性无关）。
func BuildTools2Static(ep *EndpointTools) []Tool {
	return []Tool{ep.SearchTool()}
}

// TestEndpointTool_BigIntArgPrecision：大 ID 参数经 UseNumber 全链路保真到上游
// （对抗审查 HIGH 缺陷）——validateEndpointArgs 不再用 float64 舍掉低位。
func TestEndpointTool_BigIntArgPrecision(t *testing.T) {
	inv := &fakeInvoker{body: `{}`}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	tool, _ := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	// keyword 必填占位 + 一个大整数值（模拟模型照抄上游响应里的 uid）。
	args := json.RawMessage(`{"keyword":"x","page":6829164342857171974}`)
	if _, err := tool.Execute(ctxWithRunState(&toolRunState{activation: &activationState{}}, nil), 1, args); err != nil {
		t.Fatal(err)
	}
	if len(inv.calls) != 1 {
		t.Fatalf("应调用上游 1 次，实得 %d", len(inv.calls))
	}
	got := inv.calls[0].params["page"]
	num, ok := got.(json.Number)
	if !ok {
		t.Fatalf("大整数参数应保持 json.Number，实得 %T(%v)", got, got)
	}
	if num.String() != "6829164342857171974" {
		t.Errorf("大整数应逐位保真，实得 %s", num.String())
	}
}

// TestExecRecorded_SanitizesNonUTF8AndNUL：非 UTF-8/NUL 响应体经 execRecorded 净化后
// 才入库与回模型（对抗审查缺陷）——否则整行记账被 Postgres 拒收、限额漏计。
func TestExecRecorded_SanitizesNonUTF8AndNUL(t *testing.T) {
	fs := newFakeStore()
	inserter := &fakeToolCallInserter{}
	// GBK「你好」0xC4E3BAC3 是非法 UTF-8；夹一个 NUL。rune 数 < 截断阈值，走原样透传分支。
	dirty := "ok\xc4\xe3\xba\xc3mid\x00end"
	inv := &fakeInvoker{body: dirty}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)

	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "search_endpoints", Arguments: `{"query":"小红书 搜索 笔记"}`}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{ID: "c2", Name: entry.Name, Arguments: `{"keyword":"x"}`}}, FinishReason: "tool_calls"},
		{Content: "done", FinishReason: "stop"},
	}}
	l := New(Deps{
		Store: fs, Profiles: fs, Tools: BuildTools2Static(ep),
		Model: "m", MaxTurns: 5, SessionTTL: 30 * time.Minute,
		Endpoints: ep, ToolCalls: NewToolCallRecorder(inserter),
	})
	l.chatFn = chat.fn

	if _, err := l.HandleMessage(context.Background(), 1, "查小红书"); err != nil {
		t.Fatal(err)
	}
	// 端点记账行的 result_preview 必须是合法 UTF-8 且无 NUL（否则真库 INSERT 会失败）。
	var endpointRec *types.ToolCall
	for _, c := range inserter.calls {
		if c.ToolKind == types.ToolCallKindTikHubEndpoint {
			endpointRec = c
		}
	}
	if endpointRec == nil {
		t.Fatal("缺端点记账行")
	}
	if !utf8.ValidString(endpointRec.ResultPreview) {
		t.Errorf("result_preview 含非法 UTF-8: %q", endpointRec.ResultPreview)
	}
	if strings.ContainsRune(endpointRec.ResultPreview, 0) {
		t.Errorf("result_preview 含 NUL: %q", endpointRec.ResultPreview)
	}
	// 会话消息里的端点结果同样必须净化（agent_sessions.messages JSONB 也拒 NUL）。
	sess := fs.sessions[1]
	if strings.ContainsRune(string(sess.Messages), 0) {
		t.Error("会话 messages 含 NUL")
	}
	if !utf8.ValidString(string(sess.Messages)) {
		t.Error("会话 messages 含非法 UTF-8")
	}
}

// TestSearchEndpoints_ResultIncludesDescription：检索结果文本含端点 description
// 全文（对抗审查 MEDIUM 漂移缺陷）——注入的工具定义只保留 300 rune 摘要，其前提
// 是「更全说明已在检索结果给过」，必须真给。
func TestSearchEndpoints_ResultIncludesDescription(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	// 找一个 description 明显长于 summary 的端点做断言。
	hits := tikhubcatalog.Search("小红书 搜索 笔记", "xiaohongshu", 5)
	var target tikhubcatalog.Entry
	for _, h := range hits {
		if len([]rune(h.Entry.Description)) > len([]rune(h.Entry.Summary))+20 {
			target = h.Entry
			break
		}
	}
	if target.Name == "" {
		t.Skip("样本里没有 description 显著长于 summary 的端点")
	}
	out, _ := ep.SearchTool().Execute(
		ctxWithRunState(&toolRunState{activation: &activationState{}}, nil), 1,
		json.RawMessage(`{"query":"小红书 搜索 笔记","platform":"xiaohongshu"}`))
	// 取 description 前 30 rune 作为存在性探针（避免整串匹配受换行影响）。
	probe := string([]rune(strings.TrimSpace(target.Description))[:30])
	if !strings.Contains(out, probe) {
		t.Errorf("检索结果应含端点 %s 的 description 片段 %q", target.Name, probe)
	}
}

// TestHandleMessage_UnactivatedEndpointRejected：模型跳过检索直呼注册表端点名
// （白名单红线）→ 标准"工具不存在"自纠文案。
func TestHandleMessage_UnactivatedEndpointRejected(t *testing.T) {
	fs := newFakeStore()
	inv := &fakeInvoker{}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)

	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: entry.Name, Arguments: `{"keyword":"x"}`}}, FinishReason: "tool_calls"},
		{Content: "好的", FinishReason: "stop"},
	}}
	l := New(Deps{
		Store: fs, Profiles: fs, Tools: BuildTools2Static(ep),
		Model: "m", MaxTurns: 5, SessionTTL: 30 * time.Minute, Endpoints: ep,
	})
	l.chatFn = chat.fn

	if _, err := l.HandleMessage(context.Background(), 1, "查小红书"); err != nil {
		t.Fatal(err)
	}
	if len(inv.calls) != 0 {
		t.Fatal("未激活端点不得触发上游调用")
	}
	msgs := persistedMessages(t, fs)
	foundReject := false
	for _, m := range msgs {
		if m.Role == "tool" && strings.Contains(m.Content, "不存在") {
			foundReject = true
		}
	}
	if !foundReject {
		t.Fatalf("应有白名单拒绝回执，实际 %+v", msgs)
	}
}

// TestHandleMessage_ActivationPersistsAcrossMessages：第二条消息（同会话）
// 直接调用第一条消息激活的端点——激活集跨消息有效（TTL 内）。
func TestHandleMessage_ActivationPersistsAcrossMessages(t *testing.T) {
	fs := newFakeStore()
	inv := &fakeInvoker{body: `{}`}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)

	chat := &scriptedChat{responses: []*llm.ChatResponse{
		// 消息 1：检索 → 收敛。
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "search_endpoints",
			Arguments: `{"query":"小红书 搜索 笔记"}`}}, FinishReason: "tool_calls"},
		{Content: "找到了几个接口", FinishReason: "stop"},
		// 消息 2：直接调用（无需再检索）。
		{ToolCalls: []llm.ToolCall{{ID: "c2", Name: entry.Name, Arguments: `{"keyword":"AI"}`}}, FinishReason: "tool_calls"},
		{Content: "结果如上", FinishReason: "stop"},
	}}
	l := New(Deps{
		Store: fs, Profiles: fs, Tools: BuildTools2Static(ep),
		Model: "m", MaxTurns: 5, SessionTTL: 30 * time.Minute, Endpoints: ep,
	})
	l.chatFn = chat.fn

	ctx := context.Background()
	if _, err := l.HandleMessage(ctx, 1, "有哪些小红书接口"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.HandleMessage(ctx, 1, "查 AI 关键词"); err != nil {
		t.Fatal(err)
	}
	if len(inv.calls) != 1 {
		t.Fatalf("第二条消息应能直接调用已激活端点，上游调用 %d 次", len(inv.calls))
	}
	// 消息 2 的首轮请求就应携带动态声明（激活集从会话行恢复）。
	msg2FirstReq := chat.requests[2]
	found := false
	for _, d := range msg2FirstReq.Tools {
		if d.Name == entry.Name {
			found = true
		}
	}
	if !found {
		t.Fatal("消息 2 首轮请求应含已激活端点声明")
	}
}

// TestNew_SystemPromptEndpointNote：装配端点工具面时 system prompt 注入能力说明，
// 未装配时不注入（没有工具却教模型用只会制造拒绝循环）。
func TestNew_SystemPromptEndpointNote(t *testing.T) {
	fs := newFakeStore()
	with := New(Deps{Store: fs, Profiles: fs, Endpoints: newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 1, 1)})
	if !strings.Contains(with.sys, "search_endpoints") {
		t.Error("装配时 system prompt 应含端点检索说明")
	}
	without := New(Deps{Store: fs, Profiles: fs})
	if strings.Contains(without.sys, "search_endpoints") {
		t.Error("未装配时 system prompt 不应提端点检索")
	}
}
