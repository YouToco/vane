package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/toolsearch"
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

type fakeEndpointCatalog struct {
	definitions  map[string]toolsearch.Entry
	providers    map[string]tikhubcatalog.Entry
	matches      []toolsearch.Match
	searchErr    error
	lastLimit    int
	lastPlatform string
	digest       string
}

func (f *fakeEndpointCatalog) SearchTools(_ string, platform string, limit int) ([]toolsearch.Match, error) {
	f.lastLimit = limit
	f.lastPlatform = platform
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	out := append([]toolsearch.Match(nil), f.matches...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (f *fakeEndpointCatalog) AgentDefinition(name string) (toolsearch.Entry, bool) {
	entry, ok := f.definitions[name]
	entry.Parameters = append(json.RawMessage(nil), entry.Parameters...)
	return entry, ok
}
func (f *fakeEndpointCatalog) AgentLookup(name string) (tikhubcatalog.Entry, bool) {
	entry, ok := f.providers[name]
	return entry, ok
}
func (*fakeEndpointCatalog) Platforms() []string { return []string{"test"} }
func (f *fakeEndpointCatalog) PlatformCount(platform string) int {
	if platform == "test" {
		return len(f.providers)
	}
	return 0
}
func (f *fakeEndpointCatalog) Digest() string {
	if f.digest != "" {
		return f.digest
	}
	return strings.Repeat("d", 64)
}

func newFakeCatalog(entries ...toolsearch.Entry) *fakeEndpointCatalog {
	f := &fakeEndpointCatalog{
		definitions: make(map[string]toolsearch.Entry, len(entries)),
		providers:   make(map[string]tikhubcatalog.Entry, len(entries)),
		matches:     make([]toolsearch.Match, 0, len(entries)),
	}
	for _, entry := range entries {
		f.definitions[entry.Name] = entry
		f.providers[entry.Name] = tikhubcatalog.Entry{Name: entry.Name, Platform: "test"}
		f.matches = append(f.matches, toolsearch.Match{Entry: entry})
	}
	return f
}

func fakeToolDefinition(name string, paddingBytes int) toolsearch.Entry {
	description := strings.Repeat("x", paddingBytes)
	schema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "description": description},
		},
	})
	if err != nil {
		panic(err)
	}
	return toolsearch.Entry{
		Namespace: "social/test", Name: name,
		Description: "test tool " + name, Parameters: schema,
	}
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

func TestActivationState_ActivateOrderDedupAndFailClosedLimit(t *testing.T) {
	a := &activationState{}
	catalog := productionEndpointCatalog{}
	entries := tikhubcatalog.Entries()
	if len(entries) < maxActivatedEndpoints+1 {
		t.Fatalf("测试目录过小: %d", len(entries))
	}
	for i := 0; i < maxActivatedEndpoints; i++ {
		if err := a.activateBatch(catalog, []string{entries[i].Name}); err != nil {
			t.Fatalf("未满员不应拒绝: %v", err)
		}
	}
	// 重复激活：不动位置、不增加名额。
	if err := a.activateBatch(catalog, []string{entries[3].Name, entries[3].Name}); err != nil {
		t.Fatalf("重复激活不应拒绝: %v", err)
	}
	if a.names[3] != entries[3].Name {
		t.Fatalf("重复激活不应改变位置，实际 names[3]=%q", a.names[3])
	}
	// 满员：新批次整体拒绝，旧前缀原字节不变。
	before := append([]string(nil), a.names...)
	if err := a.activateBatch(catalog, []string{entries[maxActivatedEndpoints].Name}); err == nil {
		t.Fatal("满员后应 fail-closed")
	}
	if fmt.Sprint(a.names) != fmt.Sprint(before) {
		t.Fatalf("超限拒绝后状态被改写: before=%v after=%v", before, a.names)
	}
}

func TestActivationState_EncodeDecodeRoundTrip(t *testing.T) {
	a := &activationState{}
	a.names = []string{"ep_b", "ep_a"}
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
	if got := decodeActivation(json.RawMessage(`["a","a"]`)); len(got.names) != 0 {
		t.Fatalf("持久化重复名应 fail-closed，实际 %v", got.names)
	}
	tooMany := make([]string, maxActivatedEndpoints+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("tool_%02d", i)
	}
	raw, err := json.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeActivation(raw); len(got.names) != 0 {
		t.Fatalf("持久化超过 %d 个工具应 fail-closed，实际 %v",
			maxActivatedEndpoints, got.names)
	}
}

// ============================================================
// tool_search
// ============================================================

func newTestEndpointTools(
	inv endpointInvoker,
	counter endpointCallCounter,
	_ int,
	dailyCap int,
) *EndpointTools {
	// 测试按 1M 窗口（生产 agent 模型 deepseek-v4-pro 同档）派生上限。
	return NewEndpointTools(inv, counter, dailyCap, 1_000_000)
}

func TestToolSearchTool_ActivatesAndRecords(t *testing.T) {
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
		t.Fatalf("检索命中后应激活端点: %s", out)
	}
	// 激活的端点必须逐个出现在结果文本里（模型靠文本知道有什么可调）。
	for _, name := range state.activation.names {
		if !strings.Contains(out, name) {
			t.Errorf("结果文本缺少已激活端点 %s", name)
		}
	}
	// 检索留痕（契约 §6）：query 与候选进记账记录。
	var audit toolSearchAuditV1
	if err := json.Unmarshal([]byte(rec.RetrievalQuery), &audit); err != nil {
		t.Fatalf("tool_search audit 非法: %v: %q", err, rec.RetrievalQuery)
	}
	if audit.SchemaVersion != toolSearchAuditSchema || audit.Status != "success" ||
		audit.QuerySHA256 != summarizedTextDigest("小红书 搜索 笔记") ||
		audit.PlatformSHA256 != summarizedTextDigest("xiaohongshu") ||
		audit.CatalogDigest != tikhubcatalog.AgentCatalogDigest() ||
		audit.CandidateCount != len(state.activation.names) || !audit.Truncated {
		t.Errorf("tool_search audit 留痕不符: %+v", audit)
	}
	if strings.Contains(rec.RetrievalQuery, "小红书") || strings.Contains(string(rec.Arguments), "小红书") {
		t.Fatalf("完整 query 泄露进审计字段: retrieval=%q args=%s",
			rec.RetrievalQuery, rec.Arguments)
	}
	if len(rec.CandidateTools) != len(state.activation.names) {
		t.Errorf("候选留痕 %d 与激活数 %d 不符", len(rec.CandidateTools), len(state.activation.names))
	}
	for _, candidate := range rec.CandidateTools {
		if !strings.Contains(candidate, "\tscore=") {
			t.Errorf("候选缺稳定 score: %q", candidate)
		}
	}
	// 平台过滤是硬约束。
	for _, name := range state.activation.names {
		e, _ := tikhubcatalog.Lookup(name)
		if e.Platform != "xiaohongshu" {
			t.Errorf("platform 过滤失效：激活了 %s（%s）", name, e.Platform)
		}
	}
}

func TestToolSearchTool_SelfCorrectMessages(t *testing.T) {
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

func TestToolSearchAudit_StatusesAreStableAndBounded(t *testing.T) {
	definition := fakeToolDefinition("audit_tool", 0)
	for _, tc := range []struct {
		name          string
		catalog       *fakeEndpointCatalog
		args          json.RawMessage
		wantStatus    string
		wantError     string
		wantTruncated bool
	}{
		{
			name: "invalid", catalog: newFakeCatalog(definition),
			args:       json.RawMessage(`{"query":"owner-secret","limit":null}`),
			wantStatus: "invalid", wantError: types.ToolErrInvalidArgs,
		},
		{
			name: "zero", catalog: newFakeCatalog(),
			args:       json.RawMessage(`{"query":"owner-secret"}`),
			wantStatus: "zero",
		},
		{
			name: "error", catalog: &fakeEndpointCatalog{
				definitions: map[string]toolsearch.Entry{},
				providers:   map[string]tikhubcatalog.Entry{},
				searchErr:   errors.New("catalog unavailable: secret-internal"),
			},
			args:       json.RawMessage(`{"query":"owner-secret"}`),
			wantStatus: "error", wantError: types.ToolErrInternal,
		},
		{
			name: "success", catalog: newFakeCatalog(definition),
			args:       json.RawMessage(`{"query":"owner-secret","limit":1}`),
			wantStatus: "success", wantTruncated: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
			ep.catalog = tc.catalog
			rec := &types.ToolCall{}
			state := &toolRunState{activation: &activationState{}}
			if _, err := ep.SearchTool().Execute(ctxWithRunState(state, rec), 1, tc.args); err != nil {
				t.Fatal(err)
			}
			var audit toolSearchAuditV1
			if err := json.Unmarshal([]byte(rec.RetrievalQuery), &audit); err != nil {
				t.Fatalf("audit=%q err=%v", rec.RetrievalQuery, err)
			}
			if audit.Status != tc.wantStatus || audit.Truncated != tc.wantTruncated ||
				audit.SchemaVersion != toolSearchAuditSchema ||
				audit.CatalogDigest != tc.catalog.Digest() || rec.ErrorType != tc.wantError {
				t.Fatalf("audit=%+v error_type=%q", audit, rec.ErrorType)
			}
			for _, persisted := range []string{
				rec.RetrievalQuery, string(rec.Arguments), strings.Join(rec.CandidateTools, "\n"), rec.Error,
			} {
				if strings.Contains(persisted, "owner-secret") ||
					strings.Contains(persisted, "secret-internal") {
					t.Fatalf("审计泄露完整输入/错误: %q", persisted)
				}
			}
			if len(rec.RetrievalQuery) > 1024 || len(rec.Arguments) > 512 || len(rec.CandidateTools) > 8 {
				t.Fatalf("审计越界: retrieval=%d args=%d candidates=%d",
					len(rec.RetrievalQuery), len(rec.Arguments), len(rec.CandidateTools))
			}
		})
	}
}

func TestToolSearch_StrictBoundedArgumentsAndDefaultLimit(t *testing.T) {
	definition := fakeToolDefinition("test_search", 0)
	catalog := newFakeCatalog(definition)
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	ep.catalog = catalog
	tool := ep.SearchTool()

	state := &toolRunState{activation: &activationState{}}
	if out, _ := tool.Execute(ctxWithRunState(state, nil), 1,
		json.RawMessage(`{"query":"posts"}`)); !strings.Contains(out, definition.Name) {
		t.Fatalf("默认检索未命中: %q", out)
	}
	if catalog.lastLimit != 8 {
		t.Fatalf("缺省 limit=%d, want 8", catalog.lastLimit)
	}
	if out, _ := tool.Execute(ctxWithRunState(state, nil), 1,
		json.RawMessage(`{"query":"posts","platform":" TEST ","limit":1}`)); !strings.Contains(out, definition.Name) {
		t.Fatalf("显式 limit 检索未命中: %q", out)
	}
	if catalog.lastLimit != 1 || catalog.lastPlatform != "test" {
		t.Fatalf("catalog args limit=%d platform=%q", catalog.lastLimit, catalog.lastPlatform)
	}

	tooLong, _ := json.Marshal(map[string]string{"query": strings.Repeat("a", 513)})
	invalidUTF8 := append([]byte(`{"query":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	for _, tc := range []struct {
		name string
		raw  json.RawMessage
	}{
		{"unknown", json.RawMessage(`{"query":"x","other":1}`)},
		{"wrong_query_type", json.RawMessage(`{"query":1}`)},
		{"invalid_utf8", invalidUTF8},
		{"oversize", tooLong},
		{"limit_zero", json.RawMessage(`{"query":"x","limit":0}`)},
		{"limit_nine", json.RawMessage(`{"query":"x","limit":9}`)},
		{"limit_type", json.RawMessage(`{"query":"x","limit":"8"}`)},
		{"limit_null", json.RawMessage(`{"query":"x","limit":null}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := append([]string(nil), state.activation.names...)
			out, err := tool.Execute(ctxWithRunState(state, nil), 1, tc.raw)
			if err != nil || out == "" {
				t.Fatalf("err=%v out=%q", err, out)
			}
			if fmt.Sprint(before) != fmt.Sprint(state.activation.names) {
				t.Fatalf("非法参数改写了 activation: %v -> %v", before, state.activation.names)
			}
		})
	}
}

func TestToolSearch_ActivationCapsAreAtomic(t *testing.T) {
	t.Run("schema_bytes", func(t *testing.T) {
		first := fakeToolDefinition("large_one", 33<<10)
		second := fakeToolDefinition("large_two", 33<<10)
		catalog := newFakeCatalog(first, second)
		ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
		ep.catalog = catalog
		state := &toolRunState{activation: &activationState{}}
		out, err := ep.SearchTool().Execute(ctxWithRunState(state, nil), 1,
			json.RawMessage(`{"query":"large"}`))
		if err != nil || !strings.Contains(out, "全部拒绝") {
			t.Fatalf("err=%v out=%q", err, out)
		}
		if len(state.activation.names) != 0 {
			t.Fatalf("64KiB 超限后不得部分激活: %v", state.activation.names)
		}
	})

	t.Run("count_and_duplicates", func(t *testing.T) {
		definitions := make([]toolsearch.Entry, 0, maxActivatedEndpoints+1)
		for i := 0; i < maxActivatedEndpoints+1; i++ {
			definitions = append(definitions, fakeToolDefinition(fmt.Sprintf("tool_%02d", i), 0))
		}
		catalog := newFakeCatalog(definitions...)
		state := &activationState{}
		firstBatch := make([]string, 0, maxActivatedEndpoints)
		for _, definition := range definitions[:maxActivatedEndpoints] {
			firstBatch = append(firstBatch, definition.Name, definition.Name)
		}
		if err := state.activateBatch(catalog, firstBatch); err != nil {
			t.Fatal(err)
		}
		if len(state.names) != maxActivatedEndpoints {
			t.Fatalf("重复命中未去重: %d", len(state.names))
		}
		before := append([]string(nil), state.names...)
		if err := state.activateBatch(catalog, []string{definitions[maxActivatedEndpoints].Name}); err == nil {
			t.Fatal("17th tool should fail closed")
		}
		if fmt.Sprint(before) != fmt.Sprint(state.names) {
			t.Fatalf("超限批次改写状态: %v", state.names)
		}
	})
}

func TestToolSearch_MissingInvokerCannotAdvertiseOrActivate(t *testing.T) {
	ep := NewEndpointTools(nil, &fakeCounter{}, 200, 1_000_000)
	if _, err := NewChecked(Deps{
		Tools: BuildTools2Static(ep), Endpoints: ep,
	}); err == nil || !strings.Contains(err.Error(), "authorized endpoint invoker") {
		t.Fatalf("缺少授权 invoker 时应启动失败: %v", err)
	}
	state := &toolRunState{activation: &activationState{names: []string{testEndpoint(t).Name}}}
	out, err := ep.SearchTool().Execute(ctxWithRunState(state, nil), 1,
		json.RawMessage(`{"query":"posts"}`))
	if err != nil || !strings.Contains(out, "未授权") {
		t.Fatalf("err=%v out=%q", err, out)
	}
	if defs := ep.Defs(state.activation); len(defs) != 0 {
		t.Fatalf("缺 invoker 时不得声明动态工具: %+v", defs)
	}
	if _, ok := ep.Resolve(testEndpoint(t).Name, state.activation); ok {
		t.Fatal("缺 invoker 时不得解析动态工具")
	}
}

func TestDynamicDefinitionsRevalidateCurrentCatalogEveryRound(t *testing.T) {
	first := fakeToolDefinition("mutable_tool", 0)
	catalog := newFakeCatalog(first)
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	ep.catalog = catalog
	state := &activationState{names: []string{first.Name}}
	if defs := ep.Defs(state); len(defs) != 1 ||
		!bytes.Equal(defs[0].Parameters, first.Parameters) {
		t.Fatalf("initial defs=%+v", defs)
	}

	changed := fakeToolDefinition(first.Name, 32)
	catalog.definitions[first.Name] = changed
	if defs := ep.Defs(state); len(defs) != 1 ||
		!bytes.Equal(defs[0].Parameters, changed.Parameters) {
		t.Fatalf("未从当前 catalog 重验 schema: %+v", defs)
	}
	delete(catalog.providers, first.Name)
	if defs := ep.Defs(state); len(defs) != 0 {
		t.Fatalf("调用路由撤权后仍在声明: %+v", defs)
	}
	if len(state.names) != 0 {
		t.Fatalf("调用路由撤权后 stale activation 未剪掉: %v", state.names)
	}
}

// TestToolSearchTool_NoRunStateStillSearches：ctx 无运行状态（防御路径）时
// 检索照常返回，只是无法激活——不 panic 是底线。
func TestToolSearchTool_NoRunStateStillSearches(t *testing.T) {
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
	if !isUntrustedResultTool(tool) {
		t.Fatal("动态端点返回第三方内容，必须进入 untrusted-result 边界")
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

func TestEndpointTool_LegacyMsgCapDoesNotLimitPlanning(t *testing.T) {
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
	if err != nil || out != "{}" {
		t.Fatalf("第 2 次应继续执行: err=%v out=%q", err, out)
	}
	if len(inv.calls) != 2 {
		t.Fatalf("旧消息限额不应拦截，实得 %d 次", len(inv.calls))
	}
	if rec.ErrorType != "" {
		t.Errorf("unexpected error type %q", rec.ErrorType)
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

	// Retryable transport failures reach the unified loop retry path.
	epErr := newTestEndpointTools(&fakeInvoker{err: types.NewAppError(types.CodeFetchTimeout, "TikHub 端点 x 调用超时", fmt.Errorf("内部细节"))}, &fakeCounter{}, 10, 200)
	tool, _ := epErr.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	out, err := tool.Execute(state(), 1, args)
	if err == nil || out != "" || !types.IsRetryable(err) {
		t.Errorf("retryable failure should reach harness: err=%v out=%q", err, out)
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
	big := strings.Repeat("数", testLimits.PerCall+100)
	ep := newTestEndpointTools(&fakeInvoker{body: big}, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	tool, _ := ep.Resolve(entry.Name, &activationState{names: []string{entry.Name}})
	out, err := tool.Execute(ctxWithRunState(&toolRunState{activation: &activationState{}}, nil), 1, json.RawMessage(`{"keyword":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	// 契约 §3.5：截断提示必须带可兑现的句柄 + 取回工具指引 + 预览混淆压制。
	if !strings.Contains(out, "res-") || !strings.Contains(out, "read_endpoint_result") {
		t.Errorf("超限结果应给缓存句柄与取回指引: %s", out[len(out)-200:])
	}
	if !strings.Contains(out, "不要把上面的预览当作完整数据回答") {
		t.Error("缺预览混淆压制话术")
	}
	if got := len([]rune(out)); got > testLimits.PerCall+600 {
		t.Errorf("截断后（含提示）仍有 %d rune，提示开销超预算", got)
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
	// 已激活但注册表没有（re-gen 下线）：逐轮重验后剪掉 stale 名称，
	// 其他仍在当前目录+调用路由的名称可继续使用。
	ghost := &activationState{names: []string{"ghost_endpoint", entry.Name}}
	if _, ok := ep.Resolve("ghost_endpoint", ghost); ok {
		t.Fatal("注册表已下线的端点不应解析")
	}
	defs := ep.Defs(ghost)
	if len(defs) != 1 || defs[0].Name != entry.Name {
		t.Fatalf("Defs 应剪掉 stale authority 并保留当前有效声明，实得 %+v", defs)
	}
	if len(ghost.names) != 1 || ghost.names[0] != entry.Name {
		t.Fatalf("stale activation 未被剪掉: %v", ghost.names)
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
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "tool_search",
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
		t.Fatalf("轮 1 应只带静态 tool_search 声明，实得 %d 个", n)
	}
	r2names := map[string]bool{}
	var injected llm.ToolDef
	for _, d := range chat.requests[1].Tools {
		r2names[d.Name] = true
		if d.Name == entry.Name {
			injected = d
		}
	}
	if !r2names[entry.Name] {
		t.Fatalf("轮 2 应含已激活端点 %s 的声明，实得 %v", entry.Name, r2names)
	}
	canonical, ok := tikhubcatalog.AgentDefinition(entry.Name)
	if !ok || injected.Description != canonical.Description ||
		!bytes.Equal(injected.Parameters, canonical.Parameters) {
		t.Fatalf("注入声明不是 AgentDefinition 原字节: injected=%+v canonical=%+v",
			injected, canonical)
	}
	// 静态声明必须仍在最前（前缀稳定纪律）。
	if chat.requests[1].Tools[0].Name != "tool_search" {
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
	var persistedAudit toolSearchAuditV1
	if err := json.Unmarshal([]byte(search.RetrievalQuery), &persistedAudit); err != nil ||
		persistedAudit.Status != "success" || !persistedAudit.Truncated ||
		persistedAudit.CatalogDigest != tikhubcatalog.AgentCatalogDigest() ||
		search.DurationMs < 0 || search.ResultSize != len(search.ResultPreview) {
		t.Errorf("search bounded audit 不符: audit=%+v receipt=%+v err=%v",
			persistedAudit, search, err)
	}
	if endpoint.ToolKind != types.ToolCallKindTikHubEndpoint || endpoint.EndpointPath != entry.Path ||
		endpoint.HTTPStatus == nil || *endpoint.HTTPStatus != 200 {
		t.Errorf("endpoint 记账不符: %+v", endpoint)
	}
	if search.TraceID == "" || search.TraceID != endpoint.TraceID {
		t.Errorf("两条记账应共享同一 trace_id: %q vs %q", search.TraceID, endpoint.TraceID)
	}
}

func TestHandleMessage_TaintedEndpointKeepsReadOnlyResearchAndCurrentCache(t *testing.T) {
	fs := newFakeStore()
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	const victimSecret = "VICTIM-OTHER-SESSION-CANARY"
	victimHandle := ep.results.put(1, entry.Name, []byte(victimSecret))
	if victimHandle != "res-1" {
		t.Fatalf("测试前置句柄=%q，期望 res-1", victimHandle)
	}
	largeBody := strings.Repeat("x", ep.limits.PerCall+10) + "CURRENT-PRIVATE-TAIL"
	ep.inv = &fakeInvoker{body: largeBody}

	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{ID: "search", Name: "tool_search",
			Arguments: `{"query":"小红书 搜索 笔记"}`}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{ID: "endpoint", Name: entry.Name,
			Arguments: `{"keyword":"AI"}`}}, FinishReason: "tool_calls"},
		// 恶意端点结果猜同用户旧句柄：即使 cache.get 的 userID 会通过，也必须
		// 被本消息 allowed handle 集合挡住。
		{ToolCalls: []llm.ToolCall{{ID: "victim-page", Name: "read_endpoint_result",
			Arguments: `{"handle":"res-1","limit":100}`}}, FinishReason: "tool_calls"},
		// 当前端点刚生成的是 res-2，允许从被截断位置续读。
		{ToolCalls: []llm.ToolCall{{ID: "current-page", Name: "read_endpoint_result",
			Arguments: fmt.Sprintf(`{"handle":"res-2","offset":%d,"limit":100}`, ep.limits.PerCall+10)}}, FinishReason: "tool_calls"},
		{Content: "已基于本地缓存结果回答", FinishReason: "stop"},
	}}
	l := New(Deps{
		Store: fs, Profiles: fs,
		Tools: []ToolSpec{ep.SearchTool(), ep.ReadResultTool()},
		Model: "m", MaxTurns: 5, SessionTTL: 30 * time.Minute,
		Endpoints: ep,
	})
	l.chatFn = chat.fn

	if _, err := l.HandleMessage(context.Background(), 1, "查小红书大结果"); err != nil {
		t.Fatal(err)
	}
	if len(chat.requests) != 5 {
		t.Fatalf("期望检索、端点、越权拒绝、当前缓存续读、收敛五轮，实得 %d", len(chat.requests))
	}
	for _, idx := range []int{2, 3, 4} {
		defs := chat.requests[idx].Tools
		foundRead := false
		for _, def := range defs {
			if def.Name == "read_endpoint_result" {
				foundRead = true
			}
		}
		if !foundRead {
			t.Fatalf("taint 后第 %d 轮应保留本轮缓存续读，实得 %+v", idx+1, defs)
		}
	}
	afterVictim, _ := json.Marshal(chat.requests[3].Messages)
	if strings.Contains(string(afterVictim), victimSecret) {
		t.Fatalf("taint 后不得读取同用户其他会话句柄: %s", afterVictim)
	}
	// The outbound projection intentionally strips rejected native tool
	// protocol; absence of the victim secret proves the guessed handle did not
	// cross the per-turn allow-set.
	afterCurrent, _ := json.Marshal(chat.requests[4].Messages)
	if strings.Contains(string(afterCurrent), victimSecret) {
		t.Fatalf("续读投影不得混入其他会话句柄，实得 %s", afterCurrent)
	}
	// 不是动态端点产生的 taint 不得凭空取得旧缓存读取能力。
	state := &toolRunState{activation: &activationState{}, untrustedExternalResult: true}
	if defs := l.requestTools(state); len(defs) != 0 {
		t.Fatalf("没有本轮缓存句柄时不得声明 read_endpoint_result，实得 %+v", defs)
	}
}

func TestRunToolCalls_TaintedAllowsCurrentCacheReadsWithinUnifiedFuse(t *testing.T) {
	fs := newFakeStore()
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 10, 200)
	h1 := ep.results.put(1, "ep", []byte("CURRENT-ONE"))
	h2 := ep.results.put(1, "ep", []byte("CURRENT-TWO"))
	state := &toolRunState{
		activation:              &activationState{},
		untrustedExternalResult: true,
	}
	state.allowLocalResultHandle(h1)
	state.allowLocalResultHandle(h2)
	l := newTestLoop(t, fs, (&scriptedChat{}).fn, ep.ReadResultTool())
	ctx := context.WithValue(context.Background(), toolRunKey{}, state)

	replies, err := l.runToolCalls(ctx, 1, nil, []llm.ToolCall{
		{ID: "first", Name: "read_endpoint_result", Arguments: fmt.Sprintf(`{"handle":%q}`, h1)},
		{ID: "second", Name: "read_endpoint_result", Arguments: fmt.Sprintf(`{"handle":%q}`, h2)},
	})
	if err != nil {
		t.Fatalf("runToolCalls: %v", err)
	}
	if len(replies) != 2 || !strings.Contains(replies[0].Content, "CURRENT-ONE") ||
		!strings.Contains(replies[1].Content, "CURRENT-TWO") {
		t.Fatalf("两个当前句柄均应可续读，实得 %+v", replies)
	}
}

func TestHandleMessage_SearchAndUnactivatedEndpointSameBatchAreBothRejected(t *testing.T) {
	fs := newFakeStore()
	inv := &fakeInvoker{body: `{"code":200,"data":"ok"}`}
	ep := newTestEndpointTools(inv, &fakeCounter{}, 10, 200)
	entry := testEndpoint(t)
	searchArgs := `{"query":"小红书 搜索 笔记","platform":"xiaohongshu"}`
	endpointArgs := `{"keyword":"AI"}`
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		// 端点在预扫描时尚未激活；若先执行 search 再顺序 Resolve，旧实现会让
		// 同批付费调用穿透。整批必须零执行。
		{ToolCalls: []llm.ToolCall{
			{ID: "search-batch", Name: "tool_search", Arguments: searchArgs},
			{ID: "endpoint-batch", Name: entry.Name, Arguments: endpointArgs},
		}, FinishReason: "tool_calls"},
		// 按固定回执要求拆成两轮后才允许各自执行。
		{ToolCalls: []llm.ToolCall{{
			ID: "search-only", Name: "tool_search", Arguments: searchArgs,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "endpoint-only", Name: entry.Name, Arguments: endpointArgs,
		}}, FinishReason: "tool_calls"},
		{Content: "查到了", FinishReason: "stop"},
	}}
	l := New(Deps{
		Store: fs, Profiles: fs,
		Tools:      BuildTools2Static(ep),
		Model:      "deepseek-v4-pro",
		MaxTurns:   6,
		SessionTTL: 30 * time.Minute,
		Endpoints:  ep,
	})
	l.chatFn = chat.fn

	out, err := l.HandleMessage(context.Background(), 1, "查小红书 AI")
	if err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if out.Reply != "查到了" {
		t.Fatalf("Reply=%q", out.Reply)
	}
	if len(inv.calls) != 1 {
		t.Fatalf("同批未激活端点不得提前付费调用；拆分后才应调用 1 次，实得 %d", len(inv.calls))
	}
	if len(chat.requests) != 4 {
		t.Fatalf("期望批拒+检索+端点+收敛共 4 次请求，实得 %d", len(chat.requests))
	}
	if defs := chat.requests[1].Tools; len(defs) != 1 || defs[0].Name != "tool_search" {
		t.Fatalf("整批拒绝后 activation 必须仍为空，第二轮只能声明 tool_search，实得 %+v", defs)
	}
	replies := map[string]string{}
	for _, m := range chat.requests[1].Messages {
		if m.Role == "tool" {
			replies[m.ToolCallID] = m.Content
		}
	}
	for _, id := range []string{"search-batch", "endpoint-batch"} {
		if replies[id] != toolMsgExternalBatch {
			t.Fatalf("%s 应命中整批固定拒绝，实得 %q", id, replies[id])
		}
	}
}

// BuildTools2Static 测试装配：只装 tool_search（其余静态工具与本特性无关）。
func BuildTools2Static(ep *EndpointTools) []ToolSpec {
	return []ToolSpec{ep.SearchTool()}
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
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "tool_search", Arguments: `{"query":"小红书 搜索 笔记"}`}}, FinishReason: "tool_calls"},
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
	if endpointRec.Provider != "tikhub" || endpointRec.UsageQuantity != 1 {
		t.Fatalf("TikHub endpoint billing receipt = provider %q quantity %v",
			endpointRec.Provider, endpointRec.UsageQuantity)
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

// TestToolSearch_ResultIncludesDescription：检索结果文本含端点 description
// 全文（对抗审查 MEDIUM 漂移缺陷）——注入的工具定义只保留 300 rune 摘要，其前提
// 是「更全说明已在检索结果给过」，必须真给。
func TestToolSearch_ResultIncludesDescription(t *testing.T) {
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
		{ToolCalls: []llm.ToolCall{{ID: "c1", Name: "tool_search",
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
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 1, 1)
	with := New(Deps{Store: fs, Profiles: fs, Tools: BuildTools2Static(ep), Endpoints: ep})
	if !strings.Contains(with.sys, "tool_search") {
		t.Error("装配时 system prompt 应含端点检索说明")
	}
	without := New(Deps{Store: fs, Profiles: fs, Endpoints: ep})
	if strings.Contains(without.sys, "tool_search") {
		t.Error("未注册 tool_search 时 system prompt 不应广告端点检索")
	}
}

func TestNewChecked_DeferredToolCollisionsAndSplitResolverFailClosed(t *testing.T) {
	for _, name := range []string{"tool_search", "read_endpoint_result", "search_endpoints", "static_collision"} {
		t.Run(name, func(t *testing.T) {
			ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 1, 1)
			ep.catalog = newFakeCatalog(fakeToolDefinition(name, 0))
			tools := BuildTools2Static(ep)
			if name == "static_collision" {
				tools = append(tools, testToolSpecs(&fakeTool{name: name})...)
			}
			if _, err := NewChecked(Deps{Tools: tools, Endpoints: ep}); err == nil {
				t.Fatalf("deferred/static collision %q should fail startup", name)
			}
		})
	}
	if _, err := NewChecked(Deps{
		Tools: testToolSpecs(&fakeTool{name: "search_endpoints"}),
	}); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("retired static search_endpoints should fail startup: %v", err)
	}
	retiredCatalog := newFakeCatalog(fakeToolDefinition("search_endpoints", 0))
	if err := (&activationState{}).activateBatch(retiredCatalog, []string{"search_endpoints"}); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("retired deferred search_endpoints should not activate: %v", err)
	}

	epOne := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 1, 1)
	epTwo := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 1, 1)
	if _, err := NewChecked(Deps{
		Tools: BuildTools2Static(epOne), Endpoints: epTwo,
	}); err == nil || !strings.Contains(err.Error(), "split") {
		t.Fatalf("split tool_search/resolver should fail startup: %v", err)
	}
}

func TestToolSearch_OwnerOnlyAndLegacyNameAbsent(t *testing.T) {
	ep := newTestEndpointTools(&fakeInvoker{}, &fakeCounter{}, 1, 1)
	public := BuildPublicResearchTools(ep, nil)
	a2aTools, err := FilterAuthorizedTools(public, AuthorizationA2AReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(a2aTools) != 0 {
		t.Fatalf("A2A must remain tool-free, got %+v", a2aTools)
	}
	loop := New(Deps{Tools: BuildTools2Static(ep), Endpoints: ep})
	state := &toolRunState{activation: &activationState{}}
	if _, ok := loop.resolveTool("search_endpoints", state); ok {
		t.Fatal("retired search_endpoints must not resolve")
	}
	defs := loop.requestTools(state)
	if len(defs) != 1 || defs[0].Name != "tool_search" {
		t.Fatalf("initial inventory=%+v, want only tool_search", defs)
	}
}
