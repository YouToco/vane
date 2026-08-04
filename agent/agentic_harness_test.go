package agent

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/tikhubcatalog"
)

func TestGroundedResearchCompletesSearchReadAndAnswerInOneMessage(t *testing.T) {
	searcher := &fakeWebSearcher{results: []fetcher.SearchResult{{
		Title: "Introducing GPT-Live",
		URL:   "https://openai.com/index/introducing-gpt-live/",
		Text:  "We plan to bring GPT-Live to the API soon.",
	}}}
	reader := &fakePageReader{
		title: "Introducing GPT-Live",
		text:  "We plan to bring GPT-Live to the API soon. No API price is listed.",
	}
	exa := newTestExaTools(searcher, reader)
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "search", Name: "web_search", Arguments: `{}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "read", Name: "read_page",
			Arguments: `{"url":"https://openai.com/index/introducing-gpt-live/"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "OpenAI 官方页只说明 API 即将推出，当前证据没有提供 API 定价：https://openai.com/index/introducing-gpt-live/",
			FinishReason: "stop"},
	}}
	loop := newTestLoop(
		t, newFakeStore(), chat.fn, exa.SearchTool(), exa.ReadPageTool(),
	)

	out, err := loop.HandleExternalContextMessage(
		t.Context(), 7,
		"[追问上下文]\n旧推送正文\n[追问上下文结束]\n用户的追问：GPT-Live 是否已提供 API 定价？",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		out.Reply,
		"**来源**\n- [来源 · openai.com](https://openai.com/index/introducing-gpt-live/)",
	) {
		t.Fatalf("outcome=%+v", out)
	}
	if searcher.calls != 1 || reader.calls != 1 || len(chat.requests) != 3 {
		t.Fatalf("search=%d read=%d requests=%d",
			searcher.calls, reader.calls, len(chat.requests))
	}
	if len(chat.requests[1].Tools) == 0 {
		t.Fatal("first external result must retain a read-only research surface")
	}
}

func TestGroundedResearchRejectsSearchSummaryUntilPageRead(t *testing.T) {
	const official = "https://openai.com/index/introducing-gpt-live/"
	searcher := &fakeWebSearcher{results: []fetcher.SearchResult{{
		Title: "Introducing GPT-Live",
		URL:   official,
		Text:  "We plan to bring GPT-Live to the API soon.",
	}}}
	reader := &fakePageReader{
		title: "Introducing GPT-Live",
		text:  "We plan to bring GPT-Live to the API soon.",
	}
	exa := newTestExaTools(searcher, reader)
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "search", Name: "web_search",
			Arguments: `{"query":"OpenAI GPT-Live API pricing"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "只看摘要就回答：" + official, FinishReason: "stop"},
		{ToolCalls: []llm.ToolCall{{
			ID: "read", Name: "read_page",
			Arguments: `{"url":"` + official + `"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "官方只说 API 即将推出，尚无 API 定价：" + official,
			FinishReason: "stop"},
	}}
	loop := newTestLoop(
		t, newFakeStore(), chat.fn, exa.SearchTool(), exa.ReadPageTool(),
	)

	out, err := loop.HandleMessage(
		t.Context(), 7, "请查 GPT-Live 是否已经提供 API 定价",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply == replyGroundedPageNotRead ||
		!strings.Contains(out.Reply, "**来源**") ||
		searcher.calls != 1 || reader.calls != 1 || len(chat.requests) != 4 {
		t.Fatalf(
			"out=%+v search=%d read=%d requests=%d",
			out, searcher.calls, reader.calls, len(chat.requests),
		)
	}
	if !strings.Contains(
		chat.requests[2].Messages[0].Content,
		"必须调用 read_page",
	) {
		t.Fatal("page-read correction was not added to the retry system prompt")
	}
}

func TestGroundedResearchFailsClosedWhenPageIsNeverRead(t *testing.T) {
	const official = "https://openai.com/index/introducing-gpt-live/"
	searcher := &fakeWebSearcher{results: []fetcher.SearchResult{{
		Title: "Introducing GPT-Live",
		URL:   official,
		Text:  "Candidate summary only.",
	}}}
	exa := newTestExaTools(searcher, &fakePageReader{})
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "search", Name: "web_search",
			Arguments: `{"query":"OpenAI GPT-Live API pricing"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "第一次直接回答：" + official, FinishReason: "stop"},
		{Content: "第二次仍直接回答：" + official, FinishReason: "stop"},
	}}
	loop := newTestLoop(
		t, newFakeStore(), chat.fn, exa.SearchTool(), exa.ReadPageTool(),
	)

	out, err := loop.HandleMessage(
		t.Context(), 7, "请查 GPT-Live 是否已经提供 API 定价",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != replyGroundedPageNotRead ||
		searcher.calls != 1 || len(chat.requests) != 3 {
		t.Fatalf(
			"out=%+v search=%d requests=%d",
			out, searcher.calls, len(chat.requests),
		)
	}
}

func TestGroundedResearchRequiresReadAfterLatestSearch(t *testing.T) {
	const (
		initial  = "https://example.com/background"
		official = "https://openai.com/index/introducing-gpt-live/"
	)
	searcher := &fakeWebSearcher{results: []fetcher.SearchResult{{
		Title: "Introducing GPT-Live",
		URL:   official,
		Text:  "Candidate summary only.",
	}}}
	reader := &fakePageReader{
		title: "Public page",
		text:  "Public page body.",
	}
	exa := newTestExaTools(searcher, reader)
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "early-read", Name: "read_page",
			Arguments: `{"url":"` + initial + `"}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "later-search", Name: "web_search",
			Arguments: `{"query":"OpenAI GPT-Live API pricing"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "搜索摘要直接回答：" + official, FinishReason: "stop"},
		{ToolCalls: []llm.ToolCall{{
			ID: "required-read", Name: "read_page",
			Arguments: `{"url":"` + official + `"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "读取原文后回答：" + official, FinishReason: "stop"},
	}}
	loop := newTestLoop(
		t, newFakeStore(), chat.fn, exa.SearchTool(), exa.ReadPageTool(),
	)

	out, err := loop.HandleMessage(
		t.Context(), 7, "请核验 OpenAI GPT-Live API 定价",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply == replyGroundedPageNotRead ||
		!strings.Contains(out.Reply, "**来源**") ||
		searcher.calls != 1 || reader.calls != 2 || len(chat.requests) != 5 {
		t.Fatalf(
			"out=%+v search=%d reads=%d requests=%d",
			out, searcher.calls, reader.calls, len(chat.requests),
		)
	}
}

func TestGroundedResearchReservesFinalRoundAfterLastTurnPageRead(t *testing.T) {
	const official = "https://openai.com/index/introducing-gpt-live/"
	searcher := &fakeWebSearcher{results: []fetcher.SearchResult{{
		Title: "Introducing GPT-Live", URL: official,
		Text: "Candidate summary only.",
	}}}
	reader := &fakePageReader{title: "Official", text: "Official body."}
	exa := newTestExaTools(searcher, reader)
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "search", Name: "web_search",
			Arguments: `{"query":"OpenAI GPT-Live API pricing"}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "read", Name: "read_page",
			Arguments: `{"url":"` + official + `"}`,
		}}, FinishReason: "tool_calls"},
		{Content: "原文结论：" + official, FinishReason: "stop"},
	}}
	loop := newTestLoop(
		t, newFakeStore(), chat.fn, exa.SearchTool(), exa.ReadPageTool(),
	)
	loop.maxTurns = 2

	out, err := loop.HandleMessage(
		t.Context(), 7, "请核验 OpenAI GPT-Live API 定价",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply == replyMaxTurns || len(chat.requests) != 3 ||
		len(chat.requests[2].Tools) != 0 ||
		!strings.Contains(out.Reply, "**来源**") {
		t.Fatalf("out=%+v requests=%d final_tools=%d",
			out, len(chat.requests), len(chat.requests[2].Tools))
	}
}

func TestGroundedResearchRejectsToolsOnReservedFinalRound(t *testing.T) {
	const official = "https://openai.com/index/introducing-gpt-live/"
	searcher := &fakeWebSearcher{results: []fetcher.SearchResult{{
		Title: "Introducing GPT-Live", URL: official,
		Text: "Candidate summary only.",
	}}}
	reader := &fakePageReader{title: "Official", text: "Official body."}
	exa := newTestExaTools(searcher, reader)
	chat := &scriptedChat{responses: []*llm.ChatResponse{
		{ToolCalls: []llm.ToolCall{{
			ID: "search", Name: "web_search",
			Arguments: `{"query":"OpenAI GPT-Live API pricing"}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "read", Name: "read_page",
			Arguments: `{"url":"` + official + `"}`,
		}}, FinishReason: "tool_calls"},
		{ToolCalls: []llm.ToolCall{{
			ID: "hallucinated-read", Name: "read_page",
			Arguments: `{"url":"https://example.com/extra"}`,
		}}, FinishReason: "tool_calls"},
	}}
	loop := newTestLoop(
		t, newFakeStore(), chat.fn, exa.SearchTool(), exa.ReadPageTool(),
	)
	loop.maxTurns = 2

	out, err := loop.HandleMessage(
		t.Context(), 7, "请核验 OpenAI GPT-Live API 定价",
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != replyExternalProtocolFailure ||
		reader.calls != 1 || len(chat.requests) != 3 ||
		len(chat.requests[2].Tools) != 0 {
		t.Fatalf("out=%+v reads=%d requests=%d final_tools=%d",
			out, reader.calls, len(chat.requests), len(chat.requests[2].Tools))
	}
}

func TestOwnerAgentCatalogIsNotKeywordRouted(t *testing.T) {
	exa := NewExaTools(&fakeWebSearcher{}, &fakePageReader{}, nil, 0)
	chat := &scriptedChat{responses: []*llm.ChatResponse{{
		Content: "基于当前信息回答。",
	}}}
	tools := BuildOwnerTools(nil, ManageTasksDeps{}, nil, nil, exa)
	loop := New(Deps{
		Tools: tools, OwnerAgent: true, Evidence: &fakeAgentEvidenceWriter{},
	})
	loop.chatFn = chat.fn
	if _, _, err := loop.RunOnce(
		t.Context(), 7, nil, "GPT-Live 是否已提供 API 定价？",
	); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if len(chat.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(chat.requests))
	}
	got := make(map[string]bool, len(chat.requests[0].Tools))
	for _, def := range chat.requests[0].Tools {
		got[def.Name] = true
	}
	for _, required := range []string{"query_my_intelligence", "manage_tasks",
		"update_profile", "web_search", "read_page"} {
		if !got[required] {
			t.Fatalf("owner catalog missing %s: %v", required, got)
		}
	}
	for _, retired := range []string{"view_profile", "list_schedules", "create_schedule"} {
		if got[retired] {
			t.Fatalf("owner catalog exposed retired tool %s: %v", retired, got)
		}
	}
}

func TestUnifiedLoopFuseRejectsDuplicateAndCap(t *testing.T) {
	tool := &fakeTool{name: "read_once", result: "ok"}
	spec := newToolSpec(tool, ownerPolicy(
		Effects(EffectInternalRead), BudgetNone,
	))
	loop := New(Deps{})
	state := &toolRunState{}
	ctx := context.WithValue(
		context.Background(), toolRunKey{}, state,
	)
	if got, err := loop.execRecordedAgentic(
		ctx, 7, nil, spec, []byte(`{"x":1}`),
	); err != nil || got != "ok" {
		t.Fatalf("first execution got=%q err=%v", got, err)
	}
	if got, err := loop.execRecordedAgentic(
		ctx, 7, nil, spec, []byte(`{"x":1}`),
	); err != nil || got != toolMsgDuplicateCall || len(tool.calls) != 1 {
		t.Fatalf("duplicate got=%q err=%v calls=%d", got, err, len(tool.calls))
	}

	capped := &toolRunState{toolExecutions: maxToolExecutionsPerMessage}
	ctx = context.WithValue(context.Background(), toolRunKey{}, capped)
	if got, err := loop.execRecordedAgentic(
		ctx, 7, nil, spec, []byte(`{"x":2}`),
	); err != nil || got != toolMsgLoopFuse ||
		capped.loopBreakReason != "tool_execution_cap" {
		t.Fatalf("cap got=%q err=%v reason=%q", got, err, capped.loopBreakReason)
	}
}

func TestUnifiedLoopFuseReservesToolFreePartialSynthesis(t *testing.T) {
	tool := &fakeTool{name: "query_my_intelligence", result: `{"rows":[]}`}
	responses := make([]*llm.ChatResponse, 0, maxToolExecutionsPerMessage+2)
	for index := 0; index <= maxToolExecutionsPerMessage; index++ {
		responses = append(responses, &llm.ChatResponse{
			ToolCalls: []llm.ToolCall{{
				ID: "query_" + strconv.Itoa(index), Name: tool.name,
				Arguments: `{"window":` + strconv.Itoa(index) + `}`,
			}},
			FinishReason: "tool_calls",
		})
	}
	responses = append(responses, &llm.ChatResponse{
		Content: "基于已查到的证据给出部分结论，并明确历史缺口。",
	})
	chat := &scriptedChat{responses: responses}
	loop := New(Deps{
		Tools: testToolSpecs(tool), Model: "deepseek-v4-pro",
		MaxTurns: maxToolExecutionsPerMessage + 2,
	})
	loop.chatFn = chat.fn

	out, _, err := loop.RunOnce(t.Context(), 7, nil, "为什么当时这么判断？")
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "基于已查到的证据给出部分结论，并明确历史缺口。" {
		t.Fatalf("reply=%q", out.Reply)
	}
	if len(tool.calls) != maxToolExecutionsPerMessage {
		t.Fatalf("executed calls=%d, want %d", len(tool.calls), maxToolExecutionsPerMessage)
	}
	if len(chat.requests) != maxToolExecutionsPerMessage+2 {
		t.Fatalf("model turns=%d, want %d", len(chat.requests), maxToolExecutionsPerMessage+2)
	}
	if len(chat.requests[len(chat.requests)-1].Tools) != 0 {
		t.Fatal("post-fuse synthesis turn still exposed tools")
	}
}

func TestDynamicDescriptionsHideProviderAndTransport(t *testing.T) {
	entry := tikhubcatalog.Entry{
		Summary: "Search public posts",
		Description: "TikHub internal\nRequest Method: GET\n" +
			"Endpoint Path: /api/v1/private\nPurpose: search posts",
	}
	got := strings.ToLower(endpointDefDescription(entry))
	for _, forbidden := range []string{"tikhub", "/api/v1", "request method", "endpoint path"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("description leaked %q: %s", forbidden, got)
		}
	}
}

func TestProductionToolSchemasHideProvidersAndTransport(t *testing.T) {
	exa := NewExaTools(&fakeWebSearcher{}, &fakePageReader{}, nil, 0)
	endpoints := NewEndpointTools(nil, nil, 0, 1_000_000)
	static := BuildOwnerTools(nil, ManageTasksDeps{}, nil, endpoints, exa)
	for _, spec := range static {
		assertPublicToolSurface(t, spec.Name(),
			spec.Description(), string(spec.Parameters()))
	}
	for _, entry := range tikhubcatalog.Entries() {
		assertPublicToolSurface(t, entry.Name,
			endpointDefDescription(entry), string(endpointParamsSchema(entry)))
	}
}

func assertPublicToolSurface(
	t *testing.T,
	name string,
	values ...string,
) {
	t.Helper()
	visible := strings.ToLower(strings.Join(values, "\n"))
	for _, forbidden := range []string{
		"tikhub", "/api/v1", "/api/v2",
		"request method", "endpoint path",
	} {
		if strings.Contains(visible, forbidden) {
			t.Fatalf("tool %s leaked %q in model surface: %q", name, forbidden, visible)
		}
	}
	if regexp.MustCompile(`\bexa\b`).MatchString(visible) {
		t.Fatalf("tool %s leaked provider name Exa in model surface: %q", name, visible)
	}
}
