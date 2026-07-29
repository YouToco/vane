package agent

import (
	"context"
	"regexp"
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
	if out.Confirm != nil ||
		!strings.Contains(out.Reply, "https://openai.com/") {
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

func TestIntentToolkitsNarrowFirstRequest(t *testing.T) {
	exa := NewExaTools(&fakeWebSearcher{}, &fakePageReader{}, nil, 0, 0)
	loop := New(Deps{Tools: BuildTools(
		nil, nil, nil, nil, nil, nil, exa,
	)})
	state := &toolRunState{
		ownerRequest: "GPT-Live 是否已提供 API 定价？",
		intents:      classifyOwnerIntents("GPT-Live 是否已提供 API 定价？"),
	}
	defs := loop.requestTools(state)
	got := make(map[string]bool, len(defs))
	for _, def := range defs {
		got[def.Name] = true
	}
	if !got["web_search"] || !got["read_page"] {
		t.Fatalf("web toolkit missing: %v", got)
	}
	for _, hidden := range []string{
		"view_profile", "list_sources", "list_schedules", "create_schedule",
	} {
		if got[hidden] {
			t.Errorf("unrelated tool %s was exposed", hidden)
		}
	}
}

func TestUnifiedLoopFuseRejectsDuplicateAndCap(t *testing.T) {
	tool := &fakeTool{name: "read_once", result: "ok"}
	spec := newToolSpec(tool, ownerPolicy(
		Effects(EffectInternalRead), ConfirmationNone, BudgetNone,
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
	exa := NewExaTools(&fakeWebSearcher{}, &fakePageReader{}, nil, 0, 0)
	endpoints := NewEndpointTools(nil, nil, 0, 0, 1_000_000)
	static := BuildTools(
		nil, nil, nil, nil, endpoints, nil, exa,
		&fakeDefinitionEditController{},
	)
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
