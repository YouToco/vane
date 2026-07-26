package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/agentcontext"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

type contextShadowStore struct {
	*fakeStore
	mu         sync.Mutex
	candidates []agentcontext.CandidateSnapshot
	sealErr    error
}

func (s *contextShadowStore) SealAgentTurnContextSnapshot(
	_ context.Context,
	_ agentcontext.Scope,
	candidate agentcontext.CandidateSnapshot,
) (agentcontext.SealResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candidates = append(s.candidates, candidate)
	if s.sealErr != nil {
		return agentcontext.SealResult{}, s.sealErr
	}
	return agentcontext.SealResult{Sealed: true}, nil
}

func (s *contextShadowStore) snapshots() []agentcontext.CandidateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]agentcontext.CandidateSnapshot(nil), s.candidates...)
}

func TestContextShadowDoesNotChangeLegacyRequestOrOutcome(t *testing.T) {
	base := newFakeStore()
	store := &contextShadowStore{
		fakeStore: base,
		sealErr:   errors.New("injected shadow failure"),
	}
	var got llm.ChatRequest
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, req llm.ChatRequest) (*llm.ChatResponse, error) {
			got = cloneChatRequest(req)
			return &llm.ChatResponse{Content: "legacy reply"}, nil
		},
	)
	outcome, err := loop.HandleMessage(t.Context(), 7, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Reply != "legacy reply" {
		t.Fatalf("outcome=%+v", outcome)
	}
	want := llm.ChatRequest{
		Model: "deepseek-v4-pro",
		Messages: []llm.ChatMessage{
			{Role: "system", Content: withSystem(loop.sys,
				[]llm.ChatMessage{}, "", loop.renderProfile)[0].Content},
			{Role: "user", Content: "hello"},
		},
		MaxTokens:       iptr(replyMaxTokens),
		DisableThinking: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chat request changed by shadow:\ngot=%+v\nwant=%+v", got, want)
	}
	snapshots := store.snapshots()
	if len(snapshots) != 1 || snapshots[0].ModelStep != 1 {
		t.Fatalf("snapshots=%+v", snapshots)
	}
}

func TestContextShadowTracksToolsetTransitionAndRedactsTaintedStep(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	const attack = "EXTERNAL-CONTENT-DO-NOT-SEAL"
	tool := &fakeTool{
		name: "external", result: attack, untrusted: true,
	}
	call := llm.ToolCall{ID: "call-1", Name: "external", Arguments: `{}`}
	requests := 0
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, req llm.ChatRequest) (*llm.ChatResponse, error) {
			requests++
			if requests == 1 {
				return &llm.ChatResponse{ToolCalls: []llm.ToolCall{call}}, nil
			}
			return &llm.ChatResponse{Content: "safe summary"}, nil
		},
		tool,
	)
	if _, err := loop.HandleMessage(t.Context(), 7, "research"); err != nil {
		t.Fatal(err)
	}
	snapshots := store.snapshots()
	if len(snapshots) != 2 {
		t.Fatalf("snapshots=%+v", snapshots)
	}
	if snapshots[0].ToolsetDigest == snapshots[1].ToolsetDigest {
		t.Fatal("two model steps did not capture the toolset transition")
	}
	if snapshots[1].Replayable ||
		snapshots[1].UntrustedDigest == "" {
		t.Fatalf("tainted snapshot=%+v", snapshots[1])
	}
	raw, err := json.Marshal(snapshots[1])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), attack) {
		t.Fatal("tainted external raw reached shadow candidate")
	}
}

func TestContextShadowCandidateMessagesExactlyMirrorProfileRequest(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	store.profiles[7] = &types.Profile{
		UserID: 7, Industry: "AI SaaS", Occupation: "Founder",
		Tags: []string{"agents", "security"},
	}
	var request llm.ChatRequest
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, got llm.ChatRequest) (*llm.ChatResponse, error) {
			request = cloneChatRequest(got)
			return &llm.ChatResponse{Content: "reply"}, nil
		},
	)
	if _, err := loop.HandleMessage(t.Context(), 7, "question"); err != nil {
		t.Fatal(err)
	}
	snapshots := store.snapshots()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots=%+v", snapshots)
	}
	want := make([]agentcontext.Message, len(request.Messages))
	for i, message := range request.Messages {
		want[i] = shadowMessage(message)
	}
	if !reflect.DeepEqual(snapshots[0].CandidateMessages, want) {
		t.Fatalf(
			"candidate does not mirror profile-bearing request:\ngot=%+v\nwant=%+v",
			snapshots[0].CandidateMessages, want,
		)
	}
	if !strings.Contains(
		snapshots[0].CandidateMessages[0].Content,
		"AI SaaS",
	) {
		t.Fatal("profile system content missing from candidate")
	}
}

func TestContextShadowUsesLLMContextWindowRegistry(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	loop := newContextShadowLoop(
		t, store,
		func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
			return &llm.ChatResponse{Content: "unused"}, nil
		},
	)
	meta := chatMeta{
		traceID: "turn-window", userID: 7,
		scope: agentcontext.Scope{
			TenantID: 1, UserID: 7, SessionID: 1,
		},
	}
	for _, model := range []string{"kimi-k2.6", "deepseek-v4-pro", "future-model"} {
		request := llm.ChatRequest{
			Model: model,
			Messages: []llm.ChatMessage{
				{Role: "system", Content: "system"},
				{Role: "user", Content: "question"},
			},
			MaxTokens: iptr(256),
		}
		candidate, err := loop.buildShadowAgentContext(
			meta, request, &toolRunState{activation: &activationState{}}, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		if candidate.ContextWindowTokens != llm.ContextWindowTokens(model) {
			t.Fatalf(
				"model %s window=%d want=%d",
				model, candidate.ContextWindowTokens,
				llm.ContextWindowTokens(model),
			)
		}
	}
}

func TestContextShadowDirectCreationHasNoHistoricalMessages(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	session, err := store.CreateAgentSession(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	session.Messages = json.RawMessage(
		`[{"role":"user","content":"old secret"},{"role":"assistant","content":"old answer"}]`,
	)
	store.sessions[session.ID] = session
	create := &fakeTool{name: "create_schedule", mutating: true}
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			return &llm.ChatResponse{
				ToolCalls: []llm.ToolCall{{
					ID: "create-1", Name: "create_schedule",
					Arguments: `{"name":"x"}`,
				}},
			}, nil
		},
		create,
	)
	loop.taskCreation = &fakeCreationController{
		proposeResult: task.CreationProposal{
			ID: "proposal-1", Summary: "create",
		},
	}
	_, _ = loop.HandleMessage(
		t.Context(), 7, "确认创建，直接生成确认卡，不要再次搜索。",
	)
	snapshots := store.snapshots()
	if len(snapshots) == 0 {
		t.Fatal("direct creation produced no shadow")
	}
	raw, _ := json.Marshal(snapshots[0].CandidateMessages)
	if strings.Contains(string(raw), "old secret") {
		t.Fatal("direct creation shadow retained cropped history")
	}
}

func TestContextShadowPendingFinalIsIndependentStep(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	write := &fakeTool{name: "write", mutating: true}
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			return &llm.ChatResponse{ToolCalls: []llm.ToolCall{{
				ID: "write-1", Name: "write", Arguments: `{}`,
			}}}, nil
		},
		write,
	)
	outcome, err := loop.HandleMessage(t.Context(), 7, "change")
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Confirm == nil {
		t.Fatal("expected pending confirmation")
	}
	snapshots := store.snapshots()
	if len(snapshots) != 2 ||
		snapshots[0].ModelStep != 1 ||
		snapshots[1].ModelStep != 2 {
		t.Fatalf("pending snapshots=%+v", snapshots)
	}
}

func TestRunOnceContextShadowNeverPersistsOwnerSnapshot(t *testing.T) {
	store := &contextShadowStore{fakeStore: newFakeStore()}
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			return &llm.ChatResponse{Content: "a2a"}, nil
		},
	)
	if _, _, err := loop.RunOnce(
		t.Context(), 7, nil, "a2a question",
	); err != nil {
		t.Fatal(err)
	}
	if snapshots := store.snapshots(); len(snapshots) != 0 {
		t.Fatalf("RunOnce persisted owner snapshots: %+v", snapshots)
	}
}

func newContextShadowLoop(
	t *testing.T,
	store *contextShadowStore,
	chat func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error),
	tools ...Tool,
) *Loop {
	t.Helper()
	loop := New(Deps{
		Store: store, Profiles: store,
		Tools: testToolSpecs(tools...),
		TaskCreation: &fakeCreationController{
			confirmErr: task.ErrCreationOperationNotFound,
			cancelErr:  task.ErrCreationOperationNotFound,
		},
		Model: "deepseek-v4-pro", MaxTurns: 5,
		SessionTTL: 30 * time.Minute,
	})
	loop.chatFn = chat
	return loop
}

func cloneChatRequest(request llm.ChatRequest) llm.ChatRequest {
	clone := request
	clone.Messages = append([]llm.ChatMessage(nil), request.Messages...)
	clone.Tools = append([]llm.ToolDef(nil), request.Tools...)
	return clone
}
