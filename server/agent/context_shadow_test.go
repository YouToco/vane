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

	"github.com/YouToco/vane/server/agentcontext"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/types"
)

type contextShadowStore struct {
	*fakeStore
	mu             sync.Mutex
	candidates     []agentcontext.CandidateSnapshot
	sealErr        error
	sealPanic      bool
	sealStarted    chan context.Context
	attemptStarted chan contextShadowAttempt
	sealRelease    <-chan struct{}
	blockStep      int
	blockStarted   chan<- struct{}
	blockRelease   <-chan struct{}
}

type contextShadowAttempt struct {
	ctx       context.Context
	candidate agentcontext.CandidateSnapshot
}

func (s *contextShadowStore) SealAgentTurnContextSnapshot(
	ctx context.Context,
	_ agentcontext.Scope,
	candidate agentcontext.CandidateSnapshot,
) (agentcontext.SealResult, error) {
	if s.sealPanic {
		panic("context shadow test panic")
	}
	if s.sealStarted != nil {
		s.sealStarted <- ctx
	}
	if s.attemptStarted != nil {
		s.attemptStarted <- contextShadowAttempt{
			ctx: ctx, candidate: candidate,
		}
	}
	if candidate.ContextStep == s.blockStep && s.blockRelease != nil {
		if s.blockStarted != nil {
			s.blockStarted <- struct{}{}
		}
		select {
		case <-s.blockRelease:
		case <-ctx.Done():
			return agentcontext.SealResult{}, ctx.Err()
		}
	}
	if s.sealRelease != nil {
		select {
		case <-s.sealRelease:
		case <-ctx.Done():
			return agentcontext.SealResult{}, ctx.Err()
		}
	}
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

func TestContextShadowSealPanicIsContainedAndDrainCompletes(t *testing.T) {
	store := &contextShadowStore{
		fakeStore: newFakeStore(), sealPanic: true,
	}
	loop := newContextShadowLoop(
		t, store,
		func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
			return &llm.ChatResponse{Content: "unused"}, nil
		},
	)
	ctx := context.WithValue(t.Context(), chatMetaKey{}, chatMeta{
		traceID: "turn-panic", userID: 7,
		scope: agentcontext.Scope{
			TenantID: 1, UserID: 7, SessionID: 1,
		},
	})
	prepared := loop.prepareAgentContextShadow(
		ctx,
		llm.ChatRequest{
			Model: loop.model,
			Messages: []llm.ChatMessage{
				{Role: "system", Content: loop.sys},
				{Role: "user", Content: "question"},
			},
			MaxTokens: iptr(replyMaxTokens),
		},
		&toolRunState{activation: &activationState{}},
		1,
	)
	loop.sealPreparedAgentContextShadow(ctx, prepared)
	drainCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := loop.DrainSessionWrites(drainCtx); err != nil {
		t.Fatalf("panic escaped or blocked drain: %v", err)
	}
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
		EnableThinking:  true,
		ReasoningEffort: llm.ReasoningEffortHigh,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chat request changed by shadow:\ngot=%+v\nwant=%+v", got, want)
	}
	snapshots := drainContextShadows(t, loop, store)
	if len(snapshots) != 1 || snapshots[0].ContextStep != 1 {
		t.Fatalf("snapshots=%+v", snapshots)
	}
}

func TestContextShadowSlowStoreCannotDelayOrCancelLegacyCall(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	store := &contextShadowStore{
		fakeStore: newFakeStore(), sealStarted: started, sealRelease: release,
	}
	var request llm.ChatRequest
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, got llm.ChatRequest) (*llm.ChatResponse, error) {
			request = cloneChatRequest(got)
			return &llm.ChatResponse{Content: "legacy reply"}, nil
		},
	)
	parent, cancelParent := context.WithCancel(t.Context())
	type result struct {
		outcome Outcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outcome, err := loop.HandleMessage(parent, 7, "hello")
		done <- result{outcome: outcome, err: err}
	}()

	var got result
	select {
	case got = <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("slow shadow Store blocked legacy HandleMessage")
	}
	if got.err != nil || got.outcome.Reply != "legacy reply" {
		t.Fatalf("legacy outcome changed: outcome=%+v err=%v", got.outcome, got.err)
	}
	if len(request.Messages) != 2 ||
		!reflect.DeepEqual(
			request.Messages[1],
			llm.ChatMessage{Role: "user", Content: "hello"},
		) {
		t.Fatalf("legacy request changed: %+v", request)
	}

	var sealCtx context.Context
	select {
	case sealCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("post-chat context seal was not admitted")
	}
	cancelParent()
	select {
	case <-sealCtx.Done():
		t.Fatalf("caller cancellation reached shadow seal: %v", sealCtx.Err())
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if snapshots := drainContextShadows(t, loop, store); len(snapshots) != 1 {
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
	snapshots := drainContextShadows(t, loop, store)
	if len(snapshots) != 2 {
		t.Fatalf("snapshots=%+v", snapshots)
	}
	byStep := snapshotsByContextStep(t, snapshots)
	first, firstOK := byStep[1]
	tainted, taintedOK := byStep[2]
	if !firstOK || !taintedOK {
		t.Fatalf("context steps=%v, want 1 and 2", byStep)
	}
	if first.TurnID != tainted.TurnID {
		t.Fatalf("turn identity diverged: %q/%q", first.TurnID, tainted.TurnID)
	}
	if first.ToolsetDigest == tainted.ToolsetDigest {
		t.Fatal("two context steps did not capture the toolset transition")
	}
	if tainted.Replayable || tainted.UntrustedDigest == "" {
		t.Fatalf("tainted snapshot=%+v", tainted)
	}
	raw, err := json.Marshal(tainted)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), attack) {
		t.Fatal("tainted external raw reached shadow candidate")
	}
}

func TestContextShadowCompletionOrderDoesNotDefineContextIdentity(t *testing.T) {
	blockStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	store := &contextShadowStore{
		fakeStore: newFakeStore(), blockStep: 1,
		blockStarted: blockStarted, blockRelease: releaseFirst,
	}
	tool := &fakeTool{
		name: "external", result: "external", untrusted: true,
	}
	call := llm.ToolCall{
		ID: "call-1", Name: "external", Arguments: `{}`,
	}
	requests := 0
	loop := newContextShadowLoop(
		t, store,
		func(_ context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			requests++
			if requests == 1 {
				return &llm.ChatResponse{
					ToolCalls: []llm.ToolCall{call},
				}, nil
			}
			select {
			case <-blockStarted:
			case <-time.After(time.Second):
				t.Fatal("context step 1 did not enter the blocking Store")
			}
			return &llm.ChatResponse{Content: "safe summary"}, nil
		},
		tool,
	)
	if _, err := loop.HandleMessage(t.Context(), 7, "research"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(store.snapshots()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	completed := store.snapshots()
	if len(completed) != 1 || completed[0].ContextStep != 2 {
		t.Fatalf("completion order=%+v, want context step 2 first", completed)
	}
	close(releaseFirst)
	snapshots := drainContextShadows(t, loop, store)
	byStep := snapshotsByContextStep(t, snapshots)
	if len(byStep) != 2 || byStep[1].TurnID != byStep[2].TurnID ||
		byStep[1].ToolsetDigest == byStep[2].ToolsetDigest ||
		byStep[2].Replayable {
		t.Fatalf("out-of-order snapshot identities=%+v", byStep)
	}
}

func snapshotsByContextStep(
	t *testing.T,
	snapshots []agentcontext.CandidateSnapshot,
) map[int]agentcontext.CandidateSnapshot {
	t.Helper()
	byStep := make(map[int]agentcontext.CandidateSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if _, exists := byStep[snapshot.ContextStep]; exists {
			t.Fatalf("duplicate context step %d: %+v",
				snapshot.ContextStep, snapshots)
		}
		byStep[snapshot.ContextStep] = snapshot
	}
	return byStep
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
	snapshots := drainContextShadows(t, loop, store)
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
	if strings.Contains(
		snapshots[0].CandidateMessages[0].Content,
		"AI SaaS",
	) {
		t.Fatal("owner profile must be queried through query_my_intelligence, not injected")
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

func TestShadowMessageGroupsEmitContiguousExactOrdinalRanges(t *testing.T) {
	_, groups := shadowMessageGroups([]llm.ChatMessage{
		{Role: "system", Content: "system"},
		{Role: "user", Content: "first"},
		{Role: "assistant", Content: "answer"},
		{Role: "user", Content: "second"},
		{Role: "assistant", ToolCalls: []llm.ToolCall{{
			ID: "call-1", Name: "read", Arguments: `{}`,
		}}},
		{Role: "tool", Content: "result", ToolCallID: "call-1"},
	}, nil)
	if len(groups) != 2 {
		t.Fatalf("groups=%+v", groups)
	}
	var previousLast int64
	for i, group := range groups {
		if group.FirstMessageOrdinal != previousLast+1 ||
			group.LastMessageOrdinal-group.FirstMessageOrdinal+1 !=
				int64(len(group.Messages)) {
			t.Fatalf("group %d has fabricated/non-contiguous range: %+v", i, group)
		}
		previousLast = group.LastMessageOrdinal
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
	if snapshots := drainContextShadows(t, loop, store); len(snapshots) != 0 {
		t.Fatalf("RunOnce persisted owner snapshots: %+v", snapshots)
	}
}

func drainContextShadows(
	t *testing.T,
	loop *Loop,
	store *contextShadowStore,
) []agentcontext.CandidateSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := loop.DrainSessionWrites(ctx); err != nil {
		t.Fatal(err)
	}
	return store.snapshots()
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
