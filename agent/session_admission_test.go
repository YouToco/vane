package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/task"
)

func TestSharedSessionAdmissionSerializesOwnerAndWebForSameUser(t *testing.T) {
	store := newFakeStore()
	admission := NewSessionAdmissionCoordinator()
	ownerEntered := make(chan struct{})
	releaseOwner := make(chan struct{})
	webEntered := make(chan struct{})
	owner, web := newSharedAdmissionTestLoops(t, store, admission,
		func(ctx context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			close(ownerEntered)
			select {
			case <-releaseOwner:
				return &llm.ChatResponse{Content: "owner 完成。"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
			close(webEntered)
			return &llm.ChatResponse{Content: "请补充监控频率？"}, nil
		})

	type result struct {
		out Outcome
		err error
	}
	ownerDone := make(chan result, 1)
	go func() {
		out, err := owner.HandleMessage(t.Context(), 7, "先记录 owner turn")
		ownerDone <- result{out: out, err: err}
	}()
	waitClosed(t, ownerEntered, "owner turn did not enter model")

	webStarted := make(chan struct{})
	webDone := make(chan result, 1)
	go func() {
		close(webStarted)
		out, err := web.HandleTaskCreationMessage(
			t.Context(), 7, "9a4ca406-70d2-4af2-b3ef-acde86339067",
			"再记录 Web turn",
		)
		webDone <- result{out: out, err: err}
	}()
	waitClosed(t, webStarted, "web turn did not start")
	assertNotClosed(t, webEntered,
		"Web turn entered the model while owner held shared admission")
	if got := store.sessionCount(); got != 1 {
		t.Fatalf("active sessions while owner holds admission=%d, want 1", got)
	}

	close(releaseOwner)
	if got := waitResult(t, ownerDone, "owner turn"); got.err != nil || got.out.Reply != "owner 完成。" {
		t.Fatalf("owner outcome=%+v err=%v", got.out, got.err)
	}
	if got := waitResult(t, webDone, "web turn"); got.err != nil || got.out.Reply != "请补充监控频率？" {
		t.Fatalf("web outcome=%+v err=%v", got.out, got.err)
	}
	if got := store.sessionCount(); got != 1 {
		t.Fatalf("shared owner/Web turns created %d sessions, want 1", got)
	}
	messages := persistedMessages(t, store)
	if store.lastTurnCount != 2 || len(messages) != 4 ||
		messages[0].Content != "先记录 owner turn" ||
		messages[2].Content != "再记录 Web turn" {
		t.Fatalf("shared turns were lost or reordered: turn_count=%d messages=%+v",
			store.lastTurnCount, messages)
	}
}

func TestSharedSessionAdmissionDoesNotBlockDifferentUsers(t *testing.T) {
	store := newFakeStore()
	admission := NewSessionAdmissionCoordinator()
	ownerEntered := make(chan struct{})
	releaseOwner := make(chan struct{})
	owner, web := newSharedAdmissionTestLoops(t, store, admission,
		func(ctx context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			close(ownerEntered)
			select {
			case <-releaseOwner:
				return &llm.ChatResponse{Content: "owner 完成。"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
			return &llm.ChatResponse{Content: "请补充监控频率？"}, nil
		})

	ownerDone := make(chan error, 1)
	go func() {
		_, err := owner.HandleMessage(t.Context(), 7, "阻塞 user 7")
		ownerDone <- err
	}()
	waitClosed(t, ownerEntered, "owner turn did not enter model")

	webDone := make(chan error, 1)
	go func() {
		_, err := web.HandleTaskCreationMessage(
			t.Context(), 8, "58a783cb-4213-45ae-8421-cd1bf9dd4585",
			"user 8 的 Web turn",
		)
		webDone <- err
	}()
	select {
	case err := <-webDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("different user was blocked by shared admission")
	}
	close(releaseOwner)
	if err := waitError(t, ownerDone, "owner turn"); err != nil {
		t.Fatal(err)
	}
}

func TestSharedSessionAdmissionHonorsCanceledWaiter(t *testing.T) {
	store := newFakeStore()
	admission := NewSessionAdmissionCoordinator()
	ownerEntered := make(chan struct{})
	releaseOwner := make(chan struct{})
	webEntered := make(chan struct{})
	owner, web := newSharedAdmissionTestLoops(t, store, admission,
		func(ctx context.Context, _ llm.ChatRequest) (*llm.ChatResponse, error) {
			close(ownerEntered)
			select {
			case <-releaseOwner:
				return &llm.ChatResponse{Content: "owner 完成。"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
			close(webEntered)
			return &llm.ChatResponse{Content: "不应执行"}, nil
		})

	ownerDone := make(chan error, 1)
	go func() {
		_, err := owner.HandleMessage(t.Context(), 7, "阻塞 admission")
		ownerDone <- err
	}()
	waitClosed(t, ownerEntered, "owner turn did not enter model")
	baselineReads := store.getActiveCount()

	ctx, cancel := context.WithCancel(t.Context())
	webDone := make(chan error, 1)
	go func() {
		_, err := web.HandleTaskCreationMessage(
			ctx, 7, "0e45482a-e0f4-4115-be1e-a7ca2c687bec",
			"取消的 Web turn",
		)
		webDone <- err
	}()
	cancel()
	if err := waitError(t, webDone, "canceled Web turn"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error=%v, want context.Canceled", err)
	}
	assertNotClosed(t, webEntered, "canceled waiter entered the model")
	if got := store.getActiveCount(); got != baselineReads {
		t.Fatalf("canceled waiter reached session Store: reads=%d want=%d",
			got, baselineReads)
	}

	close(releaseOwner)
	if err := waitError(t, ownerDone, "owner turn"); err != nil {
		t.Fatal(err)
	}
}

func TestSharedSessionAdmissionKeepsDualLoopDrainIndependent(t *testing.T) {
	store := newFakeStore()
	if _, err := store.CreateAgentSession(t.Context(), 42); err != nil {
		t.Fatal(err)
	}
	admission := NewSessionAdmissionCoordinator()
	owner := New(Deps{Store: store, SessionAdmission: admission})
	web := New(Deps{Store: store, SessionAdmission: admission})
	lock := admission.lockForUser(42)
	if err := lock.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	owner.NotifyEvent(t.Context(), 42, "owner-side-write", "owner notice")
	web.NotifyEvent(t.Context(), 42, "web-side-write", "web notice")

	drainCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ownerDrained := make(chan error, 1)
	webDrained := make(chan error, 1)
	go func() { ownerDrained <- owner.DrainSessionWrites(drainCtx) }()
	go func() { webDrained <- web.DrainSessionWrites(drainCtx) }()
	assertNoErrorYet(t, ownerDrained, "owner drain ignored its accepted side-write")
	assertNoErrorYet(t, webDrained, "web drain ignored its accepted side-write")

	lock.Unlock()
	if err := waitError(t, ownerDrained, "owner drain"); err != nil {
		t.Fatal(err)
	}
	if err := waitError(t, webDrained, "web drain"); err != nil {
		t.Fatal(err)
	}
	if got := store.appendCount(); got != 2 {
		t.Fatalf("dual Loop drain lost side-writes: got=%d want=2", got)
	}
	if err := owner.DrainSessionWrites(t.Context()); err != nil {
		t.Fatalf("repeated owner drain: %v", err)
	}
	if err := web.DrainSessionWrites(t.Context()); err != nil {
		t.Fatalf("repeated web drain: %v", err)
	}
}

func TestRunOnceRemainsSessionlessOutsideAdmission(t *testing.T) {
	admission := NewSessionAdmissionCoordinator()
	loop := New(Deps{SessionAdmission: admission})
	loop.chatFn = func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error) {
		return &llm.ChatResponse{Content: "A2A sessionless reply"}, nil
	}
	lock := admission.lockForUser(7)
	if err := lock.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()

	done := make(chan error, 1)
	go func() {
		out, _, err := loop.RunOnce(t.Context(), 7, nil, "A2A request")
		if err == nil && out.Reply != "A2A sessionless reply" {
			err = errors.New("unexpected A2A reply")
		}
		done <- err
	}()
	if err := waitError(t, done, "sessionless RunOnce"); err != nil {
		t.Fatal(err)
	}
}

func newSharedAdmissionTestLoops(
	t *testing.T,
	store *fakeStore,
	admission *SessionAdmissionCoordinator,
	ownerChat func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error),
	webChat func(context.Context, llm.ChatRequest) (*llm.ChatResponse, error),
) (*Loop, *Loop) {
	t.Helper()
	ownerTools := []ToolSpec{
		newToolSpec(&fakeTool{name: "query_my_intelligence"}, withToolSurface(
			ownerPolicy(Effects(EffectInternalRead), BudgetNone),
			ExposureAlways, knownToolIntents, ResultTrustLocal, false)),
		newToolSpec(&fakeTool{name: "manage_tasks", mutating: true}, withToolSurface(
			ownerPolicy(Effects(EffectStateWrite, EffectDirectOwnerWrite), BudgetNone),
			ExposureAlways, knownToolIntents, ResultTrustLocal, true)),
		newToolSpec(&fakeTool{name: "update_profile", mutating: true}, withToolSurface(
			ownerPolicy(Effects(EffectStateWrite, EffectDirectOwnerWrite), BudgetNone),
			ExposureAlways, knownToolIntents, ResultTrustLocal, true)),
	}
	owner := New(Deps{
		Store: store, Profiles: store, Tools: ownerTools,
		Model: "test", MaxTurns: 2, SessionTTL: 30 * time.Minute,
		OwnerAgent: true, Evidence: &fakeAgentEvidenceWriter{},
		SessionAdmission: admission,
	})
	web := New(Deps{
		Store: store, Profiles: store,
		Tools:        testToolSpecs(&fakeTool{name: "create_schedule", mutating: true}),
		TaskCreation: &fakeCreationController{executeErr: task.ErrCreationOperationNotFound},
		Model:        "test", MaxTurns: 2, SessionTTL: 30 * time.Minute,
		SessionAdmission: admission,
	})
	owner.chatFn = ownerChat
	web.chatFn = webChat
	return owner, web
}

func waitClosed(t *testing.T, ch <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(failure)
	}
}

func assertNotClosed(t *testing.T, ch <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(failure)
	case <-time.After(75 * time.Millisecond):
	}
}

func assertNoErrorYet(t *testing.T, ch <-chan error, failure string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s: %v", failure, err)
	case <-time.After(75 * time.Millisecond):
	}
}

func waitError(t *testing.T, ch <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

func waitResult[T any](t *testing.T, ch <-chan T, name string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		var zero T
		return zero
	}
}
