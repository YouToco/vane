package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/types"
)

type cancelAwareChat struct {
	entered chan struct{}
	once    sync.Once
}

func (c *cancelAwareChat) RunOnce(ctx context.Context, _ auth.Principal, _ []llm.ChatMessage, _ string) (agent.Outcome, []llm.ChatMessage, error) {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	return agent.Outcome{}, nil, context.Cause(ctx)
}

type blockingFinalUpdateStorage struct {
	*fakeTaskStorage
	blockStatus string
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
}

type blockingGetStorage struct {
	*fakeTaskStorage
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once

	mu        sync.Mutex
	closed    bool
	postClose int
}

func (s *blockingGetStorage) GetA2ATask(ctx context.Context, id string) (*types.A2ATask, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	if s.closed {
		s.postClose++
	}
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.fakeTaskStorage.GetA2ATask(ctx, id)
}

func (s *blockingFinalUpdateStorage) UpdateA2ATask(ctx context.Context, id string, expectedVersion int64, status string, task json.RawMessage) error {
	if status == s.blockStatus {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.fakeTaskStorage.UpdateA2ATask(ctx, id, expectedVersion, status, task)
}

func TestRuntimeShutdownWaitsForDetachedTaskStoreCleanup(t *testing.T) {
	storage := &blockingFinalUpdateStorage{
		fakeTaskStorage: newFakeTaskStorage(),
		blockStatus:     string(protocol.TaskStateFailed),
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	chat := &cancelAwareChat{entered: make(chan struct{})}
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	runtime, err := Mount(mux, Deps{
		Storage:   storage,
		Content:   &fakeContent{},
		Chat:      chat,
		Principal: ownerResolver(&fakeOwner{userID: 7}),
		Token:     testToken,
		BaseURL:   srv.URL + "/a2a",
		Version:   "lifecycle-test",
	})
	if err != nil {
		t.Fatalf("Mount 失败: %v", err)
	}

	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "SendMessage",
		"params": map[string]any{
			"configuration": map[string]any{"returnImmediately": true},
			"message": map[string]any{
				"messageId": "lifecycle-message",
				"role":      "ROLE_USER",
				"parts": []map[string]any{{
					"text": `{"skill":"assistant.chat","text":"wait"}`,
				}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/a2a", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("SendMessage 失败: %v", err)
	}
	resp.Body.Close()

	select {
	case <-chat.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("detached assistant.chat 未开始")
	}

	shutdownCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(shutdownCtx) }()

	select {
	case <-storage.entered:
	case err := <-shutdownDone:
		t.Fatalf("Shutdown 在终态写开始前返回: %v", err)
	case <-time.After(time.Second):
		t.Fatal("取消执行后未尝试持久化 FAILED 终态")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("TaskStorage 终态写仍阻塞时 Shutdown 提前返回: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(storage.release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown 失败: %v", err)
	}

	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.rows) != 1 {
		t.Fatalf("持久任务数 = %d, 期望 1", len(storage.rows))
	}
	for _, row := range storage.rows {
		if row.Status != string(protocol.TaskStateFailed) {
			t.Fatalf("关停后任务状态 = %q, 期望 FAILED", row.Status)
		}
	}
}

func TestRuntimeTracksDuplicateDetachedCancel(t *testing.T) {
	storage := &blockingFinalUpdateStorage{
		fakeTaskStorage: newFakeTaskStorage(),
		blockStatus:     string(protocol.TaskStateCanceled),
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	task := newTask("cancel-duplicate", protocol.TaskStateWorking)
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.CreateA2ATask(t.Context(), &types.A2ATask{
		ID: string(task.ID), ContextID: task.ContextID,
		Status: string(task.Status.State), Task: payload,
	}); err != nil {
		t.Fatal(err)
	}

	runtime := newRuntime()
	executor := &lifecycleExecutor{inner: newExecutor(Deps{Storage: storage}), runtime: runtime}
	inner := a2asrv.NewHandler(executor, a2asrv.WithTaskStore(newTaskStore(storage)))
	handler := &lifecycleRequestHandler{RequestHandler: inner, runtime: runtime}

	type cancelResult struct {
		task *protocol.Task
		err  error
	}
	firstDone := make(chan cancelResult, 1)
	go func() {
		got, callErr := handler.CancelTask(t.Context(), &protocol.CancelTaskRequest{ID: task.ID})
		firstDone <- cancelResult{task: got, err: callErr}
	}()
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("first CancelTask did not reach terminal task-store write")
	}

	secondDone := make(chan cancelResult, 1)
	go func() {
		got, callErr := handler.CancelTask(t.Context(), &protocol.CancelTaskRequest{ID: task.ID})
		secondDone <- cancelResult{task: got, err: callErr}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		reservations := len(runtime.executions)
		runtime.mu.Unlock()
		if reservations == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("duplicate CancelTask reservation count = %d, want 2", reservations)
		}
		time.Sleep(time.Millisecond)
	}

	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned while duplicate cancel was still pending: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(storage.release)
	for i, done := range []<-chan cancelResult{firstDone, secondDone} {
		select {
		case got := <-done:
			if got.err != nil || got.task == nil || got.task.Status.State != protocol.TaskStateCanceled {
				t.Fatalf("cancel result %d = task=%+v err=%v", i, got.task, got.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("cancel result %d did not return", i)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("duplicate cancel left an undrained lifecycle token: %v", err)
	}
}

func TestRuntimeTracksCancelConcurrentWithActiveExecution(t *testing.T) {
	storage := newFakeTaskStorage()
	chat := &cancelAwareChat{entered: make(chan struct{})}
	runtime := newRuntime()
	executor := &lifecycleExecutor{inner: newExecutor(Deps{
		Storage: storage, Chat: chat, Principal: ownerResolver(&fakeOwner{userID: 7}),
	}), runtime: runtime}
	inner := a2asrv.NewHandler(executor, a2asrv.WithTaskStore(newTaskStore(storage)))
	handler := &lifecycleRequestHandler{RequestHandler: inner, runtime: runtime}

	result, err := handler.SendMessage(t.Context(), &protocol.SendMessageRequest{
		Config:  &protocol.SendMessageConfig{ReturnImmediately: true},
		Message: chatMsg("keep running"),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*protocol.Task)
	if !ok {
		t.Fatalf("SendMessage result = %T, want *Task", result)
	}
	select {
	case <-chat.entered:
	case <-time.After(time.Second):
		t.Fatal("active execution did not start")
	}
	canceled, err := handler.CancelTask(t.Context(), &protocol.CancelTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatalf("CancelTask active execution: %v", err)
	}
	if canceled.Status.State != protocol.TaskStateCanceled {
		t.Fatalf("CancelTask status = %s, want CANCELED", canceled.Status.State)
	}
	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("active cancel did not drain: %v", err)
	}
}

func TestRuntimeShutdownClosesAdmission(t *testing.T) {
	runtime := newRuntime()
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatalf("空 Runtime Shutdown 失败: %v", err)
	}
	if _, err := runtime.reserve(t.Context()); !errors.Is(err, errRuntimeShuttingDown) {
		t.Fatalf("关停后 reserve 错误 = %v, 期望 errRuntimeShuttingDown", err)
	}
}

func TestClientCancellationCannotDropPreExecutorReservation(t *testing.T) {
	storage := &blockingGetStorage{
		fakeTaskStorage: newFakeTaskStorage(),
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	task := newTask("continuation-before-executor", protocol.TaskStateInputRequired)
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.CreateA2ATask(t.Context(), &types.A2ATask{
		ID: string(task.ID), ContextID: task.ContextID,
		Status: string(task.Status.State), Task: payload,
	}); err != nil {
		t.Fatal(err)
	}

	runtime := newRuntime()
	executor := &lifecycleExecutor{inner: newExecutor(Deps{
		Storage: storage, Content: &fakeContent{},
	}), runtime: runtime}
	inner := a2asrv.NewHandler(executor, a2asrv.WithTaskStore(newTaskStore(storage)))
	handler := &lifecycleRequestHandler{RequestHandler: inner, runtime: runtime}
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	message := textMsg("continuation")
	message.TaskID = task.ID
	sendDone := make(chan error, 1)
	go func() {
		_, callErr := handler.SendMessage(requestCtx, &protocol.SendMessageRequest{Message: message})
		sendDone <- callErr
	}()
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("continuation TaskStorage.Get did not block before AgentExecutor")
	}
	cancelRequest()
	select {
	case err := <-sendDone:
		t.Fatalf("client cancellation escaped runtime ownership before executor start: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(t.Context(), time.Second)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Runtime.Shutdown returned while detached factory Get was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(storage.release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Runtime.Shutdown after factory release: %v", err)
	}
	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("SendMessage handler did not finish after runtime shutdown")
	}
	storage.mu.Lock()
	storage.closed = true
	postClose := storage.postClose
	storage.mu.Unlock()
	if postClose != 0 {
		t.Fatalf("TaskStorage calls after lifecycle drain = %d", postClose)
	}
}

func TestRuntimeShutdownDrainsPreExecutorFailureWithoutCleanup(t *testing.T) {
	storage := &blockingGetStorage{
		fakeTaskStorage: newFakeTaskStorage(),
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
		err:             errors.New("injected task-store read failure"),
	}
	task := newTask("factory-failure", protocol.TaskStateInputRequired)
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.CreateA2ATask(t.Context(), &types.A2ATask{
		ID: string(task.ID), ContextID: task.ContextID,
		Status: string(task.Status.State), Task: payload,
	}); err != nil {
		t.Fatal(err)
	}

	runtime := newRuntime()
	executor := &lifecycleExecutor{inner: newExecutor(Deps{Storage: storage}), runtime: runtime}
	inner := a2asrv.NewHandler(executor, a2asrv.WithTaskStore(newTaskStore(storage)))
	handler := &lifecycleRequestHandler{RequestHandler: inner, runtime: runtime}
	message := textMsg("continuation")
	message.TaskID = task.ID
	sendDone := make(chan error, 1)
	go func() {
		_, callErr := handler.SendMessage(t.Context(), &protocol.SendMessageRequest{Message: message})
		sendDone <- callErr
	}()
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("factory TaskStorage.Get did not block")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(t.Context(), time.Second)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before factory failure resolved: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(storage.release)
	select {
	case err := <-sendDone:
		if err == nil {
			t.Fatal("SendMessage unexpectedly succeeded after factory failure")
		}
	case <-time.After(time.Second):
		t.Fatal("SendMessage did not report factory failure")
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("factory failure left an undrained lifecycle token: %v", err)
	}
}
