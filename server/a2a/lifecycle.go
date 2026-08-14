package a2a

import (
	"context"
	"errors"
	"iter"
	"sync"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

var errRuntimeShuttingDown = errors.New("a2a runtime is shutting down")

type lifecycleTokenKey struct{}

type lifecycleToken uint64

type lifecycleExecution struct {
	started         bool
	cleaned         bool
	handlerDone     bool
	requestCancel   context.CancelCauseFunc
	executionCancel context.CancelCauseFunc
}

// Runtime owns every detached execution created by the A2A SDK. The SDK uses
// context.WithoutCancel for ReturnImmediately requests, so net/http shutdown
// alone cannot prove those goroutines have stopped touching TaskStorage.
//
// A reservation is installed before the SDK can detach a goroutine. Shutdown
// closes admission, cancels executions that have started, and waits until the
// SDK cleaner has finished all event processing and task-store writes. Setup
// that has not reached AgentExecutor is deliberately not canceled: the SDK
// detached that work from the request context and does not invoke Cleanup when
// its factory fails, so the handler result is the only completion signal.
type Runtime struct {
	mu         sync.Mutex
	accepting  bool
	next       lifecycleToken
	executions map[lifecycleToken]*lifecycleExecution
	changed    chan struct{}
}

func newRuntime() *Runtime {
	return &Runtime{
		accepting:  true,
		executions: make(map[lifecycleToken]*lifecycleExecution),
		changed:    make(chan struct{}),
	}
}

func (r *Runtime) signalLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *Runtime) reserve(ctx context.Context) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return nil, errRuntimeShuttingDown
	}
	r.next++
	token := r.next
	// Ignore client disconnect after admission: the SDK explicitly promises
	// detached task execution. Runtime owns the replacement cancellation signal.
	requestCtx, requestCancel := context.WithCancelCause(context.WithoutCancel(ctx))
	r.executions[token] = &lifecycleExecution{requestCancel: requestCancel}
	r.signalLocked()
	return context.WithValue(requestCtx, lifecycleTokenKey{}, token), nil
}

func lifecycleTokenFrom(ctx context.Context) (lifecycleToken, bool) {
	token, ok := ctx.Value(lifecycleTokenKey{}).(lifecycleToken)
	return token, ok
}

func (r *Runtime) begin(ctx context.Context) (context.Context, error) {
	token, ok := lifecycleTokenFrom(ctx)
	if !ok {
		return nil, errors.New("a2a execution has no lifecycle reservation")
	}

	r.mu.Lock()
	state, ok := r.executions[token]
	if !ok || state.started {
		r.mu.Unlock()
		return nil, errors.New("a2a execution lifecycle reservation is invalid")
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	state.started = true
	state.executionCancel = cancel
	closing := !r.accepting
	r.signalLocked()
	r.mu.Unlock()

	if closing {
		cancel(errRuntimeShuttingDown)
	}
	return runCtx, nil
}

// handlerReturned completes the transport side of a reservation. A handler
// cannot return successfully from an execution before AgentExecutor begins;
// an unstarted return therefore proves either factory failure or a duplicate
// cancel waiter, neither of which will receive an SDK Cleanup callback.
func (r *Runtime) handlerReturned(ctx context.Context) {
	token, ok := lifecycleTokenFrom(ctx)
	if !ok {
		return
	}
	r.mu.Lock()
	state, ok := r.executions[token]
	if !ok {
		r.mu.Unlock()
		return
	}
	state.handlerDone = true
	remove := state.cleaned || !state.started
	requestCancel := state.requestCancel
	if remove {
		delete(r.executions, token)
		r.signalLocked()
	}
	r.mu.Unlock()
	if remove && requestCancel != nil {
		requestCancel(nil)
	}
}

func (r *Runtime) finish(ctx context.Context) {
	token, ok := lifecycleTokenFrom(ctx)
	if !ok {
		return
	}
	r.mu.Lock()
	state, ok := r.executions[token]
	var executionCancel, requestCancel context.CancelCauseFunc
	var handlerDone bool
	if ok {
		state.cleaned = true
		handlerDone = state.handlerDone
		executionCancel = state.executionCancel
		requestCancel = state.requestCancel
		if handlerDone {
			delete(r.executions, token)
			r.signalLocked()
		}
	}
	r.mu.Unlock()
	if executionCancel != nil {
		executionCancel(nil)
	}
	if handlerDone && requestCancel != nil {
		requestCancel(nil)
	}
}

// Shutdown rejects new executions, cancels admitted detached work, and waits
// for the SDK cleaner. A timeout is a hard safety failure: callers must keep
// TaskStorage and other execution dependencies open rather than close them
// underneath a still-running goroutine.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.accepting = false
	cancels := make([]context.CancelCauseFunc, 0, len(r.executions))
	for _, state := range r.executions {
		// The SDK uses context.WithoutCancel before factory setup. Canceling the
		// request side cannot stop that detached work and can make its handler
		// return before setup failure is observable, leaking the reservation.
		// Once begin has run, executionCancel is the runtime-owned stop signal.
		if state.executionCancel != nil {
			cancels = append(cancels, state.executionCancel)
		}
	}
	r.signalLocked()
	r.mu.Unlock()

	for _, cancel := range cancels {
		cancel(errRuntimeShuttingDown)
	}

	for {
		r.mu.Lock()
		if len(r.executions) == 0 {
			r.mu.Unlock()
			return nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmtLifecycleShutdownError(ctx.Err())
		}
	}
}

func fmtLifecycleShutdownError(err error) error {
	return errors.Join(errors.New("a2a detached executions did not drain"), err)
}

type lifecycleRequestHandler struct {
	a2asrv.RequestHandler
	runtime *Runtime
}

func (h *lifecycleRequestHandler) SendMessage(ctx context.Context, req *protocol.SendMessageRequest) (protocol.SendMessageResult, error) {
	trackedCtx, err := h.runtime.reserve(ctx)
	if err != nil {
		return nil, protocol.NewError(protocol.ErrInternalError, "server is shutting down")
	}
	result, err := h.RequestHandler.SendMessage(trackedCtx, req)
	h.runtime.handlerReturned(trackedCtx)
	return result, err
}

func (h *lifecycleRequestHandler) CancelTask(ctx context.Context, req *protocol.CancelTaskRequest) (*protocol.Task, error) {
	trackedCtx, err := h.runtime.reserve(ctx)
	if err != nil {
		return nil, protocol.NewError(protocol.ErrInternalError, "server is shutting down")
	}
	result, err := h.RequestHandler.CancelTask(trackedCtx, req)
	h.runtime.handlerReturned(trackedCtx)
	return result, err
}

func (h *lifecycleRequestHandler) SendStreamingMessage(ctx context.Context, req *protocol.SendMessageRequest) iter.Seq2[protocol.Event, error] {
	// Vane advertises Streaming=false and the capability guard rejects this
	// before task execution, so no detached lifecycle is created here.
	return h.RequestHandler.SendStreamingMessage(ctx, req)
}

type lifecycleExecutor struct {
	inner   a2asrv.AgentExecutor
	runtime *Runtime
}

func (e *lifecycleExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[protocol.Event, error] {
	runCtx, err := e.runtime.begin(ctx)
	if err != nil {
		return func(yield func(protocol.Event, error) bool) {
			yield(nil, err)
		}
	}
	return e.inner.Execute(runCtx, execCtx)
}

func (e *lifecycleExecutor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[protocol.Event, error] {
	runCtx, err := e.runtime.begin(ctx)
	if err != nil {
		return func(yield func(protocol.Event, error) bool) {
			yield(nil, err)
		}
	}
	return e.inner.Cancel(runCtx, execCtx)
}

func (e *lifecycleExecutor) Cleanup(ctx context.Context, execCtx *a2asrv.ExecutorContext, result protocol.SendMessageResult, err error) {
	if cleaner, ok := e.inner.(a2asrv.AgentExecutionCleaner); ok {
		cleaner.Cleanup(ctx, execCtx, result, err)
	}
	e.runtime.finish(ctx)
}

var _ a2asrv.RequestHandler = (*lifecycleRequestHandler)(nil)
var _ a2asrv.AgentExecutor = (*lifecycleExecutor)(nil)
var _ a2asrv.AgentExecutionCleaner = (*lifecycleExecutor)(nil)
