package agentcontinuation

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/store"
)

type fakeActionDispatchStore struct {
	mu sync.Mutex

	actions        []store.AgentActionContinuation
	projectStarted chan struct{}
	projectRelease chan struct{}
	projectErr     error
	acquireNil     bool
	projectCalls   int
	releaseCalls   int
	releaseCtxErr  error
	releaseHasTTL  bool
	retryAfter     time.Duration
	owners         []string
	tenantLimit    int
	actionLimit    int
	active         int
	maxActive      int
}

func (f *fakeActionDispatchStore) ListDueAgentActionContinuationTenantIDs(
	context.Context, time.Time, int64, int,
) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tenantLimit = actionTenantPageSize
	if len(f.actions) == 0 {
		return nil, nil
	}
	return []int64{f.actions[0].TenantID}, nil
}

func (f *fakeActionDispatchStore) ListDueAgentActionContinuations(
	_ context.Context,
	_ int64,
	_ time.Time,
	limit int,
) ([]store.AgentActionContinuation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actionLimit = limit
	return append([]store.AgentActionContinuation(nil), f.actions...), nil
}

func (f *fakeActionDispatchStore) AcquireAgentActionContinuation(
	_ context.Context,
	actionID string,
	tenantID, userID int64,
	owner string,
	leaseDuration time.Duration,
) (*store.AgentActionContinuation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.owners = append(f.owners, owner)
	if f.acquireNil {
		return nil, nil
	}
	for i := range f.actions {
		if f.actions[i].ActionID != actionID {
			continue
		}
		acquired := f.actions[i]
		acquired.TenantID = tenantID
		acquired.UserID = userID
		acquired.LeaseOwner = &owner
		acquired.LeaseFence = 1
		acquired.AttemptCount = 1
		expires := time.Now().Add(leaseDuration)
		acquired.LeaseExpiresAt = &expires
		return &acquired, nil
	}
	return nil, store.ErrAgentActionTerminal
}

func (f *fakeActionDispatchStore) ProjectAgentActionContinuation(
	ctx context.Context,
	_ store.AgentActionContinuationLease,
) error {
	f.mu.Lock()
	f.projectCalls++
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	started := f.projectStarted
	release := f.projectRelease
	err := f.projectErr
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	} else if err != nil {
		<-ctx.Done()
	}
	f.mu.Lock()
	f.active--
	f.mu.Unlock()
	return err
}

func (f *fakeActionDispatchStore) ReleaseAgentActionContinuation(
	ctx context.Context,
	_ store.AgentActionContinuationLease,
	retryAfter time.Duration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls++
	f.releaseCtxErr = ctx.Err()
	_, f.releaseHasTTL = ctx.Deadline()
	f.retryAfter = retryAfter
	return nil
}

func fakeAction(id string) store.AgentActionContinuation {
	return store.AgentActionContinuation{
		ActionID: id, TenantID: 1, UserID: 2,
		SessionID: 3, SourceID: 4,
		Status: store.AgentActionStatusConfirmed,
	}
}

func TestActionDispatcherStartsImmediatelyAndWaitProvesDrain(
	t *testing.T,
) {
	st := &fakeActionDispatchStore{
		actions:        []store.AgentActionContinuation{fakeAction("a1")},
		projectStarted: make(chan struct{}, 1),
		projectRelease: make(chan struct{}),
	}
	dispatcher, err := NewActionDispatcher(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.projectStarted:
	case <-time.After(time.Second):
		t.Fatal("startup action scan did not begin")
	}
	dispatcher.Stop()
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelWait()
	if err := dispatcher.Wait(waitCtx); !errors.Is(
		err, context.DeadlineExceeded,
	) {
		t.Fatalf("Wait before drain=%v, want deadline", err)
	}
	close(st.projectRelease)
	waitCtx, cancelWait = context.WithTimeout(t.Context(), time.Second)
	defer cancelWait()
	if err := dispatcher.Wait(waitCtx); err != nil {
		t.Fatalf("Wait after drain: %v", err)
	}
}

func TestActionDispatcherReleasesCancelledTransientFailureDetached(
	t *testing.T,
) {
	st := &fakeActionDispatchStore{
		actions:        []store.AgentActionContinuation{fakeAction("a1")},
		projectStarted: make(chan struct{}, 1),
		projectErr:     errors.New("transient database failure"),
	}
	dispatcher, err := NewActionDispatcher(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.projectStarted:
	case <-time.After(time.Second):
		t.Fatal("project did not start")
	}
	dispatcher.Stop()
	waitCtx, cancelWait := context.WithTimeout(t.Context(), time.Second)
	defer cancelWait()
	if err := dispatcher.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.releaseCalls != 1 {
		t.Fatalf("release calls=%d want=1", st.releaseCalls)
	}
	if st.releaseCtxErr != nil || !st.releaseHasTTL {
		t.Fatalf(
			"release context err/deadline=%v/%t want nil/true",
			st.releaseCtxErr, st.releaseHasTTL)
	}
	if st.retryAfter != 5*time.Second {
		t.Fatalf("retry=%v want=5s", st.retryAfter)
	}
}

func TestActionDispatcherBoundsConcurrencyAndUsesStableOwner(
	t *testing.T,
) {
	actions := make([]store.AgentActionContinuation, 8)
	for i := range actions {
		actions[i] = fakeAction(string(rune('a' + i)))
	}
	st := &fakeActionDispatchStore{
		actions:        actions,
		projectStarted: make(chan struct{}, len(actions)),
		projectRelease: make(chan struct{}),
	}
	dispatcher, err := NewActionDispatcher(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- dispatcher.DispatchOnce(t.Context()) }()
	for range actionConcurrency {
		select {
		case <-st.projectStarted:
		case <-time.After(time.Second):
			t.Fatal("bounded workers did not start")
		}
	}
	st.mu.Lock()
	if st.maxActive != actionConcurrency {
		t.Fatalf(
			"max active=%d want=%d", st.maxActive, actionConcurrency)
	}
	st.mu.Unlock()
	close(st.projectRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.tenantLimit != actionTenantPageSize ||
		st.actionLimit != actionPageSize {
		t.Fatalf(
			"page limits tenant/action=%d/%d want=%d/%d",
			st.tenantLimit, st.actionLimit,
			actionTenantPageSize, actionPageSize)
	}
	if len(st.owners) != len(actions) {
		t.Fatalf("owners=%d want=%d", len(st.owners), len(actions))
	}
	wantOwner := st.owners[0]
	for _, owner := range st.owners {
		if owner != wantOwner {
			t.Fatalf("owner changed within dispatcher: %q != %q",
				owner, wantOwner)
		}
	}
	const prefix = "agent-action-dispatcher-"
	if !strings.HasPrefix(wantOwner, prefix) {
		t.Fatalf("owner=%q missing stable prefix", wantOwner)
	}
	if _, err := uuid.Parse(strings.TrimPrefix(wantOwner, prefix)); err != nil {
		t.Fatalf("owner=%q does not contain UUID: %v", wantOwner, err)
	}
}

func TestActionDispatcherRejectsNilSuccessfulAcquisition(t *testing.T) {
	st := &fakeActionDispatchStore{
		actions:    []store.AgentActionContinuation{fakeAction("a1")},
		acquireNil: true,
	}
	dispatcher, err := NewActionDispatcher(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = dispatcher.DispatchOnce(t.Context())
	if err == nil || !strings.Contains(
		err.Error(), "action acquisition returned no action",
	) {
		t.Fatalf("DispatchOnce error=%v, want nil acquisition rejection", err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.projectCalls != 0 || st.releaseCalls != 0 {
		t.Fatalf(
			"nil acquisition project/release=%d/%d want=0/0",
			st.projectCalls, st.releaseCalls)
	}
}

func TestActionRetryBackoffCapsAtFifteenMinutes(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt int64
		want    time.Duration
	}{
		{name: "floor", attempt: 0, want: 5 * time.Second},
		{name: "growth", attempt: 4, want: 40 * time.Second},
		{name: "cap", attempt: 100, want: 15 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := actionRetryBackoff(test.attempt); got != test.want {
				t.Fatalf("backoff=%v want=%v", got, test.want)
			}
		})
	}
}

func TestActionDispatcherFutureBoundaryIsClampedByStoreDatabaseClock(
	t *testing.T,
) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate action dispatcher test")
	}
	dispatcherPath := filepath.Join(
		filepath.Dir(testFile), "action_dispatcher.go")
	dispatcherRaw, err := os.ReadFile(dispatcherPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(
		string(dispatcherRaw),
		"boundary := time.Now().Add(24 * time.Hour)",
	); got != 1 {
		t.Fatalf("future due boundary count=%d want=1", got)
	}

	storePath := filepath.Clean(filepath.Join(
		filepath.Dir(testFile), "..", "store", "agent_action_projection.go"))
	storeRaw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatal(err)
	}
	storeSource := string(storeRaw)
	for queryClamp, want := range map[string]int{
		"next_attempt_at<=LEAST($1,clock_timestamp())": 1,
		"next_attempt_at<=LEAST($2,clock_timestamp())": 1,
	} {
		if got := strings.Count(storeSource, queryClamp); got != want {
			t.Fatalf(
				"database-clock due clamp %q count=%d want=%d",
				queryClamp, got, want)
		}
	}
}
