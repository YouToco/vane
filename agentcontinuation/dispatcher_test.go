package agentcontinuation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/store"
)

type fakeStore struct {
	mu sync.Mutex

	fact           store.AgentSessionFact
	projectStarted chan struct{}
	projectRelease chan struct{}
	projectErr     error
	projectCalls   int
	releaseCalls   int
}

func (f *fakeStore) ListDueAgentSessionFactTenantIDs(
	context.Context, time.Time, int64, int,
) ([]int64, error) {
	return []int64{f.fact.TenantID}, nil
}

func (f *fakeStore) ListDueAgentSessionFacts(
	context.Context, int64, time.Time, int,
) ([]store.AgentSessionFact, error) {
	return []store.AgentSessionFact{f.fact}, nil
}

func (f *fakeStore) AcquireAgentSessionFact(
	_ context.Context,
	params store.AcquireAgentSessionFactParams,
) (*store.AgentSessionFact, error) {
	acquired := f.fact
	acquired.LeaseOwner = &params.LeaseOwner
	acquired.LeaseFence = 1
	acquired.AttemptCount = 1
	expires := time.Now().Add(params.LeaseDuration)
	acquired.LeaseExpiresAt = &expires
	return &acquired, nil
}

func (f *fakeStore) ProjectAgentSessionFact(
	context.Context,
	store.AgentSessionFactLease,
) error {
	f.mu.Lock()
	f.projectCalls++
	started := f.projectStarted
	release := f.projectRelease
	err := f.projectErr
	f.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if release != nil {
		<-release
	}
	return err
}

func (f *fakeStore) ReleaseAgentSessionFact(
	context.Context,
	store.AgentSessionFactLease,
	time.Duration,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls++
	return nil
}

func validFakeStore() *fakeStore {
	sessionID := int64(3)
	digest := "dfed88ef6b7d5c9eab5531c81a6084583f8d7f2782c6e50c92dc7a694f998c6c"
	return &fakeStore{fact: store.AgentSessionFact{
		ID: 1, TenantID: 1, UserID: 2, FactID: 4,
		SourceIdentity:  "feedback-click:4",
		SessionID:       &sessionID,
		SessionMessages: []byte(`[{"role":"user","content":"fact"}]`),
		PayloadDigest:   &digest,
		Status:          store.AgentSessionFactStatusPending,
		NextAttemptAt:   time.Now(),
	}}
}

func TestRunStartsImmediatelyAndDrainsAdmittedProjection(
	t *testing.T,
) {
	st := validFakeStore()
	st.projectStarted = make(chan struct{})
	st.projectRelease = make(chan struct{})
	dispatcher, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		dispatcher.Run(ctx)
		close(done)
	}()
	select {
	case <-st.projectStarted:
	case <-time.After(time.Second):
		t.Fatal("startup scan did not begin")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned before admitted projection drained")
	case <-time.After(50 * time.Millisecond):
	}
	close(st.projectRelease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not drain after projection completed")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.projectCalls != 1 {
		t.Fatalf("project calls=%d want=1", st.projectCalls)
	}
}

func TestDispatchOnceReleasesTransientFailure(t *testing.T) {
	st := validFakeStore()
	st.projectErr = errors.New("transient database failure")
	dispatcher, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.DispatchOnce(t.Context()); err == nil {
		t.Fatal("transient projection failure must be reported")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.projectCalls != 1 || st.releaseCalls != 1 {
		t.Fatalf("project=%d release=%d want=1/1",
			st.projectCalls, st.releaseCalls)
	}
}
