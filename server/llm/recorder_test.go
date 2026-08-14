package llm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

func TestNewRecorderNilStoreIsNoop(t *testing.T) {
	r := NewRecorder(nil)
	userID := int64(7)
	r.Record(t.Context(), &types.LLMCall{})
	if err := r.CheckQuota(t.Context(), &userID, 1); err != nil {
		t.Fatalf("nil store quota check = %v, want nil", err)
	}
	r.ReconcileQuota(t.Context(), &userID, 1, 1)
}

type blockingRecorderStore struct {
	insertSeen chan struct{}
	adjustSeen chan struct{}
	mu         sync.Mutex
	deadlines  []time.Time
}

func (s *blockingRecorderStore) rememberDeadline(ctx context.Context, seen chan struct{}) error {
	deadline, ok := ctx.Deadline()
	if ok {
		s.mu.Lock()
		s.deadlines = append(s.deadlines, deadline)
		s.mu.Unlock()
	}
	close(seen)
	<-ctx.Done()
	return ctx.Err()
}

func (s *blockingRecorderStore) InsertLLMCall(ctx context.Context, _ *types.LLMCall) (int64, error) {
	return 0, s.rememberDeadline(ctx, s.insertSeen)
}

func (*blockingRecorderStore) TryConsumeForUser(context.Context, int64, store.QuotaBucket, float64) error {
	return nil
}

func (s *blockingRecorderStore) AdjustForUser(ctx context.Context, _ int64, _ store.QuotaBucket, _ float64) error {
	return s.rememberDeadline(ctx, s.adjustSeen)
}

func TestFinishCallAccountingBoundsDetachedWrites(t *testing.T) {
	st := &blockingRecorderStore{insertSeen: make(chan struct{}), adjustSeen: make(chan struct{})}
	recorder := &Recorder{st: st}
	parent, cancelParent := context.WithCancel(t.Context())
	cancelParent()
	userID := int64(7)

	done := make(chan struct{})
	go func() {
		recorder.finishCallAccountingWithin(parent, &types.LLMCall{TraceID: "bounded-tail"}, &userID, 100, 80, 20*time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("detached accounting tail ignored its deadline")
	}
	select {
	case <-st.insertSeen:
	default:
		t.Fatal("Record was not attempted")
	}
	select {
	case <-st.adjustSeen:
	default:
		t.Fatal("ReconcileQuota was not attempted after Record timeout")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.deadlines) != 2 || !st.deadlines[0].Equal(st.deadlines[1]) {
		t.Fatalf("Record/Reconcile deadlines = %v, want one shared deadline", st.deadlines)
	}
}
