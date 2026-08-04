package pushrecovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
)

type fakeAuthorityDiscoveryStore struct {
	fakeStore
	mu        sync.Mutex
	tasks     []string
	listCalls int
	listErr   error
	slowTask  string
	runOrder  []string
}

func (s *fakeAuthorityDiscoveryStore) ListEnabledResearchV3RecoveryTaskIDs(
	_ context.Context, after string, limit int,
) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	if s.listErr != nil {
		return nil, s.listErr
	}
	tasks := append([]string(nil), s.tasks...)
	sort.Strings(tasks)
	result := make([]string, 0, limit)
	for _, taskID := range tasks {
		if taskID > after {
			result = append(result, taskID)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func (*fakeAuthorityDiscoveryStore) ReadPushEffectRecoveryCutoff(context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (s *fakeAuthorityDiscoveryStore) ListRecoverablePushEffectTenantIDs(
	ctx context.Context, taskID string, _ time.Time, _ int64, _ int,
) ([]int64, error) {
	s.mu.Lock()
	s.runOrder = append(s.runOrder, taskID)
	slow := s.slowTask == taskID
	s.mu.Unlock()
	if slow {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}

func (*fakeAuthorityDiscoveryStore) ListRecoverablePushEffects(
	context.Context, string, int64, time.Time, string, int,
) ([]pusheffect.Effect, error) {
	return nil, nil
}

func newAuthorityRunnerForTest(
	t *testing.T, store *fakeAuthorityDiscoveryStore, exclude string,
) *AuthorityRunner {
	t.Helper()
	runner, err := NewAuthorityRunner(AuthorityRunnerDeps{
		Store: store, Sender: &fakeSender{}, HistoryResolver: &fakeHistoryResolver{},
		ExcludeTaskID: exclude,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		RunnerConfig:  RunnerConfig{PassTimeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestAuthorityRunnerDiscoversNewDurableAuthoritiesAndExcludesStaticCanary(t *testing.T) {
	store := &fakeAuthorityDiscoveryStore{tasks: []string{"task-b", "task-a"}}
	runner := newAuthorityRunnerForTest(t, store, "task-a")
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if runner.runners["task-a"] != nil || runner.runners["task-b"] == nil {
		t.Fatalf("initial exact runners=%v", runner.runners)
	}
	store.mu.Lock()
	store.tasks = append(store.tasks, "task-c")
	store.mu.Unlock()
	if err := runner.runPass(t.Context(), "test"); err != nil {
		t.Fatal(err)
	}
	if runner.runners["task-c"] == nil {
		t.Fatal("newly enabled database authority was not discovered")
	}
	store.mu.Lock()
	store.tasks = []string{"task-c"}
	store.mu.Unlock()
	if err := runner.runPass(t.Context(), "evict"); err != nil {
		t.Fatal(err)
	}
	if len(runner.runners) != 1 || runner.runners["task-c"] == nil ||
		runner.runners["task-b"] != nil {
		t.Fatalf("revoked authority runner cache was not pruned: %v", runner.runners)
	}
}

func TestAuthorityRunnerPaginationIsBounded(t *testing.T) {
	tasks := make([]string, maxAuthorityTasksPerPass+1)
	for index := range tasks {
		tasks[index] = fmt.Sprintf("task-%04d", index)
	}
	store := &fakeAuthorityDiscoveryStore{tasks: tasks}
	runner := newAuthorityRunnerForTest(t, store, "")
	if err := runner.RunStartup(t.Context()); err == nil {
		t.Fatal("unbounded authority discovery succeeded")
	}
	if store.listCalls > maxAuthorityTasksPerPass/defaultAuthorityTaskPageSize+1 {
		t.Fatalf("authority discovery calls=%d", store.listCalls)
	}
}

func TestAuthorityRunnerRotatesPastSlowTaskAndKeepsCompleteSnapshotOnDiscoveryError(t *testing.T) {
	store := &fakeAuthorityDiscoveryStore{
		tasks: []string{"task-a", "task-b"}, slowTask: "task-a",
	}
	runner, err := NewAuthorityRunner(AuthorityRunnerDeps{
		Store: store, Sender: &fakeSender{}, HistoryResolver: &fakeHistoryResolver{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		RunnerConfig: RunnerConfig{
			PassTimeout: 100 * time.Millisecond, AttemptTimeout: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.runPass(t.Context(), "slow"); err == nil {
		t.Fatal("slow authority pass unexpectedly succeeded")
	}
	store.mu.Lock()
	store.slowTask = ""
	store.runOrder = nil
	store.mu.Unlock()
	if err := runner.runPass(t.Context(), "rotated"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	order := append([]string(nil), store.runOrder...)
	store.listErr = errors.New("catalog unavailable")
	store.tasks = []string{"task-c"}
	store.mu.Unlock()
	if len(order) == 0 || order[0] != "task-b" {
		t.Fatalf("recovery did not rotate past slow task: %v", order)
	}
	if err := runner.runPass(t.Context(), "catalog-error"); err == nil {
		t.Fatal("authority catalog error was ignored")
	}
	if runner.runners["task-a"] == nil || runner.runners["task-b"] == nil ||
		runner.runners["task-c"] != nil {
		t.Fatalf("partial discovery replaced complete runner snapshot: %v", runner.runners)
	}
}

func TestNewAuthorityRunnerRejectsInvalidLifecycleBeforeDiscovery(t *testing.T) {
	_, err := NewAuthorityRunner(AuthorityRunnerDeps{
		Store: &fakeAuthorityDiscoveryStore{}, Sender: &fakeSender{},
		HistoryResolver: &fakeHistoryResolver{},
		RunnerConfig:    RunnerConfig{PassTimeout: maxPassTimeout + time.Second},
	})
	if err == nil {
		t.Fatal("invalid authority lifecycle succeeded with no discovered tasks")
	}
}
