package pushrecovery

import (
	"context"
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
}

func (s *fakeAuthorityDiscoveryStore) ListEnabledResearchV3RecoveryTaskIDs(
	_ context.Context, after string, limit int,
) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
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

func (*fakeAuthorityDiscoveryStore) ListRecoverablePushEffectTenantIDs(
	context.Context, string, time.Time, int64, int,
) ([]int64, error) {
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
