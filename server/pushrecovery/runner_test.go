package pushrecovery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/server/pusheffect"
)

type fakeDiscoveryStore struct {
	effects     []pusheffect.Effect
	cutoff      time.Time
	seenCutoffs []time.Time
	mu          sync.Mutex
}

func (s *fakeDiscoveryStore) ReadPushEffectRecoveryCutoff(
	_ context.Context,
) (time.Time, error) {
	if s.cutoff.IsZero() {
		s.cutoff = time.Date(2026, 7, 25, 20, 0, 0, 0, time.UTC)
	}
	return s.cutoff, nil
}

func (s *fakeDiscoveryStore) ListRecoveryTenantCatalogPage(
	_ context.Context,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if limit <= 0 {
		return nil, errors.New("invalid discovery scope")
	}
	if afterTenantID >= 1 || len(s.effects) == 0 {
		return nil, nil
	}
	return []int64{1}, nil
}

func (s *fakeDiscoveryStore) ListRecoverablePushEffects(
	_ context.Context,
	taskID string,
	tenantID int64,
	cutoff time.Time,
	afterEffectID string,
	limit int,
) ([]pusheffect.Effect, error) {
	if taskID != "task-one" || tenantID != 1 || limit <= 0 {
		return nil, errors.New("invalid effect discovery scope")
	}
	s.mu.Lock()
	s.seenCutoffs = append(s.seenCutoffs, cutoff)
	s.mu.Unlock()
	result := make([]pusheffect.Effect, 0, limit)
	for _, effect := range s.effects {
		if effect.ID <= afterEffectID {
			continue
		}
		result = append(result, effect)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

type fakeAttempter struct {
	fn         func(context.Context, pusheffect.Scope) (Outcome, error)
	current    atomic.Int64
	maxCurrent atomic.Int64
	calls      atomic.Int64
}

func (a *fakeAttempter) Attempt(
	ctx context.Context,
	scope pusheffect.Scope,
) (Outcome, error) {
	a.calls.Add(1)
	current := a.current.Add(1)
	defer a.current.Add(-1)
	for {
		max := a.maxCurrent.Load()
		if current <= max || a.maxCurrent.CompareAndSwap(max, current) {
			break
		}
	}
	return a.fn(ctx, scope)
}

func newRunnerTestEffects(n int) []pusheffect.Effect {
	effects := make([]pusheffect.Effect, n)
	for i := range effects {
		effects[i].ID = "effect-" + string(rune('a'+i))
		effects[i].TenantID = 1
		effects[i].UserID = int64(i + 1)
	}
	return effects
}

func newTestRunner(
	t *testing.T,
	effects []pusheffect.Effect,
	attempter *fakeAttempter,
	config RunnerConfig,
) *Runner {
	t.Helper()
	config.ExactTaskID = "task-one"
	runner, err := NewRunner(RunnerDeps{
		Store:       &fakeDiscoveryStore{effects: effects},
		Coordinator: attempter,
		Config:      config,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestRunnerRecordsOutcomeExactlyOnceWithAndWithoutError(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		runner := newTestRunner(t, newRunnerTestEffects(1), &fakeAttempter{
			fn: func(context.Context, pusheffect.Scope) (Outcome, error) {
				return OutcomeSent, nil
			},
		}, RunnerConfig{})
		if err := runner.RunStartup(t.Context()); err != nil {
			t.Fatal(err)
		}
		got := runner.Snapshot()
		if got.Attempts != 1 || got.Sent != 1 || got.AttemptErrors != 0 {
			t.Fatalf("snapshot=%+v", got)
		}
	})

	t.Run("meaningful outcome and error", func(t *testing.T) {
		runner := newTestRunner(t, newRunnerTestEffects(1), &fakeAttempter{
			fn: func(context.Context, pusheffect.Scope) (Outcome, error) {
				return OutcomeAmbiguous, errors.New("provider result unknown")
			},
		}, RunnerConfig{})
		if err := runner.RunStartup(t.Context()); err != nil {
			t.Fatalf("durably checkpointed outcome must converge pass: %v", err)
		}
		got := runner.Snapshot()
		if got.Attempts != 1 || got.Ambiguous != 1 ||
			got.AttemptErrors != 1 || got.PassErrors != 0 {
			t.Fatalf("snapshot=%+v", got)
		}
	})

	t.Run("empty successful outcome is rejected", func(t *testing.T) {
		runner := newTestRunner(t, newRunnerTestEffects(1), &fakeAttempter{
			fn: func(context.Context, pusheffect.Scope) (Outcome, error) {
				return "", nil
			},
		}, RunnerConfig{})
		if err := runner.RunStartup(t.Context()); err == nil {
			t.Fatal("empty outcome must fail the pass")
		}
		if got := runner.Snapshot(); got.AttemptErrors != 1 {
			t.Fatalf("snapshot=%+v", got)
		}
	})
}

func TestRunnerBoundsConcurrencyEffectsAndPassDuration(t *testing.T) {
	attempter := &fakeAttempter{
		fn: func(ctx context.Context, _ pusheffect.Scope) (Outcome, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	}
	runner := newTestRunner(t, newRunnerTestEffects(8), attempter,
		RunnerConfig{
			PageSize: 2, MaxConcurrent: 2, MaxEffects: 3,
			PassTimeout:    40 * time.Millisecond,
			AttemptTimeout: 100 * time.Millisecond,
		})
	start := time.Now()
	err := runner.RunStartup(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunStartup error=%v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded pass took %v (want pass+attempt+epsilon)", elapsed)
	}
	if max := attempter.maxCurrent.Load(); max > 2 {
		t.Fatalf("max concurrency=%d", max)
	}
	if calls := attempter.calls.Load(); calls != 2 {
		t.Fatalf("attempt calls=%d, want two admitted before pass deadline", calls)
	}
}

func TestRunnerSerializesOverlappingPasses(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	attempter := &fakeAttempter{
		fn: func(context.Context, pusheffect.Scope) (Outcome, error) {
			once.Do(func() { close(started) })
			<-release
			return OutcomeSent, nil
		},
	}
	runner := newTestRunner(t, newRunnerTestEffects(1), attempter,
		RunnerConfig{PassTimeout: time.Second})
	firstDone := make(chan error, 1)
	go func() { firstDone <- runner.RunStartup(t.Context()) }()
	<-started

	secondCtx, cancelSecond := context.WithTimeout(
		t.Context(), 30*time.Millisecond)
	defer cancelSecond()
	if err := runner.RunStartup(secondCtx); !errors.Is(
		err, context.DeadlineExceeded) {
		t.Fatalf("overlapping pass error=%v", err)
	}
	if calls := attempter.calls.Load(); calls != 1 {
		t.Fatalf("overlapping scan started attempts=%d", calls)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRotatesCursorAcrossLimitedPasses(t *testing.T) {
	var (
		mu  sync.Mutex
		ids []string
	)
	attempter := &fakeAttempter{
		fn: func(_ context.Context, scope pusheffect.Scope) (Outcome, error) {
			mu.Lock()
			ids = append(ids, scope.ID)
			mu.Unlock()
			return OutcomeIgnored, nil
		},
	}
	runner := newTestRunner(t, newRunnerTestEffects(5), attempter,
		RunnerConfig{PageSize: 2, MaxEffects: 2, MaxConcurrent: 1})
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), ids...)
	mu.Unlock()
	want := []string{
		"effect-a", "effect-b", "effect-c", "effect-d", "effect-e",
		"effect-a", "effect-b",
	}
	if len(got) != len(want) {
		t.Fatalf("attempted=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attempted=%v want=%v", got, want)
		}
	}
	if snapshot := runner.Snapshot(); snapshot.LimitedPasses != 3 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestRunnerUsesDatabaseCutoffAndPassDeadlineDoesNotCancelAdmittedAttempt(
	t *testing.T,
) {
	cutoff := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
	store := &fakeDiscoveryStore{
		effects: newRunnerTestEffects(1), cutoff: cutoff,
	}
	attempter := &fakeAttempter{
		fn: func(ctx context.Context, _ pusheffect.Scope) (Outcome, error) {
			time.Sleep(40 * time.Millisecond)
			if err := ctx.Err(); err != nil {
				return "", err
			}
			return OutcomeSent, nil
		},
	}
	runner, err := NewRunner(RunnerDeps{
		Store: store, Coordinator: attempter,
		Config: RunnerConfig{
			ExactTaskID:    "task-one",
			PassTimeout:    20 * time.Millisecond,
			AttemptTimeout: time.Second,
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = runner.RunStartup(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pass error=%v", err)
	}
	if snapshot := runner.Snapshot(); snapshot.Sent != 1 {
		t.Fatalf("admitted attempt was canceled by pass deadline: %+v", snapshot)
	}
	store.mu.Lock()
	seen := append([]time.Time(nil), store.seenCutoffs...)
	store.mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("no discovery cutoff observed")
	}
	for _, got := range seen {
		if !got.Equal(cutoff) {
			t.Fatalf("discovery cutoff=%v want=%v", got, cutoff)
		}
	}
}

func TestRunnerLogsOnlyLowCardinalityRecoverySignals(t *testing.T) {
	var output bytes.Buffer
	effects := newRunnerTestEffects(1)
	effects[0].ID = "effect-sensitive"
	runner, err := NewRunner(RunnerDeps{
		Store: &fakeDiscoveryStore{effects: effects},
		Coordinator: &fakeAttempter{
			fn: func(context.Context, pusheffect.Scope) (Outcome, error) {
				return OutcomeAmbiguous,
					errors.New("recipient-card-provider-uuid-sensitive")
			},
		},
		Config: RunnerConfig{ExactTaskID: "task-one"},
		Logger: slog.New(slog.NewJSONHandler(&output, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	logged := output.String()
	for _, forbidden := range []string{
		"effect-sensitive", "task-one",
		"recipient-card-provider-uuid-sensitive",
	} {
		if bytes.Contains([]byte(logged), []byte(forbidden)) {
			t.Fatalf("recovery log leaked %q: %s", forbidden, logged)
		}
	}
	for _, required := range []string{
		`"trigger":"startup"`, `"ambiguous_total":1`,
		`"error_code":`,
	} {
		if !bytes.Contains([]byte(logged), []byte(required)) {
			t.Fatalf("recovery log missing %q: %s", required, logged)
		}
	}
}
