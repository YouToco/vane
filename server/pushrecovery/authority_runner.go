package pushrecovery

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const defaultAuthorityTaskPageSize = 100
const maxAuthorityTasksPerPass = 1000

type AuthorityDiscoveryStore interface {
	DiscoveryStore
	Store
	ListEnabledResearchV3RecoveryTaskIDs(
		context.Context, string, int,
	) ([]string, error)
}

type AuthorityRunnerDeps struct {
	Store           AuthorityDiscoveryStore
	Sender          Sender
	HistoryResolver HistoryResolver
	RunnerConfig    RunnerConfig
	Logger          *slog.Logger
	ExcludeTaskID   string
}

// AuthorityRunner discovers durable enabled V3 authorities and delegates each
// task to the existing exact-task coordinator. Discovery never grants send
// authority; the coordinator's claim transaction rechecks the same DB row.
type AuthorityRunner struct {
	store           AuthorityDiscoveryStore
	sender          Sender
	historyResolver HistoryResolver
	config          RunnerConfig
	logger          *slog.Logger
	excludeTaskID   string
	passMu          sync.Mutex
	mu              sync.Mutex
	runners         map[string]*Runner
	lastTaskID      string
}

func NewAuthorityRunner(deps AuthorityRunnerDeps) (*AuthorityRunner, error) {
	if deps.Store == nil || deps.Sender == nil || deps.HistoryResolver == nil ||
		(deps.ExcludeTaskID != "" && !validExactTaskID(deps.ExcludeTaskID)) {
		return nil, ErrDependencies
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.RunnerConfig.Interval <= 0 {
		deps.RunnerConfig.Interval = defaultRecoveryInterval
	} else if deps.RunnerConfig.Interval > maxRecoveryInterval {
		return nil, ErrDependencies
	}
	if deps.RunnerConfig.PageSize <= 0 {
		deps.RunnerConfig.PageSize = defaultRecoveryPageSize
	} else if deps.RunnerConfig.PageSize > maxRecoveryPageSize {
		return nil, ErrDependencies
	}
	if deps.RunnerConfig.MaxConcurrent <= 0 {
		deps.RunnerConfig.MaxConcurrent = defaultRecoveryConcurrency
	} else if deps.RunnerConfig.MaxConcurrent > maxRecoveryConcurrency {
		return nil, ErrDependencies
	}
	if deps.RunnerConfig.AttemptTimeout <= 0 {
		deps.RunnerConfig.AttemptTimeout = defaultAttemptTimeout
	} else if deps.RunnerConfig.AttemptTimeout > maxAttemptTimeout {
		return nil, ErrDependencies
	}
	if deps.RunnerConfig.PassTimeout <= 0 {
		deps.RunnerConfig.PassTimeout = defaultPassTimeout
	} else if deps.RunnerConfig.PassTimeout > maxPassTimeout {
		return nil, ErrDependencies
	}
	if deps.RunnerConfig.MaxEffects <= 0 {
		deps.RunnerConfig.MaxEffects = defaultMaxEffectsPerPass
	} else if deps.RunnerConfig.MaxEffects > maxEffectsPerPass {
		return nil, ErrDependencies
	}
	return &AuthorityRunner{
		store: deps.Store, sender: deps.Sender,
		historyResolver: deps.HistoryResolver, config: deps.RunnerConfig,
		logger: deps.Logger, excludeTaskID: deps.ExcludeTaskID,
		runners: make(map[string]*Runner),
	}, nil
}

func (r *AuthorityRunner) RunStartup(ctx context.Context) error {
	return r.runPass(ctx, "startup")
}

func (r *AuthorityRunner) Run(ctx context.Context) {
	if r == nil {
		return
	}
	timer := time.NewTimer(r.config.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := r.runPass(ctx, "periodic"); err != nil &&
				!errors.Is(err, context.Canceled) {
				r.logger.WarnContext(ctx,
					"V3 authority push recovery pass failed")
			}
			timer.Reset(r.config.Interval)
		}
	}
}

func (r *AuthorityRunner) runPass(ctx context.Context, trigger string) error {
	if r == nil || r.store == nil {
		return ErrDependencies
	}
	r.passMu.Lock()
	defer r.passMu.Unlock()
	passCtx, cancel := context.WithTimeout(ctx, r.config.PassTimeout)
	defer cancel()
	tasks, err := r.discoverAuthorityTasks(passCtx)
	if err != nil {
		// A partial catalog is not authority truth. Keep the previous complete
		// snapshot and every task Runner's recovery cursor for the next pass.
		return err
	}
	runnable := make([]string, 0, len(tasks))
	seen := make(map[string]struct{}, len(tasks))
	for _, taskID := range tasks {
		if taskID == r.excludeTaskID {
			continue
		}
		runnable = append(runnable, taskID)
		seen[taskID] = struct{}{}
	}
	r.retainRunners(seen)
	if len(runnable) == 0 {
		r.lastTaskID = ""
		return nil
	}
	start := 0
	for index, taskID := range runnable {
		if taskID > r.lastTaskID {
			start = index
			break
		}
		if index == len(runnable)-1 {
			start = 0
		}
	}
	var passErrors []error
	for offset := 0; offset < len(runnable); offset++ {
		if err := passCtx.Err(); err != nil {
			passErrors = append(passErrors, err)
			break
		}
		taskID := runnable[(start+offset)%len(runnable)]
		runner, err := r.runnerFor(taskID)
		if err != nil {
			passErrors = append(passErrors, err)
			r.lastTaskID = taskID
			continue
		}
		if err := runner.runPass(passCtx, "v3-authority-"+trigger); err != nil {
			passErrors = append(passErrors, err)
		}
		// Advance even on task-local timeout/failure: the next pass starts after
		// this task, so one slow authority cannot permanently starve its peers.
		r.lastTaskID = taskID
	}
	return errors.Join(passErrors...)
}

func (r *AuthorityRunner) discoverAuthorityTasks(ctx context.Context) ([]string, error) {
	after := ""
	tasksFound := make([]string, 0, defaultAuthorityTaskPageSize)
	for {
		tasks, err := r.store.ListEnabledResearchV3RecoveryTaskIDs(
			ctx, after, defaultAuthorityTaskPageSize)
		if err != nil {
			return nil, err
		}
		for _, taskID := range tasks {
			if taskID <= after {
				return nil, errors.New("V3 authority recovery cursor did not advance")
			}
			after = taskID
			tasksFound = append(tasksFound, taskID)
			if len(tasksFound) > maxAuthorityTasksPerPass {
				return nil, errors.New("V3 authority recovery task bound exceeded")
			}
		}
		if len(tasks) < defaultAuthorityTaskPageSize {
			break
		}
	}
	return tasksFound, nil
}

func (r *AuthorityRunner) retainRunners(seen map[string]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for taskID := range r.runners {
		if _, ok := seen[taskID]; !ok {
			delete(r.runners, taskID)
		}
	}
}

func (r *AuthorityRunner) runnerFor(taskID string) (*Runner, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if runner := r.runners[taskID]; runner != nil {
		return runner, nil
	}
	coordinator, err := New(Deps{
		Store: r.store, Sender: r.sender, HistoryResolver: r.historyResolver,
		Config: Config{ExactTaskID: taskID},
	})
	if err != nil {
		return nil, err
	}
	config := r.config
	config.ExactTaskID = taskID
	runner, err := NewRunner(RunnerDeps{
		Store: r.store, Coordinator: coordinator, Config: config, Logger: r.logger,
	})
	if err != nil {
		return nil, err
	}
	r.runners[taskID] = runner
	return runner, nil
}
