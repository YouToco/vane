package periodicbrief

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	CoordinatorInterval = 30 * time.Second
	CoordinatorTimeout  = 30 * time.Second
	CoordinatorPageSize = 100
	CoordinatorWorkers  = 4
)

type CoordinatorStore interface {
	ListPeriodicReportTaskIDsV1(
		context.Context, string, int,
	) ([]string, error)
	EvaluatePeriodicReportCadenceV1(
		context.Context, string, time.Time,
	) error
	GetPeriodicReportScheduleV1(
		context.Context, string,
	) (store.PeriodicReportScheduleV1, error)
	PreparePeriodicBriefIntentV1(
		context.Context, int64, int64, string,
		store.BriefReportCadenceV1, time.Time, time.Time,
	) (store.PeriodicBriefIntentV1, error)
	BindPeriodicBriefIntentRunV1(
		context.Context, int64, int64, int64, string,
	) error
}

type Coordinator struct {
	store       CoordinatorStore
	temporal    client.Client
	taskQueue   string
	enabled     bool
	allowAll    bool
	exactTaskID string
	logger      *slog.Logger
}

func NewCoordinator(
	st CoordinatorStore,
	temporalClient client.Client,
	taskQueue string,
	enabled bool,
	exactTaskID string,
	allowAll bool,
	logger *slog.Logger,
) (*Coordinator, error) {
	if st == nil || temporalClient == nil || taskQueue == "" {
		return nil, errors.New("periodic Brief coordinator dependencies are incomplete")
	}
	if enabled && (exactTaskID == "") == !allowAll {
		return nil, errors.New("periodic Brief rollout scope is invalid")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		store: st, temporal: temporalClient, taskQueue: taskQueue,
		enabled: enabled, exactTaskID: exactTaskID,
		allowAll: allowAll, logger: logger,
	}, nil
}

func (c *Coordinator) RunStartup(ctx context.Context) error {
	if !c.enabled {
		return nil
	}
	passCtx, cancel := context.WithTimeout(ctx, CoordinatorTimeout)
	defer cancel()
	return c.runOnce(passCtx)
}

func (c *Coordinator) Run(ctx context.Context) {
	if !c.enabled {
		return
	}
	ticker := time.NewTicker(CoordinatorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			passCtx, cancel := context.WithTimeout(ctx, CoordinatorTimeout)
			err := c.runOnce(passCtx)
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				c.logger.WarnContext(ctx,
					"periodic Brief coordinator pass failed",
					"error_code", types.CodeOf(err))
			}
		}
	}
}

func (c *Coordinator) runOnce(ctx context.Context) error {
	if !c.allowAll {
		return c.runTask(ctx, c.exactTaskID)
	}
	var (
		after string
		errs  []error
	)
	for {
		taskIDs, err := c.store.ListPeriodicReportTaskIDsV1(
			ctx, after, CoordinatorPageSize)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		if len(taskIDs) == 0 {
			break
		}
		sem := make(chan struct{}, CoordinatorWorkers)
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for _, taskID := range taskIDs {
			select {
			case <-ctx.Done():
				errs = append(errs, ctx.Err())
				goto drain
			case sem <- struct{}{}:
			}
			wg.Add(1)
			go func(taskID string) {
				defer wg.Done()
				defer func() { <-sem }()
				if taskErr := c.runTask(ctx, taskID); taskErr != nil {
					errMu.Lock()
					errs = append(errs, taskErr)
					errMu.Unlock()
				}
			}(taskID)
		}
	drain:
		wg.Wait()
		after = taskIDs[len(taskIDs)-1]
		if len(taskIDs) < CoordinatorPageSize || ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

func (c *Coordinator) runTask(ctx context.Context, taskID string) error {
	now := time.Now()
	if err := c.store.EvaluatePeriodicReportCadenceV1(
		ctx, taskID, now); err != nil {
		return err
	}
	candidate, err := c.store.GetPeriodicReportScheduleV1(
		ctx, taskID)
	if err != nil {
		return err
	}
	start, end, err := previousNaturalPeriodV1(
		now, candidate.Settings.Timezone,
		candidate.Settings.Cadence)
	if err != nil {
		return err
	}
	intent, err := c.store.PreparePeriodicBriefIntentV1(
		ctx, candidate.TenantID, candidate.UserID, candidate.TaskID,
		candidate.Settings.Cadence, start, end)
	if err != nil {
		return err
	}
	run, err := c.temporal.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID: intent.WorkflowID, TaskQueue: c.taskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		WorkflowV1,
		WorkflowInputV1{
			IntentID: intent.ID, TenantID: intent.TenantID,
			UserID: intent.UserID,
		},
	)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			// A durable binding proves this exact period workflow was already
			// started successfully. Temporal may report a different current
			// run after an operator reset; the sealed intent must not rewrite
			// its original execution identity during process startup.
			runID, bind := existingPeriodicBriefRunBinding(
				intent.TemporalRunID, alreadyStarted.RunId)
			if !bind {
				return nil
			}
			if runID != "" {
				return c.store.BindPeriodicBriefIntentRunV1(
					ctx, intent.TenantID, intent.UserID, intent.ID,
					runID)
			}
			description, describeErr :=
				c.temporal.DescribeWorkflowExecution(
					ctx, intent.WorkflowID, "")
			if describeErr != nil {
				return describeErr
			}
			info := description.GetWorkflowExecutionInfo()
			if info == nil || info.GetExecution() == nil ||
				info.GetExecution().GetRunId() == "" {
				return errors.New(
					"periodic Brief execution identity is unavailable")
			}
			return c.store.BindPeriodicBriefIntentRunV1(
				ctx, intent.TenantID, intent.UserID, intent.ID,
				info.GetExecution().GetRunId())
		}
		return err
	}
	return c.store.BindPeriodicBriefIntentRunV1(
		ctx, intent.TenantID, intent.UserID, intent.ID, run.GetRunID())
}

// existingPeriodicBriefRunBinding returns whether an already-started workflow
// still needs its Temporal run identity persisted. A non-empty stored identity
// is sealed evidence of a completed start+bind operation and must never be
// rewritten by a later process startup.
func existingPeriodicBriefRunBinding(
	storedRunID, reportedRunID string,
) (string, bool) {
	if storedRunID != "" {
		return "", false
	}
	return reportedRunID, true
}

func previousNaturalPeriodV1(
	now time.Time,
	timezone string,
	cadence store.BriefReportCadenceV1,
) (time.Time, time.Time, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	local := now.In(location)
	var startLocal, endLocal time.Time
	switch cadence {
	case store.BriefReportCadenceDaily:
		endLocal = time.Date(
			local.Year(), local.Month(), local.Day(),
			0, 0, 0, 0, location)
		startLocal = endLocal.AddDate(0, 0, -1)
	case store.BriefReportCadenceWeekly:
		dayStart := time.Date(
			local.Year(), local.Month(), local.Day(),
			0, 0, 0, 0, location)
		daysSinceMonday := (int(dayStart.Weekday()) + 6) % 7
		endLocal = dayStart.AddDate(0, 0, -daysSinceMonday)
		startLocal = endLocal.AddDate(0, 0, -7)
	case store.BriefReportCadenceMonthly:
		endLocal = time.Date(
			local.Year(), local.Month(), 1,
			0, 0, 0, 0, location)
		startLocal = endLocal.AddDate(0, -1, 0)
	default:
		return time.Time{}, time.Time{},
			errors.New("periodic Brief cadence is invalid")
	}
	return startLocal.UTC(), endLocal.UTC(), nil
}
