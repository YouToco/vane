package periodicbrief

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/store"
)

const (
	CoordinatorInterval = 30 * time.Second
	CoordinatorTimeout  = 30 * time.Second
)

type CoordinatorStore interface {
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
	exactTaskID string
	logger      *slog.Logger
}

func NewCoordinator(
	st CoordinatorStore,
	temporalClient client.Client,
	taskQueue, exactTaskID string,
	logger *slog.Logger,
) (*Coordinator, error) {
	if st == nil || temporalClient == nil || taskQueue == "" {
		return nil, errors.New("periodic Brief coordinator dependencies are incomplete")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		store: st, temporal: temporalClient, taskQueue: taskQueue,
		exactTaskID: exactTaskID, logger: logger,
	}, nil
}

func (c *Coordinator) RunStartup(ctx context.Context) error {
	if c.exactTaskID == "" {
		return nil
	}
	passCtx, cancel := context.WithTimeout(ctx, CoordinatorTimeout)
	defer cancel()
	return c.runOnce(passCtx)
}

func (c *Coordinator) Run(ctx context.Context) {
	if c.exactTaskID == "" {
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
					"error", err)
			}
		}
	}
}

func (c *Coordinator) runOnce(ctx context.Context) error {
	candidate, err := c.store.GetPeriodicReportScheduleV1(
		ctx, c.exactTaskID)
	if err != nil {
		return err
	}
	start, end, err := previousNaturalPeriodV1(
		time.Now(), candidate.Settings.Timezone,
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
			return nil
		}
		return err
	}
	return c.store.BindPeriodicBriefIntentRunV1(
		ctx, intent.TenantID, intent.UserID, intent.ID, run.GetRunID())
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
