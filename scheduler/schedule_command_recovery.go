package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/YouToco/vane/types"
)

const (
	scheduleCommandRecoveryInterval = 10 * time.Second
)

// RecoverScheduleCommandsOnce makes one bounded pass over the immutable
// recovery snapshot. A failed early command cannot starve later tenants
// because pagination advances by the durable (tenant,id) identity.
func (s *Scheduler) RecoverScheduleCommandsOnce(ctx context.Context) error {
	return s.recoverScheduleCommands(
		ctx, ScheduleCommandRecoveryPassTimeout,
	)
}

func (s *Scheduler) recoverScheduleCommands(
	ctx context.Context,
	passBudget time.Duration,
) error {
	if passBudget <= 0 {
		return errors.New("schedule command recovery pass budget must be positive")
	}
	passCtx, cancelPass := context.WithTimeout(ctx, passBudget)
	defer cancelPass()

	commandStore, ok := s.st.(scheduleCommandStore)
	if !ok {
		return types.NewAppError(
			types.CodeInternal, "任务命令恢复控制面未配置", nil,
		)
	}
	var (
		afterTenantID int64
		afterID       string
		processed     int
		recoveryErrs  []error
	)
	budgetError := func() error {
		return fmt.Errorf(
			"schedule command recovery pass budget exhausted "+
				"(processed=%d cursor_tenant=%d cursor_id=%q): %w",
			processed, afterTenantID, afterID, passCtx.Err(),
		)
	}
	for {
		commands, err := commandStore.ListPendingScheduleCommands(
			passCtx, afterTenantID, afterID,
		)
		if err != nil {
			if passCtx.Err() != nil {
				err = errors.Join(err, budgetError())
			}
			return errors.Join(append(recoveryErrs, err)...)
		}
		if len(commands) == 0 {
			return errors.Join(recoveryErrs...)
		}
		for i := range commands {
			command := &commands[i]
			afterTenantID, afterID = command.TenantID, command.ID
			attemptCtx, cancel := s.newScheduleCommandWorkContext(passCtx)
			err := s.runScheduleCommandAttempt(attemptCtx, command)
			cancel()
			processed++
			// A permanent missing Temporal schedule was atomically checkpointed
			// as blocked. It needs operator visibility, not infinite startup
			// failure or periodic retry.
			if err != nil && !errors.Is(err, types.ErrNotFound) {
				recoveryErrs = append(recoveryErrs, err)
			}
			if passCtx.Err() != nil {
				return errors.Join(
					append(recoveryErrs, budgetError())...,
				)
			}
		}
	}
}

// RunScheduleCommandRecovery periodically resumes intents left by request
// cancellation, lost responses, commit ambiguity, or process exit.
func (s *Scheduler) RunScheduleCommandRecovery(ctx context.Context) {
	ticker := time.NewTicker(scheduleCommandRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.RecoverScheduleCommandsOnce(ctx); err != nil &&
				!errors.Is(err, context.Canceled) {
				slog.Error(
					"scheduler: recover durable schedule commands",
					"err", err,
				)
			}
		}
	}
}
