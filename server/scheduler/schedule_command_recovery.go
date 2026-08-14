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
	scheduleCommandRecoveryPageSize = 256
)

type scheduleCommandRecoveryCursor struct {
	tenantID  int64
	commandID string
}

// RecoverScheduleCommandsOnce makes one bounded pass from a persistent in-process
// (tenant,id) cursor. The cursor advances before a remote attempt, so a command
// that consumes the whole pass cannot starve later commands or tenants.
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
	s.commandRecoveryMu.Lock()
	defer s.commandRecoveryMu.Unlock()
	passCtx, cancelPass := context.WithTimeout(ctx, passBudget)
	defer cancelPass()

	commandStore, ok := s.st.(scheduleCommandStore)
	if !ok {
		return types.NewAppError(
			types.CodeInternal, "任务命令恢复控制面未配置", nil,
		)
	}
	cursorStore, ok := s.st.(scheduleCommandRecoveryCursorStore)
	if !ok {
		return types.NewAppError(
			types.CodeInternal, "任务命令持久恢复游标未配置", nil,
		)
	}
	tenantCursor, commandCursor, err := cursorStore.LoadScheduleCommandRecoveryCursor(passCtx)
	if err != nil {
		return err
	}
	s.commandRecoveryCursor = scheduleCommandRecoveryCursor{
		tenantID: tenantCursor, commandID: commandCursor,
	}
	var (
		afterTenantID = s.commandRecoveryCursor.tenantID
		processed     int
		recoveryErrs  []error
	)
	budgetError := func() error {
		return fmt.Errorf(
			"schedule command recovery pass budget exhausted "+
				"(processed=%d cursor_tenant=%d cursor_id=%q): %w",
			processed, s.commandRecoveryCursor.tenantID,
			s.commandRecoveryCursor.commandID, passCtx.Err(),
		)
	}
	persistCursor := func(cursor scheduleCommandRecoveryCursor) error {
		if err := cursorStore.SaveScheduleCommandRecoveryCursor(
			passCtx, cursor.tenantID, cursor.commandID,
		); err != nil {
			return err
		}
		s.commandRecoveryCursor = cursor
		return nil
	}
	processTenant := func(tenantID int64, afterID string) error {
		if err := persistCursor(scheduleCommandRecoveryCursor{
			tenantID: tenantID, commandID: afterID,
		}); err != nil {
			return err
		}
		for {
			commands, listErr := commandStore.ListPendingScheduleCommands(
				passCtx, tenantID, afterID)
			if listErr != nil {
				recoveryErrs = append(recoveryErrs, listErr)
				if passCtx.Err() != nil {
					return budgetError()
				}
				return nil
			}
			for i := range commands {
				command := &commands[i]
				afterID = command.ID
				if err := persistCursor(scheduleCommandRecoveryCursor{
					tenantID: tenantID, commandID: afterID,
				}); err != nil {
					return err
				}
				attemptCtx, cancel := s.newScheduleCommandWorkContext(passCtx)
				err := s.runScheduleCommandAttempt(attemptCtx, command)
				cancel()
				processed++
				if err != nil && !errors.Is(err, types.ErrNotFound) {
					recoveryErrs = append(recoveryErrs, err)
				}
				if passCtx.Err() != nil {
					return budgetError()
				}
			}
			if len(commands) < scheduleCommandRecoveryPageSize {
				return nil
			}
		}
	}

	// Resume the exact tenant page that was interrupted on the previous pass.
	// Catalog pagination starts strictly after that tenant only once its
	// remaining command page has had a chance to run.
	if afterTenantID > 0 {
		if err := processTenant(
			afterTenantID, s.commandRecoveryCursor.commandID,
		); err != nil {
			return errors.Join(append(recoveryErrs, err)...)
		}
	}
	for {
		tenantIDs, err := s.st.ListRecoveryTenantCatalogPage(
			passCtx, afterTenantID, scheduleCommandRecoveryPageSize)
		if err != nil {
			if passCtx.Err() != nil {
				err = errors.Join(err, budgetError())
			}
			return errors.Join(append(recoveryErrs, err)...)
		}
		if len(tenantIDs) == 0 {
			if err := persistCursor(scheduleCommandRecoveryCursor{}); err != nil {
				return errors.Join(append(recoveryErrs, err)...)
			}
			return errors.Join(recoveryErrs...)
		}
		for _, tenantID := range tenantIDs {
			if err := processTenant(tenantID, ""); err != nil {
				return errors.Join(append(recoveryErrs, err)...)
			}
			afterTenantID = tenantID
		}
		if len(tenantIDs) < scheduleCommandRecoveryPageSize {
			if err := persistCursor(scheduleCommandRecoveryCursor{}); err != nil {
				return errors.Join(append(recoveryErrs, err)...)
			}
			return errors.Join(recoveryErrs...)
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
