package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

type scheduleCommandBudgetStore struct {
	scheduleStore
	scheduleCommandRecoveryMemoryCursor
	command    types.ScheduleCommand
	beginCalls int
}

type scheduleCommandRecoveryMemoryCursor struct {
	tenantID  int64
	commandID string
}

func (s *scheduleCommandRecoveryMemoryCursor) LoadScheduleCommandRecoveryCursor(
	context.Context,
) (int64, string, error) {
	return s.tenantID, s.commandID, nil
}

func (s *scheduleCommandRecoveryMemoryCursor) SaveScheduleCommandRecoveryCursor(
	_ context.Context, tenantID int64, commandID string,
) error {
	s.tenantID, s.commandID = tenantID, commandID
	return nil
}

func (s *scheduleCommandBudgetStore) ListRecoveryTenantCatalogPage(
	_ context.Context, afterTenantID int64, _ int,
) ([]int64, error) {
	if afterTenantID >= s.command.TenantID {
		return nil, nil
	}
	return []int64{s.command.TenantID}, nil
}

func (s *scheduleCommandBudgetStore) CreateOrLoadScheduleCommand(
	context.Context,
	int64,
	int64,
	string,
	string,
	types.ScheduleCommandKind,
	string,
	string,
) (*types.ScheduleCommand, error) {
	panic("unexpected CreateOrLoadScheduleCommand")
}

func (s *scheduleCommandBudgetStore) LoadScheduleCommand(
	context.Context,
	int64,
	int64,
	string,
) (*types.ScheduleCommand, error) {
	panic("unexpected LoadScheduleCommand")
}

func (s *scheduleCommandBudgetStore) BeginScheduleCommandAttempt(
	ctx context.Context,
	_, _ int64,
	_ string,
) (
	*types.ScheduleCommand,
	*types.Schedule,
	func(context.Context) error,
	func(context.Context, string, string) error,
	func(context.Context) error,
	error,
) {
	s.beginCalls++
	<-ctx.Done()
	return nil, nil, nil, nil, nil, ctx.Err()
}

func (s *scheduleCommandBudgetStore) ListPendingScheduleCommands(
	_ context.Context,
	_ int64,
	afterID string,
) ([]types.ScheduleCommand, error) {
	if afterID >= s.command.ID {
		return nil, nil
	}
	return []types.ScheduleCommand{s.command}, nil
}

func TestScheduleCommandRecoveryPassBudgetStopsStartupBarrier(t *testing.T) {
	st := &scheduleCommandBudgetStore{
		command: types.ScheduleCommand{
			ID:             "00000000-0000-0000-0000-000000000001",
			TenantID:       1,
			UserID:         2,
			TaskID:         "budget-task",
			IdempotencyKey: "budget-key",
			Kind:           types.ScheduleCommandPause,
			Status:         types.ScheduleCommandPending,
		},
	}
	s := New(nil, "unused", st)
	const budget = 30 * time.Millisecond
	started := time.Now()
	err := s.recoverScheduleCommands(t.Context(), budget)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recovery error=%v, want deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), "processed=1") {
		t.Fatalf("recovery error lacks progress evidence: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("recovery ignored %s pass budget; elapsed=%s", budget, elapsed)
	}
	if st.beginCalls != 1 {
		t.Fatalf("attempts=%d, want exactly one bounded attempt", st.beginCalls)
	}
}

type scheduleCommandFairnessStore struct {
	scheduleStore
	scheduleCommandRecoveryMemoryCursor
	commands map[int64]types.ScheduleCommand
	attempts map[int64]int
}

func (s *scheduleCommandFairnessStore) ListRecoveryTenantCatalogPage(
	_ context.Context, afterTenantID int64, _ int,
) ([]int64, error) {
	ids := make([]int64, 0, len(s.commands))
	for _, tenantID := range []int64{1, 2} {
		if tenantID > afterTenantID {
			ids = append(ids, tenantID)
		}
	}
	return ids, nil
}

func (s *scheduleCommandFairnessStore) ListPendingScheduleCommands(
	_ context.Context, tenantID int64, afterID string,
) ([]types.ScheduleCommand, error) {
	command, ok := s.commands[tenantID]
	if !ok || command.ID <= afterID {
		return nil, nil
	}
	return []types.ScheduleCommand{command}, nil
}

func (s *scheduleCommandFairnessStore) CreateOrLoadScheduleCommand(
	context.Context, int64, int64, string, string,
	types.ScheduleCommandKind, string, string,
) (*types.ScheduleCommand, error) {
	panic("unexpected CreateOrLoadScheduleCommand")
}

func (s *scheduleCommandFairnessStore) LoadScheduleCommand(
	context.Context, int64, int64, string,
) (*types.ScheduleCommand, error) {
	panic("unexpected LoadScheduleCommand")
}

func (s *scheduleCommandFairnessStore) BeginScheduleCommandAttempt(
	ctx context.Context, tenantID, _ int64, _ string,
) (
	*types.ScheduleCommand,
	*types.Schedule,
	func(context.Context) error,
	func(context.Context, string, string) error,
	func(context.Context) error,
	error,
) {
	s.attempts[tenantID]++
	if tenantID == 1 {
		<-ctx.Done()
		return nil, nil, nil, nil, nil, ctx.Err()
	}
	return nil, nil, nil, nil, nil, types.ErrNotFound
}

func TestScheduleCommandRecoveryCursorPreventsCrossTenantStarvation(t *testing.T) {
	command := func(id string, tenantID int64) types.ScheduleCommand {
		return types.ScheduleCommand{
			ID: id, TenantID: tenantID, UserID: tenantID,
			TaskID: "task", IdempotencyKey: "key-" + id,
			Kind: types.ScheduleCommandPause, Status: types.ScheduleCommandPending,
		}
	}
	st := &scheduleCommandFairnessStore{
		commands: map[int64]types.ScheduleCommand{
			1: command("00000000-0000-0000-0000-000000000001", 1),
			2: command("00000000-0000-0000-0000-000000000002", 2),
		},
		attempts: make(map[int64]int),
	}
	firstScheduler := New(nil, "unused", st)
	if err := firstScheduler.recoverScheduleCommands(t.Context(), 30*time.Millisecond); !errors.Is(
		err, context.DeadlineExceeded,
	) {
		t.Fatalf("first pass error=%v, want deadline", err)
	}
	if st.attempts[1] != 1 || st.attempts[2] != 0 {
		t.Fatalf("first pass attempts=%v", st.attempts)
	}
	// Recreate Scheduler to model the production startup process exiting after
	// its bounded Gate. Only the Store-backed cursor survives this boundary.
	secondScheduler := New(nil, "unused", st)
	if err := secondScheduler.recoverScheduleCommands(t.Context(), time.Second); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if st.attempts[1] != 1 || st.attempts[2] != 1 {
		t.Fatalf("second pass did not advance to tenant 2: %v", st.attempts)
	}
}
