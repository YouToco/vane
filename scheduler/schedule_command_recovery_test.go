package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

type scheduleCommandBudgetStore struct {
	scheduleStore
	command    types.ScheduleCommand
	beginCalls int
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
	context.Context,
	int64,
	string,
) ([]types.ScheduleCommand, error) {
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
