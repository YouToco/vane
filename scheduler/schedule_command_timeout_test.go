package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"

	"github.com/YouToco/vane/types"
)

type scheduleCommandTimeoutClient struct {
	client.Client
	service workflowservice.WorkflowServiceClient
}

func (c *scheduleCommandTimeoutClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.service
}

type scheduleCommandFirstRPCBlackhole struct {
	workflowservice.WorkflowServiceClient
	mu          sync.Mutex
	blackholed  bool
	paused      bool
	deleted     bool
	remoteCalls int
}

func (s *scheduleCommandFirstRPCBlackhole) waitFirst(
	ctx context.Context,
) error {
	s.mu.Lock()
	s.remoteCalls++
	if s.blackholed {
		s.mu.Unlock()
		return ctx.Err()
	}
	s.blackholed = true
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (s *scheduleCommandFirstRPCBlackhole) DescribeSchedule(
	ctx context.Context,
	_ *workflowservice.DescribeScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	s.mu.Lock()
	first := !s.blackholed
	s.mu.Unlock()
	if first {
		if err := s.waitFirst(ctx); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remoteCalls++
	if s.deleted {
		return nil, errors.New("unexpected Describe after delete")
	}
	return &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{
			State: &schedulepb.ScheduleState{Paused: s.paused},
		},
	}, nil
}

func (s *scheduleCommandFirstRPCBlackhole) PatchSchedule(
	ctx context.Context,
	request *workflowservice.PatchScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.PatchScheduleResponse, error) {
	s.mu.Lock()
	first := !s.blackholed
	s.mu.Unlock()
	if first {
		if err := s.waitFirst(ctx); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remoteCalls++
	if request.GetPatch().GetPause() != "" {
		s.paused = true
	}
	if request.GetPatch().GetUnpause() != "" {
		s.paused = false
	}
	return &workflowservice.PatchScheduleResponse{}, nil
}

func (s *scheduleCommandFirstRPCBlackhole) DeleteSchedule(
	ctx context.Context,
	_ *workflowservice.DeleteScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DeleteScheduleResponse, error) {
	s.mu.Lock()
	first := !s.blackholed
	s.mu.Unlock()
	if first {
		if err := s.waitFirst(ctx); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remoteCalls++
	s.deleted = true
	return &workflowservice.DeleteScheduleResponse{}, nil
}

type scheduleCommandTimeoutStore struct {
	scheduleStore
	mu      sync.Mutex
	command *types.ScheduleCommand
	locked  bool
}

func (s *scheduleCommandTimeoutStore) ResolveActiveTenantForUser(
	context.Context,
	int64,
) (int64, error) {
	return 1, nil
}

func (s *scheduleCommandTimeoutStore) CreateOrLoadScheduleCommand(
	_ context.Context,
	tenantID, userID int64,
	taskID, key string,
	kind types.ScheduleCommandKind,
	payloadDigest, remoteRequestID string,
) (*types.ScheduleCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.command == nil {
		s.command = &types.ScheduleCommand{
			ID:       "00000000-0000-0000-0000-000000000001",
			TenantID: tenantID, UserID: userID, TaskID: taskID,
			IdempotencyKey: key, Kind: kind,
			PayloadDigest: payloadDigest, RemoteRequestID: remoteRequestID,
			Status: types.ScheduleCommandPending,
			Phase:  types.ScheduleCommandIntent,
		}
	}
	copy := *s.command
	return &copy, nil
}

func (s *scheduleCommandTimeoutStore) LoadScheduleCommand(
	context.Context,
	int64,
	int64,
	string,
) (*types.ScheduleCommand, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *s.command
	return &copy, nil
}

func (s *scheduleCommandTimeoutStore) BeginScheduleCommandAttempt(
	_ context.Context,
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
	s.mu.Lock()
	if s.locked {
		s.mu.Unlock()
		return nil, nil, nil, nil, nil, errors.New("task lock still held")
	}
	s.locked = true
	copy := *s.command
	s.mu.Unlock()

	released := false
	release := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !released {
			s.locked = false
			released = true
		}
	}
	complete := func(context.Context) error {
		s.mu.Lock()
		s.command.Status = types.ScheduleCommandCompleted
		s.command.Phase = types.ScheduleCommandCompletedPhase
		s.mu.Unlock()
		release()
		return nil
	}
	block := func(context.Context, string, string) error {
		s.mu.Lock()
		s.command.Status = types.ScheduleCommandBlocked
		s.command.Phase = types.ScheduleCommandBlockedPhase
		s.mu.Unlock()
		release()
		return nil
	}
	rollback := func(context.Context) error {
		release()
		return nil
	}
	return &copy, &types.Schedule{}, complete, block, rollback, nil
}

func (s *scheduleCommandTimeoutStore) ListPendingScheduleCommands(
	ctx context.Context,
	_ int64,
	_ string,
) ([]types.ScheduleCommand, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.command == nil ||
		s.command.Status != types.ScheduleCommandPending {
		return nil, nil
	}
	return []types.ScheduleCommand{*s.command}, nil
}

func (s *scheduleCommandTimeoutStore) state() (
	types.ScheduleCommandStatus,
	bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.command.Status, s.locked
}

func TestScheduleCommandFirstRPCBlackholeUsesSharedAttemptBudget(
	t *testing.T,
) {
	for _, kind := range []types.ScheduleCommandKind{
		types.ScheduleCommandRun,
		types.ScheduleCommandPause,
		types.ScheduleCommandResume,
		types.ScheduleCommandDelete,
	} {
		t.Run(string(kind), func(t *testing.T) {
			st := &scheduleCommandTimeoutStore{}
			remote := &scheduleCommandFirstRPCBlackhole{
				paused: kind == types.ScheduleCommandResume,
			}
			s := New(
				&scheduleCommandTimeoutClient{service: remote},
				"unused",
				st,
				WithTaskScheduleNamespace("test"),
				withScheduleCommandAttemptTimeout(40*time.Millisecond),
			)
			started := time.Now()
			var err error
			switch kind {
			case types.ScheduleCommandRun:
				err = s.TriggerScheduleNowIdempotent(
					t.Context(), "blackhole-task", 7, "blackhole-key",
				)
			case types.ScheduleCommandPause:
				err = s.PausePushIdempotent(
					t.Context(), "blackhole-task", 7, "blackhole-key",
				)
			case types.ScheduleCommandResume:
				err = s.ResumePushIdempotent(
					t.Context(), "blackhole-task", 7, "blackhole-key",
				)
			case types.ScheduleCommandDelete:
				err = s.DeletePushIdempotent(
					t.Context(), "blackhole-task", 7, "blackhole-key",
				)
			}
			if err == nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("first RPC blackhole error=%v", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("attempt exceeded internal budget: %s", elapsed)
			}
			status, locked := st.state()
			if status != types.ScheduleCommandPending || locked {
				t.Fatalf("after timeout status=%s locked=%t", status, locked)
			}

			if err := s.RecoverScheduleCommandsOnce(t.Context()); err != nil {
				t.Fatalf("recover pending command: %v", err)
			}
			status, locked = st.state()
			if status != types.ScheduleCommandCompleted || locked {
				t.Fatalf("after recovery status=%s locked=%t", status, locked)
			}
		})
	}
}
