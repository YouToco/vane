package scheduler

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type scheduleCommandTimeoutClient struct {
	client.Client
	service      workflowservice.WorkflowServiceClient
	mu           sync.Mutex
	options      client.StartWorkflowOptions
	workflowType interface{}
	args         []interface{}
	executeCalls int
}

func (c *scheduleCommandTimeoutClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.service
}

func (c *scheduleCommandTimeoutClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflowType interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.options = options
	c.workflowType = workflowType
	c.args = append([]interface{}(nil), args...)
	c.executeCalls++
	return nil, nil
}

type scheduleCommandFirstRPCBlackhole struct {
	workflowservice.WorkflowServiceClient
	mu          sync.Mutex
	blackholed  bool
	paused      bool
	deleted     bool
	remoteCalls int
	action      *schedulepb.ScheduleAction
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
			Action: s.action,
			State:  &schedulepb.ScheduleState{Paused: s.paused},
		},
	}, nil
}

type scheduleCommandStaticDescribe struct {
	workflowservice.WorkflowServiceClient
	action *schedulepb.ScheduleAction
}

type scheduleCommandNotFoundDescribe struct {
	workflowservice.WorkflowServiceClient
}

func (*scheduleCommandNotFoundDescribe) DescribeSchedule(
	context.Context,
	*workflowservice.DescribeScheduleRequest,
	...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	return nil, serviceerror.NewNotFound("schedule does not exist")
}

func (s *scheduleCommandStaticDescribe) DescribeSchedule(
	context.Context,
	*workflowservice.DescribeScheduleRequest,
	...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	return &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{Action: s.action},
	}, nil
}

func rawResumeScheduleAction(
	taskID, runtimeVersion string,
) *schedulepb.ScheduleAction {
	return rawScheduleActionFromClient(
		&client.ScheduleWorkflowAction{
			ID: "wf-" + taskID,
			Args: []interface{}{workflow.PushParams{
				TenantID:       1,
				UserID:         7,
				RunKind:        workflow.PushRunKindScheduled,
				ExecutionMode:  types.ExecutionModeCompiled,
				RuntimeVersion: runtimeVersion,
				ScheduleID:     taskID,
			}},
		},
		converter.GetDefaultDataConverter(),
	)
}

func rawResearchV3ScheduleAction(
	taskID string,
	input workflow.ResearchScheduledInputV3,
) *schedulepb.ScheduleAction {
	return rawScheduleActionFromClient(
		&client.ScheduleWorkflowAction{
			ID: "wf-" + taskID, Workflow: workflow.ResearchScheduledWorkflowV3Name,
			TaskQueue: "sealed-v3-tq", Args: []interface{}{input},
		},
		converter.GetDefaultDataConverter(),
	)
}

func TestDurableRunStartsFormalResearchV3Envelope(t *testing.T) {
	const taskID = "task-v3-manual-run"
	input := workflow.ResearchScheduledInputV3{
		TenantID: 1, UserID: 7, TaskID: taskID,
		ActionAuthorizationToken: strings.Repeat("a", 64),
	}
	remote := &scheduleCommandStaticDescribe{
		action: rawResearchV3ScheduleAction(taskID, input),
	}
	client := &scheduleCommandTimeoutClient{service: remote}
	st := &scheduleCommandTimeoutStore{
		researchV3AuthorityEnabled: true,
		researchV3AuthorityToken:   input.ActionAuthorizationToken,
	}
	s := New(client, "vane-tq", st,
		WithTaskScheduleNamespace("test"),
		WithResearchRuntimeV3AuthorityCanary("task-next-cutover"),
	)
	command := &types.ScheduleCommand{
		ID:       "01900000-0000-7000-8000-000000000001",
		TenantID: 1, UserID: 7, TaskID: taskID,
		Kind: types.ScheduleCommandRun, CreatedAt: time.Date(
			2026, time.August, 3, 12, 34, 56, 0, time.UTC,
		),
	}

	if err := s.applyScheduleCommandRemote(t.Context(), command); err != nil {
		t.Fatalf("run formal Research V3 Action: %v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.executeCalls != 1 || client.options.ID != manualTaskWorkflowID(
		command.ID, command.CreatedAt,
	) || client.options.TaskQueue != "sealed-v3-tq" || len(client.args) != 1 {
		t.Fatalf("V3 manual execution=%d options=%+v args=%#v",
			client.executeCalls, client.options, client.args)
	}
	if reflect.ValueOf(client.workflowType).Pointer() !=
		reflect.ValueOf(workflow.ResearchScheduledWorkflowV3).Pointer() {
		t.Fatalf("manual workflow type=%T, want ResearchScheduledWorkflowV3",
			client.workflowType)
	}
	if got, ok := client.args[0].(workflow.ResearchScheduledInputV3); !ok || got != input {
		t.Fatalf("manual V3 input=%#v, want %#v", client.args[0], input)
	}
}

func TestDurableRunRejectsFormalResearchV3WithoutEnabledAuthority(t *testing.T) {
	const taskID = "task-v3-manual-run-no-authority"
	input := workflow.ResearchScheduledInputV3{
		TenantID: 1, UserID: 7, TaskID: taskID,
		ActionAuthorizationToken: strings.Repeat("b", 64),
	}
	client := &scheduleCommandTimeoutClient{service: &scheduleCommandStaticDescribe{
		action: rawResearchV3ScheduleAction(taskID, input),
	}}
	s := New(client, "vane-tq", &scheduleCommandTimeoutStore{},
		WithTaskScheduleNamespace("test"))
	err := s.applyScheduleCommandRemote(t.Context(), &types.ScheduleCommand{
		ID:       "01900000-0000-7000-8000-000000000002",
		TenantID: 1, UserID: 7, TaskID: taskID,
		Kind: types.ScheduleCommandRun, CreatedAt: time.Now().UTC(),
	})
	if types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("manual V3 without enabled authority error=%v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.executeCalls != 0 {
		t.Fatalf("unauthorized V3 manual executions=%d", client.executeCalls)
	}
}

func TestDurableRunRejectsRetiredPushAction(t *testing.T) {
	const taskID = "task-retired-manual-run"
	client := &scheduleCommandTimeoutClient{service: &scheduleCommandStaticDescribe{
		action: rawResumeScheduleAction(taskID, workflow.CompiledRuntimeSnapshotV1),
	}}
	s := New(client, "vane-tq", &scheduleCommandTimeoutStore{},
		WithTaskScheduleNamespace("test"))
	err := s.applyScheduleCommandRemote(t.Context(), &types.ScheduleCommand{
		ID:       "01900000-0000-7000-8000-000000000003",
		TenantID: 1, UserID: 7, TaskID: taskID,
		Kind: types.ScheduleCommandRun, CreatedAt: time.Now().UTC(),
	})
	if types.CodeOf(err) != types.CodeConflict {
		t.Fatalf("retired manual run error=%v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.executeCalls != 0 {
		t.Fatalf("retired manual workflow executions=%d", client.executeCalls)
	}
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
	scheduleCommandRecoveryMemoryCursor
	scheduleStore
	mu                         sync.Mutex
	command                    *types.ScheduleCommand
	locked                     bool
	toolDefinition             bool
	schedule                   types.Schedule
	researchV3AuthorityEnabled bool
	researchV3AuthorityToken   string
}

func (s *scheduleCommandTimeoutStore) VerifyEnabledResearchV3ActionAuthorization(
	_ context.Context,
	_, _ int64,
	_ string,
	token string,
) error {
	if !s.researchV3AuthorityEnabled || token != s.researchV3AuthorityToken {
		return types.NewAppError(
			types.CodeConflict, "Research V3 authority unavailable", types.ErrConflict,
		)
	}
	return nil
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

func (s *scheduleCommandTimeoutStore) HasCurrentToolApprovedDefinition(
	context.Context,
	int64, int64,
	string,
) (bool, error) {
	return s.toolDefinition, nil
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
	schedule := s.schedule
	return &copy, &schedule, complete, block, rollback, nil
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

func (s *scheduleCommandTimeoutStore) ListRecoveryTenantCatalogPage(
	context.Context, int64, int,
) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.command == nil {
		return nil, nil
	}
	return []int64{s.command.TenantID}, nil
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
			token := strings.Repeat("c", 64)
			st := &scheduleCommandTimeoutStore{
				researchV3AuthorityEnabled: true,
				researchV3AuthorityToken:   token,
				schedule: types.Schedule{
					ID: "blackhole-task", TenantID: 1, UserID: 7,
					Status:        types.ScheduleStatusActive,
					ExecutionMode: types.ExecutionModeDiscoverAtRun,
				},
			}
			remote := &scheduleCommandFirstRPCBlackhole{
				paused: kind == types.ScheduleCommandResume,
				action: rawResearchV3ScheduleAction("blackhole-task",
					workflow.ResearchScheduledInputV3{
						TenantID: 1, UserID: 7, TaskID: "blackhole-task",
						ActionAuthorizationToken: token,
					}),
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

func TestDurableResumeRejectsRetiredPushAction(t *testing.T) {
	const taskID = "task-tool-resume-durable"
	st := &scheduleCommandTimeoutStore{
		toolDefinition: true,
		schedule: types.Schedule{
			ID: taskID, TenantID: 1, UserID: 7,
			Status:        types.ScheduleStatusPaused,
			ExecutionMode: types.ExecutionModeCompiled,
		},
	}
	s := New(
		&scheduleCommandTimeoutClient{
			service: &scheduleCommandStaticDescribe{
				action: rawResumeScheduleAction(
					taskID,
					workflow.CompiledRuntimeToolSnapshotV2,
				),
			},
		},
		"unused", st,
		WithCompiledRuntimeRollout(true, taskID, false),
	)
	err := s.ResumePushIdempotent(
		t.Context(), taskID, 7, "resume-retired-action")
	if err == nil {
		t.Fatal("durable resume accepted a retired Push Action")
	}
	status, locked := st.state()
	if status != types.ScheduleCommandBlocked || locked {
		t.Fatalf("blocked resume state=%s locked=%v", status, locked)
	}
}

func TestDurableResumeBlocksMissingTemporalSchedule(t *testing.T) {
	const taskID = "task-resume-missing-remote"
	st := &scheduleCommandTimeoutStore{
		schedule: types.Schedule{
			ID: taskID, TenantID: 1, UserID: 7,
			Status:        types.ScheduleStatusPaused,
			ExecutionMode: types.ExecutionModeCompiled,
		},
	}
	s := New(
		&scheduleCommandTimeoutClient{
			service: &scheduleCommandNotFoundDescribe{},
		},
		"unused", st,
	)
	err := s.ResumePushIdempotent(
		t.Context(), taskID, 7, "resume-missing-remote")
	if err == nil || !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("missing Temporal schedule error=%v", err)
	}
	status, locked := st.state()
	if status != types.ScheduleCommandBlocked || locked {
		t.Fatalf("missing remote resume state=%s locked=%v", status, locked)
	}
}
