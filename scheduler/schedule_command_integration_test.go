//go:build integration

package scheduler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

func TestScheduleCommandIntegration_PostgreSQLTemporalFaultMatrix(t *testing.T) {
	dbURL := os.Getenv("VANE_TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("未设置 VANE_TEST_DATABASE_URL 或 DATABASE_URL")
	}
	const (
		namespace = "schedule-command-integration"
		taskQueue = "schedule-command-integration"
	)
	startCtx, cancelStart := context.WithTimeout(t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(startCtx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Namespace: namespace},
		LogLevel:      "error",
	})
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop Temporal dev server: %v", err)
		}
		server.Client().Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("migrate PostgreSQL: %v", err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("open PostgreSQL store: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.ValidateScheduleCommandRuntimeRole(ctx); err != nil {
		t.Fatalf("schedule command role gate: %v", err)
	}
	user, err := st.UpsertUserByOpenID(
		ctx, "schedule-command-integration-"+uuid.NewString(),
		"schedule command integration",
	)
	if err != nil {
		t.Fatalf("create integration user: %v", err)
	}
	if err := st.AddMembership(
		ctx, 1, user.ID, types.MembershipRoleOwner,
	); err != nil {
		t.Fatalf("attach integration tenant: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
		defer cleanupCancel()
		cleanupPool, poolErr := pgxpool.New(cleanupCtx, dbURL)
		if poolErr != nil {
			t.Errorf("open cleanup PostgreSQL pool: %v", poolErr)
			return
		}
		defer cleanupPool.Close()
		for _, query := range []string{
			`DELETE FROM schedule_commands WHERE user_id=$1`,
			`DELETE FROM research_v3_delivery_authorities WHERE user_id=$1`,
			`DELETE FROM schedules WHERE user_id=$1`,
			`DELETE FROM memberships WHERE user_id=$1`,
			`DELETE FROM users WHERE id=$1`,
		} {
			if _, err := cleanupPool.Exec(
				cleanupCtx, query, user.ID,
			); err != nil {
				t.Errorf("cleanup query %q: %v", query, err)
			}
		}
	})

	base := New(
		server.Client(), taskQueue, st,
		WithTaskScheduleNamespace(namespace),
	)
	taskID, err := base.CreatePush(
		ctx, user.ID,
		ScheduleSpec{Cron: "0 0 1 1 *", TZ: "UTC"},
		workflow.PushScope{}, "schedule command integration",
	)
	if err != nil {
		t.Fatalf("create integration schedule: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
		defer cleanupCancel()
		err := server.Client().ScheduleClient().GetHandle(
			cleanupCtx, taskID,
		).Delete(cleanupCtx)
		if err != nil {
			var notFound *serviceerror.NotFound
			if !errors.As(err, &notFound) {
				t.Errorf("cleanup Temporal schedule: %v", err)
			}
		}
	})

	faults := &scheduleCommandResponseLossService{
		WorkflowServiceClient: server.Client().WorkflowService(),
		requestIDs:            make(map[string]int),
	}
	faultClient := &scheduleCommandFaultClient{
		Client: server.Client(), service: faults,
	}
	faultScheduler := New(
		faultClient, taskQueue, st,
		WithTaskScheduleNamespace(namespace),
	)
	recoveryScheduler := New(
		faultClient, taskQueue, st,
		WithTaskScheduleNamespace(namespace),
	)

	// Temporal started the command-bound manual workflow but the response was
	// lost. The operation remains intent; a fresh process retries the same
	// workflow ID and adopts AlreadyStarted without touching the Schedule.
	faults.arm("trigger")
	const runKey = "integration-trigger-response-loss"
	if err := faultScheduler.TriggerScheduleNowIdempotent(
		ctx, taskID, user.ID, runKey,
	); err == nil {
		t.Fatal("trigger response loss unexpectedly reported success")
	}
	run, err := st.LoadScheduleCommand(ctx, 1, user.ID, runKey)
	if err != nil || run.Status != types.ScheduleCommandPending {
		t.Fatalf("run after response loss=%+v err=%v", run, err)
	}
	if err := recoveryScheduler.RecoverScheduleCommandsOnce(ctx); err != nil {
		t.Fatalf("recover trigger response loss: %v", err)
	}
	if err := base.TriggerScheduleNowIdempotent(
		ctx, taskID, user.ID, runKey,
	); err != nil {
		t.Fatalf("replay completed trigger: %v", err)
	}
	run = mustScheduleCommand(
		t, st, ctx, 1, user.ID, runKey,
	)
	if run.Status != types.ScheduleCommandCompleted {
		t.Fatalf("run checkpoint=%+v", run)
	}
	manualWorkflowID := manualTaskWorkflowID(run.ID, run.CreatedAt)
	if got := faults.requestCount(manualWorkflowID); got != 2 {
		t.Fatalf("manual workflow attempts=%d, want response-loss retry 2", got)
	}
	assertScheduleActionCount(t, ctx, server.Client(), taskID, 0)
	if _, err := server.Client().WorkflowService().DescribeWorkflowExecution(
		ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: manualWorkflowID,
			},
		},
	); err != nil {
		t.Fatalf("describe command-bound manual workflow: %v", err)
	}

	// Pause response loss is converged in the same attempt by Describe: no
	// reverse compensation and no DB/Temporal split.
	faults.arm("pause")
	const pauseKey = "integration-pause-response-loss"
	if err := faultScheduler.PausePushIdempotent(
		ctx, taskID, user.ID, pauseKey,
	); err != nil {
		t.Fatalf("pause applied-then-error convergence: %v", err)
	}
	assertScheduleCommandState(
		t, ctx, st, server.Client(), taskID, user.ID, pauseKey, true,
	)

	// PostgreSQL commit succeeded but its response was lost. Exact readback
	// adopts the terminal checkpoint rather than issuing a compensating Pause.
	commitLostStore := &scheduleCommandCompletionFaultStore{
		Store: st, mode: scheduleCommandCompleteThenError,
	}
	commitLostScheduler := New(
		server.Client(), taskQueue, commitLostStore,
		WithTaskScheduleNamespace(namespace),
	)
	const resumeCommitKey = "integration-resume-commit-response-loss"
	if err := commitLostScheduler.ResumePushIdempotent(
		ctx, taskID, user.ID, resumeCommitKey,
	); err != nil {
		t.Fatalf("resume commit response loss readback: %v", err)
	}
	assertScheduleCommandState(
		t, ctx, st, server.Client(), taskID, user.ID,
		resumeCommitKey, false,
	)

	// A lost Pause response followed by a blackholed Describe must release the
	// PostgreSQL transaction/advisory lock at the detached fact-read deadline.
	// The operation remains pending and a fresh recovery pass can converge it.
	faults.armWithDescribeBlackhole("pause")
	const pauseBlackholeKey = "integration-pause-describe-blackhole"
	blackholeStarted := time.Now()
	if err := faultScheduler.PausePushIdempotent(
		ctx, taskID, user.ID, pauseBlackholeKey,
	); err == nil {
		t.Fatal("pause Describe blackhole unexpectedly reported success")
	}
	if elapsed := time.Since(blackholeStarted); elapsed >
		scheduleCommandFactReadbackTimeout+3*time.Second {
		t.Fatalf("Describe blackhole held task lock for %s", elapsed)
	}
	if got := mustScheduleCommand(
		t, st, ctx, 1, user.ID, pauseBlackholeKey,
	).Status; got != types.ScheduleCommandPending {
		t.Fatalf("blackholed pause status=%s, want pending", got)
	}
	recoverCtx, cancelRecover := context.WithTimeout(ctx, 3*time.Second)
	if err := base.RecoverScheduleCommandsOnce(recoverCtx); err != nil {
		cancelRecover()
		t.Fatalf("recover pause after Describe deadline: %v", err)
	}
	cancelRecover()
	assertScheduleCommandState(
		t, ctx, st, server.Client(), taskID, user.ID,
		pauseBlackholeKey, true,
	)
	if err := base.ResumePushIdempotent(
		ctx, taskID, user.ID, "integration-resume-after-blackhole",
	); err != nil {
		t.Fatalf("resume after Describe blackhole: %v", err)
	}

	// Simulate process exit after remote Resume but before the atomic mirror
	// checkpoint. A new process observes the Temporal fact and completes PG.
	if err := base.PausePushIdempotent(
		ctx, taskID, user.ID, "integration-pause-before-crash",
	); err != nil {
		t.Fatalf("pause before crash fixture: %v", err)
	}
	crashStore := &scheduleCommandCompletionFaultStore{
		Store: st, mode: scheduleCommandDropBeforeComplete,
	}
	crashScheduler := New(
		server.Client(), taskQueue, crashStore,
		WithTaskScheduleNamespace(namespace),
	)
	const resumeCrashKey = "integration-resume-crash-before-checkpoint"
	if err := crashScheduler.ResumePushIdempotent(
		ctx, taskID, user.ID, resumeCrashKey,
	); err == nil {
		t.Fatal("checkpoint crash unexpectedly reported success")
	}
	if got := mustScheduleCommand(
		t, st, ctx, 1, user.ID, resumeCrashKey,
	).Status; got != types.ScheduleCommandPending {
		t.Fatalf("crashed resume status=%s, want pending", got)
	}
	if err := base.RecoverScheduleCommandsOnce(ctx); err != nil {
		t.Fatalf("recover resume after process exit: %v", err)
	}
	assertScheduleCommandState(
		t, ctx, st, server.Client(), taskID, user.ID, resumeCrashKey, false,
	)

	// Replace only the stored Action with the formal V3 envelope, as the
	// cutover saga does. A durable one-off run must start that exact protocol,
	// survive response loss under the same schedule_commands row, and leave the
	// recurring spec/action-count untouched.
	handle := server.Client().ScheduleClient().GetHandle(ctx, taskID)
	beforeV3, err := handle.Describe(ctx)
	if err != nil {
		t.Fatalf("describe before V3 manual run: %v", err)
	}
	v3Input := workflow.ResearchScheduledInputV3{
		TenantID: 1, UserID: user.ID, TaskID: taskID,
		ActionAuthorizationToken: strings.Repeat("c", 64),
	}
	authorityDigest := sha256.Sum256([]byte(v3Input.ActionAuthorizationToken))
	authorityPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("open V3 authority fixture pool: %v", err)
	}
	if _, err := authorityPool.Exec(ctx, `
		INSERT INTO research_v3_delivery_authorities (
		    tenant_id,user_id,task_id,generation,definition_version,
		    definition_digest,target_action_digest,action_authorization_digest,
		    status,enabled_at
		) VALUES ($1,$2,$3,1,1,$4,$5,$6,'enabled',clock_timestamp())`,
		1, user.ID, taskID, strings.Repeat("d", 64),
		strings.Repeat("e", 64), fmt.Sprintf("%x", authorityDigest),
	); err != nil {
		authorityPool.Close()
		t.Fatalf("install V3 authority fixture: %v", err)
	}
	authorityPool.Close()
	if err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			schedule := in.Description.Schedule
			action, ok := schedule.Action.(*client.ScheduleWorkflowAction)
			if !ok || action == nil {
				return nil, errors.New("integration schedule Action is not a workflow")
			}
			updated := *action
			updated.Workflow = workflow.ResearchScheduledWorkflowV3Name
			updated.Args = []interface{}{v3Input}
			schedule.Action = &updated
			return &client.ScheduleUpdate{Schedule: &schedule}, nil
		},
	}); err != nil {
		t.Fatalf("install formal V3 Action fixture: %v", err)
	}
	formalBeforeV3, err := handle.Describe(ctx)
	if err != nil {
		t.Fatalf("describe installed formal V3 Action: %v", err)
	}
	v3FaultScheduler := New(
		faultClient, taskQueue, st,
		WithTaskScheduleNamespace(namespace),
		WithResearchRuntimeV3AuthorityCanary("task-next-cutover"),
	)
	v3RecoveryScheduler := New(
		faultClient, taskQueue, st,
		WithTaskScheduleNamespace(namespace),
		WithResearchRuntimeV3AuthorityCanary("task-next-cutover"),
	)
	faults.arm("trigger")
	const v3RunKey = "integration-v3-trigger-response-loss"
	if err := v3FaultScheduler.TriggerScheduleNowIdempotent(
		ctx, taskID, user.ID, v3RunKey,
	); err == nil {
		t.Fatal("V3 trigger response loss unexpectedly reported success")
	}
	v3Run := mustScheduleCommand(t, st, ctx, 1, user.ID, v3RunKey)
	if v3Run.Status != types.ScheduleCommandPending {
		t.Fatalf("V3 run after response loss=%+v", v3Run)
	}
	if err := v3RecoveryScheduler.RecoverScheduleCommandsOnce(ctx); err != nil {
		t.Fatalf("recover V3 trigger response loss: %v", err)
	}
	v3Run = mustScheduleCommand(t, st, ctx, 1, user.ID, v3RunKey)
	if v3Run.Status != types.ScheduleCommandCompleted {
		t.Fatalf("V3 run checkpoint=%+v", v3Run)
	}
	v3WorkflowID := manualTaskWorkflowID(v3Run.ID, v3Run.CreatedAt)
	if got := faults.requestCount(v3WorkflowID); got != 2 {
		t.Fatalf("V3 manual workflow attempts=%d, want response-loss retry 2", got)
	}
	v3Execution, err := server.Client().WorkflowService().DescribeWorkflowExecution(
		ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: v3WorkflowID},
		},
	)
	if err != nil {
		t.Fatalf("describe V3 command-bound manual workflow: %v", err)
	}
	if got := v3Execution.GetWorkflowExecutionInfo().GetType().GetName(); got != workflow.ResearchScheduledWorkflowV3Name {
		t.Fatalf("V3 manual workflow type=%q", got)
	}
	history := server.Client().GetWorkflowHistory(
		ctx, v3WorkflowID, "", false,
		enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	started, err := history.Next()
	if err != nil {
		t.Fatalf("read V3 manual workflow start event: %v", err)
	}
	var startedInput workflow.ResearchScheduledInputV3
	if attrs := started.GetWorkflowExecutionStartedEventAttributes(); attrs == nil {
		t.Fatalf("first V3 history event is not WorkflowExecutionStarted: %v", started.GetEventType())
	} else if err := converter.GetDefaultDataConverter().FromPayloads(
		attrs.GetInput(), &startedInput,
	); err != nil {
		t.Fatalf("decode V3 manual workflow input: %v", err)
	}
	if startedInput != v3Input {
		t.Fatalf("V3 manual workflow input=%+v, want %+v", startedInput, v3Input)
	}
	afterV3, err := handle.Describe(ctx)
	if err != nil {
		t.Fatalf("describe after V3 manual run: %v", err)
	}
	if !reflect.DeepEqual(beforeV3.Schedule.Spec, afterV3.Schedule.Spec) ||
		!reflect.DeepEqual(formalBeforeV3.Schedule.State, afterV3.Schedule.State) ||
		!reflect.DeepEqual(formalBeforeV3.Schedule.Action, afterV3.Schedule.Action) {
		t.Fatalf("V3 manual run changed schedule: before=%+v after=%+v",
			formalBeforeV3.Schedule, afterV3.Schedule)
	}
	if action, ok := afterV3.Schedule.Action.(*client.ScheduleWorkflowAction); !ok ||
		fmt.Sprint(action.Workflow) != workflow.ResearchScheduledWorkflowV3Name {
		t.Fatalf("V3 manual run changed formal Action: %#v", afterV3.Schedule.Action)
	}
	assertScheduleActionCount(t, ctx, server.Client(), taskID, 0)

	// Delete is also an intent/checkpoint command. If the process exits after
	// Temporal deletion, recovery adopts NotFound, deletes the mirror, and
	// retains the idempotency tombstone.
	deleteCrashStore := &scheduleCommandCompletionFaultStore{
		Store: st, mode: scheduleCommandDropBeforeComplete,
	}
	deleteCrashScheduler := New(
		server.Client(), taskQueue, deleteCrashStore,
		WithTaskScheduleNamespace(namespace),
	)
	const deleteKey = "integration-delete-crash-before-checkpoint"
	if err := deleteCrashScheduler.DeletePushIdempotent(
		ctx, taskID, user.ID, deleteKey,
	); err == nil {
		t.Fatal("delete checkpoint crash unexpectedly reported success")
	}
	if _, err := st.GetSchedule(ctx, taskID, user.ID); err != nil {
		t.Fatalf("mirror disappeared before recovery: %v", err)
	}
	if err := base.RecoverScheduleCommandsOnce(ctx); err != nil {
		t.Fatalf("recover delete after process exit: %v", err)
	}
	if _, err := st.GetSchedule(
		ctx, taskID, user.ID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("deleted mirror error=%v, want not found", err)
	}
	if got := mustScheduleCommand(
		t, st, ctx, 1, user.ID, deleteKey,
	).Status; got != types.ScheduleCommandCompleted {
		t.Fatalf("delete tombstone status=%s, want completed", got)
	}
	if err := base.DeletePushIdempotent(
		ctx, taskID, user.ID, deleteKey,
	); err != nil {
		t.Fatalf("delete terminal replay: %v", err)
	}
}

type scheduleCommandFaultClient struct {
	client.Client
	service workflowservice.WorkflowServiceClient
}

func (c *scheduleCommandFaultClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.service
}

func (c *scheduleCommandFaultClient) ExecuteWorkflow(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflowType interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	faults, _ := c.service.(*scheduleCommandResponseLossService)
	if faults != nil {
		faults.mu.Lock()
		faults.requestIDs[options.ID]++
		lose := faults.armed == "trigger"
		if lose {
			faults.armed = ""
		}
		faults.mu.Unlock()
		if lose {
			run, err := c.Client.ExecuteWorkflow(
				ctx, options, workflowType, args...,
			)
			if err != nil {
				return run, err
			}
			return nil, context.DeadlineExceeded
		}
	}
	return c.Client.ExecuteWorkflow(ctx, options, workflowType, args...)
}

type scheduleCommandResponseLossService struct {
	workflowservice.WorkflowServiceClient
	mu                    sync.Mutex
	armed                 string
	blackholeOnLoss       bool
	blackholeNextDescribe bool
	requestIDs            map[string]int
}

func (s *scheduleCommandResponseLossService) arm(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = kind
	s.blackholeOnLoss = false
}

func (s *scheduleCommandResponseLossService) armWithDescribeBlackhole(
	kind string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = kind
	s.blackholeOnLoss = true
}

func (s *scheduleCommandResponseLossService) requestCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestIDs[id]
}

func (s *scheduleCommandResponseLossService) PatchSchedule(
	ctx context.Context,
	request *workflowservice.PatchScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PatchScheduleResponse, error) {
	response, err := s.WorkflowServiceClient.PatchSchedule(ctx, request, opts...)
	if err != nil {
		return response, err
	}
	s.mu.Lock()
	s.requestIDs[request.GetRequestId()]++
	kind := ""
	switch {
	case request.GetPatch().GetPause() != "":
		kind = "pause"
	case request.GetPatch().GetUnpause() != "":
		kind = "resume"
	}
	lose := s.armed == kind
	if lose {
		s.armed = ""
		s.blackholeNextDescribe = s.blackholeOnLoss
		s.blackholeOnLoss = false
	}
	s.mu.Unlock()
	if lose {
		return nil, context.DeadlineExceeded
	}
	return response, nil
}

func (s *scheduleCommandResponseLossService) DescribeSchedule(
	ctx context.Context,
	request *workflowservice.DescribeScheduleRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	s.mu.Lock()
	blackhole := s.blackholeNextDescribe
	if blackhole {
		s.blackholeNextDescribe = false
	}
	s.mu.Unlock()
	if blackhole {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.WorkflowServiceClient.DescribeSchedule(ctx, request, opts...)
}

type scheduleCommandCompletionFaultMode int

const (
	scheduleCommandCompleteThenError scheduleCommandCompletionFaultMode = iota + 1
	scheduleCommandDropBeforeComplete
)

type scheduleCommandCompletionFaultStore struct {
	*store.Store
	mu   sync.Mutex
	mode scheduleCommandCompletionFaultMode
	used bool
}

func (s *scheduleCommandCompletionFaultStore) BeginScheduleCommandAttempt(
	ctx context.Context,
	tenantID, userID int64,
	key string,
) (
	*types.ScheduleCommand,
	*types.Schedule,
	func(context.Context) error,
	func(context.Context, string, string) error,
	func(context.Context) error,
	error,
) {
	command, schedule, complete, block, rollback, err :=
		s.Store.BeginScheduleCommandAttempt(
			ctx, tenantID, userID, key,
		)
	if err != nil || complete == nil {
		return command, schedule, complete, block, rollback, err
	}
	wrapped := func(completeCtx context.Context) error {
		s.mu.Lock()
		if s.used {
			s.mu.Unlock()
			return complete(completeCtx)
		}
		s.used = true
		mode := s.mode
		s.mu.Unlock()
		if mode == scheduleCommandCompleteThenError {
			if err := complete(completeCtx); err != nil {
				return err
			}
		}
		return context.DeadlineExceeded
	}
	return command, schedule, wrapped, block, rollback, nil
}

func mustScheduleCommand(
	t *testing.T,
	st *store.Store,
	ctx context.Context,
	tenantID, userID int64,
	key string,
) *types.ScheduleCommand {
	t.Helper()
	command, err := st.LoadScheduleCommand(ctx, tenantID, userID, key)
	if err != nil {
		t.Fatalf("load command %q: %v", key, err)
	}
	return command
}

func assertScheduleCommandState(
	t *testing.T,
	ctx context.Context,
	st *store.Store,
	temporal client.Client,
	taskID string,
	userID int64,
	key string,
	wantPaused bool,
) {
	t.Helper()
	command := mustScheduleCommand(t, st, ctx, 1, userID, key)
	if command.Status != types.ScheduleCommandCompleted {
		t.Fatalf("command %q status=%s", key, command.Status)
	}
	mirror, err := st.GetSchedule(ctx, taskID, userID)
	if err != nil {
		t.Fatalf("load mirror for %q: %v", key, err)
	}
	wantStatus := types.ScheduleStatusActive
	if wantPaused {
		wantStatus = types.ScheduleStatusPaused
	}
	if mirror.Status != wantStatus {
		t.Fatalf("mirror status=%s want=%s", mirror.Status, wantStatus)
	}
	description, err := temporal.ScheduleClient().GetHandle(
		ctx, taskID,
	).Describe(ctx)
	if err != nil {
		t.Fatalf("describe Temporal schedule for %q: %v", key, err)
	}
	if description.Schedule.State == nil ||
		description.Schedule.State.Paused != wantPaused {
		t.Fatalf("Temporal paused=%v want=%v",
			description.Schedule.State, wantPaused)
	}
}

func assertScheduleActionCount(
	t *testing.T,
	ctx context.Context,
	temporal client.Client,
	taskID string,
	want int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		description, err := temporal.ScheduleClient().GetHandle(
			ctx, taskID,
		).Describe(ctx)
		if err != nil {
			t.Fatalf("describe action count: %v", err)
		}
		if description.Info.NumActions == int(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("action count=%d want=%d",
				description.Info.NumActions, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
