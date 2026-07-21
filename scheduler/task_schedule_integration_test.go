//go:build integration

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/workflow"
)

// TestTaskScheduleIntegration_RealDevServerLifecycle proves the A3 contract
// against a real, in-memory Temporal dev server. No worker is started: the
// schedule is deliberately placed months in the future so activation cannot
// dispatch a workflow during this test.
func TestTaskScheduleIntegration_RealDevServerLifecycle(t *testing.T) {
	const namespace = "a3-task-schedule-integration"
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

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	const taskQueue = "a3-task-schedule-integration"
	preparer := New(server.Client(), taskQueue, nil, WithTaskScheduleNamespace(namespace))
	request := integrationTaskScheduleRequest()
	prepared, err := preparer.PrepareTaskSchedule(ctx, request)
	if err != nil {
		t.Fatalf("prepare task schedule: %v", err)
	}
	recovered := integrationRoundTripPreparedTaskSchedule(t, prepared)
	if recovered.Namespace != namespace || recovered.NamespaceID == "" || recovered.RequestDigest == "" {
		t.Fatalf("recovered prepared environment = namespace %q, namespace ID %q, digest %q", recovered.Namespace, recovered.NamespaceID, recovered.RequestDigest)
	}

	taskID, err := TaskIDForOperation(request.TenantID, request.UserID, request.OperationID)
	if err != nil {
		t.Fatalf("derive deterministic task ID: %v", err)
	}
	if recovered.TaskID != taskID {
		t.Fatalf("prepared task ID = %q, want deterministic ID %q", recovered.TaskID, taskID)
	}
	// A fresh Scheduler simulates a new process: no namespace-ID cache or
	// in-process gate survives, and lifecycle recovery receives only JSON.
	scheduler := New(server.Client(), taskQueue, nil, WithTaskScheduleNamespace(namespace))

	created, err := scheduler.EnsurePausedTask(ctx, recovered)
	if err != nil {
		description, describeErr := server.Client().ScheduleClient().GetHandle(ctx, taskID).Describe(ctx)
		if describeErr != nil {
			t.Fatalf("create paused task schedule: %v (diagnostic Describe: %v)", err, describeErr)
		}
		t.Fatalf("create paused task schedule: %v (raw action: %#v)", err, description.Schedule.Action)
	}
	if created.Disposition != TaskScheduleEnsured {
		t.Fatalf("first ensure disposition = %q, want %q", created.Disposition, TaskScheduleEnsured)
	}
	if created.Snapshot.TaskID != taskID || created.Snapshot.State != TaskSchedulePausedVirginExact {
		t.Fatalf("created snapshot = %+v, want task %q in %q", created.Snapshot, taskID, TaskSchedulePausedVirginExact)
	}

	description, err := server.Client().ScheduleClient().GetHandle(ctx, taskID).Describe(ctx)
	if err != nil {
		t.Fatalf("raw Describe after create: %v", err)
	}
	assertIntegrationPausedVirgin(t, description)
	if description.Schedule.Spec == nil {
		t.Fatal("raw Describe returned nil schedule spec")
	}
	if got := len(description.Schedule.Spec.CronExpressions); got != 0 {
		t.Fatalf("server Describe retained %d CronExpressions, want canonical Calendars only", got)
	}
	if got := len(description.Schedule.Spec.Calendars); got != 1 {
		t.Fatalf("server Describe Calendars count = %d, want 1", got)
	}

	paused, err := scheduler.DescribeTask(ctx, recovered)
	if err != nil {
		t.Fatalf("verified Describe of paused virgin task: %v", err)
	}
	if paused.State != TaskSchedulePausedProvisioningExact || paused.NumActions != 0 {
		t.Fatalf("verified paused snapshot = %+v, want paused virgin with zero actions", paused)
	}

	replayedCreate, err := scheduler.EnsurePausedTask(ctx, recovered)
	if err != nil {
		t.Fatalf("replay canonicalized existing schedule: %v", err)
	}
	if replayedCreate.Disposition != TaskScheduleEnsured {
		t.Fatalf("second ensure disposition = %q, want %q", replayedCreate.Disposition, TaskScheduleEnsured)
	}
	if replayedCreate.Snapshot != created.Snapshot {
		t.Fatalf("replayed snapshot = %+v, want original %+v", replayedCreate.Snapshot, created.Snapshot)
	}
	rawExpected, err := scheduler.buildTaskScheduleExpected(ctx, recovered, "collision_probe", true)
	if err != nil {
		t.Fatalf("build raw collision probe: %v", err)
	}
	rawCollision, err := rawExpected.createRequest()
	if err != nil {
		t.Fatalf("build raw CreateSchedule collision request: %v", err)
	}
	rawCollision.RequestId = taskScheduleRequestID("collision_probe", recovered.RequestDigest)
	_, err = server.Client().WorkflowService().CreateSchedule(ctx, rawCollision)
	if _, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); !ok {
		t.Fatalf("raw duplicate CreateSchedule error = %T %v, want WorkflowExecutionAlreadyStarted", err, err)
	}

	conflicting := request
	conflicting.Spec.Cron = integrationCronAtHour(4)
	conflictingPrepared, err := preparer.PrepareTaskSchedule(ctx, conflicting)
	if err != nil {
		t.Fatalf("prepare conflicting definition: %v", err)
	}
	if _, err := scheduler.EnsurePausedTask(ctx, conflictingPrepared); !errors.Is(err, ErrTaskScheduleConflict) {
		t.Fatalf("ensure same task ID with different definition error = %v, want conflict", err)
	}
	stillPaused, err := scheduler.DescribeTask(ctx, recovered)
	if err != nil {
		t.Fatalf("Describe original after rejected conflict: %v", err)
	}
	if stillPaused.State != TaskSchedulePausedProvisioningExact {
		t.Fatalf("state after rejected conflict = %q, want %q", stillPaused.State, TaskSchedulePausedProvisioningExact)
	}

	active, err := scheduler.ActivateTask(ctx, recovered, created.Snapshot)
	if err != nil {
		t.Fatalf("activate exact paused task: %v", err)
	}
	if active.State != TaskScheduleActiveVirginExact || active.NumActions != 0 {
		t.Fatalf("active snapshot = %+v, want active virgin with zero actions", active)
	}
	description, err = server.Client().ScheduleClient().GetHandle(ctx, taskID).Describe(ctx)
	if err != nil {
		t.Fatalf("raw Describe after activate: %v", err)
	}
	if description.Schedule.State == nil || description.Schedule.State.Paused {
		t.Fatalf("raw state after activate = %+v, want active", description.Schedule.State)
	}
	if description.Info.NumActions != 0 || len(description.Info.RunningWorkflows) != 0 {
		t.Fatalf("activation unexpectedly dispatched without a worker: info = %+v", description.Info)
	}

	activeAgain, err := scheduler.ActivateTask(ctx, recovered, created.Snapshot)
	if err != nil {
		t.Fatalf("idempotent activate: %v", err)
	}
	if activeAgain.State != TaskScheduleActiveVirginExact {
		t.Fatalf("second activate state = %q, want %q", activeAgain.State, TaskScheduleActiveVirginExact)
	}

	// Paused is not synonymous with "safe provisioning state". A schedule that
	// was activated and then manually paused can still have zero actions; the
	// versioned lifecycle phase is what prevents a retry from adopting it as new.
	if err := server.Client().ScheduleClient().GetHandle(ctx, taskID).Pause(ctx, client.SchedulePauseOptions{
		// Forge the exact public provisioning marker. The action fingerprint's
		// active phase, not this caller-controlled text, must keep the task unsafe.
		Note: recovered.Creation.Note,
	}); err != nil {
		t.Fatalf("pause activated schedule: %v", err)
	}
	pausedAfterActivation, err := scheduler.DescribeTask(ctx, recovered)
	if err != nil {
		t.Fatalf("Describe manually paused activated schedule: %v", err)
	}
	if pausedAfterActivation.State != TaskSchedulePausedUsedExact || pausedAfterActivation.NumActions != 0 {
		t.Fatalf("manually paused activated snapshot = %+v, want paused-used with zero actions", pausedAfterActivation)
	}
	if _, err := scheduler.EnsurePausedTask(ctx, recovered); !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("ensure manually paused activated schedule error = %v, want unsafe state", err)
	}
	if _, err := scheduler.ActivateTask(ctx, recovered, created.Snapshot); !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("activate manually paused activated schedule error = %v, want unsafe state", err)
	}

	if err := scheduler.DeleteTask(ctx, recovered); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := scheduler.DeleteTask(ctx, recovered); err != nil {
		t.Fatalf("second idempotent delete: %v", err)
	}
	_, err = server.Client().ScheduleClient().GetHandle(ctx, taskID).Describe(ctx)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
		t.Fatalf("raw Describe after two deletes error = %v, want Temporal NotFound", err)
	}

	t.Run("interval anchor round trip", func(t *testing.T) {
		assertIntegrationIntervalAnchorRoundTrip(t, ctx, server.Client(), preparer, scheduler)
	})
	t.Run("out-of-band state replay changes revision", func(t *testing.T) {
		assertIntegrationRevisionRejectsOutOfBandReplay(t, ctx, server.Client(), preparer, scheduler)
	})
	t.Run("concurrent request replay across schedulers", func(t *testing.T) {
		assertIntegrationConcurrentEnsureReplay(t, ctx, server.Client(), preparer, taskQueue, namespace)
	})
}

func assertIntegrationConcurrentEnsureReplay(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	preparer *Scheduler,
	taskQueue string,
	namespace string,
) {
	t.Helper()
	request := integrationTaskScheduleRequest()
	request.OperationID = "integration-operation-a3-concurrent-replay"
	request.NLDescription = "A3 concurrent request replay integration task"
	prepared, err := preparer.PrepareTaskSchedule(ctx, request)
	if err != nil {
		t.Fatalf("prepare concurrent replay task: %v", err)
	}

	type outcome struct {
		result EnsurePausedTaskResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for range 2 {
		scheduler := New(temporalClient, taskQueue, nil, WithTaskScheduleNamespace(namespace))
		go func() {
			<-start
			var result EnsurePausedTaskResult
			var callErr error
			for attempt := 0; attempt < 10; attempt++ {
				result, callErr = scheduler.EnsurePausedTask(ctx, prepared)
				if callErr == nil ||
					(!errors.Is(callErr, ErrTaskScheduleOutcomeUnknown) &&
						!errors.Is(callErr, ErrTaskScheduleTransient)) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			outcomes <- outcome{result: result, err: callErr}
		}()
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	for i, got := range []outcome{first, second} {
		if got.err != nil || got.result.Disposition != TaskScheduleEnsured ||
			got.result.Snapshot.State != TaskSchedulePausedVirginExact {
			t.Fatalf("concurrent outcome %d: result=%+v err=%T %v", i, got.result, got.err, got.err)
		}
	}
	if first.result.Snapshot != second.result.Snapshot {
		t.Fatalf("concurrent receipts differ: first=%+v second=%+v", first.result, second.result)
	}
	cleanup := New(temporalClient, taskQueue, nil, WithTaskScheduleNamespace(namespace))
	if err := cleanup.DeleteTask(ctx, prepared); err != nil {
		t.Fatalf("delete concurrent replay task: %v", err)
	}
}

func assertIntegrationRevisionRejectsOutOfBandReplay(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	preparer *Scheduler,
	scheduler *Scheduler,
) {
	t.Helper()
	request := integrationTaskScheduleRequest()
	request.OperationID = "integration-operation-a3-revision-replay"
	request.NLDescription = "A3 out-of-band revision replay integration task"
	prepared, err := preparer.PrepareTaskSchedule(ctx, request)
	if err != nil {
		t.Fatalf("prepare revision replay task: %v", err)
	}
	ensured, err := scheduler.EnsurePausedTask(ctx, prepared)
	if err != nil {
		t.Fatalf("ensure revision replay task: %v", err)
	}
	handle := temporalClient.ScheduleClient().GetHandle(ctx, prepared.TaskID)
	if err := handle.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "out-of-band active"}); err != nil {
		t.Fatalf("out-of-band Unpause: %v", err)
	}
	if err := handle.Pause(ctx, client.SchedulePauseOptions{Note: prepared.Creation.Note}); err != nil {
		t.Fatalf("out-of-band Pause replay: %v", err)
	}

	replayed, err := scheduler.DescribeTask(ctx, prepared)
	if err != nil {
		t.Fatalf("Describe replayed provisioning state: %v", err)
	}
	if replayed.State != TaskSchedulePausedProvisioningExact {
		t.Fatalf("replayed current state = %+v, want exact provisioning representation", replayed)
	}
	if replayed.Revision == ensured.Snapshot.Revision {
		t.Fatalf("out-of-band patches did not change revision: before=%q after=%q",
			ensured.Snapshot.Revision, replayed.Revision)
	}
	expected, err := scheduler.buildTaskScheduleExpected(ctx, prepared, "request_replay", true)
	if err != nil {
		t.Fatalf("build same-request replay: %v", err)
	}
	replayRequest, err := expected.createRequest()
	if err != nil {
		t.Fatalf("build same-request CreateSchedule replay: %v", err)
	}
	replayResponse, err := temporalClient.WorkflowService().CreateSchedule(ctx, replayRequest)
	if err != nil {
		t.Fatalf("same RequestID CreateSchedule replay: %v", err)
	}
	if got := taskScheduleRevision(replayResponse.GetConflictToken()); got != ensured.Snapshot.Revision {
		t.Fatalf("request replay token=%q, want immutable create token %q", got, ensured.Snapshot.Revision)
	}
	if _, err := scheduler.EnsurePausedTask(ctx, prepared); !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("Ensure after out-of-band replay error = %v, want unsafe state", err)
	}
	if _, err := scheduler.ActivateTask(ctx, prepared, ensured.Snapshot); !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("activate after out-of-band replay error = %v, want unsafe state", err)
	}
	if err := scheduler.DeleteTask(ctx, prepared); err != nil {
		t.Fatalf("delete revision replay task: %v", err)
	}
}

func assertIntegrationIntervalAnchorRoundTrip(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	preparer *Scheduler,
	scheduler *Scheduler,
) {
	t.Helper()

	const (
		every      = 6 * time.Hour
		wantOffset = time.Hour + 17*time.Minute + 23*time.Second
	)
	request := TaskScheduleRequest{
		TenantID:    71,
		UserID:      721,
		OperationID: "integration-operation-a3-interval",
		Spec: ScheduleSpec{
			EverySeconds: int(every / time.Second),
			AnchorAt:     "2026-01-02T09:17:23+08:00",
			TZ:           "UTC",
		},
		Scope:          workflow.PushScope{TopN: 2},
		NLDescription:  "A3 real Temporal anchored interval integration task",
		PreparedDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}

	prepared, err := preparer.PrepareTaskSchedule(ctx, request)
	if err != nil {
		t.Fatalf("prepare anchored interval task schedule: %v", err)
	}
	prepared = integrationRoundTripPreparedTaskSchedule(t, prepared)
	if prepared.Timing.Calendar != nil {
		t.Fatalf("prepared interval timing has calendar: %+v", prepared.Timing.Calendar)
	}
	if got := time.Duration(prepared.Timing.EveryNanos); got != every {
		t.Fatalf("prepared interval every = %v, want %v", got, every)
	}
	if got := time.Duration(prepared.Timing.OffsetNanos); got != wantOffset {
		t.Fatalf("prepared interval offset = %v, want anchor phase %v", got, wantOffset)
	}

	created, err := scheduler.EnsurePausedTask(ctx, prepared)
	if err != nil {
		t.Fatalf("ensure paused anchored interval task: %v", err)
	}
	if created.Disposition != TaskScheduleEnsured || created.Snapshot.State != TaskSchedulePausedVirginExact {
		t.Fatalf("created anchored interval result = %+v, want created paused virgin exact", created)
	}

	description, err := temporalClient.ScheduleClient().GetHandle(ctx, prepared.TaskID).Describe(ctx)
	if err != nil {
		t.Fatalf("raw Describe anchored interval task: %v", err)
	}
	assertIntegrationPausedVirgin(t, description)
	if description.Schedule.Spec == nil {
		t.Fatal("raw Describe anchored interval returned nil schedule spec")
	}
	if got := len(description.Schedule.Spec.Intervals); got != 1 {
		t.Fatalf("server Describe anchored interval count = %d, want 1", got)
	}
	if got := len(description.Schedule.Spec.Calendars); got != 0 {
		t.Fatalf("server Describe anchored interval retained %d calendars, want 0", got)
	}
	if got := len(description.Schedule.Spec.CronExpressions); got != 0 {
		t.Fatalf("server Describe anchored interval retained %d cron expressions, want 0", got)
	}
	serverInterval := description.Schedule.Spec.Intervals[0]
	if got, want := serverInterval.Every, time.Duration(prepared.Timing.EveryNanos); got != want {
		t.Fatalf("server interval every = %v, want prepared %v", got, want)
	}
	if got, want := serverInterval.Offset, time.Duration(prepared.Timing.OffsetNanos); got != want {
		t.Fatalf("server interval offset = %v, want prepared %v", got, want)
	}

	described, err := scheduler.DescribeTask(ctx, prepared)
	if err != nil {
		t.Fatalf("verified Describe anchored interval task: %v", err)
	}
	if described.TaskID != prepared.TaskID || described.State != TaskSchedulePausedProvisioningExact || described.NumActions != 0 {
		t.Fatalf("verified anchored interval snapshot = %+v, want prepared task paused virgin exact", described)
	}

	if err := scheduler.DeleteTask(ctx, prepared); err != nil {
		t.Fatalf("delete anchored interval task: %v", err)
	}
	_, err = temporalClient.ScheduleClient().GetHandle(ctx, prepared.TaskID).Describe(ctx)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
		t.Fatalf("raw Describe deleted anchored interval error = %v, want Temporal NotFound", err)
	}
}

func integrationRoundTripPreparedTaskSchedule(t *testing.T, prepared PreparedTaskSchedule) PreparedTaskSchedule {
	t.Helper()
	checkpoint, err := json.Marshal(prepared)
	if err != nil {
		t.Fatalf("marshal prepared task schedule checkpoint: %v", err)
	}
	var recovered PreparedTaskSchedule
	if err := json.Unmarshal(checkpoint, &recovered); err != nil {
		t.Fatalf("unmarshal prepared task schedule checkpoint: %v", err)
	}
	return recovered
}

func integrationTaskScheduleRequest() TaskScheduleRequest {
	return TaskScheduleRequest{
		TenantID:       71,
		UserID:         721,
		OperationID:    "integration-operation-a3",
		Spec:           ScheduleSpec{Cron: integrationCronAtHour(3), TZ: "UTC"},
		Scope:          workflow.PushScope{SourceIDs: []int64{101, 202}, TopN: 3},
		NLDescription:  "A3 real Temporal integration task",
		PreparedDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}

func integrationCronAtHour(hour int) string {
	// Six months ahead keeps both candidate definitions far from their next
	// action time on every day this test can run, including year boundaries.
	month := time.Now().UTC().AddDate(0, 6, 0).Month()
	return fmt.Sprintf("17 %d 1 %d *", hour, month)
}

func assertIntegrationPausedVirgin(t *testing.T, description *client.ScheduleDescription) {
	t.Helper()
	if description == nil {
		t.Fatal("raw Describe returned nil description")
	}
	if description.Schedule.State == nil || !description.Schedule.State.Paused {
		t.Fatalf("raw state after create = %+v, want paused", description.Schedule.State)
	}
	if description.Info.NumActions != 0 ||
		len(description.Info.RunningWorkflows) != 0 ||
		len(description.Info.RecentActions) != 0 {
		t.Fatalf("new paused schedule is not virgin: info = %+v", description.Info)
	}
}
