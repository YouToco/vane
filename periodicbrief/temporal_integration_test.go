//go:build integration

package periodicbrief

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type temporalPeriodicActivityProbe struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *temporalPeriodicActivityProbe) Synthesize(
	context.Context,
	SynthesizeInputV1,
) (types.PeriodicBriefReportV1, error) {
	p.once.Do(func() { close(p.started) })
	<-p.release
	return types.PeriodicBriefReportV1{}, nil
}

func TestPeriodicWorkflowExternalTerminationReplaysAndRecoveryConverges(
	t *testing.T,
) {
	namespace := fmt.Sprintf(
		"vane-periodic-integration-%d", time.Now().UnixNano())
	startCtx, cancelStart := context.WithTimeout(
		t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(
		startCtx,
		testsuite.DevServerOptions{
			ClientOptions: &client.Options{Namespace: namespace},
			LogLevel:      "error",
		},
	)
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		server.Client().Close()
		if stopErr := server.Stop(); stopErr != nil {
			t.Errorf("stop Temporal dev server: %v", stopErr)
		}
	})

	probe := &temporalPeriodicActivityProbe{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	taskQueue := "vane-periodic-integration"
	temporalWorker := worker.New(server.Client(), taskQueue,
		worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(
		WorkflowV1,
		sdkworkflow.RegisterOptions{Name: WorkflowNameV1},
	)
	temporalWorker.RegisterActivityWithOptions(
		probe.Synthesize,
		activity.RegisterOptions{Name: "SynthesizePeriodicBriefV1"},
	)
	if err := temporalWorker.Start(); err != nil {
		t.Fatalf("start Temporal worker: %v", err)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(probe.release) })
		temporalWorker.Stop()
	})

	workflowID := fmt.Sprintf(
		"periodic-external-terminate-%d", time.Now().UnixNano())
	run, err := server.Client().ExecuteWorkflow(
		t.Context(),
		client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: taskQueue,
		},
		WorkflowNameV1,
		WorkflowInputV1{IntentID: 9, TenantID: 4, UserID: 5},
	)
	if err != nil {
		t.Fatalf("start periodic workflow: %v", err)
	}
	select {
	case <-probe.started:
	case <-time.After(10 * time.Second):
		t.Fatal("periodic synthesis activity did not start")
	}
	if err := server.Client().TerminateWorkflow(
		t.Context(), run.GetID(), run.GetRunID(),
		"P2-D integration external termination",
	); err != nil {
		t.Fatalf("terminate periodic workflow: %v", err)
	}
	if err := run.Get(t.Context(), nil); err == nil {
		t.Fatal("externally terminated workflow returned success")
	}
	description, err := server.Client().DescribeWorkflowExecution(
		t.Context(), run.GetID(), run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	if got := description.GetWorkflowExecutionInfo().GetStatus(); got != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("periodic workflow status=%s", got)
	}

	history := &historypb.History{}
	iterator := server.Client().GetWorkflowHistory(
		t.Context(), run.GetID(), run.GetRunID(), true,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, historyErr := iterator.Next()
		if historyErr != nil {
			t.Fatalf("read terminated history: %v", historyErr)
		}
		history.Events = append(history.Events, event)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		WorkflowV1,
		sdkworkflow.RegisterOptions{Name: WorkflowNameV1},
	)
	if err := replayer.ReplayWorkflowHistory(
		sdklog.NewStructuredLogger(slog.New(
			slog.NewTextHandler(io.Discard, nil))),
		history,
	); err != nil {
		t.Fatalf("replay terminated periodic history: %v", err)
	}

	brief := periodicRecoveryBriefFixture(t)
	fakeStore := &periodicRecoveryStoreFake{
		loaded: store.PeriodicBriefIntentInputsV1{
			Intent: store.PeriodicBriefIntentV1{
				ID: 9, TenantID: 4, UserID: 5, TaskID: "task-v1",
				Cadence: "weekly", Timezone: "UTC",
				PeriodStart: brief.GeneratedAt.AddDate(0, 0, -1),
				PeriodEnd:   brief.GeneratedAt.AddDate(0, 0, 1),
				InputDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				RunOutcomeIDs: []int64{8},
				OutcomeDigest: "dddddddddddddddddddddddddddddddd" +
					"dddddddddddddddddddddddddddddddd",
				SourceCoverage: types.RunCompletenessComplete,
				Processing:     types.RunCompletenessComplete,
			},
			Briefs: []types.BriefV1{brief},
		},
	}
	runner, err := NewRecoveryRunner(
		fakeStore, server.Client(),
		&periodicRecoverySenderFake{},
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.recoverOne(
		t.Context(),
		store.PeriodicSynthesisRecoveryCandidateV1{
			Kind:          "prepared",
			IntentID:      9,
			TenantID:      4,
			UserID:        5,
			WorkflowID:    run.GetID(),
			TemporalRunID: run.GetRunID(),
		},
	); err != nil {
		t.Fatalf("recover externally terminated report: %v", err)
	}
	if fakeStore.recovered == nil ||
		fakeStore.recovered.GenerationMode !=
			types.ExecutiveGenerationFallback ||
		fakeStore.recovered.Processing !=
			types.RunCompletenessPartial {
		t.Fatalf("terminated recovery draft=%+v", fakeStore.recovered)
	}
	releaseOnce.Do(func() { close(probe.release) })
}
