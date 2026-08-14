//go:build integration

package agentfirstaudit

import (
	"context"
	"os"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	vaneworkflow "github.com/YouToco/vane/server/workflow"
)

func TestRetentionClockEvidenceRealTemporalHistory(t *testing.T) {
	const (
		namespace  = "agent-first-retention-audit"
		taskQueue  = "agent-first-retention-audit"
		workflowID = "agent-first-retention-clock-integration"
		revision   = "0123456789abcdef0123456789abcdef01234567"
	)
	temporalCLI := os.Getenv("VANE_TEMPORAL_CLI_PATH")
	if temporalCLI == "" {
		t.Fatal("VANE_TEMPORAL_CLI_PATH is required for the integration Gate")
	}
	startContext, cancelStart := context.WithTimeout(t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(startContext, testsuite.DevServerOptions{
		ExistingPath:  temporalCLI,
		ClientOptions: &client.Options{Namespace: namespace}, LogLevel: "error",
	})
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		server.Client().Close()
		if err := server.Stop(); err != nil {
			t.Errorf("stop Temporal dev server: %v", err)
		}
	})

	buildID := "vane/" + revision
	workerInstance := worker.New(server.Client(), taskQueue, worker.Options{
		BuildID: buildID, WorkerStopTimeout: 10 * time.Second,
	})
	workerInstance.RegisterWorkflow(vaneworkflow.AgentFirstRetentionClockWorkflowV1)
	if err := workerInstance.Start(); err != nil {
		t.Fatalf("start Temporal worker: %v", err)
	}
	t.Cleanup(workerInstance.Stop)

	nonce := "integration-nonce-1"
	run, err := server.Client().ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue,
	}, vaneworkflow.AgentFirstRetentionClockWorkflowV1,
		vaneworkflow.AgentFirstRetentionClockRequestV1{
			Nonce: nonce, SourceRevision: revision,
		})
	if err != nil {
		t.Fatalf("execute retention clock: %v", err)
	}
	var result vaneworkflow.AgentFirstRetentionClockResultV1
	if err := run.Get(t.Context(), &result); err != nil {
		t.Fatalf("await retention clock: %v", err)
	}
	evidence, err := ReadRetentionClockEvidence(t.Context(),
		server.Client().WorkflowService(), RetentionClockExpectation{
			Namespace: namespace, WorkflowID: workflowID, RunID: run.GetRunID(),
			TaskQueue: taskQueue, Nonce: nonce, SourceRevision: revision,
			WorkerBuildID: buildID,
		})
	if err != nil {
		t.Fatalf("validate real retention clock history: %v", err)
	}
	if evidence.EventCount != 5 || evidence.WorkerBuildID != buildID ||
		!evidence.ObservedAtUTC.Equal(mustParseIntegrationTime(t, result.ObservedAtUTC)) {
		t.Fatalf("evidence=%+v result=%+v", evidence, result)
	}
}

func mustParseIntegrationTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
