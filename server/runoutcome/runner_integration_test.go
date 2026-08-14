//go:build integration

package runoutcome

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestRealTemporalExternalTerminationConvergesThroughRecovery(t *testing.T) {
	const namespace = "p1b-run-outcome-integration"
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
		if err := server.Stop(); err != nil {
			t.Errorf("stop Temporal dev server: %v", err)
		}
	})

	const workflowID = "p1b-external-termination"
	run, err := server.Client().ExecuteWorkflow(
		t.Context(),
		client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: "p1b-no-worker-needed",
		},
		"unregistered-p1b-workflow",
	)
	if err != nil {
		t.Fatal(err)
	}
	runID := run.GetRunID()
	if runID == "" {
		t.Fatal("Temporal did not assign an exact RunID")
	}
	if err := server.Client().TerminateWorkflow(
		t.Context(), workflowID, runID, "integration termination",
	); err != nil {
		t.Fatal(err)
	}

	candidate := recoveryCandidate(1, workflowID)
	candidate.Identity.TemporalRunID = runID
	st := &recoveryStoreFake{
		candidates: []store.RunOutcomeRecoveryCandidateV1{candidate},
	}
	runner, err := NewRunner(
		st, TemporalInspector{Client: server.Client()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.claims) != 1 ||
		st.claims[0].Result != types.RunResultInterrupted ||
		st.claims[0].FailureCode != "workflow_terminated" {
		t.Fatalf("termination recovery claims = %+v", st.claims)
	}
}
