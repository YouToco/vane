//go:build integration

package temporalintegration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func canonicalServerRoundTripWorkflow(ctx workflow.Context, input string) (string, error) {
	return "round-trip:" + input, nil
}

func TestCanonicalTemporalServerPostgreSQLRoundTrip(t *testing.T) {
	address := os.Getenv("VANE_TEMPORAL_ADDRESS")
	if address == "" {
		t.Fatal("VANE_TEMPORAL_ADDRESS is required for the canonical Temporal integration gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	taskQueue := fmt.Sprintf("vane-full-probe-%d", time.Now().UnixNano())
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(
		canonicalServerRoundTripWorkflow,
		workflow.RegisterOptions{Name: "vane.full-gate-probe/v1"},
	)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{ID: taskQueue, TaskQueue: taskQueue},
		"vane.full-gate-probe/v1",
		"ok",
	)
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := run.Get(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result != "round-trip:ok" {
		t.Fatalf("unexpected workflow result: %q", result)
	}
}
