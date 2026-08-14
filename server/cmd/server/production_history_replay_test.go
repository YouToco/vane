//go:build productionreplay

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/YouToco/vane/server/periodicbrief"
	vaneworkflow "github.com/YouToco/vane/server/workflow"
	"go.temporal.io/sdk/worker"
)

type productionHistoryReplayContract struct {
	Schema    string `json:"schema"`
	Histories []struct {
		Workflow string `json:"workflow"`
		File     string `json:"file"`
		Status   string `json:"status"`
	} `json:"histories"`
}

// TestProductionHistoryReplay is deliberately behind an explicit build tag.
// The full gate obtains histories through the read-only broker into a private
// temporary directory. Production payloads must never be checked into Git.
func TestProductionHistoryReplay(t *testing.T) {
	directory := os.Getenv("VANE_TEMPORAL_HISTORY_DIR")
	if directory == "" || !filepath.IsAbs(directory) {
		t.Fatal("VANE_TEMPORAL_HISTORY_DIR must name the broker-provided absolute directory")
	}
	contractPath := filepath.Join("..", "..", "..", "contracts", "temporal", "production-history-replay.json")
	raw, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatal(err)
	}
	var contract productionHistoryReplayContract
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Schema != "vane.temporal-production-history-replay/v1" || len(contract.Histories) != 5 {
		t.Fatalf("unexpected replay contract: schema=%q histories=%d", contract.Schema, len(contract.Histories))
	}

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(vaneworkflow.PushPipelineWorkflow)
	replayer.RegisterWorkflow(vaneworkflow.ResearchShadowWorkflowV3)
	replayer.RegisterWorkflow(vaneworkflow.ResearchScheduledWorkflowV3)
	replayer.RegisterWorkflow(vaneworkflow.AgentFirstRetentionClockWorkflowV1)
	replayer.RegisterWorkflow(periodicbrief.WorkflowV1)

	seen := map[string]bool{}
	for _, history := range contract.Histories {
		if history.Workflow == "" || history.File == "" || seen[history.Workflow] {
			t.Fatalf("invalid or duplicate history contract entry: %+v", history)
		}
		seen[history.Workflow] = true
		path := filepath.Join(directory, history.File)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("required %s history is not a regular file: %s: %v", history.Status, path, err)
		}
		if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
			t.Fatalf("replay %s from %s: %v", history.Workflow, path, err)
		}
	}
}
