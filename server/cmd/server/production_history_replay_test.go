//go:build productionreplay

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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

const maxProductionHistoryBytes = 128 << 20

func bindProductionHistory(path string, expectedWorkflow string) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	info, err := os.Lstat(path)
	if err != nil {
		return zero, fmt.Errorf("history is missing: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 ||
		info.Size() > maxProductionHistoryBytes {
		return zero, fmt.Errorf("history is unsafe, empty, or oversized")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return zero, err
	}
	var envelope struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.Events) == 0 {
		return zero, fmt.Errorf("history envelope is invalid: %w", err)
	}
	var first struct {
		EventType string `json:"eventType"`
		Started   struct {
			WorkflowType struct {
				Name string `json:"name"`
			} `json:"workflowType"`
		} `json:"workflowExecutionStartedEventAttributes"`
	}
	if err := json.Unmarshal(envelope.Events[0], &first); err != nil {
		return zero, fmt.Errorf("history first event is invalid: %w", err)
	}
	if first.EventType != "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED" ||
		first.Started.WorkflowType.Name != expectedWorkflow {
		return zero, fmt.Errorf(
			"history workflow type %q differs from contract %q",
			first.Started.WorkflowType.Name, expectedWorkflow,
		)
	}
	return sha256.Sum256(raw), nil
}

func TestProductionHistoryTypeBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	raw := `{"events":[{"eventType":"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",` +
		`"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"WorkflowA"}}}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := bindProductionHistory(path, "WorkflowA"); err != nil {
		t.Fatalf("exact workflow binding rejected: %v", err)
	}
	if _, err := bindProductionHistory(path, "WorkflowB"); err == nil {
		t.Fatal("mismatched workflow binding was accepted")
	}
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
	if contract.Schema != "vane.temporal-production-history-replay/v1" || len(contract.Histories) != 2 {
		t.Fatalf("unexpected replay contract: schema=%q histories=%d", contract.Schema, len(contract.Histories))
	}

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(vaneworkflow.ResearchShadowWorkflowV3)
	replayer.RegisterWorkflow(vaneworkflow.ResearchScheduledWorkflowV3)

	seen := map[string]bool{}
	seenDigests := map[[sha256.Size]byte]string{}
	for _, history := range contract.Histories {
		if history.Workflow == "" || history.File == "" ||
			filepath.Base(history.File) != history.File ||
			(history.Status != "production" && history.Status != "retained") ||
			seen[history.Workflow] {
			t.Fatalf("invalid or duplicate history contract entry: %+v", history)
		}
		seen[history.Workflow] = true
		path := filepath.Join(directory, history.File)
		digest, err := bindProductionHistory(path, history.Workflow)
		if err != nil {
			t.Fatalf("required %s history is not bound to %s: %s: %v",
				history.Status, history.Workflow, path, err)
		}
		if previous, duplicate := seenDigests[digest]; duplicate {
			t.Fatalf("history for %s duplicates history for %s", history.Workflow, previous)
		}
		seenDigests[digest] = history.Workflow
		if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
			t.Fatalf("replay %s from %s: %v", history.Workflow, path, err)
		}
	}
	for _, required := range []string{
		vaneworkflow.ResearchScheduledWorkflowV3Name,
		"ResearchShadowWorkflowV3",
	} {
		if !seen[required] {
			t.Fatalf("current production history contract omits %s", required)
		}
	}
}
