package scheduler

import (
	"testing"

	"github.com/YouToco/vane/workflow"
)

func TestCompiledRuntimeRollout_RuntimeVersionFor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		enabled  bool
		canaryID string
		allowAll bool
		taskID   string
		want     string
	}{
		{name: "disabled", taskID: "task-a"},
		{name: "single canary match", enabled: true, canaryID: "task-a", taskID: "task-a", want: workflow.CompiledRuntimeSnapshotV1},
		{name: "single canary mismatch", enabled: true, canaryID: "task-a", taskID: "task-b"},
		{name: "allow all", enabled: true, allowAll: true, taskID: "task-b", want: workflow.CompiledRuntimeSnapshotV1},
		{name: "empty task always disabled", enabled: true, allowAll: true},
		{name: "whitespace task always disabled", enabled: true, allowAll: true, taskID: "  "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := compiledRuntimeRollout{enabled: tt.enabled, canaryID: tt.canaryID, allowAll: tt.allowAll}
			if got := r.runtimeVersionFor(tt.taskID); got != tt.want {
				t.Fatalf("runtimeVersionFor(%q) = %q, want %q", tt.taskID, got, tt.want)
			}
		})
	}
}

func TestWithCompiledRuntimeRollout_InvalidCombinationsFailClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		enabled  bool
		canaryID string
		allowAll bool
	}{
		{name: "disabled with canary", canaryID: "task-a"},
		{name: "disabled allow all", allowAll: true},
		{name: "enabled without target", enabled: true},
		{name: "enabled with both targets", enabled: true, canaryID: "task-a", allowAll: true},
		{name: "enabled whitespace canary", enabled: true, canaryID: "  "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{}
			WithCompiledRuntimeRollout(tt.enabled, tt.canaryID, tt.allowAll)(s)
			if got := s.compiledRuntime.runtimeVersionFor("task-a"); got != "" {
				t.Fatalf("invalid rollout returned runtime version %q", got)
			}
		})
	}
}

func TestWithCompiledRuntimeRollout_TrimsSingleCanary(t *testing.T) {
	t.Parallel()
	s := &Scheduler{}
	WithCompiledRuntimeRollout(true, "  task-a  ", false)(s)
	if got := s.compiledRuntime.runtimeVersionFor("task-a"); got != workflow.CompiledRuntimeSnapshotV1 {
		t.Fatalf("trimmed canary returned runtime version %q", got)
	}
}
