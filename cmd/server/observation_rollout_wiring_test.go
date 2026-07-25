package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestObservationRolloutWiringLogsEffectiveTaskIDs(t *testing.T) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate observation rollout wiring test")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(self), "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, required := range []string{
		"workflow.WithObservationRuntime(",
		"cfg.Pipeline.ObservationShadowCanaryScheduleID",
		"cfg.Pipeline.ObservationAuthorityCanaryScheduleID",
		`"observation rollout configured"`,
		`"shadow_task_id"`,
		`"authority_task_id"`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("main.go missing effective observation rollout evidence %q",
				required)
		}
	}
}
