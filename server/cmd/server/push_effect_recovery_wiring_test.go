package main

import (
	"os"
	"strings"
	"testing"
)

func TestPushEffectRecoveryStartupAndDrainOrdering(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	workerStart := strings.Index(source, "w.Start()")
	if workerStart < 0 {
		t.Fatal("worker ingress is unavailable")
	}
	for _, gate := range []string{
		"manager.PrepareOutbound(outboundCtx)",
		"pushRecoveryRunner.RunStartup(ctx)",
		"authorityPushRecoveryRunner.RunStartup(ctx)",
	} {
		index := strings.Index(source, gate)
		if index < 0 || index >= workerStart {
			t.Fatalf("startup gate %q index=%d worker=%d", gate, index, workerStart)
		}
	}
	for _, lifecycle := range []string{
		"pushRecoveryRunner.Run(pushRecoveryCtx)",
		"authorityPushRecoveryRunner.Run(pushRecoveryCtx)",
		"stopPushRecovery()",
	} {
		if !strings.Contains(source, lifecycle) {
			t.Fatalf("recovery lifecycle is missing %q", lifecycle)
		}
	}
	if strings.Count(source, "stopPushRecovery()") < 3 {
		t.Fatal("startup/A2A/final shutdown drains are incomplete")
	}
}
