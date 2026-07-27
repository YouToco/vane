package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestActionContinuationWiringStartsBeforeIngressAndDrainsBeforeStoreClose(
	t *testing.T,
) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate action continuation wiring test")
	}
	mainPath := filepath.Join(filepath.Dir(testFile), "main.go")
	raw, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for fragment, want := range map[string]int{
		"agentcontinuation.NewActionController(st)":         1,
		"agentcontinuation.NewActionProposalController(st)": 1,
		"ActionContinuation: actionController":              1,
		"ActionProposal:     actionProposalController":      1,
		"agentcontinuation.NewActionDispatcher(":            1,
		"actionDispatcher.Start(ctx)":                       1,
		"actionDispatcher.Stop()":                           1,
		"actionDispatcher.Wait(actionDrainCtx)":             1,
	} {
		if got := strings.Count(source, fragment); got != want {
			t.Fatalf("%q count=%d want=%d", fragment, got, want)
		}
	}

	startAt := strings.Index(source, "actionDispatcher.Start(ctx)")
	workerAt := strings.Index(source, "if err := w.Start(); err != nil")
	managerAt := strings.Index(source, "manager.Start(ctx)")
	if startAt < 0 || workerAt < 0 || managerAt < 0 ||
		!(startAt < workerAt && workerAt < managerAt) {
		t.Fatalf(
			"action dispatcher startup order invalid: action=%d worker=%d manager=%d",
			startAt, workerAt, managerAt)
	}

	shutdownAt := strings.LastIndex(
		source, "if err := stopActionDispatcher(); err != nil")
	sessionDrainAt := strings.LastIndex(
		source, "if err := drainAgentSessions(); err != nil")
	storeCloseAt := strings.LastIndex(source, "st.Close()")
	if shutdownAt < 0 || sessionDrainAt < 0 || storeCloseAt < 0 ||
		!(shutdownAt < sessionDrainAt && sessionDrainAt < storeCloseAt) {
		t.Fatalf(
			"action dispatcher shutdown order invalid: action=%d session=%d store=%d",
			shutdownAt, sessionDrainAt, storeCloseAt)
	}
}
