package agent

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestContextShadowWiringGuardsEveryLoopChatCall(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	loopPath := filepath.Join(filepath.Dir(thisFile), "loop.go")
	raw, err := os.ReadFile(loopPath)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	const call = "l.chatFn(ctx,"
	var calls int
	searchFrom := 0
	for {
		offset := strings.Index(source[searchFrom:], call)
		if offset < 0 {
			break
		}
		callAt := searchFrom + offset
		prefixStart := max(0, callAt-240)
		prefix := source[prefixStart:callAt]
		if !strings.Contains(prefix, "l.shadowAgentContext(ctx,") {
			t.Fatalf("chatFn call at byte %d bypasses context shadow", callAt)
		}
		calls++
		searchFrom = callAt + len(call)
	}
	if calls != 2 {
		t.Fatalf("Loop chatFn call count=%d, want exactly main + final", calls)
	}
}
