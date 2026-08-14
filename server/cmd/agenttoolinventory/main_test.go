package main

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/server/tikhubcatalog"
)

func TestRenderContainsEveryRegisteredToolExactlyOnce(t *testing.T) {
	payload := string(render())
	for _, tool := range staticTools {
		if got := strings.Count(payload, "| `"+tool.name+"` |"); got != 1 {
			t.Fatalf("static tool %q rows=%d, want 1", tool.name, got)
		}
	}
	for _, entry := range tikhubcatalog.Entries() {
		if got := strings.Count(payload, "| `"+entry.Name+"` |"); got != 1 {
			t.Fatalf("dynamic tool %q rows=%d, want 1", entry.Name, got)
		}
	}
}
