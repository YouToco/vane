package scheduler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetiredSchedulerActionSurfaceIsAbsent(t *testing.T) {
	retired := map[string]struct{}{
		"CreatePush": {}, "UpdatePush": {},
		"TriggerScheduleNow": {}, "PausePush": {}, "ResumePush": {},
		"DeletePush": {}, "triggerScheduleNowLegacy": {},
		"changePushPausedLegacy": {}, "deletePushLegacy": {},
		"legacyScheduleCommandKey": {},
		"checkActiveLimit":         {},
	}
	required := map[string]bool{
		"TriggerScheduleNowIdempotent": false,
		"PausePushIdempotent":          false,
		"ResumePushIdempotent":         false,
		"DeletePushIdempotent":         false,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
			strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Clean(entry.Name())
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if _, forbidden := retired[function.Name.Name]; forbidden {
				t.Errorf("retired scheduler entry %s remains in %s", function.Name.Name, path)
			}
			if _, ok := required[function.Name.Name]; ok {
				required[function.Name.Name] = true
			}
		}
	}
	for name, found := range required {
		if !found {
			t.Errorf("current idempotent scheduler entry %s is missing", name)
		}
	}
}
