package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestTaskRunSnapshotCutoverProductionEntryIsRuntimeAdminOnly(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve cutover entry guard")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	wantCalls := map[string]string{
		"ControlTaskRunSnapshotCutover":   "cmd/runtimeadmin/main.go",
		"GetTaskRunSnapshotCutoverStatus": "cmd/runtimeadmin/main.go",
	}
	got := make(map[string][]string)
	err := filepath.WalkDir(root,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == ".git" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" ||
				strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			parsed, err := parser.ParseFile(
				token.NewFileSet(), path, raw, 0)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, guarded := wantCalls[selector.Sel.Name]; guarded {
					got[selector.Sel.Name] = append(
						got[selector.Sel.Name], relative)
				}
				return true
			})
			if strings.Contains(string(raw),
				"SET LOCAL ROLE vane_snapshot_cutover_operator") &&
				relative != "store/task_run_snapshot_cutover_control.go" {
				t.Errorf("operator role entry escaped Store controller: %s",
					relative)
			}
			if strings.Contains(string(raw),
				"FROM task_run_snapshot_v2_cutover_control(") &&
				relative != "store/task_run_snapshot_cutover_control.go" {
				t.Errorf("raw cutover primitive escaped Store controller: %s",
					relative)
			}
			if strings.Contains(string(raw),
				"FROM task_run_snapshot_v2_rebase_definition_edit(") &&
				relative != "store/task_definition_edit_cutover.go" {
				t.Errorf(
					"definition-edit rebase primitive escaped Store controller: %s",
					relative,
				)
			}
			for _, forbidden := range []string{
				"INSERT INTO task_run_snapshot_v2_cutover_events",
				"SET run_snapshot_cutover_event_id",
			} {
				if strings.Contains(string(raw), forbidden) {
					t.Errorf("raw cutover mutation escaped migration primitive: %s contains %q",
						relative, forbidden)
				}
			}
			if strings.Contains(string(raw), "V2CutoverEventID:") &&
				relative != "store/task_run_snapshots.go" {
				t.Errorf("cutover marker writer escaped derived admission: %s",
					relative)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for call, want := range wantCalls {
		if len(got[call]) != 1 || got[call][0] != want {
			t.Errorf("%s production callers = %v, want [%s]",
				call, got[call], want)
		}
	}
}
