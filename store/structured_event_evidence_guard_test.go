package store

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestStructuredEventEvidenceV1HasExactProductionCallPoints(
	t *testing.T,
) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate structured event evidence guard")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	targets := map[string]bool{
		"LoadStructuredEventEvidenceForTaskRunV1": true,
		"GenerateStructuredWithEvidencePolicyV3":  true,
		"PrepareBriefDraftV3":                     true,
		"ReserveObservedEventProvenanceV1":        true,
		"CardGenOutcomeV3":                        true,
	}
	found := make(map[string][]string)
	err := filepath.WalkDir(root, func(
		path string, entry os.DirEntry, err error,
	) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		for _, declaration := range file.Decls {
			function := "<global>"
			node := ast.Node(declaration)
			if declared, ok := declaration.(*ast.FuncDecl); ok {
				function = declared.Name.Name
				node = declared.Body
			}
			ast.Inspect(node, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || !targets[selector.Sel.Name] {
					return true
				}
				found[selector.Sel.Name] = append(
					found[selector.Sel.Name],
					fmt.Sprintf("%s:%s", relative, function))
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range found {
		sort.Strings(found[name])
	}
	want := map[string][]string{
		"LoadStructuredEventEvidenceForTaskRunV1": {
			"workflow/activities.go:loadCardGenEventEvidenceV1",
		},
		"GenerateStructuredWithEvidencePolicyV3": {
			"workflow/activities.go:cardGen",
			"workflow/tool_pipeline_v2.go:CardGenToolCandidatesV2",
		},
		"PrepareBriefDraftV3": {
			"workflow/activities.go:PrepareCanonicalBriefV1",
		},
		"ReserveObservedEventProvenanceV1": {
			"workflow/activities.go:preparePushDeliveries",
		},
		"CardGenOutcomeV3": {
			"cmd/server/main.go:run",
			"workflow/workflow.go:PushPipelineWorkflow",
		},
	}
	if !reflect.DeepEqual(found, want) {
		t.Fatalf("P2-B1 production call points=%v want=%v", found, want)
	}
}
