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

// P2-D keeps legacy direct Freeze/Load dark while adding only its explicit
// synthesis, artifact and recovery seams.
func TestCanonicalBriefP1EHasOnlyScopedProductionCallPoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate guard source")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	protected := map[string]bool{
		"FreezeBriefV1":                true,
		"LoadBriefV1":                  true,
		"renderCanonicalBriefShadowV1": true,
	}
	scoped := map[string]map[string]bool{
		"CreatePendingRunOutcomeV1": {
			"workflow/activities.go": true,
		},
		"FinalizeRunOutcomeClaimV1": {
			"workflow/activities.go": true,
		},
		"LoadPreparedBriefDraftV1": {
			"workflow/activities.go":           true,
			"executivebriefrecovery/runner.go": true,
		},
		"PrepareBriefDraftV1": {
			"workflow/activities.go": true,
		},
		"PrepareBriefDraftV2": {
			"workflow/activities.go": true,
		},
		"PrepareBriefDraftV3": {
			"workflow/activities.go": true,
		},
		"LoadStructuredEventEvidenceForTaskRunV1": {
			"workflow/activities.go": true,
		},
		"GenerateStructuredWithEvidencePolicyV3": {
			"workflow/activities.go": true,
		},
		"LoadSealedEmptyBriefBatchV1": {
			"workflow/activities.go": true,
		},
		"SealEmptyBriefBatchV1": {
			"workflow/activities.go": true,
		},
		"ListStaleRunOutcomeCandidatesV1": {
			"runoutcome/runner.go": true,
		},
		"FinalizeRecoveredRunOutcomeClaimV1": {
			"runoutcome/runner.go": true,
		},
		"BeginRunOutcomeV1": {
			"workflow/workflow.go": true,
			"cmd/server/main.go":   true,
		},
		"FinalizeRunOutcomeV1": {
			"workflow/workflow.go": true,
			"cmd/server/main.go":   true,
		},
		"PrepareCanonicalBriefV1": {
			"workflow/workflow.go": true,
			"cmd/server/main.go":   true,
		},
		"SynthesizeExecutiveBriefV1": {
			"workflow/workflow.go": true,
			"cmd/server/main.go":   true,
		},
		"FreezeExecutiveBriefV1": {
			"workflow/workflow.go": true,
			"cmd/server/main.go":   true,
		},
		"ListExecutiveSynthesisRecoveryCandidatesV1": {
			"executivebriefrecovery/runner.go": true,
		},
		"LoadExecutiveSynthesisReceiptV1": {
			"executivebriefrecovery/runner.go": true,
		},
		"RecoverExecutiveSynthesisFallbackV1": {
			"executivebriefrecovery/runner.go": true,
		},
		"FreezeExecutiveBriefArtifactV1": {
			"workflow/executive_brief_activities.go": true,
			"executivebriefrecovery/runner.go":       true,
		},
		"CardGenOutcomeV3": {
			"workflow/workflow.go": true,
			"cmd/server/main.go":   true,
		},
		"ListTaskBriefsV1": {
			"api/briefs.go": true,
		},
		"LoadCanonicalBriefForFeedbackV1": {
			"feedback/service.go": true,
		},
	}
	var calls []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
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
		for _, name := range canonicalBriefProtectedSelectors(file, protected) {
			relative, relErr := filepath.Rel(root, path)
			if relErr != nil {
				relative = path
			}
			relative = filepath.ToSlash(relative)
			calls = append(calls, relative+":"+name)
		}
		for name, allowedFiles := range scoped {
			found := canonicalBriefProtectedSelectors(
				file, map[string]bool{name: true})
			if len(found) == 0 {
				continue
			}
			relative, relErr := filepath.Rel(root, path)
			if relErr != nil {
				relative = path
			}
			relative = filepath.ToSlash(relative)
			if !allowedFiles[relative] {
				for range found {
					calls = append(calls, relative+":"+name)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Fatalf("P1-E production scope escaped: %v", calls)
	}
}

func TestCanonicalBriefP1AGuardDetectsMethodValueAliases(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "alias.go", `package p
var freezeDark = (*Store).FreezeBriefV1
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := canonicalBriefProtectedSelectors(
		file, map[string]bool{"FreezeBriefV1": true})
	if len(found) != 1 || found[0] != "FreezeBriefV1" {
		t.Fatalf("method-value alias escaped guard: %v", found)
	}
}

func canonicalBriefProtectedSelectors(
	file *ast.File, protected map[string]bool,
) []string {
	var found []string
	// Inspect every selector reference, not only direct CallExpr functions:
	// method values and method expressions can otherwise alias a dark method.
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok && protected[selector.Sel.Name] {
			found = append(found, selector.Sel.Name)
		}
		return true
	})
	return found
}
