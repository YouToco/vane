package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPushEffectRecoveryStoreHasZeroProductionCallPoints(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == "vendor" || name == "third_party" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && pushEffectRecoverySelectorViolation(
				selector.Sel.Name, path) {
				position := fset.Position(selector.Pos())
				t.Errorf("push effect recovery API is wired in PR-B: %s:%d",
					position.Filename, position.Line)
			}
			return true
		})
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestPushEffectRecoveryGuardRejectsLegacyCoordinatorMutation(t *testing.T) {
	t.Parallel()

	source := `package pushrecovery
func mutated(s interface{ ClaimPushEffectReconciliation(); RecordPushEffectSent() }) {
	s.ClaimPushEffectReconciliation()
	s.RecordPushEffectSent()
}`
	file, err := parser.ParseFile(
		token.NewFileSet(), "pushrecovery/coordinator.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	violations := 0
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok &&
			pushEffectRecoverySelectorViolation(
				selector.Sel.Name, "pushrecovery/coordinator.go") {
			violations++
		}
		return true
	})
	if violations != 2 {
		t.Fatalf("legacy coordinator mutation violations=%d, want 2", violations)
	}
}

func pushEffectRecoverySelectorViolation(method, path string) bool {
	coordinator := strings.HasSuffix(
		filepath.ToSlash(path), "/pushrecovery/coordinator.go")
	runner := strings.HasSuffix(
		filepath.ToSlash(path), "/pushrecovery/runner.go")
	switch method {
	case "TakeOverStalePushEffect",
		"ClaimAuthorizedPushEffect",
		"ClaimAuthorizedPushEffectReconciliation",
		"DeferOrBlockPushEffectReconciliation",
		"BlockConflictingPushEffectHistory",
		"BlockExhaustedPushEffectAttempts",
		"BlockExpiredUnclaimedPushEffect":
		return !coordinator
	case "ReadPushEffectRecoveryCutoff",
		"ListRecoverablePushEffects":
		return !runner
	case
		"ClaimPushEffectReconciliation",
		"RecordPushEffectSent",
		"BlockPushEffect":
		return true
	default:
		return false
	}
}
