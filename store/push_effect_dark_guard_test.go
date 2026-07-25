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

func TestPushEffectStoreCallPointsStayInFencedCoordinator(t *testing.T) {
	methods := map[string]bool{
		"ClaimPushBatchDeliveryAuthority":          true,
		"PushEffectBatchStarted":                   true,
		"CreatePushEffect":                         true,
		"LoadPushEffect":                           true,
		"ListRecoverablePushEffectTenantIDs":       true,
		"ListRecoverablePushEffects":               true,
		"ClaimPushEffect":                          true,
		"ClaimPushEffectReconciliation":            true,
		"TakeOverStalePushEffect":                  true,
		"RecordPushEffectDefiniteFailure":          true,
		"RecordPushEffectAmbiguous":                true,
		"RecordPushEffectSent":                     true,
		"RecordPushEffectSentWithDeliveries":       true,
		"AuthorizePushEffectRunSideEffect":         true,
		"ClaimAuthorizedPushEffect":                true,
		"ClaimAuthorizedPushEffectReconciliation":  true,
		"DeferPushEffectReconciliation":            true,
		"DeferPushEffectReconciliationUntilExpiry": true,
		"DeferOrBlockPushEffectReconciliation":     true,
		"BlockExpiredPushEffect":                   true,
		"BlockConflictingPushEffectHistory":        true,
		"BlockPushEffect":                          true,
	}
	expected := map[string]int{
		"ClaimPushBatchDeliveryAuthority":         1,
		"PushEffectBatchStarted":                  1,
		"CreatePushEffect":                        1,
		"LoadPushEffect":                          1,
		"ListRecoverablePushEffectTenantIDs":      2,
		"ListRecoverablePushEffects":              1,
		"ClaimPushEffect":                         1,
		"ClaimPushEffectReconciliation":           1,
		"TakeOverStalePushEffect":                 1,
		"RecordPushEffectDefiniteFailure":         2,
		"RecordPushEffectAmbiguous":               2,
		"RecordPushEffectSentWithDeliveries":      4,
		"AuthorizePushEffectRunSideEffect":        1,
		"ClaimAuthorizedPushEffect":               1,
		"ClaimAuthorizedPushEffectReconciliation": 1,
		"DeferOrBlockPushEffectReconciliation":    1,
		"BlockConflictingPushEffectHistory":       1,
	}
	found := make(map[string]int)
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	allowedFile := filepath.Join(root, "workflow", "activities.go")
	recoveryFile := filepath.Join(root, "pushrecovery", "coordinator.go")
	authorizationFile := filepath.Join(
		root,
		"store",
		"push_effect_authorization.go",
	)
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
			strings.HasSuffix(path, "_test.go") ||
			filepath.Clean(path) == filepath.Join(root, "store", "push_effects.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || !methods[selector.Sel.Name] {
					return true
				}
				position := fset.Position(selector.Pos())
				cleanPath := filepath.Clean(path)
				allowedCall := (cleanPath == allowedFile &&
					isPushEffectCoordinatorReceiver(selector.X) &&
					(((selector.Sel.Name == "PushEffectBatchStarted" ||
						selector.Sel.Name == "ClaimPushBatchDeliveryAuthority") &&
						function.Name.Name == "Push") ||
						(selector.Sel.Name != "PushEffectBatchStarted" &&
							function.Name.Name == "sendDurablePushChunk"))) ||
					(cleanPath == recoveryFile &&
						isPushRecoveryStoreReceiver(selector.X) &&
						allowedPushRecoveryStoreCall(
							function.Name.Name,
							selector.Sel.Name,
						)) ||
					(cleanPath == authorizationFile &&
						function.Name.Name ==
							"AuthorizePushEffectRunSideEffect" &&
						selector.Sel.Name == "LoadPushEffect" &&
						isStoreReceiver(selector.X))
				if !allowedCall {
					t.Errorf(
						"push effect Store API escaped fenced coordinator: %s:%d (%s)",
						position.Filename, position.Line, selector.Sel.Name,
					)
					return true
				}
				found[selector.Sel.Name]++
				return true
			})
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for method, want := range expected {
		if got := found[method]; got != want {
			t.Errorf("fenced call count %s=%d, want %d", method, got, want)
		}
	}
	for method, got := range found {
		if _, ok := expected[method]; !ok && got != 0 {
			t.Errorf("unexpected fenced call %s=%d", method, got)
		}
	}
}

func isPushEffectCoordinatorReceiver(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "pushEffectStore" {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == "a"
}

func isPushRecoveryStoreReceiver(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "store" {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == "c"
}

func isStoreReceiver(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "s"
}

func allowedPushRecoveryStoreCall(functionName, methodName string) bool {
	allowed := map[string]map[string]bool{
		"RecoverOnce": {
			"ListRecoverablePushEffectTenantIDs": true,
			"ListRecoverablePushEffects":         true,
		},
		"recoverEffectWithCheckpoint": {
			"TakeOverStalePushEffect": true,
		},
		"recoverAmbiguous": {
			"RecordPushEffectSentWithDeliveries": true,
			"BlockConflictingPushEffectHistory":  true,
		},
		"recoverSafeSend": {
			"AuthorizePushEffectRunSideEffect":        true,
			"ClaimAuthorizedPushEffect":               true,
			"ClaimAuthorizedPushEffectReconciliation": true,
		},
		"sendClaimed": {
			"RecordPushEffectSentWithDeliveries": true,
			"RecordPushEffectDefiniteFailure":    true,
			"RecordPushEffectAmbiguous":          true,
		},
		"deferAmbiguous": {
			"DeferOrBlockPushEffectReconciliation": true,
		},
	}
	return allowed[functionName][methodName]
}
