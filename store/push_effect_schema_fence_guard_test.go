package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPushEffectSchemaAdmissionGuardsEveryWriter(t *testing.T) {
	fset := token.NewFileSet()
	roleFile := parseStoreGuardFile(t, fset, "push_effect_tx.go")
	roleTx := findStoreGuardFunction(t, roleFile, "beginPushEffectRoleTx")
	beginPos := storeGuardCallPosition(roleTx.Body, "beginTx")
	fencePos := storeGuardCallPosition(
		roleTx.Body,
		"lockPushEffectSchemaWriter",
	)
	tenantPos := storeGuardStringPosition(roleTx.Body, "set_config('app.tenant_id'")
	if beginPos == token.NoPos || fencePos == token.NoPos ||
		tenantPos == token.NoPos ||
		!(beginPos < fencePos && fencePos < tenantPos) {
		t.Fatalf(
			"role transaction admission order begin/fence/tenant=%d/%d/%d",
			beginPos,
			fencePos,
			tenantPos,
		)
	}

	purgeFile := parseStoreGuardFile(t, fset, "tenant_purge.go")
	purge := findStoreGuardFunction(t, purgeFile, "PurgeTenant")
	purgeFence := storeGuardCallPosition(
		purge.Body,
		"lockPushEffectSchemaWriter",
	)
	purgeTenant := storeGuardStringPosition(
		purge.Body,
		"set_config('app.tenant_id'",
	)
	purgeRoot := storeGuardCallPosition(purge.Body, "lockTenantAdmissionRoot")
	if purgeFence == token.NoPos || purgeTenant == token.NoPos ||
		purgeRoot == token.NoPos ||
		!(purgeFence < purgeTenant && purgeTenant < purgeRoot) {
		t.Fatalf(
			"purge admission order fence/tenant/root=%d/%d/%d",
			purgeFence,
			purgeTenant,
			purgeRoot,
		)
	}

	expectedWriters := map[string]bool{
		"CreatePushEffect":                     true,
		"ClaimPushEffect":                      true,
		"ClaimPushEffectReconciliation":        true,
		"TakeOverStalePushEffect":              true,
		"tryDeferPushEffectReconciliation":     true,
		"blockExpiredPushEffectReconciliation": true,
		"recordPushEffectFailure":              true,
		"RecordPushEffectSent":                 true,
		"RecordPushEffectSentWithDeliveries":   true,
		"BlockPushEffect":                      true,
	}
	found := make(map[string]bool)
	err := filepath.WalkDir(".", func(
		path string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil ||
				!storeGuardMutatesPushEffects(function.Body) {
				continue
			}
			found[function.Name.Name] = true
			if filepath.Base(path) != "push_effects.go" ||
				!storeGuardCallsPushEffectRoleTx(function.Body) {
				t.Errorf(
					"push_effects writer bypasses role/schema admission: %s:%s",
					path,
					function.Name.Name,
				)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range expectedWriters {
		if !found[name] {
			t.Errorf("expected push_effects writer not guarded: %s", name)
		}
	}
	for name := range found {
		if !expectedWriters[name] {
			t.Errorf("new push_effects writer requires guard review: %s", name)
		}
	}
}

func parseStoreGuardFile(
	t *testing.T,
	fset *token.FileSet,
	name string,
) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(fset, name, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func findStoreGuardFunction(
	t *testing.T,
	file *ast.File,
	name string,
) *ast.FuncDecl {
	t.Helper()
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}
	t.Fatalf("function %s not found", name)
	return nil
}

func storeGuardCallPosition(body *ast.BlockStmt, name string) token.Pos {
	position := token.NoPos
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			if function.Name == name && position == token.NoPos {
				position = call.Pos()
			}
		case *ast.SelectorExpr:
			if function.Sel.Name == name && position == token.NoPos {
				position = call.Pos()
			}
		}
		return true
	})
	return position
}

func storeGuardStringPosition(
	body *ast.BlockStmt,
	fragment string,
) token.Pos {
	position := token.NoPos
	ast.Inspect(body, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err == nil && strings.Contains(value, fragment) &&
			position == token.NoPos {
			position = literal.Pos()
		}
		return true
	})
	return position
}

func storeGuardMutatesPushEffects(body *ast.BlockStmt) bool {
	mutates := false
	ast.Inspect(body, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		normalized := strings.ToUpper(strings.Join(strings.Fields(value), " "))
		if strings.Contains(normalized, "INSERT INTO PUSH_EFFECTS") ||
			strings.Contains(normalized, "UPDATE PUSH_EFFECTS") ||
			strings.Contains(normalized, "DELETE FROM PUSH_EFFECTS") {
			mutates = true
		}
		return true
	})
	return mutates
}

func storeGuardCallsPushEffectRoleTx(body *ast.BlockStmt) bool {
	for _, name := range []string{
		"beginPushEffectCoordinatorTx",
		"beginPushEffectReceiptTx",
		"beginPushEffectOperatorTx",
	} {
		if storeGuardCallPosition(body, name) != token.NoPos {
			return true
		}
	}
	return false
}
