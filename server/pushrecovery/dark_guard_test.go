package pushrecovery

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

func TestPushRecoveryLifecycleHasNarrowProductionWiring(t *testing.T) {
	t.Parallel()

	root := filepath.Clean("..")
	exportedMethods := make(map[string]bool)
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(
		path string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		violations := inspectRecoveryAST(file, path, exportedMethods)
		for _, violation := range violations {
			t.Error(violation)
		}
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if value == "github.com/YouToco/vane/pushrecovery" {
				clean := filepath.ToSlash(path)
				if !strings.HasSuffix(clean, "/cmd/server/main.go") {
					t.Errorf("recovery import outside composition root: %s", path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(exportedMethods) != 1 || !exportedMethods["Attempt"] {
		t.Fatalf("exported coordinator methods=%v, want Attempt only", exportedMethods)
	}
}

func TestPushRecoveryDarkGuardRejectsSamePackageWrapperMutation(t *testing.T) {
	t.Parallel()

	source := `package pushrecovery
import (
	"context"
	"time"
	"github.com/YouToco/vane/pusheffect"
)
func (c *Coordinator) Run(ctx context.Context) {
	go func() {}()
	_ = time.NewTicker(time.Second)
	attempt := c.Attempt
	_, _ = attempt(ctx, pusheffect.Scope{})
}`
	file, err := parser.ParseFile(
		token.NewFileSet(), "pushrecovery/evil.go", source,
		parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	methods := make(map[string]bool)
	violations := inspectRecoveryAST(
		file, "pushrecovery/evil.go", methods)
	if len(violations) < 3 || !methods["Run"] {
		t.Fatalf(
			"guard accepted same-package runner mutation: methods=%v violations=%v",
			methods, violations,
		)
	}
}

func inspectRecoveryAST(
	file *ast.File,
	path string,
	exportedMethods map[string]bool,
) []string {
	violations := make([]string, 0)
	inRecoveryPackage := file.Name.Name == "pushrecovery"
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.GoStmt:
			if inRecoveryPackage {
				violations = append(
					violations, path+": recovery goroutine is forbidden")
			}
		case *ast.FuncDecl:
			if inRecoveryPackage && typed.Recv != nil &&
				receiverNamesCoordinator(typed.Recv) &&
				ast.IsExported(typed.Name.Name) {
				exportedMethods[typed.Name.Name] = true
			}
		case *ast.SelectorExpr:
			if inRecoveryPackage && typed.Sel.Name == "Attempt" &&
				!strings.HasSuffix(
					filepath.ToSlash(path), "/pushrecovery/runner.go") {
				violations = append(
					violations,
					path+": Coordinator.Attempt production selection is forbidden",
				)
			}
		case *ast.CallExpr:
			selector, ok := typed.Fun.(*ast.SelectorExpr)
			if !ok {
				break
			}
			if inRecoveryPackage &&
				(selector.Sel.Name == "NewTicker" ||
					selector.Sel.Name == "Tick") &&
				!strings.HasSuffix(
					filepath.ToSlash(path), "/pushrecovery/runner.go") {
				violations = append(
					violations, path+": recovery ticker is forbidden")
			}
			if inRecoveryPackage && selector.Sel.Name == "MethodByName" &&
				len(typed.Args) == 1 {
				if literal, ok := typed.Args[0].(*ast.BasicLit); ok &&
					literal.Value == `"Attempt"` {
					violations = append(
						violations,
						path+": reflective Coordinator.Attempt lookup is forbidden",
					)
				}
			}
		}
		return true
	})
	return violations
}

func receiverNamesCoordinator(fields *ast.FieldList) bool {
	if fields == nil || len(fields.List) != 1 {
		return false
	}
	switch typed := fields.List[0].Type.(type) {
	case *ast.Ident:
		return typed.Name == "Coordinator"
	case *ast.StarExpr:
		ident, ok := typed.X.(*ast.Ident)
		return ok && ident.Name == "Coordinator"
	default:
		return false
	}
}
