package channelruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestChannelInvocationBoundary is a mutation guard for the narrow contract:
// changing a workflow back to SendTextEffect/PrepareTelegramOutbound, minting a
// permit outside Store, or restoring Telegram's hard-coded Owner role must make
// this test red.
func TestChannelInvocationBoundary(t *testing.T) {
	_, current, _, _ := runtime.Caller(0)
	serverRoot := filepath.Clean(filepath.Join(filepath.Dir(current), ".."))
	fset := token.NewFileSet()
	err := filepath.WalkDir(serverRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") ||
			strings.Contains(path, string(filepath.Separator)+"third_party"+string(filepath.Separator)) {
			return nil
		}
		rel, err := filepath.Rel(serverRoot, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, violation := range channelBoundaryViolations(file, rel) {
			t.Error(violation)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func channelBoundaryViolations(file *ast.File, rel string) []string {
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		// Count references, not only CallExpr.Fun. This is intentional: a method
		// or function value alias is an equally effective provider bypass.
		switch selector.Sel.Name {
		case "SendTextEffect", "PrepareTelegramOutbound",
			"PrepareAggregateTelegramOutbound":
			if !strings.HasPrefix(rel, "telegram"+string(filepath.Separator)) &&
				!strings.HasPrefix(rel, "store"+string(filepath.Separator)) {
				violations = append(violations,
					rel+" references provider-specific "+selector.Sel.Name)
			}
		case "BindDurableSend":
			if rel != filepath.Join("channelruntime", "runtime.go") &&
				!strings.HasPrefix(rel, "store"+string(filepath.Separator)) {
				violations = append(violations,
					rel+" mints or aliases SendPermit authority")
			}
		}
		if rel == filepath.Join("telegram", "manager.go") &&
			selector.Sel.Name == "MembershipRoleOwner" {
			violations = append(violations,
				rel+" hard-codes Telegram principal Owner role")
		}
		return true
	})
	return violations
}

func TestChannelInvocationBoundaryRejectsFunctionValueAliases(t *testing.T) {
	for _, source := range []string{
		`package workflow; func f(x interface{ SendTextEffect() }) { _ = x.SendTextEffect }`,
		`package workflow; func f(x interface{ PrepareTelegramOutbound() }) { send := x.PrepareTelegramOutbound; _ = send }`,
		`package workflow; import "github.com/YouToco/vane/server/channelruntime"; var mint = channelruntime.BindDurableSend`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutation.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := channelBoundaryViolations(file,
			filepath.Join("workflow", "mutation.go")); len(got) == 0 {
			t.Fatalf("function-value alias escaped guard: %s", source)
		}
	}
}
