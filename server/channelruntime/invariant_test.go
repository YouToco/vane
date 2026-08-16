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
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				selector, ok := n.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch selector.Sel.Name {
				case "SendTextEffect", "PrepareTelegramOutbound",
					"PrepareAggregateTelegramOutbound":
					if !strings.HasPrefix(rel, "telegram"+string(filepath.Separator)) &&
						!strings.HasPrefix(rel, "store"+string(filepath.Separator)) {
						t.Errorf("%s calls provider-specific %s", rel, selector.Sel.Name)
					}
				case "BindDurableSend":
					if rel != filepath.Join("channelruntime", "runtime.go") &&
						!strings.HasPrefix(rel, "store"+string(filepath.Separator)) {
						t.Errorf("%s mints SendPermit outside Store", rel)
					}
				}
			case *ast.SelectorExpr:
				if rel == filepath.Join("telegram", "manager.go") &&
					n.Sel.Name == "MembershipRoleOwner" {
					t.Errorf("%s hard-codes Telegram principal Owner role", rel)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
