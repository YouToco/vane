package channelruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
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
	imports := make(map[string]string)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			violations = append(violations, rel+" has malformed import "+spec.Path.Value)
			continue
		}
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
		if spec.Name != nil && spec.Name.Name == "." &&
			(path == "github.com/YouToco/vane/server/channelruntime" ||
				path == "github.com/YouToco/vane/server/telegram") {
			violations = append(violations,
				rel+" dot-imports a channel invocation authority")
		}
		if path == "github.com/YouToco/vane/server/telegram" &&
			!strings.HasPrefix(rel, "telegram"+string(filepath.Separator)) &&
			!strings.HasPrefix(rel, "api"+string(filepath.Separator)) &&
			!strings.HasPrefix(rel, filepath.Join("cmd", "server")+string(filepath.Separator)) {
			violations = append(violations,
				rel+" imports provider package telegram from business code")
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		field, ok := node.(*ast.Field)
		if ok && fieldNamesContain(field, "Send") &&
			containsChannelRuntimeSelector(field.Type, imports, "SendPermit") &&
			rel != filepath.Join("channelruntime", "runtime.go") &&
			!strings.HasPrefix(rel, "telegram"+string(filepath.Separator)) {
			violations = append(violations,
				rel+" declares a direct SendPermit adapter entrypoint")
		}
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
		case "Adapter":
			if containsChannelRuntimeSelector(selector, imports, "Adapter") &&
				rel != filepath.Join("channelruntime", "runtime.go") {
				violations = append(violations,
					rel+" references the provider Adapter boundary directly")
			}
		case "NewDispatcher":
			if containsChannelRuntimeSelector(selector, imports, "NewDispatcher") &&
				!strings.HasPrefix(rel,
					filepath.Join("cmd", "server")+string(filepath.Separator)) {
				violations = append(violations,
					rel+" constructs the sole channel Dispatcher outside composition")
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

func fieldNamesContain(field *ast.Field, wanted string) bool {
	for _, name := range field.Names {
		if name.Name == wanted {
			return true
		}
	}
	return false
}

func containsChannelRuntimeSelector(
	node ast.Node, imports map[string]string, wanted string,
) bool {
	found := false
	ast.Inspect(node, func(candidate ast.Node) bool {
		selector, ok := candidate.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != wanted {
			return true
		}
		identifier, ok := selector.X.(*ast.Ident)
		if ok && imports[identifier.Name] ==
			"github.com/YouToco/vane/server/channelruntime" {
			found = true
			return false
		}
		return true
	})
	return found
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

func TestChannelInvocationBoundaryRejectsExactImportMutations(t *testing.T) {
	mutations := []string{
		`package workflow; import . "github.com/YouToco/vane/server/channelruntime"; var mint = BindDurableSend`,
		`package periodicbrief; import "github.com/YouToco/vane/server/telegram"; var direct *telegram.Fleet`,
	}
	for _, source := range mutations {
		file, err := parser.ParseFile(token.NewFileSet(), "mutation.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := channelBoundaryViolations(file,
			filepath.Join("workflow", "mutation.go")); len(got) == 0 {
			t.Fatalf("channel import mutation escaped guard: %s", source)
		}
	}
}

func TestChannelInvocationBoundaryRejectsDirectAdapterMutation(t *testing.T) {
	source := `package workflow
import (
  "context"
  "github.com/YouToco/vane/server/channelruntime"
)
type bypass interface {
  Send(context.Context, channelruntime.SendPermit) (channelruntime.ProviderObservation, error)
}`
	file, err := parser.ParseFile(token.NewFileSet(), "mutation.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := channelBoundaryViolations(file,
		filepath.Join("workflow", "mutation.go")); len(got) == 0 {
		t.Fatal("direct SendPermit adapter mutation escaped guard")
	}
}
