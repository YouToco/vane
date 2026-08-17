package mcpclient

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Migration 153 is deliberately dark. A later activation change must update
// this guard together with its coordinator-role grant and production UAT;
// merely importing the data-only foundation must not create a call point.
func TestRemoteMCPProductionCoordinatorHasZeroCallPoints(t *testing.T) {
	findings, err := remoteMCPProductionCallPoints(filepath.Clean(".."))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("migration 153 gained a production MCP call point:\n%s",
			strings.Join(findings, "\n"))
	}
}

func remoteMCPProductionCallPoints(serverRoot string) ([]string, error) {
	var findings []string
	err := filepath.WalkDir(serverRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		aliases := map[string]struct{}{}
		for _, imported := range parsed.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil || value != "github.com/YouToco/vane/server/mcpclient" {
				continue
			}
			alias := "mcpclient"
			if imported.Name != nil {
				alias = imported.Name.Name
			}
			if alias == "." {
				findings = append(findings, path+": dot-imports mcpclient")
				continue
			}
			aliases[alias] = struct{}{}
		}
		samePackage := parsed.Name.Name == "mcpclient" &&
			filepath.Base(filepath.Dir(path)) == "mcpclient"
		implementationFile := samePackage && filepath.Clean(path) ==
			filepath.Join(filepath.Clean(serverRoot), "mcpclient", "runtime.go")
		forbiddenSymbols := map[string]struct{}{
			"Coordinator": {}, "ApprovedConnectionV1": {}, "RuntimeBindingV153": {},
			"InvocationLedgerV1": {}, "LedgerPermitV1": {},
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				if identifier, ok := value.X.(*ast.Ident); ok {
					if _, imported := aliases[identifier.Name]; imported {
						if _, forbidden := forbiddenSymbols[value.Sel.Name]; forbidden {
							findings = append(findings, path+": references remote MCP runtime authority "+value.Sel.Name)
						}
					}
				}
			case *ast.CallExpr:
				if selector, ok := value.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Invoke" &&
					(samePackage || len(aliases) != 0) {
					findings = append(findings, path+": calls an Invoke method in MCP-wired production code")
				}
			case *ast.InterfaceType:
				if samePackage || len(aliases) != 0 {
					for _, field := range value.Methods.List {
						for _, name := range field.Names {
							if name.Name == "Invoke" {
								findings = append(findings, path+": introduces an MCP Invoke interface seam")
							}
						}
					}
				}
			case *ast.TypeSpec:
				if value.Assign.IsValid() && (samePackage || len(aliases) != 0) {
					ast.Inspect(value.Type, func(aliasNode ast.Node) bool {
						identifier, ok := aliasNode.(*ast.Ident)
						if ok {
							if _, forbidden := forbiddenSymbols[identifier.Name]; forbidden {
								findings = append(findings, path+": aliases remote MCP runtime authority")
							}
						}
						return true
					})
				}
			case *ast.FuncDecl:
				if implementationFile && value.Body != nil {
					ast.Inspect(value.Body, func(bodyNode ast.Node) bool {
						identifier, ok := bodyNode.(*ast.Ident)
						if ok {
							if _, forbidden := forbiddenSymbols[identifier.Name]; forbidden {
								findings = append(findings, path+": runtime implementation body references authority symbol "+identifier.Name)
							}
						}
						return true
					})
				}
			case *ast.ValueSpec:
				if implementationFile {
					for _, expression := range value.Values {
						ast.Inspect(expression, func(valueNode ast.Node) bool {
							identifier, ok := valueNode.(*ast.Ident)
							if ok {
								if _, forbidden := forbiddenSymbols[identifier.Name]; forbidden {
									findings = append(findings, path+": runtime implementation aliases authority symbol "+identifier.Name)
								}
							}
							return true
						})
					}
				}
			case *ast.Ident:
				if samePackage && !implementationFile {
					if _, forbidden := forbiddenSymbols[value.Name]; forbidden {
						findings = append(findings, path+": references same-package MCP runtime authority "+value.Name)
					}
				}
			}
			return true
		})
		return nil
	})
	return findings, err
}

func TestRemoteMCPZeroCallPointGuardCoversAliasesInterfacesAndSamePackage(t *testing.T) {
	for _, test := range []struct {
		name, source string
		runtimeFile  bool
	}{
		{name: "same package construction", source: `package mcpclient
var active = Coordinator{}`},
		{name: "import alias", source: `package wiring
import remote "github.com/YouToco/vane/server/mcpclient"
type active = remote.Coordinator`},
		{name: "interface seam", source: `package wiring
import _ "github.com/YouToco/vane/server/mcpclient"
type invoker interface { Invoke() error }`},
		{name: "method call", source: `package wiring
import _ "github.com/YouToco/vane/server/mcpclient"
func run(value interface{ Invoke() error }) error { return value.Invoke() }`},
		{name: "runtime implementation alias", runtimeFile: true, source: `package mcpclient
type Coordinator struct{}
var activate = Coordinator{}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			mcpDir := filepath.Join(root, "mcpclient")
			if err := os.MkdirAll(mcpDir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "wiring.go")
			if strings.HasPrefix(test.source, "package mcpclient") {
				path = filepath.Join(mcpDir, "wiring.go")
			}
			if test.runtimeFile {
				path = filepath.Join(mcpDir, "runtime.go")
			}
			if err := os.WriteFile(path, []byte(test.source), 0o600); err != nil {
				t.Fatal(err)
			}
			findings, err := remoteMCPProductionCallPoints(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(findings) == 0 {
				t.Fatal("dark guard missed production call-point mutation")
			}
		})
	}
}
