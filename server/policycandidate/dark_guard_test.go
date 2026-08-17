package policycandidate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const packageImportPath = "github.com/YouToco/vane/server/policycandidate"

// This first slice must stay a data-only dark foundation. Any non-test caller
// would create an activation path that requires a separate review and rollout.
func TestPolicyCandidateHasNoProductionCaller(t *testing.T) {
	serverRoot, packageRoot := policyCandidateRoots(t)
	err := filepath.WalkDir(serverRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") || filepath.Dir(path) == packageRoot {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if value == packageImportPath {
				t.Errorf("production caller imports dark policy candidate package: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPolicyCandidateDependenciesCannotExecuteOrPersist(t *testing.T) {
	_, packageRoot := policyCandidateRoots(t)
	allowed := map[string]struct{}{
		"crypto/sha256": {}, "encoding/hex": {}, "encoding/json": {},
		"errors": {}, "fmt": {}, "reflect": {}, "slices": {}, "strings": {},
		"unicode/utf8": {},
		"github.com/YouToco/vane/server/internal/strictjson": {},
		"github.com/YouToco/vane/server/types":               {},
	}
	files, err := parser.ParseDir(token.NewFileSet(), packageRoot, func(info os.FileInfo) bool {
		return strings.HasSuffix(info.Name(), ".go") && !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files["policycandidate"].Files {
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := allowed[value]; !ok {
				t.Errorf("dark policy package imports executable/persistent authority %q", value)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool { return true })
	}
}

func policyCandidateRoots(t *testing.T) (string, string) {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate policycandidate package")
	}
	packageRoot := filepath.Dir(filename)
	return filepath.Dir(packageRoot), packageRoot
}
