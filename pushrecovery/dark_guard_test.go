package pushrecovery

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

func TestPushRecoveryHasZeroProductionCallPoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate push recovery package")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(
		path string,
		entry os.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			base := entry.Name()
			if base == ".git" || base == "third_party" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") ||
			filepath.Dir(path) == filepath.Join(root, "pushrecovery") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		imported, importErr := fileImportsPushRecovery(file)
		if importErr != nil {
			return importErr
		}
		if imported {
			t.Errorf("production import of dark push recovery: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPushRecoveryDarkGuardCatchesMainAndWorkflowWiring(t *testing.T) {
	for _, packageName := range []string{"main", "workflow"} {
		t.Run(packageName, func(t *testing.T) {
			source := `package ` + packageName + `
import recovery "github.com/YouToco/vane/pushrecovery"
func wireRecovery() { _, _ = recovery.New(recovery.Deps{}) }
`
			file, err := parser.ParseFile(
				token.NewFileSet(),
				packageName+".go",
				source,
				0,
			)
			if err != nil {
				t.Fatal(err)
			}
			imported, err := fileImportsPushRecovery(file)
			if err != nil {
				t.Fatal(err)
			}
			if !imported {
				t.Fatal("dark guard missed production recovery wiring mutation")
			}
		})
	}
}

func fileImportsPushRecovery(file *ast.File) (bool, error) {
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return false, err
		}
		if strings.HasSuffix(value, "/pushrecovery") {
			return true, nil
		}
	}
	return false, nil
}
