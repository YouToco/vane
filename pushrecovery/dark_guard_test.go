package pushrecovery

import (
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
		for _, spec := range file.Imports {
			value, unquoteErr := strconv.Unquote(spec.Path.Value)
			if unquoteErr != nil {
				return unquoteErr
			}
			if strings.HasSuffix(value, "/pushrecovery") {
				t.Errorf("production import of dark push recovery: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
