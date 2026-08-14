package taskhealth

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestTaskHealthProductionImportIsLimitedToBriefWebProjectionV1(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate task health guard")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), ".."))
	const packagePath = "github.com/YouToco/vane/server/taskhealth"
	var imports []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (entry.Name() == ".git" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if importPath == packagePath {
				relative, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				imports = append(imports, filepath.ToSlash(relative))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(imports)
	allowed := []string{
		"api/brief_projections.go",
		"api/briefs.go",
	}
	if len(imports) != len(allowed) {
		t.Fatalf(
			"task health production import scope drifted: %v",
			imports,
		)
	}
	for index := range allowed {
		if imports[index] != allowed[index] {
			t.Fatalf(
				"task health production import scope drifted: %v",
				imports,
			)
		}
	}
}
