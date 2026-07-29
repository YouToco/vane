package exploration

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestExplorationHasNoProductionImportV1 keeps the first rollout dark. Any
// production call point, including Feishu/delivery renderers, must update this
// guard intentionally after the task runtime contract is frozen.
func TestExplorationHasNoProductionImportV1(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate exploration guard")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), ".."))
	imports, err := productionExplorationImportsV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(imports) != 0 {
		t.Fatalf("exploration gained production imports before rollout: %v", imports)
	}
	forbidden, err := forbiddenPackageImportsV1(
		filepath.Join(root, "exploration"),
		[]string{
			"github.com/YouToco/vane/api",
			"github.com/YouToco/vane/feishu",
			"github.com/YouToco/vane/llm",
			"github.com/YouToco/vane/pusher",
			"github.com/YouToco/vane/store",
			"github.com/YouToco/vane/workflow",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(forbidden) != 0 {
		t.Fatalf("exploration imported a forbidden production surface: %v", forbidden)
	}
}

func TestExplorationZeroCallGuardDetectsProductionImportV1(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "feishu", "card.go")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`package feishu
import _ "github.com/YouToco/vane/exploration"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	imports, err := productionExplorationImportsV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(imports) != 1 || imports[0] != "feishu/card.go" {
		t.Fatalf("guard missed forbidden import: %v", imports)
	}
}

func productionExplorationImportsV1(root string) ([]string, error) {
	const packagePath = "github.com/YouToco/vane/exploration"
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
		if !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
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
				imports = append(imports, relativeExplorationPathV1(root, path))
			}
		}
		return nil
	})
	return imports, err
}

func forbiddenPackageImportsV1(
	root string,
	forbiddenPrefixes []string,
) ([]string, error) {
	var matches []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
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
			for _, forbidden := range forbiddenPrefixes {
				if importPath == forbidden ||
					strings.HasPrefix(importPath, forbidden+"/") {
					matches = append(matches,
						filepath.Base(path)+":"+importPath)
				}
			}
		}
		return nil
	})
	return matches, err
}

func relativeExplorationPathV1(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(relative)
}
