package testgate

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var forbiddenSkipMethods = map[string]struct{}{
	"Skip": {}, "Skipf": {}, "SkipNow": {},
}

// Violation identifies a direct testing skip outside the sealed capability gate.
type Violation struct {
	Path   string
	Line   int
	Column int
	Method string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s:%d:%d: direct testing.%s is forbidden; use a narrow testgate capability", v.Path, v.Line, v.Column, v.Method)
}

// Scan walks first-party Go packages below root. It uses go/types selections,
// rather than receiver spelling, so import aliases, testing.T aliases, promoted
// methods, method expressions, and method values cannot evade the policy.
func Scan(root string) ([]Violation, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	type packageKey struct{ dir, name string }
	packages := make(map[packageKey][]string)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "third_party") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.PackageClauseOnly)
		if parseErr != nil {
			return parseErr
		}
		key := packageKey{dir: filepath.Dir(path), name: file.Name.Name}
		packages[key] = append(packages[key], path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var violations []Violation
	for key, paths := range packages {
		fileSet := token.NewFileSet()
		files := make([]*ast.File, 0, len(paths))
		for _, path := range paths {
			file, parseErr := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
			if parseErr != nil {
				return nil, parseErr
			}
			files = append(files, file)
		}
		info := &types.Info{Selections: make(map[*ast.SelectorExpr]*types.Selection)}
		config := &types.Config{
			Importer: importer.Default(),
			// The repository compile/test gates own complete type correctness. This
			// scanner only needs the selections that go/types can prove originate
			// in testing; unrelated unavailable module export data is not an escape.
			Error: func(error) {},
		}
		_, _ = config.Check(key.name, fileSet, files, info)
		for _, file := range files {
			path := fileSet.Position(file.Pos()).Filename
			sealed := sealedCapabilitySelector(root, path, file)
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				selection := info.Selections[selector]
				if selection == nil {
					return true
				}
				object := selection.Obj()
				if object == nil || object.Pkg() == nil || object.Pkg().Path() != "testing" {
					return true
				}
				if _, forbidden := forbiddenSkipMethods[object.Name()]; !forbidden {
					return true
				}
				if selector == sealed && object.Name() == "Skip" {
					return true
				}
				position := fileSet.Position(selector.Sel.Pos())
				relative, relErr := filepath.Rel(root, position.Filename)
				if relErr != nil {
					relative = position.Filename
				}
				violations = append(violations, Violation{
					Path: filepath.ToSlash(relative), Line: position.Line,
					Column: position.Column, Method: object.Name(),
				})
				return true
			})
		}
	}
	sort.Slice(violations, func(i, j int) bool {
		if violations[i].Path != violations[j].Path {
			return violations[i].Path < violations[j].Path
		}
		if violations[i].Line != violations[j].Line {
			return violations[i].Line < violations[j].Line
		}
		return violations[i].Column < violations[j].Column
	})
	return violations, nil
}

func sealedCapabilitySelector(root, path string, file *ast.File) *ast.SelectorExpr {
	want := filepath.Join(root, "internal", "testgate", "capability.go")
	same, err := filepath.EvalSymlinks(path)
	if err != nil {
		same = filepath.Clean(path)
	}
	wantResolved, err := filepath.EvalSymlinks(want)
	if err != nil {
		wantResolved = filepath.Clean(want)
	}
	if same != wantResolved {
		return nil
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "unavailable" || function.Body == nil || len(function.Body.List) == 0 {
			continue
		}
		statement, ok := function.Body.List[len(function.Body.List)-1].(*ast.ExprStmt)
		if !ok {
			return nil
		}
		call, ok := statement.X.(*ast.CallExpr)
		if !ok {
			return nil
		}
		selector, _ := call.Fun.(*ast.SelectorExpr)
		return selector
	}
	return nil
}

type skipAllowlist struct {
	Schema  string               `json:"schema"`
	Entries []skipAllowlistEntry `json:"entries"`
}

type skipAllowlistEntry struct {
	File    string `json:"file"`
	Test    string `json:"test"`
	Owner   string `json:"owner"`
	Reason  string `json:"reason"`
	Expires string `json:"expires"`
}

// ValidateAllowlist rejects malformed, duplicate, expired, or path-escaping
// entries. The migration starts with an empty list; adding one is an explicit,
// reviewable exception and never authorizes direct Go testing skips.
func ValidateAllowlist(path string, now time.Time) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	var policy skipAllowlist
	if err := decoder.Decode(&policy); err != nil {
		return fmt.Errorf("decode skip allowlist: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("skip allowlist contains trailing JSON value")
		}
		return fmt.Errorf("skip allowlist trailing JSON: %w", err)
	}
	if policy.Schema != "vane.test-skip-allowlist/v1" {
		return fmt.Errorf("unexpected skip allowlist schema %q", policy.Schema)
	}
	if policy.Entries == nil {
		return fmt.Errorf("skip allowlist entries must be an array")
	}
	seen := make(map[string]struct{}, len(policy.Entries))
	for index, entry := range policy.Entries {
		if entry.File == "" || entry.Test == "" || entry.Owner == "" || entry.Reason == "" || entry.Expires == "" {
			return fmt.Errorf("skip allowlist entry %d has an empty required field", index)
		}
		clean := filepath.Clean(entry.File)
		if filepath.IsAbs(entry.File) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("skip allowlist entry %d escapes the repository: %q", index, entry.File)
		}
		expires, parseErr := time.Parse("2006-01-02", entry.Expires)
		if parseErr != nil {
			return fmt.Errorf("skip allowlist entry %d has invalid expiry: %w", index, parseErr)
		}
		if !expires.After(now.UTC().Truncate(24 * time.Hour)) {
			return fmt.Errorf("skip allowlist entry %d expired on %s", index, entry.Expires)
		}
		key := filepath.ToSlash(clean) + "\x00" + entry.Test
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate skip allowlist entry for %s %s", entry.File, entry.Test)
		}
		seen[key] = struct{}{}
	}
	return nil
}
