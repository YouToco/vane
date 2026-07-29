package store

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestFetchTargetCutoverHasNoCurrentLegacyNamespaces keeps the old account
// source model from returning through a production file or package. Applied
// SQL migrations, immutable wire readers and explicit compatibility tests are
// the only places allowed to know the retired names.
func TestFetchTargetCutoverHasNoCurrentLegacyNamespaces(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve guard file")
	}
	root := filepath.Dir(filepath.Dir(file))
	for _, retiredPath := range []string{
		"sourcecatalog",
		"sourcespec",
		filepath.Join("store", "sources.go"),
		filepath.Join("store", "schedule_sources.go"),
		filepath.Join("store", "subscriptions.go"),
		filepath.Join("api", "subscriptions.go"),
	} {
		if _, err := os.Stat(filepath.Join(root, retiredPath)); !os.IsNotExist(err) {
			t.Fatalf("retired current path still exists: %s", retiredPath)
		}
	}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel == ".git" || rel == ".codex-tmp" ||
				rel == filepath.Join("store", "migrations") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lower := strings.ToLower(string(body))
		for _, retired := range []string{"sourcecatalog", "schedule_sources"} {
			if strings.Contains(lower, retired) {
				t.Errorf("retired namespace %q remains in production Go file %s", retired, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
