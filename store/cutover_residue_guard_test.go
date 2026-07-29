package store

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestTaskToolRuntimeHasNoRetiredCurrentPackages keeps the old catalog/spec
// layers and account source product from returning as production packages.
func TestTaskToolRuntimeHasNoRetiredCurrentPackages(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve guard file")
	}
	root := filepath.Dir(filepath.Dir(file))
	for _, retiredPath := range []string{
		"capabilitycatalog",
		"fetchspec",
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

	for _, rel := range []string{
		filepath.Join("agent", "tools.go"),
		filepath.Join("agent", "loop.go"),
	} {
		body, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		for _, retired := range []string{"approved_fetch_plan", "fetch_requirements"} {
			if strings.Contains(string(body), retired) {
				t.Errorf("retired model field %q remains in %s", retired, rel)
			}
		}
	}

	migrationFiles, err := filepath.Glob(filepath.Join(root, "store", "migrations", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range migrationFiles {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		upper := strings.ToUpper(string(body))
		for _, forbidden := range []string{
			"CREATE TABLE TASK_SOURCES",
			"CREATE TABLE USER_SOURCES",
			"CREATE TABLE SOURCE_INSTANCES",
			"CREATE TABLE TASK_TOOL_INVOCATIONS",
			"CREATE TABLE RUN_TOOL_INVOCATIONS",
		} {
			if strings.Contains(upper, forbidden) {
				t.Errorf("Source entity schema %q is forbidden in %s",
					forbidden, filepath.Base(path))
			}
		}
	}
}
