package store

import (
	"io/fs"
	"strings"
	"testing"
)

func TestMigrationFilenamesHaveUniqueVersions(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]string, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		version, _, ok := strings.Cut(name, "_")
		if !ok || len(version) != 3 {
			t.Fatalf("migration filename has no canonical version: %q", name)
		}
		if previous, duplicate := seen[version]; duplicate {
			t.Fatalf("migration version %s is duplicated by %q and %q", version, previous, name)
		}
		seen[version] = name
	}
}
