package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/internal/testshard"
)

func TestFinalizeRunStatusRecordsFailure(t *testing.T) {
	base := runStatus{
		Strategy:              "stable-fnv1a",
		ExpectedTests:         12,
		ObservedTests:         9,
		MissingTests:          3,
		HistoricalTimingTests: 0,
	}
	failed := []int{2, 0}
	got := finalizeRunStatus(
		base,
		"shards",
		"shard 2 failed",
		1500*time.Millisecond,
		2250*time.Millisecond,
		4*time.Second,
		failed,
	)
	failed[0] = 99

	if got.ExitCode != 1 || got.Phase != "shards" ||
		got.Error != "shard 2 failed" {
		t.Fatalf("failure status = %+v", got)
	}
	if got.BuildSeconds != 1.5 || got.ShardWallSeconds != 2.25 ||
		got.TotalSeconds != 4 {
		t.Fatalf("failure timings = %+v", got)
	}
	if !reflect.DeepEqual(got.FailedShards, []int{2, 0}) {
		t.Fatalf("failed shards = %v", got.FailedShards)
	}
}

func TestStoreShardCommandSerializesTestsWithinOneDatabase(t *testing.T) {
	args := storeShardCommandArgs(
		"/tmp/store.test", "^(TestA|TestB)$", 40*time.Minute,
		"/tmp/store.coverage.out",
	)
	if got := countExact(args, "-test.parallel=1"); got != 1 {
		t.Fatalf("parallel serialization args=%q count=%d", args, got)
	}
	if got := countExact(args, "-test.count=1"); got != 1 {
		t.Fatalf("single-run args=%q count=%d", args, got)
	}
}

func countExact(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func TestResolveRepoRelativeFileRejectsAbsoluteAndParentPaths(t *testing.T) {
	repo := t.TempDir()
	timingDir := filepath.Join(repo, "timings")
	if err := os.Mkdir(timingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	timingPath := filepath.Join(timingDir, "store.json")
	if err := os.WriteFile(timingPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := resolveRepoRelativeFile(repo, filepath.Join("timings", "store.json"))
	if err != nil {
		t.Fatal(err)
	}
	resolvedTimingPath, err := filepath.EvalSymlinks(timingPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != resolvedTimingPath {
		t.Fatalf("resolved path = %q, want %q", got, resolvedTimingPath)
	}
	if _, err := resolveRepoRelativeFile(repo, timingPath); err == nil {
		t.Fatal("absolute timing path was accepted")
	}
	for _, path := range []string{
		filepath.Join("..", "store.json"),
		"timings/../timings/store.json",
		`timings\..\store.json`,
	} {
		if _, err := resolveRepoRelativeFile(repo, path); err == nil {
			t.Fatalf("parent traversal %q was accepted", path)
		}
	}
}

func TestResolveRepoRelativeFileRejectsResolvedEscape(t *testing.T) {
	repo := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "store.json")
	if err := os.WriteFile(outsideFile, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(repo, "outside")
	if err := os.Symlink(outside, link); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "privilege") {
			t.Skipf("symlink creation unavailable: %v", err)
		}
		t.Fatal(err)
	}
	if _, err := resolveRepoRelativeFile(
		repo,
		filepath.Join("outside", "store.json"),
	); err == nil {
		t.Fatal("symlink escape was accepted")
	}
}

func TestLoadOptionalTimingsFallsBackOnlyForMissingCorruptAndEmpty(t *testing.T) {
	repo := t.TempDir()
	for name, contents := range map[string]string{
		"corrupt.jsonl": "not-json\n",
		"empty.jsonl":   "",
		"valid.jsonl":   `{"Action":"pass","Test":"TestA","Elapsed":1.5}` + "\n",
	} {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		path       string
		wantStatus string
		wantTiming float64
	}{
		{path: "", wantStatus: "not_provided"},
		{path: "missing.jsonl", wantStatus: "missing_fallback"},
		{path: "corrupt.jsonl", wantStatus: "corrupt_fallback"},
		{path: "empty.jsonl", wantStatus: "empty_fallback"},
		{path: "valid.jsonl", wantStatus: "loaded", wantTiming: 1.5},
	} {
		t.Run(tc.wantStatus, func(t *testing.T) {
			got, err := loadOptionalTimings(repo, tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if got.status != tc.wantStatus || got.timings["TestA"] != tc.wantTiming {
				t.Fatalf("timing input=%+v", got)
			}
			if tc.wantStatus != "loaded" {
				plan, err := testshard.BuildPlan(
					[]string{"TestA", "TestB", "TestC"}, got.timings, 2,
				)
				if err != nil || plan.Strategy != "stable-fnv1a" {
					t.Fatalf("fallback plan=%+v err=%v", plan, err)
				}
			}
		})
	}
	if _, err := loadOptionalTimings(repo, "../untrusted.jsonl"); err == nil {
		t.Fatal("path authority failure fell back instead of failing closed")
	}
}

func TestTimingSeedCommandGeneratesAndRevalidatesCanonicalJSONL(t *testing.T) {
	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, "artifacts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(repo, "tests.txt"),
		[]byte("TestBravo\nTestAlpha\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	raw := strings.Join([]string{
		`{"Action":"run","Test":"TestAlpha"}`,
		`{"Action":"pass","Test":"TestAlpha","Elapsed":1}`,
		`{"Action":"pass","Test":"TestBravo","Elapsed":2}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(repo, "raw.jsonl"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := timingSeed([]string{
		"--repo", repo,
		"--tests", "tests.txt",
		"--output", "artifacts/store.timings.jsonl",
		"--manifest", "artifacts/store.timings.manifest.json",
		"raw.jsonl",
	}); err != nil {
		t.Fatal(err)
	}
	if err := timingSeed([]string{
		"--repo", repo,
		"--tests", "tests.txt",
		"artifacts/store.timings.jsonl",
	}); err != nil {
		t.Fatalf("generated seed did not revalidate: %v", err)
	}
	seed, err := os.ReadFile(filepath.Join(repo, "artifacts", "store.timings.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(seed), "\n") != 2 ||
		!strings.HasPrefix(string(seed), `{"Action":"pass","Test":"TestAlpha"`) {
		t.Fatalf("canonical seed=%q", seed)
	}
}
