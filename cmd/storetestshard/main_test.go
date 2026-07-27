package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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
	if got != timingPath {
		t.Fatalf("resolved path = %q, want %q", got, timingPath)
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
