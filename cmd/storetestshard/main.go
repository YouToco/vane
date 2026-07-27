package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/YouToco/vane/internal/testshard"
)

const storePackage = "github.com/YouToco/vane/store"

type stringList []string

func (values *stringList) String() string {
	return strings.Join(*values, ",")
}

func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

type runStatus struct {
	ExitCode              int     `json:"exit_code"`
	Strategy              string  `json:"strategy"`
	ExpectedTests         int     `json:"expected_tests"`
	ObservedTests         int     `json:"observed_tests"`
	DuplicateTests        int     `json:"duplicate_tests"`
	MissingTests          int     `json:"missing_tests"`
	BuildSeconds          float64 `json:"build_seconds"`
	ShardWallSeconds      float64 `json:"shard_wall_seconds"`
	TotalSeconds          float64 `json:"total_seconds"`
	HistoricalTimingTests int     `json:"historical_timing_tests"`
}

type shardResult struct {
	index   int
	elapsed time.Duration
	err     error
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: storetestshard <run|merge-coverage> [options]"))
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "merge-coverage":
		err = mergeCoverage(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatal(err)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	var databaseURLs stringList
	flags.Var(&databaseURLs, "database-url", "independent PostgreSQL URL; repeat once per shard")
	artifactDir := flags.String("artifacts", "tmp/store-sharding/run", "artifact directory")
	timingPath := flags.String("timings", "", "optional prior go test -json timing artifact")
	repoDir := flags.String("repo", ".", "repository root")
	timeout := flags.Duration("timeout", 20*time.Minute, "timeout for each shard")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(databaseURLs) == 0 {
		return errors.New("at least one --database-url is required")
	}

	repo, err := filepath.Abs(*repoDir)
	if err != nil {
		return err
	}
	artifacts, err := filepath.Abs(*artifactDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(artifacts, 0o755); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}

	started := time.Now()
	buildStarted := time.Now()
	binaryName := "store.test"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(artifacts, binaryName)
	build := exec.Command(
		"go", "test", "-c",
		"-race",
		"-cover",
		"-covermode=atomic",
		"-coverpkg="+storePackage,
		"-o", binaryPath,
		"./store",
	)
	build.Dir = repo
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("build store test binary: %w", err)
	}
	buildElapsed := time.Since(buildStarted)

	storeDir := filepath.Join(repo, "store")
	listCommand := exec.Command(binaryPath, "-test.list=^Test")
	listCommand.Dir = storeDir
	listOutput, err := listCommand.Output()
	if err != nil {
		return fmt.Errorf("list compiled store tests: %w", err)
	}
	listPath := filepath.Join(artifacts, "store-tests.list.txt")
	if err := os.WriteFile(listPath, listOutput, 0o644); err != nil {
		return err
	}
	tests, err := testshard.ParseTestList(strings.NewReader(string(listOutput)))
	if err != nil {
		return err
	}

	timings := map[string]float64(nil)
	if *timingPath != "" {
		timingFile, err := os.Open(*timingPath)
		if err != nil {
			return fmt.Errorf("open timing artifact: %w", err)
		}
		timings, err = testshard.ParseTimings(timingFile)
		closeErr := timingFile.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}

	plan, err := testshard.BuildPlan(tests, timings, len(databaseURLs))
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(artifacts, "store-shard-plan.json"), plan); err != nil {
		return err
	}
	for _, shard := range plan.Shards {
		manifest := strings.Join(shard.Tests, "\n") + "\n"
		path := filepath.Join(artifacts, fmt.Sprintf("store-shard-%d.tests.txt", shard.Index))
		if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
			return err
		}
	}

	shardStarted := time.Now()
	results := make(chan shardResult, len(plan.Shards))
	var wait sync.WaitGroup
	for i := range plan.Shards {
		shard := plan.Shards[i]
		databaseURL := databaseURLs[i]
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- runShard(storeDir, artifacts, binaryPath, databaseURL, shard, *timeout)
		}()
	}
	wait.Wait()
	close(results)
	shardElapsed := time.Since(shardStarted)
	var shardErrors []error
	for result := range results {
		fmt.Printf("store shard %d finished in %.3fs\n", result.index, result.elapsed.Seconds())
		if result.err != nil {
			shardErrors = append(shardErrors, result.err)
		}
	}
	if len(shardErrors) > 0 {
		return errors.Join(shardErrors...)
	}

	observed := make([][]string, len(plan.Shards))
	coveragePaths := make([]string, len(plan.Shards))
	jsonPaths := make([]string, len(plan.Shards))
	for i := range plan.Shards {
		jsonPaths[i] = filepath.Join(artifacts, fmt.Sprintf("store-shard-%d.test.json", i))
		file, err := os.Open(jsonPaths[i])
		if err != nil {
			return err
		}
		observed[i], err = testshard.ObservedTopLevelRuns(file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		coveragePaths[i] = filepath.Join(artifacts, fmt.Sprintf("store-shard-%d.coverage.out", i))
	}
	if err := testshard.VerifyObserved(plan, observed); err != nil {
		return fmt.Errorf("execution integrity: %w", err)
	}
	if err := mergeProfiles(filepath.Join(artifacts, "store.coverage.out"), coveragePaths, true); err != nil {
		return fmt.Errorf("merge store coverage: %w", err)
	}
	combinedJSON := filepath.Join(artifacts, "store.test.json")
	if err := concatenate(combinedJSON, jsonPaths); err != nil {
		return err
	}

	status := runStatus{
		ExitCode:              0,
		Strategy:              plan.Strategy,
		ExpectedTests:         len(tests),
		ObservedTests:         countObserved(observed),
		DuplicateTests:        0,
		MissingTests:          0,
		BuildSeconds:          roundSeconds(buildElapsed),
		ShardWallSeconds:      roundSeconds(shardElapsed),
		TotalSeconds:          roundSeconds(time.Since(started)),
		HistoricalTimingTests: plan.HistoricalTimings,
	}
	if err := writeJSON(filepath.Join(artifacts, "store-shard-status.json"), status); err != nil {
		return err
	}
	fmt.Printf(
		"store shard integrity verified: expected=%d observed=%d duplicate=0 missing=0 strategy=%s wall=%.3fs\n",
		status.ExpectedTests,
		status.ObservedTests,
		status.Strategy,
		status.ShardWallSeconds,
	)
	return nil
}

func runShard(
	storeDir string,
	artifacts string,
	binaryPath string,
	databaseURL string,
	shard testshard.Shard,
	timeout time.Duration,
) shardResult {
	started := time.Now()
	jsonPath := filepath.Join(artifacts, fmt.Sprintf("store-shard-%d.test.json", shard.Index))
	coveragePath := filepath.Join(artifacts, fmt.Sprintf("store-shard-%d.coverage.out", shard.Index))
	output, err := os.Create(jsonPath)
	if err != nil {
		return shardResult{index: shard.Index, err: err}
	}
	command := exec.Command(
		"go", "tool", "test2json",
		"-t",
		"-p", storePackage,
		binaryPath,
		"-test.v=test2json",
		"-test.count=1",
		"-test.run="+shard.RunRegex,
		"-test.timeout="+timeout.String(),
		"-test.coverprofile="+coveragePath,
	)
	command.Dir = storeDir
	command.Env = withEnvironment(os.Environ(), "DATABASE_URL", databaseURL)
	command.Stdout = output
	command.Stderr = os.Stderr
	runErr := command.Run()
	closeErr := output.Close()
	if runErr != nil {
		return shardResult{
			index:   shard.Index,
			elapsed: time.Since(started),
			err:     fmt.Errorf("shard %d: %w", shard.Index, runErr),
		}
	}
	if closeErr != nil {
		return shardResult{index: shard.Index, elapsed: time.Since(started), err: closeErr}
	}
	return shardResult{index: shard.Index, elapsed: time.Since(started)}
}

func mergeCoverage(args []string) error {
	flags := flag.NewFlagSet("merge-coverage", flag.ContinueOnError)
	output := flags.String("output", "", "merged coverage profile")
	requireIdentical := flags.Bool("require-identical", false, "require every input to contain the same blocks")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("--output is required")
	}
	if len(flags.Args()) == 0 {
		return errors.New("at least one input profile is required")
	}
	return mergeProfiles(*output, flags.Args(), *requireIdentical)
}

func mergeProfiles(outputPath string, inputPaths []string, requireIdentical bool) error {
	readers := make([]io.Reader, 0, len(inputPaths))
	files := make([]*os.File, 0, len(inputPaths))
	for _, path := range inputPaths {
		file, err := os.Open(path)
		if err != nil {
			closeFiles(files)
			return err
		}
		files = append(files, file)
		readers = append(readers, file)
	}
	output, err := os.Create(outputPath)
	if err != nil {
		closeFiles(files)
		return err
	}
	mergeErr := testshard.MergeCoverage(output, readers, requireIdentical)
	closeOutputErr := output.Close()
	closeInputErr := closeFiles(files)
	return errors.Join(mergeErr, closeOutputErr, closeInputErr)
}

func concatenate(outputPath string, inputPaths []string) error {
	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	for _, path := range inputPaths {
		input, err := os.Open(path)
		if err != nil {
			_ = output.Close()
			return err
		}
		if _, err := io.Copy(output, input); err != nil {
			_ = input.Close()
			_ = output.Close()
			return err
		}
		if err := input.Close(); err != nil {
			_ = output.Close()
			return err
		}
	}
	return output.Close()
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func withEnvironment(environment []string, key, value string) []string {
	prefix := strings.ToUpper(key) + "="
	result := make([]string, 0, len(environment)+1)
	for _, item := range environment {
		if !strings.HasPrefix(strings.ToUpper(item), prefix) {
			result = append(result, item)
		}
	}
	return append(result, key+"="+value)
}

func closeFiles(files []*os.File) error {
	var errs []error
	for _, file := range files {
		if err := file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func countObserved(observed [][]string) int {
	count := 0
	for _, tests := range observed {
		count += len(tests)
	}
	return count
}

func roundSeconds(duration time.Duration) float64 {
	return float64(duration.Round(time.Millisecond)) / float64(time.Second)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
