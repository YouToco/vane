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
	"sort"
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
	ExitCode              int       `json:"exit_code"`
	Phase                 string    `json:"phase"`
	Error                 string    `json:"error,omitempty"`
	FailedShards          []int     `json:"failed_shards,omitempty"`
	Strategy              string    `json:"strategy"`
	ExpectedTests         int       `json:"expected_tests"`
	ObservedTests         int       `json:"observed_tests"`
	DuplicateTests        int       `json:"duplicate_tests"`
	MissingTests          int       `json:"missing_tests"`
	BuildSeconds          float64   `json:"build_seconds"`
	ShardWallSeconds      float64   `json:"shard_wall_seconds"`
	ShardSeconds          []float64 `json:"shard_seconds,omitempty"`
	TotalSeconds          float64   `json:"total_seconds"`
	HistoricalTimingTests int       `json:"historical_timing_tests"`
	TimingInputStatus     string    `json:"timing_input_status"`
}

type timingInput struct {
	timings map[string]float64
	status  string
}

type shardResult struct {
	index   int
	elapsed time.Duration
	err     error
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: storetestshard <run|timing-seed|merge-coverage> [options]"))
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "timing-seed":
		err = timingSeed(os.Args[2:])
	case "merge-coverage":
		err = mergeCoverage(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatal(err)
	}
}

func run(args []string) (runErr error) {
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

	artifacts, err := filepath.Abs(*artifactDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(artifacts, 0o755); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}

	started := time.Now()
	phase := "setup"
	status := runStatus{TimingInputStatus: "not_provided"}
	if *timingPath != "" {
		status.TimingInputStatus = "pending"
	}
	var buildElapsed time.Duration
	var shardElapsed time.Duration
	var failedShards []int
	statusPath := filepath.Join(artifacts, "store-shard-status.json")
	defer func() {
		errorMessage := ""
		if runErr != nil {
			errorMessage = runErr.Error()
		}
		finalStatus := finalizeRunStatus(
			status,
			phase,
			errorMessage,
			buildElapsed,
			shardElapsed,
			time.Since(started),
			failedShards,
		)
		if err := writeJSON(statusPath, finalStatus); err != nil {
			runErr = errors.Join(
				runErr,
				fmt.Errorf("write store shard status: %w", err),
			)
		}
	}()

	if len(databaseURLs) == 0 {
		return errors.New("at least one --database-url is required")
	}

	repo, err := filepath.Abs(*repoDir)
	if err != nil {
		return err
	}
	repo, err = filepath.EvalSymlinks(repo)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}

	phase = "build"
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
		buildElapsed = time.Since(buildStarted)
		return fmt.Errorf("build store test binary: %w", err)
	}
	buildElapsed = time.Since(buildStarted)

	phase = "list"
	storeDir := filepath.Join(repo, "store")
	listCommand := exec.Command(binaryPath, "-test.list=.")
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
	status.ExpectedTests = len(tests)

	timing, err := loadOptionalTimings(repo, *timingPath)
	if err != nil {
		return err
	}
	status.TimingInputStatus = timing.status

	phase = "plan"
	plan, err := testshard.BuildPlan(tests, timing.timings, len(databaseURLs))
	if err != nil {
		return err
	}
	status.Strategy = plan.Strategy
	status.HistoricalTimingTests = plan.HistoricalTimings
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

	phase = "shards"
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
	shardElapsed = time.Since(shardStarted)
	var shardErrors []error
	status.ShardSeconds = make([]float64, len(plan.Shards))
	for result := range results {
		fmt.Printf("store shard %d finished in %.3fs\n", result.index, result.elapsed.Seconds())
		status.ShardSeconds[result.index] = roundSeconds(result.elapsed)
		if result.err != nil {
			failedShards = append(failedShards, result.index)
			shardErrors = append(shardErrors, result.err)
		}
	}
	sort.Ints(failedShards)
	if len(shardErrors) > 0 {
		return errors.Join(shardErrors...)
	}

	phase = "integrity"
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
	status.ObservedTests, status.DuplicateTests, status.MissingTests =
		observationStats(tests, observed)
	if err := testshard.VerifyObserved(plan, observed); err != nil {
		return fmt.Errorf("execution integrity: %w", err)
	}

	phase = "coverage"
	if err := mergeProfiles(filepath.Join(artifacts, "store.coverage.out"), coveragePaths, true); err != nil {
		return fmt.Errorf("merge store coverage: %w", err)
	}

	phase = "combine-json"
	combinedJSON := filepath.Join(artifacts, "store.test.json")
	if err := concatenate(combinedJSON, jsonPaths); err != nil {
		return err
	}

	phase = "complete"
	fmt.Printf(
		"store shard integrity verified: expected=%d observed=%d duplicate=0 missing=0 strategy=%s wall=%.3fs\n",
		status.ExpectedTests,
		status.ObservedTests,
		status.Strategy,
		roundSeconds(shardElapsed),
	)
	return nil
}

func timingSeed(args []string) error {
	flags := flag.NewFlagSet("timing-seed", flag.ContinueOnError)
	repoDir := flags.String("repo", ".", "repository root")
	testListPath := flags.String("tests", "", "authoritative top-level test list")
	outputPath := flags.String("output", "", "optional canonical timing seed JSONL output")
	manifestPath := flags.String("manifest", "", "optional timing seed summary JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *testListPath == "" {
		return errors.New("--tests is required")
	}
	if len(flags.Args()) == 0 {
		return errors.New("at least one timing JSONL input is required")
	}
	repo, err := filepath.Abs(*repoDir)
	if err != nil {
		return err
	}
	repo, err = filepath.EvalSymlinks(repo)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	resolvedTests, err := resolveRepoRelativeFile(repo, *testListPath)
	if err != nil {
		return fmt.Errorf("resolve test list: %w", err)
	}
	testList, err := os.Open(resolvedTests)
	if err != nil {
		return fmt.Errorf("open test list: %w", err)
	}
	tests, parseErr := testshard.ParseTestList(testList)
	closeErr := testList.Close()
	if err := errors.Join(parseErr, closeErr); err != nil {
		return err
	}

	readers := make([]io.Reader, 0, len(flags.Args()))
	files := make([]*os.File, 0, len(flags.Args()))
	for _, path := range flags.Args() {
		resolved, err := resolveRepoRelativeFile(repo, path)
		if err != nil {
			_ = closeFiles(files)
			return fmt.Errorf("resolve timing input: %w", err)
		}
		file, err := os.Open(resolved)
		if err != nil {
			_ = closeFiles(files)
			return fmt.Errorf("open timing input: %w", err)
		}
		files = append(files, file)
		readers = append(readers, file)
	}
	seed, summary, buildErr := testshard.BuildTimingSeed(tests, readers)
	closeInputsErr := closeFiles(files)
	if err := errors.Join(buildErr, closeInputsErr); err != nil {
		return err
	}
	if *outputPath != "" {
		resolved, err := resolveRepoRelativeOutput(repo, *outputPath)
		if err != nil {
			return fmt.Errorf("resolve timing seed output: %w", err)
		}
		if err := os.WriteFile(resolved, seed, 0o644); err != nil {
			return fmt.Errorf("write timing seed: %w", err)
		}
	}
	if *manifestPath != "" {
		resolved, err := resolveRepoRelativeOutput(repo, *manifestPath)
		if err != nil {
			return fmt.Errorf("resolve timing seed manifest: %w", err)
		}
		if err := writeJSON(resolved, summary); err != nil {
			return fmt.Errorf("write timing seed manifest: %w", err)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(summary)
}

func loadOptionalTimings(repo, path string) (timingInput, error) {
	if path == "" {
		return timingInput{status: "not_provided"}, nil
	}
	resolved, err := resolveRepoRelativeFile(repo, path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(os.Stderr, "timing input missing; using stable FNV-1a fallback")
			return timingInput{status: "missing_fallback"}, nil
		}
		return timingInput{}, fmt.Errorf("resolve timing artifact: %w", err)
	}
	file, err := os.Open(resolved)
	if err != nil {
		return timingInput{}, fmt.Errorf("open timing artifact: %w", err)
	}
	timings, parseErr := testshard.ParseTimings(file)
	closeErr := file.Close()
	if closeErr != nil {
		return timingInput{}, closeErr
	}
	if parseErr != nil {
		fmt.Fprintln(os.Stderr, "timing input corrupt; using stable FNV-1a fallback")
		return timingInput{status: "corrupt_fallback"}, nil
	}
	if len(timings) == 0 {
		fmt.Fprintln(os.Stderr, "timing input empty; using stable FNV-1a fallback")
		return timingInput{status: "empty_fallback"}, nil
	}
	return timingInput{timings: timings, status: "loaded"}, nil
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
	commandArgs := storeShardCommandArgs(binaryPath, shard.RunRegex, timeout, coveragePath)
	command := exec.Command(commandArgs[0], commandArgs[1:]...)
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

func storeShardCommandArgs(
	binaryPath string,
	runRegex string,
	timeout time.Duration,
	coveragePath string,
) []string {
	return []string{
		"go", "tool", "test2json",
		"-t",
		"-p", storePackage,
		binaryPath,
		"-test.v=test2json",
		"-test.count=1",
		// Each shard owns one database. Preserve the historical Store Gate's
		// within-database serialization while the independent shards themselves
		// run concurrently against separate PostgreSQL instances.
		"-test.parallel=1",
		"-test.run=" + runRegex,
		"-test.timeout=" + timeout.String(),
		"-test.coverprofile=" + coveragePath,
	}
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

func finalizeRunStatus(
	status runStatus,
	phase string,
	errorMessage string,
	buildElapsed time.Duration,
	shardElapsed time.Duration,
	totalElapsed time.Duration,
	failedShards []int,
) runStatus {
	status.Phase = phase
	status.Error = errorMessage
	status.BuildSeconds = roundSeconds(buildElapsed)
	status.ShardWallSeconds = roundSeconds(shardElapsed)
	status.TotalSeconds = roundSeconds(totalElapsed)
	status.FailedShards = append([]int(nil), failedShards...)
	if errorMessage == "" {
		status.ExitCode = 0
	} else {
		status.ExitCode = 1
	}
	return status
}

func observationStats(expected []string, observed [][]string) (
	observedCount int,
	duplicateCount int,
	missingCount int,
) {
	counts := make(map[string]int)
	for _, shardTests := range observed {
		for _, name := range shardTests {
			observedCount++
			counts[name]++
			if counts[name] > 1 {
				duplicateCount++
			}
		}
	}
	for _, name := range expected {
		if counts[name] == 0 {
			missingCount++
		}
	}
	return observedCount, duplicateCount, missingCount
}

func resolveRepoRelativeFile(repo, path string) (string, error) {
	if err := validateRepoRelativePath(path); err != nil {
		return "", err
	}

	resolvedRepo, err := filepath.EvalSymlinks(repo)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(
		filepath.Join(resolvedRepo, filepath.Clean(path)),
	)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(resolvedRepo, resolvedPath)
	if err != nil {
		return "", err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("resolved path escapes repository: %q", path)
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("path is not a regular file: %q", path)
	}
	return resolvedPath, nil
}

func resolveRepoRelativeOutput(repo, path string) (string, error) {
	if err := validateRepoRelativePath(path); err != nil {
		return "", err
	}
	resolvedRepo, err := filepath.EvalSymlinks(repo)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	joined := filepath.Join(resolvedRepo, filepath.Clean(path))
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(joined))
	if err != nil {
		return "", err
	}
	resolvedPath := filepath.Join(resolvedParent, filepath.Base(joined))
	if err := requirePathInsideRepo(resolvedRepo, resolvedPath, path); err != nil {
		return "", err
	}
	if info, err := os.Lstat(resolvedPath); err == nil {
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("output path is not a regular file: %q", path)
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return resolvedPath, nil
}

func validateRepoRelativePath(path string) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if filepath.IsAbs(path) || filepath.VolumeName(path) != "" {
		return fmt.Errorf("path must be repository-relative: %q", path)
	}
	for _, part := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if part == ".." {
			return fmt.Errorf("path must not contain parent traversal: %q", path)
		}
	}
	return nil
}

func requirePathInsideRepo(repo, resolvedPath, suppliedPath string) error {
	relative, err := filepath.Rel(repo, resolvedPath)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("resolved path escapes repository: %q", suppliedPath)
	}
	return nil
}

func roundSeconds(duration time.Duration) float64 {
	return float64(duration.Round(time.Millisecond)) / float64(time.Second)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
