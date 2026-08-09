package testshard

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Plan struct {
	Version           int     `json:"version"`
	Strategy          string  `json:"strategy"`
	TestCount         int     `json:"test_count"`
	HistoricalTimings int     `json:"historical_timings"`
	EstimatedWall     float64 `json:"estimated_wall_seconds"`
	Shards            []Shard `json:"shards"`
}

type Shard struct {
	Index            int      `json:"index"`
	Tests            []string `json:"tests"`
	RunRegex         string   `json:"run_regex"`
	EstimatedSeconds float64  `json:"estimated_seconds"`
}

type testEvent struct {
	Action  string  `json:"Action"`
	Test    string  `json:"Test"`
	Elapsed float64 `json:"Elapsed"`
}

type TimingSeedSummary struct {
	Version        int    `json:"version"`
	TestCount      int    `json:"test_count"`
	TerminalEvents int    `json:"terminal_events"`
	SHA256         string `json:"sha256"`
}

func ParseTestList(r io.Reader) ([]string, error) {
	var tests []string
	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		name := strings.TrimSpace(scanner.Text())
		if name == "" {
			continue
		}
		if !isTopLevelTest(name) {
			return nil, fmt.Errorf("unsupported non-test runnable in list: %q", name)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate test in list: %s", name)
		}
		seen[name] = struct{}{}
		tests = append(tests, name)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read test list: %w", err)
	}
	if len(tests) == 0 {
		return nil, errors.New("test list contains no top-level tests")
	}
	sort.Strings(tests)
	return tests, nil
}

func ParseTimings(r io.Reader) (map[string]float64, error) {
	totals := make(map[string]float64)
	counts := make(map[string]int)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("parse timing JSON line %d: %w", line, err)
		}
		if !isTopLevelTest(event.Test) || event.Elapsed <= 0 {
			continue
		}
		switch event.Action {
		case "pass", "fail", "skip":
			totals[event.Test] += event.Elapsed
			counts[event.Test]++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read timing JSON: %w", err)
	}
	result := make(map[string]float64, len(totals))
	for name, total := range totals {
		result[name] = total / float64(counts[name])
	}
	return result, nil
}

// BuildTimingSeed projects one or more go test -json streams to a canonical,
// sorted top-level terminal JSONL seed. Every authoritative test must have
// exactly one terminal event; subtests, package events and output are omitted.
func BuildTimingSeed(expected []string, readers []io.Reader) ([]byte, TimingSeedSummary, error) {
	want, err := normalizeTests(expected)
	if err != nil {
		return nil, TimingSeedSummary{}, err
	}
	if len(readers) == 0 {
		return nil, TimingSeedSummary{}, errors.New("timing seed requires at least one input")
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, name := range want {
		wantSet[name] = struct{}{}
	}
	events := make(map[string]testEvent, len(want))
	for inputIndex, reader := range readers {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		line := 0
		for scanner.Scan() {
			line++
			var event testEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				return nil, TimingSeedSummary{}, fmt.Errorf(
					"parse timing seed input %d line %d: %w", inputIndex, line, err,
				)
			}
			if !isTopLevelTest(event.Test) ||
				(event.Action != "pass" && event.Action != "fail" && event.Action != "skip") {
				continue
			}
			if _, ok := wantSet[event.Test]; !ok {
				return nil, TimingSeedSummary{}, fmt.Errorf(
					"unexpected top-level terminal test %s", event.Test,
				)
			}
			if event.Elapsed <= 0 {
				return nil, TimingSeedSummary{}, fmt.Errorf(
					"top-level terminal test %s has non-positive elapsed time", event.Test,
				)
			}
			if _, duplicate := events[event.Test]; duplicate {
				return nil, TimingSeedSummary{}, fmt.Errorf(
					"duplicate top-level terminal test %s", event.Test,
				)
			}
			events[event.Test] = event
		}
		if err := scanner.Err(); err != nil {
			return nil, TimingSeedSummary{}, fmt.Errorf(
				"read timing seed input %d: %w", inputIndex, err,
			)
		}
	}
	if len(events) != len(want) {
		missing := make([]string, 0, len(want)-len(events))
		for _, name := range want {
			if _, ok := events[name]; !ok {
				missing = append(missing, name)
			}
		}
		return nil, TimingSeedSummary{}, fmt.Errorf(
			"timing seed terminal events %d do not match %d tests; missing=%s",
			len(events), len(want), strings.Join(missing, ","),
		)
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for _, name := range want {
		if err := encoder.Encode(events[name]); err != nil {
			return nil, TimingSeedSummary{}, err
		}
	}
	seed := output.Bytes()
	digest := sha256.Sum256(seed)
	return append([]byte(nil), seed...), TimingSeedSummary{
		Version:        1,
		TestCount:      len(want),
		TerminalEvents: len(events),
		SHA256:         fmt.Sprintf("%x", digest),
	}, nil
}

func BuildPlan(tests []string, timings map[string]float64, shardCount int) (Plan, error) {
	if shardCount < 1 {
		return Plan{}, errors.New("shard count must be positive")
	}
	if len(tests) == 0 {
		return Plan{}, errors.New("cannot shard an empty test list")
	}
	normalized, err := normalizeTests(tests)
	if err != nil {
		return Plan{}, err
	}

	plan := Plan{
		Version:   1,
		TestCount: len(normalized),
		Shards:    make([]Shard, shardCount),
	}
	for i := range plan.Shards {
		plan.Shards[i].Index = i
	}

	known := 0
	for _, name := range normalized {
		if timings[name] > 0 {
			known++
		}
	}
	plan.HistoricalTimings = known
	if known == 0 {
		plan.Strategy = "stable-fnv1a"
		for _, name := range normalized {
			index := int(stableHash(name) % uint64(shardCount))
			plan.Shards[index].Tests = append(plan.Shards[index].Tests, name)
		}
	} else {
		plan.Strategy = "historical-lpt"
		fallback := fallbackWeight(normalized, timings)
		type weightedTest struct {
			name   string
			weight float64
		}
		items := make([]weightedTest, 0, len(normalized))
		for _, name := range normalized {
			weight := timings[name]
			if weight <= 0 {
				weight = fallback
			}
			items = append(items, weightedTest{name: name, weight: weight})
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].weight == items[j].weight {
				return items[i].name < items[j].name
			}
			return items[i].weight > items[j].weight
		})
		for _, item := range items {
			index := lightestShard(plan.Shards)
			plan.Shards[index].Tests = append(plan.Shards[index].Tests, item.name)
			plan.Shards[index].EstimatedSeconds += item.weight
		}
	}

	for i := range plan.Shards {
		sort.Strings(plan.Shards[i].Tests)
		plan.Shards[i].RunRegex = ExactRunRegex(plan.Shards[i].Tests)
		if plan.Shards[i].EstimatedSeconds > plan.EstimatedWall {
			plan.EstimatedWall = plan.Shards[i].EstimatedSeconds
		}
	}
	if err := VerifyPlan(normalized, plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func VerifyPlan(expected []string, plan Plan) error {
	want, err := normalizeTests(expected)
	if err != nil {
		return err
	}
	owners := make(map[string]int, len(want))
	for _, shard := range plan.Shards {
		for _, name := range shard.Tests {
			if previous, exists := owners[name]; exists {
				return fmt.Errorf("test %s appears in shards %d and %d", name, previous, shard.Index)
			}
			owners[name] = shard.Index
		}
	}
	if len(owners) != len(want) {
		return fmt.Errorf("planned test count %d does not match expected %d", len(owners), len(want))
	}
	for _, name := range want {
		if _, ok := owners[name]; !ok {
			return fmt.Errorf("test %s is missing from plan", name)
		}
	}
	for name := range owners {
		index := sort.SearchStrings(want, name)
		if index == len(want) || want[index] != name {
			return fmt.Errorf("unexpected test %s in plan", name)
		}
	}
	return nil
}

func ExactRunRegex(tests []string) string {
	if len(tests) == 0 {
		return "^$"
	}
	escaped := make([]string, len(tests))
	for i, name := range tests {
		escaped[i] = regexp.QuoteMeta(name)
	}
	return "^(" + strings.Join(escaped, "|") + ")$"
}

func ObservedTopLevelRuns(r io.Reader) ([]string, error) {
	var result []string
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("parse test JSON line %d: %w", line, err)
		}
		if event.Action == "run" && isTopLevelTest(event.Test) {
			result = append(result, event.Test)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read test JSON: %w", err)
	}
	return result, nil
}

func VerifyObserved(plan Plan, observedByShard [][]string) error {
	if len(observedByShard) != len(plan.Shards) {
		return fmt.Errorf("observed shard count %d does not match plan %d", len(observedByShard), len(plan.Shards))
	}
	allObserved := make(map[string]int)
	for i, observed := range observedByShard {
		expected := append([]string(nil), plan.Shards[i].Tests...)
		sort.Strings(expected)
		got, err := normalizeTests(observed)
		if err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
		if len(got) != len(expected) {
			return fmt.Errorf("shard %d observed %d tests, expected %d", i, len(got), len(expected))
		}
		for j := range expected {
			if got[j] != expected[j] {
				return fmt.Errorf("shard %d observed mismatch at %d: got %s, expected %s", i, j, got[j], expected[j])
			}
			if previous, exists := allObserved[got[j]]; exists {
				return fmt.Errorf("test %s observed in shards %d and %d", got[j], previous, i)
			}
			allObserved[got[j]] = i
		}
	}
	return nil
}

func MergeCoverage(w io.Writer, readers []io.Reader, requireIdentical bool) error {
	if len(readers) == 0 {
		return errors.New("no coverage profiles to merge")
	}
	mode := ""
	counts := make(map[string]uint64)
	var baseline map[string]struct{}
	for index, reader := range readers {
		profileMode, entries, err := readCoverage(reader)
		if err != nil {
			return fmt.Errorf("coverage input %d: %w", index, err)
		}
		if mode == "" {
			mode = profileMode
		} else if profileMode != mode {
			return fmt.Errorf("coverage input %d mode %s differs from %s", index, profileMode, mode)
		}
		current := make(map[string]struct{}, len(entries))
		for key, count := range entries {
			current[key] = struct{}{}
			counts[key] += count
		}
		if index == 0 {
			baseline = current
		} else if requireIdentical && !sameKeys(baseline, current) {
			return fmt.Errorf("coverage input %d block set differs from input 0", index)
		}
	}
	if _, err := fmt.Fprintf(w, "mode: %s\n", mode); err != nil {
		return err
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "%s %d\n", key, counts[key]); err != nil {
			return err
		}
	}
	return nil
}

func readCoverage(r io.Reader) (string, map[string]uint64, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return "", nil, errors.New("empty coverage profile")
	}
	header := strings.Fields(scanner.Text())
	if len(header) != 2 || header[0] != "mode:" {
		return "", nil, fmt.Errorf("invalid coverage header %q", scanner.Text())
	}
	entries := make(map[string]uint64)
	line := 1
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			return "", nil, fmt.Errorf("invalid coverage line %d", line)
		}
		count, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("invalid coverage count on line %d: %w", line, err)
		}
		key := fields[0] + " " + fields[1]
		if _, exists := entries[key]; exists {
			return "", nil, fmt.Errorf("duplicate coverage block on line %d", line)
		}
		entries[key] = count
	}
	if err := scanner.Err(); err != nil {
		return "", nil, err
	}
	return header[1], entries, nil
}

func normalizeTests(tests []string) ([]string, error) {
	result := append([]string(nil), tests...)
	sort.Strings(result)
	for i, name := range result {
		if !isTopLevelTest(name) {
			return nil, fmt.Errorf("invalid top-level test name %q", name)
		}
		if i > 0 && result[i-1] == name {
			return nil, fmt.Errorf("duplicate test %s", name)
		}
	}
	return result, nil
}

func isTopLevelTest(name string) bool {
	return strings.HasPrefix(name, "Test") &&
		!strings.ContainsAny(name, " \t/") &&
		name != ""
}

func stableHash(name string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(name))
	return hash.Sum64()
}

func fallbackWeight(tests []string, timings map[string]float64) float64 {
	var known []float64
	for _, name := range tests {
		if timings[name] > 0 {
			known = append(known, timings[name])
		}
	}
	sort.Float64s(known)
	if len(known) == 0 {
		return 1
	}
	middle := len(known) / 2
	if len(known)%2 == 1 {
		return math.Max(known[middle], 0.001)
	}
	return math.Max((known[middle-1]+known[middle])/2, 0.001)
}

func lightestShard(shards []Shard) int {
	index := 0
	for i := 1; i < len(shards); i++ {
		if shards[i].EstimatedSeconds < shards[index].EstimatedSeconds {
			index = i
		}
	}
	return index
}

func sameKeys(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if _, ok := b[key]; !ok {
			return false
		}
	}
	return true
}
