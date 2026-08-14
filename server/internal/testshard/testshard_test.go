package testshard

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestBuildPlanStableHashIsDeterministicAndComplete(t *testing.T) {
	tests := []string{"TestCharlie", "TestAlpha", "TestBravo", "TestDelta"}
	first, err := BuildPlan(tests, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPlan([]string{"TestDelta", "TestBravo", "TestAlpha", "TestCharlie"}, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	if first.Strategy != "stable-fnv1a" {
		t.Fatalf("strategy = %q", first.Strategy)
	}
	if first.Shards[0].RunRegex != second.Shards[0].RunRegex ||
		first.Shards[1].RunRegex != second.Shards[1].RunRegex ||
		first.Shards[2].RunRegex != second.Shards[2].RunRegex {
		t.Fatalf("stable plan changed with input order:\n%+v\n%+v", first, second)
	}
	if err := VerifyPlan(tests, first); err != nil {
		t.Fatal(err)
	}
}

func TestBuildPlanHistoricalLPTBalancesKnownDurations(t *testing.T) {
	tests := []string{"TestA", "TestB", "TestC", "TestD", "TestE"}
	plan, err := BuildPlan(tests, map[string]float64{
		"TestA": 9,
		"TestB": 8,
		"TestC": 7,
		"TestD": 6,
		"TestE": 5,
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Strategy != "historical-lpt" {
		t.Fatalf("strategy = %q", plan.Strategy)
	}
	if plan.EstimatedWall != 20 {
		t.Fatalf("estimated wall = %v, want 20", plan.EstimatedWall)
	}
}

func TestParseTimingsUsesOnlyTopLevelTerminalEvents(t *testing.T) {
	input := strings.Join([]string{
		`{"Action":"run","Test":"TestA"}`,
		`{"Action":"pass","Test":"TestA/sub","Elapsed":4}`,
		`{"Action":"pass","Test":"TestA","Elapsed":2}`,
		`{"Action":"pass","Test":"TestA","Elapsed":4}`,
		`{"Action":"output","Test":"TestB","Elapsed":99}`,
	}, "\n")
	timings, err := ParseTimings(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(timings) != 1 || timings["TestA"] != 3 {
		t.Fatalf("timings = %#v", timings)
	}
}

func TestBuildTimingSeedCanonicalAndExact(t *testing.T) {
	expected := []string{"TestBravo", "TestAlpha"}
	input := strings.Join([]string{
		`{"Action":"run","Test":"TestBravo"}`,
		`{"Action":"pass","Test":"TestBravo/sub","Elapsed":99}`,
		`{"Action":"pass","Test":"TestBravo","Elapsed":2.5}`,
		`{"Action":"output","Test":"TestAlpha"}`,
		`{"Action":"pass","Test":"TestAlpha","Elapsed":0}`,
	}, "\n")
	first, summary, err := BuildTimingSeed(expected, []io.Reader{strings.NewReader(input)})
	if err != nil {
		t.Fatal(err)
	}
	second, secondSummary, err := BuildTimingSeed(
		[]string{"TestAlpha", "TestBravo"},
		[]io.Reader{strings.NewReader(input)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || summary != secondSummary ||
		summary.TestCount != 2 || summary.TerminalEvents != 2 ||
		summary.ZeroDurationTests != 1 || len(summary.SHA256) != 64 {
		t.Fatalf("seed=%q summary=%+v second=%q second_summary=%+v",
			first, summary, second, secondSummary)
	}
	if strings.Contains(string(first), "/sub") ||
		!strings.HasPrefix(string(first), `{"Action":"pass","Test":"TestAlpha"`) {
		t.Fatalf("seed is not canonical top-level terminal JSONL: %s", first)
	}
	timings, err := ParseTimings(bytes.NewReader(first))
	if err != nil || timings["TestAlpha"] != 0.001 || timings["TestBravo"] != 2.5 || len(timings) != 2 {
		t.Fatalf("timings=%v err=%v", timings, err)
	}
}

func TestBuildTimingSeedRejectsMissingDuplicateUnexpectedAndInvalidElapsed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "missing", input: `{"Action":"pass","Test":"TestA","Elapsed":1}`},
		{name: "duplicate", input: strings.Join([]string{
			`{"Action":"pass","Test":"TestA","Elapsed":1}`,
			`{"Action":"pass","Test":"TestA","Elapsed":2}`,
			`{"Action":"pass","Test":"TestB","Elapsed":1}`,
		}, "\n")},
		{name: "unexpected", input: strings.Join([]string{
			`{"Action":"pass","Test":"TestA","Elapsed":1}`,
			`{"Action":"pass","Test":"TestB","Elapsed":1}`,
			`{"Action":"pass","Test":"TestC","Elapsed":1}`,
		}, "\n")},
		{name: "negative_elapsed", input: strings.Join([]string{
			`{"Action":"pass","Test":"TestA","Elapsed":1}`,
			`{"Action":"pass","Test":"TestB","Elapsed":-0.001}`,
		}, "\n")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := BuildTimingSeed(
				[]string{"TestA", "TestB"},
				[]io.Reader{strings.NewReader(tc.input)},
			); err == nil {
				t.Fatal("invalid timing seed was accepted")
			}
		})
	}
}

func TestParseTestListRejectsNonTestRunnables(t *testing.T) {
	for _, runnable := range []string{
		"BenchmarkStore",
		"FuzzStore",
		"ExampleStore",
		"HelperRunnable",
	} {
		t.Run(runnable, func(t *testing.T) {
			input := "TestStore\n" + runnable + "\n"
			if _, err := ParseTestList(strings.NewReader(input)); err == nil {
				t.Fatalf("ParseTestList accepted %q", runnable)
			}
		})
	}
}

func TestParseTestListAcceptsAndSortsTests(t *testing.T) {
	tests, err := ParseTestList(strings.NewReader("TestZulu\nTestAlpha\n"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(tests, ",") != "TestAlpha,TestZulu" {
		t.Fatalf("tests = %v", tests)
	}
}

func TestVerifyObservedRejectsMissingAndDuplicateTests(t *testing.T) {
	plan, err := BuildPlan([]string{"TestA", "TestB"}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	observed := make([][]string, 2)
	for i := range plan.Shards {
		observed[i] = append([]string(nil), plan.Shards[i].Tests...)
	}
	if err := VerifyObserved(plan, observed); err != nil {
		t.Fatal(err)
	}
	for i := range observed {
		if len(observed[i]) > 0 {
			observed[i] = observed[i][:len(observed[i])-1]
			break
		}
	}
	if err := VerifyObserved(plan, observed); err == nil {
		t.Fatal("missing observed test was accepted")
	}
}

func TestMergeCoverageSumsAtomicCountsAndChecksBlockSets(t *testing.T) {
	first := "mode: atomic\nexample/a.go:1.1,2.1 1 2\nexample/a.go:3.1,4.1 1 0\n"
	second := "mode: atomic\nexample/a.go:1.1,2.1 1 3\nexample/a.go:3.1,4.1 1 7\n"
	var output bytes.Buffer
	if err := MergeCoverage(&output, []io.Reader{
		strings.NewReader(first),
		strings.NewReader(second),
	}, true); err != nil {
		t.Fatal(err)
	}
	want := "mode: atomic\nexample/a.go:1.1,2.1 1 5\nexample/a.go:3.1,4.1 1 7\n"
	if output.String() != want {
		t.Fatalf("merged coverage:\n%s\nwant:\n%s", output.String(), want)
	}
	if err := MergeCoverage(&bytes.Buffer{}, []io.Reader{
		strings.NewReader(first),
		strings.NewReader("mode: atomic\nexample/b.go:1.1,2.1 1 1\n"),
	}, true); err == nil {
		t.Fatal("mismatched block sets were accepted")
	}
}
