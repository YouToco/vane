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
