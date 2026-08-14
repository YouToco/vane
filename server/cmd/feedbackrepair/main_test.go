package main

import "testing"

func TestRunRejectsUnsafeApplyBeforeLoadingConfiguration(t *testing.T) {
	for _, arguments := range [][]string{
		nil,
		{"-tenant", "1", "-user", "2", "-mode", "unknown"},
		{"-tenant", "1", "-user", "2", "-mode", "apply"},
	} {
		if got := run(arguments); got != 2 {
			t.Fatalf("run(%q)=%d, want usage refusal 2", arguments, got)
		}
	}
}
