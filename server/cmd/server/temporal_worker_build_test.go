package main

import "testing"

func TestTemporalWorkerBuildIDRejectsUnstampedDevelopmentBinary(t *testing.T) {
	if got := temporalWorkerBuildID(); got != "vane/development" {
		t.Fatalf("temporalWorkerBuildID()=%q", got)
	}
}
