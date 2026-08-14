package main

import "testing"

func TestTemporalWorkerBuildIDRejectsUnstampedDevelopmentBinary(t *testing.T) {
	if got := temporalWorkerBuildID(); got != "vane/development" {
		t.Fatalf("temporalWorkerBuildID()=%q", got)
	}
	revision := "ac36c9d967c0815ef1a0df3c7ac722823683b646"
	if got := temporalWorkerBuildIDForRevision(revision, true); got != "vane/"+revision {
		t.Fatalf("temporalWorkerBuildIDForRevision()=%q", got)
	}
}
