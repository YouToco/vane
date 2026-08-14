package main

import (
	"reflect"
	"testing"
)

func TestCloseServerStartupResourcesClosesTemporalThenBothStores(t *testing.T) {
	var calls []string
	closeServerStartupResources(
		func() { calls = append(calls, "temporal") },
		func() { calls = append(calls, "stores") },
	)
	if want := []string{"temporal", "stores"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("close order=%v want=%v", calls, want)
	}
}

func TestCloseServerStartupResourcesAllowsPartiallyAcquiredDependencies(t *testing.T) {
	var temporalClosed bool
	closeServerStartupResources(func() { temporalClosed = true }, nil)
	if !temporalClosed {
		t.Fatal("acquired Temporal client was not closed")
	}
	var storesClosed bool
	closeServerStartupResources(nil, func() { storesClosed = true })
	if !storesClosed {
		t.Fatal("acquired Stores were not closed")
	}
}
