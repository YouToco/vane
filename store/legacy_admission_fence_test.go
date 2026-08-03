package store

import (
	"context"
	"errors"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestLegacyAdmissionFencedStoreRejectsSourceProductWriters(t *testing.T) {
	fence, err := NewLegacyAdmissionFencedStore(&Store{})
	if err != nil {
		t.Fatal(err)
	}
	if !fence.Store.legacyAdmissionIsClosed() {
		t.Fatal("Store operation-root admission gate remained open")
	}
	ctx := context.Background()
	assertClosed := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, ErrLegacyControlPlaneAdmissionClosed) {
			t.Fatalf("%s error=%v, want legacy admission closed", name, err)
		}
	}
	_, _, err = fence.GetOrCreateFetchTarget(ctx, &types.FetchTarget{})
	assertClosed("GetOrCreateFetchTarget", err)
	assertClosed("ReplaceTaskFetchTargets",
		fence.ReplaceTaskFetchTargets(ctx, 1, "task", []int64{1}))
	assertClosed("InsertPausedCompiledTaskDefinition",
		fence.InsertPausedCompiledTaskDefinition(ctx,
			types.PausedCompiledTaskDefinition{}))
}

func TestLegacyAdmissionFencedStoreRequiresStore(t *testing.T) {
	if _, err := NewLegacyAdmissionFencedStore(nil); err == nil {
		t.Fatal("nil Store was accepted")
	}
}

func TestLegacyAdmissionFenceAlsoClosesRawProductWriters(t *testing.T) {
	st := &Store{}
	if _, err := NewLegacyAdmissionFencedStore(st); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	assertClosed := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, ErrLegacyControlPlaneAdmissionClosed) {
			t.Fatalf("%s error=%v, want legacy admission closed", name, err)
		}
	}
	_, _, err := st.GetOrCreateFetchTarget(ctx, &types.FetchTarget{})
	assertClosed("raw GetOrCreateFetchTarget", err)
	assertClosed("raw ReplaceTaskFetchTargets",
		st.ReplaceTaskFetchTargets(ctx, 1, "task", nil))
	assertClosed("raw InsertPausedCompiledTaskDefinition",
		st.InsertPausedCompiledTaskDefinition(ctx,
			types.PausedCompiledTaskDefinition{}))
	_, err = st.InsertInitialApprovedDefinition(ctx,
		taskstate.ApprovedDefinitionV1{}, "operation")
	assertClosed("raw InsertInitialApprovedDefinition", err)
}
