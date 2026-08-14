package store

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/YouToco/vane/types"
)

// ErrLegacyControlPlaneAdmissionClosed marks a rejected attempt to create new
// retained V1/V2 control-plane state. Historical rows, decoders, run readers,
// and recovery methods remain available through the embedded Store.
var ErrLegacyControlPlaneAdmissionClosed = errors.New(
	"store: legacy control-plane admission is closed",
)

// LegacyAdmissionFencedStore is the production dependency for coordinators
// that still own retained V1/V2 recovery. It deliberately embeds the complete
// Store so replay/recovery keeps its frozen implementation. Construction
// atomically closes the embedded Store's V1/V2 operation-root writers: exact
// durable replays remain readable, while a missing root is never inserted.
// Source-shaped product writers are hidden at this dependency boundary.
//
// Existing operation creation calls are exact response-loss replays: the
// underlying Store revalidates every byte before returning the durable row.
// A missing operation is never created through this boundary.
type LegacyAdmissionFencedStore struct {
	*Store
}

func NewLegacyAdmissionFencedStore(st *Store) (*LegacyAdmissionFencedStore, error) {
	if st == nil {
		return nil, errors.New("store: legacy admission fence requires a Store")
	}
	atomic.StoreUint32(&st.legacyAdmissionClosed, 1)
	return &LegacyAdmissionFencedStore{Store: st}, nil
}

func (s *Store) legacyAdmissionIsClosed() bool {
	return s != nil && atomic.LoadUint32(&s.legacyAdmissionClosed) == 1
}

func (s *LegacyAdmissionFencedStore) InsertPausedCompiledTaskDefinition(
	context.Context,
	types.PausedCompiledTaskDefinition,
) error {
	return legacyAdmissionClosed("compiled task definition v1")
}

func (s *LegacyAdmissionFencedStore) GetOrCreateFetchTarget(
	context.Context,
	*types.FetchTarget,
) (int64, bool, error) {
	return 0, false, legacyAdmissionClosed("fetch target")
}

func (s *LegacyAdmissionFencedStore) ReplaceTaskFetchTargets(
	context.Context,
	int64,
	string,
	[]int64,
) error {
	return legacyAdmissionClosed("task fetch targets")
}

func legacyAdmissionClosed(kind string) error {
	return fmt.Errorf("%w: new %s state is forbidden",
		ErrLegacyControlPlaneAdmissionClosed, kind)
}
