package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/observation"
)

func TestValidatorReserveObservedEventTwoNewBatchesConverge(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM task_observed_events WHERE tenant_id=$1`,
			f.idA.TenantID)
	})

	nextIdentity := scheduledRunIdentity(
		f.taskA, f.idA.TenantID, f.idA.UserID,
		"validator-reserve-run-"+uuid.NewString(),
	)
	nextRef, err := f.base.st.CreateOrGetCompiledTaskRunSnapshotV1(
		ctx,
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: nextIdentity,
			Policy:   testCompiledRunPolicyV1(t),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	leftBatch := createObservationBatch(
		t, f, f.idA, f.refA, "validator-left-"+uuid.NewString(),
	)
	rightBatch := createObservationBatch(
		t, f, nextIdentity, nextRef, "validator-right-"+uuid.NewString(),
	)

	type result struct {
		accepted bool
		err      error
	}
	for i := 1; i <= 20; i++ {
		event := observation.QualifiedEvent{
			PolicyDigest: strings.Repeat("a", 64),
			EventKey:     fmt.Sprintf("%064x", i),
			EventType:    "validator_concurrent_reserve",
			Subject:      "same event",
			OccurredAt:   time.Now().UTC().Truncate(time.Second),
			EvidenceJSON: json.RawMessage(`{"validator":true}`),
		}
		start := make(chan struct{})
		results := make(chan result, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			ok, reserveErr := f.base.st.ReserveObservedEventV1(
				ctx, f.idA, f.refA, leftBatch, event,
			)
			results <- result{accepted: ok, err: reserveErr}
		}()
		go func() {
			defer wg.Done()
			<-start
			ok, reserveErr := f.base.st.ReserveObservedEventV1(
				ctx, nextIdentity, nextRef, rightBatch, event,
			)
			results <- result{accepted: ok, err: reserveErr}
		}()
		close(start)
		wg.Wait()
		close(results)

		accepted := 0
		for got := range results {
			if got.err != nil {
				t.Fatalf("iteration %d reserve error: %v", i, got.err)
			}
			if got.accepted {
				accepted++
			}
		}
		if accepted != 1 {
			t.Fatalf("iteration %d accepted=%d, want exactly one", i, accepted)
		}
	}
}
