package store

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestLoadCanonicalBriefForFeedbackV1ExactScope(t *testing.T) {
	f := newCanonicalBriefFixture(t, 3)
	outcome := f.finalizedContentOutcome(t)
	brief, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, f.draft(t, outcome))
	if err != nil {
		t.Fatal(err)
	}
	deliveryID := brief.Insights[1].ID
	got, found, err := f.base.st.LoadCanonicalBriefForFeedbackV1(
		t.Context(), f.identity.UserID, deliveryID, f.batchID)
	if err != nil || !found {
		t.Fatalf("canonical feedback read found=%t err=%v", found, err)
	}
	if got.ID != brief.ID || got.Digest != brief.Digest ||
		len(got.Insights) != len(brief.Insights) {
		t.Fatalf("canonical feedback brief=%+v want=%+v", got, brief)
	}
	for index := range brief.Insights {
		if !reflect.DeepEqual(
			got.Insights[index], brief.Insights[index],
		) {
			t.Fatalf("canonical feedback insight[%d]=%+v want=%+v",
				index, got.Insights[index], brief.Insights[index])
		}
	}

	tests := []struct {
		name       string
		userID     int64
		deliveryID int64
		batchID    int64
	}{
		{"cross user", f.identity.UserID + 99, deliveryID, f.batchID},
		{"foreign delivery", f.identity.UserID, deliveryID + 9999, f.batchID},
		{"foreign batch", f.identity.UserID, deliveryID, f.batchID + 9999},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, found, err := f.base.st.LoadCanonicalBriefForFeedbackV1(
				t.Context(), test.userID, test.deliveryID, test.batchID)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				t.Fatal("cross-scope canonical Brief was visible")
			}
		})
	}
}

func TestLoadCanonicalBriefForFeedbackV1DoesNotReadPendingStage(t *testing.T) {
	f := newCanonicalBriefFixture(t, 1)
	marker, err := f.base.st.CreatePendingRunOutcomeV1(
		t.Context(), f.identity, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.PrepareBriefDraftV1(
		t.Context(), f.identity, f.ref, marker,
		f.batchID, f.deliveryAt[0],
		[]int64{f.deliveryID[0]},
	); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.base.st.LoadCanonicalBriefForFeedbackV1(
		t.Context(), f.identity.UserID, f.deliveryID[0], f.batchID,
	); err != nil || found {
		t.Fatalf("pending stage feedback read found=%t err=%v", found, err)
	}
}

func TestLoadCanonicalBriefForFeedbackV1TakesAdmissionBeforeChildLocks(
	t *testing.T,
) {
	f := newCanonicalBriefFixture(t, 1)
	outcome := f.finalizedContentOutcome(t)
	brief, err := f.base.st.FreezeBriefV1(
		t.Context(), f.identity, f.ref, f.draft(t, outcome))
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := f.base.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := lockTenantAdmissionRoot(
		t.Context(), blocker, f.identity.TenantID,
	); err != nil {
		t.Fatal(err)
	}

	type readResult struct {
		found bool
		err   error
	}
	readCh := make(chan readResult, 1)
	readCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		_, found, readErr :=
			f.base.st.LoadCanonicalBriefForFeedbackV1(
				readCtx, f.identity.UserID,
				brief.Insights[0].ID, f.batchID,
			)
		readCh <- readResult{found: found, err: readErr}
	}()

	waitDeadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		err := f.base.st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND wait_event_type='Lock'
				   AND wait_event='advisory'
				   AND query LIKE '%pg_advisory_xact_lock_shared%'
			)`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("canonical feedback reader did not wait at admission root")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := blocker.Exec(t.Context(),
		`SET LOCAL lock_timeout='1s'`); err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(t.Context(), `
		SELECT b.id,d.id
		  FROM push_batches b
		  JOIN deliveries d
		    ON d.batch_id=b.id
		   AND d.tenant_id=b.tenant_id
		   AND d.user_id=b.user_id
		 WHERE b.id=$1 AND d.id=$2
		 FOR UPDATE OF b,d`,
		f.batchID, brief.Insights[0].ID,
	); err != nil {
		t.Fatalf("reader locked child before tenant admission: %v", err)
	}
	if err := blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-readCh:
		if result.err != nil || !result.found {
			t.Fatalf("canonical feedback read found=%t err=%v",
				result.found, result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canonical feedback reader did not drain after admission release")
	}
}
