package store

import (
	"reflect"
	"testing"
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
