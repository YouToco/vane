package store

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestCompiledPromptReads_AreExactTenantScoped(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()

	f.createClaimProfile(t, "tenant-a-profile", "", nil)
	profile, err := f.base.st.GetProfileForTenant(ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil || profile.Industry != "tenant-a-profile" {
		t.Fatalf("tenant A exact profile = %+v, err=%v", profile, err)
	}
	if _, err := f.base.st.GetProfileForTenant(ctx, f.idB.TenantID, f.idB.UserID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("tenant B read tenant A profile: %v", err)
	}

	contentA := f.createContent(t, f.sourceA, "negative-from-a")
	contentB := f.createContent(t, f.sourceB, "negative-from-b")
	keyA := "prompt-read-a-" + uuid.NewString()
	keyB := "prompt-read-b-" + uuid.NewString()
	batchA, err := f.base.st.CreatePushBatchForTaskRunV1(ctx, f.idA, f.refA, keyA)
	if err != nil {
		t.Fatal(err)
	}
	batchB, err := f.base.st.CreatePushBatchForTaskRunV1(ctx, f.idB, f.refB, keyB)
	if err != nil {
		t.Fatal(err)
	}
	deliveryA, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, keyA,
		&types.Delivery{BatchID: batchA, UserID: f.idA.UserID, ContentItemID: &contentA})
	if err != nil {
		t.Fatal(err)
	}
	deliveryB, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idB, f.refB, keyB,
		&types.Delivery{BatchID: batchB, UserID: f.idB.UserID, ContentItemID: &contentB})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.st.pool.Exec(ctx,
		`INSERT INTO feedbacks (
		     tenant_id, user_id, delivery_id, action, reason_code
		 )
		 VALUES ($1, $3, $4, 'not_interested', NULL),
		        ($2, $3, $5, 'misjudged', 'not_relevant')`,
		f.idA.TenantID, f.idB.TenantID, f.idA.UserID, deliveryA, deliveryB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedbacks WHERE delivery_id IN ($1, $2)`, deliveryA, deliveryB)
	})

	since := time.Now().Add(-time.Hour)
	titlesA, err := f.base.st.ListRecentNegativeFeedbackTitlesForTenant(
		ctx, f.idA.TenantID, f.idA.UserID, since, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(titlesA) != 1 || titlesA[0] != "negative-from-a" {
		t.Fatalf("tenant A negative titles = %v", titlesA)
	}
	titlesB, err := f.base.st.ListRecentNegativeFeedbackTitlesForTenant(
		ctx, f.idB.TenantID, f.idB.UserID, since, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(titlesB) != 1 || titlesB[0] != "negative-from-b" {
		t.Fatalf("tenant B negative titles = %v", titlesB)
	}
}
