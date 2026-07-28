package store

import (
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestLegacyFeedbackRepairReplaySuppressesPairedNegatives(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	f.createClaimProfile(t, "", "", nil)

	key := "legacy-paired-replay-" + uuid.NewString()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, key,
	)
	if err != nil {
		t.Fatal(err)
	}

	type pair struct {
		negativeID int64
		typedID    int64
	}
	pairs := make([]pair, 2)
	for i := range pairs {
		contentID := f.createContent(
			t, f.sourceA, "legacy-paired-replay-"+uuid.NewString(),
		)
		deliveryID, _, _, insertErr :=
			f.base.st.InsertDeliveryForTaskRunV1(
				ctx, f.idA, f.refA, key, &types.Delivery{
					BatchID: batchID, UserID: f.idA.UserID,
					ContentItemID: &contentID, BodyMD: "legacy paired",
				},
			)
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		if err := f.base.st.pool.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,detail
			 ) VALUES ($1,$2,$3,'not_interested','')
			 RETURNING id`,
			f.idA.TenantID, f.idA.UserID, deliveryID,
		).Scan(&pairs[i].negativeID); err != nil {
			t.Fatal(err)
		}
		if err := f.base.st.pool.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,detail
			 ) VALUES ($1,$2,$3,'misjudged',$4)
			 RETURNING id`,
			f.idA.TenantID, f.idA.UserID, deliveryID,
			"这都三个月前的内容了",
		).Scan(&pairs[i].typedID); err != nil {
			t.Fatal(err)
		}
	}
	profile, err := f.base.st.GetProfileForTenant(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.st.AdvanceProfileCursor(
		ctx, f.idA.UserID, pairs[1].typedID,
		profile.UpdatedAt, profile.LastEvolvedFeedbackID,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		feedbackIDs := []int64{
			pairs[0].negativeID, pairs[0].typedID,
			pairs[1].negativeID, pairs[1].typedID,
		}
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=ANY($1)`, feedbackIDs)
	})

	preview, err := f.base.st.PreviewLegacyFeedbackRepair(
		ctx, f.idA.TenantID, f.idA.UserID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Items) != 2 ||
		preview.ReplayFromID != pairs[0].typedID {
		t.Fatalf("preview=%+v", preview)
	}
	if _, err := f.base.st.ApplyLegacyFeedbackRepair(
		ctx, f.idA.TenantID, f.idA.UserID, preview.Digest,
	); err != nil {
		t.Fatal(err)
	}

	profile, err = f.base.st.GetProfileForTenant(
		ctx, f.idA.TenantID, f.idA.UserID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := pairs[0].typedID - 1; profile.LastEvolvedFeedbackID != want {
		t.Fatalf(
			"repair cursor=%d want=%d",
			profile.LastEvolvedFeedbackID, want,
		)
	}
	rows, err := f.base.st.ListFeedbacksForEvolutionForTenant(
		ctx, f.idA.TenantID, f.idA.UserID,
		profile.LastEvolvedFeedbackID, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 ||
		rows[0].ID != pairs[0].typedID ||
		rows[1].ID != pairs[1].typedID {
		t.Fatalf(
			"replay must contain only typed problem rows, got %+v",
			rows,
		)
	}
	for _, row := range rows {
		if row.Action != types.FeedbackActionMisjudged ||
			row.ReasonCode != types.FeedbackReasonOutdated {
			t.Fatalf("unexpected replay row: %+v", row)
		}
	}
}
