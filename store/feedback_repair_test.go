package store

import (
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestInferLegacyFeedbackReason(t *testing.T) {
	cases := map[string]types.FeedbackReason{
		"这都3个月前的内容了": types.FeedbackReasonOutdated,
		"这条已经推过":     types.FeedbackReasonDuplicate,
		"结论不对":       types.FeedbackReasonFactWrong,
		"来源质量太差":     types.FeedbackReasonPoorSource,
		"和我的任务无关":    types.FeedbackReasonNotRelevant,
		"排版很糟糕":      types.FeedbackReasonOther,
	}
	for detail, want := range cases {
		if got := inferLegacyFeedbackReason(detail); got != want {
			t.Errorf("inferLegacyFeedbackReason(%q)=%q want=%q", detail, got, want)
		}
	}
}

func TestFeedbackRepairDigestBindsEveryPreviewField(t *testing.T) {
	plan := FeedbackRepairPlan{
		TenantID: 1, UserID: 2, CurrentEvolutionCursor: 9, ReplayFromID: 7,
		Items: []FeedbackRepairItem{{
			FeedbackID: 7, DeliveryID: 8, Detail: "三个月前",
			ProposedReason: types.FeedbackReasonOutdated, WasConsumed: true,
		}},
	}
	first, err := feedbackRepairDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Items[0].Detail = "changed"
	second, err := feedbackRepairDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("repair digest did not bind item detail")
	}
}

func TestApplyLegacyFeedbackRepairCreatesPendingOutdatedTriage(t *testing.T) {
	f := newCompiledRunWriteFixture(t)
	ctx := t.Context()
	if _, err := f.base.st.pool.Exec(ctx,
		`INSERT INTO profiles (tenant_id,user_id)
		 VALUES ($1,$2)`, f.idA.TenantID, f.idA.UserID); err != nil {
		t.Fatal(err)
	}
	key := "legacy-repair-" + uuid.NewString()
	batchID, err := f.base.st.CreatePushBatchForTaskRunV1(
		ctx, f.idA, f.refA, key)
	if err != nil {
		t.Fatal(err)
	}
	contentID := f.createContent(t, f.sourceA, "legacy-repair")
	deliveryID, _, _, err := f.base.st.InsertDeliveryForTaskRunV1(
		ctx, f.idA, f.refA, key, &types.Delivery{
			BatchID: batchID, UserID: f.idA.UserID,
			ContentItemID: &contentID, BodyMD: "legacy old",
		})
	if err != nil {
		t.Fatal(err)
	}
	var feedbackID int64
	if err := f.base.st.pool.QueryRow(ctx,
		`INSERT INTO feedbacks (
		     tenant_id,user_id,delivery_id,action,detail
		 ) VALUES ($1,$2,$3,'misjudged','这都3个月前的内容了')
		 RETURNING id`,
		f.idA.TenantID, f.idA.UserID, deliveryID).Scan(&feedbackID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM feedbacks WHERE id=$1`, feedbackID)
		cleanupExec(cleanupCtx, t, f.base.st,
			`DELETE FROM profiles WHERE tenant_id=$1 AND user_id=$2`,
			f.idA.TenantID, f.idA.UserID)
	})
	preview, err := f.base.st.PreviewLegacyFeedbackRepair(
		ctx, f.idA.TenantID, f.idA.UserID)
	if err != nil || len(preview.Items) != 1 ||
		preview.Items[0].ProposedReason != types.FeedbackReasonOutdated {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	if _, err := f.base.st.ApplyLegacyFeedbackRepair(
		ctx, f.idA.TenantID, f.idA.UserID, preview.Digest); err != nil {
		t.Fatal(err)
	}
	var reason, status string
	if err := f.base.st.pool.QueryRow(ctx,
		`SELECT reason_code,status
		   FROM feedback_freshness_triage WHERE feedback_id=$1`,
		feedbackID).Scan(&reason, &status); err != nil ||
		reason != string(types.FeedbackReasonOutdated) || status != "pending" {
		t.Fatalf("triage reason=%q status=%q err=%v", reason, status, err)
	}
}
