package store

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestInferLegacyFeedbackReason(t *testing.T) {
	feedbackCreatedAt := time.Date(2026, 7, 25, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		detail string
		want   types.FeedbackReason
	}{
		{"existing relative month", "这都3个月前的内容了", types.FeedbackReasonOutdated},
		{"relative Chinese one month content complaint", "一个月前的内容了啊", types.FeedbackReasonOutdated},
		{"relative days plus delayed delivery complaint", "3天前的内容怎么现在才推", types.FeedbackReasonOutdated},
		{"relative Chinese number plus delayed delivery complaint", "三天前的内容怎么现在才推", types.FeedbackReasonOutdated},
		{"absolute old ISO date with explicit complaint", "2025-10-15 的文章为什么现在才推？", types.FeedbackReasonOutdated},
		{"absolute old Chinese date with explicit complaint", "2025年10月15号的内容现在还推。", types.FeedbackReasonOutdated},
		{"absolute date content suffix requires human review", "2025年10月15日 的内容啊", types.FeedbackReasonOther},
		{"future date cannot be stale", "2099-10-15 的内容为什么现在才推", types.FeedbackReasonOther},
		{"future date cannot be bypassed by relative fallback", "2099-10-15，一个月前的内容了啊", types.FeedbackReasonOther},
		{"duplicate", "这条已经推过", types.FeedbackReasonDuplicate},
		{"duplicate wins over freshness", "3天前才推的这条已经推过", types.FeedbackReasonDuplicate},
		{"duplicate wins over relative content age", "两个月前的文章已经推过", types.FeedbackReasonDuplicate},
		{"factually wrong", "结论不对", types.FeedbackReasonFactWrong},
		{"factually wrong wins over freshness", "三天前的内容怎么现在才推，结论不对", types.FeedbackReasonFactWrong},
		{"poor source", "来源质量太差", types.FeedbackReasonPoorSource},
		{"poor source wins over freshness", "3天前的内容现在才推，来源质量太差", types.FeedbackReasonPoorSource},
		{"not relevant", "和我的任务无关", types.FeedbackReasonNotRelevant},
		{"not relevant wins over freshness", "3天前的内容怎么现在才推，和我的任务无关", types.FeedbackReasonNotRelevant},
		{"ordinary today reference", "今天的内容我晚点再看", types.FeedbackReasonOther},
		{"ordinary absolute date reference", "今天安排复盘 2025年10月15日 的内容", types.FeedbackReasonOther},
		{"ordinary date without content complaint", "日期写成 2025年10月15日 就行", types.FeedbackReasonOther},
		{"ordinary non-temporal content", "这篇内容很有意思", types.FeedbackReasonOther},
		{"relative age without content object", "一个月前开会讨论过", types.FeedbackReasonOther},
		{"ordinary relative content note", "我正在整理一个月前的内容，没问题", types.FeedbackReasonOther},
		{"distant relative age and content", "一个月前开会讨论过内容，排版糟糕", types.FeedbackReasonOther},
		{"relative content request with ah", "我需要一个月前的内容啊", types.FeedbackReasonOther},
		{"relative news request with ne", "可以看一个月前的新闻呢？", types.FeedbackReasonOther},
		{"vague delayed wording", "这个内容才推", types.FeedbackReasonOther},
		{"other", "排版很糟糕", types.FeedbackReasonOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inferLegacyFeedbackReason(
				tc.detail,
				feedbackCreatedAt,
			); got != tc.want {
				t.Errorf("inferLegacyFeedbackReason(%q)=%q want=%q", tc.detail, got, tc.want)
			}
		})
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
	f.createClaimProfile(t, "", "", nil)
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
