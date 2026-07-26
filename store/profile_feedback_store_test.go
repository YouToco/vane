package store

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestProfileStore 是 DATABASE_URL 门控的集成测试（无则跳过，与 pipeline_store_test.go
// 同一模式），覆盖 M5 契约 §15 store/profiles 段：UpsertProfileFields 首采 INSERT /
// 部分更新 nil 不改 / 不触 summary 与游标 / tags 截 12；EvolveProfile (updated_at, 游标)
// 双条件 CAS；AdvanceProfileCursor 不刷 updated_at 且校验旧游标。
func TestProfileStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 profile store 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	u, err := st.UpsertUserByOpenID(ctx, "test_profile_"+uuid.NewString(), "profile-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序：profiles → users。
		cleanupExec(ctx, t, st, `DELETE FROM profiles WHERE user_id = $1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	t.Run("首采INSERT与部分更新nil不改", func(t *testing.T) {
		if _, err := st.GetProfile(ctx, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("无画像时 GetProfile 应 ErrNotFound，实际: %v", err)
		}

		ind, occ := "科技", "后端工程师"
		p, err := st.UpsertProfileFields(ctx, u.ID, &ind, &occ, []string{"Go", "AI"})
		if err != nil {
			t.Fatalf("UpsertProfileFields() 首采失败: %v", err)
		}
		if p.Industry != "科技" || p.Occupation != "后端工程师" {
			t.Errorf("首采回读不一致: industry=%q occupation=%q", p.Industry, p.Occupation)
		}
		if len(p.Tags) != 2 || p.Tags[0] != "Go" || p.Tags[1] != "AI" {
			t.Errorf("首采 tags 回读不一致: %v", p.Tags)
		}
		if p.Summary != "" || p.LastEvolvedFeedbackID != 0 {
			t.Errorf("首采不应带 summary/游标: summary=%q cursor=%d", p.Summary, p.LastEvolvedFeedbackID)
		}

		// 部分更新：只给 occupation，industry/tags 传 nil 不改。
		occ2 := "架构师"
		p2, err := st.UpsertProfileFields(ctx, u.ID, nil, &occ2, nil)
		if err != nil {
			t.Fatalf("UpsertProfileFields() 部分更新失败: %v", err)
		}
		if p2.Industry != "科技" {
			t.Errorf("industry 传 nil 不应被改: %q", p2.Industry)
		}
		if p2.Occupation != "架构师" {
			t.Errorf("occupation 应更新为架构师: %q", p2.Occupation)
		}
		if len(p2.Tags) != 2 || p2.Tags[0] != "Go" {
			t.Errorf("tags 传 nil 不应被改: %v", p2.Tags)
		}
		// 人工写恒刷 updated_at（并发演化 CAS 失效退让的依据）。
		if !p2.UpdatedAt.After(p.UpdatedAt) {
			t.Errorf("Upsert 应刷新 updated_at：前 %v，后 %v", p.UpdatedAt, p2.UpdatedAt)
		}

		got, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		if got.Occupation != "架构师" || got.UserID != u.ID {
			t.Errorf("GetProfile 回读不一致: %+v", got)
		}
	})

	t.Run("Upsert不触summary与游标", func(t *testing.T) {
		p, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		// 先经演化写入 summary 与游标。
		if err := st.EvolveProfile(ctx, u.ID, "对 Go 与 AI 工程实践感兴趣",
			[]string{"Go", "AI", "LLM"}, 42, p.UpdatedAt, p.LastEvolvedFeedbackID); err != nil {
			t.Fatalf("EvolveProfile() 失败: %v", err)
		}

		ind := "互联网"
		p2, err := st.UpsertProfileFields(ctx, u.ID, &ind, nil, nil)
		if err != nil {
			t.Fatalf("UpsertProfileFields() 失败: %v", err)
		}
		if p2.Summary != "对 Go 与 AI 工程实践感兴趣" {
			t.Errorf("Upsert 不得触碰演化产物 summary: %q", p2.Summary)
		}
		if p2.LastEvolvedFeedbackID != 42 {
			t.Errorf("Upsert 不得触碰演化游标: %d", p2.LastEvolvedFeedbackID)
		}
	})

	t.Run("tags截12", func(t *testing.T) {
		tags := make([]string, 15)
		for i := range tags {
			tags[i] = "标签" + string(rune('A'+i))
		}
		p, err := st.UpsertProfileFields(ctx, u.ID, nil, nil, tags)
		if err != nil {
			t.Fatalf("UpsertProfileFields() 失败: %v", err)
		}
		if len(p.Tags) != 12 {
			t.Fatalf("tags 应截前 12，实际 %d 个: %v", len(p.Tags), p.Tags)
		}
		if p.Tags[0] != "标签A" || p.Tags[11] != "标签L" {
			t.Errorf("截断应保前 12 个（保序）: %v", p.Tags)
		}
	})

	t.Run("EvolveProfile双条件CAS", func(t *testing.T) {
		stale, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		// 人工修正介入（无条件写刷 updated_at）→ 过期 token 演化必须冲突退让。
		if _, err := st.UpsertProfileFields(ctx, u.ID, nil, nil, nil); err != nil {
			t.Fatalf("UpsertProfileFields() 失败: %v", err)
		}
		err = st.EvolveProfile(ctx, u.ID, "过期演化不应写入", nil,
			stale.LastEvolvedFeedbackID+1, stale.UpdatedAt, stale.LastEvolvedFeedbackID)
		if !errors.Is(err, types.ErrConflict) {
			t.Errorf("updated_at 已变，演化应 ErrConflict，实际: %v", err)
		}

		fresh, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		if fresh.Summary == "过期演化不应写入" {
			t.Error("CAS 冲突的演化不应产生任何写入")
		}
		// updated_at 对、游标错 → 同样冲突（审查 F6：封死游标回退）。
		err = st.EvolveProfile(ctx, u.ID, "游标错也不应写入", nil,
			fresh.LastEvolvedFeedbackID+1, fresh.UpdatedAt, fresh.LastEvolvedFeedbackID+999)
		if !errors.Is(err, types.ErrConflict) {
			t.Errorf("游标不符，演化应 ErrConflict，实际: %v", err)
		}

		// 双条件都对 → 成功，summary/tags/游标落库且 updated_at 刷新。
		newTags := []string{"Go", "AI", "LLM", "Rust"}
		if err := st.EvolveProfile(ctx, u.ID, "新摘要。不感兴趣：美股。", newTags,
			fresh.LastEvolvedFeedbackID+7, fresh.UpdatedAt, fresh.LastEvolvedFeedbackID); err != nil {
			t.Fatalf("EvolveProfile() 应成功，实际: %v", err)
		}
		got, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		if got.Summary != "新摘要。不感兴趣：美股。" || len(got.Tags) != 4 {
			t.Errorf("演化写入回读不一致: summary=%q tags=%v", got.Summary, got.Tags)
		}
		if got.LastEvolvedFeedbackID != fresh.LastEvolvedFeedbackID+7 {
			t.Errorf("游标应推进到 %d，实际 %d", fresh.LastEvolvedFeedbackID+7, got.LastEvolvedFeedbackID)
		}
		if !got.UpdatedAt.After(fresh.UpdatedAt) {
			t.Errorf("演化写应刷新 updated_at：前 %v，后 %v", fresh.UpdatedAt, got.UpdatedAt)
		}
		// 演化输出面收窄：industry/occupation 不在演化列清单内。
		if got.Industry != fresh.Industry || got.Occupation != fresh.Occupation {
			t.Errorf("演化不得触碰 industry/occupation: %q/%q", got.Industry, got.Occupation)
		}
	})

	t.Run("AdvanceProfileCursor不刷updated_at且校验旧游标", func(t *testing.T) {
		p, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		if err := st.AdvanceProfileCursor(ctx, u.ID, p.LastEvolvedFeedbackID+10,
			p.UpdatedAt, p.LastEvolvedFeedbackID); err != nil {
			t.Fatalf("AdvanceProfileCursor() 应成功，实际: %v", err)
		}
		got, err := st.GetProfile(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		if got.LastEvolvedFeedbackID != p.LastEvolvedFeedbackID+10 {
			t.Errorf("游标应推进到 %d，实际 %d", p.LastEvolvedFeedbackID+10, got.LastEvolvedFeedbackID)
		}
		// 关键语义：推进游标不是"画像变更"，不得刷 updated_at。
		if !got.UpdatedAt.Equal(p.UpdatedAt) {
			t.Errorf("AdvanceProfileCursor 不应刷新 updated_at：前 %v，后 %v", p.UpdatedAt, got.UpdatedAt)
		}
		if got.Summary != p.Summary {
			t.Errorf("AdvanceProfileCursor 不得触碰画像内容: %q", got.Summary)
		}
		// 拿旧游标再推 → 冲突（游标已被上一次推进，双条件之一失败）。
		err = st.AdvanceProfileCursor(ctx, u.ID, p.LastEvolvedFeedbackID+20,
			p.UpdatedAt, p.LastEvolvedFeedbackID)
		if !errors.Is(err, types.ErrConflict) {
			t.Errorf("旧游标推进应 ErrConflict，实际: %v", err)
		}
		// updated_at 未被推进动过 → 拿新游标可继续推。
		if err := st.AdvanceProfileCursor(ctx, u.ID, p.LastEvolvedFeedbackID+20,
			p.UpdatedAt, p.LastEvolvedFeedbackID+10); err != nil {
			t.Errorf("正确游标续推应成功，实际: %v", err)
		}
	})

	t.Run("removed_tags黑名单维护", func(t *testing.T) {
		// Gate ⑧ FAIL 回归（migration 014）：人工删标签入黑名单、加回出黑名单、
		// nil tags 不触碰。独立用户隔离，不依赖前序子测试的画像状态。
		uRM, err := st.UpsertUserByOpenID(ctx, "test_profile_rm_"+uuid.NewString(), "profile-rm-test")
		if err != nil {
			t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
		}
		attachTenant(t, st, uRM.ID)
		t.Cleanup(func() {
			ctx, cancel := cleanupContext()
			defer cancel()
			cleanupExec(ctx, t, st, `DELETE FROM profiles WHERE user_id = $1`, uRM.ID)
			cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, uRM.ID)
		})
		// 黑名单语义是集合，比较序无关：array_agg ORDER BY 的具体顺序取决于库
		// collation（中文在 en_US.UTF-8 与 C 下排序不同），断言顺序会跨环境碎。
		eq := func(t *testing.T, got, want []string, label string) {
			t.Helper()
			g := append([]string(nil), got...)
			w := append([]string(nil), want...)
			sort.Strings(g)
			sort.Strings(w)
			if len(g) != len(w) {
				t.Fatalf("%s: 应为 %v，实际 %v", label, want, got)
			}
			for i := range w {
				if g[i] != w[i] {
					t.Fatalf("%s: 应为 %v，实际 %v", label, want, got)
				}
			}
		}

		p, err := st.UpsertProfileFields(ctx, uRM.ID, nil, nil, []string{"甲", "乙", "丙"})
		if err != nil {
			t.Fatalf("首采失败: %v", err)
		}
		eq(t, p.RemovedTags, []string{}, "首采黑名单应为空")

		// 删「乙」→ 入列。
		p, err = st.UpsertProfileFields(ctx, uRM.ID, nil, nil, []string{"甲", "丙"})
		if err != nil {
			t.Fatalf("删乙失败: %v", err)
		}
		eq(t, p.RemovedTags, []string{"乙"}, "删乙后")

		// 再删「丙」同时新增「丁」→ 乙丙都在列（array_agg 按文本序）。
		p, err = st.UpsertProfileFields(ctx, uRM.ID, nil, nil, []string{"甲", "丁"})
		if err != nil {
			t.Fatalf("删丙加丁失败: %v", err)
		}
		eq(t, p.RemovedTags, []string{"乙", "丙"}, "删丙加丁后")

		// nil tags（只改 occupation）→ 黑名单不动。
		occ := "验证员"
		p, err = st.UpsertProfileFields(ctx, uRM.ID, nil, &occ, nil)
		if err != nil {
			t.Fatalf("nil tags 更新失败: %v", err)
		}
		eq(t, p.RemovedTags, []string{"乙", "丙"}, "nil tags 后黑名单不动")

		// 人工加回「乙」→ 出列，「丙」留列。
		p, err = st.UpsertProfileFields(ctx, uRM.ID, nil, nil, []string{"甲", "丁", "乙"})
		if err != nil {
			t.Fatalf("加回乙失败: %v", err)
		}
		eq(t, p.RemovedTags, []string{"丙"}, "加回乙后")

		// GetProfile 回读一致（RETURNING 与 SELECT 同列序）。
		got, err := st.GetProfile(ctx, uRM.ID)
		if err != nil {
			t.Fatalf("GetProfile() 失败: %v", err)
		}
		eq(t, got.RemovedTags, []string{"丙"}, "GetProfile 回读")
	})
}

// TestFeedbackStore 是 DATABASE_URL 门控的集成测试，覆盖 M5 契约 §15 的
// store/feedbacks 与 store/deliveries 段：非法 action 校验、LatestFeedbackAction
// 排序与集合语义、InsertDeepDiveFeedback 幂等双击（实测 006 部分唯一索引 arbiter）、
// ListFeedbacksForEvolution 边界、ListRecentNegativeFeedbackTitles per-delivery
// 最新态度过滤（审查 F2 定向用例）、GetDeliveryByFeishuMessageID 双保险、
// MarkDeliverySent 回填 cardJSON。
func TestFeedbackStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 feedback store 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	u, err := st.UpsertUserByOpenID(ctx, "test_feedback_"+uuid.NewString(), "feedback-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}
	attachTenant(t, st, u.ID)
	// u2 供负面清单与归属校验用例：其反馈与 u 完全隔离，互不污染 per-user 查询。
	u2, err := st.UpsertUserByOpenID(ctx, "test_feedback2_"+uuid.NewString(), "feedback-test-2")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() u2 失败: %v", err)
	}
	attachTenant(t, st, u2.ID)
	userIDs := []int64{u.ID, u2.ID}

	srcID, _, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        "https://example.com/test-feedback-" + uuid.NewString(),
		Title:      "feedback-test-source",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 失败: %v", err)
	}
	batchID, err := st.CreatePushBatch(ctx, u.ID)
	if err != nil {
		t.Fatalf("CreatePushBatch() 失败: %v", err)
	}
	batchID2, err := st.CreatePushBatch(ctx, u2.ID)
	if err != nil {
		t.Fatalf("CreatePushBatch() u2 失败: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序：feedbacks → deliveries → push_batches → content_items → sources → users。
		cleanupExec(ctx, t, st, `DELETE FROM feedbacks WHERE user_id = ANY($1)`, userIDs)
		cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE user_id = ANY($1)`, userIDs)
		cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE user_id = ANY($1)`, userIDs)
		cleanupExec(ctx, t, st, `DELETE FROM content_items WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = ANY($1)`, userIDs)
	})

	newContent := func(t *testing.T, title, content string) int64 {
		t.Helper()
		// CanonicalKey 每条唯一：007 起 content_items 按 canonical_key 全局唯一，
		// 留空会让所有 fixture 撞在同一个空串上 —— 第二条起静默返回首条的 id
		// （UpsertContentItem 冲突即回查），newContent 就再也建不出第二条内容。
		id, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "m5-" + uuid.NewString(),
			CanonicalKey: "https://example.com/m5-item-" + uuid.NewString(),
			URL:          "https://example.com/m5-item",
			Title:        title,
			Content:      content,
			ContentHash:  "m5hash-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("UpsertContentItem() 失败: %v", err)
		}
		return id
	}
	newDelivery := func(t *testing.T, userID, batch int64, contentID *int64, bodyMD string) int64 {
		t.Helper()
		id, err := st.InsertDelivery(ctx, &types.Delivery{
			BatchID: batch, UserID: userID, ContentItemID: contentID, Score: 55, BodyMD: bodyMD,
		})
		if err != nil {
			t.Fatalf("InsertDelivery() 失败: %v", err)
		}
		return id
	}
	addFeedback := func(t *testing.T, userID, deliveryID int64, action types.FeedbackAction, detail string) int64 {
		t.Helper()
		id, err := st.InsertFeedback(ctx, &types.Feedback{
			UserID: userID, DeliveryID: deliveryID, Action: action, Detail: detail,
		})
		if err != nil {
			t.Fatalf("InsertFeedback(%s) 失败: %v", action, err)
		}
		return id
	}
	addReasonFeedback := func(t *testing.T, userID, deliveryID int64, reason types.FeedbackReason, detail string) int64 {
		t.Helper()
		id, err := st.InsertFeedback(ctx, &types.Feedback{
			UserID: userID, DeliveryID: deliveryID, Action: types.FeedbackActionMisjudged,
			ReasonCode: reason, Detail: detail,
		})
		if err != nil {
			t.Fatalf("InsertFeedback(misjudged,%s) 失败: %v", reason, err)
		}
		return id
	}

	t.Run("非法action返回Validation", func(t *testing.T) {
		ci := newContent(t, "校验用内容", "正文")
		d := newDelivery(t, u.ID, batchID, &ci, "")
		_, err := st.InsertFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d, Action: "yolo",
		})
		if !errors.Is(err, types.ErrValidation) {
			t.Errorf("非法 action 应 ErrValidation，实际: %v", err)
		}
	})

	t.Run("misjudged原因校验与历史行读取", func(t *testing.T) {
		ci := newContent(t, "原因校验内容", "正文")
		d := newDelivery(t, u.ID, batchID, &ci, "")
		if _, err := st.InsertFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d, Action: types.FeedbackActionMisjudged,
			ReasonCode: types.FeedbackReason("forged"),
		}); !errors.Is(err, types.ErrValidation) {
			t.Errorf("伪造 reason_code 应 ErrValidation，实际: %v", err)
		}
		if _, err := st.InsertFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d,
			Action: types.FeedbackActionMisjudged, Detail: "新回调不得为空原因",
		}); !errors.Is(err, types.ErrValidation) {
			t.Errorf("新 misjudged 空原因应 ErrValidation，实际: %v", err)
		}
		// 发版前已存在的空 reason_code 行仍必须可审计读取；新应用写入
		// 已被上面的固定原因约束封住。
		var legacyID int64
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,detail
			 ) VALUES (
			     (SELECT tenant_id FROM deliveries WHERE id=$2),
			     $1,$2,'misjudged','旧卡补充原因'
			 )
			 RETURNING id`,
			u.ID, d).Scan(&legacyID); err != nil {
			t.Fatal(err)
		}
		rows, err := st.ListFeedbacksForEvolution(ctx, u.ID, legacyID-1, 10)
		if err != nil || len(rows) != 1 || rows[0].ReasonCode != "" || rows[0].Detail != "旧卡补充原因" {
			t.Fatalf("旧卡空原因必须可审计读取: rows=%+v err=%v", rows, err)
		}
	})

	t.Run("LatestFeedbackAction排序与集合语义", func(t *testing.T) {
		ci := newContent(t, "态度切换内容", "正文")
		d := newDelivery(t, u.ID, batchID, &ci, "")
		attitudes := []types.FeedbackAction{
			types.FeedbackActionInterested, types.FeedbackActionNotInterested,
		}

		// 无反馈 → NotFound。
		if _, err := st.LatestFeedbackAction(ctx, d, attitudes); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("无反馈应 ErrNotFound，实际: %v", err)
		}
		// 空集合是调用方 bug → Validation 而非伪装成"无反馈"。
		if _, err := st.LatestFeedbackAction(ctx, d, nil); !errors.Is(err, types.ErrValidation) {
			t.Errorf("空动作集合应 ErrValidation，实际: %v", err)
		}

		addFeedback(t, u.ID, d, types.FeedbackActionInterested, "")
		addFeedback(t, u.ID, d, types.FeedbackActionNotInterested, "")
		addReasonFeedback(t, u.ID, d, types.FeedbackReasonOther, "问题")

		// 双值集合：最新态度 = not_interested（misjudged 不在集合内，不干扰）。
		got, err := st.LatestFeedbackAction(ctx, d, attitudes)
		if err != nil {
			t.Fatalf("LatestFeedbackAction() 失败: %v", err)
		}
		if got != types.FeedbackActionNotInterested {
			t.Errorf("双值集合最新应为 not_interested，实际 %q", got)
		}
		// 集合语义：单值集合会命中旧行——这正是审查 F5 要求调用点恒传双值的原因。
		got, err = st.LatestFeedbackAction(ctx, d, []types.FeedbackAction{types.FeedbackActionInterested})
		if err != nil {
			t.Fatalf("LatestFeedbackAction(单值) 失败: %v", err)
		}
		if got != types.FeedbackActionInterested {
			t.Errorf("单值集合应命中旧 interested 行，实际 %q", got)
		}
		// 三连击：改回 interested 后双值集合最新翻转（追加式日志、最新为准）。
		addFeedback(t, u.ID, d, types.FeedbackActionInterested, "")
		got, err = st.LatestFeedbackAction(ctx, d, attitudes)
		if err != nil {
			t.Fatalf("LatestFeedbackAction() 失败: %v", err)
		}
		if got != types.FeedbackActionInterested {
			t.Errorf("三连击后最新应为 interested，实际 %q", got)
		}

		// HasFeedback：misjudged 已有、deep_dive 未有。
		has, err := st.HasFeedback(ctx, d, types.FeedbackActionMisjudged)
		if err != nil || !has {
			t.Errorf("HasFeedback(misjudged) 应 true，实际 (%v, %v)", has, err)
		}
		has, err = st.HasFeedback(ctx, d, types.FeedbackActionDeepDive)
		if err != nil || has {
			t.Errorf("HasFeedback(deep_dive) 应 false，实际 (%v, %v)", has, err)
		}
	})

	t.Run("InsertDeepDiveFeedback双击幂等回传detail", func(t *testing.T) {
		ci := newContent(t, "深度解读内容", "正文")
		d := newDelivery(t, u.ID, batchID, &ci, "")

		// action 非 deep_dive 是调用方 bug。
		if _, _, _, err := st.InsertDeepDiveFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d, Action: types.FeedbackActionInterested,
		}); !errors.Is(err, types.ErrValidation) {
			t.Errorf("非 deep_dive action 应 ErrValidation，实际: %v", err)
		}

		id1, detail1, existed1, err := st.InsertDeepDiveFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d, Action: types.FeedbackActionDeepDive,
			Detail: "第一次生成的解读正文",
		})
		if err != nil {
			t.Fatalf("InsertDeepDiveFeedback() 首插失败: %v", err)
		}
		if existed1 || detail1 != "" {
			t.Errorf("首插应 existed=false 且无既有 detail，实际 (%v, %q)", existed1, detail1)
		}
		// 双击（竞态对手）：命中 006 部分唯一索引，回传既有行 id 与 detail 供重发（审查 F4）。
		id2, detail2, existed2, err := st.InsertDeepDiveFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d, Action: types.FeedbackActionDeepDive,
			Detail: "并发对手的正文（不应写入）",
		})
		if err != nil {
			t.Fatalf("InsertDeepDiveFeedback() 二插失败: %v", err)
		}
		if !existed2 || id2 != id1 {
			t.Errorf("双击应 existed=true 且同 id：首 %d，二 (%d, existed=%v)", id1, id2, existed2)
		}
		if detail2 != "第一次生成的解读正文" {
			t.Errorf("双击应回传既有 detail 供重发，实际 %q", detail2)
		}
		// 态度行不受 deep_dive 部分唯一索引影响：同 delivery 态度仍可追加。
		addFeedback(t, u.ID, d, types.FeedbackActionInterested, "")
		// 另一 delivery 的 deep_dive 不互相冲突（换内容：同批次同内容会撞 004 唯一索引）。
		ci2 := newContent(t, "深度解读内容二", "正文")
		d2 := newDelivery(t, u.ID, batchID, &ci2, "")
		_, _, existed3, err := st.InsertDeepDiveFeedback(ctx, &types.Feedback{
			UserID: u.ID, DeliveryID: d2, Action: types.FeedbackActionDeepDive, Detail: "另一条",
		})
		if err != nil || existed3 {
			t.Errorf("不同 delivery 的 deep_dive 应各自独立，实际 (existed=%v, %v)", existed3, err)
		}
	})

	t.Run("ListFeedbacksForEvolution边界与JOIN", func(t *testing.T) {
		longContent := strings.Repeat("长", 300)
		ciLong := newContent(t, "长文标题", longContent)
		dC := newDelivery(t, u.ID, batchID, &ciLong, "")
		dNull := newDelivery(t, u.ID, batchID, nil, "") // 内容已清理（content_item_id NULL）

		fid1 := addFeedback(t, u.ID, dC, types.FeedbackActionInterested, "")
		fid2 := addFeedback(t, u.ID, dC, types.FeedbackActionQuestion, "这是啥原理")
		fid3 := addFeedback(t, u.ID, dNull, types.FeedbackActionNotInterested, "")

		// afterID 严格大于：以 fid1-1 为游标恰好取回本子测试三行（此前行 id 更小）。
		rows, err := st.ListFeedbacksForEvolution(ctx, u.ID, fid1-1, 50)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolution() 失败: %v", err)
		}
		if len(rows) != 3 || rows[0].ID != fid1 || rows[1].ID != fid2 || rows[2].ID != fid3 {
			t.Fatalf("应按 id 升序恰好返回 [%d %d %d]，实际 %+v", fid1, fid2, fid3, rows)
		}
		if rows[0].Score != 55 {
			t.Errorf("应 JOIN 出投递当时打分 55，实际 %v", rows[0].Score)
		}
		if rows[0].ContentTitle != "长文标题" {
			t.Errorf("应 JOIN 出内容标题，实际 %q", rows[0].ContentTitle)
		}
		if n := utf8.RuneCountInString(rows[0].ContentExcerpt); n != 200 {
			t.Errorf("摘录应为正文前 200 字符（left 按字符计），实际 %d", n)
		}
		if !strings.HasPrefix(longContent, rows[0].ContentExcerpt) {
			t.Error("摘录应是正文前缀")
		}
		if rows[1].Detail != "这是啥原理" {
			t.Errorf("detail 应原样带出，实际 %q", rows[1].Detail)
		}
		// 内容 NULL：行保留，标题/摘录空串。
		if rows[2].ContentTitle != "" || rows[2].ContentExcerpt != "" {
			t.Errorf("内容已清理行应空标题/摘录，实际 %q/%q", rows[2].ContentTitle, rows[2].ContentExcerpt)
		}

		// afterID = fid1：排除游标行本身。
		rows, err = st.ListFeedbacksForEvolution(ctx, u.ID, fid1, 50)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolution() 失败: %v", err)
		}
		if len(rows) != 2 || rows[0].ID != fid2 {
			t.Errorf("afterID=%d 应从 %d 开始，实际 %+v", fid1, fid2, rows)
		}
		// limit 截断。
		rows, err = st.ListFeedbacksForEvolution(ctx, u.ID, fid1-1, 2)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolution() 失败: %v", err)
		}
		if len(rows) != 2 || rows[1].ID != fid2 {
			t.Errorf("limit=2 应截断为前两行，实际 %+v", rows)
		}
	})

	t.Run("ListFeedbacksForEvolution旧卡双记录按问题原因取代负兴趣", func(t *testing.T) {
		reasons := []types.FeedbackReason{
			types.FeedbackReasonOutdated,
			types.FeedbackReasonNotRelevant,
			types.FeedbackReasonDuplicate,
			types.FeedbackReasonFactWrong,
			types.FeedbackReasonPoorSource,
			types.FeedbackReasonOther,
		}
		for _, reason := range reasons {
			t.Run(string(reason), func(t *testing.T) {
				ci := newContent(t, "旧卡配对-"+string(reason), "正文")
				deliveryID := newDelivery(t, u.ID, batchID, &ci, "")
				legacyID := addFeedback(
					t, u.ID, deliveryID,
					types.FeedbackActionNotInterested, "",
				)
				typedID := addReasonFeedback(
					t, u.ID, deliveryID, reason, "问题原因",
				)

				// limit=1 直接证明取代判断会越过分页边界查找后写入的
				// typed 行；若先分页再在 Go 中过滤，这里会错误返回旧 👎。
				rows, err := st.ListFeedbacksForEvolution(
					ctx, u.ID, legacyID-1, 1,
				)
				if err != nil {
					t.Fatalf("ListFeedbacksForEvolution() 失败: %v", err)
				}
				if len(rows) != 1 || rows[0].ID != typedID ||
					rows[0].ReasonCode != reason {
					t.Fatalf(
						"原因 %q 应取代旧 not_interested，实际 %+v",
						reason, rows,
					)
				}
			})
		}

		ciDetailed := newContent(t, "带说明的明确负兴趣", "正文")
		dDetailed := newDelivery(t, u.ID, batchID, &ciDetailed, "")
		detailedNegativeID := addFeedback(
			t, u.ID, dDetailed,
			types.FeedbackActionNotInterested, "明确不喜欢这个主题",
		)
		detailedTypedID := addReasonFeedback(
			t, u.ID, dDetailed, types.FeedbackReasonOutdated, "同时也过时",
		)
		rows, err := st.ListFeedbacksForEvolution(
			ctx, u.ID, detailedNegativeID-1, 10,
		)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolution() 带 detail 失败: %v", err)
		}
		if len(rows) != 2 || rows[0].ID != detailedNegativeID ||
			rows[1].ID != detailedTypedID {
			t.Fatalf("带 detail 的明确 not_interested 不得被取代，实际 %+v", rows)
		}

		ciUntyped := newContent(t, "旧卡未结构化误判", "正文")
		dUntyped := newDelivery(t, u.ID, batchID, &ciUntyped, "")
		untypedLegacyID := addFeedback(
			t, u.ID, dUntyped, types.FeedbackActionNotInterested, "",
		)
		var untypedMisjudgedID int64
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,detail
			 )
			 VALUES (
			     (SELECT tenant_id FROM deliveries WHERE id=$2),
			     $1,$2,'misjudged','旧卡未结构化说明'
			 )
			 RETURNING id`,
			u.ID, dUntyped,
		).Scan(&untypedMisjudgedID); err != nil {
			t.Fatal(err)
		}
		rows, err = st.ListFeedbacksForEvolution(
			ctx, u.ID, untypedLegacyID-1, 10,
		)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolution() untyped 失败: %v", err)
		}
		if len(rows) != 2 || rows[0].ID != untypedLegacyID ||
			rows[1].ID != untypedMisjudgedID {
			t.Fatalf("未结构化 misjudged 不得取代旧 not_interested，实际 %+v", rows)
		}

		ciPair := newContent(t, "先问题后明确负兴趣", "正文")
		dPair := newDelivery(t, u.ID, batchID, &ciPair, "")
		legacyID := addFeedback(
			t, u.ID, dPair, types.FeedbackActionNotInterested, "",
		)
		typedID := addReasonFeedback(
			t, u.ID, dPair, types.FeedbackReasonOutdated, "过时",
		)
		laterNegativeID := addFeedback(
			t, u.ID, dPair, types.FeedbackActionNotInterested, "",
		)

		ciOther := newContent(t, "另一投递的明确负兴趣", "正文")
		dOther := newDelivery(t, u.ID, batchID, &ciOther, "")
		otherNegativeID := addFeedback(
			t, u.ID, dOther, types.FeedbackActionNotInterested, "",
		)

		rows, err = st.ListFeedbacksForEvolution(
			ctx, u.ID, legacyID-1, 10,
		)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolution() 失败: %v", err)
		}
		wantIDs := []int64{typedID, laterNegativeID, otherNegativeID}
		if len(rows) != len(wantIDs) {
			t.Fatalf("应保留 typed、后续明确负兴趣和不同投递负兴趣，实际 %+v", rows)
		}
		for i, wantID := range wantIDs {
			if rows[i].ID != wantID {
				t.Errorf("rows[%d].ID=%d，期望 %d", i, rows[i].ID, wantID)
			}
		}

		var tenantID int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM deliveries WHERE id=$1`,
			dPair,
		).Scan(&tenantID); err != nil {
			t.Fatal(err)
		}
		rows, err = st.ListFeedbacksForEvolutionForTenant(
			ctx, tenantID, u.ID, legacyID-1, 10,
		)
		if err != nil {
			t.Fatalf("ListFeedbacksForEvolutionForTenant() 失败: %v", err)
		}
		if len(rows) != len(wantIDs) {
			t.Fatalf("精确租户读取应与同租户用户读取一致，实际 %+v", rows)
		}
		for i, wantID := range wantIDs {
			if rows[i].ID != wantID {
				t.Errorf("tenant rows[%d].ID=%d，期望 %d", i, rows[i].ID, wantID)
			}
		}
	})

	t.Run("ListFeedbacksForEvolution取代关系严格租户隔离", func(t *testing.T) {
		ci := newContent(t, "跨租户伪配对", "正文")
		deliveryID := newDelivery(t, u.ID, batchID, &ci, "")
		var tenantA, tenantB int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM deliveries WHERE id=$1`,
			deliveryID,
		).Scan(&tenantA); err != nil {
			t.Fatal(err)
		}
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO tenants (status,plan)
			 VALUES ('active','free')
			 RETURNING id`,
		).Scan(&tenantB); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO memberships (tenant_id,user_id,role)
			 VALUES ($1,$2,'owner')`,
			tenantB, u.ID,
		); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := cleanupContext()
			defer cancel()
			cleanupExec(cleanupCtx, t, st,
				`DELETE FROM feedbacks WHERE tenant_id=$1`, tenantB)
			cleanupExec(cleanupCtx, t, st,
				`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
				tenantB, u.ID)
			cleanupExec(cleanupCtx, t, st,
				`DELETE FROM tenants WHERE id=$1`, tenantB)
		})

		var legacyID, foreignTypedID int64
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action
			 )
			 VALUES ($1,$2,$3,'not_interested')
			 RETURNING id`,
			tenantA, u.ID, deliveryID,
		).Scan(&legacyID); err != nil {
			t.Fatal(err)
		}
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,reason_code
			 )
			 VALUES ($1,$2,$3,'misjudged','outdated_or_out_of_window')
			 RETURNING id`,
			tenantB, u.ID, deliveryID,
		).Scan(&foreignTypedID); err != nil {
			t.Fatal(err)
		}

		rows, err := st.ListFeedbacksForEvolutionForTenant(
			ctx, tenantA, u.ID, legacyID-1, 10,
		)
		if err != nil {
			t.Fatalf("tenant A 读取失败: %v", err)
		}
		if len(rows) != 1 || rows[0].ID != legacyID {
			t.Fatalf("其他租户 typed 行不得取代 tenant A 负兴趣，实际 %+v", rows)
		}

		rows, err = st.ListFeedbacksForEvolutionForTenant(
			ctx, tenantB, u.ID, legacyID-1, 10,
		)
		if err != nil {
			t.Fatalf("tenant B 读取失败: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("feedback 与 delivery 租户不一致时必须不可见，实际 %+v", rows)
		}

		rows, err = st.ListFeedbacksForEvolution(
			ctx, u.ID, legacyID-1, 10,
		)
		if err != nil {
			t.Fatalf("用户全局读取失败: %v", err)
		}
		if len(rows) != 2 || rows[0].ID != legacyID ||
			rows[1].ID != foreignTypedID {
			t.Fatalf("跨租户 typed 行不得压掉旧负兴趣，实际 %+v", rows)
		}
	})

	t.Run("ListRecentNegativeFeedbackTitles最新态度过滤", func(t *testing.T) {
		since := time.Now().Add(-14 * 24 * time.Hour)
		mk := func(title string) int64 {
			ci := newContent(t, title, "正文")
			return newDelivery(t, u2.ID, batchID2, &ci, "")
		}

		dA := mk("量子计算新突破")
		addFeedback(t, u2.ID, dA, types.FeedbackActionNotInterested, "") // 纯负 → 返回
		dB := mk("美股大盘复盘")
		addFeedback(t, u2.ID, dB, types.FeedbackActionNotInterested, "")
		addFeedback(t, u2.ID, dB, types.FeedbackActionInterested, "") // F2：改主意 → 不返回
		dC := mk("AI编程助手评测")
		addReasonFeedback(t, u2.ID, dC, types.FeedbackReasonOutdated, "三个月前")
		addFeedback(t, u2.ID, dC, types.FeedbackActionInterested, "") // misjudged→interested → 不返回
		dD := mk("纯误判内容")
		addReasonFeedback(t, u2.ID, dD, types.FeedbackReasonNotRelevant, "") // 与任务无关 → 返回
		dOld := mk("过时新闻")
		addFeedback(t, u2.ID, dOld, types.FeedbackActionNotInterested, "")
		addReasonFeedback(t, u2.ID, dOld, types.FeedbackReasonOutdated, "三个月前")
		dE := mk("只感兴趣内容")
		addFeedback(t, u2.ID, dE, types.FeedbackActionInterested, "") // 正面 → 不返回
		dG := mk("只追问内容")
		addFeedback(t, u2.ID, dG, types.FeedbackActionQuestion, "追问") // question 不是态度 → 不返回
		// question/deep_dive 不参与态度判定：dA 追加追问后负面态度仍是最新。
		addFeedback(t, u2.ID, dA, types.FeedbackActionQuestion, "追问一下")
		// 同标题第二条 delivery 负反馈 → Go 侧按标题去重只留一条。
		dF := mk("量子计算新突破")
		addFeedback(t, u2.ID, dF, types.FeedbackActionNotInterested, "")

		titles, err := st.ListRecentNegativeFeedbackTitles(ctx, u2.ID, since, 10)
		if err != nil {
			t.Fatalf("ListRecentNegativeFeedbackTitles() 失败: %v", err)
		}
		// 反馈时间倒序：dF（最晚）→ dD → dA（与 dF 同标题被去重）。
		want := []string{"量子计算新突破", "纯误判内容"}
		if len(titles) != len(want) || titles[0] != want[0] || titles[1] != want[1] {
			t.Errorf("负面清单应为 %v（倒序+去重），实际 %v", want, titles)
		}
		for _, banned := range []string{"美股大盘复盘", "AI编程助手评测", "过时新闻", "只感兴趣内容", "只追问内容"} {
			for _, got := range titles {
				if got == banned {
					t.Errorf("%q 不应出现在负面清单（最新态度非负/非态度）", banned)
				}
			}
		}
		// limit 截断保序。
		titles, err = st.ListRecentNegativeFeedbackTitles(ctx, u2.ID, since, 1)
		if err != nil {
			t.Fatalf("ListRecentNegativeFeedbackTitles(limit=1) 失败: %v", err)
		}
		if len(titles) != 1 || titles[0] != "量子计算新突破" {
			t.Errorf("limit=1 应只留最新一条，实际 %v", titles)
		}
		// 时间窗：since 在未来 → 全部落窗外。
		titles, err = st.ListRecentNegativeFeedbackTitles(ctx, u2.ID, time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("ListRecentNegativeFeedbackTitles(未来窗口) 失败: %v", err)
		}
		if len(titles) != 0 {
			t.Errorf("窗口外反馈不应返回，实际 %v", titles)
		}
	})

	t.Run("ListRecentNegativeFeedbackTitles空标题回退正文", func(t *testing.T) {
		// Gate ⑥ 盲区回归：X 官号类内容 title=''，负反馈曾对打分 prompt 不可见。
		// 独立用户隔离，不与「最新态度过滤」子测试的清单断言耦合。
		since := time.Now().Add(-14 * 24 * time.Hour)
		u3, err := st.UpsertUserByOpenID(ctx, "test_feedback3_"+uuid.NewString(), "feedback-test-3")
		if err != nil {
			t.Fatalf("UpsertUserByOpenID() u3 失败: %v", err)
		}
		attachTenant(t, st, u3.ID)
		batchID3, err := st.CreatePushBatch(ctx, u3.ID)
		if err != nil {
			t.Fatalf("CreatePushBatch() u3 失败: %v", err)
		}
		t.Cleanup(func() {
			ctx, cancel := cleanupContext()
			defer cancel()
			cleanupExec(ctx, t, st, `DELETE FROM feedbacks WHERE user_id = $1`, u3.ID)
			cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE user_id = $1`, u3.ID)
			cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE user_id = $1`, u3.ID)
			cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, u3.ID)
		})
		mk := func(title, content string) int64 {
			ci := newContent(t, title, content)
			return newDelivery(t, u3.ID, batchID3, &ci, "")
		}

		// 长正文（>200 字符，含 CJK）验证 left() 按字符截断而非字节。
		longContent := strings.Repeat("模型动态摘要。", 30) // 7 字符 × 30 = 210 字符
		dLong := mk("", longContent)
		addFeedback(t, u3.ID, dLong, types.FeedbackActionNotInterested, "")
		dTitled := mk("有标题内容", "标题应优先于正文")
		addFeedback(t, u3.ID, dTitled, types.FeedbackActionNotInterested, "")
		dBlank := mk("", "")
		addFeedback(t, u3.ID, dBlank, types.FeedbackActionNotInterested, "") // 双空 → 跳过
		// 同正文第二条无标题 delivery → 按回退串去重只留一条。
		dDup := mk("", longContent)
		addReasonFeedback(t, u3.ID, dDup, types.FeedbackReasonNotRelevant, "")

		titles, err := st.ListRecentNegativeFeedbackTitles(ctx, u3.ID, since, 10)
		if err != nil {
			t.Fatalf("ListRecentNegativeFeedbackTitles() 失败: %v", err)
		}
		wantHead := string([]rune(longContent)[:200])
		// 倒序：dDup（与 dLong 回退串相同被去重，留最新一次）→ dBlank 跳过 → dTitled → dLong。
		want := []string{wantHead, "有标题内容"}
		if len(titles) != len(want) || titles[0] != want[0] || titles[1] != want[1] {
			t.Errorf("空标题回退清单应为 %v，实际 %v", want, titles)
		}
	})

	t.Run("GetDeliveryForUser归属与body_md回读", func(t *testing.T) {
		ci := newContent(t, "归属校验内容", "正文")
		body := "**标题**\n一句话摘要\n[阅读原文](https://example.com/a)"
		d := newDelivery(t, u.ID, batchID, &ci, body)

		got, err := st.GetDeliveryForUser(ctx, d, u.ID)
		if err != nil {
			t.Fatalf("GetDeliveryForUser() 失败: %v", err)
		}
		if got.ID != d || got.BodyMD != body || got.Score != 55 {
			t.Errorf("回读不一致: id=%d body=%q score=%v", got.ID, got.BodyMD, got.Score)
		}
		// 越权（按钮 value 可伪造）：他人 / 不存在统一 NotFound、零副作用。
		if _, err := st.GetDeliveryForUser(ctx, d, u2.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("越权读取应 ErrNotFound，实际: %v", err)
		}
		if _, err := st.GetDeliveryForUser(ctx, -1, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("不存在投递应 ErrNotFound，实际: %v", err)
		}
	})

	t.Run("MarkDeliverySent回填cardJSON与消息反查", func(t *testing.T) {
		ci := newContent(t, "反查用内容", "正文")
		d := newDelivery(t, u.ID, batchID, &ci, "解读摘要")

		// 空串短路：库里存在 feishu_message_id='' 的未发送行（d 本身），空串反查
		// 必须 NotFound 而非命中它——Go 短路 + SQL 谓词双保险的行为断言。
		if _, err := st.GetDeliveryByFeishuMessageID(ctx, u.ID, ""); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("空 message_id 应 ErrNotFound，实际: %v", err)
		}

		msgID := "om_" + uuid.NewString()
		card := json.RawMessage(`{"schema":"2.0","header":{"title":"x"}}`)
		sentAt := time.Now()
		if err := st.MarkDeliverySent(ctx, d, msgID, card, sentAt); err != nil {
			t.Fatalf("MarkDeliverySent() 失败: %v", err)
		}

		got, err := st.GetDeliveryByFeishuMessageID(ctx, u.ID, msgID)
		if err != nil {
			t.Fatalf("GetDeliveryByFeishuMessageID() 失败: %v", err)
		}
		if got.ID != d || got.Status != types.DeliveryStatusSent || got.SentAt == nil {
			t.Errorf("发送回填不一致: %+v", got)
		}
		if got.FeishuMessageID != msgID || got.BodyMD != "解读摘要" {
			t.Errorf("message_id/body_md 回读不一致: %q/%q", got.FeishuMessageID, got.BodyMD)
		}
		// 最终卡 JSON 在 MarkDeliverySent 才落库（JSONB 不保证键序，语义比较）。
		var parsed map[string]any
		if err := json.Unmarshal(got.CardJSON, &parsed); err != nil {
			t.Fatalf("回读 card_json 解析失败: %v（原文 %s）", err, got.CardJSON)
		}
		if parsed["schema"] != "2.0" {
			t.Errorf("card_json 应为 MarkDeliverySent 回填的最终卡: %s", got.CardJSON)
		}
		// 归属：他人用同一 message_id 反查不命中。
		if _, err := st.GetDeliveryByFeishuMessageID(ctx, u2.ID, msgID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("他人反查应 ErrNotFound，实际: %v", err)
		}
		// 不存在的 message_id。
		if _, err := st.GetDeliveryByFeishuMessageID(ctx, u.ID, "om_nonexistent"); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("未知 message_id 应 ErrNotFound，实际: %v", err)
		}
	})

	t.Run("GetContentItem", func(t *testing.T) {
		ci := newContent(t, "取原文内容", "深度解读要用的正文")
		got, err := st.GetContentItem(ctx, ci)
		if err != nil {
			t.Fatalf("GetContentItem() 失败: %v", err)
		}
		if got.ID != ci || got.Title != "取原文内容" || got.Content != "深度解读要用的正文" {
			t.Errorf("回读不一致: %+v", got)
		}
		if _, err := st.GetContentItem(ctx, -1); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("不存在内容应 ErrNotFound，实际: %v", err)
		}
	})
}
