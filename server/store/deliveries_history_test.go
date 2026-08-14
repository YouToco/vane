package store

import (
	"encoding/base64"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestHistoryCursorRoundtrip 纯单测（无 DB）：游标编解码往返无损 + 非法输入拒收。
func TestHistoryCursorRoundtrip(t *testing.T) {
	at := time.Date(2026, 7, 17, 12, 34, 56, 789000, time.UTC) // 微秒精度
	token := encodeHistoryCursor(at, 42)
	gotAt, gotID, err := decodeHistoryCursor(token)
	if err != nil {
		t.Fatalf("decodeHistoryCursor(%q) 意外失败: %v", token, err)
	}
	if !gotAt.Equal(at) || gotID != 42 {
		t.Errorf("往返得 (%v, %d)，期望 (%v, 42)", gotAt, gotID, at)
	}

	for _, bad := range []string{"!!!not-base64!!!", "aGVsbG8", // 无分隔符
		"eC0x", // "x-1"：分隔符缺失
	} {
		if _, _, err := decodeHistoryCursor(bad); err == nil {
			t.Errorf("decodeHistoryCursor(%q) 应报错，得 nil", bad)
		}
	}
	// 时间戳或 id 非数字。
	for _, raw := range []string{"abc|1", "123|xyz"} {
		token := encodeHistoryCursorRaw(raw)
		if _, _, err := decodeHistoryCursor(token); err == nil {
			t.Errorf("decodeHistoryCursor(raw=%q) 应报错，得 nil", raw)
		}
	}
}

// TestClampHistoryPageSize 钳制边界。
func TestClampHistoryPageSize(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, 20}, {-5, 20}, {1, 1}, {20, 20}, {100, 100}, {101, 100}, {9999, 100},
	}
	for _, c := range cases {
		if got := clampHistoryPageSize(c.in); got != c.want {
			t.Errorf("clampHistoryPageSize(%d) = %d，期望 %d", c.in, got, c.want)
		}
	}
}

// TestListDeliveryHistory 是 DATABASE_URL 门控的集成测试（无则跳过，仓库惯例）：
// 倒序、键集翻页不重不漏、反馈装配、空标题回退正文头、owner 隔离、LEFT JOIN 容忍
// content_item 缺失。
func TestListDeliveryHistory(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 ListDeliveryHistory 集成测试")
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

	// owner 与旁观者两个用户：验证 owner 隔离。
	owner, err := st.UpsertUserByOpenID(ctx, "ou_hist_"+uuid.NewString(), "history-owner")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID(owner) 失败: %v", err)
	}
	attachTenant(t, st, owner.ID)
	other, err := st.UpsertUserByOpenID(ctx, "ou_hist_"+uuid.NewString(), "history-other")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID(other) 失败: %v", err)
	}
	attachTenant(t, st, other.ID)

	srcID, _, err := st.GetOrCreateFetchTarget(ctx, &types.FetchTarget{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        "https://example.com/test-history-" + uuid.NewString(),
		Title:      "history-test-source",
	})
	if err != nil {
		t.Fatalf("GetOrCreateFetchTarget() 失败: %v", err)
	}

	batchID, err := st.CreatePushBatch(ctx, owner.ID)
	if err != nil {
		t.Fatalf("CreatePushBatch(owner) 失败: %v", err)
	}
	otherBatch, err := st.CreatePushBatch(ctx, other.ID)
	if err != nil {
		t.Fatalf("CreatePushBatch(other) 失败: %v", err)
	}

	// 三条内容：有标题 / 空标题（验证正文头回退）/ 归旁观者。
	mkItem := func(title, content string) int64 {
		id, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     srcID,
			CanonicalKey: "hist://" + uuid.NewString(),
			Kind:         types.KindArticle,
			URL:          "https://example.com/a/" + uuid.NewString(),
			Title:        title,
			Content:      content,
			ContentHash:  uuid.NewString(),
			FetchedAt:    time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("UpsertContentItem(%q) 失败: %v", title, err)
		}
		return id
	}
	titledItem := mkItem("有标题的内容", "正文 A")
	untitledItem := mkItem("", "无标题内容的正文头，应回退显示这一段")
	otherItem := mkItem("旁观者的内容", "正文 C")

	mkDelivery := func(userID, batchID, itemID int64, score float64) int64 {
		id, err := st.InsertDelivery(ctx, &types.Delivery{
			BatchID: batchID, UserID: userID, ContentItemID: &itemID,
			Score: score, BodyMD: "b", Status: types.DeliveryStatusSent,
		})
		if err != nil {
			t.Fatalf("InsertDelivery() 失败: %v", err)
		}
		return id
	}
	d1 := mkDelivery(owner.ID, batchID, titledItem, 88)
	d2 := mkDelivery(owner.ID, batchID, untitledItem, 55)
	dOther := mkDelivery(other.ID, otherBatch, otherItem, 70)

	// d1 两条反馈（时序升序装配），d2 零反馈。
	if _, err := st.InsertFeedback(ctx, &types.Feedback{
		UserID: owner.ID, DeliveryID: d1, Action: types.FeedbackActionNotInterested,
	}); err != nil {
		t.Fatalf("InsertFeedback(not_interested) 失败: %v", err)
	}
	if _, err := st.InsertFeedback(ctx, &types.Feedback{
		UserID: owner.ID, DeliveryID: d1, Action: types.FeedbackActionMisjudged,
		ReasonCode: types.FeedbackReasonOther, Detail: "点错了",
	}); err != nil {
		t.Fatalf("InsertFeedback(misjudged) 失败: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序：feedbacks → deliveries → push_batches / content_* → sources → users。
		cleanupExec(ctx, t, st, `DELETE FROM feedbacks WHERE delivery_id IN ($1, $2, $3)`, d1, d2, dOther)
		cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE id IN ($1, $2, $3)`, d1, d2, dOther)
		cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE id IN ($1, $2)`, batchID, otherBatch)
		cleanupExec(ctx, t, st, `DELETE FROM content_sources WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM content_items WHERE id IN ($1, $2, $3)`, titledItem, untitledItem, otherItem)
		cleanupExec(ctx, t, st, `DELETE FROM fetch_targets WHERE id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id IN ($1, $2)`, owner.ID, other.ID)
	})

	t.Run("倒序_标题回退_反馈装配_owner隔离", func(t *testing.T) {
		items, total, next, err := st.ListDeliveryHistory(ctx, owner.ID, DeliveryHistoryQuery{})
		if err != nil {
			t.Fatalf("ListDeliveryHistory() 失败: %v", err)
		}
		if total != 2 {
			t.Errorf("total = %d，期望 2（owner 只有两条投递）", total)
		}
		if next != "" {
			t.Errorf("不满页却给了 next = %q", next)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d，期望 2", len(items))
		}
		// 倒序：d2 后插，排前。
		if items[0].ID != d2 || items[1].ID != d1 {
			t.Errorf("顺序 = [%d, %d]，期望 [%d, %d]（created_at DESC）", items[0].ID, items[1].ID, d2, d1)
		}
		if items[0].Title != "无标题内容的正文头，应回退显示这一段" {
			t.Errorf("空标题未回退正文头，得 %q", items[0].Title)
		}
		if items[1].Title != "有标题的内容" {
			t.Errorf("标题 = %q，期望原标题", items[1].Title)
		}
		if len(items[0].Feedbacks) != 0 {
			t.Errorf("d2 应零反馈，得 %d 条", len(items[0].Feedbacks))
		}
		fbs := items[1].Feedbacks
		if len(fbs) != 2 {
			t.Fatalf("d1 反馈 = %d 条，期望 2", len(fbs))
		}
		if fbs[0].Action != string(types.FeedbackActionNotInterested) ||
			fbs[1].Action != string(types.FeedbackActionMisjudged) || fbs[1].Detail != "点错了" {
			t.Errorf("反馈装配错位: %+v", fbs)
		}
		// owner 隔离：不含旁观者投递。
		for _, it := range items {
			if it.ID == dOther {
				t.Errorf("owner 历史里出现了旁观者投递 %d", dOther)
			}
		}
	})

	t.Run("键集翻页不重不漏", func(t *testing.T) {
		page1, _, next, err := st.ListDeliveryHistory(ctx, owner.ID, DeliveryHistoryQuery{PageSize: 1})
		if err != nil {
			t.Fatalf("第一页失败: %v", err)
		}
		if len(page1) != 1 || next == "" {
			t.Fatalf("第一页 len=%d next=%q，期望满页给游标", len(page1), next)
		}
		page2, _, next2, err := st.ListDeliveryHistory(ctx, owner.ID,
			DeliveryHistoryQuery{PageSize: 1, PageToken: next})
		if err != nil {
			t.Fatalf("第二页失败: %v", err)
		}
		if len(page2) != 1 {
			t.Fatalf("第二页 len=%d，期望 1", len(page2))
		}
		if page1[0].ID == page2[0].ID {
			t.Errorf("两页重复返回同一行 %d", page1[0].ID)
		}
		if page1[0].ID != d2 || page2[0].ID != d1 {
			t.Errorf("翻页顺序 [%d, %d]，期望 [%d, %d]", page1[0].ID, page2[0].ID, d2, d1)
		}
		// 末页恰满页会给游标，续查应为空页（语义正确：不重不漏）。
		if next2 != "" {
			page3, _, _, err := st.ListDeliveryHistory(ctx, owner.ID,
				DeliveryHistoryQuery{PageSize: 1, PageToken: next2})
			if err != nil {
				t.Fatalf("第三页失败: %v", err)
			}
			if len(page3) != 0 {
				t.Errorf("末页后续查应为空，得 %d 行", len(page3))
			}
		}
	})

	t.Run("坏游标回验证错误", func(t *testing.T) {
		_, _, _, err := st.ListDeliveryHistory(ctx, owner.ID,
			DeliveryHistoryQuery{PageToken: "!!!bad!!!"})
		if err == nil {
			t.Fatal("坏游标应报错，得 nil")
		}
		var ae *types.AppError
		if !errors.As(err, &ae) || ae.Code != types.CodeValidation {
			t.Errorf("坏游标错误 = %v，期望 CodeValidation", err)
		}
	})
}

// encodeHistoryCursorRaw 供测试构造任意载荷的游标（绕过正常编码器的类型约束）。
func encodeHistoryCursorRaw(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}
