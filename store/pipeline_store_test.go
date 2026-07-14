package store

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestPipelineStore 是 DATABASE_URL 门控的集成测试（无则跳过），覆盖 M3 store
// 扩展的关键往返：UpsertSource 按 url 幂等、加订阅→列订阅、
// InsertContentItemIfNew 按 (source_id, external_id) 去重、schedule 增查删。
func TestPipelineStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 pipeline store 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	defer st.Close()

	// 测试数据用固定前缀 + uuid 后缀，结束时按 FK 逆序清理，避免污染共享测试库。
	u, err := st.UpsertUserByOpenID(ctx, "test_pipeline_"+uuid.NewString(), "pipeline-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}

	srcURL := "https://example.com/test-pipeline-" + uuid.NewString()
	srcID, err := st.UpsertSource(ctx, &types.Source{
		Type:  types.SourceTypeRSS,
		URL:   srcURL,
		Title: "pipeline-test-source",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 失败: %v", err)
	}

	t.Cleanup(func() {
		// FK 逆序：deliveries→push_batches→content_items→subscriptions/schedules→sources→users。
		_, _ = st.pool.Exec(ctx, `DELETE FROM deliveries WHERE user_id = $1`, u.ID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM push_batches WHERE user_id = $1`, u.ID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM content_items WHERE source_id = $1`, srcID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM subscriptions WHERE user_id = $1`, u.ID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM schedules WHERE user_id = $1`, u.ID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM sources WHERE id = $1`, srcID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	t.Run("UpsertSource按url幂等", func(t *testing.T) {
		again, err := st.UpsertSource(ctx, &types.Source{
			Type:  types.SourceTypeRSS,
			URL:   srcURL,
			Title: "pipeline-test-source-renamed",
		})
		if err != nil {
			t.Fatalf("UpsertSource() 二次调用失败: %v", err)
		}
		if again != srcID {
			t.Errorf("同 url 重复 upsert 应返回同 id：首次 %d，二次 %d", srcID, again)
		}
	})

	t.Run("加订阅→列订阅", func(t *testing.T) {
		if err := st.AddSubscription(ctx, u.ID, srcID); err != nil {
			t.Fatalf("AddSubscription() 失败: %v", err)
		}
		// 幂等：ON CONFLICT DO NOTHING，重复加不报错。
		if err := st.AddSubscription(ctx, u.ID, srcID); err != nil {
			t.Fatalf("AddSubscription() 重复调用应幂等，实际报错: %v", err)
		}
		subs, err := st.ListSubscriptionsByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListSubscriptionsByUser() 失败: %v", err)
		}
		var found int
		for _, sub := range subs {
			if sub.SourceID == srcID {
				found++
			}
		}
		if found != 1 {
			t.Errorf("期望恰好 1 条对 source %d 的订阅，实际 %d 条（共 %d）", srcID, found, len(subs))
		}
	})

	t.Run("ListSubscribedSourcesByUser含非active", func(t *testing.T) {
		// 依赖前一个子测试已建立 u→srcID 的订阅关系。把 source 置为 disabled，
		// 验证 ListSubscribedSourcesByUser 仍返回它（状态灯可达），而抓取用的
		// ListActiveSourcesByUser 则将其排除。测试结束恢复为 active。
		if _, err := st.pool.Exec(ctx,
			`UPDATE sources SET status = $2 WHERE id = $1`, srcID, types.SourceStatusDisabled); err != nil {
			t.Fatalf("置 source 为 disabled 失败: %v", err)
		}
		defer func() {
			_, _ = st.pool.Exec(ctx, `UPDATE sources SET status = $2 WHERE id = $1`, srcID, types.SourceStatusActive)
		}()

		all, err := st.ListSubscribedSourcesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListSubscribedSourcesByUser() 失败: %v", err)
		}
		var foundDisabled bool
		for _, s := range all {
			if s.ID == srcID {
				foundDisabled = true
				if s.Status != types.SourceStatusDisabled {
					t.Errorf("期望回读 status=disabled，实际 %q", s.Status)
				}
			}
		}
		if !foundDisabled {
			t.Errorf("ListSubscribedSourcesByUser 应包含 disabled 的源 %d，实际未包含（共 %d）", srcID, len(all))
		}

		active, err := st.ListActiveSourcesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListActiveSourcesByUser() 失败: %v", err)
		}
		for _, s := range active {
			if s.ID == srcID {
				t.Errorf("ListActiveSourcesByUser 不应包含 disabled 的源 %d", srcID)
			}
		}
	})

	t.Run("InsertContentItemIfNew去重", func(t *testing.T) {
		item := &types.ContentItem{
			SourceID:    srcID,
			ExternalID:  "ext-" + uuid.NewString(),
			URL:         "https://example.com/item",
			Title:       "标题",
			ContentHash: "hash-" + uuid.NewString(),
		}
		id1, isNew1, err := st.InsertContentItemIfNew(ctx, item)
		if err != nil {
			t.Fatalf("InsertContentItemIfNew() 首插失败: %v", err)
		}
		if !isNew1 {
			t.Error("首次插入应 isNew=true")
		}
		// 同 (source_id, external_id) 第二次：isNew=false，返回同 id。
		id2, isNew2, err := st.InsertContentItemIfNew(ctx, item)
		if err != nil {
			t.Fatalf("InsertContentItemIfNew() 二插失败: %v", err)
		}
		if isNew2 {
			t.Error("重复插入应 isNew=false")
		}
		if id2 != id1 {
			t.Errorf("重复插入应返回同 id：首次 %d，二次 %d", id1, id2)
		}
	})

	t.Run("schedule Insert→List→Get→Delete", func(t *testing.T) {
		schedID := "push-" + uuid.NewString()
		sc := &types.Schedule{
			ID:            schedID,
			UserID:        u.ID,
			NLDescription: "每天早8点推科技",
			SpecJSON:      json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
			ScopeJSON:     json.RawMessage(`{}`),
			Status:        types.ScheduleStatusActive,
		}
		if err := st.InsertSchedule(ctx, sc); err != nil {
			t.Fatalf("InsertSchedule() 失败: %v", err)
		}

		list, err := st.ListSchedulesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListSchedulesByUser() 失败: %v", err)
		}
		var inList bool
		for _, got := range list {
			if got.ID == schedID {
				inList = true
			}
		}
		if !inList {
			t.Errorf("新建的调度 %s 未出现在列表中（共 %d 条）", schedID, len(list))
		}

		got, err := st.GetSchedule(ctx, schedID)
		if err != nil {
			t.Fatalf("GetSchedule() 失败: %v", err)
		}
		if got.UserID != u.ID || got.NLDescription != sc.NLDescription {
			t.Errorf("GetSchedule() 回读不一致：%+v", got)
		}

		if err := st.DeleteSchedule(ctx, schedID); err != nil {
			t.Fatalf("DeleteSchedule() 失败: %v", err)
		}
		_, err = st.GetSchedule(ctx, schedID)
		if err == nil {
			t.Fatal("删除后 GetSchedule() 应返回错误")
		}
		if !errors.Is(err, types.ErrNotFound) {
			t.Errorf("删除后错误应满足 errors.Is(err, types.ErrNotFound)，实际: %v", err)
		}
	})

	t.Run("推送幂等CreatePushBatchIdempotent复用批次", func(t *testing.T) {
		// 004 幂等地基核心行为一：同一 idempKey（= workflow traceID）两次调用返回同一 batch_id，
		// 使 Temporal 重试 Push Activity 时复用同一批次而非重复建批。
		idempKey := "trace-" + uuid.NewString()
		batchID1, err := st.CreatePushBatchIdempotent(ctx, u.ID, idempKey)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 首次失败: %v", err)
		}
		batchID2, err := st.CreatePushBatchIdempotent(ctx, u.ID, idempKey)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 二次失败: %v", err)
		}
		if batchID2 != batchID1 {
			t.Errorf("同 idempKey 应复用同一 batch_id：首次 %d，二次 %d", batchID1, batchID2)
		}

		// 004 幂等地基核心行为二：同一 (batch_id, content_item_id) 两次 InsertDeliveryIdempotent，
		// 第二次 existed=true，避免重试时重复投递同一条内容。
		// 先建一条内容条目拿到合法的 content_item_id（deliveries.content_item_id 有 FK）。
		ci := &types.ContentItem{
			SourceID:    srcID,
			ExternalID:  "ext-idem-" + uuid.NewString(),
			URL:         "https://example.com/idem-item",
			Title:       "幂等测试内容",
			ContentHash: "hash-idem-" + uuid.NewString(),
		}
		ciID, _, err := st.InsertContentItemIfNew(ctx, ci)
		if err != nil {
			t.Fatalf("InsertContentItemIfNew() 建内容失败: %v", err)
		}

		d := &types.Delivery{
			BatchID:       batchID1,
			UserID:        u.ID,
			ContentItemID: &ciID,
			Score:         42,
			CardJSON:      json.RawMessage(`{"k":"v"}`),
			Status:        types.DeliveryStatusPending,
		}
		id1, existed1, sentAlready1, err := st.InsertDeliveryIdempotent(ctx, d)
		if err != nil {
			t.Fatalf("InsertDeliveryIdempotent() 首插失败: %v", err)
		}
		if existed1 {
			t.Error("首次插入应 existed=false")
		}
		if sentAlready1 {
			t.Error("首次插入 status=pending，应 sentAlready=false")
		}

		id2, existed2, _, err := st.InsertDeliveryIdempotent(ctx, d)
		if err != nil {
			t.Fatalf("InsertDeliveryIdempotent() 二插失败: %v", err)
		}
		if !existed2 {
			t.Error("同 (batch_id, content_item_id) 二次插入应 existed=true")
		}
		if id2 != id1 {
			t.Errorf("重复投递应返回同 id：首次 %d，二次 %d", id1, id2)
		}
	})
}
