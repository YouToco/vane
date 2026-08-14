package store

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

// TestScheduleDashboardStore 是 DATABASE_URL 门控的集成测试（M7 功能 6.6/6.7）：
// 任务级数据面的读查询对着真库验证——运行概览、批次分页、
// trace 归集成本，以及贯穿全部查询的归属隔离（他人任务恒不可见/恒为零）。
func TestScheduleDashboardStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过任务数据面集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	registerStoreClose(t, st)

	owner, err := st.UpsertUserByOpenID(ctx, "test_dash_"+uuid.NewString(), "dash-owner")
	if err != nil {
		t.Fatalf("建 owner 失败: %v", err)
	}
	attachTenant(t, st, owner.ID)
	stranger, err := st.UpsertUserByOpenID(ctx, "test_dash_stranger_"+uuid.NewString(), "dash-stranger")
	if err != nil {
		t.Fatalf("建 stranger 失败: %v", err)
	}
	attachTenant(t, st, stranger.ID)

	mkSchedule := func(t *testing.T, userID int64) string {
		t.Helper()
		id := "push-dash-" + uuid.NewString()
		if err := st.InsertSchedule(ctx, &types.Schedule{
			ID: id, UserID: userID,
			SpecJSON: json.RawMessage(`{"cron":"30 8 * * *"}`), ScopeJSON: json.RawMessage("{}"),
			Status: types.ScheduleStatusActive,
		}); err != nil {
			t.Fatalf("InsertSchedule 失败: %v", err)
		}
		return id
	}
	schedID := mkSchedule(t, owner.ID)
	foreignSchedID := mkSchedule(t, stranger.ID)

	// 两个内部抓取目标链接到 owner 的任务。
	mkSource := func(t *testing.T) int64 {
		t.Helper()
		id, _, err := st.GetOrCreateFetchTarget(ctx, &types.FetchTarget{
			Platform: types.PlatformWeb, Capability: types.CapSearch,
			URL: "vane://web/search?q=" + uuid.NewString(), Config: json.RawMessage(`{"query":"x"}`),
		})
		if err != nil {
			t.Fatalf("GetOrCreateFetchTarget 失败: %v", err)
		}
		return id
	}
	s1, s2 := mkSource(t), mkSource(t)
	if err := st.ReplaceTaskFetchTargets(ctx, owner.ID, schedID, []int64{s1, s2}); err != nil {
		t.Fatalf("ReplaceTaskFetchTargets 失败: %v", err)
	}

	// 运行历史：b1 真实批次（2 投递、1 已发）→ b2 空批（fetch 闸门）。b2 后建，
	// 是"最近一次运行"。外人任务另有一批，用于验证隔离。
	trace1 := "trace-dash-" + uuid.NewString()
	b1, err := st.CreatePushBatchIdempotent(ctx, owner.ID, trace1, schedID)
	if err != nil {
		t.Fatalf("建批次 b1 失败: %v", err)
	}
	if err := st.UpdatePushBatchStatus(ctx, b1, types.BatchStatusDone); err != nil {
		t.Fatalf("置 b1 done 失败: %v", err)
	}
	d1, err := st.InsertDelivery(ctx, &types.Delivery{BatchID: b1, UserID: owner.ID, Score: 88, BodyMD: "第一条"})
	if err != nil {
		t.Fatalf("插投递 d1 失败: %v", err)
	}
	if err := st.MarkDeliverySent(ctx, d1, "om_dash_"+uuid.NewString(), nil, time.Now().UTC()); err != nil {
		t.Fatalf("标记 d1 已发失败: %v", err)
	}
	if _, err := st.InsertDelivery(ctx, &types.Delivery{BatchID: b1, UserID: owner.ID, Score: 70, BodyMD: "第二条（未发成）"}); err != nil {
		t.Fatalf("插投递 d2 失败: %v", err)
	}

	trace2 := "trace-dash-" + uuid.NewString()
	b2, skipped, err := st.RecordEmptyPushBatch(ctx, owner.ID, trace2, schedID,
		types.BatchExitGateFetch, types.PipelineCounts{})
	if err != nil || skipped {
		t.Fatalf("记空批 b2 失败: skipped=%v err=%v", skipped, err)
	}

	foreignTrace := "trace-dash-" + uuid.NewString()
	fb1, err := st.CreatePushBatchIdempotent(ctx, stranger.ID, foreignTrace, foreignSchedID)
	if err != nil {
		t.Fatalf("建外人批次失败: %v", err)
	}

	// 成本记账：trace1 挂 2 条 llm_calls（生产写入方 Score/CardGen 用管线 traceID，
	// 与 push_batches.idempotency_key 同源——夹具按同一键种下才含被测特征）；
	// trace2（空批）无调用；外人 trace 的成本绝不能串进来。
	mustExec := func(t *testing.T, sql string, args ...any) {
		t.Helper()
		if _, err := st.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("种子 SQL 失败: %v\n  %s", err, sql)
		}
	}
	mustExec(t, `INSERT INTO llm_calls (
			trace_id, span_name, model, tenant_id, user_id, cost_usd,
			cost_amount, cost_currency, pricing_status
		) VALUES (
			$1,'score','test-model',(SELECT tenant_id FROM schedules WHERE id=$2),
			$3,0.5,0.5,'USD','legacy'
		)`,
		trace1, schedID, owner.ID)
	mustExec(t, `INSERT INTO llm_calls (
			trace_id, span_name, model, tenant_id, user_id, cost_usd,
			cost_amount, cost_currency, pricing_status
		) VALUES (
			$1,'cardgen','test-model',(SELECT tenant_id FROM schedules WHERE id=$2),
			$3,0.25,0.25,'USD','legacy'
		)`,
		trace1, schedID, owner.ID)
	mustExec(t, `INSERT INTO llm_calls (
			trace_id, span_name, model, tenant_id, user_id, cost_usd,
			cost_amount, cost_currency, pricing_status
		) VALUES (
			$1,'score','test-model',(SELECT tenant_id FROM schedules WHERE id=$2),
			$3,9.9,9.9,'USD','legacy'
		)`,
		foreignTrace, foreignSchedID, stranger.ID)

	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		ids := []int64{owner.ID, stranger.ID}
		cleanupExec(cctx, t, st, `DELETE FROM llm_calls WHERE trace_id = ANY($1)`, []string{trace1, trace2, foreignTrace})
		cleanupExec(cctx, t, st, `DELETE FROM deliveries WHERE user_id = ANY($1)`, ids)
		cleanupExec(cctx, t, st, `DELETE FROM push_batches WHERE user_id = ANY($1)`, ids)
		cleanupExec(cctx, t, st, `DELETE FROM task_fetch_targets WHERE schedule_id = ANY($1)`, []string{schedID, foreignSchedID})
		cleanupExec(cctx, t, st, `DELETE FROM schedules WHERE user_id = ANY($1)`, ids)
		cleanupExec(cctx, t, st, `DELETE FROM fetch_targets WHERE id = ANY($1)`, []int64{s1, s2})
		cleanupExec(cctx, t, st, `DELETE FROM memberships WHERE user_id = ANY($1)`, ids)
		cleanupExec(cctx, t, st, `DELETE FROM users WHERE id = ANY($1)`, ids)
	})

	t.Run("单任务运行概览", func(t *testing.T) {
		sum, err := st.GetScheduleRunSummary(ctx, owner.ID, schedID)
		if err != nil {
			t.Fatalf("GetScheduleRunSummary 失败: %v", err)
		}
		if sum.ScheduleID != schedID {
			t.Errorf("schedule_id = %s, 期望 %s", sum.ScheduleID, schedID)
		}
		// 最近一次运行是 b2 空批：created_at 平手时按 id 倒序取大者，b2 后插必胜。
		if sum.LastRunAt == nil || sum.LastStatus != string(types.BatchStatusEmpty) ||
			sum.LastExitGate != string(types.BatchExitGateFetch) {
			t.Errorf("最近运行应为空批(fetch)，实得 at=%v status=%q gate=%q",
				sum.LastRunAt, sum.LastStatus, sum.LastExitGate)
		}
		if sum.Batches7d != 2 || sum.EmptyBatches7d != 1 || sum.SentPushes7d != 1 {
			t.Errorf("计数不符: batches=%d empty=%d sent=%d, 期望 2/1/1",
				sum.Batches7d, sum.EmptyBatches7d, sum.SentPushes7d)
		}
	})

	t.Run("概览列表只见自己的任务", func(t *testing.T) {
		items, err := st.ListScheduleRunSummaries(ctx, owner.ID)
		if err != nil {
			t.Fatalf("ListScheduleRunSummaries 失败: %v", err)
		}
		var found bool
		for _, it := range items {
			if it.ScheduleID == foreignSchedID {
				t.Fatalf("外人任务 %s 泄漏进 owner 的概览列表", foreignSchedID)
			}
			if it.ScheduleID == schedID {
				found = true
				if it.SentPushes7d != 1 {
					t.Errorf("列表行计数与单查不一致: sent=%d", it.SentPushes7d)
				}
			}
		}
		if !found {
			t.Fatalf("概览列表缺 owner 自己的任务 %s", schedID)
		}
	})

	t.Run("他人任务概览按不存在处理", func(t *testing.T) {
		if _, err := st.GetScheduleRunSummary(ctx, owner.ID, foreignSchedID); err == nil {
			t.Fatal("查他人任务概览应 NotFound，实得 nil error")
		}
	})

	t.Run("批次分页与投递计数", func(t *testing.T) {
		// 第一页（page_size=1）：最新的 b2 空批。
		page1, total, next, err := st.ListScheduleBatches(ctx, owner.ID, schedID, BatchHistoryQuery{PageSize: 1})
		if err != nil {
			t.Fatalf("ListScheduleBatches 第一页失败: %v", err)
		}
		if total != 2 || len(page1) != 1 || next == "" {
			t.Fatalf("第一页形状不符: total=%d len=%d next=%q", total, len(page1), next)
		}
		if page1[0].ID != b2 || page1[0].Status != string(types.BatchStatusEmpty) ||
			page1[0].ExitGate != string(types.BatchExitGateFetch) || page1[0].Deliveries != 0 {
			t.Errorf("第一页应为 b2 空批: %+v", page1[0])
		}
		// 第二页：b1 真实批次，2 投递 1 已发。
		page2, _, next2, err := st.ListScheduleBatches(ctx, owner.ID, schedID, BatchHistoryQuery{PageSize: 1, PageToken: next})
		if err != nil {
			t.Fatalf("ListScheduleBatches 第二页失败: %v", err)
		}
		if len(page2) != 1 || page2[0].ID != b1 {
			t.Fatalf("第二页应为 b1: %+v", page2)
		}
		if page2[0].Status != string(types.BatchStatusDone) || page2[0].Deliveries != 2 || page2[0].Sent != 1 {
			t.Errorf("b1 计数不符: status=%q deliveries=%d sent=%d, 期望 done/2/1",
				page2[0].Status, page2[0].Deliveries, page2[0].Sent)
		}
		_ = next2 // 满页时有下一页 token，翻空属正常，不再断言。

		// 拿他人任务 id 翻批次：空页零总数，不泄露存在性。
		none, ftotal, _, err := st.ListScheduleBatches(ctx, owner.ID, foreignSchedID, BatchHistoryQuery{})
		if err != nil {
			t.Fatalf("翻他人任务批次失败: %v", err)
		}
		if len(none) != 0 || ftotal != 0 {
			t.Errorf("他人任务批次应为空页: len=%d total=%d（外人批次 %d 泄漏）", len(none), ftotal, fb1)
		}
	})

	t.Run("成本按 trace 归集且不串号", func(t *testing.T) {
		cost, err := st.GetScheduleRunCost(ctx, owner.ID, schedID)
		if err != nil {
			t.Fatalf("GetScheduleRunCost 失败: %v", err)
		}
		if math.Abs(cost.LLMCostUSD-0.75) > 1e-9 || cost.LLMCalls != 2 {
			t.Errorf("LLM 成本不符: cost=%v calls=%d, 期望 0.75/2（外人 9.9 不得串入）",
				cost.LLMCostUSD, cost.LLMCalls)
		}
		if cost.LLMPricedCalls != 2 || cost.LLMEstimatedCalls != 2 {
			t.Errorf("legacy LLM 必须保守标为估算: priced=%d estimated=%d",
				cost.LLMPricedCalls, cost.LLMEstimatedCalls)
		}
		// 外人查 owner 的任务：全零。
		fcost, err := st.GetScheduleRunCost(ctx, stranger.ID, schedID)
		if err != nil {
			t.Fatalf("外人成本查询失败: %v", err)
		}
		if fcost.LLMCalls != 0 || fcost.LLMCostUSD != 0 {
			t.Errorf("外人查 owner 任务成本应全零: %+v", fcost)
		}
	})

}
