package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestEmptyPushBatchStore 是 DATABASE_URL 门控的集成测试（无则跳过，与
// pipeline_store_test.go / observability_store_test.go 同一机制），覆盖空批次写入
// （009 / 契约 §16 修订记录「空批次缺口」）里**只有真 Postgres 能验**的部分：
//
//   - ON CONFLICT 能否推断到 004 的部分唯一索引 uq_push_batches_idem
//     （谓词写歪了不会报错，只会退化成每次重试新插一行——静默的幂等失效）
//   - DO UPDATE 上那道 `WHERE push_batches.status = 'empty'` 防覆写护栏的真实行为，
//     含"未命中时 RETURNING 无行 → pgx.ErrNoRows"这条只在真库上成立的路径
//   - JSONB 往返：*int 的 nil（没跑）与 0（跑了得 0）能否原样回来
//
// 判定/编排逻辑在 workflow 包用桩验，不需要 DB——分层如此。
//
// 隔离手段是每个用例自建 user（uuid open_id）并在 Cleanup 里按 FK 逆序清掉自己的行，
// 与 pipeline_store_test.go 同款；本文件的查询都带 user_id 维度，不像探针那样全局聚合，
// 故不需要 observability_store_test.go 那种远期时间轴。
func TestEmptyPushBatchStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过空批次 store 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	// 关池必须**先注册**：t.Cleanup 是 LIFO，先注册的最后跑，故它排在下面那条
	// 清理之后。刻意不用 `defer st.Close()`——defer 在测试函数返回的那一刻就执行，
	// 早于所有 t.Cleanup，会让清理拿到一个已经关掉的连接池。
	t.Cleanup(st.Close)

	u, err := st.UpsertUserByOpenID(ctx, "test_emptybatch_"+uuid.NewString(), "empty-batch-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		// **不能用上面那个 ctx**：t.Context() 返回的 context 在 Cleanup 执行**之前**
		// 就被取消了（Go 1.24 起的既定语义），拿它发 DELETE 必然 context canceled。
		// 这个坑是静默的——配上 `_, _ =` 吞错，清理会一声不响地什么都不删，测试照样
		// 全绿，脏数据一轮轮堆在库里。本 PR 通篇在修静默失败，自己的清理不能也是。
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		exec := func(what, sql string) {
			if _, err := st.pool.Exec(cleanCtx, sql, u.ID); err != nil {
				t.Errorf("清理%s失败（脏数据会留在库里，别忽略）: %v", what, err)
			}
		}
		// FK 逆序：deliveries → push_batches → users。空批次底下没有 deliveries，
		// 但本用例用 CreatePushBatchIdempotent 建过真实批次，一并兜掉。
		exec("deliveries", `DELETE FROM deliveries WHERE user_id = $1`)
		exec("push_batches", `DELETE FROM push_batches WHERE user_id = $1`)
		exec("users", `DELETE FROM users WHERE id = $1`)
	})

	// batchRow 直接读回库里的三列原值，避免断言绕经 List 的聚合与解析——护栏用例要
	// 证明的是"这行字节没变"，经读侧解析一道就少了一层说服力（stage_counts 取原始
	// 文本而非解析后的结构体，同理）。
	batchRow := func(t *testing.T, id int64) (status, gate, stageCounts string) {
		t.Helper()
		if err := st.pool.QueryRow(ctx,
			`SELECT status, exit_gate, stage_counts::text FROM push_batches WHERE id = $1`,
			id).Scan(&status, &gate, &stageCounts); err != nil {
			t.Fatalf("回查批次 %d 失败: %v", id, err)
		}
		return status, gate, stageCounts
	}

	t.Run("闸门与漏斗往返", func(t *testing.T) {
		key := "tr-roundtrip-" + uuid.NewString()
		// dedup 闸门的招牌形状：抓到 20 条、去重后 0 条、下游三步没跑。
		counts := types.PipelineCounts{}.WithFetched(20).WithDeduped(0)
		id, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateDedup, counts)
		if err != nil {
			t.Fatalf("RecordEmptyPushBatch() 失败: %v", err)
		}
		if skipped {
			t.Fatal("全新 traceID 不该被护栏拦下")
		}
		if id == 0 {
			t.Fatal("应建出批次行，实得 id=0")
		}
		if status, gate, _ := batchRow(t, id); status != string(types.BatchStatusEmpty) ||
			gate != string(types.BatchExitGateDedup) {
			t.Errorf("库里应是 empty/dedup，实得 %q/%q", status, gate)
		}

		// 经读侧回来（探针/看板走的就是这条路）。
		sums, err := st.ListPushBatchSummaries(ctx, u.ID, time.Now().Add(-time.Hour), 10)
		if err != nil {
			t.Fatalf("ListPushBatchSummaries() 失败: %v", err)
		}
		var got *types.PushBatchSummary
		for i := range sums {
			if sums[i].ID == id {
				got = &sums[i]
			}
		}
		if got == nil {
			t.Fatal("空批次必须出现在批次历史里——这正是 009 要修的那件事")
		}
		if got.Status != types.BatchStatusEmpty || got.ExitGate != types.BatchExitGateDedup {
			t.Errorf("读侧 status/gate 不符: %q/%q", got.Status, got.ExitGate)
		}
		if got.DeliveryCount != 0 || got.SentCount != 0 {
			t.Errorf("空批次不该有投递，实得 delivery=%d sent=%d", got.DeliveryCount, got.SentCount)
		}
		if got.IdempotencyKey != key {
			t.Errorf("幂等键应可用于关联 llm_calls.trace_id，实得 %q", got.IdempotencyKey)
		}
		// JSONB 往返的核心断言：0（跑了得 0）与 nil（没跑）必须区分得开。
		// 这两者混同正是本 PR 要消灭的那类混淆，若 *int 被"顺手"改成 int 就在此炸。
		if got.StageCounts.Fetched == nil || *got.StageCounts.Fetched != 20 {
			t.Errorf("fetched 应为 20，实得 %v", got.StageCounts.Fetched)
		}
		if got.StageCounts.Deduped == nil || *got.StageCounts.Deduped != 0 {
			t.Errorf("deduped 应为 0（跑了得 0，不是没跑），实得 %v", got.StageCounts.Deduped)
		}
		if got.StageCounts.Scored != nil || got.StageCounts.Selected != nil || got.StageCounts.Cards != nil {
			t.Errorf("dedup 闸门之后的阶段没跑，必须是 nil，实得 %+v", got.StageCounts)
		}
	})

	t.Run("同幂等键重复记账复用同一行", func(t *testing.T) {
		// Temporal 重试 RecordEmptyBatch 活动的形状：同 traceID 再来一次。
		// 若 ON CONFLICT 的谓词与 004:12 的索引谓词对不上，这里会插出第二行——
		// 不报错、只是幂等静默失效，正是必须用真库验的原因。
		key := "tr-idem-" + uuid.NewString()
		counts := types.PipelineCounts{}.WithFetched(0)
		id1, _, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateFetch, counts)
		if err != nil {
			t.Fatalf("首次记账失败: %v", err)
		}
		id2, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateFetch, counts)
		if err != nil {
			t.Fatalf("重试记账失败: %v", err)
		}
		if skipped {
			t.Fatal("重试覆写自己的空批次不该被护栏拦下")
		}
		if id1 != id2 {
			t.Errorf("同一 traceID 必须复用同一批次，实得 %d vs %d", id1, id2)
		}
		var n int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*)::int FROM push_batches WHERE idempotency_key = $1`, key).Scan(&n); err != nil {
			t.Fatalf("计数失败: %v", err)
		}
		if n != 1 {
			t.Errorf("同一幂等键应恰好一行，实得 %d 行", n)
		}
	})

	// 护栏（DO UPDATE 上那行 `WHERE push_batches.status = 'empty'`）是本实现最值钱的
	// 一行 SQL：没有它，Temporal reset 一个已完成的推送运行就会把一条 status='done'、
	// 底下挂着已发投递的真实批次静默改写成 status='empty'——库里从此存着一条"没推
	// 任何东西却有 N 条投递"的自相矛盾行。三条用例分别钉死它的三种结局。
	t.Run("护栏：不覆写 done 的真实批次", func(t *testing.T) {
		// 场景真实：Temporal reset 一个已完成的推送运行时，traceID 由 SideEffect
		// 从历史重放为同一个值，而重放这趟因内容已投递会在 fetch 闸门空退。
		key := "tr-guard-done-" + uuid.NewString()
		realID, err := st.CreatePushBatchIdempotent(ctx, u.ID, key)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 失败: %v", err)
		}
		if err := st.UpdatePushBatchStatus(ctx, realID, types.BatchStatusDone); err != nil {
			t.Fatalf("UpdatePushBatchStatus() 失败: %v", err)
		}

		id, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateFetch,
			types.PipelineCounts{}.WithFetched(0))
		// 护栏拦下不是错误：该 traceID 已有真实批次，本就不该记空批次，拦对了。
		if err != nil {
			t.Fatalf("护栏跳过不该报错，实得: %v", err)
		}
		if !skipped {
			t.Error("护栏应拦下这次写入，实得 skipped=false")
		}
		if id != 0 {
			t.Errorf("护栏拦下时应返回 id=0，实得 %d", id)
		}
		// 真实批次必须**逐列原样不动**：status 不能被改成 empty，exit_gate 不能被
		// 写上一个假闸门，stage_counts 不能被覆盖成本次的漏斗。
		status, gate, sc := batchRow(t, realID)
		if status != string(types.BatchStatusDone) {
			t.Errorf("真实批次 status 必须仍是 done，实得 %q", status)
		}
		if gate != "" {
			t.Errorf("真实批次 exit_gate 必须仍是空串（跑到了 Push），实得 %q", gate)
		}
		if sc != "{}" {
			t.Errorf("真实批次 stage_counts 不得被覆写，实得 %q", sc)
		}
	})

	t.Run("护栏：不覆写 pending 的真实批次", func(t *testing.T) {
		// pending 同样是"真实批次"：它意味着 Push 活动起跑过、建了行。此刻空退的
		// 记账若覆写它，就把一次进行中/半途而废的真实推送谎报成"没东西可推"。
		// 护栏写的是 status='empty' 而不是 status<>'done'，正是为了连这种中间态也挡住。
		key := "tr-guard-pending-" + uuid.NewString()
		realID, err := st.CreatePushBatchIdempotent(ctx, u.ID, key)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 失败: %v", err)
		}

		id, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateDedup,
			types.PipelineCounts{}.WithFetched(20).WithDeduped(0))
		if err != nil {
			t.Fatalf("护栏跳过不该报错，实得: %v", err)
		}
		if !skipped || id != 0 {
			t.Errorf("pending 行应被护栏拦下，实得 skipped=%v id=%d", skipped, id)
		}
		status, gate, sc := batchRow(t, realID)
		if status != string(types.BatchStatusPending) || gate != "" || sc != "{}" {
			t.Errorf("pending 批次必须原样不动，实得 status=%q gate=%q stage_counts=%q", status, gate, sc)
		}
	})

	t.Run("反向复位：真实推送不继承空批次的判词", func(t *testing.T) {
		// 与上两条护栏用例**方向相反**，是双怀疑者审查实测挖出来的缺口：
		// 那两条挡的是 empty 盖 done，本条挡的是 done 继承 empty 的判词。
		//
		// 真实场景：今早 fetch 闸门空退 → 记下 status=empty/exit_gate=fetch →
		// 人看见了、修好信源 → Temporal reset 重跑（traceID 由 SideEffect 重放为同值）
		// → 这次抓到了内容一路走到 Push → Push 复用同一行 → 收尾只改 status。
		// 若 CreatePushBatchIdempotent 的 DO UPDATE 不复位那两列，最终会留下一条
		// status=done、挂着已发投递、却写着"没抓到任何新内容"的嵌合行——
		// 与护栏要防的那条一样自相矛盾，只是从另一个方向到达。
		key := "tr-reset-empty-then-push-" + uuid.NewString()

		emptyID, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateFetch,
			types.PipelineCounts{}.WithFetched(0))
		if err != nil || skipped {
			t.Fatalf("首次空批次记账应成功，实得 err=%v skipped=%v", err, skipped)
		}
		if status, gate, _ := batchRow(t, emptyID); status != string(types.BatchStatusEmpty) || gate != "fetch" {
			t.Fatalf("前置条件不成立：实得 status=%q gate=%q", status, gate)
		}

		// reset 重跑，这次有内容 → Push 走 CreatePushBatchIdempotent 复用同一行。
		pushID, err := st.CreatePushBatchIdempotent(ctx, u.ID, key)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 失败: %v", err)
		}
		if pushID != emptyID {
			t.Fatalf("幂等键相同必须复用同一行（这是 #1 CRITICAL 的地基），实得 %d vs %d", pushID, emptyID)
		}
		if err := st.UpdatePushBatchStatus(ctx, pushID, types.BatchStatusDone); err != nil {
			t.Fatalf("UpdatePushBatchStatus() 失败: %v", err)
		}

		status, gate, sc := batchRow(t, pushID)
		if status != string(types.BatchStatusDone) {
			t.Errorf("status 应为 done，实得 %q", status)
		}
		if gate != "" {
			t.Errorf("真实推送不得继承空批次的 exit_gate（否则库里存着一条"+
				"「没抓到新内容」却发出去了卡的自相矛盾行），实得 %q", gate)
		}
		if sc != "{}" {
			t.Errorf("真实推送不得继承空批次的漏斗，实得 %q", sc)
		}
	})

	t.Run("护栏：空批次可被同键重放覆写", func(t *testing.T) {
		// 与上两条对偶，同样重要：护栏只挡"真实批次"，空批次自己被同键重放覆写是
		// **正常且必需**的——若护栏写宽了（比如漏掉 status='empty' 只留幂等冲突），
		// 同一 traceID 的重试会恒返回 ErrNoRows→静默丢记录，等于用一个新的静默失败
		// 换掉旧的静默失败。
		key := "tr-guard-empty-" + uuid.NewString()
		id1, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateFetch,
			types.PipelineCounts{}.WithFetched(0))
		if err != nil {
			t.Fatalf("首次记账失败: %v", err)
		}
		if skipped {
			t.Fatal("首次记账不该被拦")
		}
		id2, skipped, err := st.RecordEmptyPushBatch(ctx, u.ID, key, types.BatchExitGateDedup,
			types.PipelineCounts{}.WithFetched(20).WithDeduped(0))
		if err != nil {
			t.Fatalf("覆写失败: %v", err)
		}
		if skipped {
			t.Error("空批次被同键覆写不该被护栏拦下，实得 skipped=true")
		}
		if id1 != id2 {
			t.Errorf("应复用同一行，实得 %d vs %d", id1, id2)
		}
		_, gate, _ := batchRow(t, id1)
		if gate != string(types.BatchExitGateDedup) {
			t.Errorf("闸门应被覆写成 dedup，实得 %q", gate)
		}
	})

	t.Run("校验：闸门与幂等键不得为空", func(t *testing.T) {
		// status='empty' 且 exit_gate='' 的行等于"没推，但不知道为什么"——
		// 正是本功能要消灭的形状，宁可不写。
		if _, _, err := st.RecordEmptyPushBatch(ctx, u.ID, "tr-x", "",
			types.PipelineCounts{}); !isAppCode(err, types.CodeValidation) {
			t.Errorf("空闸门应报 VALIDATION，实得: %v", err)
		}
		// 空键会落在 004 部分唯一索引之外（WHERE idempotency_key <> ''），
		// 每次重试新插一行 —— 幂等静默失效，必须挡住。
		if _, _, err := st.RecordEmptyPushBatch(ctx, u.ID, "", types.BatchExitGateFetch,
			types.PipelineCounts{}); !isAppCode(err, types.CodeValidation) {
			t.Errorf("空幂等键应报 VALIDATION，实得: %v", err)
		}
	})
}

// isAppCode 判断 err 是否为指定 Code 的 AppError（测试辅助）。
func isAppCode(err error, code types.ErrCode) bool {
	var ae *types.AppError
	if !errors.As(err, &ae) {
		return false
	}
	return ae.Code == code
}
