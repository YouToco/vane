package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// ============================================================
// 配额闸门的**行为**测试（不是存在性守卫）
// ============================================================
//
// 为什么必须有这一组：本文件的姊妹文件 quota_guard_test.go 是 AST 守卫，它只能
// 证明"CheckQuota 这个调用写在那里"。2026-07-19 的对抗审查用一个变异证明了这不够——
// **保留调用、但不理会返回值继续往下走**，闸门被完全摘除，而全仓 27 个包依然全绿。
//
// 这一组用真库 + 真 Recorder + 假上游，断言的是那个变异逃不掉的性质：
// **额度耗尽时，上游一次都不能被调到**。省下的钱正是这道护栏的全部意义，
// 而"有没有发出请求"是唯一能直接观测到它的地方。

func quotaEnv(t *testing.T) (*store.Store, int64, int64) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过配额行为测试")
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	t.Cleanup(st.Close)

	u, err := st.UpsertUserByOpenID(ctx, "llm_quota_"+uuid.NewString(), "配额行为测试")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	code := "QB" + uuid.NewString()[:10]
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatalf("签发邀请码失败: %v", err)
	}
	tn, err := st.CreateTenantWithInvite(ctx, code, u.ID)
	if err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	// 清理走 PurgeTenant——它本身就是"删掉一个租户的全部数据"的那条路径，
	// 顺带让本用例成为它的一个使用者。
	t.Cleanup(func() {
		if _, err := st.PurgeTenant(context.Background(), tn.ID, false); err != nil {
			t.Logf("清理租户 %d 失败（不影响判定）: %v", tn.ID, err)
		}
	})
	ownerIDs[tn.ID] = u.ID
	return st, u.ID, tn.ID
}

// setBalance 把 llm_tokens 桶调到指定余额——只用公开 API：先取空、再按需退还。
// 刻意不直接 UPDATE 表：那样测的是我对 schema 的记忆，而不是真实调用面。
func setBalance(t *testing.T, st *store.Store, tenantID int64, want float64) {
	t.Helper()
	ctx := t.Context()
	sts, err := st.ListQuota(ctx, tenantID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	for _, q := range sts {
		if q.Bucket != store.QuotaLLMTokens {
			continue
		}
		if q.Tokens > 0 {
			if err := st.TryConsume(ctx, tenantID, store.QuotaLLMTokens, q.Tokens); err != nil {
				t.Fatalf("取空配额失败: %v", err)
			}
		}
		if want > 0 {
			if err := st.AdjustForUser(ctx, ownerOf(t, st, tenantID), store.QuotaLLMTokens, want); err != nil {
				t.Fatalf("调整余额失败: %v", err)
			}
		}
		return
	}
	t.Fatal("租户没有 llm_tokens 桶")
}

// balanceOf 读当前余额（与 TryConsume 的判据同源，见 ListQuota 说明）。
func balanceOf(t *testing.T, st *store.Store, tenantID int64) float64 {
	t.Helper()
	sts, err := st.ListQuota(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	for _, q := range sts {
		if q.Bucket == store.QuotaLLMTokens {
			return q.Tokens
		}
	}
	t.Fatal("租户没有 llm_tokens 桶")
	return 0
}

// ownerOf 返回租户里的那个用户 id（测试里每个租户只有一个成员）。
// 这里需要 userID 是因为 AdjustForUser 按用户推导租户——那正是生产路径的形状。
var ownerIDs = map[int64]int64{}

func ownerOf(t *testing.T, _ *store.Store, tenantID int64) int64 {
	t.Helper()
	id, ok := ownerIDs[tenantID]
	if !ok {
		t.Fatalf("未登记租户 %d 的 owner", tenantID)
	}
	return id
}

// countingUpstream 起一个记录调用次数的假上游。
func countingUpstream(t *testing.T) (*Client, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(okResponseBody))
	}))
	t.Cleanup(srv.Close)
	return newTestClient(srv.URL, 4), &hits
}

// TestQuotaGate_ExhaustedBlocksUpstream_Do 是这一组的核心。
//
// 断言"上游零调用"而不是"返回了错误"：一个保留 CheckQuota 调用却忽略其返回值的
// 实现**照样会返回错误**（上游可能因别的原因失败），但它一定会把请求发出去。
// 钱花在发出请求的那一刻，所以这才是要守的性质。
func TestQuotaGate_ExhaustedBlocksUpstream_Do(t *testing.T) {
	st, userID, tenantID := quotaEnv(t)
	c, hits := countingUpstream(t)
	rec := NewRecorder(st)
	setBalance(t, st, tenantID, 0)

	_, err := Do(t.Context(), c, rec, CallMeta{TraceID: "t", SpanName: "score", UserID: &userID},
		Request{System: "s", User: "u"})

	if err == nil {
		t.Fatal("额度耗尽时 Do 必须报错")
	}
	if got := types.CodeOf(err); got != types.CodeQuotaExceeded {
		t.Errorf("错误码应为 %s，实得 %s（%v）", types.CodeQuotaExceeded, got, err)
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("额度耗尽时上游被调用了 %d 次 —— 闸门形同虚设。"+
			"钱花在发出请求的那一刻，「返回了错误」不等于「没花钱」", n)
	}
}

// TestQuotaGate_ExhaustedBlocksUpstream_DoChat：DoChat 一侧同一条。
// 这条路径是第一版彻底漏掉的，且生产实测它的 prompt 是打分的 10 倍、峰值 42 倍。
func TestQuotaGate_ExhaustedBlocksUpstream_DoChat(t *testing.T) {
	st, userID, tenantID := quotaEnv(t)
	c, hits := countingUpstream(t)
	rec := NewRecorder(st)
	setBalance(t, st, tenantID, 0)

	_, err := DoChat(t.Context(), c, rec, CallMeta{TraceID: "t", SpanName: "agent", UserID: &userID},
		ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "你好"}}})

	if err == nil {
		t.Fatal("额度耗尽时 DoChat 必须报错")
	}
	if got := types.CodeOf(err); got != types.CodeQuotaExceeded {
		t.Errorf("错误码应为 %s，实得 %s（%v）", types.CodeQuotaExceeded, got, err)
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("额度耗尽时 DoChat 调了上游 %d 次 —— agent/深挖/A2A 三条最贵的路径不受限", n)
	}
}

// TestQuotaGate_DebtIsRecorded 守住计量重做的核心性质：**实际用量一定被记进桶里，
// 哪怕会把余额扣成负数**。
//
// 前一版在这里失败得很彻底：事后扣减是全或无的，余额低于单次用量时整条 UPDATE
// 不匹配任何行，超出的部分被永久丢弃——桶显示还有余额，钱却已经花了；
// 而补充速率远快于那点象征性的事前探针，桶不降反升，实测放行 4.9 倍日额度。
//
// 构造：余额恰好够一次调用的估算、但实际用量远超估算，然后看余额有没有变负。
func TestQuotaGate_DebtIsRecorded(t *testing.T) {
	st, userID, tenantID := quotaEnv(t)
	// 上游回报一个远超估算的用量。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"model":"m","choices":[{"message":{"role":"assistant","content":"ok"}}],
			"usage":{"prompt_tokens":50000,"completion_tokens":5000}}`))
	}))
	t.Cleanup(srv.Close)
	c := newTestClient(srv.URL, 4)
	rec := NewRecorder(st)

	// 余额 5000：够付估算（约 1024+几个字符），不够付实际的 55000。
	setBalance(t, st, tenantID, 5000)

	if _, err := Do(t.Context(), c, rec, CallMeta{TraceID: "t", SpanName: "score", UserID: &userID},
		Request{System: "s", User: "u"}); err != nil {
		t.Fatalf("余额充足时不该被拒: %v", err)
	}

	tokens := balanceOf(t, st, tenantID)
	// 5000 - 55000 = -50000 量级。关键是**必须为负**。
	if tokens >= 0 {
		t.Errorf("实际用量 55000 超过余额 5000，余额应变为负数（欠账），实得 %.0f —— "+
			"扣不动就丢弃会让桶显示还有余额而钱已经花了，正是超发 4.9 倍的成因", tokens)
	}

	// 欠账必须真的拦住下一次调用。
	if err := rec.CheckQuota(t.Context(), &userID, 100); !errors.Is(err, store.ErrQuotaExceeded) {
		t.Errorf("欠账状态下必须拒绝后续调用，实得 %v —— 记了负债却不拦，等于没记", err)
	}
}

// TestQuotaGate_OverestimateIsRefunded：高估必须退还，否则长期使用会把桶系统性压低。
func TestQuotaGate_OverestimateIsRefunded(t *testing.T) {
	st, userID, tenantID := quotaEnv(t)
	c, _ := countingUpstream(t) // okResponseBody 用量仅 10+5=15
	rec := NewRecorder(st)

	const start = 100000
	setBalance(t, st, tenantID, start)

	if _, err := Do(t.Context(), c, rec, CallMeta{TraceID: "t", SpanName: "score", UserID: &userID},
		Request{System: "s", User: "u"}); err != nil {
		t.Fatalf("调用失败: %v", err)
	}

	tokens := balanceOf(t, st, tenantID)
	// 实际只用了 15，估算约 1026——差额必须退回来。
	if spent := start - tokens; spent > 100 {
		t.Errorf("实际用量 15，却净扣了 %.0f —— 估算高出的部分没有退还，"+
			"长期使用会把桶系统性压低，配额上限名不副实", spent)
	}
}

// TestQuotaGate_ConcurrentCallsCannotAllPass 是**估算存在的全部理由**。
//
// 串行调用下，光靠"无条件事后对账"就够了：第一次超支被如实记成欠账，第二次
// 就过不了事前检查。所以串行用例证明不了估算的价值——这一点是我把估算换成
// 固定值 1 做变异测试时发现的：全部串行用例照样绿（本次会话第 8 次
// 「夹具不含被测特征」，而且是我自己造出来的）。
//
// 估算真正起作用的是并发：打分/出卡以 32 路扇出跑，若事前只预扣一个象征性的
// 小额，32 路会**同时**看到"余额还够"而全部放行，直到调用完成才发现早就超了。
// 预扣真实估算量则让它们互相看见——余额只够一次，就只放行一次。
//
// 判据取"上游被调用的次数"而不是最终余额：钱花在发出请求的那一刻，
// 而余额在所有对账落库后无论如何都会显示为负，看余额区分不出这两种实现。
func TestQuotaGate_ConcurrentCallsCannotAllPass(t *testing.T) {
	st, userID, tenantID := quotaEnv(t)
	rec := NewRecorder(st)

	// 上游**阻塞**直到测试放行。这一点是本用例成立的前提：
	// 若上游立刻返回，事后对账会把高估的部分马上退还，余额随即又够下一次——
	// 那些放行是正当的，测不出并发互见（第一版夹具就栽在这里：24 路放行了 3 次，
	// 而那 3 次每次只真用了 15 token，桶完全付得起）。
	// 让调用停在飞行中，才构造出"钱已承诺、尚未结算"的那个窗口——
	// 而并发超发恰恰只发生在这个窗口里。
	release := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-release
		_, _ = w.Write([]byte(okResponseBody))
	}))
	t.Cleanup(srv.Close)
	c := newTestClient(srv.URL, 32)

	// 余额只够一次调用的估算（estimateTokens 对这个请求约为 1032）。
	setBalance(t, st, tenantID, 1500)

	const racers = 24
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = Do(context.Background(), c, rec,
				CallMeta{TraceID: "t", SpanName: "score", UserID: &userID},
				Request{System: "系统提示", User: "用户输入"})
		}()
	}
	close(start)
	// 等所有 goroutine 都撞过闸门：被拒的立刻返回，放行的停在上游。
	// 闸门是一次本地 DB 往返，300ms 足够 24 路全部走完。
	time.Sleep(300 * time.Millisecond)
	close(release)
	wg.Wait()

	// 余额 1500、单次估算约 1032：同一时刻最多只有 1 次能拿到额度。
	// 给到 2 是留给补充速率的余量（每秒补 23 个），不必卡到极限。
	if n := hits.Load(); n > 2 {
		t.Errorf("余额只够 1 次调用，%d 路并发却放行了 %d 次 —— "+
			"事前预扣的量太小，并发调用互相看不见对方。这正是第一版"+
			"（每次只预扣 1 个象征性令牌）实测超发 4.9 倍的机制", racers, n)
	}
	if hits.Load() == 0 {
		t.Error("余额够一次调用却一次都没放行 —— 预扣量估得过高，会把正常调用误拦")
	}
}
