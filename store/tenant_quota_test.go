package store

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func quotaStore(t *testing.T) *Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过配额集成测试")
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
	return st
}

// newQuotaTenant 建一个租户并返回 id（含默认配额，因为建租户路径已接上 seed）。
func newQuotaTenant(t *testing.T, st *Store) int64 {
	t.Helper()
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, "quota_"+uuid.NewString(), "配额测试")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	code := uniqueCode(t, "quota")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatalf("签发邀请码失败: %v", err)
	}
	tn, err := st.CreateTenantWithInvite(ctx, code, u.ID)
	if err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM tenant_quota WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM invites WHERE code = $1`, code)
		cleanupExec(c, t, st, `DELETE FROM tenants WHERE id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})
	return tn.ID
}

// setBucket 把某个桶调成指定余额与速率，便于精确构造边界。
func setBucket(t *testing.T, st *Store, tenantID int64, b QuotaBucket, tokens, rate, burst float64) {
	t.Helper()
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO tenant_quota (tenant_id, bucket, tokens, rate, burst)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (tenant_id, bucket) DO UPDATE
		    SET tokens = $3, rate = $4, burst = $5, updated_at = now()`,
		tenantID, string(b), tokens, rate, burst); err != nil {
		t.Fatalf("设置配额桶失败: %v", err)
	}
}

// TestQuota_ConcurrentConsumeNeverOverdraws 是这套设计存在的**全部理由**。
//
// 「先 SELECT 看够不够、再 UPDATE 扣减」在并发下必然超发：两个请求同时看到余额充足，
// 各自扣减，结果透支。而在 D3（平台垫付第三方成本）下，透支就是真金白银。
//
// 这条用例开 50 个并发去抢 10 个令牌——正确实现下**恰好 10 个成功**，
// 不多不少。多一个都说明判定与扣减之间存在窗口。
func TestQuota_ConcurrentConsumeNeverOverdraws(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	// rate=0：把补充关掉，否则测试期间"长出"的令牌会让预期值不确定。
	setBucket(t, st, tenantID, QuotaExaCalls, 10, 0, 100)

	const racers = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, denied int
	start := make(chan struct{})
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := st.TryConsume(t.Context(), tenantID, QuotaExaCalls, 1)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrQuotaExceeded):
				denied++
			default:
				t.Errorf("意外错误: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if ok != 10 {
		t.Errorf("桶里只有 10 个令牌，50 个并发应恰好 10 个成功，实得 %d —— "+
			"超发意味着判定与扣减之间有窗口，而在 D3 下透支就是真金白银", ok)
	}
	if denied != racers-10 {
		t.Errorf("应有 %d 个被拒，实得 %d", racers-10, denied)
	}

	// 余额必须归零，不能为负。
	sts, err := st.ListQuota(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	for _, s := range sts {
		if s.Bucket == QuotaExaCalls && s.Tokens > 0.001 {
			t.Errorf("取空后余额应为 0，实得 %.4f", s.Tokens)
		}
	}
}

// TestQuota_MissingBucketIsDenied：**没有配额行 = 没有额度**，而不是"无限额度"。
//
// 失败方向选反的代价很具体：忘了给新租户 seed，就成了一个静默的无限额度洞——
// 而"忘了 seed"最可能发生在新租户身上，也就是最需要设防的那一刻。
func TestQuota_MissingBucketIsDenied(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	// 删掉一个桶，模拟"没 seed 到"。
	if _, err := st.pool.Exec(t.Context(),
		`DELETE FROM tenant_quota WHERE tenant_id = $1 AND bucket = $2`,
		tenantID, string(QuotaTikHubCalls)); err != nil {
		t.Fatalf("删桶失败: %v", err)
	}

	err := st.TryConsume(t.Context(), tenantID, QuotaTikHubCalls, 1)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("桶不存在时必须拒绝（缺行=无额度），实得 %v —— "+
			"放行会让「忘了 seed」变成静默的无限额度洞", err)
	}
}

// TestQuota_RefillsOverTime：令牌按经过时间连续补充，无需后台任务。
func TestQuota_RefillsOverTime(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	// 空桶、每秒补 100：等 100ms 应该补出约 10 个。
	setBucket(t, st, tenantID, QuotaPush, 0, 100, 1000)

	if err := st.TryConsume(t.Context(), tenantID, QuotaPush, 5); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("空桶应立刻拒绝，实得 %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if err := st.TryConsume(t.Context(), tenantID, QuotaPush, 5); err != nil {
		t.Errorf("等待 150ms 后应补出约 15 个令牌，取 5 个不该被拒：%v", err)
	}
}

// TestQuota_BurstCaps：长期不用不会攒出天量令牌。
//
// 没有这个上限，一个沉寂三个月的租户一朝醒来就能瞬间打满上游——
// 那正是 burst 存在的意义（也是它不该等于"累计应得量"的原因）。
func TestQuota_BurstCaps(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	// 桶容量 10，速率很高，且把 updated_at 拨到很久以前——
	// 若没有 LEAST(burst, ...) 封顶，补充量会远超容量。
	setBucket(t, st, tenantID, QuotaFetch, 0, 1000, 10)
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenant_quota SET updated_at = now() - interval '1 hour'
		  WHERE tenant_id = $1 AND bucket = $2`, tenantID, string(QuotaFetch)); err != nil {
		t.Fatalf("拨时间失败: %v", err)
	}

	// 一小时 × 1000/s = 360 万，但容量只有 10：取 11 必须被拒。
	if err := st.TryConsume(t.Context(), tenantID, QuotaFetch, 11); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("补充必须被 burst(10) 封顶，取 11 应被拒，实得 %v —— "+
			"不封顶的话沉寂已久的租户会攒出天量令牌，一朝醒来打满上游", err)
	}
	// 取 10 应该刚好可以。
	if err := st.TryConsume(t.Context(), tenantID, QuotaFetch, 10); err != nil {
		t.Errorf("补充应达到 burst 上限 10，取 10 不该被拒：%v", err)
	}
}

// TestQuota_SeedIsIdempotent：重复 seed 不得把用掉的额度刷回满格。
// 否则任何一次重复调用都让配额形同虚设——而"重试时多调一次"太常见了。
func TestQuota_SeedIsIdempotent(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	setBucket(t, st, tenantID, QuotaExaCalls, 3, 0, 500)

	if err := st.SeedTenantQuota(t.Context(), tenantID); err != nil {
		t.Fatalf("重复 seed 失败: %v", err)
	}
	sts, err := st.ListQuota(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	for _, s := range sts {
		if s.Bucket == QuotaExaCalls && s.Tokens > 3.001 {
			t.Errorf("重复 seed 把余额从 3 刷回了 %.1f —— 配额形同虚设", s.Tokens)
		}
	}
}

// TestQuota_NewTenantIsSeeded：建租户路径必须自动 seed。
// 不 seed 的话，配合"缺行即拒绝"的失败方向，新租户会什么都用不了。
func TestQuota_NewTenantIsSeeded(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)

	sts, err := st.ListQuota(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	got := map[QuotaBucket]bool{}
	for _, s := range sts {
		got[s.Bucket] = true
	}
	for _, q := range defaultQuotas {
		if !got[q.Bucket] {
			t.Errorf("新租户缺少配额桶 %s —— 配合「缺行即拒绝」，该租户什么都用不了", q.Bucket)
		}
	}
	// 新租户应能立刻用。
	if err := st.TryConsume(t.Context(), tenantID, QuotaExaCalls, 1); err != nil {
		t.Errorf("新租户应有可用额度，实得 %v", err)
	}
}

// TestInvariant_EveryBucketIsClassified：新增桶必须显式归类为财务面或 DoS 面。
//
// 两类的失败语义不同（超预算 vs 太吵），调用方据此决定降级方式。
// 漏归类的桶会默认落进 DoS 面——一个本该硬拦的财务桶被当成"吵"来处理，
// 而这个错误不会有任何报错。
func TestInvariant_EveryBucketIsClassified(t *testing.T) {
	dosBuckets := map[QuotaBucket]bool{QuotaPush: true, QuotaFetch: true}
	for _, q := range defaultQuotas {
		if !q.Bucket.IsFinancial() && !dosBuckets[q.Bucket] {
			t.Errorf("桶 %s 既不在财务面也不在 DoS 面 —— 两类的失败语义不同，"+
				"漏归类会让本该硬拦的财务桶被当成「太吵」处理，且没有任何报错", q.Bucket)
		}
	}
}

// TestQuota_TryConsumeForUser_DerivesTenant：按 userID 扣减必须落到他的租户上。
// 这条路径是给 llm/fetcher 这些拿不到 tenant_id 的层用的，推导错了会扣到别人头上。
func TestQuota_TryConsumeForUser_DerivesTenant(t *testing.T) {
	st := quotaStore(t)
	ctx := t.Context()

	u, err := st.UpsertUserByOpenID(ctx, "quota_derive_"+uuid.NewString(), "推导测试")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	code := uniqueCode(t, "qderive")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatalf("签发邀请码失败: %v", err)
	}
	tn, err := st.CreateTenantWithInvite(ctx, code, u.ID)
	if err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM tenant_quota WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM invites WHERE code = $1`, code)
		cleanupExec(c, t, st, `DELETE FROM tenants WHERE id = $1`, tn.ID)
		cleanupExec(c, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	setBucket(t, st, tn.ID, QuotaLLMTokens, 100, 0, 1000)
	if err := st.TryConsumeForUser(ctx, u.ID, QuotaLLMTokens, 30); err != nil {
		t.Fatalf("按 userID 扣减应成功: %v", err)
	}
	sts, err := st.ListQuota(ctx, tn.ID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	for _, s := range sts {
		if s.Bucket == QuotaLLMTokens && (s.Tokens < 69.9 || s.Tokens > 70.1) {
			t.Errorf("扣减应落到该用户的租户上（100-30=70），实得 %.1f —— 推导错了会扣到别人头上", s.Tokens)
		}
	}
}

// TestQuota_NoMembershipIsDenied：用户不属于任何租户时拒绝，而不是不受限。
// 与"桶不存在即拒绝"同一个失败方向：没有归属就没有额度。
func TestQuota_NoMembershipIsDenied(t *testing.T) {
	st := quotaStore(t)
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, "quota_orphan_"+uuid.NewString(), "无归属")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	if err := st.TryConsumeForUser(ctx, u.ID, QuotaLLMTokens, 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("无租户归属的用户必须被拒（没有归属=没有额度），实得 %v", err)
	}
}

// TestInvariant_EnforcementStatusIsExplicit：每个桶都必须在 enforcedBuckets 里
// 有一条记录（哪怕是空串"未接线"）。
//
// 守的是一个很容易犯、且完全静默的错：新增一个桶、seed 上去、在管理页显示出来，
// 但从没写扣减代码。此后它看起来像一道护栏，实际一次也拦不住——
// 而"看起来有护栏"比"知道没护栏"更危险，因为后者至少还会有人去看账单。
func TestInvariant_EnforcementStatusIsExplicit(t *testing.T) {
	for _, q := range defaultQuotas {
		if _, ok := enforcedBuckets[q.Bucket]; !ok {
			t.Errorf("桶 %s 没有在 enforcedBuckets 里声明接线状态 —— "+
				"新增桶必须显式说明它在哪扣，或显式承认尚未接线（空串）。"+
				"漏声明会让一个从不扣减的桶看起来像一道护栏", q.Bucket)
		}
	}
	for b := range enforcedBuckets {
		found := false
		for _, q := range defaultQuotas {
			if q.Bucket == b {
				found = true
			}
		}
		if !found {
			t.Errorf("enforcedBuckets 里的 %s 不在 defaultQuotas 中 —— "+
				"新租户不会有这个桶，而「缺行即拒绝」会让它直接拦死业务", b)
		}
	}
}

// TestInvariant_LLMBucketIsEnforced 单独把 LLM 桶钉住。
// 它是当前唯一真正接线的桶，也是花钱最多的一个：这条声明若被改成空串
// （比如接线被回退了却忘了改状态表），必须立刻可见。
func TestInvariant_LLMBucketIsEnforced(t *testing.T) {
	if !QuotaLLMTokens.IsEnforced() {
		t.Error("llm_tokens 是当前唯一接线的桶，也是最费钱的一个 —— " +
			"它若变成未接线，配额系统就只剩一张好看的表")
	}
}
