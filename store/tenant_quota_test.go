package store

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// TestInvariant_EnforcementClaimIsTrue：声称"已接线"的桶，必须真的有代码在扣它。
//
// 这条替代了前一版那个自证式守卫——它只校验 map 里的值非空，于是把空串改成
// 任意一句话即可谎称已接线而测试全绿（2026-07-19 审查实测）。自证式守卫比没有
// 守卫更糟：它让人以为这件事有人在看。
//
// 现在的判据是可证伪的：值是包名，去那个包的源码里核实真的调用了扣减方法。
func TestInvariant_EnforcementClaimIsTrue(t *testing.T) {
	for bucket, pkg := range enforcedBuckets {
		if pkg == "" {
			continue // 显式声明未接线，另有守卫要求它必须被显式声明
		}
		if bucket == QuotaExaCalls {
			// Exa 的 V3 扣减必须和 started step + spend reservation 在同一事务里。
			// 普通 TryConsume 虽然也会扣桶，却不能证明副作用前已有不可变预留，
			// 所以不能拿通用判据替代下面更窄的专用 invariant。
			continue
		}
		files, err := filepath.Glob(filepath.Join("..", pkg, "*.go"))
		if err != nil || len(files) == 0 {
			t.Errorf("桶 %s 声称由包 %q 扣减，但该包不存在", bucket, pkg)
			continue
		}
		found := false
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			b, err := os.ReadFile(f)
			if err != nil {
				continue
			}
			src := string(b)
			// 扣减必须经这两个方法之一；只看调用点，不看注释怎么说。
			if strings.Contains(src, "TryConsumeForUser(") || strings.Contains(src, "TryConsume(") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("桶 %s 声称由包 %q 扣减，但 %s 包里找不到任何 TryConsume/TryConsumeForUser 调用 —— "+
				"「声称已接线」和「真的在扣」是两回事，而前者会让人不再去查后者", bucket, pkg, pkg)
		}
	}
}

// TestInvariant_ExaCallsUsesResearchRunReservation 把 Exa 的接线钉在 V3
// research step 的原子预留路径上。这里刻意不接受 TryConsume/TryConsumeForUser：
// 这两个方法只能证明"某处扣过桶"，证明不了 started step、预算预留和配额扣减
// 在同一个事务里提交，也挡不住响应丢失后的重放再次调用付费 provider。
func TestInvariant_ExaCallsUsesResearchRunReservation(t *testing.T) {
	if got := enforcedBuckets[QuotaExaCalls]; got != "store" {
		t.Errorf("exa_calls 必须声明由 store 的 V3 research step 原子预留路径强制执行，实得 %q", got)
	}

	begin := productionStoreFunction(t, "BeginResearchRunStepV3")
	if !functionContainsString(begin, "admit_research_run_tool_step_cap_v1") {
		t.Fatal("BeginResearchRunStepV3 没有调用 capability-bound 原子 Tool admission")
	}
	if callsFunction(begin, "TryConsume") || callsFunction(begin, "TryConsumeForUser") {
		t.Fatal("BeginResearchRunStepV3 不得用普通 TryConsume/TryConsumeForUser 扣 Exa 配额")
	}
	if callsFunction(begin, "consumeResearchRunQuotaV3") ||
		functionContainsString(begin, "reserve_research_run_quota_v3") {
		t.Fatal("BeginResearchRunStepV3 不得回退到可独立重放的旧 quota primitive")
	}
}

func productionStoreFunction(t *testing.T, name string) *ast.FuncDecl {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("枚举 store 源码失败: %v", err)
	}
	fset := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("解析 %s 失败: %v", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Name.Name == name {
				return fn
			}
		}
	}
	t.Fatalf("store 生产源码里找不到函数 %s", name)
	return nil
}

func callsFunction(fn *ast.FuncDecl, name string) bool {
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if ok && ident.Name == name {
			found = true
			return false
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == name {
			found = true
			return false
		}
		return true
	})
	return found
}

func functionContainsString(fn *ast.FuncDecl, needle string) bool {
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err == nil && strings.Contains(value, needle) {
			found = true
			return false
		}
		return true
	})
	return found
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

// TestQuota_AmbiguousTenantIsRejectedNotWaved：用户属于多个租户时**拒绝**，
// 且必须与一般数据库错误可区分。
//
// 这条守的是一个我自己差点埋下的洞：llm.Do 对配额查询失败是"放行"的旁路闸门
// （不让 DB 抖动升级成全局 LLM 停摆）。若归属不明也归入"查询失败"，那么
// tenantderive.go 承诺的"用户加入多个租户时会在正确的时刻响亮失败"，
// 在花钱路径上就被消音了——该用户获得无限额度，现场只留一行日志。
//
// 两者的正确处置相反，因为一个是暂时的、一个是确定性的：
// DB 抖动会自愈，而"属于两个租户"重试一万次还是两个租户。
//
// 用真实的双 membership 触发 Postgres 的 cardinality_violation，
// 而不是构造一个假错误——假错误只能证明 switch 分支写对了，
// 证明不了真实场景下抛出的确实是这个错误。
func TestQuota_AmbiguousTenantIsRejectedNotWaved(t *testing.T) {
	st := quotaStore(t)
	ctx := t.Context()

	u, err := st.UpsertUserByOpenID(ctx, "quota_ambig_"+uuid.NewString(), "多租户用户")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	var tenantIDs []int64
	var codes []string
	for i := 0; i < 2; i++ {
		code := uniqueCode(t, "qambig")
		if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
			t.Fatalf("签发邀请码失败: %v", err)
		}
		codes = append(codes, code)
		tn, err := st.CreateTenantWithInvite(ctx, code, u.ID)
		if err != nil {
			// 第二次可能被"一人一租户"约束拦下——若真如此，本用例的前提不成立，
			// 但那本身是好消息（约束比运行时判定更早拦住），如实跳过而不是假装通过。
			if i == 1 {
				t.Skipf("无法构造多租户用户（第二次建租户被拒: %v）—— "+
					"若这是数据库约束所致，说明归属不明在更早的层被挡住了", err)
			}
			t.Fatalf("建租户失败: %v", err)
		}
		tenantIDs = append(tenantIDs, tn.ID)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		for _, id := range tenantIDs {
			cleanupExec(c, t, st, `DELETE FROM tenant_quota WHERE tenant_id = $1`, id)
			cleanupExec(c, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, id)
		}
		for _, code := range codes {
			cleanupExec(c, t, st, `DELETE FROM invites WHERE code = $1`, code)
		}
		for _, id := range tenantIDs {
			cleanupExec(c, t, st, `DELETE FROM tenants WHERE id = $1`, id)
		}
		cleanupExec(c, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	if len(tenantIDs) != 2 {
		t.Skip("未能构造出双租户归属")
	}

	err = st.TryConsumeForUser(ctx, u.ID, QuotaLLMTokens, 1)
	if !errors.Is(err, ErrAmbiguousTenant) {
		t.Errorf("归属多个租户时应返回 ErrAmbiguousTenant，实得 %v —— "+
			"若归入一般数据库错误，llm.Do 的旁路闸门会放行，该用户获得无限额度", err)
	}
	// 必须与"额度用尽"可区分：前者是账号异常要人工介入，后者等一等就好。
	if errors.Is(err, ErrQuotaExceeded) {
		t.Error("归属不明不得与额度用尽混淆 —— 两者给用户的提示和处置完全不同")
	}
}

// TestInvariant_SchemaAllowsDebt：tenant_quota 必须允许 tokens 为负。
// 说明见 tenant_quota.go 中 AdjustForUser 下方的注释。
func TestInvariant_SchemaAllowsDebt(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	setBucket(t, st, tenantID, QuotaLLMTokens, 10, 0, 1000)

	// 直接扣成负数：schema 若还有 tokens >= 0 的 CHECK，这里会报约束错误。
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenant_quota SET tokens = -500 WHERE tenant_id = $1 AND bucket = $2`,
		tenantID, string(QuotaLLMTokens)); err != nil {
		t.Fatalf("表不允许负余额（%v）—— 欠账机制会被约束静默拒绝："+
			"对账失败只记 Warn，于是实际用量超出余额的那部分被永久丢弃，"+
			"桶显示还有额度而钱已经花了，正是超发 4.9 倍的成因", err)
	}

	// 且负余额必须真的拦住取用。
	if err := st.TryConsume(t.Context(), tenantID, QuotaLLMTokens, 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("负余额下必须拒绝取用，实得 %v —— 记了负债却不拦，等于没记", err)
	}
}

// TestInvariant_MigrationBackfillMatchesDefaults：026 的回填参数必须与 defaultQuotas 一致。
//
// 两处各写一份是不得已（迁移是 SQL、代码是 Go），但漂移的后果不对称且隐蔽：
// 存量租户拿到一套额度、新租户拿到另一套，而两者都"有配额行"，任何冒烟测试都发现不了。
func TestInvariant_MigrationBackfillMatchesDefaults(t *testing.T) {
	st := quotaStore(t)
	// tenant 1 由迁移 018 建、026 回填；新租户由 SeedTenantQuota 建。两者应完全一致。
	migrated, err := st.ListQuota(t.Context(), 1)
	if err != nil {
		t.Fatalf("查 tenant 1 配额失败: %v", err)
	}
	if len(migrated) == 0 {
		t.Fatal("tenant 1（018 建的存量租户）没有配额行 —— " +
			"配合「缺行即拒绝」，它的每一次 LLM 调用都会被拒，推送 100% 静默停摆")
	}
	want := map[QuotaBucket]QuotaDefault{}
	for _, q := range defaultQuotas {
		want[q.Bucket] = q
	}
	for _, got := range migrated {
		w, ok := want[got.Bucket]
		if !ok {
			t.Errorf("迁移回填了 defaultQuotas 里没有的桶 %s", got.Bucket)
			continue
		}
		if diff := got.Burst - w.Burst; diff > 0.01 || diff < -0.01 {
			t.Errorf("桶 %s 的 burst 漂移：迁移 %.0f vs 代码 %.0f —— "+
				"存量租户与新租户拿到不同额度，且两者都「有配额行」，冒烟测试发现不了",
				got.Bucket, got.Burst, w.Burst)
		}
		if diff := got.RatePerDay - w.Rate*86400; diff > 0.01 || diff < -0.01 {
			t.Errorf("桶 %s 的 rate 漂移：迁移 %.2f/天 vs 代码 %.2f/天",
				got.Bucket, got.RatePerDay, w.Rate*86400)
		}
		delete(want, got.Bucket)
	}
	for b := range want {
		t.Errorf("迁移回填漏了桶 %s —— 存量租户在这个桶上会被一律拒绝", b)
	}
}

// TestReconcileTenantQuota_SeedsMissingAndPreservesUsed：启动 reconcile 必须
// 补齐缺行、且**不得把已用掉的额度刷回满格**。
//
// 它是两种静默失效的唯一兜底：建租户时 seed 失败（那条路径刻意只记日志），
// 以及迁移漏回填（025 第一版就漏了存量租户）。两者的后果相同——该租户什么都
// 用不了，而且他和管理员都看不出为什么。
//
// 第二个断言同样重要：若 reconcile 顺手把所有桶刷满，它就成了一条"重启即回满额度"
// 的免费通道，配额形同虚设。
func TestReconcileTenantQuota_SeedsMissingAndPreservesUsed(t *testing.T) {
	st := quotaStore(t)
	ctx := t.Context()

	// A：模拟 seed 失败——建好租户后把配额行全删掉。
	missing := newQuotaTenant(t, st)
	if _, err := st.pool.Exec(ctx, `DELETE FROM tenant_quota WHERE tenant_id = $1`, missing); err != nil {
		t.Fatalf("删配额行失败: %v", err)
	}
	// B：正常租户，但已经用掉一部分额度。
	used := newQuotaTenant(t, st)
	setBucket(t, st, used, QuotaLLMTokens, 42, 0, 2000000)

	if _, err := st.ReconcileTenantQuota(ctx); err != nil {
		t.Fatalf("reconcile 失败: %v", err)
	}

	// A 必须被补齐，且立刻可用。
	got, err := st.ListQuota(ctx, missing)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	if len(got) != len(defaultQuotas) {
		t.Errorf("缺配额行的租户应被补齐 %d 个桶，实得 %d —— "+
			"缺行=无额度，该租户会静默地什么都用不了", len(defaultQuotas), len(got))
	}
	if err := st.TryConsume(ctx, missing, QuotaLLMTokens, 1); err != nil {
		t.Errorf("补齐后应立刻可用，实得 %v", err)
	}

	// B 的余额必须原样保留。
	for _, q := range mustListQuota(t, st, used) {
		if q.Bucket == QuotaLLMTokens && q.Tokens > 100 {
			t.Errorf("reconcile 把已用掉的额度刷回了 %.0f（原为 42）—— "+
				"那会让「重启服务」变成一条回满额度的免费通道，配额形同虚设", q.Tokens)
		}
	}
}

// TestReconcileTenantQuota_SkipsDeletingTenants：已注销的租户不该被补齐。
// 给正在等待硬删的租户发额度没有意义，还会让 purge 多删一批行。
func TestReconcileTenantQuota_SkipsDeletingTenants(t *testing.T) {
	st := quotaStore(t)
	ctx := t.Context()
	tenantID := newQuotaTenant(t, st)
	if _, err := st.pool.Exec(ctx, `DELETE FROM tenant_quota WHERE tenant_id = $1`, tenantID); err != nil {
		t.Fatalf("删配额行失败: %v", err)
	}
	if _, err := st.SoftDeleteTenant(ctx, tenantID); err != nil {
		t.Fatalf("注销租户失败: %v", err)
	}

	if _, err := st.ReconcileTenantQuota(ctx); err != nil {
		t.Fatalf("reconcile 失败: %v", err)
	}
	if got := mustListQuota(t, st, tenantID); len(got) != 0 {
		t.Errorf("已注销租户不该被补配额，实得 %d 个桶", len(got))
	}
}

func mustListQuota(t *testing.T, st *Store, tenantID int64) []QuotaStatus {
	t.Helper()
	got, err := st.ListQuota(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("查配额失败: %v", err)
	}
	return got
}
