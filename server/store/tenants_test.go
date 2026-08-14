package store

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

// tenantTestStore 建库连接；未设 DATABASE_URL 时跳过（与其余 store 集成测试同模式）。
func tenantTestStore(t *testing.T) *Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		requireDatabaseCapability(t)
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	// Most Store integration tests provision one ephemeral owner URL. V3
	// production code never falls back to it; dedicated runtime-login tests
	// below exercise NewWithResearchRuntime and its authority probe separately.
	st.beginResearchTx = st.pool.BeginTx
	if err := st.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID:  "store-tests-active",
		ActiveKeyHex: strings.Repeat("42", 32),
		RetiredKeys:  "store-tests-retired=" + strings.Repeat("24", 32),
	}); err != nil {
		t.Fatalf("configure V3 test capability: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

// uniqueCode 生成本次测试专属的邀请码，避免并行/重跑撞主键。
func uniqueCode(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), os.Getpid())
}

// testUser 建一个测试用户（memberships 有 users 外键）。
func testUser(t *testing.T, st *Store) int64 {
	t.Helper()
	u, err := st.UpsertUserByOpenID(t.Context(),
		fmt.Sprintf("ou_tenant_test_%d", time.Now().UnixNano()), "租户测试用户")
	if err != nil {
		t.Fatalf("建测试用户失败: %v", err)
	}
	return u.ID
}

func TestCreateTenantWithInvite_Success(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	code := uniqueCode(t, "ok")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatalf("签发失败: %v", err)
	}
	uid := testUser(t, st)

	tenant, err := st.CreateTenantWithInvite(ctx, code, uid)
	if err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	if tenant.ID <= 0 || tenant.Status != types.TenantStatusActive {
		t.Errorf("租户不符: %+v", tenant)
	}
	// 存量租户 1 是迁移种下的，新建的必须另起号（否则会与过渡期的 SingleTenantID 撞车）。
	if tenant.ID == 1 {
		t.Error("新建租户不应复用存量租户号 1")
	}

	// 成员关系建了，且角色是 owner。
	ms, err := st.ListMembershipsByUser(ctx, uid)
	if err != nil {
		t.Fatalf("查成员关系失败: %v", err)
	}
	if len(ms) != 1 || ms[0].TenantID != tenant.ID || ms[0].Role != types.MembershipRoleOwner {
		t.Errorf("成员关系不符: %+v", ms)
	}

	// 邀请码被消费且回填了归属租户。
	inv, err := st.GetInvite(ctx, code)
	if err != nil {
		t.Fatalf("查邀请码失败: %v", err)
	}
	if inv.UsedCount != 1 || !inv.Exhausted() {
		t.Errorf("邀请码应已用满: %+v", inv)
	}
	if inv.ConsumedByTenant == nil || *inv.ConsumedByTenant != tenant.ID {
		t.Errorf("consumed_by_tenant 未回填: %+v", inv.ConsumedByTenant)
	}
}

// TestCreateTenantWithInvite_Rejects 锁住不变量 I-A2：无有效邀请码不得创建租户。
// 三种无效形态都必须拒绝，**且都不得留下租户行**——泄漏一个租户就等于泄漏一份
// 平台垫付的 API 敞口（D3）。
func TestCreateTenantWithInvite_Rejects(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	uid := testUser(t, st)

	past := time.Now().Add(-time.Hour)
	exhausted := uniqueCode(t, "exhausted")
	if _, err := st.IssueInvite(ctx, exhausted, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateTenantWithInvite(ctx, exhausted, uid); err != nil {
		t.Fatalf("首次消费应成功: %v", err)
	}
	expired := uniqueCode(t, "expired")
	if _, err := st.IssueInvite(ctx, expired, nil, 1, &past); err != nil {
		t.Fatal(err)
	}

	cases := []struct{ name, code string }{
		{"不存在的码", uniqueCode(t, "nonexistent")},
		{"已用完的码", exhausted},
		{"已过期的码", expired},
		{"空码", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := countTenants(t, st)
			_, err := st.CreateTenantWithInvite(ctx, c.code, uid)
			if err == nil {
				t.Fatal("应被拒绝，实际成功")
			}
			assertAppCode(t, err, types.CodeValidation)
			if after := countTenants(t, st); after != before {
				t.Errorf("被拒绝时不得建租户：%d → %d", before, after)
			}
		})
	}
}

// TestCreateTenantWithInvite_ConcurrentRace 是 I-A2 的核心用例：
// N 个请求同抢一个 max_uses=1 的邀请码，**恰好 1 个成功**。
//
// 这条不是理论演练——注册端点一旦上线，同一个码被并发提交是必然发生的事
// （用户双击、脚本重放、分享给多人同时点）。若这里漏一个，平台就多垫一份
// 第三方 API 成本，而 D4 邀请制正是为封住这笔敞口而设。
func TestCreateTenantWithInvite_ConcurrentRace(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	code := uniqueCode(t, "race")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatal(err)
	}

	const racers = 12
	uids := make([]int64, racers)
	for i := range uids {
		uids[i] = testUser(t, st)
	}

	before := countTenants(t, st)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var okCount, rejectCount int
	var otherErrs []error
	start := make(chan struct{})

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(uid int64) {
			defer wg.Done()
			<-start // 尽量让所有 goroutine 同时冲，放大竞态窗口
			_, err := st.CreateTenantWithInvite(ctx, code, uid)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okCount++
			case isAppCode(err, types.CodeValidation):
				rejectCount++
			default:
				otherErrs = append(otherErrs, err)
			}
		}(uids[i])
	}
	close(start)
	wg.Wait()

	if len(otherErrs) > 0 {
		t.Fatalf("出现非预期错误（应只有成功或 CodeValidation 拒绝）: %v", otherErrs)
	}
	if okCount != 1 {
		t.Errorf("应恰好 1 个成功，实际 %d（I-A2 被击穿：一个码建出多个租户）", okCount)
	}
	if rejectCount != racers-1 {
		t.Errorf("应有 %d 个被拒，实际 %d", racers-1, rejectCount)
	}
	if after := countTenants(t, st); after != before+1 {
		t.Errorf("应恰好新增 1 个租户：%d → %d", before, after)
	}

	inv, err := st.GetInvite(ctx, code)
	if err != nil {
		t.Fatal(err)
	}
	if inv.UsedCount != 1 {
		t.Errorf("used_count 应为 1，实际 %d", inv.UsedCount)
	}
}

// TestCreateTenantWithInvite_MultiUse：max_uses=3 的码恰好能建 3 个租户，第 4 次被拒。
func TestCreateTenantWithInvite_MultiUse(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	code := uniqueCode(t, "multi")
	if _, err := st.IssueInvite(ctx, code, nil, 3, nil); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := st.CreateTenantWithInvite(ctx, code, testUser(t, st)); err != nil {
			t.Fatalf("第 %d 次消费应成功: %v", i, err)
		}
	}
	if _, err := st.CreateTenantWithInvite(ctx, code, testUser(t, st)); err == nil {
		t.Fatal("第 4 次应被拒（超出 max_uses）")
	}
	inv, err := st.GetInvite(ctx, code)
	if err != nil {
		t.Fatal(err)
	}
	if inv.UsedCount != 3 {
		t.Errorf("used_count 应为 3，实际 %d", inv.UsedCount)
	}
}

// TestMigration018_SeedsSingleTenant：过渡期所有代码写死 types.SingleTenantID=1，
// 迁移必须把它实体化，否则将来给业务表加 tenant_id 外键时无行可指。
func TestMigration018_SeedsSingleTenant(t *testing.T) {
	st := tenantTestStore(t)
	tn, err := st.GetTenant(t.Context(), int64(types.SingleTenantID))
	if err != nil {
		t.Fatalf("存量租户 %d 应存在: %v", types.SingleTenantID, err)
	}
	if tn.Status != types.TenantStatusActive {
		t.Errorf("存量租户应为 active，实际 %s", tn.Status)
	}
}

func countTenants(t *testing.T, st *Store) int {
	t.Helper()
	var n int
	if err := st.pool.QueryRow(t.Context(), `SELECT count(*) FROM tenants`).Scan(&n); err != nil {
		t.Fatalf("统计租户失败: %v", err)
	}
	return n
}

func assertAppCode(t *testing.T, err error, want types.ErrCode) {
	t.Helper()
	if !isAppCode(err, want) {
		t.Errorf("错误码不符，期望 %s，实得 %v", want, err)
	}
}
