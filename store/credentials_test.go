package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestRegisterWithInvite_Success(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	code := uniqueCode(t, "reg")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	email := fmt.Sprintf("user-%d@example.com", time.Now().UnixNano())

	u, tn, err := st.RegisterWithInvite(ctx, email, "$argon2id$fake", code)
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	if u.Email == nil || *u.Email != email {
		t.Errorf("邮箱不符: %v", u.Email)
	}
	if u.FeishuOpenID != nil {
		t.Errorf("邮箱注册用户不应有飞书身份: %v", u.FeishuOpenID)
	}
	if u.EmailVerified {
		t.Error("邀请制下首版不做邮箱验证，应为 false")
	}
	if tn.ID <= 1 {
		t.Errorf("应新建租户（非存量租户 1），实得 %d", tn.ID)
	}

	// owner 成员关系建好了。
	ms, err := st.ListMembershipsByUser(ctx, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].TenantID != tn.ID || ms[0].Role != types.MembershipRoleOwner {
		t.Errorf("成员关系不符: %+v", ms)
	}

	// 能按邮箱查回来（登录路径）。
	got, err := st.GetUserByEmail(ctx, email)
	if err != nil || got.ID != u.ID {
		t.Errorf("按邮箱查询失败: %v", err)
	}
}

// TestRegisterWithInvite_EmailCaseInsensitive 锁住归一化：
// 大小写不同的同一邮箱视为同一人。不做归一会让同一邮箱注册出两个账号，
// 且登录时不知该匹配哪个——用户被彻底锁死。
func TestRegisterWithInvite_EmailCaseInsensitive(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	base := fmt.Sprintf("MiXeD-%d@Example.COM", time.Now().UnixNano())

	c1 := uniqueCode(t, "case1")
	if _, err := st.IssueInvite(ctx, c1, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	u, _, err := st.RegisterWithInvite(ctx, base, "$argon2id$fake", c1)
	if err != nil {
		t.Fatal(err)
	}
	if u.Email == nil || *u.Email != NormalizeEmail(base) {
		t.Errorf("入库邮箱应已归一为小写: %v", u.Email)
	}

	// 同邮箱换大小写再注册 → 冲突（用全小写形式，与首次注册的大小写不同）。
	c2 := uniqueCode(t, "case2")
	if _, err := st.IssueInvite(ctx, c2, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.RegisterWithInvite(ctx, NormalizeEmail(base), "$argon2id$fake", c2); err == nil {
		t.Fatal("同一邮箱（归一后相同）重复注册应被拒")
	} else {
		assertAppCode(t, err, types.CodeConflict)
	}

	// 大小写任意组合都能查回同一个人。
	for _, variant := range []string{base, NormalizeEmail(base)} {
		got, err := st.GetUserByEmail(ctx, variant)
		if err != nil {
			t.Errorf("按 %q 查询失败: %v", variant, err)
			continue
		}
		if got.ID != u.ID {
			t.Errorf("按 %q 查到了别的用户", variant)
		}
	}
}

// TestRegisterWithInvite_RollsBackOnDuplicateEmail 是本 PR 最关键的事务用例：
// 邮箱重复导致注册失败时，**已消费的邀请码必须回滚**。
//
// 若不回滚，用户重试注册会发现码"已用完"——一次失败的注册白烧一个码，
// 而 D4 邀请制正是财务闸门，码是有限资源。
func TestRegisterWithInvite_RollsBackOnDuplicateEmail(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	email := fmt.Sprintf("dup-%d@example.com", time.Now().UnixNano())

	c1 := uniqueCode(t, "dup1")
	if _, err := st.IssueInvite(ctx, c1, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.RegisterWithInvite(ctx, email, "$argon2id$fake", c1); err != nil {
		t.Fatal(err)
	}

	// 第二个码 + 已存在的邮箱 → 必须失败，且第二个码**未被消耗**。
	c2 := uniqueCode(t, "dup2")
	if _, err := st.IssueInvite(ctx, c2, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	tenantsBefore := countTenants(t, st)

	_, _, err := st.RegisterWithInvite(ctx, email, "$argon2id$fake", c2)
	if err == nil {
		t.Fatal("重复邮箱应被拒")
	}
	assertAppCode(t, err, types.CodeConflict)

	inv, err := st.GetInvite(ctx, c2)
	if err != nil {
		t.Fatal(err)
	}
	if inv.UsedCount != 0 {
		t.Errorf("注册失败时邀请码必须回滚，实际 used_count=%d（用户白烧一个码）", inv.UsedCount)
	}
	if after := countTenants(t, st); after != tenantsBefore {
		t.Errorf("注册失败不得留下租户: %d → %d", tenantsBefore, after)
	}
}

// TestRegisterWithInvite_RejectsBadInvite：I-A2 在注册路径上同样成立。
func TestRegisterWithInvite_RejectsBadInvite(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	before := countTenants(t, st)
	email := fmt.Sprintf("noinvite-%d@example.com", time.Now().UnixNano())

	for _, code := range []string{"", uniqueCode(t, "nonexistent")} {
		_, _, err := st.RegisterWithInvite(ctx, email, "$argon2id$fake", code)
		if err == nil {
			t.Fatalf("码 %q 应被拒", code)
		}
		assertAppCode(t, err, types.CodeValidation)
	}
	if after := countTenants(t, st); after != before {
		t.Errorf("无有效邀请码不得建租户: %d → %d", before, after)
	}
	if _, err := st.GetUserByEmail(ctx, email); err == nil {
		t.Error("注册被拒时不得留下用户行")
	}
}

// TestRegisterWithInvite_ConcurrentSameInvite：并发用同一个一次性码注册不同邮箱，
// 恰好 1 个成功（与 CreateTenantWithInvite 同一条闸门，注册路径独立验证一次）。
func TestRegisterWithInvite_ConcurrentSameInvite(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	code := uniqueCode(t, "regrace")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatal(err)
	}

	const racers = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, rejected int
	start := make(chan struct{})
	stamp := time.Now().UnixNano()

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := st.RegisterWithInvite(ctx,
				fmt.Sprintf("racer%d-%d@example.com", i, stamp), "$argon2id$fake", code)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else {
				rejected++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if ok != 1 {
		t.Errorf("应恰好 1 个成功，实际 %d（I-A2 在注册路径被击穿）", ok)
	}
	if rejected != racers-1 {
		t.Errorf("应有 %d 个被拒，实际 %d", racers-1, rejected)
	}
}

// ---- 会话 ----

func TestSessionLifecycle(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	uid := testUser(t, st)
	hash := []byte(fmt.Sprintf("hash-%d-abcdefghijklmnopqrstuv", time.Now().UnixNano()))[:32]

	if err := st.CreateSession(ctx, hash, uid, 1, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("建会话失败: %v", err)
	}
	sess, err := st.LookupSession(ctx, hash)
	if err != nil {
		t.Fatalf("查会话失败: %v", err)
	}
	if sess.UserID != uid || sess.TenantID != 1 {
		t.Errorf("会话内容不符: %+v", sess)
	}
	if sess.LastSeenAt == nil {
		t.Error("查询应刷新 last_seen_at")
	}

	if err := st.DeleteSession(ctx, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LookupSession(ctx, hash); err == nil {
		t.Error("删除后不应还能查到")
	}
	// 登出是幂等的：重复删除不报错。
	if err := st.DeleteSession(ctx, hash); err != nil {
		t.Errorf("重复登出应幂等: %v", err)
	}
}

// TestLookupSession_ExpiredIsInvisible：过期会话必须与「不存在」同一结果，
// 不给调用方误用「取到了但过期」的机会。
func TestLookupSession_ExpiredIsInvisible(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	uid := testUser(t, st)
	hash := []byte(fmt.Sprintf("exp-%d-abcdefghijklmnopqrstuvw", time.Now().UnixNano()))[:32]

	if err := st.CreateSession(ctx, hash, uid, 1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LookupSession(ctx, hash); err == nil {
		t.Fatal("过期会话不应被查到")
	} else {
		assertAppCode(t, err, types.CodeNotFound)
	}

	n, err := st.DeleteExpiredSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Errorf("应至少清理 1 条过期会话，实得 %d", n)
	}
}

func TestDeleteSessionsByUser(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	uid := testUser(t, st)
	stamp := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		h := []byte(fmt.Sprintf("multi-%d-%d-abcdefghijklmnop", stamp, i))[:32]
		if err := st.CreateSession(ctx, h, uid, 1, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := st.DeleteSessionsByUser(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("应删除 3 条（登出所有设备），实得 %d", n)
	}
}
