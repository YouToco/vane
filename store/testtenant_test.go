package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// attachTenant 把用户挂进平台租户（id=1）。
//
// migration 021 起，业务表的 tenant_id 由「该用户所在的租户」推导（见 tenantderive.go），
// 没有 memberships 行的用户**写不进任何业务数据**——NOT NULL 会拦住。
// 这是刻意的：生产里租户归属只有两个来源（注册流、迁移回填），
// 「给机器人发过消息」不构成归属，否则任何陌生人发一条消息就有了租户。
//
// 测试因此要显式表达归属。用平台租户 1 而不是每次新建：多数用例只关心
// 「有归属」，不关心是哪个租户；需要跨租户隔离的用例自己建第二个租户。
func attachTenant(t *testing.T, st *Store, userID int64) {
	t.Helper()
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES (1, $1, 'owner')
		 ON CONFLICT DO NOTHING`, userID); err != nil {
		t.Fatalf("挂载租户失败: %v", err)
	}
	// 清理由各用例自己的 cleanup 负责（删 users 之前先删 memberships）——
	// 这里不注册 t.Cleanup：t.Cleanup 是后进先出，本函数注册得早、反而后执行，
	// 那时 users 已被删、外键早就报错了。
}

// testUserWithTenant 建一个**带租户归属**的测试用户，返回其 id。
// 需要写业务数据的用例一律用它，而不是裸的 UpsertUserByOpenID。
func testUserWithTenant(t *testing.T, st *Store, prefix string) int64 {
	t.Helper()
	u, err := st.UpsertUserByOpenID(t.Context(),
		fmt.Sprintf("ou_%s_%d", prefix, time.Now().UnixNano()), prefix)
	if err != nil {
		t.Fatalf("建测试用户失败: %v", err)
	}
	attachTenant(t, st, u.ID)
	return u.ID
}

// TestInvariant_TenantDerivedOnWrite 钉住 migration 021 的核心承诺：
// **每一行业务数据的 tenant_id 都等于其所有者所在的租户**。
//
// 这条不变量靠 tenantOfUser 子查询在写入时保证（见 tenantderive.go）。
// 若哪天有人把某个 INSERT 改回不带 tenant_id，NOT NULL 会先拦住；
// 若有人硬编码了一个租户号，本用例会抓到不一致。
func TestInvariant_TenantDerivedOnWrite(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	uid := testUserWithTenant(t, st, "derive")

	// 走真实写入路径造几类行。
	if err := st.AddSubscription(ctx, uid, seedSource(t, st)); err != nil {
		t.Fatalf("加订阅失败: %v", err)
	}
	if _, err := st.CreatePushBatch(ctx, uid); err != nil {
		t.Fatalf("建批次失败: %v", err)
	}
	if _, err := st.CreateAgentSession(ctx, uid); err != nil {
		t.Fatalf("建会话失败: %v", err)
	}

	// 逐表比对：tenant_id 必须与 memberships 一致，一行都不许漂。
	for _, tbl := range []string{"subscriptions", "push_batches", "agent_sessions"} {
		var bad int
		err := st.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl+` t
			 JOIN memberships m ON m.user_id = t.user_id
			WHERE t.user_id = $1 AND t.tenant_id <> m.tenant_id`, uid).Scan(&bad)
		if err != nil {
			t.Fatalf("%s 校验查询失败: %v", tbl, err)
		}
		if bad != 0 {
			t.Errorf("%s 有 %d 行的 tenant_id 与其所有者的租户不符", tbl, bad)
		}
		var total int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM `+tbl+` WHERE user_id = $1`, uid).Scan(&total); err != nil {
			t.Fatal(err)
		}
		if total == 0 {
			t.Errorf("%s 没写进任何行，用例失去意义", tbl)
		}
	}
}

// TestInvariant_NoTenantNoWrite：没有租户归属的用户**写不进业务数据**。
//
// 这是「租户归属只能来自注册流或迁移回填」的数据层体现——
// 给机器人发过消息不构成归属，否则任何陌生人发一条消息就有了租户。
func TestInvariant_NoTenantNoWrite(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, fmt.Sprintf("ou_orphan_%d", time.Now().UnixNano()), "无租户用户")
	if err != nil {
		t.Fatal(err)
	}
	// 刻意不 attachTenant。
	if err := st.AddSubscription(ctx, u.ID, seedSource(t, st)); err == nil {
		t.Error("无租户归属的用户不应能写入订阅")
	}
	if _, err := st.CreatePushBatch(ctx, u.ID); err == nil {
		t.Error("无租户归属的用户不应能建推送批次")
	}
}

// seedSource 建一个信源供订阅用（sources 是共享表，无 tenant_id）。
func seedSource(t *testing.T, st *Store) int64 {
	t.Helper()
	var id int64
	err := st.pool.QueryRow(t.Context(),
		`INSERT INTO sources (platform, capability, url, title, status)
		 VALUES ('web', 'feed', $1, '租户测试源', 'active') RETURNING id`,
		fmt.Sprintf("https://example.com/tenant-test-%d", time.Now().UnixNano())).Scan(&id)
	if err != nil {
		t.Fatalf("建测试信源失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = st.pool.Exec(context.WithoutCancel(t.Context()), `DELETE FROM sources WHERE id = $1`, id)
	})
	return id
}
