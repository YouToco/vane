package store

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// 这一组补的是**生产冒烟抓出来的覆盖缺口**。
//
// SoftDeleteTenant / RestoreTenant / TenantLiveForUser 上线时只有 workflow 侧的用例，
// 而那些用例注入的是假 store——**这三个方法从来没有对着真数据库跑过一次**。
// 结果 SoftDeleteTenant 的 SQL 里 `($2 || ' days')::interval` 让 pgx 把参数推断成 text、
// 传 int 直接报错，一路过了 CI、过了合并、部署到生产，靠人工冒烟才发现：
//
//	注销失败: DATABASE: 注销租户: failed to encode args[1]:
//	unable to encode 30 into text format for text (OID 25)
//
// 教训很具体：**带 SQL 的方法，假替身证明不了任何事**。语法、类型推断、约束、
// 返回列顺序——这些只有真库能验。

func lifecycleStore(t *testing.T) *Store {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过租户生命周期集成测试")
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

// newLifecycleTenant 建一个带成员的租户，返回 (tenantID, userID)。
func newLifecycleTenant(t *testing.T, st *Store) (int64, int64) {
	t.Helper()
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, "lifecycle_"+uuid.NewString(), "生命周期测试")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	code := uniqueCode(t, "lifecycle")
	if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
		t.Fatalf("签发邀请码失败: %v", err)
	}
	tn, err := st.CreateTenantWithInvite(ctx, code, u.ID)
	if err != nil {
		t.Fatalf("建租户失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, tn.ID)
		cleanupExec(ctx, t, st, `DELETE FROM invites WHERE code = $1`, code)
		cleanupExec(ctx, t, st, `DELETE FROM tenants WHERE id = $1`, tn.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})
	return tn.ID, u.ID
}

// TestSoftDeleteTenant_SetsRetentionWindow 是那条 SQL 的直接守卫。
// 它执行真 SQL——正是上线时缺的那一步。
func TestSoftDeleteTenant_SetsRetentionWindow(t *testing.T) {
	st := lifecycleStore(t)
	tenantID, _ := newLifecycleTenant(t, st)

	before := time.Now()
	tn, err := st.SoftDeleteTenant(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("注销失败（这正是上线时漏测、生产冒烟才发现的那条路径）: %v", err)
	}
	if tn.Status != "deleting" {
		t.Errorf("status 应为 deleting，实得 %q", tn.Status)
	}
	if tn.DeletedAt == nil {
		t.Fatal("deleted_at 应被设置")
	}
	if tn.PurgeAfter == nil {
		t.Fatal("purge_after 应被设置——没有它，硬删任务永远不知道何时该动手")
	}
	gotDays := tn.PurgeAfter.Sub(before).Hours() / 24
	if gotDays < float64(tenantRetentionDays)-1 || gotDays > float64(tenantRetentionDays)+1 {
		t.Errorf("保留期应约为 %d 天，实得 %.1f 天", tenantRetentionDays, gotDays)
	}
}

// TestSoftDeleteTenant_IdempotentDoesNotExtendPurge：反复注销不得把硬删期限往后推。
//
// 若用 `SET purge_after = now() + 30d` 而非 COALESCE，每调一次就顺延 30 天——
// 一个被反复注销的租户的数据**永远删不掉**，而这条不会有任何报错。
func TestSoftDeleteTenant_IdempotentDoesNotExtendPurge(t *testing.T) {
	st := lifecycleStore(t)
	tenantID, _ := newLifecycleTenant(t, st)

	first, err := st.SoftDeleteTenant(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("首次注销失败: %v", err)
	}
	second, err := st.SoftDeleteTenant(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("重复注销应幂等，实得错误: %v", err)
	}
	if !first.PurgeAfter.Equal(*second.PurgeAfter) {
		t.Errorf("重复注销把硬删期限从 %v 推到了 %v —— 反复注销会让数据永远删不掉",
			first.PurgeAfter, second.PurgeAfter)
	}
	if !first.DeletedAt.Equal(*second.DeletedAt) {
		t.Errorf("重复注销不该刷新 deleted_at：%v → %v", first.DeletedAt, second.DeletedAt)
	}
}

// TestRestoreTenant_ClearsAllThreeFields：恢复必须把三个字段一起清干净。
// 漏清 purge_after 会让一个已恢复的租户在 30 天后被硬删任务清掉——
// 用户看到的是"恢复成功了"，一个月后数据凭空消失。
func TestRestoreTenant_ClearsAllThreeFields(t *testing.T) {
	st := lifecycleStore(t)
	tenantID, _ := newLifecycleTenant(t, st)

	if _, err := st.SoftDeleteTenant(t.Context(), tenantID); err != nil {
		t.Fatalf("注销失败: %v", err)
	}
	tn, err := st.RestoreTenant(t.Context(), tenantID)
	if err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	if tn.Status != "active" {
		t.Errorf("status 应回到 active，实得 %q", tn.Status)
	}
	if tn.DeletedAt != nil {
		t.Errorf("deleted_at 应被清空，实得 %v", tn.DeletedAt)
	}
	if tn.PurgeAfter != nil {
		t.Errorf("purge_after 未清空（%v）—— 已恢复的租户会在到期时被硬删任务清掉，"+
			"用户看到「恢复成功」，一个月后数据凭空消失", tn.PurgeAfter)
	}
}

// TestTenantLiveForUser_TracksLifecycle：闸门要真的随生命周期翻转。
// 这是 D9「注销要真的停下来」的判据来源——它错了，管线就会继续为已注销租户花钱。
func TestTenantLiveForUser_TracksLifecycle(t *testing.T) {
	st := lifecycleStore(t)
	tenantID, userID := newLifecycleTenant(t, st)
	ctx := t.Context()

	if live, err := st.TenantLiveForUser(ctx, userID); err != nil || !live {
		t.Fatalf("新建租户应在服务中：live=%v err=%v", live, err)
	}
	if _, err := st.SoftDeleteTenant(ctx, tenantID); err != nil {
		t.Fatalf("注销失败: %v", err)
	}
	if live, err := st.TenantLiveForUser(ctx, userID); err != nil || live {
		t.Errorf("注销后应判为不在服务中，否则管线会继续为它花钱：live=%v err=%v", live, err)
	}
	if _, err := st.RestoreTenant(ctx, tenantID); err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	if live, err := st.TenantLiveForUser(ctx, userID); err != nil || !live {
		t.Errorf("恢复后应重新判为在服务中：live=%v err=%v", live, err)
	}
}

// TestTenantLiveForUser_NoMembershipIsNotLive：无归属用户判为"不在服务"。
// 那是数据异常（注册流保证每人恰好一个租户），此时"不开工"比"当作正常"安全——
// 真出了异常，宁可停也不要在无归属的状态下花钱。
func TestTenantLiveForUser_NoMembershipIsNotLive(t *testing.T) {
	st := lifecycleStore(t)
	ctx := t.Context()
	u, err := st.UpsertUserByOpenID(ctx, "orphan_"+uuid.NewString(), "无归属用户")
	if err != nil {
		t.Fatalf("建用户失败: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := cleanupContext()
		defer cancel()
		cleanupExec(c, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	if live, err := st.TenantLiveForUser(ctx, u.ID); err != nil || live {
		t.Errorf("无租户归属的用户应判为不在服务中：live=%v err=%v", live, err)
	}
}

// TestSoftDeleteTenant_NotFound：不存在的租户报 NotFound，而不是静默成功。
// 生产冒烟就是拿一个不存在的 id 试出来的——静默成功会让运维以为注销生效了。
func TestSoftDeleteTenant_NotFound(t *testing.T) {
	st := lifecycleStore(t)
	_, err := st.SoftDeleteTenant(t.Context(), 999999999)
	if err == nil {
		t.Fatal("注销不存在的租户应报错，静默成功会让运维以为生效了")
	}
	assertAppCode(t, err, types.CodeNotFound)
}

// TestTenantLiveForUser_InconsistentStateIsNotLive：status 与 deleted_at 不一致时判为不在服务。
//
// 正常路径下两个字段总是一起写，所以只看 status 就够——探针实测确认：去掉
// `deleted_at IS NULL` 后其余用例全部照绿，这道条件是**冗余的防御**。
//
// 但它防的是字段分叉：人工改库只改了一个、或将来某段代码只写其中一个。
// 那时"看哪个字段"就成了真问题，而选错的后果是**继续为一个已注销的租户花钱**。
// 这条用例直接构造该状态，让这道防御从"没人验过的冗余"变成有守卫的防线。
func TestTenantLiveForUser_InconsistentStateIsNotLive(t *testing.T) {
	st := lifecycleStore(t)
	tenantID, userID := newLifecycleTenant(t, st)
	ctx := t.Context()

	// 只设 deleted_at、故意不动 status —— 模拟字段分叉。
	if _, err := st.pool.Exec(ctx,
		`UPDATE tenants SET deleted_at = now() WHERE id = $1`, tenantID); err != nil {
		t.Fatalf("构造不一致状态失败: %v", err)
	}

	live, err := st.TenantLiveForUser(ctx, userID)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if live {
		t.Error("deleted_at 已设置却仍判为在服务中 —— 两个字段分叉时闸门必须取更保守的那个，" +
			"否则会继续为一个已注销的租户花钱")
	}
}
