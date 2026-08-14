package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/YouToco/vane/server/types"
)

// uniqueEmail 生成本次测试专属的邮箱（uq_users_email_lower 按小写唯一）。
func uniqueEmail(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d@invite-test.local", prefix, time.Now().UnixNano())
}

// consumeInvite 用注册流真实消费一次邀请码（建 user + tenant + membership），
// 返回注册邮箱。用真注册流而不是手写 UPDATE：列表要 join 出消费租户的 owner
// 邮箱，夹具必须与生产写路径同构，否则测的是自己搭的假账本。
func consumeInvite(t *testing.T, st *Store, code string) string {
	t.Helper()
	email := uniqueEmail(t, "consumer")
	if _, _, err := st.RegisterWithInvite(t.Context(), email, "test-password-hash", code); err != nil {
		t.Fatalf("注册流消费邀请码失败: %v", err)
	}
	return email
}

// TestListInvites_FieldsAndOrder：列表返回全部码、新签发在前、
// 已消费的码带消费租户 owner 邮箱。
func TestListInvites_FieldsAndOrder(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()

	// 依次签发三个码：最早的未用、中间的被消费、最新的带过期时间。
	oldest := uniqueCode(t, "list-old")
	if _, err := st.IssueInvite(ctx, oldest, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	used := uniqueCode(t, "list-used")
	if _, err := st.IssueInvite(ctx, used, nil, 1, nil); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(72 * time.Hour)
	newest := uniqueCode(t, "list-new")
	if _, err := st.IssueInvite(ctx, newest, nil, 1, &future); err != nil {
		t.Fatal(err)
	}
	email := consumeInvite(t, st, used)

	items, err := st.ListInvites(ctx)
	if err != nil {
		t.Fatalf("ListInvites 失败: %v", err)
	}

	// 库是共享的（其他用例、历史数据都在），只认自己的三行，按 code 定位。
	idx := map[string]int{}
	byCode := map[string]InviteWithConsumer{}
	for i, it := range items {
		if it.Code == oldest || it.Code == used || it.Code == newest {
			idx[it.Code] = i
			byCode[it.Code] = it
		}
	}
	if len(byCode) != 3 {
		t.Fatalf("三个码应全部在列表中，实际找到 %d 个", len(byCode))
	}

	// 新签发在前（issued_at DESC）。
	if !(idx[newest] < idx[used] && idx[used] < idx[oldest]) {
		t.Errorf("顺序应为 新→旧，实得下标 newest=%d used=%d oldest=%d",
			idx[newest], idx[used], idx[oldest])
	}

	// 已消费的码：计数、时间、归属租户、owner 邮箱全要在。
	u := byCode[used]
	if u.UsedCount != 1 || !u.Exhausted() {
		t.Errorf("已消费码计数不符: used_count=%d max_uses=%d", u.UsedCount, u.MaxUses)
	}
	if u.ConsumedAt == nil || u.ConsumedByTenant == nil {
		t.Errorf("已消费码应有消费时间与归属租户: %+v", u.Invite)
	}
	if u.ConsumerEmail == nil || *u.ConsumerEmail != email {
		t.Errorf("消费者邮箱应为 %q，实得 %v", email, u.ConsumerEmail)
	}

	// 未消费的码：一切消费侧字段为空。
	for _, code := range []string{oldest, newest} {
		it := byCode[code]
		if it.UsedCount != 0 || it.ConsumerEmail != nil || it.ConsumedAt != nil {
			t.Errorf("未消费码 %q 不应有消费痕迹: used_count=%d email=%v at=%v",
				code, it.UsedCount, it.ConsumerEmail, it.ConsumedAt)
		}
	}
	if byCode[newest].ExpiresAt == nil {
		t.Errorf("带过期时间签发的码，列表里 expires_at 不应为空")
	}
	if byCode[oldest].ExpiresAt != nil {
		t.Errorf("永不过期签发的码，列表里 expires_at 应为空")
	}
}

// TestDeleteUnusedInvite 锁住作废语义：只有从未被使用的码可删，
// 其余情形**报错而非静默**——管理员以为码废了、实际还能用，比报错糟糕得多。
func TestDeleteUnusedInvite(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()

	t.Run("未使用的码删除成功", func(t *testing.T) {
		code := uniqueCode(t, "del-ok")
		if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
			t.Fatal(err)
		}
		if err := st.DeleteUnusedInvite(ctx, code); err != nil {
			t.Fatalf("作废未使用的码应成功: %v", err)
		}
		if _, err := st.GetInvite(ctx, code); err == nil {
			t.Error("作废后码仍可查到 —— 没删干净")
		} else {
			assertAppCode(t, err, types.CodeNotFound)
		}
	})

	t.Run("已过期但未使用的码也可删除", func(t *testing.T) {
		past := time.Now().Add(-time.Hour)
		code := uniqueCode(t, "del-expired")
		if _, err := st.IssueInvite(ctx, code, nil, 1, &past); err != nil {
			t.Fatal(err)
		}
		if err := st.DeleteUnusedInvite(ctx, code); err != nil {
			t.Fatalf("过期未使用的码应可清理: %v", err)
		}
	})

	t.Run("已用完的码报冲突且行保留", func(t *testing.T) {
		code := uniqueCode(t, "del-used")
		if _, err := st.IssueInvite(ctx, code, nil, 1, nil); err != nil {
			t.Fatal(err)
		}
		consumeInvite(t, st, code)

		err := st.DeleteUnusedInvite(ctx, code)
		if err == nil {
			t.Fatal("作废已使用的码应报错，实际静默成功 —— 审计线索被抹掉")
		}
		assertAppCode(t, err, types.CodeConflict)
		inv, gerr := st.GetInvite(ctx, code)
		if gerr != nil || inv.UsedCount != 1 {
			t.Errorf("拒绝作废后行应原样保留: inv=%+v err=%v", inv, gerr)
		}
	})

	t.Run("部分使用的多用码同样报冲突", func(t *testing.T) {
		code := uniqueCode(t, "del-partial")
		if _, err := st.IssueInvite(ctx, code, nil, 2, nil); err != nil {
			t.Fatal(err)
		}
		consumeInvite(t, st, code) // 用掉 1/2，仍可继续使用

		err := st.DeleteUnusedInvite(ctx, code)
		if err == nil {
			t.Fatal("部分使用的码已放进来过租户，作废应报错")
		}
		assertAppCode(t, err, types.CodeConflict)
	})

	t.Run("不存在的码报未找到", func(t *testing.T) {
		err := st.DeleteUnusedInvite(ctx, uniqueCode(t, "del-nonexistent"))
		if err == nil {
			t.Fatal("不存在的码应报错")
		}
		assertAppCode(t, err, types.CodeNotFound)
	})

	t.Run("空码报校验错误", func(t *testing.T) {
		err := st.DeleteUnusedInvite(ctx, "")
		if err == nil {
			t.Fatal("空码应报错")
		}
		assertAppCode(t, err, types.CodeValidation)
	})
}
