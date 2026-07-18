package store

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestSourceDisableEnable 是功能 5.2「自动停用 + 重新启用」的 store 层集成测试
// （DATABASE_URL 门控）：DisableSourceIfActive 的一次性翻转/幂等、停用后被抓取扇出
// 排除、EnableSource 的归属校验与 fail_count 清零。
func TestSourceDisableEnable(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 source disable/enable 集成测试")
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

	owner, err := st.UpsertUserByOpenID(ctx, "test_disable_owner_"+uuid.NewString(), "disable-owner")
	if err != nil {
		t.Fatalf("建 owner 失败: %v", err)
	}
	attachTenant(t, st, owner.ID)
	stranger, err := st.UpsertUserByOpenID(ctx, "test_disable_stranger_"+uuid.NewString(), "disable-stranger")
	if err != nil {
		t.Fatalf("建 stranger 失败: %v", err)
	}
	attachTenant(t, st, stranger.ID)
	srcID, _, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        "https://example.com/disable-test-" + uuid.NewString(),
		Title:      "disable-test-source",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 失败: %v", err)
	}
	if err := st.AddSubscription(ctx, owner.ID, srcID); err != nil {
		t.Fatalf("AddSubscription() 失败: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM subscriptions WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = ANY($1)`, []int64{owner.ID, stranger.ID})
	})

	t.Run("DisableSourceIfActive 一次性翻转 + 幂等", func(t *testing.T) {
		disabled, err := st.DisableSourceIfActive(ctx, srcID)
		if err != nil {
			t.Fatalf("DisableSourceIfActive() 失败: %v", err)
		}
		if !disabled {
			t.Fatal("active 源首次停用应返回 disabled=true（真翻转）")
		}
		src, err := st.GetSource(ctx, srcID)
		if err != nil {
			t.Fatalf("GetSource() 失败: %v", err)
		}
		if src.Status != types.SourceStatusDisabled {
			t.Fatalf("停用后 status 应为 disabled，实得 %q", src.Status)
		}
		// 二次调用：已是 disabled，WHERE status='active' 命不中 → false（幂等，不重复告警）。
		again, err := st.DisableSourceIfActive(ctx, srcID)
		if err != nil {
			t.Fatalf("DisableSourceIfActive() 二次失败: %v", err)
		}
		if again {
			t.Fatal("已 disabled 的源再次停用应返回 false（未翻转），否则会重复发停用告警")
		}
	})

	t.Run("停用后被抓取扇出排除", func(t *testing.T) {
		due, err := st.ListDueSourcesByUser(ctx, owner.ID)
		if err != nil {
			t.Fatalf("ListDueSourcesByUser() 失败: %v", err)
		}
		for _, s := range due {
			if s.ID == srcID {
				t.Fatal("已停用的源不该出现在到期抓取列表里")
			}
		}
	})

	t.Run("EnableSource 归属校验 + 清零 fail_count", func(t *testing.T) {
		// 先把 fail_count 抬高，验证启用时被清零（否则启用后一次失败又被停）。
		if err := st.UpdateSourceFetchState(ctx, srcID, time.Now(), time.Now(), disableFailCountForTest); err != nil {
			t.Fatalf("抬高 fail_count 失败: %v", err)
		}

		// 非订阅者（stranger）启用：EXISTS 子查询命不中 → false，且不改状态。
		enabled, err := st.EnableSource(ctx, stranger.ID, srcID)
		if err != nil {
			t.Fatalf("EnableSource(stranger) 失败: %v", err)
		}
		if enabled {
			t.Fatal("非订阅者不该能启用他人订阅的源（归属校验必须在 SQL 内挡住）")
		}
		if src, _ := st.GetSource(ctx, srcID); src.Status != types.SourceStatusDisabled {
			t.Fatal("非订阅者启用失败后源状态不该变（仍 disabled）")
		}

		// 订阅者（owner）启用：翻回 active + fail_count 清零 + next_fetch_at 前移。
		enabled, err = st.EnableSource(ctx, owner.ID, srcID)
		if err != nil {
			t.Fatalf("EnableSource(owner) 失败: %v", err)
		}
		if !enabled {
			t.Fatal("订阅者启用自己订阅的源应返回 true")
		}
		src, err := st.GetSource(ctx, srcID)
		if err != nil {
			t.Fatalf("GetSource() 失败: %v", err)
		}
		if src.Status != types.SourceStatusActive {
			t.Fatalf("启用后 status 应为 active，实得 %q", src.Status)
		}
		if src.FailCount != 0 {
			t.Fatalf("启用后 fail_count 必须清零（否则一次失败又跨停用阈值），实得 %d", src.FailCount)
		}
		// 启用后重新进入到期抓取列表。
		due, err := st.ListDueSourcesByUser(ctx, owner.ID)
		if err != nil {
			t.Fatalf("ListDueSourcesByUser() 失败: %v", err)
		}
		var found bool
		for _, s := range due {
			if s.ID == srcID {
				found = true
			}
		}
		if !found {
			t.Fatal("启用后源应重新出现在到期抓取列表里（next_fetch_at=now）")
		}
	})
}

// disableFailCountForTest 是测试用的一个"抬高的失败计数"，仅用于验证 EnableSource 清零它。
const disableFailCountForTest = 12
