package api

import (
	"testing"
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// TestToInviteItem 钉住 DTO 的状态语义与 types.Invite 对齐。
//
// 最容易写错的一处：used 的判据是**用满**（used_count >= max_uses，即 Exhausted），
// 不是「用过」（used_count > 0）。写成后者，一个 2/5 的多用码会在管理面显示成
// 「已用」，管理员据此以为它废了、把它再发给别人——而它其实还能注册 3 个租户。
func TestToInviteItem(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	email := "consumer@example.com"
	tenant := int64(42)

	cases := []struct {
		name        string
		in          store.InviteWithConsumer
		wantUsed    bool
		wantExpired bool
	}{
		{
			name: "未用未过期",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "A", IssuedAt: past, ExpiresAt: &future, MaxUses: 1, UsedCount: 0,
			}},
			wantUsed: false, wantExpired: false,
		},
		{
			name: "已用满",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "B", IssuedAt: past, MaxUses: 1, UsedCount: 1,
				ConsumedByTenant: &tenant, ConsumedAt: &past,
			}, ConsumerEmail: &email},
			wantUsed: true, wantExpired: false,
		},
		{
			name: "已过期未用",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "C", IssuedAt: past, ExpiresAt: &past, MaxUses: 1, UsedCount: 0,
			}},
			wantUsed: false, wantExpired: true,
		},
		{
			name: "多用码部分使用不算已用",
			in: store.InviteWithConsumer{Invite: types.Invite{
				Code: "D", IssuedAt: past, MaxUses: 5, UsedCount: 2,
				ConsumedByTenant: &tenant, ConsumedAt: &past,
			}, ConsumerEmail: &email},
			wantUsed: false, wantExpired: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toInviteItem(c.in, now)
			if got.Used != c.wantUsed || got.Expired != c.wantExpired {
				t.Errorf("状态判定不符: used=%v(期望 %v) expired=%v(期望 %v)",
					got.Used, c.wantUsed, got.Expired, c.wantExpired)
			}
			if got.Code != c.in.Code || !got.CreatedAt.Equal(c.in.IssuedAt) ||
				got.MaxUses != c.in.MaxUses || got.UsedCount != c.in.UsedCount {
				t.Errorf("透传字段不符: %+v", got)
			}
			if (got.UsedBy == nil) != (c.in.ConsumerEmail == nil) {
				t.Errorf("used_by 应与 ConsumerEmail 同空同有: %v vs %v",
					got.UsedBy, c.in.ConsumerEmail)
			}
			if (got.UsedAt == nil) != (c.in.ConsumedAt == nil) {
				t.Errorf("used_at 应与 ConsumedAt 同空同有: %v vs %v",
					got.UsedAt, c.in.ConsumedAt)
			}
		})
	}
}
