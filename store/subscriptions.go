package store

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/types"
)

// AddSubscription 幂等添加订阅：重复订阅同一源是空操作（命中 uq_subscriptions_user_source）。
// ON CONFLICT DO NOTHING 而非报冲突——"再点一次订阅"应静默成功而非报错。
func (s *Store) AddSubscription(ctx context.Context, userID, sourceID int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO subscriptions (tenant_id, user_id, source_id)
		 VALUES (`+tenantOfUser+`$1), $1, $2)
		 ON CONFLICT (tenant_id, user_id, source_id) DO NOTHING`,
		userID, sourceID)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("添加订阅（user=%d, source=%d）", userID, sourceID), err)
	}
	return nil
}

// RemoveSubscription 硬删除订阅行。
// 为什么硬删而非软标记 inactive：AddSubscription 用 ON CONFLICT DO NOTHING，
// 若软删只置 status=inactive，再次订阅会命中冲突被 DO NOTHING 跳过、无法复活，
// 反成 bug。删行后重新订阅即重新 INSERT，两端语义对称。
func (s *Store) RemoveSubscription(ctx context.Context, userID, sourceID int64) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM subscriptions WHERE user_id = $1 AND source_id = $2`,
		userID, sourceID)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("移除订阅（user=%d, source=%d）", userID, sourceID), err)
	}
	return nil
}
