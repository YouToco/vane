package store

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/types"
)

// ListSubscriptionsByUser 返回该用户的全部订阅记录（含非 active，供管理页展示）。
func (s *Store) ListSubscriptionsByUser(ctx context.Context, userID int64) ([]types.Subscription, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, user_id, source_id, status, created_at
		 FROM subscriptions
		 WHERE user_id = $1
		 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的订阅", userID), err)
	}
	defer rows.Close()

	var out []types.Subscription
	for rows.Next() {
		var sub types.Subscription
		if err := rows.Scan(&sub.ID, &sub.UserID, &sub.SourceID, &sub.Status, &sub.CreatedAt); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 subscription 行", err)
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 subscription 结果集", err)
	}
	return out, nil
}

// AddSubscription 幂等添加订阅：重复订阅同一源是空操作（命中 uq_subscriptions_user_source）。
// ON CONFLICT DO NOTHING 而非报冲突——"再点一次订阅"应静默成功而非报错。
func (s *Store) AddSubscription(ctx context.Context, userID, sourceID int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO subscriptions (user_id, source_id)
		 VALUES ($1, $2)
		 ON CONFLICT (user_id, source_id) DO NOTHING`,
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
