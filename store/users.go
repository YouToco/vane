package store

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/types"
)

// UpsertUserByOpenID 按 feishu_open_id 幂等 upsert 用户。
// 飞书消息回调里每条消息都会调用，冲突时仅同步 name（用户可能改昵称），
// 不动 created_at——首次入库时间即用户首次对话时间，有留存分析价值。
// RETURNING 全列，调用方拿到的 User 恒为数据库当前真实状态。
func (s *Store) UpsertUserByOpenID(ctx context.Context, openID, name string) (*types.User, error) {
	u := &types.User{}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO users (feishu_open_id, name)
		 VALUES ($1, $2)
		 ON CONFLICT (feishu_open_id) DO UPDATE
		 SET name = EXCLUDED.name
		 RETURNING id, feishu_open_id, name, created_at`,
		openID, name,
	).Scan(&u.ID, &u.FeishuOpenID, &u.Name, &u.CreatedAt)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("upsert 用户（open_id=%s）", openID), err)
	}
	return u, nil
}
