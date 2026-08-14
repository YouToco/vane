package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// GetSetting 按 key 读取 settings 表的 JSONB 配置值。
// key 不存在时返回 CodeNotFound 的 AppError——调用方（feishu.Manager 等）
// 用 errors.Is(err, types.ErrNotFound) 区分"尚未配置"与真正的数据库故障。
func (s *Store) GetSetting(ctx context.Context, key string) (json.RawMessage, error) {
	var value json.RawMessage
	err := s.pool.QueryRow(ctx,
		`SELECT value FROM settings WHERE key = $1`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("setting %q 不存在", key), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询 setting %q", key), err)
	}
	return value, nil
}

// PutSetting 以 UPSERT 写入配置：key 冲突时覆盖 value 并刷新 updated_at
// （001 约定不建触发器，updated_at 由应用层负责）。
func (s *Store) PutSetting(ctx context.Context, key string, value json.RawMessage) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO settings (key, value, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (key) DO UPDATE
		 SET value = EXCLUDED.value, updated_at = now()`,
		key, value)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入 setting %q", key), err)
	}
	return nil
}
