package store

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/types"
)

// CreatePushBatch 新建一个推送批次并返回 batch_id。
// status 走 001 的 DB 默认 'pending'；scheduled_at 留 NULL（即时批次无预定时间，
// 由 Temporal Schedule 触发时刻决定）。
func (s *Store) CreatePushBatch(ctx context.Context, userID int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO push_batches (user_id) VALUES ($1) RETURNING id`, userID).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("创建推送批次（user=%d）", userID), err)
	}
	return id, nil
}

// CreatePushBatchIdempotent 按幂等键创建/复用批次：同一 idempKey 重复调用返回同一 batch_id。
// idempKey 用 workflow 的确定性 traceID——Temporal 重试 Push Activity 时复用同一批次，
// 是"重试不重复发卡"的地基。
//
// 为什么用 ON CONFLICT DO UPDATE 而非 DO NOTHING：DO NOTHING 在命中冲突时不返回行，
// RETURNING id 会拿到 pgx.ErrNoRows；改用 DO UPDATE（把 user_id 写成它本来的值，等价空更新）
// 保证冲突路径也有行返回、RETURNING id 恒能拿到既有批次 id。
// ON CONFLICT 的 WHERE 谓词必须与 004 的部分唯一索引 uq_push_batches_idem 一致，
// Postgres 才能推断到该索引作为 arbiter。idempKey 恒非空，故一定命中该部分索引。
func (s *Store) CreatePushBatchIdempotent(ctx context.Context, userID int64, idempKey string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO push_batches (user_id, idempotency_key) VALUES ($1, $2)
		 ON CONFLICT (idempotency_key) WHERE idempotency_key <> '' DO UPDATE SET user_id = EXCLUDED.user_id
		 RETURNING id`, userID, idempKey).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("幂等创建批次（user=%d, key=%s）", userID, idempKey), err)
	}
	return id, nil
}

// UpdatePushBatchStatus 推进批次状态。真实可观测的生命周期是 pending→done|failed：
// 没有中间态，Push 活动跑完才一次性落终态。
//
// types.BatchStatusPushing（enums.go:37，"pushing"）是**死枚举**：全仓无任何赋值，
// 也没有任何 SQL 写过它，故库里永远不会出现这个值。查询/看板不要为它留分支，
// 更不要把"卡在 pushing"当成可能的异常形状去排查——那个状态不存在。
// （枚举本身不在本 PR 删除：那是 types 包的改动，超出本 PR 的只读范围。）
//
// push_batches 无 updated_at 列（001），故只改 status。
func (s *Store) UpdatePushBatchStatus(ctx context.Context, batchID int64, status types.BatchStatus) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE push_batches SET status = $2 WHERE id = $1`, batchID, status)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新批次状态（id=%d）", batchID), err)
	}
	return nil
}
