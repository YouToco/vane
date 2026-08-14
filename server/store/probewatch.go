package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// probewatch 告警指纹的持久化（migration 027，探针实现债 P2）。
//
// 语义见 cmd/server/probewatch.go 的 lastFingerprint 注释：指纹是"上次已成功告警
// 的红灯集合"，落盘让去重跨重启生效——2026-07-19 一天 6 次部署对同一红灯重发
// 5 张卡的直接修复。单行表，读写都是 best-effort 的消费方（失败降级策略在调用方）。

// GetProbewatchFingerprint 读取上次已告警的指纹。行不存在时返回空串而非报错：
// 空串的语义是"没告警过"，宁可让调用方多发一张卡也不能让读失败挡住整轮巡检。
func (s *Store) GetProbewatchFingerprint(ctx context.Context) (string, error) {
	var fp string
	err := s.pool.QueryRow(ctx,
		`SELECT last_fingerprint FROM probewatch_state WHERE id = 1`).Scan(&fp)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", types.NewAppError(types.CodeDatabase, "读取巡检告警指纹", err)
	}
	return fp, nil
}

// SetProbewatchFingerprint 写入当前指纹（空串 = 复位）。upsert 而非裸 UPDATE：
// migration 027 已预置唯一行，但行被人工误删后裸 UPDATE 会静默 0 行——
// 落盘失效的表现恰是"重启后重发卡"，与修复目标相同的症状最难排查。
func (s *Store) SetProbewatchFingerprint(ctx context.Context, fp string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO probewatch_state (id, last_fingerprint, updated_at)
		 VALUES (1, $1, now())
		 ON CONFLICT (id) DO UPDATE SET last_fingerprint = EXCLUDED.last_fingerprint,
		                                updated_at = EXCLUDED.updated_at`, fp)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "写入巡检告警指纹", err)
	}
	return nil
}
