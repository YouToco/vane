package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// InsertDelivery 插入一条投递记录并返回 id。
// 建时投递尚未发送：feishu_message_id / sent_at 留空，由 MarkDeliverySent 回填。
func (s *Store) InsertDelivery(ctx context.Context, d *types.Delivery) (int64, error) {
	// card_json NOT NULL DEFAULT '{}'：nil / 空 RawMessage 归一为 '{}'，避免写入 NULL。
	card := d.CardJSON
	if len(card) == 0 {
		card = json.RawMessage("{}")
	}
	// status 未指定时默认 pending（001 默认值），显式传入则尊重调用方。
	status := d.Status
	if status == "" {
		status = types.DeliveryStatusPending
	}
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO deliveries (
			batch_id, user_id, content_item_id, score, card_json,
			feishu_message_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`,
		d.BatchID, d.UserID, d.ContentItemID, d.Score, card,
		d.FeishuMessageID, status,
	).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入投递（batch=%d, user=%d）", d.BatchID, d.UserID), err)
	}
	return id, nil
}

// InsertDeliveryIdempotent 幂等插入投递：同一 (batch_id, content_item_id) 已存在则不重复插，
// 返回既有记录的 id、existed=true 及其是否已 sent——供 Push 在 Temporal 重试时跳过已发条目。
// content_item_id 为 nil 时不参与唯一约束（004 部分索引 WHERE content_item_id IS NOT NULL），
// 退化为普通插入（M3 推送恒有 content_item_id，此路径仅为健壮性兜底）。
//
// 实现同 content_items.go 的模式：ON CONFLICT DO NOTHING + RETURNING id。
// 插入成功→有行返回、existed=false；命中冲突→无行（pgx.ErrNoRows），按唯一键补查既有 id 与 status。
func (s *Store) InsertDeliveryIdempotent(ctx context.Context, d *types.Delivery) (id int64, existed bool, sentAlready bool, err error) {
	// card_json NOT NULL DEFAULT '{}'：nil / 空 RawMessage 归一为 '{}'，避免写入 NULL。
	card := d.CardJSON
	if len(card) == 0 {
		card = json.RawMessage("{}")
	}
	// status 未指定时默认 pending（001 默认值），显式传入则尊重调用方。
	status := d.Status
	if status == "" {
		status = types.DeliveryStatusPending
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO deliveries (
			batch_id, user_id, content_item_id, score, card_json,
			feishu_message_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (batch_id, content_item_id) WHERE content_item_id IS NOT NULL DO NOTHING
		RETURNING id`,
		d.BatchID, d.UserID, d.ContentItemID, d.Score, card,
		d.FeishuMessageID, status,
	).Scan(&id)
	if err == nil {
		return id, false, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("幂等插入投递（batch=%d, user=%d）", d.BatchID, d.UserID), err)
	}
	// 命中批内内容唯一键冲突：投递已存在，补查其 id 与 status，判定是否已发。
	var st types.DeliveryStatus
	if qerr := s.pool.QueryRow(ctx,
		`SELECT id, status FROM deliveries WHERE batch_id = $1 AND content_item_id = $2`,
		d.BatchID, d.ContentItemID).Scan(&id, &st); qerr != nil {
		return 0, false, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("回查既有投递（batch=%d, content_item=%v）", d.BatchID, d.ContentItemID), qerr)
	}
	return id, true, st == types.DeliveryStatusSent, nil
}

// MarkDeliverySent 回填飞书消息 id、置 status=sent 并写 sent_at。
// 飞书 SendCard 成功后调用；sentAt 由调用方传入（通常为发送返回时刻）。
func (s *Store) MarkDeliverySent(ctx context.Context, id int64, feishuMessageID string, sentAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE deliveries
		 SET feishu_message_id = $2, status = $3, sent_at = $4
		 WHERE id = $1`,
		id, feishuMessageID, types.DeliveryStatusSent, sentAt)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("标记投递已发送（id=%d）", id), err)
	}
	return nil
}
