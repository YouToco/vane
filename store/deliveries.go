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

// deliveryColumns 是 deliveries 表全列，SELECT 与 scanDelivery 一一对应。
const deliveryColumns = `id, batch_id, user_id, content_item_id, score, body_md,
	card_json, feishu_message_id, status, sent_at, created_at`

// scanDelivery 把一行 deliveries 扫进 types.Delivery（复用于单行与 RETURNING）。
func scanDelivery(row pgx.Row, d *types.Delivery) error {
	return row.Scan(
		&d.ID, &d.BatchID, &d.UserID, &d.ContentItemID, &d.Score, &d.BodyMD,
		&d.CardJSON, &d.FeishuMessageID, &d.Status, &d.SentAt, &d.CreatedAt,
	)
}

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
			batch_id, user_id, content_item_id, score, body_md, card_json,
			feishu_message_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`,
		d.BatchID, d.UserID, d.ContentItemID, d.Score, d.BodyMD, card,
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
			batch_id, user_id, content_item_id, score, body_md, card_json,
			feishu_message_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (batch_id, content_item_id) WHERE content_item_id IS NOT NULL DO NOTHING
		RETURNING id`,
		d.BatchID, d.UserID, d.ContentItemID, d.Score, d.BodyMD, card,
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

// MarkDeliverySent 回填飞书消息 id 与最终卡片 JSON、置 status=sent 并写 sent_at。
// 飞书 SendCard 成功后调用；sentAt 由调用方传入（通常为发送返回时刻）。
// cardJSON 在此回填而非 Insert 时写入：最终卡的按钮 value 携带 delivery_id，
// 只能在拿到 id 之后构造（契约 §8）。唯一调用方是 Push 活动。
func (s *Store) MarkDeliverySent(ctx context.Context, id int64, feishuMessageID string, cardJSON json.RawMessage, sentAt time.Time) error {
	// card_json NOT NULL：nil / 空 RawMessage 归一为 '{}'（同 InsertDelivery）。
	if len(cardJSON) == 0 {
		cardJSON = json.RawMessage("{}")
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE deliveries
		 SET feishu_message_id = $2, card_json = $3, status = $4, sent_at = $5
		 WHERE id = $1`,
		id, feishuMessageID, cardJSON, types.DeliveryStatusSent, sentAt)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("标记投递已发送（id=%d）", id), err)
	}
	return nil
}

// GetDeliveryForUser 按 id 取投递，归属校验进 WHERE（user_id=$2）：按钮 value
// 可伪造，越权与不存在统一 CodeNotFound、零副作用（M4 契约 §10 红线对齐）。
func (s *Store) GetDeliveryForUser(ctx context.Context, id, userID int64) (*types.Delivery, error) {
	var d types.Delivery
	err := scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+`
		 FROM deliveries
		 WHERE id = $1 AND user_id = $2`,
		id, userID), &d)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("投递 id=%d 不存在或不属于用户 %d", id, userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询投递（id=%d, user=%d）", id, userID), err)
	}
	return &d, nil
}

// GetDeliveryByFeishuMessageID 追问反查：回复消息的 ParentId/RootId → delivery。
// 空串双保险（契约 §14）：Go 侧短路——未发送行 feishu_message_id 默认空串，
// 空串查询会误命中；SQL 侧显式追加"不等于空串"的字面谓词——它让 PG generic plan
// 能选中 006 的部分索引，同时是空串防线的 DB 兜底。
// 多行命中取 created_at 最新一条。
func (s *Store) GetDeliveryByFeishuMessageID(ctx context.Context, userID int64, msgID string) (*types.Delivery, error) {
	if msgID == "" {
		return nil, types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("空 message_id 无法反查投递（user=%d）", userID), nil)
	}
	var d types.Delivery
	err := scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+`
		 FROM deliveries
		 WHERE user_id = $1 AND feishu_message_id = $2 AND feishu_message_id <> ''
		 ORDER BY created_at DESC
		 LIMIT 1`,
		userID, msgID), &d)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("消息 %s 无对应投递（user=%d）", msgID, userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("按消息 id 反查投递（user=%d）", userID), err)
	}
	return &d, nil
}
