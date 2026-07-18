// 推送历史查询（M7 功能 6.4 的数据面）：deliveries 按 owner 倒序翻页，
// 联查 content_items 取展示标题/链接，聚合 feedbacks 全部动作。
// 只读——本文件不写任何表；写路径在 deliveries.go / feedbacks.go。
package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
)

// DeliveryHistoryQuery 是 ListDeliveryHistory 的过滤条件。
type DeliveryHistoryQuery struct {
	PageSize  int    // <=0 → 20；钳上限 100
	PageToken string // (created_at,id) 键集游标，本包编解码，调用方视为不透明串
}

// DeliveryHistoryItem 是推送历史一行：投递本体 + 内容摘要 + 该投递的全部反馈。
// Title 在 SQL 层做过空标题回退（COALESCE 到正文头 200 字符，同 PR#40 快通道口径），
// 前端不需要再兜底。
type DeliveryHistoryItem struct {
	ID        int64              `json:"id"`
	BatchID   int64              `json:"batch_id"`
	Score     float64            `json:"score"`
	Status    string             `json:"status"`
	SentAt    *time.Time         `json:"sent_at,omitempty"`
	CreatedAt time.Time          `json:"created_at"`
	Title     string             `json:"title"` // 展示标题（空标题已回退正文头）
	URL       string             `json:"url"`   // 原文链接；content_item 被删时为空串
	Feedbacks []DeliveryFeedback `json:"feedbacks"`
}

// DeliveryFeedback 是历史行内嵌的一条反馈。
type DeliveryFeedback struct {
	Action    string    `json:"action"` // types.FeedbackAction 原文
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

// ListDeliveryHistory 按 owner 倒序返回推送历史。
//
//	排序 = ORDER BY created_at DESC, id DESC（走 idx_deliveries_user_created）
//	翻页 = (created_at,id) 键集游标；items 满页时 next = 末行键集编码，否则空串
//	total = 该 owner 的投递总数（供前端展示；与页查询非同一快照，±1 可接受）
//
// 反馈用第二条查询按 delivery_id 批量取回再内存装配（页大小 ≤100，IN 列表可控），
// 不做三表 JOIN：feedbacks 是一对多，JOIN 会把 delivery 行乘开，聚合 JSON 又把
// 排序/分页谓词搅进聚合层——两条平查询各自走索引，语义与代价都更直白。
func (s *Store) ListDeliveryHistory(ctx context.Context, userID int64, q DeliveryHistoryQuery) (items []DeliveryHistoryItem, total int64, next string, err error) {
	pageSize := clampHistoryPageSize(q.PageSize)

	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM deliveries WHERE user_id = $1`, userID).Scan(&total); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "统计推送历史总数", err)
	}

	args := []any{userID}
	cursorCond := ""
	if q.PageToken != "" {
		cursorAt, cursorID, derr := decodeHistoryCursor(q.PageToken)
		if derr != nil {
			return nil, 0, "", derr
		}
		args = append(args, cursorAt, cursorID)
		cursorCond = fmt.Sprintf(" AND (d.created_at, d.id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize)

	// 空标题回退与 ListRecentNegativeFeedbackTitles（PR#40）同口径：
	// COALESCE(NULLIF(title,''), left(content,200))；content_item 被删（ON DELETE SET NULL）
	// 时两列均 NULL → 空串，前端显示"(内容已删除)"之类由展示层决定。
	rows, err := s.pool.Query(ctx,
		`SELECT d.id, d.batch_id, d.score, d.status, d.sent_at, d.created_at,
		        COALESCE(NULLIF(ci.title, ''), left(ci.content, 200), ''),
		        COALESCE(ci.url, '')
		 FROM deliveries d
		 LEFT JOIN content_items ci ON ci.id = d.content_item_id
		 WHERE d.user_id = $1`+cursorCond+
			fmt.Sprintf(` ORDER BY d.created_at DESC, d.id DESC LIMIT $%d`, len(args)),
		args...)
	if err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "查询推送历史", err)
	}
	defer rows.Close()

	ids := make([]int64, 0, pageSize)
	for rows.Next() {
		var it DeliveryHistoryItem
		if err := rows.Scan(&it.ID, &it.BatchID, &it.Score, &it.Status,
			&it.SentAt, &it.CreatedAt, &it.Title, &it.URL); err != nil {
			return nil, 0, "", types.NewAppError(types.CodeDatabase, "扫描推送历史行", err)
		}
		it.Feedbacks = []DeliveryFeedback{} // 序列化出 [] 而非 null
		items = append(items, it)
		ids = append(ids, it.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "遍历推送历史结果集", err)
	}

	if len(ids) > 0 {
		if err := s.attachFeedbacks(ctx, userID, ids, items); err != nil {
			return nil, 0, "", err
		}
	}

	if len(items) == pageSize {
		last := items[len(items)-1]
		next = encodeHistoryCursor(last.CreatedAt, last.ID)
	}
	return items, total, next, nil
}

// attachFeedbacks 批量取回本页 delivery 的反馈并按 delivery_id 装回 items。
// user_id 谓词冗余但保留：delivery id 来自上一条查询天然属于该 owner，
// 加谓词让本查询独立成立（防止未来复用时漏掉归属检查）。
func (s *Store) attachFeedbacks(ctx context.Context, userID int64, ids []int64, items []DeliveryHistoryItem) error {
	rows, err := s.pool.Query(ctx,
		`SELECT delivery_id, action, detail, created_at
		 FROM feedbacks
		 WHERE user_id = $1 AND delivery_id = ANY($2)
		 ORDER BY created_at ASC, id ASC`,
		userID, ids)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "查询推送历史的反馈", err)
	}
	defer rows.Close()

	byDelivery := make(map[int64][]DeliveryFeedback, len(ids))
	for rows.Next() {
		var deliveryID int64
		var fb DeliveryFeedback
		if err := rows.Scan(&deliveryID, &fb.Action, &fb.Detail, &fb.CreatedAt); err != nil {
			return types.NewAppError(types.CodeDatabase, "扫描反馈行", err)
		}
		byDelivery[deliveryID] = append(byDelivery[deliveryID], fb)
	}
	if err := rows.Err(); err != nil {
		return types.NewAppError(types.CodeDatabase, "遍历反馈结果集", err)
	}

	for i := range items {
		if fbs, ok := byDelivery[items[i].ID]; ok {
			items[i].Feedbacks = fbs
		}
	}
	return nil
}

// clampHistoryPageSize 钳制页大小到 [1,100]：<=0 → 20。
// 上限 100 比 A2A 的 200 小：本页每行还要带反馈子查询与正文头回退，行更重。
func clampHistoryPageSize(n int) int {
	switch {
	case n <= 0:
		return 20
	case n > 100:
		return 100
	default:
		return n
	}
}

// encodeHistoryCursor / decodeHistoryCursor 与 a2a_tasks.go 的游标同构
// （base64url(unixMicro "|" id)），但 id 是 int64：不共用是因为两处 id 类型不同，
// 强行泛化省不了几行反而把两个包内细节耦在一起。
func encodeHistoryCursor(createdAt time.Time, id int64) string {
	raw := strconv.FormatInt(createdAt.UnixMicro(), 10) + "|" + strconv.FormatInt(id, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeHistoryCursor(token string) (time.Time, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, 0, types.NewAppError(types.CodeValidation, "无效的分页游标", err)
	}
	micros, idStr, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, 0, types.NewAppError(types.CodeValidation, "无效的分页游标", nil)
	}
	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, 0, types.NewAppError(types.CodeValidation, "无效的分页游标", err)
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return time.Time{}, 0, types.NewAppError(types.CodeValidation, "无效的分页游标", err)
	}
	return time.UnixMicro(us), id, nil
}
