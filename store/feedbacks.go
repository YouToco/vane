package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// validFeedbackActions 是 feedbacks.action 的合法取值全集（与 types 枚举对齐，
// 001 沿用"枚举由应用层校验、不建 CHECK"的约定，这里就是那道应用层校验）。
var validFeedbackActions = map[types.FeedbackAction]bool{
	types.FeedbackActionInterested:    true,
	types.FeedbackActionNotInterested: true,
	types.FeedbackActionMisjudged:     true,
	types.FeedbackActionDeepDive:      true,
	types.FeedbackActionQuestion:      true,
}

// InsertFeedback 追加一条反馈并返回新行 id。feedbacks 是追加式事件日志：
// 态度可改靠追加新行、消费方取最新为准，刻意无态度类唯一约束（主控裁决——
// (delivery_id, action) 唯一索引会使第三次点击命中旧行、最新态度被错判）。
// action 不在 5 枚举内返回 CodeValidation；detail 由调用方截断。
func (s *Store) InsertFeedback(ctx context.Context, f *types.Feedback) (int64, error) {
	if !validFeedbackActions[f.Action] {
		return 0, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("非法反馈动作 %q", f.Action), nil)
	}
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO feedbacks (user_id, delivery_id, action, detail)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		f.UserID, f.DeliveryID, f.Action, f.Detail).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入反馈（delivery=%d, action=%s）", f.DeliveryID, f.Action), err)
	}
	return id, nil
}

// InsertDeepDiveFeedback 幂等插入 deep_dive 行。f.Detail 是生成正文（调用方截 4000 rune），
// 幂等命中时回传既有 detail 供重发（审查 F4：烧钱结果永不丢失）。
// ⚠️ ON CONFLICT 谓词必须与 006 部分唯一索引 uq_feedbacks_delivery_deep_dive 的
// WHERE 完全一致（action = 'deep_dive' 写作 SQL 字面量），Postgres 才能推断 arbiter；
// action 同理直接写字面量，保证插入行恒落在该部分索引覆盖域内。
//
// 实现同 InsertDeliveryIdempotent 的模式：ON CONFLICT DO NOTHING + RETURNING id。
// 插入成功→有行、existed=false；命中冲突→无行（pgx.ErrNoRows），按 delivery_id
// 回查既有行的 id 与 detail，existed=true。
func (s *Store) InsertDeepDiveFeedback(ctx context.Context, f *types.Feedback) (id int64, existingDetail string, existed bool, err error) {
	if f.Action != types.FeedbackActionDeepDive {
		return 0, "", false, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("InsertDeepDiveFeedback 只接受 deep_dive，实际 %q", f.Action), nil)
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO feedbacks (user_id, delivery_id, action, detail)
		 VALUES ($1, $2, 'deep_dive', $3)
		 ON CONFLICT (delivery_id) WHERE action = 'deep_dive' DO NOTHING
		 RETURNING id`,
		f.UserID, f.DeliveryID, f.Detail).Scan(&id)
	if err == nil {
		return id, "", false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("幂等插入深度解读反馈（delivery=%d）", f.DeliveryID), err)
	}
	// 命中部分唯一索引冲突：该 delivery 已有 deep_dive 行，回查其 id 与生成正文。
	if qerr := s.pool.QueryRow(ctx,
		`SELECT id, detail FROM feedbacks WHERE delivery_id = $1 AND action = 'deep_dive'`,
		f.DeliveryID).Scan(&id, &existingDetail); qerr != nil {
		return 0, "", false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("回查既有深度解读反馈（delivery=%d）", f.DeliveryID), qerr)
	}
	return id, existingDetail, true, nil
}

// LatestFeedbackAction 取该 delivery 在给定动作集合内最新一条的 action
// （ORDER BY created_at DESC, id DESC——同一时间戳的并发插入靠 id 兜底）。
// 无行返回 CodeNotFound。
// ⚠️ 态度语义的调用点（幂等预检、状态行 Preference）恒传 {interested, not_interested}
// 双值集合——传单值会命中旧行、复刻被否决的 (delivery_id,action) 唯一索引 bug（审查 F5）。
func (s *Store) LatestFeedbackAction(ctx context.Context, deliveryID int64, actions []types.FeedbackAction) (types.FeedbackAction, error) {
	// 空集合是调用方 bug：ANY('{}') 恒假会返回 NotFound，把误用伪装成"无反馈"。
	if len(actions) == 0 {
		return "", types.NewAppError(types.CodeValidation,
			fmt.Sprintf("LatestFeedbackAction 动作集合为空（delivery=%d）", deliveryID), nil)
	}
	// 显式转 []string：pgx 的数组编码按元素具体类型注册，具名 string 切片不保证命中。
	strs := make([]string, len(actions))
	for i, a := range actions {
		strs[i] = string(a)
	}
	var action types.FeedbackAction
	err := s.pool.QueryRow(ctx,
		`SELECT action FROM feedbacks
		 WHERE delivery_id = $1 AND action = ANY($2)
		 ORDER BY created_at DESC, id DESC
		 LIMIT 1`,
		deliveryID, strs).Scan(&action)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("投递 %d 在动作集合 %v 内无反馈", deliveryID, actions), err)
		}
		return "", types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询投递 %d 最新反馈动作", deliveryID), err)
	}
	return action, nil
}

// HasFeedback 该 delivery 是否已有指定 action 的反馈（误判一次性 / deep_dive 预检用）。
func (s *Store) HasFeedback(ctx context.Context, deliveryID int64, action types.FeedbackAction) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM feedbacks WHERE delivery_id = $1 AND action = $2)`,
		deliveryID, action).Scan(&exists)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询投递 %d 是否已有 %s 反馈", deliveryID, action), err)
	}
	return exists, nil
}

// GetFeedbackDetail 取该 delivery 指定 action 最新一条反馈的 detail；无行返回 CodeNotFound。
// deep_dive 的幂等命中路径靠它重发既有长文（契约 §10.4 第 1 步 + 审查 F4）：
// 行存在只证明"当初生成成功"，若当时的飞书发送失败，没有重发就成了
// "钱已烧、结果永久不可达"的死锁态——detail 里存着正文，重发即可自愈。
func (s *Store) GetFeedbackDetail(ctx context.Context, deliveryID int64, action types.FeedbackAction) (string, error) {
	var detail string
	err := s.pool.QueryRow(ctx,
		`SELECT detail FROM feedbacks
		 WHERE delivery_id = $1 AND action = $2
		 ORDER BY created_at DESC, id DESC
		 LIMIT 1`,
		deliveryID, action).Scan(&detail)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("投递 %d 无 %s 反馈", deliveryID, action), err)
		}
		return "", types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询投递 %d 的 %s 反馈内容", deliveryID, action), err)
	}
	return detail, nil
}
