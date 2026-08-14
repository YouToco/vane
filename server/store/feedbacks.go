package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// Retained profile-transition lock identity for historical session-fact rows.
// New feedback no longer produces those rows, but replay/downgrade code must
// keep the frozen lock domain until the schema itself can be retired.
const (
	agentSessionFactAdmissionClass = 1447120453 // "VANE"
	agentSessionFactAdmissionKey   = 1095976527 // "ASFO"
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
	return s.InsertFeedbackWithSessionCutoff(ctx, f, time.Time{})
}

// InsertFeedbackWithSessionCutoff is the legacy-compatible production feedback
// ingress. activeSince remains in the signature for existing callers, but new
// feedback no longer produces retired Agent session facts and intentionally
// ignores that historical session cutoff.
func (s *Store) InsertFeedbackWithSessionCutoff(
	ctx context.Context,
	f *types.Feedback,
	_ time.Time,
) (int64, error) {
	if f == nil {
		return 0, types.NewAppError(types.CodeValidation, "反馈不能为空", nil)
	}
	if !validFeedbackActions[f.Action] {
		return 0, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("非法反馈动作 %q", f.Action), nil)
	}
	if f.Action == types.FeedbackActionMisjudged && !f.ReasonCode.Valid() {
		return 0, types.NewAppError(types.CodeValidation,
			"问题反馈必须提供固定原因", nil)
	}
	if f.ReasonCode != "" &&
		(f.Action != types.FeedbackActionMisjudged || !f.ReasonCode.Valid()) {
		return 0, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("非法反馈原因 %q", f.ReasonCode), nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, types.NewAppError(
			types.CodeDatabase, "开始反馈事实事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var tenantID int64
	if err := tx.QueryRow(ctx,
		`SELECT tenant_id FROM deliveries WHERE id=$1 AND user_id=$2`,
		f.DeliveryID, f.UserID).Scan(&tenantID); err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("解析反馈租户（delivery=%d）", f.DeliveryID), err)
	}
	if err := setFeedbackRuntimeContext(
		ctx, tx, tenantID, f.UserID); err != nil {
		return 0, err
	}

	id, _, frozenReason, frozenDetail, err := insertFeedbackFact(
		ctx, tx, tenantID, f)
	if err != nil {
		return 0, err
	}
	if f.Action == types.FeedbackActionMisjudged {
		if _, err := tx.Exec(ctx,
			`INSERT INTO feedback_freshness_triage (
				     tenant_id,user_id,feedback_id,delivery_id,reason_code,detail,
				     status,outcome,audit_json
				 )
				 VALUES (
				     $1,$2,$3,$4,$5,$6,
				     CASE WHEN $5::text='outdated_or_out_of_window'
				          THEN 'pending' ELSE 'routed' END,
				     CASE $5::text
				       WHEN 'not_relevant' THEN 'interest_signal'
				       WHEN 'duplicate' THEN 'duplicate_diagnostic'
				       WHEN 'factually_wrong' THEN 'factual_diagnostic'
				       WHEN 'poor_source_or_evidence' THEN 'evidence_diagnostic'
				       WHEN 'other' THEN 'manual_review'
				       ELSE NULL
				     END,
				     jsonb_build_object(
				         'schema','vane.feedback-problem-triage/v1',
				         'reason_code',$5::text,'detail',$6::text
				     )
				 )
				 ON CONFLICT (feedback_id) DO NOTHING`,
			tenantID, f.UserID, id, f.DeliveryID,
			frozenReason, frozenDetail,
		); err != nil {
			return 0, types.NewAppError(types.CodeDatabase,
				"enqueue feedback freshness triage", err)
		}
		var storedReason types.FeedbackReason
		var storedDetail, auditReason, auditDetail string
		if err := tx.QueryRow(ctx,
			`SELECT reason_code,detail,
			        audit_json->>'reason_code',audit_json->>'detail'
			   FROM feedback_freshness_triage
			  WHERE tenant_id=$1 AND user_id=$2
			    AND feedback_id=$3 AND delivery_id=$4`,
			tenantID, f.UserID, id, f.DeliveryID,
		).Scan(
			&storedReason, &storedDetail, &auditReason, &auditDetail,
		); err != nil {
			return 0, types.NewAppError(types.CodeDatabase,
				"load frozen feedback freshness triage", err)
		}
		if storedReason != frozenReason || storedDetail != frozenDetail ||
			auditReason != string(frozenReason) ||
			auditDetail != frozenDetail {
			return 0, agentEventIntegrityError()
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, types.NewAppError(
			types.CodeDatabase, "提交反馈事实事务", err)
	}
	return id, nil
}

func setFeedbackRuntimeContext(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) error {
	if tenantID <= 0 || userID <= 0 {
		return types.NewAppError(
			types.CodeValidation, "反馈事实主体范围无效", types.ErrValidation)
	}
	if err := setAgentEventRuntimeContext(ctx, tx, tenantID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.user_id',$1,true)`,
		fmt.Sprintf("%d", userID)); err != nil {
		return types.NewAppError(
			types.CodeDatabase, "设置反馈事实用户上下文", err)
	}
	return nil
}

func insertFeedbackFact(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
	f *types.Feedback,
) (
	id int64,
	inserted bool,
	frozenReason types.FeedbackReason,
	frozenDetail string,
	err error,
) {
	if f.Action == types.FeedbackActionMisjudged {
		err = tx.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,reason_code,detail
			 )
			 VALUES ($1,$2,$3,'misjudged',$4,$5)
			 ON CONFLICT (delivery_id,profile_epoch)
			     WHERE action='misjudged' AND reason_code IS NOT NULL
			     DO NOTHING
			 RETURNING id`,
			tenantID, f.UserID, f.DeliveryID, f.ReasonCode, f.Detail,
		).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx,
				`SELECT f.id,f.reason_code,f.detail
				   FROM feedbacks f
				   JOIN deliveries d
				     ON d.id=f.delivery_id AND d.tenant_id=f.tenant_id
				    AND d.user_id=f.user_id
				   LEFT JOIN profiles p
				     ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
				   LEFT JOIN profile_claim_states s
				     ON s.tenant_id=f.tenant_id AND s.user_id=f.user_id
				  WHERE f.delivery_id=$1 AND f.action='misjudged'
				    AND f.reason_code IS NOT NULL
				    AND f.profile_epoch=CASE WHEN p.user_id IS NULL
				         THEN 0 ELSE COALESCE(s.active_epoch,-1) END`,
				f.DeliveryID,
			).Scan(&id, &frozenReason, &frozenDetail)
			if err != nil {
				return 0, false, "", "", types.NewAppError(
					types.CodeDatabase,
					"load existing problem feedback", err)
			}
			if !frozenReason.Valid() {
				return 0, false, "", "", agentEventIntegrityError()
			}
			return id, false, frozenReason, frozenDetail, nil
		}
		frozenReason, frozenDetail = f.ReasonCode, f.Detail
	} else {
		err = tx.QueryRow(ctx,
			`INSERT INTO feedbacks (
			     tenant_id,user_id,delivery_id,action,reason_code,detail
			 )
			 VALUES ($1,$2,$3,$4,NULLIF($5,''),$6)
			 RETURNING id`,
			tenantID, f.UserID, f.DeliveryID, f.Action,
			f.ReasonCode, f.Detail,
		).Scan(&id)
	}
	if err != nil {
		return 0, false, "", "", types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入反馈（delivery=%d, action=%s）",
				f.DeliveryID, f.Action), err)
	}
	return id, true, frozenReason, frozenDetail, nil
}

// InsertDeepDiveFeedback 幂等插入 deep_dive 行。f.Detail 是生成正文（调用方截 4000 rune），
// 幂等命中时回传既有 detail 供重发（审查 F4：烧钱结果永不丢失）。
// ⚠️ ON CONFLICT 谓词必须与 067 部分唯一索引
// uq_feedbacks_delivery_epoch_deep_dive 的
// WHERE 完全一致（action = 'deep_dive' 写作 SQL 字面量），Postgres 才能推断 arbiter；
// action 同理直接写字面量，保证插入行恒落在该部分索引覆盖域内。
//
// 实现同 InsertDeliveryIdempotent 的模式：ON CONFLICT DO NOTHING + RETURNING id。
// 插入成功→有行、existed=false；命中冲突→无行（pgx.ErrNoRows），按 delivery_id
// 回查既有行的 id 与 detail，existed=true。
func (s *Store) InsertDeepDiveFeedback(ctx context.Context, f *types.Feedback) (id int64, existingDetail string, existed bool, err error) {
	if f == nil {
		return 0, "", false, types.NewAppError(
			types.CodeValidation, "深度解读反馈不能为空", nil)
	}
	if f.Action != types.FeedbackActionDeepDive {
		return 0, "", false, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("InsertDeepDiveFeedback 只接受 deep_dive，实际 %q", f.Action), nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, "", false, types.NewAppError(
			types.CodeDatabase, "开始深度解读反馈事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var tenantID int64
	if err := tx.QueryRow(ctx,
		`SELECT tenant_id FROM deliveries WHERE id=$1 AND user_id=$2`,
		f.DeliveryID, f.UserID,
	).Scan(&tenantID); err != nil {
		return 0, "", false, types.NewAppError(
			types.CodeDatabase,
			fmt.Sprintf("解析深度解读反馈租户（delivery=%d）", f.DeliveryID),
			err)
	}
	if err := setFeedbackRuntimeContext(
		ctx, tx, tenantID, f.UserID,
	); err != nil {
		return 0, "", false, err
	}
	id, existingDetail, existed, err = insertDeepDiveFeedbackFact(
		ctx, tx, tenantID, f, nil)
	if err != nil {
		return 0, "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, "", false, types.NewAppError(
			types.CodeDatabase, "提交深度解读反馈事务", err)
	}
	return id, existingDetail, existed, nil
}

func insertDeepDiveFeedbackFact(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
	f *types.Feedback,
	afterConflict func(),
) (id int64, existingDetail string, existed bool, err error) {
	err = tx.QueryRow(ctx,
		`INSERT INTO feedbacks (tenant_id, user_id, delivery_id, action, detail)
		 VALUES ($1,$2,$3,'deep_dive',$4)
		 ON CONFLICT (delivery_id,profile_epoch)
		     WHERE action = 'deep_dive' DO NOTHING
		 RETURNING id`,
		tenantID, f.UserID, f.DeliveryID, f.Detail).Scan(&id)
	if err == nil {
		return id, "", false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("幂等插入深度解读反馈（delivery=%d）", f.DeliveryID), err)
	}
	if afterConflict != nil {
		afterConflict()
	}
	// 命中部分唯一索引冲突：该 delivery 已有 deep_dive 行，回查其 id 与生成正文。
	if qerr := tx.QueryRow(ctx,
		`SELECT f.id,f.detail
		   FROM feedbacks f
		   LEFT JOIN profiles p
		     ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		   LEFT JOIN profile_claim_states s
		     ON s.tenant_id=f.tenant_id AND s.user_id=f.user_id
		  WHERE f.delivery_id=$1 AND f.action='deep_dive'
		    AND f.profile_epoch=CASE WHEN p.user_id IS NULL
		         THEN 0 ELSE COALESCE(s.active_epoch,-1) END`,
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
		`SELECT f.action
		   FROM feedbacks f
		   JOIN deliveries d
		     ON d.id=f.delivery_id AND d.tenant_id=f.tenant_id
		    AND d.user_id=f.user_id
		   LEFT JOIN profiles p
		     ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		   LEFT JOIN profile_claim_states s
		     ON s.tenant_id=d.tenant_id AND s.user_id=d.user_id
		  WHERE f.delivery_id=$1 AND f.action=ANY($2)
		    AND f.profile_epoch=CASE WHEN p.user_id IS NULL
		         THEN 0 ELSE COALESCE(s.active_epoch,-1) END
		  ORDER BY f.created_at DESC,f.id DESC
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
		`SELECT EXISTS (
		   SELECT 1
		     FROM feedbacks f
		     JOIN deliveries d
		       ON d.id=f.delivery_id AND d.tenant_id=f.tenant_id
		      AND d.user_id=f.user_id
		     LEFT JOIN profiles p
		       ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		     LEFT JOIN profile_claim_states s
		       ON s.tenant_id=d.tenant_id AND s.user_id=d.user_id
		    WHERE f.delivery_id=$1 AND f.action=$2
		      AND f.profile_epoch=CASE WHEN p.user_id IS NULL
		           THEN 0 ELSE COALESCE(s.active_epoch,-1) END
		 )`,
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
		`SELECT f.detail
		   FROM feedbacks f
		   JOIN deliveries d
		     ON d.id=f.delivery_id AND d.tenant_id=f.tenant_id
		    AND d.user_id=f.user_id
		   LEFT JOIN profiles p
		     ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		   LEFT JOIN profile_claim_states s
		     ON s.tenant_id=d.tenant_id AND s.user_id=d.user_id
		  WHERE f.delivery_id=$1 AND f.action=$2
		    AND f.profile_epoch=CASE WHEN p.user_id IS NULL
		         THEN 0 ELSE COALESCE(s.active_epoch,-1) END
		  ORDER BY f.created_at DESC,f.id DESC
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
