package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// FeedbackRepairPlan is an operator-visible, content-addressed preview. Apply
// recomputes the same plan under a profile row lock and refuses drift.
type FeedbackRepairPlan struct {
	TenantID               int64                `json:"tenant_id"`
	UserID                 int64                `json:"user_id"`
	CurrentEvolutionCursor int64                `json:"current_evolution_cursor"`
	ReplayFromID           int64                `json:"replay_from_id"`
	CollateralReplayCount  int64                `json:"collateral_replay_count"`
	Items                  []FeedbackRepairItem `json:"items"`
	Digest                 string               `json:"digest"`
}

type FeedbackRepairItem struct {
	FeedbackID     int64                `json:"feedback_id"`
	DeliveryID     int64                `json:"delivery_id"`
	Detail         string               `json:"detail"`
	ProposedReason types.FeedbackReason `json:"proposed_reason"`
	WasConsumed    bool                 `json:"was_consumed"`
}

// PreviewLegacyFeedbackRepair produces no writes. It only targets the latest
// legacy misjudged row per delivery whose cause was previously stored solely
// as free text.
func (s *Store) PreviewLegacyFeedbackRepair(
	ctx context.Context, tenantID, userID int64,
) (FeedbackRepairPlan, error) {
	if tenantID <= 0 || userID <= 0 {
		return FeedbackRepairPlan{}, types.NewAppError(
			types.CodeValidation, "反馈修复范围无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "开启反馈修复预览", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setTenantContext(ctx, tx, tenantID); err != nil {
		return FeedbackRepairPlan{}, err
	}
	plan, err := buildLegacyFeedbackRepairPlan(ctx, tx, tenantID, userID, false)
	if err != nil {
		return FeedbackRepairPlan{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "提交反馈修复预览", err)
	}
	return plan, nil
}

// ApplyLegacyFeedbackRepair requires the exact digest printed by preview. It
// restores typed causes and rewinds the evolution cursor only when necessary;
// profile content is not modified here, so the normal Evolver remains the sole
// learning writer.
func (s *Store) ApplyLegacyFeedbackRepair(
	ctx context.Context, tenantID, userID int64, expectedDigest string,
) (FeedbackRepairPlan, error) {
	if tenantID <= 0 || userID <= 0 || len(expectedDigest) != 64 {
		return FeedbackRepairPlan{}, types.NewAppError(
			types.CodeValidation, "反馈修复确认参数无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "开启反馈修复事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setTenantContext(ctx, tx, tenantID); err != nil {
		return FeedbackRepairPlan{}, err
	}
	plan, err := buildLegacyFeedbackRepairPlan(ctx, tx, tenantID, userID, true)
	if err != nil {
		return FeedbackRepairPlan{}, err
	}
	if plan.Digest != expectedDigest {
		return FeedbackRepairPlan{}, types.NewAppError(
			types.CodeConflict, "反馈修复预览已变化，请重新预览", types.ErrConflict)
	}
	for _, item := range plan.Items {
		tag, err := tx.Exec(ctx,
			`UPDATE feedbacks
			    SET reason_code = $1
			  WHERE tenant_id = $2 AND user_id = $3 AND id = $4
			    AND action = 'misjudged' AND reason_code IS NULL
			    AND detail = $5`,
			item.ProposedReason, tenantID, userID, item.FeedbackID, item.Detail)
		if err != nil {
			return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "恢复反馈原因码", err)
		}
		if tag.RowsAffected() != 1 {
			return FeedbackRepairPlan{}, types.NewAppError(
				types.CodeConflict, "反馈修复目标已变化", types.ErrConflict)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO feedback_freshness_triage (
			     tenant_id,user_id,feedback_id,delivery_id,reason_code,detail,
			     status,outcome,audit_json
			 )
			 SELECT f.tenant_id,f.user_id,f.id,f.delivery_id,f.reason_code,f.detail,
			        CASE WHEN f.reason_code='outdated_or_out_of_window'
			             THEN 'pending' ELSE 'routed' END,
			        CASE f.reason_code
			          WHEN 'not_relevant' THEN 'interest_signal'
			          WHEN 'duplicate' THEN 'duplicate_diagnostic'
			          WHEN 'factually_wrong' THEN 'factual_diagnostic'
			          WHEN 'poor_source_or_evidence' THEN 'evidence_diagnostic'
			          WHEN 'other' THEN 'manual_review'
			          ELSE NULL
			        END,
			        jsonb_build_object(
			            'schema','vane.feedback-problem-triage/v1',
			            'reason_code',f.reason_code,'detail',f.detail,
			            'source','legacy_feedback_repair'
			        )
			   FROM feedbacks f
			  WHERE f.tenant_id=$1 AND f.user_id=$2 AND f.id=$3
			 ON CONFLICT (feedback_id) DO NOTHING`,
			tenantID, userID, item.FeedbackID,
		); err != nil {
			return FeedbackRepairPlan{}, types.NewAppError(
				types.CodeDatabase, "创建历史反馈诊断记录", err)
		}
	}
	if plan.ReplayFromID > 0 &&
		plan.ReplayFromID <= plan.CurrentEvolutionCursor {
		tag, err := tx.Exec(ctx,
			`UPDATE profiles
			    SET last_evolved_feedback_id = $1, updated_at = clock_timestamp()
			  WHERE tenant_id = $2 AND user_id = $3
			    AND last_evolved_feedback_id = $4`,
			plan.ReplayFromID-1, tenantID, userID, plan.CurrentEvolutionCursor)
		if err != nil {
			return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "重置反馈演化游标", err)
		}
		if tag.RowsAffected() != 1 {
			return FeedbackRepairPlan{}, types.NewAppError(
				types.CodeConflict, "反馈画像游标已变化", types.ErrConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "提交反馈修复", err)
	}
	return plan, nil
}

func buildLegacyFeedbackRepairPlan(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	lockProfile bool,
) (FeedbackRepairPlan, error) {
	lock := ""
	if lockProfile {
		lock = " FOR UPDATE"
	}
	var cursor int64
	err := tx.QueryRow(ctx,
		`SELECT last_evolved_feedback_id
		   FROM profiles
		  WHERE tenant_id = $1 AND user_id = $2`+lock,
		tenantID, userID).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return FeedbackRepairPlan{}, types.NewAppError(
			types.CodeNotFound, "反馈修复画像不存在", err)
	}
	if err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "读取反馈演化游标", err)
	}
	rows, err := tx.Query(ctx,
		`SELECT id, delivery_id, detail, created_at
		   FROM (
		       SELECT DISTINCT ON (delivery_id)
		              id, delivery_id, action, reason_code, detail, created_at
		         FROM feedbacks
		        WHERE tenant_id = $1 AND user_id = $2
		          AND action = 'misjudged'
		        ORDER BY delivery_id, created_at DESC, id DESC
		   ) latest
		  WHERE reason_code IS NULL
		    AND btrim(detail) <> ''
		  ORDER BY id`,
		tenantID, userID)
	if err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "查询历史反馈修复候选", err)
	}
	defer rows.Close()
	plan := FeedbackRepairPlan{
		TenantID: tenantID, UserID: userID, CurrentEvolutionCursor: cursor,
	}
	for rows.Next() {
		var item FeedbackRepairItem
		var feedbackCreatedAt time.Time
		if err := rows.Scan(
			&item.FeedbackID, &item.DeliveryID, &item.Detail, &feedbackCreatedAt,
		); err != nil {
			return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "扫描历史反馈修复候选", err)
		}
		item.ProposedReason = inferLegacyFeedbackReason(item.Detail, feedbackCreatedAt)
		item.WasConsumed = item.FeedbackID <= cursor
		plan.Items = append(plan.Items, item)
		if plan.ReplayFromID == 0 || item.FeedbackID < plan.ReplayFromID {
			plan.ReplayFromID = item.FeedbackID
		}
	}
	if err := rows.Err(); err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "遍历历史反馈修复候选", err)
	}
	if plan.ReplayFromID > 0 && plan.ReplayFromID <= cursor {
		if err := tx.QueryRow(ctx,
			`SELECT count(*)
			   FROM feedbacks
			  WHERE tenant_id = $1 AND user_id = $2
			    AND id >= $3 AND id <= $4`,
			tenantID, userID, plan.ReplayFromID, cursor).
			Scan(&plan.CollateralReplayCount); err != nil {
			return FeedbackRepairPlan{}, types.NewAppError(types.CodeDatabase, "统计反馈重放范围", err)
		}
		var repairedConsumed int64
		for _, item := range plan.Items {
			if item.WasConsumed {
				repairedConsumed++
			}
		}
		plan.CollateralReplayCount -= repairedConsumed
		if plan.CollateralReplayCount < 0 {
			plan.CollateralReplayCount = 0
		}
	}
	digest, err := feedbackRepairDigest(plan)
	if err != nil {
		return FeedbackRepairPlan{}, types.NewAppError(types.CodeInternal, "生成反馈修复摘要", err)
	}
	plan.Digest = digest
	return plan, nil
}

func feedbackRepairDigest(plan FeedbackRepairPlan) (string, error) {
	plan.Digest = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

var (
	// These patterns deliberately recognize only explicit temporal statements.
	// They are used to repair historical free-text feedback, so a conservative
	// false negative is preferable to teaching the profile a wrong diagnosis.
	legacyRelativeAgePattern = regexp.MustCompile(`(?:[1-9][0-9]*|[一二三四五六七八九十百千万两]+)\s*(?:天|日|周|星期|个月|月|年)\s*前`)
	// Submatches 1-3 are YYYY年M月D日/号; 4-6 are YYYY-MM-DD. Keeping
	// parsing local and strict lets a future date fail closed instead of being
	// misclassified as stale merely because it appears beside a complaint.
	legacyAbsoluteDatePattern = regexp.MustCompile(`(?:(20[0-9]{2})年(0?[1-9]|1[0-2])月(0?[1-9]|[12][0-9]|3[01])(?:日|号)|(20[0-9]{2})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]))`)
)

func inferLegacyFeedbackReason(
	detail string,
	feedbackCreatedAt time.Time,
) types.FeedbackReason {
	normalized := strings.ToLower(strings.TrimSpace(detail))
	switch {
	// Specific diagnostics must win over freshness. A feedback such as
	// “3天前才推的这条已经推过” is a duplicate diagnosis, not a topic or
	// window preference.
	case strings.Contains(normalized, "重复"), strings.Contains(normalized, "推过"):
		return types.FeedbackReasonDuplicate
	case strings.Contains(normalized, "错误"), strings.Contains(normalized, "不对"):
		return types.FeedbackReasonFactWrong
	case strings.Contains(normalized, "来源"), strings.Contains(normalized, "证据"):
		return types.FeedbackReasonPoorSource
	case strings.Contains(normalized, "无关"), strings.Contains(normalized, "不相关"):
		return types.FeedbackReasonNotRelevant
	case strings.Contains(normalized, "过时"),
		strings.Contains(normalized, "旧闻"),
		strings.Contains(normalized, "时间范围"):
		return types.FeedbackReasonOutdated
	case isExplicitDelayedDeliveryComplaint(normalized, feedbackCreatedAt):
		return types.FeedbackReasonOutdated
	case legacyRelativeAgePattern.MatchString(normalized) &&
		(strings.Contains(normalized, "这都") || strings.Contains(normalized, "太久") ||
			strings.Contains(normalized, "太早")):
		return types.FeedbackReasonOutdated
	default:
		return types.FeedbackReasonOther
	}
}

// isExplicitDelayedDeliveryComplaint recognizes two conservative historical
// phrasings:
//   - a relative age plus an explicit "why only push now" complaint;
//   - a past absolute date plus the same explicit delivery complaint.
//
// It intentionally does not treat bare “今天”, a bare calendar date, or vague
// “才推/还推” wording as freshness feedback: those phrases are common in
// ordinary task descriptions and must remain available for human review.
func isExplicitDelayedDeliveryComplaint(
	normalized string,
	feedbackCreatedAt time.Time,
) bool {
	hasDeliveryDelayComplaint := strings.Contains(normalized, "怎么现在才推") ||
		strings.Contains(normalized, "为什么现在才推") ||
		strings.Contains(normalized, "现在才推") ||
		strings.Contains(normalized, "这么久才推") ||
		strings.Contains(normalized, "现在还推")
	if legacyRelativeAgePattern.MatchString(normalized) && hasDeliveryDelayComplaint {
		return true
	}
	return hasDeliveryDelayComplaint &&
		legacyPastAbsoluteDate(normalized, feedbackCreatedAt)
}

func legacyPastAbsoluteDate(normalized string, now time.Time) bool {
	match := legacyAbsoluteDatePattern.FindStringSubmatch(normalized)
	if len(match) == 0 {
		return false
	}
	year, month, day := match[1], match[2], match[3]
	if year == "" {
		year, month, day = match[4], match[5], match[6]
	}
	y, _ := strconv.Atoi(year)
	m, _ := strconv.Atoi(month)
	d, _ := strconv.Atoi(day)
	date := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	if date.Year() != y || int(date.Month()) != m || date.Day() != d {
		return false
	}
	todaySource := now.UTC()
	today := time.Date(todaySource.Year(), todaySource.Month(), todaySource.Day(), 0, 0, 0, 0, time.UTC)
	return date.Before(today)
}

func setTenantContext(ctx context.Context, tx pgx.Tx, tenantID int64) error {
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return types.NewAppError(types.CodeDatabase, "设置反馈修复租户上下文", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return types.NewAppError(types.CodeDatabase, "进入反馈修复受限角色", err)
	}
	return nil
}
