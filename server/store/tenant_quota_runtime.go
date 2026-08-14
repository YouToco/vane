package store

import (
	"context"
	"fmt"
	"math"

	"github.com/YouToco/vane/types"
	"github.com/jackc/pgx/v5"
)

// QuotaRule is a live tenant bucket configuration. It is used only to verify
// that the financial gate exists while preparing a run; it never enters the
// immutable snapshot because one shared balance cannot obey multiple frozen
// rate/burst generations at once.
type QuotaRule struct {
	Bucket QuotaBucket
	Rate   float64
	Burst  float64
}

func (r QuotaRule) Validate() error {
	if !r.Bucket.Valid() || r.Rate <= 0 || r.Burst <= 0 ||
		math.IsNaN(r.Rate) || math.IsInf(r.Rate, 0) ||
		math.IsNaN(r.Burst) || math.IsInf(r.Burst, 0) {
		return types.NewAppError(types.CodeValidation,
			"运行配额规则无效", types.ErrValidation)
	}
	return nil
}

// LoadQuotaRule returns the current tenant rule. Prepared runs snapshot only
// bucket identity/enforcement generation; this read is a fail-closed readiness
// check that the live financial gate exists.
func (s *Store) LoadQuotaRule(ctx context.Context, tenantID int64, bucket QuotaBucket) (QuotaRule, error) {
	rule := QuotaRule{Bucket: bucket}
	if tenantID <= 0 || !bucket.Valid() {
		return QuotaRule{}, types.NewAppError(types.CodeValidation,
			"运行配额查询参数无效", types.ErrValidation)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT rate, burst FROM tenant_quota WHERE tenant_id = $1 AND bucket = $2`,
		tenantID, string(bucket)).Scan(&rule.Rate, &rule.Burst); err != nil {
		return QuotaRule{}, classifyQuotaErr(err,
			fmt.Sprintf("读取运行配额规则（tenant=%d bucket=%s）", tenantID, bucket))
	}
	if err := rule.Validate(); err != nil {
		return QuotaRule{}, err
	}
	return rule, nil
}

// LoadResearchQuotaRuleV3 reads only the rate/burst policy for the exact
// authenticated research task. The long-lived server runtime deliberately has
// no direct tenant_quota privilege: a SECURITY DEFINER projection verifies the
// bound tenant/user/task and never exposes the mutable token balance.
func (s *Store) LoadResearchQuotaRuleV3(
	ctx context.Context, identity types.RunIdentity, bucket QuotaBucket,
) (QuotaRule, error) {
	rule := QuotaRule{Bucket: bucket}
	if identity.Validate() != nil ||
		identity.RunKind != types.RunSnapshotKindScheduled || !bucket.Valid() {
		return QuotaRule{}, types.NewAppError(types.CodeValidation,
			"research V3 配额查询参数无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return QuotaRule{}, classifyQuotaErr(err, "开始 research V3 配额规则查询")
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := bindResearchV3AppScopeTx(
		ctx, tx, identity.TenantID, identity.UserID,
	); err != nil {
		return QuotaRule{}, err
	}
	if err := tx.QueryRow(ctx, `
		SELECT out_rate,out_burst
		  FROM resolve_research_quota_rule_v1($1,$2,$3,$4)`,
		identity.TenantID, identity.UserID, identity.TaskID, string(bucket),
	).Scan(&rule.Rate, &rule.Burst); err != nil {
		return QuotaRule{}, classifyQuotaErr(err,
			fmt.Sprintf("读取 research V3 配额规则（tenant=%d bucket=%s）",
				identity.TenantID, bucket))
	}
	if err := rule.Validate(); err != nil {
		return QuotaRule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QuotaRule{}, classifyQuotaErr(err, "提交 research V3 配额规则查询")
	}
	return rule, nil
}

// AdjustForTenant reconciles an already-authorized precharge against
// that exact tenant. It deliberately does not require current membership: the
// paid request may finish after revocation, and its durable receipt must still
// settle the tenant that committed the spend. The current row's rate/burst are
// used consistently with every other caller of the shared bucket. The update
// is unconditional and may record debt.
func (s *Store) AdjustForTenant(
	ctx context.Context,
	tenantID int64,
	bucket QuotaBucket,
	delta float64,
) error {
	if delta == 0 {
		return nil
	}
	if tenantID <= 0 {
		return types.NewAppError(types.CodeValidation,
			"运行配额租户无效", types.ErrValidation)
	}
	if !bucket.Valid() {
		return types.NewAppError(types.CodeValidation,
			"运行配额桶无效", types.ErrValidation)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE tenant_quota
		    SET tokens = LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))) + $3,
		        updated_at = now()
		  WHERE tenant_id = $1
		    AND bucket = $2`,
		tenantID, string(bucket), delta)
	if err != nil {
		return classifyQuotaErr(err,
			fmt.Sprintf("按运行租户对账配额（tenant=%d bucket=%s）", tenantID, bucket))
	}
	if tag.RowsAffected() != 1 {
		// Compiled accounting is fail-closed: unlike the legacy best-effort
		// path, a frozen financial contract must never report success when its
		// tenant row vanished (or the user no longer resolves to that tenant).
		return ErrQuotaExceeded
	}
	return nil
}
