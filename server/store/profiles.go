package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/types"
)

// maxProfileTags 是 tags 的库内上限（与演化守门、update_profile 人工上限统一为 12，
// 契约 §2）；profilehint 展示截 10 是刻意分层，与本上限无关。
const maxProfileTags = 12

// profileColumns 是 profiles 表全列，SELECT 与 scanProfile 一一对应。
const profileColumns = `id, user_id, industry, occupation, tags, removed_tags, summary,
	token_budget_daily, tokens_used_today, token_reset_at,
	last_evolved_feedback_id, created_at, updated_at`
const profileColumnsQualified = `p.id,p.user_id,p.industry,p.occupation,p.tags,
	p.removed_tags,p.summary,p.token_budget_daily,p.tokens_used_today,
	p.token_reset_at,p.last_evolved_feedback_id,p.created_at,p.updated_at`

// scanProfile 把一行 profiles 扫进 types.Profile（复用于单行与 RETURNING）。
func scanProfile(row pgx.Row, p *types.Profile) error {
	return row.Scan(
		&p.ID, &p.UserID, &p.Industry, &p.Occupation, &p.Tags, &p.RemovedTags, &p.Summary,
		&p.TokenBudgetDaily, &p.TokensUsedToday, &p.TokenResetAt,
		&p.LastEvolvedFeedbackID, &p.CreatedAt, &p.UpdatedAt,
	)
}

func scanProfileWithAuthority(row pgx.Row, p *types.Profile) error {
	return row.Scan(
		&p.ID, &p.UserID, &p.Industry, &p.Occupation, &p.Tags, &p.RemovedTags, &p.Summary,
		&p.TokenBudgetDaily, &p.TokensUsedToday, &p.TokenResetAt,
		&p.LastEvolvedFeedbackID, &p.CreatedAt, &p.UpdatedAt,
		&p.ProfileEpoch, &p.ProfileVersion,
	)
}

// 写路径纪律（契约 §3.1，CAS 约定前提）：除本文件三个写方法外禁止任何代码直写 profiles。
//   - 首采（UpsertProfileFields）：仅在 profile 不存在时原子创建 manual claims；
//   - 演化（EvolveProfile / AdvanceProfileCursor）：(updated_at, 游标) 双条件 CAS，冲突即退让。

// GetProfile 按 user_id 取画像；无行返回 CodeNotFound。
func (s *Store) GetProfile(ctx context.Context, userID int64) (*types.Profile, error) {
	var p types.Profile
	err := scanProfileWithAuthority(s.pool.QueryRow(ctx,
		`SELECT `+profileColumnsQualified+`,s.active_epoch,s.version
		   FROM profiles p
		   JOIN profile_claim_states s
		     ON s.tenant_id=p.tenant_id AND s.user_id=p.user_id
		  WHERE p.user_id = $1`, userID), &p)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("用户 %d 无画像", userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 画像", userID), err)
	}
	return &p, nil
}

// GetProfileForTenant is the compiled-runtime read path. It refuses to select
// a profile merely because the user currently belongs to some tenant; the row
// must be owned by the exact tenant frozen in the run snapshot.
func (s *Store) GetProfileForTenant(ctx context.Context, tenantID, userID int64) (*types.Profile, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, types.NewAppError(types.CodeValidation, "画像租户范围无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启租户画像读取事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "固定租户画像读取 search_path", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "设置租户画像读取范围", err)
	}
	var p types.Profile
	err = scanProfile(tx.QueryRow(ctx,
		`SELECT `+profileColumns+`
		   FROM profiles
		  WHERE tenant_id = $1 AND user_id = $2`,
		tenantID, userID), &p)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("租户 %d 的用户 %d 无画像", tenantID, userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询租户 %d 的用户 %d 画像", tenantID, userID), err)
	}
	// vane_app intentionally sees no claim-state version. Read that second
	// half through the exact-subject claim authority in the same repeatable
	// read snapshot. This preserves the complete Profile contract (including
	// the surrogate id used by evolve ledgers) without widening vane_app or
	// granting profile billing columns to the claim role.
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "进入租户画像读取 authority", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT active_epoch,version
		   FROM profile_claim_states
		  WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID).Scan(&p.ProfileEpoch, &p.ProfileVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("租户 %d 的用户 %d 无画像 authority", tenantID, userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询租户 %d 的用户 %d 画像 authority", tenantID, userID), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交租户画像读取事务", err)
	}
	return &p, nil
}

// UpsertProfileFields 保留原名以兼容 Agent 首采调用面，但 062 后仅允许首次创建。
// profile、claim_state(v0) 与 manual seed claims 在同一事务提交；已有 profile
// fail-closed，纠正必须走来源级 claim action，避免 profiles/ledger 双权威漂移。
func (s *Store) UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error) {
	if len(tags) > maxProfileTags {
		tags = tags[:maxProfileTags]
	}
	var tenantID, membershipCount int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(min(tenant_id),0),count(*)
		   FROM memberships WHERE user_id=$1`,
		userID).Scan(&tenantID, &membershipCount); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("解析画像租户（user=%d）", userID), err)
	}
	if membershipCount == 0 {
		return nil, types.NewAppError(types.CodeNotFound, "用户没有可用租户 membership", nil)
	}
	if membershipCount != 1 {
		return nil, types.NewAppError(
			types.CodeConflict, "用户存在多个租户 membership，旧画像首采入口拒绝猜测归属", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启画像首采事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "固定画像首采 search_path", err)
	}
	if exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "锁定画像首采租户准入", err)
	} else if !exists {
		return nil, types.NewAppError(types.CodeNotFound, "租户不存在", nil)
	}
	if err := lockExactProfileMembershipRoot(
		ctx, tx, tenantID, userID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "设置画像首采范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "进入画像首采 authority", err)
	}
	var existingUserID int64
	err = tx.QueryRow(ctx,
		`SELECT user_id FROM profiles WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`,
		tenantID, userID).Scan(&existingUserID)
	if err == nil {
		return nil, types.NewAppError(
			types.CodeConflict,
			"来源级画像 authority 已启用，已有画像只能通过主张纠正",
			nil,
		)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeDatabase, "检查画像首采状态", err)
	}
	var p types.Profile
	p.UserID = userID
	err = scanProfileEdit(tx.QueryRow(ctx,
		`INSERT INTO profiles
		    (tenant_id,user_id,industry,occupation,tags,updated_at)
		 VALUES ($1,$2,COALESCE($3,''),COALESCE($4,''),
		         COALESCE($5::text[],'{}'),now())
		 RETURNING `+profileEditColumns,
		tenantID, userID, industry, occupation, tags), &p)
	if err != nil {
		if pgErr := new(pgconn.PgError); errors.As(err, &pgErr) &&
			pgErr.Code == "23505" {
			return nil, types.NewAppError(types.CodeConflict, "画像已经存在", err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("创建画像字段（user=%d）", userID), err)
	}
	if err := seedInitialManualProfileClaimsTx(
		ctx, tx, tenantID, userID, &p); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交画像首采事务", err)
	}
	return &p, nil
}

// EvolveProfile 演化写：SET 列清单只有 summary/tags/last_evolved_feedback_id +
// updated_at=now()（演化输出面硬收窄，契约 §14；标签只增不减由 evolver 守门）。
// CAS 谓词 WHERE user_id AND updated_at=$expectedAt AND last_evolved_feedback_id=$expectedCursor：
// 游标入 CAS token 是因为 AdvanceProfileCursor 不刷 updated_at——若不校验游标，
// 慢演化写回会把已推进的游标回退、反馈被二次消费（审查 F6）。
// 0 行命中返回 CodeConflict，调用方丢弃本次演化（游标不动，下轮在新画像上重新消费）。
func (s *Store) EvolveProfile(
	ctx context.Context, userID int64, summary string, tags []string,
	newCursor int64, expectedAt time.Time, expectedCursor int64,
	expectedEpoch, expectedVersion int64,
) error {
	if tags == nil {
		tags = []string{}
	}
	var tenantID int64
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id FROM profiles WHERE user_id=$1`, userID).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("画像演化 CAS 未命中（user=%d）", userID), nil)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询演化画像租户（user=%d）", userID), err)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启画像演化事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return types.NewAppError(types.CodeDatabase, "固定画像演化 search_path", err)
	}
	if exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID); err != nil {
		return types.NewAppError(types.CodeDatabase, "锁定画像演化租户准入", err)
	} else if !exists {
		return types.NewAppError(types.CodeNotFound, "租户不存在", nil)
	}
	if err := lockExactProfileMembershipRoot(
		ctx, tx, tenantID, userID); err != nil {
		return err
	}
	if err := evolveProfileClaimsTx(
		ctx, tx, tenantID, userID, summary, tags, newCursor,
		expectedAt, expectedCursor, expectedEpoch, expectedVersion, false,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交画像演化事务", err)
	}
	return nil
}

// AdvanceProfileCursor 只推进游标：不动画像内容、刻意不刷 updated_at（游标推进
// 不是"画像变更"，刷了会把并发人工修正的 CAS 语义搅浑）。CAS 谓词同 EvolveProfile
// （updated_at + 旧游标双条件），冲突返回 CodeConflict 由调用方静默跳过。
// 用途：演化"语义失败"时标记该批反馈已消费防死循环（契约 §9）。
func (s *Store) AdvanceProfileCursor(
	ctx context.Context, userID int64, newCursor int64,
	expectedAt time.Time, expectedCursor int64,
	expectedEpoch, expectedVersion int64,
) error {
	var tenantID int64
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id FROM profiles WHERE user_id=$1`, userID).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("画像游标推进 CAS 未命中（user=%d）", userID), nil)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "查询画像游标租户", err)
	}
	tx, err := s.beginProfileClaimScopedTx(ctx, tenantID, true, userID)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启画像游标事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockProfileTx(ctx, tx, tenantID, userID); err != nil {
		return err
	}
	version, _, profileEpoch, err := lockProfileClaimStateTx(
		ctx, tx, tenantID, userID)
	if err != nil {
		return err
	}
	if err := bindProfileEpochTx(ctx, tx, profileEpoch); err != nil {
		return err
	}
	if profileEpoch != expectedEpoch || version != expectedVersion {
		return types.NewAppError(
			types.CodeConflict, "画像游标 epoch/version CAS 未命中", nil)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE profiles
		 SET last_evolved_feedback_id = $3
		 WHERE tenant_id = $1 AND user_id = $2
		   AND updated_at = $4 AND last_evolved_feedback_id = $5
		   AND EXISTS (
		     SELECT 1 FROM profile_claim_states s
		      WHERE s.tenant_id=$1 AND s.user_id=$2
		        AND s.active_epoch=$6
		   )`,
		tenantID, userID, newCursor, expectedAt, expectedCursor, profileEpoch)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("推进画像演化游标（user=%d）", userID), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("画像游标推进 CAS 未命中（user=%d）：画像已被并发修改或游标已推进", userID), nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交画像游标事务", err)
	}
	return nil
}
