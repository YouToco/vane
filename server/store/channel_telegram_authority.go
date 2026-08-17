package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/types"
)

const channelRuntimeLoginRole = "vane_channel_runtime"

// TelegramRuntimeAuthority pins one running stored-Bot manager to the exact
// credential generation that created it. AppIdentity is the immutable numeric
// Bot ID returned by getMe. A zero tenant/user/generation triple is reserved
// for the legacy environment adapter; mixed shapes are rejected.
//
// The process pin is accepted only together with migration 155's independently
// authenticated database role and FORCE-RLS attestation row.
type TelegramRuntimeAuthority struct {
	TenantID             int64
	UserID               int64
	CredentialGeneration int64
	AppIdentity          string
}

// VerifyTelegramRuntimeAuthority is the activation/startup half of the dark
// gate.  A schema-owner Store deliberately fails this check.  No stored Bot
// secret or provider endpoint may be touched before it succeeds.
func (s *Store) VerifyTelegramRuntimeAuthority(
	ctx context.Context, authority TelegramRuntimeAuthority,
) error {
	if s == nil || s.pool == nil {
		return types.NewAppError(types.CodeConflict,
			"Telegram channel runtime 未配置", types.ErrConflict)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram runtime 证明", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := attestTelegramRuntimeAuthorityTx(ctx, tx, authority); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram runtime 证明", err)
	}
	return nil
}

// attestTelegramRuntimeAuthorityTx proves all four independently mutable
// authorities in one database snapshot: login role/catalog, FORCE-RLS policy,
// exact credential generation/app identity, and live tenant membership.
func attestTelegramRuntimeAuthorityTx(
	ctx context.Context, tx pgx.Tx, authority TelegramRuntimeAuthority,
) (types.MembershipRole, error) {
	if err := authority.validate(); err != nil {
		return "", err
	}
	if !authority.stored() {
		return "", types.NewAppError(types.CodeConflict,
			"Telegram stored-Bot runtime authority 缺失", types.ErrConflict)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),
		set_config('app.user_id',$2,true)`, fmt.Sprint(authority.TenantID),
		fmt.Sprint(authority.UserID)); err != nil {
		return "", types.NewAppError(types.CodeDatabase,
			"设置 Telegram runtime scope", err)
	}
	var roleSafe, forceRLS, policySafe bool
	if err := tx.QueryRow(ctx, `
		SELECT session_user=current_user AND session_user=$1 AND
		       role.rolcanlogin AND NOT role.rolsuper AND NOT role.rolbypassrls AND
		       NOT role.rolcreatedb AND NOT role.rolcreaterole AND
		       NOT role.rolreplication AND NOT role.rolinherit,
		       relation.relrowsecurity AND relation.relforcerowsecurity,
		       (SELECT count(*)=1 AND bool_and(policy.polpermissive) AND
		               bool_and(policy.polcmd='r') AND
		               bool_and(policy.polroles=ARRAY[role.oid]::oid[]) AND
		               bool_and(pg_catalog.pg_get_expr(policy.polqual,policy.polrelid)
		                 LIKE '%app.tenant_id%' AND
		                 pg_catalog.pg_get_expr(policy.polqual,policy.polrelid)
		                 LIKE '%app.user_id%')
		          FROM pg_catalog.pg_policy policy
		         WHERE policy.polrelid=relation.oid AND
		               policy.polname='channel_runtime_authority_exact_principal')
		  FROM pg_catalog.pg_roles role
		  JOIN pg_catalog.pg_class relation ON relation.oid=
		       to_regclass('public.channel_runtime_authority_attestations')
		 WHERE role.rolname=session_user`, channelRuntimeLoginRole).Scan(
		&roleSafe, &forceRLS, &policySafe); err != nil {
		return "", types.NewAppError(types.CodeConflict,
			"Telegram channel runtime catalog 尚未就绪", err)
	}
	if !roleSafe || !forceRLS || !policySafe {
		return "", types.NewAppError(types.CodeConflict,
			fmt.Sprintf("Telegram channel runtime authority 不安全 role=%t force_rls=%t policy=%t",
				roleSafe, forceRLS, policySafe), types.ErrConflict)
	}
	var role types.MembershipRole
	var attested bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1
		FROM channel_runtime_authority_attestations
		WHERE tenant_id=$1 AND user_id=$2 AND credential_generation=$3 AND
		      app_identity=$4 AND status='active')`, authority.TenantID,
		authority.UserID, authority.CredentialGeneration,
		authority.AppIdentity).Scan(&attested)
	if err == nil && attested {
		err = tx.QueryRow(ctx, `SELECT public.attest_channel_runtime_authority_v155(
			$1,$2,$3,$4)`, authority.TenantID, authority.UserID,
			authority.CredentialGeneration, authority.AppIdentity).Scan(&role)
	}
	var permissionErr *pgconn.PgError
	if errors.As(err, &permissionErr) && permissionErr.Code == "42501" {
		return "", types.NewAppError(types.CodeConflict,
			"Telegram runtime 凭证或成员关系已失效", types.ErrConflict)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(types.CodeConflict,
			"Telegram runtime 凭证或成员关系已失效", types.ErrConflict)
	}
	if !attested {
		return "", types.NewAppError(types.CodeConflict,
			"Telegram runtime authority attestation 已失效", types.ErrConflict)
	}
	if err != nil {
		return "", types.NewAppError(types.CodeDatabase,
			"读取 Telegram runtime authority", err)
	}
	return role, nil
}

func (a TelegramRuntimeAuthority) stored() bool {
	return a.TenantID > 0 && a.UserID > 0 && a.CredentialGeneration > 0
}

func (a TelegramRuntimeAuthority) validate() error {
	if strings.TrimSpace(a.AppIdentity) != a.AppIdentity || a.AppIdentity == "" ||
		len(a.AppIdentity) > 128 {
		return types.NewAppError(types.CodeValidation,
			"Telegram runtime authority 无效", types.ErrValidation)
	}
	if a.stored() {
		return nil
	}
	if a.TenantID != 0 || a.UserID != 0 || a.CredentialGeneration != 0 {
		return types.NewAppError(types.CodeValidation,
			"Telegram runtime authority 无效", types.ErrValidation)
	}
	return nil
}

// lockTelegramRuntimeAuthority establishes the common lock order for every
// stored-Bot claim: credential scope advisory lock -> exact active generation
// -> membership/tenant -> identity/route/effect. Rotation and revoke take the
// same advisory lock before mutating the credential and channel authority, so
// an old manager can never claim new provider work after the mutation commits.
func lockTelegramRuntimeAuthority(
	ctx context.Context, tx pgx.Tx, authority TelegramRuntimeAuthority,
) (types.MembershipRole, error) {
	if err := authority.validate(); err != nil {
		return "", err
	}
	if !authority.stored() {
		return "", nil
	}
	scope := CredentialScope{
		Kind: "user", TenantID: authority.TenantID, UserID: authority.UserID,
		Provider: channelProviderTelegram, Purpose: "bot_api",
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		credentialLockKey(scope)); err != nil {
		return "", types.NewAppError(types.CodeDatabase,
			"锁定 Telegram runtime 凭证", err)
	}
	role, err := attestTelegramRuntimeAuthorityTx(ctx, tx, authority)
	if err != nil {
		return "", err
	}
	return role, nil
}

// revokeTelegramChannelAuthorityTx revokes all not-yet-provider-crossed state
// for one user's Telegram credential in the caller's credential transaction.
// sending/ambiguous/sent states are immutable audit because provider acceptance
// can no longer be disproved.
func revokeTelegramChannelAuthorityTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64, reason string,
) error {
	if tenantID <= 0 || userID <= 0 ||
		(reason != "credential_rotated" && reason != "credential_revoked") {
		return types.NewAppError(types.CodeValidation,
			"Telegram credential authority 撤销参数无效", types.ErrValidation)
	}
	if _, err := tx.Exec(ctx, `
		SELECT ci.id
		  FROM channel_identities ci
		 WHERE ci.tenant_id=$1 AND ci.user_id=$2 AND ci.provider=$3 AND
		       ci.status='active'
		 ORDER BY ci.id FOR UPDATE`, tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram credential identity", err)
	}
	// Every mutator and claim uses identity -> route -> effect after the shared
	// credential/scope admission. This prevents revoke/rotate from deadlocking
	// against a sender that had already selected the same route.
	if _, err := tx.Exec(ctx, `
		SELECT cr.id
		  FROM channel_routes cr
		 WHERE cr.tenant_id=$1 AND cr.user_id=$2 AND cr.provider=$3
		 ORDER BY cr.id FOR UPDATE`, tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram credential routes", err)
	}
	if _, err := tx.Exec(ctx, `
		SELECT r.provider,r.app_identity,r.provider_update_id
		  FROM channel_ingress_receipts r
		 WHERE r.tenant_id=$1 AND r.user_id=$2 AND r.provider=$3 AND
		       r.status IN ('pending','processing','reply_ready')
		 ORDER BY r.provider,r.app_identity,r.provider_update_id FOR UPDATE`,
		tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram credential ingress effects", err)
	}
	if _, err := tx.Exec(ctx, `
		SELECT e.effect_id
		  FROM channel_outbound_effects e
		 WHERE e.tenant_id=$1 AND e.user_id=$2 AND e.provider=$3 AND
		       e.status='prepared'
		 ORDER BY e.effect_id FOR UPDATE`, tenantID, userID,
		channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram credential outbound effects", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE channel_ingress_receipts
		   SET status='failed',reply_text=NULL,error_code=$4,
		       processing_lease=NULL,lease_expires_at=NULL,next_send_at=NULL,
		       updated_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		       status IN ('pending','processing','reply_ready')`,
		tenantID, userID, channelProviderTelegram, reason); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 未跨 provider ingress", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE channel_outbound_effects
		   SET status='failed',error_code=$4,next_send_at=NULL,
		       updated_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND status='prepared'`,
		tenantID, userID, channelProviderTelegram, reason); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 未跨 provider outbound", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM channel_link_requests
		 WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND consumed_at IS NULL`,
		tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 未消费配对码", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM channel_route_link_requests
		 WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND consumed_at IS NULL`,
		tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 未消费路由码", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE channel_routes
		SET status='revoked',revoked_at=clock_timestamp()
		WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND status='active'`,
		tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram credential routes", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE channel_identities
		   SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND status='active'`,
		tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram credential identity", err)
	}
	return nil
}
