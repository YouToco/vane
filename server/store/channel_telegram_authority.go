package store

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// TelegramRuntimeAuthority pins one running stored-Bot manager to the exact
// credential generation that created it. AppIdentity is the immutable numeric
// Bot ID returned by getMe. A zero tenant/user/generation triple is reserved
// for the legacy environment adapter; mixed shapes are rejected.
//
// This is a process-to-Store fence, not a substitute for the future narrow
// database role. Channel runtime remains dark for SaaS canary until FORCE RLS
// and a non-owner runtime role are installed and independently verified.
type TelegramRuntimeAuthority struct {
	TenantID             int64
	UserID               int64
	CredentialGeneration int64
	AppIdentity          string
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
	var role types.MembershipRole
	err := tx.QueryRow(ctx, `
		SELECT m.role
		  FROM credential_vault_entries c
		  JOIN memberships m ON m.tenant_id=c.tenant_id AND m.user_id=c.user_id
		  JOIN tenants t ON t.id=c.tenant_id
		 WHERE c.scope_kind='user' AND c.tenant_id=$1 AND c.user_id=$2 AND
		       c.provider=$3 AND c.purpose='bot_api' AND c.generation=$4 AND
		       c.external_identity=$5 AND c.status='active' AND
		       t.status='active' AND t.deleted_at IS NULL
		 FOR SHARE OF c,m,t`, authority.TenantID, authority.UserID,
		channelProviderTelegram, authority.CredentialGeneration,
		authority.AppIdentity).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", types.NewAppError(types.CodeConflict,
			"Telegram runtime 凭证或成员关系已失效", types.ErrConflict)
	}
	if err != nil {
		return "", types.NewAppError(types.CodeDatabase,
			"复核 Telegram runtime 凭证", err)
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
	if _, err := tx.Exec(ctx, `
		UPDATE channel_routes
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
