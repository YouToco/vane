package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const channelProviderTelegram = "telegram"

func channelMediaEnvelopeEqual(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == 0 && len(right) == 0
	}
	leftEnvelope, leftErr := types.DecodeChannelMessageEnvelopeV1(left)
	rightEnvelope, rightErr := types.DecodeChannelMessageEnvelopeV1(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftCanonical, leftErr := types.MarshalChannelMessageEnvelopeV1(leftEnvelope)
	rightCanonical, rightErr := types.MarshalChannelMessageEnvelopeV1(rightEnvelope)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCanonical, rightCanonical)
}

// ChannelIdentity is an authenticated external actor bound to an existing
// Vane tenant membership. External IDs are routing data, never Vane user IDs.
type ChannelIdentity struct {
	ID             int64
	TenantID       int64
	UserID         int64
	Provider       string
	AppIdentity    string
	ExternalUserID string
	ProviderChatID string
	Status         string
	BoundAt        time.Time
	RevokedAt      *time.Time
}

// ChannelIngress is the durable unit claimed by a channel worker.
type ChannelIngress struct {
	Provider          string
	AppIdentity       string
	ProviderUpdateID  string
	IdentityID        int64
	TenantID          int64
	UserID            int64
	MembershipRole    types.MembershipRole
	ExternalUserID    string
	ProviderChatID    string
	ProviderThreadID  string
	ProviderMessageID string
	RouteID           int64
	IngressKind       string
	CallbackQueryID   string
	PayloadDigest     string
	InputText         string
	MediaEnvelope     []byte
	StableTurnID      string
	Status            string
	AttemptCount      int
	ProcessingLease   string
	ReplyText         string
}

type ChannelDeliveryBlockStats struct {
	Count    int
	OldestAt *time.Time
}

// TelegramBlockedReplyStats exposes terminal failures and provider-crossed
// receipts that need operator attention. They never become an automatic resend
// queue; provider authentication rejection separately makes readiness fail.
func (s *Store) TelegramBlockedReplyStats(
	ctx context.Context, appIdentity string,
) (ChannelDeliveryBlockStats, error) {
	var stats ChannelDeliveryBlockStats
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*)::integer,MIN(updated_at) FROM (
		   SELECT updated_at FROM channel_ingress_receipts
		    WHERE provider=$1 AND app_identity=$2 AND
		          status IN ('sending','ambiguous','failed') AND
		          (status <> 'failed' OR error_code NOT IN
		             ('identity_revoked','route_revoked','bot_membership_lost','topic_closed'))
		   UNION ALL
		   SELECT updated_at FROM channel_outbound_effects
		    WHERE provider=$1 AND app_identity=$2 AND
		          status IN ('sending','ambiguous','failed') AND
		          (status <> 'failed' OR error_code NOT IN
		             ('bot_membership_lost','topic_closed'))
		 ) blocked`,
		channelProviderTelegram, appIdentity).Scan(&stats.Count, &stats.OldestAt)
	if err != nil {
		return ChannelDeliveryBlockStats{}, types.NewAppError(types.CodeDatabase,
			"读取 Telegram 阻断回复", err)
	}
	return stats, nil
}

func (s *Store) TelegramBlockedReplyStatsForUser(
	ctx context.Context, appIdentity string, tenantID, userID int64,
) (ChannelDeliveryBlockStats, error) {
	var stats ChannelDeliveryBlockStats
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*)::integer,MIN(updated_at) FROM (
		   SELECT updated_at FROM channel_ingress_receipts
		    WHERE provider=$1 AND app_identity=$2 AND tenant_id=$3 AND user_id=$4 AND
		          status IN ('sending','ambiguous','failed') AND
		          (status <> 'failed' OR error_code NOT IN
		             ('identity_revoked','route_revoked','bot_membership_lost','topic_closed'))
		   UNION ALL
		   SELECT updated_at FROM channel_outbound_effects
		    WHERE provider=$1 AND app_identity=$2 AND tenant_id=$3 AND user_id=$4 AND
		          status IN ('sending','ambiguous','failed') AND
		          (status <> 'failed' OR error_code NOT IN
		             ('bot_membership_lost','topic_closed'))
		 ) blocked`,
		channelProviderTelegram, appIdentity, tenantID, userID,
	).Scan(&stats.Count, &stats.OldestAt)
	if err != nil {
		return ChannelDeliveryBlockStats{}, types.NewAppError(types.CodeDatabase,
			"读取用户 Telegram 阻断回复", err)
	}
	return stats, nil
}

func validateTelegramIdentityParts(appIdentity, actorID, chatID string) error {
	for _, value := range []string{appIdentity, actorID, chatID} {
		if strings.TrimSpace(value) != value || value == "" || len(value) > 128 {
			return types.NewAppError(types.CodeValidation,
				"Telegram 身份范围无效", types.ErrValidation)
		}
	}
	return nil
}

// IssueTelegramLinkRequest stores only the hash of a one-time pairing token.
// tenantID/userID must come from the authenticated Web session.
func (s *Store) IssueTelegramLinkRequest(
	ctx context.Context,
	tenantID, userID int64,
	appIdentity string,
	tokenHash []byte,
	expiresAt time.Time,
) error {
	if tenantID <= 0 || userID <= 0 || len(tokenHash) != 32 ||
		validateTelegramIdentityParts(appIdentity, "pending", "pending") != nil ||
		!expiresAt.After(time.Now()) {
		return types.NewAppError(types.CodeValidation,
			"Telegram 配对请求无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram 配对请求事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	lockKey := fmt.Sprintf("channel-link/v1:%s", channelProviderTelegram)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 配对请求", err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM channel_link_requests
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        consumed_at IS NULL`, tenantID, userID,
		channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销旧 Telegram 配对请求", err)
	}
	command, err := tx.Exec(ctx,
		`INSERT INTO channel_link_requests
		 (token_hash,tenant_id,user_id,provider,app_identity,expires_at)
		 SELECT $1,$2,$3,$4,$5,$6
		   FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		  WHERE m.tenant_id=$2 AND m.user_id=$3 AND t.status='active'`,
		tokenHash, tenantID, userID, channelProviderTelegram,
		appIdentity, expiresAt,
	)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "创建 Telegram 配对请求", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"当前账号不属于可用租户", types.ErrConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram 配对请求", err)
	}
	return nil
}

// ConsumeTelegramLinkRequest atomically consumes a one-time pairing request
// and binds the exact private-chat actor. Exact response-loss replay returns
// the same identity; a changed actor/chat/digest fails closed.
func (s *Store) ConsumeTelegramLinkRequest(
	ctx context.Context,
	tokenHash []byte,
	appIdentity, actorID, chatID, requestDigest string,
) (ChannelIdentity, bool, error) {
	if len(tokenHash) != 32 ||
		validateTelegramIdentityParts(appIdentity, actorID, chatID) != nil ||
		len(requestDigest) != 64 {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeValidation,
			"Telegram 配对凭据无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
			"开启 Telegram 配对事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Pairing and unlink use the same lock order: provider authority first,
	// then request/identity rows. This prevents a consumed token from racing a
	// successful unlink and avoids token-row/advisory-lock inversion.
	lockKey := fmt.Sprintf("channel-link/v1:%s", channelProviderTelegram)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 配对范围", err)
	}

	var tenantID, userID int64
	var provider, expectedApp string
	var expiresAt time.Time
	var consumedAt *time.Time
	var consumedIdentityID *int64
	var storedDigest *string
	err = tx.QueryRow(ctx,
		`SELECT tenant_id,user_id,provider,app_identity,expires_at,
		        consumed_at,consumed_identity_id,request_digest
		   FROM channel_link_requests
		  WHERE token_hash=$1 FOR UPDATE`, tokenHash,
	).Scan(&tenantID, &userID, &provider, &expectedApp, &expiresAt,
		&consumedAt, &consumedIdentityID, &storedDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeNotFound,
			"Telegram 配对码无效或已过期", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
			"读取 Telegram 配对请求", err)
	}
	if provider != channelProviderTelegram || expectedApp != appIdentity {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeConflict,
			"Telegram 配对码不属于当前机器人", types.ErrConflict)
	}
	if consumedAt != nil {
		if consumedIdentityID == nil || storedDigest == nil ||
			*storedDigest != requestDigest {
			return ChannelIdentity{}, false, types.NewAppError(types.CodeConflict,
				"Telegram 配对码已被不同请求使用", types.ErrConflict)
		}
		identity, ierr := scanChannelIdentity(tx.QueryRow(ctx,
			`SELECT id,tenant_id,user_id,provider,app_identity,external_user_id,
			        provider_chat_id,status,bound_at,revoked_at
			   FROM channel_identities
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
			*consumedIdentityID, tenantID, userID))
		if ierr != nil || identity.Status != "active" ||
			identity.ExternalUserID != actorID ||
			identity.ProviderChatID != chatID ||
			identity.AppIdentity != appIdentity {
			return ChannelIdentity{}, false, types.NewAppError(types.CodeConflict,
				"Telegram 配对重放范围不一致", types.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
				"提交 Telegram 配对重放", err)
		}
		return identity, false, nil
	}
	if !expiresAt.After(time.Now()) {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeNotFound,
			"Telegram 配对码无效或已过期", types.ErrNotFound)
	}

	// Pairing is rare and security-sensitive. A provider-wide lock serializes
	// bot-generation rotation as well as actor/user uniqueness checks; using an
	// app-only lock would let the same Vane user bind two bot generations in a
	// race and strand the old authority.
	var activeTenant bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		    WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active')`,
		tenantID, userID).Scan(&activeTenant); err != nil || !activeTenant {
		if err != nil {
			return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
				"验证 Telegram 配对成员关系", err)
		}
		return ChannelIdentity{}, false, types.NewAppError(types.CodeConflict,
			"Telegram 配对账号已不可用", types.ErrConflict)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel_identities
		    SET status='revoked',revoked_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        app_identity<>$4 AND status='active'`,
		tenantID, userID, provider, appIdentity); err != nil {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
			"撤销旧 Telegram bot generation", err)
	}

	var identity ChannelIdentity
	identity, err = findActiveTelegramIdentityTx(
		ctx, tx, appIdentity, actorID, chatID, tenantID, userID)
	if err != nil {
		return ChannelIdentity{}, false, err
	}
	if identity.ID == 0 {
		identity, err = scanChannelIdentity(tx.QueryRow(ctx,
			`INSERT INTO channel_identities
			 (tenant_id,user_id,provider,app_identity,external_user_id,
			  provider_chat_id,chat_type)
			 VALUES ($1,$2,$3,$4,$5,$6,'private')
			 RETURNING id,tenant_id,user_id,provider,app_identity,
			           external_user_id,provider_chat_id,status,bound_at,revoked_at`,
			tenantID, userID, provider, appIdentity, actorID, chatID))
		if err != nil {
			if isUniqueViolation(err) {
				return ChannelIdentity{}, false, types.NewAppError(types.CodeConflict,
					"Telegram 账号或 Vane 账号已绑定", types.ErrConflict)
			}
			return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
				"建立 Telegram 身份绑定", err)
		}
	}
	if _, err := ensureTelegramPrivateRouteTx(ctx, tx, identity); err != nil {
		return ChannelIdentity{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel_link_requests
		    SET consumed_at=clock_timestamp(),consumed_identity_id=$2,
		        request_digest=$3
		  WHERE token_hash=$1 AND consumed_at IS NULL`,
		tokenHash, identity.ID, requestDigest); err != nil {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
			"消费 Telegram 配对请求", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelIdentity{}, false, types.NewAppError(types.CodeDatabase,
			"提交 Telegram 配对事务", err)
	}
	return identity, true, nil
}

func findActiveTelegramIdentityTx(
	ctx context.Context,
	tx pgx.Tx,
	appIdentity, actorID, chatID string,
	tenantID, userID int64,
) (ChannelIdentity, error) {
	rows, err := tx.Query(ctx,
		`SELECT id,tenant_id,user_id,provider,app_identity,external_user_id,
		        provider_chat_id,status,bound_at,revoked_at
		   FROM channel_identities
		  WHERE provider=$1 AND status='active' AND
		        ((app_identity=$2 AND external_user_id=$3) OR
		         (tenant_id=$4 AND user_id=$5 AND provider=$1))
		  ORDER BY id FOR UPDATE`,
		channelProviderTelegram, appIdentity, actorID, tenantID, userID)
	if err != nil {
		return ChannelIdentity{}, types.NewAppError(types.CodeDatabase,
			"检查 Telegram 现有绑定", err)
	}
	defer rows.Close()
	var found ChannelIdentity
	for rows.Next() {
		identity, scanErr := scanChannelIdentity(rows)
		if scanErr != nil {
			return ChannelIdentity{}, types.NewAppError(types.CodeDatabase,
				"扫描 Telegram 现有绑定", scanErr)
		}
		if identity.AppIdentity != appIdentity ||
			identity.ExternalUserID != actorID ||
			identity.ProviderChatID != chatID ||
			identity.TenantID != tenantID || identity.UserID != userID {
			return ChannelIdentity{}, types.NewAppError(types.CodeConflict,
				"Telegram 账号或 Vane 账号已绑定", types.ErrConflict)
		}
		found = identity
	}
	if err := rows.Err(); err != nil {
		return ChannelIdentity{}, types.NewAppError(types.CodeDatabase,
			"遍历 Telegram 现有绑定", err)
	}
	return found, nil
}

func scanChannelIdentity(row pgx.Row) (ChannelIdentity, error) {
	var identity ChannelIdentity
	err := row.Scan(&identity.ID, &identity.TenantID, &identity.UserID,
		&identity.Provider, &identity.AppIdentity, &identity.ExternalUserID,
		&identity.ProviderChatID, &identity.Status, &identity.BoundAt,
		&identity.RevokedAt)
	return identity, err
}

// ResolveTelegramIdentity is the only principal source for Telegram ingress.
func (s *Store) ResolveTelegramIdentity(
	ctx context.Context, appIdentity, actorID, chatID string,
) (ChannelIdentity, error) {
	if err := validateTelegramIdentityParts(appIdentity, actorID, chatID); err != nil {
		return ChannelIdentity{}, err
	}
	identity, err := scanChannelIdentity(s.pool.QueryRow(ctx,
		`SELECT ci.id,ci.tenant_id,ci.user_id,ci.provider,ci.app_identity,
		        ci.external_user_id,ci.provider_chat_id,ci.status,
		        ci.bound_at,ci.revoked_at
		   FROM channel_identities ci
		   JOIN memberships m ON m.tenant_id=ci.tenant_id AND m.user_id=ci.user_id
		   JOIN tenants t ON t.id=ci.tenant_id
		  WHERE ci.provider=$1 AND ci.app_identity=$2 AND
		        ci.external_user_id=$3 AND ci.provider_chat_id=$4 AND
		        ci.chat_type='private' AND ci.status='active' AND t.status='active'`,
		channelProviderTelegram, appIdentity, actorID, chatID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIdentity{}, types.NewAppError(types.CodeNotFound,
			"Telegram 身份尚未绑定", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIdentity{}, types.NewAppError(types.CodeDatabase,
			"解析 Telegram 身份", err)
	}
	return identity, nil
}

func (s *Store) GetTelegramIdentityForUser(
	ctx context.Context, tenantID, userID int64, appIdentity string,
) (ChannelIdentity, error) {
	identity, err := scanChannelIdentity(s.pool.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,provider,app_identity,external_user_id,
		        provider_chat_id,status,bound_at,revoked_at
		   FROM channel_identities
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        app_identity=$4 AND status='active'`,
		tenantID, userID, channelProviderTelegram, appIdentity))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIdentity{}, types.NewAppError(types.CodeNotFound,
			"Telegram 尚未绑定", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIdentity{}, types.NewAppError(types.CodeDatabase,
			"读取 Telegram 绑定", err)
	}
	return identity, nil
}

func (s *Store) RevokeTelegramIdentity(
	ctx context.Context, tenantID, userID int64, appIdentity string,
) error {
	return s.revokeTelegramIdentity(ctx, TelegramRuntimeAuthority{
		TenantID: tenantID, UserID: userID, AppIdentity: appIdentity,
	})
}

// RevokeTelegramIdentityAuthorized pins unlink to the same credential
// generation as ingress/reply/outbound claims.
func (s *Store) RevokeTelegramIdentityAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority,
) error {
	if !authority.stored() {
		return types.NewAppError(types.CodeValidation,
			"Telegram runtime authority 无效", types.ErrValidation)
	}
	return s.revokeTelegramIdentity(ctx, authority)
}

func (s *Store) revokeTelegramIdentity(
	ctx context.Context, authority TelegramRuntimeAuthority,
) error {
	tenantID, userID, appIdentity := authority.TenantID, authority.UserID,
		authority.AppIdentity
	retryClear := ""
	if s.channelSendRetry {
		retryClear = ",next_send_at=NULL"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram 解绑事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if authority.CredentialGeneration > 0 {
		if _, err := lockTelegramRuntimeAuthority(ctx, tx, authority); err != nil {
			return err
		}
	} else {
		var admitted bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM memberships m JOIN tenants t ON t.id=m.tenant_id
			WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active' AND
			      t.deleted_at IS NULL)`, tenantID, userID).Scan(&admitted); err != nil {
			return types.NewAppError(types.CodeDatabase,
				"复核 Telegram 解绑 scope", err)
		}
		if !admitted {
			return types.NewAppError(types.CodeConflict,
				"Telegram 成员关系已失效", types.ErrConflict)
		}
	}
	lockKey := fmt.Sprintf("channel-link/v1:%s", channelProviderTelegram)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 解绑范围", err)
	}
	var identityID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM channel_identities
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        app_identity=$4 AND status='active'
		  FOR UPDATE`, tenantID, userID, channelProviderTelegram,
		appIdentity).Scan(&identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeNotFound,
			"Telegram 尚未绑定", types.ErrNotFound)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 绑定", err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM channel_routes
		WHERE identity_id=$1 ORDER BY id FOR UPDATE`, identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 解绑路由", err)
	}
	if _, err := tx.Exec(ctx, `SELECT provider,app_identity,provider_update_id
		FROM channel_ingress_receipts WHERE identity_id=$1 AND
		status IN ('pending','processing','reply_ready')
		ORDER BY provider,app_identity,provider_update_id FOR UPDATE`, identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 解绑 effects", err)
	}
	if _, err := tx.Exec(ctx, `SELECT e.effect_id
		FROM channel_outbound_effects e JOIN channel_routes cr ON cr.id=e.route_id
		WHERE cr.identity_id=$1 AND e.status='prepared'
		ORDER BY e.effect_id FOR UPDATE OF e`, identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 解绑 outbound effects", err)
	}
	// Pending, already-claimed processing, and pre-provider replies lose channel
	// authority atomically with the identity. A processing Agent operation was
	// authorized before this row lock linearized unlink; its later settlement
	// cannot send because the receipt is now terminal. A sending receipt is
	// retained as immutable ambiguous audit: provider acceptance is already
	// unknowable, but it does not retain ingress power.
	if _, err := tx.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='failed',reply_text=NULL,error_code='identity_revoked',
		        processing_lease=NULL,lease_expires_at=NULL`+retryClear+`,
		        updated_at=clock_timestamp()
		  WHERE identity_id=$1 AND
		        status IN ('pending','processing','reply_ready')`,
		identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"取消 Telegram 未执行消息", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE channel_outbound_effects e
		SET status='failed',error_code='identity_revoked',next_send_at=NULL,
		    updated_at=clock_timestamp()
		FROM channel_routes cr WHERE cr.id=e.route_id AND cr.identity_id=$1 AND
		    e.status='prepared'`, identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"取消 Telegram 未发送 outbound", err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM channel_link_requests
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        consumed_at IS NULL`, tenantID, userID,
		channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 未消费配对码", err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM channel_route_link_requests
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        consumed_at IS NULL`, tenantID, userID,
		channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 未消费群组连接码", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel_routes
		    SET status='revoked',revoked_at=clock_timestamp()
		  WHERE identity_id=$1 AND status='active'`, identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销 Telegram 路由", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel_identities
		    SET status='revoked',revoked_at=clock_timestamp()
		  WHERE id=$1 AND status='active'`, identityID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"解除 Telegram 绑定", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram 解绑", err)
	}
	return nil
}

// AcceptTelegramIngress inserts the authenticated update exactly once. A
// duplicate carrying different semantic bytes or principal scope conflicts.
func (s *Store) AcceptTelegramIngress(
	ctx context.Context,
	identity ChannelIdentity,
	updateID, payloadDigest, inputText, stableTurnID string,
) (bool, error) {
	resolvedIdentity, route, err := s.ResolveTelegramRoute(
		ctx, identity.AppIdentity, identity.ExternalUserID,
		identity.ProviderChatID, "0")
	if err != nil || resolvedIdentity.ID != identity.ID {
		if err == nil {
			err = types.NewAppError(types.CodeConflict,
				"Telegram 私聊身份范围不一致", types.ErrConflict)
		}
		return false, err
	}
	return s.AcceptTelegramRoutedIngress(ctx, identity, route, updateID,
		payloadDigest, inputText, stableTurnID, "", "message", "", nil)
}

// AcceptTelegramRoutedIngress inserts an authenticated private/group/topic
// update exactly once. The identity and route are both re-checked under row
// locks, so unlink or route revocation linearizes before Agent admission.
func (s *Store) AcceptTelegramRoutedIngress(
	ctx context.Context, identity ChannelIdentity, route ChannelRoute,
	updateID, payloadDigest, inputText, stableTurnID, providerMessageID,
	ingressKind, callbackQueryID string, mediaEnvelope []byte,
) (bool, error) {
	if identity.ID <= 0 || identity.TenantID <= 0 || identity.UserID <= 0 ||
		identity.Provider != channelProviderTelegram || identity.Status != "active" ||
		route.ID <= 0 || route.IdentityID != identity.ID || route.Status != "active" ||
		route.TenantID != identity.TenantID || route.UserID != identity.UserID ||
		route.Provider != identity.Provider || route.AppIdentity != identity.AppIdentity ||
		strings.TrimSpace(updateID) != updateID || updateID == "" || len(updateID) > 128 ||
		len(payloadDigest) != 64 || strings.TrimSpace(inputText) == "" ||
		len(inputText) > 65536 || len(providerMessageID) > 128 ||
		(ingressKind != "message" && ingressKind != "command" && ingressKind != "callback") ||
		(ingressKind == "callback") != (callbackQueryID != "") || len(callbackQueryID) > 128 {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram update 范围无效", types.ErrValidation)
	}
	if len(mediaEnvelope) > 0 {
		if ingressKind != "message" || inputText != "telegram:media-help" {
			return false, types.NewAppError(types.CodeValidation,
				"Telegram 媒体入口语义无效", types.ErrValidation)
		}
		if _, err := types.DecodeChannelMessageEnvelopeV1(mediaEnvelope); err != nil {
			return false, err
		}
	}
	parsedTurn, err := uuid.Parse(stableTurnID)
	if err != nil || parsedTurn.String() != stableTurnID {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram update 稳定 turn 无效", types.ErrValidation)
	}
	parsedUpdate, err := strconv.ParseInt(updateID, 10, 64)
	if err != nil || parsedUpdate < 0 || strconv.FormatInt(parsedUpdate, 10) != updateID {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram update ID 无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"开启 Telegram update 事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var lockedIdentityID, lockedRouteID int64
	err = tx.QueryRow(ctx,
		`SELECT ci.id,cr.id FROM channel_identities ci
		 JOIN channel_routes cr ON cr.identity_id=ci.id
		 JOIN memberships m ON m.tenant_id=ci.tenant_id AND m.user_id=ci.user_id
		 JOIN tenants t ON t.id=ci.tenant_id
		WHERE ci.id=$1 AND ci.tenant_id=$2 AND ci.user_id=$3 AND
		      ci.provider=$4 AND ci.app_identity=$5 AND
		      ci.external_user_id=$6 AND ci.status='active' AND
		      cr.id=$7 AND cr.tenant_id=ci.tenant_id AND cr.user_id=ci.user_id AND
		      cr.provider=$4 AND cr.app_identity=$5 AND
		      cr.provider_chat_id=$8 AND cr.provider_thread_id=$9 AND
		      cr.status='active' AND t.status='active'
		FOR UPDATE OF ci,cr`, identity.ID, identity.TenantID, identity.UserID,
		identity.Provider, identity.AppIdentity, identity.ExternalUserID,
		route.ID, route.ProviderChatID, route.ProviderThreadID,
	).Scan(&lockedIdentityID, &lockedRouteID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, types.NewAppError(types.CodeNotFound,
			"Telegram 身份已失效", types.ErrNotFound)
	}
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"复核 Telegram 身份", err)
	}
	command, err := tx.Exec(ctx,
		`INSERT INTO channel_ingress_receipts
		 (provider,app_identity,provider_update_id,identity_id,tenant_id,user_id,
		  external_user_id,provider_chat_id,payload_digest,input_text,stable_turn_id,
		  route_id,provider_thread_id,provider_message_id,ingress_kind,callback_query_id,
		  media_envelope)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
		         NULLIF($17,'')::jsonb)
		 ON CONFLICT (provider,app_identity,provider_update_id) DO NOTHING`,
		identity.Provider, identity.AppIdentity, updateID, identity.ID,
		identity.TenantID, identity.UserID, identity.ExternalUserID,
		route.ProviderChatID, payloadDigest, inputText, stableTurnID, route.ID,
		route.ProviderThreadID, providerMessageID, ingressKind, callbackQueryID,
		string(mediaEnvelope))
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"接收 Telegram update", err)
	}
	if command.RowsAffected() == 1 {
		if err := tx.Commit(ctx); err != nil {
			return false, types.NewAppError(types.CodeDatabase,
				"提交 Telegram update", err)
		}
		return true, nil
	}
	var storedDigest, storedText, storedTurn, storedMessage, storedKind, storedCallback string
	var storedMedia []byte
	var storedIdentity, storedTenant, storedUser, storedRoute int64
	err = tx.QueryRow(ctx,
		`SELECT identity_id,tenant_id,user_id,payload_digest,input_text,
		        stable_turn_id::text,route_id,provider_message_id,
		        ingress_kind,callback_query_id,media_envelope
		   FROM channel_ingress_receipts
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3`,
		identity.Provider, identity.AppIdentity, updateID,
	).Scan(&storedIdentity, &storedTenant, &storedUser,
		&storedDigest, &storedText, &storedTurn, &storedRoute,
		&storedMessage, &storedKind, &storedCallback, &storedMedia)
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"核对 Telegram update 重放", err)
	}
	if storedIdentity != identity.ID || storedTenant != identity.TenantID ||
		storedUser != identity.UserID || storedDigest != payloadDigest ||
		storedText != inputText || storedTurn != stableTurnID ||
		storedRoute != route.ID || storedMessage != providerMessageID ||
		storedKind != ingressKind || storedCallback != callbackQueryID ||
		!channelMediaEnvelopeEqual(storedMedia, mediaEnvelope) {
		return false, types.NewAppError(types.CodeConflict,
			"Telegram update_id 已被不同请求占用", types.ErrConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"提交 Telegram update 重放", err)
	}
	return false, nil
}

// ClaimNextTelegramIngress claims one pending or safely replayable pre-send
// update. The sending state is intentionally excluded: after crossing the
// provider request boundary, a crashed process cannot prove non-delivery.
func (s *Store) ClaimNextTelegramIngress(
	ctx context.Context, appIdentity string, lease time.Duration,
) (ChannelIngress, error) {
	return s.ClaimNextTelegramIngressAuthorized(ctx, TelegramRuntimeAuthority{
		AppIdentity: appIdentity,
	}, lease)
}

// ClaimNextTelegramIngressAuthorized claims under the exact stored credential
// generation owned by the Manager. The legacy adapter reaches this method with
// only AppIdentity and still receives a live database role, never a fabricated
// owner role.
func (s *Store) ClaimNextTelegramIngressAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority, lease time.Duration,
) (ChannelIngress, error) {
	appIdentity := authority.AppIdentity
	if appIdentity == "" || lease < 5*time.Second || lease > 10*time.Minute {
		return ChannelIngress{}, types.NewAppError(types.CodeValidation,
			"Telegram update claim 参数无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram update claim", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	pinnedRole, err := lockTelegramRuntimeAuthority(ctx, tx, authority)
	if err != nil {
		return ChannelIngress{}, err
	}
	var identityID, routeID int64
	var liveRole types.MembershipRole
	err = tx.QueryRow(ctx,
		`SELECT ci.id,cr.id,m.role
		   FROM channel_ingress_receipts r
		   JOIN channel_identities ci ON ci.id=r.identity_id
		   JOIN channel_routes cr ON cr.id=r.route_id
		   JOIN memberships m ON m.tenant_id=r.tenant_id AND m.user_id=r.user_id
		   JOIN tenants t ON t.id=r.tenant_id
		  WHERE ci.provider=$1 AND ci.app_identity=$2 AND ci.status='active' AND
		        r.tenant_id=ci.tenant_id AND r.user_id=ci.user_id AND
		        cr.tenant_id=r.tenant_id AND cr.user_id=r.user_id AND
		        cr.identity_id=ci.id AND cr.provider=ci.provider AND
		        cr.app_identity=ci.app_identity AND cr.status='active' AND
		        t.status='active' AND t.deleted_at IS NULL AND
		        ($3::bigint=0 OR (r.tenant_id=$3 AND r.user_id=$4)) AND
		        (r.status='pending' OR
		         (r.status='processing' AND r.lease_expires_at<=clock_timestamp())) AND
		        NOT EXISTS (
		          SELECT 1 FROM channel_ingress_receipts earlier
		           WHERE earlier.identity_id=r.identity_id AND
		                 earlier.provider_update_id::bigint < r.provider_update_id::bigint AND
		                 earlier.status IN ('pending','processing','reply_ready'))
		  ORDER BY r.provider_update_id::bigint
		  FOR UPDATE OF ci,cr SKIP LOCKED LIMIT 1`,
		channelProviderTelegram, appIdentity, authority.TenantID,
		authority.UserID).Scan(&identityID, &routeID, &liveRole)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIngress{}, types.NewAppError(types.CodeNotFound,
			"没有待处理 Telegram update", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram update 身份", err)
	}
	if authority.stored() && pinnedRole != liveRole {
		return ChannelIngress{}, types.NewAppError(types.CodeConflict,
			"Telegram runtime 成员角色已变化", types.ErrConflict)
	}
	claimID := uuid.NewString()
	var item ChannelIngress
	err = tx.QueryRow(ctx,
		`WITH candidate AS (
		   SELECT provider,app_identity,provider_update_id
		     FROM channel_ingress_receipts
		    WHERE provider=$1 AND app_identity=$2 AND identity_id=$5 AND route_id=$6 AND
		          (status='pending' OR
		           (status='processing' AND lease_expires_at<=clock_timestamp())) AND
		          NOT EXISTS (
		            SELECT 1 FROM channel_ingress_receipts earlier
		             WHERE earlier.identity_id=channel_ingress_receipts.identity_id AND
		                   earlier.provider_update_id::bigint <
		                     channel_ingress_receipts.provider_update_id::bigint AND
		                   earlier.status IN
		                     ('pending','processing','reply_ready'))
		    ORDER BY provider_update_id::bigint
		    FOR UPDATE SKIP LOCKED LIMIT 1
		 )
		 UPDATE channel_ingress_receipts r
		    SET status='processing',processing_lease=$3,
		        lease_expires_at=clock_timestamp()+make_interval(secs=>$4),
		        attempt_count=attempt_count+1,updated_at=clock_timestamp()
		   FROM candidate c
		  WHERE r.provider=c.provider AND r.app_identity=c.app_identity AND
		        r.provider_update_id=c.provider_update_id
		 RETURNING r.provider,r.app_identity,r.provider_update_id,r.identity_id,
		           r.tenant_id,r.user_id,r.external_user_id,r.provider_chat_id,
		           r.provider_thread_id,r.provider_message_id,r.route_id,
		           r.ingress_kind,r.callback_query_id,
		           r.payload_digest,r.input_text,r.media_envelope,
		           r.stable_turn_id::text,r.status,
		           r.attempt_count,r.processing_lease::text,COALESCE(r.reply_text,'')`,
		channelProviderTelegram, appIdentity, claimID, int64(lease/time.Second),
		identityID, routeID,
	).Scan(&item.Provider, &item.AppIdentity, &item.ProviderUpdateID,
		&item.IdentityID, &item.TenantID, &item.UserID,
		&item.ExternalUserID, &item.ProviderChatID, &item.ProviderThreadID,
		&item.ProviderMessageID, &item.RouteID, &item.IngressKind,
		&item.CallbackQueryID, &item.PayloadDigest,
		&item.InputText, &item.MediaEnvelope, &item.StableTurnID, &item.Status,
		&item.AttemptCount, &item.ProcessingLease, &item.ReplyText)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIngress{}, types.NewAppError(types.CodeNotFound,
			"没有待处理 Telegram update", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"领取 Telegram update", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram update claim", err)
	}
	item.MembershipRole = liveRole
	return item, nil
}

func (s *Store) MarkTelegramIngressReplyReady(
	ctx context.Context, item ChannelIngress, reply string,
) error {
	retryClear := ""
	if s.channelSendRetry {
		retryClear = ",next_send_at=NULL"
	}
	if strings.TrimSpace(reply) == "" || len(reply) > 262144 {
		return types.NewAppError(types.CodeValidation,
			"Telegram 回复内容无效", types.ErrValidation)
	}
	command, err := s.pool.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='reply_ready',reply_text=$5,processing_lease=NULL,
		        lease_expires_at=NULL`+retryClear+`,updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        status='processing' AND processing_lease=$4`,
		item.Provider, item.AppIdentity, item.ProviderUpdateID,
		item.ProcessingLease, reply)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"保存 Telegram Agent 回复", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"Telegram update processing lease 已失效", types.ErrConflict)
	}
	return nil
}

func (s *Store) MarkTelegramIngressFailed(
	ctx context.Context, item ChannelIngress, errorCode string,
) error {
	if strings.TrimSpace(errorCode) == "" || len(errorCode) > 64 {
		errorCode = "internal"
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='failed',error_code=$5,processing_lease=NULL,
		        lease_expires_at=NULL,updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        status='processing' AND processing_lease=$4`,
		item.Provider, item.AppIdentity, item.ProviderUpdateID,
		item.ProcessingLease, errorCode)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"记录 Telegram update 失败", err)
	}
	return nil
}

func (s *Store) ClaimTelegramReplySend(
	ctx context.Context, provider, appIdentity, updateID string,
) (ChannelIngress, error) {
	if provider != channelProviderTelegram {
		return ChannelIngress{}, types.NewAppError(types.CodeValidation,
			"Telegram 回复 provider 无效", types.ErrValidation)
	}
	return s.ClaimTelegramReplySendAuthorized(ctx, TelegramRuntimeAuthority{
		AppIdentity: appIdentity,
	}, updateID)
}

func (s *Store) ClaimTelegramReplySendAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority, updateID string,
) (ChannelIngress, error) {
	provider, appIdentity := channelProviderTelegram, authority.AppIdentity
	dueClause, retryClear := "", ""
	if s.channelSendRetry {
		dueClause = " AND (r.next_send_at IS NULL OR r.next_send_at<=clock_timestamp())"
		retryClear = ",next_send_at=NULL"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram 回复发送 claim", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := lockTelegramRuntimeAuthority(ctx, tx, authority); err != nil {
		return ChannelIngress{}, err
	}
	var identityID int64
	err = tx.QueryRow(ctx,
		`SELECT ci.id
		   FROM channel_ingress_receipts r
		   JOIN channel_identities ci ON ci.id=r.identity_id
		   JOIN channel_routes cr ON cr.id=r.route_id
		   JOIN memberships m ON m.tenant_id=r.tenant_id AND m.user_id=r.user_id
		   JOIN tenants t ON t.id=r.tenant_id
		  WHERE r.provider=$1 AND r.app_identity=$2 AND
		        r.provider_update_id=$3 AND r.status='reply_ready'`+dueClause+` AND
		        r.tenant_id=ci.tenant_id AND r.user_id=ci.user_id AND
		        cr.tenant_id=r.tenant_id AND cr.user_id=r.user_id AND
		        cr.identity_id=ci.id AND ci.status='active' AND cr.status='active' AND
		        t.status='active' AND t.deleted_at IS NULL AND
		        ($4::bigint=0 OR (r.tenant_id=$4 AND r.user_id=$5))
		  FOR UPDATE OF ci,cr`, provider, appIdentity, updateID,
		authority.TenantID, authority.UserID).Scan(&identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIngress{}, types.NewAppError(types.CodeConflict,
			"Telegram 回复不在可发送状态", types.ErrConflict)
	}
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 回复身份", err)
	}
	var item ChannelIngress
	err = tx.QueryRow(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='sending'`+retryClear+`,updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        identity_id=$4 AND status='reply_ready'`+
			strings.ReplaceAll(dueClause, "r.", "")+`
		 RETURNING provider,app_identity,provider_update_id,identity_id,
		           tenant_id,user_id,external_user_id,provider_chat_id,
		           provider_thread_id,provider_message_id,route_id,
		           ingress_kind,callback_query_id,
		           payload_digest,input_text,media_envelope,stable_turn_id::text,status,
		           attempt_count,'',reply_text`,
		provider, appIdentity, updateID, identityID,
	).Scan(&item.Provider, &item.AppIdentity, &item.ProviderUpdateID,
		&item.IdentityID, &item.TenantID, &item.UserID,
		&item.ExternalUserID, &item.ProviderChatID, &item.ProviderThreadID,
		&item.ProviderMessageID, &item.RouteID, &item.IngressKind,
		&item.CallbackQueryID, &item.PayloadDigest,
		&item.InputText, &item.MediaEnvelope, &item.StableTurnID, &item.Status,
		&item.AttemptCount, &item.ProcessingLease, &item.ReplyText)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIngress{}, types.NewAppError(types.CodeConflict,
			"Telegram 回复不在可发送状态", types.ErrConflict)
	}
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"领取 Telegram 回复发送", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram 回复发送 claim", err)
	}
	return item, nil
}

// ClaimNextTelegramReplySend recovers the safe pre-provider crash window. It
// may claim only reply_ready; sending is intentionally not recoverable because
// a previous process may already have crossed Telegram's request boundary.
func (s *Store) ClaimNextTelegramReplySend(
	ctx context.Context, appIdentity string,
) (ChannelIngress, error) {
	return s.ClaimNextTelegramReplySendAuthorized(ctx, TelegramRuntimeAuthority{
		AppIdentity: appIdentity,
	})
}

func (s *Store) ClaimNextTelegramReplySendAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority,
) (ChannelIngress, error) {
	appIdentity := authority.AppIdentity
	dueClause, retryClear := "", ""
	if s.channelSendRetry {
		dueClause = " AND (r.next_send_at IS NULL OR r.next_send_at<=clock_timestamp())"
		retryClear = ",next_send_at=NULL"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram 回复恢复 claim", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := lockTelegramRuntimeAuthority(ctx, tx, authority); err != nil {
		return ChannelIngress{}, err
	}
	var identityID, routeID int64
	err = tx.QueryRow(ctx,
		`SELECT ci.id,cr.id
		   FROM channel_ingress_receipts r
		   JOIN channel_identities ci ON ci.id=r.identity_id
		   JOIN channel_routes cr ON cr.id=r.route_id
		   JOIN memberships m ON m.tenant_id=r.tenant_id AND m.user_id=r.user_id
		   JOIN tenants t ON t.id=r.tenant_id
		  WHERE ci.provider=$1 AND ci.app_identity=$2 AND ci.status='active' AND
		        r.tenant_id=ci.tenant_id AND r.user_id=ci.user_id AND
		        cr.tenant_id=r.tenant_id AND cr.user_id=r.user_id AND
		        cr.identity_id=ci.id AND cr.status='active' AND
		        t.status='active' AND t.deleted_at IS NULL AND
		        ($3::bigint=0 OR (r.tenant_id=$3 AND r.user_id=$4)) AND
		        r.status='reply_ready'`+dueClause+` AND
		        NOT EXISTS (
		          SELECT 1 FROM channel_ingress_receipts earlier
		           WHERE earlier.identity_id=r.identity_id AND
		                 earlier.provider_update_id::bigint < r.provider_update_id::bigint AND
		                 earlier.status IN ('pending','processing','reply_ready'))
		  ORDER BY r.provider_update_id::bigint
		  FOR UPDATE OF ci,cr SKIP LOCKED LIMIT 1`,
		channelProviderTelegram, appIdentity, authority.TenantID,
		authority.UserID).Scan(&identityID, &routeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIngress{}, types.NewAppError(types.CodeNotFound,
			"没有待发送 Telegram 回复", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 回复恢复身份", err)
	}
	var item ChannelIngress
	err = tx.QueryRow(ctx,
		`WITH candidate AS (
		   SELECT provider,app_identity,provider_update_id
		     FROM channel_ingress_receipts
		    WHERE provider=$1 AND app_identity=$2 AND identity_id=$3 AND route_id=$4 AND
		          status='reply_ready'`+strings.ReplaceAll(dueClause, "r.", "")+` AND
		          NOT EXISTS (
		            SELECT 1 FROM channel_ingress_receipts earlier
		             WHERE earlier.identity_id=channel_ingress_receipts.identity_id AND
		                   earlier.provider_update_id::bigint <
		                     channel_ingress_receipts.provider_update_id::bigint AND
		                   earlier.status IN
		                     ('pending','processing','reply_ready'))
		    ORDER BY provider_update_id::bigint
		    FOR UPDATE SKIP LOCKED LIMIT 1
		 )
		 UPDATE channel_ingress_receipts r
		    SET status='sending'`+retryClear+`,updated_at=clock_timestamp()
		   FROM candidate c
		  WHERE r.provider=c.provider AND r.app_identity=c.app_identity AND
		        r.provider_update_id=c.provider_update_id
		 RETURNING r.provider,r.app_identity,r.provider_update_id,r.identity_id,
		           r.tenant_id,r.user_id,r.external_user_id,r.provider_chat_id,
		           r.provider_thread_id,r.provider_message_id,r.route_id,
		           r.ingress_kind,r.callback_query_id,
		           r.payload_digest,r.input_text,r.media_envelope,
		           r.stable_turn_id::text,r.status,
		           r.attempt_count,'',r.reply_text`,
		channelProviderTelegram, appIdentity, identityID, routeID,
	).Scan(&item.Provider, &item.AppIdentity, &item.ProviderUpdateID,
		&item.IdentityID, &item.TenantID, &item.UserID,
		&item.ExternalUserID, &item.ProviderChatID, &item.ProviderThreadID,
		&item.ProviderMessageID, &item.RouteID, &item.IngressKind,
		&item.CallbackQueryID, &item.PayloadDigest,
		&item.InputText, &item.MediaEnvelope, &item.StableTurnID, &item.Status,
		&item.AttemptCount, &item.ProcessingLease, &item.ReplyText)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIngress{}, types.NewAppError(types.CodeNotFound,
			"没有待发送 Telegram 回复", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"恢复 Telegram 回复发送", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelIngress{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram 回复恢复 claim", err)
	}
	return item, nil
}

func (s *Store) CompleteTelegramReply(
	ctx context.Context, item ChannelIngress, messageIDs []string,
) error {
	encoded, err := json.Marshal(messageIDs)
	if err != nil || len(messageIDs) == 0 {
		return types.NewAppError(types.CodeValidation,
			"Telegram message IDs 无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启 Telegram 回复结算", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	command, err := tx.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='completed',provider_message_ids=$4,
		        updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        status='sending'`,
		item.Provider, item.AppIdentity, item.ProviderUpdateID, string(encoded))
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"结算 Telegram 回复", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"Telegram 回复结算状态冲突", types.ErrConflict)
	}
	for _, messageID := range messageIDs {
		if strings.TrimSpace(messageID) != messageID || messageID == "" || len(messageID) > 128 {
			return types.NewAppError(types.CodeValidation,
				"Telegram message ID 无效", types.ErrValidation)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_message_mappings
			 (provider,app_identity,provider_chat_id,provider_thread_id,
			  provider_message_id,tenant_id,user_id,route_id,direction,
			  message_kind,correlation_key)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'outbound',$9,$10)
			 ON CONFLICT DO NOTHING`, item.Provider, item.AppIdentity,
			item.ProviderChatID, item.ProviderThreadID, messageID,
			item.TenantID, item.UserID, item.RouteID,
			item.IngressKind, "ingress:"+item.ProviderUpdateID); err != nil {
			return types.NewAppError(types.CodeDatabase,
				"记录 Telegram 消息映射", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交 Telegram 回复结算", err)
	}
	return nil
}

// DeferTelegramReply moves a provider-declared 429 back to the safe pre-send
// boundary. It is valid only before any chunk was accepted. Once the bounded
// retry budget is exhausted the reply becomes operator-visible failed state.
func (s *Store) DeferTelegramReply(
	ctx context.Context, item ChannelIngress, retryAfter time.Duration,
	maxRetries int,
) (bool, error) {
	if !s.channelSendRetry {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram 回复限流迁移尚未生效", types.ErrValidation)
	}
	if retryAfter < time.Second || retryAfter > 24*time.Hour ||
		retryAfter%time.Second != 0 || maxRetries < 1 || maxRetries > 100 {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram 回复限流参数无效", types.ErrValidation)
	}
	var status string
	err := s.pool.QueryRow(ctx,
		`UPDATE channel_ingress_receipts
		    SET send_retry_count=send_retry_count+1,
		        status=CASE WHEN send_retry_count<$5 THEN 'reply_ready' ELSE 'failed' END,
		        reply_text=CASE WHEN send_retry_count<$5 THEN reply_text ELSE NULL END,
		        next_send_at=CASE WHEN send_retry_count<$5
		                     THEN clock_timestamp()+make_interval(secs=>$4) ELSE NULL END,
		        error_code=CASE WHEN send_retry_count<$5 THEN NULL
		                   ELSE 'rate_limit_exhausted' END,
		        updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        status='sending' AND provider_message_ids IS NULL
		 RETURNING status`, item.Provider, item.AppIdentity, item.ProviderUpdateID,
		int64(retryAfter/time.Second), maxRetries).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, types.NewAppError(types.CodeConflict,
			"Telegram 回复限流状态冲突", types.ErrConflict)
	}
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"延后 Telegram 限流回复", err)
	}
	return status == "reply_ready", nil
}

// MarkTelegramReplyRejected records a provider-declared rejection before any
// chunk was accepted. It is terminal and distinct from ambiguous response loss;
// a future operator retry must be an explicit new effect.
func (s *Store) MarkTelegramReplyRejected(
	ctx context.Context, item ChannelIngress, errorCode string,
) error {
	if strings.TrimSpace(errorCode) == "" || len(errorCode) > 64 {
		errorCode = "provider_rejected"
	}
	command, err := s.pool.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='failed',reply_text=NULL,error_code=$4,
		        updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        status='sending' AND provider_message_ids IS NULL`,
		item.Provider, item.AppIdentity, item.ProviderUpdateID, errorCode)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"记录 Telegram 明确拒绝", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"Telegram 明确拒绝状态冲突", types.ErrConflict)
	}
	return nil
}

func (s *Store) MarkTelegramReplyAmbiguous(
	ctx context.Context, item ChannelIngress, messageIDs []string, errorCode string,
) error {
	if strings.TrimSpace(errorCode) == "" || len(errorCode) > 64 {
		errorCode = "provider_ambiguous"
	}
	var encoded any
	if len(messageIDs) > 0 {
		payload, _ := json.Marshal(messageIDs)
		encoded = string(payload)
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='ambiguous',provider_message_ids=$4,error_code=$5,
		        updated_at=clock_timestamp()
		  WHERE provider=$1 AND app_identity=$2 AND provider_update_id=$3 AND
		        status='sending'`,
		item.Provider, item.AppIdentity, item.ProviderUpdateID,
		encoded, errorCode)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"阻断不确定 Telegram 回复", err)
	}
	return nil
}
