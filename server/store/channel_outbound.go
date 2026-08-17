package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/channelruntime"
	"github.com/YouToco/vane/server/types"
)

type ChannelOutboundEffect struct {
	EffectID           string
	TenantID           int64
	UserID             int64
	RouteID            int64
	Provider           string
	AppIdentity        string
	ProviderChatID     string
	ProviderThreadID   string
	EffectKind         string
	PayloadText        string
	PayloadDigest      string
	Status             string
	ProviderMessageIDs []string
	ErrorCode          string
}

func scanChannelOutbound(row pgx.Row) (ChannelOutboundEffect, error) {
	var effect ChannelOutboundEffect
	var effectID uuid.UUID
	var messageIDs []byte
	var errorCode *string
	err := row.Scan(&effectID, &effect.TenantID, &effect.UserID,
		&effect.RouteID, &effect.Provider, &effect.AppIdentity,
		&effect.ProviderChatID, &effect.ProviderThreadID, &effect.EffectKind,
		&effect.PayloadText, &effect.PayloadDigest, &effect.Status,
		&messageIDs, &errorCode)
	if err != nil {
		return ChannelOutboundEffect{}, err
	}
	effect.EffectID = effectID.String()
	if len(messageIDs) > 0 && string(messageIDs) != "null" {
		if err := json.Unmarshal(messageIDs, &effect.ProviderMessageIDs); err != nil {
			return ChannelOutboundEffect{}, err
		}
	}
	if errorCode != nil {
		effect.ErrorCode = *errorCode
	}
	sum := sha256.Sum256([]byte(effect.PayloadText))
	if effect.TenantID <= 0 || effect.UserID <= 0 || effect.RouteID <= 0 ||
		effect.Provider != channelProviderTelegram || !validOutboundKind(effect.EffectKind) ||
		hex.EncodeToString(sum[:]) != effect.PayloadDigest {
		return ChannelOutboundEffect{}, errors.New("channel outbound integrity mismatch")
	}
	return effect, nil
}

const channelOutboundColumns = `effect_id,tenant_id,user_id,route_id,provider,
	app_identity,provider_chat_id,provider_thread_id,effect_kind,payload_text,
	payload_digest,status,provider_message_ids,error_code`

const channelOutboundColumnsE = `e.effect_id,e.tenant_id,e.user_id,e.route_id,e.provider,
	e.app_identity,e.provider_chat_id,e.provider_thread_id,e.effect_kind,e.payload_text,
	e.payload_digest,e.status,e.provider_message_ids,e.error_code`

func validOutboundKind(kind string) bool {
	if len(kind) < 1 || len(kind) > 64 || kind[0] < 'a' || kind[0] > 'z' {
		return false
	}
	for _, char := range kind {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') &&
			char != '_' && char != '-' {
			return false
		}
	}
	return true
}

// PrepareTelegramOutbound freezes the exact route and payload under a stable
// caller-provided UUID. Exact replay returns the existing effect; changed
// payload or scope conflicts before any provider call.
func (s *Store) PrepareTelegramOutbound(
	ctx context.Context, tenantID, userID, routeID int64,
	effectID, effectKind, text string,
) (ChannelOutboundEffect, error) {
	parsed, err := uuid.Parse(effectID)
	if err != nil || parsed.String() != effectID || tenantID <= 0 || userID <= 0 ||
		routeID <= 0 || !validOutboundKind(effectKind) ||
		strings.TrimSpace(text) == "" || len(text) > 262144 {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeValidation,
			"Telegram outbound effect 无效", types.ErrValidation)
	}
	digestBytes := sha256.Sum256([]byte(text))
	digest := hex.EncodeToString(digestBytes[:])
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram outbound 准备", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var route ChannelRoute
	route, err = scanChannelRoute(tx.QueryRow(ctx,
		`SELECT cr.id,cr.tenant_id,cr.user_id,cr.identity_id,cr.provider,
		        cr.app_identity,cr.provider_chat_id,cr.provider_thread_id,
		        cr.chat_type,cr.route_kind,cr.status,cr.bound_at,cr.revoked_at
		   FROM channel_routes cr
		 JOIN channel_identities ci ON ci.id=cr.identity_id
		 JOIN memberships m ON m.tenant_id=cr.tenant_id AND m.user_id=cr.user_id
		 JOIN tenants t ON t.id=cr.tenant_id
		WHERE cr.id=$1 AND cr.tenant_id=$2 AND cr.user_id=$3 AND
		      cr.provider=$4 AND cr.status='active' AND ci.status='active' AND
		      t.status='active' FOR UPDATE OF cr`, routeID, tenantID, userID,
		channelProviderTelegram))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeNotFound,
			"Telegram outbound 路由不可用", types.ErrNotFound)
	}
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound 路由", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_outbound_effects
		 (effect_id,tenant_id,user_id,route_id,provider,app_identity,
		  provider_chat_id,provider_thread_id,effect_kind,payload_text,payload_digest)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (effect_id) DO NOTHING`, effectID, tenantID, userID,
		routeID, route.Provider, route.AppIdentity, route.ProviderChatID,
		route.ProviderThreadID, effectKind, text, digest); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"准备 Telegram outbound effect", err)
	}
	effect, err := scanChannelOutbound(tx.QueryRow(ctx,
		`SELECT `+channelOutboundColumns+` FROM channel_outbound_effects
		  WHERE effect_id=$1 FOR UPDATE`, effectID))
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"读取 Telegram outbound effect", err)
	}
	if effect.TenantID != tenantID || effect.UserID != userID ||
		effect.RouteID != routeID || effect.Provider != route.Provider ||
		effect.AppIdentity != route.AppIdentity ||
		effect.ProviderChatID != route.ProviderChatID ||
		effect.ProviderThreadID != route.ProviderThreadID ||
		effect.EffectKind != effectKind || effect.PayloadDigest != digest ||
		effect.PayloadText != text {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound effect ID 已被不同请求占用", types.ErrConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram outbound 准备", err)
	}
	return effect, nil
}

func (s *Store) ClaimTelegramOutbound(
	ctx context.Context, effectID string,
) (ChannelOutboundEffect, error) {
	return s.ClaimTelegramOutboundAuthorized(ctx, TelegramRuntimeAuthority{
		AppIdentity: "legacy",
	}, effectID)
}

// ClaimTelegramOutboundAuthorized rechecks the exact active manager
// credential, tenant, membership, identity and route in the same transaction
// that crosses prepared -> sending.
func (s *Store) ClaimTelegramOutboundAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority, effectID string,
) (ChannelOutboundEffect, error) {
	return s.claimTelegramOutboundAuthorized(ctx, authority, effectID, nil)
}

// ClaimTelegramOutboundPermitAuthorized binds the durable permit before the
// prepared -> sending transition. A mismatched/forged permit cannot poison an
// otherwise sendable effect by claiming it first and checking afterward.
func (s *Store) ClaimTelegramOutboundPermitAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority,
	permit channelruntime.SendPermit,
) (ChannelOutboundEffect, error) {
	if permit.Validate() != nil || permit.Provider() != channelruntime.ProviderTelegram {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeValidation,
			"Telegram send permit 无效", types.ErrValidation)
	}
	return s.claimTelegramOutboundAuthorized(
		ctx, authority, permit.EffectID(), &permit)
}

func (s *Store) claimTelegramOutboundAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority, effectID string,
	permit *channelruntime.SendPermit,
) (ChannelOutboundEffect, error) {
	dueClause, retryClear := "", ""
	if s.channelSendRetry {
		dueClause = " AND (e.next_send_at IS NULL OR e.next_send_at<=clock_timestamp())"
		retryClear = ",next_send_at=NULL"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram outbound claim", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if authority.AppIdentity == "legacy" {
		// Compatibility callers historically selected the effect by UUID only.
		// They still receive membership/tenant/identity/route revalidation below,
		// but no stored credential claim. Production Managers never use this path.
		authority.AppIdentity = ""
	} else if _, err := lockTelegramRuntimeAuthority(ctx, tx, authority); err != nil {
		return ChannelOutboundEffect{}, err
	}
	var identityID, routeID int64
	var permitTenantID, permitUserID, permitRouteID int64
	var permitKind, permitDigest string
	if permit != nil {
		permitTenantID, permitUserID, permitRouteID =
			permit.TenantID(), permit.UserID(), permit.RouteID()
		permitKind, permitDigest = permit.EffectKind(), permit.PayloadDigest()
	}
	// Select only immutable foreign keys first, then acquire the universal
	// identity -> route -> effect lock order shared with unlink and credential
	// rotation. No effect is locked before its authority ancestors.
	err = tx.QueryRow(ctx,
		`SELECT ci.id,cr.id FROM channel_outbound_effects e
		 JOIN channel_routes cr ON cr.id=e.route_id
		 JOIN channel_identities ci ON ci.id=cr.identity_id
		WHERE e.effect_id=$1 AND e.provider=$2`, effectID,
		channelProviderTelegram).Scan(&identityID, &routeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound effect 不可发送", types.ErrConflict)
	}
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"读取 Telegram outbound authority", err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM channel_identities
		WHERE id=$1 AND status='active' FOR UPDATE`, identityID); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound identity", err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM channel_routes
		WHERE id=$1 AND identity_id=$2 AND status='active' FOR UPDATE`,
		routeID, identityID); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound route", err)
	}
	effect, err := scanChannelOutbound(tx.QueryRow(ctx,
		`SELECT `+channelOutboundColumnsE+` FROM channel_outbound_effects e
		 JOIN channel_routes cr ON cr.id=e.route_id
		 JOIN channel_identities ci ON ci.id=cr.identity_id
		 JOIN memberships m ON m.tenant_id=e.tenant_id AND m.user_id=e.user_id
		 JOIN tenants t ON t.id=e.tenant_id
		WHERE e.effect_id=$1 AND e.provider=$2 AND
		      e.status IN ('prepared','sent') AND
		      (e.status='sent' OR (true`+dueClause+`)) AND
		      e.tenant_id=cr.tenant_id AND e.user_id=cr.user_id AND
		      cr.identity_id=ci.id AND cr.tenant_id=ci.tenant_id AND
		      cr.user_id=ci.user_id AND cr.provider=e.provider AND
		      cr.app_identity=e.app_identity AND cr.status='active' AND
		      ci.status='active' AND t.status='active' AND t.deleted_at IS NULL AND
		      ($3='' OR e.app_identity=$3) AND
		      ($4::bigint=0 OR (e.tenant_id=$4 AND e.user_id=$5)) AND
		      ($6::bigint=0 OR (e.tenant_id=$6 AND e.user_id=$7 AND
		       e.route_id=$8 AND e.effect_kind=$9 AND e.payload_digest=$10))
		FOR UPDATE OF e`, effectID, channelProviderTelegram,
		authority.AppIdentity, authority.TenantID, authority.UserID,
		permitTenantID, permitUserID, permitRouteID, permitKind, permitDigest))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound effect 不可发送", types.ErrConflict)
	}
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound effect", err)
	}
	if effect.Status == "sent" {
		if permit == nil {
			return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
				"Telegram outbound effect 已发送", types.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
				"提交 Telegram outbound 已发送重放", err)
		}
		return effect, nil
	}
	effect, err = scanChannelOutbound(tx.QueryRow(ctx,
		`UPDATE channel_outbound_effects
		    SET status='sending'`+retryClear+`,updated_at=clock_timestamp()
		  WHERE effect_id=$1 AND route_id=$2 AND status='prepared'`+
			strings.ReplaceAll(dueClause, "e.", "")+`
		  RETURNING `+channelOutboundColumns, effectID, routeID))
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound effect claim 冲突", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram outbound claim", err)
	}
	return effect, nil
}

// ClaimNextTelegramOutbound recovers only effects that are proven not to have
// crossed the provider boundary: newly prepared effects and durable 429
// deferrals whose provider retry_after has elapsed. sending/ambiguous rows are
// intentionally excluded because Telegram has no history lookup or caller
// idempotency key that could make a resend safe.
func (s *Store) ClaimNextTelegramOutbound(
	ctx context.Context, appIdentity string,
) (ChannelOutboundEffect, error) {
	return s.ClaimNextTelegramOutboundAuthorized(ctx, TelegramRuntimeAuthority{
		AppIdentity: appIdentity,
	})
}

func (s *Store) ClaimNextTelegramOutboundAuthorized(
	ctx context.Context, authority TelegramRuntimeAuthority,
) (ChannelOutboundEffect, error) {
	appIdentity := authority.AppIdentity
	if strings.TrimSpace(appIdentity) != appIdentity || appIdentity == "" ||
		len(appIdentity) > 128 {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeValidation,
			"Telegram Bot 身份无效", types.ErrValidation)
	}
	dueClause, retryClear := "", ""
	if s.channelSendRetry {
		dueClause = " AND (e.next_send_at IS NULL OR e.next_send_at<=clock_timestamp())"
		retryClear = ",next_send_at=NULL"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"开启 Telegram outbound 恢复", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := lockTelegramRuntimeAuthority(ctx, tx, authority); err != nil {
		return ChannelOutboundEffect{}, err
	}
	var effectID uuid.UUID
	var identityID, routeID int64
	err = tx.QueryRow(ctx,
		`SELECT ci.id,cr.id,e.effect_id FROM channel_outbound_effects e
		 JOIN channel_routes cr ON cr.id=e.route_id
		 JOIN channel_identities ci ON ci.id=cr.identity_id
		 JOIN memberships m ON m.tenant_id=e.tenant_id AND m.user_id=e.user_id
		 JOIN tenants t ON t.id=e.tenant_id
		WHERE e.provider=$1 AND e.app_identity=$2 AND e.status='prepared'`+
			dueClause+` AND e.tenant_id=cr.tenant_id AND e.user_id=cr.user_id AND
		      cr.identity_id=ci.id AND cr.tenant_id=ci.tenant_id AND
		      cr.user_id=ci.user_id AND cr.provider=e.provider AND
		      cr.app_identity=e.app_identity AND cr.status='active' AND
		      ci.status='active' AND t.status='active' AND t.deleted_at IS NULL AND
		      ($3::bigint=0 OR (e.tenant_id=$3 AND e.user_id=$4))
		ORDER BY COALESCE(e.next_send_at,e.created_at),e.created_at,e.effect_id
		LIMIT 1`,
		channelProviderTelegram, appIdentity, authority.TenantID,
		authority.UserID).Scan(&identityID, &routeID, &effectID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeNotFound,
			"没有待恢复的 Telegram outbound effect", types.ErrNotFound)
	}
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"领取 Telegram outbound 恢复", err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM channel_identities
		WHERE id=$1 AND status='active' FOR UPDATE`, identityID); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound 恢复 identity", err)
	}
	if _, err := tx.Exec(ctx, `SELECT id FROM channel_routes
		WHERE id=$1 AND identity_id=$2 AND status='active' FOR UPDATE`,
		routeID, identityID); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound 恢复 route", err)
	}
	if _, err := tx.Exec(ctx, `SELECT effect_id FROM channel_outbound_effects e
		WHERE effect_id=$1 AND route_id=$2 AND status='prepared'`+dueClause+`
		FOR UPDATE`, effectID, routeID); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound 恢复 authority 已变化", err)
	}
	effect, err := scanChannelOutbound(tx.QueryRow(ctx,
		`UPDATE channel_outbound_effects
		    SET status='sending'`+retryClear+`,updated_at=clock_timestamp()
		  WHERE effect_id=$1 AND status='prepared'
		  RETURNING `+channelOutboundColumns, effectID))
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound 恢复领取冲突", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"提交 Telegram outbound 恢复领取", err)
	}
	return effect, nil
}

func (s *Store) CompleteTelegramOutbound(
	ctx context.Context, effect ChannelOutboundEffect, messageIDs []string,
) error {
	if len(messageIDs) == 0 {
		return types.NewAppError(types.CodeValidation,
			"Telegram outbound message IDs 无效", types.ErrValidation)
	}
	for _, id := range messageIDs {
		if strings.TrimSpace(id) != id || id == "" || len(id) > 128 {
			return types.NewAppError(types.CodeValidation,
				"Telegram outbound message ID 无效", types.ErrValidation)
		}
	}
	encoded, _ := json.Marshal(messageIDs)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram outbound 结算", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	command, err := tx.Exec(ctx,
		`UPDATE channel_outbound_effects
		    SET status='sent',provider_message_ids=$2,updated_at=clock_timestamp()
		  WHERE effect_id=$1 AND status='sending'`, effect.EffectID, string(encoded))
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"结算 Telegram outbound", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"Telegram outbound 结算冲突", types.ErrConflict)
	}
	for _, messageID := range messageIDs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO channel_message_mappings
			 (provider,app_identity,provider_chat_id,provider_thread_id,
			  provider_message_id,tenant_id,user_id,route_id,direction,
			  message_kind,correlation_key)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'outbound','notification',$9)`,
			effect.Provider, effect.AppIdentity, effect.ProviderChatID,
			effect.ProviderThreadID, messageID, effect.TenantID, effect.UserID,
			effect.RouteID, "effect:"+effect.EffectID); err != nil {
			return types.NewAppError(types.CodeDatabase,
				"记录 Telegram outbound 消息映射", err)
		}
	}
	if err := settleAggregateTelegramOutboundTx(
		ctx, tx, effect, messageIDs); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram outbound 结算", err)
	}
	return nil
}

func (s *Store) MarkTelegramOutboundRejected(
	ctx context.Context, effect ChannelOutboundEffect, errorCode string,
) error {
	return s.finishTelegramOutbound(ctx, effect, "failed", nil, errorCode)
}

// DeferTelegramOutbound preserves a definite 429 non-send as a durable
// prepared effect. The stable effect ID can be retried after next_send_at;
// ambiguous/provider-crossed states never enter this method.
func (s *Store) DeferTelegramOutbound(
	ctx context.Context, effect ChannelOutboundEffect, retryAfter time.Duration,
	maxRetries int,
) (bool, error) {
	if !s.channelSendRetry {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram outbound 限流迁移尚未生效", types.ErrValidation)
	}
	if retryAfter < time.Second || retryAfter > 24*time.Hour ||
		retryAfter%time.Second != 0 || maxRetries < 1 || maxRetries > 100 {
		return false, types.NewAppError(types.CodeValidation,
			"Telegram outbound 限流参数无效", types.ErrValidation)
	}
	var status string
	err := s.pool.QueryRow(ctx,
		`UPDATE channel_outbound_effects
		    SET send_retry_count=send_retry_count+1,
		        status=CASE WHEN send_retry_count<$4 THEN 'prepared' ELSE 'failed' END,
		        next_send_at=CASE WHEN send_retry_count<$4
		                     THEN clock_timestamp()+make_interval(secs=>$3) ELSE NULL END,
		        error_code=CASE WHEN send_retry_count<$4 THEN NULL
		                   ELSE 'rate_limit_exhausted' END,
		        updated_at=clock_timestamp()
		  WHERE effect_id=$1 AND route_id=$2 AND status='sending' AND
		        provider_message_ids IS NULL
		 RETURNING status`, effect.EffectID, effect.RouteID,
		int64(retryAfter/time.Second), maxRetries).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, types.NewAppError(types.CodeConflict,
			"Telegram outbound 限流状态冲突", types.ErrConflict)
	}
	if err != nil {
		return false, types.NewAppError(types.CodeDatabase,
			"延后 Telegram outbound 限流", err)
	}
	return status == "prepared", nil
}

func (s *Store) MarkTelegramOutboundAmbiguous(
	ctx context.Context, effect ChannelOutboundEffect, messageIDs []string,
	errorCode string,
) error {
	return s.finishTelegramOutbound(ctx, effect, "ambiguous", messageIDs, errorCode)
}

func (s *Store) finishTelegramOutbound(
	ctx context.Context, effect ChannelOutboundEffect, status string,
	messageIDs []string, errorCode string,
) error {
	if strings.TrimSpace(errorCode) == "" || len(errorCode) > 64 {
		errorCode = "provider_" + status
	}
	var encoded any
	if len(messageIDs) > 0 {
		payload, _ := json.Marshal(messageIDs)
		encoded = string(payload)
	}
	command, err := s.pool.Exec(ctx,
		`UPDATE channel_outbound_effects
		    SET status=$2,provider_message_ids=$3,error_code=$4,
		        updated_at=clock_timestamp()
		  WHERE effect_id=$1 AND status='sending'`,
		effect.EffectID, status, encoded, errorCode)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"结算 Telegram outbound 失败", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict,
			"Telegram outbound 失败结算冲突", types.ErrConflict)
	}
	return nil
}
