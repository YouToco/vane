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
	var routeID int64
	err = tx.QueryRow(ctx,
		`SELECT cr.id FROM channel_outbound_effects e
		 JOIN channel_routes cr ON cr.id=e.route_id
		 JOIN channel_identities ci ON ci.id=cr.identity_id
		WHERE e.effect_id=$1 AND e.provider=$2 AND e.status='prepared'`+dueClause+` AND
		      cr.status='active' AND ci.status='active'
		FOR UPDATE OF cr`, effectID, channelProviderTelegram).Scan(&routeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeConflict,
			"Telegram outbound effect 不可发送", types.ErrConflict)
	}
	if err != nil {
		return ChannelOutboundEffect{}, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram outbound authority", err)
	}
	effect, err := scanChannelOutbound(tx.QueryRow(ctx,
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
