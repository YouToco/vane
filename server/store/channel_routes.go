package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

type ChannelRoute struct {
	ID               int64
	TenantID         int64
	UserID           int64
	IdentityID       int64
	Provider         string
	AppIdentity      string
	ProviderChatID   string
	ProviderThreadID string
	ChatType         string
	RouteKind        string
	Status           string
	BoundAt          time.Time
	RevokedAt        *time.Time
}

func scanChannelRoute(row pgx.Row) (ChannelRoute, error) {
	var route ChannelRoute
	err := row.Scan(&route.ID, &route.TenantID, &route.UserID,
		&route.IdentityID, &route.Provider, &route.AppIdentity,
		&route.ProviderChatID, &route.ProviderThreadID, &route.ChatType,
		&route.RouteKind, &route.Status, &route.BoundAt, &route.RevokedAt)
	return route, err
}

const channelRouteColumns = `id,tenant_id,user_id,identity_id,provider,
	app_identity,provider_chat_id,provider_thread_id,chat_type,route_kind,
	status,bound_at,revoked_at`

func validTelegramRouteParts(chatID, threadID, chatType string) bool {
	if strings.TrimSpace(chatID) != chatID || chatID == "" || len(chatID) > 128 ||
		strings.TrimSpace(threadID) != threadID || threadID == "" || len(threadID) > 32 {
		return false
	}
	parsedChat, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil || parsedChat == 0 || strconv.FormatInt(parsedChat, 10) != chatID {
		return false
	}
	parsedThread, err := strconv.ParseInt(threadID, 10, 64)
	if err != nil || parsedThread < 0 || strconv.FormatInt(parsedThread, 10) != threadID {
		return false
	}
	switch chatType {
	case "group", "supergroup":
		return true
	default:
		return false
	}
}

func telegramRouteKind(chatType, threadID string) string {
	if chatType == "supergroup" && threadID != "0" {
		return "topic"
	}
	return "group"
}

// EnsureTelegramPrivateRoute is called while the identity-pairing transaction
// holds the provider authority lock. It upgrades legacy/private identity rows
// into the route model without changing their authorization semantics.
func ensureTelegramPrivateRouteTx(
	ctx context.Context, tx pgx.Tx, identity ChannelIdentity,
) (ChannelRoute, error) {
	_, err := tx.Exec(ctx,
		`INSERT INTO channel_routes
		 (tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
		  provider_thread_id,chat_type,route_kind)
		 VALUES ($1,$2,$3,$4,$5,$6,'0','private','private')
		 ON CONFLICT (provider,app_identity,provider_chat_id,provider_thread_id)
		 WHERE status='active' DO NOTHING`,
		identity.TenantID, identity.UserID, identity.ID, identity.Provider,
		identity.AppIdentity, identity.ProviderChatID)
	if err != nil {
		return ChannelRoute{}, types.NewAppError(types.CodeDatabase,
			"建立 Telegram 私聊路由", err)
	}
	route, err := scanChannelRoute(tx.QueryRow(ctx,
		`SELECT `+channelRouteColumns+` FROM channel_routes
		  WHERE provider=$1 AND app_identity=$2 AND provider_chat_id=$3 AND
		        provider_thread_id='0' AND status='active' FOR UPDATE`,
		channelProviderTelegram, identity.AppIdentity, identity.ProviderChatID))
	if err != nil || route.IdentityID != identity.ID ||
		route.TenantID != identity.TenantID || route.UserID != identity.UserID {
		if err == nil {
			err = types.ErrConflict
		}
		return ChannelRoute{}, types.NewAppError(types.CodeConflict,
			"Telegram 私聊路由已被其他身份占用", err)
	}
	return route, nil
}

func (s *Store) IssueTelegramRouteLinkRequest(
	ctx context.Context, tenantID, userID int64, appIdentity string,
	tokenHash []byte, expiresAt time.Time,
) error {
	if tenantID <= 0 || userID <= 0 || len(tokenHash) != 32 ||
		validateTelegramIdentityParts(appIdentity, "pending", "pending") != nil ||
		!expiresAt.After(time.Now()) {
		return types.NewAppError(types.CodeValidation,
			"Telegram 群组连接请求无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram 群组连接请求", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	lockKey := fmt.Sprintf("channel-link/v1:%s", channelProviderTelegram)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 群组连接请求", err)
	}
	var identityID int64
	err = tx.QueryRow(ctx,
		`SELECT ci.id FROM channel_identities ci
		 JOIN memberships m ON m.tenant_id=ci.tenant_id AND m.user_id=ci.user_id
		 JOIN tenants t ON t.id=ci.tenant_id
		WHERE ci.tenant_id=$1 AND ci.user_id=$2 AND ci.provider=$3 AND
		      ci.app_identity=$4 AND ci.status='active' AND t.status='active'
		FOR KEY SHARE OF ci,m,t`, tenantID, userID,
		channelProviderTelegram, appIdentity).Scan(&identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeConflict,
			"请先完成 Telegram 私聊绑定", types.ErrConflict)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"验证 Telegram 群组连接身份", err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM channel_route_link_requests
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND consumed_at IS NULL`,
		tenantID, userID, channelProviderTelegram); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"撤销旧 Telegram 群组连接码", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_route_link_requests
		 (token_hash,tenant_id,user_id,provider,app_identity,expires_at)
		 VALUES ($1,$2,$3,$4,$5,$6)`, tokenHash, tenantID, userID,
		channelProviderTelegram, appIdentity, expiresAt); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"创建 Telegram 群组连接码", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram 群组连接请求", err)
	}
	return nil
}

func (s *Store) ConsumeTelegramRouteLinkRequest(
	ctx context.Context, tokenHash []byte, appIdentity, actorID, chatID,
	threadID, chatType, requestDigest string,
) (ChannelRoute, bool, error) {
	if len(tokenHash) != 32 ||
		validateTelegramIdentityParts(appIdentity, actorID, chatID) != nil ||
		!validTelegramRouteParts(chatID, threadID, chatType) ||
		len(requestDigest) != 64 {
		return ChannelRoute{}, false, types.NewAppError(types.CodeValidation,
			"Telegram 群组连接凭据无效", types.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"开启 Telegram 群组连接事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	lockKey := fmt.Sprintf("channel-link/v1:%s", channelProviderTelegram)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 群组连接范围", err)
	}
	var tenantID, userID int64
	var provider, expectedApp string
	var expiresAt time.Time
	var consumedAt *time.Time
	var consumedRouteID *int64
	var storedDigest *string
	err = tx.QueryRow(ctx,
		`SELECT tenant_id,user_id,provider,app_identity,expires_at,
		        consumed_at,consumed_route_id,request_digest
		   FROM channel_route_link_requests WHERE token_hash=$1 FOR UPDATE`,
		tokenHash).Scan(&tenantID, &userID, &provider, &expectedApp,
		&expiresAt, &consumedAt, &consumedRouteID, &storedDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelRoute{}, false, types.NewAppError(types.CodeNotFound,
			"Telegram 群组连接码无效或已过期", types.ErrNotFound)
	}
	if err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"读取 Telegram 群组连接请求", err)
	}
	if provider != channelProviderTelegram || expectedApp != appIdentity {
		return ChannelRoute{}, false, types.NewAppError(types.CodeConflict,
			"Telegram 群组连接码不属于当前机器人", types.ErrConflict)
	}
	if consumedAt != nil {
		if consumedRouteID == nil || storedDigest == nil || *storedDigest != requestDigest {
			return ChannelRoute{}, false, types.NewAppError(types.CodeConflict,
				"Telegram 群组连接码已被不同请求使用", types.ErrConflict)
		}
		route, scanErr := scanChannelRoute(tx.QueryRow(ctx,
			`SELECT `+channelRouteColumns+` FROM channel_routes
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
			*consumedRouteID, tenantID, userID))
		if scanErr != nil || route.Status != "active" ||
			route.ProviderChatID != chatID || route.ProviderThreadID != threadID {
			return ChannelRoute{}, false, types.NewAppError(types.CodeConflict,
				"Telegram 群组连接重放范围不一致", types.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
				"提交 Telegram 群组连接重放", err)
		}
		return route, false, nil
	}
	if !expiresAt.After(time.Now()) {
		return ChannelRoute{}, false, types.NewAppError(types.CodeNotFound,
			"Telegram 群组连接码无效或已过期", types.ErrNotFound)
	}
	identity, err := scanChannelIdentity(tx.QueryRow(ctx,
		`SELECT ci.id,ci.tenant_id,ci.user_id,ci.provider,ci.app_identity,
		        ci.external_user_id,ci.provider_chat_id,ci.status,
		        ci.bound_at,ci.revoked_at
		   FROM channel_identities ci
		   JOIN memberships m ON m.tenant_id=ci.tenant_id AND m.user_id=ci.user_id
		   JOIN tenants t ON t.id=ci.tenant_id
		  WHERE ci.tenant_id=$1 AND ci.user_id=$2 AND ci.provider=$3 AND
		        ci.app_identity=$4 AND ci.external_user_id=$5 AND
		        ci.status='active' AND t.status='active' FOR UPDATE OF ci`,
		tenantID, userID, provider, appIdentity, actorID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelRoute{}, false, types.NewAppError(types.CodeConflict,
			"只有已绑定的 Vane owner 可以连接群组", types.ErrConflict)
	}
	if err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"验证 Telegram 群组 owner", err)
	}
	routeKind := telegramRouteKind(chatType, threadID)
	route, err := scanChannelRoute(tx.QueryRow(ctx,
		`INSERT INTO channel_routes
		 (tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
		  provider_thread_id,chat_type,route_kind)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (provider,app_identity,provider_chat_id,provider_thread_id)
		 WHERE status='active' DO UPDATE SET status=channel_routes.status
		 RETURNING `+channelRouteColumns,
		tenantID, userID, identity.ID, provider, appIdentity, chatID,
		threadID, chatType, routeKind))
	if err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"建立 Telegram 群组路由", err)
	}
	if route.TenantID != tenantID || route.UserID != userID ||
		route.IdentityID != identity.ID || route.RouteKind != routeKind {
		return ChannelRoute{}, false, types.NewAppError(types.CodeConflict,
			"Telegram 群组或话题已连接其他 Vane 账号", types.ErrConflict)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel_route_link_requests
		    SET consumed_at=clock_timestamp(),consumed_route_id=$2,request_digest=$3
		  WHERE token_hash=$1 AND consumed_at IS NULL`,
		tokenHash, route.ID, requestDigest); err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"消费 Telegram 群组连接请求", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ChannelRoute{}, false, types.NewAppError(types.CodeDatabase,
			"提交 Telegram 群组连接事务", err)
	}
	return route, true, nil
}

func (s *Store) ResolveTelegramRoute(
	ctx context.Context, appIdentity, actorID, chatID, threadID string,
) (ChannelIdentity, ChannelRoute, error) {
	if validateTelegramIdentityParts(appIdentity, actorID, chatID) != nil ||
		strings.TrimSpace(threadID) != threadID || threadID == "" {
		return ChannelIdentity{}, ChannelRoute{}, types.NewAppError(
			types.CodeValidation, "Telegram 路由范围无效", types.ErrValidation)
	}
	row := s.pool.QueryRow(ctx,
		`SELECT ci.id,ci.tenant_id,ci.user_id,ci.provider,ci.app_identity,
		        ci.external_user_id,ci.provider_chat_id,ci.status,
		        ci.bound_at,ci.revoked_at,
		        cr.id,cr.tenant_id,cr.user_id,cr.identity_id,cr.provider,
		        cr.app_identity,cr.provider_chat_id,cr.provider_thread_id,
		        cr.chat_type,cr.route_kind,cr.status,cr.bound_at,cr.revoked_at
		   FROM channel_routes cr
		   JOIN channel_identities ci ON ci.id=cr.identity_id
		   JOIN memberships m ON m.tenant_id=cr.tenant_id AND m.user_id=cr.user_id
		   JOIN tenants t ON t.id=cr.tenant_id
		  WHERE cr.provider=$1 AND cr.app_identity=$2 AND
		        cr.provider_chat_id=$3 AND cr.provider_thread_id=$4 AND
		        cr.status='active' AND ci.status='active' AND
		        ci.external_user_id=$5 AND t.status='active'`,
		channelProviderTelegram, appIdentity, chatID, threadID, actorID)
	var identity ChannelIdentity
	var route ChannelRoute
	err := row.Scan(&identity.ID, &identity.TenantID, &identity.UserID,
		&identity.Provider, &identity.AppIdentity, &identity.ExternalUserID,
		&identity.ProviderChatID, &identity.Status, &identity.BoundAt,
		&identity.RevokedAt, &route.ID, &route.TenantID, &route.UserID,
		&route.IdentityID, &route.Provider, &route.AppIdentity,
		&route.ProviderChatID, &route.ProviderThreadID, &route.ChatType,
		&route.RouteKind, &route.Status, &route.BoundAt, &route.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelIdentity{}, ChannelRoute{}, types.NewAppError(
			types.CodeNotFound, "Telegram 路由尚未授权", types.ErrNotFound)
	}
	if err != nil {
		return ChannelIdentity{}, ChannelRoute{}, types.NewAppError(
			types.CodeDatabase, "解析 Telegram 路由", err)
	}
	return identity, route, nil
}

func (s *Store) ListTelegramRoutesForUser(
	ctx context.Context, tenantID, userID int64, appIdentity string,
) ([]ChannelRoute, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+channelRouteColumns+` FROM channel_routes
		  WHERE tenant_id=$1 AND user_id=$2 AND provider=$3 AND
		        app_identity=$4 AND status='active'
		  ORDER BY CASE route_kind WHEN 'private' THEN 0 WHEN 'group' THEN 1
		           WHEN 'topic' THEN 2 ELSE 3 END,bound_at,id`,
		tenantID, userID, channelProviderTelegram, appIdentity)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"读取 Telegram 路由", err)
	}
	defer rows.Close()
	var routes []ChannelRoute
	for rows.Next() {
		route, scanErr := scanChannelRoute(rows)
		if scanErr != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"扫描 Telegram 路由", scanErr)
		}
		routes = append(routes, route)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"遍历 Telegram 路由", err)
	}
	return routes, nil
}

func (s *Store) RevokeTelegramRoute(
	ctx context.Context, tenantID, userID, routeID int64, appIdentity string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"开启 Telegram 群组解绑事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var identityID int64
	err = tx.QueryRow(ctx,
		`SELECT identity_id FROM channel_routes
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND provider=$4 AND
		        app_identity=$5 AND route_kind<>'private' AND status='active'
		  FOR UPDATE`, routeID, tenantID, userID, channelProviderTelegram,
		appIdentity).Scan(&identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeNotFound,
			"Telegram 群组路由不存在", types.ErrNotFound)
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"锁定 Telegram 群组路由", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel_ingress_receipts
		    SET status='failed',reply_text=NULL,error_code='route_revoked',
		        processing_lease=NULL,lease_expires_at=NULL,
		        updated_at=clock_timestamp()
		  WHERE route_id=$1 AND status IN ('pending','processing','reply_ready')`,
		routeID); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"取消 Telegram 路由未执行消息", err)
	}
	command, err := tx.Exec(ctx,
		`UPDATE channel_routes
		    SET status='revoked',revoked_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND provider=$4 AND
		        app_identity=$5 AND route_kind<>'private' AND status='active'`,
		routeID, tenantID, userID, channelProviderTelegram, appIdentity)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"解除 Telegram 群组路由", err)
	}
	if command.RowsAffected() != 1 {
		return types.NewAppError(types.CodeNotFound,
			"Telegram 群组路由不存在", types.ErrNotFound)
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase,
			"提交 Telegram 群组解绑", err)
	}
	_ = identityID
	return nil
}
