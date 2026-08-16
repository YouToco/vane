package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func permissionDeniedChannelStore(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	if err := Migrate(t.Context(), databaseURL); err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	role := fmt.Sprintf("channel_denied_%d", time.Now().UnixNano())
	quotedRole := pgx.Identifier{role}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE ROLE "+quotedRole+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET ROLE "+quotedRole)
		return err
	}
	restricted, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := restricted.Ping(t.Context()); err != nil {
		restricted.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restricted.Close()
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+quotedRole)
		admin.Close()
	})
	st := &Store{pool: restricted, channelSendRetry: true,
		beginTx: restricted.BeginTx}
	if err := st.ConfigureCredentialVault("test", strings.Repeat("55", 32), ""); err != nil {
		t.Fatal(err)
	}
	return st
}

func requireStoreFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("permission-denied operation unexpectedly succeeded")
	}
}

func TestChannelStoresFailClosedAtInnerDatabaseBoundariesPostgres(t *testing.T) {
	st := permissionDeniedChannelStore(t)
	ctx := t.Context()
	future := time.Now().Add(time.Minute)
	tokenHash := make([]byte, 32)
	digest := strings.Repeat("a", 64)
	turnID := uuid.NewString()
	planID := uuid.NewString()
	effectID := uuid.NewString()

	requireStoreFailure(t, st.IssueTelegramLinkRequest(ctx, 1, 1, "123", tokenHash, future))
	_, _, err := st.ConsumeTelegramLinkRequest(ctx, tokenHash, "123", "456", "789", digest)
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.IssueTelegramRouteLinkRequest(ctx, 1, 1, "123", tokenHash, future))
	_, _, err = st.ConsumeTelegramRouteLinkRequest(ctx, tokenHash, "123", "456", "-100", "1", "supergroup", digest)
	requireStoreFailure(t, err)
	_, _, err = st.ResolveTelegramRoute(ctx, "123", "456", "-100", "1")
	requireStoreFailure(t, err)
	_, err = st.ListTelegramRoutesForUser(ctx, 1, 1, "123")
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.RevokeTelegramRoute(ctx, 1, 1, 1, "123"))
	requireStoreFailure(t, st.MigrateTelegramRoutes(ctx, "123", "-100", "-101"))
	requireStoreFailure(t, st.InvalidateTelegramDestination(ctx, "123", "-100", "", "bot_membership_lost"))

	_, err = st.TelegramBlockedReplyStats(ctx, "123")
	requireStoreFailure(t, err)
	_, err = st.TelegramBlockedReplyStatsForUser(ctx, "123", 1, 1)
	requireStoreFailure(t, err)
	_, err = st.ResolveTelegramIdentity(ctx, "123", "456", "789")
	requireStoreFailure(t, err)
	_, err = st.GetTelegramIdentityForUser(ctx, 1, 1, "123")
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.RevokeTelegramIdentity(ctx, 1, 1, "123"))

	identity := ChannelIdentity{ID: 1, TenantID: 1, UserID: 1, Provider: channelProviderTelegram,
		AppIdentity: "123", ExternalUserID: "456", ProviderChatID: "789", Status: "active"}
	route := ChannelRoute{ID: 1, TenantID: 1, UserID: 1, IdentityID: 1,
		Provider: channelProviderTelegram, AppIdentity: "123", ProviderChatID: "789",
		ProviderThreadID: "0", Status: "active"}
	_, err = st.AcceptTelegramRoutedIngress(ctx, identity, route, "1", digest, "hello", turnID, "2", "message", "", nil)
	requireStoreFailure(t, err)
	_, err = st.ClaimNextTelegramIngress(ctx, "123", time.Minute)
	requireStoreFailure(t, err)
	item := ChannelIngress{Provider: channelProviderTelegram, AppIdentity: "123", ProviderUpdateID: "1", ProcessingLease: "lease"}
	requireStoreFailure(t, st.MarkTelegramIngressReplyReady(ctx, item, "reply"))
	requireStoreFailure(t, st.MarkTelegramIngressFailed(ctx, item, "agent_failed"))
	_, err = st.ClaimTelegramReplySend(ctx, channelProviderTelegram, "123", "1")
	requireStoreFailure(t, err)
	_, err = st.ClaimNextTelegramReplySend(ctx, "123")
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.CompleteTelegramReply(ctx, item, []string{"3"}))
	_, err = st.DeferTelegramReply(ctx, item, time.Second, 3)
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.MarkTelegramReplyRejected(ctx, item, "forbidden"))
	requireStoreFailure(t, st.MarkTelegramReplyAmbiguous(ctx, item, nil, "timeout"))

	_, err = st.PrepareTelegramOutbound(ctx, 1, 1, 1, effectID, "periodic_report", "body")
	requireStoreFailure(t, err)
	_, err = st.ClaimTelegramOutbound(ctx, effectID)
	requireStoreFailure(t, err)
	_, err = st.ClaimNextTelegramOutbound(ctx, "123")
	requireStoreFailure(t, err)
	effect := ChannelOutboundEffect{EffectID: effectID, RouteID: 1, Provider: channelProviderTelegram,
		AppIdentity: "123", ProviderChatID: "789", ProviderThreadID: "0", TenantID: 1, UserID: 1}
	requireStoreFailure(t, st.CompleteTelegramOutbound(ctx, effect, []string{"3"}))
	_, err = st.DeferTelegramOutbound(ctx, effect, time.Second, 3)
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.MarkTelegramOutboundRejected(ctx, effect, "forbidden"))
	requireStoreFailure(t, st.MarkTelegramOutboundAmbiguous(ctx, effect, nil, "timeout"))

	_, err = st.PrepareArtifactDeliveryPlan(ctx, 1, 1, "task", ArtifactDeliveryPeriodicReport,
		"artifact", DeliveryChannelPreference{Selection: DeliveryChannelFeishu})
	requireStoreFailure(t, err)
	_, err = st.LoadArtifactDeliveryPlan(ctx, 1, 1, "task", ArtifactDeliveryPeriodicReport, "artifact")
	requireStoreFailure(t, err)
	_, err = st.PrepareAggregateTelegramOutbound(ctx, planID, 1, 1, "task", 1,
		0, 1, []int64{1}, 1, effectID, "body")
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.SettleAggregateTelegramOutbound(ctx, 1, 1, planID, effectID))

	scope := CredentialScope{Kind: "user", TenantID: 1, UserID: 1,
		Provider: "telegram", Purpose: "bot_api"}
	_, err = st.RotateCredential(ctx, scope, json.RawMessage(`{"bot_token":"1:x"}`),
		json.RawMessage(`{"bot_id":1}`), 1)
	requireStoreFailure(t, err)
	_, err = st.CredentialStatus(ctx, scope, 1)
	requireStoreFailure(t, err)
	_, err = st.ActiveCredentialMetadata(ctx, scope)
	requireStoreFailure(t, err)
	_, err = st.ListActiveUserCredentialMetadata(ctx, "telegram", "bot_api")
	requireStoreFailure(t, err)
	_, err = st.LatestCredentialMetadata(ctx, scope)
	requireStoreFailure(t, err)
	requireStoreFailure(t, st.UseCredential(ctx, scope, 0, func([]byte, CredentialMetadata) error { return nil }))
	requireStoreFailure(t, st.RevokeCredential(ctx, scope, 1))
}
