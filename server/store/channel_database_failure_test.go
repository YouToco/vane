package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// This test exercises every public channel/vault operation after the database
// authority disappears. Besides improving coverage, it proves these paths
// return classified errors instead of panicking or silently succeeding during
// a pool shutdown.
func TestChannelAndCredentialStoresFailClosedAfterPoolShutdown(t *testing.T) {
	dbURL := freshMigrationDatabase(t, "channel_store_closed_pool")
	if err := migrate(t.Context(), dbURL, 0); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ConfigureCredentialVault("closed-pool-key", strings.Repeat("42", 32), ""); err != nil {
		t.Fatal(err)
	}
	st.channelSendRetry = true
	st.Close()

	ctx := t.Context()
	requireError := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s unexpectedly succeeded after pool shutdown", name)
		}
	}
	_, err = st.TelegramBlockedReplyStats(ctx, "100")
	requireError("TelegramBlockedReplyStats", err)
	_, err = st.TelegramBlockedReplyStatsForUser(ctx, "100", 1, 2)
	requireError("TelegramBlockedReplyStatsForUser", err)
	requireError("IssueTelegramLinkRequest", st.IssueTelegramLinkRequest(
		ctx, 1, 2, "100", make([]byte, 32), time.Now().Add(time.Hour)))
	_, _, err = st.ConsumeTelegramLinkRequest(ctx, make([]byte, 32),
		"100", "200", "200", strings.Repeat("a", 64))
	requireError("ConsumeTelegramLinkRequest", err)
	_, err = st.ResolveTelegramIdentity(ctx, "100", "200", "200")
	requireError("ResolveTelegramIdentity", err)
	_, err = st.GetTelegramIdentityForUser(ctx, 1, 2, "100")
	requireError("GetTelegramIdentityForUser", err)
	requireError("RevokeTelegramIdentity", st.RevokeTelegramIdentity(ctx, 1, 2, "100"))

	identity := ChannelIdentity{ID: 3, TenantID: 1, UserID: 2,
		Provider: channelProviderTelegram, AppIdentity: "100",
		ExternalUserID: "200", ProviderChatID: "200", Status: "active"}
	_, err = st.AcceptTelegramIngress(ctx, identity, "1", strings.Repeat("b", 64),
		"hello", uuid.NewString())
	requireError("AcceptTelegramIngress", err)
	_, err = st.ClaimNextTelegramIngress(ctx, "100", 30*time.Second)
	requireError("ClaimNextTelegramIngress", err)
	ingress := ChannelIngress{Provider: channelProviderTelegram,
		AppIdentity: "100", ProviderUpdateID: "1", ProcessingLease: uuid.NewString(),
		ProviderChatID: "200", RouteID: 3, TenantID: 1, UserID: 2,
		IngressKind: "message"}
	requireError("MarkTelegramIngressReplyReady",
		st.MarkTelegramIngressReplyReady(ctx, ingress, "reply"))
	requireError("MarkTelegramIngressFailed",
		st.MarkTelegramIngressFailed(ctx, ingress, "internal"))
	_, err = st.ClaimTelegramReplySend(ctx, channelProviderTelegram, "100", "1")
	requireError("ClaimTelegramReplySend", err)
	_, err = st.ClaimNextTelegramReplySend(ctx, "100")
	requireError("ClaimNextTelegramReplySend", err)
	requireError("CompleteTelegramReply", st.CompleteTelegramReply(ctx, ingress, []string{"9"}))
	_, err = st.DeferTelegramReply(ctx, ingress, time.Second, 2)
	requireError("DeferTelegramReply", err)
	requireError("MarkTelegramReplyRejected",
		st.MarkTelegramReplyRejected(ctx, ingress, "rejected"))
	requireError("MarkTelegramReplyAmbiguous",
		st.MarkTelegramReplyAmbiguous(ctx, ingress, []string{"9"}, "ambiguous"))

	requireError("IssueTelegramRouteLinkRequest", st.IssueTelegramRouteLinkRequest(
		ctx, 1, 2, "100", make([]byte, 32), time.Now().Add(time.Hour)))
	_, _, err = st.ConsumeTelegramRouteLinkRequest(ctx, make([]byte, 32),
		"100", "200", "-1001", "0", "supergroup", strings.Repeat("c", 64))
	requireError("ConsumeTelegramRouteLinkRequest", err)
	_, _, err = st.ResolveTelegramRoute(ctx, "100", "200", "-1001", "0")
	requireError("ResolveTelegramRoute", err)
	_, err = st.ListTelegramRoutesForUser(ctx, 1, 2, "100")
	requireError("ListTelegramRoutesForUser", err)
	requireError("RevokeTelegramRoute", st.RevokeTelegramRoute(ctx, 1, 2, 3, "100"))
	requireError("MigrateTelegramRoutes", st.MigrateTelegramRoutes(ctx, "100", "-1001", "-1002"))
	requireError("InvalidateTelegramDestination",
		st.InvalidateTelegramDestination(ctx, "100", "-1001", "0", "bot_membership_lost"))

	effectID := uuid.NewString()
	_, err = st.PrepareTelegramOutbound(ctx, 1, 2, 3, effectID, "brief", "body")
	requireError("PrepareTelegramOutbound", err)
	_, err = st.ClaimTelegramOutbound(ctx, effectID)
	requireError("ClaimTelegramOutbound", err)
	_, err = st.ClaimNextTelegramOutbound(ctx, "100")
	requireError("ClaimNextTelegramOutbound", err)
	effect := ChannelOutboundEffect{EffectID: effectID, TenantID: 1, UserID: 2,
		RouteID: 3, Provider: channelProviderTelegram, AppIdentity: "100",
		ProviderChatID: "200", EffectKind: "brief", PayloadText: "body"}
	requireError("CompleteTelegramOutbound", st.CompleteTelegramOutbound(ctx, effect, []string{"9"}))
	requireError("MarkTelegramOutboundRejected",
		st.MarkTelegramOutboundRejected(ctx, effect, "rejected"))
	_, err = st.DeferTelegramOutbound(ctx, effect, time.Second, 2)
	requireError("DeferTelegramOutbound", err)
	requireError("MarkTelegramOutboundAmbiguous",
		st.MarkTelegramOutboundAmbiguous(ctx, effect, []string{"9"}, "ambiguous"))

	preference := DeliveryChannelPreference{Selection: DeliveryChannelFeishu}
	planID := uuid.NewString()
	_, err = st.PrepareArtifactDeliveryPlan(ctx, 1, 2, "task", ArtifactDeliveryAggregateBrief,
		"artifact", preference)
	requireError("PrepareArtifactDeliveryPlan", err)
	_, err = st.LoadArtifactDeliveryPlan(ctx, 1, 2, "task",
		ArtifactDeliveryAggregateBrief, "artifact")
	requireError("LoadArtifactDeliveryPlan", err)
	_, err = st.ResolveDeliveryChannelPreference(ctx, 1, 2, "task")
	requireError("ResolveDeliveryChannelPreference", err)
	_, err = st.PutAccountDeliveryChannelPreference(ctx, 1, 2, DeliveryChannelFeishu, nil)
	requireError("PutAccountDeliveryChannelPreference", err)
	_, err = st.PutTaskDeliveryChannelPreference(ctx, 1, 2, "task", DeliveryChannelFeishu, nil)
	requireError("PutTaskDeliveryChannelPreference", err)
	_, err = st.DeleteTaskDeliveryChannelPreference(ctx, 1, 2, "task")
	requireError("DeleteTaskDeliveryChannelPreference", err)
	_, err = st.PrepareAggregateTelegramOutbound(ctx, planID, 1, 2, "task", 4,
		0, 1, []int64{5}, 3, effectID, "body")
	requireError("PrepareAggregateTelegramOutbound", err)
	requireError("SettleAggregateTelegramOutbound",
		st.SettleAggregateTelegramOutbound(ctx, 1, 2, planID, effectID))

	userScope := CredentialScope{Kind: "user", TenantID: 1, UserID: 2,
		Provider: "telegram", Purpose: "bot_api"}
	_, err = st.RotateCredential(ctx, userScope, json.RawMessage(`{"bot_token":"x"}`),
		json.RawMessage(`{"bot_id":100}`), 2)
	requireError("RotateCredential", err)
	_, err = st.CredentialStatus(ctx, userScope, 2)
	requireError("CredentialStatus", err)
	_, err = st.ActiveCredentialMetadata(ctx, userScope)
	requireError("ActiveCredentialMetadata", err)
	_, err = st.ListActiveUserCredentialMetadata(ctx, "telegram", "bot_api")
	requireError("ListActiveUserCredentialMetadata", err)
	_, err = st.LatestCredentialMetadata(ctx, userScope)
	requireError("LatestCredentialMetadata", err)
	requireError("UseCredential", st.UseCredential(ctx, userScope, 1,
		func([]byte, CredentialMetadata) error { return nil }))
	requireError("RevokeCredential", st.RevokeCredential(ctx, userScope, 2))
	_, err = st.ListPurgeableTenants(ctx)
	requireError("ListPurgeableTenants", err)
	_, err = st.PurgeTenant(ctx, 1, true)
	requireError("PurgeTenant", err)
}
