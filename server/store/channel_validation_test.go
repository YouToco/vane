package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func requireValidationCode(t *testing.T, err error) {
	t.Helper()
	if types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("err=%v code=%s", err, types.CodeOf(err))
	}
}

func TestChannelStoreValidationFailsBeforeDatabaseAccess(t *testing.T) {
	s := &Store{}
	ctx := t.Context()
	future := time.Now().Add(time.Minute)
	requireValidationCode(t, s.IssueTelegramLinkRequest(ctx, 0, 1, "bot", make([]byte, 32), future))
	_, _, err := s.ConsumeTelegramLinkRequest(ctx, nil, "bot", "actor", "chat", strings.Repeat("a", 64))
	requireValidationCode(t, err)
	_, _, err = s.ConsumeTelegramRouteLinkRequest(ctx, nil, "bot", "actor", "-1", "0", "supergroup", strings.Repeat("a", 64))
	requireValidationCode(t, err)
	requireValidationCode(t, s.IssueTelegramRouteLinkRequest(ctx, 1, 1, " bot", make([]byte, 32), future))
	_, _, err = s.ResolveTelegramRoute(ctx, "bot", "actor", "chat", "")
	requireValidationCode(t, err)

	identity := ChannelIdentity{ID: 1, TenantID: 1, UserID: 1, Provider: channelProviderTelegram, AppIdentity: "bot", ExternalUserID: "actor", Status: "active"}
	route := ChannelRoute{ID: 1, IdentityID: 1, TenantID: 1, UserID: 1, Provider: channelProviderTelegram, AppIdentity: "bot", ProviderChatID: "-1", ProviderThreadID: "0", Status: "active"}
	_, err = s.AcceptTelegramRoutedIngress(ctx, identity, route, "bad update", strings.Repeat("a", 64), "hello", uuid.NewString(), "1", "message", "", nil)
	requireValidationCode(t, err)
	_, err = s.AcceptTelegramRoutedIngress(ctx, identity, route, "1", strings.Repeat("a", 64), "telegram:media-help", uuid.NewString(), "1", "message", "", []byte(`{"schema":"bad"}`))
	requireValidationCode(t, err)
	_, err = s.AcceptTelegramRoutedIngress(ctx, identity, route, "1", strings.Repeat("a", 64), "hello", "not-a-uuid", "1", "message", "", nil)
	requireValidationCode(t, err)
	_, err = s.AcceptTelegramRoutedIngress(ctx, identity, route, "01", strings.Repeat("a", 64), "hello", uuid.NewString(), "1", "message", "", nil)
	requireValidationCode(t, err)
	_, err = s.ClaimNextTelegramIngress(ctx, "", time.Minute)
	requireValidationCode(t, err)
	requireValidationCode(t, s.MarkTelegramIngressReplyReady(ctx, ChannelIngress{}, " "))

	_, err = s.PrepareTelegramOutbound(ctx, 0, 1, 1, uuid.NewString(), "notice", "hello")
	requireValidationCode(t, err)
	_, err = s.ClaimNextTelegramOutbound(ctx, " bot")
	requireValidationCode(t, err)
	requireValidationCode(t, s.CompleteTelegramOutbound(ctx, ChannelOutboundEffect{}, nil))
	requireValidationCode(t, s.CompleteTelegramOutbound(ctx, ChannelOutboundEffect{}, []string{" bad"}))
	_, err = s.DeferTelegramOutbound(ctx, ChannelOutboundEffect{}, time.Second, 1)
	requireValidationCode(t, err)
	s.channelSendRetry = true
	_, err = s.DeferTelegramOutbound(ctx, ChannelOutboundEffect{}, time.Millisecond, 1)
	requireValidationCode(t, err)

	_, err = s.PrepareAggregateTelegramOutbound(ctx, "bad", 1, 1, "task", 1, 0, 1, []int64{1}, 1, uuid.NewString(), "text")
	requireValidationCode(t, err)
	_, err = s.PrepareAggregateTelegramOutbound(ctx, uuid.NewString(), 1, 1, "task", 1, 0, 1, []int64{0}, 1, uuid.NewString(), "text")
	requireValidationCode(t, err)
	requireValidationCode(t, s.SettleAggregateTelegramOutbound(ctx, 0, 1, uuid.NewString(), uuid.NewString()))

	preference := DeliveryChannelPreference{Selection: DeliveryChannelFeishu}
	_, err = s.PrepareArtifactDeliveryPlan(ctx, 0, 1, "task", ArtifactDeliveryPeriodicReport, "key", preference)
	requireValidationCode(t, err)
	_, err = s.LoadArtifactDeliveryPlan(ctx, 1, 1, "task", "bad", "key")
	requireValidationCode(t, err)

	s.channelSendRetry = false
	requireValidationCode(t, s.MigrateTelegramRoutes(ctx, "bot", "-1", "-2"))
	requireValidationCode(t, s.InvalidateTelegramDestination(ctx, "bot", "-1", "", "bot_membership_lost"))
	s.channelSendRetry = true
	requireValidationCode(t, s.MigrateTelegramRoutes(ctx, "bot", "-1", "-1"))
	requireValidationCode(t, s.MigrateTelegramRoutes(ctx, "bot", "01", "-2"))
	requireValidationCode(t, s.InvalidateTelegramDestination(ctx, "bot", "-1", "bad", "topic_closed"))
}

func TestCredentialVaultPureValidationAndMetadata(t *testing.T) {
	var nilStore *Store
	if err := nilStore.ConfigureCredentialVault("id", strings.Repeat("11", 32), ""); err == nil {
		t.Fatal("nil store accepted vault configuration")
	}
	s := &Store{}
	if s.CredentialVaultReady() {
		t.Fatal("empty store reported vault ready")
	}
	if err := s.ConfigureCredentialVault("id", "bad", ""); err == nil {
		t.Fatal("invalid key accepted")
	}
	if err := s.ConfigureCredentialVault("id", strings.Repeat("11", 32), ""); err != nil {
		t.Fatal(err)
	}
	if !s.CredentialVaultReady() {
		t.Fatal("configured store reported vault unavailable")
	}

	validScope := CredentialScope{Kind: "user", TenantID: 1, UserID: 2, Provider: "telegram", Purpose: "bot_api"}
	requireValidationCode(t, validateCredentialInput(CredentialScope{}, json.RawMessage(`{}`), json.RawMessage(`{}`), 1))
	requireValidationCode(t, validateCredentialInput(validScope, json.RawMessage(`[]`), json.RawMessage(`{}`), 1))
	if err := validateCredentialInput(validScope, json.RawMessage(`{"bot_token":"x"}`), json.RawMessage(`{"bot_id":"1"}`), 2); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []CredentialScope{
		{Kind: "platform", TenantID: 1, Provider: "llm", Purpose: "shared_runtime"},
		{Kind: "tenant", Provider: "feishu", Purpose: "app_credentials"},
		{Kind: "user", TenantID: 1, Provider: "telegram", Purpose: "bot_api"},
		{Kind: "platform", Provider: "Bad", Purpose: "shared_runtime"},
	} {
		requireValidationCode(t, validateCredentialScope(scope))
	}
	if nullableCredentialTenant(CredentialScope{Kind: "platform"}) != nil ||
		nullableCredentialUser(CredentialScope{Kind: "tenant"}) != nil {
		t.Fatal("nullable credential scope leaked identifiers")
	}
	if nullableCredentialTenant(validScope) != int64(1) ||
		nullableCredentialUser(validScope) != int64(2) || credentialLockKey(validScope) == "" {
		t.Fatal("valid credential scope projection failed")
	}
	requireValidationCode(t, credentialValidationError("invalid"))

	tests := []struct {
		scope    CredentialScope
		metadata json.RawMessage
		want     string
		wantErr  bool
	}{
		{scope: validScope, metadata: json.RawMessage(`{"bot_id":"123"}`), want: "123"},
		{scope: validScope, metadata: json.RawMessage(`{"bot_id":"bad"}`), wantErr: true},
		{scope: CredentialScope{Kind: "user", TenantID: 1, UserID: 2, Provider: "feishu", Purpose: "app_credentials"}, metadata: json.RawMessage(`{"app_id":"cli_test"}`), want: "cli_test"},
		{scope: CredentialScope{Kind: "user", TenantID: 1, UserID: 2, Provider: "feishu", Purpose: "app_credentials"}, metadata: json.RawMessage(`[]`), wantErr: true},
		{scope: CredentialScope{Kind: "platform", Provider: "llm", Purpose: "shared_runtime"}, metadata: json.RawMessage(`{}`)},
	}
	for _, test := range tests {
		got, err := credentialExternalIdentity(test.scope, test.metadata)
		gotValue := ""
		if got != nil {
			gotValue = *got
		}
		if (err != nil) != test.wantErr || gotValue != test.want {
			t.Fatalf("provider=%s got=%q err=%v", test.scope.Provider, gotValue, err)
		}
	}
}

func TestChannelPureHelpersCoverCanonicalAndInvalidForms(t *testing.T) {
	if !validTelegramRouteParts("-100", "0", "group") ||
		!validTelegramRouteParts("-100", "42", "supergroup") ||
		validTelegramRouteParts("01", "0", "group") ||
		validTelegramRouteParts("-100", "-1", "supergroup") ||
		validTelegramRouteParts("-100", "0", "private") {
		t.Fatal("Telegram route canonicalization mismatch")
	}
	if telegramRouteKind("supergroup", "42") != "topic" ||
		telegramRouteKind("supergroup", "0") != "group" {
		t.Fatal("Telegram route kind mismatch")
	}
	if validOutboundKind("") || validOutboundKind("Bad") ||
		!validOutboundKind("periodic_report") || validOutboundKind(strings.Repeat("a", 65)) {
		t.Fatal("outbound kind validation mismatch")
	}
	left := []byte(`{"schema":"vane.channel-message/v1","caption":"look","items":[{"kind":"image","provider_file_id":"file"}]}`)
	right := []byte(`{"items":[{"provider_file_id":"file","kind":"image"}],"caption":"look","schema":"vane.channel-message/v1"}`)
	if !channelMediaEnvelopeEqual(nil, nil) ||
		channelMediaEnvelopeEqual([]byte(`{"a":1}`), []byte(`bad`)) ||
		!channelMediaEnvelopeEqual(left, right) {
		t.Fatal("media envelope semantic equality mismatch")
	}
	if !validArtifactDeliveryKind(ArtifactDeliveryAggregateBrief) ||
		validArtifactDeliveryKind("other") {
		t.Fatal("artifact kind validation mismatch")
	}
}
