package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestMigration135And136Boundaries(t *testing.T) {
	tests := []struct {
		path      string
		required  []string
		forbidden []string
	}{
		{
			path: "migrations/135_agent_conversation_scopes.sql",
			required: []string{"conversation_scope", "idx_agent_sessions_user_scope_status",
				"routed Agent session history exists"},
			forbidden: []string{"provider_chat_id", "external_user_id"},
		},
		{
			path: "migrations/136_channel_rate_limit_retry.sql",
			required: []string{"send_retry_count", "next_send_at",
				"channel rate-limit history exists"},
			forbidden: []string{"sending' OR", "ambiguous' OR"},
		},
	}
	for _, test := range tests {
		payload, err := migrationsFS.ReadFile(test.path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(payload)
		for _, fragment := range test.required {
			if !strings.Contains(text, fragment) {
				t.Errorf("%s lost boundary %q", test.path, fragment)
			}
		}
		for _, fragment := range test.forbidden {
			if strings.Contains(text, fragment) {
				t.Errorf("%s contains forbidden boundary %q", test.path, fragment)
			}
		}
	}
}

func TestScopedAgentSessionsPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 136); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	userID, _ := migration129Identity(t, database, "telegram-session-scope")

	owner, err := st.CreateAgentSession(t.Context(), userID)
	if err != nil || owner.ConversationScope != "owner" {
		t.Fatalf("owner=%+v err=%v", owner, err)
	}
	private, err := st.CreateAgentSessionInScope(
		t.Context(), userID, "channel-route:11")
	if err != nil {
		t.Fatal(err)
	}
	group, err := st.CreateAgentSessionInScope(
		t.Context(), userID, "channel-route:22")
	if err != nil {
		t.Fatal(err)
	}
	for scope, wantID := range map[string]int64{
		"channel-route:11": private.ID,
		"channel-route:22": group.ID,
	} {
		got, getErr := st.GetActiveAgentSessionInScope(
			t.Context(), userID, scope, time.Now().Add(-time.Hour))
		if getErr != nil || got.ID != wantID || got.ConversationScope != scope {
			t.Fatalf("scope=%s got=%+v err=%v", scope, got, getErr)
		}
	}
	gotOwner, err := st.GetActiveAgentSession(
		t.Context(), userID, time.Now().Add(-time.Hour))
	if err != nil || gotOwner.ID != owner.ID || gotOwner.ConversationScope != "owner" {
		t.Fatalf("owner lookup=%+v err=%v", gotOwner, err)
	}
	if _, err := st.CreateAgentSessionInScope(
		t.Context(), userID, "telegram chat 42"); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("unsafe scope err=%v", err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE agent_sessions SET updated_at=clock_timestamp()-interval '2 hours'
		  WHERE id=$1`, private.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetActiveAgentSessionInScope(
		t.Context(), userID, "channel-route:11",
		time.Now().Add(-time.Hour)); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("expired scoped session err=%v", err)
	}
	if gotOwner, err = st.GetActiveAgentSession(
		t.Context(), userID, time.Now().Add(-time.Hour)); err != nil ||
		gotOwner.ID != owner.ID {
		t.Fatalf("scope expiry changed owner=%+v err=%v", gotOwner, err)
	}
	if _, err := provider.DownTo(t.Context(), 134); err == nil ||
		!strings.Contains(err.Error(), "routed Agent session history exists") {
		t.Fatalf("scoped history downgrade err=%v", err)
	}
}

func telegramLifecycleFixture(
	t *testing.T, databaseURL string,
) (*Store, string, int64, int64, ChannelIdentity, ChannelRoute, func()) {
	t.Helper()
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	if _, err := provider.UpTo(t.Context(), 136); err != nil {
		drop()
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		drop()
		t.Fatal(err)
	}
	userID, tenantID := migration129Identity(t, database, "telegram-lifecycle")
	identityToken := sha256.Sum256(bytes.Repeat([]byte{0x61}, 32))
	if err := st.IssueTelegramLinkRequest(t.Context(), tenantID, userID,
		"12345", identityToken[:], time.Now().Add(10*time.Minute)); err != nil {
		st.Close()
		drop()
		t.Fatal(err)
	}
	identity, _, err := st.ConsumeTelegramLinkRequest(t.Context(),
		identityToken[:], "12345", "777", "777", strings.Repeat("a", 64))
	if err != nil {
		st.Close()
		drop()
		t.Fatal(err)
	}
	routeToken := sha256.Sum256(bytes.Repeat([]byte{0x62}, 32))
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), tenantID, userID,
		"12345", routeToken[:], time.Now().Add(10*time.Minute)); err != nil {
		st.Close()
		drop()
		t.Fatal(err)
	}
	route, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		routeToken[:], "12345", "777", "-99", "0", "group",
		strings.Repeat("b", 64))
	if err != nil {
		st.Close()
		drop()
		t.Fatal(err)
	}
	cleanup := func() { st.Close(); drop() }
	return st, scratchURL, tenantID, userID, identity, route, cleanup
}

func TestTelegramRateLimitAndLifecyclePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	st, scratchURL, tenantID, userID, identity, route, cleanup :=
		telegramLifecycleFixture(t, databaseURL)
	t.Cleanup(cleanup)
	// Open a separate Store to prove retry state survives process-local memory.
	restarted, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, restarted)

	turnID := uuid.NewString()
	created, err := st.AcceptTelegramRoutedIngress(t.Context(), identity, route,
		"9200", strings.Repeat("c", 64), "列出任务", turnID, "10", "message", "")
	if err != nil || !created {
		t.Fatalf("accept=%t err=%v", created, err)
	}
	claimed, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTelegramIngressReplyReady(t.Context(), claimed, "任务列表"); err != nil {
		t.Fatal(err)
	}
	sending, err := st.ClaimTelegramReplySend(
		t.Context(), "telegram", "12345", "9200")
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := st.DeferTelegramReply(
		t.Context(), sending, 2*time.Second, 2)
	if err != nil || !scheduled {
		t.Fatalf("scheduled=%t err=%v", scheduled, err)
	}
	if _, err := restarted.ClaimNextTelegramReplySend(
		t.Context(), "12345"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("rate limit was claimable early err=%v", err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE channel_ingress_receipts SET next_send_at=clock_timestamp()-interval '1 second'
		  WHERE provider='telegram' AND app_identity='12345' AND provider_update_id='9200'`); err != nil {
		t.Fatal(err)
	}
	sending, err = restarted.ClaimNextTelegramReplySend(t.Context(), "12345")
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err = restarted.DeferTelegramReply(
		t.Context(), sending, time.Second, 1)
	if err != nil || scheduled {
		t.Fatalf("exhausted scheduled=%t err=%v", scheduled, err)
	}
	stats, err := st.TelegramBlockedReplyStatsForUser(
		t.Context(), "12345", tenantID, userID)
	if err != nil || stats.Count != 1 {
		t.Fatalf("rate-limit blocked=%+v err=%v", stats, err)
	}

	effectID := uuid.NewString()
	effect, err := st.PrepareTelegramOutbound(t.Context(), tenantID, userID,
		route.ID, effectID, "test", "发送测试")
	if err != nil {
		t.Fatal(err)
	}
	effect, err = st.ClaimTelegramOutbound(t.Context(), effectID)
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err = st.DeferTelegramOutbound(
		t.Context(), effect, 2*time.Second, 2)
	if err != nil || !scheduled {
		t.Fatalf("outbound scheduled=%t err=%v", scheduled, err)
	}
	if _, err := restarted.ClaimTelegramOutbound(
		t.Context(), effectID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("outbound rate limit was claimable early err=%v", err)
	}

	// Migration preserves the internal route/session scope and retargets only
	// effects that have not crossed the provider boundary.
	pendingEffectID := uuid.NewString()
	if _, err := st.PrepareTelegramOutbound(t.Context(), tenantID, userID,
		route.ID, pendingEffectID, "brief", "待迁移简报"); err != nil {
		t.Fatal(err)
	}
	if err := st.MigrateTelegramRoutes(
		t.Context(), "12345", "-99", "-10099"); err != nil {
		t.Fatal(err)
	}
	resolvedIdentity, migrated, err := st.ResolveTelegramRoute(
		t.Context(), "12345", "777", "-10099", "0")
	if err != nil || resolvedIdentity.ID != identity.ID || migrated.ID != route.ID ||
		migrated.ChatType != "supergroup" {
		t.Fatalf("identity=%+v route=%+v err=%v", resolvedIdentity, migrated, err)
	}
	if _, _, err := st.ResolveTelegramRoute(t.Context(), "12345", "777",
		"-99", "0"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("old group route remains active err=%v", err)
	}
	topicToken := sha256.Sum256(bytes.Repeat([]byte{0x63}, 32))
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), tenantID, userID,
		"12345", topicToken[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	topic, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		topicToken[:], "12345", "777", "-10099", "88", "supergroup",
		strings.Repeat("d", 64))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.InvalidateTelegramDestination(t.Context(), "12345",
		"-10099", "88", "topic_closed"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ResolveTelegramRoute(t.Context(), "12345", "777",
		"-10099", "88"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("closed topic route remains active err=%v", err)
	}
	if _, groupRoute, err := st.ResolveTelegramRoute(t.Context(), "12345", "777",
		"-10099", "0"); err != nil || groupRoute.ID != route.ID || topic.ID == route.ID {
		t.Fatalf("topic close changed group=%+v topic=%+v err=%v", groupRoute, topic, err)
	}
	if err := st.InvalidateTelegramDestination(t.Context(), "12345",
		"-10099", "", "bot_membership_lost"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ResolveTelegramRoute(t.Context(), "12345", "777",
		"-10099", "0"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("removed bot route remains active err=%v", err)
	}
	stats, err = st.TelegramBlockedReplyStatsForUser(
		t.Context(), "12345", tenantID, userID)
	if err != nil || stats.Count != 1 {
		t.Fatalf("lifecycle cancellation polluted blocked stats=%+v err=%v", stats, err)
	}
}
