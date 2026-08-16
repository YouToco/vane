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

func TestMigration134ChannelRouteBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/134_channel_routes.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE channel_routes",
		"CREATE TABLE channel_route_link_requests",
		"CREATE TABLE channel_message_mappings",
		"CREATE TABLE channel_outbound_effects",
		"provider_thread_id",
		"uq_channel_route_destination_active",
		"route_kind='topic'",
		"FOREIGN KEY (tenant_id,user_id,identity_id)",
		"ALTER TABLE channel_routes ENABLE ROW LEVEL SECURITY",
		"refusing downgrade while routed channel history exists",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 134 lost boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"telegram_username", "feishu_message_id", "GRANT ALL",
		"DROP TABLE channel_identities",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 134 introduced unsafe coupling %q", forbidden)
		}
	}
}

func TestTelegramRouteAndOutboundValidationRejectsBeforeDatabase(t *testing.T) {
	for _, test := range []struct {
		chatID, threadID, chatType string
		want                       bool
	}{
		{chatID: "-1007", threadID: "0", chatType: "group", want: true},
		{chatID: "-1007", threadID: "88", chatType: "supergroup", want: true},
		{chatID: "0", threadID: "0", chatType: "group"},
		{chatID: "01", threadID: "0", chatType: "group"},
		{chatID: "-1007", threadID: "-1", chatType: "supergroup"},
		{chatID: "-1007", threadID: "01", chatType: "supergroup"},
		{chatID: "-1007", threadID: "0", chatType: "channel"},
	} {
		if got := validTelegramRouteParts(test.chatID, test.threadID, test.chatType); got != test.want {
			t.Fatalf("parts=(%q,%q,%q) got=%t want=%t",
				test.chatID, test.threadID, test.chatType, got, test.want)
		}
	}
	if telegramRouteKind("supergroup", "88") != "topic" ||
		telegramRouteKind("group", "0") != "group" {
		t.Fatal("route kind derivation changed")
	}
	for value, want := range map[string]bool{
		"brief": true, "periodic_report-v2": true, "": false,
		"Upper": false, "has space": false, strings.Repeat("a", 65): false,
	} {
		if got := validOutboundKind(value); got != want {
			t.Fatalf("outbound kind=%q got=%t want=%t", value, got, want)
		}
	}
	st := &Store{}
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), 0, 0, "",
		nil, time.Now()); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("issue validation err=%v", err)
	}
	if _, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(), nil,
		"", "", "", "", "", ""); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("consume validation err=%v", err)
	}
	if _, _, err := st.ResolveTelegramRoute(t.Context(), "", "", "", ""); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("resolve validation err=%v", err)
	}
	for _, test := range []struct{ effectID, kind, text string }{
		{effectID: "bad", kind: "test", text: "x"},
		{effectID: uuid.NewString(), kind: "Bad Kind", text: "x"},
		{effectID: uuid.NewString(), kind: "test", text: " "},
	} {
		if _, err := st.PrepareTelegramOutbound(t.Context(), 1, 1, 1,
			test.effectID, test.kind, test.text); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("prepare validation=%+v err=%v", test, err)
		}
	}
	if err := st.CompleteTelegramOutbound(t.Context(), ChannelOutboundEffect{}, nil); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("complete validation err=%v", err)
	}
}

func TestTelegramGroupTopicRoutePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 139); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	userID, tenantID := migration129Identity(t, database, "telegram-route-owner")

	identityToken := sha256.Sum256(bytes.Repeat([]byte{0x31}, 32))
	if err := st.IssueTelegramLinkRequest(t.Context(), tenantID, userID,
		"12345", identityToken[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	identity, _, err := st.ConsumeTelegramLinkRequest(t.Context(),
		identityToken[:], "12345", "777", "777", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	privateIdentity, privateRoute, err := st.ResolveTelegramRoute(
		t.Context(), "12345", "777", "777", "0")
	if err != nil || privateIdentity.ID != identity.ID || privateRoute.RouteKind != "private" {
		t.Fatalf("private identity=%+v route=%+v err=%v",
			privateIdentity, privateRoute, err)
	}

	routeToken := sha256.Sum256(bytes.Repeat([]byte{0x32}, 32))
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), tenantID, userID,
		"12345", routeToken[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	route, first, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		routeToken[:], "12345", "777", "-1007", "88", "supergroup",
		strings.Repeat("b", 64))
	if err != nil || !first || route.RouteKind != "topic" || route.IdentityID != identity.ID {
		t.Fatalf("route=%+v first=%t err=%v", route, first, err)
	}
	replay, first, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		routeToken[:], "12345", "777", "-1007", "88", "supergroup",
		strings.Repeat("b", 64))
	if err != nil || first || replay.ID != route.ID {
		t.Fatalf("replay=%+v first=%t err=%v", replay, first, err)
	}
	if _, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		routeToken[:], "12345", "777", "-1007", "88", "supergroup",
		strings.Repeat("d", 64)); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed route replay err=%v", err)
	}
	missingToken := sha256.Sum256(bytes.Repeat([]byte{0x7f}, 32))
	if _, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		missingToken[:], "12345", "777", "-1007", "88", "supergroup",
		strings.Repeat("e", 64)); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("missing route token err=%v", err)
	}
	wrongAppToken := sha256.Sum256(bytes.Repeat([]byte{0x41}, 32))
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), tenantID, userID,
		"12345", wrongAppToken[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		wrongAppToken[:], "99999", "777", "-1008", "0", "group",
		strings.Repeat("f", 64)); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("foreign app route token err=%v", err)
	}
	foreignActorToken := sha256.Sum256(bytes.Repeat([]byte{0x42}, 32))
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), tenantID, userID,
		"12345", foreignActorToken[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		foreignActorToken[:], "12345", "778", "-1008", "0", "group",
		strings.Repeat("1", 64)); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("foreign actor route token err=%v", err)
	}
	expiredToken := sha256.Sum256(bytes.Repeat([]byte{0x43}, 32))
	if err := st.IssueTelegramRouteLinkRequest(t.Context(), tenantID, userID,
		"12345", expiredToken[:], time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE channel_route_link_requests
		    SET created_at=clock_timestamp()-interval '2 hours',
		        expires_at=clock_timestamp()-interval '1 hour'
		  WHERE token_hash=$1`, expiredToken[:]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ConsumeTelegramRouteLinkRequest(t.Context(),
		expiredToken[:], "12345", "777", "-1008", "0", "group",
		strings.Repeat("2", 64)); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("expired route token err=%v", err)
	}
	for _, scope := range []struct{ actor, thread string }{
		{actor: "778", thread: "88"}, {actor: "777", thread: "89"},
	} {
		if _, _, err := st.ResolveTelegramRoute(t.Context(), "12345",
			scope.actor, "-1007", scope.thread); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("foreign scope=%+v err=%v", scope, err)
		}
	}
	resolvedIdentity, resolvedRoute, err := st.ResolveTelegramRoute(
		t.Context(), "12345", "777", "-1007", "88")
	if err != nil || resolvedIdentity.ID != identity.ID || resolvedRoute.ID != route.ID {
		t.Fatalf("resolved identity=%+v route=%+v err=%v",
			resolvedIdentity, resolvedRoute, err)
	}
	routes, err := st.ListTelegramRoutesForUser(t.Context(), tenantID, userID, "12345")
	if err != nil || len(routes) != 2 || routes[0].RouteKind != "private" ||
		routes[1].RouteKind != "topic" {
		t.Fatalf("routes=%+v err=%v", routes, err)
	}
	if err := st.RevokeTelegramRoute(t.Context(), tenantID, userID,
		privateRoute.ID, "12345"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("private route revoked independently err=%v", err)
	}
	turnID := uuid.NewString()
	created, err := st.AcceptTelegramRoutedIngress(t.Context(), identity, route,
		"9100", strings.Repeat("c", 64), "列出我的任务", turnID,
		"44", "command", "", nil)
	if err != nil || !created {
		t.Fatalf("accept created=%t err=%v", created, err)
	}
	claimed, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Minute)
	if err != nil || claimed.RouteID != route.ID ||
		claimed.ProviderThreadID != "88" || claimed.ProviderMessageID != "44" ||
		claimed.IngressKind != "command" {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if err := st.MarkTelegramIngressReplyReady(t.Context(), claimed, "任务列表"); err != nil {
		t.Fatal(err)
	}
	sending, err := st.ClaimTelegramReplySend(t.Context(), "telegram", "12345", "9100")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteTelegramReply(t.Context(), sending, []string{"501"}); err != nil {
		t.Fatal(err)
	}
	var mappedThread, direction, kind string
	if err := database.QueryRowContext(t.Context(),
		`SELECT provider_thread_id,direction,message_kind
		   FROM channel_message_mappings
		  WHERE provider='telegram' AND app_identity='12345' AND
		        provider_chat_id='-1007' AND provider_message_id='501'`,
	).Scan(&mappedThread, &direction, &kind); err != nil {
		t.Fatal(err)
	}
	if mappedThread != "88" || direction != "outbound" || kind != "command" {
		t.Fatalf("mapping thread=%s direction=%s kind=%s",
			mappedThread, direction, kind)
	}
	effectID := uuid.NewString()
	if _, err := st.PrepareTelegramOutbound(t.Context(), tenantID, userID,
		route.ID+9999, uuid.NewString(), "test", "missing route"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("missing outbound route err=%v", err)
	}
	effect, err := st.PrepareTelegramOutbound(t.Context(), tenantID, userID,
		route.ID, effectID, "periodic_report", "周期报告正文")
	if err != nil || effect.Status != "prepared" || effect.ProviderThreadID != "88" {
		t.Fatalf("prepared effect=%+v err=%v", effect, err)
	}
	replayEffect, err := st.PrepareTelegramOutbound(t.Context(), tenantID, userID,
		route.ID, effectID, "periodic_report", "周期报告正文")
	if err != nil || replayEffect.EffectID != effectID {
		t.Fatalf("effect replay=%+v err=%v", replayEffect, err)
	}
	if _, err := st.PrepareTelegramOutbound(t.Context(), tenantID, userID,
		route.ID, effectID, "periodic_report", "变化正文"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed effect replay err=%v", err)
	}
	claimedEffect, err := st.ClaimTelegramOutbound(t.Context(), effectID)
	if err != nil || claimedEffect.Status != "sending" {
		t.Fatalf("claimed effect=%+v err=%v", claimedEffect, err)
	}
	if err := st.CompleteTelegramOutbound(t.Context(), claimedEffect,
		[]string{" bad "}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid outbound receipt err=%v", err)
	}
	if err := st.CompleteTelegramOutbound(t.Context(), claimedEffect,
		[]string{"502"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimTelegramOutbound(t.Context(), effectID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("sent effect became resendable err=%v", err)
	}
	ambiguousID := uuid.NewString()
	ambiguousEffect, err := st.PrepareTelegramOutbound(t.Context(), tenantID,
		userID, route.ID, ambiguousID, "brief", "可能已发送")
	if err != nil {
		t.Fatal(err)
	}
	ambiguousEffect, err = st.ClaimTelegramOutbound(t.Context(), ambiguousID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTelegramOutboundAmbiguous(t.Context(), ambiguousEffect,
		nil, "transport_ambiguous"); err != nil {
		t.Fatal(err)
	}
	stats, err := st.TelegramBlockedReplyStatsForUser(t.Context(), "12345",
		tenantID, userID)
	if err != nil || stats.Count != 1 {
		t.Fatalf("outbound blocked stats=%+v err=%v", stats, err)
	}
	rejectedID := uuid.NewString()
	rejectedEffect, err := st.PrepareTelegramOutbound(t.Context(), tenantID,
		userID, route.ID, rejectedID, "test", "明确拒绝")
	if err != nil {
		t.Fatal(err)
	}
	rejectedEffect, err = st.ClaimTelegramOutbound(t.Context(), rejectedID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTelegramOutboundRejected(t.Context(), rejectedEffect, ""); err != nil {
		t.Fatal(err)
	}
	stats, err = st.TelegramBlockedReplyStatsForUser(t.Context(), "12345",
		tenantID, userID)
	if err != nil || stats.Count != 2 {
		t.Fatalf("terminal outbound stats=%+v err=%v", stats, err)
	}
	if err := st.RevokeTelegramRoute(t.Context(), tenantID, userID,
		route.ID, "12345"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ResolveTelegramRoute(t.Context(), "12345", "777",
		"-1007", "88"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked route resolved err=%v", err)
	}
}
