package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestMigration141ChannelBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/141_channel_ingress.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE channel_identities",
		"CREATE TABLE channel_link_requests",
		"CREATE TABLE channel_ingress_receipts",
		"FOREIGN KEY (tenant_id,user_id)",
		"chat_type='private'",
		"uq_channel_identity_provider_actor_active",
		"uq_channel_identity_user_provider_active",
		"PRIMARY KEY (provider,app_identity,provider_update_id)",
		"ck_channel_ingress_telegram_update_id",
		"CREATE UNIQUE INDEX uq_channel_ingress_stable_turn",
		"status IN ('pending','processing','reply_ready','sending'",
		"ALTER TABLE channel_identities ENABLE ROW LEVEL SECURITY",
		"REVOKE ALL ON channel_identities,channel_link_requests,channel_ingress_receipts",
		"refusing downgrade while Telegram channel history exists",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 141 lost boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"telegram_username", "feishu_open_id", "DROP TABLE users",
		"GRANT ALL", "drop_pending_updates",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 141 introduced unsafe coupling %q", forbidden)
		}
	}
}

func TestTelegramBindingAndIngressReplayPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 147); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	userID, tenantID := migration129Identity(t, database, "telegram-owner")

	rawToken := bytes.Repeat([]byte{0x5a}, 32)
	tokenHash := sha256.Sum256(rawToken)
	expiresAt := time.Now().Add(10 * time.Minute)
	unknownHash := sha256.Sum256(bytes.Repeat([]byte{0x59}, 32))
	if err := st.IssueTelegramLinkRequest(
		t.Context(), tenantID, userID+99999, "12345", unknownHash[:], expiresAt); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("non-member link request err=%v", err)
	}
	if err := st.IssueTelegramLinkRequest(
		t.Context(), tenantID, userID, "12345", tokenHash[:], expiresAt); err != nil {
		t.Fatal(err)
	}
	requestDigest := strings.Repeat("a", 64)
	const workers = 8
	results := make(chan ChannelIdentity, workers)
	errs := make(chan error, workers)
	var firstConsumptions atomic.Int32
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identity, first, consumeErr := st.ConsumeTelegramLinkRequest(
				t.Context(), tokenHash[:], "12345", "777", "777", requestDigest)
			if first {
				firstConsumptions.Add(1)
			}
			results <- identity
			errs <- consumeErr
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	var identity ChannelIdentity
	for consumeErr := range errs {
		if consumeErr != nil {
			t.Fatalf("concurrent exact consume: %v", consumeErr)
		}
	}
	for got := range results {
		if identity.ID == 0 {
			identity = got
		}
		if got.ID != identity.ID || got.TenantID != tenantID || got.UserID != userID {
			t.Fatalf("consume identities diverged: first=%+v got=%+v", identity, got)
		}
	}
	if firstConsumptions.Load() != 1 {
		t.Fatalf("first consumptions=%d", firstConsumptions.Load())
	}
	if _, _, err := st.ConsumeTelegramLinkRequest(
		t.Context(), tokenHash[:], "12345", "778", "778", requestDigest,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed actor replay err=%v", err)
	}
	if _, _, err := st.ConsumeTelegramLinkRequest(
		t.Context(), tokenHash[:], "12345", "777", "777", strings.Repeat("f", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed digest replay err=%v", err)
	}
	wrongBotHash := sha256.Sum256(bytes.Repeat([]byte{0x58}, 32))
	if err := st.IssueTelegramLinkRequest(
		t.Context(), tenantID, userID, "12345", wrongBotHash[:], expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ConsumeTelegramLinkRequest(
		t.Context(), wrongBotHash[:], "54321", "777", "777", requestDigest,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("wrong bot consume err=%v", err)
	}
	expiredHash := sha256.Sum256(bytes.Repeat([]byte{0x57}, 32))
	if err := st.IssueTelegramLinkRequest(
		t.Context(), tenantID, userID, "12345", expiredHash[:], expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE channel_link_requests
		    SET created_at=clock_timestamp()-interval '2 seconds',
		        expires_at=clock_timestamp()-interval '1 second'
		  WHERE token_hash=$1`, expiredHash[:]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ConsumeTelegramLinkRequest(
		t.Context(), expiredHash[:], "12345", "777", "777", requestDigest,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("expired consume err=%v", err)
	}
	resolved, err := st.ResolveTelegramIdentity(
		t.Context(), "12345", "777", "777")
	if err != nil || resolved.ID != identity.ID {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
	bound, err := st.GetTelegramIdentityForUser(t.Context(), tenantID, userID, "12345")
	if err != nil || bound.ID != identity.ID {
		t.Fatalf("bound=%+v err=%v", bound, err)
	}
	if _, err := st.GetTelegramIdentityForUser(t.Context(), tenantID, userID, "other-bot"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("wrong bot binding err=%v", err)
	}
	if _, err := st.ResolveTelegramIdentity(t.Context(), " bad", "777", "777"); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid identity err=%v", err)
	}
	if _, err := st.ResolveTelegramIdentity(t.Context(), "12345", "778", "778"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("unbound identity err=%v", err)
	}
	if err := st.IssueTelegramLinkRequest(
		t.Context(), tenantID, userID, "12345", []byte("short"), expiresAt); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid link request err=%v", err)
	}
	if _, _, err := st.ConsumeTelegramLinkRequest(
		t.Context(), []byte("short"), "12345", "777", "777", requestDigest); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid consume err=%v", err)
	}
	if _, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Second); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid lease err=%v", err)
	}
	for _, tc := range []struct {
		name     string
		identity ChannelIdentity
		updateID string
		digest   string
		text     string
		stableID string
	}{
		{name: "identity", identity: ChannelIdentity{}, updateID: "7000", digest: strings.Repeat("7", 64), text: "x", stableID: uuid.NewString()},
		{name: "update", identity: identity, updateID: "not-number", digest: strings.Repeat("7", 64), text: "x", stableID: uuid.NewString()},
		{name: "digest", identity: identity, updateID: "7000", digest: "short", text: "x", stableID: uuid.NewString()},
		{name: "text", identity: identity, updateID: "7000", digest: strings.Repeat("7", 64), text: " ", stableID: uuid.NewString()},
		{name: "turn", identity: identity, updateID: "7000", digest: strings.Repeat("7", 64), text: "x", stableID: "bad"},
	} {
		if _, err := st.AcceptTelegramIngress(
			t.Context(), tc.identity, tc.updateID, tc.digest, tc.text, tc.stableID); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("invalid accept %s err=%v", tc.name, err)
		}
	}

	// Exercise the certain provider-success settlement separately from the
	// ambiguous and rejected paths below.
	completeTurn := uuid.NewString()
	if created, err := st.AcceptTelegramIngress(
		t.Context(), identity, "8000", strings.Repeat("8", 64),
		"完成发送", completeTurn); err != nil || !created {
		t.Fatalf("complete ingress created=%t err=%v", created, err)
	}
	completeClaim, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Minute)
	if err != nil || completeClaim.ProviderUpdateID != "8000" {
		t.Fatalf("complete claim=%+v err=%v", completeClaim, err)
	}
	if err := st.MarkTelegramIngressReplyReady(t.Context(), completeClaim, "已完成"); err != nil {
		t.Fatal(err)
	}
	staleCompleteClaim := completeClaim
	staleCompleteClaim.ProcessingLease = uuid.NewString()
	if err := st.MarkTelegramIngressReplyReady(t.Context(), staleCompleteClaim, "stale"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale processing lease err=%v", err)
	}
	completeSend, err := st.ClaimTelegramReplySend(
		t.Context(), "telegram", "12345", "8000")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteTelegramReply(t.Context(), completeSend, []string{"501"}); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteTelegramReply(t.Context(), completeSend, nil); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("empty provider IDs err=%v", err)
	}
	if err := st.CompleteTelegramReply(t.Context(), completeSend, []string{"502"}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("duplicate completion err=%v", err)
	}
	if _, err := st.ClaimNextTelegramReplySend(t.Context(), "12345"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("empty reply recovery err=%v", err)
	}

	turnID := uuid.NewString()
	created, err := st.AcceptTelegramIngress(
		t.Context(), identity, "9001", strings.Repeat("b", 64),
		"列出我的任务", turnID)
	if err != nil || !created {
		t.Fatalf("first ingress created=%t err=%v", created, err)
	}
	created, err = st.AcceptTelegramIngress(
		t.Context(), identity, "9001", strings.Repeat("b", 64),
		"列出我的任务", turnID)
	if err != nil || created {
		t.Fatalf("exact replay created=%t err=%v", created, err)
	}
	if _, err := st.AcceptTelegramIngress(
		t.Context(), identity, "9001", strings.Repeat("c", 64),
		"删除任务", turnID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("changed update replay err=%v", err)
	}

	claims := make(chan ChannelIngress, workers)
	claimErrs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, claimErr := st.ClaimNextTelegramIngress(
				t.Context(), "12345", time.Minute)
			claims <- claimed
			claimErrs <- claimErr
		}()
	}
	wg.Wait()
	close(claims)
	close(claimErrs)
	claimedCount := 0
	var claimed ChannelIngress
	for claimErr := range claimErrs {
		if claimErr == nil {
			claimedCount++
		} else if !errors.Is(claimErr, types.ErrNotFound) {
			t.Fatalf("claim err=%v", claimErr)
		}
	}
	for got := range claims {
		if got.ProcessingLease != "" {
			claimed = got
		}
	}
	if claimedCount != 1 || claimed.StableTurnID != turnID {
		t.Fatalf("claimed_count=%d claim=%+v", claimedCount, claimed)
	}
	if err := st.MarkTelegramIngressReplyReady(
		t.Context(), claimed, "这是耐久回复"); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after reply_ready and before the exact send claim. The
	// startup recovery claim must find it without re-entering Agent processing.
	sending, err := st.ClaimNextTelegramReplySend(t.Context(), "12345")
	if err != nil || sending.ReplyText != "这是耐久回复" {
		t.Fatalf("sending=%+v err=%v", sending, err)
	}
	if err := st.MarkTelegramReplyAmbiguous(
		t.Context(), sending, []string{"first-chunk"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimTelegramReplySend(
		t.Context(), "telegram", "12345", "9001"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("ambiguous reply became resendable: %v", err)
	}
	if _, err := st.ClaimNextTelegramIngress(
		t.Context(), "12345", time.Minute); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("ambiguous ingress became replayable: %v", err)
	}

	// Two later updates for one identity have a durable head-of-line. The
	// second cannot enter Agent while the first is processing or reply_ready.
	for _, update := range []struct{ id, text string }{
		{"9002", "创建任务"}, {"9003", "删除任务"},
	} {
		if created, err := st.AcceptTelegramIngress(
			t.Context(), identity, update.id, strings.Repeat(update.id[len(update.id)-1:], 64),
			update.text, uuid.NewString()); err != nil || !created {
			t.Fatalf("accept ordered update %s created=%t err=%v", update.id, created, err)
		}
	}
	first, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Minute)
	if err != nil || first.ProviderUpdateID != "9002" {
		t.Fatalf("first ordered claim=%+v err=%v", first, err)
	}
	if _, err := st.ClaimNextTelegramIngress(
		t.Context(), "12345", time.Minute); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("second update bypassed processing head: %v", err)
	}
	if err := st.MarkTelegramIngressReplyReady(t.Context(), first, "已创建"); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTelegramIngressReplyReady(t.Context(), first, " "); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("blank reply err=%v", err)
	}
	if _, err := st.ClaimNextTelegramIngress(
		t.Context(), "12345", time.Minute); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("second update bypassed reply head: %v", err)
	}
	firstSend, err := st.ClaimTelegramReplySend(
		t.Context(), "telegram", "12345", "9002")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTelegramReplyRejected(
		t.Context(), firstSend, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTelegramReplyRejected(t.Context(), firstSend, "again"); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("duplicate provider rejection err=%v", err)
	}
	second, err := st.ClaimNextTelegramIngress(t.Context(), "12345", time.Minute)
	if err != nil || second.ProviderUpdateID != "9003" {
		t.Fatalf("second ordered claim=%+v err=%v", second, err)
	}
	if err := st.MarkTelegramIngressFailed(t.Context(), second, ""); err != nil {
		t.Fatal(err)
	}

	// Unlink revokes every unconsumed bearer link, leaves provider-crossed
	// sending audit in place, and makes a previously resolved identity stale.
	secondToken := sha256.Sum256(bytes.Repeat([]byte{0x6b}, 32))
	if err := st.IssueTelegramLinkRequest(
		t.Context(), tenantID, userID, "12345", secondToken[:],
		time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE channel_ingress_receipts SET status='sending',reply_text='审计保留',
		 error_code=NULL WHERE provider='telegram' AND app_identity='12345' AND
		 provider_update_id='9001'`); err != nil {
		t.Fatal(err)
	}
	// Keep one deterministic pre-provider receipt so unlink must preserve a
	// failed/identity_revoked audit independently of which side wins the race
	// below.
	if _, err := st.AcceptTelegramIngress(
		t.Context(), identity, "9004", strings.Repeat("d", 64),
		"等待解绑取消的消息", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	// Pause both stale accept and unlink behind the same identity row. Whichever
	// wins after release, unlink must linearize to no active identity and no
	// executable receipt; there is no stale-resolve TOCTOU.
	gateTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var lockedID int64
	if err := gateTx.QueryRowContext(t.Context(),
		`SELECT id FROM channel_identities WHERE id=$1 FOR UPDATE`, identity.ID,
	).Scan(&lockedID); err != nil {
		_ = gateTx.Rollback()
		t.Fatal(err)
	}
	acceptDone := make(chan error, 1)
	revokeDone := make(chan error, 1)
	started := make(chan struct{}, 2)
	go func() {
		started <- struct{}{}
		_, acceptErr := st.AcceptTelegramIngress(
			t.Context(), identity, "9005", strings.Repeat("e", 64),
			"与解绑并发的消息", uuid.NewString())
		acceptDone <- acceptErr
	}()
	go func() {
		started <- struct{}{}
		revokeDone <- st.RevokeTelegramIdentity(
			t.Context(), tenantID, userID, "12345")
	}()
	<-started
	<-started
	if err := gateTx.Commit(); err != nil {
		t.Fatal(err)
	}
	acceptErr := <-acceptDone
	if acceptErr != nil && !errors.Is(acceptErr, types.ErrNotFound) {
		t.Fatalf("concurrent stale accept err=%v", acceptErr)
	}
	if err := <-revokeDone; err != nil {
		t.Fatalf("concurrent unlink with retained sending audit: %v", err)
	}
	var executable int
	if err := database.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM channel_ingress_receipts
		  WHERE identity_id=$1 AND status IN ('pending','processing','reply_ready')`,
		identity.ID).Scan(&executable); err != nil || executable != 0 {
		t.Fatalf("executable receipts after unlink=%d err=%v", executable, err)
	}
	stats, err := st.TelegramBlockedReplyStats(t.Context(), "12345")
	if err != nil || stats.Count < 3 || stats.OldestAt == nil {
		t.Fatalf("blocked reply stats=%+v err=%v", stats, err)
	}
	var revokedFailures int
	if err := database.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM channel_ingress_receipts
		  WHERE identity_id=$1 AND status='failed' AND error_code='identity_revoked'`,
		identity.ID).Scan(&revokedFailures); err != nil || revokedFailures == 0 {
		t.Fatalf("revoked receipt audit=%d err=%v", revokedFailures, err)
	}
	if stats.Count != 3 {
		t.Fatalf("identity-revoked cancellation leaked into blocked stats: %+v", stats)
	}
	userStats, err := st.TelegramBlockedReplyStatsForUser(
		t.Context(), "12345", tenantID, userID)
	if err != nil || userStats.Count != stats.Count || userStats.OldestAt == nil {
		t.Fatalf("user blocked reply stats=%+v err=%v", userStats, err)
	}
	if _, _, err := st.ConsumeTelegramLinkRequest(
		t.Context(), secondToken[:], "12345", "777", "777", requestDigest,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("unlinked bearer token remained valid: %v", err)
	}
	if _, err := st.AcceptTelegramIngress(
		t.Context(), identity, "9006", strings.Repeat("f", 64),
		"解绑后的消息", uuid.NewString()); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("stale resolved identity accepted after unlink: %v", err)
	}
	if err := st.RevokeTelegramIdentity(t.Context(), tenantID, userID, "12345"); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("duplicate unlink err=%v", err)
	}
	// Every storage boundary must surface canceled/unavailable database work;
	// none may silently report success or turn a transient fault into a durable
	// provider acknowledgement.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	validItem := ChannelIngress{Provider: "telegram", AppIdentity: "12345",
		ProviderUpdateID: "9999", ProcessingLease: uuid.NewString()}
	checks := []struct {
		name string
		call func() error
	}{
		{name: "global stats", call: func() error { _, err := st.TelegramBlockedReplyStats(canceled, "12345"); return err }},
		{name: "user stats", call: func() error {
			_, err := st.TelegramBlockedReplyStatsForUser(canceled, "12345", tenantID, userID)
			return err
		}},
		{name: "issue", call: func() error {
			return st.IssueTelegramLinkRequest(canceled, tenantID, userID, "12345", bytes.Repeat([]byte{1}, 32), time.Now().Add(time.Minute))
		}},
		{name: "consume", call: func() error {
			_, _, err := st.ConsumeTelegramLinkRequest(canceled, bytes.Repeat([]byte{1}, 32), "12345", "777", "777", requestDigest)
			return err
		}},
		{name: "resolve", call: func() error { _, err := st.ResolveTelegramIdentity(canceled, "12345", "777", "777"); return err }},
		{name: "binding", call: func() error { _, err := st.GetTelegramIdentityForUser(canceled, tenantID, userID, "12345"); return err }},
		{name: "revoke", call: func() error { return st.RevokeTelegramIdentity(canceled, tenantID, userID, "12345") }},
		{name: "accept", call: func() error {
			_, err := st.AcceptTelegramIngress(canceled, identity, "9999", strings.Repeat("9", 64), "x", uuid.NewString())
			return err
		}},
		{name: "claim ingress", call: func() error { _, err := st.ClaimNextTelegramIngress(canceled, "12345", time.Minute); return err }},
		{name: "reply ready", call: func() error { return st.MarkTelegramIngressReplyReady(canceled, validItem, "x") }},
		{name: "ingress failed", call: func() error { return st.MarkTelegramIngressFailed(canceled, validItem, "internal") }},
		{name: "claim reply", call: func() error { _, err := st.ClaimTelegramReplySend(canceled, "telegram", "12345", "9999"); return err }},
		{name: "recover reply", call: func() error { _, err := st.ClaimNextTelegramReplySend(canceled, "12345"); return err }},
		{name: "complete reply", call: func() error { return st.CompleteTelegramReply(canceled, validItem, []string{"1"}) }},
		{name: "reject reply", call: func() error { return st.MarkTelegramReplyRejected(canceled, validItem, "rejected") }},
		{name: "ambiguous reply", call: func() error { return st.MarkTelegramReplyAmbiguous(canceled, validItem, nil, "ambiguous") }},
	}
	for _, check := range checks {
		if err := check.call(); err == nil {
			t.Fatalf("%s ignored canceled context", check.name)
		}
	}
	if _, err := provider.DownTo(t.Context(), 132); err == nil ||
		(!strings.Contains(err.Error(), "Telegram channel history") &&
			!strings.Contains(err.Error(), "routed channel history")) {
		t.Fatalf("channel Down destroyed retained Telegram history: %v", err)
	}
}
