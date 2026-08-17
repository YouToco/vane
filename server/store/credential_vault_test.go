package store

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/channelruntime"
	"github.com/YouToco/vane/server/types"
)

func configureTestCredentialVault(t *testing.T, st *Store) {
	t.Helper()
	if err := st.ConfigureCredentialVault(
		"test-key-1", strings.Repeat("42", 32),
		"test-key-0="+strings.Repeat("24", 32)); err != nil {
		t.Fatal(err)
	}
}

func credentialVaultTestStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), latestMigrationVersion); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	configureTestCredentialVault(t, st)
	return st, database
}

func TestCredentialVaultTenantRotationRetentionAndAuthorization(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ctx := t.Context()
	ownerID, tenantID := migration129Identity(t, database, "credential-vault-owner")
	var memberID int64
	if err := database.QueryRowContext(ctx, `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'member') RETURNING id`,
		fmt.Sprintf("ou_credential_vault_member_%d", time.Now().UnixNano()),
	).Scan(&memberID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES($1,$2,'member')`, tenantID, memberID); err != nil {
		t.Fatal(err)
	}

	scope := CredentialScope{Kind: "tenant", TenantID: tenantID,
		Provider: "telegram", Purpose: fmt.Sprintf("bot_api_%d", time.Now().UnixNano())}
	firstSecret := json.RawMessage(`{"bot_token":"synthetic-first"}`)
	first, err := st.RotateCredential(ctx, scope, firstSecret,
		json.RawMessage(`{"bot_username":"test_bot"}`), ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != 1 || first.Status != "active" || len(first.Fingerprint) != 64 {
		t.Fatalf("unexpected first metadata: %+v", first)
	}
	var storedCiphertext []byte
	if err := st.pool.QueryRow(ctx, `SELECT ciphertext FROM credential_vault_entries
		WHERE tenant_id=$1 AND provider=$2 AND purpose=$3 AND generation=1`,
		scope.TenantID, scope.Provider, scope.Purpose).Scan(&storedCiphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(storedCiphertext, []byte("synthetic-first")) {
		t.Fatal("database ciphertext contains plaintext token")
	}
	if _, err := st.CredentialStatus(ctx, scope, memberID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("member read err=%v, want hidden not-found", err)
	}
	if _, err := st.RotateCredential(ctx, scope,
		json.RawMessage(`{"bot_token":"stolen"}`), json.RawMessage(`{}`), memberID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("member rotate err=%v, want hidden not-found", err)
	}

	second, err := st.RotateCredential(ctx, scope,
		json.RawMessage(`{"bot_token":"synthetic-second"}`),
		json.RawMessage(`{"bot_username":"test_bot"}`), ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != 2 || second.Fingerprint == first.Fingerprint {
		t.Fatalf("unexpected second metadata: %+v", second)
	}
	for generation, want := range map[int64]string{1: "synthetic-first", 2: "synthetic-second", 0: "synthetic-second"} {
		var got string
		err := st.UseCredential(ctx, scope, generation,
			func(secret []byte, _ CredentialMetadata) error {
				var payload struct {
					BotToken string `json:"bot_token"`
				}
				if err := json.Unmarshal(secret, &payload); err != nil {
					return err
				}
				got = payload.BotToken
				return nil
			})
		if err != nil || got != want {
			t.Fatalf("generation %d=(%q,%v), want %q", generation, got, err, want)
		}
	}
	var active, retired int
	if err := st.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status='active'),count(*) FILTER (WHERE status='retired')
		FROM credential_vault_entries WHERE tenant_id=$1 AND
		provider=$2 AND purpose=$3`, scope.TenantID, scope.Provider, scope.Purpose,
	).Scan(&active, &retired); err != nil {
		t.Fatal(err)
	}
	if active != 1 || retired != 1 {
		t.Fatalf("active=%d retired=%d", active, retired)
	}
}

func TestCredentialVaultConcurrentRotationHasOneActiveGeneration(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ctx := t.Context()
	var ownerID int64
	if err := database.QueryRowContext(ctx, `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'platform-owner') RETURNING id`,
		"credential-vault-platform-owner-"+fmt.Sprint(time.Now().UnixNano()),
	).Scan(&ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, ownerID); err != nil {
		t.Fatal(err)
	}
	scope := CredentialScope{Kind: "platform", Provider: "llm",
		Purpose: fmt.Sprintf("primary_api_%d", time.Now().UnixNano())}
	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := st.RotateCredential(ctx, scope,
				json.RawMessage(fmt.Sprintf(`{"api_key":"synthetic-%d"}`, index)),
				json.RawMessage(`{"provider":"openai-compatible"}`), ownerID)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var total, active int
	var minGeneration, maxGeneration int64
	if err := st.pool.QueryRow(ctx, `SELECT count(*),
		count(*) FILTER (WHERE status='active'),min(generation),max(generation)
		FROM credential_vault_entries WHERE scope_kind='platform' AND
		provider=$1 AND purpose=$2`, scope.Provider, scope.Purpose).Scan(
		&total, &active, &minGeneration, &maxGeneration); err != nil {
		t.Fatal(err)
	}
	if total != writers || active != 1 || minGeneration != 1 || maxGeneration != writers {
		t.Fatalf("total=%d active=%d generations=%d..%d", total, active,
			minGeneration, maxGeneration)
	}
	if err := st.RevokeCredential(ctx, scope, ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CredentialStatus(ctx, scope, ownerID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("status after revoke err=%v, want not found", err)
	}
}

func TestMigration145CredentialVaultBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/145_credential_vault.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE credential_vault_entries",
		"scope_kind='platform' AND tenant_id IS NULL",
		"scope_kind='tenant' AND tenant_id IS NOT NULL",
		"octet_length(nonce)=12",
		"uq_credential_vault_platform_active",
		"uq_credential_vault_tenant_active",
		"REVOKE ALL ON credential_vault_entries FROM PUBLIC,vane_app",
		"ALTER TABLE credential_vault_entries ENABLE ROW LEVEL SECURITY",
		"migration 145 down refused: encrypted credential history exists",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 145 lost boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{"app_secret TEXT", "bot_token TEXT", "api_key TEXT", "GRANT ALL"} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 145 introduced plaintext/overbroad authority %q", forbidden)
		}
	}
}

func TestMigration145DownRefusesCredentialHistoryPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), latestMigrationVersion); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	configureTestCredentialVault(t, st)
	var ownerID int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'platform-owner') RETURNING id`,
		"credential-vault-down-owner-"+fmt.Sprint(time.Now().UnixNano()),
	).Scan(&ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES(1,$1,'owner')`, ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RotateCredential(t.Context(), CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: "down_guard",
	}, json.RawMessage(`{"api_key":"synthetic"}`), json.RawMessage(`{}`), ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 136); err == nil ||
		!strings.Contains(err.Error(), "encrypted credential history exists") {
		t.Fatalf("migration 145 down accepted retained credential: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM credential_vault_entries`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 136); err != nil {
		t.Fatalf("empty credential vault could not downgrade: %v", err)
	}
	var exists bool
	if err := database.QueryRowContext(t.Context(),
		`SELECT to_regclass('public.credential_vault_entries') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("credential vault table survived successful migration 145 down")
	}
}

func TestCredentialVaultExternalBotIdentityIsUniqueAcrossTenants(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ownerA, tenantA := migration129Identity(t, database, "credential-bot-a")
	ownerB, tenantB := migration129Identity(t, database, "credential-bot-b")
	metadata := json.RawMessage(`{"bot_id":778899,"bot_username":"shared_bot"}`)
	secret := json.RawMessage(`{"bot_token":"778899:synthetic","webhook_secret":"synthetic"}`)
	if _, err := st.RotateCredential(t.Context(), CredentialScope{
		Kind: "user", TenantID: tenantA, UserID: ownerA,
		Provider: "telegram", Purpose: "bot_api",
	}, secret, metadata, ownerA); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RotateCredential(t.Context(), CredentialScope{
		Kind: "user", TenantID: tenantB, UserID: ownerB,
		Provider: "telegram", Purpose: "bot_api",
	}, secret, metadata, ownerB); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("duplicate bot err=%v, want conflict", err)
	}
	items, err := st.ListActiveUserCredentialMetadata(
		t.Context(), "telegram", "bot_api")
	if err != nil {
		t.Fatal(err)
	}
	var matched []CredentialMetadata
	for _, item := range items {
		var payload struct {
			BotID int64 `json:"bot_id"`
		}
		if err := json.Unmarshal(item.Metadata, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.BotID == 778899 {
			matched = append(matched, item)
		}
	}
	if len(matched) != 1 || matched[0].TenantID != tenantA {
		t.Fatalf("active duplicate bot rows=%+v", matched)
	}
}

func TestCredentialVaultOrdinaryMemberOwnsOnlyTheirUserScope(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ownerID, tenantID := migration129Identity(t, database, "credential-user-owner")
	var memberID int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO users(feishu_open_id,name)
		VALUES($1,'credential user member') RETURNING id`,
		fmt.Sprintf("credential-user-member-%d", time.Now().UnixNano())).Scan(&memberID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES($1,$2,'member')`, tenantID, memberID); err != nil {
		t.Fatal(err)
	}
	scope := CredentialScope{Kind: "user", TenantID: tenantID, UserID: memberID,
		Provider: "telegram", Purpose: "bot_api"}
	if _, err := st.RotateCredential(t.Context(), scope,
		json.RawMessage(`{"bot_token":"9911:synthetic","webhook_secret":"synthetic"}`),
		json.RawMessage(`{"bot_id":9911,"bot_username":"member_bot"}`), memberID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CredentialStatus(t.Context(), scope, ownerID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("tenant owner read member secret metadata err=%v, want hidden", err)
	}
	foreign := scope
	foreign.UserID = ownerID
	if _, err := st.RotateCredential(t.Context(), foreign,
		json.RawMessage(`{"bot_token":"9912:synthetic","webhook_secret":"synthetic"}`),
		json.RawMessage(`{"bot_id":9912}`), memberID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("member wrote another user scope err=%v, want hidden", err)
	}
}

func TestTelegramCredentialRotationRevokesAuthorityAndPinsManagerGeneration(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ctx := t.Context()
	userID, tenantID := migration129Identity(t, database, "telegram-runtime-pin")
	scope := CredentialScope{Kind: "user", TenantID: tenantID, UserID: userID,
		Provider: "telegram", Purpose: "bot_api"}
	secret := json.RawMessage(`{"bot_token":"778899:synthetic","webhook_secret":"synthetic"}`)
	metadata := json.RawMessage(`{"bot_id":778899,"bot_username":"runtime_pin"}`)
	first, err := st.RotateCredential(ctx, scope, secret, metadata, userID)
	if err != nil {
		t.Fatal(err)
	}
	bind := func(actor, chat string) (ChannelIdentity, ChannelRoute) {
		t.Helper()
		var identityID int64
		if err := database.QueryRowContext(ctx, `
			INSERT INTO channel_identities
			 (tenant_id,user_id,provider,app_identity,external_user_id,
			  provider_chat_id,chat_type)
			 VALUES($1,$2,'telegram','778899',$3,$4,'private') RETURNING id`,
			tenantID, userID, actor, chat).Scan(&identityID); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(ctx, `
			INSERT INTO channel_routes
			 (tenant_id,user_id,identity_id,provider,app_identity,
			  provider_chat_id,provider_thread_id,chat_type,route_kind)
			 VALUES($1,$2,$3,'telegram','778899',$4,'0','private','private')`,
			tenantID, userID, identityID, chat); err != nil {
			t.Fatal(err)
		}
		identity, route, err := st.ResolveTelegramRoute(
			ctx, "778899", actor, chat, "0")
		if err != nil {
			t.Fatal(err)
		}
		return identity, route
	}
	identity, route := bind("actor-old", "chat-old")
	if _, err := database.ExecContext(ctx, `UPDATE memberships SET role='member'
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	turnID := uuid.NewString()
	if _, err := st.AcceptTelegramRoutedIngress(ctx, identity, route, "78001",
		strings.Repeat("a", 64), "hello", turnID, "1", "message", "", nil); err != nil {
		t.Fatal(err)
	}
	authorityV1 := TelegramRuntimeAuthority{TenantID: tenantID, UserID: userID,
		CredentialGeneration: first.Generation, AppIdentity: "778899"}
	runtimeStore := channelRuntimeTestStore(t, database, st)
	claimedIngress, err := runtimeStore.ClaimNextTelegramIngressAuthorized(
		ctx, authorityV1, 30*time.Second)
	if err != nil || claimedIngress.MembershipRole != types.MembershipRoleMember {
		t.Fatalf("live principal role=%q err=%v", claimedIngress.MembershipRole, err)
	}
	preparedID := uuid.NewString()
	if _, err := st.PrepareTelegramOutbound(ctx, tenantID, userID, route.ID,
		preparedID, "periodic_report", "frozen body"); err != nil {
		t.Fatal(err)
	}
	second, err := st.RotateCredential(ctx, scope, secret, metadata, userID)
	if err != nil {
		t.Fatal(err)
	}
	var identityStatus, routeStatus, ingressStatus, ingressCode,
		outboundStatus, outboundCode string
	if err := database.QueryRowContext(ctx, `
		SELECT ci.status,cr.status,r.status,r.error_code,e.status,e.error_code
		  FROM channel_identities ci
		  JOIN channel_routes cr ON cr.identity_id=ci.id
		  JOIN channel_ingress_receipts r ON r.identity_id=ci.id
		  JOIN channel_outbound_effects e ON e.route_id=cr.id
		 WHERE ci.id=$1 AND e.effect_id=$2`, identity.ID, preparedID).Scan(
		&identityStatus, &routeStatus, &ingressStatus, &ingressCode,
		&outboundStatus, &outboundCode); err != nil {
		t.Fatal(err)
	}
	if identityStatus != "revoked" || routeStatus != "revoked" ||
		ingressStatus != "failed" || ingressCode != "credential_rotated" ||
		outboundStatus != "failed" || outboundCode != "credential_rotated" {
		t.Fatalf("rotation state identity=%s route=%s ingress=%s/%s outbound=%s/%s",
			identityStatus, routeStatus, ingressStatus, ingressCode,
			outboundStatus, outboundCode)
	}
	_, newRoute := bind("actor-new", "chat-new")
	newEffectID := uuid.NewString()
	newEffect, err := st.PrepareTelegramOutbound(ctx, tenantID, userID, newRoute.ID,
		newEffectID, "periodic_report", "new generation")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeStore.ClaimTelegramOutboundAuthorized(
		ctx, authorityV1, newEffectID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("retired manager generation claimed new work: %v", err)
	}
	authorityV2 := authorityV1
	authorityV2.CredentialGeneration = second.Generation
	forged, err := channelruntime.BindDurableSend(
		channelruntime.ProviderTelegram, tenantID, userID+1, newRoute.ID,
		newEffectID, newEffect.EffectKind, newEffect.PayloadDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeStore.ClaimTelegramOutboundPermitAuthorized(
		ctx, authorityV2, forged); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("forged permit was not rejected before claim: %v", err)
	}
	if _, err := runtimeStore.ClaimTelegramOutboundAuthorized(ctx, authorityV2, newEffectID); err != nil {
		t.Fatalf("active manager generation could not claim: %v", err)
	}
	pendingOnRevoke := uuid.NewString()
	if _, err := st.PrepareTelegramOutbound(ctx, tenantID, userID, newRoute.ID,
		pendingOnRevoke, "periodic_report", "revoke me"); err != nil {
		t.Fatal(err)
	}
	if err := st.RevokeCredential(ctx, scope, userID); err != nil {
		t.Fatal(err)
	}
	var newIdentityStatus, sendingStatus, revokedPreparedStatus,
		revokedPreparedCode string
	if err := database.QueryRowContext(ctx, `
		SELECT ci.status,sending.status,pending.status,pending.error_code
		  FROM channel_identities ci
		  JOIN channel_routes cr ON cr.identity_id=ci.id
		  JOIN channel_outbound_effects sending ON sending.route_id=cr.id
		  JOIN channel_outbound_effects pending ON pending.route_id=cr.id
		 WHERE ci.id=$1 AND sending.effect_id=$2 AND pending.effect_id=$3`,
		newRoute.IdentityID, newEffectID, pendingOnRevoke).Scan(
		&newIdentityStatus, &sendingStatus, &revokedPreparedStatus,
		&revokedPreparedCode); err != nil {
		t.Fatal(err)
	}
	if newIdentityStatus != "revoked" || sendingStatus != "sending" ||
		revokedPreparedStatus != "failed" ||
		revokedPreparedCode != "credential_revoked" {
		t.Fatalf("revoke state identity=%s sending=%s pending=%s/%s",
			newIdentityStatus, sendingStatus, revokedPreparedStatus,
			revokedPreparedCode)
	}
}

func TestTelegramOutboundClaimAndCredentialRotationLinearizePostgres(t *testing.T) {
	st, database := credentialVaultTestStore(t)
	ctx := t.Context()
	userID, tenantID := migration129Identity(t, database, "telegram-claim-rotation-race")
	scope := CredentialScope{Kind: "user", TenantID: tenantID, UserID: userID,
		Provider: "telegram", Purpose: "bot_api"}
	secret := json.RawMessage(`{"bot_token":"881155:synthetic","webhook_secret":"synthetic"}`)
	metadata := json.RawMessage(`{"bot_id":881155,"bot_username":"race"}`)
	credential, err := st.RotateCredential(ctx, scope, secret, metadata, userID)
	if err != nil {
		t.Fatal(err)
	}
	var identityID, routeID int64
	if err := database.QueryRowContext(ctx, `INSERT INTO channel_identities
		(tenant_id,user_id,provider,app_identity,external_user_id,
		 provider_chat_id,chat_type)
		VALUES($1,$2,'telegram','881155','actor','chat','private') RETURNING id`,
		tenantID, userID).Scan(&identityID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(ctx, `INSERT INTO channel_routes
		(tenant_id,user_id,identity_id,provider,app_identity,provider_chat_id,
		 provider_thread_id,chat_type,route_kind)
		VALUES($1,$2,$3,'telegram','881155','chat','0','private','private')
		RETURNING id`, tenantID, userID, identityID).Scan(&routeID); err != nil {
		t.Fatal(err)
	}
	effectID := uuid.NewString()
	if _, err := st.PrepareTelegramOutbound(ctx, tenantID, userID, routeID,
		effectID, "periodic_report", "linearize me"); err != nil {
		t.Fatal(err)
	}
	runtimeStore := channelRuntimeTestStore(t, database, st)
	authority := TelegramRuntimeAuthority{TenantID: tenantID, UserID: userID,
		CredentialGeneration: credential.Generation, AppIdentity: "881155"}
	start := make(chan struct{})
	claimErr := make(chan error, 1)
	rotateErr := make(chan error, 1)
	go func() {
		<-start
		_, err := runtimeStore.ClaimTelegramOutboundAuthorized(ctx, authority, effectID)
		claimErr <- err
	}()
	go func() {
		<-start
		_, err := st.RotateCredential(ctx, scope, secret, metadata, userID)
		rotateErr <- err
	}()
	close(start)
	if err := <-rotateErr; err != nil {
		t.Fatalf("concurrent rotation failed: %v", err)
	}
	claimedErr := <-claimErr
	if claimedErr != nil && !errors.Is(claimedErr, types.ErrConflict) {
		t.Fatalf("claim returned non-linearizable error: %v", claimedErr)
	}
	var identityStatus, routeStatus, effectStatus, effectCode string
	if err := database.QueryRowContext(ctx, `SELECT ci.status,cr.status,e.status,
		COALESCE(e.error_code,'') FROM channel_identities ci
		JOIN channel_routes cr ON cr.identity_id=ci.id
		JOIN channel_outbound_effects e ON e.route_id=cr.id
		WHERE ci.id=$1 AND e.effect_id=$2`, identityID, effectID).Scan(
		&identityStatus, &routeStatus, &effectStatus, &effectCode); err != nil {
		t.Fatal(err)
	}
	if identityStatus != "revoked" || routeStatus != "revoked" {
		t.Fatalf("rotation did not atomically revoke identity/route: %s/%s",
			identityStatus, routeStatus)
	}
	if claimedErr == nil {
		if effectStatus != "sending" || effectCode != "" {
			t.Fatalf("claim-first effect=%s/%s", effectStatus, effectCode)
		}
	} else if effectStatus != "failed" || effectCode != "credential_rotated" {
		t.Fatalf("rotation-first effect=%s/%s claim=%v",
			effectStatus, effectCode, claimedErr)
	}
}

func TestMigration146CredentialExternalIdentityBoundary(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/146_credential_external_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"ADD COLUMN external_identity TEXT",
		"uq_credential_vault_active_external_identity",
		"metadata->>'bot_id'",
		"metadata->>'app_id'",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 146 lost boundary %q", fragment)
		}
	}
}
