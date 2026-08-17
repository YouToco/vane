package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

func TestStoredFetchCredentialOverridesEnvironmentAndFailsClosedPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.ConfigureCredentialVault("fetch-loader-test", strings.Repeat("52", 32), ""); err != nil {
		t.Fatal(err)
	}
	user, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("fetch-loader-owner-%d", time.Now().UnixNano()), "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(ctx, 1, user.ID, types.MembershipRoleOwner); err != nil {
		t.Fatal(err)
	}
	cleanup, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = cleanup.Exec(cleanupCtx, `DELETE FROM credential_vault_entries WHERE created_by_user_id=$1`, user.ID)
		_, _ = cleanup.Exec(cleanupCtx, `DELETE FROM memberships WHERE user_id=$1`, user.ID)
		_, _ = cleanup.Exec(cleanupCtx, `DELETE FROM users WHERE id=$1`, user.ID)
		cleanup.Close()
	})
	secret, _ := json.Marshal(storedFetchSecret{
		ExaAPIKey: "database-exa-key", TikHubAPIKey: "database-tikhub-key",
	})
	metadata := json.RawMessage(`{"providers":["exa","tikhub"]}`)
	rotated, err := st.RotateCredential(ctx, store.CredentialScope{
		Kind: "platform", Provider: "fetch", Purpose: databaseFetchCredentialPurpose,
	}, secret, metadata, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	target := config.FetchConfig{
		ExaAPIKey: "environment-exa-key", TikhubAPIKey: "environment-tikhub-key",
		CompiledExaCredentialGeneration: 3, CompiledTikHubCredentialGeneration: 4,
	}
	if err := applyStoredFetchCredential(ctx, st, &target); err != nil {
		t.Fatal(err)
	}
	if target.ExaAPIKey != "database-exa-key" || target.TikhubAPIKey != "database-tikhub-key" ||
		target.CompiledExaCredentialGeneration != rotated.Generation ||
		target.CompiledTikHubCredentialGeneration != rotated.Generation {
		t.Fatalf("database fetch bundle not authoritative: %+v", target)
	}
	if err := st.RevokeCredential(ctx, store.CredentialScope{
		Kind: "platform", Provider: "fetch", Purpose: databaseFetchCredentialPurpose,
	}, user.ID); err != nil {
		t.Fatal(err)
	}
	revokedFallback := config.FetchConfig{ExaAPIKey: "must-not-revive-environment"}
	if err := applyStoredFetchCredential(ctx, st, &revokedFallback); err == nil {
		t.Fatal("revoked database fetch credential silently fell back to environment")
	}
	if revokedFallback.ExaAPIKey != "must-not-revive-environment" {
		t.Fatalf("revoked load partially mutated environment config: %+v", revokedFallback)
	}
	if _, err := st.RotateCredential(ctx, store.CredentialScope{
		Kind: "platform", Provider: "fetch", Purpose: databaseFetchCredentialPurpose,
	}, secret, metadata, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cleanup.Exec(ctx, `UPDATE credential_vault_entries
		SET ciphertext=set_byte(ciphertext,0,(get_byte(ciphertext,0)+1)%256)
		WHERE scope_kind='platform' AND provider='fetch' AND purpose=$1 AND status='active'`,
		databaseFetchCredentialPurpose); err != nil {
		t.Fatal(err)
	}
	fallback := config.FetchConfig{TikhubAPIKey: "must-not-fallback"}
	if err := applyStoredFetchCredential(ctx, st, &fallback); err == nil {
		t.Fatal("tampered database fetch credential silently fell back to environment")
	}
	if fallback.TikhubAPIKey != "must-not-fallback" {
		t.Fatalf("failed load partially mutated environment config: %+v", fallback)
	}
}
