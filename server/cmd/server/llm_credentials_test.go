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

func TestStoredLLMCredentialOverridesEnvironmentAndTamperFailsClosedPostgres(t *testing.T) {
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
	if err := st.ConfigureCredentialVault("loader-test-key", strings.Repeat("42", 32), ""); err != nil {
		t.Fatal(err)
	}
	user, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("credential-loader-owner-%d", time.Now().UnixNano()), "owner")
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
	secret, _ := json.Marshal(storedLLMSecret{
		APIKey: "database-key", AgentAPIKey: "database-kimi-key",
	})
	metadata, _ := json.Marshal(storedLLMMetadata{
		Provider: "deepseek", BaseURL: "https://database.example",
		AgentProvider: "kimi", AgentBaseURL: "https://api.moonshot.cn/v1",
		Model: "database-pipeline", AgentModel: "database-agent",
		ResearchModel: "database-research", MaxConcurrent: 7,
	})
	rotated, err := st.RotateCredential(ctx, store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: databaseLLMCredentialPurpose,
	}, secret, metadata, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	target := config.LLMConfig{
		Provider: "deepseek", BaseURL: "https://environment.example",
		APIKey: "environment-key", Model: "environment-model",
	}
	if err := applyStoredLLMCredential(ctx, st, &target); err != nil {
		t.Fatal(err)
	}
	if target.APIKey != "database-key" || target.BaseURL != "https://database.example" ||
		target.AgentProvider != "kimi" || target.AgentBaseURL != "https://api.moonshot.cn/v1" ||
		target.AgentAPIKey != "database-kimi-key" ||
		target.Model != "database-pipeline" || target.AgentModel != "database-agent" ||
		target.ResearchModel != "database-research" || target.MaxConcurrent != 7 ||
		target.CompiledCredentialGeneration != rotated.Generation ||
		target.CompiledEndpointGeneration != rotated.Generation {
		t.Fatalf("database LLM route not authoritative: %+v", target)
	}
	if err := st.RevokeCredential(ctx, store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: databaseLLMCredentialPurpose,
	}, user.ID); err != nil {
		t.Fatal(err)
	}
	revokedFallback := config.LLMConfig{APIKey: "must-not-revive-environment"}
	if err := applyStoredLLMCredential(ctx, st, &revokedFallback); err == nil {
		t.Fatal("revoked database credential silently fell back to environment")
	}
	if revokedFallback.APIKey != "must-not-revive-environment" {
		t.Fatalf("revoked load partially mutated environment config: %+v", revokedFallback)
	}
	if _, err := st.RotateCredential(ctx, store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: databaseLLMCredentialPurpose,
	}, secret, metadata, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cleanup.Exec(ctx, `UPDATE credential_vault_entries
		SET ciphertext=set_byte(ciphertext,0,(get_byte(ciphertext,0)+1)%256)
		WHERE scope_kind='platform' AND provider='llm' AND purpose=$1 AND status='active'`,
		databaseLLMCredentialPurpose); err != nil {
		t.Fatal(err)
	}
	fallback := config.LLMConfig{APIKey: "must-not-fallback"}
	if err := applyStoredLLMCredential(ctx, st, &fallback); err == nil {
		t.Fatal("tampered database credential silently fell back to environment")
	}
	if fallback.APIKey != "must-not-fallback" {
		t.Fatalf("failed load partially mutated environment config: %+v", fallback)
	}
}
