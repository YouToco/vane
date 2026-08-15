package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const databaseLLMCredentialPurpose = "shared_runtime"

type storedLLMSecret struct {
	APIKey string `json:"api_key"`
}

type storedLLMMetadata struct {
	Provider      string `json:"provider"`
	BaseURL       string `json:"base_url"`
	Model         string `json:"model"`
	AgentModel    string `json:"agent_model"`
	ResearchModel string `json:"research_model"`
	MaxConcurrent int    `json:"max_concurrent"`
}

// applyStoredLLMCredential makes the active database generation authoritative
// at startup. No row preserves the environment compatibility path; a present
// row that cannot decrypt fails startup instead of silently using an old key.
func applyStoredLLMCredential(
	ctx context.Context, st *store.Store, target *config.LLMConfig,
) error {
	if st == nil || target == nil {
		return errors.New("LLM credential loader is unavailable")
	}
	scope := store.CredentialScope{
		Kind: "platform", Provider: "llm", Purpose: databaseLLMCredentialPurpose,
	}
	metadata, err := st.ActiveCredentialMetadata(ctx, scope)
	if errors.Is(err, types.ErrNotFound) {
		latest, historyErr := st.LatestCredentialMetadata(ctx, scope)
		if errors.Is(historyErr, types.ErrNotFound) {
			return nil
		}
		if historyErr != nil {
			return fmt.Errorf("read LLM credential history: %w", historyErr)
		}
		return fmt.Errorf(
			"database LLM credential has no active generation (latest generation %d is %s)",
			latest.Generation, latest.Status)
	}
	if err != nil {
		return fmt.Errorf("read active LLM credential metadata: %w", err)
	}
	if !st.CredentialVaultReady() {
		return errors.New("active database LLM credential exists but credential vault keyring is unavailable")
	}
	var secret storedLLMSecret
	var route storedLLMMetadata
	if err := json.Unmarshal(metadata.Metadata, &route); err != nil {
		return errors.New("active database LLM credential metadata is invalid")
	}
	err = st.UseCredential(ctx, scope, metadata.Generation,
		func(secretJSON []byte, _ store.CredentialMetadata) error {
			return json.Unmarshal(secretJSON, &secret)
		})
	if err != nil {
		return fmt.Errorf("decrypt active database LLM credential: %w", err)
	}
	next := *target
	next.Provider = route.Provider
	next.BaseURL = route.BaseURL
	next.APIKey = secret.APIKey
	next.Model = route.Model
	next.AgentProvider = route.Provider
	next.AgentBaseURL = route.BaseURL
	next.AgentAPIKey = secret.APIKey
	next.AgentModel = route.AgentModel
	next.ResearchModel = route.ResearchModel
	next.MaxConcurrent = route.MaxConcurrent
	next.CompiledEndpointGeneration = metadata.Generation
	next.CompiledCredentialGeneration = metadata.Generation
	if next.Provider != "deepseek" || next.APIKey == "" || next.BaseURL == "" ||
		next.Model == "" || next.AgentModel == "" || next.ResearchModel == "" ||
		next.MaxConcurrent < 1 {
		return errors.New("active database LLM credential is incomplete or unsupported")
	}
	*target = next
	return nil
}
