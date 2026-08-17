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

const databaseFetchCredentialPurpose = "shared_runtime"

type storedFetchSecret struct {
	ExaAPIKey    string `json:"exa_api_key"`
	TikHubAPIKey string `json:"tikhub_api_key"`
}

// applyStoredFetchCredential makes a durable database generation authoritative
// at safe process start. Absence preserves the environment compatibility path;
// once any generation exists, revoke/tamper must fail closed rather than revive
// a stale VPS key.
func applyStoredFetchCredential(
	ctx context.Context, st *store.Store, target *config.FetchConfig,
) error {
	if st == nil || target == nil {
		return errors.New("fetch credential loader is unavailable")
	}
	scope := store.CredentialScope{
		Kind: "platform", Provider: "fetch", Purpose: databaseFetchCredentialPurpose,
	}
	metadata, err := st.ActiveCredentialMetadata(ctx, scope)
	if errors.Is(err, types.ErrNotFound) {
		latest, historyErr := st.LatestCredentialMetadata(ctx, scope)
		if errors.Is(historyErr, types.ErrNotFound) {
			return nil
		}
		if historyErr != nil {
			return fmt.Errorf("read fetch credential history: %w", historyErr)
		}
		return fmt.Errorf(
			"database fetch credential has no active generation (latest generation %d is %s)",
			latest.Generation, latest.Status)
	}
	if err != nil {
		return fmt.Errorf("read active fetch credential metadata: %w", err)
	}
	if !st.CredentialVaultReady() {
		return errors.New("active database fetch credential exists but credential vault keyring is unavailable")
	}
	var secret storedFetchSecret
	err = st.UseCredential(ctx, scope, metadata.Generation,
		func(secretJSON []byte, _ store.CredentialMetadata) error {
			return json.Unmarshal(secretJSON, &secret)
		})
	if err != nil {
		return fmt.Errorf("decrypt active fetch credential: %w", err)
	}
	if secret.ExaAPIKey == "" || secret.TikHubAPIKey == "" {
		return errors.New("active database fetch credential is incomplete")
	}
	next := *target
	next.ExaAPIKey = secret.ExaAPIKey
	next.TikhubAPIKey = secret.TikHubAPIKey
	next.CompiledExaCredentialGeneration = metadata.Generation
	next.CompiledTikHubCredentialGeneration = metadata.Generation
	*target = next
	return nil
}
