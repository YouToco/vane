package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/fetcher"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const fetchProviderCredentialPurpose = "shared_runtime"

type storedProviderAPIKey struct {
	APIKey string `json:"api_key"`
}

type providerAPIKeyGeneration struct {
	Generation int64
	APIKey     string
}

type storedFetchCredentialSet struct {
	Current        config.FetchConfig
	RetainedExa    []config.FetchConfig
	RetainedTikHub []config.FetchConfig
}

// loadStoredFetchCredentials makes database generations authoritative without
// weakening safe migration. A scope that has never existed may still use its
// environment compatibility value. Once history exists, an absent active
// generation is a durable tombstone and startup fails instead of resurrecting
// an older VPS secret.
func loadStoredFetchCredentials(
	ctx context.Context, st *store.Store, target config.FetchConfig,
) (storedFetchCredentialSet, error) {
	next := target
	exa, exaManaged, err := loadProviderAPIKeyHistory(ctx, st, "exa")
	if err != nil {
		return storedFetchCredentialSet{}, err
	}
	tikHub, tikHubManaged, err := loadProviderAPIKeyHistory(ctx, st, "tikhub")
	if err != nil {
		return storedFetchCredentialSet{}, err
	}
	if exaManaged {
		next.ExaAPIKey = exa[0].APIKey
		next.CompiledExaCredentialGeneration = exa[0].Generation
	}
	if tikHubManaged {
		next.TikhubAPIKey = tikHub[0].APIKey
		next.CompiledTikHubCredentialGeneration = tikHub[0].Generation
	}
	result := storedFetchCredentialSet{Current: next}
	for _, generation := range exa[1:] {
		retained := next
		retained.ExaAPIKey = generation.APIKey
		retained.CompiledExaCredentialGeneration = generation.Generation
		result.RetainedExa = append(result.RetainedExa, retained)
	}
	for _, generation := range tikHub[1:] {
		retained := next
		retained.TikhubAPIKey = generation.APIKey
		retained.CompiledTikHubCredentialGeneration = generation.Generation
		result.RetainedTikHub = append(result.RetainedTikHub, retained)
	}
	return result, nil
}

func loadProviderAPIKeyHistory(
	ctx context.Context, st *store.Store, provider string,
) ([]providerAPIKeyGeneration, bool, error) {
	if st == nil {
		return nil, false, errors.New("provider credential loader is unavailable")
	}
	scope := store.CredentialScope{
		Kind: "platform", Provider: provider, Purpose: fetchProviderCredentialPurpose,
	}
	history, err := st.ListCredentialMetadata(ctx, scope)
	if errors.Is(err, types.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %s credential history: %w", provider, err)
	}
	if len(history) == 0 || history[0].Status != "active" {
		return nil, true, fmt.Errorf(
			"database %s credential has no active generation", provider)
	}
	if !st.CredentialVaultReady() {
		return nil, true, fmt.Errorf(
			"active database %s credential exists but credential vault keyring is unavailable",
			provider)
	}
	loaded := make([]providerAPIKeyGeneration, 0, len(history))
	for _, metadata := range history {
		if metadata.Status == "revoked" {
			continue
		}
		var secret storedProviderAPIKey
		if err := st.UseCredential(ctx, scope, metadata.Generation,
			func(secretJSON []byte, _ store.CredentialMetadata) error {
				return json.Unmarshal(secretJSON, &secret)
			}); err != nil {
			return nil, true, fmt.Errorf(
				"decrypt database %s credential generation %d: %w",
				provider, metadata.Generation, err)
		}
		secret.APIKey = strings.TrimSpace(secret.APIKey)
		if secret.APIKey == "" || len(secret.APIKey) > 16<<10 {
			return nil, true, fmt.Errorf(
				"database %s credential generation %d is invalid",
				provider, metadata.Generation)
		}
		loaded = append(loaded, providerAPIKeyGeneration{
			Generation: metadata.Generation, APIKey: secret.APIKey,
		})
	}
	if len(loaded) == 0 || loaded[0].Generation != history[0].Generation {
		return nil, true, fmt.Errorf(
			"database %s active credential is unreadable", provider)
	}
	return loaded, true, nil
}

func validateResolvedRuntimeCredentials(cfg *config.Config) error {
	if cfg != nil && (cfg.Pipeline.ResearchV3RuntimeEnabled ||
		cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "" ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "") &&
		strings.TrimSpace(cfg.Fetch.ExaAPIKey) == "" {
		return errors.New("Research V3 runtime requires an active Exa credential generation")
	}
	return nil
}

// buildRetainedFetchRoutes keeps immutable snapshots on their exact provider
// generation. Each retained config is filtered to the provider it actually
// owns so a historical Exa generation cannot duplicate or relabel a TikHub
// route (and vice versa).
func buildRetainedFetchRoutes(
	set storedFetchCredentialSet,
	seen fetcher.SeenChecker,
	recorder fetcher.BindingCallRecorder,
) ([]fetcher.RuntimeFetchRouteV1, error) {
	result := make([]fetcher.RuntimeFetchRouteV1, 0,
		len(set.RetainedExa)*3+len(set.RetainedTikHub)*9)
	for _, retained := range set.RetainedExa {
		routes, err := fetcher.NewRuntimeFetchRoutesV1(retained, seen, recorder)
		if err != nil {
			return nil, fmt.Errorf("build retained Exa generation %d: %w",
				retained.CompiledExaCredentialGeneration, err)
		}
		for _, route := range routes {
			if routeUsesCredential(route.Capability,
				runtimepolicy.CredentialIDExaPrimaryV1,
				retained.CompiledExaCredentialGeneration) {
				result = append(result, route)
			}
		}
	}
	for _, retained := range set.RetainedTikHub {
		routes, err := fetcher.NewRuntimeFetchRoutesV1(retained, seen, recorder)
		if err != nil {
			return nil, fmt.Errorf("build retained TikHub generation %d: %w",
				retained.CompiledTikHubCredentialGeneration, err)
		}
		for _, route := range routes {
			if routeUsesCredential(route.Capability,
				runtimepolicy.CredentialIDTikHubPrimaryV1,
				retained.CompiledTikHubCredentialGeneration) {
				result = append(result, route)
			}
		}
	}
	return result, nil
}

func routeUsesCredential(
	capability runtimepolicy.CapabilityV1,
	id runtimepolicy.CredentialIDV1,
	generation int64,
) bool {
	want := runtimepolicy.CredentialRefV1{ID: id, Generation: generation}
	if capability.CredentialRef == want {
		return true
	}
	for _, dependency := range capability.DependencyCredentialRefs {
		if dependency == want {
			return true
		}
	}
	return false
}
