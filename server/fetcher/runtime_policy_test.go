package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
)

func TestRuntimeFetchRegistryRoutesExactExaGeneration(t *testing.T) {
	server1, calls1 := exaRuntimeServer(t, "exa-key-1")
	server2, calls2 := exaRuntimeServer(t, "exa-key-2")

	exa1 := NewExa(config.FetchConfig{ExaAPIKey: "exa-key-1"}, nil)
	exa1.searchURL = server1.URL
	exa2 := NewExa(config.FetchConfig{ExaAPIKey: "exa-key-2"}, nil)
	exa2.searchURL = server2.URL
	resolver := mustRuntimeFetchResolver(t,
		RuntimeFetchRouteV1{Capability: exaRuntimeCapability(types.CapSearch, 1), ExaSearch: exa1},
		RuntimeFetchRouteV1{Capability: exaRuntimeCapability(types.CapSearch, 2), ExaSearch: exa2},
	)
	multi := &Multi{runtimeV1: resolver}
	source := exaSrc(9, `{"query":"retained route"}`)

	for _, generation := range []int64{1, 2} {
		if _, err := multi.FetchWithPolicyV1(
			t.Context(), source, exaRuntimeCapability(types.CapSearch, generation), nil,
		); err != nil {
			t.Fatalf("generation %d: FetchWithPolicyV1() error = %v", generation, err)
		}
	}
	if got := calls1.Load(); got != 1 {
		t.Fatalf("generation 1 endpoint calls = %d, want 1", got)
	}
	if got := calls2.Load(); got != 1 {
		t.Fatalf("generation 2 endpoint calls = %d, want 1", got)
	}
}

func TestNewMultiWithRuntimeRoutesRetainsOldGeneration(t *testing.T) {
	server1, calls1 := exaRuntimeServer(t, "exa-key-1")
	server2, calls2 := exaRuntimeServer(t, "exa-key-2")
	oldRoutes, err := NewRuntimeFetchRoutesV1(config.FetchConfig{
		ExaAPIKey: "exa-key-1", TikhubAPIKey: "tikhub-key-1",
		CompiledExaCredentialGeneration: 1, CompiledTikHubCredentialGeneration: 1,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range oldRoutes {
		if oldRoutes[i].ExaSearch != nil {
			oldRoutes[i].ExaSearch.searchURL = server1.URL
		}
	}
	multi, err := NewMultiWithRuntimeRoutesV1(config.FetchConfig{
		ExaAPIKey: "exa-key-2", TikhubAPIKey: "tikhub-key-2",
		CompiledExaCredentialGeneration: 2, CompiledTikHubCredentialGeneration: 2,
	}, nil, nil, oldRoutes...)
	if err != nil {
		t.Fatal(err)
	}
	multi.exa.searchURL = server2.URL
	source := exaSrc(9, `{"query":"retained across rotation"}`)
	for _, generation := range []int64{1, 2} {
		if _, err := multi.FetchWithPolicyV1(
			t.Context(), source, exaRuntimeCapability(types.CapSearch, generation), nil,
		); err != nil {
			t.Fatalf("generation %d: FetchWithPolicyV1() error = %v", generation, err)
		}
	}
	if calls1.Load() != 1 || calls2.Load() != 1 {
		t.Fatalf("retained/current endpoint calls = (%d, %d), want (1, 1)",
			calls1.Load(), calls2.Load())
	}
}

func TestRuntimeFetchRegistryRoutesExactTikHubGeneration(t *testing.T) {
	server1, calls1 := tikHubRuntimeServer(t, "tikhub-key-1")
	server2, calls2 := tikHubRuntimeServer(t, "tikhub-key-2")
	binding1 := NewBinding(
		config.FetchConfig{TikhubAPIKey: "tikhub-key-1"}, nil, nil,
		tikhubinvoke.WithBaseURL(server1.URL),
	)
	binding2 := NewBinding(
		config.FetchConfig{TikhubAPIKey: "tikhub-key-2"}, nil, nil,
		tikhubinvoke.WithBaseURL(server2.URL),
	)
	resolver := mustRuntimeFetchResolver(t,
		RuntimeFetchRouteV1{
			Capability: bindingRuntimeCapability(types.PlatformXHS, types.CapSearch, 1),
			Binding:    binding1,
		},
		RuntimeFetchRouteV1{
			Capability: bindingRuntimeCapability(types.PlatformXHS, types.CapSearch, 2),
			Binding:    binding2,
		},
	)
	multi := &Multi{runtimeV1: resolver}
	source := types.FetchTarget{
		ID: 7, Platform: types.PlatformXHS, Capability: types.CapSearch,
		Config: json.RawMessage(`{"keyword":"vane"}`),
	}

	for _, generation := range []int64{1, 2} {
		if _, err := multi.FetchWithPolicyV1(t.Context(), source,
			bindingRuntimeCapability(types.PlatformXHS, types.CapSearch, generation), nil); err != nil {
			t.Fatalf("generation %d: FetchWithPolicyV1() error = %v", generation, err)
		}
	}
	if got := calls1.Load(); got != 1 {
		t.Fatalf("generation 1 endpoint calls = %d, want 1", got)
	}
	if got := calls2.Load(); got != 1 {
		t.Fatalf("generation 2 endpoint calls = %d, want 1", got)
	}
}

func TestRuntimeFetchRegistryFreezesBindingRouteAndOmitsSourceAttribution(
	t *testing.T,
) {
	server, calls := tikHubRuntimeServer(t, "tikhub-key-1")
	recorder := &fakeRecorder{}
	binding := NewBinding(
		config.FetchConfig{TikhubAPIKey: "tikhub-key-1"}, nil, recorder,
		tikhubinvoke.WithBaseURL(server.URL),
	)
	capability := bindingRuntimeCapability(
		types.PlatformXHS, types.CapSearch, 1)
	resolver := mustRuntimeFetchResolver(t, RuntimeFetchRouteV1{
		Capability: capability, Binding: binding,
	})

	key := bindingKey{types.PlatformXHS, types.CapSearch}
	originalTemplate := bindingTemplatesV1[key]
	originalCatalog := retainedBindingCatalogV1[originalTemplate.Endpoint]
	t.Cleanup(func() {
		bindingTemplatesV1[key] = originalTemplate
		retainedBindingCatalogV1[originalTemplate.Endpoint] = originalCatalog
	})
	mutatedTemplate := originalTemplate
	mutatedTemplate.Endpoint = "must_not_be_consulted_after_resolution"
	bindingTemplatesV1[key] = mutatedTemplate
	mutatedCatalog := originalCatalog
	mutatedCatalog.Path = "/must-not-be-consulted-after-resolution"
	retainedBindingCatalogV1[originalTemplate.Endpoint] = mutatedCatalog

	multi := &Multi{runtimeV1: resolver}
	items, err := multi.FetchWithPolicyV1(
		t.Context(),
		types.FetchTarget{
			Platform: types.PlatformXHS, Capability: types.CapSearch,
			Config: json.RawMessage(`{"keyword":"vane"}`),
		},
		capability,
		nil,
	)
	if err != nil || len(items) == 0 {
		t.Fatalf("frozen Binding route failed: items=%d err=%v",
			len(items), err)
	}
	if calls.Load() != 1 {
		t.Fatalf("frozen Binding route calls=%d, want 1", calls.Load())
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.rows) != 1 || recorder.rows[0].SourceID != nil {
		t.Fatalf("Source-free Binding attribution=%+v, want nil source_id",
			recorder.rows)
	}
}

func TestRuntimeFetchRegistryRSSFreezesExaEnrichmentGeneration(t *testing.T) {
	server1, calls1 := exaContentsRuntimeServer(t, "exa-key-1")
	server2, calls2 := exaContentsRuntimeServer(t, "exa-key-2")
	feedURL := serveFeed(t, hnLikeFeed(1))
	source := feedSource(feedURL)

	rss1 := newTestFetcher()
	rss1.seen = &enrichSeen{}
	contents1 := NewExaContents(config.FetchConfig{ExaAPIKey: "exa-key-1"}, nil)
	contents1.contentURL = server1.URL
	rss1.enricher = contents1
	rss2 := newTestFetcher()
	rss2.seen = &enrichSeen{}
	contents2 := NewExaContents(config.FetchConfig{ExaAPIKey: "exa-key-2"}, nil)
	contents2.contentURL = server2.URL
	rss2.enricher = contents2

	resolver := mustRuntimeFetchResolver(t,
		RuntimeFetchRouteV1{Capability: rssRuntimeCapability(1), RSS: rss1},
		RuntimeFetchRouteV1{Capability: rssRuntimeCapability(2), RSS: rss2},
	)
	multi := &Multi{runtimeV1: resolver}
	for _, generation := range []int64{1, 2} {
		items, err := multi.FetchWithPolicyV1(
			t.Context(), source, rssRuntimeCapability(generation), nil,
		)
		if err != nil {
			t.Fatalf("generation %d: FetchWithPolicyV1() error = %v", generation, err)
		}
		if len(items) != 1 {
			t.Fatalf("generation %d items = %d, want 1", generation, len(items))
		}
	}
	if got := calls1.Load(); got != 1 {
		t.Fatalf("RSS dependency generation 1 calls = %d, want 1", got)
	}
	if got := calls2.Load(); got != 1 {
		t.Fatalf("RSS dependency generation 2 calls = %d, want 1", got)
	}
}

func TestNewMultiWithOnlyGenerationTwoNeverFallsBackToCurrent(t *testing.T) {
	server, calls := exaRuntimeServer(t, "exa-key-2")
	multi := NewMulti(config.FetchConfig{
		ExaAPIKey: "exa-key-2", CompiledExaCredentialGeneration: 2,
		CompiledTikHubCredentialGeneration: 2,
	}, nil, nil)
	// NewMulti registers this exact client in the compiled registry.
	multi.exa.searchURL = server.URL
	source := exaSrc(9, `{"query":"no fallback"}`)

	_, err := multi.FetchWithPolicyV1(
		t.Context(), source, exaRuntimeCapability(types.CapSearch, 1), nil,
	)
	if err == nil {
		t.Fatal("missing generation 1 must fail closed")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("missing generation reached current endpoint %d times", got)
	}
	if _, err := multi.FetchWithPolicyV1(
		t.Context(), source, exaRuntimeCapability(types.CapSearch, 2), nil,
	); err != nil {
		t.Fatalf("registered generation 2 failed: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("registered generation endpoint calls = %d, want 1", got)
	}
}

func TestValidateRuntimeFetchRouteV1UsesExactRegistryWithoutNetwork(t *testing.T) {
	server, calls := exaRuntimeServer(t, "exa-key-2")
	multi := NewMulti(config.FetchConfig{
		ExaAPIKey: "exa-key-2", TikhubAPIKey: "tikhub-key-2",
		CompiledExaCredentialGeneration: 2, CompiledTikHubCredentialGeneration: 2,
	}, nil, nil)
	multi.exa.searchURL = server.URL

	tests := []struct {
		name       string
		source     types.FetchTarget
		capability runtimepolicy.CapabilityV1
		wantErr    bool
	}{
		{
			name: "current Exa generation",
			source: types.FetchTarget{
				Platform: types.PlatformWeb, Capability: types.CapSearch,
			},
			capability: exaRuntimeCapability(types.CapSearch, 2),
		},
		{
			name: "missing Exa generation",
			source: types.FetchTarget{
				Platform: types.PlatformWeb, Capability: types.CapSearch,
			},
			capability: exaRuntimeCapability(types.CapSearch, 1),
			wantErr:    true,
		},
		{
			name: "current RSS dependency generation",
			source: types.FetchTarget{
				Platform: types.PlatformWeb, Capability: types.CapFeed,
			},
			capability: rssRuntimeCapability(2),
		},
		{
			name: "missing RSS dependency generation",
			source: types.FetchTarget{
				Platform: types.PlatformWeb, Capability: types.CapFeed,
			},
			capability: rssRuntimeCapability(1),
			wantErr:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := multi.ValidateRuntimeFetchRouteV1(test.capability, test.source)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateRuntimeFetchRouteV1() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("pure route validation reached network %d times", got)
	}
}

func TestRuntimeFetchRegistryRejectsWrongShapeBeforeNetwork(t *testing.T) {
	server, calls := exaRuntimeServer(t, "exa-key-1")
	exa := NewExa(config.FetchConfig{ExaAPIKey: "exa-key-1"}, nil)
	exa.searchURL = server.URL
	resolver := mustRuntimeFetchResolver(t, RuntimeFetchRouteV1{
		Capability: exaRuntimeCapability(types.CapSearch, 1), ExaSearch: exa,
	})
	multi := &Multi{runtimeV1: resolver}
	source := exaSrc(9, `{"query":"must not run"}`)

	tests := []struct {
		name   string
		mutate func(*runtimepolicy.CapabilityV1)
	}{
		{name: "wrong kind", mutate: func(capability *runtimepolicy.CapabilityV1) {
			capability.Kind = string(types.KindPageContent)
		}},
		{name: "wrong credential purpose", mutate: func(capability *runtimepolicy.CapabilityV1) {
			capability.CredentialRef.ID = runtimepolicy.CredentialIDTikHubPrimaryV1
		}},
		{name: "wrong implementation", mutate: func(capability *runtimepolicy.CapabilityV1) {
			capability.ImplementationVersion = runtimepolicy.CapabilityImplementationBindingV1
			capability.CredentialRef.ID = runtimepolicy.CredentialIDTikHubPrimaryV1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capability := exaRuntimeCapability(types.CapSearch, 1)
			test.mutate(&capability)
			if _, err := multi.FetchWithPolicyV1(t.Context(), source, capability, nil); err == nil {
				t.Fatal("invalid route shape must fail")
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid route shapes reached network %d times", got)
	}
}

func TestRuntimeFetchResolverRejectsDuplicateAndMismatchedExecutor(t *testing.T) {
	capability := exaRuntimeCapability(types.CapSearch, 1)
	exa := NewExa(config.FetchConfig{ExaAPIKey: "key"}, nil)
	route := RuntimeFetchRouteV1{Capability: capability, ExaSearch: exa}
	if _, err := NewRuntimeFetchResolverV1(route, route); err == nil {
		t.Fatal("duplicate retained route must be rejected")
	}
	if _, err := NewRuntimeFetchResolverV1(RuntimeFetchRouteV1{
		Capability: capability, ExaContents: NewExaContents(config.FetchConfig{ExaAPIKey: "key"}, nil),
	}); err == nil {
		t.Fatal("executor/capability mismatch must be rejected")
	}
}

func TestValidateRuntimeCapabilityV1AllowsAnyPositiveRetainedGeneration(t *testing.T) {
	for _, generation := range []int64{1, 2, 99} {
		source := types.FetchTarget{Platform: types.PlatformWeb, Capability: types.CapSearch}
		if err := ValidateRuntimeCapabilityV1(
			exaRuntimeCapability(types.CapSearch, generation), source,
		); err != nil {
			t.Fatalf("generation %d shape rejected: %v", generation, err)
		}
	}

	missingDependency := rssRuntimeCapability(1)
	missingDependency.DependencyCredentialRefs = nil
	if err := ValidateRuntimeCapabilityV1(missingDependency, types.FetchTarget{
		Platform: types.PlatformWeb, Capability: types.CapFeed,
	}); err == nil {
		t.Fatal("RSS without frozen Exa dependency must be rejected")
	}
	sourceMismatch := exaRuntimeCapability(types.CapSearch, 1)
	if err := ValidateRuntimeCapabilityV1(sourceMismatch, types.FetchTarget{
		Platform: types.PlatformWeb, Capability: types.CapContents,
	}); err == nil {
		t.Fatal("source mismatch must be rejected")
	}
}

func TestMultiFetchWithPolicyV1FailsBeforeNetworkWithoutRegistry(t *testing.T) {
	source := types.FetchTarget{
		ID: 7, Platform: types.PlatformWeb, Capability: types.CapFeed,
		URL: "https://must-not-be-called.invalid/feed",
	}
	if _, err := (&Multi{}).FetchWithPolicyV1(
		context.Background(), source, rssRuntimeCapability(1), nil,
	); err == nil {
		t.Fatal("missing retained registry must fail closed")
	}
}

func mustRuntimeFetchResolver(
	t *testing.T,
	routes ...RuntimeFetchRouteV1,
) *RuntimeFetchResolverV1 {
	t.Helper()
	resolver, err := NewRuntimeFetchResolverV1(routes...)
	if err != nil {
		t.Fatalf("NewRuntimeFetchResolverV1() error = %v", err)
	}
	return resolver
}

func exaRuntimeCapability(
	capability types.Capability,
	generation int64,
) runtimepolicy.CapabilityV1 {
	kind := types.KindArticle
	if capability == types.CapContents {
		kind = types.KindPageContent
	}
	return runtimepolicy.CapabilityV1{
		Platform: string(types.PlatformWeb), Capability: string(capability), Kind: string(kind),
		ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: generation,
		},
	}
}

func rssRuntimeCapability(exaGeneration int64) runtimepolicy.CapabilityV1 {
	return runtimepolicy.CapabilityV1{
		Platform: string(types.PlatformWeb), Capability: string(types.CapFeed),
		Kind:                  string(types.KindArticle),
		ImplementationVersion: runtimepolicy.CapabilityImplementationRSSV1,
		DependencyCredentialRefs: []runtimepolicy.CredentialRefV1{{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: exaGeneration,
		}},
	}
}

func bindingRuntimeCapability(
	platform types.Platform,
	capability types.Capability,
	generation int64,
) runtimepolicy.CapabilityV1 {
	return runtimepolicy.CapabilityV1{
		Platform: string(platform), Capability: string(capability), Kind: string(types.KindArticle),
		ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDTikHubPrimaryV1, Generation: generation,
		},
	}
}

func exaRuntimeServer(t *testing.T, wantKey string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if got := request.Header.Get("x-api-key"); got != wantKey {
			t.Errorf("x-api-key = %q, want %q", got, wantKey)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sampleExaResponse))
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

func tikHubRuntimeServer(t *testing.T, wantKey string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if got := request.Header.Get("Authorization"); got != "Bearer "+wantKey {
			t.Errorf("Authorization = %q, want Bearer credential generation", got)
		}
		if request.URL.Path != pathSearch {
			t.Errorf("path = %q, want %q", request.URL.Path, pathSearch)
		}
		_, _ = w.Write([]byte(sampleTikhubResponse))
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

func exaContentsRuntimeServer(t *testing.T, wantKey string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if got := request.Header.Get("x-api-key"); got != wantKey {
			t.Errorf("x-api-key = %q, want %q", got, wantKey)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w,
			`{"results":[{"id":"id","url":"https://example.com/a/0","title":"Title","text":%q}],"statuses":[{"status":"success","source":"crawled"}]}`,
			longBody,
		)
	}))
	t.Cleanup(server.Close)
	return server, &calls
}
