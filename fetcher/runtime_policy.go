package fetcher

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/types"
)

type runtimeFetchExecutorKindV1 uint8

const (
	runtimeFetchExecutorRSSV1 runtimeFetchExecutorKindV1 = iota + 1
	runtimeFetchExecutorExaSearchV1
	runtimeFetchExecutorExaContentsV1
	runtimeFetchExecutorProductStatusV1
	runtimeFetchExecutorBindingV1
)

type runtimeFetchCapabilityShapeV1 struct {
	kind           types.Kind
	implementation runtimepolicy.CapabilityImplementationIDV1
	executor       runtimeFetchExecutorKindV1
}

type runtimeFetchRouteKeyV1 struct {
	platform             string
	capability           string
	kind                 string
	implementation       runtimepolicy.CapabilityImplementationIDV1
	credential           runtimepolicy.CredentialRefV1
	dependencyCredential runtimepolicy.CredentialRefV1
}

// RuntimeFetchRouteV1 binds one exact, non-secret snapshot route to a concrete
// retained executor. Exactly one executor field must be set and must match the
// capability. Rotations add routes; they never change an existing key's
// meaning.
type RuntimeFetchRouteV1 struct {
	Capability    runtimepolicy.CapabilityV1
	RSS           *Fetcher
	ExaSearch     *ExaFetcher
	ExaContents   *ExaContentsFetcher
	ProductStatus *ProductStatusFetcher
	Binding       *BindingFetcher
}

type runtimeFetchExecutorV1 struct {
	kind               runtimeFetchExecutorKindV1
	rss                *Fetcher
	exaSearch          *ExaFetcher
	exaContents        *ExaContentsFetcher
	productStatus      *ProductStatusFetcher
	binding            *BindingFetcher
	bindingSpec        bindingSpec
	bindingEntry       tikhubcatalog.Entry
	bindingEnrichEntry *tikhubcatalog.Entry
}

// RuntimeFetchResolverV1 is immutable after construction and safe for
// concurrent Activities. Resolution is exact across platform, capability,
// output kind, implementation version, primary credential generation, and
// the RSS Exa-enrichment credential generation.
type RuntimeFetchResolverV1 struct {
	routes map[runtimeFetchRouteKeyV1]runtimeFetchExecutorV1
}

// NewRuntimeFetchResolverV1 builds an exact retained-route registry. It
// rejects duplicate keys and executor/capability mismatches during process
// composition rather than waiting for the first scheduled run.
func NewRuntimeFetchResolverV1(
	routes ...RuntimeFetchRouteV1,
) (*RuntimeFetchResolverV1, error) {
	resolver := &RuntimeFetchResolverV1{
		routes: make(map[runtimeFetchRouteKeyV1]runtimeFetchExecutorV1, len(routes)),
	}
	for _, route := range routes {
		source := types.FetchTarget{
			Platform:   types.Platform(route.Capability.Platform),
			Capability: types.Capability(route.Capability.Capability),
		}
		if err := ValidateRuntimeCapabilityV1(route.Capability, source); err != nil {
			return nil, fmt.Errorf("fetcher: invalid retained runtime route: %w", err)
		}
		shape, _ := runtimeFetchShapeV1(source.Platform, source.Capability)
		executor, err := route.executorV1(shape.executor)
		if err != nil {
			return nil, err
		}
		key := runtimeFetchKeyV1(route.Capability)
		if _, duplicate := resolver.routes[key]; duplicate {
			return nil, fmt.Errorf("fetcher: duplicate retained runtime route")
		}
		resolver.routes[key] = executor
	}
	return resolver, nil
}

func (route RuntimeFetchRouteV1) executorV1(
	want runtimeFetchExecutorKindV1,
) (runtimeFetchExecutorV1, error) {
	count := 0
	if route.RSS != nil {
		count++
	}
	if route.ExaSearch != nil {
		count++
	}
	if route.ExaContents != nil {
		count++
	}
	if route.ProductStatus != nil {
		count++
	}
	if route.Binding != nil {
		count++
	}
	if count != 1 {
		return runtimeFetchExecutorV1{}, fmt.Errorf(
			"fetcher: retained runtime route must have exactly one executor")
	}
	executor := runtimeFetchExecutorV1{
		kind: want, rss: route.RSS, exaSearch: route.ExaSearch,
		exaContents: route.ExaContents, productStatus: route.ProductStatus,
		binding: route.Binding,
	}
	valid := (want == runtimeFetchExecutorRSSV1 && route.RSS != nil) ||
		(want == runtimeFetchExecutorExaSearchV1 && route.ExaSearch != nil) ||
		(want == runtimeFetchExecutorExaContentsV1 && route.ExaContents != nil) ||
		(want == runtimeFetchExecutorProductStatusV1 && route.ProductStatus != nil) ||
		(want == runtimeFetchExecutorBindingV1 && route.Binding != nil)
	if !valid {
		return runtimeFetchExecutorV1{}, fmt.Errorf(
			"fetcher: retained runtime executor does not match capability")
	}
	if want == runtimeFetchExecutorBindingV1 {
		retained, err := resolveRetainedBindingRouteV1(
			types.Platform(route.Capability.Platform),
			types.Capability(route.Capability.Capability))
		if err != nil {
			return runtimeFetchExecutorV1{}, err
		}
		executor.bindingSpec = retained.spec
		executor.bindingEntry = retained.entry
		executor.bindingEnrichEntry = retained.enrichEntry
	}
	return executor, nil
}

func (r *RuntimeFetchResolverV1) resolve(
	capability runtimepolicy.CapabilityV1,
) (runtimeFetchExecutorV1, bool) {
	if r == nil {
		return runtimeFetchExecutorV1{}, false
	}
	executor, ok := r.routes[runtimeFetchKeyV1(capability)]
	return executor, ok
}

func (executor runtimeFetchExecutorV1) fetch(
	ctx context.Context,
	source types.FetchTarget,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	switch executor.kind {
	case runtimeFetchExecutorRSSV1:
		return executor.rss.fetchRSSWithEffectGate(
			ctx, source, enrichMaxPerRound, beforeEffect)
	case runtimeFetchExecutorExaSearchV1:
		return executor.exaSearch.fetchWithEffectGate(ctx, source, beforeEffect)
	case runtimeFetchExecutorExaContentsV1:
		return executor.exaContents.fetchWithEffectGate(ctx, source, beforeEffect)
	case runtimeFetchExecutorProductStatusV1:
		return executor.productStatus.fetchWithEffectGate(ctx, source, beforeEffect)
	case runtimeFetchExecutorBindingV1:
		return executor.binding.fetchWithRetainedRouteV1(
			ctx, source, executor.bindingSpec, executor.bindingEntry,
			executor.bindingEnrichEntry, beforeEffect)
	default:
		return nil, types.NewAppError(types.CodeInternal,
			"compiled fetch capability v1 has an invalid retained executor", nil)
	}
}

// ValidateRuntimeFetchRouteV1 performs the same exact registry resolution as
// FetchWithPolicyV1 without running an executor or making a network call.
func (m *Multi) ValidateRuntimeFetchRouteV1(
	capability runtimepolicy.CapabilityV1,
	source types.FetchTarget,
) error {
	if m == nil {
		return types.NewAppError(types.CodeInternal,
			"compiled fetcher v1 is not configured", nil)
	}
	if err := ValidateRuntimeCapabilityV1(capability, source); err != nil {
		return types.NewAppError(types.CodeValidation,
			"compiled fetch capability v1 is invalid", err)
	}
	if m.runtimeV1Err != nil {
		return types.NewAppError(types.CodeInternal,
			"compiled fetch route registry is invalid", m.runtimeV1Err)
	}
	if _, ok := m.runtimeV1.resolve(capability); !ok {
		return types.NewAppError(types.CodeValidation,
			"compiled fetch capability v1 has no retained executor", nil)
	}
	return nil
}

// FetchWithPolicyV1 executes through the exact retained V1 route. It never
// consults the mutable current source catalog and never falls back to the
// legacy/current executor when an old generation is absent.
func (m *Multi) FetchWithPolicyV1(
	ctx context.Context,
	source types.FetchTarget,
	capability runtimepolicy.CapabilityV1,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	if err := m.ValidateRuntimeFetchRouteV1(capability, source); err != nil {
		return nil, err
	}
	executor, ok := m.runtimeV1.resolve(capability)
	if !ok { // Validation above resolves the same immutable map.
		return nil, types.NewAppError(types.CodeInternal,
			"compiled fetch route registry changed after validation", nil)
	}
	return executor.fetch(ctx, source, beforeEffect)
}

// ValidateRuntimeCapabilityV1 verifies the immutable route shape and its
// match to source. Credential generations may be any positive value here;
// availability of that exact generation is decided only by the resolver.
func ValidateRuntimeCapabilityV1(
	capability runtimepolicy.CapabilityV1,
	source types.FetchTarget,
) error {
	policy := runtimepolicy.CapabilityCatalogV1{
		SchemaVersion: runtimepolicy.CapabilityCatalogSchemaVersionV1,
		Allowed:       []runtimepolicy.CapabilityV1{capability},
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	if capability.Platform != string(source.Platform) ||
		capability.Capability != string(source.Capability) {
		return fmt.Errorf("fetcher: frozen capability does not match source")
	}
	shape, ok := runtimeFetchShapeV1(source.Platform, source.Capability)
	if !ok {
		return fmt.Errorf("fetcher: frozen capability has no retained v1 implementation")
	}
	if capability.Kind != string(shape.kind) ||
		capability.ImplementationVersion != shape.implementation {
		return fmt.Errorf("fetcher: frozen capability route shape is invalid")
	}
	return nil
}

func runtimeFetchKeyV1(capability runtimepolicy.CapabilityV1) runtimeFetchRouteKeyV1 {
	key := runtimeFetchRouteKeyV1{
		platform: capability.Platform, capability: capability.Capability,
		kind: capability.Kind, implementation: capability.ImplementationVersion,
		credential: capability.CredentialRef,
	}
	if len(capability.DependencyCredentialRefs) == 1 {
		key.dependencyCredential = capability.DependencyCredentialRefs[0]
	}
	return key
}

func runtimeFetchShapeV1(
	platform types.Platform,
	capability types.Capability,
) (runtimeFetchCapabilityShapeV1, bool) {
	switch {
	case platform == types.PlatformWeb && capability == types.CapFeed:
		return runtimeFetchCapabilityShapeV1{
			kind:           types.KindArticle,
			implementation: runtimepolicy.CapabilityImplementationRSSV1,
			executor:       runtimeFetchExecutorRSSV1,
		}, true
	case platform == types.PlatformWeb && capability == types.CapSearch:
		return runtimeFetchCapabilityShapeV1{
			kind:           types.KindArticle,
			implementation: runtimepolicy.CapabilityImplementationExaV1,
			executor:       runtimeFetchExecutorExaSearchV1,
		}, true
	case platform == types.PlatformWeb && capability == types.CapContents:
		return runtimeFetchCapabilityShapeV1{
			kind:           types.KindPageContent,
			implementation: runtimepolicy.CapabilityImplementationExaV1,
			executor:       runtimeFetchExecutorExaContentsV1,
		}, true
	case platform == types.PlatformWeb && capability == types.CapProductStatus:
		return runtimeFetchCapabilityShapeV1{
			kind:           types.KindPageContent,
			implementation: runtimepolicy.CapabilityImplementationProductStatusV1,
			executor:       runtimeFetchExecutorProductStatusV1,
		}, true
	case IsBindingBacked(platform, capability):
		return runtimeFetchCapabilityShapeV1{
			kind:           types.KindArticle,
			implementation: runtimepolicy.CapabilityImplementationBindingV1,
			executor:       runtimeFetchExecutorBindingV1,
		}, true
	default:
		return runtimeFetchCapabilityShapeV1{}, false
	}
}
