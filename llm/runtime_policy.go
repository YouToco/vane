package llm

import (
	"fmt"

	"github.com/YouToco/vane/runtimepolicy"
)

type runtimeModelRouteKeyV1 struct {
	provider   runtimepolicy.ModelProviderIDV1
	endpoint   runtimepolicy.EndpointRefV1
	credential runtimepolicy.CredentialRefV1
}

// RuntimeModelRouteV1 binds opaque, non-secret snapshot references to one
// concrete retained client. Rotations register a new generation instead of
// changing the meaning of an existing key.
type RuntimeModelRouteV1 struct {
	Provider      runtimepolicy.ModelProviderIDV1
	Endpoint      runtimepolicy.EndpointRefV1
	CredentialRef runtimepolicy.CredentialRefV1
	Client        *Client
}

// RuntimeModelResolverV1 is immutable after construction and therefore safe
// for concurrent Activities. It never falls back to a "current" client.
type RuntimeModelResolverV1 struct {
	routes map[runtimeModelRouteKeyV1]*Client
}

func NewRuntimeModelResolverV1(routes ...RuntimeModelRouteV1) (*RuntimeModelResolverV1, error) {
	resolver := &RuntimeModelResolverV1{
		routes: make(map[runtimeModelRouteKeyV1]*Client, len(routes)),
	}
	for _, route := range routes {
		key := runtimeModelRouteKeyV1{
			provider: route.Provider, endpoint: route.Endpoint,
			credential: route.CredentialRef,
		}
		if route.Client == nil || route.Provider == "" ||
			route.Endpoint.ID == "" || route.Endpoint.Generation <= 0 ||
			route.CredentialRef.ID == "" || route.CredentialRef.Generation <= 0 {
			return nil, fmt.Errorf("llm: invalid retained runtime model route")
		}
		if _, exists := resolver.routes[key]; exists {
			return nil, fmt.Errorf("llm: duplicate retained runtime model route")
		}
		resolver.routes[key] = route.Client
	}
	return resolver, nil
}

// ResolveRuntimeModelPolicyV1 returns the executor named by the exact frozen
// provider/endpoint/credential generations. A missing historical generation is
// an error before any network operation; the current client is never used as a
// compatibility fallback.
func (r *RuntimeModelResolverV1) ResolveRuntimeModelPolicyV1(
	policy runtimepolicy.ModelPolicyV1,
) (*Client, error) {
	if r == nil {
		return nil, fmt.Errorf("llm: runtime model resolver is nil")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	client := r.routes[runtimeModelRouteKeyV1{
		provider: policy.Provider, endpoint: policy.Endpoint,
		credential: policy.CredentialRef,
	}]
	if client == nil {
		return nil, fmt.Errorf("llm: retained runtime model route is unavailable")
	}
	return client, nil
}

// ValidateRuntimeModelPolicyV1 resolves the controlled V1 route to this
// client. Endpoint URLs and credential values remain private client fields;
// the snapshot contains only purpose-bound aliases and generations.
func (c *Client) ValidateRuntimeModelPolicyV1(policy runtimepolicy.ModelPolicyV1) error {
	if c == nil {
		return fmt.Errorf("llm: runtime model client is nil")
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	if string(policy.Provider) != c.provider ||
		policy.Endpoint.ID != runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1 ||
		policy.Endpoint.Generation != runtimepolicy.PrimaryGenerationV1 ||
		policy.CredentialRef.ID != runtimepolicy.CredentialIDLLMPrimaryV1 ||
		policy.CredentialRef.Generation != runtimepolicy.PrimaryGenerationV1 {
		return fmt.Errorf("llm: runtime model route is not available")
	}
	return nil
}
