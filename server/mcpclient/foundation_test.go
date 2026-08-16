package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"testing"

	"github.com/google/uuid"
)

type fixedResolver map[string][]netip.Addr

func (r fixedResolver) LookupNetIP(_ context.Context, _ string, host string) ([]netip.Addr, error) {
	addresses, ok := r[host]
	if !ok {
		return nil, errors.New("missing host")
	}
	return addresses, nil
}

func TestEndpointValidatorRequiresPublicHTTPSOnEveryResolution(t *testing.T) {
	validator := EndpointValidator{Resolver: fixedResolver{
		"mcp.example.com":     {netip.MustParseAddr("93.184.216.34")},
		"rebound.example.com": {netip.MustParseAddr("127.0.0.1")},
		"mixed.example.com":   {netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("10.0.0.1")},
	}}
	resolved, err := validator.Validate(t.Context(), "https://mcp.example.com/v1/mcp")
	if err != nil || resolved.Host != "mcp.example.com" || len(resolved.Addresses) != 1 {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
	for _, unsafeURL := range []string{
		"http://mcp.example.com/mcp", "https://user:secret@mcp.example.com/mcp",
		"https://mcp.example.com/mcp?token=secret", "https://127.0.0.1/mcp",
		"https://169.254.169.254/latest/meta-data", "https://[::1]/mcp",
	} {
		if _, err := validator.Validate(t.Context(), unsafeURL); err == nil {
			t.Fatalf("accepted unsafe URL %q", unsafeURL)
		}
	}
	if _, err := validator.Validate(t.Context(), "https://mixed.example.com/mcp"); !errors.Is(err, ErrUnsafeEndpoint) {
		t.Fatalf("mixed DNS error=%v", err)
	}
	if _, err := validator.ValidateRedirect(t.Context(), "https://rebound.example.com/mcp"); !errors.Is(err, ErrUnsafeEndpoint) {
		t.Fatalf("rebind redirect error=%v", err)
	}
}

func TestValidateTransportRejectsDownloadedAndLegacyRuntimes(t *testing.T) {
	for _, transport := range []string{"stdio", "sse", "local", "docker"} {
		if err := ValidateTransport(transport, ProtocolVersion20251125); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("transport %q error=%v", transport, err)
		}
	}
	if err := ValidateTransport(TransportStreamableHTTP, ProtocolVersion20251125); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTransport(TransportStreamableHTTP, "2024-11-05"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("legacy protocol error=%v", err)
	}
}

func TestFreezeReadOnlyToolsUsesOnlyLocalAuthorityAndCanonicalSchemas(t *testing.T) {
	remote := []RemoteTool{
		{Name: "z.read", Description: "untrusted Z", InputSchema: json.RawMessage(`{"properties":{"q":{"type":"string"}},"type":"object"}`), Annotations: json.RawMessage(`{"readOnlyHint":false}`)},
		{Name: "a.read", Description: "untrusted A", InputSchema: json.RawMessage(`{ "type" : "object" }`), Annotations: json.RawMessage(`{"readOnlyHint":true}`)},
	}
	policies := map[string]LocalToolPolicy{
		"a.read": {ReadOnly: true, Budget: 2}, "z.read": {ReadOnly: true, Budget: 1},
	}
	first, err := FreezeReadOnlyTools(remote, policies)
	if err != nil {
		t.Fatal(err)
	}
	second, err := FreezeReadOnlyTools([]RemoteTool{remote[1], remote[0]}, policies)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || string(first.Payload) != string(second.Payload) ||
		first.Tools[0].Name != "a.read" || len(first.Digest) != 64 {
		t.Fatalf("catalogs differ first=%s second=%s tools=%+v", first.Digest, second.Digest, first.Tools)
	}
	if string(first.Tools[0].InputSchema) != `{"type":"object"}` {
		t.Fatalf("schema not canonical: %s", first.Tools[0].InputSchema)
	}

	remoteOnly := map[string]LocalToolPolicy{"z.read": {ReadOnly: true, Budget: 1}}
	if _, err := FreezeReadOnlyTools(remote, remoteOnly); !errors.Is(err, ErrUnsafeTool) {
		t.Fatalf("missing local approval error=%v", err)
	}
	writePolicy := map[string]LocalToolPolicy{
		"a.read": {ReadOnly: false, Budget: 1}, "z.read": {ReadOnly: true, Budget: 1},
	}
	if _, err := FreezeReadOnlyTools(remote, writePolicy); !errors.Is(err, ErrUnsafeTool) {
		t.Fatalf("write policy error=%v", err)
	}
}

func TestFreezeReadOnlyToolsRejectsAmbiguousSchemas(t *testing.T) {
	policy := map[string]LocalToolPolicy{"read": {ReadOnly: true, Budget: 1}}
	for _, schema := range []string{
		`[]`, `{"type":"object","type":"array"}`, `{"type":"object"} trailing`,
	} {
		_, err := FreezeReadOnlyTools([]RemoteTool{{Name: "read", InputSchema: json.RawMessage(schema)}}, policy)
		if !errors.Is(err, ErrInvalidToolSchema) {
			t.Fatalf("schema=%s error=%v", schema, err)
		}
	}
}

func TestRemoteSessionBindingIsExactInvocationScoped(t *testing.T) {
	scope := InvocationScope{TenantID: 1, UserID: 2, ConnectionID: uuid.New(), InvocationID: uuid.New()}
	binding, err := BindRemoteSession(scope, "remote-session")
	if err != nil || !binding.Matches(scope, "remote-session") {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
	other := scope
	other.UserID++
	if binding.Matches(other, "remote-session") || binding.Matches(scope, "different") {
		t.Fatal("session binding crossed principal or remote session")
	}
	if _, err := BindRemoteSession(InvocationScope{}, "x"); !errors.Is(err, ErrSessionScope) {
		t.Fatal(err)
	}
}
