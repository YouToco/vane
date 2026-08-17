// Package mcpclient contains the non-executing safety foundation for remote
// MCP connections. It may resolve DNS for SSRF admission, but it never
// downloads, starts, or invokes an MCP server.
package mcpclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	TransportStreamableHTTP = "streamable_http"
	ProtocolVersion20250618 = "2025-06-18"
	ProtocolVersion20251125 = "2025-11-25"
	FrozenToolCatalogV1     = "vane.mcp-frozen-tool-catalog/v1"
	MaxTools                = 128
	MaxSchemaBytes          = 256 << 10
	MaxCatalogBytes         = 2 << 20
)

var (
	ErrInvalidEndpoint     = errors.New("mcpclient: invalid endpoint")
	ErrUnsafeEndpoint      = errors.New("mcpclient: endpoint is not public")
	ErrUnsupported         = errors.New("mcpclient: unsupported transport or protocol")
	ErrUnsafeTool          = errors.New("mcpclient: tool is not locally approved read-only")
	ErrInvalidToolSchema   = errors.New("mcpclient: invalid tool schema")
	ErrSessionScope        = errors.New("mcpclient: invalid session scope")
	toolNamePattern        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
	hostNamePattern        = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)
	blockedAddressPrefixes = mustPrefixes(
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
		"169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
		"192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24",
		"224.0.0.0/4", "240.0.0.0/4", "::/128", "::1/128", "fc00::/7",
		"fe80::/10", "ff00::/8", "2001:db8::/32",
	)
)

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type EndpointValidator struct{ Resolver Resolver }

type ResolvedEndpoint struct {
	URL       string
	Host      string
	Addresses []netip.Addr
}

// Validate resolves every hostname and rejects the whole endpoint if any DNS
// answer is not public. Callers must repeat this for every request and redirect
// rather than trusting this result as a permanent DNS pin.
func (v EndpointValidator) Validate(ctx context.Context, rawURL string) (ResolvedEndpoint, error) {
	parsed, host, err := validateEndpointURL(rawURL)
	if err != nil {
		return ResolvedEndpoint{}, err
	}
	addresses := make([]netip.Addr, 0, 2)
	if literal, literalErr := netip.ParseAddr(host); literalErr == nil {
		addresses = append(addresses, literal.Unmap())
	} else {
		resolver := v.Resolver
		if resolver == nil {
			resolver = net.DefaultResolver
		}
		addresses, err = resolver.LookupNetIP(ctx, "ip", host)
		if err != nil || len(addresses) == 0 {
			return ResolvedEndpoint{}, fmt.Errorf("%w: DNS lookup failed", ErrInvalidEndpoint)
		}
	}
	seen := make(map[netip.Addr]struct{}, len(addresses))
	normalized := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if !isPublicAddress(address) {
			return ResolvedEndpoint{}, fmt.Errorf("%w: host resolved to %s", ErrUnsafeEndpoint, address)
		}
		if _, duplicate := seen[address]; !duplicate {
			seen[address] = struct{}{}
			normalized = append(normalized, address)
		}
	}
	slices.SortFunc(normalized, func(a, b netip.Addr) int { return a.Compare(b) })
	return ResolvedEndpoint{URL: parsed.String(), Host: host, Addresses: normalized}, nil
}

// ValidateRedirect intentionally performs a fresh DNS resolution. A redirect
// target never inherits the original host's SSRF decision.
func (v EndpointValidator) ValidateRedirect(ctx context.Context, rawURL string) (ResolvedEndpoint, error) {
	return v.Validate(ctx, rawURL)
}

func ValidateTransport(transport, protocol string) error {
	if transport != TransportStreamableHTTP {
		return fmt.Errorf("%w: only Streamable HTTP is accepted", ErrUnsupported)
	}
	if protocol != ProtocolVersion20250618 && protocol != ProtocolVersion20251125 {
		return fmt.Errorf("%w: protocol %q", ErrUnsupported, protocol)
	}
	return nil
}

// ValidateEndpointSyntax performs the network-free half of endpoint admission.
// A successful result still requires EndpointValidator.Validate immediately
// before persistence and before each connection attempt.
func ValidateEndpointSyntax(rawURL string) error {
	_, _, err := validateEndpointURL(rawURL)
	return err
}

func validateEndpointURL(rawURL string) (*url.URL, string, error) {
	if strings.TrimSpace(rawURL) != rawURL || len(rawURL) == 0 || len(rawURL) > 2048 || !utf8.ValidString(rawURL) {
		return nil, "", fmt.Errorf("%w: malformed URL", ErrInvalidEndpoint)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !parsed.IsAbs() || parsed.Scheme != "https" || parsed.Opaque != "" {
		return nil, "", fmt.Errorf("%w: absolute HTTPS URL required", ErrInvalidEndpoint)
	}
	if parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return nil, "", fmt.Errorf("%w: credentials, query, and fragment are forbidden", ErrInvalidEndpoint)
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" || strings.Contains(host, "%") {
		return nil, "", fmt.Errorf("%w: invalid host", ErrInvalidEndpoint)
	}
	if _, err := netip.ParseAddr(host); err != nil {
		if !hostNamePattern.MatchString(host) || !validDNSLabels(host) {
			return nil, "", fmt.Errorf("%w: non-ASCII or invalid host", ErrInvalidEndpoint)
		}
	}
	if literal, err := netip.ParseAddr(host); err == nil && !isPublicAddress(literal.Unmap()) {
		return nil, "", fmt.Errorf("%w: host is not public", ErrUnsafeEndpoint)
	}
	if port := parsed.Port(); port != "" {
		value, portErr := strconv.Atoi(port)
		if portErr != nil || value < 1 || value > 65535 {
			return nil, "", fmt.Errorf("%w: invalid port", ErrInvalidEndpoint)
		}
	}
	parsed.Scheme = "https"
	return parsed, host, nil
}

func validDNSLabels(host string) bool {
	if len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

func isPublicAddress(address netip.Addr) bool {
	if !address.IsValid() || !address.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range blockedAddressPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

func mustPrefixes(values ...string) []netip.Prefix {
	result := make([]netip.Prefix, len(values))
	for i, value := range values {
		result[i] = netip.MustParsePrefix(value)
	}
	return result
}

type RemoteTool struct {
	Name         string
	Description  string
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
	Annotations  json.RawMessage // retained only as untrusted input; never policy authority
}

type LocalToolPolicy struct {
	ReadOnly bool   `json:"read_only"`
	Budget   uint16 `json:"budget"`
}

type FrozenTool struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"input_schema"`
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`
	Policy       LocalToolPolicy `json:"policy"`
}

type FrozenToolCatalog struct {
	SchemaVersion string       `json:"schema_version"`
	Tools         []FrozenTool `json:"tools"`
	Payload       []byte       `json:"-"`
	Digest        string       `json:"digest"`
}

// FreezeReadOnlyTools canonicalizes remote schemas, but accepts read-only
// authority exclusively from localPolicy. Remote annotations are ignored.
func FreezeReadOnlyTools(remote []RemoteTool, localPolicy map[string]LocalToolPolicy) (FrozenToolCatalog, error) {
	if len(remote) > MaxTools {
		return FrozenToolCatalog{}, fmt.Errorf("%w: too many tools", ErrInvalidToolSchema)
	}
	tools := make([]FrozenTool, 0, len(remote))
	seen := make(map[string]struct{}, len(remote))
	for _, candidate := range remote {
		if !toolNamePattern.MatchString(candidate.Name) || len(candidate.Description) > 4096 ||
			!utf8.ValidString(candidate.Description) || strings.ContainsRune(candidate.Description, 0) {
			return FrozenToolCatalog{}, fmt.Errorf("%w: invalid tool metadata", ErrInvalidToolSchema)
		}
		if _, duplicate := seen[candidate.Name]; duplicate {
			return FrozenToolCatalog{}, fmt.Errorf("%w: duplicate tool %q", ErrInvalidToolSchema, candidate.Name)
		}
		seen[candidate.Name] = struct{}{}
		policy, approved := localPolicy[candidate.Name]
		if !approved || !policy.ReadOnly || policy.Budget == 0 {
			return FrozenToolCatalog{}, fmt.Errorf("%w: %s", ErrUnsafeTool, candidate.Name)
		}
		canonical, err := canonicalJSONObject(candidate.InputSchema)
		if err != nil {
			return FrozenToolCatalog{}, err
		}
		var canonicalOutput json.RawMessage
		if len(candidate.OutputSchema) != 0 {
			canonicalOutput, err = canonicalJSONObject(candidate.OutputSchema)
			if err != nil {
				return FrozenToolCatalog{}, err
			}
		}
		tools = append(tools, FrozenTool{
			Name: candidate.Name, Description: candidate.Description,
			InputSchema: canonical, OutputSchema: canonicalOutput, Policy: policy,
		})
	}
	slices.SortFunc(tools, func(a, b FrozenTool) int { return strings.Compare(a.Name, b.Name) })
	payload, err := json.Marshal(struct {
		SchemaVersion string       `json:"schema_version"`
		Tools         []FrozenTool `json:"tools"`
	}{FrozenToolCatalogV1, tools})
	if err != nil || len(payload) > MaxCatalogBytes {
		return FrozenToolCatalog{}, fmt.Errorf("%w: catalog encoding", ErrInvalidToolSchema)
	}
	sum := sha256.Sum256(payload)
	return FrozenToolCatalog{SchemaVersion: FrozenToolCatalogV1, Tools: tools,
		Payload: payload, Digest: hex.EncodeToString(sum[:])}, nil
}

func canonicalJSONObject(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 || len(raw) > MaxSchemaBytes {
		return nil, fmt.Errorf("%w: schema size", ErrInvalidToolSchema)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeUniqueJSON(decoder, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToolSchema, err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("%w: input schema must be an object", ErrInvalidToolSchema)
	}
	if token, tokenErr := decoder.Token(); !errors.Is(tokenErr, io.EOF) || token != nil {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalidToolSchema)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode schema", ErrInvalidToolSchema)
	}
	return canonical, nil
}

func decodeUniqueJSON(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return nil, keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate key %q", key)
			}
			value, valueErr := decodeUniqueJSON(decoder, depth+1)
			if valueErr != nil {
				return nil, valueErr
			}
			object[key] = value
		}
		end, endErr := decoder.Token()
		if endErr != nil || end != json.Delim('}') {
			return nil, errors.New("unterminated object")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, valueErr := decodeUniqueJSON(decoder, depth+1)
			if valueErr != nil {
				return nil, valueErr
			}
			array = append(array, value)
		}
		end, endErr := decoder.Token()
		if endErr != nil || end != json.Delim(']') {
			return nil, errors.New("unterminated array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected delimiter")
	}
}

type InvocationScope struct {
	TenantID     int64
	UserID       int64
	ConnectionID uuid.UUID
	InvocationID uuid.UUID
}

type SessionBinding struct {
	Scope           InvocationScope
	RemoteSessionID string
}

func BindRemoteSession(scope InvocationScope, remoteSessionID string) (SessionBinding, error) {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.ConnectionID == uuid.Nil ||
		scope.InvocationID == uuid.Nil || len(remoteSessionID) == 0 || len(remoteSessionID) > 1024 ||
		!utf8.ValidString(remoteSessionID) || strings.ContainsAny(remoteSessionID, "\r\n\x00") {
		return SessionBinding{}, ErrSessionScope
	}
	return SessionBinding{Scope: scope, RemoteSessionID: remoteSessionID}, nil
}

func (b SessionBinding) Matches(scope InvocationScope, remoteSessionID string) bool {
	return b.Scope == scope && b.RemoteSessionID == remoteSessionID
}
