package mcpclient

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/server/capabilityruntime"
	"github.com/google/uuid"
)

const (
	ExternalToolResultV1 = "vane.external-tool-result/v1"
	maxRPCResponseBytes  = 2 << 20
	maxToolListPages     = 16
)

var (
	ErrPermitRequired    = errors.New("mcpclient: exact ledger execution permit required")
	ErrSchemaDrift       = errors.New("mcpclient: remote tool schema drifted")
	ErrRemoteProtocol    = errors.New("mcpclient: invalid remote protocol response")
	ErrAmbiguousCall     = errors.New("mcpclient: tool call response is ambiguous")
	ErrManualApproval    = errors.New("mcpclient: exact tool catalog lacks manual approval")
	ErrCredentialBinding = errors.New("mcpclient: credential binding is unavailable")
)

// LedgerPermitV1 is returned only after the durable ledger has prepared and
// acquired the exact invocation. The low-level client checks it before its
// first DNS lookup. This package remains dark: no production package constructs
// a Coordinator or calls Invoke in migration 153.
type LedgerPermitV1 struct {
	InvocationDigest string
	BindingDigest    string
	TenantID         int64
	UserID           int64
	Attempt          int64
}

func (p LedgerPermitV1) validFor(invocation capabilityruntime.InvocationV1,
	binding RuntimeBindingV153,
) bool {
	bindingDigest, err := binding.digest()
	return p.InvocationDigest == invocation.InvocationDigest &&
		err == nil && p.BindingDigest == bindingDigest &&
		p.TenantID == int64(invocation.Principal.TenantID) &&
		p.UserID == invocation.Principal.UserID && p.Attempt == 1
}

// InvocationLedgerV1 deliberately exposes Prepare and Acquire as distinct
// calls so a test can prove no resolver or HTTP transport is reachable between
// a rejected checkpoint and the effect boundary.
type InvocationLedgerV1 interface {
	PrepareRemoteMCPV1(context.Context, capabilityruntime.InvocationV1, RuntimeBindingV153) error
	AcquireRemoteMCPV1(context.Context, capabilityruntime.InvocationV1, RuntimeBindingV153) (LedgerPermitV1, error)
	SettleRemoteMCPV1(context.Context, capabilityruntime.InvocationV1, LedgerPermitV1,
		capabilityruntime.ReceiptV1) error
}

// RuntimeBindingV153 is an exact projection of one immutable migration-153
// row. The ledger permit binds its digest, so an approval cannot be retained
// while swapping an endpoint that happens to advertise the same catalog.
type RuntimeBindingV153 struct {
	TenantID                int64  `json:"tenant_id"`
	OwnerUserID             int64  `json:"owner_user_id"`
	CapabilityID            string `json:"capability_id"`
	CapabilityVersionID     string `json:"capability_version_id"`
	Visibility              string `json:"visibility"`
	CapabilityVersionDigest string `json:"capability_version_digest"`
	EndpointURL             string `json:"endpoint_url"`
	ProtocolVersion         string `json:"protocol_version"`
	Authentication          string `json:"authentication_kind"`
	ConnectionSchemaDigest  string `json:"connection_schema_digest"`
	ApprovedCatalogDigest   string `json:"approved_catalog_digest"`
	ApprovedByUserID        int64  `json:"approved_by_user_id"`
	ApprovedAtUnixNano      int64  `json:"approved_at_unix_nano"`
}

func (b RuntimeBindingV153) digest() (string, error) {
	payload, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (b RuntimeBindingV153) validateFor(invocation capabilityruntime.InvocationV1) error {
	if b.TenantID != int64(invocation.Principal.TenantID) ||
		b.OwnerUserID != invocation.Capability.OwnerUserID ||
		b.CapabilityID != invocation.Capability.ID ||
		b.CapabilityVersionID != invocation.Capability.VersionID ||
		b.Visibility != string(invocation.Capability.Scope) ||
		b.CapabilityVersionDigest != invocation.Capability.VersionDigest ||
		b.ApprovedCatalogDigest != invocation.Capability.OperationSchemaDigest ||
		b.ApprovedByUserID <= 0 || b.ApprovedAtUnixNano <= 0 ||
		!isSHA256(b.ConnectionSchemaDigest) || !isSHA256(b.ApprovedCatalogDigest) {
		return ErrManualApproval
	}
	if err := ValidateTransport(TransportStreamableHTTP, b.ProtocolVersion); err != nil {
		return err
	}
	return ValidateEndpointSyntax(b.EndpointURL)
}

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

// ApprovedConnectionV1 must contain the exact migration-153 row projection.
// Remote annotations and installation-time self-reports never populate the
// local policies.
type ApprovedConnectionV1 struct {
	Binding           RuntimeBindingV153
	LocalToolPolicies map[string]LocalToolPolicy
}

func (p ApprovedConnectionV1) validateFor(invocation capabilityruntime.InvocationV1) error {
	if err := invocation.Validate(); err != nil {
		return err
	}
	if invocation.Capability.Kind != capabilityruntime.CapabilityKindRemoteMCP {
		return ErrManualApproval
	}
	if err := p.Binding.validateFor(invocation); err != nil {
		return err
	}
	// Migration 153 intentionally leaves credential decryption and injection
	// dark. A credentialful version cannot silently degrade to anonymous access.
	if p.Binding.Authentication != "none" || invocation.Credential != (capabilityruntime.CredentialRefV1{}) {
		return ErrCredentialBinding
	}
	if len(p.LocalToolPolicies) == 0 {
		return ErrManualApproval
	}
	policy, ok := p.LocalToolPolicies[invocation.Operation]
	if !ok || !policy.ReadOnly || policy.Budget == 0 {
		return ErrUnsafeTool
	}
	return nil
}

type Coordinator struct {
	Ledger   InvocationLedgerV1
	Resolver Resolver
	Dialer   *net.Dialer
	// roundTripper is package-private deterministic test injection. External
	// production packages cannot bypass the public-HTTPS transport constructor.
	roundTripper http.RoundTripper
}

// Invoke is the only high-level execution entry in this package. It performs
// all network-free validation, then Prepare, then Acquire, and only then hands
// the exact permit to the transport client where DNS can occur.
func (c Coordinator) Invoke(ctx context.Context, invocation capabilityruntime.InvocationV1,
	plan ApprovedConnectionV1,
) (capabilityruntime.AdapterResultV1, error) {
	if c.Ledger == nil {
		return capabilityruntime.AdapterResultV1{}, ErrPermitRequired
	}
	if err := plan.validateFor(invocation); err != nil {
		return capabilityruntime.AdapterResultV1{}, err
	}
	if err := c.Ledger.PrepareRemoteMCPV1(ctx, invocation, plan.Binding); err != nil {
		return capabilityruntime.AdapterResultV1{}, err
	}
	permit, err := c.Ledger.AcquireRemoteMCPV1(ctx, invocation, plan.Binding)
	if err != nil {
		return capabilityruntime.AdapterResultV1{}, err
	}
	if !permit.validFor(invocation, plan.Binding) {
		return capabilityruntime.AdapterResultV1{}, ErrPermitRequired
	}
	output, phase, err := c.invokePermitted(ctx, permit, invocation, plan)
	if err != nil {
		if phase == phaseToolCallSent {
			// Do not manufacture a definite failure after the request crossed the
			// effect boundary. Leaving the lease executing lets ledger 152 expire
			// it to unknown_effect with its append-only ambiguous receipt.
			return capabilityruntime.AdapterResultV1{}, fmt.Errorf("%w: %v", ErrAmbiguousCall, err)
		}
		status := capabilityruntime.ReceiptStatusDefiniteFailed
		errorClass := "mcp_transport_failed"
		if errors.Is(err, ErrSchemaDrift) || errors.Is(err, ErrUnsafeTool) ||
			errors.Is(err, ErrRemoteProtocol) {
			status = capabilityruntime.ReceiptStatusRejected
			errorClass = "mcp_policy_rejected"
		}
		receipt, receiptErr := capabilityruntime.NewReceiptV1(invocation, status,
			permit.Attempt, "", nil, errorClass, status == capabilityruntime.ReceiptStatusDefiniteFailed)
		if receiptErr != nil {
			return capabilityruntime.AdapterResultV1{}, receiptErr
		}
		if settleErr := c.settle(ctx, invocation, permit, receipt); settleErr != nil {
			return capabilityruntime.AdapterResultV1{}, settleErr
		}
		return capabilityruntime.AdapterResultV1{Receipt: receipt}, err
	}
	receipt, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusSucceeded, permit.Attempt,
		"application/json", output, "", false)
	if err != nil {
		return capabilityruntime.AdapterResultV1{}, err
	}
	if err := c.settle(ctx, invocation, permit, receipt); err != nil {
		return capabilityruntime.AdapterResultV1{}, err
	}
	result := capabilityruntime.AdapterResultV1{Receipt: receipt, SanitizedOutput: output}
	return result, result.ValidateFor(invocation)
}

func (c Coordinator) settle(ctx context.Context, invocation capabilityruntime.InvocationV1,
	permit LedgerPermitV1, receipt capabilityruntime.ReceiptV1,
) error {
	settleContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return c.Ledger.SettleRemoteMCPV1(settleContext, invocation, permit, receipt)
}

type executionPhase uint8

const (
	phaseBeforeToolCall executionPhase = iota
	phaseToolCallSent
)

func (c Coordinator) invokePermitted(ctx context.Context, permit LedgerPermitV1,
	invocation capabilityruntime.InvocationV1, plan ApprovedConnectionV1,
) ([]byte, executionPhase, error) {
	if !permit.validFor(invocation, plan.Binding) {
		return nil, phaseBeforeToolCall, ErrPermitRequired
	}
	var transport http.RoundTripper = &publicHTTPSRoundTripper{
		validator: EndpointValidator{Resolver: c.Resolver}, dialer: c.Dialer,
	}
	if c.roundTripper != nil {
		transport = c.roundTripper
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(invocation.Policy.TimeoutMillis) * time.Millisecond,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("mcp redirect limit exceeded")
			}
			return nil
		},
	}
	rpc := rpcClient{client: client, endpoint: plan.Binding.EndpointURL,
		protocolVersion: plan.Binding.ProtocolVersion, scope: InvocationScope{
			TenantID: int64(invocation.Principal.TenantID), UserID: invocation.Principal.UserID,
			ConnectionID: parseUUIDOrNil(invocation.Capability.VersionID),
			InvocationID: permitInvocationUUID(invocation.InvocationDigest),
		}}
	if err := rpc.initialize(ctx); err != nil {
		return nil, phaseBeforeToolCall, err
	}
	remoteTools, err := rpc.listTools(ctx)
	if err != nil {
		return nil, phaseBeforeToolCall, err
	}
	catalog, err := FreezeReadOnlyTools(remoteTools, plan.LocalToolPolicies)
	if err != nil {
		return nil, phaseBeforeToolCall, err
	}
	if catalog.Digest != plan.Binding.ApprovedCatalogDigest ||
		catalog.Digest != invocation.Capability.OperationSchemaDigest {
		return nil, phaseBeforeToolCall, ErrSchemaDrift
	}
	found := false
	for _, tool := range catalog.Tools {
		if tool.Name == invocation.Operation {
			found = true
			break
		}
	}
	if !found {
		return nil, phaseBeforeToolCall, ErrUnsafeTool
	}
	result, err := rpc.callTool(ctx, invocation.Operation, invocation.Arguments)
	if err != nil {
		return nil, phaseToolCallSent, err
	}
	external, err := json.Marshal(struct {
		Schema  string          `json:"schema"`
		Trust   string          `json:"trust"`
		Tainted bool            `json:"tainted"`
		Tool    string          `json:"tool"`
		Result  json.RawMessage `json:"result"`
	}{ExternalToolResultV1, "external", true, invocation.Operation, result})
	if err != nil || int64(len(external)) > invocation.Policy.MaxOutputBytes {
		return nil, phaseToolCallSent, errors.New("mcp result exceeds frozen output budget")
	}
	return external, phaseToolCallSent, nil
}

type publicHTTPSRoundTripper struct {
	validator EndpointValidator
	dialer    *net.Dialer
}

func (r *publicHTTPSRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost || request.URL == nil {
		return nil, ErrRemoteProtocol
	}
	resolved, err := r.validator.Validate(request.Context(), request.URL.String())
	if err != nil {
		return nil, err
	}
	dialer := r.dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: 10 * time.Second, KeepAlive: -1}
	}
	transport := &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12},
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, splitErr := net.SplitHostPort(address)
		if splitErr != nil || !strings.EqualFold(strings.TrimSuffix(host, "."), resolved.Host) {
			return nil, ErrUnsafeEndpoint
		}
		var last error
		for _, ip := range resolved.Addresses {
			if !ip.IsValid() {
				continue
			}
			connection, dialErr := dialer.DialContext(ctx, network,
				net.JoinHostPort(ip.String(), port))
			if dialErr == nil {
				return connection, nil
			}
			last = dialErr
		}
		if last == nil {
			last = ErrUnsafeEndpoint
		}
		return nil, last
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	response.Body = &closingBody{ReadCloser: response.Body, close: transport.CloseIdleConnections}
	return response, nil
}

type closingBody struct {
	io.ReadCloser
	close func()
}

func (b *closingBody) Close() error {
	err := b.ReadCloser.Close()
	b.close()
	return err
}

type rpcClient struct {
	client          *http.Client
	endpoint        string
	protocolVersion string
	scope           InvocationScope
	session         *SessionBinding
	nextID          int64
}

type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

func (c *rpcClient) initialize(ctx context.Context) error {
	result, response, err := c.call(ctx, "initialize", map[string]any{
		"protocolVersion": c.protocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "vane", "version": "dark-v1"},
	})
	if err != nil {
		return err
	}
	var initialized struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := decodeRemoteObject(result, &initialized); err != nil ||
		initialized.ProtocolVersion != c.protocolVersion {
		return ErrRemoteProtocol
	}
	if remoteSession := response.Header.Get("Mcp-Session-Id"); remoteSession != "" {
		binding, err := BindRemoteSession(c.scope, remoteSession)
		if err != nil {
			return err
		}
		c.session = &binding
	}
	return c.notify(ctx, "notifications/initialized", map[string]any{})
}

func (c *rpcClient) listTools(ctx context.Context) ([]RemoteTool, error) {
	tools := make([]RemoteTool, 0)
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < maxToolListPages; page++ {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, _, err := c.call(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var listed struct {
			Tools []struct {
				Name         string          `json:"name"`
				Description  string          `json:"description"`
				InputSchema  json.RawMessage `json:"inputSchema"`
				OutputSchema json.RawMessage `json:"outputSchema"`
				Annotations  json.RawMessage `json:"annotations"`
			} `json:"tools"`
			NextCursor string `json:"nextCursor"`
		}
		if err := decodeRemoteObject(result, &listed); err != nil {
			return nil, ErrRemoteProtocol
		}
		for _, tool := range listed.Tools {
			tools = append(tools, RemoteTool{Name: tool.Name, Description: tool.Description,
				InputSchema: tool.InputSchema, OutputSchema: tool.OutputSchema,
				Annotations: tool.Annotations})
		}
		if len(tools) > MaxTools {
			return nil, ErrInvalidToolSchema
		}
		if listed.NextCursor == "" {
			return tools, nil
		}
		if _, duplicate := seen[listed.NextCursor]; duplicate {
			return nil, ErrRemoteProtocol
		}
		seen[listed.NextCursor] = struct{}{}
		cursor = listed.NextCursor
	}
	return nil, ErrRemoteProtocol
}

func (c *rpcClient) callTool(ctx context.Context, name string,
	arguments json.RawMessage,
) (json.RawMessage, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, ErrRemoteProtocol
	}
	result, _, err := c.call(ctx, "tools/call", map[string]any{
		"name": name, "arguments": object,
	})
	return result, err
}

func (c *rpcClient) call(ctx context.Context, method string, params any) (
	json.RawMessage, *http.Response, error,
) {
	c.nextID++
	id := c.nextID
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": params,
	})
	if err != nil {
		return nil, nil, err
	}
	response, err := c.post(ctx, payload)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	body, err := readRPCBody(response)
	if err != nil {
		return nil, response, err
	}
	var envelope rpcEnvelope
	if err := decodeRemoteObject(body, &envelope); err != nil || envelope.JSONRPC != "2.0" ||
		envelope.Method != "" || len(envelope.ID) == 0 ||
		string(envelope.ID) != strconv.FormatInt(id, 10) || len(envelope.Error) != 0 ||
		len(envelope.Result) == 0 {
		return nil, response, ErrRemoteProtocol
	}
	return envelope.Result, response, nil
}

func (c *rpcClient) notify(ctx context.Context, method string, params any) error {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": method, "params": params,
	})
	if err != nil {
		return err
	}
	response, err := c.post(ctx, payload)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNoContent &&
		response.StatusCode != http.StatusOK {
		return ErrRemoteProtocol
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, maxRPCResponseBytes+1))
	return err
}

func (c *rpcClient) post(ctx context.Context, payload []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", c.protocolVersion)
	if c.session != nil {
		if !c.session.Matches(c.scope, c.session.RemoteSessionID) {
			return nil, ErrSessionScope
		}
		request.Header.Set("Mcp-Session-Id", c.session.RemoteSessionID)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		return nil, ErrRemoteProtocol
	}
	return response, nil
}

func readRPCBody(response *http.Response) ([]byte, error) {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return nil, ErrRemoteProtocol
	}
	if mediaType == "application/json" {
		payload, err := io.ReadAll(io.LimitReader(response.Body, maxRPCResponseBytes+1))
		if err != nil || len(payload) == 0 || len(payload) > maxRPCResponseBytes {
			return nil, ErrRemoteProtocol
		}
		return payload, nil
	}
	if mediaType != "text/event-stream" {
		return nil, ErrRemoteProtocol
	}
	scanner := bufio.NewScanner(io.LimitReader(response.Body, maxRPCResponseBytes+1))
	scanner.Buffer(make([]byte, 64<<10), maxRPCResponseBytes)
	var data bytes.Buffer
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			if data.Len() != 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
		if line == "" && data.Len() != 0 {
			break
		}
	}
	if scanner.Err() != nil || data.Len() == 0 || data.Len() > maxRPCResponseBytes {
		return nil, ErrRemoteProtocol
	}
	return data.Bytes(), nil
}

// decodeRemoteObject rejects duplicate keys and excessive nesting throughout
// the untrusted response while allowing additive MCP metadata that Vane does
// not use as policy authority (for example title, icons, or _meta).
func decodeRemoteObject(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	value, err := decodeUniqueJSON(decoder, 0)
	if err != nil {
		return err
	}
	if _, ok := value.(map[string]any); !ok {
		return ErrRemoteProtocol
	}
	if token, tokenErr := decoder.Token(); !errors.Is(tokenErr, io.EOF) || token != nil {
		return ErrRemoteProtocol
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return ErrRemoteProtocol
	}
	return json.Unmarshal(canonical, target)
}

func parseUUIDOrNil(value string) uuid.UUID {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}

func permitInvocationUUID(digest string) uuid.UUID {
	if len(digest) < 32 {
		return uuid.Nil
	}
	decoded, err := hex.DecodeString(digest[:32])
	if err != nil {
		return uuid.Nil
	}
	parsed, err := uuid.FromBytes(decoded)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}
