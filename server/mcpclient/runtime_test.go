package mcpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/capabilityruntime"
	"github.com/YouToco/vane/server/types"
	"github.com/google/uuid"
)

type fakeInvocationLedger struct {
	events           []string
	prepareErr       error
	acquireErr       error
	permit           LedgerPermitV1
	settled          []capabilityruntime.ReceiptV1
	settleContextErr error
	settleErr        error
}

func (l *fakeInvocationLedger) PrepareRemoteMCPV1(_ context.Context,
	_ capabilityruntime.InvocationV1, _ RuntimeBindingV153,
) error {
	l.events = append(l.events, "prepare")
	return l.prepareErr
}

func (l *fakeInvocationLedger) AcquireRemoteMCPV1(_ context.Context,
	invocation capabilityruntime.InvocationV1, binding RuntimeBindingV153,
) (LedgerPermitV1, error) {
	l.events = append(l.events, "acquire")
	if l.acquireErr != nil {
		return LedgerPermitV1{}, l.acquireErr
	}
	if l.permit == (LedgerPermitV1{}) {
		bindingDigest, err := binding.digest()
		if err != nil {
			return LedgerPermitV1{}, err
		}
		l.permit = LedgerPermitV1{InvocationDigest: invocation.InvocationDigest,
			BindingDigest: bindingDigest,
			TenantID:      int64(invocation.Principal.TenantID), UserID: invocation.Principal.UserID,
			Attempt: 1}
	}
	return l.permit, nil
}

func (l *fakeInvocationLedger) SettleRemoteMCPV1(ctx context.Context,
	_ capabilityruntime.InvocationV1, _ LedgerPermitV1,
	receipt capabilityruntime.ReceiptV1,
) error {
	l.events = append(l.events, "settle")
	l.settleContextErr = ctx.Err()
	l.settled = append(l.settled, receipt)
	return l.settleErr
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type fakeMCPRoundTripper struct {
	t            *testing.T
	tools        []RemoteTool
	events       []string
	reverse      bool
	failCall     bool
	cancelOnList func()
	sessionIDs   []string
}

func (r *fakeMCPRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	payload, err := io.ReadAll(request.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	var message struct {
		Method string          `json:"method"`
		ID     json.RawMessage `json:"id"`
		Params map[string]any  `json:"params"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		r.t.Fatal(err)
	}
	r.events = append(r.events, message.Method)
	r.sessionIDs = append(r.sessionIDs, request.Header.Get("Mcp-Session-Id"))
	if request.Header.Get("Authorization") != "" || request.Header.Get("X-API-Key") != "" {
		r.t.Fatal("credential header appeared in credentialless dark runtime")
	}
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader("")), Request: request}
	response.Header.Set("Content-Type", "application/json")
	switch message.Method {
	case "initialize":
		capabilities, ok := message.Params["capabilities"].(map[string]any)
		if !ok || len(capabilities) != 0 {
			r.t.Fatalf("client advertised reverse capabilities: %#v", message.Params)
		}
		response.Header.Set("Mcp-Session-Id", "session-one")
		if r.reverse {
			response.Body = io.NopCloser(strings.NewReader(
				`{"jsonrpc":"2.0","id":1,"method":"sampling/createMessage","params":{}}`))
			break
		}
		response.Body = io.NopCloser(strings.NewReader(
			`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}`))
	case "notifications/initialized":
		response.StatusCode = http.StatusAccepted
		response.Header.Del("Content-Type")
	case "tools/list":
		if r.cancelOnList != nil {
			r.cancelOnList()
		}
		encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 2,
			"result": map[string]any{"tools": r.tools}})
		if err != nil {
			r.t.Fatal(err)
		}
		response.Body = io.NopCloser(bytes.NewReader(encoded))
	case "tools/call":
		if r.failCall {
			return nil, errors.New("synthetic response loss")
		}
		response.Body = io.NopCloser(strings.NewReader(
			`{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"untrusted"}]}}`))
	default:
		r.t.Fatalf("unexpected MCP method %q", message.Method)
	}
	return response, nil
}

func approvedMCPFixture(t *testing.T) (capabilityruntime.InvocationV1, ApprovedConnectionV1, []RemoteTool) {
	t.Helper()
	tools := []RemoteTool{{Name: "search.read", Description: "remote description",
		InputSchema:  json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`),
		OutputSchema: json.RawMessage(`{"type":"object","properties":{"content":{"type":"array"}}}`),
		Annotations:  json.RawMessage(`{"readOnlyHint":false}`)}}
	policies := map[string]LocalToolPolicy{"search.read": {ReadOnly: true, Budget: 1}}
	catalog, err := FreezeReadOnlyTools(tools, policies)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal: capabilityruntime.PrincipalV1{TenantID: 7, UserID: 11,
			Role: types.MembershipRoleOwner, ActorType: types.ActorTypeUser,
			MembershipAuthorizationGeneration: 3},
		Capability: capabilityruntime.CapabilityRefV1{
			Kind:  capabilityruntime.CapabilityKindRemoteMCP,
			Scope: capabilityruntime.CapabilityScopePersonal, OwnerUserID: 11,
			ID:            "5a4234dc-b278-4dc2-94ae-3e2600235bda",
			VersionID:     "1677c42d-068d-4d40-a3ec-df5934ff8a2b",
			VersionDigest: strings.Repeat("a", 64), OperationSchemaDigest: catalog.Digest,
		},
		Operation: "search.read",
		Policy: capabilityruntime.PolicyV1{SchemaVersion: capabilityruntime.PolicySchemaVersionV1,
			Effects:  []capabilityruntime.EffectV1{capabilityruntime.EffectNetworkRead},
			ReadOnly: true, Network: capabilityruntime.NetworkPolicyPublicHTTPSReadOnly,
			Isolation: capabilityruntime.IsolationRemoteHTTPS, TimeoutMillis: 10_000,
			MaxAttempts: 1, MaxInputBytes: 4096, MaxOutputBytes: 64 << 10},
		Arguments: json.RawMessage(`{"q":"vane"}`), IdempotencyKey: "mcp-fixture-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := ApprovedConnectionV1{Binding: RuntimeBindingV153{
		TenantID: 7, OwnerUserID: 11, CapabilityID: invocation.Capability.ID,
		CapabilityVersionID:     invocation.Capability.VersionID,
		Visibility:              string(invocation.Capability.Scope),
		CapabilityVersionDigest: invocation.Capability.VersionDigest,
		EndpointURL:             "https://mcp.example.com/rpc", ProtocolVersion: ProtocolVersion20251125,
		Authentication: "none", ConnectionSchemaDigest: catalog.Digest,
		ApprovedCatalogDigest: catalog.Digest, ApprovedByUserID: 11,
		ApprovedAtUnixNano: 1,
	}, LocalToolPolicies: policies}
	return invocation, plan, tools
}

func TestCoordinatorRequiresPrepareAcquireBeforeAnyMCPTransport(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	for _, test := range []struct {
		name       string
		prepareErr error
		acquireErr error
		permit     LedgerPermitV1
		wantEvents []string
	}{
		{name: "prepare rejected", prepareErr: errors.New("prepare denied"), wantEvents: []string{"prepare"}},
		{name: "acquire rejected", acquireErr: errors.New("acquire denied"), wantEvents: []string{"prepare", "acquire"}},
		{name: "forged permit", permit: LedgerPermitV1{InvocationDigest: strings.Repeat("f", 64), TenantID: 7, UserID: 11, Attempt: 1}, wantEvents: []string{"prepare", "acquire"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger := &fakeInvocationLedger{prepareErr: test.prepareErr,
				acquireErr: test.acquireErr, permit: test.permit}
			remote := &fakeMCPRoundTripper{t: t, tools: tools}
			_, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, plan)
			if err == nil || strings.Join(ledger.events, ",") != strings.Join(test.wantEvents, ",") {
				t.Fatalf("events=%v err=%v", ledger.events, err)
			}
			if len(remote.events) != 0 {
				t.Fatalf("MCP transport reached before permit: %v", remote.events)
			}
		})
	}
}

func TestCoordinatorPermitBindsExactApprovedEndpointAndVersion(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	originalDigest, err := plan.Binding.digest()
	if err != nil {
		t.Fatal(err)
	}
	plan.Binding.EndpointURL = "https://other.example.com/rpc"
	ledger := &fakeInvocationLedger{permit: LedgerPermitV1{
		InvocationDigest: invocation.InvocationDigest, BindingDigest: originalDigest,
		TenantID: 7, UserID: 11, Attempt: 1,
	}}
	remote := &fakeMCPRoundTripper{t: t, tools: tools}
	if _, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(
		t.Context(), invocation, plan); !errors.Is(err, ErrPermitRequired) {
		t.Fatalf("same-catalog endpoint swap err=%v", err)
	}
	if len(remote.events) != 0 {
		t.Fatalf("endpoint swap crossed network boundary: %v", remote.events)
	}
}

func TestCoordinatorUsesLiveCatalogBeforeCallAndTaintsExternalResult(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	ledger := &fakeInvocationLedger{}
	remote := &fakeMCPRoundTripper{t: t, tools: tools}
	result, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, plan)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(remote.events, ","); got != "initialize,notifications/initialized,tools/list,tools/call" {
		t.Fatalf("unexpected protocol order %s", got)
	}
	if len(remote.sessionIDs) != 4 || remote.sessionIDs[0] != "" ||
		remote.sessionIDs[1] != "session-one" || remote.sessionIDs[3] != "session-one" {
		t.Fatalf("session was not invocation-scoped: %v", remote.sessionIDs)
	}
	var envelope struct {
		Schema  string `json:"schema"`
		Trust   string `json:"trust"`
		Tainted bool   `json:"tainted"`
		Tool    string `json:"tool"`
	}
	if err := json.Unmarshal(result.SanitizedOutput, &envelope); err != nil ||
		envelope.Schema != ExternalToolResultV1 || envelope.Trust != "external" ||
		!envelope.Tainted || envelope.Tool != "search.read" {
		t.Fatalf("external result lost taint: %s err=%v", result.SanitizedOutput, err)
	}
	if len(ledger.settled) != 1 || ledger.settled[0].Status != capabilityruntime.ReceiptStatusSucceeded {
		t.Fatalf("settlement=%+v", ledger.settled)
	}
}

func TestCoordinatorRejectsSchemaDriftBeforeToolCall(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	tools[0].OutputSchema = json.RawMessage(`{"type":"object","properties":{"changed":{"type":"boolean"}}}`)
	ledger := &fakeInvocationLedger{}
	remote := &fakeMCPRoundTripper{t: t, tools: tools}
	_, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, plan)
	if !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("schema drift err=%v", err)
	}
	if strings.Contains(strings.Join(remote.events, ","), "tools/call") {
		t.Fatalf("schema drift reached tool call: %v", remote.events)
	}
	if len(ledger.settled) != 1 || ledger.settled[0].Status != capabilityruntime.ReceiptStatusRejected {
		t.Fatalf("schema drift settlement=%+v", ledger.settled)
	}
}

func TestCoordinatorSettlesPolicyRejectionAfterCallerCancellation(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	tools[0].OutputSchema = json.RawMessage(`{"type":"object","properties":{"drift":{"type":"boolean"}}}`)
	ledger := &fakeInvocationLedger{}
	ctx, cancel := context.WithCancel(t.Context())
	remote := &fakeMCPRoundTripper{t: t, tools: tools, cancelOnList: cancel}
	_, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(ctx, invocation, plan)
	if !errors.Is(err, ErrSchemaDrift) || len(ledger.settled) != 1 || ledger.settleContextErr != nil {
		t.Fatalf("cancelled settlement err=%v receipts=%+v contextErr=%v",
			err, ledger.settled, ledger.settleContextErr)
	}
}

func TestCoordinatorRejectsReverseExpansionAndCredentialfulPlans(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	ledger := &fakeInvocationLedger{}
	remote := &fakeMCPRoundTripper{t: t, tools: tools, reverse: true}
	_, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, plan)
	if !errors.Is(err, ErrRemoteProtocol) || len(ledger.settled) != 1 ||
		ledger.settled[0].Status != capabilityruntime.ReceiptStatusRejected {
		t.Fatalf("reverse expansion err=%v settlement=%+v", err, ledger.settled)
	}

	credentialful := plan
	credentialful.Binding.Authentication = "bearer"
	ledger = &fakeInvocationLedger{}
	remote = &fakeMCPRoundTripper{t: t, tools: tools}
	if _, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, credentialful); !errors.Is(err, ErrCredentialBinding) {
		t.Fatalf("credentialful dark plan err=%v", err)
	}
	if len(ledger.events) != 0 || len(remote.events) != 0 {
		t.Fatalf("credentialful plan crossed boundary ledger=%v remote=%v", ledger.events, remote.events)
	}
}

func TestCoordinatorLeavesResponseLossForLedgerUnknownEffect(t *testing.T) {
	invocation, plan, tools := approvedMCPFixture(t)
	ledger := &fakeInvocationLedger{}
	remote := &fakeMCPRoundTripper{t: t, tools: tools, failCall: true}
	_, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, plan)
	if !errors.Is(err, ErrAmbiguousCall) || len(ledger.settled) != 0 {
		t.Fatalf("response loss err=%v settlement=%+v", err, ledger.settled)
	}
}

type rotatingResolver struct {
	mu      sync.Mutex
	answers [][]netip.Addr
	calls   int
}

func (r *rotatingResolver) LookupNetIP(_ context.Context, _, _ string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := r.calls
	r.calls++
	if index >= len(r.answers) {
		index = len(r.answers) - 1
	}
	return r.answers[index], nil
}

func TestPublicHTTPSTransportRevalidatesDNSOnEveryRoundTrip(t *testing.T) {
	resolver := &rotatingResolver{answers: [][]netip.Addr{
		{netip.MustParseAddr("93.184.216.34")},
		{netip.MustParseAddr("127.0.0.1")},
	}}
	transport := &publicHTTPSRoundTripper{validator: EndpointValidator{Resolver: resolver},
		dialer: &net.Dialer{Timeout: time.Millisecond}}
	firstContext, cancelFirst := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelFirst()
	request, err := http.NewRequestWithContext(firstContext, http.MethodPost,
		"https://mcp.example.com:1/rpc", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = transport.RoundTrip(request)
	secondContext, cancelSecond := context.WithTimeout(t.Context(), time.Second)
	defer cancelSecond()
	request, _ = http.NewRequestWithContext(secondContext, http.MethodPost,
		"https://mcp.example.com:1/rpc", strings.NewReader("{}"))
	if _, err := transport.RoundTrip(request); !errors.Is(err, ErrUnsafeEndpoint) {
		t.Fatalf("DNS rebinding err=%v", err)
	}
	if resolver.calls != 2 {
		t.Fatalf("resolver calls=%d, want per-roundtrip revalidation", resolver.calls)
	}
}

func TestReadRPCBodyAcceptsOneBoundedSSEDataEvent(t *testing.T) {
	response := &http.Response{Header: http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"id\":1}\n\n"))}
	payload, err := readRPCBody(response)
	if err != nil || string(payload) != "{\"jsonrpc\":\"2.0\",\n\"id\":1}" {
		t.Fatalf("SSE payload=%q err=%v", payload, err)
	}
}

func TestDecodeRemoteObjectAllowsAdditiveMetadataButRejectsDuplicateKeys(t *testing.T) {
	var target struct {
		Name string `json:"name"`
	}
	if err := decodeRemoteObject([]byte(`{"name":"tool","title":"ignored","_meta":{"x":1}}`),
		&target); err != nil || target.Name != "tool" {
		t.Fatalf("additive metadata target=%+v err=%v", target, err)
	}
	if err := decodeRemoteObject([]byte(`{"name":"first","name":"second"}`),
		&target); err == nil {
		t.Fatal("duplicate remote key was accepted")
	}
}

func TestApprovedConnectionValidationAndCoordinatorFailureBoundaries(t *testing.T) {
	invocation, plan, _ := approvedMCPFixture(t)
	if _, err := (Coordinator{}).Invoke(t.Context(), invocation, plan); !errors.Is(err, ErrPermitRequired) {
		t.Fatalf("nil ledger err=%v", err)
	}

	invalidInvocation := invocation
	invalidInvocation.InvocationDigest = ""
	if err := plan.validateFor(invalidInvocation); err == nil {
		t.Fatal("invalid invocation was accepted")
	}
	for _, test := range []struct {
		name string
		edit func(*ApprovedConnectionV1)
		want error
	}{
		{name: "approval", edit: func(value *ApprovedConnectionV1) { value.Binding.ApprovedByUserID = 0 }, want: ErrManualApproval},
		{name: "version", edit: func(value *ApprovedConnectionV1) { value.Binding.CapabilityVersionDigest = strings.Repeat("f", 64) }, want: ErrManualApproval},
		{name: "catalog", edit: func(value *ApprovedConnectionV1) { value.Binding.ApprovedCatalogDigest = strings.Repeat("f", 64) }, want: ErrManualApproval},
		{name: "connection schema", edit: func(value *ApprovedConnectionV1) { value.Binding.ConnectionSchemaDigest = "bad" }, want: ErrManualApproval},
		{name: "protocol", edit: func(value *ApprovedConnectionV1) { value.Binding.ProtocolVersion = "legacy" }, want: ErrUnsupported},
		{name: "endpoint", edit: func(value *ApprovedConnectionV1) { value.Binding.EndpointURL = "http://mcp.example.com" }, want: ErrInvalidEndpoint},
		{name: "empty policy", edit: func(value *ApprovedConnectionV1) { value.LocalToolPolicies = nil }, want: ErrManualApproval},
		{name: "missing operation", edit: func(value *ApprovedConnectionV1) {
			value.LocalToolPolicies = map[string]LocalToolPolicy{"other.read": {ReadOnly: true, Budget: 1}}
		}, want: ErrUnsafeTool},
		{name: "write policy", edit: func(value *ApprovedConnectionV1) {
			value.LocalToolPolicies[invocation.Operation] = LocalToolPolicy{ReadOnly: false, Budget: 1}
		}, want: ErrUnsafeTool},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := plan
			candidate.LocalToolPolicies = map[string]LocalToolPolicy{}
			for name, policy := range plan.LocalToolPolicies {
				candidate.LocalToolPolicies[name] = policy
			}
			test.edit(&candidate)
			if err := candidate.validateFor(invocation); !errors.Is(err, test.want) {
				t.Fatalf("validation err=%v want=%v", err, test.want)
			}
		})
	}

	ledger := &fakeInvocationLedger{settleErr: errors.New("settlement unavailable")}
	remote := &fakeMCPRoundTripper{t: t, tools: func() []RemoteTool {
		_, _, tools := approvedMCPFixture(t)
		return tools
	}()}
	if _, err := (Coordinator{Ledger: ledger, roundTripper: remote}).Invoke(t.Context(), invocation, plan); err == nil ||
		!strings.Contains(err.Error(), "settlement unavailable") {
		t.Fatalf("settlement error=%v", err)
	}
	if _, _, err := (Coordinator{}).invokePermitted(t.Context(), LedgerPermitV1{}, invocation, plan); !errors.Is(err, ErrPermitRequired) {
		t.Fatalf("direct forged permit err=%v", err)
	}
}

func TestRPCClientPaginationAndProtocolRefusals(t *testing.T) {
	response := func(request *http.Request, status int, contentType, body string) *http.Response {
		header := make(http.Header)
		if contentType != "" {
			header.Set("Content-Type", contentType)
		}
		return &http.Response{StatusCode: status, Header: header,
			Body: io.NopCloser(strings.NewReader(body)), Request: request}
	}
	clientFor := func(handler roundTripperFunc) *rpcClient {
		return &rpcClient{client: &http.Client{Transport: handler}, endpoint: "https://mcp.example.com/rpc",
			protocolVersion: ProtocolVersion20251125, scope: InvocationScope{TenantID: 1, UserID: 2}}
	}

	t.Run("two pages", func(t *testing.T) {
		calls := 0
		client := clientFor(func(request *http.Request) (*http.Response, error) {
			calls++
			payload, _ := io.ReadAll(request.Body)
			var message struct {
				ID     int64          `json:"id"`
				Params map[string]any `json:"params"`
			}
			if err := json.Unmarshal(payload, &message); err != nil {
				t.Fatal(err)
			}
			cursor := "next"
			if calls == 2 {
				if message.Params["cursor"] != "next" {
					t.Fatalf("cursor=%v", message.Params)
				}
				cursor = ""
			}
			body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID,
				"result": map[string]any{"tools": []any{}, "nextCursor": cursor}})
			return response(request, http.StatusOK, "application/json", string(body)), nil
		})
		tools, err := client.listTools(t.Context())
		if err != nil || len(tools) != 0 || calls != 2 {
			t.Fatalf("tools=%v calls=%d err=%v", tools, calls, err)
		}
	})

	for _, test := range []struct {
		name string
		body func(id int64) string
		want error
	}{
		{name: "malformed list", body: func(id int64) string { return `{"jsonrpc":"2.0","id":1,"result":[]}` }, want: ErrRemoteProtocol},
		{name: "duplicate cursor", body: func(id int64) string {
			return `{"jsonrpc":"2.0","id":` + strconv.FormatInt(id, 10) + `,"result":{"tools":[],"nextCursor":"same"}}`
		}, want: ErrRemoteProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := clientFor(func(request *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(request.Body)
				var message struct {
					ID int64 `json:"id"`
				}
				_ = json.Unmarshal(body, &message)
				return response(request, http.StatusOK, "application/json", test.body(message.ID)), nil
			})
			if _, err := client.listTools(t.Context()); !errors.Is(err, test.want) {
				t.Fatalf("list err=%v", err)
			}
		})
	}

	t.Run("page cap", func(t *testing.T) {
		client := clientFor(func(request *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(request.Body)
			var message struct {
				ID int64 `json:"id"`
			}
			_ = json.Unmarshal(body, &message)
			payload := `{"jsonrpc":"2.0","id":` + strconv.FormatInt(message.ID, 10) +
				`,"result":{"tools":[],"nextCursor":"page-` + strconv.FormatInt(message.ID, 10) + `"}}`
			return response(request, http.StatusOK, "application/json", payload), nil
		})
		if _, err := client.listTools(t.Context()); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("page cap err=%v", err)
		}
	})

	t.Run("initialize mismatch and bad session", func(t *testing.T) {
		client := clientFor(func(request *http.Request) (*http.Response, error) {
			return response(request, http.StatusOK, "application/json",
				`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"wrong"}}`), nil
		})
		if err := client.initialize(t.Context()); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("protocol mismatch err=%v", err)
		}

		client = clientFor(func(request *http.Request) (*http.Response, error) {
			value := response(request, http.StatusOK, "application/json",
				`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25"}}`)
			value.Header.Set("Mcp-Session-Id", strings.Repeat("s", 1025))
			return value, nil
		})
		if err := client.initialize(t.Context()); !errors.Is(err, ErrSessionScope) {
			t.Fatalf("bad session err=%v", err)
		}
	})
}

func TestRPCClientRequestAndBodyBoundaries(t *testing.T) {
	response := func(request *http.Request, status int, contentType, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{contentType}},
			Body: io.NopCloser(strings.NewReader(body)), Request: request}
	}
	t.Run("call tool input", func(t *testing.T) {
		client := &rpcClient{}
		if _, err := client.callTool(t.Context(), "x", json.RawMessage(`[]`)); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("array input err=%v", err)
		}
		if _, err := client.callTool(t.Context(), "x", json.RawMessage(`{`)); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("malformed input err=%v", err)
		}
	})

	t.Run("post boundaries", func(t *testing.T) {
		client := &rpcClient{client: &http.Client{}, endpoint: "://bad", protocolVersion: ProtocolVersion20251125}
		if _, err := client.post(t.Context(), []byte(`{}`)); err == nil {
			t.Fatal("bad request URL accepted")
		}
		client = &rpcClient{client: &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			return response(request, http.StatusUnauthorized, "application/json", `{}`), nil
		})}, endpoint: "https://mcp.example.com", protocolVersion: ProtocolVersion20251125}
		if _, err := client.post(t.Context(), []byte(`{}`)); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("HTTP rejection err=%v", err)
		}
		client.session = &SessionBinding{Scope: InvocationScope{TenantID: 9}, RemoteSessionID: "session"}
		if _, err := client.post(t.Context(), []byte(`{}`)); !errors.Is(err, ErrSessionScope) {
			t.Fatalf("cross-scope session err=%v", err)
		}
	})

	t.Run("notify status", func(t *testing.T) {
		client := &rpcClient{client: &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			return response(request, http.StatusCreated, "application/json", `{}`), nil
		})}, endpoint: "https://mcp.example.com", protocolVersion: ProtocolVersion20251125}
		if err := client.notify(t.Context(), "notifications/initialized", map[string]any{}); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("notify status err=%v", err)
		}
	})

	for _, test := range []struct {
		name, contentType, body string
	}{
		{name: "invalid content type", contentType: "%%%", body: `{}`},
		{name: "unsupported content type", contentType: "text/plain", body: `{}`},
		{name: "empty JSON", contentType: "application/json", body: ""},
		{name: "oversized JSON", contentType: "application/json", body: strings.Repeat("x", maxRPCResponseBytes+1)},
		{name: "empty SSE", contentType: "text/event-stream", body: "event: ping\n\n"},
		{name: "oversized SSE line", contentType: "text/event-stream", body: "data: " + strings.Repeat("x", maxRPCResponseBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := readRPCBody(response(nil, http.StatusOK, test.contentType, test.body)); !errors.Is(err, ErrRemoteProtocol) {
				t.Fatalf("body err=%v", err)
			}
		})
	}

	var target map[string]any
	for _, payload := range []string{`[]`, `{} {}`} {
		if err := decodeRemoteObject([]byte(payload), &target); !errors.Is(err, ErrRemoteProtocol) {
			t.Fatalf("decode %q err=%v", payload, err)
		}
	}
	if parseUUIDOrNil("not-a-uuid") != uuid.Nil || permitInvocationUUID("short") != uuid.Nil ||
		permitInvocationUUID(strings.Repeat("z", 64)) != uuid.Nil {
		t.Fatal("invalid UUID material did not fail closed")
	}
	valid := "1677c42d-068d-4d40-a3ec-df5934ff8a2b"
	if parseUUIDOrNil(valid) == uuid.Nil || permitInvocationUUID(strings.ReplaceAll(valid, "-", "")+strings.Repeat("0", 32)) == uuid.Nil {
		t.Fatal("valid UUID material was rejected")
	}
}

func TestPublicTransportMethodAndClosingBody(t *testing.T) {
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://mcp.example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&publicHTTPSRoundTripper{}).RoundTrip(request); !errors.Is(err, ErrRemoteProtocol) {
		t.Fatalf("GET transport err=%v", err)
	}
	closed := false
	body := &closingBody{ReadCloser: io.NopCloser(strings.NewReader("x")), close: func() { closed = true }}
	if err := body.Close(); err != nil || !closed {
		t.Fatalf("closing body err=%v closed=%v", err, closed)
	}
}
