package researchgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/runtimepolicy"
)

const (
	testDigestV1     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testCapabilityV1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type memoryRepositoryV1 struct {
	mu             sync.Mutex
	claimed        bool
	settled        bool
	claims         int
	request        FrozenRequestV1
	recover        bool
	settleFailures int
	settleCalls    int
}

func TestMainServerHasNoGatewayDatabaseAuthority(t *testing.T) {
	source, err := os.ReadFile("../cmd/server/main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"ResearchGatewayRuntimeURL", "NewWithResearchRuntimeCapabilityAndGateway", "store.Migrate("} {
		if strings.Contains(string(source), forbidden) {
			t.Fatalf("main server contains forbidden gateway/owner authority %q", forbidden)
		}
	}
	if !strings.Contains(string(source), "NewUnixClientV1") {
		t.Fatal("main server does not construct the UDS-only gateway client")
	}
}

func TestDeploymentTemplatesDoNotLeakOwnerOrGatewaySecretsToServer(t *testing.T) {
	serverEnv, err := os.ReadFile("../deploy/server.env.example")
	if err != nil {
		t.Fatal(err)
	}
	serverUnit, err := os.ReadFile("../deploy/vane.service")
	if err != nil {
		t.Fatal(err)
	}
	combined := string(serverEnv) + string(serverUnit)
	for _, forbidden := range []string{"POSTGRES_PASSWORD", "VANE_MIGRATION_DB_URL",
		"gateway_db_url", "research_llm_api_key_gen1", "VANE_GATEWAY_LLM_ROUTES_JSON"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("long-lived server template contains forbidden authority %q", forbidden)
		}
	}
	if !strings.Contains(string(serverEnv), "vane_server_runtime") {
		t.Fatal("server env does not use the non-owner runtime login")
	}
}

func (r *memoryRepositoryV1) Claim(_ context.Context, binding ExecuteRequestV1) (ClaimV1, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claims++
	if binding.ReservationID != r.request.ReservationID || binding.RequestDigest != r.request.RequestDigest {
		return ClaimV1{}, errors.New("binding mismatch")
	}
	first := !r.claimed
	r.claimed = true
	return ClaimV1{FirstWriter: first, Settled: r.settled, Request: r.request}, nil
}

func (r *memoryRepositoryV1) Settle(_ context.Context, _ ExecuteRequestV1, _ FrozenRequestV1, _ SettlementV1) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.settleCalls++
	if r.settleCalls <= r.settleFailures {
		return errors.New("transient settlement failure")
	}
	r.settled = true
	return nil
}

func TestServiceV1RetriesSettlementWithoutRepeatingProvider(t *testing.T) {
	service, repository, provider := testServiceV1(t)
	repository.settleFailures = 2
	response, err := service.Execute(t.Context(), testBindingV1())
	if err != nil || response.Status != StatusSettledV1 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if provider.calls.Load() != 1 || repository.settleCalls != 3 {
		t.Fatalf("provider=%d settlement calls=%d", provider.calls.Load(), repository.settleCalls)
	}
}

func (r *memoryRepositoryV1) Recover(context.Context, ExecuteRequestV1) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recover {
		r.settled = true
	}
	return r.recover, nil
}

type countingProviderV1 struct{ calls atomic.Int32 }

func (p *countingProviderV1) Complete(context.Context, FrozenRequestV1) (llm.MeasuredCallV3, error) {
	p.calls.Add(1)
	return llm.MeasuredCallV3{Attempted: true, UsageKnown: true}, nil
}

func testServiceV1(t *testing.T) (*ServiceV1, *memoryRepositoryV1, *countingProviderV1) {
	t.Helper()
	repository := &memoryRepositoryV1{request: FrozenRequestV1{
		ReservationID: 7, RequestDigest: testDigestV1, Provider: "deepseek",
		Model: "model", SystemPrompt: "secret system", UserPrompt: "secret user",
		MaxTokens: 32,
	}}
	provider := &countingProviderV1{}
	service, err := NewServiceV1(repository, provider)
	if err != nil {
		t.Fatal(err)
	}
	return service, repository, provider
}

func testBindingV1() ExecuteRequestV1 {
	return ExecuteRequestV1{ReservationID: 7, RequestDigest: testDigestV1,
		RunCapability: testCapabilityV1}
}

func TestServiceV1ConcurrentReservationCallsProviderOnce(t *testing.T) {
	service, _, provider := testServiceV1(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := service.Execute(t.Context(), testBindingV1())
			if err != nil || (response.Status != StatusSettledV1 && response.Status != StatusInFlightV1) {
				t.Errorf("response=%+v err=%v", response, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := provider.calls.Load(); got != 1 {
		t.Fatalf("provider calls=%d, want 1", got)
	}
}

type dropFirstResponseTransportV1 struct {
	base    http.RoundTripper
	dropped atomic.Bool
}

func (t *dropFirstResponseTransportV1) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if t.dropped.CompareAndSwap(false, true) {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, errors.New("simulated response loss")
	}
	return response, nil
}

func TestClientV1ResponseLossRetryDoesNotRepeatProvider(t *testing.T) {
	service, _, provider := testServiceV1(t)
	server := httptest.NewUnstartedServer(service.Handler())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server.Listener = listener
	server.Start()
	defer server.Close()
	transport := &dropFirstResponseTransportV1{base: http.DefaultTransport}
	client, err := NewClientV1(&http.Client{Transport: transport}, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client.retryDelay = time.Millisecond
	response, err := client.Execute(t.Context(), testBindingV1())
	if err != nil || response.Status != StatusSettledV1 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if got := provider.calls.Load(); got != 1 {
		t.Fatalf("provider calls=%d, want 1", got)
	}
}

func TestClientV1PollsInFlightUntilRecoverySettles(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if requests.Add(1) < 3 {
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"status":"in_flight"}`)
			return
		}
		_, _ = io.WriteString(w, `{"status":"settled"}`)
	}))
	defer server.Close()
	client, err := NewClientV1(server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client.retryDelay = time.Millisecond
	response, err := client.Execute(t.Context(), testBindingV1())
	if err != nil || response.Status != StatusSettledV1 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("requests=%d, want 3", got)
	}
}

func TestServiceV1RecoversAbandonedClaimWithoutProviderRetry(t *testing.T) {
	service, repository, provider := testServiceV1(t)
	repository.claimed = true
	repository.recover = true
	response, err := service.Execute(t.Context(), testBindingV1())
	if err != nil || response.Status != StatusSettledV1 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if provider.calls.Load() != 0 {
		t.Fatal("recovery retried provider")
	}
}

func TestLLMProviderV1MissingFrozenGenerationFailsBeforeSend(t *testing.T) {
	resolver, err := llm.NewRuntimeModelResolverV1()
	if err != nil {
		t.Fatal(err)
	}
	measured, err := (LLMProviderV1{Resolver: resolver}).Complete(t.Context(), FrozenRequestV1{
		Provider: runtimepolicy.ModelProviderDeepSeekV1,
		Endpoint: runtimepolicy.EndpointRefV1{
			ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 7},
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 9},
		Model: "deepseek-chat", SystemPrompt: "system", UserPrompt: "user", MaxTokens: 8,
	})
	if err == nil || measured.Attempted {
		t.Fatalf("missing frozen route measured=%+v err=%v", measured, err)
	}
}

func TestLLMProviderV1ForwardsFrozenDisableThinkingToUpstream(t *testing.T) {
	var requestBody map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Errorf("decode upstream request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"model":"deepseek-v4-pro",
			"choices":[{"message":{"role":"assistant","content":"{\"steps\":[]}"}}],
			"usage":{"prompt_tokens":10,"completion_tokens":5,
			"prompt_cache_hit_tokens":4,"prompt_cache_miss_tokens":6}
		}`)
	}))
	defer upstream.Close()

	client := llm.New(config.LLMConfig{
		Provider: "deepseek", BaseURL: upstream.URL, APIKey: "test-key",
		Model: "deepseek-v4-flash", MaxConcurrent: 1,
	})
	endpoint := runtimepolicy.EndpointRefV1{
		ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 3,
	}
	credential := runtimepolicy.CredentialRefV1{
		ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 4,
	}
	resolver, err := llm.NewRuntimeModelResolverV1(llm.RuntimeModelRouteV1{
		Provider: runtimepolicy.ModelProviderDeepSeekV1, Endpoint: endpoint,
		CredentialRef: credential, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	measured, err := (LLMProviderV1{Resolver: resolver}).Complete(t.Context(), FrozenRequestV1{
		RunSnapshotID: 17, TenantID: 23, UserID: 29, TraceID: "trace-v3",
		Stage: "planner", SystemPrompt: "system", UserPrompt: "user",
		Provider: runtimepolicy.ModelProviderDeepSeekV1, Endpoint: endpoint,
		CredentialRef: credential, Model: "deepseek-v4-pro", MaxTokens: 4096,
		DisableThinking: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !measured.Attempted || !measured.DisableThinking || measured.Response == nil ||
		measured.Response.Content == "" {
		t.Fatalf("measured=%+v", measured)
	}
	if requestBody["model"] != "deepseek-v4-pro" {
		t.Fatalf("upstream model=%v", requestBody["model"])
	}
	thinking, ok := requestBody["thinking"].(map[string]any)
	if !ok || thinking["type"] != "disabled" {
		t.Fatalf("upstream thinking=%v", requestBody["thinking"])
	}
}

func TestHandlerV1RejectsCallerSuppliedProviderFields(t *testing.T) {
	service, repository, provider := testServiceV1(t)
	server := httptest.NewServer(service.Handler())
	defer server.Close()
	payload := []byte(`{"reservation_id":7,"request_digest":"` + testDigestV1 +
		`","run_capability":"` + testCapabilityV1 + `","model":"forged","usage":999,"completion":"forged"}`)
	response, err := http.Post(server.URL+ExecutePathV1, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", response.StatusCode)
	}
	if repository.claims != 0 || provider.calls.Load() != 0 {
		t.Fatal("forged request reached repository or provider")
	}
}
