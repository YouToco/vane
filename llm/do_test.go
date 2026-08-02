package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type capturingRecorderStore struct {
	mu                sync.Mutex
	calls             []types.LLMCall
	legacyTryCalls    int
	legacyAdjustCalls int
	legacyAdjustments []float64
	onTryConsume      func()
}

type doTestRoundTripFunc func(*http.Request) (*http.Response, error)

func (f doTestRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (s *capturingRecorderStore) InsertLLMCall(_ context.Context, call *types.LLMCall) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, *call)
	return int64(len(s.calls)), nil
}

func (s *capturingRecorderStore) TryConsumeForUser(
	context.Context,
	int64,
	store.QuotaBucket,
	float64,
) error {
	s.mu.Lock()
	s.legacyTryCalls++
	hook := s.onTryConsume
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (s *capturingRecorderStore) AdjustForUser(
	_ context.Context,
	_ int64,
	_ store.QuotaBucket,
	adjustment float64,
) error {
	s.mu.Lock()
	s.legacyAdjustCalls++
	s.legacyAdjustments = append(s.legacyAdjustments, adjustment)
	s.mu.Unlock()
	return nil
}

func (s *capturingRecorderStore) onlyCall(t *testing.T) types.LLMCall {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) != 1 {
		t.Fatalf("recorded calls = %d, want 1", len(s.calls))
	}
	return s.calls[0]
}

func TestDo_ErrorAuditUsesRequestedModelOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	st := &capturingRecorderStore{}
	recorder := &Recorder{st: st}
	request := Request{User: "compiled run", Model: "deepseek-v4-pro"}
	_, err := Do(t.Context(), newTestClient(srv.URL, 1), recorder,
		CallMeta{TraceID: "runtime-model-error", SpanName: "score"}, request)
	if err == nil {
		t.Fatal("Do() error = nil, want upstream failure")
	}
	call := st.onlyCall(t)
	if call.Model != request.Model {
		t.Errorf("error audit model = %q, want requested override %q", call.Model, request.Model)
	}
	if call.Error == "" {
		t.Error("error audit row omitted the upstream failure")
	}
}

func TestDo_RecordsExactRunSnapshotAttribution(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	st := &capturingRecorderStore{}
	ctx := WithRunSnapshotAttribution(t.Context(), 91)
	_, _ = Do(ctx, newTestClient(srv.URL, 1), &Recorder{st: st},
		CallMeta{TraceID: "exact-run", SpanName: "score"},
		Request{User: "compiled run", Model: "deepseek-v4-pro"})
	call := st.onlyCall(t)
	if call.RunSnapshotID == nil || *call.RunSnapshotID != 91 {
		t.Fatalf("run snapshot attribution = %v, want 91", call.RunSnapshotID)
	}
}

func TestDo_RecordsUsageFromInvalidSuccessfulResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"deepseek-v4-pro",
			"choices":[],
			"usage":{
				"prompt_tokens":100,
				"completion_tokens":25,
				"prompt_cache_hit_tokens":40,
				"prompt_cache_miss_tokens":60,
				"completion_tokens_details":{"reasoning_tokens":10}
			}
		}`))
	}))
	t.Cleanup(srv.Close)

	st := &capturingRecorderStore{}
	resp, err := Do(t.Context(), newTestClient(srv.URL, 1), &Recorder{st: st},
		CallMeta{TraceID: "invalid-success-usage", SpanName: "score"},
		Request{User: "compiled run", Model: "deepseek-v4-pro"})
	if err == nil || resp != nil {
		t.Fatalf("Do() = response %v, error %v; want hidden metadata and error", resp, err)
	}
	call := st.onlyCall(t)
	if call.PromptTokens != 100 || call.CompletionTokens != 25 {
		t.Fatalf("recorded tokens = (%d,%d), want (100,25)",
			call.PromptTokens, call.CompletionTokens)
	}
	if call.PromptCacheHitTokens == nil || *call.PromptCacheHitTokens != 40 ||
		call.PromptCacheMissTokens == nil || *call.PromptCacheMissTokens != 60 {
		t.Fatalf("recorded cache tokens = (%v,%v), want (40,60)",
			call.PromptCacheHitTokens, call.PromptCacheMissTokens)
	}
	if call.ReasoningTokens == nil || *call.ReasoningTokens != 10 {
		t.Fatalf("recorded reasoning tokens = %v, want 10", call.ReasoningTokens)
	}
	if call.Error == "" {
		t.Fatal("invalid successful response audit omitted error")
	}
}

type routingRecorderStore struct {
	insertCalls int

	legacyTryCalls    int
	legacyAdjustCalls int
	legacyTryBucket   store.QuotaBucket
	legacyAdjust      store.QuotaBucket

	frozenTryCalls    int
	frozenAdjustCalls int
	frozenTenantID    int64
	frozenTryAmount   float64
	frozenAdjustDelta float64
}

func (s *routingRecorderStore) InsertLLMCall(context.Context, *types.LLMCall) (int64, error) {
	s.insertCalls++
	return int64(s.insertCalls), nil
}

func (s *routingRecorderStore) TryConsumeForUser(
	_ context.Context,
	_ int64,
	bucket store.QuotaBucket,
	_ float64,
) error {
	s.legacyTryCalls++
	s.legacyTryBucket = bucket
	return nil
}

func (s *routingRecorderStore) AdjustForUser(
	_ context.Context,
	_ int64,
	bucket store.QuotaBucket,
	_ float64,
) error {
	s.legacyAdjustCalls++
	s.legacyAdjust = bucket
	return nil
}

func (s *routingRecorderStore) AdjustForTenant(
	_ context.Context,
	tenantID int64,
	_ store.QuotaBucket,
	delta float64,
) error {
	s.frozenAdjustCalls++
	s.frozenTenantID = tenantID
	s.frozenAdjustDelta = delta
	return nil
}

func validFrozenLLMQuotaRule() runtimepolicy.QuotaBucketV1 {
	return runtimepolicy.QuotaBucketV1{
		Name:               string(store.QuotaLLMTokens),
		Financial:          true,
		EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
	}
}

func successfulQuotaTestClient(t *testing.T) (*Client, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(okResponseBody))
	}))
	t.Cleanup(srv.Close)
	return newTestClient(srv.URL, 1), &hits
}

func TestDo_CompiledQuotaUsesPostSemaphoreGateAndExactTenantSettlement(t *testing.T) {
	c, hits := successfulQuotaTestClient(t)
	st := &routingRecorderStore{}
	recorder := &Recorder{st: st}
	userID := int64(41)
	tenantID := int64(9)
	rule := validFrozenLLMQuotaRule()
	originalRule := rule
	req := Request{System: "s", User: "u"}

	beforeSpend := func(_ context.Context, amount float64) error {
		st.frozenTryCalls++
		st.frozenTryAmount = amount
		return nil
	}
	if _, err := Do(t.Context(), c, recorder,
		CallMeta{TraceID: "frozen-rule", SpanName: "score", TenantID: &tenantID,
			UserID: &userID, QuotaRule: &rule, BeforeSpend: beforeSpend},
		req); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	if rule != originalRule {
		t.Fatalf("Do mutated frozen rule: got %+v want %+v", rule, originalRule)
	}
	if st.legacyTryCalls != 0 || st.legacyAdjustCalls != 0 {
		t.Fatalf("frozen run used legacy quota path: try=%d adjust=%d",
			st.legacyTryCalls, st.legacyAdjustCalls)
	}
	if st.frozenTryCalls != 1 || st.frozenAdjustCalls != 1 {
		t.Fatalf("compiled calls = reserve %d adjust %d, want 1/1",
			st.frozenTryCalls, st.frozenAdjustCalls)
	}
	if st.frozenTenantID != tenantID {
		t.Errorf("settlement tenant = %d, want %d", st.frozenTenantID, tenantID)
	}
	wantEstimate := estimateTokens(len([]rune(req.System))+len([]rune(req.User)), req.MaxTokens)
	if st.frozenTryAmount != wantEstimate {
		t.Errorf("precharge amount = %v, want %v", st.frozenTryAmount, wantEstimate)
	}
	if wantDelta := wantEstimate - 15; st.frozenAdjustDelta != wantDelta {
		t.Errorf("reconcile delta = %v, want %v", st.frozenAdjustDelta, wantDelta)
	}
}

func TestDo_CompiledUnknownUsageFailureRetainsConservativeReservation(t *testing.T) {
	tests := []struct {
		name       string
		makeClient func(*testing.T) (*Client, context.Context, context.CancelFunc)
	}{
		{
			name: "upstream unavailable",
			makeClient: func(t *testing.T) (*Client, context.Context, context.CancelFunc) {
				t.Helper()
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "upstream failed after accepting request", http.StatusBadGateway)
				}))
				t.Cleanup(srv.Close)
				return newTestClient(srv.URL, 1), t.Context(), func() {}
			},
		},
		{
			name: "request timeout",
			makeClient: func(t *testing.T) (*Client, context.Context, context.CancelFunc) {
				t.Helper()
				client := newTestClient("http://llm.test", 1)
				client.httpClient = &http.Client{Transport: doTestRoundTripFunc(func(
					req *http.Request,
				) (*http.Response, error) {
					<-req.Context().Done()
					return nil, req.Context().Err()
				})}
				ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
				return client, ctx, cancel
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, ctx, cancel := test.makeClient(t)
			defer cancel()
			st := &routingRecorderStore{}
			userID, tenantID := int64(41), int64(9)
			rule := validFrozenLLMQuotaRule()
			var reservations atomic.Int64
			_, err := Do(ctx, client, &Recorder{st: st}, CallMeta{
				TraceID: "compiled-unknown-usage", SpanName: "score",
				TenantID: &tenantID, UserID: &userID, QuotaRule: &rule,
				BeforeSpend: func(context.Context, float64) error {
					reservations.Add(1)
					return nil
				},
			}, Request{System: "s", User: "u"})
			if types.CodeOf(err) != types.CodeLLMUnavailable {
				t.Fatalf("error code = %s, want %s", types.CodeOf(err), types.CodeLLMUnavailable)
			}
			if reservations.Load() != 1 {
				t.Fatalf("quota reservations = %d, want 1", reservations.Load())
			}
			if st.frozenAdjustCalls != 0 {
				t.Fatalf("unknown usage settlements = %d, want 0", st.frozenAdjustCalls)
			}
			if st.insertCalls != 1 {
				t.Fatalf("ledger calls = %d, want 1", st.insertCalls)
			}
		})
	}
}

func TestDo_LegacyUnknownUsageFailureRetainsConservativeReservation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream failed after accepting request", http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	userID := int64(42)
	st := &capturingRecorderStore{}
	_, err := Do(t.Context(), newTestClient(srv.URL, 1), &Recorder{st: st},
		CallMeta{TraceID: "legacy-unknown-usage", SpanName: "score", UserID: &userID},
		Request{System: "s", User: "u"})
	if types.CodeOf(err) != types.CodeLLMUnavailable {
		t.Fatalf("error code = %s, want %s", types.CodeOf(err), types.CodeLLMUnavailable)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.legacyTryCalls != 1 {
		t.Fatalf("quota reservations = %d, want 1", st.legacyTryCalls)
	}
	if st.legacyAdjustCalls != 0 {
		t.Fatalf("unknown usage refunds = %d, want 0", st.legacyAdjustCalls)
	}
	if len(st.calls) != 1 || st.calls[0].Error == "" {
		t.Fatalf("ledger calls = %#v, want one explicit error row", st.calls)
	}
}

func TestDo_WithoutFrozenQuotaRulePreservesLegacyRouting(t *testing.T) {
	c, hits := successfulQuotaTestClient(t)
	st := &routingRecorderStore{}
	userID := int64(42)

	if _, err := Do(t.Context(), c, &Recorder{st: st},
		CallMeta{TraceID: "legacy-rule", SpanName: "score", UserID: &userID},
		Request{System: "s", User: "u"}); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	if st.legacyTryCalls != 1 || st.legacyAdjustCalls != 1 ||
		st.legacyTryBucket != store.QuotaLLMTokens || st.legacyAdjust != store.QuotaLLMTokens {
		t.Errorf("legacy routing = try %d/%q adjust %d/%q",
			st.legacyTryCalls, st.legacyTryBucket,
			st.legacyAdjustCalls, st.legacyAdjust)
	}
	if st.frozenTryCalls != 0 || st.frozenAdjustCalls != 0 {
		t.Errorf("legacy run used frozen quota path: try=%d adjust=%d",
			st.frozenTryCalls, st.frozenAdjustCalls)
	}
}

func TestDo_FrozenQuotaFailsClosedBeforeUpstream(t *testing.T) {
	userID := int64(43)
	tenantID := int64(10)
	validRule := validFrozenLLMQuotaRule()
	legacyOnly := &capturingRecorderStore{}
	tests := []struct {
		name       string
		recorder   *Recorder
		rule       runtimepolicy.QuotaBucketV1
		legacyOnly *capturingRecorderStore
	}{
		{name: "recorder without exact tenant settlement", recorder: &Recorder{st: legacyOnly}, rule: validRule, legacyOnly: legacyOnly},
		{name: "nil store cannot enforce frozen rule", recorder: NewRecorder(nil), rule: validRule},
		{
			name: "wrong bucket", recorder: &Recorder{st: &routingRecorderStore{}},
			rule: func() runtimepolicy.QuotaBucketV1 {
				rule := validRule
				rule.Name = string(store.QuotaPush)
				return rule
			}(),
		},
		{
			name: "non-financial rule", recorder: &Recorder{st: &routingRecorderStore{}},
			rule: func() runtimepolicy.QuotaBucketV1 {
				rule := validRule
				rule.Financial = false
				return rule
			}(),
		},
		{
			name: "unsupported enforcement", recorder: &Recorder{st: &routingRecorderStore{}},
			rule: func() runtimepolicy.QuotaBucketV1 {
				rule := validRule
				rule.EnforcementVersion = "token-bucket/v1"
				return rule
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, hits := successfulQuotaTestClient(t)
			_, err := Do(t.Context(), c, test.recorder,
				CallMeta{TraceID: "frozen-fail-closed", SpanName: "score", TenantID: &tenantID,
					UserID: &userID, QuotaRule: &test.rule,
					BeforeSpend: func(context.Context, float64) error { return nil }},
				Request{System: "s", User: "u"})
			if err == nil || types.CodeOf(err) != types.CodeInternal {
				t.Fatalf("Do() error = %v, want CodeInternal", err)
			}
			if got := hits.Load(); got != 0 {
				t.Errorf("invalid/unsupported frozen rule reached upstream %d times", got)
			}
			if test.legacyOnly != nil {
				test.legacyOnly.mu.Lock()
				defer test.legacyOnly.mu.Unlock()
				if test.legacyOnly.legacyTryCalls != 0 ||
					test.legacyOnly.legacyAdjustCalls != 0 || len(test.legacyOnly.calls) != 1 {
					t.Errorf("unsupported frozen rule fell back to legacy or lost its failure audit: try=%d adjust=%d records=%d",
						test.legacyOnly.legacyTryCalls, test.legacyOnly.legacyAdjustCalls,
						len(test.legacyOnly.calls))
				}
			}
		})
	}
}

func TestDo_CompiledGateRunsAfterSemaphoreWaitAndBlocksRevokedRequest(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
		}
		_, _ = w.Write([]byte(okResponseBody))
	}))
	t.Cleanup(srv.Close)
	client := newTestClient(srv.URL, 1)

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.Complete(t.Context(), Request{User: "occupy slot"})
		firstDone <- err
	}()
	<-firstEntered

	recStore := &routingRecorderStore{}
	userID, tenantID := int64(41), int64(9)
	rule := validFrozenLLMQuotaRule()
	gateCalled := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		_, err := Do(t.Context(), client, &Recorder{st: recStore}, CallMeta{
			TraceID: "queued-revoke", SpanName: "score", TenantID: &tenantID,
			UserID: &userID, QuotaRule: &rule,
			BeforeSpend: func(context.Context, float64) error {
				close(gateCalled)
				return store.ErrQuotaExceeded
			},
		}, Request{User: "must not leave"})
		secondDone <- err
	}()

	select {
	case <-gateCalled:
		t.Fatal("compiled gate ran before the queued request acquired the semaphore")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first request error = %v", err)
	}
	if err := <-secondDone; types.CodeOf(err) != types.CodeQuotaExceeded {
		t.Fatalf("queued request error = %v, want CodeQuotaExceeded", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want only the first request", got)
	}
	if recStore.frozenAdjustCalls != 0 {
		t.Fatalf("rejected queued request reconciled an unmade reservation %d times", recStore.frozenAdjustCalls)
	}
}

func TestDoObservationShadow_IsAuditedWithoutTenantQuota(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		hits.Add(1)
		_, _ = w.Write([]byte(okResponseBody))
	}))
	t.Cleanup(srv.Close)

	recStore := &routingRecorderStore{}
	userID, tenantID := int64(41), int64(9)
	var gates atomic.Int64
	if _, err := DoObservationShadow(t.Context(), newTestClient(srv.URL, 1),
		&Recorder{st: recStore}, CallMeta{
			TraceID: "observation-shadow", SpanName: "qualify_events",
			TenantID: &tenantID, UserID: &userID,
		}, Request{User: "shadow candidate"},
		func(context.Context, float64) error {
			gates.Add(1)
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if gates.Load() != 1 || hits.Load() != 1 {
		t.Fatalf("independent gate=%d upstream=%d, want 1/1",
			gates.Load(), hits.Load())
	}
	if recStore.legacyTryCalls != 0 || recStore.legacyAdjustCalls != 0 ||
		recStore.frozenTryCalls != 0 || recStore.frozenAdjustCalls != 0 {
		t.Fatalf("independent spend touched tenant quota: %+v", recStore)
	}
	if recStore.insertCalls != 1 {
		t.Fatalf("independent spend llm_calls=%d, want 1",
			recStore.insertCalls)
	}
}

func TestDoObservationShadow_RejectsProductionQuotaMixBeforeUpstream(t *testing.T) {
	tests := []struct {
		name string
		meta func(*CallMeta)
	}{
		{
			name: "compiled before spend",
			meta: func(meta *CallMeta) {
				meta.BeforeSpend = func(context.Context, float64) error {
					return nil
				}
			},
		},
		{
			name: "compiled quota rule",
			meta: func(meta *CallMeta) {
				rule := validFrozenLLMQuotaRule()
				meta.QuotaRule = &rule
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(
				w http.ResponseWriter, _ *http.Request,
			) {
				hits.Add(1)
				_, _ = w.Write([]byte(okResponseBody))
			}))
			t.Cleanup(srv.Close)
			userID, tenantID := int64(41), int64(9)
			meta := CallMeta{
				TenantID: &tenantID, UserID: &userID,
			}
			test.meta(&meta)
			if _, err := DoObservationShadow(
				t.Context(), newTestClient(srv.URL, 1),
				&Recorder{st: &routingRecorderStore{}}, meta,
				Request{User: "must not leave"},
				func(context.Context, float64) error {
					return nil
				},
			); types.CodeOf(err) != types.CodeInternal {
				t.Fatalf("DoObservationShadow() error=%v, want CodeInternal", err)
			}
			if hits.Load() != 0 {
				t.Fatalf("ambiguous independent spend reached upstream %d times",
					hits.Load())
			}
		})
	}
}

func TestDoObservationShadow_RejectsMissingAuthorization(t *testing.T) {
	if _, err := DoObservationShadow(
		t.Context(), nil, nil, CallMeta{}, Request{}, nil,
	); types.CodeOf(err) != types.CodeInternal {
		t.Fatalf("DoObservationShadow() error=%v, want CodeInternal", err)
	}
}
