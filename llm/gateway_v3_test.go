package llm

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

type fixedGatewayKeyV3 struct{ key []byte }

func (fixedGatewayKeyV3) PrepareResearchLLMGatewayReceiptV3(context.Context, GatewayCallBindingV3) error {
	return nil
}

type concurrentClaimSignerV3 struct {
	fixedGatewayKeyV3
	claimed atomic.Bool
}

func (s *concurrentClaimSignerV3) ResearchLLMGatewayAttemptStartedV3(context.Context, GatewayCallBindingV3) (types.ResearchLLMGatewayAttemptStateV3, error) {
	return types.ResearchLLMGatewayAttemptNoneV3, nil
}
func (s *concurrentClaimSignerV3) MarkResearchLLMGatewaySendStartedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error) {
	return s.claimed.CompareAndSwap(false, true), nil
}
func (*concurrentClaimSignerV3) SignConservativeResearchLLMGatewayRecoveryV3(context.Context, GatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error) {
	return types.ResearchLLMGatewayReceiptV3{}, errors.New("in flight")
}

func TestMeasuredGatewayV3ConcurrentSameBindingSendsOnce(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		time.Sleep(30 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	t.Cleanup(srv.Close)
	signer := &concurrentClaimSignerV3{fixedGatewayKeyV3: fixedGatewayKeyV3{key: []byte(strings.Repeat("q", 32))}}
	gateway, _ := NewMeasuredGatewayV3(newTestClient(srv.URL, 2), signer)
	binding := GatewayCallBindingV3{ReservationID: 55, RequestDigest: strings.Repeat("e", 64)}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := gateway.DoMeasured(t.Context(), binding, CallMeta{}, Request{User: "u"})
			errs <- err
		}()
	}
	close(start)
	<-errs
	<-errs
	if requests.Load() != 1 {
		t.Fatalf("provider requests=%d, want 1", requests.Load())
	}
}
func (fixedGatewayKeyV3) ResearchLLMGatewayAttemptStartedV3(context.Context, GatewayCallBindingV3) (types.ResearchLLMGatewayAttemptStateV3, error) {
	return types.ResearchLLMGatewayAttemptNoneV3, nil
}
func (fixedGatewayKeyV3) MarkResearchLLMGatewaySendStartedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error) {
	return true, nil
}
func (fixedGatewayKeyV3) MarkResearchLLMGatewayPreSendRejectedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error) {
	return true, nil
}

func (k fixedGatewayKeyV3) FinalizeMeasuredResearchLLMGatewayReceiptV3(
	_ context.Context, _ GatewayCallBindingV3, receipt types.ResearchLLMGatewayReceiptV3,
) (types.ResearchLLMGatewayReceiptV3, error) {
	receipt.KeyID = "test-key"
	payload, err := receipt.CanonicalPayload()
	if err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	mac := hmac.New(sha256.New, k.key)
	_, _ = mac.Write(payload)
	receipt.Signature = mac.Sum(nil)
	return receipt, nil
}
func (k fixedGatewayKeyV3) SignConservativeResearchLLMGatewayRecoveryV3(_ context.Context, b GatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error) {
	r := types.ResearchLLMGatewayReceiptV3{SchemaVersion: types.ResearchLLMGatewayReceiptSchemaV3,
		KeyID: "test-key", SignedAtUnixMillis: time.Now().UnixMilli(), ReservationID: b.ReservationID,
		RequestDigest: b.RequestDigest, Attempted: true, Outcome: "indeterminate", ErrorCode: string(types.CodeLLMUnavailable)}
	r.Call.Provider = "deepseek"
	r.Call.Model = "model"
	r.Call.Error = "gateway recovery: provider outcome unavailable"
	p, _ := r.CanonicalPayload()
	m := hmac.New(sha256.New, k.key)
	_, _ = m.Write(p)
	r.Signature = m.Sum(nil)
	return r, nil
}
func (k fixedGatewayKeyV3) SignConfirmedZeroResearchLLMGatewayRecoveryV3(_ context.Context, b GatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error) {
	r, _ := k.SignConservativeResearchLLMGatewayRecoveryV3(context.Background(), b)
	r.Attempted = false
	r.DefinitelyZeroUsage = true
	r.Outcome = "failed"
	r.ErrorCode = string(types.CodeLLMBadRequest)
	return r, nil
}

func TestMeasuredGatewayV3SignsMeasuredReceipt(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"model":"reported","usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`))
	}))
	t.Cleanup(srv.Close)
	gateway, err := NewMeasuredGatewayV3(newTestClient(srv.URL, 1),
		fixedGatewayKeyV3{key: key})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := gateway.DoMeasured(t.Context(), GatewayCallBindingV3{
		ReservationID: 9, RequestDigest: strings.Repeat("a", 64),
	}, CallMeta{}, Request{System: "system", User: "user"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := receipt.CanonicalPayload()
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(receipt.Signature, mac.Sum(nil)) {
		t.Fatal("invalid gateway signature")
	}
	if receipt.Outcome != "completed" || !receipt.Attempted || !receipt.UsageKnown {
		t.Fatalf("receipt=%+v", receipt)
	}
}

type failingAfterSendSignerV3 struct{ marked atomic.Int32 }

func (*failingAfterSendSignerV3) PrepareResearchLLMGatewayReceiptV3(context.Context, types.ResearchLLMGatewayCallBindingV3) error {
	return nil
}
func (*failingAfterSendSignerV3) ResearchLLMGatewayAttemptStartedV3(context.Context, types.ResearchLLMGatewayCallBindingV3) (types.ResearchLLMGatewayAttemptStateV3, error) {
	return types.ResearchLLMGatewayAttemptNoneV3, nil
}
func (s *failingAfterSendSignerV3) MarkResearchLLMGatewaySendStartedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error) {
	s.marked.Add(1)
	return true, nil
}
func (*failingAfterSendSignerV3) MarkResearchLLMGatewayPreSendRejectedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error) {
	return true, nil
}
func (*failingAfterSendSignerV3) FinalizeMeasuredResearchLLMGatewayReceiptV3(context.Context, types.ResearchLLMGatewayCallBindingV3, types.ResearchLLMGatewayReceiptV3) (types.ResearchLLMGatewayReceiptV3, error) {
	return types.ResearchLLMGatewayReceiptV3{}, errors.New("signer unavailable")
}
func (*failingAfterSendSignerV3) SignConservativeResearchLLMGatewayRecoveryV3(context.Context, types.ResearchLLMGatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error) {
	return types.ResearchLLMGatewayReceiptV3{}, errors.New("recovery unavailable")
}
func (*failingAfterSendSignerV3) SignConfirmedZeroResearchLLMGatewayRecoveryV3(context.Context, types.ResearchLLMGatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error) {
	return types.ResearchLLMGatewayReceiptV3{}, errors.New("recovery unavailable")
}

func TestMeasuredGatewayV3SignerFailureAfterSendIsNonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	t.Cleanup(srv.Close)
	signer := &failingAfterSendSignerV3{}
	gateway, _ := NewMeasuredGatewayV3(newTestClient(srv.URL, 1), signer)
	receipt, err := gateway.DoMeasured(t.Context(), GatewayCallBindingV3{
		ReservationID: 9, RequestDigest: strings.Repeat("a", 64),
	}, CallMeta{}, Request{User: "user"})
	if err == nil || types.IsRetryable(err) || !receipt.Attempted || signer.marked.Load() != 1 {
		t.Fatalf("receipt=%+v err=%v retryable=%v marked=%d", receipt, err,
			types.IsRetryable(err), signer.marked.Load())
	}
}

func TestMeasuredGatewayV3PreSendFailureSignsConfirmedZero(t *testing.T) {
	key := []byte(strings.Repeat("z", 32))
	gateway, _ := NewMeasuredGatewayV3(newTestClient("http://127.0.0.1:1", 1),
		fixedGatewayKeyV3{key: key})
	receipt, err := gateway.DoMeasured(t.Context(), GatewayCallBindingV3{
		ReservationID: 10, RequestDigest: strings.Repeat("b", 64),
	}, CallMeta{}, Request{User: "user", BeforeSend: func(context.Context) error {
		return types.NewAppError(types.CodeLLMBadRequest, "rejected", nil)
	}})
	if err == nil || receipt.Attempted || !receipt.DefinitelyZeroUsage || len(receipt.Signature) != 32 {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}
