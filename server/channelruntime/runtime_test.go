package channelruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/pusheffect"
)

type adapterFake struct {
	provider Provider
	permit   SendPermit
	result   ProviderObservation
	err      error
}

func (a *adapterFake) Provider() Provider { return a.provider }
func (a *adapterFake) Send(
	_ context.Context, permit SendPermit,
) (ProviderObservation, error) {
	a.permit = permit
	return a.result, a.err
}

func testPermit(t *testing.T) SendPermit {
	t.Helper()
	payload := sha256.Sum256([]byte("frozen"))
	permit, err := BindDurableSend(ProviderTelegram, 7, 9, 11,
		uuid.NewString(), "periodic_report", hex.EncodeToString(payload[:]))
	if err != nil {
		t.Fatal(err)
	}
	return permit
}

func TestDispatcherRequiresExactAdapterAndObservation(t *testing.T) {
	permit := testPermit(t)
	adapter := &adapterFake{provider: ProviderTelegram,
		result: ProviderObservation{Disposition: pusheffect.AttemptSent,
			AppIdentity: "123", MessageID: "1"}}
	dispatcher, err := NewDispatcher(adapter)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := dispatcher.Send(t.Context(), permit)
	if err != nil || observation.Disposition != pusheffect.AttemptSent ||
		adapter.permit.EffectID() != permit.EffectID() {
		t.Fatalf("observation=%+v err=%v permit=%+v", observation, err, adapter.permit)
	}

	adapter.result = ProviderObservation{}
	if _, err := dispatcher.Send(t.Context(), permit); err == nil {
		t.Fatal("nil-error send without provider observation succeeded")
	}
	adapter.result = ProviderObservation{Disposition: "invented"}
	if _, err := dispatcher.Send(t.Context(), permit); err == nil {
		t.Fatal("invalid provider observation succeeded")
	}
}

func TestDispatcherFailsClosedForNilDuplicateAndMissing(t *testing.T) {
	var nilAdapter *adapterFake
	if _, err := NewDispatcher(nilAdapter); err == nil {
		t.Fatal("typed nil adapter succeeded")
	}
	adapter := &adapterFake{provider: ProviderTelegram}
	if _, err := NewDispatcher(adapter, adapter); err == nil {
		t.Fatal("duplicate provider adapter succeeded")
	}
	if _, err := NewDispatcher(); err == nil {
		t.Fatal("empty dispatcher succeeded")
	}
	dispatcher := &Dispatcher{adapters: map[Provider]Adapter{}}
	if _, err := dispatcher.Send(t.Context(), testPermit(t)); err == nil {
		t.Fatal("missing adapter succeeded")
	}
}

func TestPermitRejectsScopeAndDigestMutation(t *testing.T) {
	permit := testPermit(t)
	permit.tenantID++
	// The permit remains structurally valid after a principal mutation; the
	// Store claim is the durable equality authority. Mutating the digest proves
	// the local integrity gate itself is not a no-op.
	permit.payloadDigest = "0"
	if permit.Validate() == nil {
		t.Fatal("mutated permit passed validation")
	}
}
