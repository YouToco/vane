// Package channelruntime defines the narrow provider-neutral boundary between
// durable business workflows and external notification adapters.
//
// A workflow may prepare a durable provider effect through Store and receive a
// SendPermit. It cannot pass provider credentials, arbitrary destinations or
// unfrozen payload bytes to an adapter. Dispatcher accepts only that permit and
// returns the provider's typed observation.
//
// Security rollout status: this package does not activate a SaaS canary. The
// current channel tables are not FORCE RLS and the primary Store still has
// schema-owner compatibility authority. A future migration must install and
// verify the narrow non-owner channel runtime role before production canary.
package channelruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/pusheffect"
)

type Provider string

const ProviderTelegram Provider = "telegram"

func (p Provider) Valid() bool {
	return p == ProviderTelegram
}

// SendPermit is an immutable projection of one already-durable effect. Fields
// are deliberately private: only BindDurableSend can construct a value, and a
// repository AST guard restricts production constructor calls to Store.
type SendPermit struct {
	provider      Provider
	tenantID      int64
	userID        int64
	routeID       int64
	effectID      string
	effectKind    string
	payloadDigest string
}

// BindDurableSend is the Store-side constructor. It accepts a digest, never
// payload bytes or provider credentials. Callers outside server/store are
// rejected by the repository invariant test.
func BindDurableSend(
	provider Provider, tenantID, userID, routeID int64,
	effectID, effectKind, payloadDigest string,
) (SendPermit, error) {
	parsed, err := uuid.Parse(effectID)
	if !provider.Valid() || tenantID <= 0 || userID <= 0 || routeID <= 0 ||
		err != nil || parsed.String() != effectID ||
		strings.TrimSpace(effectKind) != effectKind || effectKind == "" ||
		len(effectKind) > 64 || !validDigest(payloadDigest) {
		return SendPermit{}, errors.New("channel runtime: durable send permit is invalid")
	}
	return SendPermit{provider: provider, tenantID: tenantID, userID: userID,
		routeID: routeID, effectID: effectID, effectKind: effectKind,
		payloadDigest: payloadDigest}, nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func (p SendPermit) Validate() error {
	_, err := BindDurableSend(p.provider, p.tenantID, p.userID, p.routeID,
		p.effectID, p.effectKind, p.payloadDigest)
	return err
}

func (p SendPermit) Provider() Provider    { return p.provider }
func (p SendPermit) TenantID() int64       { return p.tenantID }
func (p SendPermit) UserID() int64         { return p.userID }
func (p SendPermit) RouteID() int64        { return p.routeID }
func (p SendPermit) EffectID() string      { return p.effectID }
func (p SendPermit) EffectKind() string    { return p.effectKind }
func (p SendPermit) PayloadDigest() string { return p.payloadDigest }

type ProviderObservation = pusheffect.ProviderObservation

// Adapter owns one provider boundary. It receives no secret and no mutable
// destination; both are resolved from its pinned runtime and the durable effect.
type Adapter interface {
	Provider() Provider
	Send(context.Context, SendPermit) (ProviderObservation, error)
}

type Dispatcher struct {
	adapters map[Provider]Adapter
}

func NewDispatcher(adapters ...Adapter) (*Dispatcher, error) {
	if len(adapters) == 0 {
		return nil, errors.New("channel runtime: no adapters configured")
	}
	d := &Dispatcher{adapters: make(map[Provider]Adapter, len(adapters))}
	for _, adapter := range adapters {
		if adapter == nil ||
			(reflect.ValueOf(adapter).Kind() == reflect.Pointer &&
				reflect.ValueOf(adapter).IsNil()) || !adapter.Provider().Valid() {
			return nil, errors.New("channel runtime: adapter is invalid")
		}
		if _, exists := d.adapters[adapter.Provider()]; exists {
			return nil, errors.New("channel runtime: duplicate adapter")
		}
		d.adapters[adapter.Provider()] = adapter
	}
	return d, nil
}

func (d *Dispatcher) Send(
	ctx context.Context, permit SendPermit,
) (ProviderObservation, error) {
	if d == nil || permit.Validate() != nil {
		return ProviderObservation{}, errors.New("channel runtime: send permit rejected")
	}
	adapter := d.adapters[permit.Provider()]
	if adapter == nil {
		return ProviderObservation{}, errors.New("channel runtime: adapter unavailable")
	}
	observation, err := adapter.Send(ctx, permit)
	if err == nil && observation.Disposition == "" {
		return ProviderObservation{}, errors.New(
			"channel runtime: adapter returned no provider observation")
	}
	if observation.Disposition != "" &&
		(strings.TrimSpace(observation.AppIdentity) == "" ||
			(observation.Disposition == pusheffect.AttemptSent &&
				strings.TrimSpace(observation.MessageID) == "")) {
		return ProviderObservation{}, errors.New(
			"channel runtime: provider observation lacks authority evidence")
	}
	if observation.Disposition != "" &&
		observation.Disposition != pusheffect.AttemptSent &&
		observation.Disposition != pusheffect.AttemptDefiniteNotSent &&
		observation.Disposition != pusheffect.AttemptAmbiguous {
		return ProviderObservation{}, errors.New("channel runtime: provider observation invalid")
	}
	return observation, err
}
