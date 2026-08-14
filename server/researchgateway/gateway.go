package researchgateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

const ExecutePathV1 = "/v1/research/llm/execute"

type ExecuteRequestV1 struct {
	ReservationID int64  `json:"reservation_id"`
	RequestDigest string `json:"request_digest"`
	RunCapability string `json:"run_capability"`
}

func (r ExecuteRequestV1) Validate() error {
	if r.ReservationID <= 0 || len(r.RequestDigest) != sha256.Size*2 ||
		len(r.RunCapability) != sha256.Size*2 || strings.ToLower(r.RequestDigest) != r.RequestDigest ||
		strings.ToLower(r.RunCapability) != r.RunCapability {
		return errors.New("research gateway request binding is invalid")
	}
	if _, err := hex.DecodeString(r.RequestDigest); err != nil {
		return errors.New("research gateway request digest is invalid")
	}
	if _, err := hex.DecodeString(r.RunCapability); err != nil {
		return errors.New("research gateway run capability is invalid")
	}
	return nil
}

type ExecuteStatusV1 string

const (
	StatusSettledV1  ExecuteStatusV1 = "settled"
	StatusInFlightV1 ExecuteStatusV1 = "in_flight"
)

type ExecuteResponseV1 struct {
	Status ExecuteStatusV1 `json:"status"`
}

// FrozenRequestV1 never crosses the gateway HTTP boundary. It is reconstructed
// from the reservation and run capability by the gateway-only database role.
type FrozenRequestV1 struct {
	ReservationID   int64
	RequestDigest   string
	RunSnapshotID   int64
	TenantID        int64
	UserID          int64
	TaskID          string
	TraceID         string
	Stage           string
	SystemPrompt    string
	UserPrompt      string
	Provider        runtimepolicy.ModelProviderIDV1
	Endpoint        runtimepolicy.EndpointRefV1
	CredentialRef   runtimepolicy.CredentialRefV1
	Model           string
	Temperature     float32
	MaxTokens       int
	DisableThinking bool
}

type ClaimV1 struct {
	FirstWriter bool
	Settled     bool
	Request     FrozenRequestV1
}

type SettlementV1 struct {
	Measured  llm.MeasuredCallV3
	Outcome   string
	ErrorCode string
}

type RepositoryV1 interface {
	Claim(context.Context, ExecuteRequestV1) (ClaimV1, error)
	Recover(context.Context, ExecuteRequestV1) (bool, error)
	Settle(context.Context, ExecuteRequestV1, FrozenRequestV1, SettlementV1) error
}

type ProviderV1 interface {
	Complete(context.Context, FrozenRequestV1) (llm.MeasuredCallV3, error)
}

type ServiceV1 struct {
	repository RepositoryV1
	provider   ProviderV1
	inflight   sync.WaitGroup
}

const settlementAttemptsV1 = 5

func NewServiceV1(repository RepositoryV1, provider ProviderV1) (*ServiceV1, error) {
	if repository == nil || provider == nil {
		return nil, errors.New("research gateway requires repository and provider")
	}
	return &ServiceV1{repository: repository, provider: provider}, nil
}

func (s *ServiceV1) Execute(ctx context.Context, binding ExecuteRequestV1) (ExecuteResponseV1, error) {
	if err := binding.Validate(); err != nil {
		return ExecuteResponseV1{}, err
	}
	s.inflight.Add(1)
	defer s.inflight.Done()
	claim, err := s.repository.Claim(ctx, binding)
	if err != nil {
		return ExecuteResponseV1{}, err
	}
	if !claim.FirstWriter {
		if claim.Settled {
			return ExecuteResponseV1{Status: StatusSettledV1}, nil
		}
		recovered, recoverErr := s.repository.Recover(ctx, binding)
		if recoverErr != nil {
			return ExecuteResponseV1{}, recoverErr
		}
		if recovered {
			return ExecuteResponseV1{Status: StatusSettledV1}, nil
		}
		return ExecuteResponseV1{Status: StatusInFlightV1}, nil
	}
	measured, callErr := s.provider.Complete(ctx, claim.Request)
	outcome := "completed"
	if callErr != nil {
		outcome = "failed"
		if measured.Attempted && !measured.UsageKnown && !measured.DefinitelyZeroUsage {
			outcome = "indeterminate"
		}
	}
	settlement := SettlementV1{Measured: measured, Outcome: outcome}
	if callErr != nil {
		settlement.ErrorCode = string(types.CodeOf(callErr))
	}
	var settleErr error
	for attempt := 0; attempt < settlementAttemptsV1; attempt++ {
		settleErr = s.repository.Settle(ctx, binding, claim.Request, settlement)
		if settleErr == nil {
			break
		}
		delay := 200 * time.Millisecond * time.Duration(1<<attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ExecuteResponseV1{}, fmt.Errorf("research gateway settle: %w", ctx.Err())
		case <-timer.C:
		}
	}
	if settleErr != nil {
		return ExecuteResponseV1{}, fmt.Errorf("research gateway settle: %w", settleErr)
	}
	return ExecuteResponseV1{Status: StatusSettledV1}, nil
}

func (s *ServiceV1) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.inflight.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type LLMProviderV1 struct{ Resolver *llm.RuntimeModelResolverV1 }

func (p LLMProviderV1) Complete(ctx context.Context, frozen FrozenRequestV1) (llm.MeasuredCallV3, error) {
	if p.Resolver == nil {
		return llm.MeasuredCallV3{}, errors.New("research gateway provider is unavailable")
	}
	client, err := p.Resolver.ResolveRuntimeModelRouteV1(
		frozen.Provider, frozen.Endpoint, frozen.CredentialRef)
	if err != nil {
		// Resolution happens before DoMeasuredV3 and therefore before any
		// Provider network effect. Missing retained generations fail closed.
		return llm.MeasuredCallV3{}, err
	}
	temperature, maxTokens := frozen.Temperature, frozen.MaxTokens
	return llm.DoMeasuredV3(ctx, client, llm.CallMeta{
		TenantID: &frozen.TenantID, UserID: &frozen.UserID, TraceID: frozen.TraceID,
		SpanName: frozen.Stage, RefType: "research_run_snapshot", RefID: &frozen.RunSnapshotID,
	}, llm.Request{System: frozen.SystemPrompt, User: frozen.UserPrompt,
		Model: frozen.Model, Temperature: &temperature, MaxTokens: &maxTokens,
		DisableThinking: frozen.DisableThinking})
}

func (s *ServiceV1) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(ExecutePathV1, s.handleExecute)
	return mux
}
