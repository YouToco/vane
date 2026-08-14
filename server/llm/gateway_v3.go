package llm

import (
	"context"
	"errors"
	"time"

	"github.com/YouToco/vane/types"
)

// GatewayReceiptSignerV3 is implemented by the separately authenticated
// gateway Store pool. Verifier key material never enters model/runtime DTOs.
type GatewayReceiptSignerV3 interface {
	PrepareResearchLLMGatewayReceiptV3(context.Context, types.ResearchLLMGatewayCallBindingV3) error
	ResearchLLMGatewayAttemptStartedV3(context.Context, types.ResearchLLMGatewayCallBindingV3) (types.ResearchLLMGatewayAttemptStateV3, error)
	MarkResearchLLMGatewaySendStartedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error)
	MarkResearchLLMGatewayPreSendRejectedV3(context.Context, types.ResearchLLMGatewaySendIntentV3) (bool, error)
	FinalizeMeasuredResearchLLMGatewayReceiptV3(context.Context, types.ResearchLLMGatewayCallBindingV3, types.ResearchLLMGatewayReceiptV3) (types.ResearchLLMGatewayReceiptV3, error)
	SignConservativeResearchLLMGatewayRecoveryV3(context.Context, types.ResearchLLMGatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error)
	SignConfirmedZeroResearchLLMGatewayRecoveryV3(context.Context, types.ResearchLLMGatewayCallBindingV3) (types.ResearchLLMGatewayReceiptV3, error)
}

type MeasuredGatewayV3 struct {
	client *Client
	signer GatewayReceiptSignerV3
	now    func() time.Time
}

func NewMeasuredGatewayV3(client *Client, signer GatewayReceiptSignerV3) (*MeasuredGatewayV3, error) {
	if client == nil || signer == nil {
		return nil, errors.New("llm: measured gateway requires client and signer")
	}
	return &MeasuredGatewayV3{client: client, signer: signer, now: time.Now}, nil
}

type GatewayCallBindingV3 = types.ResearchLLMGatewayCallBindingV3

// DoMeasured performs exactly one provider call and signs only the resulting
// measured bytes/usage. An attempted call with missing usage is deliberately
// attested as indeterminate; recovery must retain the reservation and must not
// resend merely because a signed terminal receipt is absent.
func (g *MeasuredGatewayV3) DoMeasured(
	ctx context.Context, binding GatewayCallBindingV3, meta CallMeta, req Request,
) (types.ResearchLLMGatewayReceiptV3, error) {
	if err := g.signer.PrepareResearchLLMGatewayReceiptV3(ctx, binding); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	state, err := g.signer.ResearchLLMGatewayAttemptStartedV3(ctx, binding)
	if err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	if state == types.ResearchLLMGatewayAttemptSendStartedV3 {
		r, recoveryErr := g.signer.SignConservativeResearchLLMGatewayRecoveryV3(ctx, binding)
		if recoveryErr != nil {
			return r, types.NewAppError(types.CodeInternal, "gateway send already started; do not retry provider", recoveryErr)
		}
		return r, nil
	}
	if state == types.ResearchLLMGatewayAttemptPreSendRejectedV3 {
		return g.signer.SignConfirmedZeroResearchLLMGatewayRecoveryV3(ctx, binding)
	}
	claimLost := false
	callerBeforeSend := req.BeforeSend
	req.BeforeSend = func(sendCtx context.Context) error {
		if callerBeforeSend != nil {
			if err := callerBeforeSend(sendCtx); err != nil {
				return err
			}
		}
		intentCall := measuredLLMCallV3(sendCtx, g.client, meta, req, nil, nil, time.Now())
		first, err := g.signer.MarkResearchLLMGatewaySendStartedV3(sendCtx,
			types.ResearchLLMGatewaySendIntentV3{Binding: binding, Call: intentCall,
				DisableThinking: req.DisableThinking})
		if err != nil {
			return err
		}
		if !first {
			claimLost = true
			return errors.New("gateway send already claimed")
		}
		return nil
	}
	measured, callErr := DoMeasuredV3(ctx, g.client, meta, req)
	if claimLost {
		r, recoveryErr := g.signer.SignConservativeResearchLLMGatewayRecoveryV3(ctx, binding)
		if recoveryErr != nil {
			return r, types.NewAppError(types.CodeInternal, "gateway send claimed concurrently; do not retry provider", recoveryErr)
		}
		return r, nil
	}
	outcome := "failed"
	if callErr == nil {
		outcome = "completed"
	} else if measured.Attempted && !measured.UsageKnown && !measured.DefinitelyZeroUsage {
		outcome = "indeterminate"
	}
	receipt := types.ResearchLLMGatewayReceiptV3{
		SchemaVersion:      types.ResearchLLMGatewayReceiptSchemaV3,
		SignedAtUnixMillis: g.now().UTC().UnixMilli(),
		ReservationID:      binding.ReservationID,
		RequestDigest:      binding.RequestDigest,
		Call:               measured.Call, DisableThinking: measured.DisableThinking,
		Attempted: measured.Attempted, UsageKnown: measured.UsageKnown,
		DefinitelyZeroUsage: measured.DefinitelyZeroUsage,
		Outcome:             outcome,
	}
	if callErr != nil {
		receipt.ErrorCode = string(types.CodeOf(callErr))
	}
	if !measured.Attempted {
		intentCall := measured.Call
		first, markErr := g.signer.MarkResearchLLMGatewayPreSendRejectedV3(ctx,
			types.ResearchLLMGatewaySendIntentV3{Binding: binding, Call: intentCall,
				DisableThinking: req.DisableThinking})
		if markErr != nil {
			return types.ResearchLLMGatewayReceiptV3{}, markErr
		}
		if !first {
			return g.signer.SignConfirmedZeroResearchLLMGatewayRecoveryV3(ctx, binding)
		}
	}
	signed, signErr := g.signer.FinalizeMeasuredResearchLLMGatewayReceiptV3(ctx, binding, receipt)
	if signErr != nil {
		if measured.Attempted {
			// The durable send marker forbids provider retry. Return the unsigned
			// measured facts for diagnostics together with a non-retryable error;
			// recovery leaves the reservation indeterminate.
			return receipt, types.NewAppError(types.CodeInternal,
				"gateway signature unavailable after provider send; do not retry", signErr)
		}
		return types.ResearchLLMGatewayReceiptV3{}, signErr
	}
	return signed, callErr
}
