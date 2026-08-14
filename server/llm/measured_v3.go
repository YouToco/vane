package llm

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/YouToco/vane/server/types"
)

// MeasuredCallV3 is the side-effect-free accounting result of exactly one
// Client.Complete call. It deliberately does not reserve quota, invoke a
// Recorder, or persist anything: a V3 coordinator must atomically persist the
// returned receipt with its own durable spend claim.
//
// Attempted means the request crossed Client's BeforeSend gate. It is a
// conservative provider-effect boundary: the provider may have observed the
// request, even when the transport later reports an error. DefinitelyNotAttempted
// means that boundary was never crossed. UsageKnown means Response contains the
// provider usage metadata accepted by Client. DefinitelyZeroUsage covers local
// pre-send rejection and explicit HTTP 4xx/429 rejection; otherwise an attempted
// call with unknown usage must conservatively retain its reservation.
type MeasuredCallV3 struct {
	Response               *Response
	Call                   types.LLMCall
	Attempted              bool
	UsageKnown             bool
	DefinitelyNotAttempted bool
	DefinitelyZeroUsage    bool
	DisableThinking        bool
}

// DoMeasuredV3 performs exactly one Client.Complete call and returns the exact
// LLMCall projection that the legacy Do path would have handed to Recorder.
// Persistence and quota settlement are intentionally the caller's responsibility.
func DoMeasuredV3(
	ctx context.Context,
	c *Client,
	meta CallMeta,
	req Request,
) (MeasuredCallV3, error) {
	var crossedSendGate atomic.Bool
	callerBeforeSend := req.BeforeSend
	req.BeforeSend = func(sendCtx context.Context) error {
		if callerBeforeSend != nil {
			if err := callerBeforeSend(sendCtx); err != nil {
				return err
			}
		}
		// Complete calls this only after acquiring its semaphore and validating
		// the payload, immediately before constructing and sending HTTP.
		crossedSendGate.Store(true)
		return nil
	}

	start := time.Now()
	resp, err := c.Complete(ctx, req)
	call := measuredLLMCallV3(ctx, c, meta, req, resp, err, start)
	attempted := crossedSendGate.Load()
	usageKnown := resp != nil && resp.UsageReported
	definitelyNotAttempted := !attempted
	definitelyZeroUsage := definitelyNotAttempted || (!usageKnown &&
		(types.CodeOf(err) == types.CodeLLMBadRequest ||
			types.CodeOf(err) == types.CodeLLMRateLimit))

	return MeasuredCallV3{
		Response:               resp,
		Call:                   call,
		Attempted:              attempted,
		UsageKnown:             usageKnown,
		DefinitelyNotAttempted: definitelyNotAttempted,
		DefinitelyZeroUsage:    definitelyZeroUsage,
		DisableThinking:        req.DisableThinking,
	}, err
}

func measuredLLMCallV3(
	ctx context.Context,
	c *Client,
	meta CallMeta,
	req Request,
	resp *Response,
	err error,
	start time.Time,
) types.LLMCall {
	call := types.LLMCall{
		RunSnapshotID: runSnapshotAttribution(ctx),
		TenantID:      meta.TenantID,
		TraceID:       meta.TraceID,
		SpanName:      meta.SpanName,
		UserID:        meta.UserID,
		RefType:       meta.RefType,
		RefID:         meta.RefID,
		Provider:      c.provider,
		Model:         c.requestModel(req.Model),
		SystemPrompt:  req.System,
		UserPrompt:    req.User,
		Temperature:   req.Temperature,
		MaxTokens:     req.MaxTokens,
	}
	if resp != nil {
		call.Model = resp.Model
		call.PromptTokens = resp.PromptTokens
		call.CompletionTokens = resp.CompletionTokens
		call.LatencyMs = resp.LatencyMs
		if resp.CacheTokensReported {
			hitTokens, missTokens := resp.CacheHitTokens, resp.CacheMissTokens
			call.PromptCacheHitTokens = &hitTokens
			call.PromptCacheMissTokens = &missTokens
			hit := hitTokens > 0
			call.PrefixCacheHit = &hit
		}
		if resp.ReasoningTokensReported {
			reasoningTokens := resp.ReasoningTokens
			call.ReasoningTokens = &reasoningTokens
		}
	}
	if err != nil {
		call.Error = err.Error()
		if resp == nil {
			call.LatencyMs = int(time.Since(start).Milliseconds())
		}
	} else if resp != nil {
		call.Completion = resp.Content
	}
	return call
}
