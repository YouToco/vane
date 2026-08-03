package fetcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// ResearchExecutionStatusV3 is deliberately independent from provider and
// Temporal retry terminology. In particular, indeterminate means an upstream
// effect may have happened but Vane cannot prove a complete, billable result;
// callers must persist that state and must not call the provider again.
type ResearchExecutionStatusV3 string

const (
	ResearchExecutionSuccessV3         ResearchExecutionStatusV3 = "success"
	ResearchExecutionDefiniteFailureV3 ResearchExecutionStatusV3 = "definite_failure"
	ResearchExecutionIndeterminateV3   ResearchExecutionStatusV3 = "indeterminate"
)

type ResearchExecutionErrorCodeV3 string

const (
	ResearchExecutionInvalidRequestV3       ResearchExecutionErrorCodeV3 = "invalid_request"
	ResearchExecutionRouteUnavailableV3     ResearchExecutionErrorCodeV3 = "route_unavailable"
	ResearchExecutionRecoveryNoReplayV3     ResearchExecutionErrorCodeV3 = "recovery_no_provider_replay"
	ResearchExecutionEffectDeniedV3         ResearchExecutionErrorCodeV3 = "effect_denied"
	ResearchExecutionProviderRejectedV3     ResearchExecutionErrorCodeV3 = "provider_rejected"
	ResearchExecutionProviderReportedV3     ResearchExecutionErrorCodeV3 = "provider_reported_failure"
	ResearchExecutionProviderUncertainV3    ResearchExecutionErrorCodeV3 = "provider_outcome_uncertain"
	ResearchExecutionProviderTruncatedV3    ResearchExecutionErrorCodeV3 = "provider_result_truncated"
	ResearchExecutionProviderReceiptV3      ResearchExecutionErrorCodeV3 = "provider_receipt_unavailable"
	ResearchExecutionProviderCostUnknownV3  ResearchExecutionErrorCodeV3 = "provider_cost_unknown"
	ResearchExecutionProviderCostExceededV3 ResearchExecutionErrorCodeV3 = "provider_cost_exceeded"
	ResearchExecutionProviderUsageUnknownV3 ResearchExecutionErrorCodeV3 = "provider_usage_unknown"
)

// ResearchExecutionRequestV3 contains the already-frozen Tool grant and
// canonical arguments. FirstWriter is mandatory: a recovery path that did not
// create the immutable started step must load the durable receipt and never
// enter this network executor. Store.BeginResearchRunStepV3 is the single
// durable effect gate; the executor deliberately has no second callback that
// could disagree with or bypass that claim.
type ResearchExecutionRequestV3 struct {
	FirstWriter   bool                                   `json:"first_writer"`
	Identity      types.RunIdentity                      `json:"identity"`
	RunSnapshotID int64                                  `json:"run_snapshot_id"`
	PlanDigest    string                                 `json:"plan_digest"`
	Ordinal       int                                    `json:"ordinal"`
	InvocationID  string                                 `json:"invocation_id"`
	Tool          runtimepolicy.ResearchToolDefinitionV3 `json:"tool"`
	Arguments     json.RawMessage                        `json:"arguments"`
}

// ResearchExecutionReceiptV3 is the complete provider-side receipt returned
// to the coordinator before Store persistence. A zero CostMicroUSD is only a
// real value when CostKnown is true. Provider error prose is intentionally not
// present and therefore can never be mistaken for model-visible evidence.
type ResearchExecutionReceiptV3 struct {
	Status               ResearchExecutionStatusV3    `json:"status"`
	TraceID              string                       `json:"trace_id,omitempty"`
	Provider             string                       `json:"provider,omitempty"`
	Attempted            bool                         `json:"attempted"`
	UsageQuantity        float64                      `json:"usage_quantity"`
	UsageKnown           bool                         `json:"usage_known"`
	CostMicroUSD         int64                        `json:"cost_micro_usd"`
	CostKnown            bool                         `json:"cost_known"`
	ProviderTruncated    bool                         `json:"provider_truncated"`
	HTTPStatus           *int                         `json:"http_status,omitempty"`
	DurationMS           int                          `json:"duration_ms"`
	Result               []byte                       `json:"result,omitempty"`
	NormalizedResultSize int                          `json:"normalized_result_size"`
	ModelResultTruncated bool                         `json:"model_result_truncated"`
	ResultDigest         string                       `json:"result_digest,omitempty"`
	ErrorCode            ResearchExecutionErrorCodeV3 `json:"error_code,omitempty"`
}

// Validate rejects impossible combinations before the coordinator persists a
// receipt. Ownership and invocation binding remain the Store step's job.
func (r ResearchExecutionReceiptV3) Validate() error {
	if r.Provider != "" && r.Provider != "exa" {
		return types.NewAppError(types.CodeValidation,
			"research V3 provider receipt is invalid", nil)
	}
	if r.DurationMS < 0 || r.DurationMS > 86_400_000 ||
		(r.Attempted && !validResearchDigestV3(r.TraceID)) ||
		(!r.Attempted && (r.TraceID != "" || r.DurationMS != 0)) {
		return types.NewAppError(types.CodeValidation,
			"research V3 provider trace receipt is invalid", nil)
	}
	if r.HTTPStatus != nil && (*r.HTTPStatus < 100 || *r.HTTPStatus > 599) {
		return types.NewAppError(types.CodeValidation,
			"research V3 provider status is invalid", nil)
	}
	if (!r.UsageKnown && r.UsageQuantity != 0) ||
		(r.UsageKnown && !finitePositiveV3(r.UsageQuantity)) ||
		(!r.CostKnown && r.CostMicroUSD != 0) ||
		(r.CostKnown && r.CostMicroUSD < 0) ||
		(r.ProviderTruncated && !r.Attempted) ||
		(r.ModelResultTruncated != (r.NormalizedResultSize > len(r.Result))) {
		return types.NewAppError(types.CodeValidation,
			"research V3 provider accounting receipt is invalid", nil)
	}
	switch r.Status {
	case ResearchExecutionSuccessV3:
		resultSum := sha256.Sum256(r.Result)
		if r.Provider != "exa" || !r.Attempted || !r.UsageKnown ||
			!finitePositiveV3(r.UsageQuantity) || !r.CostKnown || r.CostMicroUSD < 0 ||
			r.ProviderTruncated || r.HTTPStatus == nil || *r.HTTPStatus < 200 ||
			*r.HTTPStatus >= 300 || len(r.Result) == 0 || !utf8.Valid(r.Result) ||
			r.NormalizedResultSize < len(r.Result) || r.ResultDigest == "" ||
			bytes.IndexByte(r.Result, 0) >= 0 || r.ErrorCode != "" ||
			!validResearchDigestV3(r.ResultDigest) ||
			r.ResultDigest != hex.EncodeToString(resultSum[:]) {
			return types.NewAppError(types.CodeValidation,
				"research V3 success receipt is invalid", nil)
		}
	case ResearchExecutionDefiniteFailureV3, ResearchExecutionIndeterminateV3:
		if r.ErrorCode == "" || len(r.Result) != 0 || r.ResultDigest != "" ||
			r.NormalizedResultSize != 0 || r.ModelResultTruncated ||
			(r.Status == ResearchExecutionIndeterminateV3 && !r.Attempted) {
			return types.NewAppError(types.CodeValidation,
				"research V3 failure receipt is invalid", nil)
		}
	default:
		return types.NewAppError(types.CodeValidation,
			"research V3 execution status is invalid", nil)
	}
	return nil
}

// ResearchExecutorV3 is a scheduled-only, one-attempt Exa adapter. It does not
// call the interactive Agent loop and contains no application retry. The two
// retained provider leaves use POST, so net/http does not transparently replay
// them as an idempotent GET after a partial transport failure.
type ResearchExecutorV3 struct {
	exaGeneration int64
	search        *ExaFetcher
	contents      *ExaContentsFetcher
	recorder      *researchReceiptRecorderV3
}

// NewResearchExecutorV3 constructs dedicated retained Exa leaves so their
// accounting receipts cannot be confused with interactive or legacy fetches.
func NewResearchExecutorV3(cfg config.FetchConfig) (*ResearchExecutorV3, error) {
	generation := cfg.CompiledExaCredentialGeneration
	if generation == 0 {
		generation = runtimepolicy.PrimaryGenerationV1
	}
	if generation <= 0 || strings.TrimSpace(cfg.ExaAPIKey) == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"research V3 Exa retained credential is unavailable", nil)
	}
	recorder := newResearchReceiptRecorderV3()
	return &ResearchExecutorV3{
		exaGeneration: generation,
		search:        NewExa(cfg, recorder),
		contents:      NewExaContents(cfg, recorder),
		recorder:      recorder,
	}, nil
}

// ExecuteOnceV3 performs at most one provider request. It always returns a
// typed receipt and never returns provider prose as a successful Result.
func (e *ResearchExecutorV3) ExecuteOnceV3(
	ctx context.Context,
	req ResearchExecutionRequestV3,
) ResearchExecutionReceiptV3 {
	if !req.FirstWriter {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "", false,
			ResearchExecutionRecoveryNoReplayV3)
	}
	if e == nil || e.recorder == nil || e.search == nil || e.contents == nil ||
		req.Identity.Validate() != nil || req.RunSnapshotID <= 0 ||
		!validResearchDigestV3(req.PlanDigest) ||
		!validResearchInvocationV3(req.InvocationID) {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "", false,
			ResearchExecutionInvalidRequestV3)
	}
	if !e.matchesFrozenRouteV3(req.Tool) {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "", false,
			ResearchExecutionRouteUnavailableV3)
	}
	canonical, err := acquisitiontool.CanonicalizeToolArgumentsV1(
		req.Tool.Name, req.Arguments)
	if err != nil || !bytes.Equal(canonical, req.Arguments) {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "exa", false,
			ResearchExecutionInvalidRequestV3)
	}
	target, err := acquisitiontool.MaterializeApprovedToolCallV1(
		req.Tool.Name, "v1", canonical)
	if err != nil || target == nil {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "exa", false,
			ResearchExecutionInvalidRequestV3)
	}

	callKey := researchCallKeyV3(req)
	if callKey == "" {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "exa", false,
			ResearchExecutionInvalidRequestV3)
	}
	callKey, ok := e.recorder.begin(callKey)
	if !ok {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "exa", false,
			ResearchExecutionRecoveryNoReplayV3)
	}
	defer e.recorder.end(callKey)

	admitted := false
	gate := func(context.Context) error {
		admitted = true
		return nil
	}
	callCtx := WithBindingRunAttribution(
		ctx, callKey, req.Identity.TenantID, req.Identity.UserID, req.RunSnapshotID)
	var items []types.ContentItem
	switch req.Tool.Implementation {
	case runtimepolicy.ResearchToolExaSearchV3:
		items, err = e.search.fetchWithEffectGate(callCtx, *target, gate)
	case runtimepolicy.ResearchToolExaContentsV3:
		items, err = e.contents.fetchWithEffectGate(callCtx, *target, gate)
	default:
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "exa", false,
			ResearchExecutionRouteUnavailableV3)
	}
	providerReceipt, recorded := e.recorder.take(callKey)
	if recorded && !researchReceiptMatchesRequestV3(providerReceipt, req, callKey) {
		recorded = false
	}
	if err != nil {
		if !admitted {
			return researchFailureReceiptV3(
				ResearchExecutionDefiniteFailureV3, "exa", false,
				ResearchExecutionEffectDeniedV3)
		}
		return e.failedProviderReceiptV3(providerReceipt, recorded, callKey, err)
	}
	if !admitted {
		return researchFailureReceiptV3(
			ResearchExecutionDefiniteFailureV3, "exa", false,
			ResearchExecutionEffectDeniedV3)
	}
	return e.successProviderReceiptV3(req.Tool, items, providerReceipt, recorded, callKey)
}

func (e *ResearchExecutorV3) matchesFrozenRouteV3(
	tool runtimepolicy.ResearchToolDefinitionV3,
) bool {
	policy, err := runtimepolicy.BuildResearchToolPolicyV3(
		[]runtimepolicy.ResearchToolDefinitionV3{tool})
	if err != nil || len(policy.AllowedTools) != 1 ||
		tool.ImplementationGeneration != 1 || tool.Provider != "exa" ||
		tool.CredentialRef.ID != runtimepolicy.CredentialIDExaPrimaryV1 ||
		tool.CredentialRef.Generation != e.exaGeneration {
		return false
	}
	original, originalErr := json.Marshal(tool)
	normalized, normalizedErr := json.Marshal(policy.AllowedTools[0])
	return originalErr == nil && normalizedErr == nil && bytes.Equal(original, normalized)
}

func (e *ResearchExecutorV3) failedProviderReceiptV3(
	call types.ToolCall, recorded bool, callKey string, providerErr error,
) ResearchExecutionReceiptV3 {
	if !recorded {
		receipt := researchFailureReceiptV3(
			ResearchExecutionIndeterminateV3, "exa", true,
			ResearchExecutionProviderReceiptV3)
		receipt.TraceID = callKey
		return receipt
	}
	receipt := researchReceiptFromCallV3(call)
	receipt.Status = ResearchExecutionIndeterminateV3
	receipt.ErrorCode = ResearchExecutionProviderUncertainV3
	if errors.Is(providerErr, ErrPageUnreachable) {
		receipt.Status = ResearchExecutionDefiniteFailureV3
		receipt.ErrorCode = ResearchExecutionProviderReportedV3
	} else if call.ResultSize > e.providerBodyCapV3(call.ToolName) {
		receipt.ProviderTruncated = true
		receipt.ErrorCode = ResearchExecutionProviderTruncatedV3
	} else if call.HTTPStatus != nil && *call.HTTPStatus >= 400 && *call.HTTPStatus < 500 {
		receipt.Status = ResearchExecutionDefiniteFailureV3
		receipt.ErrorCode = ResearchExecutionProviderRejectedV3
	}
	return receipt
}

func (e *ResearchExecutorV3) successProviderReceiptV3(
	tool runtimepolicy.ResearchToolDefinitionV3,
	items []types.ContentItem,
	call types.ToolCall,
	recorded bool,
	callKey string,
) ResearchExecutionReceiptV3 {
	if !recorded {
		receipt := researchFailureReceiptV3(
			ResearchExecutionIndeterminateV3, "exa", true,
			ResearchExecutionProviderReceiptV3)
		receipt.TraceID = callKey
		return receipt
	}
	receipt := researchReceiptFromCallV3(call)
	if call.ResultSize > e.providerBodyCapV3(call.ToolName) {
		receipt.Status = ResearchExecutionIndeterminateV3
		receipt.ProviderTruncated = true
		receipt.ErrorCode = ResearchExecutionProviderTruncatedV3
		return receipt
	}
	if call.HTTPStatus == nil || *call.HTTPStatus < http.StatusOK ||
		*call.HTTPStatus >= http.StatusMultipleChoices {
		receipt.Status = ResearchExecutionIndeterminateV3
		receipt.ErrorCode = ResearchExecutionProviderReceiptV3
		return receipt
	}
	if !receipt.UsageKnown {
		receipt.Status = ResearchExecutionIndeterminateV3
		receipt.ErrorCode = ResearchExecutionProviderUsageUnknownV3
		return receipt
	}
	if !receipt.CostKnown {
		receipt.Status = ResearchExecutionIndeterminateV3
		receipt.ErrorCode = ResearchExecutionProviderCostUnknownV3
		return receipt
	}
	if receipt.CostMicroUSD > tool.MaxCostMicroUSD {
		receipt.Status = ResearchExecutionIndeterminateV3
		receipt.ErrorCode = ResearchExecutionProviderCostExceededV3
		return receipt
	}
	payload, err := json.Marshal(researchDocumentsV3(items))
	if err != nil {
		receipt.Status = ResearchExecutionIndeterminateV3
		receipt.ErrorCode = ResearchExecutionProviderReceiptV3
		return receipt
	}
	visible := types.NormalizeModelVisibleToolResult(payload)
	receipt.Status = ResearchExecutionSuccessV3
	receipt.Result = visible.Visible
	receipt.NormalizedResultSize = visible.NormalizedSize
	receipt.ModelResultTruncated = visible.Truncated
	receipt.ResultDigest = visible.Digest
	return receipt
}

func (e *ResearchExecutorV3) providerBodyCapV3(toolName string) int {
	switch toolName {
	case "exa:search":
		return int(e.search.maxBytes)
	case "exa:contents":
		return int(e.contents.maxBytes)
	default:
		return 0
	}
}

type researchDocumentV3 struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	PublishedAt string `json:"published_at,omitempty"`
	Author      string `json:"author,omitempty"`
	Text        string `json:"text"`
}

func researchDocumentsV3(items []types.ContentItem) []researchDocumentV3 {
	out := make([]researchDocumentV3, 0, len(items))
	for _, item := range items {
		document := researchDocumentV3{
			Title: item.Title, URL: item.URL, Author: item.Author, Text: item.Content,
		}
		if item.PublishedAt != nil {
			document.PublishedAt = item.PublishedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
		}
		out = append(out, document)
	}
	return out
}

func researchReceiptFromCallV3(call types.ToolCall) ResearchExecutionReceiptV3 {
	receipt := ResearchExecutionReceiptV3{
		TraceID: call.TraceID, Provider: "exa", Attempted: true,
		UsageQuantity: call.UsageQuantity,
		UsageKnown:    finitePositiveV3(call.UsageQuantity),
		DurationMS:    call.DurationMs,
	}
	if call.HTTPStatus != nil && *call.HTTPStatus > 0 {
		status := *call.HTTPStatus
		receipt.HTTPStatus = &status
	}
	if call.CostUSD != nil && finitePositiveV3(*call.CostUSD) &&
		*call.CostUSD <= float64(math.MaxInt64)/1_000_000 {
		receipt.CostKnown = true
		// Round upward at the micro-dollar boundary so admission never
		// understates a positive provider charge because of float conversion.
		receipt.CostMicroUSD = int64(math.Ceil(*call.CostUSD * 1_000_000))
	}
	return receipt
}

func researchFailureReceiptV3(
	status ResearchExecutionStatusV3,
	provider string,
	attempted bool,
	code ResearchExecutionErrorCodeV3,
) ResearchExecutionReceiptV3 {
	return ResearchExecutionReceiptV3{
		Status: status, Provider: provider, Attempted: attempted, ErrorCode: code,
	}
}

func finitePositiveV3(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validResearchInvocationV3(value string) bool {
	return value != "" && len(value) <= 255 && utf8.ValidString(value) &&
		!strings.ContainsRune(value, 0) && strings.TrimSpace(value) == value
}

func validResearchDigestV3(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// ResearchExecutionTraceV3 derives the immutable provider-call claim shared by
// the network executor and the persistence coordinator. A receipt is valid for
// exactly one authenticated run snapshot, frozen plan, and invocation.
func ResearchExecutionTraceV3(
	identity types.RunIdentity,
	runSnapshotID int64,
	planDigest string,
	ordinal int,
	invocationID string,
) (string, error) {
	return runcontext.ResearchExecutionTraceV3(
		identity, runSnapshotID, planDigest, ordinal, invocationID)
}

func researchCallKeyV3(req ResearchExecutionRequestV3) string {
	traceID, err := ResearchExecutionTraceV3(
		req.Identity, req.RunSnapshotID, req.PlanDigest, req.Ordinal, req.InvocationID)
	if err != nil {
		return ""
	}
	return traceID
}

func researchReceiptMatchesRequestV3(
	call types.ToolCall,
	req ResearchExecutionRequestV3,
	callKey string,
) bool {
	expectedToolName := ""
	switch req.Tool.Implementation {
	case runtimepolicy.ResearchToolExaSearchV3:
		expectedToolName = "exa:search"
	case runtimepolicy.ResearchToolExaContentsV3:
		expectedToolName = "exa:contents"
	}
	return call.TraceID == callKey && call.TenantID != nil &&
		*call.TenantID == req.Identity.TenantID && call.UserID != nil &&
		*call.UserID == req.Identity.UserID && call.RunSnapshotID != nil &&
		*call.RunSnapshotID == req.RunSnapshotID && expectedToolName != "" &&
		call.ToolName == expectedToolName
}

// researchReceiptRecorderV3 is private to dedicated V3 leaves. begin/end make
// concurrent use of one invocation ID fail before an effect. Durable replay
// prevention is the coordinator's first-writer Store boundary; keeping a
// process-lifetime used-ID set here would both leak memory and collide across
// separate runs whose plans legitimately reuse local invocation names.
type researchReceiptRecorderV3 struct {
	mu       sync.Mutex
	active   map[string]struct{}
	receipts map[string]types.ToolCall
}

func newResearchReceiptRecorderV3() *researchReceiptRecorderV3 {
	return &researchReceiptRecorderV3{
		active:   make(map[string]struct{}),
		receipts: make(map[string]types.ToolCall),
	}
}

func (r *researchReceiptRecorderV3) begin(invocationID string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.active[invocationID]; exists {
		return "", false
	}
	r.active[invocationID] = struct{}{}
	return invocationID, true
}

func (r *researchReceiptRecorderV3) end(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active, key)
	delete(r.receipts, key)
}

func (r *researchReceiptRecorderV3) take(key string) (types.ToolCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	receipt, ok := r.receipts[key]
	if ok {
		delete(r.receipts, key)
	}
	return receipt, ok
}

func (r *researchReceiptRecorderV3) RecordBindingCall(
	ctx context.Context,
	rec *types.ToolCall,
) {
	if rec == nil {
		return
	}
	trace, _, _ := BindingAttributionFromContext(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, active := r.active[trace]; !active {
		return
	}
	copy := *rec
	copy.Arguments = bytes.Clone(rec.Arguments)
	if rec.HTTPStatus != nil {
		status := *rec.HTTPStatus
		copy.HTTPStatus = &status
	}
	if rec.CostUSD != nil {
		cost := *rec.CostUSD
		copy.CostUSD = &cost
	}
	r.receipts[trace] = copy
}
