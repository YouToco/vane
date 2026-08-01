package types

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
)

const ResearchLLMGatewayReceiptSchemaV3 = "vane.research-llm-gateway-receipt/v1"

type ResearchLLMGatewayCallBindingV3 struct {
	ReservationID int64
	RequestDigest string
	Identity      RunIdentity
	SnapshotRef   ResearchRunSnapshotRefV3
}

type ResearchLLMGatewaySendIntentV3 struct {
	Binding         ResearchLLMGatewayCallBindingV3
	Call            LLMCall
	DisableThinking bool
}

type ResearchLLMGatewayAttemptStateV3 string

const (
	ResearchLLMGatewayAttemptNoneV3            ResearchLLMGatewayAttemptStateV3 = "none"
	ResearchLLMGatewayAttemptSendStartedV3     ResearchLLMGatewayAttemptStateV3 = "send_started"
	ResearchLLMGatewayAttemptPreSendRejectedV3 ResearchLLMGatewayAttemptStateV3 = "pre_send_rejected"
)

// ResearchLLMGatewayReceiptV3 is an attestation emitted at the provider I/O
// boundary. Signature is deliberately opaque to callers; PostgreSQL is the
// verification authority and binds the verified receipt to the spend ledger.
type ResearchLLMGatewayReceiptV3 struct {
	SchemaVersion       string
	KeyID               string
	SignedAtUnixMillis  int64
	ReservationID       int64
	RequestDigest       string
	Call                LLMCall
	DisableThinking     bool
	Attempted           bool
	UsageKnown          bool
	DefinitelyZeroUsage bool
	Outcome             string
	ErrorCode           string
	Signature           []byte
}

func gatewayTextDigestV3(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func gatewayOptionalIntV3(value *int) string {
	if value == nil {
		return "null"
	}
	return strconv.Itoa(*value)
}

func gatewayOptionalBoolV3(value *bool) string {
	if value == nil {
		return "null"
	}
	return strconv.FormatBool(*value)
}

// CanonicalPayload returns the byte-for-byte payload implemented by migration
// 094. Large/model-controlled strings are represented by SHA-256 digests,
// avoiding delimiter ambiguity while still binding their exact UTF-8 bytes.
func (r ResearchLLMGatewayReceiptV3) CanonicalPayload() ([]byte, error) {
	if r.SchemaVersion != ResearchLLMGatewayReceiptSchemaV3 ||
		r.KeyID == "" || r.ReservationID <= 0 || r.SignedAtUnixMillis <= 0 ||
		len(r.RequestDigest) != sha256.Size*2 || r.Call.Model == "" {
		return nil, errors.New("types: invalid research LLM gateway receipt")
	}
	fields := []string{
		r.SchemaVersion,
		r.KeyID,
		strconv.FormatInt(r.SignedAtUnixMillis, 10),
		strconv.FormatInt(r.ReservationID, 10),
		r.RequestDigest,
		gatewayTextDigestV3(r.Call.SystemPrompt),
		gatewayTextDigestV3(r.Call.UserPrompt),
		gatewayTextDigestV3(r.Call.Completion),
		gatewayTextDigestV3(r.Call.Provider),
		gatewayTextDigestV3(r.Call.Model),
		strconv.Itoa(r.Call.PromptTokens),
		strconv.Itoa(r.Call.CompletionTokens),
		gatewayOptionalIntV3(r.Call.PromptCacheHitTokens),
		gatewayOptionalIntV3(r.Call.PromptCacheMissTokens),
		gatewayOptionalIntV3(r.Call.ReasoningTokens),
		strconv.Itoa(r.Call.LatencyMs),
		gatewayOptionalBoolV3(r.Call.PrefixCacheHit),
		strconv.FormatBool(r.DisableThinking),
		gatewayTextDigestV3(r.Call.Error),
		strconv.FormatBool(r.Attempted),
		strconv.FormatBool(r.UsageKnown),
		strconv.FormatBool(r.DefinitelyZeroUsage),
		r.Outcome,
		gatewayTextDigestV3(r.ErrorCode),
	}
	return []byte(strings.Join(fields, "\n")), nil
}
