package taskstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/types"
)

// AdaptiveStateSchemaVersionV1 identifies the retained V1 adaptive-state
// reader. A future field or changed interpretation requires a new version.
const AdaptiveStateSchemaVersionV1 = "vane.task-adaptive-state/v1"

// QueryVariantV1 is a bounded query alternative for one already-approved
// source. The Store must verify SourceID belongs to the same approved task and
// that Query remains within its confirmed intent.
type QueryVariantV1 struct {
	SourceID int64  `json:"source_id"`
	Query    string `json:"query"`
}

// ReadCapabilityV1 identifies a registered read-only capability. It cannot
// name an implementation, endpoint, credential, or write tool.
type ReadCapabilityV1 struct {
	Platform   types.Platform   `json:"platform"`
	Capability types.Capability `json:"capability"`
}

// RunStatsV1 is a finite set of monotonic aggregate counters. Detailed costs,
// timings, outputs, and evidence remain in per-run ledgers.
type RunStatsV1 struct {
	AttemptedRuns       int64 `json:"attempted_runs"`
	SuccessfulRuns      int64 `json:"successful_runs"`
	EmptyRuns           int64 `json:"empty_runs"`
	FailedRuns          int64 `json:"failed_runs"`
	ConsecutiveFailures int64 `json:"consecutive_failures"`
}

// AdaptiveStateV1 contains only automatically mutable, low-risk state. The
// last-known-good pointer is intentionally a table column, not part of this
// payload, so its approval proof can be fenced independently.
type AdaptiveStateV1 struct {
	SchemaVersion   string             `json:"schema_version"`
	TenantID        int64              `json:"tenant_id"`
	UserID          int64              `json:"user_id"`
	TaskID          string             `json:"task_id"`
	QueryVariants   []QueryVariantV1   `json:"query_variants"`
	CapabilityOrder []ReadCapabilityV1 `json:"capability_order"`
	SourceOrder     []int64            `json:"source_order"`
	RunStats        RunStatsV1         `json:"run_stats"`
}

// AdaptiveStateInputV1 is the explicit construction boundary. Every slice is
// required, including an intentionally empty initial state.
type AdaptiveStateInputV1 struct {
	TenantID        int64
	UserID          int64
	TaskID          string
	QueryVariants   []QueryVariantV1
	CapabilityOrder []ReadCapabilityV1
	SourceOrder     []int64
	RunStats        RunStatsV1
}

type adaptiveStateV1Wire AdaptiveStateV1

// BuildAdaptiveStateV1 constructs a validated, defensively copied state.
func BuildAdaptiveStateV1(input AdaptiveStateInputV1) (AdaptiveStateV1, error) {
	return normalizeAdaptiveStateV1(AdaptiveStateV1{
		SchemaVersion:   AdaptiveStateSchemaVersionV1,
		TenantID:        input.TenantID,
		UserID:          input.UserID,
		TaskID:          input.TaskID,
		QueryVariants:   input.QueryVariants,
		CapabilityOrder: input.CapabilityOrder,
		SourceOrder:     input.SourceOrder,
		RunStats:        input.RunStats,
	})
}

// Validate verifies the complete V1 state without mutating it.
func (s AdaptiveStateV1) Validate() error {
	_, err := normalizeAdaptiveStateV1(s)
	return err
}

// MarshalJSON validates a state before returning canonical JSON.
func (s AdaptiveStateV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeAdaptiveStateV1(s)
	if err != nil {
		return nil, err
	}
	return marshalBounded(adaptiveStateV1Wire(normalized), maxAdaptiveBytes,
		"adaptive state")
}

// UnmarshalJSON strictly decodes the retained V1 wire.
func (s *AdaptiveStateV1) UnmarshalJSON(payload []byte) error {
	if s == nil || len(payload) == 0 || len(payload) > maxAdaptiveBytes {
		return invalidState("adaptive state json size is invalid")
	}
	var wire adaptiveStateV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidState("adaptive state json is invalid")
	}
	normalized, err := normalizeAdaptiveStateV1(AdaptiveStateV1(wire))
	if err != nil {
		return err
	}
	*s = normalized
	return nil
}

// EncodeAdaptiveStateV1 returns validated canonical JSON.
func EncodeAdaptiveStateV1(state AdaptiveStateV1) ([]byte, error) {
	return json.Marshal(state)
}

// DecodeAdaptiveStateV1 strictly decodes V1 adaptive state.
func DecodeAdaptiveStateV1(payload []byte) (AdaptiveStateV1, error) {
	if len(payload) == 0 || len(payload) > maxAdaptiveBytes {
		return AdaptiveStateV1{}, invalidState("adaptive state json size is invalid")
	}
	var state AdaptiveStateV1
	if err := json.Unmarshal(payload, &state); err != nil {
		return AdaptiveStateV1{}, invalidState("adaptive state json is invalid")
	}
	return state, nil
}

// DigestAdaptiveStateV1 returns the lowercase SHA-256 digest of canonical
// adaptive-state bytes.
func DigestAdaptiveStateV1(state AdaptiveStateV1) (string, error) {
	canonical, err := EncodeAdaptiveStateV1(state)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeAdaptiveStateV1(state AdaptiveStateV1) (AdaptiveStateV1, error) {
	if state.SchemaVersion != AdaptiveStateSchemaVersionV1 {
		return AdaptiveStateV1{}, invalidState("adaptive state schema version is unsupported")
	}
	if state.TenantID <= 0 || state.UserID <= 0 ||
		!validIdentifier(state.TaskID, maxTaskIDBytes) {
		return AdaptiveStateV1{}, invalidState("adaptive state identity is invalid")
	}
	if state.QueryVariants == nil || len(state.QueryVariants) > maxQueryVariantCount {
		return AdaptiveStateV1{}, invalidState("adaptive query variant count is invalid")
	}
	if state.CapabilityOrder == nil || len(state.CapabilityOrder) > maxCapabilityCount {
		return AdaptiveStateV1{}, invalidState("adaptive capability order size is invalid")
	}
	if state.SourceOrder == nil || len(state.SourceOrder) > maxSourceCount {
		return AdaptiveStateV1{}, invalidState("adaptive source order size is invalid")
	}

	state.QueryVariants = slices.Clone(state.QueryVariants)
	seenQueries := make(map[QueryVariantV1]struct{}, len(state.QueryVariants))
	for _, variant := range state.QueryVariants {
		if variant.SourceID <= 0 || !validSingleLineText(variant.Query, maxQueryBytes, false) {
			return AdaptiveStateV1{}, invalidState("adaptive query variant is invalid")
		}
		if _, duplicate := seenQueries[variant]; duplicate {
			return AdaptiveStateV1{}, invalidState("adaptive query variant is duplicated")
		}
		seenQueries[variant] = struct{}{}
	}

	state.CapabilityOrder = slices.Clone(state.CapabilityOrder)
	seenCapabilities := make(map[ReadCapabilityV1]struct{}, len(state.CapabilityOrder))
	for _, capability := range state.CapabilityOrder {
		if !validReadCapability(capability.Platform, capability.Capability) {
			return AdaptiveStateV1{}, invalidState("adaptive read capability is invalid")
		}
		if _, duplicate := seenCapabilities[capability]; duplicate {
			return AdaptiveStateV1{}, invalidState("adaptive read capability is duplicated")
		}
		seenCapabilities[capability] = struct{}{}
	}

	state.SourceOrder = slices.Clone(state.SourceOrder)
	seenSourceIDs := make(map[int64]struct{}, len(state.SourceOrder))
	for _, sourceID := range state.SourceOrder {
		if sourceID <= 0 {
			return AdaptiveStateV1{}, invalidState("adaptive source id is invalid")
		}
		if _, duplicate := seenSourceIDs[sourceID]; duplicate {
			return AdaptiveStateV1{}, invalidState("adaptive source id is duplicated")
		}
		seenSourceIDs[sourceID] = struct{}{}
	}
	if err := validateRunStatsV1(state.RunStats); err != nil {
		return AdaptiveStateV1{}, err
	}
	if _, err := marshalBounded(adaptiveStateV1Wire(state), maxAdaptiveBytes,
		"adaptive state"); err != nil {
		return AdaptiveStateV1{}, err
	}
	return state, nil
}

func validateRunStatsV1(stats RunStatsV1) error {
	if stats.AttemptedRuns < 0 || stats.SuccessfulRuns < 0 || stats.EmptyRuns < 0 ||
		stats.FailedRuns < 0 || stats.ConsecutiveFailures < 0 {
		return invalidState("adaptive run stats are negative")
	}
	if stats.SuccessfulRuns > stats.AttemptedRuns ||
		stats.EmptyRuns > stats.AttemptedRuns-stats.SuccessfulRuns ||
		stats.FailedRuns != stats.AttemptedRuns-stats.SuccessfulRuns-stats.EmptyRuns ||
		stats.ConsecutiveFailures > stats.FailedRuns {
		return invalidState("adaptive run stats are inconsistent")
	}
	return nil
}
