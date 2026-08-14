package taskstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"sort"
	"time"

	"github.com/YouToco/vane/internal/strictjson"
)

const AdaptiveStateSchemaVersionV2 = "vane.task-adaptive-state/v2"

type InvocationStatusV1 string

const (
	InvocationStatusActive  InvocationStatusV1 = "active"
	InvocationStatusBackoff InvocationStatusV1 = "backoff"
)

// InvocationAdaptiveStateV1 is mutable acquisition state scoped to one exact
// approved Tool invocation. Cursor is Tool-owned opaque JSON; it cannot change
// the Tool name, version, or approved arguments covered by InvocationDigest.
type InvocationAdaptiveStateV1 struct {
	InvocationDigest string             `json:"invocation_digest"`
	Cursor           json.RawMessage    `json:"cursor"`
	Status           InvocationStatusV1 `json:"status"`
	NextFetchAt      *time.Time         `json:"next_fetch_at"`
	LastFetchedAt    *time.Time         `json:"last_fetched_at"`
	FailCount        int64              `json:"fail_count"`
}

type AdaptiveStateV2 struct {
	SchemaVersion    string                      `json:"schema_version"`
	TenantID         int64                       `json:"tenant_id"`
	UserID           int64                       `json:"user_id"`
	TaskID           string                      `json:"task_id"`
	InvocationStates []InvocationAdaptiveStateV1 `json:"invocation_states"`
	RunStats         RunStatsV1                  `json:"run_stats"`
}

type AdaptiveStateInputV2 struct {
	TenantID         int64
	UserID           int64
	TaskID           string
	InvocationStates []InvocationAdaptiveStateV1
	RunStats         RunStatsV1
}

type adaptiveStateV2Wire AdaptiveStateV2

func BuildAdaptiveStateV2(input AdaptiveStateInputV2) (AdaptiveStateV2, error) {
	return normalizeAdaptiveStateV2(AdaptiveStateV2{
		SchemaVersion: AdaptiveStateSchemaVersionV2,
		TenantID:      input.TenantID, UserID: input.UserID, TaskID: input.TaskID,
		InvocationStates: input.InvocationStates, RunStats: input.RunStats,
	})
}

func (s AdaptiveStateV2) Validate() error {
	_, err := normalizeAdaptiveStateV2(s)
	return err
}

func (s AdaptiveStateV2) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeAdaptiveStateV2(s)
	if err != nil {
		return nil, err
	}
	return marshalBounded(adaptiveStateV2Wire(normalized), maxAdaptiveBytes,
		"adaptive state")
}

func (s *AdaptiveStateV2) UnmarshalJSON(payload []byte) error {
	if s == nil || len(payload) == 0 || len(payload) > maxAdaptiveBytes {
		return invalidState("adaptive state json size is invalid")
	}
	var wire adaptiveStateV2Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidState("adaptive state json is invalid")
	}
	normalized, err := normalizeAdaptiveStateV2(AdaptiveStateV2(wire))
	if err != nil {
		return err
	}
	*s = normalized
	return nil
}

func EncodeAdaptiveStateV2(state AdaptiveStateV2) ([]byte, error) {
	return json.Marshal(state)
}

func DecodeAdaptiveStateV2(payload []byte) (AdaptiveStateV2, error) {
	if len(payload) == 0 || len(payload) > maxAdaptiveBytes {
		return AdaptiveStateV2{}, invalidState("adaptive state json size is invalid")
	}
	var state AdaptiveStateV2
	if err := json.Unmarshal(payload, &state); err != nil {
		return AdaptiveStateV2{}, invalidState("adaptive state json is invalid")
	}
	return state, nil
}

func DigestAdaptiveStateV2(state AdaptiveStateV2) (string, error) {
	canonical, err := EncodeAdaptiveStateV2(state)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeAdaptiveStateV2(state AdaptiveStateV2) (AdaptiveStateV2, error) {
	if state.SchemaVersion != AdaptiveStateSchemaVersionV2 {
		return AdaptiveStateV2{}, invalidState("adaptive state schema version is unsupported")
	}
	if state.TenantID <= 0 || state.UserID <= 0 ||
		!validIdentifier(state.TaskID, maxTaskIDBytes) {
		return AdaptiveStateV2{}, invalidState("adaptive state identity is invalid")
	}
	if state.InvocationStates == nil || len(state.InvocationStates) > maxToolCallCount {
		return AdaptiveStateV2{}, invalidState("adaptive invocation state count is invalid")
	}
	state.InvocationStates = slices.Clone(state.InvocationStates)
	seen := make(map[string]struct{}, len(state.InvocationStates))
	for index := range state.InvocationStates {
		current := state.InvocationStates[index]
		if !validSHA256Hex(current.InvocationDigest) || current.FailCount < 0 ||
			!validInvocationStatusV1(current.Status) {
			return AdaptiveStateV2{}, invalidState("adaptive invocation state is invalid")
		}
		cursor, err := canonicalJSONObjectBounded(current.Cursor, "adaptive invocation cursor",
			maxCursorBytes)
		if err != nil {
			return AdaptiveStateV2{}, err
		}
		current.Cursor = cursor
		if current.NextFetchAt != nil {
			if current.NextFetchAt.IsZero() {
				return AdaptiveStateV2{}, invalidState("adaptive invocation timestamp is invalid")
			}
			utc := current.NextFetchAt.UTC()
			current.NextFetchAt = &utc
		}
		if current.LastFetchedAt != nil {
			if current.LastFetchedAt.IsZero() {
				return AdaptiveStateV2{}, invalidState("adaptive invocation timestamp is invalid")
			}
			utc := current.LastFetchedAt.UTC()
			current.LastFetchedAt = &utc
		}
		if _, duplicate := seen[current.InvocationDigest]; duplicate {
			return AdaptiveStateV2{}, invalidState("adaptive invocation digest is duplicated")
		}
		seen[current.InvocationDigest] = struct{}{}
		state.InvocationStates[index] = current
	}
	sort.Slice(state.InvocationStates, func(i, j int) bool {
		return state.InvocationStates[i].InvocationDigest <
			state.InvocationStates[j].InvocationDigest
	})
	if err := validateRunStatsV1(state.RunStats); err != nil {
		return AdaptiveStateV2{}, err
	}
	if _, err := marshalBounded(adaptiveStateV2Wire(state), maxAdaptiveBytes,
		"adaptive state"); err != nil {
		return AdaptiveStateV2{}, err
	}
	return state, nil
}

func validInvocationStatusV1(status InvocationStatusV1) bool {
	switch status {
	case InvocationStatusActive, InvocationStatusBackoff:
		return true
	default:
		return false
	}
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func canonicalJSONObjectBounded(
	raw json.RawMessage,
	field string,
	maxBytes int,
) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxBytes {
		return nil, invalidState(field + " json size is invalid")
	}
	canonical, err := canonicalJSONObject(raw, field)
	if err != nil || len(canonical) > maxBytes {
		return nil, invalidState(field + " json size is invalid")
	}
	return canonical, nil
}
