// Package pusheffect defines the immutable wire and state machine for one
// externally visible push side effect. It has no workflow or provider wiring.
package pusheffect

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/YouToco/vane/internal/strictjson"
)

const (
	SchemaVersion = "vane.push-effect/v1"

	maxIdentityBytes = 512
	maxTargetBytes   = 1024
	maxCardBytes     = 2 << 20
	maxDeliveries    = 256
	maxCanonicalSize = 3 << 20
)

var ErrInvalid = errors.New("push effect: invalid")

type Status string

const (
	StatusPrepared       Status = "prepared"
	StatusSending        Status = "sending"
	StatusAmbiguous      Status = "ambiguous"
	StatusSent           Status = "sent"
	StatusDefiniteFailed Status = "definite_failed"
	StatusBlocked        Status = "blocked"
)

func (s Status) Valid() bool {
	switch s {
	case StatusPrepared, StatusSending, StatusAmbiguous, StatusSent,
		StatusDefiniteFailed, StatusBlocked:
		return true
	default:
		return false
	}
}

// AttemptDisposition reports what one provider request proved. It never
// summarizes the whole durable effect: a later definite rejection cannot
// erase an earlier ambiguous boundary crossing.
type AttemptDisposition string

const (
	AttemptDefiniteNotSent AttemptDisposition = "definite_not_sent"
	AttemptAmbiguous       AttemptDisposition = "ambiguous"
	AttemptSent            AttemptDisposition = "sent"
)

// ProviderObservation is the provider adapter's typed result. MessageID is
// required for sent; AppIdentity identifies the exact provider client
// generation selected before the request, including when a reconfiguration
// races with an in-flight send. ChatID is supplementary routing evidence, not
// proof of delivery by itself.
type ProviderObservation struct {
	Disposition AttemptDisposition
	AppIdentity string
	MessageID   string
	ChatID      string
}

type HistoryQuery struct {
	EffectID       string
	ProviderChatID string
	AppIdentity    string
	// CardDigest is the immutable SHA-256 checkpoint of the exact frozen card
	// bytes. A marker match without this digest is not positive send evidence.
	CardDigest string
	StartTime  time.Time
	EndTime    time.Time
}

// HistoryObservation contains positive provider evidence only. MatchCount=0
// is never proof that no send occurred; MatchCount>1 is a conflict requiring a
// blocked/operator resolution.
type HistoryObservation struct {
	MatchCount int
	MessageID  string
}

type Scope struct {
	ID       string
	TenantID int64
	UserID   int64
}

type Prepared struct {
	ID            string
	TenantID      int64
	UserID        int64
	TaskID        string
	RunSnapshotID int64
	RunID         string
	StepID        string
	ChunkIndex    int
	ChunkCount    int
	BatchID       int64
	DeliveryIDs   []int64
	// ObservationEventKeys is aligned with DeliveryIDs when at least one
	// delivery represents a qualified observation. Empty entries identify
	// ordinary deliveries in a mixed aggregate.
	ObservationEventKeys []string
	Provider             string
	AppIdentity          string
	ProviderChatID       string
	Target               string
	Card                 []byte
	ProviderUUID         string
	IdempotencyExpiresAt time.Time
}

func (p Prepared) Scope() Scope {
	return Scope{ID: p.ID, TenantID: p.TenantID, UserID: p.UserID}
}

type Effect struct {
	Prepared
	SchemaVersion     string
	CanonicalPayload  []byte
	PayloadDigest     string
	CardDigest        string
	Status            Status
	LeaseOwner        string
	LeaseUntil        *time.Time
	TakeoverNotBefore *time.Time
	Fence             int64
	Attempt           int
	NextAttemptAt     time.Time
	ProviderMessageID string
	FailureClass      string
	AmbiguousSince    *time.Time
	SentAt            *time.Time
	BlockedAt         *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Lease struct {
	Scope
	LeaseOwner string
	Fence      int64
}

type ClaimParams struct {
	Scope
	LeaseOwner    string
	LeaseDuration time.Duration
}

type FailureParams struct {
	Lease
	Class      string
	RetryAfter time.Duration
}

type Resolution struct {
	Scope
	ExpectedFence int64
	Class         string
}

// ReconciliationSchedule asks the Store to make one database-clock decision:
// defer an exact ambiguous fence while the provider UUID window is open, or
// atomically block it once that window has expired.
type ReconciliationSchedule struct {
	Resolution
	RetryAfter   time.Duration
	UntilExpiry  bool
	ExpiredClass string
}

type ReconciliationDecision string

const (
	ReconciliationDeferred ReconciliationDecision = "deferred"
	ReconciliationBlocked  ReconciliationDecision = "blocked"
)

type SentReceipt struct {
	Scope
	ExpectedFence        int64
	LeaseOwner           string
	ProviderMessageID    string
	ObservationEventKeys []string
}

type wireV1 struct {
	SchemaVersion        string   `json:"schema_version"`
	EffectID             string   `json:"effect_id"`
	TenantID             int64    `json:"tenant_id"`
	UserID               int64    `json:"user_id"`
	TaskID               string   `json:"task_id"`
	RunSnapshotID        int64    `json:"run_snapshot_id"`
	RunID                string   `json:"run_id"`
	StepID               string   `json:"step_id"`
	ChunkIndex           int      `json:"chunk_index"`
	ChunkCount           int      `json:"chunk_count"`
	BatchID              int64    `json:"batch_id"`
	DeliveryIDs          []int64  `json:"delivery_ids"`
	ObservationEventKeys []string `json:"observation_event_keys,omitempty"`
	Provider             string   `json:"provider"`
	AppIdentity          string   `json:"app_identity"`
	ProviderChatID       string   `json:"provider_chat_id"`
	Target               string   `json:"target"`
	CardBase64           string   `json:"card_base64"`
	CardDigest           string   `json:"card_digest"`
	ProviderUUID         string   `json:"provider_uuid"`
	IdempotencyExpiresAt string   `json:"idempotency_expires_at"`
}

type Canonical struct {
	prepared   Prepared
	payload    []byte
	digest     string
	cardDigest string
}

func (c Canonical) Prepared() Prepared {
	p := c.prepared
	p.DeliveryIDs = slices.Clone(p.DeliveryIDs)
	p.ObservationEventKeys = slices.Clone(p.ObservationEventKeys)
	p.Card = slices.Clone(p.Card)
	return p
}

func (c Canonical) Payload() []byte    { return slices.Clone(c.payload) }
func (c Canonical) Digest() string     { return c.digest }
func (c Canonical) CardDigest() string { return c.cardDigest }

// CardMatchesDigest compares provider history bytes with the immutable digest
// stored on the effect. It rejects malformed/non-canonical digest strings and
// uses a constant-time comparison for the fixed-size checkpoint.
func CardMatchesDigest(card []byte, expected string) bool {
	return constantDigestEqual(digest(card), expected)
}

// CardDigest returns the exact immutable card-byte checkpoint persisted by the
// effect protocol and supplied to provider-history reconciliation.
func CardDigest(card []byte) string {
	return digest(card)
}

// ValidCardDigest reports whether a stored/query digest uses the canonical
// lowercase SHA-256 representation accepted by the effect protocol.
func ValidCardDigest(value string) bool {
	return validDigest(value)
}

func Canonicalize(p Prepared) (Canonical, error) {
	if err := validatePrepared(p); err != nil {
		return Canonical{}, err
	}
	cardDigest := digest(p.Card)
	wire := wireV1{
		SchemaVersion: SchemaVersion,
		EffectID:      p.ID, TenantID: p.TenantID, UserID: p.UserID,
		TaskID: p.TaskID, RunSnapshotID: p.RunSnapshotID, RunID: p.RunID,
		StepID: p.StepID, ChunkIndex: p.ChunkIndex, ChunkCount: p.ChunkCount,
		BatchID: p.BatchID, DeliveryIDs: slices.Clone(p.DeliveryIDs),
		ObservationEventKeys: slices.Clone(p.ObservationEventKeys),
		Provider:             p.Provider, AppIdentity: p.AppIdentity,
		ProviderChatID: p.ProviderChatID, Target: p.Target,
		CardBase64: base64.StdEncoding.EncodeToString(p.Card),
		CardDigest: cardDigest, ProviderUUID: p.ProviderUUID,
		IdempotencyExpiresAt: p.IdempotencyExpiresAt.Format(time.RFC3339Nano),
	}
	payload, err := json.Marshal(wire)
	if err != nil || len(payload) == 0 || len(payload) > maxCanonicalSize {
		return Canonical{}, invalid("canonical payload is invalid")
	}
	return Canonical{
		prepared:   clonePrepared(p),
		payload:    payload,
		digest:     digest(payload),
		cardDigest: cardDigest,
	}, nil
}

func Decode(payload []byte, storedDigest string) (Canonical, error) {
	if len(payload) == 0 || len(payload) > maxCanonicalSize ||
		!validDigest(storedDigest) {
		return Canonical{}, invalid("stored payload is invalid")
	}
	var wire *wireV1
	if err := strictjson.Decode(payload, &wire); err != nil || wire == nil ||
		wire.SchemaVersion != SchemaVersion {
		return Canonical{}, invalid("stored schema is unsupported")
	}
	card, err := base64.StdEncoding.Strict().DecodeString(wire.CardBase64)
	if err != nil || !constantDigestEqual(digest(card), wire.CardDigest) {
		return Canonical{}, invalid("stored card checkpoint is invalid")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, wire.IdempotencyExpiresAt)
	if err != nil {
		return Canonical{}, invalid("stored idempotency expiry is invalid")
	}
	canonical, err := Canonicalize(Prepared{
		ID: wire.EffectID, TenantID: wire.TenantID, UserID: wire.UserID,
		TaskID: wire.TaskID, RunSnapshotID: wire.RunSnapshotID, RunID: wire.RunID,
		StepID: wire.StepID, ChunkIndex: wire.ChunkIndex, ChunkCount: wire.ChunkCount,
		BatchID: wire.BatchID, DeliveryIDs: wire.DeliveryIDs,
		ObservationEventKeys: wire.ObservationEventKeys,
		Provider:             wire.Provider, AppIdentity: wire.AppIdentity,
		ProviderChatID: wire.ProviderChatID, Target: wire.Target,
		Card: card, ProviderUUID: wire.ProviderUUID,
		IdempotencyExpiresAt: expiresAt,
	})
	if err != nil || !bytes.Equal(payload, canonical.payload) ||
		!constantDigestEqual(storedDigest, canonical.digest) {
		return Canonical{}, invalid("stored bytes are not canonical")
	}
	return canonical, nil
}

func validatePrepared(p Prepared) error {
	if !validIdentity(p.ID) || p.TenantID <= 0 || p.UserID <= 0 ||
		!validIdentity(p.TaskID) || p.RunSnapshotID <= 0 ||
		!validIdentity(p.RunID) || !validIdentity(p.StepID) ||
		p.ChunkCount <= 0 || p.ChunkIndex < 0 || p.ChunkIndex >= p.ChunkCount ||
		p.BatchID <= 0 || len(p.DeliveryIDs) == 0 ||
		len(p.DeliveryIDs) > maxDeliveries ||
		!validIdentity(p.Provider) || !validIdentity(p.AppIdentity) ||
		!validIdentity(p.ProviderChatID) ||
		!validText(p.Target, maxTargetBytes) ||
		len(p.Card) == 0 || len(p.Card) > maxCardBytes || !utf8.Valid(p.Card) ||
		p.IdempotencyExpiresAt.IsZero() ||
		p.IdempotencyExpiresAt.Location() != time.UTC ||
		!p.IdempotencyExpiresAt.Equal(
			p.IdempotencyExpiresAt.Truncate(time.Microsecond)) {
		return invalid("prepared identity or payload is invalid")
	}
	var card map[string]any
	if err := strictjson.Decode(p.Card, &card); err != nil || card == nil {
		return invalid("card must be a strict json object")
	}
	for i, id := range p.DeliveryIDs {
		if id <= 0 || (i > 0 && id <= p.DeliveryIDs[i-1]) {
			return invalid("delivery ids must be positive and strictly increasing")
		}
	}
	if len(p.ObservationEventKeys) > 0 {
		if len(p.ObservationEventKeys) != len(p.DeliveryIDs) {
			return invalid("observation event keys must align with delivery ids")
		}
		hasEvent := false
		for _, key := range p.ObservationEventKeys {
			if key == "" {
				continue
			}
			if !validDigest(key) {
				return invalid("observation event key is invalid")
			}
			hasEvent = true
		}
		if !hasEvent {
			return invalid("observation event keys must not be all empty")
		}
	}
	parsed, err := uuid.Parse(p.ProviderUUID)
	if err != nil || parsed.String() != p.ProviderUUID {
		return invalid("provider uuid is not canonical")
	}
	return nil
}

func validIdentity(value string) bool {
	return validText(value, maxIdentityBytes) &&
		!strings.ContainsAny(value, "\r\n\t")
}

func validText(value string, maximum int) bool {
	return value != "" && value == strings.TrimSpace(value) &&
		len(value) <= maximum && utf8.ValidString(value)
}

func clonePrepared(p Prepared) Prepared {
	p.DeliveryIDs = slices.Clone(p.DeliveryIDs)
	p.ObservationEventKeys = slices.Clone(p.ObservationEventKeys)
	p.Card = slices.Clone(p.Card)
	return p
}

// ValidObservationEventKey reports whether a receipt event key uses the
// canonical lowercase SHA-256 representation frozen by the observation
// protocol.
func ValidObservationEventKey(value string) bool {
	return validDigest(value)
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func constantDigestEqual(left, right string) bool {
	return validDigest(left) && validDigest(right) &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, message)
}
