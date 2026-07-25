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

type Scope struct {
	ID       string
	TenantID int64
	UserID   int64
}

type Prepared struct {
	ID                   string
	TenantID             int64
	UserID               int64
	TaskID               string
	RunSnapshotID        int64
	RunID                string
	StepID               string
	ChunkIndex           int
	ChunkCount           int
	BatchID              int64
	DeliveryIDs          []int64
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

type SentReceipt struct {
	Scope
	ExpectedFence     int64
	LeaseOwner        string
	ProviderMessageID string
}

type wireV1 struct {
	SchemaVersion        string  `json:"schema_version"`
	EffectID             string  `json:"effect_id"`
	TenantID             int64   `json:"tenant_id"`
	UserID               int64   `json:"user_id"`
	TaskID               string  `json:"task_id"`
	RunSnapshotID        int64   `json:"run_snapshot_id"`
	RunID                string  `json:"run_id"`
	StepID               string  `json:"step_id"`
	ChunkIndex           int     `json:"chunk_index"`
	ChunkCount           int     `json:"chunk_count"`
	BatchID              int64   `json:"batch_id"`
	DeliveryIDs          []int64 `json:"delivery_ids"`
	Provider             string  `json:"provider"`
	AppIdentity          string  `json:"app_identity"`
	ProviderChatID       string  `json:"provider_chat_id"`
	Target               string  `json:"target"`
	CardBase64           string  `json:"card_base64"`
	CardDigest           string  `json:"card_digest"`
	ProviderUUID         string  `json:"provider_uuid"`
	IdempotencyExpiresAt string  `json:"idempotency_expires_at"`
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
	p.Card = slices.Clone(p.Card)
	return p
}

func (c Canonical) Payload() []byte    { return slices.Clone(c.payload) }
func (c Canonical) Digest() string     { return c.digest }
func (c Canonical) CardDigest() string { return c.cardDigest }

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
		Provider: p.Provider, AppIdentity: p.AppIdentity,
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
		Provider: wire.Provider, AppIdentity: wire.AppIdentity,
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
	p.Card = slices.Clone(p.Card)
	return p
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
