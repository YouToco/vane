// Package agentledger defines the versioned, append-only semantic event wire
// used to rebuild Agent session projections. It deliberately contains no
// runtime wiring: task creation, definition edits, and Temporal remain their
// own business truth sources.
package agentledger

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const (
	// SchemaVersion identifies the immutable v1 event envelope. Incompatible
	// payload changes must introduce a new reader rather than reinterpret rows.
	SchemaVersion = "vane.agent-event/v1"

	maxEventBodyBytes    = 256 << 10
	maxCanonicalEventLen = maxEventBodyBytes + 1024
)

var ErrInvalidEvent = errors.New("agent ledger: invalid event")

// Kind identifies a semantic transition. Bodies stay kind-specific JSON
// objects so later projectors can evolve independently while the outer wire,
// ordering, and integrity rules remain frozen.
type Kind string

const (
	KindTurnStarted           Kind = "turn_started"
	KindUserMessage           Kind = "user_message"
	KindAssistantMessage      Kind = "assistant_message"
	KindToolCall              Kind = "tool_call"
	KindToolResult            Kind = "tool_result"
	KindConfirmationRequested Kind = "confirmation_requested"
	KindConfirmationResolved  Kind = "confirmation_resolved"
	KindTurnCompleted         Kind = "turn_completed"
)

// Valid reports whether k belongs to the frozen v1 vocabulary.
func (k Kind) Valid() bool {
	switch k {
	case KindTurnStarted,
		KindUserMessage,
		KindAssistantMessage,
		KindToolCall,
		KindToolResult,
		KindConfirmationRequested,
		KindConfirmationResolved,
		KindTurnCompleted:
		return true
	default:
		return false
	}
}

// Input is one event before sequence allocation. Body must be a strict JSON
// object: duplicate keys, unknown outer-envelope fields, invalid UTF-8, null,
// arrays, and multiple top-level values are rejected. Bodies may contain user
// or external text and therefore must never be logged. Credentials, tokens,
// passwords, and secret values are forbidden at the caller boundary.
type Input struct {
	Kind Kind
	Body json.RawMessage
}

// Scope is the complete tenant/user/session boundary for every ledger method.
// No field may be omitted or treated as a wildcard.
type Scope struct {
	TenantID  int64
	UserID    int64
	SessionID int64
}

// AppendBatch is one atomic append request. IdempotencyKey identifies the
// complete ordered event slice within Scope.
type AppendBatch struct {
	Scope          Scope
	IdempotencyKey string
	Events         []Input
}

// Event is one durable row returned by the Store after full integrity checks.
type Event struct {
	ID             int64
	Scope          Scope
	Sequence       int64
	IdempotencyKey string
	BatchIndex     int
	BatchSize      int
	Kind           Kind
	SchemaVersion  string
	Payload        json.RawMessage
	PayloadDigest  string
	BatchDigest    string
	CreatedAt      time.Time
}

// CanonicalEvent is a validated immutable v1 event envelope. Accessors return
// defensive copies so callers cannot mutate bytes after digest calculation.
type CanonicalEvent struct {
	kind    Kind
	body    []byte
	payload []byte
	digest  string
}

func (e CanonicalEvent) Kind() Kind {
	return e.kind
}

func (e CanonicalEvent) Payload() []byte {
	return slices.Clone(e.payload)
}

// Body returns the canonical kind-specific JSON object. Projectors consume
// this retained representation instead of reinterpreting the outer envelope.
func (e CanonicalEvent) Body() []byte {
	return slices.Clone(e.body)
}

func (e CanonicalEvent) Digest() string {
	return e.digest
}

type envelopeV1 struct {
	SchemaVersion string          `json:"schema_version"`
	Kind          Kind            `json:"kind"`
	Body          json.RawMessage `json:"body"`
}

// Canonicalize validates input and emits the one accepted byte representation.
// V1 sorts object keys and removes insignificant whitespace, but deliberately
// preserves a JSON number's lexical spelling: 1, 1.0, and 1e0 are distinct
// semantic input bytes and therefore produce distinct digests. A future number
// normalization rule requires a new schema version.
func Canonicalize(input Input) (CanonicalEvent, error) {
	if !input.Kind.Valid() {
		return CanonicalEvent{}, invalidEvent("event kind is unsupported")
	}
	body, err := canonicalObject(input.Body)
	if err != nil {
		return CanonicalEvent{}, err
	}
	payload, err := json.Marshal(envelopeV1{
		SchemaVersion: SchemaVersion,
		Kind:          input.Kind,
		Body:          body,
	})
	if err != nil {
		return CanonicalEvent{}, invalidEvent("event envelope cannot be encoded")
	}
	if len(payload) > maxCanonicalEventLen {
		return CanonicalEvent{}, invalidEvent("canonical event exceeds the size limit")
	}
	return CanonicalEvent{
		kind:    input.Kind,
		body:    body,
		payload: payload,
		digest:  digest(payload),
	}, nil
}

// Decode verifies schema, strict canonical bytes, and the stored digest. It is
// the retained v1 reader used by list/replay; corruption never becomes a
// partially visible event.
func Decode(payload []byte, storedDigest string) (CanonicalEvent, error) {
	if len(payload) == 0 || len(payload) > maxCanonicalEventLen {
		return CanonicalEvent{}, invalidEvent("stored event size is invalid")
	}
	var env *envelopeV1
	if err := strictjson.Decode(payload, &env); err != nil || env == nil {
		return CanonicalEvent{}, invalidEvent("stored event envelope is invalid")
	}
	if env.SchemaVersion != SchemaVersion || !env.Kind.Valid() {
		return CanonicalEvent{}, invalidEvent("stored event schema is unsupported")
	}
	canonical, err := Canonicalize(Input{Kind: env.Kind, Body: env.Body})
	if err != nil {
		return CanonicalEvent{}, invalidEvent("stored event body is invalid")
	}
	if !bytes.Equal(payload, canonical.payload) {
		return CanonicalEvent{}, invalidEvent("stored event bytes are not canonical")
	}
	if !validDigest(storedDigest) ||
		subtle.ConstantTimeCompare([]byte(storedDigest), []byte(canonical.digest)) != 1 {
		return CanonicalEvent{}, invalidEvent("stored event digest does not match")
	}
	return canonical, nil
}

// BatchDigest binds an ordered event batch. Sequence numbers are allocated by
// the Store and intentionally excluded, allowing response-lost retries to
// compare the caller's original semantic bytes before returning stored rows.
func BatchDigest(events []CanonicalEvent) (string, error) {
	if len(events) == 0 {
		return "", invalidEvent("event batch is empty")
	}
	payloads := make([]json.RawMessage, len(events))
	for i := range events {
		if events[i].kind.Valid() && len(events[i].payload) > 0 &&
			validDigest(events[i].digest) {
			payloads[i] = events[i].payload
			continue
		}
		return "", invalidEvent("event batch contains an invalid canonical event")
	}
	wire, err := json.Marshal(struct {
		SchemaVersion string            `json:"schema_version"`
		Events        []json.RawMessage `json:"events"`
	}{
		SchemaVersion: "vane.agent-event-batch/v1",
		Events:        payloads,
	})
	if err != nil {
		return "", invalidEvent("event batch cannot be encoded")
	}
	return digest(wire), nil
}

func canonicalObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxEventBodyBytes {
		return nil, invalidEvent("event body size is invalid")
	}
	var object map[string]any
	if err := strictjson.Decode(raw, &object); err != nil || object == nil {
		return nil, invalidEvent("event body must be a strict json object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, invalidEvent("event body cannot be canonicalized")
	}
	return canonical, nil
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

func invalidEvent(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidEvent, message)
}
