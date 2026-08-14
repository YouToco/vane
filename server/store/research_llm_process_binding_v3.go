package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// ResearchLLMProcessGatewayBindingV1 is the only control-plane value that may
// authorize the main process to ask the isolated research gateway to execute a
// previously frozen V3 LLM reservation. The bearer remains private and this
// value deliberately refuses generic serialization; OpenForProcessGatewayV1
// is the one explicit process-boundary escape hatch.
//
// ReservationID and RequestDigest are public for correlation only. Their
// sealed copies prevent a caller from mutating a valid binding into a request
// for another reservation after Store authorization.
type ResearchLLMProcessGatewayBindingV1 struct {
	ReservationID int64
	RequestDigest string

	sealedReservationID int64
	sealedRequestDigest [sha256.Size]byte
	runCapability       [sha256.Size]byte
	sealedCapability    [sha256.Size]byte
	snapshotID          int64
	referenceDigest     string
}

func (ResearchLLMProcessGatewayBindingV1) String() string   { return "<redacted>" }
func (ResearchLLMProcessGatewayBindingV1) GoString() string { return "<redacted>" }

func (ResearchLLMProcessGatewayBindingV1) LogValue() slog.Value {
	return slog.StringValue("<redacted>")
}

func (ResearchLLMProcessGatewayBindingV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("research process gateway binding cannot be serialized")
}

func (ResearchLLMProcessGatewayBindingV1) MarshalText() ([]byte, error) {
	return nil, errors.New("research process gateway binding cannot be serialized")
}

// OpenForProcessGatewayV1 returns the minimal wire values only after verifying
// that the public correlation fields still equal the Store-sealed binding.
// Callers must pass the result directly to the UDS-only gateway client and
// must never retain it in Temporal inputs/results, logs or durable records.
func (b ResearchLLMProcessGatewayBindingV1) OpenForProcessGatewayV1() (
	reservationID int64, requestDigest, runCapabilityHex string, err error,
) {
	decodedDigest, decodeErr := hex.DecodeString(b.RequestDigest)
	capabilityDigest := sha256.Sum256(b.runCapability[:])
	if b.ReservationID <= 0 || b.ReservationID != b.sealedReservationID ||
		len(decodedDigest) != sha256.Size || decodeErr != nil ||
		b.snapshotID <= 0 || len(b.referenceDigest) != sha256.Size*2 ||
		subtle.ConstantTimeCompare(decodedDigest, b.sealedRequestDigest[:]) != 1 ||
		subtle.ConstantTimeCompare(capabilityDigest[:], b.sealedCapability[:]) != 1 ||
		subtle.ConstantTimeCompare(b.runCapability[:], make([]byte, sha256.Size)) == 1 {
		return 0, "", "", errors.New("research process gateway binding is invalid")
	}
	return b.ReservationID, b.RequestDigest,
		hex.EncodeToString(b.runCapability[:]), nil
}

// ResolveResearchLLMProcessGatewayBindingV1 reconstructs the run capability
// server-side for one already-created, frozen LLM reservation. Neither the
// request digest nor the bearer is accepted from the caller. Exact
// tenant/user/task/workflow/run/snapshot ownership, capability lifetime and
// revocation, reservation scope, and frozen-request binding are checked in one
// control-plane transaction before the transient bearer is returned.
func (s *Store) ResolveResearchLLMProcessGatewayBindingV1(
	ctx context.Context, identity types.RunIdentity,
	snapshot types.ResearchRunSnapshotRefV3, reservationID int64,
) (ResearchLLMProcessGatewayBindingV1, error) {
	if s == nil || s.pool == nil || !s.researchCapabilityConfigured ||
		reservationID <= 0 || identity.Validate() != nil ||
		snapshot.ValidateFor(identity) != nil {
		return ResearchLLMProcessGatewayBindingV1{},
			researchRunValidationError("research process gateway binding is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return ResearchLLMProcessGatewayBindingV1{},
			researchRunDatabaseError("begin research process gateway binding", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Pool connections normally run as vane_app. The binder is granted only to
	// the NOINHERIT session login; transaction-local ROLE NONE reaches that
	// exact authority and automatically restores vane_app at transaction end.
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE NONE`); err != nil {
		return ResearchLLMProcessGatewayBindingV1{},
			researchRunDatabaseError("enter research process binder authority", err)
	}
	var (
		boundReservationID   int64
		requestDigest        string
		capabilityKeyID      string
		capabilityGeneration int
		storedCapabilityHash []byte
	)
	err = tx.QueryRow(ctx,
		`SELECT out_reservation_id,out_request_digest,out_capability_key_id,
		        out_capability_generation,out_capability_hash
		   FROM bind_research_llm_process_gateway_v1($1,$2,$3,$4,$5,$6,$7,$8)`,
		identity.TenantID, identity.UserID, identity.TaskID,
		identity.TemporalWorkflowID, identity.TemporalRunID,
		snapshot.SnapshotID, snapshot.ReferenceDigest, reservationID,
	).Scan(&boundReservationID, &requestDigest, &capabilityKeyID,
		&capabilityGeneration, &storedCapabilityHash)
	if err != nil {
		return ResearchLLMProcessGatewayBindingV1{},
			researchRunDatabaseError("bind research process gateway request", err)
	}
	key, retained := s.researchCapabilityKeys[capabilityKeyID]
	if !retained || capabilityGeneration <= 0 ||
		boundReservationID != reservationID || len(storedCapabilityHash) != sha256.Size {
		return ResearchLLMProcessGatewayBindingV1{},
			researchRunValidationError("research process gateway capability is unavailable")
	}
	bindingBytes, err := json.Marshal(researchRunCapabilityBindingV1{
		SchemaVersion: researchRunCapabilityBindingSchemaV1,
		SnapshotID:    snapshot.SnapshotID, ReferenceDigest: snapshot.ReferenceDigest,
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
	})
	if err != nil {
		return ResearchLLMProcessGatewayBindingV1{}, researchRunIntegrityError()
	}
	rawCapability, capabilityDigest := deriveResearchRunCapabilityV1(key, bindingBytes)
	if subtle.ConstantTimeCompare(
		storedCapabilityHash, capabilityDigest[:],
	) != 1 {
		return ResearchLLMProcessGatewayBindingV1{}, researchRunConflictError()
	}
	requestDigestBytes, err := hex.DecodeString(requestDigest)
	if err != nil || len(requestDigestBytes) != sha256.Size {
		return ResearchLLMProcessGatewayBindingV1{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchLLMProcessGatewayBindingV1{},
			researchRunDatabaseError("commit research process gateway binding", err)
	}
	var sealedDigest [sha256.Size]byte
	copy(sealedDigest[:], requestDigestBytes)
	sealedCapability := sha256.Sum256(rawCapability[:])
	return ResearchLLMProcessGatewayBindingV1{
		ReservationID: reservationID, RequestDigest: requestDigest,
		sealedReservationID: reservationID, sealedRequestDigest: sealedDigest,
		runCapability: rawCapability, sealedCapability: sealedCapability,
		snapshotID:      snapshot.SnapshotID,
		referenceDigest: snapshot.ReferenceDigest,
	}, nil
}
