package store

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const (
	researchRunCapabilityBindingSchemaV1 = "vane.research-run-capability-binding/v1"
	defaultResearchRunCapabilityTTL      = 90 * 24 * time.Hour
)

type ResearchRunCapabilityConfigV1 struct {
	ActiveKeyID  string
	ActiveKeyHex string
	// RetiredKeys is a comma-separated key-id=lowercase-hex list. Retired keys
	// derive existing registrations only; new runs always use ActiveKeyID.
	RetiredKeys string
	TTL         time.Duration
}

// ResearchRunCapabilityV1 is deliberately opaque and non-serializable. It is
// reconstructed inside an Activity from the Temporal-safe snapshot reference,
// used for one Store transaction, and never put in Workflow history or logs.
type ResearchRunCapabilityV1 struct {
	raw             [sha256.Size]byte
	snapshotID      int64
	referenceDigest string
}

func (ResearchRunCapabilityV1) String() string   { return "<redacted>" }
func (ResearchRunCapabilityV1) GoString() string { return "<redacted>" }

func (ResearchRunCapabilityV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("research run capability cannot be serialized")
}

type researchRunCapabilityBindingV1 struct {
	SchemaVersion      string `json:"schema_version"`
	SnapshotID         int64  `json:"snapshot_id"`
	ReferenceDigest    string `json:"reference_digest"`
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	TemporalRunID      string `json:"temporal_run_id"`
	TenantID           int64  `json:"tenant_id"`
	UserID             int64  `json:"user_id"`
	TaskID             string `json:"task_id"`
}

type researchRunCapabilityRegistrationV1 struct {
	ID               int64
	RunSnapshotID    int64
	TenantID         int64
	UserID           int64
	TaskID           string
	WorkflowID       string
	TemporalRunID    string
	ReferenceDigest  string
	KeyID            string
	Generation       int
	CapabilityDigest [sha256.Size]byte
	NotAfter         time.Time
}

func decodeResearchRunCapabilityKeyV1(keyID, keyHex string) (string, [sha256.Size]byte, error) {
	var key [sha256.Size]byte
	keyID = strings.TrimSpace(keyID)
	if keyID == "" || len(keyID) > 64 || strings.TrimSpace(keyHex) != keyHex ||
		len(keyHex) != hex.EncodedLen(sha256.Size) || strings.ToLower(keyHex) != keyHex {
		return "", key, errors.New("research capability key configuration is invalid")
	}
	for i, r := range keyID {
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || (i > 0 && (r == '.' || r == '_' || r == '-'))) {
			return "", key, errors.New("research capability key id is invalid")
		}
	}
	decoded, err := hex.DecodeString(keyHex)
	if err != nil || len(decoded) != sha256.Size {
		return "", key, errors.New("research capability key must be 32-byte lowercase hex")
	}
	copy(key[:], decoded)
	if subtle.ConstantTimeCompare(key[:], make([]byte, sha256.Size)) == 1 {
		return "", key, errors.New("research capability key cannot be all zero")
	}
	return keyID, key, nil
}

func (s *Store) configureResearchRunCapabilityV1(config ResearchRunCapabilityConfigV1) error {
	if s == nil {
		return errors.New("store is nil")
	}
	keyID, key, err := decodeResearchRunCapabilityKeyV1(
		config.ActiveKeyID, config.ActiveKeyHex)
	if err != nil {
		return err
	}
	keys := map[string][sha256.Size]byte{keyID: key}
	if strings.TrimSpace(config.RetiredKeys) != "" {
		for _, entry := range strings.Split(config.RetiredKeys, ",") {
			parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
			if len(parts) != 2 {
				return errors.New("retired research capability keyring is invalid")
			}
			retiredID, retiredKey, err := decodeResearchRunCapabilityKeyV1(
				parts[0], parts[1])
			if err != nil || retiredID == keyID {
				return errors.New("retired research capability keyring is invalid")
			}
			if _, duplicate := keys[retiredID]; duplicate {
				return errors.New("retired research capability keyring has duplicate key id")
			}
			keys[retiredID] = retiredKey
		}
	}
	ttl := config.TTL
	if ttl == 0 {
		ttl = defaultResearchRunCapabilityTTL
	}
	if ttl < 7*24*time.Hour || ttl > 400*24*time.Hour {
		return errors.New("research capability TTL must be between 7 and 400 days")
	}
	s.researchCapabilityActiveKey = keyID
	s.researchCapabilityKeys = keys
	s.researchCapabilityTTL = ttl
	s.researchCapabilityConfigured = true
	return nil
}

func (s *Store) resolveResearchRunCapabilityV1(
	ctx context.Context, ref types.ResearchRunSnapshotRefV3,
) (ResearchRunCapabilityV1, error) {
	if s == nil || !s.researchCapabilityConfigured || s.pool == nil {
		return ResearchRunCapabilityV1{}, researchRunValidationError(
			"research run capability is unavailable")
	}
	if err := ref.ValidateFor(ref.Identity()); err != nil {
		return ResearchRunCapabilityV1{}, researchRunValidationError(
			"research run capability reference is invalid")
	}
	bindingBytes, err := json.Marshal(researchRunCapabilityBindingV1{
		SchemaVersion: researchRunCapabilityBindingSchemaV1,
		SnapshotID:    ref.SnapshotID, ReferenceDigest: ref.ReferenceDigest,
		TemporalWorkflowID: ref.TemporalWorkflowID, TemporalRunID: ref.TemporalRunID,
		TenantID: ref.TenantID, UserID: ref.UserID, TaskID: ref.TaskID,
	})
	if err != nil {
		return ResearchRunCapabilityV1{}, researchRunIntegrityError()
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ResearchRunCapabilityV1{}, researchRunDatabaseError(
			"begin research capability registration", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		fmt.Sprintf("research-capability/v1:%d", ref.SnapshotID)); err != nil {
		return ResearchRunCapabilityV1{}, researchRunDatabaseError(
			"lock research capability registration", err)
	}
	if err := validateControlResearchRunSnapshotRefV3(ctx, tx, ref); err != nil {
		return ResearchRunCapabilityV1{}, err
	}

	registration, found, err := loadResearchRunCapabilityRegistrationV1(ctx, tx, ref.SnapshotID)
	if err != nil {
		return ResearchRunCapabilityV1{}, err
	}
	if found {
		key, retained := s.researchCapabilityKeys[registration.KeyID]
		if !retained {
			return ResearchRunCapabilityV1{}, researchRunValidationError(
				"research run capability key is no longer retained")
		}
		raw, digest := deriveResearchRunCapabilityV1(key, bindingBytes)
		if registration.RunSnapshotID != ref.SnapshotID ||
			registration.TenantID != ref.TenantID || registration.UserID != ref.UserID ||
			registration.TaskID != ref.TaskID ||
			registration.WorkflowID != ref.TemporalWorkflowID ||
			registration.TemporalRunID != ref.TemporalRunID ||
			registration.ReferenceDigest != ref.ReferenceDigest ||
			subtle.ConstantTimeCompare(registration.CapabilityDigest[:], digest[:]) != 1 {
			return ResearchRunCapabilityV1{}, researchRunConflictError()
		}
		if !registration.NotAfter.After(time.Now()) {
			return ResearchRunCapabilityV1{}, researchRunValidationError(
				"research run capability expired")
		}
		if err := tx.Commit(ctx); err != nil {
			return ResearchRunCapabilityV1{}, researchRunDatabaseError(
				"commit research capability recovery", err)
		}
		return ResearchRunCapabilityV1{
			raw: raw, snapshotID: ref.SnapshotID, referenceDigest: ref.ReferenceDigest,
		}, nil
	}

	key := s.researchCapabilityKeys[s.researchCapabilityActiveKey]
	raw, digest := deriveResearchRunCapabilityV1(key, bindingBytes)
	{
		var snapshotCreatedAt time.Time
		if err := tx.QueryRow(ctx,
			`SELECT created_at FROM task_run_snapshots WHERE id=$1`, ref.SnapshotID,
		).Scan(&snapshotCreatedAt); err != nil {
			return ResearchRunCapabilityV1{}, researchRunDatabaseError(
				"load research capability lifetime", err)
		}
		notAfter := snapshotCreatedAt.Add(s.researchCapabilityTTL)
		if !notAfter.After(time.Now()) {
			return ResearchRunCapabilityV1{}, researchRunValidationError(
				"research run capability issuance window expired")
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO research_run_capabilities (
			     run_snapshot_id,tenant_id,user_id,task_id,temporal_workflow_id,
			     temporal_run_id,reference_digest,key_id,generation,
			     capability_hash,not_after
			 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10)`,
			ref.SnapshotID, ref.TenantID, ref.UserID, ref.TaskID,
			ref.TemporalWorkflowID, ref.TemporalRunID, ref.ReferenceDigest,
			s.researchCapabilityActiveKey, digest[:], notAfter)
		if err != nil {
			return ResearchRunCapabilityV1{}, researchRunDatabaseError(
				"register research run capability", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ResearchRunCapabilityV1{}, researchRunDatabaseError(
			"commit research capability registration", err)
	}
	return ResearchRunCapabilityV1{
		raw: raw, snapshotID: ref.SnapshotID, referenceDigest: ref.ReferenceDigest,
	}, nil
}

// registerResearchRunCapabilityInControlTxV1 is used by snapshot admission so
// the immutable snapshot and its capability hash become visible atomically.
// It returns no bearer; the Activity resolves that transient value after the
// owner transaction commits.
func (s *Store) registerResearchRunCapabilityInControlTxV1(
	ctx context.Context, tx pgx.Tx, ref types.ResearchRunSnapshotRefV3,
) error {
	if s == nil || tx == nil || !s.researchCapabilityConfigured ||
		ref.ValidateFor(ref.Identity()) != nil {
		return researchRunValidationError("research run capability is unavailable")
	}
	bindingBytes, err := json.Marshal(researchRunCapabilityBindingV1{
		SchemaVersion: researchRunCapabilityBindingSchemaV1,
		SnapshotID:    ref.SnapshotID, ReferenceDigest: ref.ReferenceDigest,
		TemporalWorkflowID: ref.TemporalWorkflowID, TemporalRunID: ref.TemporalRunID,
		TenantID: ref.TenantID, UserID: ref.UserID, TaskID: ref.TaskID,
	})
	if err != nil {
		return researchRunIntegrityError()
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		fmt.Sprintf("research-capability/v1:%d", ref.SnapshotID)); err != nil {
		return researchRunDatabaseError("lock research capability registration", err)
	}
	if err := validateControlResearchRunSnapshotRefV3(ctx, tx, ref); err != nil {
		return err
	}
	registration, found, err := loadResearchRunCapabilityRegistrationV1(
		ctx, tx, ref.SnapshotID)
	if err != nil {
		return err
	}
	if found {
		key, retained := s.researchCapabilityKeys[registration.KeyID]
		if !retained {
			return researchRunValidationError(
				"research run capability key is no longer retained")
		}
		_, digest := deriveResearchRunCapabilityV1(key, bindingBytes)
		if registration.RunSnapshotID != ref.SnapshotID ||
			registration.TenantID != ref.TenantID ||
			registration.UserID != ref.UserID || registration.TaskID != ref.TaskID ||
			registration.WorkflowID != ref.TemporalWorkflowID ||
			registration.TemporalRunID != ref.TemporalRunID ||
			registration.ReferenceDigest != ref.ReferenceDigest ||
			subtle.ConstantTimeCompare(registration.CapabilityDigest[:], digest[:]) != 1 {
			return researchRunConflictError()
		}
		return nil
	}
	key := s.researchCapabilityKeys[s.researchCapabilityActiveKey]
	_, digest := deriveResearchRunCapabilityV1(key, bindingBytes)
	var snapshotCreatedAt time.Time
	if err := tx.QueryRow(ctx,
		`SELECT created_at FROM task_run_snapshots WHERE id=$1`, ref.SnapshotID,
	).Scan(&snapshotCreatedAt); err != nil {
		return researchRunDatabaseError("load research capability lifetime", err)
	}
	notAfter := snapshotCreatedAt.Add(s.researchCapabilityTTL)
	if !notAfter.After(time.Now()) {
		return researchRunValidationError(
			"research run capability issuance window expired")
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO research_run_capabilities (
		     run_snapshot_id,tenant_id,user_id,task_id,temporal_workflow_id,
		     temporal_run_id,reference_digest,key_id,generation,
		     capability_hash,not_after
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10)`,
		ref.SnapshotID, ref.TenantID, ref.UserID, ref.TaskID,
		ref.TemporalWorkflowID, ref.TemporalRunID, ref.ReferenceDigest,
		s.researchCapabilityActiveKey, digest[:], notAfter); err != nil {
		return researchRunDatabaseError("register research run capability", err)
	}
	return nil
}

func deriveResearchRunCapabilityV1(
	key [sha256.Size]byte, binding []byte,
) ([sha256.Size]byte, [sha256.Size]byte) {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(binding)
	var raw [sha256.Size]byte
	copy(raw[:], mac.Sum(nil))
	return raw, sha256.Sum256(raw[:])
}

func loadResearchRunCapabilityRegistrationV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64,
) (researchRunCapabilityRegistrationV1, bool, error) {
	var registration researchRunCapabilityRegistrationV1
	var capabilityHash []byte
	err := tx.QueryRow(ctx,
		`SELECT id,run_snapshot_id,tenant_id,user_id,task_id,
		        temporal_workflow_id,temporal_run_id,reference_digest,key_id,
		        generation,capability_hash,not_after
		   FROM research_run_capabilities
		  WHERE run_snapshot_id=$1 AND revoked_at IS NULL
		  ORDER BY generation DESC LIMIT 1`, snapshotID,
	).Scan(&registration.ID, &registration.RunSnapshotID,
		&registration.TenantID, &registration.UserID, &registration.TaskID,
		&registration.WorkflowID, &registration.TemporalRunID,
		&registration.ReferenceDigest, &registration.KeyID,
		&registration.Generation, &capabilityHash, &registration.NotAfter)
	if errors.Is(err, pgx.ErrNoRows) {
		return researchRunCapabilityRegistrationV1{}, false, nil
	}
	if err != nil {
		return researchRunCapabilityRegistrationV1{}, false,
			researchRunDatabaseError("load research run capability", err)
	}
	if len(capabilityHash) != sha256.Size {
		return researchRunCapabilityRegistrationV1{}, false, researchRunIntegrityError()
	}
	copy(registration.CapabilityDigest[:], capabilityHash)
	return registration, true, nil
}

func validateControlResearchRunSnapshotRefV3(
	ctx context.Context, tx pgx.Tx, expected types.ResearchRunSnapshotRefV3,
) error {
	row, found, err := loadResearchRunSnapshotRowV3(ctx, tx,
		CreateOrGetTaskRunSnapshotParams{
			TenantID: expected.TenantID, UserID: expected.UserID,
			TaskID: expected.TaskID, TemporalWorkflowID: expected.TemporalWorkflowID,
			TemporalRunID: expected.TemporalRunID,
		})
	if err != nil {
		return err
	}
	stored, err := validateStoredResearchRunSnapshotV3(expected.Identity(), row)
	if !found || err != nil || stored != expected {
		return researchRunIntegrityError()
	}
	return nil
}

func (s *Store) loadControlResearchRunSnapshotRefV3(
	ctx context.Context, identity types.RunIdentity, snapshotID int64,
) (types.ResearchRunSnapshotRefV3, error) {
	if snapshotID <= 0 || identity.Validate() != nil || s == nil || s.pool == nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunValidationError(
			"research run capability scope is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError(
			"begin control snapshot read", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	row, found, err := loadResearchRunSnapshotRowV3(ctx, tx,
		CreateOrGetTaskRunSnapshotParams{
			TenantID: identity.TenantID, UserID: identity.UserID,
			TaskID: identity.TaskID, TemporalWorkflowID: identity.TemporalWorkflowID,
			TemporalRunID: identity.TemporalRunID,
		})
	if err != nil {
		return types.ResearchRunSnapshotRefV3{}, err
	}
	ref, err := validateStoredResearchRunSnapshotV3(identity, row)
	if !found || err != nil || ref.SnapshotID != snapshotID {
		return types.ResearchRunSnapshotRefV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError(
			"commit control snapshot read", err)
	}
	return ref, nil
}

func (s *Store) beginScopedResearchRunTransactionV3(
	ctx context.Context, options pgx.TxOptions, identity types.RunIdentity,
	snapshotID int64,
) (pgx.Tx, types.ResearchRunSnapshotRefV3, error) {
	ref, err := s.loadControlResearchRunSnapshotRefV3(ctx, identity, snapshotID)
	if err != nil {
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	capability, err := s.resolveResearchRunCapabilityV1(ctx, ref)
	if err != nil {
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	tx, err := s.beginResearchTransaction(ctx, options)
	if err != nil {
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	if err := setResearchRunCapabilityScopeV3(ctx, tx, identity, ref, capability); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	return tx, ref, nil
}

func setResearchRunCapabilityScopeV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	ref types.ResearchRunSnapshotRefV3, capability ResearchRunCapabilityV1,
) error {
	if tx == nil || ref.ValidateFor(identity) != nil ||
		capability.snapshotID != ref.SnapshotID ||
		capability.referenceDigest != ref.ReferenceDigest {
		return researchRunValidationError("research run capability scope is invalid")
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return researchRunDatabaseError("set research run search path", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.research_run_capability_v1',$1,true),
		        set_config('app.tenant_id',$2,true),set_config('app.user_id',$3,true)`,
		hex.EncodeToString(capability.raw[:]), fmt.Sprint(identity.TenantID),
		fmt.Sprint(identity.UserID)); err != nil {
		return researchRunDatabaseError("install research run capability", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+researchRuntimeCapabilityRole); err != nil {
		return researchRunDatabaseError("enter research run role", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT require_research_run_capability_v1($1,$2,$3,$4,$5,$6,$7)`,
		ref.SnapshotID, ref.ReferenceDigest, identity.TenantID, identity.UserID,
		identity.TaskID, identity.TemporalWorkflowID, identity.TemporalRunID); err != nil {
		return researchRunDatabaseError("verify research run capability", err)
	}
	return nil
}
