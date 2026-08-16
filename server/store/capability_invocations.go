package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/capabilityruntime"
	"github.com/YouToco/vane/server/types"
)

const capabilityInvocationCoordinatorRole = "vane_capability_invocation_coordinator"

type CapabilityInvocationStatusV1 string

const (
	CapabilityInvocationPending        CapabilityInvocationStatusV1 = "pending"
	CapabilityInvocationExecuting      CapabilityInvocationStatusV1 = "executing"
	CapabilityInvocationSucceeded      CapabilityInvocationStatusV1 = "succeeded"
	CapabilityInvocationDefiniteFailed CapabilityInvocationStatusV1 = "definite_failed"
	CapabilityInvocationRejected       CapabilityInvocationStatusV1 = "rejected"
	CapabilityInvocationUnknownEffect  CapabilityInvocationStatusV1 = "unknown_effect"
)

// BuiltinCapabilityRegistryV1 is the control-plane proof that an exact
// builtin operation and schema digest are still registered. The ledger never
// treats an arbitrary identifier from an invocation as an approved builtin.
type BuiltinCapabilityRegistryV1 interface {
	VerifyBuiltinCapabilityV1(
		context.Context,
		capabilityruntime.CapabilityRefV1,
		string,
	) error
}

type CapabilityInvocationRecordV1 struct {
	ID                    uuid.UUID
	Invocation            capabilityruntime.InvocationV1
	Status                CapabilityInvocationStatusV1
	LeaseOwner            string
	LeaseUntil            *time.Time
	Fence                 int64
	Attempt               int64
	CurrentReceiptOrdinal *int64
	CurrentReceipt        *capabilityruntime.ReceiptV1
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type CapabilityInvocationLeaseV1 struct {
	InvocationID      uuid.UUID
	TenantID          types.TenantID
	UserID            int64
	InvocationDigest  string
	IdempotencyDigest string
	LeaseOwner        string
	Fence             int64
	Attempt           int64
	LeaseUntil        time.Time
}

func (s *Store) PrepareCapabilityInvocationV1(
	ctx context.Context,
	invocation capabilityruntime.InvocationV1,
	builtins BuiltinCapabilityRegistryV1,
) (CapabilityInvocationRecordV1, error) {
	payload, err := capabilityruntime.EncodeInvocationV1(invocation)
	if err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationValidation("invocation contract is invalid", err)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("begin prepare transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := admitCapabilityInvocationTenant(ctx, tx, invocation); err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	if err := validateCapabilityInvocationAuthority(ctx, tx, invocation, builtins); err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	if err := setCapabilityInvocationRole(ctx, tx, invocation); err != nil {
		return CapabilityInvocationRecordV1{}, err
	}

	invocationID := uuid.New()
	credential := invocation.Credential
	var a2aToken any
	if invocation.Principal.A2ATokenAuthorityID != "" {
		a2aToken = invocation.Principal.A2ATokenAuthorityID
	}
	var requiredScope any
	if invocation.Principal.RequiredA2AScope != "" {
		requiredScope = string(invocation.Principal.RequiredA2AScope)
	}
	credentialValues := nullableCapabilityCredential(credential)
	_, err = tx.Exec(ctx, `INSERT INTO capability_invocations(
		id,tenant_id,user_id,principal_role,actor_type,membership_generation,
		a2a_token_authority_id,required_a2a_scope,
		capability_kind,capability_scope,capability_owner_user_id,capability_id,
		capability_version_id,capability_version_digest,operation_schema_digest,
		operation,policy_digest,
		credential_opaque_ref,credential_opaque_ref_digest,credential_provider,
		credential_purpose,credential_scope,credential_user_id,
		credential_generation,credential_fingerprint,
		idempotency_key,idempotency_digest,invocation_digest,
		invocation_payload,invocation_payload_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
		       $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)
		ON CONFLICT (tenant_id,user_id,idempotency_digest) DO NOTHING`,
		invocationID, invocation.Principal.TenantID, invocation.Principal.UserID,
		invocation.Principal.Role, invocation.Principal.ActorType,
		invocation.Principal.MembershipAuthorizationGeneration, a2aToken, requiredScope,
		invocation.Capability.Kind, invocation.Capability.Scope,
		invocation.Capability.OwnerUserID, invocation.Capability.ID,
		invocation.Capability.VersionID, invocation.Capability.VersionDigest,
		invocation.Capability.OperationSchemaDigest, invocation.Operation,
		invocation.PolicyDigest,
		credentialValues[0], credentialValues[1], credentialValues[2], credentialValues[3],
		credentialValues[4], credentialValues[5], credentialValues[6], credentialValues[7],
		invocation.IdempotencyKey, invocation.IdempotencyDigest, invocation.InvocationDigest,
		payload, capabilityInvocationSHA256(payload))
	if err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("insert invocation checkpoint", err)
	}
	record, err := loadCapabilityInvocationRecord(ctx, tx, invocation)
	if err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	if record.Invocation.InvocationDigest != invocation.InvocationDigest ||
		stringMustEncodeInvocation(record.Invocation) != string(payload) {
		return CapabilityInvocationRecordV1{}, capabilityInvocationConflict(
			"idempotency key is already bound to a different invocation")
	}
	if err := tx.Commit(ctx); err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("commit invocation prepare", err)
	}
	return record, nil
}

// AcquireCapabilityInvocationV1 is deliberately one-shot. It can move only a
// pending checkpoint to executing. An executing row is never handed out again,
// even to the same worker; once its DB-clock lease expires the only legal
// automatic transition is unknown_effect with an append-only ambiguous receipt.
func (s *Store) AcquireCapabilityInvocationV1(
	ctx context.Context,
	invocation capabilityruntime.InvocationV1,
	builtins BuiltinCapabilityRegistryV1,
	leaseOwner string,
	leaseDuration time.Duration,
) (CapabilityInvocationLeaseV1, CapabilityInvocationRecordV1, error) {
	if err := invocation.Validate(); err != nil {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{},
			capabilityInvocationValidation("invocation contract is invalid", err)
	}
	leaseOwner = strings.TrimSpace(leaseOwner)
	leaseMillis := leaseDuration.Milliseconds()
	if leaseOwner == "" || len(leaseOwner) > 255 || leaseMillis <= 0 ||
		leaseDuration != time.Duration(leaseMillis)*time.Millisecond ||
		leaseMillis > invocation.Policy.TimeoutMillis {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{},
			capabilityInvocationValidation("lease is outside the frozen invocation policy", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{},
			capabilityInvocationDatabase("begin acquire transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := admitCapabilityInvocationTenant(ctx, tx, invocation); err != nil {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{}, err
	}
	authorityErr := validateCapabilityInvocationAuthority(ctx, tx, invocation, builtins)
	if err := setCapabilityInvocationRole(ctx, tx, invocation); err != nil {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{}, err
	}
	record, err := loadCapabilityInvocationRecordForUpdate(ctx, tx, invocation)
	if err != nil {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{}, err
	}
	if record.Invocation.InvocationDigest != invocation.InvocationDigest {
		return CapabilityInvocationLeaseV1{}, record,
			capabilityInvocationConflict("idempotency key is bound to a different invocation")
	}

	if record.Status == CapabilityInvocationExecuting {
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT lease_until<=clock_timestamp()
			FROM capability_invocations WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			invocation.Principal.TenantID, invocation.Principal.UserID, record.ID).Scan(&expired); err != nil {
			return CapabilityInvocationLeaseV1{}, record,
				capabilityInvocationDatabase("read invocation lease clock", err)
		}
		if expired {
			ambiguous, receiptErr := capabilityruntime.NewReceiptV1(invocation,
				capabilityruntime.ReceiptStatusAmbiguous, record.Attempt, "", nil,
				"lease_expired_unknown_effect", false)
			if receiptErr != nil {
				return CapabilityInvocationLeaseV1{}, record,
					capabilityInvocationValidation("build ambiguous receipt", receiptErr)
			}
			if err := appendCapabilityInvocationReceipt(ctx, tx, record, ambiguous, 1,
				record.LeaseOwner); err != nil {
				return CapabilityInvocationLeaseV1{}, record, err
			}
			if _, err := tx.Exec(ctx, `UPDATE capability_invocations
				SET status='unknown_effect',lease_owner='',lease_until=NULL,current_receipt_ordinal=1
				WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
				invocation.Principal.TenantID, invocation.Principal.UserID, record.ID); err != nil {
				return CapabilityInvocationLeaseV1{}, record,
					capabilityInvocationDatabase("quarantine expired invocation", err)
			}
			record, err = loadCapabilityInvocationRecord(ctx, tx, invocation)
			if err != nil {
				return CapabilityInvocationLeaseV1{}, record, err
			}
			if err := tx.Commit(ctx); err != nil {
				return CapabilityInvocationLeaseV1{}, record,
					capabilityInvocationDatabase("commit unknown invocation effect", err)
			}
			if authorityErr != nil {
				return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{}, authorityErr
			}
			return CapabilityInvocationLeaseV1{}, record,
				capabilityInvocationConflict("expired execution has an unknown effect and cannot be retried")
		}
		if authorityErr != nil {
			return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{}, authorityErr
		}
		return CapabilityInvocationLeaseV1{}, record,
			capabilityInvocationConflict("invocation is already executing")
	}
	if authorityErr != nil {
		return CapabilityInvocationLeaseV1{}, CapabilityInvocationRecordV1{}, authorityErr
	}
	if record.Status != CapabilityInvocationPending {
		return CapabilityInvocationLeaseV1{}, record,
			capabilityInvocationConflict("invocation is already settled and must be replayed")
	}

	var lease CapabilityInvocationLeaseV1
	err = tx.QueryRow(ctx, `UPDATE capability_invocations
		SET status='executing',lease_owner=$4,
		    lease_until=clock_timestamp()+($5*interval '1 millisecond'),fence=1,attempt=1
		WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		RETURNING id,tenant_id,user_id,invocation_digest,idempotency_digest,
		          lease_owner,fence,attempt,lease_until`,
		invocation.Principal.TenantID, invocation.Principal.UserID, record.ID,
		leaseOwner, leaseMillis).Scan(&lease.InvocationID, &lease.TenantID, &lease.UserID,
		&lease.InvocationDigest, &lease.IdempotencyDigest, &lease.LeaseOwner,
		&lease.Fence, &lease.Attempt, &lease.LeaseUntil)
	if err != nil {
		return CapabilityInvocationLeaseV1{}, record,
			capabilityInvocationDatabase("acquire invocation lease", err)
	}
	record, err = loadCapabilityInvocationRecord(ctx, tx, invocation)
	if err != nil {
		return CapabilityInvocationLeaseV1{}, record, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CapabilityInvocationLeaseV1{}, record,
			capabilityInvocationDatabase("commit invocation acquisition", err)
	}
	return lease, record, nil
}

// SettleCapabilityInvocationV1 records provider truth even if membership or
// A2A authority was revoked after Acquire. It proves the exact frozen
// invocation and lease instead of re-authorizing a new effect.
func (s *Store) SettleCapabilityInvocationV1(
	ctx context.Context,
	invocation capabilityruntime.InvocationV1,
	lease CapabilityInvocationLeaseV1,
	receipt capabilityruntime.ReceiptV1,
) (CapabilityInvocationRecordV1, error) {
	if err := receipt.ValidateFor(invocation); err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationValidation("receipt contract is invalid", err)
	}
	if receipt.Status == capabilityruntime.ReceiptStatusAmbiguous {
		return CapabilityInvocationRecordV1{}, capabilityInvocationValidation(
			"only the coordinator may create an ambiguous lease-expiry receipt", nil)
	}
	if lease.InvocationID == uuid.Nil || lease.TenantID != invocation.Principal.TenantID ||
		lease.UserID != invocation.Principal.UserID ||
		lease.InvocationDigest != invocation.InvocationDigest ||
		lease.IdempotencyDigest != invocation.IdempotencyDigest ||
		strings.TrimSpace(lease.LeaseOwner) == "" || lease.Fence != 1 || lease.Attempt != 1 ||
		receipt.Attempt != lease.Attempt {
		return CapabilityInvocationRecordV1{}, capabilityInvocationValidation("settlement lease is invalid", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("begin settlement transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := admitCapabilityInvocationTenant(ctx, tx, invocation); err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	if err := setCapabilityInvocationRole(ctx, tx, invocation); err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	record, err := loadCapabilityInvocationRecordForUpdate(ctx, tx, invocation)
	if err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	if record.ID != lease.InvocationID || record.Invocation.InvocationDigest != lease.InvocationDigest ||
		record.Fence != lease.Fence || record.Attempt != lease.Attempt {
		return CapabilityInvocationRecordV1{}, capabilityInvocationConflict("settlement does not match exact invocation lease")
	}
	if record.Status == CapabilityInvocationExecuting && record.LeaseOwner != lease.LeaseOwner {
		return CapabilityInvocationRecordV1{}, capabilityInvocationConflict("settlement lease owner differs")
	}
	if record.Status != CapabilityInvocationExecuting && record.Status != CapabilityInvocationUnknownEffect {
		if record.CurrentReceipt != nil && record.CurrentReceipt.ReceiptDigest == receipt.ReceiptDigest {
			if err := tx.Commit(ctx); err != nil {
				return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("commit settlement replay", err)
			}
			return record, nil
		}
		return CapabilityInvocationRecordV1{}, capabilityInvocationConflict("invocation already has a different terminal receipt")
	}
	ordinal := int64(1)
	if record.Status == CapabilityInvocationUnknownEffect {
		if record.CurrentReceiptOrdinal == nil || *record.CurrentReceiptOrdinal != 1 {
			return CapabilityInvocationRecordV1{}, capabilityInvocationConflict("unknown-effect receipt checkpoint is invalid")
		}
		ordinal = 2
	}
	if err := appendCapabilityInvocationReceipt(ctx, tx, record, receipt, ordinal,
		lease.LeaseOwner); err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	status := CapabilityInvocationStatusV1(receipt.Status)
	if _, err := tx.Exec(ctx, `UPDATE capability_invocations
		SET status=$4,lease_owner='',lease_until=NULL,current_receipt_ordinal=$5
		WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		invocation.Principal.TenantID, invocation.Principal.UserID, record.ID,
		status, ordinal); err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("checkpoint invocation settlement", err)
	}
	record, err = loadCapabilityInvocationRecord(ctx, tx, invocation)
	if err != nil {
		return CapabilityInvocationRecordV1{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("commit invocation settlement", err)
	}
	return record, nil
}

func admitCapabilityInvocationTenant(
	ctx context.Context, tx pgx.Tx, invocation capabilityruntime.InvocationV1,
) error {
	exists, err := lockTenantAdmissionRootShared(ctx, tx, int64(invocation.Principal.TenantID))
	if err != nil {
		return capabilityInvocationDatabase("lock tenant admission root", err)
	}
	if !exists {
		return capabilityInvocationForbidden("explicit invocation workspace does not exist")
	}
	return nil
}

func validateCapabilityInvocationAuthority(
	ctx context.Context,
	tx pgx.Tx,
	invocation capabilityruntime.InvocationV1,
	builtins BuiltinCapabilityRegistryV1,
) error {
	var liveGeneration int64
	err := tx.QueryRow(ctx, `SELECT m.authorization_generation
		FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		WHERE m.tenant_id=$1 AND m.user_id=$2 AND t.status='active' AND t.deleted_at IS NULL
		FOR SHARE OF m,t`, invocation.Principal.TenantID,
		invocation.Principal.UserID).Scan(&liveGeneration)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return capabilityInvocationForbidden("active workspace membership is required")
		}
		return capabilityInvocationDatabase("prove invocation membership", err)
	}
	if liveGeneration != invocation.Principal.MembershipAuthorizationGeneration {
		return capabilityInvocationForbidden("membership authority generation changed")
	}
	if invocation.Principal.ActorType == types.ActorTypeServiceAccount {
		tokenID, parseErr := uuid.Parse(invocation.Principal.A2ATokenAuthorityID)
		if parseErr != nil || tokenID == uuid.Nil {
			return capabilityInvocationForbidden("A2A token authority is invalid")
		}
		var authorized bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM a2a_access_tokens
			WHERE id=$1 AND tenant_id=$2 AND principal_user_id=$3 AND actor_type='service_account'
			  AND membership_generation=$4 AND revoked_at IS NULL AND expires_at>clock_timestamp()
			  AND scopes @> ARRAY[$5]::text[] FOR SHARE)`, tokenID,
			invocation.Principal.TenantID, invocation.Principal.UserID,
			invocation.Principal.MembershipAuthorizationGeneration,
			invocation.Principal.RequiredA2AScope).Scan(&authorized); err != nil {
			return capabilityInvocationDatabase("prove exact A2A token authority", err)
		}
		if !authorized {
			return capabilityInvocationForbidden("exact A2A token authority is inactive")
		}
	}

	switch invocation.Capability.Kind {
	case capabilityruntime.CapabilityKindBuiltinTool:
		if builtins == nil {
			return capabilityInvocationForbidden("builtin registry proof is required")
		}
		if err := builtins.VerifyBuiltinCapabilityV1(ctx, invocation.Capability,
			invocation.Operation); err != nil {
			return capabilityInvocationForbidden("builtin operation or schema is not registered")
		}
	case capabilityruntime.CapabilityKindDeclarativeSkill:
		if invocation.Credential != (capabilityruntime.CredentialRefV1{}) {
			return capabilityInvocationForbidden("declarative Skill cannot receive a credential")
		}
		if err := validateStoredCapabilityVersion(ctx, tx, invocation, "skill"); err != nil {
			return err
		}
	case capabilityruntime.CapabilityKindRemoteMCP:
		if err := validateStoredCapabilityVersion(ctx, tx, invocation, "mcp"); err != nil {
			return err
		}
	case capabilityruntime.CapabilityKindSandboxScript:
		return capabilityInvocationForbidden("sandbox scripts are not enabled in capability ledger v1")
	default:
		return capabilityInvocationForbidden("capability kind is unsupported")
	}
	// Migration 152 intentionally does not invent a credential alias mapping:
	// existing MCP versions retain an opaque vault:* label while vault rows are
	// addressed by scope/provider/purpose/generation. Until the independent
	// immutable binding substrate exists, accepting either coordinate set as a
	// proof of the other would let a caller substitute another same-tenant
	// secret. The ledger can freeze these fields, but this coordinator cannot
	// prepare any credentialful effect yet.
	if invocation.Credential != (capabilityruntime.CredentialRefV1{}) {
		return capabilityInvocationForbidden("immutable credential reference binding is not available")
	}
	return nil
}

func validateStoredCapabilityVersion(
	ctx context.Context, tx pgx.Tx, invocation capabilityruntime.InvocationV1, storedKind string,
) error {
	capabilityID, err := uuid.Parse(invocation.Capability.ID)
	if err != nil || capabilityID == uuid.Nil {
		return capabilityInvocationForbidden("capability identity is not an installed immutable version")
	}
	versionID, err := uuid.Parse(invocation.Capability.VersionID)
	if err != nil || versionID == uuid.Nil {
		return capabilityInvocationForbidden("capability version identity is invalid")
	}
	visibility := string(invocation.Capability.Scope)
	if visibility == string(capabilityruntime.CapabilityScopePersonal) {
		visibility = "personal"
	}
	var versionDigest, schemaDigest, credentialRef string
	var compatible bool
	if storedKind == "skill" {
		err = tx.QueryRow(ctx, `SELECT v.payload_digest,s.file_manifest_digest,v.compatible,''
			FROM user_capabilities c JOIN user_capability_versions v
			  ON (v.tenant_id,v.owner_user_id,v.capability_id)=(c.tenant_id,c.owner_user_id,c.id)
			JOIN skill_capability_versions s ON s.capability_version_id=v.id
			WHERE c.tenant_id=$1 AND c.owner_user_id=$2 AND c.id=$3 AND c.kind='skill'
			  AND c.visibility=$4 AND c.status='active' AND v.id=$5 AND NOT s.contains_scripts
			FOR SHARE OF c,v,s`, invocation.Principal.TenantID,
			invocation.Capability.OwnerUserID, capabilityID, visibility, versionID).
			Scan(&versionDigest, &schemaDigest, &compatible, &credentialRef)
	} else {
		err = tx.QueryRow(ctx, `SELECT v.payload_digest,m.tool_schema_digest,v.compatible,m.credential_ref
			FROM user_capabilities c JOIN user_capability_versions v
			  ON (v.tenant_id,v.owner_user_id,v.capability_id)=(c.tenant_id,c.owner_user_id,c.id)
			JOIN mcp_connection_versions m ON m.capability_version_id=v.id
			WHERE c.tenant_id=$1 AND c.owner_user_id=$2 AND c.id=$3 AND c.kind='mcp'
			  AND c.visibility=$4 AND c.status='active' AND v.id=$5
			FOR SHARE OF c,v,m`, invocation.Principal.TenantID,
			invocation.Capability.OwnerUserID, capabilityID, visibility, versionID).
			Scan(&versionDigest, &schemaDigest, &compatible, &credentialRef)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return capabilityInvocationForbidden("exact capability version is unavailable")
		}
		return capabilityInvocationDatabase("prove exact capability version", err)
	}
	if !compatible || versionDigest != invocation.Capability.VersionDigest ||
		schemaDigest != invocation.Capability.OperationSchemaDigest {
		return capabilityInvocationForbidden("capability version or schema digest changed")
	}
	if storedKind == "mcp" && credentialRef != invocation.Credential.OpaqueRef {
		return capabilityInvocationForbidden("MCP credential reference differs from frozen version")
	}
	return nil
}

func setCapabilityInvocationRole(
	ctx context.Context, tx pgx.Tx, invocation capabilityruntime.InvocationV1,
) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),
		set_config('app.user_id',$2,true)`, fmt.Sprint(invocation.Principal.TenantID),
		fmt.Sprint(invocation.Principal.UserID)); err != nil {
		return capabilityInvocationDatabase("set invocation RLS scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+capabilityInvocationCoordinatorRole); err != nil {
		return capabilityInvocationDatabase("enter invocation coordinator role", err)
	}
	return nil
}

func loadCapabilityInvocationRecord(
	ctx context.Context, queryer interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}, invocation capabilityruntime.InvocationV1,
) (CapabilityInvocationRecordV1, error) {
	return scanCapabilityInvocationRecord(queryer.QueryRow(ctx, `SELECT
		i.id,i.invocation_payload,i.status,i.lease_owner,i.lease_until,i.fence,i.attempt,
		i.current_receipt_ordinal,i.created_at,i.updated_at,r.receipt_payload
		FROM capability_invocations i LEFT JOIN capability_invocation_receipts r
		 ON (r.tenant_id,r.user_id,r.invocation_id,r.receipt_ordinal)=
		    (i.tenant_id,i.user_id,i.id,i.current_receipt_ordinal)
		WHERE i.tenant_id=$1 AND i.user_id=$2 AND i.idempotency_digest=$3`,
		invocation.Principal.TenantID, invocation.Principal.UserID,
		invocation.IdempotencyDigest), invocation)
}

func loadCapabilityInvocationRecordForUpdate(
	ctx context.Context, tx pgx.Tx, invocation capabilityruntime.InvocationV1,
) (CapabilityInvocationRecordV1, error) {
	return scanCapabilityInvocationRecord(tx.QueryRow(ctx, `SELECT
		i.id,i.invocation_payload,i.status,i.lease_owner,i.lease_until,i.fence,i.attempt,
		i.current_receipt_ordinal,i.created_at,i.updated_at,r.receipt_payload
		FROM capability_invocations i LEFT JOIN capability_invocation_receipts r
		 ON (r.tenant_id,r.user_id,r.invocation_id,r.receipt_ordinal)=
		    (i.tenant_id,i.user_id,i.id,i.current_receipt_ordinal)
		WHERE i.tenant_id=$1 AND i.user_id=$2 AND i.idempotency_digest=$3
		FOR UPDATE OF i`, invocation.Principal.TenantID, invocation.Principal.UserID,
		invocation.IdempotencyDigest), invocation)
}

func scanCapabilityInvocationRecord(
	row pgx.Row, expected capabilityruntime.InvocationV1,
) (CapabilityInvocationRecordV1, error) {
	var record CapabilityInvocationRecordV1
	var invocationPayload, receiptPayload []byte
	if err := row.Scan(&record.ID, &invocationPayload, &record.Status, &record.LeaseOwner,
		&record.LeaseUntil, &record.Fence, &record.Attempt,
		&record.CurrentReceiptOrdinal, &record.CreatedAt, &record.UpdatedAt,
		&receiptPayload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CapabilityInvocationRecordV1{}, capabilityInvocationConflict("invocation was not prepared")
		}
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("load invocation checkpoint", err)
	}
	invocation, err := capabilityruntime.DecodeInvocationV1(invocationPayload)
	if err != nil {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("decode durable invocation contract", err)
	}
	if invocation.Principal.TenantID != expected.Principal.TenantID ||
		invocation.Principal.UserID != expected.Principal.UserID ||
		invocation.IdempotencyDigest != expected.IdempotencyDigest {
		return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase(
			"durable invocation scope differs from its ledger key", types.ErrDatabase)
	}
	record.Invocation = invocation
	if len(receiptPayload) != 0 {
		receipt, err := capabilityruntime.DecodeReceiptV1(receiptPayload, invocation)
		if err != nil {
			return CapabilityInvocationRecordV1{}, capabilityInvocationDatabase("decode durable invocation receipt", err)
		}
		record.CurrentReceipt = &receipt
	}
	return record, nil
}

func appendCapabilityInvocationReceipt(
	ctx context.Context,
	tx pgx.Tx,
	record CapabilityInvocationRecordV1,
	receipt capabilityruntime.ReceiptV1,
	ordinal int64,
	leaseOwner string,
) error {
	payload, err := capabilityruntime.EncodeReceiptV1(receipt, record.Invocation)
	if err != nil {
		return capabilityInvocationValidation("receipt cannot be encoded", err)
	}
	var resultDigest, resultMediaType any
	var resultSize any
	var resultPayload any
	if receipt.Status == capabilityruntime.ReceiptStatusSucceeded {
		resultDigest = receipt.Result.Digest
		resultSize = receipt.Result.SizeBytes
		resultMediaType = receipt.Result.MediaType
		resultPayload = receipt.Result.SanitizedPayload
	}
	_, err = tx.Exec(ctx, `INSERT INTO capability_invocation_receipts(
		tenant_id,user_id,invocation_id,invocation_digest,idempotency_digest,
		receipt_ordinal,fence,attempt,settled_by_lease_owner,status,
		result_digest,result_size_bytes,result_media_type,sanitized_result_payload,
		error_class,retryable,receipt_digest,receipt_payload,receipt_payload_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
		record.Invocation.Principal.TenantID, record.Invocation.Principal.UserID,
		record.ID, record.Invocation.InvocationDigest, record.Invocation.IdempotencyDigest,
		ordinal, record.Fence, record.Attempt, leaseOwner, receipt.Status,
		resultDigest, resultSize, resultMediaType, resultPayload, receipt.ErrorClass,
		receipt.Retryable, receipt.ReceiptDigest, payload, capabilityInvocationSHA256(payload))
	if err != nil {
		return capabilityInvocationDatabase("append invocation receipt", err)
	}
	return nil
}

func nullableCapabilityCredential(credential capabilityruntime.CredentialRefV1) [8]any {
	if credential == (capabilityruntime.CredentialRefV1{}) {
		return [8]any{}
	}
	return [8]any{credential.OpaqueRef, credential.OpaqueRefDigest,
		credential.Provider, credential.Purpose, credential.Scope, credential.UserID,
		credential.Generation, credential.Fingerprint}
}

func stringMustEncodeInvocation(invocation capabilityruntime.InvocationV1) string {
	payload, err := capabilityruntime.EncodeInvocationV1(invocation)
	if err != nil {
		return ""
	}
	return string(payload)
}

func capabilityInvocationSHA256(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func capabilityInvocationValidation(message string, cause error) error {
	if cause == nil {
		cause = types.ErrValidation
	}
	return types.NewAppError(types.CodeValidation, message, cause)
}

func capabilityInvocationForbidden(message string) error {
	return types.NewAppError(types.CodeForbidden, message, types.ErrForbidden)
}

func capabilityInvocationConflict(message string) error {
	return types.NewAppError(types.CodeConflict, message, types.ErrConflict)
}

func capabilityInvocationDatabase(operation string, err error) error {
	return types.NewAppError(types.CodeDatabase, operation, err)
}
