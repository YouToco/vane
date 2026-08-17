package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/server/capabilityruntime"
	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

type capabilityInvocationScanRow struct {
	id             uuid.UUID
	invocation     []byte
	status         CapabilityInvocationStatusV1
	leaseOwner     string
	leaseUntil     *time.Time
	fence          int64
	attempt        int64
	receiptOrdinal *int64
	createdAt      time.Time
	updatedAt      time.Time
	receipt        []byte
	err            error
}

type capabilityInvocationFaultTx struct {
	pgx.Tx
	failContains string
	commitErr    error
	fired        bool
}

func (tx *capabilityInvocationFaultTx) Exec(
	ctx context.Context, sql string, arguments ...any,
) (pgconn.CommandTag, error) {
	if !tx.fired && tx.failContains != "" && strings.Contains(sql, tx.failContains) {
		tx.fired = true
		return pgconn.CommandTag{}, errors.New("injected capability invocation failure")
	}
	return tx.Tx.Exec(ctx, sql, arguments...)
}

func (tx *capabilityInvocationFaultTx) QueryRow(
	ctx context.Context, sql string, arguments ...any,
) pgx.Row {
	if !tx.fired && tx.failContains != "" && strings.Contains(sql, tx.failContains) {
		tx.fired = true
		return capabilityInvocationScanRow{err: errors.New("injected capability invocation failure")}
	}
	return tx.Tx.QueryRow(ctx, sql, arguments...)
}

func (tx *capabilityInvocationFaultTx) Commit(ctx context.Context) error {
	if tx.commitErr != nil {
		return tx.commitErr
	}
	return tx.Tx.Commit(ctx)
}

func (r capabilityInvocationScanRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 11 {
		return errors.New("unexpected scan destination count")
	}
	*(dest[0].(*uuid.UUID)) = r.id
	*(dest[1].(*[]byte)) = r.invocation
	*(dest[2].(*CapabilityInvocationStatusV1)) = r.status
	*(dest[3].(*string)) = r.leaseOwner
	*(dest[4].(**time.Time)) = r.leaseUntil
	*(dest[5].(*int64)) = r.fence
	*(dest[6].(*int64)) = r.attempt
	*(dest[7].(**int64)) = r.receiptOrdinal
	*(dest[8].(*time.Time)) = r.createdAt
	*(dest[9].(*time.Time)) = r.updatedAt
	*(dest[10].(*[]byte)) = r.receipt
	return nil
}

func TestCapabilityInvocationLocalRefusalAndDurableDecodeCoverage(t *testing.T) {
	invocation := capabilityInvocationTestBuiltin(t, 1, 2, 3, "coverage-local", `{"query":"x"}`)
	registry := exactBuiltinRegistryV1{capability: invocation.Capability, operation: invocation.Operation}
	invalid := invocation
	invalid.SchemaVersion = ""

	if _, err := (*Store)(nil).PrepareCapabilityInvocationV1(t.Context(), invalid, registry); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid prepare err=%v", err)
	}
	if _, _, err := (*Store)(nil).AcquireCapabilityInvocationV1(t.Context(), invalid, registry,
		"worker", time.Millisecond); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid acquire err=%v", err)
	}
	for name, input := range map[string]struct {
		owner    string
		duration time.Duration
	}{
		"blank owner":       {owner: "  ", duration: time.Millisecond},
		"long owner":        {owner: strings.Repeat("w", 256), duration: time.Millisecond},
		"zero duration":     {owner: "worker"},
		"fractional millis": {owner: "worker", duration: time.Millisecond + time.Nanosecond},
		"over policy":       {owner: "worker", duration: 3 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := (*Store)(nil).AcquireCapabilityInvocationV1(t.Context(), invocation,
				registry, input.owner, input.duration); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("err=%v", err)
			}
		})
	}

	if _, err := (*Store)(nil).SettleCapabilityInvocationV1(t.Context(), invocation,
		CapabilityInvocationLeaseV1{}, capabilityruntime.ReceiptV1{}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid receipt err=%v", err)
	}
	ambiguous, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusAmbiguous, 1, "", nil, "unknown", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (*Store)(nil).SettleCapabilityInvocationV1(t.Context(), invocation,
		CapabilityInvocationLeaseV1{}, ambiguous); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("caller ambiguous receipt err=%v", err)
	}
	succeeded, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusSucceeded, 1, "text/plain", []byte("ok"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (*Store)(nil).SettleCapabilityInvocationV1(t.Context(), invocation,
		CapabilityInvocationLeaseV1{} /* valid receipt, invalid lease */, succeeded); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("invalid lease err=%v", err)
	}

	payload, err := capabilityruntime.EncodeInvocationV1(invocation)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	base := capabilityInvocationScanRow{
		id: uuid.New(), invocation: payload, status: CapabilityInvocationPending,
		createdAt: now, updatedAt: now,
	}
	if _, err := scanCapabilityInvocationRecord(capabilityInvocationScanRow{err: pgx.ErrNoRows}, invocation); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("missing durable row err=%v", err)
	}
	if _, err := scanCapabilityInvocationRecord(capabilityInvocationScanRow{err: errors.New("scan failed")}, invocation); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("scan failure err=%v", err)
	}
	corrupt := base
	corrupt.invocation = []byte("{")
	if _, err := scanCapabilityInvocationRecord(corrupt, invocation); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("corrupt invocation err=%v", err)
	}
	wrongScope := invocation
	wrongScope.Principal.UserID++
	if _, err := scanCapabilityInvocationRecord(base, wrongScope); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("wrong durable scope err=%v", err)
	}
	corruptReceipt := base
	corruptReceipt.receipt = []byte("{")
	ordinal := int64(1)
	corruptReceipt.receiptOrdinal = &ordinal
	if _, err := scanCapabilityInvocationRecord(corruptReceipt, invocation); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("corrupt receipt err=%v", err)
	}
	receiptPayload, err := capabilityruntime.EncodeReceiptV1(succeeded, invocation)
	if err != nil {
		t.Fatal(err)
	}
	complete := base
	complete.receipt = receiptPayload
	complete.receiptOrdinal = &ordinal
	record, err := scanCapabilityInvocationRecord(complete, invocation)
	if err != nil || record.CurrentReceipt == nil || record.CurrentReceipt.ReceiptDigest != succeeded.ReceiptDigest {
		t.Fatalf("decoded record=%+v err=%v", record, err)
	}

	credential := capabilityruntime.CredentialRefV1{
		OpaqueRef: "vault:coverage", OpaqueRefDigest: strings.Repeat("a", 64),
		Provider: "provider", Purpose: "purpose", Scope: capabilityruntime.CredentialScopeTenant,
		Generation: 1, Fingerprint: strings.Repeat("b", 64),
	}
	if values := nullableCapabilityCredential(credential); values[0] != credential.OpaqueRef || values[6] != credential.Generation {
		t.Fatalf("credential values=%v", values)
	}
	if values := nullableCapabilityCredential(capabilityruntime.CredentialRefV1{}); values != ([8]any{}) {
		t.Fatalf("empty credential values=%v", values)
	}
	if got := stringMustEncodeInvocation(invalid); got != "" {
		t.Fatalf("invalid invocation encoded as %q", got)
	}
	if capabilityInvocationSHA256([]byte("x")) == "" ||
		!errors.Is(capabilityInvocationValidation("validation", nil), types.ErrValidation) ||
		!errors.Is(capabilityInvocationForbidden("forbidden"), types.ErrForbidden) ||
		!errors.Is(capabilityInvocationConflict("conflict"), types.ErrConflict) ||
		!errors.Is(capabilityInvocationDatabase("database", types.ErrDatabase), types.ErrDatabase) {
		t.Fatal("capability invocation error helpers lost their typed cause")
	}
}

func TestCapabilityInvocationAuthorityAndSettlementRefusalsPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 152); err != nil {
		t.Fatal(err)
	}
	userID, tenantID := migration128Identity(t, database)
	var generation int64
	if err := database.QueryRowContext(t.Context(), `SELECT authorization_generation
		FROM memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	invocation := capabilityInvocationTestBuiltin(t, tenantID, userID, generation,
		"coverage-db", `{"query":"x"}`)
	registry := exactBuiltinRegistryV1{capability: invocation.Capability, operation: invocation.Operation}

	beginFailure := &Store{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		return nil, errors.New("injected begin failure")
	}}
	if _, err := beginFailure.PrepareCapabilityInvocationV1(t.Context(), invocation, registry); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("prepare begin failure err=%v", err)
	}
	if _, _, err := beginFailure.AcquireCapabilityInvocationV1(t.Context(), invocation, registry,
		"worker", time.Second); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("acquire begin failure err=%v", err)
	}
	validReceipt, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusSucceeded, 1, "text/plain", []byte("ok"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	validLeaseShape := CapabilityInvocationLeaseV1{
		InvocationID: uuid.New(), TenantID: types.TenantID(tenantID), UserID: userID,
		InvocationDigest: invocation.InvocationDigest, IdempotencyDigest: invocation.IdempotencyDigest,
		LeaseOwner: "worker", Fence: 1, Attempt: 1,
	}
	if _, err := beginFailure.SettleCapabilityInvocationV1(t.Context(), invocation,
		validLeaseShape, validReceipt); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("settle begin failure err=%v", err)
	}

	roleFailure := *st
	roleFailure.beginTx = func(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
		tx, err := st.beginTx(ctx, options)
		if err != nil {
			return nil, err
		}
		return &capabilityInvocationFaultTx{Tx: tx, failContains: "SET LOCAL ROLE"}, nil
	}
	if _, err := roleFailure.PrepareCapabilityInvocationV1(t.Context(), invocation, registry); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("prepare role failure err=%v", err)
	}

	tx, err := st.beginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	invalidCapabilityID := invocation
	invalidCapabilityID.Capability.ID = "not-a-uuid"
	if err := validateStoredCapabilityVersion(t.Context(), tx, invalidCapabilityID, "skill"); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("invalid capability id err=%v", err)
	}
	invalidVersionID := invocation
	invalidVersionID.Capability.ID = uuid.NewString()
	invalidVersionID.Capability.VersionID = "not-a-uuid"
	if err := validateStoredCapabilityVersion(t.Context(), tx, invalidVersionID, "skill"); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("invalid capability version id err=%v", err)
	}
	missingMCP := invocation
	missingMCP.Capability.ID = uuid.NewString()
	missingMCP.Capability.VersionID = uuid.NewString()
	missingMCP.Capability.Scope = capabilityruntime.CapabilityScopePersonal
	missingMCP.Capability.OwnerUserID = userID
	if err := validateStoredCapabilityVersion(t.Context(), tx, missingMCP, "mcp"); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("missing MCP version err=%v", err)
	}
	credentialSkill := invocation
	credentialSkill.Capability.Kind = capabilityruntime.CapabilityKindDeclarativeSkill
	credentialSkill.Credential = capabilityruntime.CredentialRefV1{OpaqueRef: "vault:not-authorized"}
	if err := validateCapabilityInvocationAuthority(t.Context(), tx, credentialSkill, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("credentialed declarative Skill err=%v", err)
	}
	sandbox := invocation
	sandbox.Capability.Kind = capabilityruntime.CapabilityKindSandboxScript
	if err := validateCapabilityInvocationAuthority(t.Context(), tx, sandbox, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("sandbox authority err=%v", err)
	}
	unsupported := invocation
	unsupported.Capability.Kind = capabilityruntime.CapabilityKind("future-kind")
	if err := validateCapabilityInvocationAuthority(t.Context(), tx, unsupported, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("unsupported authority err=%v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	wrongTenant := capabilityInvocationTestBuiltin(t, tenantID+999_999, userID, generation,
		"wrong-tenant", `{"query":"x"}`)
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), wrongTenant,
		exactBuiltinRegistryV1{capability: wrongTenant.Capability, operation: wrongTenant.Operation}); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("missing workspace err=%v", err)
	}
	wrongGeneration := invocation
	wrongGeneration.Principal.MembershipAuthorizationGeneration++
	wrongGeneration, err = capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal: wrongGeneration.Principal, Capability: invocation.Capability,
		Operation: invocation.Operation, Policy: invocation.Policy,
		Arguments: json.RawMessage(`{"query":"generation"}`), IdempotencyKey: "wrong-generation",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), wrongGeneration,
		exactBuiltinRegistryV1{capability: wrongGeneration.Capability, operation: wrongGeneration.Operation}); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("generation drift err=%v", err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("missing builtin registry err=%v", err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation,
		exactBuiltinRegistryV1{}); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("registry mismatch err=%v", err)
	}

	service := invocation
	service.Principal.ActorType = types.ActorTypeServiceAccount
	service.Principal.A2ATokenAuthorityID = uuid.NewString()
	service.Principal.RequiredA2AScope = types.A2AScopeAssistantChat
	service, err = capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal: service.Principal, Capability: invocation.Capability,
		Operation: invocation.Operation, Policy: invocation.Policy,
		Arguments: json.RawMessage(`{"query":"service"}`), IdempotencyKey: "inactive-service",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), service,
		exactBuiltinRegistryV1{capability: service.Capability, operation: service.Operation}); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("inactive service authority err=%v", err)
	}

	missing := capabilityInvocationTestBuiltin(t, tenantID, userID, generation,
		"never-prepared", `{"query":"x"}`)
	if _, _, err := st.AcquireCapabilityInvocationV1(t.Context(), missing,
		exactBuiltinRegistryV1{capability: missing.Capability, operation: missing.Operation},
		"worker", time.Second); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("unprepared acquire err=%v", err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation, registry); err != nil {
		t.Fatal(err)
	}
	if _, _, err := roleFailure.AcquireCapabilityInvocationV1(t.Context(), invocation, registry,
		"worker", time.Second); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("acquire role failure err=%v", err)
	}
	lease, _, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation, registry,
		"worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusSucceeded, 1, "text/plain", []byte("ok"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roleFailure.SettleCapabilityInvocationV1(t.Context(), invocation,
		lease, receipt); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("settle role failure err=%v", err)
	}
	wrongOwner := lease
	wrongOwner.LeaseOwner = "other-worker"
	if _, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, wrongOwner, receipt); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("wrong lease owner err=%v", err)
	}
	wrongID := lease
	wrongID.InvocationID = uuid.New()
	if _, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, wrongID, receipt); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("wrong lease id err=%v", err)
	}
	if _, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, lease, receipt); err != nil {
		t.Fatal(err)
	}
	otherReceipt, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusDefiniteFailed, 1, "", nil, "provider_rejected", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, lease, otherReceipt); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("different terminal receipt err=%v", err)
	}
	if _, _, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation, registry,
		"worker", time.Second); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("settled reacquire err=%v", err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM memberships
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	removedMember := capabilityInvocationTestBuiltin(t, tenantID, userID, generation,
		"removed-member", `{"query":"x"}`)
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), removedMember,
		exactBuiltinRegistryV1{capability: removedMember.Capability,
			operation: removedMember.Operation}); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("removed membership err=%v", err)
	}
}
