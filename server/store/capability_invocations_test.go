package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/server/capabilityruntime"
	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/types"
)

type exactBuiltinRegistryV1 struct {
	capability capabilityruntime.CapabilityRefV1
	operation  string
}

func (r exactBuiltinRegistryV1) VerifyBuiltinCapabilityV1(
	_ context.Context, capability capabilityruntime.CapabilityRefV1, operation string,
) error {
	if capability != r.capability || operation != r.operation {
		return errors.New("builtin registry mismatch")
	}
	return nil
}

func TestCapabilityInvocationCoordinatorPostgres(t *testing.T) {
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
		"effect-1", `{"query":"vane","limit":10}`)
	registry := exactBuiltinRegistryV1{capability: invocation.Capability, operation: invocation.Operation}
	prepared, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation, registry)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Status != CapabilityInvocationPending || prepared.ID.String() == "" {
		t.Fatalf("prepared=%+v", prepared)
	}
	replay, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation, registry)
	if err != nil || replay.ID != prepared.ID {
		t.Fatalf("prepare replay id=%s err=%v", replay.ID, err)
	}

	// Stable idempotency ignores arguments and frozen version coordinates, so
	// same-key drift must conflict instead of opening a second effect identity.
	drift := capabilityInvocationTestBuiltin(t, tenantID, userID, generation,
		"effect-1", `{"query":"other","limit":10}`)
	if drift.IdempotencyDigest != invocation.IdempotencyDigest ||
		drift.InvocationDigest == invocation.InvocationDigest {
		t.Fatal("test fixture did not create stable-idempotency invocation drift")
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), drift,
		exactBuiltinRegistryV1{capability: drift.Capability, operation: drift.Operation}); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("same key drift err=%v, want conflict", err)
	}
	opaqueRef := "vault:unbound-search-key"
	credentialful, err := capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal: invocation.Principal, Capability: invocation.Capability,
		Operation: invocation.Operation, Policy: invocation.Policy,
		Credential: capabilityruntime.CredentialRefV1{
			OpaqueRef: opaqueRef, OpaqueRefDigest: capabilityInvocationSHA256([]byte(opaqueRef)),
			Provider: "exa", Purpose: "search", Scope: capabilityruntime.CredentialScopeTenant,
			Generation: 1, Fingerprint: strings.Repeat("e", 64),
		},
		Arguments: json.RawMessage(`{"query":"credential"}`), IdempotencyKey: "unbound-credential",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), credentialful,
		exactBuiltinRegistryV1{capability: credentialful.Capability,
			operation: credentialful.Operation}); !errors.Is(err, types.ErrForbidden) ||
		!strings.Contains(err.Error(), "credential reference binding") {
		t.Fatalf("unbound opaque credential ref err=%v", err)
	}

	lease, executing, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation,
		registry, "worker-a", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if executing.Status != CapabilityInvocationExecuting || lease.Fence != 1 || lease.Attempt != 1 {
		t.Fatalf("lease=%+v record=%+v", lease, executing)
	}
	if _, busy, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation,
		registry, "worker-a", 500*time.Millisecond); !errors.Is(err, types.ErrConflict) ||
		busy.Status != CapabilityInvocationExecuting {
		t.Fatalf("ordinary reacquire status=%s err=%v", busy.Status, err)
	}

	// Revocation after execution begins must stop new effects but cannot erase
	// the provider truth already returned to the exact lease holder.
	if _, err := database.ExecContext(t.Context(), `DELETE FROM memberships
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		t.Fatal(err)
	}
	receipt, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusSucceeded, 1, "application/json",
		[]byte(`{"answer":"ok"}`), "", false)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, lease, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != CapabilityInvocationSucceeded || settled.CurrentReceipt == nil ||
		settled.CurrentReceipt.ReceiptDigest != receipt.ReceiptDigest {
		t.Fatalf("settled=%+v", settled)
	}
	settlementReplay, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, lease, receipt)
	if err != nil || settlementReplay.CurrentReceipt == nil ||
		settlementReplay.CurrentReceipt.ReceiptDigest != receipt.ReceiptDigest {
		t.Fatalf("settlement replay=%+v err=%v", settlementReplay, err)
	}

	var invocationCount, receiptCount int
	if err := database.QueryRowContext(t.Context(), `SELECT
		(SELECT count(*) FROM capability_invocations WHERE tenant_id=$1),
		(SELECT count(*) FROM capability_invocation_receipts WHERE tenant_id=$1)`,
		tenantID).Scan(&invocationCount, &receiptCount); err != nil {
		t.Fatal(err)
	}
	if invocationCount != 1 || receiptCount != 1 {
		t.Fatalf("durable rows invocation=%d receipt=%d", invocationCount, receiptCount)
	}
	if _, err := provider.DownTo(t.Context(), 151); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade") {
		t.Fatalf("nonempty migration 152 downgrade err=%v", err)
	}
}

func TestCapabilityInvocationExpiredLeaseBecomesUnknownEffect(t *testing.T) {
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
		"effect-expiry", `{"query":"lease"}`)
	registry := exactBuiltinRegistryV1{capability: invocation.Capability, operation: invocation.Operation}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation, registry); err != nil {
		t.Fatal(err)
	}
	lease, _, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation,
		registry, "worker-expired", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var expired bool
		if err := database.QueryRowContext(t.Context(), `SELECT lease_until<=clock_timestamp()
			FROM capability_invocations WHERE id=$1`, lease.InvocationID).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("database-clock lease did not expire")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, unknown, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation,
		registry, "worker-other", 30*time.Millisecond); !errors.Is(err, types.ErrConflict) ||
		unknown.Status != CapabilityInvocationUnknownEffect || unknown.CurrentReceipt == nil ||
		unknown.CurrentReceipt.Status != capabilityruntime.ReceiptStatusAmbiguous ||
		unknown.CurrentReceipt.Retryable {
		t.Fatalf("expired acquire status=%s receipt=%+v err=%v",
			unknown.Status, unknown.CurrentReceipt, err)
	}
	if _, stillUnknown, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation,
		registry, "worker-third", 30*time.Millisecond); !errors.Is(err, types.ErrConflict) ||
		stillUnknown.Status != CapabilityInvocationUnknownEffect {
		t.Fatalf("unknown effect was reacquired status=%s err=%v", stillUnknown.Status, err)
	}

	// A delayed exact worker response can replace the ambiguous current truth,
	// but only by appending ordinal two; the ambiguous receipt remains durable.
	late, err := capabilityruntime.NewReceiptV1(invocation,
		capabilityruntime.ReceiptStatusSucceeded, 1, "text/plain", []byte("late truth"), "", false)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := st.SettleCapabilityInvocationV1(t.Context(), invocation, lease, late)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != CapabilityInvocationSucceeded || settled.CurrentReceiptOrdinal == nil ||
		*settled.CurrentReceiptOrdinal != 2 {
		t.Fatalf("late settlement=%+v", settled)
	}
	var ordinals []int64
	rows, err := database.QueryContext(t.Context(), `SELECT receipt_ordinal
		FROM capability_invocation_receipts WHERE invocation_id=$1 ORDER BY receipt_ordinal`,
		lease.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ordinal int64
		if err := rows.Scan(&ordinal); err != nil {
			t.Fatal(err)
		}
		ordinals = append(ordinals, ordinal)
	}
	if len(ordinals) != 2 || ordinals[0] != 1 || ordinals[1] != 2 {
		t.Fatalf("receipt ordinals=%v", ordinals)
	}
	dryRun, err := st.PurgeTenant(t.Context(), tenantID, true)
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.Rows["capability_invocations"] != 1 ||
		dryRun.Rows["capability_invocation_receipts"] != 2 {
		t.Fatalf("capability purge dry-run rows=%v", dryRun.Rows)
	}
	report, err := st.PurgeTenant(t.Context(), tenantID, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows["capability_invocations"] != 1 ||
		report.Rows["capability_invocation_receipts"] != 2 {
		t.Fatalf("capability purge rows=%v", report.Rows)
	}
}

func TestCapabilityInvocationConcurrentAcquireSingleLease(t *testing.T) {
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
		"effect-race", `{"query":"race"}`)
	registry := exactBuiltinRegistryV1{capability: invocation.Capability, operation: invocation.Operation}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), invocation, registry); err != nil {
		t.Fatal(err)
	}
	const contenders = 8
	var wg sync.WaitGroup
	wg.Add(contenders)
	errs := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			defer wg.Done()
			_, _, err := st.AcquireCapabilityInvocationV1(t.Context(), invocation,
				registry, "worker-race", time.Second)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	var acquired, conflicted int
	for err := range errs {
		switch {
		case err == nil:
			acquired++
		case errors.Is(err, types.ErrConflict):
			conflicted++
		default:
			t.Fatalf("concurrent acquire err=%v", err)
		}
	}
	if acquired != 1 || conflicted != contenders-1 {
		t.Fatalf("acquired=%d conflicted=%d", acquired, conflicted)
	}
}

func TestCapabilityInvocationServiceAuthorityAndDeclarativeSkillPostgres(t *testing.T) {
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

	service := migration139Issue(t, st, types.IssueA2AAccessToken{
		TenantID: tenantID, ActorUserID: userID, PrincipalUserID: userID,
		ActorType: types.ActorTypeServiceAccount, ServiceAccountLabel: "capability-reader",
		Scopes:    []types.A2AScope{types.A2AScopeAssistantChat},
		TokenHash: bytes.Repeat([]byte{0x52}, 32), ExpiresAt: time.Now().Add(time.Hour),
	}, "capability-service")
	serviceInvocation := capabilityInvocationTestBuiltinWithPrincipal(t,
		capabilityruntime.PrincipalV1{
			TenantID: types.TenantID(tenantID), UserID: userID, Role: types.MembershipRoleOwner,
			ActorType:                         types.ActorTypeServiceAccount,
			MembershipAuthorizationGeneration: generation,
			A2ATokenAuthorityID:               service.ID, RequiredA2AScope: types.A2AScopeAssistantChat,
		}, "service-effect", `{"query":"service"}`)
	registry := exactBuiltinRegistryV1{capability: serviceInvocation.Capability,
		operation: serviceInvocation.Operation}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), serviceInvocation, registry); err != nil {
		t.Fatal(err)
	}
	if err := st.RevokeA2AAccessToken(t.Context(), tenantID, userID, service.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.AcquireCapabilityInvocationV1(t.Context(), serviceInvocation,
		registry, "service-worker", time.Second); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("revoked service token acquire err=%v, want forbidden", err)
	}

	manifest := json.RawMessage(`{"schema_version":"vane.skill-package/v1"}`)
	fileManifest := json.RawMessage(`{"files":["SKILL.md"]}`)
	skillMD := []byte("# Read-only market analyst")
	capability, version, err := st.CreateSkillCapability(t.Context(), types.CreateSkillCapability{
		TenantID: tenantID, ActorUserID: userID, Visibility: types.UserCapabilityPersonal,
		Slug: "market-analyst", DisplayName: "Market Analyst", Source: types.UserCapabilityUpload,
		PayloadDigest: digestBytes(manifest), Manifest: manifest, Compatible: true,
		Skill: types.SkillCapabilityVersion{
			Name: "market-analyst", Description: "Read-only analyst",
			SkillMDDigest: digestBytes(skillMD), ArchiveDigest: strings.Repeat("c", 64),
			FileManifest: fileManifest,
			Files: []types.SkillCapabilityFile{{
				Path: "SKILL.md", Kind: "skill_md", Digest: digestBytes(skillMD), Content: skillMD,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE user_capabilities
		SET status='active' WHERE tenant_id=$1 AND id=$2`, tenantID, capability.ID); err != nil {
		t.Fatal(err)
	}
	skillInvocation, err := capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal: capabilityruntime.PrincipalV1{
			TenantID: types.TenantID(tenantID), UserID: userID, Role: types.MembershipRoleOwner,
			ActorType: types.ActorTypeUser, MembershipAuthorizationGeneration: generation,
		},
		Capability: capabilityruntime.CapabilityRefV1{
			Kind:  capabilityruntime.CapabilityKindDeclarativeSkill,
			Scope: capabilityruntime.CapabilityScopePersonal, OwnerUserID: userID,
			ID: capability.ID.String(), VersionID: version.ID.String(),
			VersionDigest:         version.PayloadDigest,
			OperationSchemaDigest: digestBytes(fileManifest),
		},
		Operation: "render_context",
		Policy: capabilityruntime.PolicyV1{
			SchemaVersion: capabilityruntime.PolicySchemaVersionV1,
			Effects:       []capabilityruntime.EffectV1{capabilityruntime.EffectInternalRead},
			ReadOnly:      true, Network: capabilityruntime.NetworkPolicyNone,
			Isolation: capabilityruntime.IsolationInProcess, TimeoutMillis: 1_000,
			MaxAttempts: 1, MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10,
		},
		Arguments: json.RawMessage(`{"topic":"pricing"}`), IdempotencyKey: "skill-effect",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), skillInvocation, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("compatible draft without activated lifecycle err=%v, want forbidden", err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO user_capability_events(
		tenant_id,capability_id,owner_user_id,visibility,actor_user_id,event_kind,version_id,details)
		VALUES($1,$2,$3,'personal',$3,'activated',$4,'{}')`,
		tenantID, capability.ID, userID, version.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), skillInvocation, nil); err != nil {
		t.Fatal(err)
	}
	var successorID string
	if err := database.QueryRowContext(t.Context(), `INSERT INTO user_capability_versions(
		id,capability_id,tenant_id,owner_user_id,version,visibility,source_kind,
		source_ref,payload_digest,manifest_payload,compatible,created_by)
		SELECT gen_random_uuid(),capability_id,tenant_id,owner_user_id,version+1,visibility,
		       source_kind,source_ref,payload_digest,manifest_payload,compatible,created_by
		FROM user_capability_versions WHERE id=$1 RETURNING id`, version.ID).Scan(&successorID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE user_capabilities
		SET current_version_id=$3 WHERE tenant_id=$1 AND id=$2`,
		tenantID, capability.ID, successorID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), skillInvocation, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("previously activated historical version err=%v, want forbidden", err)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE user_capabilities
		SET current_version_id=$3 WHERE tenant_id=$1 AND id=$2`,
		tenantID, capability.ID, version.ID); err != nil {
		t.Fatal(err)
	}
	badRef := skillInvocation.Capability
	badRef.OperationSchemaDigest = strings.Repeat("d", 64)
	badSchema, err := capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal: skillInvocation.Principal, Capability: badRef,
		Operation: skillInvocation.Operation, Policy: skillInvocation.Policy,
		Arguments: json.RawMessage(`{"topic":"pricing"}`), IdempotencyKey: "bad-skill-schema",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareCapabilityInvocationV1(t.Context(), badSchema, nil); !errors.Is(err, types.ErrForbidden) {
		t.Fatalf("drifted declarative Skill schema err=%v", err)
	}
}

func capabilityInvocationTestBuiltin(
	t *testing.T, tenantID, userID, generation int64, key, arguments string,
) capabilityruntime.InvocationV1 {
	t.Helper()
	return capabilityInvocationTestBuiltinWithPrincipal(t, capabilityruntime.PrincipalV1{
		TenantID: types.TenantID(tenantID), UserID: userID, Role: types.MembershipRoleOwner,
		ActorType: types.ActorTypeUser, MembershipAuthorizationGeneration: generation,
	}, key, arguments)
}

func capabilityInvocationTestBuiltinWithPrincipal(
	t *testing.T, principal capabilityruntime.PrincipalV1, key, arguments string,
) capabilityruntime.InvocationV1 {
	t.Helper()
	capability := capabilityruntime.CapabilityRefV1{
		Kind: capabilityruntime.CapabilityKindBuiltinTool, Scope: capabilityruntime.CapabilityScopePlatform,
		ID: "web_search", VersionID: "builtin-v1", OwnerUserID: 0,
		VersionDigest: strings.Repeat("a", 64), OperationSchemaDigest: strings.Repeat("b", 64),
	}
	invocation, err := capabilityruntime.NewInvocationV1(capabilityruntime.InvocationInputV1{
		Principal:  principal,
		Capability: capability, Operation: "web_search",
		Policy: capabilityruntime.PolicyV1{
			SchemaVersion: capabilityruntime.PolicySchemaVersionV1,
			Effects: []capabilityruntime.EffectV1{
				capabilityruntime.EffectBillable, capabilityruntime.EffectNetworkRead,
			},
			ReadOnly: true, Network: capabilityruntime.NetworkPolicyPublicHTTPSReadOnly,
			Isolation: capabilityruntime.IsolationInProcess, TimeoutMillis: 2_000,
			MaxAttempts: 1, MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10,
		},
		Arguments: json.RawMessage(arguments), IdempotencyKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	return invocation
}
