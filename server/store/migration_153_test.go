package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/mcpclient"
	"github.com/YouToco/vane/server/types"
	"github.com/google/uuid"
)

func TestMigration153MCPRuntimeBindingStaticContract(t *testing.T) {
	payload, err := os.ReadFile("migrations/153_mcp_runtime_binding.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, required := range []string{
		"CREATE TABLE mcp_runtime_bindings",
		"capability_version_digest",
		"endpoint_url",
		"protocol_version",
		"connection_schema_digest",
		"approved_catalog_digest",
		"credential_opaque_ref_digest=encode(sha256",
		"credential_entry_id",
		"credential_scope_kind",
		"credential_tenant_id",
		"credential_user_id",
		"credential_generation",
		"credential_fingerprint",
		"authentication_kind IN ('api_key','bearer')",
		"OAuth and unsupported MCP authentication remain disabled",
		"exact human approval authority is absent",
		"mcp_runtime_reader_authorized_v153",
		"membership.user_id=p_user_id",
		"tenant.status='active'",
		"ALTER TABLE mcp_runtime_bindings FORCE ROW LEVEL SECURITY",
		"TO vane_capability_invocation_coordinator",
		"REVOKE ALL ON mcp_runtime_bindings FROM PUBLIC,vane_app",
		"reject_mcp_runtime_binding_mutation_v1",
		"assert_vane_mcp_runtime_binding_v153",
		"pg_catalog.pg_get_constraintdef",
		"schema_safe := schema_digest=",
		"refusing downgrade while retained MCP runtime bindings exist",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("migration 153 omitted %q", required)
		}
	}
	for _, forbidden := range []string{
		"GRANT INSERT ON mcp_runtime_bindings TO vane_server_runtime",
		"GRANT UPDATE ON mcp_runtime_bindings TO vane_server_runtime",
		"GRANT vane_capability_invocation_coordinator TO vane_server_runtime",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("dark migration enabled production binding authority %q", forbidden)
		}
	}
}

func TestMigration153MCPRuntimeBindingPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 153); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	configureTestCredentialVault(t, st)
	ownerID, tenantID := migration129Identity(t, database, "mcp-binding-owner")

	remoteTools := []mcpclient.RemoteTool{{Name: "search.read", Description: "read",
		InputSchema: json.RawMessage(`{"type":"object"}`)}}
	catalog, err := mcpclient.FreezeReadOnlyTools(remoteTools,
		map[string]mcpclient.LocalToolPolicy{"search.read": {ReadOnly: true, Budget: 1}})
	if err != nil {
		t.Fatal(err)
	}
	manifest := json.RawMessage(`{"schema":"vane.mcp-test/v1"}`)
	manifestSum := sha256.Sum256(manifest)
	credentialRef := "vault:mcp-personal-fixture"
	capability, version, err := st.CreateMCPCapability(t.Context(), types.CreateMCPCapability{
		TenantID: tenantID, ActorUserID: ownerID, Visibility: types.UserCapabilityPersonal,
		Slug: "mcp-binding", DisplayName: "MCP binding",
		PayloadDigest: hex.EncodeToString(manifestSum[:]), Manifest: manifest,
		Connection: types.MCPConnectionVersion{EndpointURL: "https://mcp.example.com/rpc",
			ProtocolVersion: mcpclient.ProtocolVersion20251125,
			Authentication:  types.MCPAuthenticationBearer, CredentialRef: credentialRef,
			ToolSchemaDigest: catalog.Digest, ToolSchema: catalog.Payload},
	})
	if err != nil {
		t.Fatal(err)
	}
	credential, err := st.RotateCredential(t.Context(), CredentialScope{
		Kind: "user", TenantID: tenantID, UserID: ownerID,
		Provider: "mcp", Purpose: "remote_bearer",
	}, json.RawMessage(`{"value":"fixture"}`), json.RawMessage(`{}`), ownerID)
	if err != nil {
		t.Fatal(err)
	}
	var entryID int64
	if err := database.QueryRowContext(t.Context(), `SELECT id FROM credential_vault_entries
		WHERE scope_kind='user' AND tenant_id=$1 AND user_id=$2 AND provider='mcp' AND
		purpose='remote_bearer' AND generation=$3`, tenantID, ownerID,
		credential.Generation).Scan(&entryID); err != nil {
		t.Fatal(err)
	}
	refSum := sha256.Sum256([]byte(credentialRef))
	insertBinding := `INSERT INTO mcp_runtime_bindings(
		tenant_id,owner_user_id,capability_id,capability_version_id,visibility,
		capability_version_digest,endpoint_url,protocol_version,authentication_kind,
		connection_schema_digest,approved_catalog_digest,approved_by_user_id,
		credential_opaque_ref,credential_opaque_ref_digest,credential_entry_id,
		credential_scope_kind,credential_tenant_id,credential_user_id,
		credential_provider,credential_purpose,credential_generation,credential_fingerprint)
		VALUES($1,$2,$3,$4,'personal',$5,'https://mcp.example.com/rpc',$6,'bearer',
		       $7,$7,$2,$8,$9,$10,'user',$1,$2,
		       'mcp','remote_bearer',$11,$12)`
	if _, err := database.ExecContext(t.Context(), insertBinding, tenantID, ownerID,
		capability.ID, version.ID, hex.EncodeToString(manifestSum[:]),
		mcpclient.ProtocolVersion20251125, catalog.Digest, credentialRef,
		hex.EncodeToString(refSum[:]), entryID, credential.Generation,
		credential.Fingerprint); err != nil {
		t.Fatal(err)
	}
	purgeReport, err := st.PurgeTenant(t.Context(), tenantID, true)
	if err != nil || purgeReport.Rows["mcp_runtime_bindings"] != 1 {
		t.Fatalf("binding tenant-purge dry run=%+v err=%v", purgeReport, err)
	}
	var retainedAfterDryRun int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM mcp_runtime_bindings
		WHERE tenant_id=$1`, tenantID).Scan(&retainedAfterDryRun); err != nil ||
		retainedAfterDryRun != 1 {
		t.Fatalf("binding dry-run retention=%d err=%v", retainedAfterDryRun, err)
	}

	workspaceManifest := json.RawMessage(`{"schema":"vane.mcp-workspace-test/v1"}`)
	workspaceSum := sha256.Sum256(workspaceManifest)
	workspaceCapability, workspaceVersion, err := st.CreateMCPCapability(t.Context(),
		types.CreateMCPCapability{
			TenantID: tenantID, ActorUserID: ownerID, Visibility: types.UserCapabilityWorkspace,
			Slug: "mcp-workspace-binding", DisplayName: "MCP workspace binding",
			PayloadDigest: hex.EncodeToString(workspaceSum[:]), Manifest: workspaceManifest,
			Connection: types.MCPConnectionVersion{EndpointURL: "https://workspace-mcp.example.com/rpc",
				ProtocolVersion:  mcpclient.ProtocolVersion20251125,
				Authentication:   types.MCPAuthenticationNone,
				ToolSchemaDigest: catalog.Digest, ToolSchema: catalog.Payload},
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO mcp_runtime_bindings(
		tenant_id,owner_user_id,capability_id,capability_version_id,visibility,
		capability_version_digest,endpoint_url,protocol_version,authentication_kind,
		connection_schema_digest,approved_catalog_digest,approved_by_user_id)
		VALUES($1,$2,$3,$4,'workspace',$5,'https://workspace-mcp.example.com/rpc',
		       $6,'none',$7,$7,$2)`, tenantID, ownerID, workspaceCapability.ID,
		workspaceVersion.ID, hex.EncodeToString(workspaceSum[:]),
		mcpclient.ProtocolVersion20251125, catalog.Digest); err != nil {
		t.Fatal(err)
	}
	var memberID int64
	if err := database.QueryRowContext(t.Context(), `INSERT INTO users(feishu_open_id,name)
		VALUES($1,$1) RETURNING id`, "mcp-workspace-reader-"+uuid.NewString()).Scan(&memberID); err != nil {
		t.Fatal(err)
	}
	readWorkspaceBinding := func(userID int64) int {
		t.Helper()
		tx, err := database.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(t.Context(), `SELECT
			set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)`,
			fmt.Sprint(tenantID), fmt.Sprint(userID)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(),
			`SET LOCAL ROLE vane_capability_invocation_coordinator`); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := tx.QueryRowContext(t.Context(), `SELECT count(*) FROM mcp_runtime_bindings
			WHERE capability_version_id=$1`, workspaceVersion.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if count := readWorkspaceBinding(memberID); count != 0 {
		t.Fatalf("non-member read workspace MCP binding count=%d", count)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO memberships(tenant_id,user_id,role)
		VALUES($1,$2,'member')`, tenantID, memberID); err != nil {
		t.Fatal(err)
	}
	if count := readWorkspaceBinding(memberID); count != 1 {
		t.Fatalf("live member read workspace MCP binding count=%d", count)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE tenants SET status='suspended'
		WHERE id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	if count := readWorkspaceBinding(memberID); count != 0 {
		t.Fatalf("suspended workspace retained MCP authority count=%d", count)
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE tenants SET status='active'
		WHERE id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM memberships
		WHERE tenant_id=$1 AND user_id=$2`, tenantID, memberID); err != nil {
		t.Fatal(err)
	}
	if count := readWorkspaceBinding(memberID); count != 0 {
		t.Fatalf("removed member retained workspace MCP authority count=%d", count)
	}

	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{"application SELECT", `GRANT SELECT ON mcp_runtime_bindings TO vane_app`},
		{"application column SELECT", `GRANT SELECT(credential_fingerprint)
			ON mcp_runtime_bindings TO vane_app`},
		{"tenantless RLS", `ALTER POLICY mcp_runtime_binding_exact_principal
			ON mcp_runtime_bindings USING (true)`},
		{"disabled immutable trigger", `ALTER TABLE mcp_runtime_bindings
			DISABLE TRIGGER mcp_runtime_binding_immutable_v1`},
		{"replaced immutable function", `CREATE OR REPLACE FUNCTION
			public.reject_mcp_runtime_binding_mutation_v1() RETURNS trigger
			LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public,pg_temp
			AS $replacement$ BEGIN RETURN OLD; END $replacement$`},
		{"dropped credential CHECK", `ALTER TABLE mcp_runtime_bindings
			DROP CONSTRAINT ck_mcp_runtime_binding_credential`},
		{"dropped credential FK", `ALTER TABLE mcp_runtime_bindings
			DROP CONSTRAINT mcp_runtime_bindings_credential_entry_id_fkey`},
		{"dropped primary key", `ALTER TABLE mcp_runtime_bindings
			DROP CONSTRAINT mcp_runtime_bindings_pkey`},
		{"added unique", `ALTER TABLE mcp_runtime_bindings ADD CONSTRAINT
			uq_mcp_runtime_binding_unexpected UNIQUE(capability_version_id)`},
		{"changed column nullability", `ALTER TABLE mcp_runtime_bindings
			ALTER COLUMN approved_at DROP NOT NULL`},
		{"changed column default", `ALTER TABLE mcp_runtime_bindings
			ALTER COLUMN approved_at SET DEFAULT now()`},
	} {
		t.Run("assert rejects "+mutation.name, func(t *testing.T) {
			tx, err := database.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(t.Context(), mutation.sql); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(t.Context(),
				`SELECT public.assert_vane_mcp_runtime_binding_v153()`); err == nil {
				t.Fatal("unsafe MCP binding catalog passed assertion")
			}
		})
	}
	if _, err := database.ExecContext(t.Context(), `UPDATE mcp_runtime_bindings
		SET approved_catalog_digest=$1 WHERE capability_version_id=$2`,
		strings.Repeat("f", 64), version.ID); err == nil ||
		!strings.Contains(err.Error(), "immutable") {
		t.Fatalf("immutable update err=%v", err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM mcp_runtime_bindings
		WHERE capability_version_id=$1`, version.ID); err == nil ||
		!strings.Contains(err.Error(), "immutable") {
		t.Fatalf("immutable delete err=%v", err)
	}
	if _, err := provider.DownTo(t.Context(), 152); err == nil ||
		!strings.Contains(err.Error(), "retained MCP runtime bindings") {
		t.Fatalf("retained binding downgrade err=%v", err)
	}

	// Exact mismatch must fail before any future runtime can observe it.
	otherManifest := json.RawMessage(`{"schema":"vane.mcp-test/v1","other":true}`)
	otherSum := sha256.Sum256(otherManifest)
	otherCapability, otherVersion, err := st.CreateMCPCapability(t.Context(), types.CreateMCPCapability{
		TenantID: tenantID, ActorUserID: ownerID, Visibility: types.UserCapabilityPersonal,
		Slug: "mcp-binding-mismatch", DisplayName: "MCP mismatch",
		PayloadDigest: hex.EncodeToString(otherSum[:]), Manifest: otherManifest,
		Connection: types.MCPConnectionVersion{EndpointURL: "https://mcp.example.com/rpc",
			ProtocolVersion: mcpclient.ProtocolVersion20251125,
			Authentication:  types.MCPAuthenticationBearer, CredentialRef: credentialRef,
			ToolSchemaDigest: catalog.Digest, ToolSchema: catalog.Payload},
	})
	if err != nil {
		t.Fatal(err)
	}
	swappedEndpointInsert := strings.Replace(insertBinding,
		"'https://mcp.example.com/rpc'", "'https://swapped.example.com/rpc'", 1)
	_, err = database.ExecContext(t.Context(), swappedEndpointInsert, tenantID, ownerID,
		otherCapability.ID, otherVersion.ID, hex.EncodeToString(otherSum[:]),
		mcpclient.ProtocolVersion20251125, catalog.Digest, credentialRef,
		hex.EncodeToString(refSum[:]), entryID, credential.Generation,
		credential.Fingerprint)
	if err == nil || !strings.Contains(err.Error(), "exact MCP version connection") {
		t.Fatalf("same-catalog endpoint substitution err=%v", err)
	}
	_, err = database.ExecContext(t.Context(), insertBinding, tenantID, ownerID,
		otherCapability.ID, otherVersion.ID, hex.EncodeToString(otherSum[:]),
		mcpclient.ProtocolVersion20251125, catalog.Digest, credentialRef,
		hex.EncodeToString(refSum[:]), entryID, credential.Generation,
		strings.Repeat("0", 64))
	if err == nil || !strings.Contains(err.Error(), "vault coordinates differ") {
		t.Fatalf("mismatched fingerprint binding err=%v", err)
	}

	cleanupTx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cleanupTx.ExecContext(t.Context(), `SELECT
		set_config('app.tenant_id',$1,true),set_config('app.tenant_purge','on',true)`,
		fmt.Sprint(tenantID)); err != nil {
		cleanupTx.Rollback()
		t.Fatal(err)
	}
	if _, err := cleanupTx.ExecContext(t.Context(), `DELETE FROM mcp_runtime_bindings
		WHERE tenant_id=$1`, tenantID); err != nil {
		cleanupTx.Rollback()
		t.Fatal(err)
	}
	if err := cleanupTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 152); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialfulCapabilityLedgerStillFailsClosedUntilRuntimeBindingActivation(t *testing.T) {
	// This slice intentionally does not remove migration 152's credentialful
	// Prepare rejection. Keep a source-level guard so a later partial refactor
	// cannot claim the migration-153 row alone enabled secret use.
	payload, err := os.ReadFile("capability_invocations.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload),
		"immutable credential reference binding is not available") {
		t.Fatal(errors.New("credentialful ledger fail-closed guard disappeared"))
	}
}
