package store

import (
	"bytes"
	"testing"
)

func TestMigration134ContainsWorkspaceIsolationInvariants(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/134_multi_workspace_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]byte{
		[]byte("workspace_invites"),
		[]byte("token_hash  BYTEA"),
		[]byte("uq_workspace_invites_pending_email"),
		[]byte("ck_workspace_invites_email_normalized"),
		[]byte("ck_memberships_role"),
		[]byte("uq_tenants_personal_owner"),
		[]byte("ENABLE ROW LEVEL SECURITY"),
		[]byte("FORCE ROW LEVEL SECURITY"),
		[]byte("app.workspace_invite_hash"),
	} {
		if !bytes.Contains(payload, want) {
			t.Fatalf("migration 134 missing %q", want)
		}
	}
	if bytes.Contains(payload, []byte("token TEXT")) {
		t.Fatal("migration must never persist raw invite tokens")
	}
}
