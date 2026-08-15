package store

import (
	"bytes"
	"testing"
)

func TestMigration137AccountSecurityInvariants(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/137_account_email_security_lifecycle.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]byte{
		[]byte("account_security_tokens"),
		[]byte("account_security_audit_events"),
		[]byte("token_hash         BYTEA"),
		[]byte("octet_length(token_hash)=32"),
		[]byte("email_verification"),
		[]byte("password_reset"),
		[]byte("reauth"),
		[]byte("ENABLE ROW LEVEL SECURITY"),
		[]byte("FORCE ROW LEVEL SECURITY"),
		[]byte("app.account_security_token_hash"),
		[]byte("GRANT SELECT,INSERT ON account_security_audit_events"),
	} {
		if !bytes.Contains(payload, want) {
			t.Fatalf("migration 137 missing %q", want)
		}
	}
	for _, forbidden := range [][]byte{
		[]byte("token TEXT"),
		[]byte("raw_token"),
		[]byte("password TEXT"),
	} {
		if bytes.Contains(payload, forbidden) {
			t.Fatalf("migration 137 persists forbidden secret shape %q", forbidden)
		}
	}
}
