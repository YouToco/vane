package credentialvault

import (
	"bytes"
	"strings"
	"testing"
)

func testVault(t *testing.T) *Vault {
	t.Helper()
	vault, err := New(Config{
		ActiveKeyID: "key-1", ActiveKey: bytes.Repeat([]byte{0x42}, 32),
		RetiredKeys: map[string][]byte{"key-0": bytes.Repeat([]byte{0x24}, 32)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return vault
}

func TestSealOpenRoundTripAndRandomNonce(t *testing.T) {
	vault := testVault(t)
	scope := Scope{Kind: "tenant", TenantID: 7, Provider: "telegram",
		Purpose: "bot_api", Generation: 3}
	first, err := vault.Seal(scope, []byte("synthetic-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := vault.Seal(scope, []byte("synthetic-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Nonce, second.Nonce) || bytes.Equal(first.Ciphertext, second.Ciphertext) {
		t.Fatal("each seal must use a fresh nonce")
	}
	got, err := vault.Open(scope, first)
	if err != nil || string(got) != "synthetic-test-secret" {
		t.Fatalf("Open()=(%q,%v)", got, err)
	}
	if strings.Contains(string(first.Ciphertext), "synthetic-test-secret") {
		t.Fatal("ciphertext leaked plaintext")
	}
}

func TestOpenFailsClosedAcrossAuthorityBoundaries(t *testing.T) {
	vault := testVault(t)
	scope := Scope{Kind: "platform", Provider: "llm", Purpose: "primary_api", Generation: 1}
	envelope, err := vault.Seal(scope, []byte("synthetic-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []Scope{
		{Kind: "platform", Provider: "llm", Purpose: "primary_api", Generation: 2},
		{Kind: "platform", Provider: "llm", Purpose: "agent_api", Generation: 1},
		{Kind: "tenant", TenantID: 1, Provider: "llm", Purpose: "primary_api", Generation: 1},
	}
	for _, altered := range tests {
		if _, err := vault.Open(altered, envelope); err == nil {
			t.Fatalf("altered scope unexpectedly decrypted: %+v", altered)
		}
	}
	tampered := envelope
	tampered.Ciphertext = append([]byte(nil), envelope.Ciphertext...)
	tampered.Ciphertext[0] ^= 0xff
	if _, err := vault.Open(scope, tampered); err == nil {
		t.Fatal("tampered ciphertext unexpectedly decrypted")
	}
}

func TestUserCredentialAADBindsTenantAndUser(t *testing.T) {
	vault := testVault(t)
	scope := Scope{Kind: "user", TenantID: 7, UserID: 70,
		Provider: "telegram", Purpose: "bot_api", Generation: 1}
	envelope, err := vault.Seal(scope, []byte("synthetic-user-secret"))
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []Scope{
		{Kind: "user", TenantID: 7, UserID: 71, Provider: "telegram", Purpose: "bot_api", Generation: 1},
		{Kind: "user", TenantID: 8, UserID: 70, Provider: "telegram", Purpose: "bot_api", Generation: 1},
		{Kind: "tenant", TenantID: 7, Provider: "telegram", Purpose: "bot_api", Generation: 1},
	} {
		if _, err := vault.Open(altered, envelope); err == nil {
			t.Fatalf("altered user authority decrypted: %+v", altered)
		}
	}
}

func TestRetiredKeyDecryptsButNewWritesUseActiveKey(t *testing.T) {
	oldVault, err := New(Config{ActiveKeyID: "key-0", ActiveKey: bytes.Repeat([]byte{0x24}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{Kind: "tenant", TenantID: 9, Provider: "feishu",
		Purpose: "app_credentials", Generation: 1}
	oldEnvelope, err := oldVault.Seal(scope, []byte("old-synthetic-secret"))
	if err != nil {
		t.Fatal(err)
	}
	rotated := testVault(t)
	if _, err := rotated.Open(scope, oldEnvelope); err != nil {
		t.Fatalf("retired key could not decrypt retained generation: %v", err)
	}
	newEnvelope, err := rotated.Seal(scope, []byte("new-synthetic-secret"))
	if err != nil || newEnvelope.KeyID != "key-1" {
		t.Fatalf("new envelope key=%q err=%v", newEnvelope.KeyID, err)
	}
}

func TestParseKeyringRejectsMalformedAndDuplicateKeys(t *testing.T) {
	active := strings.Repeat("42", 32)
	if _, err := ParseKeyring("key-1", active, "key-0="+strings.Repeat("24", 32)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ active, retired string }{
		{"short", ""},
		{strings.Repeat("00", 32), ""},
		{strings.Repeat("AB", 32), ""},
		{active, "missing-equals"},
		{active, "key-0=" + strings.Repeat("24", 32) + ",key-0=" + strings.Repeat("25", 32)},
	} {
		if _, err := ParseKeyring("key-1", test.active, test.retired); err == nil {
			t.Fatalf("ParseKeyring(%q,%q) unexpectedly succeeded", test.active, test.retired)
		}
	}
}
