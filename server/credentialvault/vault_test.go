package credentialvault

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

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
		{active, "key-0=" + strings.Repeat("gg", 32)},
		{active, "key-0=" + strings.Repeat("24", 32) + ",key-0=" + strings.Repeat("25", 32)},
	} {
		if _, err := ParseKeyring("key-1", test.active, test.retired); err == nil {
			t.Fatalf("ParseKeyring(%q,%q) unexpectedly succeeded", test.active, test.retired)
		}
	}
}

func TestVaultRejectsInvalidConstructionScopesAndEnvelopes(t *testing.T) {
	validKey := bytes.Repeat([]byte{0x42}, 32)
	for _, config := range []Config{
		{ActiveKeyID: "Bad", ActiveKey: validKey},
		{ActiveKeyID: "key", ActiveKey: make([]byte, 32)},
		{ActiveKeyID: "key", ActiveKey: validKey,
			RetiredKeys: map[string][]byte{"key": bytes.Repeat([]byte{1}, 32)}},
		{ActiveKeyID: "key", ActiveKey: validKey,
			RetiredKeys: map[string][]byte{"old": []byte("short")}},
	} {
		if _, err := New(config); err == nil {
			t.Fatalf("unsafe config accepted: %+v", config)
		}
	}
	validScope := Scope{Kind: "platform", Provider: "llm", Purpose: "shared_runtime", Generation: 1}
	var nilVault *Vault
	if _, err := nilVault.Seal(validScope, []byte("secret")); err == nil {
		t.Fatal("nil vault sealed")
	}
	if _, err := nilVault.Open(validScope, Envelope{}); err == nil {
		t.Fatal("nil vault opened")
	}
	vault := testVault(t)
	for _, scope := range []Scope{
		{Kind: "other", Provider: "llm", Purpose: "shared_runtime", Generation: 1},
		{Kind: "tenant", Provider: "llm", Purpose: "shared_runtime", Generation: 1},
		{Kind: "platform", Provider: "Bad", Purpose: "shared_runtime", Generation: 1},
	} {
		if _, err := vault.Seal(scope, []byte("secret")); err == nil {
			t.Fatalf("invalid scope sealed: %+v", scope)
		}
	}
	if _, err := vault.Seal(validScope, nil); err == nil {
		t.Fatal("empty plaintext sealed")
	}
	vault.random = failingReader{}
	if _, err := vault.Seal(validScope, []byte("secret")); err == nil {
		t.Fatal("nonce failure ignored")
	}
	vault = testVault(t)
	envelope, err := vault.Seal(validScope, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Envelope){
		func(value *Envelope) { value.Version = "v2" },
		func(value *Envelope) { value.KeyID = "missing" },
		func(value *Envelope) { value.Nonce = nil },
		func(value *Envelope) { value.Fingerprint = strings.Repeat("0", 64) },
	} {
		candidate := envelope
		candidate.Nonce = append([]byte(nil), envelope.Nonce...)
		candidate.Ciphertext = append([]byte(nil), envelope.Ciphertext...)
		mutate(&candidate)
		if _, err := vault.Open(validScope, candidate); err == nil {
			t.Fatalf("invalid envelope opened: %+v", candidate)
		}
	}
}
