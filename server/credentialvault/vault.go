// Package credentialvault encrypts provider credentials before they cross the
// database boundary. Database rows retain only authenticated ciphertext and
// non-secret routing metadata; the keyring remains process/deployment state.
package credentialvault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

const envelopeVersion = "v1"

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// Scope is the immutable authority bound into AES-GCM additional data.
// TenantID/UserID identify an ordinary user's channel credential. UserID is
// omitted from legacy platform/tenant AAD so existing v1 envelopes remain
// decryptable byte-for-byte.
type Scope struct {
	Kind       string
	TenantID   int64
	UserID     int64
	Provider   string
	Purpose    string
	Generation int64
}

// Envelope is safe to persist. Ciphertext includes the GCM authentication tag.
type Envelope struct {
	Version     string
	KeyID       string
	Nonce       []byte
	Ciphertext  []byte
	Fingerprint string
}

// Config carries one active encryption key and optional decrypt-only retired
// keys. All keys are exactly 32 bytes (AES-256).
type Config struct {
	ActiveKeyID string
	ActiveKey   []byte
	RetiredKeys map[string][]byte
}

// Vault is immutable after construction and safe for concurrent use.
type Vault struct {
	activeKeyID string
	keys        map[string][32]byte
	random      io.Reader
}

func New(config Config) (*Vault, error) {
	activeID := strings.TrimSpace(config.ActiveKeyID)
	if !identifierPattern.MatchString(activeID) {
		return nil, errors.New("credential vault: active key id is invalid")
	}
	if len(config.ActiveKey) != 32 ||
		subtle.ConstantTimeCompare(config.ActiveKey, make([]byte, 32)) == 1 {
		return nil, errors.New("credential vault: active key must be 32 bytes")
	}
	keys := make(map[string][32]byte, len(config.RetiredKeys)+1)
	var active [32]byte
	copy(active[:], config.ActiveKey)
	keys[activeID] = active
	for rawID, rawKey := range config.RetiredKeys {
		keyID := strings.TrimSpace(rawID)
		if !identifierPattern.MatchString(keyID) || keyID == activeID {
			return nil, errors.New("credential vault: retired key id is invalid or duplicated")
		}
		if len(rawKey) != 32 ||
			subtle.ConstantTimeCompare(rawKey, make([]byte, 32)) == 1 {
			return nil, errors.New("credential vault: retired key must be 32 bytes")
		}
		var key [32]byte
		copy(key[:], rawKey)
		keys[keyID] = key
	}
	return &Vault{activeKeyID: activeID, keys: keys, random: rand.Reader}, nil
}

func (v *Vault) Seal(scope Scope, plaintext []byte) (Envelope, error) {
	if v == nil {
		return Envelope{}, errors.New("credential vault: unavailable")
	}
	if err := validateScope(scope); err != nil {
		return Envelope{}, err
	}
	if len(plaintext) == 0 || len(plaintext) > 64<<10 {
		return Envelope{}, errors.New("credential vault: plaintext length is invalid")
	}
	key := v.keys[v.activeKeyID]
	gcm, err := newGCM(key)
	if err != nil {
		return Envelope{}, errors.New("credential vault: initialize cipher")
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(v.random, nonce); err != nil {
		return Envelope{}, errors.New("credential vault: generate nonce")
	}
	aad := additionalData(scope, v.activeKeyID)
	ciphertext := gcm.Seal(nil, nonce, plaintext, aad)
	return Envelope{
		Version: envelopeVersion, KeyID: v.activeKeyID,
		Nonce: nonce, Ciphertext: ciphertext,
		Fingerprint: fingerprint(key, scope, plaintext),
	}, nil
}

func (v *Vault) Open(scope Scope, envelope Envelope) ([]byte, error) {
	if v == nil {
		return nil, errors.New("credential vault: unavailable")
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	if envelope.Version != envelopeVersion ||
		!identifierPattern.MatchString(envelope.KeyID) {
		return nil, errors.New("credential vault: invalid envelope")
	}
	key, ok := v.keys[envelope.KeyID]
	if !ok {
		return nil, errors.New("credential vault: encryption key is unavailable")
	}
	gcm, err := newGCM(key)
	if err != nil || len(envelope.Nonce) != gcm.NonceSize() ||
		len(envelope.Ciphertext) < gcm.Overhead() {
		return nil, errors.New("credential vault: invalid envelope")
	}
	plaintext, err := gcm.Open(nil, envelope.Nonce, envelope.Ciphertext,
		additionalData(scope, envelope.KeyID))
	if err != nil {
		return nil, errors.New("credential vault: authentication failed")
	}
	if !hmac.Equal([]byte(envelope.Fingerprint),
		[]byte(fingerprint(key, scope, plaintext))) {
		return nil, errors.New("credential vault: fingerprint mismatch")
	}
	return plaintext, nil
}

func newGCM(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func validateScope(scope Scope) error {
	if scope.Kind != "platform" && scope.Kind != "tenant" && scope.Kind != "user" {
		return errors.New("credential vault: scope kind is invalid")
	}
	if (scope.Kind == "tenant" && (scope.TenantID <= 0 || scope.UserID != 0)) ||
		(scope.Kind == "platform" && (scope.TenantID != 0 || scope.UserID != 0)) ||
		(scope.Kind == "user" && (scope.TenantID <= 0 || scope.UserID <= 0)) {
		return errors.New("credential vault: tenant scope is invalid")
	}
	if !identifierPattern.MatchString(scope.Provider) ||
		!identifierPattern.MatchString(scope.Purpose) || scope.Generation <= 0 {
		return errors.New("credential vault: scope identity is invalid")
	}
	return nil
}

func additionalData(scope Scope, keyID string) []byte {
	parts := []string{
		"vane-credential-vault", envelopeVersion, scope.Kind,
		strconv.FormatInt(scope.TenantID, 10),
	}
	if scope.UserID > 0 {
		parts = append(parts, strconv.FormatInt(scope.UserID, 10))
	}
	parts = append(parts, scope.Provider, scope.Purpose,
		strconv.FormatInt(scope.Generation, 10), keyID,
	)
	return []byte(strings.Join(parts, "\x00"))
}

func fingerprint(key [32]byte, scope Scope, plaintext []byte) string {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("vane-credential-fingerprint\x00"))
	_, _ = mac.Write(additionalData(scope, "fingerprint"))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(plaintext)
	return hex.EncodeToString(mac.Sum(nil))
}

// ParseKeyring parses deployment configuration without ever returning a
// printable representation of key bytes. Retired is comma-separated id=hex.
func ParseKeyring(activeID, activeHex, retired string) (Config, error) {
	decode := func(value string) ([]byte, error) {
		trimmed := strings.TrimSpace(value)
		if trimmed != value || strings.ToLower(trimmed) != trimmed {
			return nil, errors.New("key must be lowercase hexadecimal without whitespace")
		}
		decoded, err := hex.DecodeString(trimmed)
		if err != nil || len(decoded) != 32 ||
			subtle.ConstantTimeCompare(decoded, make([]byte, 32)) == 1 {
			return nil, errors.New("key must be 64 hexadecimal characters")
		}
		return decoded, nil
	}
	active, err := decode(activeHex)
	if err != nil {
		return Config{}, fmt.Errorf("credential vault: active key: %w", err)
	}
	config := Config{ActiveKeyID: strings.TrimSpace(activeID), ActiveKey: active,
		RetiredKeys: map[string][]byte{}}
	if strings.TrimSpace(retired) == "" {
		return config, nil
	}
	for _, entry := range strings.Split(retired, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return Config{}, errors.New("credential vault: retired key entry is invalid")
		}
		key, err := decode(parts[1])
		if err != nil {
			return Config{}, fmt.Errorf("credential vault: retired key: %w", err)
		}
		keyID := strings.TrimSpace(parts[0])
		if _, exists := config.RetiredKeys[keyID]; exists {
			return Config{}, errors.New("credential vault: retired key id is duplicated")
		}
		config.RetiredKeys[keyID] = key
	}
	return config, nil
}
