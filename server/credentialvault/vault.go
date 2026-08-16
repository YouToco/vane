// Package credentialvault encrypts secret material before persistence. The
// ciphertext is authenticated against its exact workspace, user, purpose, and
// key version so copying a row across scopes cannot grant authority.
package credentialvault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const SchemaV1 = "vane.encrypted-credential/v1"

type Scope struct {
	TenantID int64
	UserID   int64
	Kind     string
}

type Envelope struct {
	Schema     string `json:"schema"`
	KeyVersion string `json:"key_version"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

type Vault struct {
	primary string
	keys    map[string][]byte
	random  io.Reader
}

// New copies all keys. Each key must be a 32-byte AES-256 key. Callers should
// obtain these values from a process credential, never from application JSON.
func New(primary string, keys map[string][]byte) (*Vault, error) {
	primary = strings.TrimSpace(primary)
	if primary == "" || len(keys) == 0 {
		return nil, errors.New("credentialvault: primary key version is required")
	}
	copied := make(map[string][]byte, len(keys))
	for version, key := range keys {
		if strings.TrimSpace(version) != version || version == "" || strings.ContainsAny(version, "\x00\r\n:") {
			return nil, errors.New("credentialvault: invalid key version")
		}
		if len(key) != 32 {
			return nil, fmt.Errorf("credentialvault: key %q must be 32 bytes", version)
		}
		copied[version] = append([]byte(nil), key...)
	}
	if _, ok := copied[primary]; !ok {
		return nil, errors.New("credentialvault: primary key is missing")
	}
	return &Vault{primary: primary, keys: copied, random: rand.Reader}, nil
}

func (v *Vault) Seal(scope Scope, plaintext []byte) (Envelope, error) {
	if err := validateScope(scope); err != nil {
		return Envelope{}, err
	}
	if v == nil || len(v.keys[v.primary]) != 32 {
		return Envelope{}, errors.New("credentialvault: vault is not initialized")
	}
	if len(plaintext) == 0 || len(plaintext) > 1<<20 {
		return Envelope{}, errors.New("credentialvault: plaintext size is outside limits")
	}
	aead, err := newAEAD(v.keys[v.primary])
	if err != nil {
		return Envelope{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(v.random, nonce); err != nil {
		return Envelope{}, fmt.Errorf("credentialvault: generate nonce: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, additionalData(scope, v.primary))
	return Envelope{
		Schema: SchemaV1, KeyVersion: v.primary,
		Nonce:      base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawStdEncoding.EncodeToString(ciphertext),
	}, nil
}

func (v *Vault) Open(scope Scope, envelope Envelope) ([]byte, error) {
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	if v == nil || envelope.Schema != SchemaV1 || envelope.KeyVersion == "" {
		return nil, errors.New("credentialvault: unsupported envelope")
	}
	key, ok := v.keys[envelope.KeyVersion]
	if !ok {
		return nil, errors.New("credentialvault: key version is unavailable")
	}
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce, err := base64.RawStdEncoding.Strict().DecodeString(envelope.Nonce)
	if err != nil || len(nonce) != aead.NonceSize() {
		return nil, errors.New("credentialvault: invalid nonce")
	}
	ciphertext, err := base64.RawStdEncoding.Strict().DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) < aead.Overhead() {
		return nil, errors.New("credentialvault: invalid ciphertext")
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, additionalData(scope, envelope.KeyVersion))
	if err != nil {
		return nil, errors.New("credentialvault: authentication failed")
	}
	return plaintext, nil
}

// NeedsRotation reports whether a valid envelope was written with a retained
// non-primary key. Rotation is an explicit Store operation; Open never mutates.
func (v *Vault) NeedsRotation(envelope Envelope) bool {
	return v != nil && envelope.Schema == SchemaV1 && envelope.KeyVersion != "" && envelope.KeyVersion != v.primary
}

func (e Envelope) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func Parse(raw []byte) (Envelope, error) {
	var envelope Envelope
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("credentialvault: decode envelope: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Envelope{}, errors.New("credentialvault: trailing envelope data")
	}
	if envelope.Schema != SchemaV1 || envelope.KeyVersion == "" || envelope.Nonce == "" || envelope.Ciphertext == "" {
		return Envelope{}, errors.New("credentialvault: incomplete envelope")
	}
	return envelope, nil
}

func validateScope(scope Scope) error {
	if scope.TenantID <= 0 || scope.UserID < 0 || strings.TrimSpace(scope.Kind) != scope.Kind ||
		scope.Kind == "" || len(scope.Kind) > 128 || strings.ContainsAny(scope.Kind, "\x00\r\n:") {
		return errors.New("credentialvault: invalid credential scope")
	}
	return nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("credentialvault: initialize cipher: %w", err)
	}
	return cipher.NewGCM(block)
}

func additionalData(scope Scope, keyVersion string) []byte {
	return []byte(SchemaV1 + "\x00" + keyVersion + "\x00" +
		strconv.FormatInt(scope.TenantID, 10) + "\x00" +
		strconv.FormatInt(scope.UserID, 10) + "\x00" + scope.Kind)
}
