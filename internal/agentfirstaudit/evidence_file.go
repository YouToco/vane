package agentfirstaudit

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const maxRetentionEvidenceBytes = 64 << 20

// PersistCanonicalEvidence writes an immutable content-addressed manifest.
// The caller must still append the matching PostgreSQL attestation: this file
// preserves full audit evidence but does not grant runtime authority itself.
func PersistCanonicalEvidence(directory, digest string, payload []byte) (string, error) {
	root, dir, err := openSecureEvidenceRoot(directory)
	if err != nil {
		return "", err
	}
	defer root.Close()
	defer dir.Close()
	if err := validateEvidencePayload(digest, payload); err != nil {
		return "", err
	}
	name := digest + ".json"
	if existing, readErr := readEvidenceFromRoot(root, name, digest); readErr == nil {
		if !bytes.Equal(existing, payload) {
			return "", fmt.Errorf("retention evidence digest collision")
		}
		return filepath.Join(filepath.Clean(directory), name), nil
	} else if !os.IsNotExist(readErr) {
		return "", readErr
	}

	var nonce [16]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return "", fmt.Errorf("create retention evidence nonce: %w", err)
	}
	temporary := "." + digest + "-" + hex.EncodeToString(nonce[:]) + ".tmp"
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return "", fmt.Errorf("create retention evidence: %w", err)
	}
	keepTemporary := true
	defer func() {
		_ = file.Close()
		if keepTemporary {
			_ = root.Remove(temporary)
		}
	}()
	if written, err := file.Write(payload); err != nil || written != len(payload) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return "", fmt.Errorf("write retention evidence: %w", err)
	}
	if err := file.Chmod(0o640); err != nil {
		return "", fmt.Errorf("set retention evidence mode: %w", err)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("sync retention evidence: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close retention evidence: %w", err)
	}
	if err := root.Rename(temporary, name); err != nil {
		return "", fmt.Errorf("publish retention evidence: %w", err)
	}
	keepTemporary = false
	if err := dir.Sync(); err != nil {
		return "", fmt.Errorf("sync retention evidence directory: %w", err)
	}
	written, err := readEvidenceFromRoot(root, name, digest)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(written, payload) {
		return "", fmt.Errorf("published retention evidence differs")
	}
	return filepath.Join(filepath.Clean(directory), name), nil
}

func ReadCanonicalEvidence(directory, digest string) ([]byte, error) {
	root, dir, err := openSecureEvidenceRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	defer dir.Close()
	return readEvidenceFromRoot(root, digest+".json", digest)
}

func openSecureEvidenceRoot(directory string) (*os.Root, *os.File, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, nil, fmt.Errorf("retention evidence directory is not canonical absolute path")
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect retention evidence directory: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 ||
		!trustedAuthorityOwner(stat.Uid) {
		return nil, nil, fmt.Errorf("retention evidence directory authority is unsafe")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, nil, fmt.Errorf("open retention evidence root: %w", err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		root.Close()
		return nil, nil, fmt.Errorf("open retention evidence directory: %w", err)
	}
	return root, dir, nil
}

func trustedAuthorityOwner(uid uint32) bool {
	return uid == 0 || int(uid) == os.Geteuid()
}

func readEvidenceFromRoot(root *os.Root, name, digest string) ([]byte, error) {
	if !validLowerHex(digest, sha256.Size) || name != digest+".json" {
		return nil, fmt.Errorf("retention evidence digest is invalid")
	}
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o640 || info.Size() <= 0 ||
		info.Size() > maxRetentionEvidenceBytes {
		return nil, fmt.Errorf("retention evidence file authority is invalid")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open retention evidence: %w", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxRetentionEvidenceBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read retention evidence: %w", err)
	}
	if err := validateEvidencePayload(digest, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func validateEvidencePayload(digest string, payload []byte) error {
	if !validLowerHex(digest, sha256.Size) || len(payload) == 0 ||
		len(payload) > maxRetentionEvidenceBytes {
		return fmt.Errorf("retention evidence payload is invalid")
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("retention evidence payload digest differs")
	}
	return nil
}
