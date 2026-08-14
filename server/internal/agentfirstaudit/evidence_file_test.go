package agentfirstaudit

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistCanonicalEvidenceIsContentAddressedAndRecoverable(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "evidence")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"schema_version":"test/v1"}`)
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	path, err := PersistCanonicalEvidence(directory, digest, payload)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(directory, digest+".json") {
		t.Fatalf("path=%q", path)
	}
	second, err := PersistCanonicalEvidence(directory, digest, payload)
	if err != nil || second != path {
		t.Fatalf("second=%q err=%v", second, err)
	}
	loaded, err := ReadCanonicalEvidence(directory, digest)
	if err != nil || string(loaded) != string(payload) {
		t.Fatalf("loaded=%q err=%v", loaded, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestCanonicalEvidenceRejectsTamperAndUnsafeDirectory(t *testing.T) {
	base := t.TempDir()
	directory := filepath.Join(base, "evidence")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"schema_version":"test/v1"}`)
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	path, err := PersistCanonicalEvidence(directory, digest, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"tampered":true}`), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCanonicalEvidence(directory, digest); err == nil {
		t.Fatal("tampered evidence accepted")
	}
	if err := os.Chmod(directory, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCanonicalEvidence(directory, digest); err == nil {
		t.Fatal("group/world-writable evidence directory accepted")
	}
	symlink := filepath.Join(base, "linked")
	if err := os.Symlink(directory, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCanonicalEvidence(symlink, digest); err == nil {
		t.Fatal("symlink evidence directory accepted")
	}
}
