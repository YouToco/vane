package agentfirstaudit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadVerifiedReleaseReceiptBindsCanonicalReceiptAndBinaries(t *testing.T) {
	directory := t.TempDir()
	collector := filepath.Join(directory, "agentfirstretention")
	vane := filepath.Join(directory, "vane")
	if err := os.WriteFile(collector, []byte("collector"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(vane, []byte("vane"), 0o700); err != nil {
		t.Fatal(err)
	}
	receipt := ReleaseReceipt{
		SchemaVersion: "vane.release-receipt/v1", SourceRevision: strings.Repeat("a", 40),
		ControlPlaneRevision: strings.Repeat("f", 40), DeployRunID: "123456",
		BuildRunAttempt:             2,
		BackendArchiveDigest:        strings.Repeat("b", 64),
		BackendManifestDigest:       strings.Repeat("c", 64),
		ServerReleaseContractDigest: strings.Repeat("d", 64),
		VaneDigest:                  digestBytes([]byte("vane")), CollectorDigest: digestBytes([]byte("collector")),
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(directory, "release-receipt.json")
	if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	verified, err := ReadVerifiedReleaseReceipt(receiptPath, collector, vane)
	if err != nil {
		t.Fatal(err)
	}
	if verified.receipt != receipt || !validLowerHex(verified.deployDigest, 32) {
		t.Fatalf("verified=%+v", verified)
	}

	if err := os.WriteFile(vane, []byte("changed"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadVerifiedReleaseReceipt(receiptPath, collector, vane); err == nil {
		t.Fatal("changed live binary accepted")
	}
	if err := os.WriteFile(receiptPath, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadVerifiedReleaseReceipt(receiptPath, collector, vane); err == nil {
		t.Fatal("noncanonical release receipt accepted")
	}
}

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
