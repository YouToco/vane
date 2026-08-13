package agentfirstaudit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/YouToco/vane/internal/strictjson"
)

const maxReleaseReceiptBytes = 16 << 10

type ReleaseReceipt struct {
	SchemaVersion               string `json:"schema_version"`
	SourceRevision              string `json:"source_revision"`
	ControlPlaneRevision        string `json:"control_plane_revision"`
	DeployRunID                 string `json:"deploy_run_id"`
	DeployRunAttempt            int64  `json:"deploy_run_attempt"`
	BackendArchiveDigest        string `json:"backend_archive_sha256"`
	BackendManifestDigest       string `json:"backend_manifest_sha256"`
	ServerReleaseContractDigest string `json:"server_release_contract_sha256"`
	VaneDigest                  string `json:"vane_sha256"`
	CollectorDigest             string `json:"agentfirstretention_sha256"`
}

type VerifiedReleaseReceipt struct {
	receipt      ReleaseReceipt
	canonical    []byte
	deployDigest string
}

func (verified VerifiedReleaseReceipt) SourceRevision() string {
	return verified.receipt.SourceRevision
}

func (verified VerifiedReleaseReceipt) DeployDigest() string {
	return verified.deployDigest
}

func ReadVerifiedReleaseReceipt(
	receiptPath string,
	collectorPath string,
	liveVanePath string,
) (VerifiedReleaseReceipt, error) {
	if !canonicalAbsolutePath(receiptPath) || !canonicalAbsolutePath(collectorPath) ||
		!canonicalAbsolutePath(liveVanePath) {
		return VerifiedReleaseReceipt{}, fmt.Errorf("release receipt paths are invalid")
	}
	payload, err := readRootAuthorityFile(receiptPath, maxReleaseReceiptBytes)
	if err != nil {
		return VerifiedReleaseReceipt{}, fmt.Errorf("read release receipt: %w", err)
	}
	var receipt ReleaseReceipt
	if err := strictjson.DecodeExact(payload, &receipt); err != nil {
		return VerifiedReleaseReceipt{}, fmt.Errorf("decode release receipt: %w", err)
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(canonical, payload) ||
		receipt.SchemaVersion != "vane.release-receipt/v1" ||
		!validSourceRevision(receipt.SourceRevision) ||
		!validSourceRevision(receipt.ControlPlaneRevision) ||
		!canonicalUnsignedDecimal(receipt.DeployRunID) || receipt.DeployRunAttempt <= 0 {
		return VerifiedReleaseReceipt{}, fmt.Errorf("release receipt is not canonical")
	}
	for _, digest := range []string{
		receipt.BackendArchiveDigest, receipt.BackendManifestDigest,
		receipt.ServerReleaseContractDigest, receipt.VaneDigest, receipt.CollectorDigest,
	} {
		if !validLowerHex(digest, sha256.Size) {
			return VerifiedReleaseReceipt{}, fmt.Errorf("release receipt digest is invalid")
		}
	}
	collectorDigest, err := digestAuthorityFile(collectorPath)
	if err != nil {
		return VerifiedReleaseReceipt{}, fmt.Errorf("digest collector binary: %w", err)
	}
	vaneDigest, err := digestAuthorityFile(liveVanePath)
	if err != nil {
		return VerifiedReleaseReceipt{}, fmt.Errorf("digest live vane binary: %w", err)
	}
	if collectorDigest != receipt.CollectorDigest || vaneDigest != receipt.VaneDigest {
		return VerifiedReleaseReceipt{}, fmt.Errorf("release receipt binary binding differs")
	}
	sum := sha256.Sum256(payload)
	return VerifiedReleaseReceipt{
		receipt: receipt, canonical: bytes.Clone(payload),
		deployDigest: hex.EncodeToString(sum[:]),
	}, nil
}

func (verified VerifiedReleaseReceipt) validate() error {
	if len(verified.canonical) == 0 || verified.receipt.SourceRevision == "" {
		return fmt.Errorf("verified release receipt is absent")
	}
	canonical, err := json.Marshal(verified.receipt)
	if err != nil || !bytes.Equal(canonical, verified.canonical) {
		return fmt.Errorf("verified release receipt canonical bytes differ")
	}
	sum := sha256.Sum256(canonical)
	if hex.EncodeToString(sum[:]) != verified.deployDigest {
		return fmt.Errorf("verified release receipt digest differs")
	}
	return nil
}

func canonicalUnsignedDecimal(value string) bool {
	if value == "" || len(value) > 20 || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, current := range value {
		if current < '0' || current > '9' {
			return false
		}
	}
	return true
}

func canonicalAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path
}

func readRootAuthorityFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o022 != 0 || !trustedAuthorityOwner(stat.Uid) ||
		info.Size() <= 0 || info.Size() > maximum {
		return nil, fmt.Errorf("file authority is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || int64(len(payload)) > maximum {
		return nil, fmt.Errorf("file exceeds authority bounds")
	}
	return payload, nil
}

func digestAuthorityFile(path string) (string, error) {
	const maxBinaryBytes = 512 << 20
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o022 != 0 || !trustedAuthorityOwner(stat.Uid) ||
		info.Size() <= 0 || info.Size() > maxBinaryBytes {
		return "", fmt.Errorf("binary authority is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	read, err := io.Copy(hash, io.LimitReader(file, maxBinaryBytes+1))
	if err != nil || read != info.Size() {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
