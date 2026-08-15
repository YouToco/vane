// Package sandbox contains the dark, out-of-process execution foundation.
// It is deliberately not wired into the Vane server or Skill runtime.
package sandbox

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const ProtocolVersion = "vane.sandbox/v1"

var safeID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

// Request is the complete authority envelope accepted by sandboxd. Artifact
// paths are intentionally absent: callers can select only a pre-approved,
// content-addressed capability version and policy.
type Request struct {
	ProtocolVersion   string `json:"protocol_version"`
	TenantID          int64  `json:"tenant_id"`
	UserID            int64  `json:"user_id"`
	CapabilityID      string `json:"capability_id"`
	CapabilityVersion string `json:"capability_version_sha256"`
	InvocationID      string `json:"invocation_id"`
	Policy            Policy `json:"policy"`
	PolicyDigest      string `json:"policy_sha256"`
	RequestDigest     string `json:"request_sha256"`
	Input             []byte `json:"input"`
}

// Policy is closed and network-free. Adding any network field requires a new
// protocol version and an independent security review.
type Policy struct {
	VCPUCount       int   `json:"vcpu_count"`
	MemoryMiB       int   `json:"memory_mib"`
	PIDsMax         int   `json:"pids_max"`
	CPUQuotaMicros  int64 `json:"cpu_quota_micros"`
	CPUPeriodMicros int64 `json:"cpu_period_micros"`
	WallTimeoutMS   int64 `json:"wall_timeout_ms"`
	OutputBytesMax  int64 `json:"output_bytes_max"`
	TmpfsBytesMax   int64 `json:"tmpfs_bytes_max"`
	GuestUID        int   `json:"guest_uid"`
	GuestGID        int   `json:"guest_gid"`
	NetworkDisabled bool  `json:"network_disabled"`
	RootReadOnly    bool  `json:"root_read_only"`
	CodeReadOnly    bool  `json:"code_read_only"`
}

type Result struct {
	InvocationID string        `json:"invocation_id"`
	Status       string        `json:"status"`
	Output       []byte        `json:"output,omitempty"`
	Duration     time.Duration `json:"duration_ns"`
	ErrorCode    string        `json:"error_code,omitempty"`
}

func (p Policy) Validate() error {
	if p.VCPUCount < 1 || p.VCPUCount > 8 || p.MemoryMiB < 64 || p.MemoryMiB > 8192 ||
		p.PIDsMax < 1 || p.PIDsMax > 4096 || p.CPUQuotaMicros < 1 ||
		p.CPUPeriodMicros < 1000 || p.CPUQuotaMicros > p.CPUPeriodMicros*int64(p.VCPUCount) ||
		p.WallTimeoutMS < 10 || p.WallTimeoutMS > int64((10*time.Minute)/time.Millisecond) ||
		p.OutputBytesMax < 1 || p.OutputBytesMax > 16<<20 ||
		p.TmpfsBytesMax < 1<<20 || p.TmpfsBytesMax > 1<<30 {
		return errors.New("sandbox policy resource limits are outside the closed envelope")
	}
	if p.GuestUID <= 0 || p.GuestGID <= 0 {
		return errors.New("sandbox guest must be non-root")
	}
	if !p.NetworkDisabled || !p.RootReadOnly || !p.CodeReadOnly {
		return errors.New("sandbox v1 requires no network and read-only root/code")
	}
	return nil
}

func (p Policy) Digest() (string, error) { return digestJSON(p) }

func (r Request) Validate(maxInput int) error {
	if r.ProtocolVersion != ProtocolVersion || r.TenantID <= 0 || r.UserID <= 0 ||
		!safeID.MatchString(r.CapabilityID) || !safeID.MatchString(r.InvocationID) {
		return errors.New("sandbox request identity is invalid")
	}
	if maxInput < 1 || len(r.Input) > maxInput {
		return errors.New("sandbox request input exceeds limit")
	}
	if err := requireSHA256("capability version", r.CapabilityVersion); err != nil {
		return err
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	policyDigest, err := r.Policy.Digest()
	if err != nil || !constantDigestEqual(policyDigest, r.PolicyDigest) {
		return errors.New("sandbox policy digest mismatch")
	}
	want, err := r.digest(false)
	if err != nil || !constantDigestEqual(want, r.RequestDigest) {
		return errors.New("sandbox request digest mismatch")
	}
	return nil
}

func (r Request) Seal() (Request, error) {
	policyDigest, err := r.Policy.Digest()
	if err != nil {
		return r, err
	}
	r.PolicyDigest = policyDigest
	digest, err := r.digest(false)
	if err != nil {
		return r, err
	}
	r.RequestDigest = digest
	return r, nil
}

func (r Request) digest(includeDigest bool) (string, error) {
	if !includeDigest {
		r.RequestDigest = ""
	}
	return digestJSON(r)
}

func digestJSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode digest input: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func requireSHA256(label, value string) error {
	if len(value) != sha256.Size*2 {
		return fmt.Errorf("%s must be a sha256 digest", label)
	}
	if _, err := hex.DecodeString(value); err != nil || strings.ToLower(value) != value {
		return fmt.Errorf("%s must be lowercase hexadecimal", label)
	}
	return nil
}

func constantDigestEqual(left, right string) bool {
	if err := requireSHA256("digest", left); err != nil {
		return false
	}
	if err := requireSHA256("digest", right); err != nil {
		return false
	}
	var diff byte
	for i := range left {
		diff |= left[i] ^ right[i]
	}
	return diff == 0
}
