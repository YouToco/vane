//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/YouToco/vane/server/sandbox"
	"golang.org/x/sys/unix"
)

var exactRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)

func gateUnitName(revision string) string { return "vane-firecracker-gate-" + revision + ".service" }

func gateRuntimeRoot(revision string) string { return "/run/vane-firecracker-gate-" + revision }

type releaseReceipt struct {
	SchemaVersion               string `json:"schema_version"`
	SourceRevision              string `json:"source_revision"`
	ControlPlaneRevision        string `json:"control_plane_revision"`
	DeployRunID                 string `json:"deploy_run_id"`
	BuildRunAttempt             int    `json:"build_run_attempt"`
	BackendArchiveSHA256        string `json:"backend_archive_sha256"`
	BackendManifestSHA256       string `json:"backend_manifest_sha256"`
	ServerReleaseContractSHA256 string `json:"server_release_contract_sha256"`
	VaneSHA256                  string `json:"vane_sha256"`
	RetentionSHA256             string `json:"agentfirstretention_sha256"`
}

type fileManifestEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Mode   int    `json:"mode"`
}

type backendManifest struct {
	Schema                int                 `json:"schema"`
	Component             string              `json:"component"`
	SourceSHA             string              `json:"source_sha"`
	Archive               string              `json:"archive"`
	ArchiveSHA256         string              `json:"archive_sha256"`
	ArchiveSize           int64               `json:"archive_size"`
	Files                 []fileManifestEntry `json:"files"`
	ServerReleaseContract string              `json:"server_release_contract"`
	ControlPlaneRevision  string              `json:"control_plane_revision"`
	DeployRunID           string              `json:"deploy_run_id"`
	BuildRunAttempt       int                 `json:"build_run_attempt"`
}

type sandboxArtifact struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size_bytes"`
}

type sandboxManifest struct {
	Schema             string                     `json:"schema"`
	SourceRevision     string                     `json:"source_revision"`
	Architecture       string                     `json:"architecture"`
	FirecrackerVersion string                     `json:"firecracker_version"`
	SandboxdSHA256     string                     `json:"sandboxd_sha256"`
	Artifacts          map[string]sandboxArtifact `json:"artifacts"`
}

type releaseGateBackend interface {
	Preflight(context.Context) error
	RunGateSelfTest(context.Context, sandbox.Request) (sandbox.Result, error)
}

type releaseGateEnvironment struct {
	releaseRoot      string
	currentPath      string
	runtimeRoot      func(string) string
	executable       func() (string, error)
	requireDirectory func(string) error
	cgroupParent     func(string, bool) (string, error)
	verifyCgroup     func(string) error
	createNetNS      func(string) (string, func() error, error)
	newBackend       func(sandbox.FirecrackerConfig) (releaseGateBackend, error)
	random           io.Reader
	now              func() time.Time
	writeReceipt     func(string, map[string]any) error
}

func productionReleaseGateEnvironment() releaseGateEnvironment {
	return releaseGateEnvironment{
		releaseRoot: "/opt/vane/releases", currentPath: "/opt/vane/current",
		runtimeRoot: gateRuntimeRoot, executable: os.Executable,
		requireDirectory: requireRootDirectory, cgroupParent: exactGateCgroupParent,
		verifyCgroup: verifyGateCgroupLimits, createNetNS: sandbox.CreateGateNetNS,
		newBackend: func(config sandbox.FirecrackerConfig) (releaseGateBackend, error) {
			return sandbox.NewFirecrackerBackend(config, nil)
		},
		random: rand.Reader, now: time.Now, writeReceipt: writeDurableReceipt,
	}
}

func runReleaseGate(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runReleaseGateWithAuthority(ctx, arguments, stdout, stderr, os.Geteuid(), executeReleaseGate)
}

func runReleaseGateWithAuthority(ctx context.Context, arguments []string, stdout, stderr io.Writer, effectiveUID int,
	execute func(context.Context, string, string) (map[string]any, error)) int {
	flags := flag.NewFlagSet("sandboxd release-gate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	revision := flags.String("sha", "", "exact deployed source revision")
	receiptPath := flags.String("receipt", "", "new durable Gate receipt")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || !exactRevision.MatchString(*revision) || !filepath.IsAbs(*receiptPath) {
		fmt.Fprintln(stderr, "usage: sandboxd release-gate --sha <40-hex> --receipt <absolute-new-path>")
		return 2
	}
	if runtime.GOOS != "linux" || effectiveUID != 0 {
		fmt.Fprintln(stderr, "Firecracker release Gate requires Linux root with KVM")
		return 1
	}
	value, err := execute(ctx, *revision, *receiptPath)
	if err != nil {
		fmt.Fprintf(stderr, "Firecracker release Gate failed: %v\n", err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		return 1
	}
	return 0
}

func runReleaseGateReap(arguments []string, stderr io.Writer) int {
	return runReleaseGateReapWithAuthority(arguments, stderr, os.Geteuid(), exactGateCgroupParent, sandbox.ReapGateRuntime)
}

func runReleaseGateNetNSPreflight(arguments []string, stdout, stderr io.Writer) int {
	return runReleaseGateNetNSPreflightWithAuthority(arguments, stdout, stderr, os.Geteuid(), exactGateCgroupParent,
		gateRuntimeRoot, requireRootDirectory, probeGateInternetFamiliesBlocked,
		sandbox.CreateGateNetNS, sandbox.InspectGateNetNS)
}

func runReleaseGateNetNSPreflightWithAuthority(arguments []string, stdout, stderr io.Writer, effectiveUID int,
	cgroupParent func(string, bool) (string, error), runtimeRoot func(string) string,
	requireDirectory func(string) error, probeInternetFamilies func() error,
	createNetNS func(string) (string, func() error, error), inspectNetNS func(string) error) int {
	flags := flag.NewFlagSet("sandboxd release-gate-netns-preflight", flag.ContinueOnError)
	flags.SetOutput(stderr)
	revision := flags.String("sha", "", "exact deployed source revision")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || !exactRevision.MatchString(*revision) {
		fmt.Fprintln(stderr, "usage: sandboxd release-gate-netns-preflight --sha <40-hex>")
		return 2
	}
	if runtime.GOOS != "linux" || effectiveUID != 0 {
		fmt.Fprintln(stderr, "Firecracker release Gate netns preflight requires Linux root")
		return 1
	}
	parent, err := cgroupParent(*revision, false)
	root := runtimeRoot(*revision)
	if err == nil && parent != "system.slice/"+gateUnitName(*revision) {
		err = errors.New("Firecracker release Gate netns preflight cgroup differs")
	}
	if err == nil {
		err = requireDirectory(root)
	}
	if err == nil {
		err = probeInternetFamilies()
	}
	gateRoot := filepath.Join(root, "firecracker-work")
	gateRootCreated := false
	if err == nil {
		err = os.Mkdir(gateRoot, 0o700)
		gateRootCreated = err == nil
	}
	if err == nil {
		err = os.Chmod(gateRoot, 0o700)
	}
	var cleanup func() error
	var path string
	if err == nil {
		path, cleanup, err = createNetNS(gateRoot)
	}
	if err == nil {
		err = inspectNetNS(path)
	}
	if cleanup != nil {
		err = errors.Join(err, cleanup())
	}
	if gateRootCreated {
		if removeErr := os.Remove(gateRoot); err == nil {
			err = removeErr
		} else if removeErr != nil {
			err = errors.Join(err, removeErr)
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "Firecracker release Gate netns preflight failed: %v\n", err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(map[string]any{
		"schema": "vane.firecracker-netns-preflight/v1", "revision": *revision,
		"private_network": true, "af_unix": true, "af_netlink": true,
		"af_inet": false, "af_inet6": false, "ok": true,
	}); err != nil {
		return 1
	}
	return 0
}

func probeGateInternetFamiliesBlocked() error {
	return probeGateInternetFamiliesBlockedWithSocket(unix.Socket, unix.Close)
}

func probeGateInternetFamiliesBlockedWithSocket(
	socket func(domain, socketType, protocol int) (int, error),
	closeDescriptor func(int) error,
) error {
	for _, family := range []struct {
		name   string
		domain int
	}{
		{"AF_INET", unix.AF_INET},
		{"AF_INET6", unix.AF_INET6},
	} {
		descriptor, err := socket(family.domain, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		if err == nil {
			_ = closeDescriptor(descriptor)
			return fmt.Errorf("Firecracker release Gate %s socket unexpectedly available", family.name)
		}
		if !errors.Is(err, unix.EAFNOSUPPORT) && !errors.Is(err, unix.EPERM) {
			return fmt.Errorf("Firecracker release Gate %s refusal is not authoritative: %w", family.name, err)
		}
	}
	return nil
}

func runReleaseGateReapWithAuthority(arguments []string, stderr io.Writer, effectiveUID int,
	cgroupParent func(string, bool) (string, error), reap func(string) error) int {
	flags := flag.NewFlagSet("sandboxd release-gate-reap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	revision := flags.String("sha", "", "exact deployed source revision")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || !exactRevision.MatchString(*revision) {
		fmt.Fprintln(stderr, "usage: sandboxd release-gate-reap --sha <40-hex>")
		return 2
	}
	if runtime.GOOS != "linux" || effectiveUID != 0 {
		fmt.Fprintln(stderr, "Firecracker release Gate reaper requires Linux root")
		return 1
	}
	parent, err := cgroupParent(*revision, true)
	if err == nil {
		_ = parent // exact unit authority is required before touching its runtime tree.
		err = reap(gateRuntimeRoot(*revision))
	}
	if err != nil {
		fmt.Fprintf(stderr, "Firecracker release Gate reaper failed: %v\n", err)
		return 1
	}
	return 0
}

func exactGateCgroupParent(revision string, allowControl bool) (string, error) {
	return exactGateCgroupParentWithReader(revision, allowControl, func() ([]byte, error) {
		return os.ReadFile("/proc/self/cgroup")
	})
}

func exactGateCgroupParentWithReader(revision string, allowControl bool, read func() ([]byte, error)) (string, error) {
	payload, err := read()
	if err != nil || len(payload) > 64<<10 {
		return "", errors.New("cannot read exact unified service cgroup")
	}
	return parseExactGateCgroup(payload, revision, allowControl)
}

func parseExactGateCgroup(payload []byte, revision string, allowControl bool) (string, error) {
	want := "/system.slice/" + gateUnitName(revision)
	found := ""
	for _, line := range strings.Split(strings.TrimSpace(string(payload)), "\n") {
		if strings.HasPrefix(line, "0::") {
			if found != "" {
				return "", errors.New("duplicate unified service cgroup authority")
			}
			found = strings.TrimPrefix(line, "0::")
		}
	}
	if found != want && (!allowControl || found != want+"/.control") {
		return "", fmt.Errorf("release Gate cgroup is not exact: %q", found)
	}
	return strings.TrimPrefix(want, "/"), nil
}

func verifyGateCgroupLimits(parent string) error {
	return verifyGateCgroupLimitsAt("/sys/fs/cgroup", parent)
}

func verifyGateCgroupLimitsAt(cgroupRoot, parent string) error {
	root := filepath.Join(cgroupRoot, parent)
	read := func(name string) (string, error) {
		payload, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || len(payload) > 128 {
			return "", fmt.Errorf("read Firecracker Gate %s limit", name)
		}
		return strings.TrimSpace(string(payload)), nil
	}
	cpu, errCPU := read("cpu.max")
	memory, errMemory := read("memory.max")
	pids, errPIDs := read("pids.max")
	if err := errors.Join(errCPU, errMemory, errPIDs); err != nil {
		return err
	}
	return validateGateCgroupLimits(cpu, memory, pids)
}

func validateGateCgroupLimits(cpu, memory, pids string) error {
	if cpu != "100000 100000" || memory != "268435456" || pids != "64" {
		return fmt.Errorf("Firecracker Gate cgroup limits differ: cpu=%q memory=%q pids=%q", cpu, memory, pids)
	}
	return nil
}

func executeReleaseGate(ctx context.Context, revision, receiptPath string) (map[string]any, error) {
	return executeReleaseGateWithEnvironment(ctx, revision, receiptPath, productionReleaseGateEnvironment())
}

func executeReleaseGateWithEnvironment(ctx context.Context, revision, receiptPath string,
	environment releaseGateEnvironment) (map[string]any, error) {
	releaseDir := filepath.Join(environment.releaseRoot, revision)
	current, err := filepath.EvalSymlinks(environment.currentPath)
	if err != nil || current != releaseDir {
		return nil, errors.New("Firecracker Gate revision is not the exact active release")
	}
	if err := environment.requireDirectory(releaseDir); err != nil {
		return nil, err
	}
	receiptFile := filepath.Join(releaseDir, "release-receipt.json")
	backendManifestFile := filepath.Join(releaseDir, "backend-manifest.json")
	sandboxManifestFile := filepath.Join(releaseDir, "sandbox", "manifest.json")
	var release releaseReceipt
	if err := readExactJSON(receiptFile, 64<<10, &release); err != nil || release.SchemaVersion != "vane.release-receipt/v1" || release.SourceRevision != revision {
		return nil, errors.New("release receipt is not exact for Firecracker Gate")
	}
	if digestFile(backendManifestFile) != release.BackendManifestSHA256 {
		return nil, errors.New("backend manifest is not bound by the release receipt")
	}
	var archiveManifest backendManifest
	if err := readExactJSON(backendManifestFile, 10<<20, &archiveManifest); err != nil || archiveManifest.Schema != 2 || archiveManifest.SourceSHA != revision {
		return nil, errors.New("backend manifest is not exact for Firecracker Gate")
	}
	entries := make(map[string]fileManifestEntry, len(archiveManifest.Files))
	for _, entry := range archiveManifest.Files {
		if _, exists := entries[entry.Path]; exists {
			return nil, errors.New("backend manifest contains duplicate files")
		}
		entries[entry.Path] = entry
	}
	for _, name := range []string{"bin/sandboxd", "sandbox/manifest.json"} {
		entry, ok := entries[name]
		path := filepath.Join(releaseDir, filepath.FromSlash(name))
		if !ok || digestFile(path) != entry.SHA256 || sizeFile(path) != entry.Size {
			return nil, fmt.Errorf("release Gate binding is invalid for %s", name)
		}
	}
	self, err := environment.executable()
	if err != nil || digestFile(self) != entries["bin/sandboxd"].SHA256 {
		return nil, errors.New("running sandboxd is not the release-bound binary")
	}
	var artifacts sandboxManifest
	if err := readExactJSON(sandboxManifestFile, 64<<10, &artifacts); err != nil ||
		artifacts.Schema != "vane.firecracker-release-artifacts/v1" || artifacts.SourceRevision != revision ||
		artifacts.Architecture != "x86_64" || artifacts.SandboxdSHA256 != entries["bin/sandboxd"].SHA256 {
		return nil, errors.New("Firecracker artifact manifest is not release-bound")
	}
	expectedNames := []string{"code.raw", "firecracker", "jailer", "rootfs.cpio", "vmlinux"}
	if len(artifacts.Artifacts) != len(expectedNames) {
		return nil, errors.New("Firecracker artifact set is not closed")
	}
	for _, name := range expectedNames {
		artifact, ok := artifacts.Artifacts[name]
		entry, bound := entries["sandbox/"+name]
		path := filepath.Join(releaseDir, "sandbox", name)
		if !ok || !bound || artifact.SHA256 != entry.SHA256 || artifact.Size != entry.Size ||
			digestFile(path) != artifact.SHA256 || sizeFile(path) != artifact.Size {
			return nil, fmt.Errorf("Firecracker artifact is not exact: %s", name)
		}
	}
	cgroupParent, err := environment.cgroupParent(revision, false)
	if err != nil {
		return nil, err
	}
	if err := environment.verifyCgroup(cgroupParent); err != nil {
		return nil, err
	}
	runtimeRoot := environment.runtimeRoot(revision)
	if err := environment.requireDirectory(runtimeRoot); err != nil {
		return nil, errors.New("Firecracker Gate systemd runtime authority is unsafe")
	}
	gateRoot := filepath.Join(runtimeRoot, "firecracker-work")
	if err := os.Mkdir(gateRoot, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(gateRoot, 0o700); err != nil || environment.requireDirectory(gateRoot) != nil {
		return nil, errors.New("Firecracker Gate work root is unsafe")
	}
	workRoot, err := os.MkdirTemp(gateRoot, "work-")
	if err != nil {
		_ = os.Remove(gateRoot)
		return nil, err
	}
	cleanupWork := func() error { return os.Remove(workRoot) }
	netNS, cleanupNetNS, err := environment.createNetNS(workRoot)
	if err != nil {
		_ = cleanupWork()
		_ = os.Remove(gateRoot)
		return nil, err
	}
	cleaned := false
	defer func() {
		if !cleaned {
			_ = cleanupNetNS()
			_ = cleanupWork()
			_ = os.Remove(gateRoot)
		}
	}()
	pin := func(name string) sandbox.ArtifactPin {
		item := artifacts.Artifacts[name]
		return sandbox.ArtifactPin{Path: filepath.Join(releaseDir, "sandbox", name), SHA256: item.SHA256, SizeBytes: item.Size}
	}
	codeVersion := artifacts.Artifacts["code.raw"].SHA256
	config := sandbox.FirecrackerConfig{Firecracker: pin("firecracker"), Jailer: pin("jailer"), Kernel: pin("vmlinux"),
		RootFS: pin("rootfs.cpio"), CodeImages: map[string]sandbox.ArtifactPin{sandbox.GateCapabilityID + "@" + codeVersion: pin("code.raw")},
		NetNSPath: netNS, FirecrackerVersion: artifacts.FirecrackerVersion, JailerVersion: artifacts.FirecrackerVersion,
		WorkRoot: workRoot, CgroupRoot: "/sys/fs/cgroup", CgroupParent: cgroupParent, InheritCgroupLimits: true,
		JailerUIDStart: 62000, JailerGIDStart: 62000, IsolationSlots: 1, Production: true}
	if err := config.BindServiceIdentities(65533, 0, 0); err != nil {
		return nil, err
	}
	backend, err := environment.newBackend(config)
	if err != nil {
		return nil, err
	}
	if err := backend.Preflight(ctx); err != nil {
		return nil, err
	}
	challenge := make([]byte, 32)
	if _, err := io.ReadFull(environment.random, challenge); err != nil {
		return nil, err
	}
	invocation := "gate-" + hex.EncodeToString(challenge[:8])
	policy := sandbox.Policy{VCPUCount: 1, MemoryMiB: 128, PIDsMax: 64, CPUQuotaMicros: 100000,
		CPUPeriodMicros: 100000, WallTimeoutMS: 15000, OutputBytesMax: 256 << 10,
		TmpfsBytesMax: 16 << 20, GuestUID: 10001, GuestGID: 10001,
		NetworkDisabled: true, RootReadOnly: true, CodeReadOnly: true}
	request, err := (sandbox.Request{ProtocolVersion: sandbox.ProtocolVersion, TenantID: 1, UserID: 1,
		CapabilityID: sandbox.GateCapabilityID, CapabilityVersion: codeVersion, InvocationID: invocation,
		Policy: policy, Input: challenge}).Seal()
	if err != nil {
		return nil, err
	}
	started := environment.now()
	result, err := backend.RunGateSelfTest(ctx, request)
	duration := environment.now().Sub(started)
	if err != nil || result.Status != "succeeded" {
		return nil, errors.Join(err, errors.New("Firecracker guest self-test did not succeed"))
	}
	if err := cleanupNetNS(); err != nil {
		return nil, fmt.Errorf("Firecracker Gate namespace cleanup failed: %w", err)
	}
	if err := cleanupWork(); err != nil {
		return nil, fmt.Errorf("Firecracker Gate crash scrub failed: %w", err)
	}
	if err := os.Remove(gateRoot); err != nil {
		return nil, fmt.Errorf("Firecracker Gate root scrub failed: %w", err)
	}
	cleaned = true
	artifactDigests := make(map[string]string, len(expectedNames))
	for _, name := range expectedNames {
		artifactDigests[name] = artifacts.Artifacts[name].SHA256
	}
	guestDigest := sha256.Sum256(result.Output)
	evidence := map[string]any{"schema": "vane.firecracker-production-gate/v1", "revision": revision,
		"release_receipt_sha256": digestFile(receiptFile), "backend_manifest_sha256": digestFile(backendManifestFile),
		"sandboxd_sha256": entries["bin/sandboxd"].SHA256, "artifacts": artifactDigests,
		"guest_receipt_sha256": hex.EncodeToString(guestDigest[:]),
		"guest_receipt_base64": base64.StdEncoding.EncodeToString(result.Output), "invocation_id": invocation,
		"duration_ms": duration.Milliseconds(), "network_interfaces": 0, "mmds": false,
		"jailer_uid": 62000, "jailer_gid": 62000, "cgroup_v2": true, "cgroup_parent": cgroupParent,
		"cpu_max": "100000 100000", "memory_max_bytes": 256 << 20, "pids_max": 64,
		"scrubbed": true, "ok": true}
	if err := environment.writeReceipt(receiptPath, evidence); err != nil {
		return nil, err
	}
	return evidence, nil
}

func readExactJSON(path string, limit int64, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(payload)) > limit {
		return errors.New("release Gate JSON is oversized")
	}
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.InputOffset() != int64(len(bytes.TrimSpace(payload))) {
		return errors.New("release Gate JSON has trailing content")
	}
	return nil
}

func digestFile(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	value := sha256.New()
	if _, err := io.Copy(value, file); err != nil {
		return ""
	}
	return hex.EncodeToString(value.Sum(nil))
}

func sizeFile(path string) int64 {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return -1
	}
	return info.Size()
}

func requireRootDirectory(path string) error {
	return requireDirectoryChain(path, "/", 0)
}

func requireDirectoryChain(path, stop string, expectedUID uint32) error {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		statValue, ok := info.Sys().(*syscall.Stat_t)
		if !ok || statValue.Uid != expectedUID || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("root directory chain is unsafe: %s", current)
		}
		if current == stop {
			return nil
		}
		if current == "/" {
			return errors.New("directory authority stop is not an ancestor")
		}
	}
}

func writeDurableReceipt(path string, value map[string]any) error {
	return writeDurableReceiptWithAuthority(path, value, requireRootDirectory)
}

func writeDurableReceiptWithAuthority(path string, value map[string]any, requireDirectory func(string) error) error {
	parent := filepath.Dir(path)
	if err := requireDirectory(parent); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_SYNC, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(payload, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	directory, openErr := os.Open(parent)
	if openErr != nil {
		return errors.Join(writeErr, syncErr, closeErr, openErr)
	}
	directorySyncErr := directory.Sync()
	directoryCloseErr := directory.Close()
	return errors.Join(writeErr, syncErr, closeErr, directorySyncErr, directoryCloseErr)
}
