//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/YouToco/vane/server/sandbox"
	"golang.org/x/sys/unix"
)

type releaseGateBackendFixture struct {
	preflightErr error
	runErr       error
	status       string
}

//go:embed release_gate.go
var releaseGateProductionSource string

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write") }

func (value releaseGateBackendFixture) Preflight(context.Context) error { return value.preflightErr }

func (value releaseGateBackendFixture) RunGateSelfTest(_ context.Context, request sandbox.Request) (sandbox.Result, error) {
	status := value.status
	if status == "" {
		status = "succeeded"
	}
	return sandbox.Result{InvocationID: request.InvocationID, Status: status, Output: []byte("guest-receipt")}, value.runErr
}

func fixtureDigest(payload []byte) string {
	value := sha256.Sum256(payload)
	return hex.EncodeToString(value[:])
}

func writeFixtureJSON(t *testing.T, path string, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return payload
}

type releaseGateFixture struct {
	revision    string
	root        string
	runtimeRoot string
	releaseDir  string
	environment releaseGateEnvironment
	written     *map[string]any
}

func rebindBackendManifest(t *testing.T, fixture releaseGateFixture, archive backendManifest) {
	t.Helper()
	path := filepath.Join(fixture.releaseDir, "backend-manifest.json")
	payload := writeFixtureJSON(t, path, archive)
	receiptPath := filepath.Join(fixture.releaseDir, "release-receipt.json")
	var receipt releaseReceipt
	if err := readExactJSON(receiptPath, 64<<10, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.BackendManifestSHA256 = fixtureDigest(payload)
	writeFixtureJSON(t, receiptPath, receipt)
}

func rebindSandboxManifest(t *testing.T, fixture releaseGateFixture, manifest sandboxManifest) {
	t.Helper()
	path := filepath.Join(fixture.releaseDir, "sandbox", "manifest.json")
	payload := writeFixtureJSON(t, path, manifest)
	backendPath := filepath.Join(fixture.releaseDir, "backend-manifest.json")
	var archive backendManifest
	if err := readExactJSON(backendPath, 10<<20, &archive); err != nil {
		t.Fatal(err)
	}
	for index := range archive.Files {
		if archive.Files[index].Path == "sandbox/manifest.json" {
			archive.Files[index].SHA256 = fixtureDigest(payload)
			archive.Files[index].Size = int64(len(payload))
		}
	}
	rebindBackendManifest(t, fixture, archive)
}

func newReleaseGateFixture(t *testing.T) releaseGateFixture {
	revision := strings.Repeat("c", 40)
	root := t.TempDir()
	releaseRoot := filepath.Join(root, "releases")
	releaseDir := filepath.Join(releaseRoot, revision)
	sandboxDir := filepath.Join(releaseDir, "sandbox")
	if err := os.MkdirAll(filepath.Join(releaseDir, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sandboxDir, 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"bin/sandboxd": []byte("sandboxd-fixture"), "sandbox/code.raw": []byte("code"),
		"sandbox/firecracker": []byte("firecracker"), "sandbox/jailer": []byte("jailer"),
		"sandbox/rootfs.cpio": []byte("rootfs"), "sandbox/vmlinux": []byte("kernel"),
	}
	artifactValues := make(map[string]sandboxArtifact, 5)
	entries := make([]fileManifestEntry, 0, 7)
	for name, payload := range files {
		path := filepath.Join(releaseDir, filepath.FromSlash(name))
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, fileManifestEntry{Path: name, SHA256: fixtureDigest(payload), Size: int64(len(payload)), Mode: 0o600})
		if strings.HasPrefix(name, "sandbox/") {
			artifactValues[strings.TrimPrefix(name, "sandbox/")] = sandboxArtifact{SHA256: fixtureDigest(payload), Size: int64(len(payload))}
		}
	}
	manifest := sandboxManifest{Schema: "vane.firecracker-release-artifacts/v1", SourceRevision: revision,
		Architecture: "x86_64", FirecrackerVersion: "v1.16.1", SandboxdSHA256: fixtureDigest(files["bin/sandboxd"]),
		Artifacts: artifactValues}
	manifestPayload := writeFixtureJSON(t, filepath.Join(sandboxDir, "manifest.json"), manifest)
	entries = append(entries, fileManifestEntry{Path: "sandbox/manifest.json", SHA256: fixtureDigest(manifestPayload), Size: int64(len(manifestPayload)), Mode: 0o600})
	archive := backendManifest{Schema: 2, Component: "server", SourceSHA: revision, Files: entries}
	backendPayload := writeFixtureJSON(t, filepath.Join(releaseDir, "backend-manifest.json"), archive)
	release := releaseReceipt{SchemaVersion: "vane.release-receipt/v1", SourceRevision: revision,
		BackendManifestSHA256: fixtureDigest(backendPayload)}
	writeFixtureJSON(t, filepath.Join(releaseDir, "release-receipt.json"), release)
	current := filepath.Join(root, "current")
	if err := os.Symlink(releaseDir, current); err != nil {
		t.Fatal(err)
	}
	runtimeRoot := filepath.Join(root, "runtime")
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	written := new(map[string]any)
	clock := time.Unix(100, 0)
	environment := releaseGateEnvironment{
		releaseRoot: releaseRoot, currentPath: current, runtimeRoot: func(string) string { return runtimeRoot },
		executable:       func() (string, error) { return filepath.Join(releaseDir, "bin", "sandboxd"), nil },
		requireDirectory: func(string) error { return nil },
		cgroupParent: func(string, bool) (string, error) {
			return "system.slice/vane-firecracker-gate-" + revision + ".service", nil
		},
		verifyCgroup: func(string) error { return nil },
		createNetNS: func(parent string) (string, func() error, error) {
			path := filepath.Join(parent, "empty-netns-fixture")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				return "", nil, err
			}
			return path, func() error { return os.Remove(path) }, nil
		},
		newBackend: func(sandbox.FirecrackerConfig) (releaseGateBackend, error) { return releaseGateBackendFixture{}, nil },
		random:     strings.NewReader(strings.Repeat("r", 32)), now: func() time.Time { clock = clock.Add(time.Millisecond); return clock },
		writeReceipt: func(_ string, value map[string]any) error { *written = value; return nil },
	}
	return releaseGateFixture{revision: revision, root: root, runtimeRoot: runtimeRoot, releaseDir: releaseDir,
		environment: environment, written: written}
}

func TestExecuteReleaseGateThroughBoundEnvironment(t *testing.T) {
	fixture := newReleaseGateFixture(t)
	evidence, err := executeReleaseGateWithEnvironment(t.Context(), fixture.revision,
		filepath.Join(fixture.root, "receipt.json"), fixture.environment)
	if err != nil {
		t.Fatal(err)
	}
	if evidence["ok"] != true || evidence["scrubbed"] != true || (*fixture.written)["revision"] != fixture.revision {
		t.Fatalf("incomplete Gate evidence=%v written=%v", evidence, *fixture.written)
	}
	if _, err := os.Stat(filepath.Join(fixture.runtimeRoot, "firecracker-work")); !os.IsNotExist(err) {
		t.Fatalf("successful Gate did not scrub work root: %v", err)
	}
}

func TestExecuteReleaseGateRejectsEveryRuntimeFailure(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*releaseGateFixture)
	}{
		{"inactive", func(value *releaseGateFixture) { value.environment.currentPath = filepath.Join(value.root, "missing") }},
		{"release-directory", func(value *releaseGateFixture) {
			value.environment.requireDirectory = func(path string) error {
				if path == value.releaseDir {
					return errors.New("release")
				}
				return nil
			}
		}},
		{"executable", func(value *releaseGateFixture) {
			value.environment.executable = func() (string, error) { return "", errors.New("executable") }
		}},
		{"cgroup-parent", func(value *releaseGateFixture) {
			value.environment.cgroupParent = func(string, bool) (string, error) { return "", errors.New("cgroup") }
		}},
		{"cgroup-limits", func(value *releaseGateFixture) {
			value.environment.verifyCgroup = func(string) error { return errors.New("limits") }
		}},
		{"runtime-directory", func(value *releaseGateFixture) {
			value.environment.requireDirectory = func(path string) error {
				if path == value.runtimeRoot {
					return errors.New("runtime")
				}
				return nil
			}
		}},
		{"netns", func(value *releaseGateFixture) {
			value.environment.createNetNS = func(string) (string, func() error, error) { return "", nil, errors.New("netns") }
		}},
		{"backend", func(value *releaseGateFixture) {
			value.environment.newBackend = func(sandbox.FirecrackerConfig) (releaseGateBackend, error) { return nil, errors.New("backend") }
		}},
		{"preflight", func(value *releaseGateFixture) {
			value.environment.newBackend = func(sandbox.FirecrackerConfig) (releaseGateBackend, error) {
				return releaseGateBackendFixture{preflightErr: errors.New("preflight")}, nil
			}
		}},
		{"random", func(value *releaseGateFixture) { value.environment.random = strings.NewReader("short") }},
		{"run-error", func(value *releaseGateFixture) {
			value.environment.newBackend = func(sandbox.FirecrackerConfig) (releaseGateBackend, error) {
				return releaseGateBackendFixture{runErr: errors.New("run")}, nil
			}
		}},
		{"run-status", func(value *releaseGateFixture) {
			value.environment.newBackend = func(sandbox.FirecrackerConfig) (releaseGateBackend, error) {
				return releaseGateBackendFixture{status: "failed"}, nil
			}
		}},
		{"namespace-cleanup", func(value *releaseGateFixture) {
			value.environment.createNetNS = func(parent string) (string, func() error, error) {
				path := filepath.Join(parent, "empty-netns-fixture")
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					return "", nil, err
				}
				return path, func() error { return errors.New("cleanup") }, nil
			}
		}},
		{"receipt", func(value *releaseGateFixture) {
			value.environment.writeReceipt = func(string, map[string]any) error { return errors.New("receipt") }
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newReleaseGateFixture(t)
			test.mutate(&fixture)
			if _, err := executeReleaseGateWithEnvironment(t.Context(), fixture.revision,
				filepath.Join(fixture.root, "receipt.json"), fixture.environment); err == nil {
				t.Fatal("runtime failure was accepted")
			}
		})
	}
}

func TestExecuteReleaseGateRejectsEveryBindingMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*releaseGateFixture)
	}{
		{"release-receipt", func(value *releaseGateFixture) {
			writeFixtureJSON(t, filepath.Join(value.releaseDir, "release-receipt.json"), map[string]any{"schema_version": "wrong"})
		}},
		{"backend-digest", func(value *releaseGateFixture) {
			if err := os.WriteFile(filepath.Join(value.releaseDir, "backend-manifest.json"), []byte("mutation"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"backend-schema", func(value *releaseGateFixture) {
			var archive backendManifest
			if err := readExactJSON(filepath.Join(value.releaseDir, "backend-manifest.json"), 10<<20, &archive); err != nil {
				t.Fatal(err)
			}
			archive.Schema = 1
			rebindBackendManifest(t, *value, archive)
		}},
		{"backend-duplicate", func(value *releaseGateFixture) {
			var archive backendManifest
			if err := readExactJSON(filepath.Join(value.releaseDir, "backend-manifest.json"), 10<<20, &archive); err != nil {
				t.Fatal(err)
			}
			archive.Files = append(archive.Files, archive.Files[0])
			rebindBackendManifest(t, *value, archive)
		}},
		{"bound-file", func(value *releaseGateFixture) {
			if err := os.WriteFile(filepath.Join(value.releaseDir, "bin", "sandboxd"), []byte("mutation"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"sandbox-manifest", func(value *releaseGateFixture) {
			var manifest sandboxManifest
			if err := readExactJSON(filepath.Join(value.releaseDir, "sandbox", "manifest.json"), 64<<10, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.Architecture = "aarch64"
			rebindSandboxManifest(t, *value, manifest)
		}},
		{"artifact-set", func(value *releaseGateFixture) {
			var manifest sandboxManifest
			if err := readExactJSON(filepath.Join(value.releaseDir, "sandbox", "manifest.json"), 64<<10, &manifest); err != nil {
				t.Fatal(err)
			}
			delete(manifest.Artifacts, "code.raw")
			rebindSandboxManifest(t, *value, manifest)
		}},
		{"artifact", func(value *releaseGateFixture) {
			if err := os.WriteFile(filepath.Join(value.releaseDir, "sandbox", "code.raw"), []byte("mutation"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newReleaseGateFixture(t)
			test.mutate(&fixture)
			if _, err := executeReleaseGateWithEnvironment(t.Context(), fixture.revision,
				filepath.Join(fixture.root, "receipt.json"), fixture.environment); err == nil {
				t.Fatal("binding mutation accepted")
			}
		})
	}
}

func TestGuestWorkerBoundedReceipt(t *testing.T) {
	if os.Getenv("VANE_GUEST_WORKER_TEST_HELPER") == "1" {
		uid, uidErr := strconv.Atoi(os.Getenv("VANE_GUEST_WORKER_UID"))
		gid, gidErr := strconv.Atoi(os.Getenv("VANE_GUEST_WORKER_GID"))
		if uidErr != nil || gidErr != nil {
			t.Fatal("invalid helper identity")
		}
		input := []byte("guest-worker-fixture")
		arguments := []string{strconv.Itoa(len(input)), fixtureDigest(input), strings.Repeat("a", 64),
			"gate-0123456789abcdef", strconv.Itoa(uid), strconv.Itoa(gid)}
		if code := runGuestWorker(arguments, os.Stdout, os.Stderr); code != 0 {
			t.Fatalf("guest worker helper code=%d", code)
		}
		return
	}
	input := []byte("guest-worker-fixture")
	file := filepath.Join(t.TempDir(), "input.raw")
	if err := os.WriteFile(file, input, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	uid, gid := os.Geteuid(), os.Getegid()
	command := exec.Command(os.Args[0], "-test.run=^TestGuestWorkerBoundedReceipt$")
	command.ExtraFiles = []*os.File{opened}
	if uid == 0 || gid == 0 {
		uid, gid = 10001, 10001
		command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}}
	}
	command.Env = append(os.Environ(), "VANE_GUEST_WORKER_TEST_HELPER=1",
		"VANE_GUEST_WORKER_UID="+strconv.Itoa(uid), "VANE_GUEST_WORKER_GID="+strconv.Itoa(gid))
	payload, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("guest worker subprocess: %v: %s", err, payload)
	}
	marker := []byte(guestReceiptMarker)
	index := bytes.Index(payload, marker)
	if index < 0 {
		t.Fatalf("guest receipt marker missing: %s", payload)
	}
	line := bytes.SplitN(payload[index+len(marker):], []byte("\n"), 2)[0]
	var receipt map[string]any
	if err := json.Unmarshal(line, &receipt); err != nil || receipt["invocation_id"] != "gate-0123456789abcdef" ||
		receipt["input_sha256"] != fixtureDigest(input) {
		t.Fatalf("guest receipt=%v err=%v", receipt, err)
	}
	var stderr bytes.Buffer
	if code := runGuest([]string{"unexpected"}, &bytes.Buffer{}, &stderr); code != 1 {
		t.Fatalf("guest init accepted invalid context code=%d", code)
	}
}

func guestInitFixture(t *testing.T, commandLine string) guestInitEnvironment {
	t.Helper()
	openFile := func() (*os.File, error) {
		return os.CreateTemp(t.TempDir(), "guest-device-")
	}
	return guestInitEnvironment{
		pid: 1, mkdirAll: func(string, os.FileMode) error { return nil },
		mount:       func(string, string, string, uintptr, string) error { return nil },
		readCmdline: func() ([]byte, error) { return []byte(commandLine), nil },
		openInput:   openFile, openTTY: openFile,
		runWorker: func(_ *os.File, _ *os.File, arguments []string, uid, gid uint32) error {
			if len(arguments) != 7 || arguments[0] != "guest-worker" || uid != 10001 || gid != 10002 {
				return errors.New("worker authority mismatch")
			}
			return nil
		},
		sync: func() {}, reboot: func() error { return nil },
	}
}

func TestGuestInitStateMachineAndFailures(t *testing.T) {
	valid := "console=ttyS0 vane.input_bytes=20 vane.input_sha256=" + strings.Repeat("a", 64) +
		" vane.request_sha256=" + strings.Repeat("b", 64) + " vane.invocation=gate-0123456789abcdef" +
		" vane.uid=10001 vane.gid=10002"
	environment := guestInitFixture(t, valid)
	var events []string
	environment.mount = func(source, target, filesystem string, flags uintptr, data string) error {
		events = append(events, fmt.Sprintf("mount:%s:%s:%s:%d:%s", source, target, filesystem, flags, data))
		return nil
	}
	environment.openInput = func() (*os.File, error) {
		events = append(events, "open:/dev/vdb")
		return os.CreateTemp(t.TempDir(), "guest-device-")
	}
	environment.openTTY = func() (*os.File, error) {
		events = append(events, "open:/dev/ttyS0")
		return os.CreateTemp(t.TempDir(), "guest-device-")
	}
	if code := runGuestInit(nil, &bytes.Buffer{}, environment); code != 0 {
		t.Fatalf("valid guest init code=%d", code)
	}
	want := []string{
		fmt.Sprintf("mount:proc:/proc:proc:%d:", uintptr(unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC)),
		fmt.Sprintf("mount:devtmpfs:/dev:devtmpfs:%d:mode=0755", uintptr(unix.MS_NOSUID|unix.MS_NOEXEC)),
		fmt.Sprintf("mount:sysfs:/sys:sysfs:%d:", uintptr(unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC)),
		"open:/dev/vdb", "open:/dev/ttyS0",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("guest filesystem/device order=%q want=%q", events, want)
	}
	tests := []struct {
		name   string
		mutate func(*guestInitEnvironment)
	}{
		{"mkdir", func(value *guestInitEnvironment) {
			value.mkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
		}},
		{"proc-mount", func(value *guestInitEnvironment) {
			value.mount = func(source, _ string, _ string, _ uintptr, _ string) error {
				if source == "proc" {
					return errors.New("mount")
				}
				return nil
			}
		}},
		{"devtmpfs-mount", func(value *guestInitEnvironment) {
			value.mount = func(source, _ string, _ string, _ uintptr, _ string) error {
				if source == "devtmpfs" {
					return errors.New("mount")
				}
				return nil
			}
		}},
		{"sys-mount", func(value *guestInitEnvironment) {
			value.mount = func(source, _ string, _ string, _ uintptr, _ string) error {
				if source == "sysfs" {
					return errors.New("mount")
				}
				return nil
			}
		}},
		{"cmdline-read", func(value *guestInitEnvironment) {
			value.readCmdline = func() ([]byte, error) { return nil, errors.New("read") }
		}},
		{"duplicate", func(value *guestInitEnvironment) {
			value.readCmdline = func() ([]byte, error) { return []byte(valid + " vane.uid=10001"), nil }
		}},
		{"incomplete", func(value *guestInitEnvironment) {
			value.readCmdline = func() ([]byte, error) { return []byte("vane.uid=10001"), nil }
		}},
		{"limits", func(value *guestInitEnvironment) {
			value.readCmdline = func() ([]byte, error) {
				return []byte(strings.Replace(valid, "vane.input_bytes=20", "vane.input_bytes=0", 1)), nil
			}
		}},
		{"input", func(value *guestInitEnvironment) {
			value.openInput = func() (*os.File, error) { return nil, errors.New("input") }
		}},
		{"tty", func(value *guestInitEnvironment) {
			value.openTTY = func() (*os.File, error) { return nil, errors.New("tty") }
		}},
		{"worker", func(value *guestInitEnvironment) {
			value.runWorker = func(*os.File, *os.File, []string, uint32, uint32) error { return errors.New("worker") }
		}},
		{"reboot", func(value *guestInitEnvironment) { value.reboot = func() error { return errors.New("reboot") } }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := guestInitFixture(t, valid)
			test.mutate(&environment)
			if code := runGuestInit(nil, &bytes.Buffer{}, environment); code != 1 {
				t.Fatalf("failure stage accepted code=%d", code)
			}
		})
	}
}

func TestGuestDevtmpfsMountProvidesKernelDeviceNodes(t *testing.T) {
	target := filepath.Join(t.TempDir(), "dev")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	err := unix.Mount("devtmpfs", target, "devtmpfs", uintptr(unix.MS_NOSUID|unix.MS_NOEXEC), "mode=0755")
	if os.Geteuid() != 0 {
		if err == nil {
			_ = unix.Unmount(target, 0)
			t.Fatal("unprivileged process mounted the guest device filesystem")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			_ = unix.Unmount(target, 0)
		}
	})
	info, err := os.Stat(filepath.Join(target, "null"))
	if err != nil || info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice == 0 {
		t.Fatalf("devtmpfs did not populate a character device: mode=%v err=%v", infoMode(info), err)
	}
	if err := unix.Unmount(target, 0); err != nil {
		t.Fatal(err)
	}
	mounted = false
}

func infoMode(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode()
}

func TestGuestWorkerStateMachineWithoutSubprocess(t *testing.T) {
	input := []byte("guest-worker-fixture")
	arguments := []string{strconv.Itoa(len(input)), fixtureDigest(input), strings.Repeat("a", 64),
		"gate-0123456789abcdef", "10001", "10002"}
	var stdout, stderr bytes.Buffer
	if code := runGuestWorkerWithEnvironment(arguments, &stdout, &stderr, 10001, 10002,
		bytes.NewReader(input), func() (bool, bool) { return true, true }); code != 0 {
		t.Fatalf("worker success code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), guestReceiptMarker) {
		t.Fatalf("worker receipt missing: %s", stdout.String())
	}
	mutations := []struct {
		name   string
		args   []string
		uid    int
		gid    int
		input  []byte
		stdout io.Writer
	}{
		{"arity", arguments[:5], 10001, 10002, input, &bytes.Buffer{}},
		{"root", arguments, 0, 10002, input, &bytes.Buffer{}},
		{"limits", append([]string{"0"}, arguments[1:]...), 10001, 10002, input, &bytes.Buffer{}},
		{"identity", arguments, 10003, 10002, input, &bytes.Buffer{}},
		{"truncated", arguments, 10001, 10002, input[:3], &bytes.Buffer{}},
		{"digest", arguments, 10001, 10002, []byte("guest-worker-mutation"), &bytes.Buffer{}},
		{"writer", arguments, 10001, 10002, input, failingWriter{}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if code := runGuestWorkerWithEnvironment(mutation.args, mutation.stdout, &bytes.Buffer{}, mutation.uid,
				mutation.gid, bytes.NewReader(mutation.input), func() (bool, bool) { return true, true }); code != 1 {
				t.Fatalf("worker mutation accepted code=%d", code)
			}
		})
	}
}

func TestGuestNetworkEvidenceMutations(t *testing.T) {
	loopback := []net.Interface{{Name: "lo", Flags: net.FlagLoopback}}
	emptyRoutes := func(string) ([]byte, error) { return []byte("Iface Destination\n"), nil }
	if only, noDefault := inspectGuestNetworkWithEnvironment(loopback, emptyRoutes); !only || !noDefault {
		t.Fatalf("empty loopback topology only=%v noDefault=%v", only, noDefault)
	}
	if only, noDefault := inspectGuestNetworkWithEnvironment([]net.Interface{{Name: "eth0"}}, emptyRoutes); only || noDefault {
		t.Fatal("non-loopback interface accepted")
	}
	if only, noDefault := inspectGuestNetworkWithEnvironment(loopback, func(string) ([]byte, error) { return nil, errors.New("read") }); !only || noDefault {
		t.Fatal("missing route authority accepted")
	}
	ipv4 := func(path string) ([]byte, error) {
		if strings.HasSuffix(path, "/route") {
			return []byte("Iface Destination\nlo 00000000\n"), nil
		}
		return []byte(""), nil
	}
	if only, noDefault := inspectGuestNetworkWithEnvironment(loopback, ipv4); !only || noDefault {
		t.Fatal("IPv4 default route accepted")
	}
	ipv6Line := strings.Repeat("0", 32) + " 00 " + strings.Repeat("1", 32) + " 00 " + strings.Repeat("2", 32) + " 00000000 00 00 00000000\n"
	ipv6 := func(path string) ([]byte, error) {
		if strings.Contains(path, "ipv6") {
			return []byte(ipv6Line), nil
		}
		return []byte("Iface Destination\n"), nil
	}
	if only, noDefault := inspectGuestNetworkWithEnvironment(loopback, ipv6); !only || noDefault {
		t.Fatal("IPv6 default route accepted")
	}
}

func TestReleaseGateParserAndDurableReceiptFailClosed(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runReleaseGate(t.Context(), nil, &stdout, &stderr); code != 2 {
		t.Fatalf("empty Gate arguments code=%d", code)
	}
	if os.Geteuid() != 0 {
		path := filepath.Join(t.TempDir(), "gate.json")
		if err := writeDurableReceipt(path, map[string]any{"schema": "fixture/v1", "ok": true}); err == nil {
			t.Fatal("unprivileged process wrote a supposedly root-owned Gate receipt")
		}
		return
	}
	root, err := os.MkdirTemp("/var/lib", "vane-gate-test-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	receipt := filepath.Join(root, "gate.json")
	value := map[string]any{"schema": "fixture/v1", "ok": true}
	if err := writeDurableReceipt(receipt, value); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(receipt); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("durable receipt mode=%v", info.Mode().Perm())
	}
	if err := writeDurableReceipt(receipt, value); err == nil {
		t.Fatal("durable receipt was overwritten")
	}
	var decoded map[string]any
	if err := readExactJSON(receipt, 1024, &decoded); err != nil || decoded["ok"] != true {
		t.Fatalf("durable receipt decode=%v err=%v", decoded, err)
	}
	duplicate := filepath.Join(root, "duplicate.json")
	if err := os.WriteFile(duplicate, []byte(`{"ok":true,"ok":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := readExactJSON(duplicate, 1024, &decoded); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate Gate JSON err=%v", err)
	}
	if digestFile(receipt) == "" || sizeFile(receipt) <= 0 {
		t.Fatal("durable receipt digest/size evidence is missing")
	}
	payload, _ := json.Marshal(value)
	if !bytes.Contains(payload, []byte(`"ok":true`)) {
		t.Fatal("fixture canonicalization failed")
	}
}

func TestReleaseGateCgroupAuthorityIsExactAndReaperParserIsClosed(t *testing.T) {
	revision := strings.Repeat("a", 40)
	want := "system.slice/vane-firecracker-gate-" + revision + ".service"
	parent, err := parseExactGateCgroup([]byte("0::/"+want+"\n"), revision, false)
	if err != nil || parent != want {
		t.Fatalf("exact cgroup parent=%q err=%v", parent, err)
	}
	for _, payload := range []string{
		"0::/system.slice/another.service\n",
		"1:name=systemd:/" + want + "\n",
		"0::/" + want + "\n0::/" + want + "\n",
	} {
		if _, err := parseExactGateCgroup([]byte(payload), revision, false); err == nil {
			t.Fatalf("ambiguous cgroup authority accepted: %q", payload)
		}
	}
	if parent, err := parseExactGateCgroup([]byte("0::/"+want+"/.control\n"), revision, true); err != nil || parent != want {
		t.Fatalf("exact systemd control cgroup parent=%q err=%v", parent, err)
	}
	if _, err := parseExactGateCgroup([]byte("0::/"+want+"/.control\n"), revision, false); err == nil {
		t.Fatal("main Gate process accepted the systemd control cgroup")
	}
	if err := validateGateCgroupLimits("100000 100000", "268435456", "64"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	parentPath := filepath.Join(root, filepath.FromSlash(want))
	if err := os.MkdirAll(parentPath, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"cpu.max": "100000 100000\n", "memory.max": "268435456\n", "pids.max": "64\n"} {
		if err := os.WriteFile(filepath.Join(parentPath, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyGateCgroupLimitsAt(root, want); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(parentPath, "pids.max")); err != nil {
		t.Fatal(err)
	}
	if err := verifyGateCgroupLimitsAt(root, want); err == nil {
		t.Fatal("missing cgroup limit accepted")
	}
	for _, mutation := range [][3]string{
		{"max 100000", "268435456", "64"},
		{"100000 100000", "max", "64"},
		{"100000 100000", "268435456", "32"},
	} {
		if err := validateGateCgroupLimits(mutation[0], mutation[1], mutation[2]); err == nil {
			t.Fatalf("mutated Gate cgroup limit accepted: %v", mutation)
		}
	}
	var stderr bytes.Buffer
	if code := runReleaseGateReap([]string{"--sha", "not-a-revision"}, &stderr); code != 2 {
		t.Fatalf("invalid reaper arguments code=%d", code)
	}
	stderr.Reset()
	if code := runReleaseGate(t.Context(), []string{"--sha", revision, "--receipt", "/tmp/gate.json"}, &bytes.Buffer{}, &stderr); os.Geteuid() != 0 && code != 1 {
		t.Fatalf("non-root release Gate code=%d", code)
	}
	stderr.Reset()
	if code := runReleaseGateReap([]string{"--sha", revision}, &stderr); os.Geteuid() != 0 && code != 1 {
		t.Fatalf("non-root release Gate reaper code=%d", code)
	}
	environment := productionReleaseGateEnvironment()
	if environment.releaseRoot != "/opt/vane/releases" || environment.currentPath != "/opt/vane/current" ||
		environment.random == nil || environment.newBackend == nil || environment.writeReceipt == nil {
		t.Fatal("production release Gate environment is incomplete")
	}
}

func TestReleaseGateCLIStateMachinesWithInjectedAuthority(t *testing.T) {
	revision := strings.Repeat("e", 40)
	arguments := []string{"--sha", revision, "--receipt", "/tmp/gate.json"}
	execute := func(context.Context, string, string) (map[string]any, error) { return map[string]any{"ok": true}, nil }
	var stdout, stderr bytes.Buffer
	if code := runReleaseGateWithAuthority(t.Context(), arguments, &stdout, &stderr, 0, execute); code != 0 {
		t.Fatalf("release CLI success code=%d stderr=%s", code, stderr.String())
	}
	if code := runReleaseGateWithAuthority(t.Context(), arguments, &bytes.Buffer{}, &bytes.Buffer{}, 0,
		func(context.Context, string, string) (map[string]any, error) { return nil, errors.New("execute") }); code != 1 {
		t.Fatalf("release CLI execution failure code=%d", code)
	}
	if code := runReleaseGateWithAuthority(t.Context(), arguments, failingWriter{}, &bytes.Buffer{}, 0, execute); code != 1 {
		t.Fatalf("release CLI writer failure code=%d", code)
	}
	if code := runReleaseGateReapWithAuthority([]string{"--sha", revision}, &bytes.Buffer{}, 0,
		func(string, bool) (string, error) { return "system.slice/fixture", nil }, func(string) error { return nil }); code != 0 {
		t.Fatalf("reaper success code=%d", code)
	}
	if code := runReleaseGateReapWithAuthority([]string{"--sha", revision}, &bytes.Buffer{}, 0,
		func(string, bool) (string, error) { return "", errors.New("cgroup") }, func(string) error { return nil }); code != 1 {
		t.Fatalf("reaper cgroup failure code=%d", code)
	}
	if code := runReleaseGateReapWithAuthority([]string{"--sha", revision}, &bytes.Buffer{}, 0,
		func(string, bool) (string, error) { return "system.slice/fixture", nil }, func(string) error { return errors.New("reap") }); code != 1 {
		t.Fatalf("reaper cleanup failure code=%d", code)
	}
	want := []byte("0::/system.slice/" + gateUnitName(revision) + "\n")
	if parent, err := exactGateCgroupParentWithReader(revision, false, func() ([]byte, error) { return want, nil }); err != nil || parent == "" {
		t.Fatalf("reader cgroup parent=%q err=%v", parent, err)
	}
	for _, read := range []func() ([]byte, error){
		func() ([]byte, error) { return nil, errors.New("read") },
		func() ([]byte, error) { return make([]byte, (64<<10)+1), nil },
	} {
		if _, err := exactGateCgroupParentWithReader(revision, false, read); err == nil {
			t.Fatal("invalid cgroup reader authority accepted")
		}
	}
}

func TestReleaseGateNetNSPreflightStateMachine(t *testing.T) {
	revision := strings.Repeat("f", 40)
	arguments := []string{"--sha", revision}
	wantParent := "system.slice/" + gateUnitName(revision)
	newFixture := func(t *testing.T) (string, func(string, bool) (string, error), func(string) string,
		func(string) error, func(string) (string, func() error, error), func(string) error) {
		t.Helper()
		root := t.TempDir()
		return root,
			func(string, bool) (string, error) { return wantParent, nil },
			func(string) string { return root },
			func(string) error { return nil },
			func(parent string) (string, func() error, error) {
				path := filepath.Join(parent, "empty-netns-fixture")
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					return "", nil, err
				}
				return path, func() error { return os.Remove(path) }, nil
			},
			func(string) error { return nil }
	}
	_, cgroup, runtimeRoot, requireDirectory, createNetNS, inspectNetNS := newFixture(t)
	blockedInternetFamilies := func() error { return nil }
	var stdout, stderr bytes.Buffer
	if code := runReleaseGateNetNSPreflightWithAuthority(arguments, &stdout, &stderr, 0, cgroup,
		runtimeRoot, requireDirectory, blockedInternetFamilies, createNetNS, inspectNetNS); code != 0 ||
		!strings.Contains(stdout.String(), `"af_netlink":true`) || !strings.Contains(stdout.String(), `"af_inet":false`) ||
		!strings.Contains(stdout.String(), `"af_inet6":false`) {
		t.Fatalf("netns preflight success code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if code := runReleaseGateNetNSPreflightWithAuthority(nil, &bytes.Buffer{}, &bytes.Buffer{}, 0, cgroup,
		runtimeRoot, requireDirectory, blockedInternetFamilies, createNetNS, inspectNetNS); code != 2 {
		t.Fatalf("empty netns preflight arguments code=%d", code)
	}
	if code := runReleaseGateNetNSPreflightWithAuthority(arguments, &bytes.Buffer{}, &bytes.Buffer{}, 1, cgroup,
		runtimeRoot, requireDirectory, blockedInternetFamilies, createNetNS, inspectNetNS); code != 1 {
		t.Fatalf("unprivileged netns preflight code=%d", code)
	}

	mutations := []struct {
		name    string
		cgroup  func(string, bool) (string, error)
		require func(string) error
		probe   func() error
		create  func(string) (string, func() error, error)
		inspect func(string) error
	}{
		{"cgroup-error", func(string, bool) (string, error) { return "", errors.New("cgroup") }, requireDirectory, blockedInternetFamilies, createNetNS, inspectNetNS},
		{"cgroup-mismatch", func(string, bool) (string, error) { return "system.slice/other.service", nil }, requireDirectory, blockedInternetFamilies, createNetNS, inspectNetNS},
		{"runtime", cgroup, func(string) error { return errors.New("runtime") }, blockedInternetFamilies, createNetNS, inspectNetNS},
		{"internet-family", cgroup, requireDirectory, func() error { return errors.New("AF_INET unexpectedly available") }, createNetNS, inspectNetNS},
		{"create", cgroup, requireDirectory, blockedInternetFamilies, func(string) (string, func() error, error) { return "", nil, errors.New("create") }, inspectNetNS},
		{"inspect", cgroup, requireDirectory, blockedInternetFamilies, createNetNS, func(string) error { return errors.New("inspect") }},
		{"cleanup", cgroup, requireDirectory, blockedInternetFamilies, func(parent string) (string, func() error, error) {
			path := filepath.Join(parent, "empty-netns-fixture")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				return "", nil, err
			}
			return path, func() error { return errors.Join(os.Remove(path), errors.New("cleanup")) }, nil
		}, inspectNetNS},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			_, _, mutatedRoot, _, _, _ := newFixture(t)
			if code := runReleaseGateNetNSPreflightWithAuthority(arguments, &bytes.Buffer{}, &bytes.Buffer{}, 0,
				mutation.cgroup, mutatedRoot, mutation.require, mutation.probe, mutation.create, mutation.inspect); code != 1 {
				t.Fatalf("netns preflight mutation accepted code=%d", code)
			}
		})
	}
}

func TestGateInternetFamilyProbeRejectsOrdinaryHost(t *testing.T) {
	if err := probeGateInternetFamiliesBlocked(); err == nil {
		t.Fatal("ordinary host with internet address families was reported as blocked")
	}
}

func TestGateInternetFamilyProbeCallsBothFamiliesAndFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		refusals   map[int]error
		wantCalls  []int
		wantClosed []int
		wantError  bool
	}{
		{"both-blocked", map[int]error{unix.AF_INET: unix.EAFNOSUPPORT, unix.AF_INET6: unix.EPERM}, []int{unix.AF_INET, unix.AF_INET6}, nil, false},
		{"inet-available", map[int]error{unix.AF_INET6: unix.EAFNOSUPPORT}, []int{unix.AF_INET}, []int{100 + unix.AF_INET}, true},
		{"inet-ambiguous", map[int]error{unix.AF_INET: unix.EINVAL}, []int{unix.AF_INET}, nil, true},
		{"inet6-available", map[int]error{unix.AF_INET: unix.EAFNOSUPPORT}, []int{unix.AF_INET, unix.AF_INET6}, []int{100 + unix.AF_INET6}, true},
		{"inet6-ambiguous", map[int]error{unix.AF_INET: unix.EPERM, unix.AF_INET6: unix.EINVAL}, []int{unix.AF_INET, unix.AF_INET6}, nil, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls, closed []int
			err := probeGateInternetFamiliesBlockedWithSocket(
				func(domain, socketType, protocol int) (int, error) {
					if socketType != unix.SOCK_STREAM|unix.SOCK_CLOEXEC || protocol != 0 {
						return -1, errors.New("socket shape drifted")
					}
					calls = append(calls, domain)
					if refusal, exists := test.refusals[domain]; exists {
						return -1, refusal
					}
					return 100 + domain, nil
				},
				func(descriptor int) error {
					closed = append(closed, descriptor)
					return nil
				},
			)
			if (err != nil) != test.wantError || !slices.Equal(calls, test.wantCalls) || !slices.Equal(closed, test.wantClosed) {
				t.Fatalf("probe err=%v calls=%v closed=%v, want error=%t calls=%v closed=%v",
					err, calls, closed, test.wantError, test.wantCalls, test.wantClosed)
			}
		})
	}
}

func TestProductionNetNSPreflightWiresTheRealSocketProbe(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "release_gate.go", releaseGateProductionSource, 0)
	if err != nil {
		t.Fatal(err)
	}
	functions := make(map[string]*ast.FuncDecl)
	for _, declaration := range parsed.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			functions[function.Name.Name] = function
		}
	}
	requireSingleReturnCall := func(name, target string) *ast.CallExpr {
		t.Helper()
		function := functions[name]
		if function == nil || function.Body == nil || len(function.Body.List) != 1 {
			t.Fatalf("%s must remain a single production return binding", name)
		}
		statement, ok := function.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(statement.Results) != 1 {
			t.Fatalf("%s must return exactly one production call", name)
		}
		call, ok := statement.Results[0].(*ast.CallExpr)
		if !ok {
			t.Fatalf("%s must return a production call", name)
		}
		callee, named := call.Fun.(*ast.Ident)
		if !named || callee.Name != target {
			t.Fatalf("%s must call %s directly", name, target)
		}
		return call
	}
	wrapper := requireSingleReturnCall("runReleaseGateNetNSPreflight", "runReleaseGateNetNSPreflightWithAuthority")
	if len(wrapper.Args) != 10 {
		t.Fatalf("production preflight authority argument count=%d", len(wrapper.Args))
	}
	probe, ok := wrapper.Args[7].(*ast.Ident)
	if !ok || probe.Name != "probeGateInternetFamiliesBlocked" {
		t.Fatal("production preflight is not wired to the real internet-family probe")
	}
	realProbe := requireSingleReturnCall("probeGateInternetFamiliesBlocked", "probeGateInternetFamiliesBlockedWithSocket")
	if len(realProbe.Args) != 2 {
		t.Fatalf("real socket probe argument count=%d", len(realProbe.Args))
	}
	for index, want := range []string{"Socket", "Close"} {
		selector, ok := realProbe.Args[index].(*ast.SelectorExpr)
		if !ok {
			t.Fatalf("real socket probe argument %d is not unix.%s", index, want)
		}
		packageName, packageOK := selector.X.(*ast.Ident)
		if !packageOK || packageName.Name != "unix" || selector.Sel.Name != want {
			t.Fatalf("real socket probe argument %d is not unix.%s", index, want)
		}
	}
}

func TestReleaseGateFileHelpersAndDirectoryAuthority(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := requireDirectoryChain(child, root, uint32(os.Geteuid())); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(child, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := requireDirectoryChain(child, root, uint32(os.Geteuid())); err == nil {
		t.Fatal("writable directory authority accepted")
	}
	if err := os.Chmod(child, 0o700); err != nil {
		t.Fatal(err)
	}
	receipt := filepath.Join(child, "durable.json")
	if err := writeDurableReceiptWithAuthority(receipt, map[string]any{"ok": true}, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := writeDurableReceiptWithAuthority(receipt, map[string]any{"ok": true}, func(string) error { return nil }); err == nil {
		t.Fatal("durable receipt overwrite accepted")
	}
	var value map[string]any
	if err := readExactJSON(receipt, 8, &value); err == nil {
		t.Fatal("oversized JSON accepted")
	}
	trailing := filepath.Join(child, "trailing.json")
	if err := os.WriteFile(trailing, []byte("{\"ok\":true} trailing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := readExactJSON(trailing, 1024, &value); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if digestFile(filepath.Join(child, "missing")) != "" || sizeFile(filepath.Join(child, "missing")) != -1 || sizeFile(child) != -1 {
		t.Fatal("missing/non-regular file evidence accepted")
	}
}
