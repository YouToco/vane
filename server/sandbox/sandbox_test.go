package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestDigestAndClosedPolicy(t *testing.T) {
	request := testRequest(t, "invocation-a", 1, 2)
	if err := request.Validate(1024); err != nil {
		t.Fatal(err)
	}
	request.PolicyDigest = strings.Repeat("0", 64)
	if err := request.Validate(1024); err == nil {
		t.Fatal("wrong policy digest accepted")
	}
	request = testRequest(t, "invocation-b", 1, 2)
	request.Policy.NetworkDisabled = false
	request, _ = request.Seal()
	if err := request.Validate(1024); err == nil {
		t.Fatal("network-enabled policy accepted")
	}
}

func TestServiceValidatesAuthorityThenAlwaysRemainsDark(t *testing.T) {
	policy := testPolicy()
	digest, _ := policy.Digest()
	service, err := NewService(ServiceConfig{MaxInputBytes: 1024,
		AllowedPolicyDigests: map[string]struct{}{digest: {}}})
	if err != nil {
		t.Fatal(err)
	}
	first := testRequest(t, "same-invocation", 1, 2)
	if result, err := service.Execute(t.Context(), first); !errors.Is(err, ErrDarkFoundation) ||
		result.ErrorCode != "dark_foundation" {
		t.Fatalf("service did not remain dark result=%+v err=%v", result, err)
	}
	replay := testRequest(t, "same-invocation", 9, 10)
	if result, err := service.Execute(t.Context(), replay); !errors.Is(err, ErrDarkFoundation) ||
		result.ErrorCode != "dark_foundation" {
		t.Fatalf("cross-tenant request escaped dark boundary result=%+v err=%v", result, err)
	}
	bad := first
	bad.PolicyDigest = strings.Repeat("0", 64)
	if _, err := service.Execute(t.Context(), bad); err == nil || errors.Is(err, ErrDarkFoundation) {
		t.Fatalf("invalid request reached dark response: %v", err)
	}
	if _, err := NewService(ServiceConfig{MaxInputBytes: 1,
		AllowedPolicyDigests: map[string]struct{}{strings.Repeat("A", 64): {}}}); err == nil {
		t.Fatal("uppercase policy digest accepted")
	}
}

func TestDefaultFirecrackerBackendCannotLaunchOrCreateWorkdir(t *testing.T) {
	config := testFirecrackerConfig(t)
	launcher := &recordingLauncher{}
	backend, err := NewFirecrackerBackend(config, launcher)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Run(t.Context(), testRequest(t, "production-dark", 1, 2)); !errors.Is(err, ErrDarkFoundation) {
		t.Fatalf("default backend err=%v", err)
	}
	if launcher.plan.Executable != "" {
		t.Fatalf("dark backend called launcher: %+v", launcher.plan)
	}
	entries, err := os.ReadDir(config.WorkRoot)
	if err != nil || len(entries) != 0 {
		t.Fatalf("dark backend created invocation work entries=%v err=%v", entries, err)
	}
}

func TestProductionCallersCannotReferenceDarkSandbox(t *testing.T) {
	_, sourcePath, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path unavailable")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(sourcePath), "..", ".."))
	for _, relative := range []string{"server/cmd/server", "server/agent", "server/skillpkg", "server/mcpclient"} {
		root := filepath.Join(repositoryRoot, relative)
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".go" {
				return walkErr
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			var forbidden string
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.ImportSpec:
					literal, _ := strconv.Unquote(value.Path.Value)
					if strings.HasSuffix(literal, "/server/sandbox") {
						forbidden = literal
					}
				case *ast.Ident:
					if value.Name == "sandbox" || value.Name == "sandboxd" {
						forbidden = value.Name
					}
				case *ast.BasicLit:
					if value.Kind == token.STRING && strings.Contains(value.Value, "sandboxd") {
						forbidden = "sandboxd"
					}
				}
				return forbidden == ""
			})
			if forbidden != "" {
				return fmt.Errorf("%s references dark sandbox authority %q", path, forbidden)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDaemonRequiresAuthorizedSocketPeer(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "vane-sandbox-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "sandboxd.sock")
	policy := testPolicy()
	digest, _ := policy.Digest()
	service, _ := NewService(ServiceConfig{MaxInputBytes: 1024,
		AllowedPolicyDigests: map[string]struct{}{digest: {}}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	parentInfo, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	parentUID, parentGID, ok := fileOwner(parentInfo)
	if !ok {
		t.Fatal("socket parent owner unavailable")
	}
	daemon := &Daemon{SocketPath: socket, Socket: SocketContract{
		ParentUID: parentUID, ParentGID: parentGID, ParentMode: parentInfo.Mode().Perm(),
		SocketUID: os.Geteuid(), SocketGID: os.Getegid(), SocketMode: 0o660,
		allowUnprivilegedForTest: true,
	}, Service: service, Authorizer: authorizerFunc(
		func(*net.UnixConn) error { return errors.New("wrong uid") })}
	go func() { done <- daemon.Serve(ctx) }()
	waitForSocket(t, socket)
	if _, err := (Client{SocketPath: socket}).Execute(t.Context(),
		testRequest(t, "socket-denied", 1, 2)); err == nil {
		t.Fatal("unauthorized socket peer succeeded")
	}
	if got := publicErrorCode(errors.New("tenant=42 /sensitive/path")); got != "execution_failed" {
		t.Fatalf("internal error was exposed as %q", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDaemonAuthorizedRoundTripDoesNotWaitForEOF(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "vane-sandbox-roundtrip-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	uid, gid, ok := fileOwner(info)
	if !ok {
		t.Fatal("socket parent owner unavailable")
	}
	request := testRequest(t, "authorized-roundtrip", 1, 2)
	service, err := NewService(ServiceConfig{MaxInputBytes: 1024,
		AllowedPolicyDigests: map[string]struct{}{request.PolicyDigest: {}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	socket := filepath.Join(directory, "sandboxd.sock")
	daemon := Daemon{SocketPath: socket, Socket: SocketContract{
		ParentUID: uid, ParentGID: gid, ParentMode: 0o700,
		SocketUID: os.Geteuid(), SocketGID: os.Getegid(), SocketMode: 0o660,
		allowUnprivilegedForTest: true,
	}, Service: service, Authorizer: authorizerFunc(func(*net.UnixConn) error { return nil })}
	go func() { done <- daemon.Serve(ctx) }()
	waitForSocket(t, socket)
	roundtripCtx, roundtripCancel := context.WithTimeout(t.Context(), time.Second)
	defer roundtripCancel()
	result, err := (Client{SocketPath: socket}).Execute(roundtripCtx, request)
	if err == nil || err.Error() != "dark_foundation" || result.Status != "disabled" {
		t.Fatalf("authorized dark roundtrip result=%+v err=%v", result, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWireFrameRejectsOversizeAndTrailingWhitespace(t *testing.T) {
	frame := func(payload []byte) []byte {
		result := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(result[:4], uint32(len(payload)))
		copy(result[4:], payload)
		return result
	}
	var decoded map[string]any
	if err := readWire(bytes.NewReader(frame([]byte(`{"ok":true} `))), &decoded); err == nil {
		t.Fatal("JSON trailing whitespace inside a frame was accepted")
	}
	var oversized [4]byte
	binary.BigEndian.PutUint32(oversized[:], maxWireBytes+1)
	if err := readWire(bytes.NewReader(oversized[:]), &decoded); err == nil {
		t.Fatal("oversized declared frame was accepted")
	}
	if err := writeWire(io.Discard, map[string]string{"payload": strings.Repeat("x", maxWireBytes)}); err == nil {
		t.Fatal("oversized encoded frame was accepted")
	}
}

func TestDaemonExistingSocketAndInodeReplacementFailClosed(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "vane-sandbox-socket-contract-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	info, _ := os.Stat(directory)
	uid, gid, _ := fileOwner(info)
	contract := SocketContract{ParentUID: uid, ParentGID: gid,
		ParentMode: info.Mode().Perm(), SocketUID: os.Geteuid(), SocketGID: os.Getegid(),
		SocketMode: 0o660, allowUnprivilegedForTest: true}
	productionMutation := contract
	productionMutation.allowUnprivilegedForTest = false
	if err := rejectUnsafeSocketPath(filepath.Join(directory, "root-contract.sock"), productionMutation); err == nil {
		t.Fatal("non-root socket authority accepted by production contract")
	}
	policy := testPolicy()
	digest, _ := policy.Digest()
	service, _ := NewService(ServiceConfig{MaxInputBytes: 1024,
		AllowedPolicyDigests: map[string]struct{}{digest: {}}})
	socket := filepath.Join(directory, "sandboxd.sock")
	if err := os.WriteFile(socket, []byte("do-not-delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	daemon := &Daemon{SocketPath: socket, Socket: contract, Service: service,
		Authorizer: authorizerFunc(func(*net.UnixConn) error { return nil })}
	if err := daemon.Serve(t.Context()); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing socket path err=%v", err)
	}
	payload, err := os.ReadFile(socket)
	if err != nil || string(payload) != "do-not-delete" {
		t.Fatalf("existing path was removed/changed payload=%q err=%v", payload, err)
	}
	if err := os.Remove(socket); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	replacer := authorizerFunc(func(*net.UnixConn) error {
		if err := os.Remove(socket); err != nil {
			return err
		}
		if err := os.WriteFile(socket, []byte("replacement"), 0o600); err != nil {
			return err
		}
		return errors.New("reject after replacement")
	})
	daemon.Authorizer = replacer
	go func() { done <- daemon.Serve(ctx) }()
	waitForSocket(t, socket)
	_, _ = (Client{SocketPath: socket}).Execute(t.Context(),
		testRequest(t, "inode-replacement", 1, 2))
	cancel()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "inode changed") {
		t.Fatalf("socket inode replacement cleanup err=%v", err)
	}
	payload, err = os.ReadFile(socket)
	if err != nil || string(payload) != "replacement" {
		t.Fatalf("replacement inode was deleted payload=%q err=%v", payload, err)
	}
}

func TestNetNSMountPointRequiresTopologyInspection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.netns")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := verifyEmptyNetNS(path, false, func(got string) error {
		called = true
		if got != path {
			t.Fatalf("netns verifier path=%q want=%q", got, path)
		}
		return nil
	}); err != nil || !called {
		t.Fatalf("netns topology verifier not enforced called=%t err=%v", called, err)
	}
	if err := verifyEmptyNetNS(path, false, func(string) error {
		return errors.New("eth0/default-route mutation")
	}); err == nil {
		t.Fatal("netns topology failure accepted")
	}
}

func TestRelativeNetNSPathFailsWithoutAncestorLoop(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		done <- verifyEmptyNetNS("relative-empty.netns", false, func(string) error { return nil })
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not absolute") {
			t.Fatalf("relative netns err=%v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("relative netns path caused non-progressing ancestor traversal")
	}
}

func TestArtifactPinsRejectDigestSymlinkAndWritablePath(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "kernel")
	if err := os.WriteFile(path, []byte("kernel"), 0o400); err != nil {
		t.Fatal(err)
	}
	pin := ArtifactPin{Path: path, SHA256: sha256Hex([]byte("kernel"))}
	if err := verifyPinnedArtifact("kernel", pin, false); err != nil {
		t.Fatal(err)
	}
	wrong := pin
	wrong.SHA256 = strings.Repeat("0", 64)
	if err := verifyPinnedArtifact("kernel", wrong, false); err == nil {
		t.Fatal("wrong artifact digest accepted")
	}
	symlink := filepath.Join(directory, "kernel-link")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatal(err)
	}
	if err := verifyPinnedArtifact("kernel", ArtifactPin{Path: symlink, SHA256: pin.SHA256}, false); err == nil {
		t.Fatal("symlink artifact accepted")
	}
	hardlink := filepath.Join(directory, "kernel-hardlink")
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if err := verifyPinnedArtifact("kernel", pin, false); err == nil {
		t.Fatal("hard-linked artifact accepted")
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	if err := verifyPinnedArtifact("kernel", pin, false); err == nil {
		t.Fatal("world-writable artifact accepted")
	}
}

func TestProtectedDirectoryChainRejectsWritableAncestor(t *testing.T) {
	ancestor := t.TempDir()
	if err := os.Chmod(ancestor, 0o700); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(ancestor, "trusted", "work")
	if err := os.MkdirAll(leaf, 0o700); err != nil {
		t.Fatal(err)
	}
	leafInfo, err := os.Stat(leaf)
	if err != nil {
		t.Fatal(err)
	}
	leafUID, _, ok := fileOwner(leafInfo)
	if !ok {
		t.Fatal("directory owner unavailable")
	}
	if err := verifyDirectoryChainUntil(leaf, ancestor, leafUID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(ancestor, "trusted"), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := verifyDirectoryChainUntil(leaf, ancestor, leafUID); err == nil {
		t.Fatal("writable work/socket ancestor accepted")
	}
}

func TestScrubTreeUnlinksSymlinkWithoutTouchingExternalVictim(t *testing.T) {
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim")
	want := []byte("must remain unchanged")
	if err := os.WriteFile(victim, want, 0o400); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "invocation")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(root, "attacker-link")); err != nil {
		t.Fatal(err)
	}
	if err := scrubTree(root); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(victim)
	if err != nil || !bytes.Equal(payload, want) {
		t.Fatalf("external victim content changed payload=%q err=%v", payload, err)
	}
	info, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Fatalf("external victim mode changed mode=%v", info.Mode().Perm())
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("invocation root survived scrub: %v", err)
	}
}

func TestFirecrackerTestHarnessEnforcesTimeoutAndOutputCap(t *testing.T) {
	config := testFirecrackerConfig(t)
	config.darkLaunchForTest = true
	timeoutBackend, _ := NewFirecrackerBackend(config, launcherFunc(
		func(ctx context.Context, _ LaunchPlan) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}))
	timeoutRequest := testRequest(t, "timeout", 1, 2)
	timeoutRequest.Policy.WallTimeoutMS = 10
	timeoutRequest, _ = timeoutRequest.Seal()
	if result, err := timeoutBackend.Run(t.Context(), timeoutRequest); !errors.Is(err, context.DeadlineExceeded) ||
		result.ErrorCode != "wall_timeout" {
		t.Fatalf("timeout result=%+v err=%v", result, err)
	}

	outputBackend, _ := NewFirecrackerBackend(config, launcherFunc(
		func(context.Context, LaunchPlan) ([]byte, error) { return make([]byte, 129), nil }))
	outputRequest := testRequest(t, "output", 1, 2)
	outputRequest.Policy.OutputBytesMax = 128
	outputRequest, _ = outputRequest.Seal()
	if result, err := outputBackend.Run(t.Context(), outputRequest); !errors.Is(err, ErrOutputLimit) ||
		result.ErrorCode != "output_limit" || result.Output != nil {
		t.Fatalf("output result=%+v err=%v", result, err)
	}
}

func TestRouteFixturesPermitOnlyLoopback(t *testing.T) {
	ipv4Loopback := "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 0000007F 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := validateRouteTable(strings.NewReader(ipv4Loopback), false); err != nil {
		t.Fatalf("loopback IPv4 route rejected: %v", err)
	}
	ipv6Loopback := strings.Repeat("0", 31) + "1 80 " + strings.Repeat("0", 32) +
		" 00 " + strings.Repeat("0", 32) + " 00000000 00000000 00000000 00000001 lo\n"
	if err := validateRouteTable(strings.NewReader(ipv6Loopback), true); err != nil {
		t.Fatalf("loopback IPv6 route rejected: %v", err)
	}
	if err := validateRouteTable(strings.NewReader(strings.Replace(ipv4Loopback, "lo ", "eth0 ", 1)), false); err == nil {
		t.Fatal("non-loopback IPv4/default route accepted")
	}
	if err := validateRouteTable(strings.NewReader(strings.Replace(ipv6Loopback, " lo", " eth0", 1)), true); err == nil {
		t.Fatal("non-loopback IPv6 route accepted")
	}
	defaultViaLoopback := strings.Replace(ipv4Loopback, "0000007F", "00000000", 1)
	defaultViaLoopback = strings.Replace(defaultViaLoopback, "000000FF", "00000000", 1)
	if err := validateRouteTable(strings.NewReader(defaultViaLoopback), false); err == nil {
		t.Fatal("IPv4 default route via loopback accepted")
	}
}

func TestJailerIdentityPoolRejectsBoundsAndServiceCollision(t *testing.T) {
	config := testFirecrackerConfig(t)
	config.JailerUIDStart = int(maxHostIdentity)
	config.IsolationSlots = 2
	if _, err := NewFirecrackerBackend(config, &recordingLauncher{}); err == nil {
		t.Fatal("overflowing jailer UID pool accepted")
	}
	config.IsolationSlots = 1
	if _, err := NewFirecrackerBackend(config, &recordingLauncher{}); err != nil {
		t.Fatalf("maximum non-sentinel jailer UID rejected: %v", err)
	}
	config.JailerUIDStart = int(maxHostIdentity) + 1
	if _, err := NewFirecrackerBackend(config, &recordingLauncher{}); err == nil {
		t.Fatal("uid_t all-ones sentinel accepted")
	}
	config = testFirecrackerConfig(t)
	config.Production = true
	if err := config.BindServiceIdentities(uint32(config.JailerUIDStart), 0, config.JailerGIDStart+100); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFirecrackerBackend(config, &recordingLauncher{}); err == nil ||
		!strings.Contains(err.Error(), "collides") {
		t.Fatalf("service UID collision err=%v", err)
	}
	if err := config.BindServiceIdentities(1001, 0, config.JailerGIDStart); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFirecrackerBackend(config, &recordingLauncher{}); err == nil ||
		!strings.Contains(err.Error(), "collides") {
		t.Fatalf("socket GID collision err=%v", err)
	}
	if err := config.BindServiceIdentities(1001, 0, int(maxHostIdentity)+1); err == nil {
		t.Fatal("socket gid_t all-ones sentinel accepted")
	}
	if err := config.BindServiceIdentities(^uint32(0), 0, 1001); err == nil {
		t.Fatal("Vane service uid_t sentinel accepted")
	}
}

func TestAssignedHostIdentityIsRejected(t *testing.T) {
	if err := verifyUnusedIdentityRange(os.Geteuid(), 1, true); err == nil ||
		!strings.Contains(err.Error(), "already assigned") {
		t.Fatalf("assigned current UID err=%v", err)
	}
}

func TestReleaseVersionOutputMustMatchExactly(t *testing.T) {
	if err := validateVersionOutput("Firecracker", "v1.15.1", "Firecracker v1.15.1\n"); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ binary, version, output string }{
		{"Firecracker", "v1.15.1", "Firecracker v1.15.10"},
		{"Firecracker", "v1.15.1", "Firecracker debug v1.15.1"},
		{"Jailer", "v1.15.1", "Firecracker v1.15.1"},
		{"Firecracker", "1.15.1", "Firecracker 1.15.1"},
	} {
		if err := validateVersionOutput(mutation.binary, mutation.version, mutation.output); err == nil {
			t.Fatalf("version mutation accepted: %+v", mutation)
		}
	}
}

func TestFirecrackerPlanHasNoNetworkReadOnlyCgroupsAndCrashCleanup(t *testing.T) {
	config := testFirecrackerConfig(t)
	config.darkLaunchForTest = true
	launcher := &recordingLauncher{err: errors.New("simulated jailer crash")}
	backend, err := NewFirecrackerBackend(config, launcher)
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(t, "crash-cleanup", 1, 2)
	if _, err := backend.Run(t.Context(), request); err == nil {
		t.Fatal("simulated crash was hidden")
	}
	plan := launcher.plan
	for _, required := range []string{"cpu.max=50000 100000", "memory.max=134217728", "pids.max=32"} {
		if !slices.Contains(plan.Arguments, required) {
			t.Errorf("jailer plan missing %q: %v", required, plan.Arguments)
		}
	}
	if len(plan.JailerID) > 64 || !regexp.MustCompile(`^[a-zA-Z0-9-]+$`).MatchString(plan.JailerID) ||
		plan.JailerID == plan.InvocationID {
		t.Fatalf("unsafe jailer id %q", plan.JailerID)
	}
	if argumentAfter(plan.Arguments, "--netns") != config.NetNSPath {
		t.Fatalf("jailer plan omitted pinned empty netns: %v", plan.Arguments)
	}
	configPayload, err := os.ReadFile(launcher.configCopy)
	if err != nil {
		t.Fatal(err)
	}
	var vm map[string]json.RawMessage
	if err := json.Unmarshal(configPayload, &vm); err != nil {
		t.Fatal(err)
	}
	if _, ok := vm["network-interfaces"]; ok {
		t.Fatal("network interface appeared in v1 plan")
	}
	if _, ok := vm["mmds-config"]; ok {
		t.Fatal("metadata service appeared in v1 plan")
	}
	var drives []struct {
		ReadOnly bool `json:"is_read_only"`
	}
	if err := json.Unmarshal(vm["drives"], &drives); err != nil {
		t.Fatal(err)
	}
	if len(drives) != 2 || !drives[0].ReadOnly || !drives[1].ReadOnly {
		t.Fatalf("drives are not closed read-only set: %+v", drives)
	}
	if _, err := os.Stat(plan.WorkDir); !os.IsNotExist(err) {
		t.Fatalf("crashed invocation directory not scrubbed: %v", err)
	}
}

func TestFirecrackerBindsCodeVersionAndReportsScrubFailure(t *testing.T) {
	config := testFirecrackerConfig(t)
	config.darkLaunchForTest = true
	launcher := &recordingLauncher{}
	backend, err := NewFirecrackerBackend(config, launcher)
	if err != nil {
		t.Fatal(err)
	}
	wrong := testRequest(t, "wrong-code", 1, 2)
	wrong.CapabilityVersion = sha256Hex([]byte("another-code"))
	wrong, _ = wrong.Seal()
	if _, err := backend.Run(t.Context(), wrong); err == nil ||
		!strings.Contains(err.Error(), "exact trusted code image") {
		t.Fatalf("unregistered capability version err=%v", err)
	}
	backend.scrub = func(string) error { return errors.New("scrub mutation") }
	if _, err := backend.Run(t.Context(), testRequest(t, "scrub-fails", 1, 2)); err == nil ||
		!strings.Contains(err.Error(), "scrub mutation") {
		t.Fatalf("scrub failure was hidden: %v", err)
	}
}

func TestConcurrentFirecrackerPlansUseDistinctJailerIdentities(t *testing.T) {
	config := testFirecrackerConfig(t)
	config.darkLaunchForTest = true
	config.IsolationSlots = 2
	launcher := &blockingLauncher{entered: make(chan LaunchPlan, 2), release: make(chan struct{})}
	backend, _ := NewFirecrackerBackend(config, launcher)
	var group sync.WaitGroup
	group.Add(2)
	for i, id := range []string{"tenant-one", "tenant-two"} {
		go func(index int, invocation string) {
			defer group.Done()
			_, _ = backend.Run(t.Context(), testRequest(t, invocation, int64(index+1), int64(index+10)))
		}(i, id)
	}
	first, second := <-launcher.entered, <-launcher.entered
	if argumentAfter(first.Arguments, "--uid") == argumentAfter(second.Arguments, "--uid") ||
		first.WorkDir == second.WorkDir {
		t.Fatalf("concurrent invocations shared identity/workdir: %+v %+v", first, second)
	}
	close(launcher.release)
	group.Wait()
}

type authorizerFunc func(*net.UnixConn) error

func (f authorizerFunc) Authorize(conn *net.UnixConn) error { return f(conn) }

type recordingLauncher struct {
	plan       LaunchPlan
	configCopy string
	err        error
}

func (l *recordingLauncher) Run(_ context.Context, plan LaunchPlan) ([]byte, error) {
	l.plan = plan
	source := filepath.Join(plan.WorkDir, "jailer", "firecracker", plan.JailerID, "root", "config.json")
	payload, _ := os.ReadFile(source)
	l.configCopy = filepath.Join(filepath.Dir(plan.WorkDir), plan.InvocationID+"-config-copy.json")
	_ = os.WriteFile(l.configCopy, payload, 0o600)
	return nil, l.err
}

type blockingLauncher struct {
	entered chan LaunchPlan
	release chan struct{}
}

type launcherFunc func(context.Context, LaunchPlan) ([]byte, error)

func (f launcherFunc) Run(ctx context.Context, plan LaunchPlan) ([]byte, error) {
	return f(ctx, plan)
}

func (l *blockingLauncher) Run(_ context.Context, plan LaunchPlan) ([]byte, error) {
	l.entered <- plan
	<-l.release
	return nil, nil
}

func testRequest(t *testing.T, invocation string, tenantID, userID int64) Request {
	t.Helper()
	request := Request{ProtocolVersion: ProtocolVersion, TenantID: tenantID, UserID: userID,
		CapabilityID: "skill.catalog.readonly", CapabilityVersion: sha256Hex([]byte("capability-package")),
		InvocationID: invocation, Policy: testPolicy(), Input: []byte(`{"query":"safe"}`)}
	sealed, err := request.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func testPolicy() Policy {
	return Policy{VCPUCount: 1, MemoryMiB: 128, PIDsMax: 32,
		CPUQuotaMicros: 50000, CPUPeriodMicros: 100000, WallTimeoutMS: 1000,
		OutputBytesMax: 4096, TmpfsBytesMax: 16 << 20, GuestUID: 10001, GuestGID: 10001,
		NetworkDisabled: true, RootReadOnly: true, CodeReadOnly: true}
}

func testFirecrackerConfig(t *testing.T) FirecrackerConfig {
	t.Helper()
	directory := t.TempDir()
	work := filepath.Join(directory, "work")
	if err := os.Mkdir(work, 0o700); err != nil {
		t.Fatal(err)
	}
	pins := make(map[string]ArtifactPin)
	for _, name := range []string{"firecracker", "jailer", "kernel", "rootfs", "code"} {
		path := filepath.Join(directory, name)
		payload := []byte(name)
		if err := os.WriteFile(path, payload, 0o500); err != nil {
			t.Fatal(err)
		}
		pins[name] = ArtifactPin{Path: path, SHA256: sha256Hex(payload)}
	}
	netNS := filepath.Join(directory, "empty.netns")
	if err := os.WriteFile(netNS, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	codeVersion := sha256Hex([]byte("capability-package"))
	return FirecrackerConfig{Firecracker: pins["firecracker"], Jailer: pins["jailer"],
		Kernel: pins["kernel"], RootFS: pins["rootfs"], NetNSPath: netNS,
		CodeImages: map[string]ArtifactPin{"skill.catalog.readonly@" + codeVersion: pins["code"]},
		WorkRoot:   work, JailerUIDStart: 20000, JailerGIDStart: 20000, IsolationSlots: 1}
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("sandbox socket did not appear")
}

func argumentAfter(arguments []string, name string) string {
	for index := range arguments {
		if arguments[index] == name && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	return ""
}
