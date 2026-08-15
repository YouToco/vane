package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrDarkFoundation = errors.New("sandbox Firecracker execution remains dark")

type ArtifactPin struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type FirecrackerConfig struct {
	Firecracker        ArtifactPin            `json:"firecracker"`
	Jailer             ArtifactPin            `json:"jailer"`
	Kernel             ArtifactPin            `json:"kernel"`
	RootFS             ArtifactPin            `json:"rootfs"`
	CodeImages         map[string]ArtifactPin `json:"code_images"`
	NetNSPath          string                 `json:"empty_netns_path"`
	FirecrackerVersion string                 `json:"firecracker_version"`
	JailerVersion      string                 `json:"jailer_version"`
	WorkRoot           string                 `json:"work_root"`
	JailerUIDStart     int                    `json:"jailer_uid_start"`
	JailerGIDStart     int                    `json:"jailer_gid_start"`
	IsolationSlots     int                    `json:"isolation_slots"`
	Production         bool                   `json:"production"`
	darkLaunchForTest  bool
	netNSVerifier      func(string) error
}

type LaunchPlan struct {
	InvocationID string
	JailerID     string
	Executable   string
	Arguments    []string
	WorkDir      string
	OutputLimit  int64
}

type Launcher interface {
	Run(context.Context, LaunchPlan) ([]byte, error)
}

type FirecrackerBackend struct {
	config   FirecrackerConfig
	launcher Launcher
	scrub    func(string) error
	mu       sync.Mutex
	free     []int
}

func NewFirecrackerBackend(config FirecrackerConfig, launcher Launcher) (*FirecrackerBackend, error) {
	if launcher == nil {
		launcher = execLauncher{}
	}
	if config.IsolationSlots < 1 || config.IsolationSlots > 1024 ||
		config.JailerUIDStart <= 0 || config.JailerGIDStart <= 0 {
		return nil, errors.New("sandbox jailer identity pool is invalid")
	}
	free := make([]int, config.IsolationSlots)
	for i := range free {
		free[i] = i
	}
	return &FirecrackerBackend{config: config, launcher: launcher, scrub: scrubTree, free: free}, nil
}

func (b *FirecrackerBackend) Preflight(ctx context.Context) error {
	if runtime.GOOS != "linux" {
		return errors.New("Firecracker sandbox requires Linux")
	}
	if err := inspectKVM("/dev/kvm"); err != nil {
		return err
	}
	for label, pin := range map[string]ArtifactPin{
		"firecracker": b.config.Firecracker, "jailer": b.config.Jailer,
		"kernel": b.config.Kernel, "rootfs": b.config.RootFS,
	} {
		if err := verifyPinnedArtifact(label, pin, b.config.Production); err != nil {
			return err
		}
	}
	if len(b.config.CodeImages) == 0 {
		return errors.New("sandbox code image registry is empty")
	}
	for binding, pin := range b.config.CodeImages {
		capabilityID, version, ok := strings.Cut(binding, "@")
		if !ok || !safeID.MatchString(capabilityID) || requireSHA256("code version", version) != nil {
			return fmt.Errorf("sandbox code image binding %q is invalid", binding)
		}
		if err := verifyPinnedArtifact("code image "+binding, pin, b.config.Production); err != nil {
			return err
		}
	}
	verifier := b.config.netNSVerifier
	if verifier == nil {
		verifier = inspectNetworkNamespace
	}
	if err := verifyEmptyNetNS(b.config.NetNSPath, b.config.Production, verifier); err != nil {
		return err
	}
	if b.config.Production && (strings.Contains(strings.ToLower(b.config.Firecracker.Path), "debug") ||
		strings.Contains(strings.ToLower(b.config.Jailer.Path), "debug")) {
		return errors.New("debug Firecracker artifacts are forbidden in production")
	}
	if err := verifyTrustedDirectory(b.config.WorkRoot, b.config.Production); err != nil {
		return fmt.Errorf("sandbox work root: %w", err)
	}
	if b.config.FirecrackerVersion != b.config.JailerVersion {
		return errors.New("firecracker and jailer must use the same approved release")
	}
	if err := verifyVersion(ctx, b.config.Firecracker.Path, "Firecracker", b.config.FirecrackerVersion); err != nil {
		return fmt.Errorf("firecracker version: %w", err)
	}
	if err := verifyVersion(ctx, b.config.Jailer.Path, "Jailer", b.config.JailerVersion); err != nil {
		return fmt.Errorf("jailer version: %w", err)
	}
	return nil
}

// Run exists for a Linux-only integration harness, but production remains
// fail-closed until the pinned guest I/O protocol is independently reviewed.
func (b *FirecrackerBackend) Run(ctx context.Context, request Request) (Result, error) {
	if !b.config.darkLaunchForTest {
		return Result{Status: "disabled", ErrorCode: "dark_foundation"}, ErrDarkFoundation
	}
	maxInput := len(request.Input)
	if maxInput < 1 {
		maxInput = 1
	}
	if err := request.Validate(maxInput); err != nil {
		return Result{}, err
	}
	slot, err := b.acquireSlot()
	if err != nil {
		return Result{}, err
	}
	defer b.releaseSlot(slot)
	plan, cleanup, err := b.buildPlan(request, slot)
	if err != nil {
		return Result{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(request.Policy.WallTimeoutMS)*time.Millisecond)
	defer cancel()
	output, err := b.launcher.Run(runCtx, plan)
	cleanupErr := cleanup()
	if cleanupErr != nil {
		return Result{}, errors.Join(err, fmt.Errorf("scrub sandbox invocation: %w", cleanupErr))
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return Result{Status: "killed", ErrorCode: "wall_timeout"}, context.DeadlineExceeded
	}
	if int64(len(output)) > request.Policy.OutputBytesMax {
		return Result{Status: "killed", ErrorCode: "output_limit"}, ErrOutputLimit
	}
	return Result{Output: output}, err
}

func (b *FirecrackerBackend) buildPlan(request Request, slot int) (LaunchPlan, func() error, error) {
	codePin, ok := b.config.CodeImages[request.CapabilityID+"@"+request.CapabilityVersion]
	if !ok {
		return LaunchPlan{}, func() error { return nil },
			errors.New("capability version has no exact trusted code image")
	}
	runRoot, err := os.MkdirTemp(b.config.WorkRoot, "invocation-")
	if err != nil {
		return LaunchPlan{}, func() error { return nil }, fmt.Errorf("create sandbox invocation root: %w", err)
	}
	cleanup := func() error { return b.scrub(runRoot) }
	if err := os.Chmod(runRoot, 0o700); err != nil {
		return LaunchPlan{}, func() error { return nil }, errors.Join(err, cleanup())
	}
	id := jailerID(request)
	chrootBase := filepath.Join(runRoot, "jailer")
	jailRoot := filepath.Join(chrootBase, filepath.Base(b.config.Firecracker.Path), id, "root")
	if err := os.MkdirAll(jailRoot, 0o700); err != nil {
		return LaunchPlan{}, func() error { return nil }, errors.Join(err, cleanup())
	}
	for name, pin := range map[string]ArtifactPin{
		"vmlinux": b.config.Kernel, "rootfs.ext4": b.config.RootFS, "code.ext4": codePin,
	} {
		if err := copyPinnedFile(pin, filepath.Join(jailRoot, name)); err != nil {
			return LaunchPlan{}, func() error { return nil }, errors.Join(err, cleanup())
		}
	}
	vmConfig := map[string]any{
		"boot-source": map[string]any{
			"kernel_image_path": "/vmlinux",
			"boot_args": fmt.Sprintf("ro panic=1 reboot=k nomodules ipv6.disable=1 init=/sbin/vane-sandbox-init vane.tmpfs_bytes=%d vane.uid=%d vane.gid=%d",
				request.Policy.TmpfsBytesMax, request.Policy.GuestUID, request.Policy.GuestGID),
		},
		"drives": []map[string]any{
			{"drive_id": "rootfs", "path_on_host": "/rootfs.ext4", "is_root_device": true, "is_read_only": true},
			{"drive_id": "code", "path_on_host": "/code.ext4", "is_root_device": false, "is_read_only": true},
		},
		"machine-config": map[string]any{"vcpu_count": request.Policy.VCPUCount,
			"mem_size_mib": request.Policy.MemoryMiB, "smt": false},
	}
	payload, err := json.Marshal(vmConfig)
	if err != nil {
		return LaunchPlan{}, func() error { return nil }, errors.Join(err, cleanup())
	}
	configPath := filepath.Join(jailRoot, "config.json")
	if err := os.WriteFile(configPath, payload, 0o400); err != nil {
		return LaunchPlan{}, func() error { return nil }, errors.Join(err, cleanup())
	}
	uid, gid := b.config.JailerUIDStart+slot, b.config.JailerGIDStart+slot
	if os.Geteuid() == 0 {
		if err := filepath.Walk(jailRoot, func(path string, _ os.FileInfo, walkErr error) error {
			if walkErr == nil {
				return os.Chown(path, uid, gid)
			}
			return walkErr
		}); err != nil {
			return LaunchPlan{}, func() error { return nil }, errors.Join(err, cleanup())
		}
	}
	args := []string{"--id", id, "--exec-file", b.config.Firecracker.Path,
		"--uid", fmt.Sprint(uid), "--gid", fmt.Sprint(gid),
		"--chroot-base-dir", chrootBase, "--netns", b.config.NetNSPath,
		"--cgroup-version", "2",
		"--cgroup", fmt.Sprintf("cpu.max=%d %d", request.Policy.CPUQuotaMicros, request.Policy.CPUPeriodMicros),
		"--cgroup", fmt.Sprintf("memory.max=%d", int64(request.Policy.MemoryMiB)<<20),
		"--cgroup", fmt.Sprintf("pids.max=%d", request.Policy.PIDsMax),
		"--resource-limit", fmt.Sprintf("fsize=%d", request.Policy.OutputBytesMax),
		"--resource-limit", "no-file=128", "--new-pid-ns", "--",
		"--api-sock", "/run/firecracker.socket", "--config-file", "/config.json"}
	return LaunchPlan{InvocationID: request.InvocationID, JailerID: id, Executable: b.config.Jailer.Path,
		Arguments: args, WorkDir: runRoot, OutputLimit: request.Policy.OutputBytesMax}, cleanup, nil
}

func (b *FirecrackerBackend) acquireSlot() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.free) == 0 {
		return 0, errors.New("sandbox isolation slots exhausted")
	}
	last := len(b.free) - 1
	slot := b.free[last]
	b.free = b.free[:last]
	return slot, nil
}

func (b *FirecrackerBackend) releaseSlot(slot int) {
	b.mu.Lock()
	b.free = append(b.free, slot)
	b.mu.Unlock()
}

func verifyPinnedArtifact(label string, pin ArtifactPin, production bool) error {
	if !filepath.IsAbs(pin.Path) || requireSHA256(label, pin.SHA256) != nil {
		return fmt.Errorf("%s artifact pin is invalid", label)
	}
	if err := verifyTrustedPath(pin.Path, production); err != nil {
		return fmt.Errorf("%s artifact path: %w", label, err)
	}
	file, err := os.Open(pin.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if !constantDigestEqual(hex.EncodeToString(hash.Sum(nil)), pin.SHA256) {
		return fmt.Errorf("%s artifact digest mismatch", label)
	}
	return nil
}

func verifyTrustedPath(path string, production bool) error {
	clean := filepath.Clean(path)
	for current := clean; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if (current == clean && (info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0)) ||
			(production && (info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0)) {
			return errors.New("artifact is symlink/writable or production parent is writable")
		}
		if production {
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat.Uid != 0 {
				return errors.New("production artifact path is not root-owned")
			}
		}
		if current == string(filepath.Separator) {
			break
		}
	}
	info, err := os.Stat(clean)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("artifact is not a regular file")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		return errors.New("artifact must have exactly one hard link")
	}
	return nil
}

func verifyTrustedDirectory(path string, production bool) error {
	if !filepath.IsAbs(path) {
		return errors.New("path is not absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("directory is missing, symlinked, or writable by group/other")
	}
	if production {
		if err := verifyProtectedDirectoryChain(path, 0); err != nil {
			return err
		}
	}
	return nil
}

func verifyProtectedDirectoryChain(path string, expectedUID uint32) error {
	return verifyDirectoryChainUntil(path, string(filepath.Separator), expectedUID)
}

func verifyDirectoryChainUntil(path, stop string, expectedUID uint32) error {
	cleanStop := filepath.Clean(stop)
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != expectedUID || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("directory ancestor %q is not owned/protected exactly (mode=%#o)",
				current, info.Mode().Perm())
		}
		if current == cleanStop {
			return nil
		}
		if current == string(filepath.Separator) {
			return errors.New("directory stop is not an ancestor")
		}
	}
}

func verifyEmptyNetNS(path string, production bool, verifier func(string) error) error {
	if err := verifyTrustedPath(path, production); err != nil {
		return fmt.Errorf("sandbox empty netns: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() != 0 || info.Mode().Perm() != 0o600 {
		return errors.New("sandbox empty netns must be an exact 0600 empty mount point")
	}
	if verifier == nil {
		return errors.New("sandbox netns verifier is missing")
	}
	if err := verifier(path); err != nil {
		return fmt.Errorf("sandbox netns topology is not isolated: %w", err)
	}
	return nil
}

func copyPinnedFile(pin ArtifactPin, destination string) error {
	source, err := os.Open(pin.Path)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(target, source)
	closeErr := target.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return verifyPinnedArtifact("copied", ArtifactPin{Path: destination, SHA256: pin.SHA256}, false)
}

func scrubTree(path string) error {
	var cleanupErr error
	walkErr := filepath.Walk(path, func(current string, info os.FileInfo, err error) error {
		if err == nil && info != nil {
			cleanupErr = errors.Join(cleanupErr, os.Chmod(current, 0o700))
		}
		return nil
	})
	return errors.Join(cleanupErr, walkErr, os.RemoveAll(path))
}

func jailerID(request Request) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s:%s:%s", request.TenantID,
		request.UserID, request.CapabilityID, request.CapabilityVersion, request.InvocationID)))
	return "vane-" + hex.EncodeToString(sum[:24])
}
