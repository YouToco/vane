package sandbox

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
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
	Firecracker         ArtifactPin            `json:"firecracker"`
	Jailer              ArtifactPin            `json:"jailer"`
	Kernel              ArtifactPin            `json:"kernel"`
	RootFS              ArtifactPin            `json:"rootfs"`
	CodeImages          map[string]ArtifactPin `json:"code_images"`
	NetNSPath           string                 `json:"empty_netns_path"`
	FirecrackerVersion  string                 `json:"firecracker_version"`
	JailerVersion       string                 `json:"jailer_version"`
	WorkRoot            string                 `json:"work_root"`
	JailerUIDStart      int                    `json:"jailer_uid_start"`
	JailerGIDStart      int                    `json:"jailer_gid_start"`
	IsolationSlots      int                    `json:"isolation_slots"`
	Production          bool                   `json:"production"`
	darkLaunchForTest   bool
	netNSVerifier       func(string) error
	reservedUIDs        []uint32
	reservedGIDs        []uint32
	hostIdentitiesBound bool
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
	if err := validateIdentityPool(config); err != nil {
		return nil, err
	}
	if config.Production && !config.hostIdentitiesBound {
		return nil, errors.New("production sandbox host service identities are not bound")
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

// uid_t/gid_t all-ones is reserved as the kernel/API "no identity" sentinel.
const maxHostIdentity = uint64(1<<32 - 2)

func (c *FirecrackerConfig) BindServiceIdentities(vaneServerUID uint32, socketUID, socketGID int) error {
	if vaneServerUID == 0 || uint64(vaneServerUID) > maxHostIdentity || socketUID < 0 || socketGID < 0 ||
		uint64(socketUID) > maxHostIdentity || uint64(socketGID) > maxHostIdentity {
		return errors.New("sandbox host service identity is outside uint32")
	}
	c.reservedUIDs = []uint32{vaneServerUID, uint32(socketUID)}
	c.reservedGIDs = []uint32{uint32(socketGID)}
	c.hostIdentitiesBound = true
	return nil
}

func validateIdentityPool(config FirecrackerConfig) error {
	if config.IsolationSlots < 1 || config.IsolationSlots > 1024 ||
		config.JailerUIDStart <= 0 || config.JailerGIDStart <= 0 {
		return errors.New("sandbox jailer identity pool is invalid")
	}
	slots := uint64(config.IsolationSlots)
	uidStart, gidStart := uint64(config.JailerUIDStart), uint64(config.JailerGIDStart)
	uidEnd, gidEnd := uidStart+slots-1, gidStart+slots-1
	if uidEnd > maxHostIdentity || gidEnd > maxHostIdentity {
		return errors.New("sandbox jailer identity pool exceeds uint32")
	}
	for _, reserved := range config.reservedUIDs {
		if value := uint64(reserved); value >= uidStart && value <= uidEnd {
			return errors.New("sandbox jailer UID pool collides with a service identity")
		}
	}
	for _, reserved := range config.reservedGIDs {
		if value := uint64(reserved); value >= gidStart && value <= gidEnd {
			return errors.New("sandbox jailer GID pool collides with a service identity")
		}
	}
	return nil
}

func (b *FirecrackerBackend) Preflight(ctx context.Context) error {
	if runtime.GOOS != "linux" {
		return errors.New("Firecracker sandbox requires Linux")
	}
	if b.config.Production {
		if err := verifyUnusedIdentityRange(b.config.JailerUIDStart, b.config.IsolationSlots, true); err != nil {
			return err
		}
		if err := verifyUnusedIdentityRange(b.config.JailerGIDStart, b.config.IsolationSlots, false); err != nil {
			return err
		}
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
	if b.config.Production {
		if err := verifyStaticELF(b.config.Firecracker.Path); err != nil {
			return fmt.Errorf("firecracker production artifact: %w", err)
		}
		if err := verifyStaticELF(b.config.Jailer.Path); err != nil {
			return fmt.Errorf("jailer production artifact: %w", err)
		}
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

func verifyStaticELF(path string) error {
	file, err := elf.Open(path)
	if err != nil {
		return fmt.Errorf("parse static ELF: %w", err)
	}
	defer file.Close()
	if file.Type != elf.ET_EXEC {
		return fmt.Errorf("ELF type %s is not a static executable", file.Type)
	}
	for _, program := range file.Progs {
		if program.Type == elf.PT_INTERP {
			return errors.New("dynamic ELF PT_INTERP is forbidden")
		}
	}
	return nil
}

func verifyUnusedIdentityRange(start, slots int, users bool) error {
	for offset := 0; offset < slots; offset++ {
		identity := fmt.Sprint(uint64(start) + uint64(offset))
		if users {
			_, err := user.LookupId(identity)
			if err == nil {
				return fmt.Errorf("sandbox jailer UID %s is already assigned", identity)
			}
			var unknown user.UnknownUserError
			if !errors.As(err, &unknown) {
				return fmt.Errorf("verify sandbox jailer UID %s: %w", identity, err)
			}
			continue
		}
		_, err := user.LookupGroupId(identity)
		if err == nil {
			return fmt.Errorf("sandbox jailer GID %s is already assigned", identity)
		}
		var unknown user.UnknownGroupError
		if !errors.As(err, &unknown) {
			return fmt.Errorf("verify sandbox jailer GID %s: %w", identity, err)
		}
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
	if !filepath.IsAbs(path) {
		return errors.New("trusted artifact path is not absolute")
	}
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
		if filepath.Dir(current) == current {
			return errors.New("trusted artifact ancestor traversal made no progress")
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
	if !filepath.IsAbs(path) || !filepath.IsAbs(stop) {
		return errors.New("directory chain paths must be absolute")
	}
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
		if filepath.Dir(current) == current {
			return errors.New("directory ancestor traversal made no progress")
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
	var directories []string
	walkErr := filepath.WalkDir(path, func(current string, entry os.DirEntry, walkEntryErr error) error {
		if walkEntryErr != nil {
			cleanupErr = errors.Join(cleanupErr, walkEntryErr)
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			cleanupErr = errors.Join(cleanupErr, os.Remove(current))
			return nil
		}
		if entry.IsDir() {
			directories = append(directories, current)
			return nil
		}
		cleanupErr = errors.Join(cleanupErr, os.Remove(current))
		return nil
	})
	for index := len(directories) - 1; index >= 0; index-- {
		current := directories[index]
		info, err := os.Lstat(current)
		if err != nil {
			if !os.IsNotExist(err) {
				cleanupErr = errors.Join(cleanupErr, err)
			}
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			cleanupErr = errors.Join(cleanupErr, os.Remove(current))
			continue
		}
		cleanupErr = errors.Join(cleanupErr, prepareDirectoryForRemoval(current), os.Remove(current))
	}
	return errors.Join(cleanupErr, walkErr)
}

func jailerID(request Request) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s:%s:%s", request.TenantID,
		request.UserID, request.CapabilityID, request.CapabilityVersion, request.InvocationID)))
	return "vane-" + hex.EncodeToString(sum[:24])
}
