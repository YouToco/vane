//go:build linux

package sandbox

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExecLauncherTimeoutKillsForkedProcessGroup(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "escaped-child")
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := (execLauncher{}).Run(ctx, LaunchPlan{Executable: "/bin/sh",
		Arguments: []string{"-c", fmt.Sprintf("(sleep 0.4; touch %q) & wait", marker)},
		WorkDir:   t.TempDir(), OutputLimit: 1024})
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatalf("fork timeout did not terminate promptly err=%v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("fork escaped process-group kill: %v", err)
	}
}

func TestExecLauncherOutputCapKillsProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := (execLauncher{}).Run(ctx, LaunchPlan{Executable: "/bin/sh",
		Arguments: []string{"-c", "while :; do printf 0123456789; done"},
		WorkDir:   t.TempDir(), OutputLimit: 128})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("unbounded producer err=%v", err)
	}
}

func TestExecLauncherPidfdKillsSetsidChildOutsideJailerProcessGroup(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "escaped-firecracker")
	pidFile := filepath.Join(directory, "firecracker.pid")
	script := filepath.Join(directory, "jailer-fixture")
	payload := "#!/bin/sh\nsetsid sh -c 'sleep 0.4; touch \"$1\"' sh \"$1\" &\n" +
		"child=$!\numask 077\nprintf %s \"$child\" > \"$2\"\nwait \"$child\"\n"
	if err := os.WriteFile(script, []byte(payload), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	_, err := (execLauncher{}).Run(ctx, LaunchPlan{Executable: script,
		Arguments: []string{marker, pidFile}, WorkDir: directory, OutputLimit: 1024, PIDFile: pidFile,
		pidFileUID: uint32(os.Geteuid())})
	if err == nil {
		t.Fatal("setsid Firecracker fixture ignored the wall deadline")
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("setsid Firecracker fixture escaped pidfd kill: %v", err)
	}
}

func TestJailerPIDFileRequiresRootOwnedCanonicalSingleLinkAuthority(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "firecracker.pid")
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	fd, ready, err := tryOpenJailerPIDFileForUID(path, uint32(os.Geteuid()))
	if err != nil || !ready {
		t.Fatalf("canonical PID authority ready=%v err=%v", ready, err)
	}
	if err := syscall.Close(fd); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tryOpenJailerPIDFileForUID(path, uint32(os.Geteuid())); err == nil {
		t.Fatal("loose PID authority was accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/proc/self/stat", path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tryOpenJailerPIDFileForUID(path, uint32(os.Geteuid())); err == nil {
		t.Fatal("symlink PID authority was accepted")
	}
}

func TestJailerPIDFileRejectsEveryAuthorityMutation(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "firecracker.pid")
	uid := uint32(os.Geteuid())
	if _, ready, err := tryOpenJailerPIDFileForUID(path, uid); err != nil || ready {
		t.Fatalf("missing PID file ready=%v err=%v", ready, err)
	}
	for name, payload := range map[string]string{
		"empty": "", "oversized": strings.Repeat("1", 21), "zero": "0", "spaced": " 2", "newline": "2\n",
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			_, ready, err := tryOpenJailerPIDFileForUID(path, uid)
			if payload == "" {
				if err != nil || ready {
					t.Fatalf("empty PID authority ready=%v err=%v", ready, err)
				}
			} else if err == nil {
				t.Fatalf("mutated PID authority accepted: %q", payload)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(directory, "hardlink")
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tryOpenJailerPIDFileForUID(path, uid); err == nil {
		t.Fatal("multiply linked PID authority accepted")
	}
}

func TestLinuxPlatformPureAuthorityHelpers(t *testing.T) {
	if err := inspectKVM("/dev/null"); err != nil {
		t.Fatalf("accessible character device rejected: %v", err)
	}
	regular := filepath.Join(t.TempDir(), "regular")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := inspectKVM(regular); err == nil {
		t.Fatal("regular file accepted as KVM")
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := prepareDirectoryForRemoval(directory); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory removal preparation mode=%v err=%v", info.Mode().Perm(), err)
	}
	cgroup := filepath.Join(t.TempDir(), "cgroup")
	if err := os.WriteFile(cgroup, []byte("0::/fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if value, err := unifiedCgroupPath(cgroup); err != nil || value != "/fixture" {
		t.Fatalf("unified cgroup=%q err=%v", value, err)
	}
	for _, payload := range []string{"", "1:name=systemd:/fixture\n", "0::/a\n0::/b\n"} {
		if err := os.WriteFile(cgroup, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := unifiedCgroupPath(cgroup); err == nil {
			t.Fatalf("ambiguous cgroup authority accepted: %q", payload)
		}
	}
	for _, unsafe := range []string{"", "/tmp/cgroup.kill", "/sys/fs/cgroup/system.slice/vane-firecracker-gate-short.service/cgroup.kill"} {
		if err := killGateCgroup(unsafe); err == nil {
			t.Fatalf("unsafe cgroup kill accepted: %q", unsafe)
		}
	}
	exactMissing := "/sys/fs/cgroup/system.slice/vane-firecracker-gate-" + strings.Repeat("f", 40) + ".service/cgroup.kill"
	if err := killGateCgroup(exactMissing); err == nil {
		t.Fatal("missing exact cgroup kill file was accepted")
	}
	if _, _, err := tryOpenJailerPIDFile(filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Fatalf("production PID wrapper rejected a missing file: %v", err)
	}
}

func TestJailerSupervisorStateMachineWithoutFirecracker(t *testing.T) {
	done := make(chan struct{})
	close(done)
	if err := superviseJailerChild(t.Context(), done, LaunchPlan{}, 999999, func() {}); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	malformed := filepath.Join(directory, "malformed.pid")
	if err := os.WriteFile(malformed, []byte("not-a-pid"), 0o600); err != nil {
		t.Fatal(err)
	}
	cancelled := false
	if err := superviseJailerChild(t.Context(), make(chan struct{}), LaunchPlan{PIDFile: malformed,
		pidFileUID: uint32(os.Geteuid())}, 999999, func() { cancelled = true }); err == nil || !cancelled {
		t.Fatalf("malformed PID supervisor err=%v cancelled=%v", err, cancelled)
	}
	missing := filepath.Join(directory, "missing.pid")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	started := time.Now()
	if err := superviseJailerChild(ctx, make(chan struct{}), LaunchPlan{PIDFile: missing,
		pidFileUID: uint32(os.Geteuid())}, 999999, func() {}); err == nil || time.Since(started) > 2*time.Second {
		t.Fatalf("missing PID cancellation err=%v", err)
	}
	command := exec.Command("/bin/sh", "-c", "sleep 30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	pidFile := filepath.Join(directory, "child.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(command.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if err := superviseJailerChild(ctx, make(chan struct{}), LaunchPlan{PIDFile: pidFile,
		pidFileUID: uint32(os.Geteuid())}, 999999, func() {}); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("cancelled supervised child exited successfully")
	}
}

func TestVersionProbeHasTimeoutAndOutputCap(t *testing.T) {
	directory := t.TempDir()
	hanging := filepath.Join(directory, "hanging-version")
	if err := os.WriteFile(hanging, []byte("#!/bin/sh\nsleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := verifyVersion(ctx, hanging, "Firecracker", "v1.15.1"); err == nil ||
		!strings.Contains(err.Error(), "timed out") {
		t.Fatalf("hanging version probe err=%v", err)
	}
	large := filepath.Join(directory, "large-version")
	if err := os.WriteFile(large, []byte("#!/bin/sh\nwhile :; do printf 0123456789; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifyVersion(t.Context(), large, "Firecracker", "v1.15.1"); !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("unbounded version output err=%v", err)
	}
}

func TestProductionArtifactRejectsDynamicELF(t *testing.T) {
	if err := verifyStaticELF("/bin/sh"); err == nil {
		t.Fatal("dynamic system shell accepted as static Firecracker artifact")
	}
}

func TestCreateGateNetNSProducesRealLoopbackOnlyMountAndScrubsIt(t *testing.T) {
	if os.Geteuid() != 0 {
		if _, cleanup, err := CreateGateNetNS(t.TempDir()); err == nil {
			if cleanup != nil {
				_ = cleanup()
			}
			t.Fatal("unprivileged process created the release Gate network namespace")
		}
		return
	}
	path, cleanup, err := CreateGateNetNS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cleaned := false
	defer func() {
		if !cleaned {
			_ = cleanup()
		}
	}()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o444 {
		mode := os.FileMode(0)
		if info != nil {
			mode = info.Mode().Perm()
		}
		t.Fatalf("named network namespace mode=%v err=%v", mode, err)
	}
	if err := inspectNetworkNamespace(path); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	cleaned = true
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("named network namespace survived cleanup: %v", err)
	}
}

func TestCreateGateNetNSStateMachineWithFakeSyscalls(t *testing.T) {
	newFixture := func() (gateNetNSOperations, *bool, *bool) {
		removed, unmounted := new(bool), new(bool)
		return gateNetNSOperations{
			openOriginal: func() (int, error) { return 42, nil }, close: func(int) error { return nil },
			createTarget: func(parent string) (string, error) { return filepath.Join(parent, "empty-netns-fixture"), nil },
			unshare:      func() error { return nil }, mount: func(string) error { return nil }, setns: func(int) error { return nil },
			unmount: func(string, int) error { *unmounted = true; return nil },
			remove:  func(string) error { *removed = true; return nil },
		}, removed, unmounted
	}
	operations, removed, unmounted := newFixture()
	path, cleanup, err := createGateNetNSWithOperations("/fixture", operations)
	if err != nil || path != "/fixture/empty-netns-fixture" {
		t.Fatalf("fake namespace path=%q err=%v", path, err)
	}
	if err := cleanup(); err != nil || !*removed || !*unmounted {
		t.Fatalf("fake namespace cleanup removed=%v unmounted=%v err=%v", *removed, *unmounted, err)
	}
	tests := []struct {
		name   string
		mutate func(*gateNetNSOperations)
	}{
		{"open", func(value *gateNetNSOperations) {
			value.openOriginal = func() (int, error) { return -1, errors.New("open") }
		}},
		{"target", func(value *gateNetNSOperations) {
			value.createTarget = func(string) (string, error) { return "", errors.New("target") }
		}},
		{"unshare", func(value *gateNetNSOperations) { value.unshare = func() error { return errors.New("unshare") } }},
		{"mount", func(value *gateNetNSOperations) { value.mount = func(string) error { return errors.New("mount") } }},
		{"restore", func(value *gateNetNSOperations) { value.setns = func(int) error { return errors.New("restore") } }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operations, _, _ := newFixture()
			test.mutate(&operations)
			if _, _, err := createGateNetNSWithOperations("/fixture", operations); err == nil {
				t.Fatal("fake syscall failure accepted")
			}
		})
	}
}

func TestCreateGateNetNSRetiresThreadWhenRestoreFails(t *testing.T) {
	operations, _, _ := func() (gateNetNSOperations, *bool, *bool) {
		removed, unmounted := new(bool), new(bool)
		return gateNetNSOperations{
			openOriginal: func() (int, error) { return 42, nil }, close: func(int) error { return nil },
			createTarget: func(parent string) (string, error) { return filepath.Join(parent, "empty-netns-fixture"), nil },
			unshare:      func() error { return nil }, mount: func(string) error { return nil },
			setns:   func(int) error { return errors.New("restore mutation") },
			unmount: func(string, int) error { *unmounted = true; return nil },
			remove:  func(string) error { *removed = true; return nil },
		}, removed, unmounted
	}()
	locked, unlocked := false, false
	result := createGateNetNSOnDedicatedThread(
		"/fixture",
		func() gateNetNSOperations { return operations },
		func() { locked = true },
		func() { unlocked = true },
	)
	if !locked || unlocked || result.restored || result.err == nil || !strings.Contains(result.err.Error(), "restore mutation") {
		t.Fatalf("failed restore thread state locked=%v unlocked=%v restored=%v err=%v", locked, unlocked, result.restored, result.err)
	}

	operations.setns = func(int) error { return nil }
	locked, unlocked = false, false
	result = createGateNetNSOnDedicatedThread(
		"/fixture",
		func() gateNetNSOperations { return operations },
		func() { locked = true },
		func() { unlocked = true },
	)
	if !locked || !unlocked || !result.restored || result.err != nil {
		t.Fatalf("successful restore thread state locked=%v unlocked=%v restored=%v err=%v", locked, unlocked, result.restored, result.err)
	}
}

func TestNetworkInspectionUsesCallingThreadProcView(t *testing.T) {
	want := [2]string{"/proc/thread-self/net/route", "/proc/thread-self/net/ipv6_route"}
	for index, routeFile := range currentThreadRouteFiles {
		if routeFile.path != want[index] {
			t.Fatalf("route file %d is not bound to the calling thread: %q", index, routeFile.path)
		}
	}
}

func TestGateCrashReaperUnmountsNamespaceAndScrubsRuntimeTree(t *testing.T) {
	if os.Geteuid() != 0 {
		if err := ReapGateRuntime(t.TempDir()); err == nil {
			t.Fatal("noncanonical Gate runtime root was accepted")
		}
		return
	}
	revision := strings.Repeat("b", 40)
	runtimeRoot := filepath.Join("/run", "vane-firecracker-gate-"+revision)
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ReapGateRuntime(runtimeRoot)
		_ = os.RemoveAll(runtimeRoot)
	})
	gateRoot := filepath.Join(runtimeRoot, "firecracker-work")
	workRoot := filepath.Join(gateRoot, "work-crash-fixture")
	if err := os.MkdirAll(workRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestGateCrashNamespaceHelper$")
	command.Env = append(os.Environ(), "VANE_GATE_CRASH_HELPER=1", "VANE_GATE_CRASH_WORK="+workRoot)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "ready\n" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("crash helper readiness=%q err=%v", line, err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := command.Wait()
	exitErr, ok := waitErr.(*exec.ExitError)
	status, statusOK := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !statusOK || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("crash helper was not terminated by SIGKILL: %v", waitErr)
	}
	if err := ReapGateRuntime(runtimeRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(gateRoot); !os.IsNotExist(err) {
		t.Fatalf("crashed Gate work tree survived reaper: %v", err)
	}
}

func TestGateReaperTopologyWithoutPrivilege(t *testing.T) {
	authorityRoot := t.TempDir()
	revision := strings.Repeat("d", 40)
	runtimeRoot := filepath.Join(authorityRoot, "vane-firecracker-gate-"+revision)
	if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := reapGateRuntimeWithAuthority(runtimeRoot, authorityRoot, uint32(os.Geteuid())); err != nil {
		t.Fatalf("empty runtime authority was not idempotent: %v", err)
	}
	gateRoot := filepath.Join(runtimeRoot, "firecracker-work")
	workRoot := filepath.Join(gateRoot, "work-fixture")
	if err := os.MkdirAll(workRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workRoot, "empty-netns-fixture"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reapGateRuntimeWithUnmount(runtimeRoot, authorityRoot, uint32(os.Geteuid()), func(string) error { return syscall.EINVAL }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(gateRoot); !os.IsNotExist(err) {
		t.Fatalf("fixture work tree survived reaper: %v", err)
	}
	for _, mutation := range []string{runtimeRoot + "/../" + filepath.Base(runtimeRoot), filepath.Join(authorityRoot, "unexpected")} {
		if err := reapGateRuntimeWithUnmount(mutation, authorityRoot, uint32(os.Geteuid()), func(string) error { return nil }); err == nil {
			t.Fatalf("unsafe runtime topology accepted: %q", mutation)
		}
	}
	if err := os.Chmod(runtimeRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := reapGateRuntimeWithUnmount(runtimeRoot, authorityRoot, uint32(os.Geteuid()), func(string) error { return nil }); err == nil {
		t.Fatal("loose runtime root accepted")
	}
	if err := reapGateRuntimeWithUnmount(filepath.Join(authorityRoot, "vane-firecracker-gate-"+strings.Repeat("e", 40)),
		authorityRoot, uint32(os.Geteuid()), func(string) error { return nil }); err == nil {
		t.Fatal("missing runtime root accepted")
	}
	if err := os.Chmod(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	unexpected := filepath.Join(runtimeRoot, "firecracker-work", "unexpected")
	if err := os.MkdirAll(unexpected, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unexpected, "empty-netns-bad"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reapGateRuntimeWithUnmount(runtimeRoot, authorityRoot, uint32(os.Geteuid()), func(string) error { return nil }); err == nil {
		t.Fatal("unexpected namespace topology accepted")
	}
	if err := os.RemoveAll(filepath.Join(runtimeRoot, "firecracker-work")); err != nil {
		t.Fatal(err)
	}
	workRoot = filepath.Join(runtimeRoot, "firecracker-work", "work-unmount")
	if err := os.MkdirAll(workRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workRoot, "empty-netns-failure"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reapGateRuntimeWithUnmount(runtimeRoot, authorityRoot, uint32(os.Geteuid()), func(string) error { return syscall.EPERM }); err == nil {
		t.Fatal("namespace unmount failure was hidden")
	}
}

func TestGateCrashNamespaceHelper(t *testing.T) {
	if os.Getenv("VANE_GATE_CRASH_HELPER") != "1" {
		return
	}
	workRoot := os.Getenv("VANE_GATE_CRASH_WORK")
	if _, _, err := CreateGateNetNS(workRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}
