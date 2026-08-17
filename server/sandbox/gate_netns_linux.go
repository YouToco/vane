//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	gateRuntimeNamePattern = regexp.MustCompile(`^vane-firecracker-gate-[0-9a-f]{40}$`)
)

type gateNetNSOperations struct {
	openOriginal func() (int, error)
	close        func(int) error
	createTarget func(string) (string, error)
	unshare      func() error
	mount        func(string) error
	setns        func(int) error
	unmount      func(string, int) error
	remove       func(string) error
}

func productionGateNetNSOperations(parent string) gateNetNSOperations {
	threadNetNS := fmt.Sprintf("/proc/self/task/%d/ns/net", unix.Gettid())
	return gateNetNSOperations{
		openOriginal: func() (int, error) { return unix.Open(threadNetNS, unix.O_RDONLY|unix.O_CLOEXEC, 0) },
		close:        unix.Close,
		createTarget: func(string) (string, error) {
			file, err := os.CreateTemp(parent, "empty-netns-")
			if err != nil {
				return "", err
			}
			path := file.Name()
			if err := errors.Join(file.Close(), os.Chmod(path, 0o600)); err != nil {
				_ = os.Remove(path)
				return "", err
			}
			return path, nil
		},
		unshare: func() error { return unix.Unshare(unix.CLONE_NEWNET) },
		mount:   func(path string) error { return unix.Mount(threadNetNS, path, "", unix.MS_BIND, "") },
		setns:   func(fd int) error { return unix.Setns(fd, unix.CLONE_NEWNET) },
		unmount: unix.Unmount, remove: os.Remove,
	}
}

// CreateGateNetNS creates one root-only, loopback-only network namespace for a
// release self-test. The returned cleanup is mandatory and reports unmount or
// unlink failures instead of hiding possible namespace persistence.
func CreateGateNetNS(parent string) (string, func() error, error) {
	result := createGateNetNSOnDedicatedThread(
		parent,
		func() gateNetNSOperations { return productionGateNetNSOperations(parent) },
		runtime.LockOSThread,
		runtime.UnlockOSThread,
	)
	return result.path, result.cleanup, result.err
}

type gateNetNSCreation struct {
	path     string
	cleanup  func() error
	restored bool
	err      error
}

// createGateNetNSOnDedicatedThread mirrors inspectNetworkNamespace's retired
// thread contract. If restoring the original namespace fails, the goroutine
// exits while still locked and the Go runtime destroys that OS thread instead
// of returning an attacker-controlled network namespace to the thread pool.
func createGateNetNSOnDedicatedThread(parent string, operations func() gateNetNSOperations,
	lockThread, unlockThread func()) gateNetNSCreation {
	done := make(chan gateNetNSCreation, 1)
	go func() {
		lockThread()
		result := createGateNetNSWithRestoreState(parent, operations())
		if result.restored {
			unlockThread()
		}
		done <- result
	}()
	return <-done
}

func createGateNetNSWithOperations(parent string, operations gateNetNSOperations) (string, func() error, error) {
	result := createGateNetNSWithRestoreState(parent, operations)
	return result.path, result.cleanup, result.err
}

func createGateNetNSWithRestoreState(parent string, operations gateNetNSOperations) gateNetNSCreation {
	original, err := operations.openOriginal()
	if err != nil {
		return gateNetNSCreation{restored: true, err: fmt.Errorf("open original network namespace: %w", err)}
	}
	defer operations.close(original)
	path, err := operations.createTarget(parent)
	if err != nil {
		return gateNetNSCreation{restored: true, err: err}
	}
	if err := operations.unshare(); err != nil {
		_ = operations.remove(path)
		return gateNetNSCreation{restored: true, err: fmt.Errorf("create empty network namespace: %w", err)}
	}
	mountErr := operations.mount(path)
	restoreErr := operations.setns(original)
	if mountErr != nil || restoreErr != nil {
		if mountErr == nil {
			_ = operations.unmount(path, unix.MNT_DETACH)
		}
		_ = operations.remove(path)
		return gateNetNSCreation{restored: restoreErr == nil, err: errors.Join(mountErr, restoreErr)}
	}
	cleanup := func() error {
		return errors.Join(operations.unmount(path, 0), operations.remove(path))
	}
	return gateNetNSCreation{path: path, cleanup: cleanup, restored: true}
}

// ReapGateRuntime removes the fixed release-Gate work tree after systemd has
// killed the unit cgroup. It is also safe after the normal in-process cleanup.
// The narrow /run topology prevents this crash path from becoming a general
// privileged recursive-delete primitive.
func ReapGateRuntime(runtimeRoot string) error {
	return reapGateRuntimeWithAuthority(runtimeRoot, "/run", 0)
}

func reapGateRuntimeWithAuthority(runtimeRoot, authorityRoot string, expectedUID uint32) error {
	return reapGateRuntimeWithUnmount(runtimeRoot, authorityRoot, expectedUID, func(path string) error {
		return unix.Unmount(path, 0)
	})
}

func reapGateRuntimeWithUnmount(runtimeRoot, authorityRoot string, expectedUID uint32, unmount func(string) error) error {
	clean := filepath.Clean(runtimeRoot)
	if clean != runtimeRoot || filepath.Dir(clean) != authorityRoot || !gateRuntimeNamePattern.MatchString(filepath.Base(clean)) {
		return errors.New("Firecracker Gate runtime root is unsafe")
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return err
	}
	stat, ownerOK := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownerOK || stat.Uid != expectedUID {
		return errors.New("Firecracker Gate runtime root differs from systemd authority")
	}
	gateRoot := filepath.Join(clean, "firecracker-work")
	if _, err := os.Lstat(gateRoot); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	var unmountErr error
	walkErr := filepath.Walk(gateRoot, func(path string, entry os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Mode()&os.ModeSymlink != 0 || entry.IsDir() || !strings.HasPrefix(entry.Name(), "empty-netns-") {
			return nil
		}
		parent := filepath.Base(filepath.Dir(path))
		if !strings.HasPrefix(parent, "work-") {
			return errors.New("Firecracker Gate namespace has an unexpected topology")
		}
		if err := unmount(path); err != nil && !errors.Is(err, unix.EINVAL) {
			unmountErr = errors.Join(unmountErr, err)
		}
		return nil
	})
	if walkErr != nil || unmountErr != nil {
		return errors.Join(walkErr, unmountErr)
	}
	return scrubTree(gateRoot)
}
