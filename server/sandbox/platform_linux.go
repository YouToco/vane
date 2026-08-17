//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func inspectKVM(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect /dev/kvm: %w", err)
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return errors.New("/dev/kvm is not a character device")
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("sandboxd cannot access /dev/kvm: %w", err)
	}
	return file.Close()
}

func prepareDirectoryForRemoval(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	chmodErr := unix.Fchmod(fd, 0o700)
	return errors.Join(chmodErr, unix.Close(fd))
}

func verifyVersion(ctx context.Context, path, binary, expected string) error {
	versionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	output, err := (execLauncher{}).Run(versionCtx, LaunchPlan{Executable: path,
		Arguments: []string{"--version"}, WorkDir: filepath.Dir(path), OutputLimit: 64 << 10})
	if err != nil {
		if errors.Is(versionCtx.Err(), context.DeadlineExceeded) {
			return errors.New("version command timed out")
		}
		return err
	}
	return validateVersionOutput(binary, expected, string(output))
}

type execLauncher struct{}

var exactGateCgroupKill = regexp.MustCompile(`^/sys/fs/cgroup/system\.slice/vane-firecracker-gate-[0-9a-f]{40}\.service/cgroup\.kill$`)

func (execLauncher) Run(ctx context.Context, plan LaunchPlan) ([]byte, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(runCtx, plan.Executable, plan.Arguments...)
	cmd.Dir = plan.WorkDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	buffer := &cappedBuffer{limit: plan.OutputLimit, cancel: cancel}
	cmd.Stdout, cmd.Stderr = buffer, buffer
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	supervisorDone := make(chan error, 1)
	go func() { supervisorDone <- superviseJailerChild(runCtx, done, plan, cmd.Process.Pid, cancel) }()
	err := cmd.Wait()
	close(done)
	supervisorErr := <-supervisorDone
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if buffer.exceeded {
		return nil, errors.Join(ErrOutputLimit, supervisorErr)
	}
	return buffer.Bytes(), errors.Join(err, supervisorErr)
}

func superviseJailerChild(ctx context.Context, done <-chan struct{}, plan LaunchPlan, processGroup int,
	cancel context.CancelFunc) error {
	if plan.PIDFile == "" {
		select {
		case <-ctx.Done():
			_ = syscall.Kill(-processGroup, syscall.SIGKILL)
		case <-done:
		}
		return nil
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	cancelled := false
	var cancellationDeadline time.Time
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			if !cancelled {
				cancelled = true
				cancellationDeadline = time.Now().Add(500 * time.Millisecond)
				_ = syscall.Kill(-processGroup, syscall.SIGKILL)
			}
		case <-ticker.C:
		}
		pidfd, ready, err := tryOpenJailerPIDFileForUID(plan.PIDFile, plan.pidFileUID)
		if err != nil {
			cancel()
			_ = syscall.Kill(-processGroup, syscall.SIGKILL)
			_ = killGateCgroup(plan.CgroupKill)
			return err
		}
		if ready {
			if cancelled {
				signalErr := unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
				closeErr := unix.Close(pidfd)
				if errors.Is(signalErr, unix.ESRCH) {
					signalErr = nil
				}
				return errors.Join(signalErr, closeErr)
			}
			select {
			case <-done:
				return unix.Close(pidfd)
			case <-ctx.Done():
				_ = syscall.Kill(-processGroup, syscall.SIGKILL)
				signalErr := unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
				closeErr := unix.Close(pidfd)
				if errors.Is(signalErr, unix.ESRCH) {
					signalErr = nil
				}
				return errors.Join(signalErr, closeErr)
			}
		}
		if cancelled && time.Now().After(cancellationDeadline) {
			if err := killGateCgroup(plan.CgroupKill); err != nil {
				return errors.Join(errors.New("Firecracker pidfd was unavailable after cancellation"), err)
			}
			return errors.New("Firecracker pidfd was unavailable; killed the exact Gate cgroup")
		}
	}
}

func tryOpenJailerPIDFile(path string) (int, bool, error) {
	return tryOpenJailerPIDFileForUID(path, 0)
}

func tryOpenJailerPIDFileForUID(path string, expectedUID uint32) (int, bool, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return -1, false, nil
	}
	if err != nil {
		return -1, false, err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return -1, false, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != expectedUID || stat.Mode&0o777 != 0o600 {
		return -1, false, errors.New("Firecracker jailer PID file authority is unsafe")
	}
	if stat.Size == 0 {
		return -1, false, nil
	}
	if stat.Size > 20 {
		return -1, false, errors.New("Firecracker jailer PID file is oversized")
	}
	payload := make([]byte, stat.Size)
	read, err := unix.Read(fd, payload)
	if err != nil || read != len(payload) {
		return -1, false, errors.New("Firecracker jailer PID file changed while reading")
	}
	pid, err := strconv.Atoi(string(payload))
	if err != nil || pid <= 1 || strconv.Itoa(pid) != string(payload) {
		return -1, false, errors.New("Firecracker jailer PID file is not canonical")
	}
	pidfd, err := unix.PidfdOpen(pid, 0)
	if errors.Is(err, unix.ESRCH) {
		return -1, false, nil
	}
	if err != nil {
		return -1, false, err
	}
	if err := verifyProcessInSupervisorCgroup(pid); err != nil {
		_ = unix.Close(pidfd)
		return -1, false, err
	}
	return pidfd, true, nil
}

func verifyProcessInSupervisorCgroup(pid int) error {
	self, err := unifiedCgroupPath("/proc/self/cgroup")
	if err != nil {
		return err
	}
	target, err := unifiedCgroupPath(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return err
	}
	if target != self && !strings.HasPrefix(target, strings.TrimSuffix(self, "/")+"/") {
		return errors.New("Firecracker child escaped the supervisor cgroup")
	}
	return nil
}

func unifiedCgroupPath(path string) (string, error) {
	payload, err := os.ReadFile(path)
	if err != nil || len(payload) > 64<<10 {
		return "", errors.New("read unified cgroup authority")
	}
	value := ""
	for _, line := range strings.Split(strings.TrimSpace(string(payload)), "\n") {
		if strings.HasPrefix(line, "0::") {
			if value != "" {
				return "", errors.New("duplicate unified cgroup authority")
			}
			value = strings.TrimPrefix(line, "0::")
		}
	}
	if !strings.HasPrefix(value, "/") {
		return "", errors.New("unified cgroup authority is missing")
	}
	return value, nil
}

func killGateCgroup(path string) error {
	if path == "" {
		return errors.New("exact Gate cgroup kill authority is unavailable")
	}
	clean := filepath.Clean(path)
	if clean != path || !exactGateCgroupKill.MatchString(clean) {
		return errors.New("exact Gate cgroup kill authority is unsafe")
	}
	fd, err := unix.Open(clean, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	_, writeErr := unix.Write(fd, []byte("1"))
	return errors.Join(writeErr, unix.Close(fd))
}

type cappedBuffer struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int64
	cancel   context.CancelFunc
	exceeded bool
}

func (b *cappedBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.limit - int64(b.buffer.Len())
	if remaining <= 0 || int64(len(payload)) > remaining {
		b.exceeded = true
		b.cancel()
		return len(payload), nil
	}
	return b.buffer.Write(payload)
}

func (b *cappedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buffer.Bytes()...)
}
