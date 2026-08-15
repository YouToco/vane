//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
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

func verifyVersion(ctx context.Context, path, binary, expected string) error {
	output, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return err
	}
	return validateVersionOutput(binary, expected, string(output))
}

type execLauncher struct{}

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
	go func() {
		select {
		case <-runCtx.Done():
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		case <-done:
		}
	}()
	err := cmd.Wait()
	close(done)
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if buffer.exceeded {
		return nil, ErrOutputLimit
	}
	return buffer.Bytes(), err
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
