//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// inspectNetworkNamespace performs the topology inspection from inside the
// target namespace. A failed restore deliberately leaves the goroutine locked:
// the Go runtime then terminates that OS thread instead of returning a thread
// in an attacker-controlled namespace to the pool.
func inspectNetworkNamespace(path string) error {
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		restored := true
		var result error
		defer func() {
			if restored {
				runtime.UnlockOSThread()
			}
			done <- result
		}()

		currentPath := fmt.Sprintf("/proc/self/task/%d/ns/net", unix.Gettid())
		current, err := os.Open(currentPath)
		if err != nil {
			result = fmt.Errorf("open current netns: %w", err)
			return
		}
		defer current.Close()
		target, err := os.Open(path)
		if err != nil {
			result = fmt.Errorf("open target netns: %w", err)
			return
		}
		defer target.Close()
		if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
			result = fmt.Errorf("enter target netns: %w", err)
			return
		}
		restored = false
		result = inspectCurrentNetworkNamespace()
		if err := unix.Setns(int(current.Fd()), unix.CLONE_NEWNET); err != nil {
			result = errors.Join(result, fmt.Errorf("restore original netns: %w", err))
			return
		}
		restored = true
	}()
	return <-done
}

func inspectCurrentNetworkNamespace() error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return fmt.Errorf("list netns interfaces: %w", err)
	}
	hasLoopback := false
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback == 0 {
			return fmt.Errorf("non-loopback interface %q exists", iface.Name)
		}
		hasLoopback = true
	}
	if !hasLoopback {
		return errors.New("netns has no loopback interface")
	}
	for _, routeFile := range []struct {
		path string
		ipv6 bool
	}{{"/proc/net/route", false}, {"/proc/net/ipv6_route", true}} {
		if err := rejectNonLoopbackRoutes(routeFile.path, routeFile.ipv6); err != nil {
			return err
		}
	}
	return nil
}

func rejectNonLoopbackRoutes(path string, ipv6 bool) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	if err := validateRouteTable(file, ipv6); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}
