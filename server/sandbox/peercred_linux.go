//go:build linux

package sandbox

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func unixPeerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var credential *unix.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credential, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if socketErr != nil || credential == nil {
		return 0, fmt.Errorf("read SO_PEERCRED: %w", socketErr)
	}
	return credential.Uid, nil
}
