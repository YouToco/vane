//go:build linux

package researchgateway

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

type peerUIDListenerV1 struct {
	net.Listener
	allowed uint32
}

func WrapPeerUIDListenerV1(listener net.Listener, allowed uint32) (net.Listener, error) {
	if listener == nil || listener.Addr().Network() != "unix" {
		return nil, errors.New("research gateway requires a unix listener")
	}
	return &peerUIDListenerV1{Listener: listener, allowed: allowed}, nil
}

func (l *peerUIDListenerV1) Accept() (net.Conn, error) {
	for {
		connection, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		unixConnection, ok := connection.(*net.UnixConn)
		if !ok {
			_ = connection.Close()
			continue
		}
		raw, err := unixConnection.SyscallConn()
		if err != nil {
			_ = connection.Close()
			continue
		}
		var credential *unix.Ucred
		var socketErr error
		if err := raw.Control(func(fd uintptr) {
			credential, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		}); err != nil || socketErr != nil || credential == nil || credential.Uid != l.allowed {
			_ = connection.Close()
			continue
		}
		return connection, nil
	}
}
