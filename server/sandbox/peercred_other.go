//go:build !linux

package sandbox

import (
	"errors"
	"net"
)

func unixPeerUID(*net.UnixConn) (uint32, error) {
	return 0, errors.New("SO_PEERCRED sandbox authentication requires Linux")
}
