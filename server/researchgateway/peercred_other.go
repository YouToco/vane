//go:build !linux

package researchgateway

import (
	"errors"
	"net"
)

func WrapPeerUIDListenerV1(net.Listener, uint32) (net.Listener, error) {
	return nil, errors.New("research gateway peer credentials require linux")
}
