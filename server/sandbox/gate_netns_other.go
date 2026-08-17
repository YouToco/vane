//go:build !linux

package sandbox

import "errors"

func CreateGateNetNS(string) (string, func() error, error) {
	return "", nil, errors.New("Firecracker release Gate requires Linux KVM")
}
