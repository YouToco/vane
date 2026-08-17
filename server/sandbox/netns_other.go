//go:build !linux

package sandbox

import "errors"

func inspectNetworkNamespace(string) error {
	return errors.New("network namespace inspection requires Linux")
}

func InspectGateNetNS(path string) error { return inspectNetworkNamespace(path) }
