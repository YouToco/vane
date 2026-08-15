//go:build !linux

package sandbox

import (
	"context"
	"errors"
)

func inspectKVM(string) error { return errors.New("Firecracker sandbox requires Linux KVM") }
func verifyVersion(context.Context, string, string, string) error {
	return errors.New("Firecracker sandbox version verification requires Linux")
}

type execLauncher struct{}

func (execLauncher) Run(context.Context, LaunchPlan) ([]byte, error) {
	return nil, errors.New("Firecracker sandbox launcher requires Linux")
}
