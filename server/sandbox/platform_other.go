//go:build !linux

package sandbox

import (
	"context"
	"errors"
)

func inspectKVM(string) error { return errors.New("Firecracker sandbox requires Linux KVM") }

// Production sandboxd is Linux-only. Portable tests never chmod by path here,
// so a symlink race cannot redirect cleanup to an external victim.
func prepareDirectoryForRemoval(string) error { return nil }
func verifyVersion(context.Context, string, string, string) error {
	return errors.New("Firecracker sandbox version verification requires Linux")
}

type execLauncher struct{}

func (execLauncher) Run(context.Context, LaunchPlan) ([]byte, error) {
	return nil, errors.New("Firecracker sandbox launcher requires Linux")
}
