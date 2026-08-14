package main

import (
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
)

func requireSymlinkCapability(t testing.TB, err error) {
	t.Helper()
	testgate.Symlink(t, err)
}
