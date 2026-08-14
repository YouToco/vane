package config

import (
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
)

func requireSystemConfigIsolationCapability(t testing.TB, path string) {
	t.Helper()
	testgate.SystemConfigIsolation(t, path)
}
