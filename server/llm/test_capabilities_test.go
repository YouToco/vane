package llm

import (
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
)

func requireDatabaseCapability(t testing.TB) {
	t.Helper()
	testgate.Database(t)
}
