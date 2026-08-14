package store

import (
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
)

func requireDatabaseCapability(t testing.TB) {
	t.Helper()
	testgate.Database(t)
}

func requireCreateDatabaseCapability(t testing.TB, err error) {
	t.Helper()
	testgate.CreateDatabase(t, err)
}

func requireDestructiveDatabaseCapability(t testing.TB) {
	t.Helper()
	testgate.DestructiveDatabase(t)
}

func requirePostgreSQLURLCapability(t testing.TB) {
	t.Helper()
	testgate.PostgreSQLURL(t)
}

func requireWideIntegerCapability(t testing.TB) {
	t.Helper()
	testgate.WideInteger(t)
}

func requireLongRunningCapability(t testing.TB) {
	t.Helper()
	testgate.LongRunning(t)
}
