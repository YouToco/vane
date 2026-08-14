// Package testgate owns the only permitted testing skip operation in Vane.
// Capability gates skip in the quick developer loop, but become hard failures
// in the exact-main full gate so missing infrastructure cannot silently reduce
// release coverage.
package testgate

import (
	"fmt"
	"os"
	"testing"
)

const fullGateEnv = "VANE_FULL_GATE"

// Database reports that a test requires the configured PostgreSQL test database.
func Database(t testing.TB) {
	t.Helper()
	unavailable(t, "database", "required test database URL is not configured")
}

// CreateDatabase reports that the configured database role cannot create an
// isolated scratch database.
func CreateDatabase(t testing.TB, err error) {
	t.Helper()
	unavailable(t, "create-database", fmt.Sprintf("scratch database creation failed: %v", err))
}

// DestructiveDatabase reports that an explicitly disposable database was not
// enabled for a destructive role/migration test.
func DestructiveDatabase(t testing.TB) {
	t.Helper()
	unavailable(t, "destructive-database", "disposable PostgreSQL opt-in is not enabled")
}

// PostgreSQLURL reports that DATABASE_URL is present but is not a PostgreSQL URL.
func PostgreSQLURL(t testing.TB) {
	t.Helper()
	unavailable(t, "postgresql-url", "test database URL is not postgres/postgresql")
}

// Symlink reports that the host filesystem cannot create the symlink required
// by a filesystem-behavior test.
func Symlink(t testing.TB, err error) {
	t.Helper()
	unavailable(t, "symlink", fmt.Sprintf("symlink creation failed: %v", err))
}

// WideInteger reports that the platform int width cannot represent the value
// required by the test.
func WideInteger(t testing.TB) {
	t.Helper()
	unavailable(t, "wide-integer", "platform int is not wider than PostgreSQL int4")
}

// LongRunning reports that a deliberately slow timing test was excluded by
// go test -short.
func LongRunning(t testing.TB) {
	t.Helper()
	unavailable(t, "long-running", "go test -short excludes the timing test")
}

// SystemConfigIsolation reports that a host-owned absolute configuration file
// cannot be hidden from the test process.
func SystemConfigIsolation(t testing.TB, path string) {
	t.Helper()
	unavailable(t, "system-config-isolation", "host configuration is visible at "+path)
}

// unavailable is deliberately the single sealed Skip call. The AST/type policy
// permits this exact function and rejects every other testing.T/B/F Skip,
// Skipf, or SkipNow selection, including aliases and promoted methods.
func unavailable(t testing.TB, capability, detail string) {
	t.Helper()
	message := fmt.Sprintf("capability unavailable [%s]: %s", capability, detail)
	if os.Getenv(fullGateEnv) == "1" {
		t.Fatalf("full gate requires %s", message)
	}
	// This is the repository's only permitted direct testing skip.
	t.Skip(message)
}
