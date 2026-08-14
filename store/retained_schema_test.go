package store

// The broad Store suite contains retained V1/protocol-1 state-machine and
// replay tests. Migration 132 intentionally makes those writers impossible,
// so the broad suite runs against the last schema where historical fixtures
// can still be constructed. Migration 132 has its own fresh-database tests
// that exercise the frozen schema, V3 pass-through, roles and catalog exactly.
func init() {
	migrationTargetVersionForTesting = 131
}
