package store

import (
	"strings"
	"testing"
)

func TestMigration120RaisesOnlyOldDefaultLLMQuota(t *testing.T) {
	t.Parallel()
	payload, err := migrationsFS.ReadFile("migrations/120_raise_default_llm_quota.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(payload), "\r\n", "\n")
	for _, required := range []string{
		"SET tokens=1000000000.0,",
		"rate=1000000000.0/86400.0,",
		"burst=1000000000.0,",
		"AND burst=2000000.0",
		"AND abs(rate-2000000.0/86400.0)<0.000001;",
		"SET tokens=LEAST(tokens,2000000.0),",
		"AND burst=1000000000.0",
		"AND abs(rate-1000000000.0/86400.0)<0.000001;",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 120 is missing default-only guard %q", required)
		}
	}
}
