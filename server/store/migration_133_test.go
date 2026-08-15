package store

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/internal/testgate"
)

func TestMigration133PolicyManifestIsOptionalAndByteExact(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/133_interactive_agent_policy_manifest.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"ADD COLUMN policy_manifest_payload BYTEA",
		"ADD COLUMN policy_manifest_digest TEXT",
		"(policy_manifest_payload IS NULL) = (policy_manifest_digest IS NULL)",
		"octet_length(policy_manifest_payload) BETWEEN 1 AND 16384",
		"encode(sha256(policy_manifest_payload), 'hex')",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("migration 133 missing exact manifest contract %q", fragment)
		}
	}
}

func TestMigration133PolicyManifestConstraintPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 133); err != nil {
		t.Fatal(err)
	}

	manifest := []byte(`{"schema_version":"vane.interactive-agent-policy-manifest/v1","lane":"owner"}`)
	sum := sha256.Sum256(manifest)
	digest := fmt.Sprintf("%x", sum[:])
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO llm_calls(trace_id,span_name,provider,model,
		    policy_manifest_payload,policy_manifest_digest)
		VALUES('manifest-valid','agent','deepseek','deepseek-v4-flash',$1,$2)`,
		manifest, digest,
	); err != nil {
		t.Fatalf("valid manifest insert: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO llm_calls(trace_id,span_name,provider,model)
		VALUES('manifest-legacy','score','deepseek','deepseek-v4-flash')`,
	); err != nil {
		t.Fatalf("legacy null manifest insert: %v", err)
	}

	for _, tc := range []struct {
		name    string
		payload any
		digest  any
	}{
		{"payload only", manifest, nil},
		{"digest only", nil, digest},
		{"wrong digest", manifest, strings.Repeat("0", 64)},
		{"empty payload", []byte{}, digest},
		{"oversize payload", make([]byte, (16<<10)+1), digest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := database.ExecContext(t.Context(), `
				INSERT INTO llm_calls(trace_id,span_name,provider,model,
				    policy_manifest_payload,policy_manifest_digest)
				VALUES('manifest-invalid','agent','deepseek','deepseek-v4-flash',$1,$2)`,
				tc.payload, tc.digest,
			); err == nil {
				t.Fatal("invalid policy manifest passed database constraint")
			}
		})
	}
}
