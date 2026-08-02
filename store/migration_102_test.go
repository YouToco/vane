package store

import (
	"strings"
	"testing"
)

func TestMigration102KeepsPreparationDarkAndPromotionRecoverable(t *testing.T) {
	payload, err := migrationsFS.ReadFile("migrations/102_research_v3_definition_prepare.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(payload), "\r\n", "\n")
	required := []string{
		"CREATE TABLE research_v3_definition_prepare_operations",
		"CREATE TABLE research_v3_prepared_definition_heads",
		"original_execution_mode",
		"definition_promoted",
		"definition_restored",
		"^research-v3-shadow-[0-9a-f]{64}$",
		"NEW.definition_digest IS DISTINCT FROM selected_definition_digest",
		"GRANT UPDATE (execution_mode,approved_definition_version,approved_definition_digest)",
		"protect_research_v3_definition_prepare_transition",
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration 102 missing %q", fragment)
		}
	}
	forbidden := []string{
		"GRANT UPDATE ON schedules TO vane_research_v3_cutover_operator",
		"GRANT SELECT ON research_v3_definition_prepare_operations TO vane_app",
		"GRANT INSERT ON research_v3_prepared_definition_heads TO vane_app",
	}
	for _, fragment := range forbidden {
		if strings.Contains(sql, fragment) {
			t.Fatalf("migration 102 exposes forbidden capability %q", fragment)
		}
	}
}
