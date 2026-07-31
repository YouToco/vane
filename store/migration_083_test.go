package store

import (
	"testing"
)

func TestMigration083PeriodicBriefTaskTreeCascadesOnDelete(t *testing.T) {
	_, db, provider := openMigration066Database(
		t, "vane_schedule_delete_periodic_cascade_083")
	if _, err := provider.UpTo(t.Context(), 83); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"fk_periodic_brief_task",
		"periodic_synthesis_receipts_intent_id_fkey",
		"fk_periodic_synthesis_scope",
		"fk_periodic_report_intent",
		"periodic_report_deliveries_report_id_fkey",
		"fk_periodic_delivery_scope",
	}
	for _, constraint := range want {
		var deleteAction string
		if err := db.QueryRowContext(t.Context(), `
			SELECT confdeltype::text
			  FROM pg_constraint
			 WHERE conname=$1`, constraint,
		).Scan(&deleteAction); err != nil {
			t.Fatalf("read %s: %v", constraint, err)
		}
		if deleteAction != "c" {
			t.Errorf("%s delete action = %q, want cascade", constraint, deleteAction)
		}
	}
}
