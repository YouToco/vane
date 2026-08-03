package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/types"
)

func TestMigration113TombstonesExpiredLegacyCreationWithoutDeletingAudit(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 110); err != nil {
		t.Fatal(err)
	}

	var userID, tenantID int64
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-111-owner','owner') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `
		INSERT INTO tenants(status,plan) VALUES('active','free') RETURNING id`).Scan(
		&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		id, expires, provider, target string
		attempt                       int
	}{
		{id: "migration-111-expired", expires: "clock_timestamp()-interval '1 hour'"},
		{id: "migration-111-live", expires: "clock_timestamp()+interval '1 hour'"},
		{id: "migration-111-non-pristine", expires: "clock_timestamp()-interval '1 hour'", attempt: 1},
		{id: "migration-113-pre-receipted", expires: "clock_timestamp()-interval '1 hour'"},
		{id: "migration-113-normalized", expires: "clock_timestamp()-interval '1 hour'",
			provider: "agent_auto/v1", target: "migration-113-normalized"},
		{id: "migration-113-wrong-provider", expires: "clock_timestamp()-interval '1 hour'",
			provider: "agent_auto/v2", target: "migration-113-wrong-provider"},
		{id: "migration-113-wrong-target", expires: "clock_timestamp()-interval '1 hour'",
			provider: "agent_auto/v1", target: "another-operation"},
	} {
		if _, err := db.ExecContext(t.Context(), `
			INSERT INTO task_creation_operations
			 (id,tenant_id,user_id,tool_name,args,summary,status,expires_at,execution_version)
			VALUES($1,$2,$3,'create_schedule','{}','legacy fixture','pending',`+
			fixture.expires+`,1)`, fixture.id, tenantID, userID); err != nil {
			t.Fatalf("seed %s: %v", fixture.id, err)
		}
		if fixture.attempt != 0 {
			if _, err := db.ExecContext(t.Context(), `UPDATE task_creation_operations SET attempt=$2
				WHERE id=$1`, fixture.id, fixture.attempt); err != nil {
				t.Fatalf("mark %s non-pristine: %v", fixture.id, err)
			}
		}
		if fixture.provider != "" || fixture.target != "" {
			if _, err := db.ExecContext(t.Context(), `UPDATE task_creation_operations
				SET receipt_provider=$2,receipt_target=$3 WHERE id=$1`,
				fixture.id, fixture.provider, fixture.target); err != nil {
				t.Fatalf("set %s receipt shape: %v", fixture.id, err)
			}
		}
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO task_creation_receipts(
		    operation_id,tenant_id,user_id,provider_key,status,
		    provider_message_id,failure_class,sent_at
		) VALUES(
		    'migration-113-pre-receipted',$1,$2,
		    md5('migration-113-pre-receipted')::uuid,'suppressed',
		    'legacy-suppressed','preexisting_terminal_fact',clock_timestamp()
		)`, tenantID, userID); err != nil {
		t.Fatal(err)
	}

	if _, err := provider.UpTo(t.Context(), 113); err != nil {
		t.Fatal(err)
	}

	var status, phase string
	var tombstoned bool
	if err := db.QueryRowContext(t.Context(), `
		SELECT status,phase,tombstoned_at IS NOT NULL
		  FROM task_creation_operations WHERE id='migration-111-expired'`).Scan(
		&status, &phase, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if status != "expired" || phase != "expired" || !tombstoned {
		t.Fatalf("expired operation status=%q phase=%q tombstoned=%v",
			status, phase, tombstoned)
	}
	var normalizedProvider, normalizedTarget string
	if err := db.QueryRowContext(t.Context(), `SELECT operation.status,
		receipt.provider,receipt.target FROM task_creation_operations operation
		JOIN task_creation_receipts receipt ON receipt.operation_id=operation.id
		WHERE operation.id='migration-113-normalized'`).Scan(
		&status, &normalizedProvider, &normalizedTarget); err != nil {
		t.Fatal(err)
	}
	if status != "expired" || normalizedProvider != "agent_auto/v1" ||
		normalizedTarget != "migration-113-normalized" {
		t.Fatalf("074-normalized tombstone=%q provider=%q target=%q",
			status, normalizedProvider, normalizedTarget)
	}
	for _, id := range []string{
		"migration-113-wrong-provider", "migration-113-wrong-target",
		"migration-113-pre-receipted",
	} {
		if err := db.QueryRowContext(t.Context(), `SELECT status,phase,tombstoned_at IS NOT NULL
			FROM task_creation_operations WHERE id=$1`, id).Scan(&status, &phase, &tombstoned); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || phase != "" || tombstoned {
			t.Fatalf("non-pristine legacy operation %s was tombstoned", id)
		}
	}
	var receiptStatus, providerMessageID, failureClass string
	if err := db.QueryRowContext(t.Context(), `
		SELECT status,provider_message_id,failure_class
		  FROM task_creation_receipts
		 WHERE operation_id='migration-111-expired'`).Scan(
		&receiptStatus, &providerMessageID, &failureClass); err != nil {
		t.Fatal(err)
	}
	if receiptStatus != "suppressed" || providerMessageID != "legacy-suppressed" ||
		failureClass != "legacy_admission_fence_expired" {
		t.Fatalf("audit receipt status=%q message=%q class=%q",
			receiptStatus, providerMessageID, failureClass)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT status,phase,tombstoned_at IS NOT NULL
		  FROM task_creation_operations WHERE id='migration-111-live'`).Scan(
		&status, &phase, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || phase != "" || tombstoned {
		t.Fatalf("live recovery operation changed: status=%q phase=%q tombstoned=%v",
			status, phase, tombstoned)
	}
	if err := db.QueryRowContext(t.Context(), `
		SELECT status,phase,tombstoned_at IS NOT NULL
		  FROM task_creation_operations WHERE id='migration-111-non-pristine'`).Scan(
		&status, &phase, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || phase != "" || tombstoned {
		t.Fatalf("non-pristine recovery operation changed: status=%q phase=%q tombstoned=%v",
			status, phase, tombstoned)
	}

	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	params := types.CreateTaskCreationOperationParams{
		ID: "migration-111-replay-" + uuid.NewString(), TenantID: tenantID,
		UserID: userID, Args: json.RawMessage(`{"intent":"retained replay"}`),
		Summary: "retained replay", ExpiresAt: time.Now().Add(time.Hour),
	}
	if _, err := st.CreateTaskCreationOperation(t.Context(), params); err != nil {
		t.Fatalf("seed retained replay: %v", err)
	}
	fence, err := NewLegacyAdmissionFencedStore(st)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fence.CreateTaskCreationOperation(t.Context(), params); err != nil {
		t.Fatalf("exact existing response replay was blocked: %v", err)
	}
	newParams := params
	newParams.ID = "migration-111-new-" + uuid.NewString()
	if _, err := fence.CreateTaskCreationOperation(t.Context(), newParams); !errors.Is(err, ErrLegacyControlPlaneAdmissionClosed) {
		t.Fatalf("new V1 admission error=%v, want closed", err)
	}

	if _, err := provider.DownTo(t.Context(), 112); err != nil {
		t.Fatal(err)
	}
	var operations, receipts int
	if err := db.QueryRowContext(t.Context(), `
		SELECT (SELECT count(*) FROM task_creation_operations
		         WHERE id='migration-111-expired'),
		       (SELECT count(*) FROM task_creation_receipts
		         WHERE operation_id='migration-111-expired')`).Scan(
		&operations, &receipts); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || receipts != 1 {
		t.Fatalf("downgrade deleted audit: operations=%d receipts=%d", operations, receipts)
	}
}
