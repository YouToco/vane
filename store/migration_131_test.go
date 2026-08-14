package store

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/agentledger"
)

func TestMigration131RemovesOnlyAgentEventBatchUpperBound(t *testing.T) {
	raw, err := os.ReadFile(
		"migrations/131_agent_session_projection_unbounded_messages.sql",
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(raw)
	for _, required := range []string{
		"DROP CONSTRAINT agent_events_batch_index_valid",
		"batch_size >= 1 AND batch_index BETWEEN 0 AND batch_size - 1",
		"LOCK TABLE agent_events IN ACCESS EXCLUSIVE MODE",
		"WHERE batch_size > 62",
		"refusing downgrade while wide agent event batches exist",
		"batch_size BETWEEN 1 AND 64",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("migration 131 missing contract fragment %q", required)
		}
	}
	for _, forbidden := range []string{
		"DROP TABLE agent_events",
		"DELETE FROM agent_events",
		"TRUNCATE agent_events",
		"ALTER TABLE agent_events DISABLE ROW LEVEL SECURITY",
		"REVOKE",
		"GRANT",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Fatalf("migration 131 changes retained authority via %q", forbidden)
		}
	}
}

func TestMigration131WideBatchAndDowngradeFencePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 131); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	var userID, sessionID int64
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO users(feishu_open_id,name)
		VALUES('migration-131-user','migration 131') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `
		INSERT INTO agent_sessions(tenant_id,user_id)
		VALUES(1,$1) RETURNING id`, userID,
	).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	// 63 events model the first formerly-unrepresentable projection:
	// turn_started + 61 messages + turn_completed.
	events := make([]agentledger.Input, 63)
	for i := range events {
		body, marshalErr := json.Marshal(map[string]any{
			"ordinal": i, "text": "retained",
		})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		events[i] = agentledger.Input{
			Kind: agentledger.KindUserMessage, Body: body,
		}
	}
	stored, err := st.AppendAgentEvents(t.Context(), agentledger.AppendBatch{
		Scope: agentledger.Scope{
			TenantID: 1, UserID: userID, SessionID: sessionID,
		},
		IdempotencyKey: "migration-131-wide", Events: events,
	})
	if err != nil || len(stored) != len(events) {
		t.Fatalf("wide append rows=%d err=%v", len(stored), err)
	}

	if _, err := provider.Down(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "refusing downgrade while wide") {
		t.Fatalf("wide immutable history did not fence downgrade: %v", err)
	}
	var version, count int
	if err := database.QueryRowContext(t.Context(), `
		SELECT (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
		       (SELECT count(*) FROM agent_events WHERE session_id=$1)`,
		sessionID,
	).Scan(&version, &count); err != nil {
		t.Fatal(err)
	}
	if version != 131 || count != len(events) {
		t.Fatalf("refused down lost authority: version=%d rows=%d", version, count)
	}

	if _, err := database.ExecContext(t.Context(),
		`DELETE FROM agent_events WHERE session_id=$1`, sessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("empty wide history should allow downgrade: %v", err)
	}
}
