package store

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigration130IsEvidenceOnlyAndOwnerAppendOnly(t *testing.T) {
	payload, err := migrationsFS.ReadFile(
		"migrations/130_agent_first_retention_attestation.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, fragment := range []string{
		"CREATE TABLE agent_first_retention_attestation_events",
		"phase IN ('baseline','prepared')",
		"CREATE FUNCTION agent_first_legacy_db_snapshot_v130() RETURNS BYTEA",
		"CREATE FUNCTION append_agent_first_retention_attestation_v130(",
		"only the direct schema owner may append attestation",
		"prepared evidence has not crossed full retention",
		"prepared evidence does not cite latest baseline",
		"NOT isfinite(requested_temporal_server_witness)",
		"requested_temporal_server_witness<database_now-interval '10 minutes'",
		"parent_event.expires_at<=database_now",
		"database_now:=clock_timestamp()",
		"attestation evidence expired before append",
		"CREATE TRIGGER agent_first_retention_history_immutable_v130",
		"BEFORE UPDATE OR DELETE",
		"BEFORE TRUNCATE",
		"ENABLE ALWAYS TRIGGER agent_first_retention_history_immutable_v130",
		"legacy database snapshot is not quiescent",
		"FOREIGN KEY (parent_digest)",
		"payload_digest=encode(sha256(canonical_payload),'hex')",
		"refusing downgrade while retention attestations exist",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Errorf("migration 130 lost evidence boundary %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"ALTER ROLE", "GRANT vane_", "SET LOCAL ROLE",
		"'db_frozen'", "'active'", "DROP CONSTRAINT fk_agent_first_retention_parent",
		"task_creation_operations_retired", "task_definition_edit_operations_retired",
		"CREATE TRIGGER task_creation", "CREATE TRIGGER task_definition_edit",
	} {
		if strings.Contains(sqlText, forbidden) {
			t.Errorf("migration 130 changed runtime authority via %q", forbidden)
		}
	}
}

func TestAgentFirstRetentionInputValidation(t *testing.T) {
	valid := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Unix(1_800_000_000, 0).UTC())
	if err := validateAgentFirstRetentionInput(valid); err != nil {
		t.Fatalf("valid baseline: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*AgentFirstRetentionAttestationInput)
	}{
		{"phase", func(in *AgentFirstRetentionAttestationInput) { in.Phase = "future" }},
		{"baseline parent", func(in *AgentFirstRetentionAttestationInput) { in.ParentDigest = strings.Repeat("a", 64) }},
		{"cluster", func(in *AgentFirstRetentionAttestationInput) { in.TemporalClusterID = " cluster" }},
		{"retention", func(in *AgentFirstRetentionAttestationInput) { in.RetentionSeconds = 0 }},
		{"namespace id", func(in *AgentFirstRetentionAttestationInput) { in.TemporalNamespaceID = "not-uuid" }},
		{"history archive", func(in *AgentFirstRetentionAttestationInput) { in.HistoryArchiveURIDigest = strings.Repeat("a", 64) }},
		{"visibility archive", func(in *AgentFirstRetentionAttestationInput) { in.VisibilityArchivalState = "unknown" }},
		{"mixed archive", func(in *AgentFirstRetentionAttestationInput) {
			in.HistoryArchivalState = AgentFirstArchivalEnabled
			in.HistoryArchiveURIDigest = strings.Repeat("a", 64)
		}},
		{"witness", func(in *AgentFirstRetentionAttestationInput) { in.TemporalServerWitness = time.Time{} }},
		{"inventory digest", func(in *AgentFirstRetentionAttestationInput) { in.WorkflowInventoryDigest = "bad" }},
		{"source revision", func(in *AgentFirstRetentionAttestationInput) { in.SourceRevision = strings.Repeat("a", 39) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := valid
			tc.mutate(&input)
			if err := validateAgentFirstRetentionInput(input); err == nil {
				t.Fatal("invalid retention evidence accepted")
			}
		})
	}
	prepared := valid
	prepared.Phase = AgentFirstRetentionPhasePrepared
	if err := validateAgentFirstRetentionInput(prepared); err == nil {
		t.Fatal("prepared evidence accepted without parent digest")
	}
}

func TestMigration130AttestationChainAuthorityAndDownGuardPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, scratchURL, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 130); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	var appInsert, appUpdate, appDelete, appExecute bool
	if err := database.QueryRowContext(t.Context(), `
		SELECT has_table_privilege('vane_app',
			         'agent_first_retention_attestation_events','INSERT'),
		       has_table_privilege('vane_app',
			         'agent_first_retention_attestation_events','UPDATE'),
		       has_table_privilege('vane_app',
			         'agent_first_retention_attestation_events','DELETE'),
		       has_function_privilege('vane_app',
		         'append_agent_first_retention_attestation_v130(text,text,text,text,text,bigint,text,text,text,text,timestamptz,text,text,text,text,text,text)',
			         'EXECUTE')`,
	).Scan(&appInsert, &appUpdate, &appDelete, &appExecute); err != nil {
		t.Fatal(err)
	}
	if appInsert || appUpdate || appDelete || appExecute {
		t.Fatalf("vane_app authority insert=%t update=%t delete=%t execute=%t",
			appInsert, appUpdate, appDelete, appExecute)
	}
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `SELECT * FROM
		append_agent_first_retention_attestation_v130(
		'baseline',NULL,'cluster','default','123e4567-e89b-42d3-a456-426614174000',
		60,'disabled',repeat('e',64),'disabled',repeat('e',64),
		clock_timestamp(),repeat('a',64),repeat('b',64),repeat('c',64),
		repeat('d',64),repeat('e',40),repeat('f',64))`); err == nil {
		t.Fatal("vane_app executed owner-only attestation append")
	}
	_ = tx.Rollback()

	witness := time.Now().UTC().Truncate(time.Microsecond)
	future := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", witness.Add(time.Minute))
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), future); err == nil ||
		!strings.Contains(err.Error(), "outside DB clock skew") {
		t.Fatalf("baseline accepted future Temporal witness: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `SELECT * FROM
		append_agent_first_retention_attestation_v130(
		'baseline',NULL,'cluster','default','123e4567-e89b-42d3-a456-426614174000',
		1,'disabled',$1,'disabled',$1,'infinity'::timestamptz,
		repeat('a',64),repeat('b',64),repeat('c',64),repeat('d',64),
		repeat('e',40),repeat('f',64))`, agentFirstEmptyDigest); err == nil {
		t.Fatal("baseline accepted infinite Temporal witness")
	}
	firstBaseline, err := st.AppendAgentFirstRetentionAttestation(t.Context(),
		agentFirstRetentionTestInput(AgentFirstRetentionPhaseBaseline, "", witness))
	if err != nil {
		t.Fatal(err)
	}
	if firstBaseline.Phase != AgentFirstRetentionPhaseBaseline ||
		firstBaseline.ParentDigest != nil ||
		firstBaseline.ExpiresAt.Sub(firstBaseline.IssuedAt) != time.Second+10*time.Minute {
		t.Fatalf("baseline evidence=%+v", firstBaseline)
	}
	if !strings.Contains(string(firstBaseline.CanonicalPayload),
		`"schema_version": "vane.agent-first-retention-attestation/v130"`) {
		t.Fatalf("canonical payload=%s", firstBaseline.CanonicalPayload)
	}

	secondBaseline, err := st.AppendAgentFirstRetentionAttestation(t.Context(),
		agentFirstRetentionTestInput(AgentFirstRetentionPhaseBaseline, "", time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	tooEarly := agentFirstRetentionTestInput(AgentFirstRetentionPhasePrepared,
		secondBaseline.PayloadDigest, time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), tooEarly); err == nil ||
		!strings.Contains(err.Error(), "full retention") {
		t.Fatalf("prepared accepted short retention: %v", err)
	}
	futurePrepared := agentFirstRetentionTestInput(AgentFirstRetentionPhasePrepared,
		secondBaseline.PayloadDigest, time.Now().UTC().Add(time.Minute))
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), futurePrepared); err == nil ||
		!strings.Contains(err.Error(), "outside DB clock skew") {
		t.Fatalf("prepared accepted future Temporal witness: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	stalePrepared := agentFirstRetentionTestInput(AgentFirstRetentionPhasePrepared,
		firstBaseline.PayloadDigest, time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), stalePrepared); err == nil ||
		!strings.Contains(err.Error(), "latest baseline") {
		t.Fatalf("prepared accepted stale baseline: %v", err)
	}
	preparedInput := agentFirstRetentionTestInput(AgentFirstRetentionPhasePrepared,
		secondBaseline.PayloadDigest, time.Now().UTC())
	prepared, err := st.AppendAgentFirstRetentionAttestation(t.Context(), preparedInput)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ParentDigest == nil || *prepared.ParentDigest != secondBaseline.PayloadDigest {
		t.Fatalf("prepared chain differs: %+v", prepared)
	}
	differentDeploy := preparedInput
	differentDeploy.ParentDigest = firstBaseline.PayloadDigest
	differentDeploy.DeployDigest = strings.Repeat("7", 64)
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), differentDeploy); err == nil {
		t.Fatal("prepared accepted a different source deployment")
	}
	changedBaseline, err := st.AppendAgentFirstRetentionAttestation(t.Context(),
		agentFirstRetentionTestInput(AgentFirstRetentionPhaseBaseline, "", time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	userID, tenantID := migration129Identity(t, database, "retention-live")
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations(
		 id,tenant_id,user_id,tool_name,args,summary,status,expires_at,executed_at,
		 execution_version,phase,receipt_provider,receipt_target,tombstoned_at)
		VALUES('migration-130-terminal',$1,$2,'create_schedule','{}','terminal',
		       'expired',clock_timestamp()-interval '1 hour',NULL,1,'expired',
		       'agent_auto/v1','migration-130-terminal',clock_timestamp())`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_receipts(
		 operation_id,tenant_id,user_id,provider,target,provider_key,status,
		 next_attempt_at,provider_message_id,sent_at)
		VALUES('migration-130-terminal',$1,$2,'agent_auto/v1',
		       'migration-130-terminal',md5('migration-130-terminal')::uuid,
		       'suppressed',clock_timestamp(),'legacy-suppressed',clock_timestamp())`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	changedPrepared := agentFirstRetentionTestInput(AgentFirstRetentionPhasePrepared,
		changedBaseline.PayloadDigest, time.Now().UTC())
	time.Sleep(1100 * time.Millisecond)
	changedPrepared.TemporalServerWitness = time.Now().UTC()
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), changedPrepared); err == nil ||
		!strings.Contains(err.Error(), "lane snapshot changed") {
		t.Fatalf("prepared accepted changed legacy history: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		UPDATE task_creation_receipts
		   SET lease_owner='retained-terminal-lease',
		       lease_until=clock_timestamp()+interval '1 minute',
		       takeover_not_before=clock_timestamp()+interval '2 minutes'
		 WHERE operation_id='migration-130-terminal'`); err != nil {
		t.Fatal(err)
	}
	leasedReceiptBaseline := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestation(
		t.Context(), leasedReceiptBaseline); err == nil ||
		!strings.Contains(err.Error(), "not quiescent") {
		t.Fatalf("baseline accepted terminal receipt lease: %v", err)
	}
	if _, err := database.ExecContext(t.Context(), `
		INSERT INTO task_creation_operations(
		 id,tenant_id,user_id,tool_name,args,summary,status,expires_at,
		 execution_version,phase)
		VALUES('migration-130-live',$1,$2,'create_schedule','{}','live',
		       'pending',clock_timestamp()+interval '1 hour',1,'')`,
		tenantID, userID); err != nil {
		t.Fatal(err)
	}
	liveBaseline := agentFirstRetentionTestInput(
		AgentFirstRetentionPhaseBaseline, "", time.Now().UTC())
	if _, err := st.AppendAgentFirstRetentionAttestation(t.Context(), liveBaseline); err == nil ||
		!strings.Contains(err.Error(), "not quiescent") {
		t.Fatalf("baseline accepted live legacy root: %v", err)
	}

	for name, statement := range map[string]string{
		"update":   `UPDATE agent_first_retention_attestation_events SET phase='prepared' WHERE id=1`,
		"delete":   `DELETE FROM agent_first_retention_attestation_events WHERE id=1`,
		"truncate": `TRUNCATE agent_first_retention_attestation_events`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := database.ExecContext(t.Context(), statement); err == nil ||
				!agentFirstPGCode(err, "23514") {
				t.Fatalf("append-only %s accepted: %v", name, err)
			}
		})
	}
	if _, err := provider.DownTo(t.Context(), 129); err == nil ||
		!strings.Contains(err.Error(), "retention attestations exist") {
		t.Fatalf("migration 130 Down destroyed evidence: %v", err)
	}
}

func TestMigration130PlainUpDoesNotChangeClusterMembershipAndEmptyDownPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	database, provider, _, drop := migration128Scratch(t, databaseURL)
	t.Cleanup(drop)
	if _, err := provider.UpTo(t.Context(), 129); err != nil {
		t.Fatal(err)
	}
	before := migration130MembershipDigest(t, database)
	if _, err := provider.UpTo(t.Context(), 130); err != nil {
		t.Fatal(err)
	}
	after := migration130MembershipDigest(t, database)
	if before != after {
		t.Fatalf("plain Up changed cluster memberships\nbefore=%s\nafter=%s", before, after)
	}
	if _, err := provider.DownTo(t.Context(), 129); err != nil {
		t.Fatal(err)
	}
}

func agentFirstRetentionTestInput(
	phase AgentFirstRetentionPhase, parent string, witness time.Time,
) AgentFirstRetentionAttestationInput {
	return AgentFirstRetentionAttestationInput{
		Phase: phase, ParentDigest: parent,
		TemporalClusterID: "temporal-cluster-1", TemporalNamespace: "default",
		TemporalNamespaceID: "123e4567-e89b-42d3-a456-426614174000",
		RetentionSeconds:    1, HistoryArchivalState: AgentFirstArchivalDisabled,
		VisibilityArchivalState:    AgentFirstArchivalDisabled,
		HistoryArchiveURIDigest:    agentFirstEmptyDigest,
		VisibilityArchiveURIDigest: agentFirstEmptyDigest,
		TemporalServerWitness:      witness,
		WorkflowInventoryDigest:    strings.Repeat("1", 64),
		ScheduleInventoryDigest:    strings.Repeat("2", 64),
		ArchiveInventoryDigest:     strings.Repeat("3", 64),
		TemporalEvidenceDigest:     strings.Repeat("4", 64),
		SourceRevision:             strings.Repeat("5", 40), DeployDigest: strings.Repeat("6", 64),
	}
}

func migration130MembershipDigest(t *testing.T, database *sql.DB) string {
	t.Helper()
	var digest string
	if err := database.QueryRowContext(t.Context(), `
		SELECT encode(sha256(convert_to(COALESCE(string_agg(
		 granted.rolname||'>'||member.rolname||':'||edge.admin_option::text||':'||
		 edge.inherit_option::text||':'||edge.set_option::text,',' ORDER BY
		 granted.rolname,member.rolname),'#'),'UTF8')),'hex')
		FROM pg_catalog.pg_auth_members edge
		JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
		JOIN pg_catalog.pg_roles member ON member.oid=edge.member`,
	).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}

func agentFirstPGCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}
