-- 085: Agent-first user intelligence evidence foundation.
--
-- These tables preserve exactly what an interactive Agent saw and said, and
-- the metadata of each user-scoped intelligence query. They are append-only
-- business/audit history. Runtime roles receive no UPDATE, DELETE or TRUNCATE.

-- +goose Up

-- Cluster-stable HMAC material. Old inactive keys remain readable so cursor
-- rotation does not invalidate an in-flight page. No runtime SQL role can
-- read this table; Store loads it through the owner connection at startup.
CREATE TABLE agent_intelligence_cursor_keys (
    key_version INTEGER     PRIMARY KEY,
    key_bytes   BYTEA       NOT NULL,
    active      BOOLEAN     NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_agent_intelligence_cursor_key_version CHECK (key_version > 0),
    CONSTRAINT ck_agent_intelligence_cursor_key_size
        CHECK (octet_length(key_bytes) BETWEEN 16 AND 64)
);
CREATE UNIQUE INDEX uq_agent_intelligence_cursor_key_active
    ON agent_intelligence_cursor_keys ((active)) WHERE active;
INSERT INTO agent_intelligence_cursor_keys(key_version,key_bytes,active)
VALUES (1,uuid_send(gen_random_uuid()),true);

-- A dedicated NOLOGIN reader keeps semantic history reads away from both the
-- broad legacy vane_app role and write coordinators.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname='vane_intelligence_reader'
    ) THEN
        CREATE ROLE vane_intelligence_reader
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
            NOLOGIN NOINHERIT NOBYPASSRLS;
    END IF;
END $$;
-- +goose StatementEnd
ALTER ROLE vane_intelligence_reader
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_intelligence_reader RESET ALL;
ALTER ROLE vane_intelligence_reader
    SET search_path=pg_catalog,public,pg_temp;
GRANT vane_intelligence_reader TO CURRENT_USER;

-- Reject a pre-owned, pre-authorized, or role-connected cluster identity.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_intelligence_reader'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '085: only migration owner may enter intelligence reader';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_intelligence_reader'
    ) THEN
        RAISE EXCEPTION
            '085: intelligence reader must not enter another role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles role_row ON role_row.oid=dep.refobjid
         WHERE role_row.rolname='vane_intelligence_reader'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='o'
           AND (
               dep.dbid=0 OR dep.dbid=(
                   SELECT oid FROM pg_database WHERE datname=current_database()
               )
           )
    ) THEN
        RAISE EXCEPTION
            '085: intelligence reader must not own database objects';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles role_row ON role_row.oid=dep.refobjid
         WHERE role_row.rolname='vane_intelligence_reader'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='a'
           AND (
               dep.dbid=(
                   SELECT oid FROM pg_database WHERE datname=current_database()
               ) OR (
                   dep.dbid=0 AND dep.classid='pg_database'::regclass
                   AND dep.objid=(
                       SELECT oid FROM pg_database WHERE datname=current_database()
                   )
               )
           )
    ) THEN
        RAISE EXCEPTION
            '085: intelligence reader has preexisting ACL in this database';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_parameter_acl parameter_acl
         WHERE has_parameter_privilege(
                   'vane_intelligence_reader',parameter_acl.parname,'SET')
            OR has_parameter_privilege(
                   'vane_intelligence_reader',parameter_acl.parname,'ALTER SYSTEM')
    ) THEN
        RAISE EXCEPTION
            '085: intelligence reader has unsafe parameter ACL';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vane_intelligence_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vane_intelligence_reader;

CREATE TABLE agent_tool_evidence (
    id                  BIGSERIAL   PRIMARY KEY,
    tenant_id           BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id             BIGINT      NOT NULL REFERENCES users (id),
    session_id          BIGINT      NOT NULL,
    trace_id            TEXT        NOT NULL,
    invocation_id       TEXT        NOT NULL,
    tool_call_id        BIGINT      NOT NULL,
    tool_name           TEXT        NOT NULL,
    arguments           JSONB       NOT NULL,
    result_bytes        BYTEA       NOT NULL,
    result_digest       TEXT        NOT NULL,
    original_size       INTEGER     NOT NULL,
    truncated           BOOLEAN     NOT NULL,
    trust_type          TEXT        NOT NULL,
    schema_version      TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_tool_evidence_session
        FOREIGN KEY (session_id,tenant_id,user_id)
        REFERENCES agent_sessions (id,tenant_id,user_id),
    CONSTRAINT fk_agent_tool_evidence_call
        FOREIGN KEY (tool_call_id) REFERENCES tool_calls (id),
    CONSTRAINT uq_agent_tool_evidence_invocation
        UNIQUE (tenant_id,user_id,trace_id,invocation_id),
    CONSTRAINT ck_agent_tool_evidence_trace
        CHECK (btrim(trace_id)=trace_id AND octet_length(trace_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_agent_tool_evidence_invocation
        CHECK (btrim(invocation_id)=invocation_id AND octet_length(invocation_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_agent_tool_evidence_tool
        CHECK (btrim(tool_name)=tool_name AND octet_length(tool_name) BETWEEN 1 AND 255),
    CONSTRAINT ck_agent_tool_evidence_result_size
        CHECK (
            octet_length(result_bytes) <= 262144 AND
            position(decode('00','hex') in result_bytes)=0 AND
            convert_from(result_bytes,'UTF8') IS NOT NULL AND
            original_size >= octet_length(result_bytes) AND
            truncated = (original_size > octet_length(result_bytes))
        ),
    CONSTRAINT ck_agent_tool_evidence_digest
        CHECK (result_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_agent_tool_evidence_trust
        CHECK (trust_type IN ('local','external')),
    CONSTRAINT ck_agent_tool_evidence_schema
        CHECK (schema_version='vane.agent-tool-evidence/v1')
);

CREATE INDEX idx_agent_tool_evidence_scope_created
    ON agent_tool_evidence
       (tenant_id,user_id,created_at DESC,id DESC);
CREATE INDEX idx_agent_tool_evidence_session_created
    ON agent_tool_evidence
       (tenant_id,user_id,session_id,created_at,id);

-- The composite FK proves owner scope; this trigger proves that the linked
-- observability row is the same invocation/result, not merely a same-user row.
-- +goose StatementBegin
CREATE FUNCTION enforce_agent_tool_evidence_call_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    call_session BIGINT;
    call_trace TEXT;
    call_tool TEXT;
    call_arguments JSONB;
    call_preview TEXT;
    call_result_size INTEGER;
    visible_text TEXT;
    expected_preview TEXT;
BEGIN
    SELECT session_id,trace_id,tool_name,arguments,result_preview,result_size
      INTO call_session,call_trace,call_tool,call_arguments,call_preview,call_result_size
      FROM public.tool_calls
     WHERE id=NEW.tool_call_id
       AND tenant_id=NEW.tenant_id
       AND user_id=NEW.user_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION '085: exact tool call is unavailable';
    END IF;
    visible_text := convert_from(NEW.result_bytes,'UTF8');
    expected_preview := CASE
        WHEN char_length(visible_text)>8192
        THEN substring(visible_text FROM 1 FOR 8192)||'…'
        ELSE visible_text
    END;
    IF call_session IS DISTINCT FROM NEW.session_id OR
       call_trace IS DISTINCT FROM NEW.trace_id OR
       call_tool IS DISTINCT FROM NEW.tool_name OR
       call_arguments IS DISTINCT FROM NEW.arguments OR
       call_result_size IS DISTINCT FROM NEW.original_size OR
       call_preview IS DISTINCT FROM expected_preview THEN
        RAISE EXCEPTION '085: tool call and exact evidence disagree';
    END IF;
    IF NEW.result_digest IS DISTINCT FROM encode(sha256(NEW.result_bytes),'hex') THEN
        RAISE EXCEPTION '085: exact tool evidence digest mismatch';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_agent_tool_evidence_call_v1() FROM PUBLIC;
CREATE TRIGGER agent_tool_evidence_call_v1
BEFORE INSERT ON agent_tool_evidence
FOR EACH ROW EXECUTE FUNCTION enforce_agent_tool_evidence_call_v1();

CREATE TABLE agent_turn_records (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id               BIGINT      NOT NULL REFERENCES users (id),
    session_id            BIGINT      NOT NULL,
    turn_id               TEXT        NOT NULL,
    trace_id              TEXT        NOT NULL,
    user_message          TEXT        NOT NULL,
    assistant_message     TEXT        NOT NULL,
    tool_invocation_ids   TEXT[]      NOT NULL DEFAULT '{}',
    action_receipts       JSONB       NOT NULL DEFAULT '[]',
    schema_version        TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_turn_records_session
        FOREIGN KEY (session_id,tenant_id,user_id)
        REFERENCES agent_sessions (id,tenant_id,user_id),
    CONSTRAINT uq_agent_turn_records_turn
        UNIQUE (tenant_id,user_id,session_id,turn_id),
    CONSTRAINT uq_agent_turn_records_trace
        UNIQUE (tenant_id,user_id,trace_id),
    CONSTRAINT ck_agent_turn_records_turn
        CHECK (btrim(turn_id)=turn_id AND octet_length(turn_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_agent_turn_records_trace
        CHECK (btrim(trace_id)=trace_id AND octet_length(trace_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_agent_turn_records_message_size
        CHECK (
            octet_length(user_message) BETWEEN 1 AND 65536 AND
            octet_length(assistant_message) BETWEEN 1 AND 262144
        ),
    CONSTRAINT ck_agent_turn_records_invocations
        CHECK (cardinality(tool_invocation_ids) <= 64),
    CONSTRAINT ck_agent_turn_records_receipts
        CHECK (jsonb_typeof(action_receipts)='array'),
    CONSTRAINT ck_agent_turn_records_schema
        CHECK (schema_version='vane.agent-turn-record/v1')
);

CREATE INDEX idx_agent_turn_records_scope_created
    ON agent_turn_records
       (tenant_id,user_id,created_at DESC,id DESC);

CREATE TABLE agent_intelligence_query_audits (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id         BIGINT      NOT NULL REFERENCES users (id),
    session_id      BIGINT,
    dataset         TEXT        NOT NULL,
    query_digest    TEXT        NOT NULL,
    query_summary   JSONB       NOT NULL,
    status          TEXT        NOT NULL,
    row_count       INTEGER     NOT NULL DEFAULT 0,
    duration_ms     INTEGER     NOT NULL DEFAULT 0,
    truncated       BOOLEAN     NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_intelligence_query_session
        FOREIGN KEY (session_id,tenant_id,user_id)
        REFERENCES agent_sessions (id,tenant_id,user_id),
    CONSTRAINT fk_agent_intelligence_query_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id),
    CONSTRAINT ck_agent_intelligence_query_dataset
        CHECK (dataset IN (
            'tasks','runs','observations','briefs',
            'agent_turns','tool_calls','profile','invalid'
        )),
    CONSTRAINT ck_agent_intelligence_query_digest
        CHECK (query_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_agent_intelligence_query_summary
        CHECK (jsonb_typeof(query_summary)='object' AND octet_length(query_summary::text)<=16384),
    CONSTRAINT ck_agent_intelligence_query_status
        CHECK (status IN ('completed','rejected','failed','timeout')),
    CONSTRAINT ck_agent_intelligence_query_counts
        CHECK (row_count BETWEEN 0 AND 100 AND duration_ms BETWEEN 0 AND 2147483647)
);

CREATE INDEX idx_agent_intelligence_query_audits_scope_created
    ON agent_intelligence_query_audits
       (tenant_id,user_id,created_at DESC,id DESC);

-- Invalid presented tenant/user pairs cannot satisfy the membership FK of the
-- ordinary user audit. Keep those fail-closed attempts in a separate,
-- owner-only security ledger. It intentionally has no user/session foreign
-- key and is never exposed through the semantic catalog.
CREATE TABLE agent_intelligence_access_denials (
    id                    BIGSERIAL   PRIMARY KEY,
    presented_tenant_id   BIGINT      NOT NULL,
    presented_user_id     BIGINT      NOT NULL,
    dataset               TEXT        NOT NULL,
    query_digest          TEXT        NOT NULL,
    reason                TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT ck_agent_intelligence_denial_identity
        CHECK (presented_tenant_id > 0 AND presented_user_id > 0),
    CONSTRAINT ck_agent_intelligence_denial_dataset
        CHECK (octet_length(dataset) BETWEEN 1 AND 64),
    CONSTRAINT ck_agent_intelligence_denial_digest
        CHECK (query_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_agent_intelligence_denial_reason
        CHECK (reason IN ('membership_mismatch'))
);

CREATE INDEX idx_agent_intelligence_access_denials_created
    ON agent_intelligence_access_denials (created_at DESC,id DESC);
REVOKE ALL ON agent_intelligence_access_denials FROM PUBLIC,vane_app,vane_intelligence_reader;
REVOKE ALL ON SEQUENCE agent_intelligence_access_denials_id_seq
    FROM PUBLIC,vane_app,vane_intelligence_reader;

-- Runtime writes are append-only and column-scoped. The owner remains capable
-- of explicit tenant erasure; ordinary application code cannot mutate evidence.
GRANT SELECT ON agent_tool_evidence,agent_turn_records TO vane_app;
GRANT INSERT (
    tenant_id,user_id,session_id,trace_id,invocation_id,tool_call_id,
    tool_name,arguments,result_bytes,result_digest,original_size,truncated,
    trust_type,schema_version
) ON agent_tool_evidence TO vane_app;
GRANT INSERT (
    tenant_id,user_id,session_id,turn_id,trace_id,user_message,
    assistant_message,tool_invocation_ids,action_receipts,schema_version
) ON agent_turn_records TO vane_app;
GRANT INSERT (
    tenant_id,user_id,session_id,dataset,query_digest,query_summary,status,
    row_count,duration_ms,truncated
) ON agent_intelligence_query_audits TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE
    agent_tool_evidence_id_seq,
    agent_turn_records_id_seq,
    agent_intelligence_query_audits_id_seq
TO vane_app;

GRANT USAGE ON SCHEMA public TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,nl_description,spec_json,status,
              execution_mode,created_at,updated_at)
    ON schedules TO vane_intelligence_reader;
GRANT SELECT (schedule_id,content)
    ON schedule_playbooks TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,task_id,temporal_workflow_id,
              temporal_run_id,run_kind,execution_mode,created_at)
    ON task_run_snapshots TO vane_intelligence_reader;
GRANT SELECT (tenant_id,user_id,run_snapshot_id,status,result,
              source_coverage,processing,failure_code,failure_message,
              finalized_at)
    ON task_run_outcomes TO vane_intelligence_reader;
GRANT SELECT (tenant_id,user_id,task_id,run_snapshot_id,
              invocation_digest,observation_payload,observation_digest,
              content_item_ids,created_at)
    ON task_run_content_provenance TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
              payload,insight_count,generated_at,created_at)
    ON brief_snapshots TO vane_intelligence_reader;
GRANT SELECT ON agent_turn_records,agent_tool_evidence
    TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,session_id,trace_id,tool_name,tool_kind,
              arguments,result_preview,result_size,created_at)
    ON tool_calls TO vane_intelligence_reader;
GRANT SELECT (id,tenant_id,user_id,industry,occupation,tags,removed_tags,
              summary,token_budget_daily,tokens_used_today,updated_at,created_at)
    ON profiles TO vane_intelligence_reader;

ALTER TABLE agent_tool_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_turn_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_intelligence_query_audits ENABLE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'agent_tool_evidence','agent_turn_records',
        'agent_intelligence_query_audits'
    ] LOOP
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',t);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint)',t);
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',t);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down

LOCK TABLE agent_tool_evidence,agent_turn_records,
    agent_intelligence_query_audits,agent_intelligence_access_denials
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_turn_records) OR
       EXISTS (SELECT 1 FROM agent_tool_evidence) OR
       EXISTS (SELECT 1 FROM agent_intelligence_query_audits) OR
       EXISTS (SELECT 1 FROM agent_intelligence_access_denials) THEN
        RAISE EXCEPTION
            '085: refusing downgrade while Agent intelligence evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON SEQUENCE
    agent_tool_evidence_id_seq,
    agent_turn_records_id_seq,
    agent_intelligence_query_audits_id_seq
FROM vane_app;
REVOKE ALL ON agent_tool_evidence,agent_turn_records,
    agent_intelligence_query_audits FROM vane_app;
REVOKE ALL ON schedules,schedule_playbooks,task_run_snapshots,
    task_run_outcomes,task_run_content_provenance,brief_snapshots,
    agent_turn_records,agent_tool_evidence,tool_calls,profiles
FROM vane_intelligence_reader;
REVOKE USAGE ON SCHEMA public FROM vane_intelligence_reader;
REVOKE vane_intelligence_reader FROM CURRENT_USER;
DROP TABLE agent_intelligence_access_denials;
DROP TABLE agent_intelligence_query_audits;
DROP TABLE agent_turn_records;
DROP TRIGGER agent_tool_evidence_call_v1 ON agent_tool_evidence;
DROP FUNCTION enforce_agent_tool_evidence_call_v1();
DROP TABLE agent_tool_evidence;
DROP TABLE agent_intelligence_cursor_keys;
