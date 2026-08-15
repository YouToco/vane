-- 138: team-workspace long-term memory ledger.
--
-- This is deliberately separate from v129's (tenant_id,user_id) personal
-- ledger. A team recall can never UNION or fall back to personal memory; its
-- only corpus is workspace_memory_records for the exact workspace tenant.

-- +goose Up

CREATE TABLE workspace_memory_authorizations (
    id                   UUID PRIMARY KEY,
    tenant_id            BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    actor_user_id        BIGINT NOT NULL REFERENCES users(id),
    actor_role           TEXT NOT NULL,
    workspace_kind       TEXT NOT NULL,
    action_kind          TEXT NOT NULL,
    target_memory_id     BIGINT,
    target_creator_user_id BIGINT,
    session_id           BIGINT NOT NULL,
    trace_id             UUID NOT NULL,
    owner_request        TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    request_digest       TEXT NOT NULL,
    consumed_event_id    BIGINT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,actor_user_id,id),
    UNIQUE (tenant_id,actor_user_id,session_id,trace_id,request_digest),
    FOREIGN KEY (session_id,tenant_id,actor_user_id)
        REFERENCES agent_sessions(id,tenant_id,user_id),
    CHECK (octet_length(owner_request) BETWEEN 1 AND 65536),
    CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
    CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CHECK (actor_role IN ('owner','admin','member')),
    CHECK (workspace_kind='team'),
    CHECK (action_kind IN ('remember','correct','forget')),
    CHECK ((action_kind='remember' AND target_memory_id IS NULL
              AND target_creator_user_id IS NULL) OR
           (action_kind IN ('correct','forget') AND target_memory_id IS NOT NULL
              AND target_creator_user_id IS NOT NULL)),
    FOREIGN KEY (target_creator_user_id) REFERENCES users(id)
);

CREATE TABLE workspace_memory_records (
    id                   BIGSERIAL PRIMARY KEY,
    tenant_id            BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    creator_user_id      BIGINT NOT NULL REFERENCES users(id),
    created_by_user_id   BIGINT NOT NULL REFERENCES users(id),
    memory_text          TEXT NOT NULL,
    evidence_source_type TEXT NOT NULL,
    evidence_source_id   UUID NOT NULL,
    authorization_id     UUID NOT NULL,
    owner_request        TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    supersedes_memory_id BIGINT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,id),
    UNIQUE (tenant_id,id,creator_user_id),
    FOREIGN KEY (tenant_id,created_by_user_id,authorization_id)
        REFERENCES workspace_memory_authorizations(tenant_id,actor_user_id,id),
    FOREIGN KEY (tenant_id,supersedes_memory_id)
        REFERENCES workspace_memory_records(tenant_id,id),
    CHECK (octet_length(memory_text) BETWEEN 1 AND 4096),
    CHECK (evidence_source_type='owner_explicit_agent_turn'),
    CHECK (octet_length(owner_request) BETWEEN 1 AND 65536),
    CHECK (authorization_digest ~ '^[0-9a-f]{64}$')
);
CREATE INDEX idx_workspace_memory_records_scope
    ON workspace_memory_records(tenant_id,id);

ALTER TABLE workspace_memory_authorizations
    ADD CONSTRAINT fk_workspace_memory_authorization_target
    FOREIGN KEY (tenant_id,target_memory_id,target_creator_user_id)
    REFERENCES workspace_memory_records(tenant_id,id,creator_user_id);

CREATE TABLE workspace_memory_events (
    id                   BIGSERIAL PRIMARY KEY,
    tenant_id            BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    actor_user_id        BIGINT NOT NULL REFERENCES users(id),
    actor_role           TEXT NOT NULL,
    event_kind           TEXT NOT NULL,
    target_memory_id     BIGINT,
    result_memory_id     BIGINT,
    evidence_source_type TEXT NOT NULL,
    evidence_source_id   UUID NOT NULL,
    authorization_id     UUID NOT NULL,
    owner_request        TEXT NOT NULL,
    authorization_digest TEXT NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,id),
    UNIQUE (tenant_id,actor_user_id,id),
    FOREIGN KEY (tenant_id,actor_user_id,authorization_id)
        REFERENCES workspace_memory_authorizations(tenant_id,actor_user_id,id),
    FOREIGN KEY (tenant_id,target_memory_id)
        REFERENCES workspace_memory_records(tenant_id,id),
    FOREIGN KEY (tenant_id,result_memory_id)
        REFERENCES workspace_memory_records(tenant_id,id),
    CHECK (event_kind IN ('remember','correct','forget')),
    CHECK (
      (event_kind='remember' AND target_memory_id IS NULL AND result_memory_id IS NOT NULL) OR
      (event_kind='correct' AND target_memory_id IS NOT NULL AND result_memory_id IS NOT NULL) OR
      (event_kind='forget' AND target_memory_id IS NOT NULL AND result_memory_id IS NULL)
    ),
    CHECK (evidence_source_type='owner_explicit_agent_turn'),
    CHECK (octet_length(owner_request) BETWEEN 1 AND 65536),
    CHECK (authorization_digest ~ '^[0-9a-f]{64}$'),
    CHECK (actor_role IN ('owner','admin','member'))
);
CREATE UNIQUE INDEX uq_workspace_memory_event_result_once
    ON workspace_memory_events(tenant_id,result_memory_id)
    WHERE result_memory_id IS NOT NULL;
CREATE UNIQUE INDEX uq_workspace_memory_event_target_once
    ON workspace_memory_events(tenant_id,target_memory_id)
    WHERE target_memory_id IS NOT NULL;
CREATE INDEX idx_workspace_memory_events_scope
    ON workspace_memory_events(tenant_id,id);

ALTER TABLE workspace_memory_authorizations
    ADD CONSTRAINT fk_workspace_memory_authorization_consumed
    FOREIGN KEY (tenant_id,actor_user_id,consumed_event_id)
    REFERENCES workspace_memory_events(tenant_id,actor_user_id,id);
CREATE UNIQUE INDEX uq_workspace_memory_authorization_consumed
    ON workspace_memory_authorizations(tenant_id,consumed_event_id)
    WHERE consumed_event_id IS NOT NULL;

CREATE TABLE workspace_memory_receipts (
    tenant_id       BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    actor_user_id   BIGINT NOT NULL REFERENCES users(id),
    idempotency_key TEXT NOT NULL,
    request_digest  TEXT NOT NULL,
    event_id        BIGINT NOT NULL,
    response_payload JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,actor_user_id,idempotency_key),
    FOREIGN KEY (tenant_id,actor_user_id,event_id)
        REFERENCES workspace_memory_events(tenant_id,actor_user_id,id),
    CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
    CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(response_payload)='object')
);

-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_workspace_memory_editor') THEN
    CREATE ROLE vane_workspace_memory_editor NOLOGIN NOINHERIT NOBYPASSRLS
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    ALTER ROLE vane_workspace_memory_editor
      SET search_path=pg_catalog,public,pg_temp;
  END IF;
  GRANT vane_workspace_memory_editor TO CURRENT_USER
    WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
END $$;
-- +goose StatementEnd

REVOKE ALL ON workspace_memory_authorizations,workspace_memory_records,
  workspace_memory_events,workspace_memory_receipts
  FROM PUBLIC,vane_app,vane_memory_editor,vane_workspace_memory_editor;
REVOKE ALL ON SEQUENCE workspace_memory_records_id_seq,
  workspace_memory_events_id_seq
  FROM PUBLIC,vane_app,vane_memory_editor,vane_workspace_memory_editor;

ALTER TABLE workspace_memory_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_authorizations FORCE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_records FORCE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_events FORCE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_memory_receipts FORCE ROW LEVEL SECURITY;

CREATE POLICY workspace_memory_authorization_actor ON workspace_memory_authorizations
  TO vane_workspace_memory_editor
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         actor_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND
         actor_role=NULLIF(current_setting('app.membership_role',true),'') AND
         workspace_kind=current_setting('app.workspace_kind',true) AND workspace_kind='team')
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              actor_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND
              actor_role=NULLIF(current_setting('app.membership_role',true),'') AND
              workspace_kind=current_setting('app.workspace_kind',true) AND workspace_kind='team');
CREATE POLICY workspace_memory_record_tenant ON workspace_memory_records
  TO vane_workspace_memory_editor
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         current_setting('app.workspace_kind',true)='team')
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              created_by_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND
              current_setting('app.workspace_kind',true)='team' AND (
                (supersedes_memory_id IS NULL AND creator_user_id=created_by_user_id) OR
                EXISTS(SELECT 1 FROM workspace_memory_authorizations authz
                  WHERE authz.tenant_id=workspace_memory_records.tenant_id
                    AND authz.actor_user_id=workspace_memory_records.created_by_user_id
                    AND authz.id=workspace_memory_records.authorization_id
                    AND authz.action_kind='correct'
                    AND authz.target_memory_id=workspace_memory_records.supersedes_memory_id
                    AND authz.target_creator_user_id=workspace_memory_records.creator_user_id)));
CREATE POLICY workspace_memory_event_tenant ON workspace_memory_events
  TO vane_workspace_memory_editor
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         current_setting('app.workspace_kind',true)='team')
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              actor_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND
              actor_role=NULLIF(current_setting('app.membership_role',true),'') AND
              current_setting('app.workspace_kind',true)='team' AND
              EXISTS(SELECT 1 FROM workspace_memory_authorizations authz
                WHERE authz.tenant_id=workspace_memory_events.tenant_id
                  AND authz.actor_user_id=workspace_memory_events.actor_user_id
                  AND authz.id=workspace_memory_events.authorization_id
                  AND authz.actor_role=workspace_memory_events.actor_role
                  AND authz.action_kind=workspace_memory_events.event_kind
                  AND authz.target_memory_id IS NOT DISTINCT FROM
                      workspace_memory_events.target_memory_id) AND
              (result_memory_id IS NULL OR EXISTS(
                SELECT 1 FROM workspace_memory_records record
                 WHERE record.tenant_id=workspace_memory_events.tenant_id
                   AND record.id=workspace_memory_events.result_memory_id
                   AND record.authorization_id=workspace_memory_events.authorization_id)));
CREATE POLICY workspace_memory_receipt_actor ON workspace_memory_receipts
  TO vane_workspace_memory_editor
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         actor_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND
         current_setting('app.workspace_kind',true)='team')
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              actor_user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND
              current_setting('app.workspace_kind',true)='team');

GRANT USAGE ON SCHEMA public TO vane_workspace_memory_editor;
GRANT SELECT,INSERT ON workspace_memory_records,workspace_memory_events,
  workspace_memory_receipts TO vane_workspace_memory_editor;
GRANT SELECT,INSERT,UPDATE(consumed_event_id)
  ON workspace_memory_authorizations TO vane_workspace_memory_editor;
GRANT USAGE,SELECT ON SEQUENCE workspace_memory_records_id_seq,
  workspace_memory_events_id_seq TO vane_workspace_memory_editor;

-- +goose StatementBegin
CREATE FUNCTION assert_vane_workspace_memory_editor_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles
    WHERE rolname='vane_workspace_memory_editor' AND NOT rolcanlogin
      AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolinherit AND NOT rolreplication AND NOT rolbypassrls) THEN
    RAISE EXCEPTION '138: workspace memory editor role is absent or unsafe';
  END IF;
  IF pg_has_role('vane_workspace_memory_editor','vane_app','MEMBER') OR
     pg_has_role('vane_app','vane_workspace_memory_editor','MEMBER') OR
     pg_has_role('vane_workspace_memory_editor','vane_memory_editor','MEMBER') OR
     pg_has_role('vane_memory_editor','vane_workspace_memory_editor','MEMBER') THEN
    RAISE EXCEPTION '138: workspace memory role must be unrelated';
  END IF;
  IF has_table_privilege('vane_workspace_memory_editor','workspace_memory_records','UPDATE') OR
     has_table_privilege('vane_workspace_memory_editor','workspace_memory_records','DELETE') OR
     has_table_privilege('vane_workspace_memory_editor','workspace_memory_events','UPDATE') OR
     has_table_privilege('vane_workspace_memory_editor','workspace_memory_events','DELETE') OR
     has_table_privilege('vane_workspace_memory_editor','workspace_memory_receipts','UPDATE') OR
     has_table_privilege('vane_workspace_memory_editor','workspace_memory_receipts','DELETE') THEN
    RAISE EXCEPTION '138: workspace memory history is not append-only';
  END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION assert_vane_workspace_memory_editor_v138() FROM PUBLIC;
SELECT assert_vane_workspace_memory_editor_v138();

-- +goose StatementBegin
CREATE FUNCTION provision_vane_server_runtime_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '138: only direct migration owner may provision runtime';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.provision_vane_server_runtime_v129();
  GRANT vane_workspace_memory_editor TO vane_server_runtime
    WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
  PERFORM public.assert_vane_workspace_memory_editor_v138();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION provision_vane_server_runtime_v138() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION deprovision_vane_server_runtime_v138() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '138: only direct migration owner may deprovision runtime';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_server_runtime') THEN
    REVOKE vane_workspace_memory_editor FROM vane_server_runtime;
  END IF;
  PERFORM public.deprovision_vane_server_runtime_v129();
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION deprovision_vane_server_runtime_v138() FROM PUBLIC;

-- +goose Down

LOCK TABLE workspace_memory_receipts,workspace_memory_events,
  workspace_memory_records,workspace_memory_authorizations IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM workspace_memory_authorizations) OR
     EXISTS (SELECT 1 FROM workspace_memory_records) OR
     EXISTS (SELECT 1 FROM workspace_memory_events) OR
     EXISTS (SELECT 1 FROM workspace_memory_receipts) THEN
    RAISE EXCEPTION '138: refusing downgrade while retained workspace memory exists';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_server_runtime') THEN
    RAISE EXCEPTION '138: deprovision vane_server_runtime before schema downgrade';
  END IF;
END $$;
-- +goose StatementEnd
DROP FUNCTION deprovision_vane_server_runtime_v138();
DROP FUNCTION provision_vane_server_runtime_v138();
DROP FUNCTION assert_vane_workspace_memory_editor_v138();
DROP TABLE workspace_memory_receipts;
ALTER TABLE workspace_memory_authorizations
  DROP CONSTRAINT fk_workspace_memory_authorization_consumed;
DROP TABLE workspace_memory_events;
ALTER TABLE workspace_memory_authorizations
  DROP CONSTRAINT fk_workspace_memory_authorization_target;
DROP TABLE workspace_memory_records;
DROP TABLE workspace_memory_authorizations;
REVOKE USAGE ON SCHEMA public FROM vane_workspace_memory_editor;
