-- 035: Agent 语义事件账本（7.7-A，零生产调用点）。
--
-- agent_sessions.messages 仍是现行主读投影；本表只冻结未来双写/projector 所需的
-- append-only 地基。事件不接管 task creation、definition edit 或 Temporal 真相。

-- +goose Up

CREATE TABLE agent_events (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id               BIGINT      NOT NULL REFERENCES users (id),
    session_id            BIGINT      NOT NULL,
    sequence              BIGINT      NOT NULL,
    batch_idempotency_key TEXT        NOT NULL,
    batch_index           INT         NOT NULL,
    batch_size            INT         NOT NULL,
    kind                  TEXT        NOT NULL,
    schema_version        TEXT        NOT NULL,
    payload               BYTEA       NOT NULL,
    payload_digest        TEXT        NOT NULL,
    batch_digest          TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_events_session_scope
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id),
    CONSTRAINT uq_agent_events_session_sequence
        UNIQUE (tenant_id, user_id, session_id, sequence),
    CONSTRAINT uq_agent_events_session_batch_index
        UNIQUE (tenant_id, user_id, session_id, batch_idempotency_key, batch_index),
    CONSTRAINT agent_events_sequence_positive CHECK (sequence > 0),
    CONSTRAINT agent_events_batch_index_valid
        CHECK (batch_size BETWEEN 1 AND 64 AND batch_index BETWEEN 0 AND batch_size - 1),
    CONSTRAINT agent_events_idempotency_key_valid CHECK (
        batch_idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$'
    ),
    CONSTRAINT agent_events_kind_valid CHECK (
        kind IN (
            'turn_started',
            'user_message',
            'assistant_message',
            'tool_call',
            'tool_result',
            'confirmation_requested',
            'confirmation_resolved',
            'turn_completed'
        )
    ),
    CONSTRAINT agent_events_schema_version_valid
        CHECK (schema_version = 'vane.agent-event/v1'),
    CONSTRAINT agent_events_payload_size_valid
        CHECK (octet_length(payload) > 0 AND octet_length(payload) <= 263168),
    CONSTRAINT agent_events_payload_digest_valid
        CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT agent_events_batch_digest_valid
        CHECK (batch_digest ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_agent_events_tenant_user_session_sequence
    ON agent_events (tenant_id, user_id, session_id, sequence);

-- Append-only at the runtime role: callers may supply only immutable event
-- fields. Database identity/time stay server-owned; mutation, deletion, and
-- TRUNCATE remain unavailable.
GRANT SELECT ON agent_events TO vane_app;
GRANT INSERT (
    tenant_id, user_id, session_id, sequence,
    batch_idempotency_key, batch_index, batch_size,
    kind, schema_version, payload, payload_digest, batch_digest
) ON agent_events TO vane_app;
GRANT USAGE ON SEQUENCE agent_events_id_seq TO vane_app;

ALTER TABLE agent_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON agent_events
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON agent_events AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint);

-- +goose Down

-- Once dual-write begins, event rows are non-regenerable audit history. Empty
-- development databases may downgrade; non-empty ledgers must fail atomically.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_events) THEN
        RAISE EXCEPTION '035: refusing downgrade while agent events exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS tenant_isolation ON agent_events;
DROP POLICY IF EXISTS tenant_visible ON agent_events;
ALTER TABLE agent_events DISABLE ROW LEVEL SECURITY;
REVOKE ALL ON SEQUENCE agent_events_id_seq FROM vane_app;
REVOKE ALL ON agent_events FROM vane_app;
DROP TABLE agent_events;
