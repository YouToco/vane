-- 055: Immutable, ledger-anchored shadow context candidates for Agent turns.
--
-- The runtime writes only when B3 says the exact session is ledger
-- authoritative. candidate_snapshot is compiler output; untrusted current-turn
-- source text has already been replaced with a fixed placeholder.

-- +goose Up

CREATE TABLE agent_turn_context_snapshots (
    id                       BIGSERIAL   PRIMARY KEY,
    tenant_id                BIGINT      NOT NULL,
    user_id                  BIGINT      NOT NULL,
    session_id               BIGINT      NOT NULL,
    turn_id                  TEXT        NOT NULL,
    model_step               INTEGER     NOT NULL,
    schema_version           TEXT        NOT NULL,
    compiler_version         TEXT        NOT NULL,
    candidate_digest         TEXT        NOT NULL,
    candidate_snapshot       JSONB       NOT NULL,
    replayable               BOOLEAN     NOT NULL,
    authority_generation     BIGINT      NOT NULL,
    ledger_head_sequence     BIGINT      NOT NULL,
    ledger_head_event_id     BIGINT      NOT NULL,
    ledger_projection_digest TEXT        NOT NULL,
    snapshot_digest          TEXT        NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT fk_agent_turn_context_snapshot_scope
        FOREIGN KEY (session_id, tenant_id, user_id)
        REFERENCES agent_sessions (id, tenant_id, user_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_agent_turn_context_snapshot_step
        UNIQUE (tenant_id, user_id, session_id, turn_id, model_step),
    CONSTRAINT agent_turn_context_snapshot_turn_id_valid
        CHECK (length(turn_id) BETWEEN 1 AND 128),
    CONSTRAINT agent_turn_context_snapshot_model_step_positive
        CHECK (model_step > 0),
    CONSTRAINT agent_turn_context_snapshot_schema_valid
        CHECK (schema_version = 'vane.agent-turn-context-snapshot/v1'),
    CONSTRAINT agent_turn_context_snapshot_compiler_valid
        CHECK (compiler_version = 'vane.agent-context/v1'),
    CONSTRAINT agent_turn_context_snapshot_candidate_digest_valid
        CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT agent_turn_context_snapshot_authority_positive
        CHECK (authority_generation > 0),
    CONSTRAINT agent_turn_context_snapshot_head_positive
        CHECK (ledger_head_sequence > 0 AND ledger_head_event_id > 0),
    CONSTRAINT agent_turn_context_snapshot_projection_digest_valid
        CHECK (ledger_projection_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT agent_turn_context_snapshot_digest_valid
        CHECK (snapshot_digest ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_agent_turn_context_snapshots_session
    ON agent_turn_context_snapshots
       (tenant_id, user_id, session_id, created_at, id);

ALTER TABLE agent_turn_context_snapshots ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON agent_turn_context_snapshots
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON agent_turn_context_snapshots AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id', true)), '')::bigint
    );

GRANT SELECT ON agent_turn_context_snapshots TO vane_app;
GRANT INSERT (
    tenant_id, user_id, session_id, turn_id, model_step,
    schema_version, compiler_version, candidate_digest, candidate_snapshot,
    replayable, authority_generation, ledger_head_sequence,
    ledger_head_event_id, ledger_projection_digest, snapshot_digest
) ON agent_turn_context_snapshots TO vane_app;
GRANT USAGE ON SEQUENCE agent_turn_context_snapshots_id_seq TO vane_app;

-- +goose Down

LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE
    /* migration 055 downgrade fence: session root first */;
LOCK TABLE agent_turn_context_snapshots IN ACCESS EXCLUSIVE MODE
    /* migration 055 downgrade fence: snapshot child second */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_turn_context_snapshots) THEN
        RAISE EXCEPTION
            '055: refusing downgrade while Agent turn context snapshots exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE USAGE ON SEQUENCE agent_turn_context_snapshots_id_seq FROM vane_app;
REVOKE INSERT (
    tenant_id, user_id, session_id, turn_id, model_step,
    schema_version, compiler_version, candidate_digest, candidate_snapshot,
    replayable, authority_generation, ledger_head_sequence,
    ledger_head_event_id, ledger_projection_digest, snapshot_digest
) ON agent_turn_context_snapshots FROM vane_app;
REVOKE SELECT ON agent_turn_context_snapshots FROM vane_app;
DROP POLICY IF EXISTS tenant_isolation ON agent_turn_context_snapshots;
DROP POLICY IF EXISTS tenant_visible ON agent_turn_context_snapshots;
ALTER TABLE agent_turn_context_snapshots DISABLE ROW LEVEL SECURITY;
DROP TABLE agent_turn_context_snapshots;
