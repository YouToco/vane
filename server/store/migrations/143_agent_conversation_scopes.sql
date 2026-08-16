-- 143: isolate Agent history by authenticated conversation route.
--
-- Legacy Web and Feishu owner conversations retain the `owner` scope. New
-- channel adapters use an internal route-derived scope, never provider actor,
-- chat, or thread identifiers. This prevents private history from entering a
-- group/topic reply while preserving the existing owner conversation contract.

-- +goose Up

ALTER TABLE agent_sessions
    ADD COLUMN conversation_scope TEXT NOT NULL DEFAULT 'owner';

ALTER TABLE agent_sessions
    ADD CONSTRAINT ck_agent_session_conversation_scope CHECK (
        conversation_scope ~ '^[a-z][a-z0-9:_-]{0,127}$'
    );

DROP INDEX idx_agent_sessions_user_status;
CREATE INDEX idx_agent_sessions_user_scope_status
    ON agent_sessions (user_id,conversation_scope,status,updated_at DESC);

-- +goose Down

LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM agent_sessions WHERE conversation_scope <> 'owner'
    ) THEN
        RAISE EXCEPTION
            'migration 143 down refused: routed Agent session history exists';
    END IF;
END
$$;
-- +goose StatementEnd

DROP INDEX idx_agent_sessions_user_scope_status;
CREATE INDEX idx_agent_sessions_user_status
    ON agent_sessions (user_id,status,updated_at DESC);
ALTER TABLE agent_sessions
    DROP CONSTRAINT ck_agent_session_conversation_scope,
    DROP COLUMN conversation_scope;
