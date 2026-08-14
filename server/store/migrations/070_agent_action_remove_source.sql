-- 070: extend exact-action durable continuation to DB-local remove_source.
--
-- remove_source keeps its complete, deduplicated 1..20 target set in the
-- existing canonical_args bytes. source_id remains the first target as a
-- backwards-compatible scalar carrier; the retained enable_source adapter is
-- unchanged.

-- +goose Up

SELECT pg_advisory_xact_lock(1447120453,1095976528)
    /* serialize durable Agent action schema admission */;
LOCK TABLE agent_action_continuations IN ACCESS EXCLUSIVE MODE;

ALTER TABLE agent_action_continuations
    DROP CONSTRAINT agent_action_continuation_identity_valid,
    DROP CONSTRAINT agent_action_continuation_versions_valid,
    DROP CONSTRAINT agent_action_continuation_terminal_valid;

ALTER TABLE agent_action_continuations
    ADD CONSTRAINT agent_action_continuation_identity_valid CHECK (
        tool_name IN ('enable_source','remove_source')
        AND source_id > 0
        AND octet_length(action_id) BETWEEN 1 AND 255
    ),
    ADD CONSTRAINT agent_action_continuation_versions_valid CHECK (
        tool_spec_version = 'vane.agent-tool-spec/v1'
        AND tool_policy_version = 'vane.agent-tool-policy/v1'
        AND (
            (tool_name = 'enable_source'
                AND adapter_version = 'vane.enable-source/postgres/v1')
            OR
            (tool_name = 'remove_source'
                AND adapter_version = 'vane.remove-source/postgres/v1')
        )
    ),
    ADD CONSTRAINT agent_action_continuation_terminal_valid CHECK (
        (status = 'completed'
            AND (
                (tool_name = 'enable_source'
                    AND terminal_code IN ('enabled','not_found'))
                OR
                (tool_name = 'remove_source'
                    AND terminal_code IN ('removed','not_subscribed'))
            )
            AND confirmed_at IS NOT NULL AND completed_at IS NOT NULL
            AND blocked_reason IS NULL)
        OR
        (status = 'confirmed' AND terminal_code IS NULL
            AND confirmed_at IS NOT NULL AND completed_at IS NULL
            AND blocked_reason IS NULL)
        OR
        (status = 'blocked' AND terminal_code IS NULL
            AND confirmed_at IS NOT NULL AND completed_at IS NULL
            AND blocked_reason IS NOT NULL)
        OR
        (status IN ('pending','cancelled','expired','rolled_back')
            AND terminal_code IS NULL AND completed_at IS NULL
            AND confirmed_at IS NULL AND blocked_reason IS NULL)
    );

GRANT SELECT (tool_name)
    ON agent_action_continuations TO vane_agent_action_proposer;
GRANT DELETE ON subscriptions TO vane_agent_action_continuator;

-- +goose Down

SELECT pg_advisory_xact_lock(1447120453,1095976528)
    /* serialize durable Agent action downgrade admission */;
LOCK TABLE pending_actions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE agent_action_continuations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE subscriptions IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM agent_action_continuations
         WHERE tool_name <> 'enable_source'
    ) OR EXISTS (
        SELECT 1 FROM pending_actions
         WHERE execution_version=2 AND tool_name <> 'enable_source'
    ) THEN
        RAISE EXCEPTION
            '070: refusing downgrade while remove_source durable actions exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE DELETE ON subscriptions FROM vane_agent_action_continuator;
REVOKE SELECT (tool_name)
    ON agent_action_continuations FROM vane_agent_action_proposer;

ALTER TABLE agent_action_continuations
    DROP CONSTRAINT agent_action_continuation_identity_valid,
    DROP CONSTRAINT agent_action_continuation_versions_valid,
    DROP CONSTRAINT agent_action_continuation_terminal_valid;

ALTER TABLE agent_action_continuations
    ADD CONSTRAINT agent_action_continuation_identity_valid CHECK (
        tool_name = 'enable_source'
        AND source_id > 0
        AND octet_length(action_id) BETWEEN 1 AND 255
    ),
    ADD CONSTRAINT agent_action_continuation_versions_valid CHECK (
        tool_spec_version = 'vane.agent-tool-spec/v1'
        AND tool_policy_version = 'vane.agent-tool-policy/v1'
        AND adapter_version = 'vane.enable-source/postgres/v1'
    ),
    ADD CONSTRAINT agent_action_continuation_terminal_valid CHECK (
        (status = 'completed'
            AND terminal_code IN ('enabled','not_found')
            AND confirmed_at IS NOT NULL AND completed_at IS NOT NULL
            AND blocked_reason IS NULL)
        OR
        (status = 'confirmed' AND terminal_code IS NULL
            AND confirmed_at IS NOT NULL AND completed_at IS NULL
            AND blocked_reason IS NULL)
        OR
        (status = 'blocked' AND terminal_code IS NULL
            AND confirmed_at IS NOT NULL AND completed_at IS NULL
            AND blocked_reason IS NOT NULL)
        OR
        (status IN ('pending','cancelled','expired','rolled_back')
            AND terminal_code IS NULL AND completed_at IS NULL
            AND confirmed_at IS NULL AND blocked_reason IS NULL)
    );
