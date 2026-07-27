-- 059: least-privilege producer for newly-created durable Agent actions.
--
-- The proposer writes the v2 pending root, its frozen continuation and the
-- generation-1 authority event in one transaction. It cannot promote legacy
-- roots, update an existing action, execute an effect, project a session, or
-- reach a provider.

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname='vane_agent_action_proposer'
    ) THEN
        BEGIN
            CREATE ROLE vane_agent_action_proposer
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_agent_action_proposer
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_agent_action_proposer RESET ALL;
ALTER ROLE vane_agent_action_proposer
    SET search_path=pg_catalog,public;
GRANT vane_agent_action_proposer TO CURRENT_USER;

-- +goose StatementBegin
DO $$
DECLARE peer text;
BEGIN
    FOREACH peer IN ARRAY ARRAY[
        'vane_app',
        'vane_agent_action_operator',
        'vane_agent_action_continuator'
    ] LOOP
        IF pg_has_role(
               'vane_agent_action_proposer',peer,'MEMBER'
           ) OR pg_has_role(
               peer,'vane_agent_action_proposer','MEMBER'
           ) THEN
            RAISE EXCEPTION
                '059: action proposer and % must be unrelated',peer;
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_agent_action_proposer'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION
            '059: only migration owner may enter action proposer';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_agent_action_proposer'
    ) THEN
        RAISE EXCEPTION
            '059: action proposer must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO vane_agent_action_proposer;

GRANT SELECT (
    id,tenant_id,user_id,session_id,tool_name,args,summary,status,
    expires_at,execution_version
) ON pending_actions TO vane_agent_action_proposer;
GRANT INSERT (
    id,tenant_id,user_id,session_id,tool_name,args,summary,status,
    expires_at,execution_version
) ON pending_actions TO vane_agent_action_proposer;

GRANT SELECT (
    action_id,tenant_id,user_id,session_id,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest,
    status,terminal_code,lease_owner,lease_fence,lease_expires_at,
    attempt_count,next_attempt_at,confirmed_at,completed_at,blocked_reason
) ON agent_action_continuations TO vane_agent_action_proposer;
GRANT INSERT (
    action_id,tenant_id,user_id,session_id,tool_name,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest,
    next_attempt_at
) ON agent_action_continuations TO vane_agent_action_proposer;

GRANT SELECT (
    action_id,generation,mode,evidence
) ON agent_action_continuation_authority_events
    TO vane_agent_action_proposer;
GRANT INSERT (
    tenant_id,user_id,action_id,generation,mode,evidence
) ON agent_action_continuation_authority_events
    TO vane_agent_action_proposer;
GRANT USAGE ON SEQUENCE
    agent_action_continuation_authority_events_id_seq
    TO vane_agent_action_proposer;

-- +goose Down

REVOKE USAGE ON SEQUENCE
    agent_action_continuation_authority_events_id_seq
    FROM vane_agent_action_proposer;
REVOKE INSERT (
    tenant_id,user_id,action_id,generation,mode,evidence
) ON agent_action_continuation_authority_events
    FROM vane_agent_action_proposer;
REVOKE SELECT (
    action_id,generation,mode,evidence
) ON agent_action_continuation_authority_events
    FROM vane_agent_action_proposer;

REVOKE INSERT (
    action_id,tenant_id,user_id,session_id,tool_name,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest,
    next_attempt_at
) ON agent_action_continuations FROM vane_agent_action_proposer;
REVOKE SELECT (
    action_id,tenant_id,user_id,session_id,source_id,
    canonical_args,args_digest,tool_spec_version,tool_spec,tool_spec_digest,
    tool_policy_version,tool_policy,tool_policy_digest,adapter_version,
    success_messages,success_digest,not_found_messages,not_found_digest,
    status,terminal_code,lease_owner,lease_fence,lease_expires_at,
    attempt_count,next_attempt_at,confirmed_at,completed_at,blocked_reason
) ON agent_action_continuations FROM vane_agent_action_proposer;

REVOKE INSERT (
    id,tenant_id,user_id,session_id,tool_name,args,summary,status,
    expires_at,execution_version
) ON pending_actions FROM vane_agent_action_proposer;
REVOKE SELECT (
    id,tenant_id,user_id,session_id,tool_name,args,summary,status,
    expires_at,execution_version
) ON pending_actions FROM vane_agent_action_proposer;

REVOKE USAGE ON SCHEMA public FROM vane_agent_action_proposer;
