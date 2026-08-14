-- 101: durable exact-task authority and recovery journal for the V3 Schedule
-- Action cutover.  This migration does not enable a task: the authority row
-- is staged first and becomes enabled only after the CAS-swapped Schedule has
-- been described in its original active state.

-- +goose Up

CREATE TABLE research_v3_delivery_authorities (
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    task_id              TEXT        NOT NULL,
    generation           BIGINT      NOT NULL,
    definition_version   BIGINT      NOT NULL,
    definition_digest    TEXT        NOT NULL,
    target_action_digest TEXT        NOT NULL,
    action_authorization_digest TEXT NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'staged',
    enabled_at           TIMESTAMPTZ,
    revoked_at           TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,task_id,generation),
    CONSTRAINT ck_research_v3_authority_identity CHECK (
        tenant_id>0 AND user_id>0 AND generation>0 AND
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        definition_version>0 AND
        definition_digest ~ '^[0-9a-f]{64}$' AND
        target_action_digest ~ '^[0-9a-f]{64}$' AND
        action_authorization_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_research_v3_authority_status CHECK (
        (status='staged' AND enabled_at IS NULL AND revoked_at IS NULL) OR
        (status='enabled' AND enabled_at IS NOT NULL AND revoked_at IS NULL) OR
        (status='revoked' AND revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX uq_research_v3_authority_live_task
    ON research_v3_delivery_authorities (tenant_id,user_id,task_id)
    WHERE status IN ('staged','enabled');

CREATE TABLE research_v3_cutover_operations (
    id                     BIGSERIAL   PRIMARY KEY,
    tenant_id              BIGINT      NOT NULL,
    user_id                BIGINT      NOT NULL,
    task_id                TEXT        NOT NULL,
    idempotency_key        TEXT        NOT NULL,
    generation             BIGINT      NOT NULL,
    definition_version     BIGINT      NOT NULL,
    definition_digest      TEXT        NOT NULL,
    frozen_schedule        BYTEA       NOT NULL,
    frozen_schedule_digest TEXT        NOT NULL,
    frozen_conflict_token  BYTEA       NOT NULL,
    conflict_token_digest  TEXT        NOT NULL,
    rollback_conflict_token BYTEA,
    rollback_token_digest   TEXT,
    target_action          BYTEA       NOT NULL,
    target_action_digest   TEXT        NOT NULL,
    action_authorization_digest TEXT   NOT NULL,
    original_paused        BOOLEAN     NOT NULL,
    phase                  TEXT        NOT NULL DEFAULT 'prepared',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_research_v3_cutover_key
        UNIQUE (tenant_id,user_id,task_id,idempotency_key),
    CONSTRAINT fk_research_v3_cutover_authority
        FOREIGN KEY (tenant_id,user_id,task_id,generation)
        REFERENCES research_v3_delivery_authorities
            (tenant_id,user_id,task_id,generation) ON DELETE RESTRICT,
    CONSTRAINT ck_research_v3_cutover_identity CHECK (
        tenant_id>0 AND user_id>0 AND generation>0 AND
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(idempotency_key)=idempotency_key AND
        octet_length(idempotency_key) BETWEEN 1 AND 512 AND
        definition_version>0 AND
        definition_digest ~ '^[0-9a-f]{64}$' AND
        frozen_schedule_digest ~ '^[0-9a-f]{64}$' AND
        conflict_token_digest ~ '^[0-9a-f]{64}$' AND
        target_action_digest ~ '^[0-9a-f]{64}$' AND
        action_authorization_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_research_v3_cutover_payload CHECK (
        octet_length(frozen_schedule) BETWEEN 1 AND 1048576 AND
        octet_length(frozen_conflict_token) BETWEEN 1 AND 4096 AND
        (rollback_conflict_token IS NULL OR
         octet_length(rollback_conflict_token) BETWEEN 1 AND 4096) AND
        ((rollback_conflict_token IS NULL AND rollback_token_digest IS NULL) OR
         (rollback_conflict_token IS NOT NULL AND
          rollback_token_digest ~ '^[0-9a-f]{64}$' AND
          rollback_token_digest=encode(sha256(rollback_conflict_token),'hex'))) AND
        octet_length(target_action) BETWEEN 1 AND 524288),
    CONSTRAINT ck_research_v3_cutover_phase CHECK (phase IN (
        'prepared','pause_requested','paused','action_swapped','active',
        'rollback_pause_requested','rollback_paused','rolled_back','aborted',
        'manual_intervention'))
);

-- Only the dedicated non-login coordinator role may stage or transition the
-- saga. Ordinary application code gets read-only evidence for same-transaction
-- delivery admission and diagnostics.
-- +goose StatementBegin
DO $$
DECLARE membership RECORD;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles
                    WHERE rolname='vane_research_v3_cutover_operator') THEN
        CREATE ROLE vane_research_v3_cutover_operator NOLOGIN NOSUPERUSER
            NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    ALTER ROLE vane_research_v3_cutover_operator NOLOGIN NOSUPERUSER
        NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    -- A pre-created role or stale deployment membership must not silently
    -- grant the long-lived server (or any other principal) cutover authority.
    FOR membership IN
        SELECT granted.rolname AS granted_name,member.rolname AS member_name
          FROM pg_auth_members edge
          JOIN pg_roles granted ON granted.oid=edge.roleid
          JOIN pg_roles member ON member.oid=edge.member
         WHERE (granted.rolname='vane_research_v3_cutover_operator'
                AND member.rolname<>current_user)
            OR (member.rolname='vane_research_v3_cutover_operator')
    LOOP
        EXECUTE format('REVOKE %I FROM %I',
                       membership.granted_name,membership.member_name);
    END LOOP;
    EXECUTE format('GRANT vane_research_v3_cutover_operator TO %I',current_user);
END $$;
-- +goose StatementEnd

-- Ordinary application and delivery code needs only the live authority row.
-- The cutover journal contains frozen Schedule bytes, conflict tokens and the
-- formal Action bearer token, so it remains visible only to the one-shot
-- migration-owner/operator capability below.
GRANT SELECT (
    tenant_id,user_id,task_id,generation,definition_version,
    definition_digest,target_action_digest,action_authorization_digest,status
) ON research_v3_delivery_authorities TO vane_app;
GRANT SELECT ON research_v3_delivery_authorities
    TO vane_push_effect_coordinator;
GRANT SELECT (
    tenant_id,user_id,id,status,execution_mode,
    approved_definition_version,approved_definition_digest
) ON schedules TO vane_push_effect_coordinator;
GRANT SELECT ON research_v3_delivery_authorities,
                research_v3_cutover_operations
    TO vane_research_v3_cutover_operator;
GRANT SELECT ON schedules,tenants,memberships,
                task_approved_definition_versions,task_run_snapshots
    TO vane_research_v3_cutover_operator;
GRANT INSERT (
    tenant_id,user_id,task_id,generation,definition_version,
    definition_digest,target_action_digest,action_authorization_digest,status
) ON research_v3_delivery_authorities TO vane_research_v3_cutover_operator;
GRANT UPDATE (status,enabled_at,revoked_at)
    ON research_v3_delivery_authorities TO vane_research_v3_cutover_operator;
GRANT INSERT (
    tenant_id,user_id,task_id,idempotency_key,generation,
    definition_version,definition_digest,frozen_schedule,
    frozen_schedule_digest,frozen_conflict_token,conflict_token_digest,
    target_action,target_action_digest,action_authorization_digest,
    original_paused,phase
) ON research_v3_cutover_operations TO vane_research_v3_cutover_operator;
GRANT UPDATE (phase,rollback_conflict_token,rollback_token_digest)
    ON research_v3_cutover_operations TO vane_research_v3_cutover_operator;
GRANT USAGE,SELECT ON SEQUENCE research_v3_cutover_operations_id_seq
    TO vane_research_v3_cutover_operator;

ALTER TABLE research_v3_delivery_authorities ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_v3_cutover_operations ENABLE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_v3_delivery_authorities','research_v3_cutover_operations'
    ] LOOP
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',
            relation_name);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint)',
            relation_name);
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',
            relation_name);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION protect_research_v3_authority_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    -- Claim admission holds this exact-task transaction lock through the
    -- push_effect transition. Enable/revoke therefore serializes with claim
    -- without granting mutation authority or bypassing row-level security.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        NEW.tenant_id::text||'/'||NEW.user_id::text||'/'||NEW.task_id,101));
    IF ROW(NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.generation,
           NEW.definition_version,NEW.definition_digest,
           NEW.target_action_digest,NEW.action_authorization_digest)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.generation,
           OLD.definition_version,OLD.definition_digest,
           OLD.target_action_digest,OLD.action_authorization_digest) THEN
        RAISE EXCEPTION '101: immutable V3 authority identity changed';
    END IF;
    IF OLD.status='revoked' OR
       (OLD.status='staged' AND NEW.status NOT IN ('enabled','revoked')) OR
       (OLD.status='enabled' AND NEW.status<>'revoked') OR
       (NEW.status='enabled' AND
          (NEW.enabled_at IS NULL OR NEW.revoked_at IS NOT NULL)) OR
       (NEW.status='revoked' AND NEW.revoked_at IS NULL) OR
       (OLD.enabled_at IS NOT NULL AND NEW.enabled_at IS DISTINCT FROM OLD.enabled_at) OR
       (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at) THEN
        RAISE EXCEPTION '101: illegal V3 authority transition';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_v3_authority_transition() FROM PUBLIC;

CREATE TRIGGER protect_research_v3_authority_identity
BEFORE UPDATE ON research_v3_delivery_authorities
FOR EACH ROW EXECUTE FUNCTION protect_research_v3_authority_transition();

-- Keep the exact Schedule head stable from admission through effect claim.
-- Ordinary Schedule RLS and column grants remain authoritative.
-- +goose StatementBegin
CREATE FUNCTION fence_research_v3_schedule_claims() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        NEW.tenant_id::text||'/'||NEW.user_id::text||'/'||NEW.id,101));
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION fence_research_v3_schedule_claims() FROM PUBLIC;

CREATE TRIGGER fence_research_v3_schedule_claim
BEFORE UPDATE OF status,execution_mode,
                 approved_definition_version,approved_definition_digest
ON schedules
FOR EACH ROW EXECUTE FUNCTION fence_research_v3_schedule_claims();

-- +goose StatementBegin
CREATE FUNCTION protect_research_v3_cutover_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
	NEW.updated_at := clock_timestamp();
    IF ROW(NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.idempotency_key,
           NEW.generation,NEW.definition_version,NEW.definition_digest,
           NEW.frozen_schedule,NEW.frozen_schedule_digest,
           NEW.frozen_conflict_token,NEW.conflict_token_digest,
           CASE WHEN OLD.rollback_conflict_token IS NULL THEN NULL
                ELSE NEW.rollback_conflict_token END,
           CASE WHEN OLD.rollback_token_digest IS NULL THEN NULL
                ELSE NEW.rollback_token_digest END,
           NEW.target_action,NEW.target_action_digest,NEW.action_authorization_digest,
           NEW.original_paused,
           NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.idempotency_key,
           OLD.generation,OLD.definition_version,OLD.definition_digest,
           OLD.frozen_schedule,OLD.frozen_schedule_digest,
           OLD.frozen_conflict_token,OLD.conflict_token_digest,
           CASE WHEN OLD.rollback_conflict_token IS NULL THEN NULL
                ELSE OLD.rollback_conflict_token END,
           CASE WHEN OLD.rollback_token_digest IS NULL THEN NULL
                ELSE OLD.rollback_token_digest END,
           OLD.target_action,OLD.target_action_digest,OLD.action_authorization_digest,
           OLD.original_paused,
           OLD.created_at) THEN
        RAISE EXCEPTION '101: immutable V3 cutover evidence changed';
    END IF;
    IF NOT (
        (OLD.phase='prepared' AND NEW.phase IN
            ('pause_requested','rollback_paused','aborted','manual_intervention')) OR
        (OLD.phase='pause_requested' AND NEW.phase IN
            ('paused','rollback_paused','aborted','manual_intervention')) OR
        (OLD.phase='paused' AND NEW.phase IN
            ('action_swapped','rollback_paused','manual_intervention')) OR
        (OLD.phase='action_swapped' AND NEW.phase IN
            ('active','rollback_pause_requested','rollback_paused','manual_intervention')) OR
        (OLD.phase='active' AND NEW.phase IN
            ('rollback_pause_requested','rollback_paused','manual_intervention')) OR
        (OLD.phase='rollback_pause_requested' AND NEW.phase IN
            ('rollback_paused','manual_intervention')) OR
        (OLD.phase='rollback_paused' AND NEW.phase IN
            ('rolled_back','manual_intervention'))
    ) THEN
        RAISE EXCEPTION '101: illegal V3 cutover phase transition';
    END IF;
    IF NEW.phase IN ('rolled_back','aborted','manual_intervention') AND NOT EXISTS (
        SELECT 1 FROM public.research_v3_delivery_authorities authority
         WHERE authority.tenant_id=NEW.tenant_id
           AND authority.user_id=NEW.user_id
           AND authority.task_id=NEW.task_id
           AND authority.generation=NEW.generation
           AND authority.status='revoked'
    ) THEN
        RAISE EXCEPTION '101: rollback checkpoint requires revoked authority';
    END IF;
    IF NEW.phase='rollback_pause_requested' AND
       (OLD.rollback_conflict_token IS NOT NULL OR
        NEW.rollback_conflict_token IS NULL OR
        NEW.rollback_token_digest IS NULL) THEN
        RAISE EXCEPTION '101: rollback pause request token is invalid';
    END IF;
    IF OLD.rollback_conflict_token IS NULL AND
       NEW.rollback_conflict_token IS NOT NULL AND
       NEW.phase<>'rollback_pause_requested' THEN
        RAISE EXCEPTION '101: rollback pause token changed outside request transition';
    END IF;
    IF OLD.rollback_conflict_token IS NOT NULL AND
       (NEW.rollback_conflict_token IS DISTINCT FROM OLD.rollback_conflict_token OR
        NEW.rollback_token_digest IS DISTINCT FROM OLD.rollback_token_digest) THEN
        RAISE EXCEPTION '101: rollback pause request token is immutable';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_v3_cutover_transition() FROM PUBLIC;

CREATE TRIGGER protect_research_v3_cutover_operation
BEFORE UPDATE ON research_v3_cutover_operations
FOR EACH ROW EXECUTE FUNCTION protect_research_v3_cutover_transition();

-- Definition edit and V3 Action cutover are mutually exclusive for one task.
-- Both trigger paths take the same transaction advisory lock before checking
-- the other durable marker, so concurrent INSERTs cannot pass by observing
-- each other's uncommitted absence.
-- +goose StatementBegin
CREATE FUNCTION exclude_definition_edit_during_research_v3_cutover()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        NEW.target_tenant_id::text||'/'||NEW.target_user_id::text||'/'||NEW.task_id,101));
    IF NEW.status IN ('pending','executing') AND EXISTS (
        SELECT 1 FROM public.research_v3_cutover_operations cutover
         WHERE cutover.tenant_id=NEW.target_tenant_id
           AND cutover.user_id=NEW.target_user_id
           AND cutover.task_id=NEW.task_id
           AND cutover.phase NOT IN ('rolled_back','aborted')
    ) THEN
        RAISE EXCEPTION '101: definition edit conflicts with V3 cutover'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION exclude_definition_edit_during_research_v3_cutover()
    FROM PUBLIC;
CREATE TRIGGER exclude_definition_edit_during_research_v3_cutover
BEFORE INSERT OR UPDATE OF status ON task_definition_edit_operations
FOR EACH ROW EXECUTE FUNCTION exclude_definition_edit_during_research_v3_cutover();

-- +goose StatementBegin
CREATE FUNCTION exclude_research_v3_cutover_during_definition_edit()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        NEW.tenant_id::text||'/'||NEW.user_id::text||'/'||NEW.task_id,101));
    IF EXISTS (
        SELECT 1 FROM public.task_definition_edit_operations edit
         WHERE edit.target_tenant_id=NEW.tenant_id
           AND edit.target_user_id=NEW.user_id
           AND edit.task_id=NEW.task_id
           AND edit.status IN ('pending','executing')
    ) THEN
        RAISE EXCEPTION '101: V3 cutover conflicts with definition edit'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION exclude_research_v3_cutover_during_definition_edit()
    FROM PUBLIC;
CREATE TRIGGER exclude_research_v3_cutover_during_definition_edit
BEFORE INSERT ON research_v3_cutover_operations
FOR EACH ROW EXECUTE FUNCTION exclude_research_v3_cutover_during_definition_edit();

-- Schedule deletion is an authority transition, not an orphaning event. It
-- uses the same exact-task lock as delivery admission: a committed delete
-- makes every later claim fail, while a claim that already owns the lock may
-- finish its provider-send transaction before deletion revokes the authority.
-- +goose StatementBegin
CREATE FUNCTION fence_research_v3_schedule_delete()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        OLD.tenant_id::text||'/'||OLD.user_id::text||'/'||OLD.id,101));
    UPDATE public.research_v3_delivery_authorities
       SET status='revoked',revoked_at=pg_catalog.clock_timestamp()
     WHERE tenant_id=OLD.tenant_id AND user_id=OLD.user_id AND task_id=OLD.id
       AND status IN ('staged','enabled');
    UPDATE public.research_v3_cutover_operations
       SET phase='manual_intervention'
     WHERE tenant_id=OLD.tenant_id AND user_id=OLD.user_id AND task_id=OLD.id
       AND phase NOT IN ('rolled_back','aborted','manual_intervention');
    RETURN OLD;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION fence_research_v3_schedule_delete() FROM PUBLIC;
CREATE TRIGGER fence_research_v3_schedule_delete
BEFORE DELETE ON schedules
FOR EACH ROW EXECUTE FUNCTION fence_research_v3_schedule_delete();

-- +goose Down

LOCK TABLE research_v3_cutover_operations,
           research_v3_delivery_authorities IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_v3_cutover_operations) OR
       EXISTS (SELECT 1 FROM research_v3_delivery_authorities) THEN
        RAISE EXCEPTION '101: refusing downgrade while V3 cutover evidence exists';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER protect_research_v3_authority_identity
    ON research_v3_delivery_authorities;
DROP TRIGGER protect_research_v3_cutover_operation
    ON research_v3_cutover_operations;
DROP TRIGGER exclude_research_v3_cutover_during_definition_edit
    ON research_v3_cutover_operations;
DROP FUNCTION exclude_research_v3_cutover_during_definition_edit();
DROP TRIGGER exclude_definition_edit_during_research_v3_cutover
    ON task_definition_edit_operations;
DROP FUNCTION exclude_definition_edit_during_research_v3_cutover();
DROP TRIGGER fence_research_v3_schedule_delete ON schedules;
DROP FUNCTION fence_research_v3_schedule_delete();
DROP TRIGGER fence_research_v3_schedule_claim ON schedules;
DROP FUNCTION fence_research_v3_schedule_claims();
DROP FUNCTION protect_research_v3_cutover_transition();
DROP FUNCTION protect_research_v3_authority_transition();
REVOKE ALL ON SEQUENCE research_v3_cutover_operations_id_seq
    FROM vane_research_v3_cutover_operator;
REVOKE ALL ON research_v3_cutover_operations,
              research_v3_delivery_authorities
    FROM vane_app,vane_research_v3_cutover_operator;
REVOKE SELECT ON research_v3_delivery_authorities
    FROM vane_push_effect_coordinator;
REVOKE SELECT (
    tenant_id,user_id,id,status,execution_mode,
    approved_definition_version,approved_definition_digest
) ON schedules FROM vane_push_effect_coordinator;
REVOKE SELECT ON schedules,tenants,memberships,
                 task_approved_definition_versions,task_run_snapshots
    FROM vane_research_v3_cutover_operator;
DROP TABLE research_v3_cutover_operations;
DROP TABLE research_v3_delivery_authorities;
