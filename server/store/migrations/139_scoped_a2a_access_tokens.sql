-- 139: workspace- and principal-bound A2A access tokens.
--
-- Raw bearer tokens never enter PostgreSQL. Authentication is resolved from a
-- SHA-256 hash, then revalidates the exact active workspace membership on every
-- request. This migration is an authority substrate only: the existing A2A
-- ingress is not switched in the same release.

-- +goose Up

CREATE SEQUENCE membership_authorization_generation_seq;
ALTER TABLE memberships
    ADD COLUMN authorization_generation BIGINT NOT NULL
        DEFAULT nextval('membership_authorization_generation_seq'),
    ADD CONSTRAINT uq_membership_authorization_generation
        UNIQUE (tenant_id,user_id,authorization_generation),
    ADD CONSTRAINT ck_membership_authorization_generation
        CHECK (authorization_generation > 0);
ALTER TABLE memberships ALTER COLUMN authorization_generation DROP DEFAULT;
GRANT USAGE,SELECT ON SEQUENCE membership_authorization_generation_seq TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION preserve_membership_authorization_generation_v139()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        NEW.authorization_generation := pg_catalog.nextval(
            'public.membership_authorization_generation_seq'::pg_catalog.regclass);
    ELSIF NEW.authorization_generation IS DISTINCT FROM OLD.authorization_generation THEN
        RAISE EXCEPTION 'membership authorization generation is immutable'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION preserve_membership_authorization_generation_v139() FROM PUBLIC;
CREATE TRIGGER trg_membership_authorization_generation_v139
BEFORE INSERT OR UPDATE ON memberships
FOR EACH ROW EXECUTE FUNCTION preserve_membership_authorization_generation_v139();

-- +goose StatementBegin
CREATE FUNCTION seal_account_security_token_consumption_v139()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL THEN
        RAISE EXCEPTION 'account security token consumption is irreversible'
            USING ERRCODE='55000';
    END IF;
    NEW.consumed_at := pg_catalog.clock_timestamp();
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION seal_account_security_token_consumption_v139() FROM PUBLIC;
CREATE TRIGGER trg_account_security_token_consumption_v139
BEFORE UPDATE OF consumed_at ON account_security_tokens
FOR EACH ROW EXECUTE FUNCTION seal_account_security_token_consumption_v139();

ALTER TABLE account_security_tokens
    ADD CONSTRAINT uq_account_security_token_scope_v139
        UNIQUE (id,tenant_id,user_id);

CREATE TABLE a2a_access_tokens (
    id                    UUID        PRIMARY KEY,
    token_hash            BYTEA       NOT NULL UNIQUE,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    principal_user_id     BIGINT      NOT NULL REFERENCES users (id),
    actor_type            TEXT        NOT NULL,
    service_account_label TEXT        NOT NULL DEFAULT '',
    scopes                TEXT[]      NOT NULL,
    issued_by             BIGINT      NOT NULL REFERENCES users (id),
    membership_generation BIGINT      NOT NULL,
    reauth_token_id       BIGINT      NOT NULL UNIQUE,
    expires_at            TIMESTAMPTZ NOT NULL,
    revoked_at            TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_a2a_access_token_scope UNIQUE (tenant_id,id),
    CONSTRAINT fk_a2a_access_token_reauth_scope
        FOREIGN KEY (reauth_token_id,tenant_id,principal_user_id)
        REFERENCES account_security_tokens (id,tenant_id,user_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_a2a_access_token_hash CHECK (octet_length(token_hash)=32),
    CONSTRAINT ck_a2a_access_token_membership_generation
        CHECK (membership_generation > 0),
    CONSTRAINT ck_a2a_access_token_actor CHECK (
        (actor_type='user' AND service_account_label='') OR
        (actor_type='service_account' AND
         octet_length(service_account_label) BETWEEN 1 AND 128 AND
         service_account_label=btrim(service_account_label) AND
         service_account_label !~ '[[:cntrl:]]')
    ),
    CONSTRAINT ck_a2a_access_token_scopes CHECK (
        scopes=ARRAY['assistant.chat']::TEXT[] OR
        scopes=ARRAY['content.query']::TEXT[] OR
        scopes=ARRAY['assistant.chat','content.query']::TEXT[]
    ),
    CONSTRAINT ck_a2a_access_token_lifetime CHECK (
        expires_at > created_at AND expires_at <= created_at + interval '90 days'
    ),
    CONSTRAINT ck_a2a_access_token_revocation CHECK (
        revoked_at IS NULL OR revoked_at >= created_at
    )
);
CREATE INDEX idx_a2a_access_tokens_principal
    ON a2a_access_tokens (tenant_id,principal_user_id,created_at DESC,id);
CREATE INDEX idx_a2a_access_tokens_active_expiry
    ON a2a_access_tokens (expires_at)
    WHERE revoked_at IS NULL;

-- +goose StatementBegin
CREATE FUNCTION seal_a2a_access_token_revocation_v139()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL THEN
        RAISE EXCEPTION 'A2A token revocation is append-only' USING ERRCODE='55000';
    END IF;
    NEW.revoked_at := pg_catalog.clock_timestamp();
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION seal_a2a_access_token_revocation_v139() FROM PUBLIC;
CREATE TRIGGER trg_a2a_access_token_revocation_v139
BEFORE UPDATE ON a2a_access_tokens
FOR EACH ROW EXECUTE FUNCTION seal_a2a_access_token_revocation_v139();

CREATE TABLE a2a_access_token_events (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       BIGINT      NOT NULL,
    token_id        UUID        NOT NULL,
    actor_user_id   BIGINT      NOT NULL REFERENCES users (id),
    event_kind      TEXT        NOT NULL,
    scopes          TEXT[]      NOT NULL,
    principal_user_id BIGINT    NOT NULL REFERENCES users (id),
    actor_type      TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_a2a_access_token_event_parent
        FOREIGN KEY (tenant_id,token_id)
        REFERENCES a2a_access_tokens (tenant_id,id) ON DELETE CASCADE,
    CONSTRAINT uq_a2a_access_token_event_lifecycle
        UNIQUE (tenant_id,token_id,event_kind),
    CONSTRAINT ck_a2a_access_token_event_kind CHECK (event_kind IN ('issued','revoked')),
    CONSTRAINT ck_a2a_access_token_event_actor CHECK (actor_type IN ('user','service_account')),
    CONSTRAINT ck_a2a_access_token_event_scopes CHECK (
        scopes=ARRAY['assistant.chat']::TEXT[] OR
        scopes=ARRAY['content.query']::TEXT[] OR
        scopes=ARRAY['assistant.chat','content.query']::TEXT[]
    )
);
CREATE INDEX idx_a2a_access_token_events_scope
    ON a2a_access_token_events (tenant_id,token_id,created_at,id);

ALTER TABLE a2a_access_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE a2a_access_tokens FORCE ROW LEVEL SECURITY;
ALTER TABLE a2a_access_token_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE a2a_access_token_events FORCE ROW LEVEL SECURITY;

-- A2A issuance is one database primitive. Callers cannot first consume a
-- generic reauthentication proof for another action and later attach it to a
-- token: the proof validation, consumption, token row and issued event share
-- one transaction and one exact session/workspace principal.
-- +goose StatementBegin
CREATE FUNCTION issue_a2a_access_token_v139(
    requested_id UUID,
    requested_hash BYTEA,
    requested_tenant BIGINT,
    requested_principal BIGINT,
    requested_actor_type TEXT,
    requested_label TEXT,
    requested_scopes TEXT[],
    requested_issuer BIGINT,
    requested_generation BIGINT,
    requested_proof_hash BYTEA,
    requested_session_hash BYTEA,
    requested_expires_at TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    live_role TEXT;
    proof_id BIGINT;
    active_count BIGINT;
    database_now TIMESTAMPTZ := pg_catalog.clock_timestamp();
BEGIN
    IF requested_tenant IS DISTINCT FROM
           NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint OR
       requested_issuer IS DISTINCT FROM
           NULLIF(pg_catalog.current_setting('app.user_id',true),'')::bigint OR
       requested_principal IS DISTINCT FROM requested_issuer THEN
        RAISE EXCEPTION 'A2A issuance principal is forbidden' USING ERRCODE='42501';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        'vane/tenant-admission/v1/'||requested_tenant::text,1447120453));
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        'vane/a2a-schema/v139',1447120453));
    SELECT membership.role INTO live_role
    FROM public.memberships membership
    JOIN public.tenants workspace ON workspace.id=membership.tenant_id
    WHERE membership.tenant_id=requested_tenant
      AND membership.user_id=requested_issuer
      AND membership.authorization_generation=requested_generation
      AND workspace.status='active' AND workspace.deleted_at IS NULL
    FOR UPDATE OF membership;
    IF live_role IS NULL OR
       (requested_actor_type='service_account' AND live_role NOT IN ('owner','admin')) THEN
        RAISE EXCEPTION 'active A2A workspace membership is required' USING ERRCODE='42501';
    END IF;

    SELECT proof.id INTO proof_id
    FROM public.account_security_tokens proof
    WHERE proof.token_hash=requested_proof_hash
      AND proof.token_kind='reauth'
      AND proof.tenant_id=requested_tenant
      AND proof.user_id=requested_issuer
      AND proof.session_token_hash=requested_session_hash
      AND proof.consumed_at IS NULL
      AND proof.expires_at>database_now
      AND EXISTS (
          SELECT 1 FROM public.user_sessions session
          WHERE session.token_hash=requested_session_hash
            AND session.tenant_id=requested_tenant
            AND session.user_id=requested_issuer
            AND session.expires_at>database_now
      )
    FOR UPDATE;
    IF proof_id IS NULL THEN
        RAISE EXCEPTION 'recent session-bound reauthentication is required'
            USING ERRCODE='42501';
    END IF;

    IF requested_expires_at < database_now+interval '5 minutes' OR
       requested_expires_at > database_now+interval '90 days' THEN
        RAISE EXCEPTION 'A2A token lifetime is invalid' USING ERRCODE='22023';
    END IF;
    SELECT count(*) INTO active_count FROM public.a2a_access_tokens
    WHERE tenant_id=requested_tenant AND principal_user_id=requested_principal
      AND revoked_at IS NULL AND expires_at>database_now;
    IF active_count>=20 THEN
        RAISE EXCEPTION 'active A2A token limit reached' USING ERRCODE='55000';
    END IF;

    UPDATE public.account_security_tokens SET consumed_at=database_now WHERE id=proof_id;
    INSERT INTO public.a2a_access_tokens(
        id,token_hash,tenant_id,principal_user_id,actor_type,
        service_account_label,scopes,issued_by,membership_generation,
        reauth_token_id,expires_at)
    VALUES(requested_id,requested_hash,requested_tenant,requested_principal,
        requested_actor_type,requested_label,requested_scopes,requested_issuer,
        requested_generation,proof_id,requested_expires_at);
    INSERT INTO public.a2a_access_token_events(
        tenant_id,token_id,actor_user_id,event_kind,scopes,
        principal_user_id,actor_type)
    VALUES(requested_tenant,requested_id,requested_issuer,'issued',requested_scopes,
        requested_principal,requested_actor_type);
END;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION issue_a2a_access_token_v139(
    UUID,BYTEA,BIGINT,BIGINT,TEXT,TEXT,TEXT[],BIGINT,BIGINT,BYTEA,BYTEA,TIMESTAMPTZ)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION issue_a2a_access_token_v139(
    UUID,BYTEA,BIGINT,BIGINT,TEXT,TEXT,TEXT[],BIGINT,BIGINT,BYTEA,BYTEA,TIMESTAMPTZ)
    TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION revoke_a2a_access_token_v139(
    requested_tenant BIGINT,
    requested_token UUID,
    requested_actor BIGINT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    live_role TEXT;
    token_row public.a2a_access_tokens%ROWTYPE;
BEGIN
    IF requested_tenant IS DISTINCT FROM
           NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint OR
       requested_actor IS DISTINCT FROM
           NULLIF(pg_catalog.current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION 'A2A revocation principal is forbidden' USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        'vane/tenant-admission/v1/'||requested_tenant::text,1447120453));
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        'vane/a2a-schema/v139',1447120453));
    SELECT membership.role INTO live_role
    FROM public.memberships membership
    JOIN public.tenants workspace ON workspace.id=membership.tenant_id
    WHERE membership.tenant_id=requested_tenant
      AND membership.user_id=requested_actor
      AND workspace.status='active' AND workspace.deleted_at IS NULL
    FOR UPDATE OF membership;
    IF live_role IS NULL THEN
        RAISE EXCEPTION 'active A2A workspace membership is required' USING ERRCODE='42501';
    END IF;
    SELECT * INTO token_row FROM public.a2a_access_tokens
    WHERE tenant_id=requested_tenant AND id=requested_token AND revoked_at IS NULL
    FOR UPDATE;
    IF token_row.id IS NULL OR NOT (
        (token_row.actor_type='user' AND token_row.principal_user_id=requested_actor) OR
        live_role IN ('owner','admin')
    ) THEN
        RAISE EXCEPTION 'A2A token not found or already revoked' USING ERRCODE='02000';
    END IF;
    UPDATE public.a2a_access_tokens SET revoked_at=pg_catalog.clock_timestamp()
    WHERE tenant_id=requested_tenant AND id=requested_token;
    INSERT INTO public.a2a_access_token_events(
        tenant_id,token_id,actor_user_id,event_kind,scopes,
        principal_user_id,actor_type)
    VALUES(token_row.tenant_id,token_row.id,requested_actor,'revoked',token_row.scopes,
        token_row.principal_user_id,token_row.actor_type);
END;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION revoke_a2a_access_token_v139(BIGINT,UUID,BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION revoke_a2a_access_token_v139(BIGINT,UUID,BIGINT) TO vane_app;

CREATE POLICY a2a_access_token_authenticate ON a2a_access_tokens
    FOR SELECT TO vane_app
    USING (
        encode(token_hash,'hex') IS NOT DISTINCT FROM
            NULLIF(current_setting('app.a2a_token_hash',true),'')
    );
CREATE POLICY a2a_access_token_manage_select ON a2a_access_tokens
    FOR SELECT TO vane_app
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
        ((actor_type='user' AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint) OR
         NULLIF(current_setting('app.membership_role',true),'') IN ('owner','admin'))
    );
CREATE POLICY a2a_access_token_manage_update ON a2a_access_tokens
    FOR UPDATE TO vane_app
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
        ((actor_type='user' AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint) OR
         NULLIF(current_setting('app.membership_role',true),'') IN ('owner','admin'))
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
        revoked_at IS NOT NULL AND
        ((actor_type='user' AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint) OR
         NULLIF(current_setting('app.membership_role',true),'') IN ('owner','admin'))
    );

CREATE POLICY a2a_access_token_event_select ON a2a_access_token_events
    FOR SELECT TO vane_app
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
        ((actor_type='user' AND principal_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint) OR
         NULLIF(current_setting('app.membership_role',true),'') IN ('owner','admin'))
    );
CREATE POLICY a2a_access_token_event_insert ON a2a_access_token_events
    FOR INSERT TO vane_app
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
        actor_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id',true),'')::bigint AND
        EXISTS (
            SELECT 1 FROM a2a_access_tokens token
            WHERE token.tenant_id=a2a_access_token_events.tenant_id
              AND token.id=a2a_access_token_events.token_id
              AND token.principal_user_id=a2a_access_token_events.principal_user_id
              AND token.actor_type=a2a_access_token_events.actor_type
              AND token.scopes=a2a_access_token_events.scopes
              AND ((a2a_access_token_events.event_kind='issued' AND
                    token.issued_by=a2a_access_token_events.actor_user_id AND
                    token.revoked_at IS NULL) OR
                   (a2a_access_token_events.event_kind='revoked' AND
                    token.revoked_at IS NOT NULL))
        )
    );

GRANT SELECT ON a2a_access_tokens TO vane_app;
GRANT SELECT ON a2a_access_token_events TO vane_app;

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vane/a2a-schema/v139',1447120453));
    LOCK TABLE memberships,a2a_access_tokens,a2a_access_token_events
        IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM a2a_access_tokens) OR
       EXISTS (SELECT 1 FROM a2a_access_token_events) THEN
        RAISE EXCEPTION '139 down refused: retained scoped A2A authority exists'
            USING ERRCODE='55000';
    END IF;
END;
$$;
-- +goose StatementEnd
DROP FUNCTION issue_a2a_access_token_v139(
    UUID,BYTEA,BIGINT,BIGINT,TEXT,TEXT,TEXT[],BIGINT,BIGINT,BYTEA,BYTEA,TIMESTAMPTZ);
DROP FUNCTION revoke_a2a_access_token_v139(BIGINT,UUID,BIGINT);
DROP TABLE a2a_access_token_events;
DROP TRIGGER trg_a2a_access_token_revocation_v139 ON a2a_access_tokens;
DROP FUNCTION seal_a2a_access_token_revocation_v139();
DROP TABLE a2a_access_tokens;
ALTER TABLE account_security_tokens
    DROP CONSTRAINT uq_account_security_token_scope_v139;
DROP TRIGGER trg_account_security_token_consumption_v139 ON account_security_tokens;
DROP FUNCTION seal_account_security_token_consumption_v139();
DROP TRIGGER trg_membership_authorization_generation_v139 ON memberships;
DROP FUNCTION preserve_membership_authorization_generation_v139();
ALTER TABLE memberships
    DROP CONSTRAINT uq_membership_authorization_generation,
    DROP CONSTRAINT ck_membership_authorization_generation,
    DROP COLUMN authorization_generation;
DROP SEQUENCE membership_authorization_generation_seq;
