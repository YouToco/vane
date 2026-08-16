-- 152: dark, response-loss-safe capability invocation coordination ledger.
--
-- This migration grants no production execution authority. The dedicated
-- coordinator role is intentionally available only to the migration owner;
-- vane_server_runtime remains forbidden until a later activation migration.

-- +goose Up

CREATE TABLE capability_invocations (
    id                              UUID        PRIMARY KEY,
    tenant_id                       BIGINT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id                         BIGINT      NOT NULL REFERENCES users(id),
    principal_role                  TEXT        NOT NULL,
    actor_type                      TEXT        NOT NULL,
    membership_generation           BIGINT      NOT NULL,
    a2a_token_authority_id          UUID,
    required_a2a_scope              TEXT,
    capability_kind                 TEXT        NOT NULL,
    capability_scope                TEXT        NOT NULL,
    capability_owner_user_id        BIGINT      NOT NULL,
    capability_id                   TEXT        NOT NULL,
    capability_version_id           TEXT        NOT NULL,
    capability_version_digest       TEXT        NOT NULL,
    operation_schema_digest         TEXT        NOT NULL,
    operation                       TEXT        NOT NULL,
    policy_digest                   TEXT        NOT NULL,
    credential_opaque_ref           TEXT,
    credential_opaque_ref_digest    TEXT,
    credential_provider             TEXT,
    credential_purpose              TEXT,
    credential_scope                TEXT,
    credential_user_id              BIGINT,
    credential_generation           BIGINT,
    credential_fingerprint          TEXT,
    idempotency_key                 TEXT        NOT NULL,
    idempotency_digest              TEXT        NOT NULL,
    invocation_digest               TEXT        NOT NULL,
    invocation_payload              BYTEA       NOT NULL,
    invocation_payload_digest       TEXT        NOT NULL,
    status                          TEXT        NOT NULL DEFAULT 'pending',
    lease_owner                     TEXT        NOT NULL DEFAULT '',
    lease_until                     TIMESTAMPTZ,
    fence                           BIGINT      NOT NULL DEFAULT 0,
    attempt                         BIGINT      NOT NULL DEFAULT 0,
    current_receipt_ordinal         BIGINT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_capability_invocation_scope UNIQUE (tenant_id,user_id,id),
    CONSTRAINT uq_capability_invocation_idempotency
        UNIQUE (tenant_id,user_id,idempotency_digest),
    CONSTRAINT uq_capability_invocation_receipt_parent
        UNIQUE (tenant_id,user_id,id,invocation_digest,idempotency_digest),
    CONSTRAINT ck_capability_invocation_principal CHECK (
        principal_role IN ('owner','admin','member') AND membership_generation>0 AND
        ((actor_type='user' AND a2a_token_authority_id IS NULL AND required_a2a_scope IS NULL) OR
         (actor_type='service_account' AND a2a_token_authority_id IS NOT NULL AND
          required_a2a_scope='assistant.chat'))
    ),
    CONSTRAINT ck_capability_invocation_capability CHECK (
        capability_kind IN ('builtin_tool','declarative_skill','remote_mcp','sandbox_script') AND
        capability_scope IN ('platform','personal','workspace') AND
        octet_length(capability_id) BETWEEN 1 AND 255 AND btrim(capability_id)=capability_id AND
        octet_length(capability_version_id) BETWEEN 1 AND 255 AND btrim(capability_version_id)=capability_version_id AND
        capability_version_digest ~ '^[0-9a-f]{64}$' AND
        operation_schema_digest ~ '^[0-9a-f]{64}$' AND
        ((capability_scope='platform' AND capability_kind='builtin_tool' AND capability_owner_user_id=0) OR
         (capability_scope='personal' AND capability_kind<>'builtin_tool' AND capability_owner_user_id=user_id) OR
         (capability_scope='workspace' AND capability_kind<>'builtin_tool' AND capability_owner_user_id>0))
    ),
    CONSTRAINT ck_capability_invocation_operation CHECK (
        octet_length(operation) BETWEEN 1 AND 255 AND btrim(operation)=operation AND
        policy_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_capability_invocation_credential CHECK (
        (credential_opaque_ref IS NULL AND credential_opaque_ref_digest IS NULL AND
         credential_provider IS NULL AND credential_purpose IS NULL AND credential_scope IS NULL AND
         credential_user_id IS NULL AND credential_generation IS NULL AND credential_fingerprint IS NULL) OR
        (credential_opaque_ref ~ '^vault:[A-Za-z0-9][A-Za-z0-9._-]{0,239}$' AND
         credential_opaque_ref_digest ~ '^[0-9a-f]{64}$' AND
         credential_opaque_ref_digest=encode(sha256(convert_to(credential_opaque_ref,'UTF8')),'hex') AND
         credential_provider ~ '^[a-z][a-z0-9_-]{0,63}$' AND
         credential_purpose ~ '^[a-z][a-z0-9_-]{0,63}$' AND
         credential_scope IN ('platform','tenant','user') AND credential_generation>0 AND
         credential_fingerprint ~ '^[0-9a-f]{64}$' AND
         ((credential_scope IN ('platform','tenant') AND credential_user_id=0) OR
          (credential_scope='user' AND credential_user_id=user_id)))
    ),
    CONSTRAINT ck_capability_invocation_digests CHECK (
        idempotency_digest ~ '^[0-9a-f]{64}$' AND invocation_digest ~ '^[0-9a-f]{64}$' AND
        invocation_payload_digest ~ '^[0-9a-f]{64}$' AND
        invocation_payload_digest=encode(sha256(invocation_payload),'hex') AND
        octet_length(invocation_payload) BETWEEN 2 AND 524288
    ),
    CONSTRAINT ck_capability_invocation_idempotency_key CHECK (
        octet_length(idempotency_key) BETWEEN 1 AND 255 AND btrim(idempotency_key)=idempotency_key
    ),
    CONSTRAINT ck_capability_invocation_checkpoint CHECK (
        (status='pending' AND lease_owner='' AND lease_until IS NULL AND fence=0 AND attempt=0 AND
         current_receipt_ordinal IS NULL) OR
        (status='executing' AND octet_length(lease_owner) BETWEEN 1 AND 255 AND
         lease_owner=btrim(lease_owner) AND lease_until IS NOT NULL AND fence=1 AND attempt=1 AND
         current_receipt_ordinal IS NULL) OR
        (status IN ('succeeded','definite_failed','rejected','unknown_effect') AND
         lease_owner='' AND lease_until IS NULL AND fence=1 AND attempt=1 AND
         current_receipt_ordinal IS NOT NULL AND current_receipt_ordinal BETWEEN 1 AND 2)
    )
);
CREATE INDEX idx_capability_invocation_scope_status
    ON capability_invocations(tenant_id,user_id,status,created_at,id);
CREATE INDEX idx_capability_invocation_expired
    ON capability_invocations(lease_until,id) WHERE status='executing';

CREATE TABLE capability_invocation_receipts (
    id                         BIGSERIAL   PRIMARY KEY,
    tenant_id                  BIGINT      NOT NULL,
    user_id                    BIGINT      NOT NULL,
    invocation_id              UUID        NOT NULL,
    invocation_digest          TEXT        NOT NULL,
    idempotency_digest         TEXT        NOT NULL,
    receipt_ordinal            BIGINT      NOT NULL,
    fence                      BIGINT      NOT NULL,
    attempt                    BIGINT      NOT NULL,
    settled_by_lease_owner     TEXT        NOT NULL,
    status                     TEXT        NOT NULL,
    result_digest              TEXT,
    result_size_bytes          BIGINT,
    result_media_type          TEXT,
    sanitized_result_payload   BYTEA,
    error_class                TEXT        NOT NULL DEFAULT '',
    retryable                  BOOLEAN     NOT NULL DEFAULT false,
    receipt_digest             TEXT        NOT NULL,
    receipt_payload            BYTEA       NOT NULL,
    receipt_payload_digest     TEXT        NOT NULL,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_capability_invocation_receipt_scope
        UNIQUE (tenant_id,user_id,invocation_id,receipt_ordinal),
    CONSTRAINT uq_capability_invocation_receipt_digest
        UNIQUE (tenant_id,user_id,invocation_id,receipt_digest),
    CONSTRAINT fk_capability_invocation_receipt_parent
        FOREIGN KEY (tenant_id,user_id,invocation_id,invocation_digest,idempotency_digest)
        REFERENCES capability_invocations(tenant_id,user_id,id,invocation_digest,idempotency_digest)
        ON DELETE CASCADE,
    CONSTRAINT ck_capability_invocation_receipt_identity CHECK (
        invocation_digest ~ '^[0-9a-f]{64}$' AND idempotency_digest ~ '^[0-9a-f]{64}$' AND
        receipt_ordinal BETWEEN 1 AND 2 AND fence=1 AND attempt=1 AND
        octet_length(settled_by_lease_owner) BETWEEN 1 AND 255 AND
        settled_by_lease_owner=btrim(settled_by_lease_owner)
    ),
    CONSTRAINT ck_capability_invocation_receipt_result CHECK (
        (status='succeeded' AND result_digest ~ '^[0-9a-f]{64}$' AND
         result_size_bytes BETWEEN 0 AND 262144 AND result_media_type IS NOT NULL AND
         octet_length(result_media_type) BETWEEN 1 AND 127 AND sanitized_result_payload IS NOT NULL AND
         result_size_bytes=octet_length(sanitized_result_payload) AND
         result_digest=encode(sha256(sanitized_result_payload),'hex') AND
         error_class='' AND NOT retryable) OR
        (status IN ('definite_failed','rejected','ambiguous') AND result_digest IS NULL AND
         result_size_bytes IS NULL AND result_media_type IS NULL AND sanitized_result_payload IS NULL AND
         error_class ~ '^[a-z][a-z0-9_]{0,63}$' AND
         (status='definite_failed' OR NOT retryable))
    ),
    CONSTRAINT ck_capability_invocation_receipt_wire CHECK (
        receipt_digest ~ '^[0-9a-f]{64}$' AND receipt_payload_digest ~ '^[0-9a-f]{64}$' AND
        receipt_payload_digest=encode(sha256(receipt_payload),'hex') AND
        octet_length(receipt_payload) BETWEEN 2 AND 524288
    )
);
CREATE INDEX idx_capability_invocation_receipt_scope
    ON capability_invocation_receipts(tenant_id,user_id,invocation_id,receipt_ordinal);

ALTER TABLE capability_invocations
    ADD CONSTRAINT fk_capability_invocation_current_receipt
    FOREIGN KEY (tenant_id,user_id,id,current_receipt_ordinal)
    REFERENCES capability_invocation_receipts(tenant_id,user_id,invocation_id,receipt_ordinal)
    DEFERRABLE INITIALLY DEFERRED;

-- +goose StatementBegin
CREATE FUNCTION enforce_capability_invocation_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE invocation_row public.capability_invocations%ROWTYPE;
BEGIN
  SELECT * INTO invocation_row FROM public.capability_invocations
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id AND id=NEW.invocation_id
   FOR UPDATE;
  IF NOT FOUND OR invocation_row.invocation_digest<>NEW.invocation_digest OR
     invocation_row.idempotency_digest<>NEW.idempotency_digest OR
     invocation_row.fence<>NEW.fence OR invocation_row.attempt<>NEW.attempt THEN
    RAISE EXCEPTION '152: receipt does not match exact invocation lease' USING ERRCODE='23514';
  END IF;
  IF invocation_row.status='executing' THEN
    IF NEW.receipt_ordinal<>1 OR invocation_row.current_receipt_ordinal IS NOT NULL OR
       invocation_row.lease_owner<>NEW.settled_by_lease_owner THEN
      RAISE EXCEPTION '152: first receipt ordinal is invalid' USING ERRCODE='23514';
    END IF;
  ELSIF invocation_row.status='unknown_effect' THEN
    IF NEW.receipt_ordinal<>invocation_row.current_receipt_ordinal+1 OR NEW.status='ambiguous' OR
       NOT EXISTS(SELECT 1 FROM public.capability_invocation_receipts previous
         WHERE previous.tenant_id=invocation_row.tenant_id AND
           previous.user_id=invocation_row.user_id AND
           previous.invocation_id=invocation_row.id AND
           previous.receipt_ordinal=invocation_row.current_receipt_ordinal AND
           previous.settled_by_lease_owner=NEW.settled_by_lease_owner) THEN
      RAISE EXCEPTION '152: late truth receipt is invalid' USING ERRCODE='23514';
    END IF;
  ELSE
    RAISE EXCEPTION '152: invocation cannot accept a receipt' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_capability_invocation_receipt_v1() FROM PUBLIC;
CREATE TRIGGER capability_invocation_receipt_v1
BEFORE INSERT ON capability_invocation_receipts
FOR EACH ROW EXECUTE FUNCTION enforce_capability_invocation_receipt_v1();

-- +goose StatementBegin
CREATE FUNCTION enforce_capability_invocation_checkpoint_v1()
RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE receipt_status text;
BEGIN
  IF (to_jsonb(NEW)-ARRAY['status','lease_owner','lease_until','fence','attempt',
       'current_receipt_ordinal','updated_at']) IS DISTINCT FROM
     (to_jsonb(OLD)-ARRAY['status','lease_owner','lease_until','fence','attempt',
       'current_receipt_ordinal','updated_at']) THEN
    RAISE EXCEPTION '152: frozen invocation fields are immutable' USING ERRCODE='55000';
  END IF;
  IF OLD.status='pending' AND NEW.status='executing' THEN
    IF NEW.fence<>1 OR NEW.attempt<>1 OR NEW.lease_until<=clock_timestamp() OR
       NEW.lease_owner='' OR NEW.current_receipt_ordinal IS NOT NULL THEN
      RAISE EXCEPTION '152: acquisition checkpoint is invalid' USING ERRCODE='23514';
    END IF;
  ELSIF OLD.status IN ('executing','unknown_effect') AND
        NEW.status IN ('succeeded','definite_failed','rejected','unknown_effect') THEN
    SELECT status INTO receipt_status FROM public.capability_invocation_receipts
     WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id AND invocation_id=NEW.id
       AND receipt_ordinal=NEW.current_receipt_ordinal;
    IF receipt_status IS NULL OR
       (NEW.status='unknown_effect' AND receipt_status<>'ambiguous') OR
       (NEW.status<>'unknown_effect' AND receipt_status<>NEW.status) OR
       (OLD.status='unknown_effect' AND NEW.status='unknown_effect') THEN
      RAISE EXCEPTION '152: terminal checkpoint lacks exact receipt' USING ERRCODE='23514';
    END IF;
  ELSE
    RAISE EXCEPTION '152: invocation checkpoint transition is forbidden' USING ERRCODE='55000';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_capability_invocation_checkpoint_v1() FROM PUBLIC;
CREATE TRIGGER capability_invocation_checkpoint_v1
BEFORE UPDATE ON capability_invocations
FOR EACH ROW EXECUTE FUNCTION enforce_capability_invocation_checkpoint_v1();

-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles
                 WHERE rolname='vane_capability_invocation_coordinator') THEN
    BEGIN
      CREATE ROLE vane_capability_invocation_coordinator NOLOGIN NOSUPERUSER NOCREATEDB
        NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
    END;
  END IF;
  ALTER ROLE vane_capability_invocation_coordinator NOLOGIN NOSUPERUSER NOCREATEDB
    NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
  ALTER ROLE vane_capability_invocation_coordinator
    SET search_path=pg_catalog,public,pg_temp;
  GRANT vane_capability_invocation_coordinator TO CURRENT_USER
    WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;
END $$;
-- +goose StatementEnd

REVOKE ALL ON capability_invocations,capability_invocation_receipts
  FROM PUBLIC,vane_app,vane_capability_invocation_coordinator;
REVOKE ALL ON SEQUENCE capability_invocation_receipts_id_seq
  FROM PUBLIC,vane_app,vane_capability_invocation_coordinator;

ALTER TABLE capability_invocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE capability_invocations FORCE ROW LEVEL SECURITY;
ALTER TABLE capability_invocation_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE capability_invocation_receipts FORCE ROW LEVEL SECURITY;

CREATE POLICY capability_invocation_select ON capability_invocations
  FOR SELECT TO vane_capability_invocation_coordinator
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         user_id=NULLIF(current_setting('app.user_id',true),'')::bigint);
CREATE POLICY capability_invocation_insert ON capability_invocations
  FOR INSERT TO vane_capability_invocation_coordinator
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              user_id=NULLIF(current_setting('app.user_id',true),'')::bigint AND status='pending');
CREATE POLICY capability_invocation_update ON capability_invocations
  FOR UPDATE TO vane_capability_invocation_coordinator
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         user_id=NULLIF(current_setting('app.user_id',true),'')::bigint)
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              user_id=NULLIF(current_setting('app.user_id',true),'')::bigint);
CREATE POLICY capability_invocation_receipt_select ON capability_invocation_receipts
  FOR SELECT TO vane_capability_invocation_coordinator
  USING (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
         user_id=NULLIF(current_setting('app.user_id',true),'')::bigint);
CREATE POLICY capability_invocation_receipt_insert ON capability_invocation_receipts
  FOR INSERT TO vane_capability_invocation_coordinator
  WITH CHECK (tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::bigint AND
              user_id=NULLIF(current_setting('app.user_id',true),'')::bigint);

GRANT USAGE ON SCHEMA public TO vane_capability_invocation_coordinator;
GRANT SELECT,INSERT ON capability_invocations TO vane_capability_invocation_coordinator;
GRANT UPDATE(status,lease_owner,lease_until,fence,attempt,current_receipt_ordinal)
  ON capability_invocations TO vane_capability_invocation_coordinator;
GRANT SELECT,INSERT ON capability_invocation_receipts TO vane_capability_invocation_coordinator;
GRANT USAGE,SELECT ON SEQUENCE capability_invocation_receipts_id_seq
  TO vane_capability_invocation_coordinator;

-- +goose StatementBegin
CREATE FUNCTION assert_vane_capability_invocation_coordinator_v152()
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE role_safe boolean; tables_safe boolean; grants_safe boolean; policies_safe boolean;
        expected_acl_count integer; unexpected_acl_count integer;
BEGIN
  IF session_user<>current_user THEN
    RAISE EXCEPTION '152: only direct migration owner may assert capability ledger authority'
      USING ERRCODE='42501';
  END IF;
  SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname='vane_capability_invocation_coordinator' AND NOT rolcanlogin
      AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolinherit
      AND NOT rolreplication AND NOT rolbypassrls
      AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[])
    AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
      JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
      WHERE granted.rolname='vane_capability_invocation_coordinator'
        AND member.rolname='vane_server_runtime') AND
    NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
      JOIN pg_catalog.pg_roles granted ON granted.oid=edge.roleid
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
      WHERE granted.rolname='vane_capability_invocation_coordinator' AND
        (member.rolname<>current_user OR edge.admin_option OR edge.inherit_option OR
         NOT edge.set_option)) AND
    NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
      JOIN pg_catalog.pg_roles member ON member.oid=edge.member
      WHERE member.rolname='vane_capability_invocation_coordinator')
  INTO role_safe;
  SELECT count(*)=2 AND bool_and(relrowsecurity AND relforcerowsecurity)
    FROM pg_catalog.pg_class WHERE oid IN(
      'public.capability_invocations'::regclass,
      'public.capability_invocation_receipts'::regclass)
  INTO tables_safe;
  SELECT has_table_privilege('vane_capability_invocation_coordinator',
           'capability_invocations','SELECT,INSERT') AND
         NOT has_table_privilege('vane_capability_invocation_coordinator',
           'capability_invocations','UPDATE,DELETE,TRUNCATE') AND
         has_column_privilege('vane_capability_invocation_coordinator',
           'capability_invocations','status','UPDATE') AND
         has_table_privilege('vane_capability_invocation_coordinator',
           'capability_invocation_receipts','SELECT,INSERT') AND
         NOT has_table_privilege('vane_capability_invocation_coordinator',
           'capability_invocation_receipts','UPDATE,DELETE,TRUNCATE') AND
         has_sequence_privilege('vane_capability_invocation_coordinator',
           'capability_invocation_receipts_id_seq','USAGE,SELECT')
  INTO grants_safe;
  WITH role_oid AS (SELECT oid FROM pg_catalog.pg_roles
    WHERE rolname='vane_capability_invocation_coordinator'), entries AS (
    SELECT relation.relname object_name,NULL::text column_name,
           acl.privilege_type,acl.is_grantable
      FROM pg_catalog.pg_class relation
      CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) acl
     WHERE relation.relname IN('capability_invocations','capability_invocation_receipts',
                               'capability_invocation_receipts_id_seq')
       AND acl.grantee=(SELECT oid FROM role_oid)
    UNION ALL
    SELECT relation.relname,attribute.attname,acl.privilege_type,acl.is_grantable
      FROM pg_catalog.pg_class relation
      JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid=relation.oid
      CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
     WHERE relation.relname='capability_invocations' AND
           acl.grantee=(SELECT oid FROM role_oid)
    UNION ALL
    SELECT namespace.nspname,NULL::text,acl.privilege_type,acl.is_grantable
      FROM pg_catalog.pg_namespace namespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(namespace.nspacl) acl
     WHERE namespace.nspname='public' AND acl.grantee=(SELECT oid FROM role_oid)
  )
  SELECT count(*) FILTER(WHERE NOT is_grantable AND (
      (object_name='capability_invocations' AND column_name IS NULL AND
       privilege_type IN('SELECT','INSERT')) OR
      (object_name='capability_invocation_receipts' AND column_name IS NULL AND
       privilege_type IN('SELECT','INSERT')) OR
      (object_name='capability_invocations' AND
       column_name IN('status','lease_owner','lease_until','fence','attempt',
                      'current_receipt_ordinal') AND privilege_type='UPDATE') OR
      (object_name='capability_invocation_receipts_id_seq' AND column_name IS NULL AND
       privilege_type IN('SELECT','USAGE')) OR
      (object_name='public' AND column_name IS NULL AND privilege_type='USAGE'))),
    count(*) FILTER(WHERE is_grantable OR NOT (
      (object_name='capability_invocations' AND column_name IS NULL AND
       privilege_type IN('SELECT','INSERT')) OR
      (object_name='capability_invocation_receipts' AND column_name IS NULL AND
       privilege_type IN('SELECT','INSERT')) OR
      (object_name='capability_invocations' AND
       column_name IN('status','lease_owner','lease_until','fence','attempt',
                      'current_receipt_ordinal') AND privilege_type='UPDATE') OR
      (object_name='capability_invocation_receipts_id_seq' AND column_name IS NULL AND
       privilege_type IN('SELECT','USAGE')) OR
      (object_name='public' AND column_name IS NULL AND privilege_type='USAGE')))
    INTO expected_acl_count,unexpected_acl_count FROM entries;
  grants_safe := grants_safe AND expected_acl_count=13 AND unexpected_acl_count=0;
  WITH expected(relname,polname,polcmd,qual,withcheck) AS (VALUES
    ('capability_invocations','capability_invocation_select','r',
     '((tenant_id = (NULLIF(current_setting(''app.tenant_id''::text, true), ''''::text))::bigint) AND (user_id = (NULLIF(current_setting(''app.user_id''::text, true), ''''::text))::bigint))',NULL::text),
    ('capability_invocations','capability_invocation_insert','a',NULL::text,
     '((tenant_id = (NULLIF(current_setting(''app.tenant_id''::text, true), ''''::text))::bigint) AND (user_id = (NULLIF(current_setting(''app.user_id''::text, true), ''''::text))::bigint) AND (status = ''pending''::text))'),
    ('capability_invocations','capability_invocation_update','w',
     '((tenant_id = (NULLIF(current_setting(''app.tenant_id''::text, true), ''''::text))::bigint) AND (user_id = (NULLIF(current_setting(''app.user_id''::text, true), ''''::text))::bigint))',
     '((tenant_id = (NULLIF(current_setting(''app.tenant_id''::text, true), ''''::text))::bigint) AND (user_id = (NULLIF(current_setting(''app.user_id''::text, true), ''''::text))::bigint))'),
    ('capability_invocation_receipts','capability_invocation_receipt_select','r',
     '((tenant_id = (NULLIF(current_setting(''app.tenant_id''::text, true), ''''::text))::bigint) AND (user_id = (NULLIF(current_setting(''app.user_id''::text, true), ''''::text))::bigint))',NULL::text),
    ('capability_invocation_receipts','capability_invocation_receipt_insert','a',NULL::text,
     '((tenant_id = (NULLIF(current_setting(''app.tenant_id''::text, true), ''''::text))::bigint) AND (user_id = (NULLIF(current_setting(''app.user_id''::text, true), ''''::text))::bigint))')
  ), actual AS (
    SELECT relation.relname,policy.polname,policy.polcmd,
           pg_catalog.pg_get_expr(policy.polqual,policy.polrelid) qual,
           pg_catalog.pg_get_expr(policy.polwithcheck,policy.polrelid) withcheck,
           policy.polpermissive,policy.polroles
      FROM pg_catalog.pg_policy policy
      JOIN pg_catalog.pg_class relation ON relation.oid=policy.polrelid
     WHERE policy.polrelid IN('public.capability_invocations'::regclass,
                              'public.capability_invocation_receipts'::regclass)
  )
  SELECT count(*)=5 AND bool_and(actual.relname IS NOT NULL AND expected.relname IS NOT NULL AND
      actual.polpermissive AND
      actual.polroles=ARRAY[(SELECT oid FROM pg_catalog.pg_roles
        WHERE rolname='vane_capability_invocation_coordinator')]::oid[] AND
      actual.polcmd=expected.polcmd AND actual.qual IS NOT DISTINCT FROM expected.qual AND
      actual.withcheck IS NOT DISTINCT FROM expected.withcheck)
    FROM expected FULL JOIN actual USING(relname,polname)
  INTO policies_safe;
  IF NOT role_safe OR NOT tables_safe OR NOT grants_safe OR NOT policies_safe THEN
    RAISE EXCEPTION '152: capability ledger contract unsafe role=% tables=% grants=% policies=%',
      role_safe,tables_safe,grants_safe,policies_safe USING ERRCODE='42501';
  END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION assert_vane_capability_invocation_coordinator_v152() FROM PUBLIC;
SELECT public.assert_vane_capability_invocation_coordinator_v152();

-- +goose Down

-- Freeze the tenant key set, then follow the same admission -> ledger order as
-- Prepare/Acquire/Settle.  This prevents a downgrade from owning the ledger
-- while a runtime transaction owns admission and waits for that ledger (the
-- former reverse-order deadlock).
LOCK TABLE tenants IN SHARE ROW EXCLUSIVE MODE;
-- +goose StatementBegin
DO $capability_invocation_down_admission$
DECLARE tenant_row record;
BEGIN
  -- The tenants table lock freezes the key set.  Acquire the same exclusive
  -- tenant-admission advisory keys that runtime uses in shared mode, in stable
  -- order, before taking either ledger table lock.
  FOR tenant_row IN SELECT id FROM tenants ORDER BY id LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
      'vane/tenant-admission/v1/'||tenant_row.id::text,1447120453));
  END LOOP;
END $capability_invocation_down_admission$;
-- +goose StatementEnd
LOCK TABLE capability_invocation_receipts,capability_invocations IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS(SELECT 1 FROM capability_invocations) OR
     EXISTS(SELECT 1 FROM capability_invocation_receipts) THEN
    RAISE EXCEPTION '152: refusing downgrade while retained capability invocation evidence exists';
  END IF;
END $$;
-- +goose StatementEnd

DROP FUNCTION assert_vane_capability_invocation_coordinator_v152();
DROP TRIGGER capability_invocation_checkpoint_v1 ON capability_invocations;
DROP FUNCTION enforce_capability_invocation_checkpoint_v1();
DROP TRIGGER capability_invocation_receipt_v1 ON capability_invocation_receipts;
DROP FUNCTION enforce_capability_invocation_receipt_v1();
ALTER TABLE capability_invocations DROP CONSTRAINT fk_capability_invocation_current_receipt;
DROP TABLE capability_invocation_receipts;
DROP TABLE capability_invocations;
REVOKE USAGE ON SCHEMA public FROM vane_capability_invocation_coordinator;
