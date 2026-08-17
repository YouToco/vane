-- 155: independently authenticated, fail-closed channel runtime admission.
--
-- This migration intentionally leaves the login NOLOGIN.  Deployment must
-- provision a distinct password and connection string before stored Bots can
-- be activated; the schema-owner connection is never a runtime authority.

-- +goose Up

-- +goose StatementBegin
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                  WHERE rolname='vane_channel_runtime') THEN
    CREATE ROLE vane_channel_runtime NOLOGIN NOSUPERUSER NOCREATEDB
      NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
  END IF;
  ALTER ROLE vane_channel_runtime NOLOGIN PASSWORD NULL NOSUPERUSER NOCREATEDB
    NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
  ALTER ROLE vane_channel_runtime RESET ALL;
  ALTER ROLE vane_channel_runtime SET search_path=pg_catalog,public,pg_temp;
END $$;
-- +goose StatementEnd

CREATE TABLE channel_runtime_authority_attestations (
  credential_id BIGINT PRIMARY KEY REFERENCES credential_vault_entries(id)
    ON DELETE CASCADE,
  tenant_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  credential_generation BIGINT NOT NULL,
  app_identity TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT fk_channel_runtime_authority_membership
    FOREIGN KEY (tenant_id,user_id)
    REFERENCES memberships(tenant_id,user_id) ON DELETE CASCADE,
  CONSTRAINT ck_channel_runtime_authority_generation
    CHECK (credential_generation>0),
  CONSTRAINT ck_channel_runtime_authority_app
    CHECK (app_identity ~ '^[1-9][0-9]{0,19}$'),
  CONSTRAINT ck_channel_runtime_authority_status
    CHECK (status IN ('active','retired','revoked')),
  UNIQUE (tenant_id,user_id,credential_generation,app_identity)
);

-- +goose StatementBegin
CREATE FUNCTION sync_channel_runtime_authority_v155() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
  IF NEW.scope_kind='user' AND NEW.provider='telegram' AND
     NEW.purpose='bot_api' AND NEW.user_id IS NOT NULL AND
     NEW.external_identity IS NOT NULL THEN
    INSERT INTO public.channel_runtime_authority_attestations(
      credential_id,tenant_id,user_id,credential_generation,app_identity,status,
      updated_at)
    VALUES(NEW.id,NEW.tenant_id,NEW.user_id,NEW.generation,
      NEW.external_identity,NEW.status,clock_timestamp())
    ON CONFLICT(credential_id) DO UPDATE SET
      tenant_id=EXCLUDED.tenant_id,user_id=EXCLUDED.user_id,
      credential_generation=EXCLUDED.credential_generation,
      app_identity=EXCLUDED.app_identity,status=EXCLUDED.status,
      updated_at=clock_timestamp();
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION sync_channel_runtime_authority_v155() FROM PUBLIC;

CREATE TRIGGER credential_channel_runtime_authority_v155
AFTER INSERT OR UPDATE OF status,generation,external_identity
ON credential_vault_entries FOR EACH ROW
EXECUTE FUNCTION sync_channel_runtime_authority_v155();

INSERT INTO channel_runtime_authority_attestations(
  credential_id,tenant_id,user_id,credential_generation,app_identity,status)
SELECT id,tenant_id,user_id,generation,external_identity,status
FROM credential_vault_entries
WHERE scope_kind='user' AND provider='telegram' AND purpose='bot_api'
  AND user_id IS NOT NULL AND external_identity IS NOT NULL;

REVOKE ALL ON channel_runtime_authority_attestations
  FROM PUBLIC,vane_app,vane_channel_runtime;
ALTER TABLE channel_runtime_authority_attestations ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_runtime_authority_attestations FORCE ROW LEVEL SECURITY;
CREATE POLICY channel_runtime_authority_exact_principal
ON channel_runtime_authority_attestations AS PERMISSIVE
FOR SELECT TO vane_channel_runtime USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);

-- +goose StatementBegin
CREATE FUNCTION attest_channel_runtime_authority_v155(
  requested_tenant bigint,requested_user bigint,requested_generation bigint,
  requested_app_identity text) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE admitted_role text;
BEGIN
  IF session_user<>'vane_channel_runtime' OR
     NULLIF(current_setting('app.tenant_id',true),'')::bigint<>requested_tenant OR
     NULLIF(current_setting('app.user_id',true),'')::bigint<>requested_user THEN
    RAISE EXCEPTION '155: channel runtime scope mismatch' USING ERRCODE='42501';
  END IF;
  SELECT m.role INTO STRICT admitted_role
    FROM public.credential_vault_entries c
    JOIN public.memberships m ON m.tenant_id=c.tenant_id AND m.user_id=c.user_id
    JOIN public.tenants t ON t.id=c.tenant_id
   WHERE c.scope_kind='user' AND c.tenant_id=requested_tenant AND
         c.user_id=requested_user AND c.provider='telegram' AND
         c.purpose='bot_api' AND c.generation=requested_generation AND
         c.external_identity=requested_app_identity AND c.status='active' AND
         t.status='active' AND t.deleted_at IS NULL
   FOR SHARE OF c,m,t;
  RETURN admitted_role;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE EXCEPTION '155: channel runtime authority inactive' USING ERRCODE='42501';
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION attest_channel_runtime_authority_v155(
  bigint,bigint,bigint,text) FROM PUBLIC;

GRANT USAGE ON SCHEMA public TO vane_channel_runtime;
GRANT SELECT ON channel_runtime_authority_attestations TO vane_channel_runtime;
GRANT SELECT(id,status,deleted_at) ON tenants TO vane_channel_runtime;
GRANT SELECT(tenant_id,user_id,role) ON memberships TO vane_channel_runtime;
GRANT EXECUTE ON FUNCTION attest_channel_runtime_authority_v155(
  bigint,bigint,bigint,text) TO vane_channel_runtime;
GRANT SELECT ON channel_identities,channel_routes,channel_ingress_receipts,
  channel_outbound_effects TO vane_channel_runtime;
-- UPDATE(status) is the minimum PostgreSQL privilege required to take a row
-- lock. Identity/route CHECK constraints require revoked_at as well, so this
-- grant cannot independently revoke either authority.
GRANT UPDATE(status) ON channel_identities,channel_routes
  TO vane_channel_runtime;
GRANT UPDATE(status,processing_lease,lease_expires_at,attempt_count,updated_at,
  next_send_at) ON channel_ingress_receipts TO vane_channel_runtime;
GRANT UPDATE(status,updated_at,next_send_at) ON channel_outbound_effects
  TO vane_channel_runtime;

-- +goose Down

-- +goose StatementBegin
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
              WHERE rolname='vane_channel_runtime' AND rolcanlogin) THEN
    RAISE EXCEPTION '155: deprovision vane_channel_runtime login before downgrade';
  END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER credential_channel_runtime_authority_v155
  ON credential_vault_entries;
DROP FUNCTION IF EXISTS sync_channel_runtime_authority_v155();
DROP FUNCTION IF EXISTS attest_channel_runtime_authority_v155(bigint,bigint,bigint,text);
DROP TABLE channel_runtime_authority_attestations;
REVOKE SELECT(id,status,deleted_at) ON tenants FROM vane_channel_runtime;
REVOKE SELECT(tenant_id,user_id,role) ON memberships FROM vane_channel_runtime;
REVOKE ALL ON channel_identities,channel_routes,channel_ingress_receipts,
  channel_outbound_effects FROM vane_channel_runtime;
REVOKE UPDATE(status) ON channel_identities,channel_routes FROM vane_channel_runtime;
REVOKE UPDATE(status,processing_lease,lease_expires_at,attempt_count,updated_at,
  next_send_at) ON channel_ingress_receipts FROM vane_channel_runtime;
REVOKE UPDATE(status,updated_at,next_send_at) ON channel_outbound_effects
  FROM vane_channel_runtime;
REVOKE ALL ON SCHEMA public FROM vane_channel_runtime;
