-- 102: delivery-dark V3 definition preparation and atomic DB-head promotion.
-- Preparing a definition never mutates the production Schedule projection.

-- +goose Up

-- Serialize the compatibility decision with every migration-101 journal and
-- authority writer for the full migration transaction.
LOCK TABLE research_v3_cutover_operations,research_v3_delivery_authorities
    IN ACCESS EXCLUSIVE MODE;

-- Migration cannot observe Temporal's remote Action/pause state. Refuse every
-- live 101 journal instead of guessing a rollback checkpoint during DDL.
-- Operators must finish rollback/abort under the 101 binary, then retry.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM research_v3_cutover_operations
         WHERE phase NOT IN ('rolled_back','aborted','manual_intervention')
    ) OR EXISTS (
        SELECT 1 FROM research_v3_delivery_authorities
         WHERE status IN ('staged','enabled')
    ) THEN
        RAISE EXCEPTION '102: non-terminal migration 101 V3 cutover journal or live authority exists'
            USING ERRCODE='55000';
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE research_v3_definition_prepare_operations (
    id                         BIGSERIAL PRIMARY KEY,
    tenant_id                  BIGINT NOT NULL,
    user_id                    BIGINT NOT NULL,
    task_id                    TEXT NOT NULL,
    idempotency_key            TEXT NOT NULL,
    target_definition_version  BIGINT NOT NULL,
    target_definition_digest   TEXT NOT NULL,
    previous_definition_version BIGINT,
    previous_definition_digest  TEXT,
    source_baseline_digest       TEXT NOT NULL,
    original_execution_mode    TEXT NOT NULL,
    original_definition_version BIGINT,
    original_definition_digest  TEXT,
    phase                      TEXT NOT NULL DEFAULT 'prepared',
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_research_v3_definition_prepare_key
        UNIQUE (tenant_id,user_id,task_id,idempotency_key),
    CONSTRAINT fk_research_v3_definition_prepare_target
        FOREIGN KEY (tenant_id,user_id,task_id,target_definition_version,
                     target_definition_digest)
        REFERENCES task_approved_definition_versions
            (tenant_id,user_id,task_id,version,definition_digest)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT fk_research_v3_definition_prepare_original
	    FOREIGN KEY (tenant_id,user_id,task_id,original_definition_version,
	                 original_definition_digest)
	    REFERENCES task_approved_definition_versions
	        (tenant_id,user_id,task_id,version,definition_digest)
	    DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_research_v3_definition_prepare_identity CHECK (
        tenant_id>0 AND user_id>0 AND
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(idempotency_key)=idempotency_key AND
        octet_length(idempotency_key) BETWEEN 1 AND 512 AND
        target_definition_version>0 AND
        target_definition_digest ~ '^[0-9a-f]{64}$' AND
        source_baseline_digest ~ '^[0-9a-f]{64}$' AND
        original_execution_mode IN ('compiled','discover_at_run') AND
        ((previous_definition_version IS NULL AND previous_definition_digest IS NULL) OR
         (previous_definition_version>0 AND previous_definition_digest ~ '^[0-9a-f]{64}$')) AND
        ((original_definition_version IS NULL AND original_definition_digest IS NULL) OR
         (original_definition_version>0 AND original_definition_digest ~ '^[0-9a-f]{64}$'))),
    CONSTRAINT ck_research_v3_definition_prepare_phase
        CHECK (phase IN ('prepared','rolled_back'))
);

CREATE TABLE research_v3_prepared_definition_heads (
    tenant_id          BIGINT NOT NULL,
    user_id            BIGINT NOT NULL,
    task_id            TEXT NOT NULL,
    definition_version BIGINT NOT NULL,
    definition_digest  TEXT NOT NULL,
    execution_mode     TEXT NOT NULL DEFAULT 'discover_at_run',
    prepare_operation_id BIGINT NOT NULL,
    base_execution_mode TEXT NOT NULL,
    base_definition_version BIGINT,
    base_definition_digest TEXT,
    source_baseline_digest TEXT NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,task_id),
    CONSTRAINT fk_research_v3_prepared_schedule
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT fk_research_v3_prepared_definition
        FOREIGN KEY (tenant_id,user_id,task_id,definition_version,
                     definition_digest,execution_mode)
        REFERENCES task_approved_definition_versions
            (tenant_id,user_id,task_id,version,definition_digest,execution_mode)
        ON DELETE RESTRICT,
    CONSTRAINT fk_research_v3_prepared_operation
        FOREIGN KEY (prepare_operation_id)
        REFERENCES research_v3_definition_prepare_operations(id) ON DELETE CASCADE,
    CONSTRAINT ck_research_v3_prepared_head CHECK (
        definition_version>0 AND definition_digest ~ '^[0-9a-f]{64}$' AND
        execution_mode='discover_at_run' AND
        base_execution_mode IN ('compiled','discover_at_run') AND
        source_baseline_digest ~ '^[0-9a-f]{64}$' AND
        ((base_definition_version IS NULL AND base_definition_digest IS NULL) OR
         (base_definition_version>0 AND base_definition_digest ~ '^[0-9a-f]{64}$')))
);

ALTER TABLE research_v3_cutover_operations
    ADD COLUMN original_execution_mode TEXT,
    ADD COLUMN original_definition_version BIGINT,
    ADD COLUMN original_definition_digest TEXT,
    ADD COLUMN source_baseline_digest TEXT;

-- Migration 101's immutable trigger intentionally rejects same-phase UPDATEs.
-- The owner must disable only that named trigger while backfilling newly added
-- audit columns, then restore it before any other statement can observe DDL.
ALTER TABLE research_v3_cutover_operations
    DISABLE TRIGGER protect_research_v3_cutover_operation;

UPDATE research_v3_cutover_operations operation
   SET original_execution_mode=schedule.execution_mode,
       original_definition_version=schedule.approved_definition_version,
       original_definition_digest=schedule.approved_definition_digest,
       source_baseline_digest=operation.definition_digest
  FROM schedules schedule
 WHERE schedule.tenant_id=operation.tenant_id
   AND schedule.user_id=operation.user_id AND schedule.id=operation.task_id;

-- A historical journal can outlive a normally deleted Schedule. Migration
-- 101 admitted only a current V3 head, so its exact target is the recoverable
-- legacy fallback when the mirror is already gone.
UPDATE research_v3_cutover_operations
   SET original_execution_mode='discover_at_run',
       original_definition_version=definition_version,
       original_definition_digest=definition_digest,
       source_baseline_digest=definition_digest
 WHERE original_execution_mode IS NULL;

ALTER TABLE research_v3_cutover_operations
    ENABLE TRIGGER protect_research_v3_cutover_operation;

ALTER TABLE research_v3_cutover_operations
    ALTER COLUMN original_execution_mode SET NOT NULL,
    ALTER COLUMN source_baseline_digest SET NOT NULL,
    ADD CONSTRAINT ck_research_v3_cutover_original_head CHECK (
        original_execution_mode IN ('compiled','discover_at_run') AND
        source_baseline_digest ~ '^[0-9a-f]{64}$' AND
        ((original_definition_version IS NULL AND original_definition_digest IS NULL) OR
         (original_definition_version>0 AND original_definition_digest ~ '^[0-9a-f]{64}$')));

ALTER TABLE research_v3_cutover_operations DROP CONSTRAINT ck_research_v3_cutover_phase;
ALTER TABLE research_v3_cutover_operations ADD CONSTRAINT ck_research_v3_cutover_phase
    CHECK (phase IN (
        'prepared','pause_requested','paused','definition_promoted','action_swapped','active',
        'rollback_pause_requested','rollback_paused','definition_restored','rolled_back',
        'aborted','manual_intervention'));

GRANT SELECT,INSERT,UPDATE (phase,updated_at)
    ON research_v3_definition_prepare_operations TO vane_research_v3_cutover_operator;
GRANT SELECT,INSERT,UPDATE,DELETE
    ON research_v3_prepared_definition_heads TO vane_research_v3_cutover_operator;
GRANT USAGE,SELECT ON SEQUENCE research_v3_definition_prepare_operations_id_seq
    TO vane_research_v3_cutover_operator;
GRANT SELECT ON schedule_playbooks TO vane_research_v3_cutover_operator;
GRANT INSERT (tenant_id,user_id,task_id,version,schema_version,execution_mode,
              definition_digest,payload,operation_ref)
    ON task_approved_definition_versions TO vane_research_v3_cutover_operator;
GRANT UPDATE (execution_mode,approved_definition_version,approved_definition_digest)
    ON schedules TO vane_research_v3_cutover_operator;
GRANT INSERT (original_execution_mode,original_definition_version,original_definition_digest,
              source_baseline_digest)
    ON research_v3_cutover_operations TO vane_research_v3_cutover_operator;
GRANT SELECT (tenant_id,user_id,task_id,definition_version,definition_digest,
              execution_mode,prepare_operation_id,base_execution_mode,
              base_definition_version,base_definition_digest,source_baseline_digest,updated_at)
    ON research_v3_prepared_definition_heads TO vane_app;

-- The long-lived server starts as vane_app with no ambient RLS scope. Resolve
-- only an exact authenticated owner tuple, then Go binds the returned tenant
-- and supplied user with transaction-local GUCs before reading any task row.
-- +goose StatementBegin
CREATE FUNCTION resolve_owned_schedule_tenant_v1(requested_task_id TEXT,
    requested_user_id BIGINT) RETURNS BIGINT
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE resolved_tenant BIGINT;
BEGIN
    IF (session_user<>current_user AND session_user<>'vane_server_runtime') OR
       requested_user_id<=0 OR requested_task_id IS NULL OR
       octet_length(requested_task_id) NOT BETWEEN 1 AND 255 THEN
        RAISE EXCEPTION '102: owned schedule resolver caller is forbidden'
            USING ERRCODE='42501';
    END IF;
    SELECT schedule.tenant_id INTO resolved_tenant
      FROM public.schedules schedule
      JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
      JOIN public.memberships membership
        ON membership.tenant_id=schedule.tenant_id
       AND membership.user_id=schedule.user_id
     WHERE schedule.id=requested_task_id
       AND schedule.user_id=requested_user_id
       AND schedule.status IN ('active','paused')
       AND tenant.status='active' AND tenant.deleted_at IS NULL
     LIMIT 1;
    RETURN resolved_tenant;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION resolve_owned_schedule_tenant_v1(TEXT,BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_owned_schedule_tenant_v1(TEXT,BIGINT) TO vane_app;

-- Exact-snapshot capability binders keep the long-lived server away from the
-- capability table while allowing snapshot+capability atomicity. They return
-- only the single registration named by the complete immutable V3 reference.
-- +goose StatementBegin
CREATE FUNCTION resolve_research_run_capability_registration_v1(
    requested_snapshot_id BIGINT,requested_tenant_id BIGINT,
    requested_user_id BIGINT,requested_task_id TEXT,
    requested_workflow_id TEXT,requested_temporal_run_id TEXT,
    requested_reference_digest TEXT
) RETURNS TABLE(out_id BIGINT,out_run_snapshot_id BIGINT,out_tenant_id BIGINT,
    out_user_id BIGINT,out_task_id TEXT,out_workflow_id TEXT,out_temporal_run_id TEXT,
    out_reference_digest TEXT,out_key_id TEXT,out_generation INTEGER,
    out_capability_hash BYTEA,out_not_after TIMESTAMPTZ)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF session_user<>current_user AND session_user<>'vane_server_runtime' THEN
        RAISE EXCEPTION '102: capability registration resolver caller is forbidden'
            USING ERRCODE='42501';
    END IF;
    IF requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '102: capability registration resolver scope is forbidden'
            USING ERRCODE='42501';
    END IF;
    RETURN QUERY
    SELECT capability.id,capability.run_snapshot_id,capability.tenant_id,
           capability.user_id,capability.task_id,capability.temporal_workflow_id,
           capability.temporal_run_id,capability.reference_digest,
           capability.key_id,capability.generation,capability.capability_hash,
           capability.not_after
      FROM public.research_run_capabilities capability
      JOIN public.task_run_snapshots snapshot
        ON snapshot.id=capability.run_snapshot_id
       AND snapshot.tenant_id=capability.tenant_id
       AND snapshot.user_id=capability.user_id
       AND snapshot.task_id=capability.task_id
       AND snapshot.temporal_workflow_id=capability.temporal_workflow_id
       AND snapshot.temporal_run_id=capability.temporal_run_id
       AND snapshot.reference_digest=capability.reference_digest
     WHERE capability.run_snapshot_id=requested_snapshot_id
       AND capability.tenant_id=requested_tenant_id
       AND capability.user_id=requested_user_id
       AND capability.task_id=requested_task_id
       AND capability.temporal_workflow_id=requested_workflow_id
       AND capability.temporal_run_id=requested_temporal_run_id
       AND capability.reference_digest=requested_reference_digest
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND capability.revoked_at IS NULL
     ORDER BY capability.generation DESC LIMIT 1;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION resolve_research_run_capability_registration_v1(
    BIGINT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_research_run_capability_registration_v1(
    BIGINT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT) TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION register_research_run_capability_registration_v1(
    requested_snapshot_id BIGINT,requested_tenant_id BIGINT,
    requested_user_id BIGINT,requested_task_id TEXT,
    requested_workflow_id TEXT,requested_temporal_run_id TEXT,
    requested_reference_digest TEXT,requested_key_id TEXT,
    requested_capability_hash BYTEA,requested_not_after TIMESTAMPTZ
) RETURNS TABLE(out_id BIGINT,out_run_snapshot_id BIGINT,out_tenant_id BIGINT,
    out_user_id BIGINT,out_task_id TEXT,out_workflow_id TEXT,out_temporal_run_id TEXT,
    out_reference_digest TEXT,out_key_id TEXT,out_generation INTEGER,
    out_capability_hash BYTEA,out_not_after TIMESTAMPTZ)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
    IF (session_user<>current_user AND session_user<>'vane_server_runtime') OR
       requested_snapshot_id<=0 OR requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_task_id IS NULL OR octet_length(requested_task_id) NOT BETWEEN 1 AND 255 OR
       requested_workflow_id IS NULL OR octet_length(requested_workflow_id) NOT BETWEEN 1 AND 512 OR
       requested_temporal_run_id IS NULL OR octet_length(requested_temporal_run_id) NOT BETWEEN 1 AND 512 OR
       requested_reference_digest !~ '^[0-9a-f]{64}$' OR
       requested_key_id !~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' OR
       octet_length(requested_capability_hash)<>32 OR
       requested_not_after<=statement_timestamp() THEN
        RAISE EXCEPTION '102: capability registration input is forbidden'
            USING ERRCODE='42501';
    END IF;
    IF requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '102: capability registration scope is forbidden'
            USING ERRCODE='42501';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('research-capability/v1:'||requested_snapshot_id::text,0));
    IF NOT EXISTS (
        SELECT 1 FROM public.task_run_snapshots snapshot
         WHERE snapshot.id=requested_snapshot_id
           AND snapshot.tenant_id=requested_tenant_id
           AND snapshot.user_id=requested_user_id
           AND snapshot.task_id=requested_task_id
           AND snapshot.temporal_workflow_id=requested_workflow_id
           AND snapshot.temporal_run_id=requested_temporal_run_id
           AND snapshot.reference_digest=requested_reference_digest
           AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
    ) THEN
        RAISE EXCEPTION '102: exact V3 snapshot is unavailable'
            USING ERRCODE='42501';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.research_run_capabilities capability
         WHERE capability.run_snapshot_id=requested_snapshot_id
           AND capability.revoked_at IS NULL
    ) THEN
        INSERT INTO public.research_run_capabilities(
            run_snapshot_id,tenant_id,user_id,task_id,temporal_workflow_id,
            temporal_run_id,reference_digest,key_id,generation,capability_hash,not_after)
        VALUES(requested_snapshot_id,requested_tenant_id,requested_user_id,
            requested_task_id,requested_workflow_id,requested_temporal_run_id,
            requested_reference_digest,requested_key_id,1,
            requested_capability_hash,requested_not_after);
    END IF;
    RETURN QUERY SELECT * FROM public.resolve_research_run_capability_registration_v1(
        requested_snapshot_id,requested_tenant_id,requested_user_id,requested_task_id,
        requested_workflow_id,requested_temporal_run_id,requested_reference_digest);
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION register_research_run_capability_registration_v1(
    BIGINT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,BYTEA,TIMESTAMPTZ) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION register_research_run_capability_registration_v1(
    BIGINT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT,BYTEA,TIMESTAMPTZ) TO vane_app;

ALTER TABLE research_v3_definition_prepare_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_v3_prepared_definition_heads ENABLE ROW LEVEL SECURITY;

-- Every mutable input to the source baseline participates in the same exact-
-- task advisory lock held by prepare, shadow snapshot admission and cutover.
-- This closes the validation-to-snapshot gap without granting vane_app row
-- mutation privileges merely to take SELECT row locks.
-- +goose StatementBegin
CREATE FUNCTION lock_research_v3_baseline_write() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE locked_tenant BIGINT; locked_user BIGINT; locked_task TEXT;
BEGIN
    IF TG_TABLE_NAME='schedules' THEN
        locked_tenant:=OLD.tenant_id; locked_user:=OLD.user_id; locked_task:=OLD.id;
    ELSIF TG_TABLE_NAME='research_v3_prepared_definition_heads' THEN
        locked_tenant:=COALESCE(NEW.tenant_id,OLD.tenant_id);
        locked_user:=COALESCE(NEW.user_id,OLD.user_id);
        locked_task:=COALESCE(NEW.task_id,OLD.task_id);
    ELSE
        SELECT schedule.tenant_id,schedule.user_id,schedule.id
          INTO locked_tenant,locked_user,locked_task
          FROM public.schedules schedule
         WHERE schedule.id=COALESCE(NEW.schedule_id,OLD.schedule_id);
    END IF;
    IF locked_tenant IS NOT NULL THEN
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            locked_tenant::text||'/'||locked_user::text||'/'||locked_task,101));
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_research_v3_baseline_write() FROM PUBLIC;
CREATE TRIGGER lock_research_v3_schedule_baseline_write
BEFORE UPDATE OF nl_description,spec_json,push_strictness,execution_mode,
    approved_definition_version,approved_definition_digest ON schedules
FOR EACH ROW EXECUTE FUNCTION lock_research_v3_baseline_write();
CREATE TRIGGER lock_research_v3_playbook_baseline_write
BEFORE INSERT OR UPDATE OR DELETE ON schedule_playbooks
FOR EACH ROW EXECUTE FUNCTION lock_research_v3_baseline_write();

-- Owner/tenant revocation must serialize before the same exact-task lock used
-- by prepare, cutover and snapshot creation. Row locks alone are insufficient:
-- an INSERT that waited on FOR SHARE may retain its older statement snapshot.
-- +goose StatementBegin
CREATE FUNCTION lock_research_v3_tenant_authorization_write() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE task RECORD;
BEGIN
    IF TG_OP='UPDATE' AND NEW.status IS NOT DISTINCT FROM OLD.status AND
       NEW.deleted_at IS NOT DISTINCT FROM OLD.deleted_at THEN
        RETURN NEW;
    END IF;
    FOR task IN
        SELECT schedule.tenant_id,schedule.user_id,schedule.id
          FROM public.schedules schedule
         WHERE schedule.tenant_id=OLD.id
         ORDER BY schedule.user_id,schedule.id
    LOOP
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            pg_catalog.format('%s/%s/%s',task.tenant_id,task.user_id,task.id),101));
    END LOOP;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_research_v3_tenant_authorization_write() FROM PUBLIC;
CREATE TRIGGER lock_research_v3_tenant_authorization_write
BEFORE UPDATE OF status,deleted_at OR DELETE ON tenants
FOR EACH ROW EXECUTE FUNCTION lock_research_v3_tenant_authorization_write();

-- +goose StatementBegin
CREATE FUNCTION lock_research_v3_membership_authorization_write() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE task RECORD;
BEGIN
    IF TG_OP='UPDATE' AND NEW.role IS NOT DISTINCT FROM OLD.role THEN
        RETURN NEW;
    END IF;
    FOR task IN
        SELECT schedule.tenant_id,schedule.user_id,schedule.id
          FROM public.schedules schedule
         WHERE schedule.tenant_id=OLD.tenant_id AND schedule.user_id=OLD.user_id
         ORDER BY schedule.id
    LOOP
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            pg_catalog.format('%s/%s/%s',task.tenant_id,task.user_id,task.id),101));
    END LOOP;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION lock_research_v3_membership_authorization_write() FROM PUBLIC;
CREATE TRIGGER lock_research_v3_membership_authorization_write
BEFORE UPDATE OF role OR DELETE ON memberships
FOR EACH ROW EXECUTE FUNCTION lock_research_v3_membership_authorization_write();

CREATE TRIGGER lock_research_v3_sidecar_baseline_write
BEFORE INSERT OR UPDATE OR DELETE ON research_v3_prepared_definition_heads
FOR EACH ROW EXECUTE FUNCTION lock_research_v3_baseline_write();

-- +goose StatementBegin
DO $$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_v3_definition_prepare_operations','research_v3_prepared_definition_heads'
    ] LOOP
        EXECUTE format('CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',relation_name);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL USING '
            '(tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint)',
            relation_name);
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL USING '
            '(user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',
            relation_name);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION protect_research_v3_definition_prepare_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    NEW.updated_at:=clock_timestamp();
	IF OLD.phase='prepared' AND NEW.phase='rolled_back' AND EXISTS (
	    SELECT 1 FROM public.research_v3_prepared_definition_heads head
	     WHERE head.prepare_operation_id=OLD.id
	) THEN
	    RAISE EXCEPTION '102: cannot rollback a published V3 prepare operation';
	END IF;
    IF ROW(NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.idempotency_key,
           NEW.target_definition_version,NEW.target_definition_digest,
           NEW.previous_definition_version,NEW.previous_definition_digest,
           NEW.source_baseline_digest,
           NEW.original_execution_mode,NEW.original_definition_version,
           NEW.original_definition_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.idempotency_key,
           OLD.target_definition_version,OLD.target_definition_digest,
           OLD.previous_definition_version,OLD.previous_definition_digest,
           OLD.source_baseline_digest,
           OLD.original_execution_mode,OLD.original_definition_version,
           OLD.original_definition_digest,OLD.created_at) OR
       NOT (OLD.phase='prepared' AND NEW.phase='rolled_back') THEN
        RAISE EXCEPTION '102: illegal V3 definition prepare transition';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_v3_definition_prepare_transition() FROM PUBLIC;
CREATE TRIGGER protect_research_v3_definition_prepare_operation
BEFORE UPDATE ON research_v3_definition_prepare_operations
FOR EACH ROW EXECUTE FUNCTION protect_research_v3_definition_prepare_transition();

-- A sidecar is not merely a pointer to V3 bytes: it freezes the exact legacy
-- production head and canonical source projection that shadow validated.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_v3_prepared_binding() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        NEW.tenant_id::text||'/'||NEW.user_id::text||'/'||NEW.task_id,101));
    IF NOT EXISTS (
        SELECT 1 FROM public.research_v3_definition_prepare_operations operation
         WHERE operation.id=NEW.prepare_operation_id
           AND operation.tenant_id=NEW.tenant_id
           AND operation.user_id=NEW.user_id
           AND operation.task_id=NEW.task_id
           AND operation.target_definition_version=NEW.definition_version
           AND operation.target_definition_digest=NEW.definition_digest
           AND operation.original_execution_mode=NEW.base_execution_mode
           AND operation.original_definition_version IS NOT DISTINCT FROM
               NEW.base_definition_version
           AND operation.original_definition_digest IS NOT DISTINCT FROM
               NEW.base_definition_digest
           AND operation.source_baseline_digest=NEW.source_baseline_digest
           AND operation.phase='prepared'
    ) THEN
        RAISE EXCEPTION '102: prepared V3 sidecar binding mismatch';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_v3_prepared_binding() FROM PUBLIC;
CREATE TRIGGER enforce_research_v3_prepared_binding
BEFORE INSERT OR UPDATE ON research_v3_prepared_definition_heads
FOR EACH ROW EXECUTE FUNCTION enforce_research_v3_prepared_binding();

-- Shadow runs bind the exact prepared sidecar while production remains on its
-- old compiled head. Formal V3 runs continue to require the production head.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION task_run_snapshot_v3_admission_fence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
    schedule_status TEXT;
    schedule_execution_mode TEXT;
    selected_definition_digest TEXT;
    approved_schema_version TEXT;
    approved_execution_mode TEXT;
    is_shadow BOOLEAN := NEW.temporal_workflow_id ~ '^research-v3-shadow-[0-9a-f]{64}$';
BEGIN
    IF is_shadow THEN
        SELECT schedule.status,schedule.execution_mode,head.definition_digest,
               definition.schema_version,definition.execution_mode
          INTO schedule_status,schedule_execution_mode,selected_definition_digest,
               approved_schema_version,approved_execution_mode
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
          JOIN public.research_v3_prepared_definition_heads head
            ON head.tenant_id=schedule.tenant_id AND head.user_id=schedule.user_id
           AND head.task_id=schedule.id
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=head.tenant_id AND definition.user_id=head.user_id
           AND definition.task_id=head.task_id AND definition.version=head.definition_version
           AND definition.definition_digest=head.definition_digest
           AND definition.execution_mode=head.execution_mode
         WHERE schedule.tenant_id=NEW.tenant_id AND schedule.user_id=NEW.user_id
           AND schedule.id=NEW.task_id
           AND tenant.status='active' AND tenant.deleted_at IS NULL
           AND membership.role='owner'
         FOR SHARE OF schedule,tenant,membership,head,definition;
    ELSE
        SELECT schedule.status,schedule.execution_mode,schedule.approved_definition_digest,
               definition.schema_version,definition.execution_mode
          INTO schedule_status,schedule_execution_mode,selected_definition_digest,
               approved_schema_version,approved_execution_mode
          FROM public.schedules schedule
          JOIN public.tenants tenant ON tenant.id=schedule.tenant_id
          JOIN public.memberships membership
            ON membership.tenant_id=schedule.tenant_id
           AND membership.user_id=schedule.user_id
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=schedule.tenant_id
           AND definition.user_id=schedule.user_id AND definition.task_id=schedule.id
           AND definition.version=schedule.approved_definition_version
           AND definition.definition_digest=schedule.approved_definition_digest
         WHERE schedule.tenant_id=NEW.tenant_id AND schedule.user_id=NEW.user_id
           AND schedule.id=NEW.task_id
           AND tenant.status='active' AND tenant.deleted_at IS NULL
           AND membership.role='owner'
         FOR SHARE OF schedule,tenant,membership,definition;
    END IF;
    IF schedule_status IS NULL OR
       (is_shadow AND schedule_status<>'active') OR
       (NOT is_shadow AND schedule_status<>'active' AND NOT (
           schedule_status='paused' AND public.authorize_manual_task_run_v1(
               NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.temporal_workflow_id))) OR
       NEW.execution_mode<>'discover_at_run' OR
       (NOT is_shadow AND schedule_execution_mode<>'discover_at_run') OR
       NEW.adaptive_version<>0 OR NEW.plan_digest<>'' OR
       NEW.v2_cutover_event_id IS NOT NULL OR
       NEW.definition_digest IS DISTINCT FROM selected_definition_digest OR
       approved_schema_version IS DISTINCT FROM 'vane.task-approved-definition/v3' OR
       approved_execution_mode IS DISTINCT FROM 'discover_at_run' THEN
        RAISE EXCEPTION '102: research snapshot admission fence rejected task state'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION task_run_snapshot_v3_admission_fence() FROM PUBLIC;

-- Replace the phase transition guard to include DB-head promotion/restoration.
DROP TRIGGER protect_research_v3_cutover_operation ON research_v3_cutover_operations;
DROP FUNCTION protect_research_v3_cutover_transition();
-- +goose StatementBegin
CREATE FUNCTION protect_research_v3_cutover_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    NEW.updated_at:=clock_timestamp();
    IF ROW(NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.idempotency_key,
           NEW.generation,NEW.definition_version,NEW.definition_digest,
           NEW.frozen_schedule,NEW.frozen_schedule_digest,
           NEW.frozen_conflict_token,NEW.conflict_token_digest,
           CASE WHEN OLD.rollback_conflict_token IS NULL THEN NULL ELSE NEW.rollback_conflict_token END,
           CASE WHEN OLD.rollback_token_digest IS NULL THEN NULL ELSE NEW.rollback_token_digest END,
           NEW.target_action,NEW.target_action_digest,NEW.action_authorization_digest,
           NEW.original_paused,NEW.original_execution_mode,
           NEW.original_definition_version,NEW.original_definition_digest,
           NEW.source_baseline_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.idempotency_key,
           OLD.generation,OLD.definition_version,OLD.definition_digest,
           OLD.frozen_schedule,OLD.frozen_schedule_digest,
           OLD.frozen_conflict_token,OLD.conflict_token_digest,
           CASE WHEN OLD.rollback_conflict_token IS NULL THEN NULL ELSE OLD.rollback_conflict_token END,
           CASE WHEN OLD.rollback_token_digest IS NULL THEN NULL ELSE OLD.rollback_token_digest END,
           OLD.target_action,OLD.target_action_digest,OLD.action_authorization_digest,
           OLD.original_paused,OLD.original_execution_mode,
           OLD.original_definition_version,OLD.original_definition_digest,
           OLD.source_baseline_digest,OLD.created_at) THEN
        RAISE EXCEPTION '102: immutable V3 cutover evidence changed';
    END IF;
    IF NOT (
        (OLD.phase='prepared' AND NEW.phase IN ('pause_requested','rollback_paused','aborted','manual_intervention')) OR
        (OLD.phase='pause_requested' AND NEW.phase IN ('paused','rollback_paused','aborted','manual_intervention')) OR
        (OLD.phase='paused' AND NEW.phase IN ('definition_promoted','rollback_paused','manual_intervention')) OR
        (OLD.phase='definition_promoted' AND NEW.phase IN ('action_swapped','rollback_paused','manual_intervention')) OR
        (OLD.phase='action_swapped' AND NEW.phase IN ('active','rollback_pause_requested','rollback_paused','manual_intervention')) OR
        (OLD.phase='active' AND NEW.phase IN ('rollback_pause_requested','rollback_paused','manual_intervention')) OR
        (OLD.phase='rollback_pause_requested' AND NEW.phase IN ('rollback_paused','manual_intervention')) OR
        (OLD.phase='rollback_paused' AND NEW.phase IN ('definition_restored','manual_intervention')) OR
        (OLD.phase='definition_restored' AND NEW.phase IN ('rolled_back','manual_intervention'))
    ) THEN RAISE EXCEPTION '102: illegal V3 cutover phase transition'; END IF;
    IF NEW.phase IN ('rolled_back','aborted','manual_intervention') AND NOT EXISTS (
        SELECT 1 FROM public.research_v3_delivery_authorities authority
         WHERE authority.tenant_id=NEW.tenant_id AND authority.user_id=NEW.user_id
           AND authority.task_id=NEW.task_id AND authority.generation=NEW.generation
           AND authority.status='revoked') THEN
        RAISE EXCEPTION '102: rollback checkpoint requires revoked authority';
    END IF;
    IF NEW.phase='rollback_pause_requested' AND
       (OLD.rollback_conflict_token IS NOT NULL OR NEW.rollback_conflict_token IS NULL OR
        NEW.rollback_token_digest IS NULL) THEN
        RAISE EXCEPTION '102: rollback pause request token is invalid';
    END IF;
    IF OLD.rollback_conflict_token IS NULL AND NEW.rollback_conflict_token IS NOT NULL AND
       NEW.phase<>'rollback_pause_requested' THEN
        RAISE EXCEPTION '102: rollback pause token changed outside request transition';
    END IF;
    IF OLD.rollback_conflict_token IS NOT NULL AND
       (NEW.rollback_conflict_token IS DISTINCT FROM OLD.rollback_conflict_token OR
        NEW.rollback_token_digest IS DISTINCT FROM OLD.rollback_token_digest) THEN
        RAISE EXCEPTION '102: rollback pause request token is immutable';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_v3_cutover_transition() FROM PUBLIC;
CREATE TRIGGER protect_research_v3_cutover_operation
BEFORE UPDATE ON research_v3_cutover_operations
FOR EACH ROW EXECUTE FUNCTION protect_research_v3_cutover_transition();

-- +goose Down
-- This changes the admission fence and recovery state machine. An automated
-- downgrade could strand a promoted head or reinterpret an in-flight phase;
-- rollback is therefore the durable operation above, not a schema downgrade.
-- +goose StatementBegin
DO $$ BEGIN
    RAISE EXCEPTION '102: irreversible V3 prepare/cutover recovery migration';
END $$;
-- +goose StatementEnd
