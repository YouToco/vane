-- 102: delivery-dark V3 definition preparation and atomic DB-head promotion.
-- Preparing a definition never mutates the production Schedule projection.

-- +goose Up

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
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_research_v3_definition_prepare_identity CHECK (
        tenant_id>0 AND user_id>0 AND
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(idempotency_key)=idempotency_key AND
        octet_length(idempotency_key) BETWEEN 1 AND 512 AND
        target_definition_version>0 AND
        target_definition_digest ~ '^[0-9a-f]{64}$' AND
        original_execution_mode IN ('compiled','discover_at_run') AND
        ((previous_definition_version IS NULL AND previous_definition_digest IS NULL) OR
         (previous_definition_version>0 AND previous_definition_digest ~ '^[0-9a-f]{64}$')) AND
        ((original_execution_mode='compiled' AND original_definition_version IS NULL AND
          original_definition_digest IS NULL) OR
         (original_execution_mode='discover_at_run' AND original_definition_version>0 AND
          original_definition_digest ~ '^[0-9a-f]{64}$'))),
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
        REFERENCES research_v3_definition_prepare_operations(id) ON DELETE RESTRICT,
    CONSTRAINT ck_research_v3_prepared_head CHECK (
        definition_version>0 AND definition_digest ~ '^[0-9a-f]{64}$' AND
        execution_mode='discover_at_run')
);

ALTER TABLE research_v3_cutover_operations
    ADD COLUMN original_execution_mode TEXT,
    ADD COLUMN original_definition_version BIGINT,
    ADD COLUMN original_definition_digest TEXT;

UPDATE research_v3_cutover_operations operation
   SET original_execution_mode=schedule.execution_mode,
       original_definition_version=schedule.approved_definition_version,
       original_definition_digest=schedule.approved_definition_digest
  FROM schedules schedule
 WHERE schedule.tenant_id=operation.tenant_id
   AND schedule.user_id=operation.user_id AND schedule.id=operation.task_id;

-- A historical journal can outlive a normally deleted Schedule. Migration
-- 101 admitted only a current V3 head, so its exact target is the recoverable
-- legacy fallback when the mirror is already gone.
UPDATE research_v3_cutover_operations
   SET original_execution_mode='discover_at_run',
       original_definition_version=definition_version,
       original_definition_digest=definition_digest
 WHERE original_execution_mode IS NULL;

ALTER TABLE research_v3_cutover_operations
    ALTER COLUMN original_execution_mode SET NOT NULL,
    ADD CONSTRAINT ck_research_v3_cutover_original_head CHECK (
        (original_execution_mode='compiled' AND original_definition_version IS NULL AND
         original_definition_digest IS NULL) OR
        (original_execution_mode='discover_at_run' AND original_definition_version>0 AND
         original_definition_digest ~ '^[0-9a-f]{64}$'));

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
GRANT INSERT (original_execution_mode,original_definition_version,original_definition_digest)
    ON research_v3_cutover_operations TO vane_research_v3_cutover_operator;
GRANT SELECT (tenant_id,user_id,task_id,definition_version,definition_digest,
              execution_mode,prepare_operation_id,updated_at)
    ON research_v3_prepared_definition_heads TO vane_app;

ALTER TABLE research_v3_definition_prepare_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_v3_prepared_definition_heads ENABLE ROW LEVEL SECURITY;

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
    IF ROW(NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.idempotency_key,
           NEW.target_definition_version,NEW.target_definition_digest,
           NEW.previous_definition_version,NEW.previous_definition_digest,
           NEW.original_execution_mode,NEW.original_definition_version,
           NEW.original_definition_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.idempotency_key,
           OLD.target_definition_version,OLD.target_definition_digest,
           OLD.previous_definition_version,OLD.previous_definition_digest,
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
         FOR SHARE OF schedule,head,definition;
    ELSE
        SELECT schedule.status,schedule.execution_mode,schedule.approved_definition_digest,
               definition.schema_version,definition.execution_mode
          INTO schedule_status,schedule_execution_mode,selected_definition_digest,
               approved_schema_version,approved_execution_mode
          FROM public.schedules schedule
          JOIN public.task_approved_definition_versions definition
            ON definition.tenant_id=schedule.tenant_id
           AND definition.user_id=schedule.user_id AND definition.task_id=schedule.id
           AND definition.version=schedule.approved_definition_version
           AND definition.definition_digest=schedule.approved_definition_digest
         WHERE schedule.tenant_id=NEW.tenant_id AND schedule.user_id=NEW.user_id
           AND schedule.id=NEW.task_id
         FOR SHARE OF schedule,definition;
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
           NEW.original_definition_version,NEW.original_definition_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.idempotency_key,
           OLD.generation,OLD.definition_version,OLD.definition_digest,
           OLD.frozen_schedule,OLD.frozen_schedule_digest,
           OLD.frozen_conflict_token,OLD.conflict_token_digest,
           CASE WHEN OLD.rollback_conflict_token IS NULL THEN NULL ELSE OLD.rollback_conflict_token END,
           CASE WHEN OLD.rollback_token_digest IS NULL THEN NULL ELSE OLD.rollback_token_digest END,
           OLD.target_action,OLD.target_action_digest,OLD.action_authorization_digest,
           OLD.original_paused,OLD.original_execution_mode,
           OLD.original_definition_version,OLD.original_definition_digest,OLD.created_at) THEN
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
