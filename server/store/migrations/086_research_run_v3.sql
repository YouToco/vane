-- 086: immutable per-run research plans and Tool step receipts for V3.
--
-- The task definition owns only the manual and durable policies. Every run
-- seals its current capability-bounded plan here, and every external Tool I/O
-- first seals a started step. Runtime roles receive no UPDATE or DELETE.

-- +goose Up

CREATE TABLE research_run_plans (
    id                          BIGSERIAL   PRIMARY KEY,
    tenant_id                   BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id                     BIGINT      NOT NULL REFERENCES users (id),
    task_id                     TEXT        NOT NULL,
    run_snapshot_id             BIGINT      NOT NULL REFERENCES task_run_snapshots (id),
    temporal_workflow_id        TEXT        NOT NULL,
    temporal_run_id             TEXT        NOT NULL,
    definition_digest           TEXT        NOT NULL,
    capability_catalog_digest   TEXT        NOT NULL,
    plan_digest                 TEXT        NOT NULL,
    plan_payload                BYTEA       NOT NULL,
    schema_version              TEXT        NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_run_plans_snapshot
        UNIQUE (tenant_id,user_id,task_id,run_snapshot_id),
    CONSTRAINT uq_research_run_plans_temporal_run UNIQUE (temporal_run_id),
    CONSTRAINT ck_research_run_plans_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_workflow_id)=temporal_workflow_id AND
        octet_length(temporal_workflow_id) BETWEEN 1 AND 512 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT ck_research_run_plans_digests CHECK (
        definition_digest ~ '^[0-9a-f]{64}$' AND
        capability_catalog_digest ~ '^[0-9a-f]{64}$' AND
        plan_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_research_run_plans_payload CHECK (
        octet_length(plan_payload) BETWEEN 2 AND 262144 AND
        position(decode('00','hex') in plan_payload)=0 AND
        convert_from(plan_payload,'UTF8') IS NOT NULL
    ),
    CONSTRAINT ck_research_run_plans_schema CHECK (
        schema_version='vane.research-run-plan/v3'
    )
);

CREATE INDEX idx_research_run_plans_scope_created
    ON research_run_plans (tenant_id,user_id,task_id,created_at DESC,id DESC);

CREATE TABLE research_run_steps (
    id                BIGSERIAL   PRIMARY KEY,
    tenant_id         BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id           BIGINT      NOT NULL REFERENCES users (id),
    task_id           TEXT        NOT NULL,
    plan_id           BIGINT      NOT NULL REFERENCES research_run_plans (id),
    temporal_run_id   TEXT        NOT NULL,
    plan_digest       TEXT        NOT NULL,
    step_ordinal      INTEGER     NOT NULL,
    phase             TEXT        NOT NULL,
    invocation_id     TEXT        NOT NULL,
    tool_name         TEXT        NOT NULL,
    request_digest    TEXT        NOT NULL,
    result_digest     TEXT,
    cost_micro_usd    BIGINT      NOT NULL DEFAULT 0,
    error_code        TEXT,
    schema_version    TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_run_steps_phase
        UNIQUE (tenant_id,user_id,temporal_run_id,plan_digest,step_ordinal,phase),
    CONSTRAINT ck_research_run_steps_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512 AND
        btrim(invocation_id)=invocation_id AND
        octet_length(invocation_id) BETWEEN 1 AND 255 AND
        btrim(tool_name)=tool_name AND octet_length(tool_name) BETWEEN 1 AND 255 AND
        step_ordinal BETWEEN 0 AND 15
    ),
    CONSTRAINT ck_research_run_steps_digests CHECK (
        plan_digest ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$' AND
        (result_digest IS NULL OR result_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_research_run_steps_phase CHECK (
        phase IN ('started','completed','failed','indeterminate')
    ),
    CONSTRAINT ck_research_run_steps_terminal_shape CHECK (
        (phase='started' AND result_digest IS NULL AND cost_micro_usd=0 AND error_code IS NULL) OR
        (phase='completed' AND result_digest IS NOT NULL AND error_code IS NULL) OR
        (phase IN ('failed','indeterminate') AND error_code IS NOT NULL AND
         btrim(error_code)=error_code AND octet_length(error_code) BETWEEN 1 AND 128)
    ),
    CONSTRAINT ck_research_run_steps_cost CHECK (cost_micro_usd>=0),
    CONSTRAINT ck_research_run_steps_schema CHECK (
        schema_version='vane.research-run-step/v3'
    )
);

CREATE UNIQUE INDEX uq_research_run_steps_terminal
    ON research_run_steps
       (tenant_id,user_id,temporal_run_id,plan_digest,step_ordinal)
    WHERE phase IN ('completed','failed','indeterminate');
CREATE INDEX idx_research_run_steps_plan
    ON research_run_steps (tenant_id,user_id,plan_id,step_ordinal,created_at,id);

-- Keep the byte-for-byte V1/V2 admission function from migrations 037/075/079
-- as the authority for legacy rows. Route only the new reference schema to a
-- separate fence so Temporal replay behavior cannot drift while V3 is dark.
DROP TRIGGER task_run_snapshot_v2_admission_fence ON task_run_snapshots;
CREATE TRIGGER task_run_snapshot_v2_admission_fence
BEFORE INSERT ON task_run_snapshots
FOR EACH ROW
WHEN (NEW.reference_schema_version <>
      'vane.research-run-snapshot-ref/v3')
EXECUTE FUNCTION task_run_snapshot_v2_admission_fence();

-- A V3 snapshot may bind only the current active DiscoverAtRun head and its
-- immutable V3 definition. The paused exception is the same owner-scoped,
-- one-shot manual-run authority already used by the V2 fence.
-- +goose StatementBegin
CREATE FUNCTION task_run_snapshot_v3_admission_fence()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    schedule_status TEXT;
    schedule_execution_mode TEXT;
    schedule_definition_version BIGINT;
    schedule_definition_digest TEXT;
    approved_schema_version TEXT;
    approved_execution_mode TEXT;
BEGIN
    SELECT schedule.status,schedule.execution_mode,
           schedule.approved_definition_version,
           schedule.approved_definition_digest,
           definition.schema_version,definition.execution_mode
      INTO schedule_status,schedule_execution_mode,
           schedule_definition_version,schedule_definition_digest,
           approved_schema_version,approved_execution_mode
      FROM public.schedules schedule
      JOIN public.task_approved_definition_versions definition
        ON definition.tenant_id=schedule.tenant_id
       AND definition.user_id=schedule.user_id
       AND definition.task_id=schedule.id
       AND definition.version=schedule.approved_definition_version
       AND definition.definition_digest=schedule.approved_definition_digest
     WHERE schedule.tenant_id=NEW.tenant_id
       AND schedule.user_id=NEW.user_id
       AND schedule.id=NEW.task_id
     FOR SHARE OF schedule,definition;

    IF schedule_status IS NULL OR
       (schedule_status<>'active' AND NOT (
           schedule_status='paused' AND
           public.authorize_manual_task_run_v1(
               NEW.tenant_id,NEW.user_id,NEW.task_id,
               NEW.temporal_workflow_id
           )
       )) OR
       NEW.execution_mode<>'discover_at_run' OR
       schedule_execution_mode<>'discover_at_run' OR
       NEW.adaptive_version<>0 OR
       NEW.plan_digest<>'' OR
       NEW.v2_cutover_event_id IS NOT NULL OR
       schedule_definition_version IS NULL OR
       NEW.definition_digest IS DISTINCT FROM schedule_definition_digest OR
       approved_schema_version IS DISTINCT FROM
           'vane.task-approved-definition/v3' OR
       approved_execution_mode IS DISTINCT FROM 'discover_at_run' THEN
        RAISE EXCEPTION
            '086: research snapshot admission fence rejected task state'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION task_run_snapshot_v3_admission_fence() FROM PUBLIC;
CREATE TRIGGER task_run_snapshot_v3_admission_fence
BEFORE INSERT ON task_run_snapshots
FOR EACH ROW
WHEN (NEW.reference_schema_version =
      'vane.research-run-snapshot-ref/v3')
EXECUTE FUNCTION task_run_snapshot_v3_admission_fence();

-- A plan can only bind the exact same-scope DiscoverAtRun snapshot. The
-- payload digest is checked separately because pgcrypto digest is immutable
-- while JSON interpretation remains in the typed Store reader.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_plan_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.task_run_snapshots snapshot
         WHERE snapshot.id=NEW.run_snapshot_id
           AND snapshot.tenant_id=NEW.tenant_id
           AND snapshot.user_id=NEW.user_id
           AND snapshot.task_id=NEW.task_id
           AND snapshot.temporal_workflow_id=NEW.temporal_workflow_id
           AND snapshot.temporal_run_id=NEW.temporal_run_id
           AND snapshot.execution_mode='discover_at_run'
           AND snapshot.reference_schema_version=
               'vane.research-run-snapshot-ref/v3'
           AND snapshot.definition_digest=NEW.definition_digest
           AND snapshot.capability_catalog_digest=NEW.capability_catalog_digest
    ) THEN
        RAISE EXCEPTION '086: research plan snapshot scope mismatch';
    END IF;
    IF NEW.plan_digest IS DISTINCT FROM encode(sha256(NEW.plan_payload),'hex') THEN
        RAISE EXCEPTION '086: research plan digest mismatch';
    END IF;
    PERFORM convert_from(NEW.plan_payload,'UTF8')::jsonb;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_plan_v3() FROM PUBLIC;
CREATE TRIGGER research_run_plan_v3
BEFORE INSERT ON research_run_plans
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_plan_v3();

-- Every terminal receipt must have an exact immutable started predecessor.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_step_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.research_run_plans plan
         WHERE plan.id=NEW.plan_id
           AND plan.tenant_id=NEW.tenant_id
           AND plan.user_id=NEW.user_id
           AND plan.task_id=NEW.task_id
           AND plan.temporal_run_id=NEW.temporal_run_id
           AND plan.plan_digest=NEW.plan_digest
    ) THEN
        RAISE EXCEPTION '086: research step plan scope mismatch';
    END IF;
    IF NEW.phase<>'started' AND NOT EXISTS (
        SELECT 1 FROM public.research_run_steps started
         WHERE started.tenant_id=NEW.tenant_id
           AND started.user_id=NEW.user_id
           AND started.temporal_run_id=NEW.temporal_run_id
           AND started.plan_digest=NEW.plan_digest
           AND started.step_ordinal=NEW.step_ordinal
           AND started.phase='started'
           AND started.plan_id=NEW.plan_id
           AND started.invocation_id=NEW.invocation_id
           AND started.tool_name=NEW.tool_name
           AND started.request_digest=NEW.request_digest
    ) THEN
        RAISE EXCEPTION '086: research terminal step has no exact start';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_step_v3() FROM PUBLIC;
CREATE TRIGGER research_run_step_v3
BEFORE INSERT ON research_run_steps
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_step_v3();

GRANT SELECT ON research_run_plans,research_run_steps TO vane_app;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,temporal_workflow_id,
    temporal_run_id,definition_digest,capability_catalog_digest,plan_digest,
    plan_payload,schema_version
) ON research_run_plans TO vane_app;
GRANT INSERT (
    tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
    step_ordinal,phase,invocation_id,tool_name,request_digest,result_digest,
    cost_micro_usd,error_code,schema_version
) ON research_run_steps TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE
    research_run_plans_id_seq,research_run_steps_id_seq TO vane_app;

ALTER TABLE research_run_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_run_steps ENABLE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['research_run_plans','research_run_steps'] LOOP
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',
            table_name);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint)',
            table_name);
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',
            table_name);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down

LOCK TABLE task_run_snapshots,research_run_steps,research_run_plans
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
           SELECT 1 FROM task_run_snapshots
            WHERE reference_schema_version=
                  'vane.research-run-snapshot-ref/v3'
       ) OR
       EXISTS (SELECT 1 FROM research_run_steps) OR
       EXISTS (SELECT 1 FROM research_run_plans) THEN
        RAISE EXCEPTION '086: refusing downgrade while research run evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON SEQUENCE research_run_plans_id_seq,research_run_steps_id_seq
    FROM vane_app;
REVOKE ALL ON research_run_steps,research_run_plans FROM vane_app;
DROP TRIGGER research_run_step_v3 ON research_run_steps;
DROP FUNCTION enforce_research_run_step_v3();
DROP TRIGGER research_run_plan_v3 ON research_run_plans;
DROP FUNCTION enforce_research_run_plan_v3();
DROP TABLE research_run_steps;
DROP TABLE research_run_plans;
DROP TRIGGER task_run_snapshot_v3_admission_fence ON task_run_snapshots;
DROP FUNCTION task_run_snapshot_v3_admission_fence();
DROP TRIGGER task_run_snapshot_v2_admission_fence ON task_run_snapshots;
CREATE TRIGGER task_run_snapshot_v2_admission_fence
BEFORE INSERT ON task_run_snapshots
FOR EACH ROW
EXECUTE FUNCTION task_run_snapshot_v2_admission_fence();
