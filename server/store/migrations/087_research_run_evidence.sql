-- 087: exact model-visible Tool evidence for V3 research runs.
--
-- A completed step and the bytes actually shown to the synthesizer are one
-- atomic fact. Runtime roles may append and read their owner-scoped evidence,
-- but can never update, delete or truncate it.

-- +goose Up

-- V3 is dark at this migration. Refuse to invent evidence for an unexpected
-- pre-migration completion instead of silently accepting an unverifiable row.
-- Serialize the check with every 086 step writer so none can enter between the
-- check and the replacement terminal fence.
SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_steps IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_steps WHERE phase='completed') THEN
        RAISE EXCEPTION
            '087: completed research steps exist without exact evidence';
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE research_run_evidence (
    id                BIGSERIAL   PRIMARY KEY,
    tenant_id         BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    user_id           BIGINT      NOT NULL REFERENCES users (id),
    task_id           TEXT        NOT NULL,
    plan_id           BIGINT      NOT NULL REFERENCES research_run_plans (id),
    started_step_id   BIGINT      NOT NULL REFERENCES research_run_steps (id),
    temporal_run_id   TEXT        NOT NULL,
    plan_digest       TEXT        NOT NULL,
    step_ordinal      INTEGER     NOT NULL,
    invocation_id     TEXT        NOT NULL,
    tool_name         TEXT        NOT NULL,
    request_digest    TEXT        NOT NULL,
    result_bytes      BYTEA       NOT NULL,
    result_digest     TEXT        NOT NULL,
    original_size     INTEGER     NOT NULL,
    truncated         BOOLEAN     NOT NULL,
    trust_type        TEXT        NOT NULL,
    schema_version    TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_run_evidence_step
        UNIQUE (tenant_id,user_id,temporal_run_id,plan_digest,step_ordinal),
    CONSTRAINT uq_research_run_evidence_started UNIQUE (started_step_id),
    CONSTRAINT ck_research_run_evidence_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512 AND
        btrim(invocation_id)=invocation_id AND
        octet_length(invocation_id) BETWEEN 1 AND 255 AND
        btrim(tool_name)=tool_name AND octet_length(tool_name) BETWEEN 1 AND 255 AND
        step_ordinal BETWEEN 0 AND 15
    ),
    CONSTRAINT ck_research_run_evidence_digests CHECK (
        plan_digest ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$' AND
        result_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_research_run_evidence_result CHECK (
        octet_length(result_bytes) <= 262144 AND
        position(decode('00','hex') in result_bytes)=0 AND
        convert_from(result_bytes,'UTF8') IS NOT NULL AND
        original_size BETWEEN octet_length(result_bytes) AND 2147483647 AND
        truncated=(original_size>octet_length(result_bytes))
    ),
    CONSTRAINT ck_research_run_evidence_trust CHECK (
        trust_type IN ('local','external')
    ),
    CONSTRAINT ck_research_run_evidence_schema CHECK (
        schema_version='vane.research-run-evidence/v3'
    )
);

CREATE INDEX idx_research_run_evidence_scope_created
    ON research_run_evidence
       (tenant_id,user_id,task_id,created_at DESC,id DESC);
CREATE INDEX idx_research_run_evidence_plan
    ON research_run_evidence
       (tenant_id,user_id,plan_id,step_ordinal,id);

-- The evidence must bind byte-for-byte to the exact immutable started step.
-- It cannot be appended after any terminal decision for that ordinal.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_evidence_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.research_run_steps started
         WHERE started.id=NEW.started_step_id
           AND started.tenant_id=NEW.tenant_id
           AND started.user_id=NEW.user_id
           AND started.task_id=NEW.task_id
           AND started.plan_id=NEW.plan_id
           AND started.temporal_run_id=NEW.temporal_run_id
           AND started.plan_digest=NEW.plan_digest
           AND started.step_ordinal=NEW.step_ordinal
           AND started.phase='started'
           AND started.invocation_id=NEW.invocation_id
           AND started.tool_name=NEW.tool_name
           AND started.request_digest=NEW.request_digest
    ) OR EXISTS (
        SELECT 1 FROM public.research_run_steps terminal
         WHERE terminal.tenant_id=NEW.tenant_id
           AND terminal.user_id=NEW.user_id
           AND terminal.temporal_run_id=NEW.temporal_run_id
           AND terminal.plan_digest=NEW.plan_digest
           AND terminal.step_ordinal=NEW.step_ordinal
           AND terminal.phase IN ('completed','failed','indeterminate')
    ) THEN
        RAISE EXCEPTION '087: research evidence has no exact open start';
    END IF;
    IF NEW.result_digest IS DISTINCT FROM
       encode(sha256(NEW.result_bytes),'hex') THEN
        RAISE EXCEPTION '087: research evidence digest mismatch';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_evidence_v3() FROM PUBLIC;
CREATE TRIGGER research_run_evidence_v3
BEFORE INSERT ON research_run_evidence
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_evidence_v3();

-- Evidence is inserted before the completed step so its BEFORE trigger can
-- prove the evidence exists. At COMMIT, this deferred inverse fence proves
-- that no caller stranded evidence without the matching terminal fact.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_evidence_terminal_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.research_run_steps terminal
         WHERE terminal.tenant_id=NEW.tenant_id
           AND terminal.user_id=NEW.user_id
           AND terminal.task_id=NEW.task_id
           AND terminal.plan_id=NEW.plan_id
           AND terminal.temporal_run_id=NEW.temporal_run_id
           AND terminal.plan_digest=NEW.plan_digest
           AND terminal.step_ordinal=NEW.step_ordinal
           AND terminal.phase='completed'
           AND terminal.invocation_id=NEW.invocation_id
           AND terminal.tool_name=NEW.tool_name
           AND terminal.request_digest=NEW.request_digest
           AND terminal.result_digest=NEW.result_digest
    ) THEN
        RAISE EXCEPTION '087: research evidence has no exact completed step';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_evidence_terminal_v3() FROM PUBLIC;
CREATE CONSTRAINT TRIGGER research_run_evidence_terminal_v3
AFTER INSERT ON research_run_evidence
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_evidence_terminal_v3();

-- Extend the 086 terminal fence: success requires exact evidence in the same
-- transaction; failed/indeterminate decisions must not strand evidence.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_step_v3()
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
    IF NEW.phase='completed' AND NOT EXISTS (
        SELECT 1 FROM public.research_run_evidence evidence
         WHERE evidence.tenant_id=NEW.tenant_id
           AND evidence.user_id=NEW.user_id
           AND evidence.task_id=NEW.task_id
           AND evidence.plan_id=NEW.plan_id
           AND evidence.temporal_run_id=NEW.temporal_run_id
           AND evidence.plan_digest=NEW.plan_digest
           AND evidence.step_ordinal=NEW.step_ordinal
           AND evidence.invocation_id=NEW.invocation_id
           AND evidence.tool_name=NEW.tool_name
           AND evidence.request_digest=NEW.request_digest
           AND evidence.result_digest=NEW.result_digest
    ) THEN
        RAISE EXCEPTION '087: completed research step has no exact evidence';
    END IF;
    IF NEW.phase IN ('failed','indeterminate') AND EXISTS (
        SELECT 1 FROM public.research_run_evidence evidence
         WHERE evidence.tenant_id=NEW.tenant_id
           AND evidence.user_id=NEW.user_id
           AND evidence.temporal_run_id=NEW.temporal_run_id
           AND evidence.plan_digest=NEW.plan_digest
           AND evidence.step_ordinal=NEW.step_ordinal
    ) THEN
        RAISE EXCEPTION '087: failed research step cannot retain evidence';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

GRANT SELECT ON research_run_evidence TO vane_app;
GRANT INSERT (
    tenant_id,user_id,task_id,plan_id,started_step_id,temporal_run_id,
    plan_digest,step_ordinal,invocation_id,tool_name,request_digest,
    result_bytes,result_digest,original_size,truncated,trust_type,schema_version
) ON research_run_evidence TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE research_run_evidence_id_seq TO vane_app;
GRANT SELECT (
    id,tenant_id,user_id,task_id,plan_id,started_step_id,temporal_run_id,
    plan_digest,step_ordinal,invocation_id,tool_name,request_digest,
    result_bytes,result_digest,original_size,truncated,trust_type,created_at
) ON research_run_evidence TO vane_intelligence_reader;

ALTER TABLE research_run_evidence ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON research_run_evidence
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON research_run_evidence AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint);
CREATE POLICY user_isolation ON research_run_evidence AS RESTRICTIVE
    FOR ALL
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_evidence,research_run_steps
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_evidence) OR
       EXISTS (SELECT 1 FROM research_run_steps WHERE phase='completed') THEN
        RAISE EXCEPTION
            '087: refusing downgrade while exact research evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON SEQUENCE research_run_evidence_id_seq FROM vane_app;
REVOKE ALL ON research_run_evidence FROM vane_app;
REVOKE ALL ON research_run_evidence FROM vane_intelligence_reader;
DROP TRIGGER research_run_evidence_terminal_v3 ON research_run_evidence;
DROP FUNCTION enforce_research_run_evidence_terminal_v3();
DROP TRIGGER research_run_evidence_v3 ON research_run_evidence;
DROP FUNCTION enforce_research_run_evidence_v3();
DROP TABLE research_run_evidence;

-- Restore the byte-for-byte 086 terminal fence.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_step_v3()
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
