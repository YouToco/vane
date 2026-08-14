-- 089: bind every V3 execution plan to the exact scheduled Tool policy.
--
-- A capability route digest alone does not attest the model-visible name,
-- argument schema or retained implementation generation. V3 is still dark,
-- so fail closed instead of accepting an older unbound plan.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_plans,task_run_snapshots IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM research_run_plans plan
          JOIN task_run_snapshots snapshot ON snapshot.id=plan.run_snapshot_id
         WHERE snapshot.reference_schema_version=
                   'vane.research-run-snapshot-ref/v3'
           AND (
               convert_from(plan.plan_payload,'UTF8')::jsonb->>
                   'tool_policy_digest' IS DISTINCT FROM snapshot.tool_policy_digest OR
               convert_from(plan.plan_payload,'UTF8')::jsonb->>
                   'capability_catalog_digest' IS DISTINCT FROM
                   snapshot.capability_catalog_digest OR
               convert_from(plan.plan_payload,'UTF8')::jsonb->>
                   'definition_digest' IS DISTINCT FROM snapshot.definition_digest
           )
    ) THEN
        RAISE EXCEPTION
            '089: existing research plan is not bound to its exact Tool policy';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_plan_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    plan_json JSONB;
BEGIN
    IF NEW.plan_digest IS DISTINCT FROM
       encode(sha256(NEW.plan_payload),'hex') THEN
        RAISE EXCEPTION '086: research plan digest mismatch';
    END IF;
    plan_json := convert_from(NEW.plan_payload,'UTF8')::jsonb;
    IF plan_json->>'schema_version' IS DISTINCT FROM
           'vane.research-execution-plan/v3' OR
       plan_json->>'definition_digest' IS DISTINCT FROM
           NEW.definition_digest OR
       plan_json->>'capability_catalog_digest' IS DISTINCT FROM
           NEW.capability_catalog_digest OR
       plan_json->>'tool_policy_digest' !~ '^[0-9a-f]{64}$' OR
       NOT EXISTS (
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
              AND snapshot.capability_catalog_digest=
                  NEW.capability_catalog_digest
              AND snapshot.tool_policy_digest=
                  plan_json->>'tool_policy_digest'
       ) THEN
        RAISE EXCEPTION
            '089: research plan snapshot or Tool policy scope mismatch';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_plan_v3() FROM PUBLIC;
-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_plans,task_run_snapshots IN ACCESS EXCLUSIVE MODE;

-- Older V3 decoders do not know tool_policy_digest in the plan payload.
-- Refuse a downgrade that would make retained rows unreadable.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_plans) THEN
        RAISE EXCEPTION
            '089: refusing downgrade while Tool-policy-bound plans exist';
    END IF;
END $$;
-- +goose StatementEnd

-- Restore the byte-for-byte 086 plan fence.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_plan_v3()
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
