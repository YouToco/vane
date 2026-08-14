-- 104: bind a V3 execution Plan to the planner-owned response projection.
--
-- Migration 092 compared the complete durable execution-plan envelope with
-- the planner completion.  That envelope also contains definition, catalog,
-- and Tool-policy digests injected by trusted runtime code, while the model is
-- deliberately allowed to return only schema_version + steps.  Consequently
-- every valid planner completion was rejected after its paid receipt settled.
--
-- Keep the immutable same-run receipt fence, but compare only the exact JSON
-- projection owned by the model.  The existing 086 Plan admission fence still
-- binds the runtime-owned digests to the frozen snapshot.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_plans,research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE FUNCTION research_plan_matches_planner_completion_v1(
    plan_payload BYTEA,
    planner_completion TEXT
) RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
    WITH input AS (
        SELECT convert_from(plan_payload,'UTF8') AS plan_text
    )
    SELECT CASE
        -- jsonb silently keeps the last duplicate key.  Preserve the raw-byte
        -- strictness of the Go decoder before any cast can erase ambiguity;
        -- WITH UNIQUE KEYS recursively covers step and arguments objects.
        WHEN NOT (plan_text IS JSON OBJECT WITH UNIQUE KEYS)
          OR NOT (planner_completion IS JSON OBJECT WITH UNIQUE KEYS)
        THEN false
        ELSE
            jsonb_typeof(plan_text::jsonb)='object'
            AND jsonb_typeof(planner_completion::jsonb)='object'
            AND plan_text::jsonb = jsonb_build_object(
                'schema_version',plan_text::jsonb->'schema_version',
                'definition_digest',plan_text::jsonb->'definition_digest',
                'capability_catalog_digest',
                    plan_text::jsonb->'capability_catalog_digest',
                'tool_policy_digest',plan_text::jsonb->'tool_policy_digest',
                'steps',plan_text::jsonb->'steps'
            )
            AND plan_text::jsonb->>'schema_version'
                    ='vane.research-execution-plan/v3'
            AND planner_completion::jsonb = jsonb_build_object(
                'schema_version',planner_completion::jsonb->'schema_version',
                'steps',planner_completion::jsonb->'steps'
            )
            AND planner_completion::jsonb->>'schema_version'
                    ='vane.research-planner-output/v3'
            AND jsonb_typeof(planner_completion::jsonb->'steps')='array'
            AND plan_text::jsonb->'steps'=planner_completion::jsonb->'steps'
        END
      FROM input
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION research_plan_matches_planner_completion_v1(BYTEA,TEXT)
    FROM PUBLIC;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_plan_llm_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP<>'INSERT' OR NEW.planner_llm_spend_reservation_id IS NULL THEN
        RAISE EXCEPTION '092: new research Plan requires an immutable planner receipt'
            USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM public.research_run_llm_spend_reservations reservation
          JOIN public.research_run_llm_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
           AND settlement.tenant_id=reservation.tenant_id
           AND settlement.user_id=reservation.user_id
           AND settlement.task_id=reservation.task_id
           AND settlement.run_snapshot_id=reservation.run_snapshot_id
           AND settlement.stage=reservation.stage
           AND settlement.round_ordinal=reservation.round_ordinal
          JOIN public.llm_calls call ON call.id=settlement.llm_call_id
         WHERE reservation.id=NEW.planner_llm_spend_reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.stage='planner'
           AND reservation.subject_id=0
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_planner' AND call.error=''
           AND public.research_plan_matches_planner_completion_v1(
                   NEW.plan_payload,call.completion)
    ) THEN
        RAISE EXCEPTION '104: research Plan differs from its planner response projection'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_plans,research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

-- Restore migration 092 byte-for-byte semantics for a deliberate downgrade.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_research_run_plan_llm_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP<>'INSERT' OR NEW.planner_llm_spend_reservation_id IS NULL THEN
        RAISE EXCEPTION '092: new research Plan requires an immutable planner receipt'
            USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM public.research_run_llm_spend_reservations reservation
          JOIN public.research_run_llm_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
           AND settlement.tenant_id=reservation.tenant_id
           AND settlement.user_id=reservation.user_id
           AND settlement.task_id=reservation.task_id
           AND settlement.run_snapshot_id=reservation.run_snapshot_id
           AND settlement.stage=reservation.stage
           AND settlement.round_ordinal=reservation.round_ordinal
          JOIN public.llm_calls call ON call.id=settlement.llm_call_id
         WHERE reservation.id=NEW.planner_llm_spend_reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.stage='planner'
           AND reservation.subject_id=0
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_planner' AND call.error=''
           AND convert_from(NEW.plan_payload,'UTF8')::jsonb=call.completion::jsonb
    ) THEN
        RAISE EXCEPTION '092: research Plan differs from its completed planner receipt'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

DROP FUNCTION research_plan_matches_planner_completion_v1(BYTEA,TEXT);
