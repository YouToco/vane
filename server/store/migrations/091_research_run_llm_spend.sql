-- 091: response-loss-safe LLM spend authority for V3 planning and synthesis.
--
-- Reservations are committed before a provider request. Settlements and the
-- exact llm_calls projection are append-only and one-to-one. The planner
-- reservation fence also accounts for Tool reservations, so both classes of
-- paid effect consume the single PlannerBudget frozen in the run snapshot.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_brief_syntheses,llm_calls,
           research_run_step_spend_reservations IN ACCESS EXCLUSIVE MODE;

-- Roles are cluster-global and may retain grants from another database or an
-- interrupted rollout. V3 may inspect the exact price projection, but can
-- never mutate or truncate the platform-owned catalog.
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON provider_price_rules
    FROM vane_research_v3_executor;

CREATE TABLE research_run_llm_spend_reservations (
    id                           BIGSERIAL   PRIMARY KEY,
    tenant_id                    BIGINT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id                      BIGINT      NOT NULL REFERENCES users(id),
    task_id                      TEXT        NOT NULL,
    run_snapshot_id              BIGINT      NOT NULL REFERENCES task_run_snapshots(id),
    stage                        TEXT        NOT NULL,
    round_ordinal                INTEGER     NOT NULL,
    subject_id                   BIGINT      NOT NULL,
    attempt_key                  TEXT        NOT NULL,
    request_digest               TEXT        NOT NULL,
    trace_id                     TEXT        NOT NULL,
    model                        TEXT        NOT NULL,
    system_prompt_digest         TEXT        NOT NULL,
    user_prompt_digest           TEXT        NOT NULL,
    temperature                  REAL        NOT NULL,
    disable_thinking             BOOLEAN     NOT NULL,
    model_policy_digest          TEXT        NOT NULL,
    quota_bucket                 TEXT        NOT NULL,
    reserved_quota_tokens        BIGINT      NOT NULL,
    reserved_completion_tokens   BIGINT      NOT NULL,
    reserved_planner_tokens      BIGINT      NOT NULL,
    reserved_cost_micro_usd      BIGINT      NOT NULL,
    pricing_rule_id              BIGINT      NOT NULL REFERENCES provider_price_rules(id),
    cost_currency                TEXT        NOT NULL,
    counts_against_planner_budget BOOLEAN    NOT NULL,
    schema_version               TEXT        NOT NULL,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_llm_spend_reservation_round
        UNIQUE (run_snapshot_id,stage,round_ordinal),
    CONSTRAINT uq_research_llm_spend_reservation_attempt UNIQUE (attempt_key),
    CONSTRAINT uq_research_llm_spend_reservation_scope
        UNIQUE (id,tenant_id,user_id,run_snapshot_id),
    CONSTRAINT ck_research_llm_spend_reservation_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(trace_id)=trace_id AND octet_length(trace_id) BETWEEN 1 AND 512 AND
        btrim(model)=model AND octet_length(model) BETWEEN 1 AND 255 AND
        round_ordinal BETWEEN 0 AND 15
    ),
    CONSTRAINT ck_research_llm_spend_reservation_digests CHECK (
        attempt_key ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$' AND
        system_prompt_digest ~ '^[0-9a-f]{64}$' AND
        user_prompt_digest ~ '^[0-9a-f]{64}$' AND
        model_policy_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_research_llm_spend_reservation_temperature CHECK (
        temperature>=0 AND temperature<=2
    ),
    CONSTRAINT ck_research_llm_spend_reservation_stage CHECK (
        (stage='planner' AND subject_id=0 AND
         counts_against_planner_budget AND reserved_planner_tokens=reserved_quota_tokens) OR
        (stage='synthesis' AND subject_id>0 AND
         NOT counts_against_planner_budget AND reserved_planner_tokens=0 AND round_ordinal=0)
    ),
    CONSTRAINT ck_research_llm_spend_reservation_amounts CHECK (
        quota_bucket='llm_tokens' AND
        reserved_quota_tokens BETWEEN 1 AND 1000000 AND
        reserved_completion_tokens BETWEEN 1 AND 1000000 AND
        reserved_quota_tokens>=reserved_completion_tokens AND
        reserved_planner_tokens BETWEEN 0 AND 1000000 AND
        reserved_cost_micro_usd BETWEEN 1 AND 100000000 AND
        cost_currency='USD'
    ),
    CONSTRAINT ck_research_llm_spend_reservation_schema CHECK (
        schema_version='vane.research-run-llm-spend-reservation/v1'
    )
);

CREATE INDEX idx_research_llm_spend_reservation_scope
    ON research_run_llm_spend_reservations
       (tenant_id,user_id,task_id,run_snapshot_id,stage,round_ordinal,id);

-- This trigger is the common lockResearchRunSpendBudgetV3 authority. Every
-- planner LLM and Tool admission locks the same snapshot before summing frozen
-- reservations; concurrent callers therefore cannot over-admit the run.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_llm_spend_reservation_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    stage_model TEXT;
    stage_system_prompt TEXT;
    stage_max_tokens BIGINT;
    stage_temperature REAL;
    stage_disable_thinking BOOLEAN;
    max_rounds INTEGER;
    max_tokens BIGINT;
    max_cost BIGINT;
    used_tokens BIGINT;
    used_cost BIGINT;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.model_policy_digest=NEW.model_policy_digest
     FOR UPDATE;
    IF snapshot_json IS NULL THEN
        RAISE EXCEPTION '091: LLM reservation snapshot scope differs'
            USING ERRCODE='23514';
    END IF;

    stage_model := snapshot_json #>> ARRAY[
        'research_model',CASE WHEN NEW.stage='planner' THEN 'planner' ELSE 'synthesis' END,'model'
    ];
    stage_max_tokens := (snapshot_json #>> ARRAY[
        'research_model',CASE WHEN NEW.stage='planner' THEN 'planner' ELSE 'synthesis' END,'max_tokens'
    ])::bigint;
    stage_system_prompt := snapshot_json #>> ARRAY[
        'research_model',CASE WHEN NEW.stage='planner' THEN 'planner' ELSE 'synthesis' END,'system_prompt'
    ];
    stage_temperature := (snapshot_json #>> ARRAY[
        'research_model',CASE WHEN NEW.stage='planner' THEN 'planner' ELSE 'synthesis' END,'temperature'
    ])::real;
    stage_disable_thinking := (snapshot_json #>> ARRAY[
        'research_model',CASE WHEN NEW.stage='planner' THEN 'planner' ELSE 'synthesis' END,'disable_thinking'
    ])::boolean;
    IF stage_model IS DISTINCT FROM NEW.model OR
       stage_max_tokens IS NULL OR
       NEW.reserved_completion_tokens<>stage_max_tokens OR
       NEW.system_prompt_digest IS DISTINCT FROM
           encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex') OR
       NEW.temperature IS DISTINCT FROM stage_temperature OR
       NEW.disable_thinking IS DISTINCT FROM stage_disable_thinking OR
       snapshot_json #>> '{research_model,quota_bucket}'<>NEW.quota_bucket THEN
        RAISE EXCEPTION '091: LLM reservation differs from frozen model policy'
            USING ERRCODE='23514';
    END IF;
    -- The reservation is an upper bound, not a caller-supplied estimate. Use
    -- the cache-miss input rate and the full frozen output allowance; CEIL
    -- prevents a fractional micro-dollar from becoming an under-reservation.
    IF NOT EXISTS (
        SELECT 1 FROM public.provider_price_rules pricing
         WHERE pricing.id=NEW.pricing_rule_id
           AND pricing.provider=(snapshot_json #>> '{research_model,provider}')
           AND pricing.resource=NEW.model AND pricing.meter='llm_tokens'
           AND pricing.currency=NEW.cost_currency
           AND pricing.effective_from<=NEW.created_at
           AND (pricing.effective_to IS NULL OR pricing.effective_to>NEW.created_at)
           AND NEW.reserved_cost_micro_usd=ceil(
                 (NEW.reserved_quota_tokens-NEW.reserved_completion_tokens)::numeric *
                    pricing.input_cache_miss_per_million +
                 NEW.reserved_completion_tokens::numeric * pricing.output_per_million
               )::bigint
    ) THEN
        RAISE EXCEPTION '091: LLM reservation differs from exact price ceiling'
            USING ERRCODE='23514';
    END IF;

    IF NEW.stage='synthesis' THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_brief_syntheses brief
             WHERE brief.id=NEW.subject_id
               AND brief.tenant_id=NEW.tenant_id AND brief.user_id=NEW.user_id
               AND brief.task_id=NEW.task_id
               AND brief.run_snapshot_id=NEW.run_snapshot_id
               AND brief.status IN ('prepared','spending')
        ) THEN
            RAISE EXCEPTION '091: synthesis reservation subject differs'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;

    max_rounds := (snapshot_json #>> '{planner_budget,max_planner_rounds}')::integer;
    max_tokens := (snapshot_json #>> '{planner_budget,max_tokens}')::bigint;
    max_cost := (snapshot_json #>> '{planner_budget,max_cost_micro_usd}')::bigint;
    -- A settlement is evidence, not refund authority. The runtime executor
    -- supplies provider usage and can be compromised or simply wrong after a
    -- response-loss edge. Never let a low/zero/unattempted report mint planner
    -- budget: the reservation remains the floor, while verified overage still
    -- consumes the larger amount.
    SELECT COALESCE(sum(GREATEST(
               reservation.reserved_planner_tokens,
               COALESCE(settlement.actual_prompt_tokens+
                        settlement.actual_completion_tokens,
                        reservation.reserved_planner_tokens)
           )),0),
           COALESCE(sum(GREATEST(
               reservation.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        reservation.reserved_cost_micro_usd)
           )),0)
      INTO used_tokens,used_cost
      FROM public.research_run_llm_spend_reservations reservation
      LEFT JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.tenant_id=NEW.tenant_id
       AND reservation.user_id=NEW.user_id
       AND reservation.counts_against_planner_budget;
    SELECT used_cost+COALESCE(sum(GREATEST(
               tool.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        tool.reserved_cost_micro_usd)
           )),0)
      INTO used_cost
      FROM public.research_run_step_spend_reservations tool
      LEFT JOIN public.research_run_step_spend_settlements settlement
        ON settlement.reservation_id=tool.id
     WHERE tool.run_snapshot_id=NEW.run_snapshot_id
       AND tool.tenant_id=NEW.tenant_id AND tool.user_id=NEW.user_id;
    IF NEW.round_ordinal>=max_rounds OR
       used_tokens+NEW.reserved_planner_tokens>max_tokens OR
       used_cost+NEW.reserved_cost_micro_usd>max_cost THEN
        RAISE EXCEPTION '091: planner reservation exceeds frozen PlannerBudget'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_llm_spend_reservation_v1() FROM PUBLIC;
CREATE TRIGGER research_run_llm_spend_reservation_v1
BEFORE INSERT ON research_run_llm_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v1();

-- Tool reservations use the same root lock and frozen max cost/call count.
-- This second-direction fence is required: checking only LLM insertions would
-- allow a concurrent Tool admission to exceed the shared planner budget.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_shared_planner_budget_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE snapshot_json JSONB; used_cost BIGINT; used_tools BIGINT;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
     FOR UPDATE;
    IF snapshot_json IS NULL THEN
        RAISE EXCEPTION '091: Tool reservation snapshot scope differs' USING ERRCODE='23514';
    END IF;
    SELECT count(*),COALESCE(sum(GREATEST(
               reservation.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        reservation.reserved_cost_micro_usd)
           )),0)
      INTO used_tools,used_cost
      FROM public.research_run_step_spend_reservations reservation
      LEFT JOIN public.research_run_step_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.tenant_id=NEW.tenant_id AND reservation.user_id=NEW.user_id;
    SELECT used_cost+COALESCE(sum(GREATEST(
               reservation.reserved_cost_micro_usd,
               COALESCE(settlement.actual_cost_micro_usd,
                        reservation.reserved_cost_micro_usd)
           )),0) INTO used_cost
      FROM public.research_run_llm_spend_reservations reservation
      LEFT JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
     WHERE reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.tenant_id=NEW.tenant_id AND reservation.user_id=NEW.user_id
       AND reservation.counts_against_planner_budget;
    IF used_tools+1>(snapshot_json #>> '{planner_budget,max_tool_calls}')::bigint OR
       used_cost+NEW.reserved_cost_micro_usd>
           (snapshot_json #>> '{planner_budget,max_cost_micro_usd}')::bigint THEN
        RAISE EXCEPTION '091: Tool reservation exceeds frozen PlannerBudget'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_shared_planner_budget_v3() FROM PUBLIC;
CREATE TRIGGER research_run_shared_planner_budget_v3
BEFORE INSERT ON research_run_step_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_shared_planner_budget_v3();

-- The sole LLM admission capability atomically debits the burst-capped tenant
-- bucket and appends the exact immutable reservation. The executor never gets
-- INSERT on the reservation table, so it cannot split, omit or replay either
-- half of the financial effect.
-- +goose StatementBegin
CREATE FUNCTION admit_research_run_llm_spend_v3(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT,
    requested_stage TEXT,
    requested_round_ordinal INTEGER,
    requested_subject_id BIGINT,
    requested_attempt_key TEXT,
    requested_request_digest TEXT,
    requested_trace_id TEXT,
    requested_user_prompt TEXT
) RETURNS TABLE(out_reservation_id BIGINT,out_first_writer BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    snapshot_model_policy_digest TEXT;
    stage_key TEXT;
    stage_model TEXT;
    stage_system_prompt TEXT;
    stage_max_tokens BIGINT;
    stage_temperature REAL;
    stage_disable_thinking BOOLEAN;
    derived_quota_tokens BIGINT;
    derived_planner_tokens BIGINT;
    reserved_cost BIGINT;
    selected_pricing_rule_id BIGINT;
    admitted_id BIGINT;
BEGIN
    IF requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_task_id IS NULL OR requested_run_snapshot_id IS NULL OR
       requested_stage IS NULL OR requested_round_ordinal IS NULL OR
       requested_subject_id IS NULL OR requested_attempt_key IS NULL OR
       requested_request_digest IS NULL OR requested_trace_id IS NULL OR
       requested_user_prompt IS NULL OR
       requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint OR
       octet_length(requested_user_prompt) NOT BETWEEN 1 AND 2097152 OR
       NOT EXISTS (
           SELECT 1 FROM public.tenants tenant
           JOIN public.memberships membership ON membership.tenant_id=tenant.id
            AND membership.user_id=requested_user_id
           WHERE tenant.id=requested_tenant_id AND tenant.status='active'
             AND tenant.deleted_at IS NULL
       ) THEN
        RAISE EXCEPTION '091: LLM admission scope or quota is invalid'
            USING ERRCODE='42501';
    END IF;
    stage_key := CASE requested_stage
        WHEN 'planner' THEN 'planner' WHEN 'synthesis' THEN 'synthesis'
        ELSE NULL END;
    IF stage_key IS NULL THEN
        RAISE EXCEPTION '091: LLM admission stage is invalid' USING ERRCODE='23514';
    END IF;

    SELECT convert_from(snapshot.payload,'UTF8')::jsonb,
           snapshot.model_policy_digest
      INTO snapshot_json,snapshot_model_policy_digest
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=requested_run_snapshot_id
       AND snapshot.tenant_id=requested_tenant_id
       AND snapshot.user_id=requested_user_id
       AND snapshot.task_id=requested_task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
     FOR UPDATE;
    IF snapshot_json IS NULL THEN
        RAISE EXCEPTION '091: LLM admission snapshot scope differs'
            USING ERRCODE='23514';
    END IF;
    -- Synthesis input is already frozen as exact model-visible bytes by 088.
    -- Do not let an executor substitute an arbitrary same-owner prompt and
    -- then make that substitution authoritative merely by hashing it here.
    IF requested_stage='synthesis' AND NOT EXISTS (
        SELECT 1 FROM public.research_brief_syntheses brief
         WHERE brief.id=requested_subject_id
           AND brief.tenant_id=requested_tenant_id
           AND brief.user_id=requested_user_id
           AND brief.task_id=requested_task_id
           AND brief.run_snapshot_id=requested_run_snapshot_id
           AND brief.context_payload=convert_to(requested_user_prompt,'UTF8')
    ) THEN
        RAISE EXCEPTION '091: synthesis prompt differs from frozen context'
            USING ERRCODE='23514';
    END IF;
    stage_model := snapshot_json #>> ARRAY['research_model',stage_key,'model'];
    stage_system_prompt := snapshot_json #>> ARRAY['research_model',stage_key,'system_prompt'];
    stage_max_tokens := (snapshot_json #>> ARRAY['research_model',stage_key,'max_tokens'])::bigint;
    stage_temperature := (snapshot_json #>> ARRAY['research_model',stage_key,'temperature'])::real;
    stage_disable_thinking := (snapshot_json #>> ARRAY['research_model',stage_key,'disable_thinking'])::boolean;
    derived_quota_tokens := octet_length(stage_system_prompt)::bigint +
        octet_length(requested_user_prompt)::bigint + 64 + stage_max_tokens;
    derived_planner_tokens := CASE requested_stage
        WHEN 'planner' THEN derived_quota_tokens ELSE 0 END;
    IF derived_quota_tokens NOT BETWEEN 1 AND 1000000 THEN
        RAISE EXCEPTION '091: LLM admission derived quota is invalid'
            USING ERRCODE='23514';
    END IF;

    SELECT reservation.id INTO admitted_id
      FROM public.research_run_llm_spend_reservations reservation
     WHERE reservation.run_snapshot_id=requested_run_snapshot_id
       AND reservation.stage=requested_stage
       AND reservation.round_ordinal=requested_round_ordinal;
    IF admitted_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_run_llm_spend_reservations reservation
             WHERE reservation.id=admitted_id
               AND reservation.tenant_id=requested_tenant_id
               AND reservation.user_id=requested_user_id
               AND reservation.task_id=requested_task_id
               AND reservation.subject_id=requested_subject_id
               AND reservation.attempt_key=requested_attempt_key
               AND reservation.request_digest=requested_request_digest
               AND reservation.trace_id=requested_trace_id
               AND reservation.model=stage_model
               AND reservation.system_prompt_digest=
                   encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex')
               AND reservation.user_prompt_digest=
                   encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex')
               AND reservation.temperature IS NOT DISTINCT FROM stage_temperature
               AND reservation.disable_thinking IS NOT DISTINCT FROM stage_disable_thinking
               AND reservation.model_policy_digest=snapshot_model_policy_digest
               AND reservation.reserved_quota_tokens=derived_quota_tokens
               AND reservation.reserved_completion_tokens=stage_max_tokens
               AND reservation.reserved_planner_tokens=derived_planner_tokens
        ) THEN
            RAISE EXCEPTION '091: LLM admission replay differs' USING ERRCODE='23514';
        END IF;
        RETURN QUERY SELECT admitted_id,false;
        RETURN;
    END IF;

    SELECT pricing.id,ceil(
             (derived_quota_tokens-stage_max_tokens)::numeric *
                pricing.input_cache_miss_per_million +
             stage_max_tokens::numeric * pricing.output_per_million
           )::bigint
      INTO selected_pricing_rule_id,reserved_cost
      FROM public.provider_price_rules pricing
     WHERE pricing.provider=(snapshot_json #>> '{research_model,provider}')
       AND pricing.resource=stage_model AND pricing.meter='llm_tokens'
       AND pricing.currency='USD' AND pricing.effective_from<=clock_timestamp()
       AND (pricing.effective_to IS NULL OR pricing.effective_to>clock_timestamp())
       AND derived_quota_tokens>=stage_max_tokens
     ORDER BY pricing.effective_from DESC,pricing.id DESC
     LIMIT 1;
    IF reserved_cost IS NULL OR reserved_cost<=0 THEN
        RAISE EXCEPTION '091: LLM admission price ceiling is unavailable'
            USING ERRCODE='23514';
    END IF;

    UPDATE public.tenant_quota quota
       SET tokens=LEAST(quota.burst,
                        quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
                  - derived_quota_tokens,
           updated_at=now()
     WHERE quota.tenant_id=requested_tenant_id AND quota.bucket='llm_tokens'
       AND LEAST(quota.burst,
                 quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
           >=derived_quota_tokens;
    IF NOT FOUND THEN
        RAISE EXCEPTION '091: LLM quota admission denied' USING ERRCODE='P0001';
    END IF;

    INSERT INTO public.research_run_llm_spend_reservations (
        tenant_id,user_id,task_id,run_snapshot_id,stage,round_ordinal,subject_id,
        attempt_key,request_digest,trace_id,model,system_prompt_digest,
        user_prompt_digest,temperature,disable_thinking,model_policy_digest,
        quota_bucket,reserved_quota_tokens,reserved_completion_tokens,
        reserved_planner_tokens,reserved_cost_micro_usd,pricing_rule_id,
        cost_currency,counts_against_planner_budget,schema_version
    ) VALUES (
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,requested_stage,requested_round_ordinal,
        requested_subject_id,requested_attempt_key,requested_request_digest,
        requested_trace_id,stage_model,
        encode(sha256(convert_to(stage_system_prompt,'UTF8')),'hex'),
        encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex'),
        stage_temperature,stage_disable_thinking,
        snapshot_model_policy_digest,'llm_tokens',derived_quota_tokens,
        stage_max_tokens,derived_planner_tokens,reserved_cost,
        selected_pricing_rule_id,'USD',requested_stage='planner',
        'vane.research-run-llm-spend-reservation/v1'
    ) RETURNING id INTO admitted_id;
    RETURN QUERY SELECT admitted_id,true;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION admit_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION admit_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) TO vane_research_v3_executor;

ALTER TABLE llm_calls
    ADD COLUMN research_run_llm_spend_reservation_id BIGINT,
    ADD COLUMN disable_thinking BOOLEAN;
ALTER TABLE llm_calls ADD CONSTRAINT fk_llm_calls_research_llm_spend_reservation
    FOREIGN KEY (research_run_llm_spend_reservation_id)
    REFERENCES research_run_llm_spend_reservations(id);
CREATE UNIQUE INDEX uq_llm_calls_research_llm_spend_reservation
    ON llm_calls(research_run_llm_spend_reservation_id)
    WHERE research_run_llm_spend_reservation_id IS NOT NULL;

ALTER TABLE llm_calls DROP CONSTRAINT fk_llm_calls_tenant;
ALTER TABLE llm_calls ADD CONSTRAINT fk_llm_calls_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE;

-- 022 granted vane_app table-wide INSERT before the V3 binding column existed.
-- Revoke that expanding privilege and restore only the legacy recorder shape,
-- so a compromised legacy path cannot reserve the unique binding first and
-- permanently deny the real settlement (or forge V3 authority).
REVOKE INSERT ON llm_calls FROM vane_app;
GRANT INSERT (
    trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    created_at
) ON llm_calls TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION protect_bound_research_llm_call_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE reservation_stage TEXT;
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.research_run_llm_spend_reservation_id IS NOT NULL AND
           (pg_trigger_depth()<=1 OR OLD.tenant_id IS NULL OR EXISTS (
                SELECT 1 FROM public.tenants tenant WHERE tenant.id=OLD.tenant_id
           )) THEN
            RAISE EXCEPTION '091: V3-bound LLM call is immutable' USING ERRCODE='42501';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' THEN
        IF OLD.research_run_llm_spend_reservation_id IS NOT NULL OR
           NEW.research_run_llm_spend_reservation_id IS NOT NULL THEN
            RAISE EXCEPTION '091: V3 LLM binding is insert-only and immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.research_run_llm_spend_reservation_id IS NULL THEN RETURN NEW; END IF;
    SELECT reservation.stage INTO reservation_stage
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
     WHERE reservation.id=NEW.research_run_llm_spend_reservation_id
       AND reservation.tenant_id=NEW.tenant_id
       AND reservation.user_id=NEW.user_id
       AND reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.trace_id=NEW.trace_id
       AND reservation.system_prompt_digest=
           encode(sha256(convert_to(NEW.system_prompt,'UTF8')),'hex')
       AND reservation.user_prompt_digest=
           encode(sha256(convert_to(NEW.user_prompt,'UTF8')),'hex')
       AND reservation.temperature IS NOT DISTINCT FROM NEW.temperature
       AND reservation.reserved_completion_tokens=NEW.max_tokens
       AND reservation.disable_thinking IS NOT DISTINCT FROM NEW.disable_thinking
       AND reservation.pricing_rule_id=NEW.pricing_rule_id
       AND reservation.cost_currency=NEW.cost_currency
       AND pricing.provider=NEW.provider
       AND pricing.resource=reservation.model AND pricing.meter='llm_tokens'
       AND btrim(NEW.model)=NEW.model AND octet_length(NEW.model) BETWEEN 1 AND 255;
    IF reservation_stage IS NULL OR NEW.span_name IS DISTINCT FROM
       (CASE reservation_stage WHEN 'planner' THEN 'research_planner'
                               ELSE 'research_synthesis' END) OR
       NEW.ref_type<>'research_run_snapshot' OR
       NEW.ref_id IS DISTINCT FROM NEW.run_snapshot_id THEN
        RAISE EXCEPTION '091: LLM call does not match exact V3 reservation'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_bound_research_llm_call_v1() FROM PUBLIC;
CREATE TRIGGER protect_bound_research_llm_call_v1
BEFORE INSERT OR UPDATE OR DELETE ON llm_calls
FOR EACH ROW EXECUTE FUNCTION protect_bound_research_llm_call_v1();

CREATE TABLE research_run_llm_spend_settlements (
    id                       BIGSERIAL   PRIMARY KEY,
    tenant_id                BIGINT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id                  BIGINT      NOT NULL REFERENCES users(id),
    task_id                  TEXT        NOT NULL,
    run_snapshot_id          BIGINT      NOT NULL REFERENCES task_run_snapshots(id),
    reservation_id           BIGINT      NOT NULL REFERENCES research_run_llm_spend_reservations(id) ON DELETE CASCADE,
    llm_call_id              BIGINT      REFERENCES llm_calls(id) ON DELETE CASCADE,
    stage                    TEXT        NOT NULL,
    round_ordinal            INTEGER     NOT NULL,
    attempt_key              TEXT        NOT NULL,
    attempted                BOOLEAN     NOT NULL,
    usage_known              BOOLEAN     NOT NULL,
    definitely_zero_usage    BOOLEAN     NOT NULL,
    actual_prompt_tokens     BIGINT      NOT NULL,
    actual_completion_tokens BIGINT      NOT NULL,
    actual_cost_micro_usd    BIGINT      NOT NULL,
    pricing_status           TEXT        NOT NULL,
    cost_currency            TEXT        NOT NULL,
    outcome                  TEXT        NOT NULL,
    error_code               TEXT        NOT NULL,
    schema_version           TEXT        NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_llm_spend_settlement_reservation UNIQUE(reservation_id),
    CONSTRAINT uq_research_llm_spend_settlement_call UNIQUE(llm_call_id),
    CONSTRAINT uq_research_llm_spend_settlement_attempt UNIQUE(attempt_key),
    CONSTRAINT ck_research_llm_spend_settlement_shape CHECK (
        stage IN ('planner','synthesis') AND round_ordinal BETWEEN 0 AND 15 AND
        attempt_key ~ '^[0-9a-f]{64}$' AND
        actual_prompt_tokens BETWEEN 0 AND 1000000 AND
        actual_completion_tokens BETWEEN 0 AND 1000000 AND
        actual_cost_micro_usd BETWEEN 0 AND 100000000 AND
        pricing_status IN ('provider_reported','calculated','estimated','unpriced') AND
        cost_currency='USD' AND outcome IN ('completed','failed','indeterminate') AND
        btrim(error_code)=error_code AND octet_length(error_code)<=128 AND
        ((NOT attempted AND NOT usage_known AND definitely_zero_usage AND
          llm_call_id IS NULL AND
          actual_prompt_tokens=0 AND actual_completion_tokens=0 AND actual_cost_micro_usd=0 AND
          outcome='failed' AND error_code<>'') OR
         (attempted AND llm_call_id IS NOT NULL AND
          ((usage_known AND NOT definitely_zero_usage AND
            actual_prompt_tokens+actual_completion_tokens>0) OR
           (NOT usage_known AND definitely_zero_usage AND
            actual_prompt_tokens=0 AND actual_completion_tokens=0 AND
            actual_cost_micro_usd=0) OR
           (NOT usage_known AND NOT definitely_zero_usage AND
            actual_prompt_tokens=0 AND actual_completion_tokens=0 AND
            pricing_status='estimated' AND outcome='indeterminate'))))
    ),
    CONSTRAINT ck_research_llm_spend_settlement_outcome CHECK (
        (outcome='completed' AND error_code='') OR
        (outcome IN ('failed','indeterminate') AND error_code<>'')
    ),
    CONSTRAINT ck_research_llm_spend_settlement_schema CHECK (
        schema_version='vane.research-run-llm-spend-settlement/v1'
    )
);

CREATE INDEX idx_research_llm_spend_settlement_scope
    ON research_run_llm_spend_settlements
       (tenant_id,user_id,task_id,run_snapshot_id,stage,round_ordinal,id);

-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_llm_spend_settlement_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE reserved_tokens BIGINT;
        reserved_cost BIGINT;
BEGIN
    SELECT reservation.reserved_quota_tokens,reservation.reserved_cost_micro_usd
      INTO reserved_tokens,reserved_cost
      FROM public.research_run_llm_spend_reservations reservation
     WHERE reservation.id=NEW.reservation_id
       AND reservation.tenant_id=NEW.tenant_id AND reservation.user_id=NEW.user_id
       AND reservation.task_id=NEW.task_id
       AND reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.stage=NEW.stage AND reservation.round_ordinal=NEW.round_ordinal
       AND reservation.attempt_key=NEW.attempt_key;
    IF reserved_tokens IS NULL OR
       (NEW.attempted AND NOT NEW.usage_known AND NOT NEW.definitely_zero_usage AND
        NEW.actual_cost_micro_usd<>reserved_cost) OR
       (NEW.usage_known AND
       NEW.actual_prompt_tokens+NEW.actual_completion_tokens>1000000) OR
       (NEW.llm_call_id IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM public.llm_calls call
            WHERE call.id=NEW.llm_call_id
              AND call.research_run_llm_spend_reservation_id=NEW.reservation_id
              AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
              AND call.run_snapshot_id=NEW.run_snapshot_id
              AND call.prompt_tokens=NEW.actual_prompt_tokens
              AND call.completion_tokens=NEW.actual_completion_tokens
              AND call.pricing_status=NEW.pricing_status
              AND call.cost_currency=NEW.cost_currency
              AND round(call.cost_usd*1000000,0)::bigint=NEW.actual_cost_micro_usd
              AND ((NEW.outcome='completed' AND call.error='') OR
                   (NEW.outcome<>'completed' AND call.error<>''))
       )) THEN
        RAISE EXCEPTION '091: LLM settlement differs from reservation or call'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_llm_spend_settlement_v1() FROM PUBLIC;
CREATE TRIGGER research_run_llm_spend_settlement_v1
BEFORE INSERT ON research_run_llm_spend_settlements
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_settlement_v1();

-- Reconcile the initial quota debit exactly once from immutable settlement.
-- Admission is the minimum authoritative charge. Executor/provider reports
-- can add overage debt, but can never refund the conservative reservation.
-- +goose StatementBegin
CREATE FUNCTION reconcile_research_run_llm_quota_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE affected BIGINT;
BEGIN
    UPDATE public.tenant_quota quota
       SET tokens=LEAST(quota.burst,
                        quota.tokens+quota.rate*EXTRACT(EPOCH FROM(now()-quota.updated_at)))
                  - GREATEST(
                      NEW.actual_prompt_tokens+NEW.actual_completion_tokens-
                        reservation.reserved_quota_tokens,
                      0
                    ),
           updated_at=now()
      FROM public.research_run_llm_spend_reservations reservation
     WHERE reservation.id=NEW.reservation_id
       AND reservation.tenant_id=NEW.tenant_id
       AND quota.tenant_id=NEW.tenant_id AND quota.bucket='llm_tokens';
    GET DIAGNOSTICS affected=ROW_COUNT;
    IF affected<>1 THEN
        RAISE EXCEPTION '091: LLM quota reconciliation has no exact reservation'
            USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reconcile_research_run_llm_quota_v3() FROM PUBLIC;
CREATE TRIGGER reconcile_research_run_llm_quota_v3
AFTER INSERT ON research_run_llm_spend_settlements
FOR EACH ROW EXECUTE FUNCTION reconcile_research_run_llm_quota_v3();

-- Immutable except for structural tenant-root cascades.
-- +goose StatementBegin
CREATE FUNCTION protect_research_run_llm_spend_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='UPDATE' OR pg_trigger_depth()<=1 OR EXISTS (
        SELECT 1 FROM public.tenants tenant WHERE tenant.id=OLD.tenant_id
    ) THEN
        RAISE EXCEPTION '091: V3 LLM spend evidence is immutable' USING ERRCODE='42501';
    END IF;
    RETURN OLD;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_run_llm_spend_v1() FROM PUBLIC;
CREATE TRIGGER protect_research_run_llm_spend_reservation_v1
BEFORE UPDATE OR DELETE ON research_run_llm_spend_reservations
FOR EACH ROW EXECUTE FUNCTION protect_research_run_llm_spend_v1();
CREATE TRIGGER protect_research_run_llm_spend_settlement_v1
BEFORE UPDATE OR DELETE ON research_run_llm_spend_settlements
FOR EACH ROW EXECUTE FUNCTION protect_research_run_llm_spend_v1();

-- The only terminal capability recomputes price from immutable catalog rates
-- and provider token/cache facts, then atomically appends the bound llm_call,
-- settlement and quota reconciliation. The executor has no direct INSERT on
-- either evidence table, so it cannot claim a forged zero-cost completion.
-- +goose StatementBegin
CREATE FUNCTION settle_research_run_llm_spend_v3(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT,
    requested_reservation_id BIGINT,
    requested_system_prompt TEXT,
    requested_user_prompt TEXT,
    requested_completion TEXT,
    requested_provider_reported_model TEXT,
    requested_prompt_tokens INTEGER,
    requested_completion_tokens INTEGER,
    requested_prompt_cache_hit_tokens INTEGER,
    requested_prompt_cache_miss_tokens INTEGER,
    requested_reasoning_tokens INTEGER,
    requested_latency_ms INTEGER,
    requested_prefix_cache_hit BOOLEAN,
    requested_temperature REAL,
    requested_max_tokens INTEGER,
    requested_disable_thinking BOOLEAN,
    requested_error TEXT,
    requested_attempted BOOLEAN,
    requested_usage_known BOOLEAN,
    requested_definitely_zero_usage BOOLEAN,
    requested_outcome TEXT,
    requested_error_code TEXT
) RETURNS TABLE(out_llm_call_id BIGINT,out_settlement_id BIGINT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    reservation_row RECORD;
    actual_prompt BIGINT;
    actual_completion BIGINT;
    actual_cost BIGINT;
    final_pricing_status TEXT;
    inserted_call_id BIGINT;
    inserted_settlement_id BIGINT;
    existing_call_id BIGINT;
    existing_settlement_id BIGINT;
BEGIN
    IF requested_tenant_id IS NULL OR requested_user_id IS NULL OR
       requested_task_id IS NULL OR requested_run_snapshot_id IS NULL OR
       requested_reservation_id IS NULL OR requested_system_prompt IS NULL OR
       requested_user_prompt IS NULL OR requested_completion IS NULL OR
       requested_provider_reported_model IS NULL OR
       btrim(requested_provider_reported_model)<>requested_provider_reported_model OR
       octet_length(requested_provider_reported_model) NOT BETWEEN 1 AND 255 OR
       octet_length(requested_completion)>8192 OR
       octet_length(requested_error)>4096 OR
       requested_prompt_tokens IS NULL OR requested_completion_tokens IS NULL OR
       requested_latency_ms IS NULL OR requested_temperature IS NULL OR
       requested_max_tokens IS NULL OR requested_disable_thinking IS NULL OR
       requested_error IS NULL OR requested_attempted IS NULL OR
       requested_usage_known IS NULL OR requested_definitely_zero_usage IS NULL OR
       requested_outcome IS NULL OR requested_error_code IS NULL OR
       requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       requested_user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '091: LLM settlement scope differs from session'
            USING ERRCODE='42501';
    END IF;
    SELECT reservation.*,
           pricing.provider AS price_provider,
           pricing.input_cache_hit_per_million AS hit_rate,
           pricing.input_cache_miss_per_million AS miss_rate,
           pricing.output_per_million AS output_rate
      INTO reservation_row
      FROM public.research_run_llm_spend_reservations reservation
      JOIN public.provider_price_rules pricing ON pricing.id=reservation.pricing_rule_id
     WHERE reservation.id=requested_reservation_id
       AND reservation.tenant_id=requested_tenant_id
       AND reservation.user_id=requested_user_id
       AND reservation.task_id=requested_task_id
       AND reservation.run_snapshot_id=requested_run_snapshot_id
       AND pricing.meter='llm_tokens' AND pricing.currency='USD'
     FOR UPDATE OF reservation;
    IF reservation_row.id IS NULL OR requested_prompt_tokens<0 OR
       requested_completion_tokens<0 OR requested_latency_ms<0 OR
       (requested_usage_known AND requested_reasoning_tokens IS NOT NULL AND
        (requested_reasoning_tokens<0 OR
         requested_reasoning_tokens>requested_completion_tokens)) OR
       octet_length(requested_system_prompt)>2097152 OR
       octet_length(requested_user_prompt)>2097152 THEN
        RAISE EXCEPTION '091: LLM settlement request is invalid'
            USING ERRCODE='23514';
    END IF;
    IF reservation_row.system_prompt_digest IS DISTINCT FROM
           encode(sha256(convert_to(requested_system_prompt,'UTF8')),'hex') OR
       reservation_row.user_prompt_digest IS DISTINCT FROM
           encode(sha256(convert_to(requested_user_prompt,'UTF8')),'hex') OR
       reservation_row.temperature IS DISTINCT FROM requested_temperature OR
       reservation_row.reserved_completion_tokens IS DISTINCT FROM requested_max_tokens OR
       reservation_row.disable_thinking IS DISTINCT FROM requested_disable_thinking THEN
        RAISE EXCEPTION '091: LLM settlement request differs from reservation'
            USING ERRCODE='23514';
    END IF;

    IF NOT requested_attempted THEN
        IF requested_usage_known OR NOT requested_definitely_zero_usage OR
           requested_prompt_tokens<>0 OR requested_completion_tokens<>0 OR
           requested_prompt_cache_hit_tokens IS NOT NULL OR
           requested_prompt_cache_miss_tokens IS NOT NULL OR
           requested_reasoning_tokens IS NOT NULL OR requested_prefix_cache_hit IS NOT NULL OR
           requested_outcome<>'failed' OR requested_error_code='' THEN
            RAISE EXCEPTION '091: unattempted LLM settlement is invalid' USING ERRCODE='23514';
        END IF;
        actual_prompt:=0; actual_completion:=0; actual_cost:=0;
        final_pricing_status:='calculated';
    ELSIF requested_usage_known THEN
        IF requested_definitely_zero_usage OR
           requested_prompt_tokens+requested_completion_tokens<=0 OR
           (requested_prompt_cache_hit_tokens IS NULL)<>
               (requested_prompt_cache_miss_tokens IS NULL) OR
           (requested_prompt_cache_hit_tokens IS NULL AND
                requested_prefix_cache_hit IS NOT NULL) OR
           (requested_prompt_cache_hit_tokens IS NOT NULL AND (
               requested_prompt_cache_hit_tokens<0 OR
               requested_prompt_cache_miss_tokens<0 OR
               requested_prompt_cache_hit_tokens+requested_prompt_cache_miss_tokens<>
                   requested_prompt_tokens OR
               requested_prefix_cache_hit IS NULL OR
               requested_prefix_cache_hit IS DISTINCT FROM
                   (requested_prompt_cache_hit_tokens>0)
           )) THEN
            RAISE EXCEPTION '091: known LLM usage is incomplete' USING ERRCODE='23514';
        END IF;
        actual_prompt:=requested_prompt_tokens;
        actual_completion:=requested_completion_tokens;
        actual_cost:=ceil(
            COALESCE(requested_prompt_cache_hit_tokens,0)::numeric*reservation_row.hit_rate+
            COALESCE(requested_prompt_cache_miss_tokens,requested_prompt_tokens)::numeric*
                reservation_row.miss_rate+
            requested_completion_tokens::numeric*reservation_row.output_rate
        )::bigint;
        final_pricing_status:=CASE
            WHEN requested_prompt_cache_hit_tokens IS NULL THEN 'estimated'
            ELSE 'calculated' END;
    ELSIF requested_definitely_zero_usage THEN
        IF requested_prompt_tokens<>0 OR requested_completion_tokens<>0 OR
           requested_prompt_cache_hit_tokens IS NOT NULL OR
           requested_prompt_cache_miss_tokens IS NOT NULL OR
           requested_reasoning_tokens IS NOT NULL OR requested_prefix_cache_hit IS NOT NULL OR
           requested_outcome<>'failed' OR requested_error_code='' THEN
            RAISE EXCEPTION '091: confirmed-zero LLM settlement is invalid' USING ERRCODE='23514';
        END IF;
        actual_prompt:=0; actual_completion:=0; actual_cost:=0;
        final_pricing_status:='calculated';
    ELSE
        IF requested_prompt_tokens<>0 OR requested_completion_tokens<>0 OR
           requested_prompt_cache_hit_tokens IS NOT NULL OR
           requested_prompt_cache_miss_tokens IS NOT NULL OR
           requested_reasoning_tokens IS NOT NULL OR requested_prefix_cache_hit IS NOT NULL OR
           requested_outcome<>'indeterminate' OR requested_error_code='' THEN
            RAISE EXCEPTION '091: unknown LLM usage must remain conservative' USING ERRCODE='23514';
        END IF;
        actual_prompt:=0; actual_completion:=0;
        actual_cost:=reservation_row.reserved_cost_micro_usd;
        final_pricing_status:='estimated';
    END IF;
    IF (requested_outcome='completed' AND (requested_error<>'' OR requested_error_code<>'')) OR
       (requested_outcome IN ('failed','indeterminate') AND requested_error_code='') OR
       requested_outcome NOT IN ('completed','failed','indeterminate') THEN
        RAISE EXCEPTION '091: LLM settlement outcome is invalid' USING ERRCODE='23514';
    END IF;

    SELECT settlement.id,settlement.llm_call_id
      INTO existing_settlement_id,existing_call_id
      FROM public.research_run_llm_spend_settlements settlement
     WHERE settlement.reservation_id=reservation_row.id;
    IF existing_settlement_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM public.research_run_llm_spend_settlements settlement
             WHERE settlement.id=existing_settlement_id
               AND settlement.attempted=requested_attempted
               AND settlement.usage_known=requested_usage_known
               AND settlement.definitely_zero_usage=requested_definitely_zero_usage
               AND settlement.actual_prompt_tokens=actual_prompt
               AND settlement.actual_completion_tokens=actual_completion
               AND settlement.actual_cost_micro_usd=actual_cost
               AND settlement.outcome=requested_outcome
               AND settlement.error_code=requested_error_code
        ) OR (requested_attempted AND NOT EXISTS (
            SELECT 1 FROM public.llm_calls call
             WHERE call.id=existing_call_id
               AND call.system_prompt=requested_system_prompt
               AND call.user_prompt=requested_user_prompt
               AND call.completion=requested_completion
               AND call.model=requested_provider_reported_model
               AND call.prompt_tokens=actual_prompt
               AND call.completion_tokens=actual_completion
               AND call.prompt_cache_hit_tokens IS NOT DISTINCT FROM
                   (CASE WHEN requested_usage_known THEN requested_prompt_cache_hit_tokens ELSE NULL END)
               AND call.prompt_cache_miss_tokens IS NOT DISTINCT FROM
                   (CASE WHEN requested_usage_known THEN requested_prompt_cache_miss_tokens ELSE NULL END)
               AND call.reasoning_tokens IS NOT DISTINCT FROM
                   (CASE WHEN requested_usage_known THEN requested_reasoning_tokens ELSE NULL END)
               AND call.latency_ms=requested_latency_ms
               AND call.prefix_cache_hit IS NOT DISTINCT FROM requested_prefix_cache_hit
               AND call.temperature IS NOT DISTINCT FROM requested_temperature
               AND call.max_tokens=requested_max_tokens
               AND call.disable_thinking IS NOT DISTINCT FROM requested_disable_thinking
               AND call.error=requested_error
        )) THEN
            RAISE EXCEPTION '091: LLM settlement replay differs' USING ERRCODE='23514';
        END IF;
        RETURN QUERY SELECT existing_call_id,existing_settlement_id;
        RETURN;
    END IF;

    IF requested_attempted THEN
        INSERT INTO public.llm_calls (
            trace_id,span_name,user_id,ref_type,ref_id,provider,model,
            system_prompt,user_prompt,completion,prompt_tokens,completion_tokens,
            latency_ms,cost_usd,prefix_cache_hit,temperature,max_tokens,
            disable_thinking,error,tenant_id,prompt_cache_hit_tokens,
            prompt_cache_miss_tokens,reasoning_tokens,pricing_rule_id,
            pricing_status,cost_amount,cost_currency,run_snapshot_id,
            research_run_llm_spend_reservation_id,created_at
        ) VALUES (
            reservation_row.trace_id,
            CASE reservation_row.stage WHEN 'planner' THEN 'research_planner'
                                       ELSE 'research_synthesis' END,
            requested_user_id,'research_run_snapshot',requested_run_snapshot_id,
            reservation_row.price_provider,requested_provider_reported_model,
            requested_system_prompt,requested_user_prompt,requested_completion,
            actual_prompt,actual_completion,requested_latency_ms,
            actual_cost::numeric/1000000::numeric,requested_prefix_cache_hit,
            requested_temperature,requested_max_tokens,requested_disable_thinking,
            requested_error,requested_tenant_id,
            CASE WHEN requested_usage_known THEN requested_prompt_cache_hit_tokens ELSE NULL END,
            CASE WHEN requested_usage_known THEN requested_prompt_cache_miss_tokens ELSE NULL END,
            CASE WHEN requested_usage_known THEN requested_reasoning_tokens ELSE NULL END,
            reservation_row.pricing_rule_id,final_pricing_status,
            actual_cost::numeric/1000000::numeric,'USD',requested_run_snapshot_id,
            reservation_row.id,clock_timestamp()
        ) RETURNING id INTO inserted_call_id;
    END IF;

    INSERT INTO public.research_run_llm_spend_settlements (
        tenant_id,user_id,task_id,run_snapshot_id,reservation_id,llm_call_id,
        stage,round_ordinal,attempt_key,attempted,usage_known,definitely_zero_usage,
        actual_prompt_tokens,actual_completion_tokens,actual_cost_micro_usd,
        pricing_status,cost_currency,outcome,error_code,schema_version
    ) VALUES (
        requested_tenant_id,requested_user_id,requested_task_id,
        requested_run_snapshot_id,reservation_row.id,inserted_call_id,
        reservation_row.stage,reservation_row.round_ordinal,reservation_row.attempt_key,
        requested_attempted,requested_usage_known,requested_definitely_zero_usage,
        actual_prompt,actual_completion,actual_cost,final_pricing_status,'USD',
        requested_outcome,requested_error_code,
        'vane.research-run-llm-spend-settlement/v1'
    ) RETURNING id INTO inserted_settlement_id;
    RETURN QUERY SELECT inserted_call_id,inserted_settlement_id;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION settle_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,
    INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,BOOLEAN,TEXT,
    BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION settle_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,
    INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,BOOLEAN,TEXT,
    BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) TO vane_research_v3_executor;

GRANT SELECT ON research_run_llm_spend_reservations,
                research_run_llm_spend_settlements TO vane_app;
GRANT SELECT ON research_run_llm_spend_reservations,
                research_run_llm_spend_settlements TO vane_research_v3_executor;
GRANT SELECT (
    id,trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,disable_thinking,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    research_run_llm_spend_reservation_id,created_at
) ON llm_calls TO vane_research_v3_executor;
GRANT SELECT (
    id,provider,resource,meter,currency,input_cache_hit_per_million,
    input_cache_miss_per_million,output_per_million,effective_from,effective_to
) ON provider_price_rules TO vane_research_v3_executor;

ALTER TABLE research_run_llm_spend_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_run_llm_spend_settlements ENABLE ROW LEVEL SECURITY;
ALTER TABLE llm_calls ENABLE ROW LEVEL SECURITY;
CREATE POLICY research_v3_scope ON llm_calls
    AS RESTRICTIVE FOR ALL TO vane_research_v3_executor
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM
            NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'research_run_llm_spend_reservations','research_run_llm_spend_settlements'
    ] LOOP
        EXECUTE format('CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',table_name);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint)',table_name);
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',table_name);
        EXECUTE format(
            'CREATE POLICY research_v3_scope ON %I AS RESTRICTIVE FOR ALL TO vane_research_v3_executor '
            'USING (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint '
            'AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint '
            'AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',table_name);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_llm_spend_settlements,llm_calls,
           research_run_llm_spend_reservations IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_llm_spend_reservations) OR
       EXISTS (SELECT 1 FROM research_run_llm_spend_settlements) OR
       EXISTS (SELECT 1 FROM llm_calls WHERE research_run_llm_spend_reservation_id IS NOT NULL) THEN
        RAISE EXCEPTION '091: refusing downgrade while V3 LLM spend authority or evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION admit_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
) FROM vane_research_v3_executor;
REVOKE ALL ON FUNCTION settle_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,
    INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,BOOLEAN,TEXT,
    BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
) FROM vane_research_v3_executor;
REVOKE ALL ON research_run_llm_spend_reservations,
    research_run_llm_spend_settlements FROM vane_research_v3_executor,vane_app;
REVOKE SELECT (
    id,provider,resource,meter,currency,input_cache_hit_per_million,
    input_cache_miss_per_million,output_per_million,effective_from,effective_to
) ON provider_price_rules FROM vane_research_v3_executor;
REVOKE INSERT (
    trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,disable_thinking,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    research_run_llm_spend_reservation_id,created_at
) ON llm_calls FROM vane_research_v3_executor;
REVOKE SELECT (
    id,trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,disable_thinking,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    research_run_llm_spend_reservation_id,created_at
) ON llm_calls FROM vane_research_v3_executor;
DROP POLICY research_v3_scope ON llm_calls;

REVOKE INSERT (
    trace_id,span_name,user_id,ref_type,ref_id,provider,model,system_prompt,
    user_prompt,completion,prompt_tokens,completion_tokens,latency_ms,cost_usd,
    prefix_cache_hit,temperature,max_tokens,error,tenant_id,
    prompt_cache_hit_tokens,prompt_cache_miss_tokens,reasoning_tokens,
    pricing_rule_id,pricing_status,cost_amount,cost_currency,run_snapshot_id,
    created_at
) ON llm_calls FROM vane_app;
GRANT INSERT ON llm_calls TO vane_app;

DROP TRIGGER protect_research_run_llm_spend_settlement_v1 ON research_run_llm_spend_settlements;
DROP TRIGGER protect_research_run_llm_spend_reservation_v1 ON research_run_llm_spend_reservations;
DROP FUNCTION protect_research_run_llm_spend_v1();
DROP FUNCTION settle_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,INTEGER,INTEGER,
    INTEGER,INTEGER,INTEGER,INTEGER,BOOLEAN,REAL,INTEGER,BOOLEAN,TEXT,
    BOOLEAN,BOOLEAN,BOOLEAN,TEXT,TEXT
);
DROP TRIGGER reconcile_research_run_llm_quota_v3 ON research_run_llm_spend_settlements;
DROP FUNCTION reconcile_research_run_llm_quota_v3();
DROP TRIGGER research_run_llm_spend_settlement_v1 ON research_run_llm_spend_settlements;
DROP FUNCTION enforce_research_run_llm_spend_settlement_v1();
DROP TABLE research_run_llm_spend_settlements;

DROP TRIGGER protect_bound_research_llm_call_v1 ON llm_calls;
DROP FUNCTION protect_bound_research_llm_call_v1();
DROP INDEX uq_llm_calls_research_llm_spend_reservation;
ALTER TABLE llm_calls DROP CONSTRAINT fk_llm_calls_research_llm_spend_reservation,
    DROP COLUMN research_run_llm_spend_reservation_id,
    DROP COLUMN disable_thinking;
ALTER TABLE llm_calls DROP CONSTRAINT fk_llm_calls_tenant;
ALTER TABLE llm_calls ADD CONSTRAINT fk_llm_calls_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id);

DROP FUNCTION admit_research_run_llm_spend_v3(
    BIGINT,BIGINT,TEXT,BIGINT,TEXT,INTEGER,BIGINT,TEXT,TEXT,TEXT,TEXT
);
DROP TRIGGER research_run_shared_planner_budget_v3 ON research_run_step_spend_reservations;
DROP FUNCTION enforce_research_run_shared_planner_budget_v3();
DROP TRIGGER research_run_llm_spend_reservation_v1 ON research_run_llm_spend_reservations;
DROP FUNCTION enforce_research_run_llm_spend_reservation_v1();
DROP TABLE research_run_llm_spend_reservations;
