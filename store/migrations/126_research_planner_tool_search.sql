-- 126: immutable local tool-search receipts for the v3.3 research planner.
-- Old v3-v3.2 snapshots, prompts, completions, plans and replay semantics are
-- retained byte-for-byte.
-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_run_plans,
           research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

CREATE TABLE research_planner_tool_search_receipts (
    id                               BIGSERIAL PRIMARY KEY,
    tenant_id                        BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id                          BIGINT NOT NULL REFERENCES users(id),
    task_id                          TEXT NOT NULL,
    run_snapshot_id                  BIGINT NOT NULL REFERENCES task_run_snapshots(id),
    round_ordinal                    INTEGER NOT NULL,
    planner_llm_spend_reservation_id BIGINT NOT NULL REFERENCES research_run_llm_spend_reservations(id),
    catalog_digest                   TEXT NOT NULL,
    receipt_payload                  BYTEA NOT NULL,
    receipt_digest                   TEXT NOT NULL,
    schema_version                   TEXT NOT NULL,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_planner_search_round UNIQUE(run_snapshot_id,round_ordinal),
    CONSTRAINT uq_research_planner_search_reservation UNIQUE(planner_llm_spend_reservation_id),
    CONSTRAINT ck_research_planner_search_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        round_ordinal BETWEEN 0 AND 7
    ),
    CONSTRAINT ck_research_planner_search_digests CHECK (
        catalog_digest ~ '^[0-9a-f]{64}$' AND
        receipt_digest ~ '^[0-9a-f]{64}$' AND
        receipt_digest=encode(sha256(receipt_payload),'hex')
    ),
    CONSTRAINT ck_research_planner_search_payload CHECK (
        octet_length(receipt_payload) BETWEEN 2 AND 262144 AND
        position(decode('00','hex') in receipt_payload)=0
    ),
    CONSTRAINT ck_research_planner_search_schema CHECK (
        schema_version='vane.research-planner-tool-search-receipt/v1'
    )
);

CREATE INDEX idx_research_planner_search_scope
    ON research_planner_tool_search_receipts
       (tenant_id,user_id,task_id,run_snapshot_id,round_ordinal,id);

-- Rebuild the exact Go encoding/json bytes. research_scope_json_string_v124
-- supplies the same HTML and U+2028/U+2029 escaping as Go.
-- +goose StatementBegin
CREATE FUNCTION research_planner_search_canonical_v126(receipt_payload BYTEA)
RETURNS BYTEA
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    receipt_text TEXT;
    receipt_json JSONB;
    matches_text TEXT;
    canonical TEXT;
BEGIN
    receipt_text := convert_from(receipt_payload,'UTF8');
    IF NOT (receipt_text IS JSON OBJECT WITH UNIQUE KEYS) THEN RETURN NULL; END IF;
    receipt_json := receipt_text::jsonb;
    IF jsonb_typeof(receipt_json) IS DISTINCT FROM 'object' OR
       (SELECT count(*) FROM jsonb_object_keys(receipt_json))<>6 OR
       (receipt_json-ARRAY['schema_version','round_ordinal','catalog_digest',
                           'query','limit','matches'])<>'{}'::jsonb OR
       receipt_json->>'schema_version' IS DISTINCT FROM
           'vane.research-planner-tool-search-receipt/v1' OR
       jsonb_typeof(receipt_json->'round_ordinal') IS DISTINCT FROM 'number' OR
       receipt_json->>'round_ordinal' !~ '^[0-7]$' OR
       jsonb_typeof(receipt_json->'catalog_digest') IS DISTINCT FROM 'string' OR
       receipt_json->>'catalog_digest' !~ '^[0-9a-f]{64}$' OR
       jsonb_typeof(receipt_json->'query') IS DISTINCT FROM 'string' OR
       octet_length(receipt_json->>'query') NOT BETWEEN 1 AND 512 OR
       NOT public.research_text_is_go_trimmed_v125(receipt_json->>'query') OR
       jsonb_typeof(receipt_json->'limit') IS DISTINCT FROM 'number' OR
       receipt_json->>'limit' !~ '^[1-8]$' OR
       jsonb_typeof(receipt_json->'matches') IS DISTINCT FROM 'array' OR
       jsonb_array_length(receipt_json->'matches')>
           (receipt_json->>'limit')::integer OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(receipt_json->'matches') match
            WHERE jsonb_typeof(match) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(match))<>3
               OR (match-ARRAY['name','schema_digest','score'])<>'{}'::jsonb
               OR jsonb_typeof(match->'name') IS DISTINCT FROM 'string'
               OR octet_length(match->>'name') NOT BETWEEN 1 AND 255
               OR NOT public.research_text_is_go_trimmed_v125(match->>'name')
               OR jsonb_typeof(match->'schema_digest') IS DISTINCT FROM 'string'
               OR match->>'schema_digest' !~ '^[0-9a-f]{64}$'
               OR jsonb_typeof(match->'score') IS DISTINCT FROM 'string'
               OR match->>'score' !~ '^[0-9]+\.[0-9]{9}$'
               OR (match->>'score')::numeric<=0
       ) OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(receipt_json->'matches') match
            GROUP BY match->>'name' HAVING count(*)>1
       ) THEN RETURN NULL; END IF;

    SELECT coalesce(string_agg(
        '{"name":'||public.research_scope_json_string_v124(match->>'name')||
        ',"schema_digest":'||public.research_scope_json_string_v124(match->>'schema_digest')||
        ',"score":'||public.research_scope_json_string_v124(match->>'score')||'}',
        ',' ORDER BY ordinal),'')
      INTO matches_text
      FROM jsonb_array_elements(receipt_json->'matches')
           WITH ORDINALITY item(match,ordinal);
    canonical := '{"schema_version":"vane.research-planner-tool-search-receipt/v1"'||
        ',"round_ordinal":'||(receipt_json->>'round_ordinal')||
        ',"catalog_digest":'||public.research_scope_json_string_v124(receipt_json->>'catalog_digest')||
        ',"query":'||public.research_scope_json_string_v124(receipt_json->>'query')||
        ',"limit":'||(receipt_json->>'limit')||
        ',"matches":['||matches_text||']}';
    RETURN convert_to(canonical,'UTF8');
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_planner_search_canonical_v126(BYTEA) FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION enforce_research_planner_search_receipt_v126()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
    receipt_json JSONB;
BEGIN
    IF TG_OP<>'INSERT' OR NEW.receipt_payload IS DISTINCT FROM
           public.research_planner_search_canonical_v126(NEW.receipt_payload) THEN
        RAISE EXCEPTION '126: planner tool search receipt is not canonical'
            USING ERRCODE='23514';
    END IF;
    receipt_json := convert_from(NEW.receipt_payload,'UTF8')::jsonb;
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.task_run_snapshots snapshot
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
       AND snapshot.tool_policy_digest=NEW.catalog_digest;
    IF snapshot_json IS NULL OR
       snapshot_json #>> '{research_model,planner,renderer_version}' IS DISTINCT FROM
           'research-planner.render/v3.3' OR
       NEW.round_ordinal IS DISTINCT FROM (receipt_json->>'round_ordinal')::integer OR
       NEW.catalog_digest IS DISTINCT FROM receipt_json->>'catalog_digest' OR
       NEW.schema_version IS DISTINCT FROM receipt_json->>'schema_version' OR
       NEW.receipt_digest IS DISTINCT FROM encode(sha256(NEW.receipt_payload),'hex') OR
       EXISTS (
           SELECT 1 FROM jsonb_array_elements(receipt_json->'matches') match
            WHERE NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(
                    snapshot_json->'research_tools'->'allowed_tools') tool
                 WHERE tool->>'name'=match->>'name'
                   AND tool->>'schema_digest'=match->>'schema_digest'
            )
       ) THEN
        RAISE EXCEPTION '126: planner tool search receipt differs from frozen catalog'
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
           AND reservation.stage='planner' AND reservation.subject_id=0
           AND reservation.round_ordinal=NEW.round_ordinal
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_planner' AND call.error=''
           AND call.completion IS JSON OBJECT WITH UNIQUE KEYS
           AND (SELECT count(*) FROM jsonb_object_keys(call.completion::jsonb))=3
           AND (call.completion::jsonb-
                ARRAY['schema_version','action','tool_search'])='{}'::jsonb
           AND call.completion::jsonb->>'schema_version'=
               'vane.research-planner-output/v3.3'
           AND call.completion::jsonb->>'action'='tool_search'
           AND jsonb_typeof(call.completion::jsonb->'tool_search')='object'
           AND (SELECT count(*) FROM jsonb_object_keys(
                    call.completion::jsonb->'tool_search'))=2
           AND (call.completion::jsonb->'tool_search'-ARRAY['query','limit'])='{}'::jsonb
           AND call.completion::jsonb #>> '{tool_search,query}'=receipt_json->>'query'
           AND jsonb_typeof(call.completion::jsonb #> '{tool_search,limit}')='number'
           AND call.completion::jsonb #>> '{tool_search,limit}'=receipt_json->>'limit'
    ) THEN
        RAISE EXCEPTION '126: planner tool search request lacks exact paid receipt'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_planner_search_receipt_v126() FROM PUBLIC;
CREATE TRIGGER enforce_research_planner_search_receipt_v126
BEFORE INSERT ON research_planner_tool_search_receipts
FOR EACH ROW EXECUTE FUNCTION enforce_research_planner_search_receipt_v126();

-- +goose StatementBegin
CREATE FUNCTION protect_research_planner_search_receipt_v126()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    RAISE EXCEPTION '126: planner tool search receipt is immutable'
        USING ERRCODE='23514';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_research_planner_search_receipt_v126() FROM PUBLIC;
CREATE TRIGGER protect_research_planner_search_receipt_v126
BEFORE UPDATE OR DELETE ON research_planner_tool_search_receipts
FOR EACH ROW EXECUTE FUNCTION protect_research_planner_search_receipt_v126();

-- +goose StatementBegin
CREATE FUNCTION research_plan_matches_planner_completion_v126(
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
        WHEN NOT (plan_text IS JSON OBJECT WITH UNIQUE KEYS)
          OR NOT (planner_completion IS JSON OBJECT WITH UNIQUE KEYS)
        THEN false
        ELSE
            jsonb_typeof(plan_text::jsonb)='object'
            AND plan_text::jsonb = jsonb_build_object(
                'schema_version',plan_text::jsonb->'schema_version',
                'definition_digest',plan_text::jsonb->'definition_digest',
                'capability_catalog_digest',plan_text::jsonb->'capability_catalog_digest',
                'tool_policy_digest',plan_text::jsonb->'tool_policy_digest',
                'steps',plan_text::jsonb->'steps')
            AND plan_text::jsonb->>'schema_version'='vane.research-execution-plan/v3'
            AND planner_completion::jsonb = jsonb_build_object(
                'schema_version',planner_completion::jsonb->'schema_version',
                'action',planner_completion::jsonb->'action',
                'steps',planner_completion::jsonb->'steps')
            AND planner_completion::jsonb->>'schema_version'=
                'vane.research-planner-output/v3.3'
            AND planner_completion::jsonb->>'action'='final'
            AND jsonb_typeof(planner_completion::jsonb->'steps')='array'
            AND plan_text::jsonb->'steps'=planner_completion::jsonb->'steps'
        END
      FROM input
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION research_plan_matches_planner_completion_v126(BYTEA,TEXT)
    FROM PUBLIC;

-- Rebind the retained trigger name to a versioned function.  The old v104
-- function remains byte-for-byte available for Down; runtime startup can now
-- prove that the v126 admission function, rather than merely its helper, is
-- the active database authority.
DROP TRIGGER research_run_plan_llm_receipt_v1 ON research_run_plans;

-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_plan_llm_receipt_v126()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    planner_renderer TEXT;
    final_round INTEGER;
    max_tool_calls INTEGER;
    completion_text TEXT;
BEGIN
    IF TG_OP<>'INSERT' OR NEW.planner_llm_spend_reservation_id IS NULL THEN
        RAISE EXCEPTION '092: new research Plan requires an immutable planner receipt'
            USING ERRCODE='23514';
    END IF;
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb #>>
               '{research_model,planner,renderer_version}',
           reservation.round_ordinal,
           (convert_from(snapshot.payload,'UTF8')::jsonb #>>
               '{planner_budget,max_tool_calls}')::integer,
           call.completion
      INTO planner_renderer,final_round,max_tool_calls,completion_text
      FROM public.task_run_snapshots snapshot
      JOIN public.research_run_llm_spend_reservations reservation
        ON reservation.id=NEW.planner_llm_spend_reservation_id
       AND reservation.tenant_id=NEW.tenant_id
       AND reservation.user_id=NEW.user_id AND reservation.task_id=NEW.task_id
       AND reservation.run_snapshot_id=NEW.run_snapshot_id
       AND reservation.stage='planner' AND reservation.subject_id=0
      JOIN public.research_run_llm_spend_settlements settlement
        ON settlement.reservation_id=reservation.id
       AND settlement.tenant_id=reservation.tenant_id
       AND settlement.user_id=reservation.user_id
       AND settlement.task_id=reservation.task_id
       AND settlement.run_snapshot_id=reservation.run_snapshot_id
       AND settlement.stage=reservation.stage
       AND settlement.round_ordinal=reservation.round_ordinal
      JOIN public.llm_calls call ON call.id=settlement.llm_call_id
       AND call.research_run_llm_spend_reservation_id=reservation.id
       AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
       AND call.run_snapshot_id=NEW.run_snapshot_id
       AND call.span_name='research_planner' AND call.error=''
     WHERE snapshot.id=NEW.run_snapshot_id
       AND snapshot.tenant_id=NEW.tenant_id AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND settlement.attempted AND settlement.usage_known
       AND NOT settlement.definitely_zero_usage
       AND settlement.outcome='completed' AND settlement.error_code='';
    IF completion_text IS NULL OR final_round IS NULL THEN
        RAISE EXCEPTION '126: research Plan lacks exact planner settlement'
            USING ERRCODE='23514';
    END IF;
    IF planner_renderer IS DISTINCT FROM 'research-planner.render/v3.3' THEN
        IF NOT public.research_plan_matches_planner_completion_v1(
                NEW.plan_payload,completion_text) THEN
            RAISE EXCEPTION '104: research Plan differs from its planner response projection'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NOT public.research_plan_matches_planner_completion_v126(
            NEW.plan_payload,completion_text) OR max_tool_calls IS NULL OR
       jsonb_array_length(completion_text::jsonb->'steps') NOT BETWEEN
           CASE WHEN max_tool_calls>=2 THEN 2 ELSE 1 END AND max_tool_calls OR
       NOT EXISTS (
        SELECT 1 FROM public.research_planner_tool_search_receipts receipt
         WHERE receipt.tenant_id=NEW.tenant_id AND receipt.user_id=NEW.user_id
           AND receipt.task_id=NEW.task_id AND receipt.run_snapshot_id=NEW.run_snapshot_id
           AND receipt.round_ordinal<final_round
    ) OR EXISTS (
        SELECT 1 FROM public.research_planner_tool_search_receipts receipt
         WHERE receipt.tenant_id=NEW.tenant_id AND receipt.user_id=NEW.user_id
           AND receipt.task_id=NEW.task_id AND receipt.run_snapshot_id=NEW.run_snapshot_id
           AND receipt.round_ordinal>=final_round
    ) OR EXISTS (
        SELECT 1
          FROM jsonb_array_elements(convert_from(NEW.plan_payload,'UTF8')::jsonb->'steps') step
         WHERE NOT EXISTS (
             SELECT 1
               FROM public.research_planner_tool_search_receipts receipt,
                    jsonb_array_elements(
                        convert_from(receipt.receipt_payload,'UTF8')::jsonb->'matches') match
              WHERE receipt.tenant_id=NEW.tenant_id AND receipt.user_id=NEW.user_id
                AND receipt.task_id=NEW.task_id AND receipt.run_snapshot_id=NEW.run_snapshot_id
                AND receipt.round_ordinal<final_round
                AND match->>'name'=step->>'tool_name'
         )
    ) THEN
        RAISE EXCEPTION '126: research Plan uses a tool without immutable search authority'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_plan_llm_receipt_v126() FROM PUBLIC;
CREATE TRIGGER research_run_plan_llm_receipt_v1
BEFORE INSERT ON research_run_plans
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_plan_llm_receipt_v126();

GRANT SELECT ON research_planner_tool_search_receipts
    TO vane_app,vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,round_ordinal,
    planner_llm_spend_reservation_id,catalog_digest,receipt_payload,
    receipt_digest,schema_version
) ON research_planner_tool_search_receipts TO vane_app,vane_research_v3_executor;
GRANT USAGE,SELECT ON SEQUENCE research_planner_tool_search_receipts_id_seq
    TO vane_app,vane_research_v3_executor;

ALTER TABLE research_planner_tool_search_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON research_planner_tool_search_receipts
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON research_planner_tool_search_receipts AS RESTRICTIVE
    FOR ALL
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint);
CREATE POLICY user_isolation ON research_planner_tool_search_receipts AS RESTRICTIVE
    FOR ALL
    USING (user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY research_v3_scope ON research_planner_tool_search_receipts AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
           user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint)
    WITH CHECK (tenant_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
                user_id IS NOT DISTINCT FROM
                NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint);
CREATE POLICY research_v3_capability_scope
    ON research_planner_tool_search_receipts AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL))
    WITH CHECK (research_run_capability_allows_v1(
        tenant_id,user_id,task_id,run_snapshot_id,NULL));

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE task_run_snapshots,research_run_plans,
           research_planner_tool_search_receipts,
           research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_planner_tool_search_receipts) OR EXISTS (
        SELECT 1 FROM task_run_snapshots snapshot
         WHERE snapshot.reference_schema_version='vane.research-run-snapshot-ref/v3'
           AND convert_from(snapshot.payload,'UTF8')::jsonb #>>
               '{research_model,planner,renderer_version}'=
               'research-planner.render/v3.3'
    ) THEN
        RAISE EXCEPTION '126: v3.3 planner history exists';
    END IF;
END $$;

-- Restore migration 104's retained trigger binding without rewriting its
-- function body.
DROP TRIGGER research_run_plan_llm_receipt_v1 ON research_run_plans;
DROP FUNCTION enforce_research_run_plan_llm_receipt_v126();
CREATE TRIGGER research_run_plan_llm_receipt_v1
BEFORE INSERT ON research_run_plans
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_plan_llm_receipt_v1();

DROP FUNCTION research_plan_matches_planner_completion_v126(BYTEA,TEXT);
DROP TRIGGER protect_research_planner_search_receipt_v126
    ON research_planner_tool_search_receipts;
DROP FUNCTION protect_research_planner_search_receipt_v126();
DROP TRIGGER enforce_research_planner_search_receipt_v126
    ON research_planner_tool_search_receipts;
DROP FUNCTION enforce_research_planner_search_receipt_v126();
DROP FUNCTION research_planner_search_canonical_v126(BYTEA);
DROP TABLE research_planner_tool_search_receipts;
