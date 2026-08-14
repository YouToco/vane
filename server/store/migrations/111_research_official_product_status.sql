-- 111: admit the first credentialless official Research V3 Tool.
--
-- The durable step/spend ledger remains the first-writer effect gate. Kimi's
-- public GoodsService is free, but consumes a separate DoS quota token so it
-- is never represented as Exa usage, credential or provider cost.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE tenants IN ACCESS EXCLUSIVE MODE;
LOCK TABLE research_run_steps,research_run_step_spend_reservations,
           research_run_step_spend_settlements,tool_calls,tenant_quota
    IN ACCESS EXCLUSIVE MODE;

INSERT INTO tenant_quota (tenant_id,bucket,tokens,rate,burst)
SELECT tenant.id,'official_calls',500.0,500.0/86400,500.0
  FROM tenants tenant
ON CONFLICT (tenant_id,bucket) DO NOTHING;

ALTER TABLE research_run_evidence
    DROP CONSTRAINT ck_research_run_evidence_trust,
    ADD CONSTRAINT ck_research_run_evidence_trust CHECK (
        trust_type IN ('local','external','official')
    );

-- Preserve migration 105's exact formal/shadow authorization and lock order.
-- Only widen its frozen grant tuple from Exa to the one explicit free route.
-- Refuse the migration if the deployed function is not the reviewed version.
-- +goose StatementBegin
DO $$
DECLARE
    definition TEXT;
    needle TEXT := E'IF derived_quota_bucket<>\'exa_calls\' OR\n       derived_max_cost NOT BETWEEN 1 AND 1000000 THEN';
    replacement TEXT := E'IF NOT ((derived_quota_bucket=\'exa_calls\' AND\n             derived_max_cost BETWEEN 1 AND 1000000) OR\n            (derived_quota_bucket=\'official_calls\' AND\n             derived_tool_name=\'web_product_status\' AND\n             derived_max_cost=1)) THEN';
BEGIN
    SELECT pg_get_functiondef(
        'admit_research_run_tool_step_cap_v1(bigint,bigint,integer)'::regprocedure)
      INTO definition;
    IF definition IS NULL OR position(needle IN definition)=0 OR
       position('official_calls' IN definition)>0 THEN
        RAISE EXCEPTION '111: Tool admission function is not the reviewed 105 version';
    END IF;
    EXECUTE replace(definition,needle,replacement);
END
$$;
-- +goose StatementEnd

-- A bound V3 projection normally shares the logical Tool name. The official
-- adapter intentionally preserves the raw upstream operation name instead.
-- +goose StatementBegin
DO $$
DECLARE
    definition TEXT;
    needle TEXT := 'AND reservation.tool_name=NEW.tool_name';
    replacement TEXT := E'AND (reservation.tool_name=NEW.tool_name OR\n                (reservation.tool_name=\'web_product_status\' AND\n                 NEW.tool_name=\'kimi:goods_list\'))';
BEGIN
    SELECT pg_get_functiondef(
        'protect_bound_research_tool_call_v1()'::regprocedure)
      INTO definition;
    IF definition IS NULL OR position(needle IN definition)=0 OR
       position('kimi:goods_list' IN definition)>0 THEN
        RAISE EXCEPTION '111: bound Tool-call guard is not the reviewed version';
    END IF;
    EXECUTE replace(definition,needle,replacement);
END
$$;
-- +goose StatementEnd

-- Settlement still binds the logical step, but verifies the raw Kimi call and
-- its zero calculated USD cost instead of asserting provider=exa.
-- +goose StatementBegin
DO $$
DECLARE
    definition TEXT;
    tool_needle TEXT := 'AND call.tool_name=NEW.tool_name';
    tool_replacement TEXT := E'AND (call.tool_name=NEW.tool_name OR\n                (NEW.tool_name=\'web_product_status\' AND\n                 call.tool_name=\'kimi:goods_list\'))';
    provider_needle TEXT := 'AND call.provider=''exa''';
    provider_replacement TEXT := E'AND call.provider=CASE\n                   WHEN NEW.tool_name=\'web_product_status\' THEN \'kimi\'\n                   ELSE \'exa\' END';
BEGIN
    SELECT pg_get_functiondef(
        'enforce_research_run_step_spend_settlement_v1()'::regprocedure)
      INTO definition;
    IF definition IS NULL OR position(tool_needle IN definition)=0 OR
       position(provider_needle IN definition)=0 OR
       position('kimi:goods_list' IN definition)>0 THEN
        RAISE EXCEPTION '111: spend settlement guard is not the reviewed version';
    END IF;
    definition := replace(definition,tool_needle,tool_replacement);
    definition := replace(definition,provider_needle,provider_replacement);
    EXECUTE definition;
END
$$;
-- +goose StatementEnd

-- +goose Down

-- An admitted official step may need response-loss recovery. Removing its
-- route or quota bucket would reinterpret immutable reservations.
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION
        '111: refusing downgrade after official Research Tool admission';
END
$$;
-- +goose StatementEnd
