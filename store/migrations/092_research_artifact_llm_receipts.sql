-- 092: bind V3 Plan and Brief artifacts to immutable completed LLM receipts.
--
-- Existing dark-run rows remain readable with NULL bindings. Every new Plan
-- and every new synthesis spend/finalization must cross the receipt fences
-- below; the runtime cannot label an arbitrary payload as model-produced.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_plans,research_brief_syntheses,
           research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE research_run_plans
    ADD COLUMN planner_llm_spend_reservation_id BIGINT,
    ADD CONSTRAINT fk_research_run_plan_planner_llm_reservation
        FOREIGN KEY (planner_llm_spend_reservation_id)
        REFERENCES research_run_llm_spend_reservations(id)
        DEFERRABLE INITIALLY DEFERRED;
CREATE UNIQUE INDEX uq_research_run_plan_planner_llm_reservation
    ON research_run_plans(planner_llm_spend_reservation_id)
    WHERE planner_llm_spend_reservation_id IS NOT NULL;

ALTER TABLE research_brief_syntheses
    ADD COLUMN synthesis_llm_spend_reservation_id BIGINT,
    ADD CONSTRAINT fk_research_brief_synthesis_llm_reservation
        FOREIGN KEY (synthesis_llm_spend_reservation_id)
        REFERENCES research_run_llm_spend_reservations(id)
        DEFERRABLE INITIALLY DEFERRED;
CREATE UNIQUE INDEX uq_research_brief_synthesis_llm_reservation
    ON research_brief_syntheses(synthesis_llm_spend_reservation_id)
    WHERE synthesis_llm_spend_reservation_id IS NOT NULL;

-- A new Plan is admitted only when the exact same-run planner call completed
-- with known usage. JSONB equality permits harmless provider whitespace while
-- still proving that the typed canonical Plan came from that model response.
-- Existing NULL-bound rows predate this fence and remain read-only.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_plan_llm_receipt_v1()
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
REVOKE ALL ON FUNCTION enforce_research_run_plan_llm_receipt_v1() FROM PUBLIC;
CREATE TRIGGER research_run_plan_llm_receipt_v1
BEFORE INSERT OR UPDATE ON research_run_plans
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_plan_llm_receipt_v1();

-- The synthesis reservation is attached exactly once on prepared->spending.
-- A spending replay must preserve that binding. Finalization additionally
-- requires the completed, known-usage response to equal the final Brief JSON.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_brief_llm_receipt_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.synthesis_llm_spend_reservation_id IS NOT NULL THEN
            RAISE EXCEPTION '092: prepared research Brief cannot pre-bind model spend'
                USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.synthesis_llm_spend_reservation_id IS NULL AND
       NEW.synthesis_llm_spend_reservation_id IS NOT NULL THEN
        IF OLD.status<>'prepared' OR NEW.status<>'spending' OR NOT EXISTS (
            SELECT 1
              FROM public.research_run_llm_spend_reservations reservation
             WHERE reservation.id=NEW.synthesis_llm_spend_reservation_id
               AND reservation.tenant_id=NEW.tenant_id
               AND reservation.user_id=NEW.user_id
               AND reservation.task_id=NEW.task_id
               AND reservation.run_snapshot_id=NEW.run_snapshot_id
               AND reservation.stage='synthesis'
               AND reservation.round_ordinal=0
               AND reservation.subject_id=NEW.id
        ) THEN
            RAISE EXCEPTION '092: research Brief spend binding differs from synthesis subject'
                USING ERRCODE='23514';
        END IF;
    ELSIF NEW.synthesis_llm_spend_reservation_id IS DISTINCT FROM
          OLD.synthesis_llm_spend_reservation_id THEN
        RAISE EXCEPTION '092: research Brief model spend binding is immutable'
            USING ERRCODE='23514';
    END IF;

    IF NEW.status='finalized' AND NOT EXISTS (
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
         WHERE reservation.id=NEW.synthesis_llm_spend_reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.stage='synthesis' AND reservation.round_ordinal=0
           AND reservation.subject_id=NEW.id
           AND settlement.attempted AND settlement.usage_known
           AND NOT settlement.definitely_zero_usage
           AND settlement.outcome='completed' AND settlement.error_code=''
           AND call.research_run_llm_spend_reservation_id=reservation.id
           AND call.tenant_id=NEW.tenant_id AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.span_name='research_synthesis' AND call.error=''
           AND convert_from(NEW.brief_payload,'UTF8')::jsonb=call.completion::jsonb
    ) THEN
        RAISE EXCEPTION '092: final research Brief differs from its completed synthesis receipt'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_brief_llm_receipt_v1() FROM PUBLIC;
CREATE TRIGGER research_brief_llm_receipt_v1
BEFORE INSERT OR UPDATE ON research_brief_syntheses
FOR EACH ROW EXECUTE FUNCTION enforce_research_brief_llm_receipt_v1();

GRANT INSERT (planner_llm_spend_reservation_id)
    ON research_run_plans TO vane_app,vane_research_v3_executor;
GRANT UPDATE (synthesis_llm_spend_reservation_id)
    ON research_brief_syntheses TO vane_app,vane_research_v3_executor;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_plans,research_brief_syntheses,
           research_run_llm_spend_reservations,
           research_run_llm_spend_settlements,llm_calls
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_plans
                WHERE planner_llm_spend_reservation_id IS NOT NULL) OR
       EXISTS (SELECT 1 FROM research_brief_syntheses
                WHERE synthesis_llm_spend_reservation_id IS NOT NULL) THEN
        RAISE EXCEPTION '092: refusing downgrade while receipt-bound artifacts exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE INSERT (planner_llm_spend_reservation_id)
    ON research_run_plans FROM vane_app,vane_research_v3_executor;
REVOKE UPDATE (synthesis_llm_spend_reservation_id)
    ON research_brief_syntheses FROM vane_app,vane_research_v3_executor;
DROP TRIGGER research_brief_llm_receipt_v1 ON research_brief_syntheses;
DROP FUNCTION enforce_research_brief_llm_receipt_v1();
DROP TRIGGER research_run_plan_llm_receipt_v1 ON research_run_plans;
DROP FUNCTION enforce_research_run_plan_llm_receipt_v1();
DROP INDEX uq_research_brief_synthesis_llm_reservation;
ALTER TABLE research_brief_syntheses
    DROP CONSTRAINT fk_research_brief_synthesis_llm_reservation,
    DROP COLUMN synthesis_llm_spend_reservation_id;
DROP INDEX uq_research_run_plan_planner_llm_reservation;
ALTER TABLE research_run_plans
    DROP CONSTRAINT fk_research_run_plan_planner_llm_reservation,
    DROP COLUMN planner_llm_spend_reservation_id;
