-- 096: a Tool admission reservation is the minimum quota charge.
--
-- 090 refunded one exa_calls token when the executor asserted that a Tool call
-- was unattempted. Until Tool provider receipts are independently verified,
-- that assertion is not refund authority. Historical refunds are left as-is;
-- this migration only makes every future settlement non-refundable.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_step_spend_reservations,
           research_run_step_spend_settlements,tenant_quota
    IN ACCESS EXCLUSIVE MODE;

-- Distinguish pre-096 history from settlements written under the permanent
-- reservation-floor rule. The executor has no INSERT privilege on this new
-- column and therefore cannot select the legacy marker value.
ALTER TABLE research_run_step_spend_settlements
    ADD COLUMN quota_floor_policy_version SMALLINT;
UPDATE research_run_step_spend_settlements
   SET quota_floor_policy_version=0;
ALTER TABLE research_run_step_spend_settlements
    ALTER COLUMN quota_floor_policy_version SET DEFAULT 1,
    ALTER COLUMN quota_floor_policy_version SET NOT NULL,
    ADD CONSTRAINT ck_research_step_spend_quota_floor_policy CHECK (
        quota_floor_policy_version IN (0,1)
    );

DROP TRIGGER refund_unattempted_research_quota_v3
    ON research_run_step_spend_settlements;
REVOKE ALL ON FUNCTION refund_unattempted_research_quota_v3() FROM PUBLIC,
    vane_app,vane_research_runtime,vane_research_v3_executor;
DROP FUNCTION refund_unattempted_research_quota_v3();

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_step_spend_reservations,
           research_run_step_spend_settlements,tenant_quota
    IN ACCESS EXCLUSIVE MODE;

-- Reintroducing refund authority after any settlement was sealed under the
-- reservation-floor rule would reinterpret immutable financial evidence.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM research_run_step_spend_settlements
         WHERE quota_floor_policy_version=1
    ) OR EXISTS (
        SELECT 1
          FROM research_run_step_spend_reservations reservation
          LEFT JOIN research_run_step_spend_settlements settlement
            ON settlement.reservation_id=reservation.id
         WHERE settlement.reservation_id IS NULL
    ) THEN
        RAISE EXCEPTION '096: refusing downgrade while quota-floor or unsettled reservations exist';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION refund_unattempted_research_quota_v3()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    affected BIGINT;
BEGIN
    UPDATE public.tenant_quota quota
       SET tokens=LEAST(
               quota.burst,
               quota.tokens + quota.rate * EXTRACT(EPOCH FROM (now() - quota.updated_at)) +
                   reservation.reserved_quota_units
           ),
           updated_at=now()
      FROM public.research_run_step_spend_reservations reservation
     WHERE reservation.id=NEW.reservation_id
       AND reservation.tenant_id=NEW.tenant_id
       AND reservation.quota_bucket='exa_calls'
       AND reservation.reserved_quota_units=1
       AND quota.tenant_id=NEW.tenant_id
       AND quota.bucket=reservation.quota_bucket;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected<>1 THEN
        RAISE EXCEPTION '090: unattempted quota refund has no exact reservation'
            USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION refund_unattempted_research_quota_v3() FROM PUBLIC;
CREATE TRIGGER refund_unattempted_research_quota_v3
AFTER INSERT ON research_run_step_spend_settlements
FOR EACH ROW WHEN (NEW.tool_call_id IS NULL AND NEW.actual_quota_units=0)
EXECUTE FUNCTION refund_unattempted_research_quota_v3();

ALTER TABLE research_run_step_spend_settlements
    DROP CONSTRAINT ck_research_step_spend_quota_floor_policy,
    DROP COLUMN quota_floor_policy_version;
