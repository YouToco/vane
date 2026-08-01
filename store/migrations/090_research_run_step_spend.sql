-- 090: response-loss-safe spend authority for every V3 research Tool step.
--
-- A started step is the point after which Tool arguments may leave the Store.
-- From this migration onward it may exist only with one exact immutable spend
-- reservation in the same transaction. Settlement is also append-only and is
-- committed with the exact terminal step; neither retry nor a mutable
-- observability row can manufacture a second debit or erase historical spend.

-- +goose Up

-- Serialize the dark-schema assertion with every V3 step writer. V3 has not
-- been cut over, so an existing start is an integrity failure: guessing a
-- historical reservation would be less safe than refusing the migration.
SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_steps,tool_calls IN ACCESS EXCLUSIVE MODE;

-- V3 transactions authenticate through a dedicated LOGIN principal, then SET
-- one NOLOGIN capability role.  The login itself has no table privileges and
-- the capability is deliberately not a member of the legacy broad vane_app
-- role. RESET ROLE therefore returns to an inert principal rather than a
-- schema owner or a general application writer.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_research_runtime') THEN
        BEGIN
            CREATE ROLE vane_research_runtime NOLOGIN NOSUPERUSER NOCREATEDB
                NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
        EXCEPTION
            -- Roles are cluster-global while each scratch database migrates
            -- independently. Close the first-use race across parallel DBs.
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    ALTER ROLE vane_research_runtime NOSUPERUSER NOCREATEDB NOCREATEROLE
        NOINHERIT NOREPLICATION NOBYPASSRLS;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='vane_research_v3_executor') THEN
        BEGIN
            CREATE ROLE vane_research_v3_executor NOLOGIN NOSUPERUSER NOCREATEDB
                NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
    ALTER ROLE vane_research_v3_executor NOLOGIN NOSUPERUSER NOCREATEDB
        NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    -- A prior rollout may have granted the broad role cluster-wide.  Revoke it
    -- explicitly because roles outlive individual databases and migrations.
    REVOKE vane_app FROM vane_research_runtime;
    REVOKE vane_app FROM vane_research_v3_executor;
    GRANT vane_research_v3_executor TO vane_research_runtime;
END
$$;
-- +goose StatementEnd
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_steps) THEN
        RAISE EXCEPTION
            '090: refusing migration while pre-ledger V3 steps exist';
    END IF;
END
$$;
-- +goose StatementEnd

-- Paid V3 effects run under the dedicated executor so the new ledgers remain
-- protected by tenant/user RLS. Give that role one deliberately tiny financial capability:
-- consume exactly one Exa invocation from the tenant already pinned in the
-- transaction scope. It never receives direct UPDATE on tenant_quota.
ALTER TABLE tenant_quota
    ADD CONSTRAINT ck_tenant_quota_finite_v3 CHECK (
        tokens > '-Infinity'::float8 AND tokens < 'Infinity'::float8 AND
        rate >= 0 AND rate < 'Infinity'::float8 AND
        burst > 0 AND burst < 'Infinity'::float8
    );

-- +goose StatementBegin
CREATE FUNCTION reserve_research_run_quota_v3(
    requested_tenant_id BIGINT,
    requested_bucket TEXT,
    requested_units DOUBLE PRECISION
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF requested_tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       NULLIF(current_setting('app.user_id',true),'') IS NULL OR
       requested_bucket<>'exa_calls' OR requested_units<>1 OR
       requested_units<='-Infinity'::float8 OR
       requested_units>='Infinity'::float8 THEN
        RETURN FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM public.tenants tenant
          JOIN public.memberships membership
            ON membership.tenant_id=tenant.id
           AND membership.user_id=
               NULLIF(current_setting('app.user_id',true),'')::bigint
         WHERE tenant.id=requested_tenant_id
           AND tenant.status='active' AND tenant.deleted_at IS NULL
    ) THEN
        RETURN FALSE;
    END IF;

    UPDATE public.tenant_quota
       SET tokens=LEAST(
               burst,
               tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))
           ) - requested_units,
           updated_at=now()
     WHERE tenant_id=requested_tenant_id
       AND bucket=requested_bucket
       AND LEAST(
               burst,
               tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))
           ) >= requested_units;
    RETURN FOUND;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reserve_research_run_quota_v3(BIGINT,TEXT,DOUBLE PRECISION)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION reserve_research_run_quota_v3(BIGINT,TEXT,DOUBLE PRECISION)
    TO vane_research_v3_executor;

CREATE TABLE research_run_step_spend_reservations (
    id                       BIGSERIAL   PRIMARY KEY,
    tenant_id                BIGINT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id                  BIGINT      NOT NULL REFERENCES users(id),
    task_id                  TEXT        NOT NULL,
    run_snapshot_id          BIGINT      NOT NULL REFERENCES task_run_snapshots(id),
    plan_id                  BIGINT      NOT NULL REFERENCES research_run_plans(id),
    started_step_id          BIGINT      NOT NULL REFERENCES research_run_steps(id) ON DELETE CASCADE,
    temporal_run_id          TEXT        NOT NULL,
    plan_digest              TEXT        NOT NULL,
    step_ordinal             INTEGER     NOT NULL,
    invocation_id            TEXT        NOT NULL,
    tool_name                TEXT        NOT NULL,
    request_digest           TEXT        NOT NULL,
    tool_policy_digest       TEXT        NOT NULL,
    quota_bucket             TEXT        NOT NULL,
    reserved_quota_units     NUMERIC(18,6) NOT NULL,
    reserved_cost_micro_usd  BIGINT      NOT NULL,
    schema_version           TEXT        NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_step_spend_reservation_started UNIQUE (started_step_id),
    CONSTRAINT uq_research_step_spend_reservation_ordinal
        UNIQUE (tenant_id,user_id,temporal_run_id,plan_digest,step_ordinal),
    CONSTRAINT ck_research_step_spend_reservation_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512 AND
        btrim(invocation_id)=invocation_id AND
        octet_length(invocation_id) BETWEEN 1 AND 255 AND
        btrim(tool_name)=tool_name AND octet_length(tool_name) BETWEEN 1 AND 255 AND
        btrim(quota_bucket)=quota_bucket AND
        quota_bucket ~ '^[a-z][a-z0-9_]{0,63}$' AND
        step_ordinal BETWEEN 0 AND 15
    ),
    CONSTRAINT ck_research_step_spend_reservation_digests CHECK (
        plan_digest ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$' AND
        tool_policy_digest ~ '^[0-9a-f]{64}$'
    ),
    -- Current V3 grants only request-metered Exa Tools. The frozen Tool policy
    -- trigger below proves the bucket and maximum cost; one admitted external
    -- request consumes exactly one call token.
    CONSTRAINT ck_research_step_spend_reservation_units CHECK (
        reserved_quota_units=1.000000
    ),
    CONSTRAINT ck_research_step_spend_reservation_cost CHECK (
        reserved_cost_micro_usd BETWEEN 1 AND 1000000
    ),
    CONSTRAINT ck_research_step_spend_reservation_schema CHECK (
        schema_version='vane.research-run-step-spend-reservation/v1'
    )
);

CREATE INDEX idx_research_step_spend_reservation_scope
    ON research_run_step_spend_reservations
       (tenant_id,user_id,task_id,run_snapshot_id,step_ordinal,id);
CREATE INDEX idx_research_step_spend_reservation_plan
    ON research_run_step_spend_reservations
       (tenant_id,user_id,plan_id,step_ordinal,id);

-- Bind a reservation to the exact immutable snapshot, plan, start and frozen
-- Tool grant. The caller cannot choose a cheaper maximum or another bucket.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_step_spend_reservation_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    snapshot_json JSONB;
BEGIN
    SELECT convert_from(snapshot.payload,'UTF8')::jsonb
      INTO snapshot_json
      FROM public.research_run_steps started
      JOIN public.research_run_plans plan
        ON plan.id=started.plan_id
      JOIN public.task_run_snapshots snapshot
        ON snapshot.id=plan.run_snapshot_id
     WHERE started.id=NEW.started_step_id
       AND started.phase='started'
       AND started.tenant_id=NEW.tenant_id
       AND started.user_id=NEW.user_id
       AND started.task_id=NEW.task_id
       AND started.plan_id=NEW.plan_id
       AND started.temporal_run_id=NEW.temporal_run_id
       AND started.plan_digest=NEW.plan_digest
       AND started.step_ordinal=NEW.step_ordinal
       AND started.invocation_id=NEW.invocation_id
       AND started.tool_name=NEW.tool_name
       AND started.request_digest=NEW.request_digest
       AND plan.tenant_id=NEW.tenant_id
       AND plan.user_id=NEW.user_id
       AND plan.task_id=NEW.task_id
       AND plan.run_snapshot_id=NEW.run_snapshot_id
       AND plan.temporal_run_id=NEW.temporal_run_id
       AND plan.plan_digest=NEW.plan_digest
       AND snapshot.tenant_id=NEW.tenant_id
       AND snapshot.user_id=NEW.user_id
       AND snapshot.task_id=NEW.task_id
       AND snapshot.temporal_run_id=NEW.temporal_run_id
       AND snapshot.reference_schema_version=
           'vane.research-run-snapshot-ref/v3'
       AND snapshot.tool_policy_digest=NEW.tool_policy_digest;

    IF snapshot_json IS NULL OR NOT EXISTS (
        SELECT 1
          FROM jsonb_array_elements(
                   snapshot_json #> '{research_tools,allowed_tools}'
               ) AS grant_row(value)
         WHERE grant_row.value->>'name'=NEW.tool_name
           AND grant_row.value->>'budget_bucket'=NEW.quota_bucket
           AND (grant_row.value->>'max_cost_micro_usd')::bigint=
               NEW.reserved_cost_micro_usd
    ) THEN
        RAISE EXCEPTION
            '090: spend reservation does not match exact V3 step and Tool policy'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_step_spend_reservation_v1()
    FROM PUBLIC;
CREATE TRIGGER research_run_step_spend_reservation_v1
BEFORE INSERT ON research_run_step_spend_reservations
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_step_spend_reservation_v1();

-- The start is inserted first and the reservation second. This deferred fence
-- proves at COMMIT that no caller can strand spend authority without its debit.
-- +goose StatementBegin
CREATE FUNCTION require_research_run_step_spend_reservation_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.research_run_step_spend_reservations reservation
         WHERE reservation.started_step_id=NEW.id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.plan_id=NEW.plan_id
           AND reservation.temporal_run_id=NEW.temporal_run_id
           AND reservation.plan_digest=NEW.plan_digest
           AND reservation.step_ordinal=NEW.step_ordinal
           AND reservation.invocation_id=NEW.invocation_id
           AND reservation.tool_name=NEW.tool_name
           AND reservation.request_digest=NEW.request_digest
    ) THEN
        RAISE EXCEPTION
            '090: V3 started step has no exact spend reservation'
            USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION require_research_run_step_spend_reservation_v1()
    FROM PUBLIC;
CREATE CONSTRAINT TRIGGER research_run_step_spend_reservation_required_v1
AFTER INSERT ON research_run_steps
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.phase='started')
EXECUTE FUNCTION require_research_run_step_spend_reservation_v1();

-- A tool_call remains the dashboard/pricing projection, but a V3-bound row is
-- now an immutable, one-to-one projection of the canonical reservation.
ALTER TABLE tool_calls
    ADD COLUMN research_run_step_spend_reservation_id BIGINT,
    ADD CONSTRAINT fk_tool_calls_research_step_spend_reservation
        FOREIGN KEY (research_run_step_spend_reservation_id)
        REFERENCES research_run_step_spend_reservations(id),
    ADD CONSTRAINT ck_tool_calls_research_step_spend_scope CHECK (
        research_run_step_spend_reservation_id IS NULL OR
        (tenant_id IS NOT NULL AND user_id IS NOT NULL AND
         run_snapshot_id IS NOT NULL)
    );
CREATE UNIQUE INDEX uq_tool_calls_research_step_spend_reservation
    ON tool_calls(research_run_step_spend_reservation_id)
    WHERE research_run_step_spend_reservation_id IS NOT NULL;

-- Bound provider evidence may only disappear as part of deleting its tenant
-- root.  The old NO ACTION tenant FK forced PurgeTenant to issue a direct
-- DELETE against tool_calls, which in turn required trusting the connection's
-- schema-owner identity.  A schema-owner session can always recover that
-- identity with RESET ROLE, so it is not a durable authorization boundary.
-- The cascade makes tenant erasure the structural delete authority instead.
ALTER TABLE tool_calls DROP CONSTRAINT fk_tool_calls_tenant;
ALTER TABLE tool_calls
    ADD CONSTRAINT fk_tool_calls_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE;

-- +goose StatementBegin
CREATE FUNCTION protect_bound_research_tool_call_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.research_run_step_spend_reservation_id IS NOT NULL THEN
            -- A real FK cascade invokes this row trigger below the tenant FK
            -- trigger, and the parent row is already invisible to this command.
            -- A direct DELETE (including after RESET ROLE) has depth one and a
            -- live parent, so neither a role nor a caller-set GUC can authorize
            -- selective evidence deletion.
            IF pg_trigger_depth()<=1 OR OLD.tenant_id IS NULL OR EXISTS (
                SELECT 1 FROM public.tenants tenant WHERE tenant.id=OLD.tenant_id
            ) THEN
                RAISE EXCEPTION '090: V3-bound tool call is immutable'
                    USING ERRCODE='42501';
            END IF;
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' THEN
        IF OLD.research_run_step_spend_reservation_id IS NOT NULL OR
           NEW.research_run_step_spend_reservation_id IS NOT NULL THEN
            RAISE EXCEPTION '090: V3 Tool binding is insert-only and immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.research_run_step_spend_reservation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM public.research_run_step_spend_reservations reservation
         WHERE reservation.id=NEW.research_run_step_spend_reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.tool_name=NEW.tool_name
    ) THEN
        RAISE EXCEPTION '090: tool call does not match exact V3 spend reservation'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION protect_bound_research_tool_call_v1() FROM PUBLIC;
CREATE TRIGGER protect_bound_research_tool_call_v1
BEFORE INSERT OR UPDATE OR DELETE ON tool_calls
FOR EACH ROW EXECUTE FUNCTION protect_bound_research_tool_call_v1();

CREATE TABLE research_run_step_spend_settlements (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id               BIGINT      NOT NULL REFERENCES users(id),
    task_id               TEXT        NOT NULL,
    run_snapshot_id       BIGINT      NOT NULL REFERENCES task_run_snapshots(id),
    plan_id               BIGINT      NOT NULL REFERENCES research_run_plans(id),
    reservation_id        BIGINT      NOT NULL REFERENCES research_run_step_spend_reservations(id) ON DELETE CASCADE,
    terminal_step_id      BIGINT      NOT NULL REFERENCES research_run_steps(id) ON DELETE CASCADE,
    tool_call_id          BIGINT      REFERENCES tool_calls(id) ON DELETE CASCADE,
    temporal_run_id       TEXT        NOT NULL,
    plan_digest           TEXT        NOT NULL,
    step_ordinal          INTEGER     NOT NULL,
    invocation_id         TEXT        NOT NULL,
    tool_name             TEXT        NOT NULL,
    request_digest        TEXT        NOT NULL,
    outcome               TEXT        NOT NULL,
    actual_quota_units    NUMERIC(18,6) NOT NULL,
    actual_cost_micro_usd BIGINT      NOT NULL,
    pricing_status        TEXT        NOT NULL,
    cost_currency         TEXT        NOT NULL,
    schema_version        TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_research_step_spend_settlement_reservation UNIQUE (reservation_id),
    CONSTRAINT uq_research_step_spend_settlement_terminal UNIQUE (terminal_step_id),
    CONSTRAINT uq_research_step_spend_settlement_tool_call UNIQUE (tool_call_id),
    CONSTRAINT uq_research_step_spend_settlement_ordinal
        UNIQUE (tenant_id,user_id,temporal_run_id,plan_digest,step_ordinal),
    CONSTRAINT ck_research_step_spend_settlement_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        btrim(temporal_run_id)=temporal_run_id AND
        octet_length(temporal_run_id) BETWEEN 1 AND 512 AND
        btrim(invocation_id)=invocation_id AND
        octet_length(invocation_id) BETWEEN 1 AND 255 AND
        btrim(tool_name)=tool_name AND octet_length(tool_name) BETWEEN 1 AND 255 AND
        step_ordinal BETWEEN 0 AND 15
    ),
    CONSTRAINT ck_research_step_spend_settlement_digests CHECK (
        plan_digest ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_research_step_spend_settlement_outcome CHECK (
        (outcome='completed' AND tool_call_id IS NOT NULL AND
         pricing_status IN ('provider_reported','calculated')) OR
        (outcome='failed' AND tool_call_id IS NULL AND
         actual_cost_micro_usd=0 AND pricing_status='unpriced') OR
        (outcome IN ('failed','indeterminate') AND tool_call_id IS NOT NULL AND
         pricing_status IN ('provider_reported','calculated','estimated'))
    ),
    CONSTRAINT ck_research_step_spend_settlement_usage CHECK (
        actual_quota_units IN (0.000000,1.000000) AND
        actual_cost_micro_usd BETWEEN 0 AND 9223372036854775807
    ),
    CONSTRAINT ck_research_step_spend_settlement_pricing CHECK (
        pricing_status IN ('provider_reported','calculated','estimated','unpriced') AND
        cost_currency='USD'
    ),
    CONSTRAINT ck_research_step_spend_settlement_schema CHECK (
        schema_version='vane.research-run-step-spend-settlement/v1'
    )
);

CREATE INDEX idx_research_step_spend_settlement_scope
    ON research_run_step_spend_settlements
       (tenant_id,user_id,task_id,run_snapshot_id,step_ordinal,id);

-- Settlement copies the reservation identity exactly, binds the exact terminal
-- receipt and, on success, the exact immutable pricing projection.
-- +goose StatementBegin
CREATE FUNCTION enforce_research_run_step_spend_settlement_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.research_run_step_spend_reservations reservation
          JOIN public.research_run_steps terminal
            ON terminal.id=NEW.terminal_step_id
         WHERE reservation.id=NEW.reservation_id
           AND reservation.tenant_id=NEW.tenant_id
           AND reservation.user_id=NEW.user_id
           AND reservation.task_id=NEW.task_id
           AND reservation.run_snapshot_id=NEW.run_snapshot_id
           AND reservation.plan_id=NEW.plan_id
           AND reservation.temporal_run_id=NEW.temporal_run_id
           AND reservation.plan_digest=NEW.plan_digest
           AND reservation.step_ordinal=NEW.step_ordinal
           AND reservation.invocation_id=NEW.invocation_id
           AND reservation.tool_name=NEW.tool_name
           AND reservation.request_digest=NEW.request_digest
           AND (
               (NEW.tool_call_id IS NULL AND NEW.outcome='failed' AND
                NEW.actual_quota_units=0) OR
               (NEW.tool_call_id IS NOT NULL AND
                reservation.reserved_quota_units=NEW.actual_quota_units)
           )
           AND (
               NEW.pricing_status<>'estimated' OR
               NEW.actual_cost_micro_usd=reservation.reserved_cost_micro_usd
           )
           AND (
               NEW.outcome<>'completed' OR
               NEW.actual_cost_micro_usd<=reservation.reserved_cost_micro_usd
           )
           AND terminal.tenant_id=NEW.tenant_id
           AND terminal.user_id=NEW.user_id
           AND terminal.task_id=NEW.task_id
           AND terminal.plan_id=NEW.plan_id
           AND terminal.temporal_run_id=NEW.temporal_run_id
           AND terminal.plan_digest=NEW.plan_digest
           AND terminal.step_ordinal=NEW.step_ordinal
           AND terminal.invocation_id=NEW.invocation_id
           AND terminal.tool_name=NEW.tool_name
           AND terminal.request_digest=NEW.request_digest
           AND terminal.phase=NEW.outcome
           AND terminal.cost_micro_usd=NEW.actual_cost_micro_usd
    ) OR (NEW.tool_call_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.tool_calls call
         WHERE call.id=NEW.tool_call_id
           AND call.research_run_step_spend_reservation_id=NEW.reservation_id
           AND call.tenant_id=NEW.tenant_id
           AND call.user_id=NEW.user_id
           AND call.run_snapshot_id=NEW.run_snapshot_id
           AND call.tool_name=NEW.tool_name
           AND call.provider='exa'
           AND (
               (NEW.outcome='completed' AND
                call.http_status BETWEEN 200 AND 299 AND
                call.usage_quantity>0 AND call.error_type='') OR
               (NEW.outcome IN ('failed','indeterminate') AND
                (call.http_status IS NULL OR call.http_status BETWEEN 100 AND 599) AND
                call.usage_quantity>=0 AND call.error_type='provider_error')
           )
           AND (
               (NEW.pricing_status='estimated' AND
                call.pricing_status='unpriced' AND
                call.cost_currency IS NULL AND call.cost_amount IS NULL AND
                call.cost_usd IS NULL) OR
               (NEW.pricing_status IN ('provider_reported','calculated') AND
                call.pricing_status=NEW.pricing_status AND
                call.cost_currency=NEW.cost_currency AND
                (call.cost_amount*1000000)::bigint=NEW.actual_cost_micro_usd AND
                (call.cost_usd*1000000)::bigint=NEW.actual_cost_micro_usd)
           )
    )) THEN
        RAISE EXCEPTION
            '090: spend settlement does not match exact reservation, terminal and Tool call'
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_research_run_step_spend_settlement_v1()
    FROM PUBLIC;
CREATE TRIGGER research_run_step_spend_settlement_v1
BEFORE INSERT ON research_run_step_spend_settlements
FOR EACH ROW EXECUTE FUNCTION enforce_research_run_step_spend_settlement_v1();

-- An explicitly unattempted provider call releases its admission token. The
-- immutable settlement is the sole authority for that compensation: this
-- avoids application-side RESET ROLE and makes duplicate refunds impossible
-- because reservation_id is unique in the settlement ledger.
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

-- Terminal receipts and settlement facts are one commit. Existing V3 terminal
-- uniqueness makes the settlement idempotency key stable.
-- +goose StatementBegin
CREATE FUNCTION require_research_run_step_spend_settlement_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM public.research_run_step_spend_settlements settlement
         WHERE settlement.terminal_step_id=NEW.id
           AND settlement.tenant_id=NEW.tenant_id
           AND settlement.user_id=NEW.user_id
           AND settlement.task_id=NEW.task_id
           AND settlement.plan_id=NEW.plan_id
           AND settlement.temporal_run_id=NEW.temporal_run_id
           AND settlement.plan_digest=NEW.plan_digest
           AND settlement.step_ordinal=NEW.step_ordinal
           AND settlement.invocation_id=NEW.invocation_id
           AND settlement.tool_name=NEW.tool_name
           AND settlement.request_digest=NEW.request_digest
           AND settlement.outcome=NEW.phase
           AND settlement.actual_cost_micro_usd=NEW.cost_micro_usd
    ) THEN
        RAISE EXCEPTION
            '090: V3 terminal step has no exact spend settlement'
            USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION require_research_run_step_spend_settlement_v1()
    FROM PUBLIC;
CREATE CONSTRAINT TRIGGER research_run_step_spend_settlement_required_v1
AFTER INSERT ON research_run_steps
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.phase IN ('completed','failed','indeterminate'))
EXECUTE FUNCTION require_research_run_step_spend_settlement_v1();

-- A generated lock column is the only UPDATE capability on schedules.  It
-- exists solely because PostgreSQL requires UPDATE privilege for SELECT ...
-- FOR SHARE; supplied values cannot change business state.
ALTER TABLE schedules ADD COLUMN research_v3_lock_capability BOOLEAN
    GENERATED ALWAYS AS (true) VIRTUAL;

GRANT USAGE ON SCHEMA public TO vane_research_v3_executor;
GRANT SELECT (id,status,deleted_at) ON tenants TO vane_research_v3_executor;
GRANT SELECT (tenant_id,user_id) ON memberships TO vane_research_v3_executor;
GRANT UPDATE (definition_edit_lock_capability)
    ON tenants,memberships TO vane_research_v3_executor;
GRANT SELECT (
    id,tenant_id,user_id,spec_json,status,execution_mode,
    approved_definition_version,approved_definition_digest
) ON schedules TO vane_research_v3_executor;
GRANT UPDATE (research_v3_lock_capability) ON schedules TO vane_research_v3_executor;
GRANT SELECT (
    tenant_id,user_id,task_id,version,schema_version,execution_mode,
    definition_digest,payload
) ON task_approved_definition_versions TO vane_research_v3_executor;
GRANT SELECT (
    id,tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
    run_kind,execution_mode,adaptive_version,capability_catalog_digest,
    tool_policy_digest,prompt_policy_digest,model_policy_digest,
    quota_policy_digest,definition_digest,plan_digest,payload_digest,
    reference_digest,reference_schema_version,payload,budget,
    v2_cutover_event_id,created_at
) ON task_run_snapshots TO vane_research_v3_executor;
GRANT INSERT (
    id,tenant_id,user_id,task_id,temporal_workflow_id,temporal_run_id,
    run_kind,execution_mode,adaptive_version,capability_catalog_digest,
    tool_policy_digest,prompt_policy_digest,model_policy_digest,
    quota_policy_digest,definition_digest,plan_digest,payload_digest,
    reference_digest,reference_schema_version,payload,budget,created_at
) ON task_run_snapshots TO vane_research_v3_executor;
GRANT SELECT ON research_run_plans,research_run_steps,research_run_evidence,
                research_brief_syntheses,research_run_step_spend_reservations,
                research_run_step_spend_settlements,tool_calls
    TO vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,temporal_workflow_id,
    temporal_run_id,definition_digest,capability_catalog_digest,plan_digest,
    plan_payload,schema_version
) ON research_run_plans TO vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
    step_ordinal,phase,invocation_id,tool_name,request_digest,result_digest,
    cost_micro_usd,error_code,schema_version
) ON research_run_steps TO vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,plan_id,started_step_id,temporal_run_id,
    plan_digest,step_ordinal,invocation_id,tool_name,request_digest,
    result_bytes,result_digest,original_size,truncated,trust_type,schema_version
) ON research_run_evidence TO vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,
    temporal_workflow_id,temporal_run_id,definition_digest,plan_digest,
    notification_threshold,request_digest,context_payload,context_digest,
    evidence_manifest,evidence_digest,history_manifest,history_digest,schema_version
) ON research_brief_syntheses TO vane_research_v3_executor;
GRANT UPDATE (
    status,significance,decision,delivery_required,
    brief_payload,brief_digest,failure_code
) ON research_brief_syntheses TO vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
    temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
    request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
    reserved_cost_micro_usd,schema_version
) ON research_run_step_spend_reservations TO vane_research_v3_executor;
GRANT INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,plan_id,reservation_id,
    terminal_step_id,tool_call_id,temporal_run_id,plan_digest,step_ordinal,
    invocation_id,tool_name,request_digest,outcome,actual_quota_units,
    actual_cost_micro_usd,pricing_status,cost_currency,schema_version
) ON research_run_step_spend_settlements TO vane_research_v3_executor;
GRANT INSERT (
    trace_id,user_id,session_id,tool_name,tool_kind,endpoint_path,
    arguments,result_preview,result_size,http_status,error_type,error,
    duration_ms,retrieval_query,candidate_tools,cost_usd,source_id,
    tenant_id,provider,usage_quantity,pricing_rule_id,pricing_status,
    cost_amount,cost_currency,run_snapshot_id,
    research_run_step_spend_reservation_id
) ON tool_calls TO vane_research_v3_executor;
GRANT USAGE,SELECT ON SEQUENCE
    task_run_snapshots_id_seq,research_run_plans_id_seq,
    research_run_steps_id_seq,research_run_evidence_id_seq,
    research_brief_syntheses_id_seq,tool_calls_id_seq,
    research_run_step_spend_reservations_id_seq,
    research_run_step_spend_settlements_id_seq TO vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION authorize_manual_task_run_v1(BIGINT,BIGINT,TEXT,TEXT),
    read_research_history_v3(BIGINT,BIGINT,TEXT,BIGINT,BIGINT),
    read_research_history_content_v3(BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER)
    TO vane_research_v3_executor;

ALTER TABLE research_run_step_spend_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_run_step_spend_settlements ENABLE ROW LEVEL SECURITY;
-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'research_run_step_spend_reservations',
        'research_run_step_spend_settlements'
    ] LOOP
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

-- Existing policies remain for legacy roles. These role-specific RESTRICTIVE
-- policies are an additional fail-closed boundary for the V3 capability.
CREATE POLICY research_v3_scope ON tenants AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (
        id IS NOT DISTINCT FROM NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND EXISTS (
            SELECT 1 FROM memberships membership
             WHERE membership.tenant_id=id
               AND membership.user_id IS NOT DISTINCT FROM
                   NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
        )
    ) WITH CHECK (false);
CREATE POLICY research_v3_scope ON memberships AS RESTRICTIVE
    FOR ALL TO vane_research_v3_executor
    USING (
        tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
        AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    ) WITH CHECK (false);
-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'schedules','task_approved_definition_versions','task_run_snapshots',
        'research_run_plans','research_run_steps','research_run_evidence',
        'research_brief_syntheses','research_run_step_spend_reservations',
        'research_run_step_spend_settlements','tool_calls'
    ] LOOP
        EXECUTE format(
            'CREATE POLICY research_v3_scope ON %I AS RESTRICTIVE FOR ALL TO vane_research_v3_executor '
            'USING (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint '
            'AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.tenant_id'',true)),'''')::bigint '
            'AND user_id IS NOT DISTINCT FROM NULLIF((SELECT current_setting(''app.user_id'',true)),'''')::bigint)',
            table_name);
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE research_run_step_spend_settlements,tool_calls,
           research_run_step_spend_reservations,research_run_steps
    IN ACCESS EXCLUSIVE MODE;

-- Financial authority and settlement evidence are non-regenerable. A normal
-- downgrade may remove the schema only while it has never admitted a call.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM research_run_step_spend_settlements) OR
       EXISTS (SELECT 1 FROM research_run_step_spend_reservations) OR
       EXISTS (
           SELECT 1 FROM tool_calls
            WHERE research_run_step_spend_reservation_id IS NOT NULL
       ) THEN
        RAISE EXCEPTION
            '090: refusing downgrade while V3 spend authority or evidence exists';
    END IF;
END
$$;
-- +goose StatementEnd

-- Role membership is cluster-global and another database in this PostgreSQL
-- cluster may still run 090. Down therefore leaves the constrained membership
-- in place, matching the established restricted-role migration convention.

DROP POLICY research_v3_scope ON tenants;
DROP POLICY research_v3_scope ON memberships;
DROP POLICY research_v3_scope ON schedules;
DROP POLICY research_v3_scope ON task_approved_definition_versions;
DROP POLICY research_v3_scope ON task_run_snapshots;
DROP POLICY research_v3_scope ON research_run_plans;
DROP POLICY research_v3_scope ON research_run_steps;
DROP POLICY research_v3_scope ON research_run_evidence;
DROP POLICY research_v3_scope ON research_brief_syntheses;
DROP POLICY research_v3_scope ON research_run_step_spend_reservations;
DROP POLICY research_v3_scope ON research_run_step_spend_settlements;
DROP POLICY research_v3_scope ON tool_calls;
REVOKE ALL ON FUNCTION authorize_manual_task_run_v1(BIGINT,BIGINT,TEXT,TEXT),
    read_research_history_v3(BIGINT,BIGINT,TEXT,BIGINT,BIGINT),
    read_research_history_content_v3(BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,INTEGER,INTEGER)
    FROM vane_research_v3_executor;
REVOKE ALL ON SEQUENCE
    task_run_snapshots_id_seq,research_run_plans_id_seq,
    research_run_steps_id_seq,research_run_evidence_id_seq,
    research_brief_syntheses_id_seq,tool_calls_id_seq,
    research_run_step_spend_reservations_id_seq,
    research_run_step_spend_settlements_id_seq FROM vane_research_v3_executor;
REVOKE ALL ON task_run_snapshots,research_run_plans,research_run_steps,
    research_run_evidence,research_brief_syntheses,
    research_run_step_spend_reservations,research_run_step_spend_settlements,
    tool_calls,task_approved_definition_versions,schedules,memberships,tenants
    FROM vane_research_v3_executor;
REVOKE UPDATE (definition_edit_lock_capability)
    ON tenants,memberships FROM vane_research_v3_executor;
ALTER TABLE schedules DROP COLUMN research_v3_lock_capability;
REVOKE USAGE ON SCHEMA public FROM vane_research_v3_executor;

REVOKE ALL ON FUNCTION reserve_research_run_quota_v3(BIGINT,TEXT,DOUBLE PRECISION)
    FROM vane_research_v3_executor;
DROP FUNCTION reserve_research_run_quota_v3(BIGINT,TEXT,DOUBLE PRECISION);
ALTER TABLE tenant_quota DROP CONSTRAINT ck_tenant_quota_finite_v3;

DROP TRIGGER research_run_step_spend_settlement_required_v1
    ON research_run_steps;
DROP FUNCTION require_research_run_step_spend_settlement_v1();
DROP TRIGGER refund_unattempted_research_quota_v3
    ON research_run_step_spend_settlements;
DROP FUNCTION refund_unattempted_research_quota_v3();
DROP TRIGGER research_run_step_spend_settlement_v1
    ON research_run_step_spend_settlements;
DROP FUNCTION enforce_research_run_step_spend_settlement_v1();
REVOKE ALL ON SEQUENCE research_run_step_spend_settlements_id_seq
    FROM vane_app;
REVOKE ALL ON research_run_step_spend_settlements FROM vane_app;
DROP TABLE research_run_step_spend_settlements;

DROP TRIGGER protect_bound_research_tool_call_v1 ON tool_calls;
DROP FUNCTION protect_bound_research_tool_call_v1();
DROP INDEX uq_tool_calls_research_step_spend_reservation;
ALTER TABLE tool_calls
    DROP CONSTRAINT fk_tool_calls_research_step_spend_reservation,
    DROP CONSTRAINT ck_tool_calls_research_step_spend_scope,
    DROP COLUMN research_run_step_spend_reservation_id;
ALTER TABLE tool_calls DROP CONSTRAINT fk_tool_calls_tenant;
ALTER TABLE tool_calls
    ADD CONSTRAINT fk_tool_calls_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id);

DROP TRIGGER research_run_step_spend_reservation_required_v1
    ON research_run_steps;
DROP FUNCTION require_research_run_step_spend_reservation_v1();
DROP TRIGGER research_run_step_spend_reservation_v1
    ON research_run_step_spend_reservations;
DROP FUNCTION enforce_research_run_step_spend_reservation_v1();
REVOKE ALL ON SEQUENCE research_run_step_spend_reservations_id_seq
    FROM vane_app;
REVOKE ALL ON research_run_step_spend_reservations FROM vane_app;
DROP TABLE research_run_step_spend_reservations;
