-- 071: task-scoped periodic Brief settings, durable intents and immutable reports.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE schedules,brief_snapshots,task_run_outcomes IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname='vane_periodic_brief_writer'
    ) THEN
        CREATE ROLE vane_periodic_brief_writer
            NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
            NOSUPERUSER NOREPLICATION NOBYPASSRLS;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_periodic_brief_writer
    NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
    NOSUPERUSER NOREPLICATION NOBYPASSRLS;
ALTER ROLE vane_periodic_brief_writer RESET ALL;
ALTER ROLE vane_periodic_brief_writer
    SET search_path=pg_catalog,public,pg_temp;
GRANT vane_periodic_brief_writer TO CURRENT_USER;

CREATE TABLE brief_report_settings (
    tenant_id              BIGINT      NOT NULL,
    user_id                BIGINT      NOT NULL,
    task_id                TEXT        NOT NULL,
    mode                   TEXT        NOT NULL DEFAULT 'auto',
    cadence                TEXT        NOT NULL DEFAULT 'weekly',
    delivery               TEXT        NOT NULL DEFAULT 'important',
    auto_candidate         TEXT,
    auto_candidate_streak  INTEGER     NOT NULL DEFAULT 0,
    auto_evaluated_at      TIMESTAMPTZ,
    cadence_changed_at     TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,task_id),
    CONSTRAINT fk_brief_report_settings_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT ck_brief_report_settings_mode
        CHECK (mode IN ('auto','manual')),
    CONSTRAINT ck_brief_report_settings_cadence
        CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT ck_brief_report_settings_delivery
        CHECK (delivery IN ('important','always','web_only')),
    CONSTRAINT ck_brief_report_settings_candidate
        CHECK (auto_candidate IS NULL OR
               auto_candidate IN ('daily','weekly','monthly')),
    CONSTRAINT ck_brief_report_settings_streak
        CHECK (auto_candidate_streak BETWEEN 0 AND 2)
);

CREATE TABLE periodic_brief_intents (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    task_id               TEXT        NOT NULL,
    cadence               TEXT        NOT NULL,
    timezone              TEXT        NOT NULL,
    period_start          TIMESTAMPTZ NOT NULL,
    period_end            TIMESTAMPTZ NOT NULL,
    workflow_id           TEXT        NOT NULL,
    temporal_run_id       TEXT,
    input_brief_ids       BIGINT[]    NOT NULL DEFAULT '{}',
    input_digest          TEXT        NOT NULL,
    run_outcome_ids       BIGINT[]    NOT NULL DEFAULT '{}',
    outcome_digest        TEXT        NOT NULL,
    source_coverage       TEXT        NOT NULL,
    processing            TEXT        NOT NULL,
    partial_reason        TEXT,
    status                TEXT        NOT NULL DEFAULT 'prepared',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at            TIMESTAMPTZ,
    finalized_at          TIMESTAMPTZ,
    CONSTRAINT uq_periodic_brief_period
        UNIQUE (tenant_id,user_id,task_id,cadence,period_start,period_end),
    CONSTRAINT uq_periodic_brief_workflow UNIQUE (workflow_id),
    CONSTRAINT fk_periodic_brief_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE RESTRICT,
    CONSTRAINT ck_periodic_brief_cadence
        CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT ck_periodic_brief_timezone
        CHECK (btrim(timezone)=timezone AND octet_length(timezone) BETWEEN 1 AND 255),
    CONSTRAINT ck_periodic_brief_period CHECK (period_start<period_end),
    CONSTRAINT ck_periodic_brief_digests CHECK (
        input_digest ~ '^[0-9a-f]{64}$' AND
        outcome_digest ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_periodic_brief_limits CHECK (
        cardinality(input_brief_ids)<=20 AND
        cardinality(run_outcome_ids)<=2048
    ),
    CONSTRAINT ck_periodic_brief_coverage
        CHECK (source_coverage IN ('complete','partial')),
    CONSTRAINT ck_periodic_brief_processing
        CHECK (processing IN ('complete','partial')),
    CONSTRAINT ck_periodic_brief_intent_status
        CHECK (status IN ('prepared','running','finalized','fallback')),
    CONSTRAINT ck_periodic_brief_intent_shape CHECK (
        (status='prepared' AND started_at IS NULL AND finalized_at IS NULL) OR
        (status='running' AND started_at IS NOT NULL AND finalized_at IS NULL) OR
        (status IN ('finalized','fallback') AND finalized_at IS NOT NULL)
    )
);

CREATE INDEX idx_periodic_brief_intents_keyset
    ON periodic_brief_intents (
        tenant_id,user_id,task_id,period_end DESC,id DESC
    );

ALTER TABLE periodic_brief_intents
    ADD CONSTRAINT uq_periodic_brief_intent_scope
    UNIQUE (id,tenant_id,user_id,task_id);

CREATE TABLE periodic_synthesis_receipts (
    intent_id            BIGINT      PRIMARY KEY
        REFERENCES periodic_brief_intents(id) ON DELETE RESTRICT,
    tenant_id            BIGINT      NOT NULL,
    user_id              BIGINT      NOT NULL,
    task_id              TEXT        NOT NULL,
    request_digest       TEXT        NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'prepared',
    generation_mode      TEXT,
    content_payload      BYTEA,
    content_digest       TEXT,
    spending_started_at  TIMESTAMPTZ,
    finalized_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_periodic_synthesis_request
        UNIQUE (tenant_id,user_id,request_digest),
    CONSTRAINT fk_periodic_synthesis_scope
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id),
    CONSTRAINT ck_periodic_synthesis_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$' AND
               (content_digest IS NULL OR content_digest ~ '^[0-9a-f]{64}$')),
    CONSTRAINT ck_periodic_synthesis_status
        CHECK (status IN ('prepared','spending','finalized','fallback')),
    CONSTRAINT ck_periodic_synthesis_generation
        CHECK (generation_mode IS NULL OR
               generation_mode IN ('model','deterministic_fallback')),
    CONSTRAINT ck_periodic_synthesis_payload CHECK (
        content_payload IS NULL OR
        octet_length(content_payload) BETWEEN 2 AND 262144
    ),
    CONSTRAINT ck_periodic_synthesis_payload_digest CHECK (
        content_payload IS NULL OR
        content_digest=encode(sha256(content_payload),'hex')
    ),
    CONSTRAINT ck_periodic_synthesis_shape CHECK (
        (status='prepared' AND generation_mode IS NULL AND
         content_payload IS NULL AND spending_started_at IS NULL AND
         finalized_at IS NULL) OR
        (status='spending' AND generation_mode IS NULL AND
         content_payload IS NULL AND spending_started_at IS NOT NULL AND
         finalized_at IS NULL) OR
        (status='finalized' AND generation_mode='model' AND
         content_payload IS NOT NULL AND spending_started_at IS NOT NULL AND
         finalized_at IS NOT NULL) OR
        (status='fallback' AND generation_mode='deterministic_fallback' AND
         content_payload IS NOT NULL AND finalized_at IS NOT NULL)
    )
);

CREATE TABLE periodic_brief_reports (
    id               BIGSERIAL   PRIMARY KEY,
    intent_id        BIGINT      NOT NULL UNIQUE,
    tenant_id        BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    task_id          TEXT        NOT NULL,
    cadence          TEXT        NOT NULL,
    period_start     TIMESTAMPTZ NOT NULL,
    period_end       TIMESTAMPTZ NOT NULL,
    schema_version   TEXT        NOT NULL,
    request_digest   TEXT        NOT NULL,
    payload_digest   TEXT        NOT NULL,
    payload          BYTEA       NOT NULL,
    generated_at     TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_periodic_report_intent
        FOREIGN KEY (intent_id,tenant_id,user_id,task_id)
        REFERENCES periodic_brief_intents (id,tenant_id,user_id,task_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_periodic_report_schema
        CHECK (schema_version='vane.periodic-brief/v1'),
    CONSTRAINT ck_periodic_report_cadence
        CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT ck_periodic_report_period CHECK (period_start<period_end),
    CONSTRAINT ck_periodic_report_request
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_periodic_report_payload CHECK (
        octet_length(payload) BETWEEN 2 AND 524288 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest=encode(sha256(payload),'hex')
    ),
    CONSTRAINT uq_periodic_report_period
        UNIQUE (tenant_id,user_id,task_id,cadence,period_start,period_end)
);

CREATE INDEX idx_periodic_brief_reports_keyset
    ON periodic_brief_reports (
        tenant_id,user_id,task_id,cadence,period_end DESC,id DESC
    );

REVOKE ALL ON brief_report_settings,periodic_brief_intents,
    periodic_synthesis_receipts,periodic_brief_reports
    FROM PUBLIC,vane_app,vane_brief_reader,vane_periodic_brief_writer;
REVOKE ALL ON SEQUENCE periodic_brief_intents_id_seq,
    periodic_brief_reports_id_seq
    FROM PUBLIC,vane_app,vane_brief_reader,vane_periodic_brief_writer;

GRANT USAGE ON SCHEMA public TO vane_periodic_brief_writer;
GRANT SELECT,INSERT,UPDATE ON brief_report_settings,
    periodic_brief_intents,periodic_synthesis_receipts
    TO vane_periodic_brief_writer;
GRANT SELECT,INSERT ON periodic_brief_reports
    TO vane_periodic_brief_writer;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload,payload_digest,
    insight_count,generated_at
) ON brief_snapshots TO vane_periodic_brief_writer;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,status,result,
    source_coverage,processing,finalized_at
) ON task_run_outcomes TO vane_periodic_brief_writer;
GRANT USAGE,SELECT ON SEQUENCE periodic_brief_intents_id_seq,
    periodic_brief_reports_id_seq TO vane_periodic_brief_writer;
GRANT SELECT ON brief_report_settings,periodic_brief_reports
    TO vane_brief_reader;

ALTER TABLE brief_report_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_brief_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_synthesis_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE periodic_brief_reports ENABLE ROW LEVEL SECURITY;

-- +goose StatementBegin
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'brief_report_settings','periodic_brief_intents',
        'periodic_synthesis_receipts','periodic_brief_reports'
    ] LOOP
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)',
            table_name
        );
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL USING
             (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.tenant_id'',true),'''')::bigint)
             WITH CHECK
             (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.tenant_id'',true),'''')::bigint)',
            table_name
        );
        EXECUTE format(
            'CREATE POLICY user_isolation ON %I AS RESTRICTIVE FOR ALL USING
             (user_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.user_id'',true),'''')::bigint)
             WITH CHECK
             (user_id IS NOT DISTINCT FROM NULLIF(current_setting(''app.user_id'',true),'''')::bigint)',
            table_name
        );
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE brief_report_settings,periodic_brief_intents,
    periodic_synthesis_receipts,periodic_brief_reports
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM periodic_brief_intents) OR
       EXISTS (SELECT 1 FROM periodic_synthesis_receipts) OR
       EXISTS (SELECT 1 FROM periodic_brief_reports) THEN
        RAISE EXCEPTION '071: refusing Down while periodic Brief state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE periodic_brief_reports;
DROP TABLE periodic_synthesis_receipts;
DROP TABLE periodic_brief_intents;
DROP TABLE brief_report_settings;
REVOKE USAGE ON SCHEMA public FROM vane_periodic_brief_writer;
REVOKE vane_periodic_brief_writer FROM CURRENT_USER;
DROP ROLE vane_periodic_brief_writer;
