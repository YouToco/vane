-- 141: freeze one business artifact's provider fan-out before any send.
--
-- A user may change the account/task preference while an Activity is retrying.
-- The first writer therefore freezes the exact provider set and Telegram route;
-- exact replay returns these bytes instead of re-reading mutable preference.

-- +goose Up

CREATE TABLE artifact_delivery_plans (
    id                  UUID        PRIMARY KEY,
    tenant_id           BIGINT      NOT NULL,
    user_id             BIGINT      NOT NULL,
    task_id             TEXT        NOT NULL,
    artifact_kind       TEXT        NOT NULL,
    artifact_key        TEXT        NOT NULL,
    selection           TEXT        NOT NULL,
    telegram_route_id   BIGINT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_artifact_delivery_plan_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE,
    CONSTRAINT fk_artifact_delivery_plan_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE RESTRICT,
    CONSTRAINT fk_artifact_delivery_plan_telegram_route
        FOREIGN KEY (tenant_id,user_id,telegram_route_id)
        REFERENCES channel_routes (tenant_id,user_id,id),
    CONSTRAINT uq_artifact_delivery_plan_fact
        UNIQUE (tenant_id,user_id,task_id,artifact_kind,artifact_key),
    CONSTRAINT uq_artifact_delivery_plan_scope
        UNIQUE (id,tenant_id,user_id),
    CONSTRAINT ck_artifact_delivery_plan_identity CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255 AND
        artifact_kind IN ('aggregate_brief','periodic_report','research_v3') AND
        btrim(artifact_key)=artifact_key AND octet_length(artifact_key) BETWEEN 1 AND 512
    ),
    CONSTRAINT ck_artifact_delivery_plan_selection CHECK (
        (selection='feishu' AND telegram_route_id IS NULL) OR
        (selection IN ('telegram','both') AND telegram_route_id IS NOT NULL)
    )
);

CREATE INDEX idx_artifact_delivery_plan_scope
    ON artifact_delivery_plans (tenant_id,user_id,task_id,created_at DESC);

ALTER TABLE artifact_delivery_plans ENABLE ROW LEVEL SECURITY;
CREATE POLICY artifact_delivery_plan_exact_principal
    ON artifact_delivery_plans
    FOR ALL
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

REVOKE ALL ON artifact_delivery_plans FROM PUBLIC,vane_app;
GRANT SELECT,INSERT ON artifact_delivery_plans TO vane_app;

-- +goose Down

LOCK TABLE artifact_delivery_plans IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM artifact_delivery_plans) THEN
        RAISE EXCEPTION '141: refusing downgrade while frozen artifact delivery plans exist';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE artifact_delivery_plans;
