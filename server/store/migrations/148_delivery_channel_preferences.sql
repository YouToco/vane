-- 148: user-selected outbound channel policy.
--
-- One user may bind both Feishu and Telegram.  The preference selects the
-- destinations for future business artifacts; this table does not itself
-- authorize provider effects. A NULL task_id is the account default. A task row is
-- an optional, future-compatible override and is constrained to an owned
-- schedule. Runtime delivery resolves this exact override before freezing an
-- immutable artifact plan.

-- +goose Up

CREATE TABLE delivery_channel_preferences (
    id          BIGSERIAL   PRIMARY KEY,
    tenant_id   BIGINT      NOT NULL,
    user_id     BIGINT      NOT NULL,
    task_id     TEXT,
    selection   TEXT        NOT NULL,
    telegram_route_id BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_delivery_channel_preference_membership
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES memberships (tenant_id,user_id) ON DELETE CASCADE,
    CONSTRAINT fk_delivery_channel_preference_task
        FOREIGN KEY (tenant_id,user_id,task_id)
        REFERENCES schedules (tenant_id,user_id,id) ON DELETE CASCADE,
    CONSTRAINT fk_delivery_channel_preference_telegram_route
        FOREIGN KEY (tenant_id,user_id,telegram_route_id)
        REFERENCES channel_routes (tenant_id,user_id,id),
    CONSTRAINT uq_delivery_channel_preference_scope
        UNIQUE NULLS NOT DISTINCT (tenant_id,user_id,task_id),
    CONSTRAINT ck_delivery_channel_preference_task CHECK (
        task_id IS NULL OR
        (btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255)
    ),
    CONSTRAINT ck_delivery_channel_preference_selection CHECK (
        selection IN ('feishu','telegram','both') AND
        (selection<>'feishu' OR telegram_route_id IS NULL)
    )
);

ALTER TABLE delivery_channel_preferences ENABLE ROW LEVEL SECURITY;

CREATE POLICY delivery_channel_preference_exact_principal
    ON delivery_channel_preferences
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

REVOKE ALL ON delivery_channel_preferences FROM PUBLIC,vane_app;
REVOKE ALL ON SEQUENCE delivery_channel_preferences_id_seq FROM PUBLIC,vane_app;
GRANT SELECT,INSERT,UPDATE,DELETE ON delivery_channel_preferences TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE delivery_channel_preferences_id_seq TO vane_app;
-- Preference writes validate only the exact principal's Telegram route. The
-- pre-existing RLS policy on channel_routes remains the visibility boundary.
GRANT SELECT ON channel_routes TO vane_app;

-- +goose StatementBegin
CREATE FUNCTION enforce_delivery_channel_preference_update_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(NEW.id,NEW.tenant_id,NEW.user_id,NEW.task_id,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id,OLD.tenant_id,OLD.user_id,OLD.task_id,OLD.created_at) THEN
        RAISE EXCEPTION '148: delivery channel preference identity is immutable';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION enforce_delivery_channel_preference_update_v1() FROM PUBLIC;
CREATE TRIGGER delivery_channel_preference_update_v1
BEFORE UPDATE ON delivery_channel_preferences
FOR EACH ROW EXECUTE FUNCTION enforce_delivery_channel_preference_update_v1();

-- +goose Down

LOCK TABLE delivery_channel_preferences IN ACCESS EXCLUSIVE MODE;
DROP TRIGGER delivery_channel_preference_update_v1
    ON delivery_channel_preferences;
DROP FUNCTION enforce_delivery_channel_preference_update_v1();
REVOKE SELECT ON channel_routes FROM vane_app;
DROP TABLE delivery_channel_preferences;
