-- 070: exact-run executive Brief synthesis receipts and immutable artifacts.
--
-- The receipt owns at-most-once spend authority. A response-lost or stale
-- spending claim can only converge to deterministic fallback; it can never be
-- re-authorized to call the provider.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

LOCK TABLE task_run_outcomes,canonical_brief_stages,brief_snapshots
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname='vane_brief_synthesis_writer'
    ) THEN
        CREATE ROLE vane_brief_synthesis_writer
            NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
            NOSUPERUSER NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname='vane_brief_synthesis_recovery'
    ) THEN
        CREATE ROLE vane_brief_synthesis_recovery
            NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
            NOSUPERUSER NOREPLICATION NOBYPASSRLS;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_brief_synthesis_writer
    NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
    NOSUPERUSER NOREPLICATION NOBYPASSRLS;
ALTER ROLE vane_brief_synthesis_writer RESET ALL;
ALTER ROLE vane_brief_synthesis_writer
    SET search_path=pg_catalog,public,pg_temp;

ALTER ROLE vane_brief_synthesis_recovery
    NOLOGIN NOINHERIT NOCREATEDB NOCREATEROLE
    NOSUPERUSER NOREPLICATION NOBYPASSRLS;
ALTER ROLE vane_brief_synthesis_recovery RESET ALL;
ALTER ROLE vane_brief_synthesis_recovery
    SET search_path=pg_catalog,public,pg_temp;

GRANT vane_brief_synthesis_writer TO CURRENT_USER;
GRANT vane_brief_synthesis_recovery TO CURRENT_USER;

CREATE TABLE executive_brief_synthesis_receipts (
    run_outcome_id     BIGINT      PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    task_id            TEXT        NOT NULL,
    run_snapshot_id    BIGINT      NOT NULL,
    push_batch_id      BIGINT      NOT NULL,
    schema_version     TEXT        NOT NULL,
    profile_epoch      BIGINT      NOT NULL,
    profile_version    BIGINT      NOT NULL,
    profile_digest     TEXT        NOT NULL,
    input_digest       TEXT        NOT NULL,
    request_digest     TEXT        NOT NULL,
    status             TEXT        NOT NULL DEFAULT 'prepared',
    generation_mode    TEXT,
    processing         TEXT,
    content_payload    BYTEA,
    content_digest     TEXT,
    spending_started_at TIMESTAMPTZ,
    finalized_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_executive_synthesis_run UNIQUE (run_snapshot_id),
    CONSTRAINT uq_executive_synthesis_batch UNIQUE (push_batch_id),
    CONSTRAINT uq_executive_synthesis_request
        UNIQUE (tenant_id,user_id,request_digest),
    CONSTRAINT fk_executive_synthesis_outcome_scope
        FOREIGN KEY (
            run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id
        ) REFERENCES task_run_outcomes (
            id,tenant_id,user_id,task_id,run_snapshot_id
        ) ON DELETE RESTRICT,
    CONSTRAINT fk_executive_synthesis_stage
        FOREIGN KEY (run_outcome_id)
        REFERENCES canonical_brief_stages (run_outcome_id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_executive_synthesis_schema
        CHECK (schema_version='vane.executive-brief/v1'),
    CONSTRAINT ck_executive_synthesis_task CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT ck_executive_synthesis_profile
        CHECK (profile_epoch>=0 AND profile_version>=0),
    CONSTRAINT ck_executive_synthesis_digests CHECK (
        profile_digest ~ '^[0-9a-f]{64}$' AND
        input_digest ~ '^[0-9a-f]{64}$' AND
        request_digest ~ '^[0-9a-f]{64}$' AND
        (content_digest IS NULL OR content_digest ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_executive_synthesis_status CHECK (
        status IN ('prepared','spending','finalized','ambiguous','fallback')
    ),
    CONSTRAINT ck_executive_synthesis_generation CHECK (
        generation_mode IS NULL OR
        generation_mode IN ('model','deterministic_fallback')
    ),
    CONSTRAINT ck_executive_synthesis_processing CHECK (
        processing IS NULL OR processing IN ('complete','partial')
    ),
    CONSTRAINT ck_executive_synthesis_payload CHECK (
        content_payload IS NULL OR
        octet_length(content_payload) BETWEEN 2 AND 262144
    ),
    CONSTRAINT ck_executive_synthesis_payload_digest CHECK (
        content_payload IS NULL OR
        content_digest=encode(sha256(content_payload),'hex')
    ),
    CONSTRAINT ck_executive_synthesis_shape CHECK (
        (
            status='prepared' AND generation_mode IS NULL AND
            processing IS NULL AND content_payload IS NULL AND
            content_digest IS NULL AND spending_started_at IS NULL AND
            finalized_at IS NULL
        ) OR (
            status='spending' AND generation_mode IS NULL AND
            processing IS NULL AND content_payload IS NULL AND
            content_digest IS NULL AND spending_started_at IS NOT NULL AND
            finalized_at IS NULL
        ) OR (
            status='finalized' AND generation_mode='model' AND
            processing='complete' AND content_payload IS NOT NULL AND
            content_digest IS NOT NULL AND spending_started_at IS NOT NULL AND
            finalized_at IS NOT NULL
        ) OR (
            status='ambiguous' AND generation_mode IS NULL AND
            processing IS NULL AND content_payload IS NULL AND
            content_digest IS NULL AND spending_started_at IS NOT NULL AND
            finalized_at IS NOT NULL
        ) OR (
            status='fallback' AND
            generation_mode='deterministic_fallback' AND
            processing='partial' AND content_payload IS NOT NULL AND
            content_digest IS NOT NULL AND finalized_at IS NOT NULL
        )
    )
);

CREATE TABLE executive_brief_artifacts (
    id                 BIGSERIAL   PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    task_id            TEXT        NOT NULL,
    run_outcome_id     BIGINT      NOT NULL,
    run_snapshot_id    BIGINT      NOT NULL,
    push_batch_id      BIGINT      NOT NULL,
    brief_snapshot_id  BIGINT      NOT NULL,
    schema_version     TEXT        NOT NULL,
    request_digest     TEXT        NOT NULL,
    payload_digest     TEXT        NOT NULL,
    payload            BYTEA       NOT NULL,
    generated_at       TIMESTAMPTZ NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_executive_artifacts_outcome UNIQUE (run_outcome_id),
    CONSTRAINT uq_executive_artifacts_run UNIQUE (run_snapshot_id),
    CONSTRAINT uq_executive_artifacts_batch UNIQUE (push_batch_id),
    CONSTRAINT uq_executive_artifacts_brief UNIQUE (brief_snapshot_id),
    CONSTRAINT fk_executive_artifacts_receipt
        FOREIGN KEY (run_outcome_id)
        REFERENCES executive_brief_synthesis_receipts (run_outcome_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_executive_artifacts_brief
        FOREIGN KEY (brief_snapshot_id)
        REFERENCES brief_snapshots (id) ON DELETE RESTRICT,
    CONSTRAINT fk_executive_artifacts_outcome_scope
        FOREIGN KEY (
            run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id
        ) REFERENCES task_run_outcomes (
            id,tenant_id,user_id,task_id,run_snapshot_id
        ) ON DELETE RESTRICT,
    CONSTRAINT ck_executive_artifacts_schema
        CHECK (schema_version='vane.executive-brief/v1'),
    CONSTRAINT ck_executive_artifacts_task CHECK (
        btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255
    ),
    CONSTRAINT ck_executive_artifacts_request_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_executive_artifacts_payload_digest CHECK (
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest=encode(sha256(payload),'hex')
    ),
    CONSTRAINT ck_executive_artifacts_payload
        CHECK (octet_length(payload) BETWEEN 2 AND 262144)
);

CREATE INDEX idx_executive_artifacts_task_generated
    ON executive_brief_artifacts (
        tenant_id,user_id,task_id,generated_at DESC,id DESC
    );

-- Terminal receipt and artifact bytes are immutable. Only the one-way receipt
-- state machine and updated_at may be changed by its dedicated authorities.
-- +goose StatementBegin
CREATE FUNCTION enforce_executive_synthesis_transition_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF ROW(
        NEW.run_outcome_id,NEW.tenant_id,NEW.user_id,NEW.task_id,
        NEW.run_snapshot_id,NEW.push_batch_id,NEW.schema_version,
        NEW.profile_epoch,NEW.profile_version,NEW.profile_digest,
        NEW.input_digest,NEW.request_digest,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.run_outcome_id,OLD.tenant_id,OLD.user_id,OLD.task_id,
        OLD.run_snapshot_id,OLD.push_batch_id,OLD.schema_version,
        OLD.profile_epoch,OLD.profile_version,OLD.profile_digest,
        OLD.input_digest,OLD.request_digest,OLD.created_at
    ) THEN
        RAISE EXCEPTION '070: executive synthesis identity is immutable';
    END IF;
    IF OLD.status IN ('finalized','fallback') THEN
        RAISE EXCEPTION '070: terminal executive synthesis is immutable';
    END IF;
    IF current_user='vane_brief_synthesis_writer' THEN
        IF NOT (
            (OLD.status='prepared' AND NEW.status IN ('spending','fallback'))
            OR
            (OLD.status='spending' AND
                NEW.status IN ('finalized','ambiguous','fallback'))
        ) THEN
            RAISE EXCEPTION '070: executive synthesis transition denied';
        END IF;
    ELSIF current_user='vane_brief_synthesis_recovery' THEN
        IF NOT (
            OLD.status IN ('spending','ambiguous') AND
            NEW.status='fallback'
        ) THEN
            RAISE EXCEPTION '070: executive synthesis recovery denied';
        END IF;
    ELSE
        RAISE EXCEPTION '070: executive synthesis authority denied';
    END IF;
    NEW.updated_at=clock_timestamp();
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_executive_synthesis_transition_v1()
    FROM PUBLIC;

CREATE TRIGGER executive_synthesis_transition_v1
BEFORE UPDATE ON executive_brief_synthesis_receipts
FOR EACH ROW EXECUTE FUNCTION enforce_executive_synthesis_transition_v1();

REVOKE ALL ON executive_brief_synthesis_receipts,
    executive_brief_artifacts
    FROM PUBLIC,vane_app,vane_brief_synthesis_writer,
         vane_brief_synthesis_recovery;
REVOKE ALL ON SEQUENCE executive_brief_artifacts_id_seq
    FROM PUBLIC,vane_app,vane_brief_synthesis_writer,
         vane_brief_synthesis_recovery;

GRANT USAGE ON SCHEMA public
    TO vane_brief_synthesis_writer,vane_brief_synthesis_recovery;

GRANT SELECT,INSERT,UPDATE ON executive_brief_synthesis_receipts
    TO vane_brief_synthesis_writer;
GRANT SELECT (
    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
    schema_version,profile_epoch,profile_version,profile_digest,input_digest,
    request_digest,status,generation_mode,processing,content_payload,
    content_digest,spending_started_at,finalized_at,created_at,updated_at
), UPDATE (
    status,generation_mode,processing,content_payload,content_digest,
    finalized_at,updated_at
) ON executive_brief_synthesis_receipts
    TO vane_brief_synthesis_recovery;

GRANT SELECT,INSERT ON executive_brief_artifacts
    TO vane_brief_synthesis_writer;
GRANT USAGE,SELECT ON SEQUENCE executive_brief_artifacts_id_seq
    TO vane_brief_synthesis_writer;
GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id
) ON brief_snapshots TO vane_brief_synthesis_writer;

GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,brief_snapshot_id,schema_version,request_digest,
    payload_digest,payload,generated_at,created_at
) ON executive_brief_artifacts TO vane_brief_reader;

ALTER TABLE executive_brief_synthesis_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON executive_brief_synthesis_receipts
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON executive_brief_synthesis_receipts
    AS RESTRICTIVE FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );
CREATE POLICY user_isolation ON executive_brief_synthesis_receipts
    AS RESTRICTIVE FOR ALL
    USING (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

ALTER TABLE executive_brief_artifacts ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON executive_brief_artifacts
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON executive_brief_artifacts
    AS RESTRICTIVE FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );
CREATE POLICY user_isolation ON executive_brief_artifacts
    AS RESTRICTIVE FOR ALL
    USING (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

LOCK TABLE task_run_outcomes,canonical_brief_stages,brief_snapshots,
    executive_brief_synthesis_receipts,executive_brief_artifacts
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM executive_brief_synthesis_receipts) OR
       EXISTS (SELECT 1 FROM executive_brief_artifacts) THEN
        RAISE EXCEPTION
            '070: refusing Down while executive Brief state exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS user_isolation ON executive_brief_artifacts;
DROP POLICY IF EXISTS tenant_isolation ON executive_brief_artifacts;
DROP POLICY IF EXISTS tenant_visible ON executive_brief_artifacts;
DROP POLICY IF EXISTS user_isolation
    ON executive_brief_synthesis_receipts;
DROP POLICY IF EXISTS tenant_isolation
    ON executive_brief_synthesis_receipts;
DROP POLICY IF EXISTS tenant_visible
    ON executive_brief_synthesis_receipts;

REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,brief_snapshot_id,schema_version,request_digest,
    payload_digest,payload,generated_at,created_at
) ON executive_brief_artifacts FROM vane_brief_reader;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id
) ON brief_snapshots FROM vane_brief_synthesis_writer;

DROP TRIGGER executive_synthesis_transition_v1
    ON executive_brief_synthesis_receipts;
DROP FUNCTION enforce_executive_synthesis_transition_v1();

DROP TABLE executive_brief_artifacts;
DROP TABLE executive_brief_synthesis_receipts;

REVOKE USAGE ON SCHEMA public
    FROM vane_brief_synthesis_writer,vane_brief_synthesis_recovery;
REVOKE vane_brief_synthesis_writer FROM CURRENT_USER;
REVOKE vane_brief_synthesis_recovery FROM CURRENT_USER;
DROP ROLE vane_brief_synthesis_writer;
DROP ROLE vane_brief_synthesis_recovery;
