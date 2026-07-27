-- 064: durable pre-render canonical Brief stage.
--
-- A content RunOutcome cannot be finalized until Push returns, but canonical
-- payload must be frozen before renderer/provider side effects. The stage is
-- immutable evidence which the common outcome CAS promotes or aborts in the
-- same transaction.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

-- Preserve the producer-compatible order established by 061 while adding
-- triggers and foreign keys across the same authority boundary.
LOCK TABLE task_run_outcomes,deliveries,push_batches,brief_snapshots
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_brief_writer') THEN
        RAISE EXCEPTION '064: vane_brief_writer role is unavailable';
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE canonical_brief_stages (
    run_outcome_id    BIGINT      PRIMARY KEY,
    tenant_id         BIGINT      NOT NULL,
    user_id           BIGINT      NOT NULL,
    task_id           TEXT        NOT NULL,
    run_snapshot_id   BIGINT      NOT NULL,
    push_batch_id     BIGINT      NOT NULL,
    schema_version    TEXT        NOT NULL,
    request_digest    TEXT        NOT NULL,
    payload           BYTEA       NOT NULL,
    insight_count     INTEGER     NOT NULL,
    generated_at      TIMESTAMPTZ NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'staged',
    brief_snapshot_id BIGINT,
    resolved_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_canonical_brief_stages_run UNIQUE (run_snapshot_id),
    CONSTRAINT uq_canonical_brief_stages_batch UNIQUE (push_batch_id),
    CONSTRAINT uq_canonical_brief_stages_brief UNIQUE (brief_snapshot_id),
    CONSTRAINT fk_canonical_brief_stages_outcome_scope
        FOREIGN KEY (
            run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id
        ) REFERENCES task_run_outcomes (
            id,tenant_id,user_id,task_id,run_snapshot_id
        ) ON DELETE RESTRICT,
    CONSTRAINT fk_canonical_brief_stages_batch_scope
        FOREIGN KEY (
            push_batch_id,tenant_id,user_id,task_id,run_snapshot_id
        ) REFERENCES push_batches (
            id,tenant_id,user_id,schedule_id,run_snapshot_id
        ) ON DELETE RESTRICT,
    CONSTRAINT fk_canonical_brief_stages_brief
        FOREIGN KEY (brief_snapshot_id)
        REFERENCES brief_snapshots (id) ON DELETE RESTRICT,
    CONSTRAINT ck_canonical_brief_stages_schema
        CHECK (schema_version='vane.brief/v1'),
    CONSTRAINT ck_canonical_brief_stages_task
        CHECK (btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_canonical_brief_stages_request_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_canonical_brief_stages_payload_size
        CHECK (octet_length(payload) BETWEEN 2 AND 33554432),
    CONSTRAINT ck_canonical_brief_stages_insight_count
        CHECK (insight_count BETWEEN 1 AND 100),
    CONSTRAINT ck_canonical_brief_stages_status
        CHECK (status IN ('staged','promoted','aborted')),
    CONSTRAINT ck_canonical_brief_stages_resolution
        CHECK (
            (status='staged' AND brief_snapshot_id IS NULL AND resolved_at IS NULL)
            OR
            (status='promoted' AND brief_snapshot_id IS NOT NULL AND resolved_at IS NOT NULL)
            OR
            (status='aborted' AND brief_snapshot_id IS NULL AND resolved_at IS NOT NULL)
        )
);

-- +goose StatementBegin
CREATE FUNCTION enforce_canonical_brief_stage_authority_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    outcome_status TEXT;
    outcome_result TEXT;
    outcome_finalized_at TIMESTAMPTZ;
    batch_state TEXT;
    exact_brief BOOLEAN;
BEGIN
    IF current_user<>'vane_brief_writer' THEN
        RAISE EXCEPTION '064: canonical Brief stage authority denied';
    END IF;

    IF TG_OP='INSERT' THEN
        IF NEW.status<>'staged' OR NEW.brief_snapshot_id IS NOT NULL
           OR NEW.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION '064: canonical Brief stage must start staged';
        END IF;
        SELECT o.status
          INTO outcome_status
          FROM public.task_run_outcomes o
         WHERE o.id=NEW.run_outcome_id
           AND o.tenant_id=NEW.tenant_id AND o.user_id=NEW.user_id
           AND o.task_id=NEW.task_id
           AND o.run_snapshot_id=NEW.run_snapshot_id;
        SELECT b.brief_state
          INTO batch_state
          FROM public.push_batches b
         WHERE b.id=NEW.push_batch_id
           AND b.tenant_id=NEW.tenant_id AND b.user_id=NEW.user_id
           AND b.schedule_id=NEW.task_id
           AND b.run_snapshot_id=NEW.run_snapshot_id;
        IF outcome_status IS DISTINCT FROM 'pending'
           OR batch_state IS DISTINCT FROM 'sealed' THEN
            RAISE EXCEPTION
                '064: canonical Brief stage admission state is invalid';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.status<>'staged' OR NEW.status NOT IN ('promoted','aborted')
       OR ROW(
           NEW.run_outcome_id,NEW.tenant_id,NEW.user_id,NEW.task_id,
           NEW.run_snapshot_id,NEW.push_batch_id,NEW.schema_version,
           NEW.request_digest,NEW.payload,NEW.insight_count,
           NEW.generated_at,NEW.created_at
       ) IS DISTINCT FROM ROW(
           OLD.run_outcome_id,OLD.tenant_id,OLD.user_id,OLD.task_id,
           OLD.run_snapshot_id,OLD.push_batch_id,OLD.schema_version,
           OLD.request_digest,OLD.payload,OLD.insight_count,
           OLD.generated_at,OLD.created_at
       ) OR NEW.resolved_at IS NULL THEN
        RAISE EXCEPTION '064: canonical Brief stage transition is invalid';
    END IF;

    SELECT o.status,o.result,o.finalized_at
      INTO outcome_status,outcome_result,outcome_finalized_at
      FROM public.task_run_outcomes o
     WHERE o.id=NEW.run_outcome_id
       AND o.tenant_id=NEW.tenant_id AND o.user_id=NEW.user_id
       AND o.task_id=NEW.task_id
       AND o.run_snapshot_id=NEW.run_snapshot_id;
    IF outcome_status IS DISTINCT FROM 'finalized'
       OR NEW.resolved_at IS DISTINCT FROM outcome_finalized_at THEN
        RAISE EXCEPTION
            '064: canonical Brief stage terminal outcome is invalid';
    END IF;

    IF NEW.status='promoted' THEN
        IF outcome_result IS DISTINCT FROM 'content'
           OR NEW.brief_snapshot_id IS NULL THEN
            RAISE EXCEPTION
                '064: canonical Brief stage promotion is invalid';
        END IF;
        SELECT true
          INTO exact_brief
          FROM public.brief_snapshots bs
         WHERE bs.id=NEW.brief_snapshot_id
           AND bs.run_outcome_id=NEW.run_outcome_id
           AND bs.tenant_id=NEW.tenant_id AND bs.user_id=NEW.user_id
           AND bs.task_id=NEW.task_id
           AND bs.run_snapshot_id=NEW.run_snapshot_id
           AND bs.push_batch_id=NEW.push_batch_id
           AND bs.schema_version=NEW.schema_version
           AND bs.request_digest=NEW.request_digest
           AND bs.insight_count=NEW.insight_count
           AND bs.generated_at=NEW.generated_at;
        IF exact_brief IS DISTINCT FROM true THEN
            RAISE EXCEPTION
                '064: canonical Brief stage promotion target is invalid';
        END IF;
    ELSIF outcome_result='content' OR NEW.brief_snapshot_id IS NOT NULL THEN
        RAISE EXCEPTION '064: canonical Brief stage abort is invalid';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_canonical_brief_stage_authority_v1()
    FROM PUBLIC;

CREATE TRIGGER canonical_brief_stage_authority_v1
BEFORE INSERT OR UPDATE ON canonical_brief_stages
FOR EACH ROW EXECUTE FUNCTION enforce_canonical_brief_stage_authority_v1();

-- Repair 061's missing database-level admission gate. A direct table INSERT
-- may only bind an exact finalized content outcome to an already sealed batch.
-- +goose StatementBegin
CREATE FUNCTION enforce_brief_snapshot_admission_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
    outcome_ok BOOLEAN;
    batch_ok BOOLEAN;
BEGIN
    IF current_user<>'vane_brief_writer' THEN
        RAISE EXCEPTION '064: canonical Brief snapshot authority denied';
    END IF;
    SELECT true
      INTO outcome_ok
      FROM public.task_run_outcomes o
     WHERE o.id=NEW.run_outcome_id
       AND o.tenant_id=NEW.tenant_id AND o.user_id=NEW.user_id
       AND o.task_id=NEW.task_id
       AND o.run_snapshot_id=NEW.run_snapshot_id
       AND o.status='finalized' AND o.result='content';
    SELECT true
      INTO batch_ok
      FROM public.push_batches b
     WHERE b.id=NEW.push_batch_id
       AND b.tenant_id=NEW.tenant_id AND b.user_id=NEW.user_id
       AND b.schedule_id=NEW.task_id
       AND b.run_snapshot_id=NEW.run_snapshot_id
       AND b.brief_state='sealed';
    IF outcome_ok IS DISTINCT FROM true OR batch_ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION '064: canonical Brief snapshot admission denied';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_brief_snapshot_admission_v1() FROM PUBLIC;

CREATE TRIGGER brief_snapshot_admission_v1
BEFORE INSERT ON brief_snapshots
FOR EACH ROW EXECUTE FUNCTION enforce_brief_snapshot_admission_v1();

-- No caller may commit a terminal outcome while its pre-render evidence is
-- unresolved. Deferred evaluation permits the common CAS to update outcome
-- first and promote/abort the stage later in the same transaction.
-- +goose StatementBegin
CREATE FUNCTION enforce_canonical_brief_stage_resolution_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NEW.status='finalized' AND EXISTS (
        SELECT 1
          FROM public.canonical_brief_stages s
         WHERE s.run_outcome_id=NEW.id AND s.status='staged'
    ) THEN
        RAISE EXCEPTION
            '064: finalized RunOutcome has unresolved canonical Brief stage';
    END IF;
    RETURN NULL;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_canonical_brief_stage_resolution_v1()
    FROM PUBLIC;

CREATE CONSTRAINT TRIGGER canonical_brief_stage_resolution_v1
AFTER UPDATE OF status ON task_run_outcomes
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_canonical_brief_stage_resolution_v1();

REVOKE ALL ON canonical_brief_stages
    FROM PUBLIC,vane_app,vane_brief_writer;
GRANT SELECT (
    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
    schema_version,request_digest,payload,insight_count,generated_at,status,
    brief_snapshot_id,resolved_at,created_at
) ON canonical_brief_stages TO vane_brief_writer;
GRANT INSERT (
    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
    schema_version,request_digest,payload,insight_count,generated_at
) ON canonical_brief_stages TO vane_brief_writer;
GRANT UPDATE (
    status,brief_snapshot_id,resolved_at
) ON canonical_brief_stages TO vane_brief_writer;

ALTER TABLE canonical_brief_stages ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON canonical_brief_stages
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON canonical_brief_stages AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );
CREATE POLICY user_isolation ON canonical_brief_stages AS RESTRICTIVE
    FOR ALL
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

LOCK TABLE task_run_outcomes,deliveries,push_batches,brief_snapshots,
    canonical_brief_stages IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM canonical_brief_stages) THEN
        RAISE EXCEPTION
            '064: refusing to drop canonical Brief stage evidence';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER canonical_brief_stage_resolution_v1 ON task_run_outcomes;
DROP FUNCTION enforce_canonical_brief_stage_resolution_v1();

DROP TRIGGER brief_snapshot_admission_v1 ON brief_snapshots;
DROP FUNCTION enforce_brief_snapshot_admission_v1();

DROP POLICY IF EXISTS user_isolation ON canonical_brief_stages;
DROP POLICY IF EXISTS tenant_isolation ON canonical_brief_stages;
DROP POLICY IF EXISTS tenant_visible ON canonical_brief_stages;
REVOKE UPDATE (
    status,brief_snapshot_id,resolved_at
) ON canonical_brief_stages FROM vane_brief_writer;
REVOKE INSERT (
    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
    schema_version,request_digest,payload,insight_count,generated_at
) ON canonical_brief_stages FROM vane_brief_writer;
REVOKE SELECT (
    run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id,push_batch_id,
    schema_version,request_digest,payload,insight_count,generated_at,status,
    brief_snapshot_id,resolved_at,created_at
) ON canonical_brief_stages FROM vane_brief_writer;
DROP TRIGGER canonical_brief_stage_authority_v1
    ON canonical_brief_stages;
DROP FUNCTION enforce_canonical_brief_stage_authority_v1();
DROP TABLE canonical_brief_stages;
