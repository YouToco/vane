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
    CONSTRAINT ck_canonical_brief_stages_payload_digest
        CHECK (request_digest=encode(sha256(payload),'hex')),
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

-- 061 admitted dark-store rows through Store validation only. Before adding
-- the database trigger, refuse to bless any retained row that is not already
-- bound to the exact finalized content outcome and sealed batch.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM brief_snapshots bs
          LEFT JOIN task_run_outcomes o
            ON o.id=bs.run_outcome_id
           AND o.tenant_id=bs.tenant_id AND o.user_id=bs.user_id
           AND o.task_id=bs.task_id
           AND o.run_snapshot_id=bs.run_snapshot_id
          LEFT JOIN push_batches b
            ON b.id=bs.push_batch_id
           AND b.tenant_id=bs.tenant_id AND b.user_id=bs.user_id
           AND b.schedule_id=bs.task_id
           AND b.run_snapshot_id=bs.run_snapshot_id
         WHERE o.id IS NULL OR o.status<>'finalized' OR o.result<>'content'
            OR b.id IS NULL OR b.brief_state<>'sealed'
    ) THEN
        RAISE EXCEPTION
            '064: retained canonical Brief admission audit failed';
    END IF;
END $$;
-- +goose StatementEnd

-- Recovery may only create a provider request for a P1-C effect after the
-- canonical stage has been promoted by a finalized content outcome. Pending
-- and aborted stages deny late sends. Batches without a stage retain the
-- pre-P1-C recovery contract.
-- +goose StatementBegin
CREATE FUNCTION canonical_brief_push_recovery_admitted_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT,
    requested_push_batch_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    stage_exists BOOLEAN;
    admitted BOOLEAN;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    IF requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_task_id IS NULL OR requested_task_id='' OR
       pg_catalog.btrim(requested_task_id)<>requested_task_id OR
       pg_catalog.octet_length(requested_task_id)>512 OR
       requested_run_snapshot_id<=0 OR requested_push_batch_id<=0 OR
       tenant_context IS DISTINCT FROM requested_tenant_id THEN
        RETURN false;
    END IF;

    SELECT true INTO stage_exists
      FROM public.canonical_brief_stages s
     WHERE s.push_batch_id=requested_push_batch_id;
    IF stage_exists IS DISTINCT FROM true THEN
        RETURN true;
    END IF;

    SELECT true INTO admitted
      FROM public.canonical_brief_stages s
      JOIN public.task_run_outcomes o
        ON o.id=s.run_outcome_id
       AND o.tenant_id=s.tenant_id AND o.user_id=s.user_id
       AND o.task_id=s.task_id
       AND o.run_snapshot_id=s.run_snapshot_id
     WHERE s.push_batch_id=requested_push_batch_id
       AND s.tenant_id=requested_tenant_id
       AND s.user_id=requested_user_id
       AND s.task_id=requested_task_id
       AND s.run_snapshot_id=requested_run_snapshot_id
       AND s.status='promoted'
       AND o.status='finalized' AND o.result='content';
    RETURN admitted IS TRUE;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION canonical_brief_push_recovery_admitted_v1(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION canonical_brief_push_recovery_admitted_v1(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) TO vane_push_effect_coordinator;

-- The writer already owns the open->sealed transition. P1-C empty receipts
-- additionally need to bind that transition to the exact physical key,
-- effect authority, and terminal batch status; no provider payload or target
-- columns are exposed.
GRANT SELECT (
    status,idempotency_key,delivery_authority
) ON push_batches TO vane_brief_writer;

-- A generic Temporal FAILED receipt is overridden to quiet only when the
-- exact run has one and only one sealed, done, effect-authority batch with no
-- delivery/effect evidence.
-- +goose StatementBegin
CREATE FUNCTION canonical_brief_empty_terminal_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_task_id TEXT,
    requested_run_snapshot_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    user_context BIGINT;
    batch_count BIGINT;
    terminal_count BIGINT;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    user_context :=
        NULLIF(pg_catalog.current_setting('app.user_id',true),'')::bigint;
    IF requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_task_id IS NULL OR requested_task_id='' OR
       pg_catalog.btrim(requested_task_id)<>requested_task_id OR
       pg_catalog.octet_length(requested_task_id)>512 OR
       requested_run_snapshot_id<=0 OR
       tenant_context IS DISTINCT FROM requested_tenant_id OR
       user_context IS DISTINCT FROM requested_user_id THEN
        RETURN false;
    END IF;

    PERFORM 1
      FROM public.push_batches b
     WHERE b.tenant_id=requested_tenant_id
       AND b.user_id=requested_user_id
       AND b.schedule_id=requested_task_id
       AND b.run_snapshot_id=requested_run_snapshot_id
     ORDER BY b.id
     FOR SHARE;
    SELECT count(*),
           count(*) FILTER (
               WHERE b.brief_state='sealed' AND b.status='done'
                 AND b.delivery_authority='effect'
                 AND NOT EXISTS (
                     SELECT 1 FROM public.deliveries d
                      WHERE d.batch_id=b.id
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM public.push_effects e
                      WHERE e.batch_id=b.id
                 )
           )
      INTO batch_count,terminal_count
      FROM public.push_batches b
     WHERE b.tenant_id=requested_tenant_id
       AND b.user_id=requested_user_id
       AND b.schedule_id=requested_task_id
       AND b.run_snapshot_id=requested_run_snapshot_id;
    RETURN batch_count=1 AND terminal_count=1;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION canonical_brief_empty_terminal_v1(
    BIGINT,BIGINT,TEXT,BIGINT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION canonical_brief_empty_terminal_v1(
    BIGINT,BIGINT,TEXT,BIGINT
) TO vane_brief_writer;

-- Receipt-only completion for a v2 nil-draft command. "legacy" leaves the
-- pre-P1-C open-batch path unchanged; a sealed canonical batch either
-- completes under exact zero-evidence predicates or is denied.
-- +goose StatementBegin
CREATE FUNCTION complete_canonical_empty_push_batch_v1(
    requested_tenant_id BIGINT,
    requested_user_id BIGINT,
    requested_push_batch_id BIGINT,
    requested_run_snapshot_id BIGINT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    tenant_context BIGINT;
    state TEXT;
    batch_task_id TEXT;
    outcome_status TEXT;
    outcome_result TEXT;
    resulting_status TEXT;
BEGIN
    tenant_context :=
        NULLIF(pg_catalog.current_setting('app.tenant_id',true),'')::bigint;
    IF requested_tenant_id<=0 OR requested_user_id<=0 OR
       requested_push_batch_id<=0 OR requested_run_snapshot_id<=0 OR
       tenant_context IS DISTINCT FROM requested_tenant_id THEN
        RETURN 'denied';
    END IF;
    SELECT b.brief_state,b.schedule_id
      INTO state,batch_task_id
      FROM public.push_batches b
     WHERE b.id=requested_push_batch_id
       AND b.tenant_id=requested_tenant_id
       AND b.user_id=requested_user_id
       AND b.run_snapshot_id=requested_run_snapshot_id;
    IF state IS NULL THEN
        RETURN 'denied';
    ELSIF state='open' THEN
        -- Lock and recheck before handing this transaction to the legacy
        -- settlement path. A concurrent canonical seal either waits behind
        -- this open-batch receipt or wins first and forces a retry; it can
        -- never change open->sealed between admission and settlement.
        SELECT b.brief_state
          INTO state
          FROM public.push_batches b
         WHERE b.id=requested_push_batch_id
           AND b.tenant_id=requested_tenant_id
           AND b.user_id=requested_user_id
           AND b.run_snapshot_id=requested_run_snapshot_id
         FOR UPDATE;
        IF state='open' THEN
            RETURN 'legacy';
        END IF;
        RETURN 'denied';
    ELSIF state<>'sealed' THEN
        RETURN 'denied';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vane/canonical-brief/v1/' ||
                requested_run_snapshot_id::text,
            1112688177
        )
    );
    SELECT o.status,o.result
      INTO outcome_status,outcome_result
      FROM public.task_run_outcomes o
     WHERE o.tenant_id=requested_tenant_id
       AND o.user_id=requested_user_id
       AND o.task_id=batch_task_id
       AND o.run_snapshot_id=requested_run_snapshot_id
     FOR SHARE;
    IF outcome_status IS NULL OR
       (outcome_status='finalized' AND
        outcome_result IS DISTINCT FROM 'quiet') THEN
        RETURN 'denied';
    END IF;

    -- Recheck and lock the batch only after the shared outcome fence. This is
    -- the same run/outcome -> batch order used by finalization.
    SELECT b.brief_state
      INTO state
      FROM public.push_batches b
     WHERE b.id=requested_push_batch_id
       AND b.tenant_id=requested_tenant_id
       AND b.user_id=requested_user_id
       AND b.run_snapshot_id=requested_run_snapshot_id
     FOR UPDATE;
    IF state IS DISTINCT FROM 'sealed' THEN
        RETURN 'denied';
    END IF;

    UPDATE public.push_batches b
       SET status='done'
     WHERE b.id=requested_push_batch_id
       AND b.tenant_id=requested_tenant_id
       AND b.user_id=requested_user_id
       AND b.run_snapshot_id=requested_run_snapshot_id
       AND b.brief_state='sealed'
       AND b.delivery_authority='effect'
       AND b.status IN ('pending','done')
       AND NOT EXISTS (
           SELECT 1 FROM public.deliveries d WHERE d.batch_id=b.id
       )
       AND NOT EXISTS (
           SELECT 1 FROM public.push_effects e WHERE e.batch_id=b.id
       )
     RETURNING b.status INTO resulting_status;
    IF resulting_status='done' THEN
        RETURN 'done';
    END IF;
    RETURN 'denied';
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION complete_canonical_empty_push_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION complete_canonical_empty_push_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
) TO vane_push_effect_receipt;

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
           AND o.run_snapshot_id=NEW.run_snapshot_id
         FOR SHARE;
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
    IF EXISTS (
        SELECT 1
          FROM push_batches b
          JOIN task_run_outcomes o
            ON o.tenant_id=b.tenant_id AND o.user_id=b.user_id
           AND o.task_id=b.schedule_id
           AND o.run_snapshot_id=b.run_snapshot_id
         WHERE b.brief_state='sealed'
           AND b.delivery_authority='effect'
           AND o.status='pending'
           AND NOT EXISTS (
               SELECT 1 FROM deliveries d WHERE d.batch_id=b.id
           )
           AND NOT EXISTS (
               SELECT 1 FROM push_effects e WHERE e.batch_id=b.id
           )
    ) THEN
        RAISE EXCEPTION
            '064: refusing to drop unsettled canonical Brief empty evidence';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER canonical_brief_stage_resolution_v1 ON task_run_outcomes;
DROP FUNCTION enforce_canonical_brief_stage_resolution_v1();

DROP TRIGGER brief_snapshot_admission_v1 ON brief_snapshots;
DROP FUNCTION enforce_brief_snapshot_admission_v1();

REVOKE EXECUTE ON FUNCTION complete_canonical_empty_push_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
) FROM vane_push_effect_receipt;
DROP FUNCTION complete_canonical_empty_push_batch_v1(
    BIGINT,BIGINT,BIGINT,BIGINT
);

REVOKE EXECUTE ON FUNCTION canonical_brief_empty_terminal_v1(
    BIGINT,BIGINT,TEXT,BIGINT
) FROM vane_brief_writer;
DROP FUNCTION canonical_brief_empty_terminal_v1(
    BIGINT,BIGINT,TEXT,BIGINT
);
REVOKE SELECT (
    status,idempotency_key,delivery_authority
) ON push_batches FROM vane_brief_writer;

REVOKE EXECUTE ON FUNCTION canonical_brief_push_recovery_admitted_v1(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
) FROM vane_push_effect_coordinator;
DROP FUNCTION canonical_brief_push_recovery_admitted_v1(
    BIGINT,BIGINT,TEXT,BIGINT,BIGINT
);

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
