-- 061: channel-neutral RunOutcomeV1 + BriefV1 dark store.
--
-- This migration has no workflow/API call point. It establishes the exact-run
-- identity, immutable whole-Brief payload, persisted rank, RLS, and a dedicated
-- least-privilege writer before P1-B/P1-C connect any production lifecycle.

-- +goose Up

-- Drain every post-047 compiled writer before changing push batch/delivery
-- protocol state. Pre-fence legacy inserts are still covered by the
-- producer-compatible table-lock order in Down.
SELECT pg_advisory_xact_lock(6215335020355474248);

-- A legacy delivery INSERT takes deliveries RowExclusive before its existing
-- batch FK reaches push_batches. Take both schema locks in that producer order
-- before any DDL so migration Up cannot form the inverse lock edge.
LOCK TABLE deliveries,push_batches
    IN ACCESS EXCLUSIVE MODE;

-- Delivery/batch and Brief/outcome scope use composite FKs. Outcome/snapshot
-- existence and exact scope use an admission trigger below, avoiding a
-- task_run_snapshots dependency lock that conflicts with existing writer order.
-- Existing deliveries must already agree with their parent batch scope. RLS
-- can hide a poisoned row from a tenant-scoped reader, so fail the migration
-- instead of letting a filtered subset masquerade as the complete batch.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM deliveries d
          JOIN push_batches b ON b.id=d.batch_id
         WHERE d.tenant_id IS DISTINCT FROM b.tenant_id
            OR d.user_id IS DISTINCT FROM b.user_id
    ) THEN
        RAISE EXCEPTION
            '061: delivery scope differs from its push batch';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE push_batches
    ADD COLUMN brief_state TEXT NOT NULL DEFAULT 'open',
    ADD CONSTRAINT ck_push_batches_brief_state
        CHECK (brief_state IN ('open','sealed')),
    ADD CONSTRAINT uq_push_batches_delivery_scope
        UNIQUE (id,tenant_id,user_id),
    ADD CONSTRAINT uq_push_batches_brief_scope
    UNIQUE (id,tenant_id,user_id,schedule_id,run_snapshot_id);

ALTER TABLE deliveries
    ADD CONSTRAINT fk_deliveries_brief_batch_scope
    FOREIGN KEY (batch_id,tenant_id,user_id)
    REFERENCES push_batches (id,tenant_id,user_id);

-- The legacy vane_app table grant predates this column. A trigger therefore
-- enforces the new authority independently of ambient table-level INSERT /
-- UPDATE: normal writers may only create open batches, and only the dedicated
-- role may perform the one-way open->sealed transition.
-- +goose StatementBegin
CREATE FUNCTION enforce_brief_batch_state_authority_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.brief_state<>'open' THEN
            RAISE EXCEPTION '061: new push batch must start open';
        END IF;
        RETURN NEW;
    END IF;
    IF current_user<>'vane_brief_writer' OR
       OLD.brief_state<>'open' OR NEW.brief_state<>'sealed' THEN
        RAISE EXCEPTION '061: brief batch seal authority denied';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_brief_batch_state_authority_v1()
    FROM PUBLIC;

CREATE TRIGGER push_batches_brief_state_authority_v1
BEFORE INSERT OR UPDATE OF brief_state ON push_batches
FOR EACH ROW EXECUTE FUNCTION enforce_brief_batch_state_authority_v1();

-- Serialize delivery creation with Brief freeze without granting the dark
-- writer broad UPDATE on deliveries. Every insert takes a key-share lock on
-- its batch; Freeze takes UPDATE on the same row, verifies the complete set,
-- then flips open->sealed. Either the delivery wins and is included, or the
-- seal wins and the late insert fails.
-- +goose StatementBegin
CREATE FUNCTION enforce_open_brief_batch_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE state text;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.batch_id,NEW.tenant_id,NEW.user_id) IS DISTINCT FROM
           ROW(OLD.batch_id,OLD.tenant_id,OLD.user_id) THEN
            RAISE EXCEPTION
                '061: canonical delivery scope is immutable';
        END IF;
        SELECT brief_state INTO state
          FROM public.push_batches
         WHERE id=OLD.batch_id
           AND tenant_id=OLD.tenant_id
           AND user_id=OLD.user_id
         FOR KEY SHARE;
        IF state IS NULL THEN
            RAISE EXCEPTION '061: delivery batch is unavailable';
        END IF;
        IF state<>'open' THEN
            RAISE EXCEPTION
                '061: canonical delivery evidence is immutable';
        END IF;
        RETURN NEW;
    END IF;
    SELECT brief_state INTO state
      FROM public.push_batches
     WHERE id=NEW.batch_id
       AND tenant_id=NEW.tenant_id
       AND user_id=NEW.user_id
     FOR KEY SHARE;
    IF state IS NULL THEN
        RAISE EXCEPTION '061: delivery batch is unavailable';
    END IF;
    IF state<>'open' THEN
        -- Temporal response-loss replay may retry the exact existing
        -- (batch_id,content_item_id) INSERT. Let it reach the retained unique
        -- arbiter so ON CONFLICT can recover the original row; no new delivery
        -- or payload mutation is admitted.
        IF TG_OP='INSERT' AND NEW.content_item_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM public.deliveries
             WHERE batch_id=NEW.batch_id
               AND tenant_id=NEW.tenant_id
               AND user_id=NEW.user_id
               AND content_item_id=NEW.content_item_id
        ) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION '061: canonical Brief batch is sealed';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_open_brief_batch_v1() FROM PUBLIC;

CREATE TRIGGER deliveries_require_open_brief_batch_v1
BEFORE INSERT OR UPDATE OF
    batch_id,tenant_id,user_id,content_item_id,body_md,created_at
ON deliveries
FOR EACH ROW EXECUTE FUNCTION enforce_open_brief_batch_v1();

CREATE TABLE task_run_outcomes (
    id               BIGSERIAL   PRIMARY KEY,
    tenant_id        BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    task_id          TEXT        NOT NULL,
    run_snapshot_id  BIGINT      NOT NULL,
    schema_version   TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending',
    result           TEXT,
    source_coverage  TEXT,
    processing       TEXT,
    failure_code     TEXT        NOT NULL DEFAULT '',
    failure_message  TEXT        NOT NULL DEFAULT '',
    finalized_at     TIMESTAMPTZ,
    outcome_digest   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_task_run_outcomes_snapshot UNIQUE (run_snapshot_id),
    CONSTRAINT uq_task_run_outcomes_brief_scope
        UNIQUE (id,tenant_id,user_id,task_id,run_snapshot_id),
    CONSTRAINT ck_task_run_outcomes_schema
        CHECK (schema_version='vane.run-outcome/v1'),
    CONSTRAINT ck_task_run_outcomes_task
        CHECK (btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_task_run_outcomes_status
        CHECK (status IN ('pending','finalized')),
    CONSTRAINT ck_task_run_outcomes_result
        CHECK (result IS NULL OR result IN ('content','quiet','failed','interrupted')),
    CONSTRAINT ck_task_run_outcomes_source_coverage
        CHECK (source_coverage IS NULL OR source_coverage IN ('complete','partial')),
    CONSTRAINT ck_task_run_outcomes_processing
        CHECK (processing IS NULL OR processing IN ('complete','partial')),
    CONSTRAINT ck_task_run_outcomes_failure_size CHECK (
        octet_length(failure_code)<=128 AND
        octet_length(failure_message)<=4096
    ),
    CONSTRAINT ck_task_run_outcomes_digest
        CHECK (outcome_digest IS NULL OR outcome_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_task_run_outcomes_terminal_shape CHECK (
        (
            status='pending' AND result IS NULL AND
            source_coverage IS NULL AND processing IS NULL AND
            failure_code='' AND failure_message='' AND
            finalized_at IS NULL AND outcome_digest IS NULL
        ) OR (
            status='finalized' AND result IS NOT NULL AND
            source_coverage IS NOT NULL AND processing IS NOT NULL AND
            finalized_at IS NOT NULL AND outcome_digest IS NOT NULL AND
            (
                (
                    result IN ('failed','interrupted') AND
                    btrim(failure_code)=failure_code AND failure_code<>''
                ) OR (
                    result IN ('content','quiet') AND
                    failure_code='' AND failure_message=''
                )
            )
        )
    )
);

-- Snapshot existence and exact tenant/user/task scope are checked on admission
-- by a definer trigger. No runtime role can delete snapshots, and tenant purge
-- deletes outcomes first. Avoiding a cross-table FK is deliberate: even
-- dropping that child FK locks task_run_snapshots and deadlocks with an
-- existing snapshot-first writer waiting on the schema advisory fence.
-- +goose StatementBegin
CREATE FUNCTION enforce_run_outcome_snapshot_scope_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint OR
       NEW.user_id IS DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint THEN
        RAISE EXCEPTION '061: run outcome snapshot scope differs';
    END IF;
    PERFORM 1
      FROM public.task_run_snapshots
     WHERE id=NEW.run_snapshot_id
       AND tenant_id=NEW.tenant_id
       AND user_id=NEW.user_id
       AND task_id=NEW.task_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION '061: run outcome snapshot scope differs';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_run_outcome_snapshot_scope_v1() FROM PUBLIC;

CREATE TRIGGER task_run_outcomes_snapshot_scope_v1
BEFORE INSERT OR UPDATE OF tenant_id,user_id,task_id,run_snapshot_id
ON task_run_outcomes
FOR EACH ROW EXECUTE FUNCTION enforce_run_outcome_snapshot_scope_v1();

CREATE INDEX idx_task_run_outcomes_tenant_user_task_created
    ON task_run_outcomes (tenant_id,user_id,task_id,created_at DESC,id DESC);

-- The writer capability is one-way at the database boundary, not merely a
-- Store convention. A finalized outcome is immutable even if a caller obtains
-- the writer role and issues SQL directly.
-- +goose StatementBegin
CREATE FUNCTION enforce_run_outcome_transition_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    IF current_user<>'vane_brief_writer' OR
       OLD.status<>'pending' OR NEW.status<>'finalized' OR
       ROW(
           NEW.id,NEW.tenant_id,NEW.user_id,NEW.task_id,
           NEW.run_snapshot_id,NEW.schema_version,NEW.created_at
       ) IS DISTINCT FROM ROW(
           OLD.id,OLD.tenant_id,OLD.user_id,OLD.task_id,
           OLD.run_snapshot_id,OLD.schema_version,OLD.created_at
       ) THEN
        RAISE EXCEPTION '061: run outcome transition authority denied';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION enforce_run_outcome_transition_v1() FROM PUBLIC;

CREATE TRIGGER task_run_outcomes_one_way_finalization_v1
BEFORE UPDATE ON task_run_outcomes
FOR EACH ROW EXECUTE FUNCTION enforce_run_outcome_transition_v1();

CREATE TABLE brief_snapshots (
    id               BIGSERIAL   PRIMARY KEY,
    tenant_id        BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    task_id          TEXT        NOT NULL,
    run_outcome_id   BIGINT      NOT NULL,
    run_snapshot_id  BIGINT      NOT NULL,
    push_batch_id    BIGINT      NOT NULL,
    schema_version   TEXT        NOT NULL,
    request_digest   TEXT        NOT NULL,
    payload_digest   TEXT        NOT NULL,
    payload          BYTEA       NOT NULL,
    insight_count    INTEGER     NOT NULL,
    generated_at     TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT uq_brief_snapshots_outcome UNIQUE (run_outcome_id),
    CONSTRAINT uq_brief_snapshots_run UNIQUE (run_snapshot_id),
    CONSTRAINT uq_brief_snapshots_batch UNIQUE (push_batch_id),
    CONSTRAINT uq_brief_snapshots_scope
        UNIQUE (id,tenant_id,user_id,task_id),
    CONSTRAINT fk_brief_snapshots_outcome_scope
        FOREIGN KEY (
            run_outcome_id,tenant_id,user_id,task_id,run_snapshot_id
        ) REFERENCES task_run_outcomes (
            id,tenant_id,user_id,task_id,run_snapshot_id
        ),
    CONSTRAINT fk_brief_snapshots_batch_scope
        FOREIGN KEY (
            push_batch_id,tenant_id,user_id,task_id,run_snapshot_id
        ) REFERENCES push_batches (
            id,tenant_id,user_id,schedule_id,run_snapshot_id
        ),
    CONSTRAINT ck_brief_snapshots_schema
        CHECK (schema_version='vane.brief/v1'),
    CONSTRAINT ck_brief_snapshots_task
        CHECK (btrim(task_id)=task_id AND octet_length(task_id) BETWEEN 1 AND 255),
    CONSTRAINT ck_brief_snapshots_request_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_brief_snapshots_payload_digest
        CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_brief_snapshots_payload_size
        CHECK (octet_length(payload) BETWEEN 2 AND 33554432),
    CONSTRAINT ck_brief_snapshots_insight_count
        CHECK (insight_count BETWEEN 1 AND 100)
);

CREATE INDEX idx_brief_snapshots_tenant_user_task_generated
    ON brief_snapshots (
        tenant_id,user_id,task_id,generated_at DESC,id DESC
    );

-- Snapshot admission is exposed through an exact-scope definer rather than
-- direct writer SELECT plus a new policy. Migration 061 therefore performs no
-- DDL on task_run_snapshots while holding the schema advisory fence.
-- +goose StatementBegin
CREATE FUNCTION read_canonical_brief_run_identity_v1(
    target_run_snapshot_id BIGINT
)
RETURNS TABLE (
    task_id TEXT,
    temporal_workflow_id TEXT,
    temporal_run_id TEXT,
    reference_schema_version TEXT,
    reference_digest TEXT,
    payload_digest TEXT
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
    RETURN QUERY
    SELECT rs.task_id,rs.temporal_workflow_id,rs.temporal_run_id,
           rs.reference_schema_version,rs.reference_digest,rs.payload_digest
      FROM public.task_run_snapshots rs
     WHERE rs.id=target_run_snapshot_id
       AND rs.tenant_id IS NOT DISTINCT FROM
           NULLIF(current_setting('app.tenant_id',true),'')::bigint
       AND rs.user_id IS NOT DISTINCT FROM
           NULLIF(current_setting('app.user_id',true),'')::bigint;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_canonical_brief_run_identity_v1(BIGINT)
    FROM PUBLIC;

-- Expose only evidence reachable from an exact tenant/user/run batch. The
-- global content tables intentionally have no tenant RLS, so the writer never
-- receives direct SELECT on them. Source title comes from the immutable run
-- snapshot; item title/URL/publication time come from durable content evidence.
-- If content appeared in multiple frozen sources, the earliest appearance
-- existing when the delivery was created is the deterministic provenance.
-- +goose StatementBegin
CREATE FUNCTION read_canonical_brief_delivery_evidence_v1(
    target_batch_id BIGINT,
    target_run_snapshot_id BIGINT
)
RETURNS TABLE (
    delivery_id BIGINT,
    evidence_complete BOOLEAN,
    body_md TEXT,
    discovered_at TIMESTAMPTZ,
    content_title TEXT,
    canonical_url TEXT,
    published_at TIMESTAMPTZ,
    source_title TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT d.id,
           ci.id IS NOT NULL AND matched_source.source_title IS NOT NULL,
           d.body_md,d.created_at,
           COALESCE(ci.title,''),COALESCE(ci.url,''),ci.published_at,
           COALESCE(matched_source.source_title,'')
      FROM public.deliveries d
      JOIN public.push_batches b
        ON b.id=d.batch_id
       AND b.tenant_id=d.tenant_id
       AND b.user_id=d.user_id
      JOIN public.task_run_snapshots rs
        ON rs.id=b.run_snapshot_id
       AND rs.tenant_id=b.tenant_id
       AND rs.user_id=b.user_id
       AND rs.task_id=b.schedule_id
      LEFT JOIN public.content_items ci ON ci.id=d.content_item_id
      LEFT JOIN LATERAL (
          SELECT frozen_source.value->>'title' AS source_title
            FROM public.content_sources cs
            JOIN LATERAL jsonb_array_elements(
                convert_from(rs.payload,'UTF8')::jsonb
                    #> '{definition,sources}'
            ) AS frozen_source(value)
              ON (frozen_source.value->>'source_id')::bigint=cs.source_id
           WHERE cs.content_item_id=ci.id
             AND cs.first_seen_at<=d.created_at
           ORDER BY cs.first_seen_at,cs.source_id
           LIMIT 1
      ) AS matched_source ON true
     WHERE b.id=target_batch_id
       AND rs.id=target_run_snapshot_id
       AND b.tenant_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
       AND b.user_id IS NOT DISTINCT FROM
           NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
     ORDER BY d.id
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION read_canonical_brief_delivery_evidence_v1(BIGINT,BIGINT)
    FROM PUBLIC;

-- Dedicated dark writer. It is unrelated to vane_app and can only select the
-- immutable run/batch scope, execute the constrained evidence reader, and
-- write these two new tables.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_brief_writer') THEN
        BEGIN
            CREATE ROLE vane_brief_writer
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_brief_writer
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_brief_writer RESET ALL;
ALTER ROLE vane_brief_writer SET search_path=pg_catalog,public,pg_temp;
GRANT vane_brief_writer TO CURRENT_USER;

-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role('vane_brief_writer','vane_app','MEMBER') OR
       pg_has_role('vane_app','vane_brief_writer','MEMBER') THEN
        RAISE EXCEPTION '061: vane_app and brief writer must be unrelated';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_brief_writer'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION '061: only migration owner may enter brief writer';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_brief_writer'
    ) THEN
        RAISE EXCEPTION '061: brief writer must not enter another role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles r ON r.oid=dep.refobjid
         WHERE r.rolname='vane_brief_writer'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='o'
           AND (
               dep.dbid=0 OR dep.dbid=(
                   SELECT oid FROM pg_database
                    WHERE datname=current_database()
               )
           )
    ) THEN
        RAISE EXCEPTION
            '061: brief writer must not own database objects';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_shdepend dep
          JOIN pg_roles r ON r.oid=dep.refobjid
         WHERE r.rolname='vane_brief_writer'
           AND dep.refclassid='pg_authid'::regclass
           AND dep.deptype='a'
           AND (
               dep.dbid=(
                   SELECT oid FROM pg_database
                    WHERE datname=current_database()
               ) OR (
                   dep.dbid=0
                   AND dep.classid='pg_database'::regclass
                   AND dep.objid=(
                       SELECT oid FROM pg_database
                        WHERE datname=current_database()
                   )
               )
           )
    ) THEN
        RAISE EXCEPTION
            '061: brief writer has preexisting ACL in this database';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_parameter_acl parameter_acl
         WHERE has_parameter_privilege(
                   'vane_brief_writer',parameter_acl.parname,'SET'
               )
            OR has_parameter_privilege(
                   'vane_brief_writer',parameter_acl.parname,'ALTER SYSTEM'
               )
    ) THEN
        RAISE EXCEPTION
            '061: brief writer has unsafe cluster parameter ACL';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON task_run_outcomes,brief_snapshots
    FROM PUBLIC,vane_app,vane_brief_writer;
REVOKE ALL ON SEQUENCE task_run_outcomes_id_seq,brief_snapshots_id_seq
    FROM PUBLIC,vane_app,vane_brief_writer;
REVOKE ALL ON task_run_snapshots,push_batches,deliveries
    FROM vane_brief_writer;

GRANT USAGE ON SCHEMA public TO vane_brief_writer;
GRANT SELECT (
    id,tenant_id,user_id,schedule_id,run_snapshot_id,brief_state
) ON push_batches TO vane_brief_writer;
GRANT UPDATE (brief_state) ON push_batches TO vane_brief_writer;

GRANT SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,schema_version,status,
    result,source_coverage,processing,failure_code,failure_message,
    finalized_at,outcome_digest,created_at
), INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,schema_version
), UPDATE (
    status,result,source_coverage,processing,failure_code,failure_message,
    finalized_at,outcome_digest
) ON task_run_outcomes TO vane_brief_writer;
GRANT USAGE,SELECT ON SEQUENCE task_run_outcomes_id_seq
    TO vane_brief_writer;

GRANT SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload_digest,payload,
    insight_count,generated_at,created_at
), INSERT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload_digest,payload,
    insight_count,generated_at
) ON brief_snapshots TO vane_brief_writer;
GRANT USAGE,SELECT ON SEQUENCE brief_snapshots_id_seq
    TO vane_brief_writer;
GRANT EXECUTE ON FUNCTION
    read_canonical_brief_run_identity_v1(BIGINT)
    TO vane_brief_writer;
GRANT EXECUTE ON FUNCTION
    read_canonical_brief_delivery_evidence_v1(BIGINT,BIGINT)
    TO vane_brief_writer;

ALTER TABLE task_run_outcomes ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON task_run_outcomes
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON task_run_outcomes AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );
CREATE POLICY user_isolation ON task_run_outcomes AS RESTRICTIVE
    FOR ALL
    USING (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

ALTER TABLE brief_snapshots ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON brief_snapshots
    FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tenant_isolation ON brief_snapshots AS RESTRICTIVE
    FOR ALL
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );
CREATE POLICY user_isolation ON brief_snapshots AS RESTRICTIVE
    FOR ALL
    USING (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- The existing batch table already has tenant RLS. This role-specific
-- restrictive policy also prevents same-tenant cross-user scope adoption.
CREATE POLICY brief_writer_identity ON push_batches AS RESTRICTIVE
    FOR ALL TO vane_brief_writer
    USING (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
        user_id IS NOT DISTINCT FROM
        NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);

-- Producers acquire deliveries before the batch row (the open-batch trigger).
-- Preserve that order so an already-admitted legacy INSERT can finish before
-- this downgrade fence inspects durable evidence.
LOCK TABLE task_run_outcomes,deliveries,push_batches,brief_snapshots
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM brief_snapshots) OR
       EXISTS (SELECT 1 FROM task_run_outcomes) OR
       EXISTS (SELECT 1 FROM push_batches WHERE brief_state='sealed') THEN
        RAISE EXCEPTION
            '061: refusing downgrade while canonical outcome/brief evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

DROP POLICY IF EXISTS brief_writer_identity ON push_batches;

DROP POLICY IF EXISTS user_isolation ON brief_snapshots;
DROP POLICY IF EXISTS tenant_isolation ON brief_snapshots;
DROP POLICY IF EXISTS tenant_visible ON brief_snapshots;
DROP POLICY IF EXISTS user_isolation ON task_run_outcomes;
DROP POLICY IF EXISTS tenant_isolation ON task_run_outcomes;
DROP POLICY IF EXISTS tenant_visible ON task_run_outcomes;

REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload_digest,payload,
    insight_count,generated_at,created_at
), INSERT (
    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
    push_batch_id,schema_version,request_digest,payload_digest,payload,
    insight_count,generated_at
) ON brief_snapshots FROM vane_brief_writer;
REVOKE USAGE,SELECT ON SEQUENCE brief_snapshots_id_seq
    FROM vane_brief_writer;
REVOKE SELECT (
    id,tenant_id,user_id,task_id,run_snapshot_id,schema_version,status,
    result,source_coverage,processing,failure_code,failure_message,
    finalized_at,outcome_digest,created_at
), INSERT (
    tenant_id,user_id,task_id,run_snapshot_id,schema_version
), UPDATE (
    status,result,source_coverage,processing,failure_code,failure_message,
    finalized_at,outcome_digest
) ON task_run_outcomes FROM vane_brief_writer;
REVOKE USAGE,SELECT ON SEQUENCE task_run_outcomes_id_seq
    FROM vane_brief_writer;
REVOKE SELECT (
    id,tenant_id,user_id,schedule_id,run_snapshot_id,brief_state
) ON push_batches FROM vane_brief_writer;
REVOKE UPDATE (brief_state) ON push_batches FROM vane_brief_writer;
REVOKE EXECUTE ON FUNCTION
    read_canonical_brief_run_identity_v1(BIGINT)
    FROM vane_brief_writer;
REVOKE EXECUTE ON FUNCTION
    read_canonical_brief_delivery_evidence_v1(BIGINT,BIGINT)
    FROM vane_brief_writer;
REVOKE USAGE ON SCHEMA public FROM vane_brief_writer;

DROP FUNCTION read_canonical_brief_delivery_evidence_v1(BIGINT,BIGINT);
DROP FUNCTION read_canonical_brief_run_identity_v1(BIGINT);

DROP TABLE brief_snapshots;
DROP TRIGGER task_run_outcomes_one_way_finalization_v1
    ON task_run_outcomes;
DROP FUNCTION enforce_run_outcome_transition_v1();
DROP TRIGGER task_run_outcomes_snapshot_scope_v1
    ON task_run_outcomes;
DROP FUNCTION enforce_run_outcome_snapshot_scope_v1();
DROP TABLE task_run_outcomes;

DROP TRIGGER deliveries_require_open_brief_batch_v1 ON deliveries;
DROP FUNCTION enforce_open_brief_batch_v1();
DROP TRIGGER push_batches_brief_state_authority_v1 ON push_batches;
DROP FUNCTION enforce_brief_batch_state_authority_v1();

ALTER TABLE deliveries
    DROP CONSTRAINT fk_deliveries_brief_batch_scope;
ALTER TABLE push_batches
    DROP CONSTRAINT uq_push_batches_brief_scope,
    DROP CONSTRAINT uq_push_batches_delivery_scope,
    DROP CONSTRAINT ck_push_batches_brief_state,
    DROP COLUMN brief_state;
