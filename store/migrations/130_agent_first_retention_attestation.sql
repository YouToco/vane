-- 130: append-only evidence for the Agent-first Temporal retention gate.
--
-- This migration changes no runtime authority. It only gives a direct schema
-- owner a durable, database-witnessed chain on which a later migration can
-- rely. In particular it does not freeze V1 creation/protocol-1 edit rows,
-- change server-runtime memberships, or grant a new application capability.

-- +goose Up

CREATE TABLE agent_first_retention_attestation_events (
    id                          BIGSERIAL   PRIMARY KEY,
    phase                       TEXT        NOT NULL,
    parent_digest               TEXT,
    temporal_cluster_id         TEXT        NOT NULL,
    temporal_namespace          TEXT        NOT NULL,
    temporal_namespace_id       TEXT        NOT NULL,
    retention_seconds           BIGINT      NOT NULL,
    history_archival_state      TEXT        NOT NULL,
    history_archive_uri_digest  TEXT        NOT NULL,
    visibility_archival_state   TEXT        NOT NULL,
    visibility_archive_uri_digest TEXT      NOT NULL,
    temporal_server_witness     TIMESTAMPTZ NOT NULL,
    workflow_inventory_digest   TEXT        NOT NULL,
    schedule_inventory_digest   TEXT        NOT NULL,
    archive_inventory_digest    TEXT        NOT NULL,
    temporal_evidence_digest    TEXT        NOT NULL,
    source_revision             TEXT        NOT NULL,
    deploy_digest               TEXT        NOT NULL,
    database_identity           BYTEA       NOT NULL,
    legacy_db_snapshot          BYTEA       NOT NULL,
    legacy_db_snapshot_digest   TEXT        NOT NULL,
    canonical_payload           BYTEA       NOT NULL,
    payload_digest              TEXT        NOT NULL,
    issued_at                   TIMESTAMPTZ NOT NULL,
    expires_at                  TIMESTAMPTZ NOT NULL,

    CONSTRAINT ck_agent_first_retention_phase CHECK (
        phase IN ('baseline','prepared')),
    CONSTRAINT ck_agent_first_retention_parent CHECK (
        (phase='baseline' AND parent_digest IS NULL) OR
        (phase='prepared' AND parent_digest ~ '^[0-9a-f]{64}$')),
    CONSTRAINT ck_agent_first_retention_temporal_identity CHECK (
        btrim(temporal_cluster_id)=temporal_cluster_id AND
        octet_length(temporal_cluster_id) BETWEEN 1 AND 512 AND
        btrim(temporal_namespace)=temporal_namespace AND
        octet_length(temporal_namespace) BETWEEN 1 AND 255 AND
        temporal_namespace_id ~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND
        retention_seconds BETWEEN 1 AND 315360000),
    CONSTRAINT ck_agent_first_retention_history_archive CHECK (
        history_archive_uri_digest ~ '^[0-9a-f]{64}$' AND (
        (history_archival_state='disabled' AND
         history_archive_uri_digest=
           'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855') OR
        (history_archival_state='enabled' AND
         history_archive_uri_digest<>
           'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'))),
    CONSTRAINT ck_agent_first_retention_visibility_archive CHECK (
        visibility_archive_uri_digest ~ '^[0-9a-f]{64}$' AND (
        (visibility_archival_state='disabled' AND
         visibility_archive_uri_digest=
           'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855') OR
        (visibility_archival_state='enabled' AND
         visibility_archive_uri_digest<>
           'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'))),
    CONSTRAINT ck_agent_first_retention_external_digests CHECK (
        workflow_inventory_digest ~ '^[0-9a-f]{64}$' AND
        schedule_inventory_digest ~ '^[0-9a-f]{64}$' AND
        archive_inventory_digest ~ '^[0-9a-f]{64}$' AND
        temporal_evidence_digest ~ '^[0-9a-f]{64}$' AND
        deploy_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_agent_first_retention_source_revision CHECK (
        source_revision ~ '^[0-9a-f]{40}$'),
    CONSTRAINT ck_agent_first_retention_database_evidence CHECK (
        octet_length(database_identity) BETWEEN 2 AND 4096 AND
        database_identity=
            convert_to((convert_from(database_identity,'UTF8')::jsonb)::text,'UTF8') AND
        octet_length(legacy_db_snapshot) BETWEEN 2 AND 65536 AND
        legacy_db_snapshot_digest ~ '^[0-9a-f]{64}$' AND
        legacy_db_snapshot_digest=encode(sha256(legacy_db_snapshot),'hex')),
    CONSTRAINT ck_agent_first_retention_payload CHECK (
        octet_length(canonical_payload) BETWEEN 2 AND 131072 AND
        payload_digest ~ '^[0-9a-f]{64}$' AND
        payload_digest=encode(sha256(canonical_payload),'hex') AND
        canonical_payload=
            convert_to((convert_from(canonical_payload,'UTF8')::jsonb)::text,'UTF8')),
    CONSTRAINT ck_agent_first_retention_time CHECK (
        isfinite(temporal_server_witness) AND isfinite(issued_at) AND
        isfinite(expires_at) AND expires_at>issued_at),
    CONSTRAINT uq_agent_first_retention_payload UNIQUE (payload_digest),
    CONSTRAINT fk_agent_first_retention_parent FOREIGN KEY (parent_digest)
        REFERENCES agent_first_retention_attestation_events (payload_digest)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX uq_agent_first_retention_child
    ON agent_first_retention_attestation_events(parent_digest)
    WHERE parent_digest IS NOT NULL;
CREATE INDEX idx_agent_first_retention_namespace_phase
    ON agent_first_retention_attestation_events(
        temporal_cluster_id,temporal_namespace,temporal_namespace_id,phase,id DESC);

-- The snapshot is intentionally semantic, not pg_current_snapshot(): it
-- summarizes every legacy lane that must be quiescent and hashes the current
-- schedule/authority projections in stable identity order. Migration 131 can
-- recompute these exact bytes in its freeze transaction.
-- +goose StatementBegin
CREATE FUNCTION agent_first_legacy_db_snapshot_v130() RETURNS BYTEA
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path=pg_catalog,public,pg_temp
AS $$
WITH creation_operations AS (
    SELECT
      count(*) AS operation_count,
      count(*) FILTER (WHERE status IN ('pending','executing')) AS active_count,
      count(*) FILTER (WHERE lease_owner<>'' OR lease_until IS NOT NULL OR
                            takeover_not_before IS NOT NULL) AS lease_count,
      count(*) FILTER (WHERE status NOT IN ('pending','executing') AND
                            NOT EXISTS (
                              SELECT 1 FROM public.task_creation_receipts receipt
                               WHERE receipt.operation_id=operation.id)) AS receipt_gap_count
      ,encode(sha256(convert_to(COALESCE(jsonb_agg(to_jsonb(operation)
          ORDER BY tenant_id,user_id,id)::text,'[]'),'UTF8')),'hex') AS operation_digest
      FROM public.task_creation_operations operation
     WHERE execution_version=1 AND tool_name='create_schedule'
), creation_receipts AS (
    SELECT count(*) AS receipt_count,
           count(*) FILTER (WHERE status='pending') AS pending_receipt_count,
           count(*) FILTER (WHERE lease_owner<>'' OR lease_until IS NOT NULL OR
                                  takeover_not_before IS NOT NULL) AS receipt_lease_count,
           encode(sha256(convert_to(COALESCE(jsonb_agg(to_jsonb(receipt)
             ORDER BY tenant_id,user_id,operation_id,id)::text,'[]'),'UTF8')),'hex')
             AS receipt_digest
      FROM public.task_creation_receipts receipt
     WHERE EXISTS (SELECT 1 FROM public.task_creation_operations operation
                    WHERE operation.id=receipt.operation_id
                      AND operation.execution_version=1
                      AND operation.tool_name='create_schedule')
), edit_operations AS (
    SELECT
      count(*) AS operation_count,
      count(*) FILTER (WHERE status IN ('pending','executing')) AS active_count,
      count(*) FILTER (WHERE lease_owner<>'' OR lease_until IS NOT NULL OR
                            takeover_not_before IS NOT NULL) AS lease_count,
      count(*) FILTER (WHERE status NOT IN ('pending','executing') AND
                            NOT EXISTS (
                              SELECT 1 FROM public.task_definition_edit_receipts receipt
                               WHERE receipt.operation_id=operation.id)) AS receipt_gap_count
      ,encode(sha256(convert_to(COALESCE(jsonb_agg(to_jsonb(operation)
          ORDER BY tenant_id,user_id,id)::text,'[]'),'UTF8')),'hex') AS operation_digest
      FROM public.task_definition_edit_operations operation
     WHERE operation_protocol=1
), edit_receipts AS (
    SELECT count(*) AS receipt_count,
           count(*) FILTER (WHERE status='pending') AS pending_receipt_count,
           count(*) FILTER (WHERE lease_owner<>'' OR lease_until IS NOT NULL OR
                                  takeover_not_before IS NOT NULL) AS receipt_lease_count,
           encode(sha256(convert_to(COALESCE(jsonb_agg(to_jsonb(receipt)
             ORDER BY tenant_id,user_id,operation_id,id)::text,'[]'),'UTF8')),'hex')
             AS receipt_digest
      FROM public.task_definition_edit_receipts receipt
     WHERE EXISTS (SELECT 1 FROM public.task_definition_edit_operations operation
                    WHERE operation.id=receipt.operation_id
                      AND operation.operation_protocol=1)
), schedule_inventory AS (
    SELECT count(*) AS item_count,
           encode(sha256(convert_to(COALESCE(jsonb_agg(
             jsonb_build_object(
               'id',id,'tenant_id',tenant_id,'user_id',user_id,'status',status,
               'execution_mode',execution_mode,
               'approved_definition_version',approved_definition_version,
               'approved_definition_digest',approved_definition_digest,
               'definition_edit_operation_id',definition_edit_operation_id,
               'definition_edit_fence',definition_edit_fence)
             ORDER BY tenant_id,user_id,id)::text,'[]'),'UTF8')),'hex') AS digest
      FROM public.schedules
), authority_inventory AS (
    SELECT count(*) AS item_count,
           encode(sha256(convert_to(COALESCE(jsonb_agg(
             jsonb_build_object(
               'tenant_id',tenant_id,'user_id',user_id,'task_id',task_id,
               'generation',generation,'definition_version',definition_version,
               'definition_digest',definition_digest,
               'target_action_digest',target_action_digest,
               'action_authorization_digest',action_authorization_digest,
               'status',status)
             ORDER BY tenant_id,user_id,task_id,generation)::text,'[]'),'UTF8')),'hex') AS digest
      FROM public.research_v3_delivery_authorities
)
SELECT convert_to(jsonb_build_object(
    'authority_inventory',jsonb_build_object(
       'digest',authority_inventory.digest,'item_count',authority_inventory.item_count),
    'legacy_creation',jsonb_build_object(
       'active_count',creation_operations.active_count,
       'lease_count',creation_operations.lease_count,
       'operation_count',creation_operations.operation_count,
       'operation_digest',creation_operations.operation_digest,
       'pending_receipt_count',creation_receipts.pending_receipt_count,
       'receipt_count',creation_receipts.receipt_count,
       'receipt_digest',creation_receipts.receipt_digest,
       'receipt_lease_count',creation_receipts.receipt_lease_count,
       'receipt_gap_count',creation_operations.receipt_gap_count),
    'protocol1_definition_edit',jsonb_build_object(
       'active_count',edit_operations.active_count,
       'lease_count',edit_operations.lease_count,
       'operation_count',edit_operations.operation_count,
       'operation_digest',edit_operations.operation_digest,
       'pending_receipt_count',edit_receipts.pending_receipt_count,
       'receipt_count',edit_receipts.receipt_count,
       'receipt_digest',edit_receipts.receipt_digest,
       'receipt_lease_count',edit_receipts.receipt_lease_count,
       'receipt_gap_count',edit_operations.receipt_gap_count),
    'schedule_inventory',jsonb_build_object(
       'digest',schedule_inventory.digest,'item_count',schedule_inventory.item_count),
    'schema_version','vane.agent-first-legacy-db-snapshot/v130'
)::text,'UTF8')
FROM creation_operations,creation_receipts,edit_operations,edit_receipts,
     schedule_inventory,authority_inventory
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION agent_first_legacy_db_snapshot_v130() FROM PUBLIC;

-- +goose StatementBegin
CREATE FUNCTION reject_agent_first_retention_history_mutation_v130()
RETURNS TRIGGER LANGUAGE plpgsql
SET search_path=pg_catalog,public AS $$
BEGIN
    RAISE EXCEPTION '130: Agent-first retention attestation is append-only'
        USING ERRCODE='23514';
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION reject_agent_first_retention_history_mutation_v130()
    FROM PUBLIC;

CREATE TRIGGER agent_first_retention_history_immutable_v130
BEFORE UPDATE OR DELETE ON agent_first_retention_attestation_events
FOR EACH ROW EXECUTE FUNCTION reject_agent_first_retention_history_mutation_v130();
CREATE TRIGGER agent_first_retention_history_no_truncate_v130
BEFORE TRUNCATE ON agent_first_retention_attestation_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_agent_first_retention_history_mutation_v130();
ALTER TABLE agent_first_retention_attestation_events
    ENABLE ALWAYS TRIGGER agent_first_retention_history_immutable_v130;
ALTER TABLE agent_first_retention_attestation_events
    ENABLE ALWAYS TRIGGER agent_first_retention_history_no_truncate_v130;

-- The only append primitive is callable by the direct schema owner. Database
-- identity, DB snapshot, issue/expiry times, canonical bytes and digests are
-- recomputed inside the function and cannot be supplied by the caller.
-- +goose StatementBegin
CREATE FUNCTION append_agent_first_retention_attestation_v130(
    requested_phase TEXT,
    requested_parent_digest TEXT,
    requested_temporal_cluster_id TEXT,
    requested_temporal_namespace TEXT,
    requested_temporal_namespace_id TEXT,
    requested_retention_seconds BIGINT,
    requested_history_archival_state TEXT,
    requested_history_archive_uri_digest TEXT,
    requested_visibility_archival_state TEXT,
    requested_visibility_archive_uri_digest TEXT,
    requested_temporal_server_witness TIMESTAMPTZ,
    requested_workflow_inventory_digest TEXT,
    requested_schedule_inventory_digest TEXT,
    requested_archive_inventory_digest TEXT,
    requested_temporal_evidence_digest TEXT,
    requested_source_revision TEXT,
    requested_deploy_digest TEXT
) RETURNS SETOF agent_first_retention_attestation_events
LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
    database_now TIMESTAMPTZ;
    database_system_identifier TEXT;
    database_name TEXT:=current_database();
    database_oid_value OID;
    database_identity_value BYTEA;
    snapshot_payload BYTEA;
    snapshot_digest TEXT;
    payload BYTEA;
    digest TEXT;
    expires TIMESTAMPTZ;
    parent_event public.agent_first_retention_attestation_events%ROWTYPE;
    latest_baseline_digest TEXT;
    inserted public.agent_first_retention_attestation_events%ROWTYPE;
BEGIN
    IF session_user<>current_user THEN
        RAISE EXCEPTION '130: only the direct schema owner may append attestation'
            USING ERRCODE='42501';
    END IF;
    IF requested_phase NOT IN ('baseline','prepared') OR
       requested_temporal_cluster_id IS NULL OR
       btrim(requested_temporal_cluster_id)<>requested_temporal_cluster_id OR
       octet_length(requested_temporal_cluster_id) NOT BETWEEN 1 AND 512 OR
       requested_temporal_namespace IS NULL OR
       btrim(requested_temporal_namespace)<>requested_temporal_namespace OR
       octet_length(requested_temporal_namespace) NOT BETWEEN 1 AND 255 OR
       requested_temporal_namespace_id !~
         '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' OR
       requested_retention_seconds NOT BETWEEN 1 AND 315360000 OR
       requested_temporal_server_witness IS NULL OR
       NOT isfinite(requested_temporal_server_witness) OR
       requested_source_revision !~ '^[0-9a-f]{40}$' OR
       requested_deploy_digest !~ '^[0-9a-f]{64}$' OR
       requested_workflow_inventory_digest !~ '^[0-9a-f]{64}$' OR
       requested_schedule_inventory_digest !~ '^[0-9a-f]{64}$' OR
       requested_archive_inventory_digest !~ '^[0-9a-f]{64}$' OR
       requested_temporal_evidence_digest !~ '^[0-9a-f]{64}$' OR
       requested_history_archival_state<>requested_visibility_archival_state OR
       NOT (((requested_history_archival_state='disabled' AND
              requested_history_archive_uri_digest=
                'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855') OR
             (requested_history_archival_state='enabled' AND
              requested_history_archive_uri_digest ~ '^[0-9a-f]{64}$' AND
              requested_history_archive_uri_digest<>
                'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')) AND
            ((requested_visibility_archival_state='disabled' AND
              requested_visibility_archive_uri_digest=
                'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855') OR
             (requested_visibility_archival_state='enabled' AND
              requested_visibility_archive_uri_digest ~ '^[0-9a-f]{64}$' AND
              requested_visibility_archive_uri_digest<>
                'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'))) THEN
        RAISE EXCEPTION '130: attestation evidence is invalid' USING ERRCODE='23514';
    END IF;

    PERFORM pg_advisory_xact_lock(6215335020355474130);
    database_now:=clock_timestamp();
    SELECT system_identifier::text INTO database_system_identifier
      FROM pg_control_system();
    SELECT oid INTO database_oid_value FROM pg_database WHERE datname=database_name;
    database_identity_value:=convert_to(jsonb_build_object(
        'database_name',database_name,
        'database_oid',database_oid_value::bigint,
        'pg_system_identifier',database_system_identifier,
        'server_version_num',current_setting('server_version_num')::bigint,
        'schema_version','vane.agent-first-database-identity/v130')::text,'UTF8');
    snapshot_payload:=public.agent_first_legacy_db_snapshot_v130();
    snapshot_digest:=encode(sha256(snapshot_payload),'hex');

    -- Temporal supplies this witness from its own server clock. Bind every
    -- observation to this database append, allowing only bounded transport and
    -- clock skew rather than accepting fabricated future/past timestamps.
    IF requested_temporal_server_witness<database_now-interval '10 minutes' OR
       requested_temporal_server_witness>database_now+interval '5 seconds' THEN
        RAISE EXCEPTION '130: Temporal server witness is outside DB clock skew'
            USING ERRCODE='22008';
    END IF;

    IF COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{legacy_creation,active_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{legacy_creation,lease_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{legacy_creation,pending_receipt_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{legacy_creation,receipt_lease_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{legacy_creation,receipt_gap_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{protocol1_definition_edit,active_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{protocol1_definition_edit,lease_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{protocol1_definition_edit,pending_receipt_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{protocol1_definition_edit,receipt_lease_count}')::bigint,-1)<>0 OR
       COALESCE((convert_from(snapshot_payload,'UTF8')::jsonb #>>
          '{protocol1_definition_edit,receipt_gap_count}')::bigint,-1)<>0 THEN
        RAISE EXCEPTION '130: legacy database snapshot is not quiescent'
            USING ERRCODE='55000';
    END IF;

    IF requested_phase='baseline' THEN
        IF requested_parent_digest IS NOT NULL THEN
            RAISE EXCEPTION '130: baseline parent must be null' USING ERRCODE='23514';
        END IF;
        expires:=database_now+make_interval(secs=>requested_retention_seconds)+
                 interval '10 minutes';
    ELSE
        IF requested_parent_digest !~ '^[0-9a-f]{64}$' THEN
            RAISE EXCEPTION '130: attestation parent is invalid' USING ERRCODE='23514';
        END IF;
        SELECT * INTO parent_event
          FROM public.agent_first_retention_attestation_events
         WHERE payload_digest=requested_parent_digest FOR SHARE;
        IF NOT FOUND OR parent_event.temporal_cluster_id<>requested_temporal_cluster_id OR
           parent_event.temporal_namespace<>requested_temporal_namespace THEN
            RAISE EXCEPTION '130: attestation parent scope differs' USING ERRCODE='23514';
        END IF;
        IF parent_event.retention_seconds<>requested_retention_seconds OR
           parent_event.temporal_namespace_id<>requested_temporal_namespace_id OR
           parent_event.history_archival_state<>requested_history_archival_state OR
           parent_event.history_archive_uri_digest<>
             requested_history_archive_uri_digest OR
           parent_event.visibility_archival_state<>requested_visibility_archival_state OR
           parent_event.visibility_archive_uri_digest<>
             requested_visibility_archive_uri_digest THEN
            RAISE EXCEPTION '130: attestation Temporal policy differs from parent'
                USING ERRCODE='23514';
        END IF;
        IF parent_event.source_revision<>requested_source_revision OR
           parent_event.deploy_digest<>requested_deploy_digest THEN
            RAISE EXCEPTION '130: attestation source or deploy differs from parent'
                USING ERRCODE='23514';
        END IF;
        IF parent_event.phase<>'baseline' OR
           parent_event.expires_at<=database_now OR
           parent_event.database_identity<>database_identity_value OR
           requested_temporal_server_witness <
             parent_event.temporal_server_witness+
             make_interval(secs=>requested_retention_seconds) THEN
            RAISE EXCEPTION '130: prepared evidence has not crossed full retention on the same database'
                USING ERRCODE='23514';
        END IF;
        IF (convert_from(parent_event.legacy_db_snapshot,'UTF8')::jsonb->
              'legacy_creation') IS DISTINCT FROM
             (convert_from(snapshot_payload,'UTF8')::jsonb->'legacy_creation') OR
           (convert_from(parent_event.legacy_db_snapshot,'UTF8')::jsonb->
              'protocol1_definition_edit') IS DISTINCT FROM
             (convert_from(snapshot_payload,'UTF8')::jsonb->
              'protocol1_definition_edit') THEN
            RAISE EXCEPTION '130: legacy lane snapshot changed across retention'
                USING ERRCODE='23514';
        END IF;
        SELECT payload_digest INTO latest_baseline_digest
          FROM public.agent_first_retention_attestation_events
         WHERE temporal_cluster_id=requested_temporal_cluster_id
           AND temporal_namespace=requested_temporal_namespace
           AND temporal_namespace_id=requested_temporal_namespace_id
           AND phase='baseline' ORDER BY id DESC LIMIT 1;
        IF latest_baseline_digest IS DISTINCT FROM requested_parent_digest THEN
            RAISE EXCEPTION '130: prepared evidence does not cite latest baseline'
                USING ERRCODE='23514';
        END IF;
        expires:=database_now+interval '10 minutes';
    END IF;

    IF expires<=clock_timestamp() OR
       (requested_phase='prepared' AND
        parent_event.expires_at<=clock_timestamp()) THEN
        RAISE EXCEPTION '130: attestation evidence expired before append'
            USING ERRCODE='22008';
    END IF;

    payload:=convert_to(jsonb_build_object(
      'archive_inventory_digest',requested_archive_inventory_digest,
      'database_identity',convert_from(database_identity_value,'UTF8')::jsonb,
      'deploy_digest',requested_deploy_digest,
      'expires_at',to_char(expires AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'history_archive_uri_digest',requested_history_archive_uri_digest,
      'history_archival_state',requested_history_archival_state,
      'issued_at',to_char(database_now AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'legacy_db_snapshot',convert_from(snapshot_payload,'UTF8')::jsonb,
      'legacy_db_snapshot_digest',snapshot_digest,
      'parent_digest',requested_parent_digest,
      'phase',requested_phase,
      'retention_seconds',requested_retention_seconds,
      'schedule_inventory_digest',requested_schedule_inventory_digest,
      'schema_version','vane.agent-first-retention-attestation/v130',
      'source_revision',requested_source_revision,
      'temporal_cluster_id',requested_temporal_cluster_id,
      'temporal_evidence_digest',requested_temporal_evidence_digest,
      'temporal_namespace',requested_temporal_namespace,
      'temporal_namespace_id',requested_temporal_namespace_id,
      'temporal_server_witness',to_char(
          requested_temporal_server_witness AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'visibility_archive_uri_digest',requested_visibility_archive_uri_digest,
      'visibility_archival_state',requested_visibility_archival_state,
      'workflow_inventory_digest',requested_workflow_inventory_digest
    )::text,'UTF8');
    digest:=encode(sha256(payload),'hex');

    INSERT INTO public.agent_first_retention_attestation_events(
      phase,parent_digest,temporal_cluster_id,temporal_namespace,temporal_namespace_id,
      retention_seconds,history_archival_state,history_archive_uri_digest,
      visibility_archival_state,visibility_archive_uri_digest,temporal_server_witness,
      workflow_inventory_digest,schedule_inventory_digest,
      archive_inventory_digest,temporal_evidence_digest,source_revision,
      deploy_digest,database_identity,legacy_db_snapshot,
      legacy_db_snapshot_digest,canonical_payload,payload_digest,issued_at,expires_at)
    VALUES(requested_phase,requested_parent_digest,requested_temporal_cluster_id,
      requested_temporal_namespace,requested_temporal_namespace_id,
      requested_retention_seconds,requested_history_archival_state,
      requested_history_archive_uri_digest,requested_visibility_archival_state,
      requested_visibility_archive_uri_digest,
      requested_temporal_server_witness,requested_workflow_inventory_digest,
      requested_schedule_inventory_digest,requested_archive_inventory_digest,
      requested_temporal_evidence_digest,requested_source_revision,
      requested_deploy_digest,database_identity_value,snapshot_payload,
      snapshot_digest,payload,digest,database_now,expires)
    RETURNING * INTO inserted;
    RETURN NEXT inserted;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION append_agent_first_retention_attestation_v130(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,
    TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;

REVOKE ALL ON agent_first_retention_attestation_events
    FROM PUBLIC,vane_app;
REVOKE ALL ON SEQUENCE agent_first_retention_attestation_events_id_seq
    FROM PUBLIC,vane_app;

-- +goose Down

LOCK TABLE agent_first_retention_attestation_events IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_first_retention_attestation_events) THEN
        RAISE EXCEPTION '130: refusing downgrade while retention attestations exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP FUNCTION append_agent_first_retention_attestation_v130(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,
    TEXT,TEXT,TEXT,TEXT,TEXT,TEXT);
DROP TRIGGER agent_first_retention_history_no_truncate_v130
    ON agent_first_retention_attestation_events;
DROP TRIGGER agent_first_retention_history_immutable_v130
    ON agent_first_retention_attestation_events;
DROP FUNCTION reject_agent_first_retention_history_mutation_v130();
DROP FUNCTION agent_first_legacy_db_snapshot_v130();
DROP TABLE agent_first_retention_attestation_events;
