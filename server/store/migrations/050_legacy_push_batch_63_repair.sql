-- 050: one-shot, physically pinned repair for legacy failed push batch 63.
--
-- No application role can call this control plane. The only write transition
-- that creates a push effect also appends the finalized adjudication in the
-- same transaction, so recovery can never observe an unaudited effect.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474248);

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname='vane_legacy_batch63_repair'
    ) THEN
        BEGIN
            CREATE ROLE vane_legacy_batch63_repair
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_legacy_batch63_repair
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
GRANT vane_legacy_batch63_repair TO CURRENT_USER;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE granted_role.rolname='vane_legacy_batch63_repair'
           AND member_role.rolname<>CURRENT_USER
    ) OR EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_legacy_batch63_repair'
    ) THEN
        RAISE EXCEPTION
            '050: legacy batch 63 repair role has an unsafe membership graph';
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE legacy_batch63_repair_events (
    id                     BIGSERIAL   PRIMARY KEY,
    batch_id               BIGINT      NOT NULL DEFAULT 63 CHECK (batch_id=63),
    phase                  TEXT        NOT NULL
        CHECK (phase IN ('finalized','blocked')),
    plan_digest            TEXT        NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
    material_digest        TEXT        NOT NULL CHECK (material_digest ~ '^[0-9a-f]{64}$'),
    evidence_digest        TEXT        NOT NULL CHECK (
        evidence_digest=
        '80bcb17806bf55d8a7d9628663a6fa16d35d9264b6be055a353cf7410774b4c3'
    ),
    evidence_class         TEXT        NOT NULL,
    service_revision       TEXT        NOT NULL,
    effect_id              TEXT        NOT NULL,
    canonical_payload      BYTEA       NOT NULL,
    payload_digest         TEXT        NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    card_digest            TEXT        NOT NULL CHECK (card_digest ~ '^[0-9a-f]{64}$'),
    idempotency_expires_at TIMESTAMPTZ NOT NULL,
    enable_by              TIMESTAMPTZ NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT legacy_batch63_repair_phase_once UNIQUE (batch_id,phase),
    CONSTRAINT legacy_batch63_repair_identity_valid CHECK (
        effect_id<>'' AND evidence_class=
            'journald_nil_client_before_provider_call/v1' AND
        service_revision ~ '^[0-9a-f]{40}$' AND
        octet_length(canonical_payload) BETWEEN 1 AND 3145728 AND
        payload_digest=encode(sha256(canonical_payload),'hex') AND
        (
          phase='blocked' OR
          (
            idempotency_expires_at>created_at AND
            idempotency_expires_at<=created_at+interval '1 hour'
          )
        ) AND
        enable_by<=idempotency_expires_at
    )
);

ALTER TABLE legacy_batch63_repair_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY legacy_batch63_repair_existing_visibility
    ON legacy_batch63_repair_events FOR SELECT USING (true);
CREATE POLICY legacy_batch63_repair_role_visibility
    ON legacy_batch63_repair_events AS RESTRICTIVE
    FOR ALL TO vane_legacy_batch63_repair USING (batch_id=63)
    WITH CHECK (batch_id=63);

REVOKE ALL ON legacy_batch63_repair_events FROM PUBLIC,vane_app;
REVOKE ALL ON SEQUENCE legacy_batch63_repair_events_id_seq FROM PUBLIC,vane_app;
GRANT USAGE ON SCHEMA public TO vane_legacy_batch63_repair;

-- The function deliberately has no batch argument. Every predicate and value
-- is a SQL literal 63. Exact immutable effect bytes are supplied by the
-- operator after Go canonicalization; the function repeats every aggregate
-- fence under one lock before making the effect visible.
-- +goose StatementBegin
CREATE FUNCTION finalize_legacy_push_batch_63_v1(
    expected_plan_digest TEXT,
    expected_material_digest TEXT,
    expected_evidence_digest TEXT,
    expected_evidence_class TEXT,
    expected_service_revision TEXT,
    expected_tenant_id BIGINT,
    expected_user_id BIGINT,
    expected_task_id TEXT,
    expected_snapshot_id BIGINT,
    expected_run_id TEXT,
    expected_effect_id TEXT,
    expected_delivery_ids BIGINT[],
    expected_canonical_payload BYTEA,
    expected_payload_digest TEXT,
    expected_card_payload BYTEA,
    expected_card_digest TEXT,
    expected_provider TEXT,
    expected_app_identity TEXT,
    expected_provider_chat_id TEXT,
    expected_target TEXT,
    expected_provider_uuid UUID,
    expected_expires_at TIMESTAMPTZ
)
RETURNS TABLE(phase TEXT, enable_by TIMESTAMPTZ)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    existing_event public.legacy_batch63_repair_events%ROWTYPE;
    batch_row public.push_batches%ROWTYPE;
    actual_delivery_ids BIGINT[];
    canonical_wire JSONB;
    canonical_delivery_ids BIGINT[];
    canonical_card BYTEA;
    feishu_setting JSONB;
    owner_setting JSONB;
    database_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    PERFORM pg_advisory_xact_lock(6215335020355474248);
    SELECT value INTO feishu_setting FROM public.settings
     WHERE key='feishu' FOR SHARE;
    SELECT value INTO owner_setting FROM public.settings
     WHERE key='feishu_owner' FOR SHARE;
    SELECT * INTO batch_row FROM public.push_batches
     WHERE id=63 FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION '050: physical batch 63 is absent';
    END IF;

    SELECT * INTO existing_event
      FROM public.legacy_batch63_repair_events repair_event
     WHERE repair_event.batch_id=63
       AND repair_event.phase='finalized';
    IF FOUND THEN
        IF existing_event.plan_digest<>expected_plan_digest OR
           existing_event.material_digest<>expected_material_digest OR
           existing_event.evidence_digest<>expected_evidence_digest OR
           existing_event.effect_id<>expected_effect_id OR
           existing_event.payload_digest<>expected_payload_digest OR
           existing_event.card_digest<>expected_card_digest OR
           existing_event.idempotency_expires_at<>expected_expires_at THEN
            RAISE EXCEPTION '050: finalized replay drift';
        END IF;
        RETURN QUERY SELECT 'finalized'::TEXT,existing_event.enable_by;
        RETURN;
    END IF;

    IF database_now+interval '45 minutes'>expected_expires_at OR
       expected_expires_at>database_now+interval '1 hour' THEN
        RAISE EXCEPTION '050: unsafe provider idempotency window';
    END IF;
    IF expected_tenant_id<>1 OR expected_user_id<>1 OR
       expected_task_id<>
         'task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063' OR
       expected_snapshot_id<>3 OR
       expected_run_id<>'019f95d0-bc42-7ce4-be0d-067c2ed6bdc2' OR
       expected_effect_id<>'05daa6d9-8044-59f7-9935-c595533ecb4c' OR
       expected_delivery_ids<>ARRAY[202,203,204,205,206]::BIGINT[] OR
       batch_row.status<>'failed' OR batch_row.delivery_authority IS NOT NULL OR
       batch_row.tenant_id<>expected_tenant_id OR
       batch_row.user_id<>expected_user_id OR
       batch_row.run_snapshot_id<>expected_snapshot_id THEN
        RAISE EXCEPTION '050: batch state or immutable scope drifted';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.task_run_snapshots s
         WHERE s.id=3
           AND s.tenant_id=expected_tenant_id
           AND s.user_id=expected_user_id
           AND s.task_id=
               'task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063'
           AND s.task_id=expected_task_id
           AND s.temporal_run_id='019f95d0-bc42-7ce4-be0d-067c2ed6bdc2'
           AND s.temporal_run_id=expected_run_id
           AND s.temporal_workflow_id=
               'wf-task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063-2026-07-24T20:28:32Z'
    ) THEN
        RAISE EXCEPTION '050: run snapshot scope drifted';
    END IF;
    PERFORM 1 FROM public.deliveries d
     WHERE d.batch_id=63
       AND d.tenant_id=expected_tenant_id
       AND d.user_id=expected_user_id
       AND d.status='pending'
       AND d.feishu_message_id=''
       AND d.sent_at IS NULL
       AND d.card_json='{}'::jsonb
     ORDER BY d.id
     FOR UPDATE;
    SELECT array_agg(d.id ORDER BY d.id) INTO actual_delivery_ids
      FROM public.deliveries d
     WHERE d.batch_id=63
       AND d.tenant_id=expected_tenant_id
       AND d.user_id=expected_user_id
       AND d.status='pending'
       AND d.feishu_message_id=''
       AND d.sent_at IS NULL
       AND d.card_json='{}'::jsonb;
    IF cardinality(actual_delivery_ids) IS DISTINCT FROM 5 OR
       actual_delivery_ids IS DISTINCT FROM
           ARRAY[202,203,204,205,206]::BIGINT[] OR
       actual_delivery_ids IS DISTINCT FROM expected_delivery_ids OR
       (
         SELECT array_agg(d.content_item_id ORDER BY d.id)
           FROM public.deliveries d WHERE d.batch_id=63
       ) IS DISTINCT FROM ARRAY[1715,1710,1775,1713,1708]::BIGINT[] OR
       (SELECT count(*) FROM public.deliveries WHERE batch_id=63)<>5 OR
       EXISTS (SELECT 1 FROM public.push_effects WHERE batch_id=63) OR
       EXISTS (
           SELECT 1 FROM public.task_observed_events
            WHERE delivery_id=ANY(expected_delivery_ids)
       ) THEN
        RAISE EXCEPTION '050: exact aggregate drifted';
    END IF;
    IF expected_evidence_class<>
       'journald_nil_client_before_provider_call/v1' OR
       expected_evidence_digest<>
           '80bcb17806bf55d8a7d9628663a6fa16d35d9264b6be055a353cf7410774b4c3' OR
       expected_service_revision<>
           '5a82b1350aba467189ba36a90105f6de3d4d65e4' OR
       expected_payload_digest<>encode(sha256(expected_canonical_payload),'hex') OR
       expected_card_digest<>encode(sha256(expected_card_payload),'hex') OR
       expected_provider<>'feishu' OR expected_effect_id<>expected_provider_uuid::text THEN
        RAISE EXCEPTION '050: immutable effect or evidence is invalid';
    END IF;
    IF feishu_setting IS NULL OR owner_setting IS NULL OR
       jsonb_typeof(feishu_setting) IS DISTINCT FROM 'object' OR
       jsonb_typeof(owner_setting) IS DISTINCT FROM 'object' OR
       (feishu_setting->>'app_id') IS DISTINCT FROM expected_app_identity OR
       (feishu_setting->>'enabled')::BOOLEAN IS DISTINCT FROM TRUE OR
       (owner_setting->>'open_id') IS DISTINCT FROM expected_target OR
       (owner_setting->>'chat_id') IS DISTINCT FROM expected_provider_chat_id OR
       (owner_setting->>'app_identity') IS DISTINCT FROM expected_app_identity THEN
        RAISE EXCEPTION '050: live provider generation drifted';
    END IF;
    BEGIN
        canonical_wire :=
            convert_from(expected_canonical_payload,'UTF8')::jsonb;
        SELECT array_agg(value::BIGINT ORDER BY ordinality)
          INTO canonical_delivery_ids
          FROM jsonb_array_elements_text(
              canonical_wire->'delivery_ids'
          ) WITH ORDINALITY;
        canonical_card := decode(canonical_wire->>'card_base64','base64');
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '050: canonical effect payload cannot be decoded';
    END;
    IF jsonb_typeof(canonical_wire)<>'object' OR
       (SELECT count(*) FROM jsonb_object_keys(canonical_wire))<>20 OR
       NOT (canonical_wire ?& ARRAY[
           'schema_version','effect_id','tenant_id','user_id','task_id',
           'run_snapshot_id','run_id','step_id','chunk_index','chunk_count',
           'batch_id','delivery_ids','provider','app_identity',
           'provider_chat_id','target','card_base64','card_digest',
           'provider_uuid','idempotency_expires_at'
       ]) OR
       jsonb_typeof(canonical_wire->'delivery_ids')<>'array' OR
       canonical_wire->>'schema_version'<>'vane.push-effect/v1' OR
       canonical_wire->>'effect_id'<>expected_effect_id OR
       (canonical_wire->>'tenant_id')::BIGINT<>expected_tenant_id OR
       (canonical_wire->>'user_id')::BIGINT<>expected_user_id OR
       canonical_wire->>'task_id'<>expected_task_id OR
       (canonical_wire->>'run_snapshot_id')::BIGINT<>expected_snapshot_id OR
       canonical_wire->>'run_id'<>expected_run_id OR
       canonical_wire->>'step_id'<>'push-legacy-batch63-repair/v1' OR
       (canonical_wire->>'chunk_index')::INTEGER<>0 OR
       (canonical_wire->>'chunk_count')::INTEGER<>1 OR
       (canonical_wire->>'batch_id')::BIGINT<>63 OR
       canonical_delivery_ids<>expected_delivery_ids OR
       canonical_wire ? 'observation_event_keys' OR
       canonical_wire->>'provider'<>expected_provider OR
       canonical_wire->>'app_identity'<>expected_app_identity OR
       canonical_wire->>'provider_chat_id'<>expected_provider_chat_id OR
       canonical_wire->>'target'<>expected_target OR
       canonical_card<>expected_card_payload OR
       canonical_wire->>'card_digest'<>expected_card_digest OR
       canonical_wire->>'provider_uuid'<>expected_provider_uuid::text OR
       (canonical_wire->>'idempotency_expires_at')::TIMESTAMPTZ<>
           expected_expires_at THEN
        RAISE EXCEPTION '050: canonical effect fields drifted';
    END IF;

    UPDATE public.push_batches
       SET status='pending',delivery_authority='effect'
     WHERE id=63;
    INSERT INTO public.push_effects (
        id,tenant_id,user_id,task_id,run_snapshot_id,run_id,step_id,
        chunk_index,chunk_count,batch_id,delivery_ids,provider,app_identity,
        provider_chat_id,target,card_payload,card_digest,provider_uuid,
        idempotency_expires_at,schema_version,canonical_payload,payload_digest
    ) VALUES (
        expected_effect_id,expected_tenant_id,expected_user_id,expected_task_id,
        expected_snapshot_id,expected_run_id,
        'push-legacy-batch63-repair/v1',0,1,63,
        expected_delivery_ids,expected_provider,expected_app_identity,
        expected_provider_chat_id,expected_target,expected_card_payload,
        expected_card_digest,expected_provider_uuid,expected_expires_at,
        'vane.push-effect/v1',expected_canonical_payload,expected_payload_digest
    );
    INSERT INTO public.legacy_batch63_repair_events (
        phase,plan_digest,material_digest,evidence_digest,evidence_class,
        service_revision,effect_id,canonical_payload,payload_digest,
        card_digest,idempotency_expires_at,enable_by
    ) VALUES (
        'finalized',expected_plan_digest,expected_material_digest,
        expected_evidence_digest,expected_evidence_class,
        expected_service_revision,expected_effect_id,expected_canonical_payload,
        expected_payload_digest,expected_card_digest,expected_expires_at,
        LEAST(database_now+interval '5 minutes',
              expected_expires_at-interval '40 minutes')
    ) RETURNING legacy_batch63_repair_events.enable_by
      INTO finalize_legacy_push_batch_63_v1.enable_by;
    phase := 'finalized';
    RETURN NEXT;
END $$;
-- +goose StatementEnd

-- Only an unclaimed prepared effect can be abandoned. Batch/effect lock order
-- matches recovery (batch, then effect), making abort-vs-claim a single winner.
-- +goose StatementBegin
CREATE FUNCTION abort_legacy_push_batch_63_v1(expected_plan_digest TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
DECLARE
    finalized public.legacy_batch63_repair_events%ROWTYPE;
    effect_row public.push_effects%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(6215335020355474248);
    PERFORM 1 FROM public.push_batches WHERE id=63 FOR UPDATE;
    SELECT * INTO finalized FROM public.legacy_batch63_repair_events
     WHERE legacy_batch63_repair_events.batch_id=63
       AND legacy_batch63_repair_events.phase='finalized';
    IF NOT FOUND OR finalized.plan_digest<>expected_plan_digest THEN
        RAISE EXCEPTION '050: exact finalized plan is absent';
    END IF;
    SELECT * INTO effect_row FROM public.push_effects
     WHERE batch_id=63 AND id=finalized.effect_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION '050: finalized repair effect is absent';
    END IF;
    IF effect_row.status='blocked' AND EXISTS (
        SELECT 1 FROM public.legacy_batch63_repair_events
         WHERE legacy_batch63_repair_events.batch_id=63
           AND legacy_batch63_repair_events.phase='blocked'
           AND legacy_batch63_repair_events.plan_digest=expected_plan_digest
    ) THEN
        RETURN 'blocked';
    END IF;
    IF effect_row.status IS DISTINCT FROM 'prepared' OR
       effect_row.fence IS DISTINCT FROM 0 OR
       effect_row.attempt IS DISTINCT FROM 0 THEN
        RAISE EXCEPTION '050: claimed effect cannot be aborted';
    END IF;
    UPDATE public.push_effects
       SET status='blocked',fence=1,attempt=1,
           failure_class='operator_enable_deadline_missed',
           blocked_at=clock_timestamp(),updated_at=clock_timestamp()
     WHERE id=effect_row.id;
    UPDATE public.push_batches SET status='failed' WHERE id=63;
    INSERT INTO public.legacy_batch63_repair_events (
        phase,plan_digest,material_digest,evidence_digest,evidence_class,
        service_revision,effect_id,canonical_payload,payload_digest,
        card_digest,idempotency_expires_at,enable_by
    ) VALUES (
        'blocked',finalized.plan_digest,finalized.material_digest,
        finalized.evidence_digest,finalized.evidence_class,
        finalized.service_revision,finalized.effect_id,
        finalized.canonical_payload,finalized.payload_digest,
        finalized.card_digest,finalized.idempotency_expires_at,
        finalized.enable_by
    );
    RETURN 'blocked';
END $$;
-- +goose StatementEnd

-- The normal coordinator receives no table privileges. This exact-task
-- predicate is its sole bridge from the dated workflow execution to the
-- finalized one-shot repair audit.
-- +goose StatementBegin
CREATE FUNCTION legacy_push_batch_63_claim_ready_v1(
    expected_effect_id TEXT,
    expected_tenant_id BIGINT,
    expected_user_id BIGINT,
    expected_task_id TEXT,
    expected_snapshot_id BIGINT,
    expected_run_id TEXT,
    expected_payload_digest TEXT
)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
    SELECT
      expected_effect_id='05daa6d9-8044-59f7-9935-c595533ecb4c' AND
      expected_tenant_id=1 AND expected_user_id=1 AND
      expected_task_id=
        'task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063' AND
      expected_snapshot_id=3 AND
      expected_run_id='019f95d0-bc42-7ce4-be0d-067c2ed6bdc2' AND
      EXISTS (
        SELECT 1
          FROM public.push_effects e
          JOIN public.push_batches b
            ON b.id=63 AND b.id=e.batch_id
          JOIN public.task_run_snapshots s
            ON s.id=3 AND s.id=e.run_snapshot_id
          JOIN public.legacy_batch63_repair_events r
            ON r.batch_id=63 AND r.phase='finalized' AND r.effect_id=e.id
         WHERE e.id='05daa6d9-8044-59f7-9935-c595533ecb4c'
           AND e.id=expected_effect_id
           AND e.tenant_id=1 AND e.tenant_id=expected_tenant_id
           AND e.user_id=1 AND e.user_id=expected_user_id
           AND e.task_id=
             'task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063'
           AND e.task_id=expected_task_id
           AND e.run_snapshot_id=3
           AND e.run_snapshot_id=expected_snapshot_id
           AND e.run_id='019f95d0-bc42-7ce4-be0d-067c2ed6bdc2'
           AND e.run_id=expected_run_id
           AND e.payload_digest=expected_payload_digest
           AND e.step_id='push-legacy-batch63-repair/v1'
           AND e.delivery_ids=ARRAY[202,203,204,205,206]::BIGINT[]
           AND e.status<>'blocked'
           AND b.tenant_id=1 AND b.user_id=1
           AND b.delivery_authority='effect'
           AND s.tenant_id=1 AND s.user_id=1
           AND s.task_id=e.task_id
           AND s.temporal_run_id=e.run_id
           AND s.temporal_workflow_id=
             'wf-task-v1-c989c72382e52a2f1f6a8d0deea24bf9b072026ae5c16ce597e8785fa5ac0063-2026-07-24T20:28:32Z'
           AND r.plan_digest~'^[0-9a-f]{64}$'
           AND r.evidence_digest=
             '80bcb17806bf55d8a7d9628663a6fa16d35d9264b6be055a353cf7410774b4c3'
           AND r.payload_digest=e.payload_digest
           AND r.canonical_payload=e.canonical_payload
      )
$$;
-- +goose StatementEnd

-- The enable-by deadline gates only the first prepared/definite-failed send
-- admission. Once a send began before the deadline, exact same-owner replay
-- and ambiguous reconciliation continue to use the identity-only predicate
-- above so crash recovery cannot be disabled by the wall clock.
-- +goose StatementBegin
CREATE FUNCTION legacy_push_batch_63_fresh_claim_ready_v1(
    expected_effect_id TEXT,
    expected_tenant_id BIGINT,
    expected_user_id BIGINT,
    expected_task_id TEXT,
    expected_snapshot_id BIGINT,
    expected_run_id TEXT,
    expected_payload_digest TEXT
)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public
AS $$
    SELECT
      public.legacy_push_batch_63_claim_ready_v1(
        expected_effect_id,expected_tenant_id,expected_user_id,
        expected_task_id,expected_snapshot_id,expected_run_id,
        expected_payload_digest
      ) AND EXISTS (
        SELECT 1
          FROM public.legacy_batch63_repair_events r
          JOIN public.push_effects e
            ON e.id=expected_effect_id AND e.id=r.effect_id
         WHERE r.batch_id=63 AND r.phase='finalized'
           AND r.effect_id=expected_effect_id
           AND r.payload_digest=expected_payload_digest
           AND (
             e.attempt>0 OR
             (
               e.status='prepared' AND e.attempt=0 AND
               clock_timestamp()<=r.enable_by
             )
           )
      )
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION finalize_legacy_push_batch_63_v1(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BIGINT[],
    BYTEA,TEXT,BYTEA,TEXT,TEXT,TEXT,TEXT,TEXT,UUID,TIMESTAMPTZ
) FROM PUBLIC,vane_app;
REVOKE ALL ON FUNCTION abort_legacy_push_batch_63_v1(TEXT)
    FROM PUBLIC,vane_app;
REVOKE ALL ON FUNCTION legacy_push_batch_63_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
) FROM PUBLIC,vane_app,vane_legacy_batch63_repair;
REVOKE ALL ON FUNCTION legacy_push_batch_63_fresh_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
) FROM PUBLIC,vane_app,vane_legacy_batch63_repair;
GRANT EXECUTE ON FUNCTION finalize_legacy_push_batch_63_v1(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BIGINT[],
    BYTEA,TEXT,BYTEA,TEXT,TEXT,TEXT,TEXT,TEXT,UUID,TIMESTAMPTZ
) TO vane_legacy_batch63_repair;
GRANT EXECUTE ON FUNCTION abort_legacy_push_batch_63_v1(TEXT)
    TO vane_legacy_batch63_repair;
GRANT EXECUTE ON FUNCTION legacy_push_batch_63_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
) TO vane_push_effect_coordinator;
GRANT EXECUTE ON FUNCTION legacy_push_batch_63_fresh_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
) TO vane_push_effect_coordinator;

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474248);
LOCK TABLE push_batches,push_effects,legacy_batch63_repair_events
    IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM legacy_batch63_repair_events) OR
       EXISTS (SELECT 1 FROM push_effects WHERE batch_id=63) OR
       EXISTS (
           SELECT 1 FROM push_batches
            WHERE id=63 AND delivery_authority IS NOT NULL
       ) THEN
        RAISE EXCEPTION '050: refusing downgrade after repair evidence exists';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE EXECUTE ON FUNCTION abort_legacy_push_batch_63_v1(TEXT)
    FROM vane_legacy_batch63_repair;
REVOKE EXECUTE ON FUNCTION legacy_push_batch_63_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
) FROM vane_push_effect_coordinator;
REVOKE EXECUTE ON FUNCTION legacy_push_batch_63_fresh_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
) FROM vane_push_effect_coordinator;
REVOKE EXECUTE ON FUNCTION finalize_legacy_push_batch_63_v1(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BIGINT[],
    BYTEA,TEXT,BYTEA,TEXT,TEXT,TEXT,TEXT,TEXT,UUID,TIMESTAMPTZ
) FROM vane_legacy_batch63_repair;
DROP FUNCTION abort_legacy_push_batch_63_v1(TEXT);
DROP FUNCTION legacy_push_batch_63_fresh_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
);
DROP FUNCTION legacy_push_batch_63_claim_ready_v1(
    TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT
);
DROP FUNCTION finalize_legacy_push_batch_63_v1(
    TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,BIGINT,TEXT,BIGINT,TEXT,TEXT,BIGINT[],
    BYTEA,TEXT,BYTEA,TEXT,TEXT,TEXT,TEXT,TEXT,UUID,TIMESTAMPTZ
);
DROP TABLE legacy_batch63_repair_events;
REVOKE USAGE ON SCHEMA public FROM vane_legacy_batch63_repair;
REVOKE vane_legacy_batch63_repair FROM CURRENT_USER;
