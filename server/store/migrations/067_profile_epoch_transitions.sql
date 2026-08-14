-- 067: profile learning epoch reset/restore authority.
--
-- profile_claim_states remains the globally monotonic CAS authority.  Epoch
-- transitions are append-only and always create active_epoch+1; neither reset
-- nor restore points the state back at an older epoch.

-- +goose Up

-- Drain every pre-067 profile/feedback writer before changing the fact fence.
SELECT pg_advisory_xact_lock(1447120453,1095976527);
-- Feedback INSERT owns feedbacks before its trigger reads profile authority.
-- Take that root first so online migration cannot form profiles->feedbacks
-- against a writer's feedbacks->profiles order.
LOCK TABLE feedbacks IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profiles IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_states IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_epochs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_claims IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_claim_events IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_claim_receipts IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE profile_feedback_epoch_fences (
    tenant_id        BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    last_feedback_id BIGINT      NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id),
    CONSTRAINT ck_profile_feedback_fence_id CHECK (last_feedback_id >= 0)
);

-- MAX is permitted only in this migration snapshot while old producers are
-- drained. Runtime reset/restore reads the fenced row and never guesses a
-- boundary from timestamps or an unfenced aggregate.
INSERT INTO profile_feedback_epoch_fences
    (tenant_id,user_id,last_feedback_id)
SELECT tenant_id,user_id,max(id)
  FROM feedbacks
 GROUP BY tenant_id,user_id;
INSERT INTO profile_feedback_epoch_fences
    (tenant_id,user_id,last_feedback_id)
SELECT tenant_id,user_id,0 FROM profiles
ON CONFLICT (tenant_id,user_id) DO NOTHING;

ALTER TABLE profile_epochs
    ADD COLUMN initial_feedback_cursor BIGINT,
    ADD COLUMN initial_claim_high_water BIGINT,
    ADD COLUMN initial_event_high_water BIGINT,
    ADD COLUMN initial_evidence_generation BIGINT,
    ADD COLUMN initial_version BIGINT,
    ADD COLUMN initial_projection_digest TEXT,
    ADD CONSTRAINT ck_profile_epoch_initial_values CHECK (
      (initial_feedback_cursor IS NULL AND
       initial_claim_high_water IS NULL AND
       initial_event_high_water IS NULL AND
       initial_evidence_generation IS NULL AND
       initial_version IS NULL AND
       initial_projection_digest IS NULL) OR
      (initial_feedback_cursor >= 0 AND
       initial_claim_high_water >= 0 AND
       initial_event_high_water >= 0 AND
       initial_evidence_generation >= 0 AND
       initial_version >= 0 AND
       initial_projection_digest ~ '^[0-9a-f]{64}$')
    );

ALTER TABLE profile_claims
    ADD COLUMN carried_from_epoch BIGINT,
    ADD COLUMN carried_from_claim_id BIGINT,
    ADD CONSTRAINT ck_profile_claim_carry_lineage CHECK (
      (carried_from_epoch IS NULL)=(carried_from_claim_id IS NULL) AND
      (carried_from_epoch IS NULL OR carried_from_epoch<>profile_epoch)
    ),
    ADD CONSTRAINT fk_profile_claim_carry_lineage
      FOREIGN KEY
        (tenant_id,user_id,carried_from_epoch,carried_from_claim_id)
      REFERENCES profile_claims
        (tenant_id,user_id,profile_epoch,id);

CREATE TABLE profile_epoch_checkpoints (
    id                    BIGSERIAL   PRIMARY KEY,
    tenant_id             BIGINT      NOT NULL,
    user_id               BIGINT      NOT NULL,
    profile_epoch         BIGINT      NOT NULL,
    schema_version        INTEGER     NOT NULL,
    state_version         BIGINT      NOT NULL,
    evidence_generation   BIGINT      NOT NULL,
    claim_high_water      BIGINT      NOT NULL,
    event_high_water      BIGINT      NOT NULL,
    feedback_cursor       BIGINT      NOT NULL,
    canonical_payload     BYTEA       NOT NULL,
    projection_digest     TEXT        NOT NULL,
    previous_anchor_digest TEXT,
    anchor_digest         TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,user_id,profile_epoch),
    UNIQUE (tenant_id,user_id,id),
    CONSTRAINT ck_profile_epoch_checkpoint_numbers CHECK (
      schema_version=1 AND state_version>=0 AND evidence_generation>=0 AND
      claim_high_water>=0 AND event_high_water>=0 AND feedback_cursor>=0
    ),
    CONSTRAINT ck_profile_epoch_checkpoint_projection_digest
      CHECK (projection_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_checkpoint_previous_anchor
      CHECK (previous_anchor_digest IS NULL OR
             previous_anchor_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_checkpoint_anchor_digest
      CHECK (anchor_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT fk_profile_epoch_checkpoint_epoch
      FOREIGN KEY (tenant_id,user_id,profile_epoch)
      REFERENCES profile_epochs (tenant_id,user_id,profile_epoch)
);

CREATE TABLE profile_epoch_events (
    id                            BIGSERIAL   PRIMARY KEY,
    tenant_id                     BIGINT      NOT NULL,
    user_id                       BIGINT      NOT NULL,
    actor_user_id                 BIGINT      NOT NULL,
    event_kind                    TEXT        NOT NULL,
    predecessor_epoch             BIGINT      NOT NULL,
    result_epoch                  BIGINT      NOT NULL,
    compensated_reset_event_id    BIGINT,
    expected_version              BIGINT      NOT NULL,
    result_version                BIGINT      NOT NULL,
    predecessor_claim_high_water  BIGINT      NOT NULL,
    predecessor_event_high_water  BIGINT      NOT NULL,
    predecessor_feedback_cursor   BIGINT      NOT NULL,
    predecessor_evidence_generation BIGINT   NOT NULL,
    predecessor_removed_tags      TEXT[]      NOT NULL DEFAULT '{}',
    feedback_boundary_id          BIGINT      NOT NULL,
    checkpoint_id                 BIGINT,
    predecessor_claim_ledger_digest TEXT      NOT NULL,
    predecessor_event_ledger_digest TEXT      NOT NULL,
    predecessor_projection_digest TEXT        NOT NULL,
    result_projection_digest      TEXT        NOT NULL,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,user_id,id),
    UNIQUE (tenant_id,user_id,result_epoch),
    CONSTRAINT ck_profile_epoch_event_actor_self
      CHECK (actor_user_id=user_id),
    CONSTRAINT ck_profile_epoch_event_kind
      CHECK (event_kind IN ('reset','restore')),
    CONSTRAINT ck_profile_epoch_event_shape CHECK (
      result_epoch=predecessor_epoch+1 AND
      ((event_kind='reset' AND compensated_reset_event_id IS NULL) OR
       (event_kind='restore' AND compensated_reset_event_id IS NOT NULL))
    ),
    CONSTRAINT ck_profile_epoch_event_versions
      CHECK (expected_version>=0 AND result_version=expected_version+1),
    CONSTRAINT ck_profile_epoch_event_watermarks CHECK (
      predecessor_claim_high_water>=0 AND
      predecessor_event_high_water>=0 AND
      predecessor_feedback_cursor>=0 AND
      predecessor_evidence_generation>=0 AND feedback_boundary_id>=0
    ),
    CONSTRAINT ck_profile_epoch_event_predecessor_digest
      CHECK (predecessor_projection_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_event_claim_ledger_digest
      CHECK (predecessor_claim_ledger_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_event_event_ledger_digest
      CHECK (predecessor_event_ledger_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_event_result_digest
      CHECK (result_projection_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT fk_profile_epoch_event_predecessor
      FOREIGN KEY (tenant_id,user_id,predecessor_epoch)
      REFERENCES profile_epochs (tenant_id,user_id,profile_epoch),
    CONSTRAINT fk_profile_epoch_event_result
      FOREIGN KEY (tenant_id,user_id,result_epoch)
      REFERENCES profile_epochs (tenant_id,user_id,profile_epoch),
    CONSTRAINT fk_profile_epoch_event_compensated
      FOREIGN KEY (tenant_id,user_id,compensated_reset_event_id)
      REFERENCES profile_epoch_events (tenant_id,user_id,id)
);
CREATE UNIQUE INDEX uq_profile_epoch_restore_compensation
    ON profile_epoch_events (tenant_id,user_id,compensated_reset_event_id)
    WHERE event_kind='restore';

CREATE TABLE profile_epoch_receipts (
    tenant_id        BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    request_digest   TEXT        NOT NULL,
    event_id         BIGINT      NOT NULL,
    response_payload JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,idempotency_key),
    CONSTRAINT ck_profile_epoch_receipt_key
      CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    CONSTRAINT ck_profile_epoch_receipt_digest
      CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_receipt_response
      CHECK (jsonb_typeof(response_payload)='object'),
    CONSTRAINT fk_profile_epoch_receipt_event
      FOREIGN KEY (tenant_id,user_id,event_id)
      REFERENCES profile_epoch_events (tenant_id,user_id,id)
);

CREATE INDEX idx_profile_epoch_events_subject
    ON profile_epoch_events (tenant_id,user_id,id);
CREATE INDEX idx_profile_epoch_receipts_event
    ON profile_epoch_receipts (tenant_id,user_id,event_id);

-- One-shot feedback operations are idempotent only inside the epoch that
-- produced them. The same delivery may be acted on again after a reset.
DROP INDEX uq_feedbacks_delivery_deep_dive;
CREATE UNIQUE INDEX uq_feedbacks_delivery_epoch_deep_dive
    ON feedbacks (delivery_id,profile_epoch)
    WHERE action='deep_dive';
DROP INDEX uq_feedbacks_delivery_typed_misjudged;
CREATE UNIQUE INDEX uq_feedbacks_delivery_epoch_typed_misjudged
    ON feedbacks (delivery_id,profile_epoch)
    WHERE action='misjudged' AND reason_code IS NOT NULL;

-- nextval must run after the per-subject fence, not as a column default before
-- the BEFORE trigger.
ALTER TABLE feedbacks ALTER COLUMN id DROP DEFAULT;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname='vane_profile_epoch_editor'
    ) THEN
      BEGIN
        CREATE ROLE vane_profile_epoch_editor
          NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
          NOLOGIN NOINHERIT NOBYPASSRLS;
      EXCEPTION
        WHEN duplicate_object OR unique_violation THEN NULL;
      END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_profile_epoch_editor
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
  NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_profile_epoch_editor RESET ALL;
ALTER ROLE vane_profile_epoch_editor
  SET search_path=pg_catalog,public,pg_temp;
GRANT vane_profile_epoch_editor TO CURRENT_USER
  WITH ADMIN FALSE,SET TRUE,INHERIT FALSE;

-- +goose StatementBegin
DO $$
BEGIN
  IF pg_has_role('vane_profile_epoch_editor','vane_app','MEMBER') OR
     pg_has_role('vane_app','vane_profile_epoch_editor','MEMBER') OR
     pg_has_role('vane_profile_epoch_editor','vane_profile_editor','MEMBER') OR
     pg_has_role('vane_profile_editor','vane_profile_epoch_editor','MEMBER') OR
     pg_has_role('vane_profile_epoch_editor','vane_profile_claim_editor','MEMBER') OR
     pg_has_role('vane_profile_claim_editor','vane_profile_epoch_editor','MEMBER')
  THEN
    RAISE EXCEPTION '067: epoch editor must be unrelated to app/profile roles';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_auth_members am
    JOIN pg_roles member_role ON member_role.oid=am.member
    JOIN pg_roles granted_role ON granted_role.oid=am.roleid
    WHERE granted_role.rolname='vane_profile_epoch_editor'
      AND member_role.rolname<>CURRENT_USER
  ) THEN
    RAISE EXCEPTION '067: only migration owner may enter epoch editor';
  END IF;
END $$;
-- +goose StatementEnd

REVOKE ALL ON profile_feedback_epoch_fences,profile_epoch_checkpoints,
  profile_epoch_events,profile_epoch_receipts
  FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
       vane_profile_epoch_editor;
REVOKE ALL ON SEQUENCE profile_epoch_checkpoints_id_seq,
  profile_epoch_events_id_seq
  FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
       vane_profile_epoch_editor;

ALTER TABLE profile_feedback_epoch_fences ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_feedback_epoch_fences FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_checkpoints FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_events FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_receipts FORCE ROW LEVEL SECURITY;

CREATE POLICY profile_feedback_epoch_fences_exact_user
ON profile_feedback_epoch_fences
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
)
WITH CHECK (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);
CREATE POLICY profile_epoch_checkpoints_exact_user ON profile_epoch_checkpoints
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
)
WITH CHECK (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);
CREATE POLICY profile_epoch_events_exact_user ON profile_epoch_events
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
)
WITH CHECK (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);
CREATE POLICY profile_epoch_receipts_exact_user ON profile_epoch_receipts
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
)
WITH CHECK (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);

GRANT USAGE ON SCHEMA public TO vane_profile_epoch_editor;
GRANT SELECT (tenant_id,user_id) ON memberships TO vane_profile_epoch_editor;
GRANT SELECT (
  tenant_id,user_id,industry,occupation,tags,removed_tags,summary,
  last_evolved_feedback_id,created_at,updated_at
) ON profiles TO vane_profile_epoch_editor;
GRANT UPDATE (
  industry,occupation,tags,removed_tags,summary,last_evolved_feedback_id,updated_at
) ON profiles TO vane_profile_epoch_editor;
GRANT SELECT,UPDATE ON profile_claim_states TO vane_profile_epoch_editor;
GRANT SELECT,INSERT ON profile_claims TO vane_profile_epoch_editor;
GRANT USAGE,SELECT ON SEQUENCE profile_claims_id_seq
  TO vane_profile_epoch_editor;
GRANT SELECT ON profile_claim_events TO vane_profile_epoch_editor;
GRANT SELECT,INSERT,UPDATE ON profile_epochs TO vane_profile_epoch_editor;
GRANT SELECT,INSERT,UPDATE ON profile_feedback_epoch_fences
  TO vane_profile_epoch_editor;
GRANT SELECT,INSERT ON profile_epoch_checkpoints,profile_epoch_events,
  profile_epoch_receipts TO vane_profile_epoch_editor;
GRANT USAGE,SELECT ON SEQUENCE profile_epoch_checkpoints_id_seq,
  profile_epoch_events_id_seq TO vane_profile_epoch_editor;
GRANT SELECT (id,tenant_id,user_id,profile_epoch) ON feedbacks
  TO vane_profile_epoch_editor;
GRANT SELECT (
  id,tenant_id,user_id,event_kind,predecessor_epoch,result_epoch,
  compensated_reset_event_id,feedback_boundary_id,checkpoint_id,
  predecessor_claim_high_water,predecessor_event_high_water,
  predecessor_feedback_cursor,predecessor_evidence_generation,
  predecessor_removed_tags,
  predecessor_claim_ledger_digest,predecessor_event_ledger_digest,
  predecessor_projection_digest,result_projection_digest
) ON profile_epoch_events TO vane_profile_claim_editor;
GRANT SELECT (
  id,tenant_id,user_id,profile_epoch
) ON feedbacks TO vane_profile_claim_editor;
GRANT SELECT (tenant_id,user_id,active_epoch)
  ON profile_claim_states TO vane_brief_reader;
-- Feedback fact retries run as vane_app after installing exact tenant/user
-- GUCs. They need only the epoch token to locate the current-epoch
-- idempotency winner; RLS still limits the read to that exact subject.
GRANT SELECT (tenant_id,user_id,active_epoch)
  ON profile_claim_states TO vane_app;
GRANT SELECT (tenant_id,user_id) ON profiles TO vane_brief_reader;
GRANT SELECT (profile_epoch) ON feedbacks TO vane_brief_reader;

-- Transition role needs the same exact-subject restrictive policy on the old
-- authority tables. Their existing permissive exact-user policy still applies.
CREATE POLICY profile_epoch_editor_identity ON profiles AS RESTRICTIVE
FOR ALL TO vane_profile_epoch_editor
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
)
WITH CHECK (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);
CREATE POLICY profile_epoch_editor_identity ON memberships AS RESTRICTIVE
FOR SELECT TO vane_profile_epoch_editor
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);
CREATE POLICY profile_epoch_reader_identity ON feedbacks AS RESTRICTIVE
FOR SELECT TO vane_profile_claim_editor,vane_profile_epoch_editor
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);
CREATE POLICY brief_reader_profile_identity ON profiles AS RESTRICTIVE
FOR SELECT TO vane_brief_reader
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);

-- The 062 projection fence predates the transition role.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.enforce_profile_claim_editor_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
  IF current_user NOT IN (
    'vane_profile_claim_editor','vane_profile_epoch_editor'
  ) THEN
    RAISE EXCEPTION
      'profiles protected fields require profile authority'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

DROP TRIGGER enforce_profile_epoch_seed_v1 ON profile_epochs;
DROP FUNCTION public.enforce_profile_epoch_seed_v1();
DROP TRIGGER enforce_profile_epoch_state_v1 ON profile_claim_states;
DROP FUNCTION public.enforce_profile_epoch_state_v1();
DROP TRIGGER require_active_profile_epoch_v1 ON profiles;
DROP TRIGGER require_active_profile_epoch_v1 ON profile_claim_receipts;
DROP TRIGGER require_active_profile_epoch_v1 ON profile_claim_events;
DROP TRIGGER require_active_profile_epoch_v1 ON profile_claims;
DROP FUNCTION public.require_active_profile_epoch_v1();

-- +goose StatementBegin
CREATE FUNCTION public.enforce_profile_epoch_seed_v2()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE current_epoch BIGINT;
BEGIN
  IF current_user='vane_profile_claim_editor' THEN
    IF NEW.profile_epoch<>0 OR EXISTS (
      SELECT 1 FROM public.profile_claim_states
       WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
    ) THEN
      RAISE EXCEPTION 'only epoch zero first-intake seed is enabled'
        USING ERRCODE='42501';
    END IF;
    RETURN NEW;
  END IF;
  IF current_user<>'vane_profile_epoch_editor' THEN
    RAISE EXCEPTION 'profile epoch transition authority required'
      USING ERRCODE='42501';
  END IF;
  SELECT active_epoch INTO current_epoch
    FROM public.profile_claim_states
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
   FOR SHARE;
  IF NOT FOUND OR NEW.profile_epoch<>current_epoch+1 OR
     NEW.profile_epoch<>
       NULLIF(current_setting('app.profile_epoch',true),'')::bigint THEN
    RAISE EXCEPTION 'profile epoch transition is stale'
      USING ERRCODE='40001';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER enforce_profile_epoch_seed_v2
BEFORE INSERT ON profile_epochs
FOR EACH ROW EXECUTE FUNCTION public.enforce_profile_epoch_seed_v2();

-- +goose StatementBegin
CREATE FUNCTION public.require_active_profile_epoch_v2()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE bound_epoch BIGINT;
DECLARE current_epoch BIGINT;
BEGIN
  bound_epoch:=NULLIF(current_setting('app.profile_epoch',true),'')::bigint;
  IF bound_epoch IS NULL THEN
    RAISE EXCEPTION 'profile writer did not bind app.profile_epoch'
      USING ERRCODE='42501';
  END IF;
  SELECT active_epoch INTO current_epoch
    FROM public.profile_claim_states
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
   FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'profile state is absent' USING ERRCODE='23514';
  END IF;
  IF current_user='vane_profile_epoch_editor' THEN
    IF bound_epoch<>current_epoch AND bound_epoch<>current_epoch+1 THEN
      RAISE EXCEPTION 'profile transition writer epoch is stale'
        USING ERRCODE='40001';
    END IF;
  ELSIF current_user='vane_profile_claim_editor' THEN
    IF bound_epoch<>current_epoch THEN
      RAISE EXCEPTION 'profile writer epoch is stale' USING ERRCODE='40001';
    END IF;
  ELSE
    RAISE EXCEPTION 'profile authority role required' USING ERRCODE='42501';
  END IF;
  IF TG_TABLE_NAME IN
       ('profile_claims','profile_claim_events','profile_claim_receipts') AND
     NULLIF(to_jsonb(NEW)->>'profile_epoch','')::bigint<>bound_epoch THEN
    RAISE EXCEPTION 'profile fact epoch does not match bound epoch'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER require_active_profile_epoch_v2
BEFORE INSERT ON profile_claims
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v2();
CREATE TRIGGER require_active_profile_epoch_v2
BEFORE INSERT ON profile_claim_events
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v2();
CREATE TRIGGER require_active_profile_epoch_v2
BEFORE INSERT ON profile_claim_receipts
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v2();
CREATE TRIGGER require_active_profile_epoch_v2
BEFORE UPDATE OF industry,occupation,tags,removed_tags,summary,
                 last_evolved_feedback_id
ON profiles
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v2();

-- +goose StatementBegin
CREATE FUNCTION public.enforce_profile_epoch_state_v2()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE bound_epoch BIGINT;
BEGIN
  bound_epoch:=NULLIF(current_setting('app.profile_epoch',true),'')::bigint;
  IF current_user='vane_profile_epoch_editor' THEN
    IF bound_epoch IS NULL OR NEW.active_epoch<>OLD.active_epoch+1 OR
       bound_epoch<>NEW.active_epoch OR NEW.version<>OLD.version+1 THEN
      RAISE EXCEPTION 'invalid profile epoch transition'
        USING ERRCODE='40001';
    END IF;
  ELSIF current_user='vane_profile_claim_editor' THEN
    IF bound_epoch IS NULL OR bound_epoch<>OLD.active_epoch OR
       NEW.active_epoch<>OLD.active_epoch OR NEW.version<OLD.version THEN
      RAISE EXCEPTION 'profile state writer epoch is stale'
        USING ERRCODE='40001';
    END IF;
  ELSE
    RAISE EXCEPTION 'profile state authority role required'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER enforce_profile_epoch_state_v2
BEFORE UPDATE ON profile_claim_states
FOR EACH ROW EXECUTE FUNCTION public.enforce_profile_epoch_state_v2();

DROP TRIGGER assign_feedback_profile_epoch_v1 ON feedbacks;
DROP FUNCTION public.assign_feedback_profile_epoch_v1();

-- +goose StatementBegin
CREATE FUNCTION public.assign_feedback_profile_epoch_v2()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
  caller_role TEXT;
  prior_tenant TEXT;
  prior_user TEXT;
  assigned_epoch BIGINT;
BEGIN
  caller_role:=current_setting('role',true);
  prior_tenant:=current_setting('app.tenant_id',true);
  prior_user:=current_setting('app.user_id',true);
  IF caller_role='vane_app' AND (
     prior_tenant IS DISTINCT FROM NEW.tenant_id::text OR
     prior_user IS DISTINCT FROM NEW.user_id::text
  ) THEN
    RAISE EXCEPTION 'feedback writer scope does not match subject'
      USING ERRCODE='42501';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended(
    'vane/tenant-admission/v1/'||NEW.tenant_id::text,1447120453));
  PERFORM 1 FROM public.tenants WHERE id=NEW.tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feedback tenant is absent' USING ERRCODE='23503';
  END IF;
  PERFORM set_config('app.tenant_id',NEW.tenant_id::text,true);
  PERFORM set_config('app.user_id',NEW.user_id::text,true);
  PERFORM 1 FROM public.memberships
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR KEY SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feedback owner membership is absent' USING ERRCODE='23503';
  END IF;
  PERFORM 1 FROM public.deliveries
   WHERE id=NEW.delivery_id AND tenant_id=NEW.tenant_id AND user_id=NEW.user_id
   FOR KEY SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feedback delivery owner does not match subject'
      USING ERRCODE='23503';
  END IF;

  INSERT INTO public.profile_feedback_epoch_fences
    (tenant_id,user_id,last_feedback_id)
  VALUES (NEW.tenant_id,NEW.user_id,0)
  ON CONFLICT (tenant_id,user_id) DO NOTHING;
  PERFORM 1 FROM public.profile_feedback_epoch_fences
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR UPDATE;

  NEW.id:=nextval('public.feedbacks_id_seq');
  UPDATE public.profile_feedback_epoch_fences
     SET last_feedback_id=NEW.id,updated_at=clock_timestamp()
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id;

  SELECT s.active_epoch INTO assigned_epoch
    FROM public.profiles p
    JOIN public.profile_claim_states s
      ON s.tenant_id=p.tenant_id AND s.user_id=p.user_id
   WHERE p.tenant_id=NEW.tenant_id AND p.user_id=NEW.user_id
   FOR SHARE OF p,s;
  IF NOT FOUND THEN
    IF EXISTS (
      SELECT 1 FROM public.profiles
       WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
    ) THEN
      RAISE EXCEPTION 'profile claim state missing for feedback owner'
        USING ERRCODE='23514';
    END IF;
    assigned_epoch:=0;
  END IF;
  NEW.profile_epoch:=assigned_epoch;
  PERFORM set_config('app.tenant_id',COALESCE(prior_tenant,''),true);
  PERFORM set_config('app.user_id',COALESCE(prior_user,''),true);
  RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION public.assign_feedback_profile_epoch_v2() FROM PUBLIC;
CREATE TRIGGER assign_feedback_profile_epoch_v2
BEFORE INSERT ON feedbacks
FOR EACH ROW EXECUTE FUNCTION public.assign_feedback_profile_epoch_v2();

-- +goose Down

SELECT pg_advisory_xact_lock(1447120453,1095976527);
LOCK TABLE feedbacks IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profiles IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_states IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_epochs IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claims IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_receipts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_epoch_receipts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_epoch_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_epoch_checkpoints IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_feedback_epoch_fences IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM profile_claim_states WHERE active_epoch<>0) OR
     EXISTS (SELECT 1 FROM profile_epochs WHERE profile_epoch<>0) OR
     EXISTS (SELECT 1 FROM profile_claims WHERE profile_epoch<>0) OR
     EXISTS (SELECT 1 FROM profile_claim_events WHERE profile_epoch<>0) OR
     EXISTS (SELECT 1 FROM profile_claim_receipts WHERE profile_epoch<>0) OR
     EXISTS (SELECT 1 FROM feedbacks WHERE profile_epoch<>0) OR
     EXISTS (SELECT 1 FROM profile_epoch_events) OR
     EXISTS (SELECT 1 FROM profile_epoch_checkpoints) OR
     EXISTS (SELECT 1 FROM profile_epoch_receipts)
  THEN
    RAISE EXCEPTION
      'refusing 067 downgrade after profile epoch transition facts exist'
      USING ERRCODE='P0001';
  END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER assign_feedback_profile_epoch_v2 ON feedbacks;
DROP FUNCTION public.assign_feedback_profile_epoch_v2();
ALTER TABLE feedbacks ALTER COLUMN id
  SET DEFAULT nextval('feedbacks_id_seq'::regclass);
DROP INDEX uq_feedbacks_delivery_epoch_typed_misjudged;
CREATE UNIQUE INDEX uq_feedbacks_delivery_typed_misjudged
  ON feedbacks (delivery_id)
  WHERE action='misjudged' AND reason_code IS NOT NULL;
DROP INDEX uq_feedbacks_delivery_epoch_deep_dive;
CREATE UNIQUE INDEX uq_feedbacks_delivery_deep_dive
  ON feedbacks (delivery_id) WHERE action='deep_dive';

DROP TRIGGER enforce_profile_epoch_state_v2 ON profile_claim_states;
DROP FUNCTION public.enforce_profile_epoch_state_v2();
DROP TRIGGER require_active_profile_epoch_v2 ON profiles;
DROP TRIGGER require_active_profile_epoch_v2 ON profile_claim_receipts;
DROP TRIGGER require_active_profile_epoch_v2 ON profile_claim_events;
DROP TRIGGER require_active_profile_epoch_v2 ON profile_claims;
DROP FUNCTION public.require_active_profile_epoch_v2();
DROP TRIGGER enforce_profile_epoch_seed_v2 ON profile_epochs;
DROP FUNCTION public.enforce_profile_epoch_seed_v2();

-- Restore the Phase A guards exactly.
-- +goose StatementBegin
CREATE FUNCTION public.enforce_profile_epoch_seed_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE state_exists BOOLEAN;
BEGIN
  IF current_user<>'vane_profile_claim_editor' OR NEW.profile_epoch<>0 THEN
    RAISE EXCEPTION 'only epoch zero first-intake seed is enabled'
      USING ERRCODE='42501';
  END IF;
  SELECT EXISTS(
    SELECT 1 FROM public.profile_claim_states
     WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
  ) INTO state_exists;
  IF state_exists THEN
    RAISE EXCEPTION 'profile epoch seed already initialized'
      USING ERRCODE='23505';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER enforce_profile_epoch_seed_v1
BEFORE INSERT ON profile_epochs
FOR EACH ROW EXECUTE FUNCTION public.enforce_profile_epoch_seed_v1();

-- +goose StatementBegin
CREATE FUNCTION public.require_active_profile_epoch_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE bound_epoch BIGINT;
DECLARE current_epoch BIGINT;
BEGIN
  bound_epoch:=NULLIF(current_setting('app.profile_epoch',true),'')::bigint;
  IF bound_epoch IS NULL THEN
    RAISE EXCEPTION 'profile writer did not bind app.profile_epoch'
      USING ERRCODE='42501';
  END IF;
  IF TG_TABLE_NAME='profiles' AND
     current_user<>'vane_profile_claim_editor' THEN
    RAISE EXCEPTION 'profile projection/cursor requires vane_profile_claim_editor'
      USING ERRCODE='42501';
  END IF;
  SELECT active_epoch INTO current_epoch
    FROM public.profile_claim_states
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR SHARE;
  IF NOT FOUND OR current_epoch<>bound_epoch THEN
    RAISE EXCEPTION 'profile writer epoch is stale' USING ERRCODE='40001';
  END IF;
  IF TG_TABLE_NAME IN
       ('profile_claims','profile_claim_events','profile_claim_receipts') AND
     NULLIF(to_jsonb(NEW)->>'profile_epoch','')::bigint<>bound_epoch THEN
    RAISE EXCEPTION 'profile fact epoch does not match active epoch'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER require_active_profile_epoch_v1 BEFORE INSERT ON profile_claims
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();
CREATE TRIGGER require_active_profile_epoch_v1 BEFORE INSERT ON profile_claim_events
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();
CREATE TRIGGER require_active_profile_epoch_v1 BEFORE INSERT ON profile_claim_receipts
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();
CREATE TRIGGER require_active_profile_epoch_v1
BEFORE UPDATE OF industry,occupation,tags,removed_tags,summary,
                 last_evolved_feedback_id ON profiles
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();

-- +goose StatementBegin
CREATE FUNCTION public.enforce_profile_epoch_state_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE bound_epoch BIGINT;
BEGIN
  bound_epoch:=NULLIF(current_setting('app.profile_epoch',true),'')::bigint;
  IF bound_epoch IS NULL OR bound_epoch<>OLD.active_epoch THEN
    RAISE EXCEPTION 'profile state writer epoch is stale'
      USING ERRCODE='40001';
  END IF;
  IF NEW.version<OLD.version THEN
    RAISE EXCEPTION 'profile claim version cannot decrease'
      USING ERRCODE='23514';
  END IF;
  IF NEW.active_epoch<>OLD.active_epoch THEN
    RAISE EXCEPTION
      'profile epoch transition authority is not enabled in phase A'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER enforce_profile_epoch_state_v1
BEFORE UPDATE ON profile_claim_states
FOR EACH ROW EXECUTE FUNCTION public.enforce_profile_epoch_state_v1();

-- Recreate the exact Phase A feedback stamper with a sequence default.
-- +goose StatementBegin
CREATE FUNCTION public.assign_feedback_profile_epoch_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
  caller_role TEXT;
  prior_tenant TEXT;
  prior_user TEXT;
  has_profile BOOLEAN:=FALSE;
  assigned_epoch BIGINT;
BEGIN
  caller_role:=current_setting('role',true);
  prior_tenant:=current_setting('app.tenant_id',true);
  prior_user:=current_setting('app.user_id',true);
  IF caller_role='vane_app' AND (
     prior_tenant IS DISTINCT FROM NEW.tenant_id::text OR
     prior_user IS DISTINCT FROM NEW.user_id::text
  ) THEN
    RAISE EXCEPTION 'feedback writer scope does not match subject'
      USING ERRCODE='42501';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'vane/tenant-admission/v1/'||NEW.tenant_id::text,1447120453));
  PERFORM 1 FROM public.tenants WHERE id=NEW.tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feedback tenant is absent' USING ERRCODE='23503';
  END IF;
  PERFORM set_config('app.tenant_id',NEW.tenant_id::text,true);
  PERFORM set_config('app.user_id',NEW.user_id::text,true);
  PERFORM 1 FROM public.memberships
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR KEY SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feedback owner membership is absent' USING ERRCODE='23503';
  END IF;
  PERFORM 1 FROM public.deliveries
   WHERE id=NEW.delivery_id AND tenant_id=NEW.tenant_id AND user_id=NEW.user_id
   FOR KEY SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'feedback delivery owner does not match subject'
      USING ERRCODE='23503';
  END IF;
  SELECT TRUE INTO has_profile FROM public.profiles
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR SHARE;
  IF has_profile THEN
    SELECT active_epoch INTO assigned_epoch FROM public.profile_claim_states
     WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR SHARE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'profile claim state missing for feedback owner'
        USING ERRCODE='23514';
    END IF;
  ELSE
    assigned_epoch:=0;
  END IF;
  NEW.profile_epoch:=assigned_epoch;
  PERFORM set_config('app.tenant_id',COALESCE(prior_tenant,''),true);
  PERFORM set_config('app.user_id',COALESCE(prior_user,''),true);
  RETURN NEW;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION public.assign_feedback_profile_epoch_v1() FROM PUBLIC;
CREATE TRIGGER assign_feedback_profile_epoch_v1
BEFORE INSERT ON feedbacks
FOR EACH ROW EXECUTE FUNCTION public.assign_feedback_profile_epoch_v1();

DROP POLICY profile_epoch_editor_identity ON memberships;
DROP POLICY profile_epoch_editor_identity ON profiles;
DROP POLICY profile_epoch_reader_identity ON feedbacks;
DROP POLICY brief_reader_profile_identity ON profiles;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.enforce_profile_claim_editor_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog,public,pg_temp
AS $$
BEGIN
  IF current_user<>'vane_profile_claim_editor' THEN
    RAISE EXCEPTION
      'profiles protected fields require vane_profile_claim_editor'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

REVOKE SELECT (
  id,tenant_id,user_id,event_kind,predecessor_epoch,result_epoch,
  compensated_reset_event_id,feedback_boundary_id,checkpoint_id,
  predecessor_claim_high_water,predecessor_event_high_water,
  predecessor_feedback_cursor,predecessor_evidence_generation,
  predecessor_removed_tags,
  predecessor_claim_ledger_digest,predecessor_event_ledger_digest,
  predecessor_projection_digest,result_projection_digest
) ON profile_epoch_events FROM vane_profile_claim_editor;
REVOKE SELECT (
  id,tenant_id,user_id,profile_epoch
) ON feedbacks FROM vane_profile_claim_editor;
REVOKE SELECT (tenant_id,user_id,active_epoch)
  ON profile_claim_states FROM vane_brief_reader;
REVOKE SELECT (tenant_id,user_id,active_epoch)
  ON profile_claim_states FROM vane_app;
REVOKE SELECT (tenant_id,user_id) ON profiles FROM vane_brief_reader;
REVOKE SELECT (profile_epoch) ON feedbacks FROM vane_brief_reader;
REVOKE ALL ON profile_epoch_receipts,profile_epoch_events,
  profile_epoch_checkpoints,profile_feedback_epoch_fences
  FROM vane_profile_epoch_editor;
REVOKE ALL ON SEQUENCE profile_epoch_events_id_seq,
  profile_epoch_checkpoints_id_seq FROM vane_profile_epoch_editor;

DROP TABLE profile_epoch_receipts;
DROP TABLE profile_epoch_events;
DROP TABLE profile_epoch_checkpoints;
DROP TABLE profile_feedback_epoch_fences;

ALTER TABLE profile_claims
  DROP CONSTRAINT fk_profile_claim_carry_lineage,
  DROP CONSTRAINT ck_profile_claim_carry_lineage,
  DROP COLUMN carried_from_claim_id,
  DROP COLUMN carried_from_epoch;
ALTER TABLE profile_epochs
  DROP CONSTRAINT ck_profile_epoch_initial_values,
  DROP COLUMN initial_projection_digest,
  DROP COLUMN initial_version,
  DROP COLUMN initial_evidence_generation,
  DROP COLUMN initial_event_high_water,
  DROP COLUMN initial_claim_high_water,
  DROP COLUMN initial_feedback_cursor;

-- Roles and memberships are cluster-scoped while goose migrations are
-- database-scoped. Keep the hardened NOLOGIN role and owner membership: another
-- database in the same cluster may still be running migration 067.
DROP OWNED BY vane_profile_epoch_editor;
