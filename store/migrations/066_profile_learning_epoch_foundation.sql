-- 066: 画像学习 epoch 地基。
--
-- 本迁移不开放 reset/restore 产品能力；它只建立将来 reset 可以安全线性化的
-- 数据归属、反馈 fence 和旧 writer 拒写边界。profiles 仍是读取投影，
-- profile_claim_states.version 仍跨 epoch 单调递增，永不归零。

-- +goose Up

-- ACCESS EXCLUSIVE on the first root drains every old claim/cursor writer
-- before this migration holds any downstream table lock. Taking weaker locks
-- one table at a time would allow a writer holding claims RowExclusive to wait
-- on state while the migration held state and waited on claims.
LOCK TABLE profiles IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_states IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_claims IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_claim_events IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE profile_claim_receipts IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE feedbacks IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE profile_claim_states
    ADD COLUMN active_epoch BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_profile_claim_state_active_epoch
        CHECK (active_epoch >= 0);

CREATE TABLE profile_epochs (
    tenant_id      BIGINT      NOT NULL,
    user_id        BIGINT      NOT NULL,
    profile_epoch  BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,profile_epoch),
    CONSTRAINT ck_profile_epoch_nonnegative CHECK (profile_epoch >= 0),
    CONSTRAINT fk_profile_epoch_profile
        FOREIGN KEY (tenant_id,user_id)
        REFERENCES profiles (tenant_id,user_id)
        ON DELETE CASCADE
);

INSERT INTO profile_epochs (tenant_id,user_id,profile_epoch)
SELECT tenant_id,user_id,0 FROM profile_claim_states;

ALTER TABLE profile_claim_states
    ADD CONSTRAINT fk_profile_claim_state_active_epoch
        FOREIGN KEY (tenant_id,user_id,active_epoch)
        REFERENCES profile_epochs (tenant_id,user_id,profile_epoch);

-- Backfill every existing fact into epoch 0, then remove defaults from
-- claim/action facts. New binaries must name profile_epoch explicitly;
-- an old claim/evolve binary therefore fails closed immediately after 066.
ALTER TABLE profile_claims
    ADD COLUMN profile_epoch BIGINT;
UPDATE profile_claims SET profile_epoch=0;
ALTER TABLE profile_claims
    ALTER COLUMN profile_epoch SET NOT NULL;

ALTER TABLE profile_claim_events
    ADD COLUMN profile_epoch BIGINT;
UPDATE profile_claim_events SET profile_epoch=0;
ALTER TABLE profile_claim_events
    ALTER COLUMN profile_epoch SET NOT NULL;

ALTER TABLE profile_claim_receipts
    ADD COLUMN profile_epoch BIGINT;
UPDATE profile_claim_receipts SET profile_epoch=0;
ALTER TABLE profile_claim_receipts
    ALTER COLUMN profile_epoch SET NOT NULL;

-- Feedback is the compatibility exception: old producers are intentionally
-- supported by the BEFORE INSERT trigger below, which always overwrites this
-- column from the fenced state (or explicit epoch 0 when no profile exists).
ALTER TABLE feedbacks
    ADD COLUMN profile_epoch BIGINT;
UPDATE feedbacks SET profile_epoch=0;
ALTER TABLE feedbacks
    ALTER COLUMN profile_epoch SET NOT NULL,
    ADD CONSTRAINT ck_feedback_profile_epoch CHECK (profile_epoch >= 0);

-- The legacy 022 policy casts an empty pooled GUC directly to bigint. Epoch
-- stamping temporarily restores an absent setting as empty, so normalize the
-- policy to fail closed without a cast error.
ALTER POLICY tenant_isolation ON feedbacks
    USING (
      tenant_id IS NOT DISTINCT FROM
      NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id IS NOT DISTINCT FROM
      NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );

ALTER TABLE profile_claims
    ADD CONSTRAINT uq_profile_claim_epoch_scope
        UNIQUE (tenant_id,user_id,profile_epoch,id),
    ADD CONSTRAINT fk_profile_claim_epoch
        FOREIGN KEY (tenant_id,user_id,profile_epoch)
        REFERENCES profile_epochs (tenant_id,user_id,profile_epoch),
    ADD CONSTRAINT fk_profile_claim_supersedes_epoch_scope
        FOREIGN KEY (tenant_id,user_id,profile_epoch,supersedes_claim_id)
        REFERENCES profile_claims (tenant_id,user_id,profile_epoch,id);

ALTER TABLE profile_claim_events
    ADD CONSTRAINT uq_profile_claim_event_epoch_scope
        UNIQUE (tenant_id,user_id,profile_epoch,id),
    ADD CONSTRAINT fk_profile_claim_event_epoch
        FOREIGN KEY (tenant_id,user_id,profile_epoch)
        REFERENCES profile_epochs (tenant_id,user_id,profile_epoch),
    ADD CONSTRAINT fk_profile_claim_event_target_claim_epoch_scope
        FOREIGN KEY (tenant_id,user_id,profile_epoch,target_claim_id)
        REFERENCES profile_claims (tenant_id,user_id,profile_epoch,id),
    ADD CONSTRAINT fk_profile_claim_event_result_claim_epoch_scope
        FOREIGN KEY (tenant_id,user_id,profile_epoch,result_claim_id)
        REFERENCES profile_claims (tenant_id,user_id,profile_epoch,id),
    ADD CONSTRAINT fk_profile_claim_event_target_event_epoch_scope
        FOREIGN KEY (tenant_id,user_id,profile_epoch,target_event_id)
        REFERENCES profile_claim_events (tenant_id,user_id,profile_epoch,id);

ALTER TABLE profile_claim_receipts
    ADD CONSTRAINT fk_profile_claim_receipt_epoch
        FOREIGN KEY (tenant_id,user_id,profile_epoch)
        REFERENCES profile_epochs (tenant_id,user_id,profile_epoch),
    ADD CONSTRAINT fk_profile_claim_receipt_event_epoch_scope
        FOREIGN KEY (tenant_id,user_id,profile_epoch,event_id)
        REFERENCES profile_claim_events (tenant_id,user_id,profile_epoch,id);

CREATE INDEX idx_profile_claims_active_epoch
    ON profile_claims (tenant_id,user_id,profile_epoch,id);
CREATE INDEX idx_profile_claim_events_active_epoch
    ON profile_claim_events (tenant_id,user_id,profile_epoch,id);
CREATE INDEX idx_feedbacks_profile_epoch
    ON feedbacks (tenant_id,user_id,profile_epoch,id);

-- Epoch rows are private authority, with the same exact-user RLS boundary as
-- the claim ledger. NULLIF keeps an empty pooled GUC fail-closed.
REVOKE ALL ON profile_epochs
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
ALTER TABLE profile_epochs ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_epochs FORCE ROW LEVEL SECURITY;
CREATE POLICY profile_epochs_exact_user ON profile_epochs
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
GRANT SELECT,INSERT ON profile_epochs TO vane_profile_claim_editor;

-- Phase A only permits the atomic first-intake seed: profile already exists,
-- no state exists yet, and the sole epoch is zero. A later reset-authority
-- migration must replace this guard before it can create later epochs; the
-- general claim editor cannot mint an epoch and bypass reset audit.
-- +goose StatementBegin
CREATE FUNCTION public.enforce_profile_epoch_seed_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    state_exists BOOLEAN;
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
END
$$;
-- +goose StatementEnd

CREATE TRIGGER enforce_profile_epoch_seed_v1
BEFORE INSERT ON profile_epochs
FOR EACH ROW EXECUTE FUNCTION public.enforce_profile_epoch_seed_v1();

-- Every explicit claim/state/projection writer must bind the epoch it locked.
-- This is the binary cutover fence: old claim/evolve/cursor code has no
-- app.profile_epoch and is rejected even while active_epoch is still zero.
-- +goose StatementBegin
CREATE FUNCTION public.require_active_profile_epoch_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    bound_epoch BIGINT;
    current_epoch BIGINT;
BEGIN
    bound_epoch :=
      NULLIF(current_setting('app.profile_epoch',true),'')::bigint;
    IF bound_epoch IS NULL THEN
        RAISE EXCEPTION 'profile writer did not bind app.profile_epoch'
          USING ERRCODE='42501';
    END IF;
    IF TG_TABLE_NAME='profiles' AND
       current_user<>'vane_profile_claim_editor' THEN
        RAISE EXCEPTION
          'profile projection/cursor requires vane_profile_claim_editor'
          USING ERRCODE='42501';
    END IF;

    SELECT active_epoch INTO current_epoch
      FROM public.profile_claim_states
     WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
     FOR SHARE;
    IF NOT FOUND OR current_epoch<>bound_epoch THEN
        RAISE EXCEPTION 'profile writer epoch is stale'
          USING ERRCODE='40001';
    END IF;

    IF TG_TABLE_NAME IN
       ('profile_claims','profile_claim_events','profile_claim_receipts')
       AND NULLIF(to_jsonb(NEW)->>'profile_epoch','')::bigint<>bound_epoch THEN
        RAISE EXCEPTION 'profile fact epoch does not match active epoch'
          USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER require_active_profile_epoch_v1
BEFORE INSERT ON profile_claims
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();
CREATE TRIGGER require_active_profile_epoch_v1
BEFORE INSERT ON profile_claim_events
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();
CREATE TRIGGER require_active_profile_epoch_v1
BEFORE INSERT ON profile_claim_receipts
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();
CREATE TRIGGER require_active_profile_epoch_v1
BEFORE UPDATE OF industry,occupation,tags,removed_tags,summary,
                 last_evolved_feedback_id
ON profiles
FOR EACH ROW EXECUTE FUNCTION public.require_active_profile_epoch_v1();

-- Same-epoch writes cannot lower the global version. Advancing an epoch is
-- exactly +1 and must also advance version, preventing ABA/version reset.
-- +goose StatementBegin
CREATE FUNCTION public.enforce_profile_epoch_state_v1()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    bound_epoch BIGINT;
BEGIN
    bound_epoch :=
      NULLIF(current_setting('app.profile_epoch',true),'')::bigint;
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
END
$$;
-- +goose StatementEnd

CREATE TRIGGER enforce_profile_epoch_state_v1
BEFORE UPDATE ON profile_claim_states
FOR EACH ROW EXECUTE FUNCTION public.enforce_profile_epoch_state_v1();

-- Old feedback writers remain compatible. The trigger takes the same
-- membership -> profile -> state fence that reset will use and stamps the
-- active epoch in the feedback fact's own transaction. A user without a
-- profile/state is truthfully epoch 0; a profile without state is corruption
-- and fails closed rather than guessing.
-- +goose StatementBegin
CREATE FUNCTION public.assign_feedback_profile_epoch_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    caller_role TEXT;
    prior_tenant TEXT;
    prior_user TEXT;
    has_profile BOOLEAN := FALSE;
    assigned_epoch BIGINT;
BEGIN
    caller_role := current_setting('role',true);
    prior_tenant := current_setting('app.tenant_id',true);
    prior_user := current_setting('app.user_id',true);
    IF caller_role='vane_app' AND (
       prior_tenant IS DISTINCT FROM NEW.tenant_id::text OR
       prior_user IS DISTINCT FROM NEW.user_id::text
    ) THEN
        RAISE EXCEPTION 'feedback writer scope does not match subject'
          USING ERRCODE='42501';
    END IF;

    -- Same per-tenant admission root as lockTenantAdmissionRoot. This must be
    -- first even for a legacy feedback binary so purge cannot pass admission
    -- while the trigger later creates a tenant-owned fact.
    PERFORM pg_advisory_xact_lock(
      hashtextextended(
        'vane/tenant-admission/v1/'||NEW.tenant_id::text,
        1447120453
      )
    );
    PERFORM 1 FROM public.tenants WHERE id=NEW.tenant_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'feedback tenant is absent'
          USING ERRCODE='23503';
    END IF;

    PERFORM set_config('app.tenant_id',NEW.tenant_id::text,true);
    PERFORM set_config('app.user_id',NEW.user_id::text,true);

    PERFORM 1 FROM public.memberships
     WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'feedback owner membership is absent'
          USING ERRCODE='23503';
    END IF;

    -- The historic schema has independent delivery_id/user_id/tenant_id FKs,
    -- not a composite subject FK. Bind the exact delivery owner here so a
    -- same-tenant user cannot teach their profile from another user's card.
    PERFORM 1 FROM public.deliveries
     WHERE id=NEW.delivery_id
       AND tenant_id=NEW.tenant_id
       AND user_id=NEW.user_id
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'feedback delivery owner does not match subject'
          USING ERRCODE='23503';
    END IF;

    SELECT TRUE INTO has_profile
      FROM public.profiles
     WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
     FOR SHARE;

    IF has_profile THEN
        SELECT active_epoch INTO assigned_epoch
          FROM public.profile_claim_states
         WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
         FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'profile claim state missing for feedback owner'
              USING ERRCODE='23514';
        END IF;
    ELSE
        assigned_epoch := 0;
    END IF;

    NEW.profile_epoch := assigned_epoch;
    PERFORM set_config('app.tenant_id',COALESCE(prior_tenant,''),true);
    PERFORM set_config('app.user_id',COALESCE(prior_user,''),true);
    RETURN NEW;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION public.assign_feedback_profile_epoch_v1()
    FROM PUBLIC;
CREATE TRIGGER assign_feedback_profile_epoch_v1
BEFORE INSERT ON feedbacks
FOR EACH ROW EXECUTE FUNCTION public.assign_feedback_profile_epoch_v1();

-- +goose Down

-- Acquire producer roots first. A concurrent writer finishes before the
-- downgrade audit, and any committed nonzero epoch fact makes Down refuse.
LOCK TABLE profiles IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_states IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_epochs IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claims IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_receipts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE feedbacks IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM profile_claim_states WHERE active_epoch<>0) OR
       EXISTS (SELECT 1 FROM profile_epochs WHERE profile_epoch<>0) OR
       EXISTS (SELECT 1 FROM profile_claims WHERE profile_epoch<>0) OR
       EXISTS (SELECT 1 FROM profile_claim_events WHERE profile_epoch<>0) OR
       EXISTS (SELECT 1 FROM profile_claim_receipts WHERE profile_epoch<>0) OR
       EXISTS (SELECT 1 FROM feedbacks WHERE profile_epoch<>0) THEN
        RAISE EXCEPTION
          'refusing 066 downgrade after nonzero profile epoch facts exist';
    END IF;
END
$$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS assign_feedback_profile_epoch_v1 ON feedbacks;
DROP FUNCTION IF EXISTS public.assign_feedback_profile_epoch_v1();
ALTER POLICY tenant_isolation ON feedbacks
    USING (
      tenant_id IS NOT DISTINCT FROM
      (SELECT current_setting('app.tenant_id',true))::bigint
    )
    WITH CHECK (
      tenant_id IS NOT DISTINCT FROM
      (SELECT current_setting('app.tenant_id',true))::bigint
    );
DROP TRIGGER IF EXISTS enforce_profile_epoch_state_v1 ON profile_claim_states;
DROP FUNCTION IF EXISTS public.enforce_profile_epoch_state_v1();
DROP TRIGGER IF EXISTS require_active_profile_epoch_v1 ON profiles;
DROP TRIGGER IF EXISTS require_active_profile_epoch_v1 ON profile_claim_receipts;
DROP TRIGGER IF EXISTS require_active_profile_epoch_v1 ON profile_claim_events;
DROP TRIGGER IF EXISTS require_active_profile_epoch_v1 ON profile_claims;
DROP FUNCTION IF EXISTS public.require_active_profile_epoch_v1();
DROP TRIGGER IF EXISTS enforce_profile_epoch_seed_v1 ON profile_epochs;
DROP FUNCTION IF EXISTS public.enforce_profile_epoch_seed_v1();

DROP INDEX idx_feedbacks_profile_epoch;
DROP INDEX idx_profile_claim_events_active_epoch;
DROP INDEX idx_profile_claims_active_epoch;

ALTER TABLE profile_claim_receipts
    DROP CONSTRAINT fk_profile_claim_receipt_event_epoch_scope,
    DROP CONSTRAINT fk_profile_claim_receipt_epoch,
    DROP COLUMN profile_epoch;
ALTER TABLE profile_claim_events
    DROP CONSTRAINT fk_profile_claim_event_target_event_epoch_scope,
    DROP CONSTRAINT fk_profile_claim_event_result_claim_epoch_scope,
    DROP CONSTRAINT fk_profile_claim_event_target_claim_epoch_scope,
    DROP CONSTRAINT fk_profile_claim_event_epoch,
    DROP CONSTRAINT uq_profile_claim_event_epoch_scope,
    DROP COLUMN profile_epoch;
ALTER TABLE profile_claims
    DROP CONSTRAINT fk_profile_claim_supersedes_epoch_scope,
    DROP CONSTRAINT fk_profile_claim_epoch,
    DROP CONSTRAINT uq_profile_claim_epoch_scope,
    DROP COLUMN profile_epoch;
ALTER TABLE feedbacks
    DROP CONSTRAINT ck_feedback_profile_epoch,
    DROP COLUMN profile_epoch;
ALTER TABLE profile_claim_states
    DROP CONSTRAINT fk_profile_claim_state_active_epoch;
REVOKE ALL ON profile_epochs FROM vane_profile_claim_editor;
DROP TABLE profile_epochs;
ALTER TABLE profile_claim_states
    DROP CONSTRAINT ck_profile_claim_state_active_epoch,
    DROP COLUMN active_epoch;
