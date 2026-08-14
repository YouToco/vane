-- +goose Up

-- Aggregate-card questions are real post-reset user activity, but they cannot
-- be attributed to any one delivery without poisoning profile learning. Keep
-- the restore barrier in a separate append-only ledger that Evolver never
-- reads. profile_epoch=0 is the same explicit pre-profile sentinel used by
-- feedbacks; later epochs are assigned under the shared feedback epoch fence.
CREATE TABLE profile_epoch_activities (
    id                BIGSERIAL   PRIMARY KEY,
    tenant_id         BIGINT      NOT NULL,
    user_id           BIGINT      NOT NULL,
    profile_epoch     BIGINT      NOT NULL DEFAULT 0,
    activity_kind     TEXT        NOT NULL,
    app_identity      TEXT        NOT NULL,
    inbound_key       TEXT        NOT NULL,
    request_digest    CHAR(64)    NOT NULL,
    source_message_id TEXT        NOT NULL,
    delivery_set_digest CHAR(64)  NOT NULL,
    wrapped_context   TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_profile_epoch_activity_kind
      CHECK (activity_kind='aggregate_question'),
    CONSTRAINT ck_profile_epoch_activity_inbound_key
      CHECK (octet_length(inbound_key) BETWEEN 1 AND 512),
    CONSTRAINT ck_profile_epoch_activity_app_identity
      CHECK (octet_length(app_identity) BETWEEN 1 AND 255),
    CONSTRAINT ck_profile_epoch_activity_digest
      CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_activity_source_message
      CHECK (octet_length(source_message_id) BETWEEN 1 AND 512),
    CONSTRAINT ck_profile_epoch_activity_delivery_set_digest
      CHECK (delivery_set_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_epoch_activity_wrapped_context
      CHECK (char_length(wrapped_context) BETWEEN 1 AND 32768),
    CONSTRAINT fk_profile_epoch_activity_tenant
      FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_profile_epoch_activity_user
      FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE (user_id,app_identity,inbound_key)
);

CREATE INDEX idx_profile_epoch_activities_subject_epoch
  ON profile_epoch_activities (tenant_id,user_id,profile_epoch,id);

-- +goose StatementBegin
CREATE FUNCTION public.assign_profile_epoch_activity_v1()
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
  aggregate_count BIGINT;
  aggregate_ids TEXT;
BEGIN
  caller_role:=current_setting('role',true);
  prior_tenant:=current_setting('app.tenant_id',true);
  prior_user:=current_setting('app.user_id',true);
  IF caller_role='vane_app' AND (
     prior_tenant IS DISTINCT FROM NEW.tenant_id::text OR
     prior_user IS DISTINCT FROM NEW.user_id::text
  ) THEN
    RAISE EXCEPTION 'profile epoch activity writer scope does not match subject'
      USING ERRCODE='42501';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended(
    'vane/tenant-admission/v1/'||NEW.tenant_id::text,1447120453));
  PERFORM 1 FROM public.tenants WHERE id=NEW.tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'profile epoch activity tenant is absent'
      USING ERRCODE='23503';
  END IF;
  PERFORM set_config('app.tenant_id',NEW.tenant_id::text,true);
  PERFORM set_config('app.user_id',NEW.user_id::text,true);
  PERFORM 1 FROM public.memberships
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR KEY SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'profile epoch activity membership is absent'
      USING ERRCODE='23503';
  END IF;

  -- The message itself is the aggregate identity. Lock every matching
  -- delivery and require at least two; an arbitrary anchor delivery is never
  -- persisted as the question target.
  SELECT count(*),string_agg(id::text,',' ORDER BY id)
    INTO aggregate_count,aggregate_ids
    FROM (
      SELECT id FROM public.deliveries
       WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id
         AND feishu_message_id=NEW.source_message_id
         AND feishu_message_id<>''
       ORDER BY id FOR KEY SHARE
    ) locked_deliveries;
  IF aggregate_count<2 THEN
    RAISE EXCEPTION 'profile epoch activity source is not an aggregate message'
      USING ERRCODE='23514';
  END IF;
  IF NEW.delivery_set_digest<>encode(sha256(convert_to(
    'vane.aggregate-question-delivery-set/v1|'||aggregate_ids,'UTF8'
  )),'hex') THEN
    RAISE EXCEPTION 'profile epoch activity delivery set changed'
      USING ERRCODE='23514';
  END IF;

  INSERT INTO public.profile_feedback_epoch_fences
    (tenant_id,user_id,last_feedback_id)
  VALUES (NEW.tenant_id,NEW.user_id,0)
  ON CONFLICT (tenant_id,user_id) DO NOTHING;
  PERFORM 1 FROM public.profile_feedback_epoch_fences
   WHERE tenant_id=NEW.tenant_id AND user_id=NEW.user_id FOR UPDATE;

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
      RAISE EXCEPTION 'profile claim state missing for epoch activity owner'
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

REVOKE ALL ON FUNCTION public.assign_profile_epoch_activity_v1() FROM PUBLIC;
CREATE TRIGGER assign_profile_epoch_activity_v1
BEFORE INSERT ON profile_epoch_activities
FOR EACH ROW EXECUTE FUNCTION public.assign_profile_epoch_activity_v1();

-- Pre-delivery-lookup replay is the point of this narrow reader: a retry must
-- survive later message-id repair without granting vane_app cross-user ledger
-- visibility. tenant_id remains a storage/RLS scope; the inbound receipt key
-- is lifetime-global for one user+app.
-- +goose StatementBegin
CREATE FUNCTION public.lookup_profile_epoch_activity_receipt_v1(
  expected_user_id BIGINT,
  expected_app_identity TEXT,
  expected_inbound_key TEXT
)
RETURNS TABLE(tenant_id BIGINT,request_digest TEXT,wrapped_context TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
DECLARE
  caller_role TEXT;
  user_context TEXT;
  app_context TEXT;
BEGIN
  caller_role:=current_setting('role',true);
  user_context:=current_setting('app.user_id',true);
  app_context:=current_setting('app.app_identity',true);
  IF caller_role IS DISTINCT FROM 'vane_app' OR
     user_context IS NULL OR user_context !~ '^[1-9][0-9]*$' OR
     user_context::bigint IS DISTINCT FROM expected_user_id OR
     app_context IS DISTINCT FROM expected_app_identity OR
     expected_app_identity IS NULL OR
     octet_length(expected_app_identity) NOT BETWEEN 1 AND 255 OR
     expected_inbound_key IS NULL OR
     octet_length(expected_inbound_key) NOT BETWEEN 1 AND 512 THEN
    RAISE EXCEPTION 'profile epoch activity receipt scope is invalid'
      USING ERRCODE='42501';
  END IF;
  RETURN QUERY
    SELECT a.tenant_id,a.request_digest::text,a.wrapped_context
      FROM public.profile_epoch_activities a
     WHERE a.user_id=expected_user_id
       AND a.app_identity=expected_app_identity
       AND a.inbound_key=expected_inbound_key;
END $$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION
  public.lookup_profile_epoch_activity_receipt_v1(BIGINT,TEXT,TEXT)
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
  public.lookup_profile_epoch_activity_receipt_v1(BIGINT,TEXT,TEXT)
  TO vane_app;

REVOKE ALL ON profile_epoch_activities
  FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
       vane_profile_epoch_editor;
REVOKE ALL ON SEQUENCE profile_epoch_activities_id_seq
  FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor,
       vane_profile_epoch_editor;

ALTER TABLE profile_epoch_activities ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_epoch_activities FORCE ROW LEVEL SECURITY;
CREATE POLICY profile_epoch_activities_exact_user
ON profile_epoch_activities
TO vane_app,vane_profile_claim_editor,vane_profile_epoch_editor
USING (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
)
WITH CHECK (
  tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
  user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
);

GRANT INSERT (
  tenant_id,user_id,profile_epoch,activity_kind,app_identity,inbound_key,
  request_digest,source_message_id,delivery_set_digest,wrapped_context
) ON profile_epoch_activities TO vane_app;
GRANT SELECT (
  id,tenant_id,user_id,profile_epoch,activity_kind,app_identity,inbound_key,
  request_digest,source_message_id,delivery_set_digest,wrapped_context
) ON profile_epoch_activities TO vane_app;
GRANT USAGE,SELECT ON SEQUENCE profile_epoch_activities_id_seq TO vane_app;
GRANT SELECT (id,tenant_id,user_id,profile_epoch)
  ON profile_epoch_activities TO vane_profile_epoch_editor;
GRANT SELECT (tenant_id,user_id,profile_epoch,activity_kind)
  ON profile_epoch_activities TO vane_profile_claim_editor;

-- +goose Down

SELECT pg_advisory_xact_lock(1447120453,1095976527);
LOCK TABLE profile_epoch_activities IN ACCESS EXCLUSIVE MODE;

-- Once a barrier exists, 067 cannot represent it and could incorrectly allow
-- restore. Refuse downgrade rather than silently widening restore authority.
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM profile_epoch_activities) THEN
    RAISE EXCEPTION
      '069 down refused: aggregate-question restore barriers exist'
      USING ERRCODE='P0001';
  END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER assign_profile_epoch_activity_v1 ON profile_epoch_activities;
DROP FUNCTION public.assign_profile_epoch_activity_v1();
DROP FUNCTION
  public.lookup_profile_epoch_activity_receipt_v1(BIGINT,TEXT,TEXT);
DROP TABLE profile_epoch_activities;
