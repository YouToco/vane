-- 062: 来源级画像纠正 authority。
--
-- profiles 继续是读取投影；claim/event ledger 是不可变事实。既有画像只能诚实地
-- 回填为 source_unavailable，不能凭空补造历史证据。

-- +goose Up

-- Fence every legacy profiles writer before taking the backfill snapshot.
-- SHARE ROW EXCLUSIVE blocks INSERT/UPDATE/DELETE (ROW EXCLUSIVE) while still
-- allowing the read surface to remain available, and is held to commit.
LOCK TABLE profiles IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE profiles
    ADD CONSTRAINT uq_profiles_tenant_user UNIQUE (tenant_id,user_id);

CREATE TABLE profile_claim_states (
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    version            BIGINT      NOT NULL DEFAULT 0,
    evidence_generation BIGINT     NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id),
    CONSTRAINT ck_profile_claim_state_version CHECK (version >= 0),
    CONSTRAINT ck_profile_claim_state_generation CHECK (evidence_generation >= 0),
    CONSTRAINT fk_profile_claim_state_profile
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES profiles (tenant_id, user_id)
);

CREATE TABLE profile_claims (
    id                 BIGSERIAL   PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    field_name         TEXT        NOT NULL,
    claim_value        TEXT        NOT NULL,
    source_state       TEXT        NOT NULL,
    source_ref_type    TEXT,
    source_ref         TEXT,
    generation         BIGINT      NOT NULL DEFAULT 0,
    supersedes_claim_id BIGINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_profile_claim_field
        CHECK (field_name IN ('industry','occupation','tag','summary')),
    CONSTRAINT ck_profile_claim_source
        CHECK (source_state IN ('evidence','manual','source_unavailable')),
    CONSTRAINT ck_profile_claim_source_ref CHECK (
        (source_state='evidence' AND source_ref_type='feedback_range'
          AND source_ref IS NOT NULL) OR
        (source_state<>'evidence' AND source_ref_type IS NULL
          AND source_ref IS NULL)
    ),
    CONSTRAINT ck_profile_claim_generation CHECK (
        generation >= 0 AND
        ((source_state='evidence' AND generation > 0) OR
         (source_state<>'evidence' AND generation=0))
    ),
    CONSTRAINT ck_profile_claim_value_length CHECK (
        length(claim_value) BETWEEN 1 AND
          CASE WHEN field_name='summary' THEN 240 ELSE 4000 END
    ),
    CONSTRAINT uq_profile_claim_scope UNIQUE (tenant_id, user_id, id),
    CONSTRAINT fk_profile_claim_state
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES profile_claim_states (tenant_id, user_id),
    CONSTRAINT fk_profile_claim_supersedes_scope
        FOREIGN KEY (tenant_id, user_id, supersedes_claim_id)
        REFERENCES profile_claims (tenant_id, user_id, id)
);

CREATE INDEX idx_profile_claims_scope
    ON profile_claims (tenant_id, user_id, id);
CREATE INDEX idx_profile_claims_generation
    ON profile_claims (tenant_id, user_id, field_name, generation DESC, id);

CREATE TABLE profile_claim_events (
    id                 BIGSERIAL   PRIMARY KEY,
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    actor_user_id      BIGINT      NOT NULL,
    event_kind         TEXT        NOT NULL,
    target_claim_id    BIGINT,
    result_claim_id    BIGINT,
    target_event_id    BIGINT,
    expected_version   BIGINT      NOT NULL,
    result_version     BIGINT      NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_profile_claim_event_actor_self
        CHECK (actor_user_id=user_id),
    CONSTRAINT ck_profile_claim_event_kind
        CHECK (event_kind IN ('correct','suppress','pin','revoke')),
    CONSTRAINT ck_profile_claim_event_shape CHECK (
        (event_kind='correct' AND target_claim_id IS NOT NULL
          AND result_claim_id IS NOT NULL AND target_event_id IS NULL) OR
        (event_kind IN ('suppress','pin') AND target_claim_id IS NOT NULL
          AND result_claim_id IS NULL AND target_event_id IS NULL) OR
        (event_kind='revoke' AND target_claim_id IS NULL
          AND result_claim_id IS NULL AND target_event_id IS NOT NULL)
    ),
    CONSTRAINT ck_profile_claim_event_versions
        CHECK (expected_version >= 0 AND result_version=expected_version+1),
    CONSTRAINT uq_profile_claim_event_scope UNIQUE (tenant_id, user_id, id),
    CONSTRAINT fk_profile_claim_event_state
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES profile_claim_states (tenant_id, user_id),
    CONSTRAINT fk_profile_claim_event_target_claim_scope
        FOREIGN KEY (tenant_id, user_id, target_claim_id)
        REFERENCES profile_claims (tenant_id, user_id, id),
    CONSTRAINT fk_profile_claim_event_result_claim_scope
        FOREIGN KEY (tenant_id, user_id, result_claim_id)
        REFERENCES profile_claims (tenant_id, user_id, id),
    CONSTRAINT fk_profile_claim_event_target_event_scope
        FOREIGN KEY (tenant_id, user_id, target_event_id)
        REFERENCES profile_claim_events (tenant_id, user_id, id)
);

CREATE UNIQUE INDEX uq_profile_claim_event_revoke_once
    ON profile_claim_events (tenant_id, user_id, target_event_id)
    WHERE event_kind='revoke';
CREATE INDEX idx_profile_claim_events_scope
    ON profile_claim_events (tenant_id, user_id, id);

CREATE TABLE profile_claim_receipts (
    tenant_id          BIGINT      NOT NULL,
    user_id            BIGINT      NOT NULL,
    idempotency_key    TEXT        NOT NULL,
    request_digest     TEXT        NOT NULL,
    event_id           BIGINT      NOT NULL,
    response_payload   JSONB       NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id, idempotency_key),
    CONSTRAINT ck_profile_claim_receipt_key
        CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    CONSTRAINT ck_profile_claim_receipt_digest
        CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_profile_claim_receipt_response
        CHECK (jsonb_typeof(response_payload)='object'),
    CONSTRAINT fk_profile_claim_receipt_state
        FOREIGN KEY (tenant_id, user_id)
        REFERENCES profile_claim_states (tenant_id, user_id),
    CONSTRAINT fk_profile_claim_receipt_event_scope
        FOREIGN KEY (tenant_id, user_id, event_id)
        REFERENCES profile_claim_events (tenant_id, user_id, id)
);

-- 旧数据没有可核验来源，只能标成 source_unavailable。
INSERT INTO profile_claim_states (tenant_id,user_id)
SELECT tenant_id,user_id FROM profiles;

INSERT INTO profile_claims
    (tenant_id,user_id,field_name,claim_value,source_state)
SELECT tenant_id,user_id,'industry',industry,'source_unavailable'
  FROM profiles WHERE industry<>''
UNION ALL
SELECT tenant_id,user_id,'occupation',occupation,'source_unavailable'
  FROM profiles WHERE occupation<>''
UNION ALL
SELECT p.tenant_id,p.user_id,'tag',t,'source_unavailable'
  FROM profiles p CROSS JOIN LATERAL unnest(p.tags) t;

-- Keep migration backfill byte-for-byte aligned with splitSummaryClaims:
-- trim the whole string, consume one Unicode character at a time, flush after
-- every delimiter (including consecutive punctuation) or 240 runes, and trim
-- each emitted piece.
WITH RECURSIVE inputs AS (
    SELECT tenant_id,user_id,
           regexp_replace(
             regexp_replace(summary,'^[[:space:]]+','','g'),
             '[[:space:]]+$','','g'
           ) AS summary
      FROM profiles
     WHERE summary<>''
), split AS (
    SELECT tenant_id,user_id,summary,0 AS pos,char_length(summary) AS total,
           ''::text AS current,0 AS current_len,NULL::text AS emitted
      FROM inputs
     WHERE summary<>''
    UNION ALL
    SELECT tenant_id,user_id,summary,pos+1,total,
           CASE WHEN current_len+1=240 OR ch IN
                     ('。','！','？','!','?','；',';',E'\n','.')
                THEN '' ELSE current||ch END,
           CASE WHEN current_len+1=240 OR ch IN
                     ('。','！','？','!','?','；',';',E'\n','.')
                THEN 0 ELSE current_len+1 END,
           CASE WHEN current_len+1=240 OR ch IN
                     ('。','！','？','!','?','；',';',E'\n','.')
                THEN regexp_replace(
                       regexp_replace(current||ch,'^[[:space:]]+','','g'),
                       '[[:space:]]+$','','g'
                     )
                ELSE NULL END
      FROM split
      CROSS JOIN LATERAL (
        SELECT substring(summary FROM pos+1 FOR 1) AS ch
      ) next_char
     WHERE pos<total
), pieces AS (
    SELECT tenant_id,user_id,pos AS end_pos,emitted AS value
      FROM split WHERE emitted IS NOT NULL AND emitted<>''
    UNION ALL
    SELECT tenant_id,user_id,pos AS end_pos,
           regexp_replace(
             regexp_replace(current,'^[[:space:]]+','','g'),
             '[[:space:]]+$','','g'
           ) AS value
      FROM split
     WHERE pos=total AND current<>''
)
INSERT INTO profile_claims
    (tenant_id,user_id,field_name,claim_value,source_state)
SELECT tenant_id,user_id,'summary',value,'source_unavailable'
  FROM pieces
 WHERE value<>''
 ORDER BY tenant_id,user_id,end_pos;

-- 独立 claim authority：不能扩张 060 vane_profile_editor 的 summary 只读边界。
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname='vane_profile_claim_editor'
    ) THEN
        BEGIN
            CREATE ROLE vane_profile_claim_editor
                NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
                NOLOGIN NOINHERIT NOBYPASSRLS;
        EXCEPTION
            WHEN duplicate_object OR unique_violation THEN NULL;
        END;
    END IF;
END $$;
-- +goose StatementEnd

ALTER ROLE vane_profile_claim_editor
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOLOGIN NOINHERIT NOBYPASSRLS;
ALTER ROLE vane_profile_claim_editor RESET ALL;
ALTER ROLE vane_profile_claim_editor
    SET search_path = pg_catalog, public, pg_temp;

GRANT vane_profile_claim_editor TO CURRENT_USER
    WITH ADMIN FALSE, SET TRUE, INHERIT FALSE;

-- +goose StatementBegin
DO $$
BEGIN
    IF pg_has_role('vane_profile_claim_editor','vane_app','MEMBER') OR
       pg_has_role('vane_app','vane_profile_claim_editor','MEMBER') OR
       pg_has_role('vane_profile_claim_editor','vane_profile_editor','MEMBER') OR
       pg_has_role('vane_profile_editor','vane_profile_claim_editor','MEMBER') THEN
        RAISE EXCEPTION '062: claim editor must be unrelated to app/edit roles';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
          JOIN pg_roles granted_role ON granted_role.oid=am.roleid
         WHERE granted_role.rolname='vane_profile_claim_editor'
           AND member_role.rolname<>CURRENT_USER
    ) THEN
        RAISE EXCEPTION '062: only migration owner may enter claim editor';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_auth_members am
          JOIN pg_roles member_role ON member_role.oid=am.member
         WHERE member_role.rolname='vane_profile_claim_editor'
    ) THEN
        RAISE EXCEPTION '062: claim editor must not enter another role';
    END IF;
END $$;
-- +goose StatementEnd

-- Normalize any pre-existing cluster role/default privilege state before
-- installing the exact capability below.
REVOKE ALL ON profile_claim_states
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
REVOKE ALL ON profile_claims
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
REVOKE ALL ON profile_claim_events
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
REVOKE ALL ON profile_claim_receipts
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
REVOKE ALL ON SEQUENCE profile_claims_id_seq
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
REVOKE ALL ON SEQUENCE profile_claim_events_id_seq
    FROM PUBLIC,vane_app,vane_profile_editor,vane_profile_claim_editor;
REVOKE ALL ON profiles FROM vane_profile_claim_editor;
REVOKE ALL ON memberships FROM vane_profile_claim_editor;
REVOKE INSERT (
    tenant_id,user_id,industry,occupation,tags,updated_at
) ON profiles FROM vane_profile_editor;
REVOKE UPDATE (industry,occupation,tags,removed_tags,updated_at)
    ON profiles FROM vane_profile_editor;

ALTER TABLE profile_claim_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_claim_states FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_claims FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_claim_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_claim_events FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_claim_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_claim_receipts FORCE ROW LEVEL SECURITY;

CREATE POLICY profile_claim_states_exact_user ON profile_claim_states
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY profile_claims_exact_user ON profile_claims
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY profile_claim_events_exact_user ON profile_claim_events
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY profile_claim_receipts_exact_user ON profile_claim_receipts
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

GRANT USAGE ON SCHEMA public TO vane_profile_claim_editor;
GRANT SELECT (tenant_id,user_id) ON memberships TO vane_profile_claim_editor;
GRANT SELECT (
    tenant_id,user_id,industry,occupation,tags,removed_tags,
    summary,last_evolved_feedback_id,created_at,updated_at
) ON profiles TO vane_profile_claim_editor;
GRANT INSERT (
    tenant_id,user_id,industry,occupation,tags,updated_at
) ON profiles TO vane_profile_claim_editor;
GRANT USAGE ON SEQUENCE profiles_id_seq TO vane_profile_claim_editor;
GRANT UPDATE (
    industry,occupation,tags,removed_tags,summary,
    last_evolved_feedback_id,updated_at
)
    ON profiles TO vane_profile_claim_editor;
GRANT SELECT,INSERT,UPDATE ON profile_claim_states TO vane_profile_claim_editor;
GRANT SELECT,INSERT ON profile_claims TO vane_profile_claim_editor;
GRANT SELECT,INSERT ON profile_claim_events TO vane_profile_claim_editor;
GRANT SELECT,INSERT ON profile_claim_receipts TO vane_profile_claim_editor;
GRANT USAGE,SELECT ON SEQUENCE profile_claims_id_seq
    TO vane_profile_claim_editor;
GRANT USAGE,SELECT ON SEQUENCE profile_claim_events_id_seq
    TO vane_profile_claim_editor;

-- profiles also carries the general 022 tenant_isolation policy. Normalize
-- its historical bare cast too: PostgreSQL may evaluate every restrictive
-- policy, so a safe claim-role policy alone cannot mask an empty-GUC cast
-- failure in the older policy.
ALTER POLICY tenant_isolation ON profiles
    USING (
      tenant_id IS NOT DISTINCT FROM
      NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id IS NOT DISTINCT FROM
      NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint
    );

CREATE POLICY profile_claim_editor_identity ON profiles AS RESTRICTIVE
    FOR ALL TO vane_profile_claim_editor
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    )
    WITH CHECK (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );
CREATE POLICY profile_claim_editor_identity ON memberships AS RESTRICTIVE
    FOR SELECT TO vane_profile_claim_editor
    USING (
      tenant_id=NULLIF((SELECT current_setting('app.tenant_id',true)),'')::bigint AND
      user_id=NULLIF((SELECT current_setting('app.user_id',true)),'')::bigint
    );

-- +goose Down

-- Acquire producer tables in the same order as writers before checking
-- emptiness. A concurrent uncommitted producer must finish first; its commit
-- then becomes visible to the fence and prevents destructive downgrade.
LOCK TABLE profile_claim_states IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claims IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_claim_receipts IN ACCESS EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM profile_claim_states) OR
       EXISTS (SELECT 1 FROM profile_claims) OR
       EXISTS (SELECT 1 FROM profile_claim_events) OR
       EXISTS (SELECT 1 FROM profile_claim_receipts) THEN
        RAISE EXCEPTION
          'refusing to drop non-empty profile claim authority ledger';
    END IF;
END $$;
-- +goose StatementEnd

DROP TABLE profile_claim_receipts;
DROP TABLE profile_claim_events;
DROP TABLE profile_claims;
DROP TABLE profile_claim_states;
ALTER TABLE profiles DROP CONSTRAINT uq_profiles_tenant_user;
DROP POLICY IF EXISTS profile_claim_editor_identity ON profiles;
DROP POLICY IF EXISTS profile_claim_editor_identity ON memberships;
ALTER POLICY tenant_isolation ON profiles
    USING (
      tenant_id IS NOT DISTINCT FROM
      (SELECT current_setting('app.tenant_id',true))::bigint
    )
    WITH CHECK (
      tenant_id IS NOT DISTINCT FROM
      (SELECT current_setting('app.tenant_id',true))::bigint
    );
REVOKE UPDATE (
    industry,occupation,tags,removed_tags,summary,
    last_evolved_feedback_id,updated_at
)
    ON profiles FROM vane_profile_claim_editor;
REVOKE INSERT (
    tenant_id,user_id,industry,occupation,tags,updated_at
) ON profiles FROM vane_profile_claim_editor;
REVOKE SELECT (
    tenant_id,user_id,industry,occupation,tags,removed_tags,
    summary,last_evolved_feedback_id,created_at,updated_at
) ON profiles FROM vane_profile_claim_editor;
REVOKE USAGE ON SEQUENCE profiles_id_seq FROM vane_profile_claim_editor;
REVOKE SELECT (tenant_id,user_id) ON memberships
    FROM vane_profile_claim_editor;
REVOKE USAGE ON SCHEMA public FROM vane_profile_claim_editor;
GRANT INSERT (
    tenant_id,user_id,industry,occupation,tags,updated_at
) ON profiles TO vane_profile_editor;
GRANT UPDATE (industry,occupation,tags,removed_tags,updated_at)
    ON profiles TO vane_profile_editor;
REVOKE vane_profile_claim_editor FROM CURRENT_USER;
