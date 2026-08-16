-- 134: multi-workspace identity control plane.
--
-- Existing tenant IDs and user_sessions remain authoritative. This migration
-- only adds product metadata and a one-time, email-bound team invitation
-- ledger; old snapshots and cookies keep their wire/storage shape.

-- +goose Up

ALTER TABLE tenants
    ADD COLUMN display_name TEXT NOT NULL DEFAULT '',
    -- Raw/internal tenant fixtures remain valid as team-shaped workspaces;
    -- account creation paths explicitly insert personal + owner together.
    ADD COLUMN workspace_kind TEXT NOT NULL DEFAULT 'team',
    ADD COLUMN personal_owner_user_id BIGINT REFERENCES users (id),
    ADD COLUMN seat_limit INT NOT NULL DEFAULT 1,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Existing tenants were created as one-person workspaces. Pick their current
-- owner deterministically; if historical corruption left no owner, the row is
-- quarantined as a team workspace instead of inventing an identity.
WITH tenant_owners AS (
    SELECT DISTINCT ON (m.tenant_id) m.tenant_id,m.user_id
    FROM memberships m
    WHERE m.role='owner'
    ORDER BY m.tenant_id,m.created_at,m.user_id
), ranked AS (
    SELECT tenant_id,user_id,
           row_number() OVER (PARTITION BY user_id ORDER BY tenant_id) AS owner_rank
    FROM tenant_owners
)
UPDATE tenants t
SET personal_owner_user_id = CASE WHEN ranked.owner_rank=1 THEN ranked.user_id END,
    workspace_kind = CASE WHEN ranked.owner_rank=1 THEN 'personal' ELSE 'team' END,
    seat_limit = CASE WHEN ranked.owner_rank=1 THEN 1 ELSE 5 END
FROM ranked
WHERE ranked.tenant_id=t.id;

UPDATE tenants t
SET display_name = CASE
        WHEN COALESCE(NULLIF(btrim(u.name), ''), NULLIF(btrim(u.email), '')) IS NULL
            THEN '个人工作区'
        ELSE COALESCE(NULLIF(btrim(u.name), ''), NULLIF(btrim(u.email), '')) || ' 的个人工作区'
    END
FROM users u
WHERE u.id=t.personal_owner_user_id;

UPDATE tenants
SET workspace_kind = 'team', seat_limit = GREATEST(seat_limit, 5),
    display_name = COALESCE(NULLIF(display_name, ''), '团队工作区')
WHERE personal_owner_user_id IS NULL;

ALTER TABLE tenants
    ADD CONSTRAINT ck_tenants_workspace_kind
        CHECK (workspace_kind IN ('personal', 'team')),
    ADD CONSTRAINT ck_tenants_seat_limit
        CHECK (seat_limit BETWEEN 1 AND 1000),
    ADD CONSTRAINT ck_tenants_personal_owner
        CHECK ((workspace_kind = 'personal') = (personal_owner_user_id IS NOT NULL));

CREATE UNIQUE INDEX uq_tenants_personal_owner
    ON tenants (personal_owner_user_id)
    WHERE workspace_kind = 'personal';

ALTER TABLE memberships
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD CONSTRAINT ck_memberships_role CHECK (role IN ('owner', 'admin', 'member'));

CREATE TABLE workspace_invites (
    id          BIGSERIAL PRIMARY KEY,
    token_hash  BYTEA       NOT NULL UNIQUE,
    tenant_id   BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    email       TEXT        NOT NULL,
    role        TEXT        NOT NULL DEFAULT 'member',
    issued_by   BIGINT      NOT NULL REFERENCES users (id),
    expires_at  TIMESTAMPTZ NOT NULL,
    consumed_by BIGINT      REFERENCES users (id),
    consumed_at TIMESTAMPTZ,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_workspace_invites_email_normalized
        CHECK (email = lower(btrim(email)) AND position('@' IN email) > 1),
    CONSTRAINT ck_workspace_invites_role CHECK (role IN ('admin', 'member')),
    CONSTRAINT ck_workspace_invites_expiry CHECK (expires_at > created_at),
    CONSTRAINT ck_workspace_invites_consumption CHECK (
        (consumed_by IS NULL AND consumed_at IS NULL) OR
        (consumed_by IS NOT NULL AND consumed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_workspace_invites_pending_email
    ON workspace_invites (tenant_id, email)
    WHERE consumed_at IS NULL AND revoked_at IS NULL;
CREATE INDEX idx_workspace_invites_expiry
    ON workspace_invites (expires_at)
    WHERE consumed_at IS NULL AND revoked_at IS NULL;

CREATE TABLE workspace_audit_events (
    id             BIGSERIAL PRIMARY KEY,
    tenant_id      BIGINT      NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    actor_user_id  BIGINT      NOT NULL REFERENCES users (id),
    target_user_id BIGINT      REFERENCES users (id),
    event_type     TEXT        NOT NULL,
    metadata       JSONB       NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_workspace_audit_events_tenant_created
    ON workspace_audit_events (tenant_id, created_at DESC, id DESC);

-- These control-plane tables are RLS-enabled now so later least-privilege
-- runtime roles cannot accidentally bypass them. The migration owner remains
-- the current Store authority; every Store mutation additionally locks and
-- validates the exact tenant+actor membership in the same transaction.
ALTER TABLE workspace_invites ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_invites FORCE ROW LEVEL SECURITY;
ALTER TABLE workspace_audit_events FORCE ROW LEVEL SECURITY;

-- The application must establish these transaction-local claims before every
-- access. Token-bound bootstrap is needed only to resolve an invitation before
-- its tenant is known; raw tokens are never placed in a GUC or database row.
CREATE POLICY workspace_invites_exact_scope ON workspace_invites
    FOR ALL TO PUBLIC
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
        OR encode(token_hash, 'hex') IS NOT DISTINCT FROM
            NULLIF(current_setting('app.workspace_invite_hash', true), '')
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
    );

CREATE POLICY workspace_audit_exact_scope ON workspace_audit_events
    FOR ALL TO PUBLIC
    USING (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
        AND actor_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id', true), '')::bigint
    )
    WITH CHECK (
        tenant_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.tenant_id', true), '')::bigint
        AND actor_user_id IS NOT DISTINCT FROM
            NULLIF(current_setting('app.user_id', true), '')::bigint
    );

GRANT SELECT,INSERT,UPDATE,DELETE ON workspace_invites,workspace_audit_events TO vane_app;
GRANT USAGE,SELECT ON workspace_invites_id_seq,workspace_audit_events_id_seq TO vane_app;

-- +goose Down

DROP TABLE workspace_audit_events;
DROP TABLE workspace_invites;
ALTER TABLE memberships DROP CONSTRAINT ck_memberships_role;
ALTER TABLE memberships DROP COLUMN updated_at;
DROP INDEX uq_tenants_personal_owner;
ALTER TABLE tenants DROP CONSTRAINT ck_tenants_personal_owner;
ALTER TABLE tenants DROP CONSTRAINT ck_tenants_seat_limit;
ALTER TABLE tenants DROP CONSTRAINT ck_tenants_workspace_kind;
ALTER TABLE tenants DROP COLUMN updated_at;
ALTER TABLE tenants DROP COLUMN seat_limit;
ALTER TABLE tenants DROP COLUMN personal_owner_user_id;
ALTER TABLE tenants DROP COLUMN workspace_kind;
ALTER TABLE tenants DROP COLUMN display_name;
