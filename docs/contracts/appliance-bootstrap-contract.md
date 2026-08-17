# Vane appliance bootstrap contract v1

Status: first binary-deployment slice. This contract deliberately does not
claim that PostgreSQL, Temporal, TLS, backups, or the credential-vault KEK are
already self-provisioned.

## Goal

A fresh Vane installation can create its first platform owner from Web without
placing an owner password or invite in an environment file. Existing
installations must never re-enter setup mode merely because a process restarts
or an external provider is unavailable.

## Authority and states

- The installation is `active` once tenant `1` has any `owner` membership.
  Suspended/deleting tenants still count as installed; suspension must not open
  a public owner-replacement path.
- With no owner, the process enters `setup_required` mode. Only `/healthz`,
  `/readyz`, `GET /api/setup/status`, and `POST /api/setup/claim` are mounted.
  Agent, Temporal workers, A2A, Feishu, Telegram, schedulers, and paid provider
  clients remain unconstructed and unreachable.
- A successful claim requests a graceful process restart. The normal runtime
  is assembled only after the next startup observes the durable owner.

## One-time token

- The raw token is 32 random bytes encoded as unpadded base64url. It is stored
  only in the host state directory as a regular mode-0600 file. The database
  stores SHA-256(token), issue time, expiry, and state; logs and HTTP status
  responses never contain the token or its digest.
- Operators retrieve it locally with `vane setup-token` (for a container,
  `docker exec <container> vane setup-token`). The token expires after 30
  minutes and is automatically, atomically rotated at expiry rather than
  extended. The command therefore always reads the current host-local token.
- Database prepare, claim, owner creation, and token consumption share a
  PostgreSQL transaction advisory lock. Concurrent claims can create exactly
  one owner. Claim rechecks every condition after the password-cost precheck.
- The public status endpoint exposes only `setup_required` or `active`. A site
  visitor cannot obtain the token or infer its digest.

## Claim transaction

`POST /api/setup/claim {token,email,password}` performs, in order:

1. exact Origin check, bounded body, source rate limit, token/email/password
   shape checks;
2. cheap token-digest precheck before Argon2;
3. locked recheck that no owner exists, tenant 1 is active, and token is pending
   and unexpired;
4. creation of the email/password user, exact tenant-1 owner membership, and
   hash-only first session in the same transaction;
5. replacement of pending token data with a claimed audit receipt and commit.

No invite or personal tenant is created by this path. The consumed token cannot
be replayed, and the raw host file is removed on the next active startup.

## Deployment boundary and roadmap

The production systemd unit uses `StateDirectory=vane`, so the rest of the
filesystem remains read-only. This slice still requires an already reachable,
migrated PostgreSQL URL. The container/appliance end state must add, in later
independent gates:

1. embedded compose/package defaults and automatic database migration;
2. generated credential-vault KEK stored in a mounted secret/state volume;
3. authenticated setup steps for LLM, information providers, channels, public
   URL/TLS, email, backup/restore, and connectivity tests;
4. database-backed non-secret runtime settings and restart/drain orchestration;
5. export/import and disaster recovery that preserve encrypted credential
   generations without exposing plaintext.

Until those gates land, documentation and UI must say that database/runtime
infrastructure configuration is still deployment-owned; this v1 only removes
manual first-owner credential provisioning.
